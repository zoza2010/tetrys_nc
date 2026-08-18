#!/bin/bash
# Compare tetrys_nc UDP vs plain TCP on the same port (different proto).
# Usage on server VM:
#   ./scripts/compare_udp_tcp.sh server
# Usage on client VM:
#   ./scripts/compare_udp_tcp.sh client SERVER_IP [runs]

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${PY:-.venv/bin/python}"
PORT=7494
FILE="${FILE:-testdata/blob_1g.bin}"
RUNS="${3:-3}"

run_tcp_server_once() {
  fuser -k "${PORT}/tcp" 2>/dev/null || true
  sleep 1
  "$PY" scripts/tcp_xfer.py server --file "$FILE" --port "$PORT" > /tmp/tcp_srv.log 2>&1 &
  sleep 1
}

run_udp_server_once() {
  fuser -k "${PORT}/udp" 2>/dev/null || true
  sleep 1
  rm -f /tmp/tetrys_srv.log
  nohup "$PY" -u -m tetrys_nc server --file "$FILE" --port "$PORT" --wan --skip-hash \
    --rate 950 --ramp-s 2 --gen-k 192 \
    > /tmp/tetrys_srv.log 2>&1 &
  sleep 3
}

if [[ "${1:-}" == "server" ]]; then
  echo "Server-side helper only prints file info:"
  ls -lh "$FILE"
  exit 0
fi

HOST="${1:?need server IP}"
MODE="${2:-both}"

tcp_client() {
  rm -f testdata/received_tcp_1g.bin
  "$PY" scripts/tcp_xfer.py client --host "$HOST" --port "$PORT" \
    --output testdata/received_tcp_1g.bin 2>&1 | grep -E "^OK:|^META:"
}

udp_client() {
  rm -f testdata/received_1g.bin
  "$PY" -u -m tetrys_nc client --host "$HOST" --port "$PORT" --wan \
    --output testdata/received_1g.bin 2>&1 | grep "^OK:"
}

if [[ "$MODE" == "tcp" || "$MODE" == "both" ]]; then
  echo "=== TCP ($RUNS runs, port $PORT) ==="
  for i in $(seq 1 "$RUNS"); do
    echo "-- tcp run $i --"
    ssh -o BatchMode=yes sysops@"$HOST" "cd ~/quic_tests/tetrys_nc && fuser -k ${PORT}/tcp 2>/dev/null; sleep 1; .venv/bin/python scripts/tcp_xfer.py server --file testdata/blob_1g.bin --port $PORT > /tmp/tcp_srv.log 2>&1 & sleep 1" 2>/dev/null || {
      echo "(run tcp server locally on $HOST first)"
      exit 1
    }
    tcp_client || true
    sleep 2
  done
fi

if [[ "$MODE" == "udp" || "$MODE" == "both" ]]; then
  echo "=== UDP tetrys ($RUNS runs, port $PORT) ==="
  for i in $(seq 1 "$RUNS"); do
    echo "-- udp run $i --"
    udp_client || true
    sleep 2
  done
fi
