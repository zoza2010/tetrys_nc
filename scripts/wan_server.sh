#!/usr/bin/env bash
# Start the Russia WAN tetrys server for tetrys-fm / client tests.
# Always uses the full testdata tree (Maya ISO, blobs, cmp_*), never a cwd-relative guess.
set -euo pipefail

REPO="${REPO:-/home/sysops/quic_tests/tetrys_nc}"
ROOT="${TETRYS_DIR:-$REPO/testdata}"
PORT="${IPERF_PORT:-7494}"
FEC="${GEN_OVERHEAD:-20}"
LOG="${TETRYS_SERVER_LOG:-/tmp/tetrys-server.log}"

cd "$REPO"
fuser -k "${PORT}/udp" 2>/dev/null || true
sleep 0.3
ss -ulnp | grep -q ":${PORT}" && {
  echo "ERROR: UDP ${PORT} still busy" >&2
  exit 2
}

PYTHONUNBUFFERED=1 nohup .venv/bin/python -u -m tetrys_nc server \
  --dir "$ROOT" \
  --port "$PORT" \
  --skip-hash \
  --allow-upload \
  --gen-overhead "$FEC" \
  >"$LOG" 2>&1 </dev/null &
echo "pid=$! root=$ROOT port=$PORT fec=${FEC}%"
sleep 0.6
head -3 "$LOG"
ss -ulnp | grep ":${PORT}" || echo "NO_PORT"
