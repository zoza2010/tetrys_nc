#!/bin/bash
cd /Users/mac/PycharmProjects/tetrys_nc || exit 1
export PYTHONPATH=src
PY=.venv/bin/python
OUT=testdata/bench_results.txt
mkdir -p testdata

exec > >(tee "$OUT") 2>&1

echo "=== START $(date) ==="
"$PY" -m pytest -q || echo "PYTEST HAD FAILURES"
echo "=== AFTER PYTEST ==="

echo "=== 64M payload=32768 ==="
"$PY" scripts/bench_transfer.py --size 64M --port 9121 --payload-size 32768 --window 8192 --redundancy 32 || true

echo "=== 64M payload=60000 ==="
"$PY" scripts/bench_transfer.py --size 64M --port 9122 --payload-size 60000 --window 4096 --redundancy 32 || true

if [[ -f testdata/blob_1g.bin ]]; then
  echo "=== 1G TRANSFER payload=32768 ==="
  "$PY" -m tetrys_nc server --file testdata/blob_1g.bin --port 9123 --skip-hash --payload-size 32768 --window 8192 --redundancy 32 >testdata/bench_server.log 2>&1 &
  SPID=$!
  sleep 0.8
  "$PY" -m tetrys_nc client --host 127.0.0.1 --port 9123 --output testdata/received_bench_1g.bin --window 8192 | tee testdata/bench_client.log
  kill "$SPID" 2>/dev/null || true
  wait "$SPID" 2>/dev/null || true
  echo "=== server log tail ==="
  tail -n 30 testdata/bench_server.log || true

  echo "=== 1G TRANSFER payload=60000 ==="
  "$PY" -m tetrys_nc server --file testdata/blob_1g.bin --port 9124 --skip-hash --payload-size 60000 --window 4096 --redundancy 32 >testdata/bench_server2.log 2>&1 &
  SPID=$!
  sleep 0.8
  "$PY" -m tetrys_nc client --host 127.0.0.1 --port 9124 --output testdata/received_bench_1g_60k.bin --window 4096 | tee testdata/bench_client2.log
  kill "$SPID" 2>/dev/null || true
  wait "$SPID" 2>/dev/null || true
  echo "=== server2 log tail ==="
  tail -n 30 testdata/bench_server2.log || true
fi

echo "=== ALL COMPLETE $(date) ==="
