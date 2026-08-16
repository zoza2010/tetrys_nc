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

echo "=== 64M gen K=48 ==="
"$PY" scripts/bench_transfer.py --size 64M --port 9121 --gen-k 48 --gen-overhead 8 || true

if [[ -f testdata/blob_1g.bin ]]; then
  echo "=== 1G gen transfer ==="
  "$PY" -m tetrys_nc server --file testdata/blob_1g.bin --port 9123 --skip-hash \
    --gen-k 48 --gen-overhead 8 --rate 2000 --ramp-s 0 --payload-size 1350 \
    >testdata/bench_server.log 2>&1 &
  SPID=$!
  sleep 0.8
  "$PY" -m tetrys_nc client --host 127.0.0.1 --port 9123 --output testdata/received_bench_1g.bin \
    | tee testdata/bench_client.log
  kill "$SPID" 2>/dev/null || true
  wait "$SPID" 2>/dev/null || true
  echo "=== server log tail ==="
  tail -n 30 testdata/bench_server.log || true
fi

echo "=== ALL COMPLETE $(date) ==="
