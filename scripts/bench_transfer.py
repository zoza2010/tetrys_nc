#!/usr/bin/env python3
"""Benchmark gen RaptorQ UDP transfer on localhost."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--size", default="64M")
    p.add_argument("--port", type=int, default=9100)
    p.add_argument("--gen-k", type=int, default=48)
    p.add_argument("--rate", type=float, default=2000.0)
    args = p.parse_args()

    testdata = ROOT / "testdata"
    testdata.mkdir(exist_ok=True)
    src = testdata / f"bench_{args.size}.bin"
    dst = testdata / f"bench_{args.size}.recv.bin"

    print(f"=== generating {src.name} ({args.size}) ===")
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "tetrys_nc",
            "genfile",
            "--output",
            str(src),
            "--size",
            args.size,
        ],
        cwd=ROOT,
    )

    server_cmd = [
        sys.executable,
        "-m",
        "tetrys_nc",
        "server",
        "--file",
        str(src),
        "--port",
        str(args.port),
        "--skip-hash",
        "--gen-k",
        str(args.gen_k),
        "--rate",
        str(args.rate),
        "--ramp-s",
        "0",
        "--payload-size",
        "1350",
    ]
    client_cmd = [
        sys.executable,
        "-m",
        "tetrys_nc",
        "client",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
        "--output",
        str(dst),
    ]

    print(f"=== transfer {src.name} ===")
    t0 = time.monotonic()
    srv = subprocess.Popen(server_cmd, cwd=ROOT)
    try:
        time.sleep(0.5)
        subprocess.check_call(client_cmd, cwd=ROOT)
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=5)
        except subprocess.TimeoutExpired:
            srv.kill()
    elapsed = time.monotonic() - t0
    size = src.stat().st_size
    print(f"OK in {elapsed:.2f}s ({size / elapsed / (1024 * 1024):.1f} MiB/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
