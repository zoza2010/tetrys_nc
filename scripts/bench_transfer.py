#!/usr/bin/env python3
"""Benchmark Tetrys UDP transfer on localhost."""

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
    p.add_argument("--payload-size", type=int, default=32768)
    p.add_argument("--window", type=int, default=8192)
    p.add_argument("--redundancy", type=int, default=0)
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

    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tetrys_nc",
            "server",
            "--file",
            str(src),
            "--port",
            str(args.port),
            "--payload-size",
            str(args.payload_size),
            "--window",
            str(args.window),
            "--redundancy",
            str(args.redundancy),
            "--skip-hash",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        time.sleep(0.5)
        print(f"=== client transfer payload={args.payload_size} window={args.window} ===")
        t0 = time.monotonic()
        client = subprocess.run(
            [
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
                "--window",
                str(args.window),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=600,
        )
        elapsed = time.monotonic() - t0
        print(client.stdout)
        if client.stderr:
            print(client.stderr, file=sys.stderr)
        print(f"wall_time={elapsed:.2f}s exit={client.returncode}")

        # Wait for server to finish
        try:
            out, _ = server.communicate(timeout=10)
            print(out)
        except subprocess.TimeoutExpired:
            server.kill()
            out, _ = server.communicate()
            print(out)

        return client.returncode
    finally:
        if server.poll() is None:
            server.kill()


if __name__ == "__main__":
    raise SystemExit(main())
