"""Reorder-insensitive RaptorQ block transfer v2 client."""

from __future__ import annotations

import argparse
from pathlib import Path

from .block_xfer import run_block_client


def run_client(host: str, port: int, output: Path, wan: bool = False) -> int:
    print(f"connecting v2 to udp://{host}:{port}" + (" (wan buffers)" if wan else ""))
    return run_block_client(host, port, output, wan=wan)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gen RaptorQ UDP file client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--output", type=Path, default=Path("received.bin"))
    p.add_argument(
        "--wan",
        action="store_true",
        help="WAN: enlarge socket buffers",
    )
    args = p.parse_args(argv)
    return run_client(args.host, args.port, args.output, wan=args.wan)


if __name__ == "__main__":
    raise SystemExit(main())
