"""Gen RaptorQ UDP file server entrypoint."""

from __future__ import annotations

import argparse
from pathlib import Path

from .gen_xfer import run_gen_server


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gen RaptorQ UDP file server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--file", type=Path, required=True)
    p.add_argument("--skip-hash", action="store_true")
    p.add_argument(
        "--wan",
        action="store_true",
        help="WAN: symbol size 1350; default rate 1000 Mbit/s if --rate omitted",
    )
    p.add_argument(
        "--payload-size",
        type=int,
        default=32768,
        help="symbol size T (ignored when --wan; default 1350 on WAN)",
    )
    p.add_argument(
        "--rate-mbit",
        "--rate",
        type=float,
        default=0.0,
        dest="rate_mbit",
        help="target UDP send rate in Mbit/s (alias: --rate). WAN default 1000",
    )
    p.add_argument(
        "--ramp-s",
        type=float,
        default=2.0,
        help="seconds to ease-in pace from 0 to --rate (0=immediate blast)",
    )
    p.add_argument("--gen-k", type=int, default=48, help="symbols per generation (~K)")
    p.add_argument(
        "--gen-overhead",
        type=int,
        default=8,
        help="RaptorQ repair overhead percent (default 8)",
    )
    args = p.parse_args(argv)

    symbol = 1350 if args.wan or args.payload_size >= 8000 else args.payload_size
    rate = args.rate_mbit
    if args.wan and rate <= 0:
        rate = 1000.0
    elif rate <= 0:
        rate = 1500.0

    return run_gen_server(
        args.host,
        args.port,
        args.file,
        symbol_size=symbol,
        gen_k=args.gen_k,
        overhead_pct=args.gen_overhead,
        rate_mbit=rate,
        ramp_s=args.ramp_s,
        skip_hash=args.skip_hash,
    )


if __name__ == "__main__":
    raise SystemExit(main())
