"""RaptorQ UDP file server."""

from __future__ import annotations

import argparse
from pathlib import Path

from .block_state import WAN_INITIAL_REPAIR_PCT, WAN_PACE_CAP_MBIT
from .block_xfer import run_block_server


def _root_and_default(dir_arg: Path | None, file_arg: Path | None) -> tuple[Path, str]:
    if file_arg is not None and file_arg.exists():
        resolved = file_arg.resolve()
        if dir_arg is None:
            return resolved.parent, resolved.name
        root = dir_arg.resolve()
        try:
            return root, str(resolved.relative_to(root))
        except ValueError:
            return root, resolved.name
    root = (dir_arg or Path(".")).resolve()
    default = "" if file_arg is None else str(file_arg)
    if Path(default).is_absolute():
        default = Path(default).name
    return root, default


def _cli_pace(wan: bool, rate_mbit: float | None) -> tuple[float, bool]:
    """Explicit --rate locks pace (CC off). Omitted --rate keeps CC on."""
    locked = rate_mbit is not None
    rate = float(rate_mbit) if locked else 0.0
    if wan and rate <= 0:
        rate = WAN_PACE_CAP_MBIT
    elif rate <= 0:
        rate = 1500.0
    return rate, not locked


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="RaptorQ UDP file server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="root directory; client READY paths are relative to this",
    )
    p.add_argument(
        "--file",
        type=Path,
        default=None,
        help="optional default relative file if the client omits a path",
    )
    p.add_argument("--skip-hash", action="store_true")
    p.add_argument(
        "--wan",
        action="store_true",
        help="WAN: symbol size 1350, 64 MiB active block window",
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
        default=None,
        dest="rate_mbit",
        help="lock UDP send rate in Mbit/s (disables rate search). "
        "Omit to search (WAN start 850, cap 1600)",
    )
    p.add_argument(
        "--ramp-s",
        type=float,
        default=0.0,
        help="seconds to ease-in pace from 0 to start rate (0=immediate blast)",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="exit after one transfer (default: stay idle and wait for the next client)",
    )
    p.add_argument(
        "--gen-k",
        type=int,
        default=None,
        help="symbols per independent coding block (default 768)",
    )
    p.add_argument(
        "--gen-overhead",
        type=int,
        default=None,
        help="initial RaptorQ repair percent (WAN default 24)",
    )
    args = p.parse_args(argv)

    symbol = 1350 if args.wan or args.payload_size >= 8000 else args.payload_size
    rate, rate_cc = _cli_pace(args.wan, args.rate_mbit)
    if args.gen_overhead is not None:
        overhead_pct = args.gen_overhead
    elif args.wan:
        overhead_pct = WAN_INITIAL_REPAIR_PCT
    else:
        overhead_pct = 0
    gen_k = args.gen_k if args.gen_k is not None else 768
    root, default_file = _root_and_default(args.dir, args.file)

    return run_block_server(
        args.host,
        args.port,
        root,
        default_file=default_file,
        symbol_size=symbol,
        block_k=gen_k,
        initial_repair_pct=overhead_pct,
        rate_mbit=rate,
        ramp_s=args.ramp_s,
        skip_hash=args.skip_hash,
        rate_cc=rate_cc,
        once=args.once,
    )


if __name__ == "__main__":
    raise SystemExit(main())
