"""RaptorQ UDP file server."""

from __future__ import annotations

import argparse
from pathlib import Path

from .block_state import (
    WAN_BLOCK_K,
    WAN_INITIAL_REPAIR_PCT,
    WAN_PACE_CAP_MBIT,
    WAN_SYMBOL_SIZE,
)
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


def _cli_pace(rate_mbit: float | None) -> tuple[float, bool]:
    """Explicit --rate locks pace (CC off). Omitted --rate keeps CC on."""
    locked = rate_mbit is not None
    rate = float(rate_mbit) if locked else WAN_PACE_CAP_MBIT
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
        "--payload-size",
        type=int,
        default=WAN_SYMBOL_SIZE,
        help=f"symbol size T (default {WAN_SYMBOL_SIZE})",
    )
    p.add_argument(
        "--rate-mbit",
        "--rate",
        type=float,
        default=None,
        dest="rate_mbit",
        help="lock UDP send rate in Mbit/s (disables rate search). "
        "Omit to search from 850 Mbit (CC; no channel cap)",
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
    p.add_argument("--gen-k", type=int, default=WAN_BLOCK_K)
    p.add_argument(
        "--gen-overhead",
        type=int,
        default=WAN_INITIAL_REPAIR_PCT,
        help=f"initial RaptorQ repair percent (default {WAN_INITIAL_REPAIR_PCT})",
    )
    args = p.parse_args(argv)
    rate, rate_cc = _cli_pace(args.rate_mbit)
    root, default_file = _root_and_default(args.dir, args.file)
    return run_block_server(
        args.host,
        args.port,
        root,
        default_file=default_file,
        symbol_size=args.payload_size,
        block_k=args.gen_k,
        initial_repair_pct=args.gen_overhead,
        rate_mbit=rate,
        ramp_s=args.ramp_s,
        skip_hash=args.skip_hash,
        rate_cc=rate_cc,
        once=args.once,
    )


if __name__ == "__main__":
    raise SystemExit(main())
