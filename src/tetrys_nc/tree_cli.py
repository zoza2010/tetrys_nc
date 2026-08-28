"""CLI for tree-stream: recursive directory as one v2 session."""

from __future__ import annotations

import argparse
from pathlib import Path

from .block_state import (
    WAN_ACTIVE_BYTES,
    WAN_BLOCK_K,
    WAN_INITIAL_REPAIR_PCT,
    WAN_PACE_CAP_MBIT,
    WAN_SYMBOL_SIZE,
)
from .tree_xfer import run_tree_client, run_tree_server


def main_treeserver(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Tree-stream UDP server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--wan", action="store_true")
    p.add_argument("--fec", type=int, default=0)
    p.add_argument("--rate-mbit", type=float, default=0.0)
    args = p.parse_args(argv)
    symbol = WAN_SYMBOL_SIZE if args.wan else 256
    block_k = WAN_BLOCK_K if args.wan else 64
    fec = args.fec if args.fec > 0 else (WAN_INITIAL_REPAIR_PCT if args.wan else 14)
    active = WAN_ACTIVE_BYTES if args.wan else 4 << 20
    rate = args.rate_mbit
    if rate <= 0:
        rate = WAN_PACE_CAP_MBIT if args.wan else 400.0
    return run_tree_server(
        args.host,
        args.port,
        args.dir,
        symbol_size=symbol,
        block_k=block_k,
        initial_repair_pct=fec,
        active_bytes=active,
        rate_mbit=rate,
    )


def main_treeclient(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Tree-stream UDP client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--wan", action="store_true")
    p.add_argument("--timeout", type=float, default=180.0)
    args = p.parse_args(argv)
    active = WAN_ACTIVE_BYTES if args.wan else 4 << 20
    print(f"connecting tree-stream to udp://{args.host}:{args.port}", flush=True)
    return run_tree_client(
        args.host,
        args.port,
        args.output,
        wan=args.wan,
        active_bytes=active,
        timeout_s=args.timeout,
    )
