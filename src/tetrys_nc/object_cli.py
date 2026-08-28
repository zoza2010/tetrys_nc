"""CLI for object-mux: enqueue a directory (optional late dir) over one session."""

from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path

from .block_state import (
    WAN_ACTIVE_BYTES,
    WAN_BLOCK_K,
    WAN_INITIAL_REPAIR_PCT,
    WAN_PACE_CAP_MBIT,
    WAN_SYMBOL_SIZE,
)
from .object_frames import is_pack_name, split_for_session
from .object_xfer import ObjectSession, run_object_client, run_object_server


def _next_pack(objects: list[tuple[str, bytes]], start: int) -> int:
    n = start
    for name, _ in objects:
        if is_pack_name(name):
            n = max(n, int(Path(name).stem.rsplit("_", 1)[-1], 10) + 1)
    return n


def _put_dir(session: ObjectSession, folder: Path, pack_start: int) -> tuple[int, int]:
    if not folder.is_dir():
        return 0, pack_start
    files = [
        (p.name, p.read_bytes())
        for p in sorted(folder.iterdir())
        if p.is_file() and not p.name.startswith(".")
    ]
    objects = split_for_session(files, pack_start=pack_start)
    for name, blob in objects:
        session.put(name, blob)
    return len(files), _next_pack(objects, pack_start)


def _wan_defaults(wan: bool, fec: int, rate: float):
    if wan:
        return WAN_SYMBOL_SIZE, WAN_BLOCK_K, fec or WAN_INITIAL_REPAIR_PCT, WAN_ACTIVE_BYTES, rate or WAN_PACE_CAP_MBIT
    return 256, 64, fec or 14, 4 << 20, rate or 400.0


def main_objserver(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Object-mux UDP server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--early", type=Path, required=True)
    p.add_argument("--late", type=Path, default=None)
    p.add_argument("--late-delay", type=float, default=0.4)
    p.add_argument("--wan", action="store_true")
    p.add_argument("--fec", type=int, default=0)
    p.add_argument("--rate-mbit", type=float, default=0.0)
    args = p.parse_args(argv)
    symbol, block_k, fec, active, rate = _wan_defaults(args.wan, args.fec, args.rate_mbit)
    session = ObjectSession()

    def feed() -> None:
        n, pack_i = _put_dir(session, args.early, 0)
        print(f"object-mux queued early files={n}", flush=True)
        if args.late is not None:
            time.sleep(max(0.0, args.late_delay))
            n2, pack_i = _put_dir(session, args.late, pack_i)
            print(f"object-mux queued late files={n2}", flush=True)
        session.close()
        print("object-mux queue closed", flush=True)

    threading.Thread(target=feed, daemon=True).start()
    return run_object_server(
        args.host, args.port, session,
        symbol_size=symbol, block_k=block_k, initial_repair_pct=fec,
        active_bytes=active, rate_mbit=rate,
    )


def main_objclient(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Object-mux UDP client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--wan", action="store_true")
    p.add_argument("--timeout", type=float, default=180.0)
    args = p.parse_args(argv)
    print(f"connecting object-mux to udp://{args.host}:{args.port}", flush=True)
    return run_object_client(
        args.host, args.port, args.output,
        wan=args.wan,
        active_bytes=WAN_ACTIVE_BYTES if args.wan else 4 << 20,
        timeout_s=args.timeout,
    )
