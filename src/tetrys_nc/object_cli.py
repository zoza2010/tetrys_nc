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


def main_objserver(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Object-mux UDP server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--early", type=Path, required=True)
    p.add_argument("--late", type=Path, default=None)
    p.add_argument("--late-delay", type=float, default=0.4)
    p.add_argument("--fec", type=int, default=WAN_INITIAL_REPAIR_PCT)
    p.add_argument("--rate-mbit", type=float, default=WAN_PACE_CAP_MBIT)
    args = p.parse_args(argv)
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
        symbol_size=WAN_SYMBOL_SIZE,
        block_k=WAN_BLOCK_K,
        initial_repair_pct=args.fec,
        active_bytes=WAN_ACTIVE_BYTES,
        rate_mbit=args.rate_mbit,
    )


def main_objclient(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Object-mux UDP client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--timeout", type=float, default=180.0)
    p.add_argument(
        "--progress",
        action="store_true",
        help="TTY bars: group total plus the in-flight file name",
    )
    args = p.parse_args(argv)
    print(f"connecting object-mux to udp://{args.host}:{args.port}", flush=True)
    return run_object_client(
        args.host, args.port, args.output,
        active_bytes=WAN_ACTIVE_BYTES,
        timeout_s=args.timeout,
        file_progress=args.progress,
    )
