"""CLI for object-mux: enqueue a directory (optional late dir) over one session."""

from __future__ import annotations

import argparse
import hashlib
import threading
import time
from pathlib import Path

from .block_state import (
    WAN_ACTIVE_BYTES,
    WAN_BLOCK_K,
    WAN_INITIAL_REPAIR_PCT,
    WAN_SYMBOL_SIZE,
)
from .object_pack import is_pack_name, split_for_session
from .object_xfer import ObjectSession, run_object_client, run_object_server


def _put_dir(session: ObjectSession, folder: Path, pack_start: int) -> tuple[int, int]:
    if not folder.is_dir():
        return 0, pack_start
    files: list[tuple[str, bytes]] = []
    for path in sorted(folder.iterdir()):
        if path.is_file() and not path.name.startswith("."):
            files.append((path.name, path.read_bytes()))
    objects = split_for_session(files, pack_start=pack_start)
    next_pack = pack_start
    for name, blob in objects:
        session.put(name, blob)
        if is_pack_name(name):
            stem = Path(name).stem  # __pack_0000
            next_pack = max(next_pack, int(stem.rsplit("_", 1)[-1], 10) + 1)
    return len(files), next_pack


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
    symbol = WAN_SYMBOL_SIZE if args.wan else 256
    block_k = WAN_BLOCK_K if args.wan else 64
    fec = args.fec if args.fec > 0 else (WAN_INITIAL_REPAIR_PCT if args.wan else 14)
    active = WAN_ACTIVE_BYTES if args.wan else 4 << 20
    rate = args.rate_mbit
    if rate <= 0:
        rate = 2500.0 if args.wan else 400.0
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
        args.host,
        args.port,
        session,
        symbol_size=symbol,
        block_k=block_k,
        initial_repair_pct=fec,
        active_bytes=active,
        rate_mbit=rate,
    )


def main_objclient(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Object-mux UDP client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--wan", action="store_true")
    p.add_argument("--timeout", type=float, default=180.0)
    args = p.parse_args(argv)
    active = WAN_ACTIVE_BYTES if args.wan else 4 << 20
    print(f"connecting object-mux to udp://{args.host}:{args.port}", flush=True)
    return run_object_client(
        args.host,
        args.port,
        args.output,
        wan=args.wan,
        active_bytes=active,
        timeout_s=args.timeout,
    )


def write_manifest(folder: Path, dest: Path) -> None:
    lines = []
    for path in sorted(folder.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        rel = path.name
        lines.append(f"{digest} {path.stat().st_size} {rel}")
    dest.write_text("\n".join(sorted(lines)) + "\n")
