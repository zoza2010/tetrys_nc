"""Tetrys NC file server — sends a file to a client over UDP."""

from __future__ import annotations

import argparse
import hashlib
import mmap
import select
import socket
import sys
import threading
import time
from pathlib import Path

from .encoder import EncoderConfig, TetrysEncoder
from .netutil import try_set_buffer
from .packets import (
    FinPacket,
    MetaPacket,
    ReadyPacket,
    WindowUpdatePacket,
    parse_packet,
)


def file_sha256(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            data = f.read(chunk)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def run_server(
    host: str,
    port: int,
    file_path: Path,
    payload_size: int = 32768,
    max_window: int = 8192,
    redundancy_every: int = 32,
    pace_us: float = 0.0,
    skip_hash: bool = False,
) -> int:
    file_path = file_path.resolve()
    if not file_path.is_file():
        print(f"error: file not found: {file_path}", file=sys.stderr)
        return 1

    file_size = file_path.stat().st_size
    if skip_hash:
        digest = ""
        print(f"file={file_path.name} size={file_size} (hash skipped)")
    else:
        print(f"computing sha256 of {file_path} ({file_size} bytes)...")
        digest = file_sha256(file_path)
        print(f"file={file_path.name} size={file_size} sha256={digest[:16]}...")

    total_symbols = (file_size + payload_size - 1) // payload_size if file_size else 0
    print(
        f"symbols={total_symbols} payload={payload_size} "
        f"window={max_window} redundancy={redundancy_every}"
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try_set_buffer(sock, socket.SO_SNDBUF, 4 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_RCVBUF, 1 * 1024 * 1024)
    sock.bind((host, port))
    sock.setblocking(False)
    print(f"Tetrys server listening on udp://{host}:{port}")
    print("waiting for client READY...")

    client_addr: tuple[str, int] | None = None
    enc = TetrysEncoder(
        EncoderConfig(
            max_window=max_window,
            redundancy_every=redundancy_every,
            payload_size=payload_size,
        )
    )
    enc_lock = threading.Lock()
    fin_from_client = threading.Event()
    stop_feedback = threading.Event()

    deadline = time.monotonic() + 600
    while client_addr is None:
        if time.monotonic() > deadline:
            print("timeout waiting for client", file=sys.stderr)
            return 1
        r, _, _ = select.select([sock], [], [], 1.0)
        if not r:
            continue
        data, addr = sock.recvfrom(65535)
        try:
            pkt = parse_packet(data)
        except ValueError:
            continue
        if isinstance(pkt, ReadyPacket):
            client_addr = addr
            if pkt.max_window > 0:
                enc.cfg.max_window = min(enc.cfg.max_window, pkt.max_window)
            print(f"client ready from {addr}, window={enc.cfg.max_window}")

    assert client_addr is not None

    meta = MetaPacket(file_size, file_path.name, payload_size, digest).pack()
    for _ in range(3):
        sock.sendto(meta, client_addr)

    def feedback_loop() -> None:
        while not stop_feedback.is_set():
            r, _, _ = select.select([sock], [], [], 0.05)
            if not r:
                continue
            while True:
                try:
                    data, addr = sock.recvfrom(65535)
                except BlockingIOError:
                    break
                if addr != client_addr:
                    continue
                try:
                    pkt = parse_packet(data)
                except ValueError:
                    continue
                if isinstance(pkt, WindowUpdatePacket):
                    with enc_lock:
                        enc.apply_feedback(pkt.cumulative_ack, pkt.plr_byte)
                elif isinstance(pkt, ReadyPacket):
                    sock.sendto(meta, client_addr)
                elif isinstance(pkt, FinPacket):
                    fin_from_client.set()
                    return

    fb_thread = threading.Thread(target=feedback_loop, name="tetrys-feedback", daemon=True)
    fb_thread.start()

    t0 = time.monotonic()
    last_progress = t0
    fin_sent = False
    batch = 2048
    file_offset = 0

    try:
        with file_path.open("rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                eof = file_size == 0
                while True:
                    if fin_from_client.is_set():
                        elapsed = time.monotonic() - t0
                        _print_summary(enc, file_size, elapsed)
                        return 0

                    sent = 0
                    while not eof and sent < batch:
                        with enc_lock:
                            can = enc.can_accept()
                        if not can:
                            break
                        if file_offset >= file_size:
                            eof = True
                            break
                        end = min(file_offset + payload_size, file_size)
                        raw = mm[file_offset:end]
                        file_offset = end
                        if len(raw) < payload_size:
                            chunk = bytes(raw) + b"\x00" * (payload_size - len(raw))
                        else:
                            chunk = bytes(raw)

                        with enc_lock:
                            # Copy out of reusable encoder buffer before unlock
                            wire = bytes(enc.add_source(chunk))
                            coded = enc.maybe_coded()
                        while True:
                            try:
                                sock.sendto(wire, client_addr)
                                break
                            except BlockingIOError:
                                select.select([], [sock], [], 0.005)
                        if coded is not None:
                            try:
                                sock.sendto(coded, client_addr)
                            except BlockingIOError:
                                pass
                        sent += 1
                        if pace_us > 0:
                            time.sleep(pace_us / 1_000_000.0)

                    if eof:
                        with enc_lock:
                            win = enc.window_size
                        if win == 0:
                            if not fin_sent:
                                fin = FinPacket(True, total_symbols).pack()
                                for _ in range(3):
                                    sock.sendto(fin, client_addr)
                                fin_sent = True
                            if fin_from_client.wait(0.1):
                                elapsed = time.monotonic() - t0
                                _print_summary(enc, file_size, elapsed)
                                return 0
                            sock.sendto(FinPacket(True, total_symbols).pack(), client_addr)
                        else:
                            with enc_lock:
                                c = enc.make_coded()
                                oldest = enc.oldest_id
                                wire = enc.pack_source_id(oldest) if oldest is not None else None
                            if c:
                                try:
                                    sock.sendto(c.pack(), client_addr)
                                except BlockingIOError:
                                    pass
                            if wire is not None:
                                try:
                                    sock.sendto(wire, client_addr)
                                except BlockingIOError:
                                    pass
                            time.sleep(0.0005)
                    elif sent == 0:
                        with enc_lock:
                            oldest = enc.oldest_id
                            wire = enc.pack_source_id(oldest) if oldest is not None else None
                        if wire is not None:
                            try:
                                sock.sendto(wire, client_addr)
                            except BlockingIOError:
                                pass
                        time.sleep(0.0002)

                    now = time.monotonic()
                    if now - last_progress >= 1.0:
                        last_progress = now
                        with enc_lock:
                            st = enc.stats()
                            done = enc.next_source_id
                        pct = 100.0 * done / total_symbols if total_symbols else 0
                        rate = file_offset / max(now - t0, 1e-6) / (1024 * 1024)
                        print(
                            f"progress {done}/{total_symbols} ({pct:.1f}%) "
                            f"win={st['window']} ack={st['cumulative_ack']} "
                            f"coded={st['sent_coded']} {rate:.1f} MiB/s"
                        )

                    if fin_sent and time.monotonic() - t0 > 3600:
                        print("timeout after FIN", file=sys.stderr)
                        return 1
            finally:
                mm.close()
    finally:
        stop_feedback.set()
        fb_thread.join(timeout=1.0)


def _print_summary(enc: TetrysEncoder, file_size: int, elapsed: float) -> None:
    st = enc.stats()
    goodput = file_size / max(elapsed, 1e-6) / (1024 * 1024)
    print(
        f"done in {elapsed:.2f}s — goodput {goodput:.2f} MiB/s — "
        f"source={st['sent_source']} coded={st['sent_coded']}"
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Tetrys NC UDP file server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--file", required=True, type=Path, help="file to send")
    p.add_argument("--payload-size", type=int, default=32768)
    p.add_argument("--window", type=int, default=8192)
    p.add_argument(
        "--redundancy",
        type=int,
        default=32,
        help="coded every N source packets (0=off; repair still via retransmit)",
    )
    p.add_argument("--pace-us", type=float, default=0.0)
    p.add_argument("--skip-hash", action="store_true")
    args = p.parse_args(argv)
    return run_server(
        args.host,
        args.port,
        args.file,
        payload_size=args.payload_size,
        max_window=args.window,
        redundancy_every=args.redundancy,
        pace_us=args.pace_us,
        skip_hash=args.skip_hash,
    )


if __name__ == "__main__":
    raise SystemExit(main())
