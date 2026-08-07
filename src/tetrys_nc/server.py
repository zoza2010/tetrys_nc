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


class RateLimiter:
    """Simple token-bucket pacing in bytes/sec."""

    def __init__(self, rate_bps: float, burst: float | None = None) -> None:
        self.max_rate = max(rate_bps, 1.0)  # hard ceiling from --rate/--rate-mbit
        self.min_rate = min(2_000_000.0, self.max_rate)  # floor, never above max
        self.rate = self.max_rate
        self.burst = burst if burst is not None else self.rate * 0.25
        self.tokens = self.burst
        self.updated = time.monotonic()

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * 0.25, self.rate * 0.05)

    def consume(self, nbytes: int) -> None:
        now = time.monotonic()
        elapsed = now - self.updated
        self.updated = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.tokens -= nbytes
        if self.tokens < 0:
            sleep_s = (-self.tokens) / self.rate
            time.sleep(sleep_s)
            self.updated = time.monotonic()
            self.tokens = 0.0


def run_server(
    host: str,
    port: int,
    file_path: Path,
    payload_size: int = 32768,
    max_window: int = 8192,
    redundancy_every: int = 32,
    coded_burst: int = 0,
    pace_us: float = 0.0,
    skip_hash: bool = False,
    wan: bool = False,
    rate_mbit: float = 0.0,
) -> int:
    file_path = file_path.resolve()
    if not file_path.is_file():
        print(f"error: file not found: {file_path}", file=sys.stderr)
        return 1

    if coded_burst <= 0:
        coded_burst = 1
    if wan:
        # WAN: start LEAN (CPU+bandwidth). Ramp repair only when PLR/NACK appear.
        # Previous 1x3 + degree=48 made pure-Python GF the bottleneck (~0.2 MiB/s)
        # even with plr=0.
        if payload_size >= 8000:
            payload_size = 1350
        if max_window < 2048:
            max_window = 2048
        if redundancy_every >= 32:
            # light periodic repair when healthy; ramps up on PLR
            redundancy_every = 8
        if coded_burst <= 1:
            coded_burst = 1
        code_degree = 8
        if rate_mbit <= 0:
            rate_mbit = 200.0
    else:
        code_degree = 8

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
        f"window={max_window} redundancy={redundancy_every}x{coded_burst} "
        f"wan={wan} rate_mbit={rate_mbit or 'unlimited'}"
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
            coded_burst=coded_burst,
            payload_size=payload_size,
            code_degree=code_degree,
        )
    )
    enc_lock = threading.Lock()
    fin_from_client = threading.Event()
    stop_feedback = threading.Event()
    ack_progress = {"ack": 0, "plr": 0, "t": time.monotonic()}

    # Pacing: WAN always; optional explicit --rate-mbit
    limiter: RateLimiter | None = None
    if rate_mbit > 0 or wan:
        start_rate = (rate_mbit if rate_mbit > 0 else 80.0) * 1_000_000 / 8
        limiter = RateLimiter(start_rate)

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
                    missing = pkt.missing_ids(limit=64)
                    with enc_lock:
                        enc.apply_feedback(pkt.cumulative_ack, pkt.plr_byte, missing)
                    ack_progress["ack"] = pkt.cumulative_ack
                    ack_progress["plr"] = pkt.plr_byte
                    ack_progress["t"] = time.monotonic()
                    # AIMD within [min_rate, max_rate] — never exceeds --rate-mbit
                    if limiter is not None:
                        plr = pkt.plr_byte * 100.0 / 256.0
                        if plr >= 40 and pkt.nb_missing_src >= 8:
                            limiter.set_rate(limiter.rate * 0.7)
                        elif plr >= 20 and pkt.nb_missing_src >= 4:
                            limiter.set_rate(limiter.rate * 0.9)
                        elif plr < 5 and pkt.nb_missing_src <= 2:
                            limiter.set_rate(limiter.rate * 1.05)
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
    batch = 64 if wan else 2048
    file_offset = 0
    last_ack_seen = 0
    stall_repairs = 0

    def send_datagram(wire: bytes) -> None:
        if limiter is not None:
            limiter.consume(len(wire))
        while True:
            try:
                sock.sendto(wire, client_addr)
                return
            except BlockingIOError:
                select.select([], [sock], [], 0.005)

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

                    # Repair only when there are real holes / losses — not on every WAN tick
                    with enc_lock:
                        repairs = enc.pop_nack_retransmit(limit=32 if wan else 4)
                        plr_b = enc.last_plr_byte
                        burst = enc.coded_burst
                        need_repair = bool(repairs) or plr_b > 0
                        n_repair_coded = burst if need_repair else 0
                        coded_repairs = []
                        for _ in range(n_repair_coded):
                            c = enc.make_coded(prefer_oldest=True)
                            if c is None:
                                break
                            coded_repairs.append(c.pack())
                    for wire in repairs:
                        send_datagram(wire)
                    for wire in coded_repairs:
                        send_datagram(wire)

                    # Detect ACK stall → light repair (not a coded flood)
                    cur_ack = ack_progress["ack"]
                    if cur_ack == last_ack_seen:
                        stall_repairs += 1
                    else:
                        stall_repairs = 0
                        last_ack_seen = cur_ack
                    if stall_repairs >= 5 and not eof:
                        with enc_lock:
                            oldest = enc.oldest_id
                            w = enc.pack_source_id(oldest) if oldest is not None else None
                            c = (
                                enc.make_coded(prefer_oldest=True)
                                if enc.last_plr_byte > 0
                                else None
                            )
                        if w:
                            send_datagram(w)
                        if c:
                            send_datagram(c.pack())

                    # Larger batches when the path is healthy (no holes)
                    cur_batch = batch
                    if wan and plr_b == 0 and not repairs:
                        cur_batch = 512

                    sent = 0
                    while not eof and sent < cur_batch:
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
                            wire = bytes(enc.add_source(chunk))
                            coded_list = enc.maybe_coded()
                        send_datagram(wire)
                        for coded in coded_list:
                            send_datagram(coded)
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
                                    send_datagram(fin)
                                fin_sent = True
                            if fin_from_client.wait(0.1):
                                elapsed = time.monotonic() - t0
                                _print_summary(enc, file_size, elapsed)
                                return 0
                            send_datagram(FinPacket(True, total_symbols).pack())
                        else:
                            with enc_lock:
                                repairs = enc.pop_nack_retransmit(limit=16)
                                c = enc.make_coded(prefer_oldest=True)
                                oldest = enc.oldest_id
                                w = enc.pack_source_id(oldest) if oldest is not None else None
                            for wire in repairs:
                                send_datagram(wire)
                            if c:
                                send_datagram(c.pack())
                            if w:
                                send_datagram(w)
                            time.sleep(0.001)
                    elif sent == 0:
                        with enc_lock:
                            repairs = enc.pop_nack_retransmit(limit=16)
                            c = enc.make_coded(prefer_oldest=True)
                            oldest = enc.oldest_id
                            w = enc.pack_source_id(oldest) if oldest is not None else None
                        for wire in repairs:
                            send_datagram(wire)
                        if c:
                            send_datagram(c.pack())
                        if w:
                            send_datagram(w)
                        time.sleep(0.001)

                    now = time.monotonic()
                    if now - last_progress >= 1.0:
                        last_progress = now
                        with enc_lock:
                            st = enc.stats()
                            done = enc.next_source_id
                        pct = 100.0 * done / total_symbols if total_symbols else 0
                        rate = file_offset / max(now - t0, 1e-6) / (1024 * 1024)
                        pace = (limiter.rate / (1024 * 1024)) if limiter else 0
                        cap = (limiter.max_rate / (1024 * 1024)) if limiter else 0
                        print(
                            f"progress {done}/{total_symbols} ({pct:.1f}%) "
                            f"win={st['window']} ack={st['cumulative_ack']} "
                            f"coded={st['sent_coded']} burst={st['coded_burst']} "
                            f"nack={st['nack_q']} plr={st['plr_byte']} "
                            f"pace={pace:.1f}/{cap:.1f}MiB/s app={rate:.1f} MiB/s"
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
    p.add_argument(
        "--coded-burst",
        type=int,
        default=0,
        help="coded packets per redundancy tick (0=auto; WAN default 3)",
    )
    p.add_argument("--pace-us", type=float, default=0.0)
    p.add_argument("--skip-hash", action="store_true")
    p.add_argument(
        "--wan",
        action="store_true",
        help="WAN profile: payload 1350, window 2048, light repair (ramps up on loss)",
    )
    p.add_argument(
        "--rate-mbit",
        "--rate",
        type=float,
        default=0.0,
        dest="rate_mbit",
        help="max UDP send rate in Mbit/s (alias: --rate). 0=unlimited unless --wan",
    )
    args = p.parse_args(argv)
    return run_server(
        args.host,
        args.port,
        args.file,
        payload_size=args.payload_size,
        max_window=args.window,
        redundancy_every=args.redundancy,
        coded_burst=args.coded_burst,
        pace_us=args.pace_us,
        skip_hash=args.skip_hash,
        wan=args.wan,
        rate_mbit=args.rate_mbit,
    )


if __name__ == "__main__":
    raise SystemExit(main())
