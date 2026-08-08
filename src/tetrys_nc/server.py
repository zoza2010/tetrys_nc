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
from .ratectl import DelayRateController, RateLimiter


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
        # WAN: no periodic coded on healthy path (NACK/stall repair only).
        if payload_size >= 8000:
            payload_size = 1350
        if max_window < 2048:
            max_window = 2048
        if redundancy_every >= 32:
            redundancy_every = 0
        if coded_burst <= 1:
            coded_burst = 1
        code_degree = 8
        if rate_mbit <= 0:
            rate_mbit = 200.0
        if redundancy_every > 0:
            print(
                f"warning: --redundancy {redundancy_every} on WAN adds coded load; "
                f"prefer --redundancy 0 (NACK repair only)",
                file=sys.stderr,
            )
    else:
        code_degree = 8

    from . import gf256

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
        f"wan={wan} rate_mbit={rate_mbit or 'unlimited'} gf={gf256.backend()}"
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
    # flight = max unacked source packets allowed (slow-start cwnd); WAN opens small
    ack_progress = {
        "ack": 0,
        "plr": 0,
        "t": time.monotonic(),
        "rtt_us": 0.0,
        "flight": 64 if wan else max_window,
    }

    # Pacing: WAN always; optional explicit --rate-mbit
    # Slow-start from a fraction of --rate, then delay-based climb (loss ≠ cut rate).
    limiter: RateLimiter | None = None
    delay_cc: DelayRateController | None = None
    if rate_mbit > 0 or wan:
        max_bps = (rate_mbit if rate_mbit > 0 else 80.0) * 1_000_000 / 8
        # Open ~3% of target (cap ~80 Mbit) — avoid flooding BDP before first RTT
        start_bps = min(max_bps, max(1_250_000.0, max_bps * 0.03))
        start_bps = min(start_bps, 10_000_000.0)
        limiter = RateLimiter(max_bps, start_bps=start_bps)
        delay_cc = DelayRateController(limiter)
        print(
            f"pace slow-start {start_bps / (1024 * 1024):.1f} → "
            f"max {max_bps / (1024 * 1024):.1f} MiB/s"
        )

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
                    prev_ack = ack_progress["ack"]
                    ack_progress["ack"] = pkt.cumulative_ack
                    ack_progress["plr"] = pkt.plr_byte
                    ack_progress["t"] = time.monotonic()
                    # Grow / shrink in-flight cap (packet cwnd) with ACK progress
                    delta = max(0, pkt.cumulative_ack - prev_ack)
                    if wan and delta > 0:
                        if delay_cc is not None and delay_cc.slow_start:
                            ack_progress["flight"] = min(
                                enc.cfg.max_window,
                                ack_progress["flight"] + max(delta, 1),
                            )
                        else:
                            ack_progress["flight"] = min(
                                enc.cfg.max_window,
                                ack_progress["flight"] + max(delta // 2, 1),
                            )
                    if wan and pkt.plr_byte >= 40:
                        ack_progress["flight"] = max(
                            64, ack_progress["flight"] // 2
                        )
                    if delay_cc is not None and pkt.echo_ts_us:
                        now_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
                        rtt = delay_cc.on_echo(pkt.echo_ts_us, now_us)
                        if rtt is not None:
                            ack_progress["rtt_us"] = rtt
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
    batch = 128 if wan else 2048
    file_offset = 0
    last_ack_seen = 0
    stall_repairs = 0

    def send_datagram(wire: bytes | memoryview) -> None:
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

                    # Repair: NACK retransmit; light coded only on holes (cap flood)
                    with enc_lock:
                        repairs = enc.pop_nack_retransmit(limit=16 if wan else 4)
                        plr_b = enc.last_plr_byte
                        burst = enc.coded_burst
                        need_repair = bool(repairs) or plr_b > 0
                        n_repair_coded = min(burst, 2) if need_repair else 0
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

                    # ACK stall → retransmit oldest; coded only if holes exist
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

                    cur_batch = batch
                    if wan and plr_b == 0 and not repairs:
                        cur_batch = 512 if (
                            delay_cc is not None and delay_cc.slow_start
                        ) else 2048

                    # Cap by window AND flight (slow-start cwnd) — do not fill 8k at t=0
                    flight_cap = int(ack_progress["flight"])
                    with enc_lock:
                        win = enc.window_size
                        room = enc.cfg.max_window - win
                        flight_room = flight_cap - win
                    n_take = min(cur_batch, max(room, 0), max(flight_room, 0))
                    chunks: list[bytes] = []
                    for _ in range(n_take):
                        if file_offset >= file_size:
                            eof = True
                            break
                        end = min(file_offset + payload_size, file_size)
                        raw = mm[file_offset:end]
                        file_offset = end
                        if len(raw) < payload_size:
                            chunks.append(
                                bytes(raw) + b"\x00" * (payload_size - len(raw))
                            )
                        else:
                            chunks.append(bytes(raw))

                    wires: list[bytes] = []
                    with enc_lock:
                        for chunk in chunks:
                            if not enc.can_accept():
                                break
                            wires.append(bytes(enc.add_source(chunk)))
                            wires.extend(enc.maybe_coded())

                    for wire in wires:
                        send_datagram(wire)
                        if pace_us > 0:
                            time.sleep(pace_us / 1_000_000.0)
                    sent = len(chunks)

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
                        rtt_ms = ack_progress["rtt_us"] / 1000.0
                        q_ms = 0.0
                        ss = ""
                        if delay_cc is not None:
                            if delay_cc.srtt_us and delay_cc.base_rtt_us:
                                q_ms = max(0.0, delay_cc.srtt_us - delay_cc.base_rtt_us) / 1000.0
                            ss = " ss" if delay_cc.slow_start else " ca"
                        flight = int(ack_progress["flight"])
                        print(
                            f"progress {done}/{total_symbols} ({pct:.1f}%) "
                            f"win={st['window']}/{flight} ack={st['cumulative_ack']} "
                            f"coded={st['sent_coded']} burst={st['coded_burst']} "
                            f"nack={st['nack_q']} plr={st['plr_byte']} "
                            f"rtt={rtt_ms:.1f}ms q={q_ms:.1f}ms "
                            f"pace={pace:.1f}/{cap:.1f}MiB/s{ss} app={rate:.1f} MiB/s"
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
        help="coded packets per redundancy tick (0=auto; WAN default 1)",
    )
    p.add_argument("--pace-us", type=float, default=0.0)
    p.add_argument("--skip-hash", action="store_true")
    p.add_argument(
        "--wan",
        action="store_true",
        help="WAN: payload 1350, delay-based CC, redundancy=0 (NACK repair), NumPy GF",
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
