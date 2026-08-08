"""Tetrys NC file server — sends a file to a client over UDP."""

from __future__ import annotations

import argparse
import hashlib
import mmap
import select
import socket
import struct
import sys
import threading
import time
from pathlib import Path

from .encoder import EncoderConfig, TetrysEncoder
from .netutil import send_datagrams, try_set_buffer
from .packets import (
    CODED_HDR_SIZE,
    PKT_CODED,
    PKT_SOURCE,
    SOURCE_HDR_SIZE,
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
    max_window: int | None = None,
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
    if max_window is None:
        max_window = 16384 if wan else 8192
    if wan:
        # WAN: MTU-sized symbols, moderate proactive FEC + NACK/HOL repair.
        if payload_size >= 8000:
            payload_size = 1350
        # Default argparse redundancy=32 → treat as "auto" for WAN → every 8.
        if redundancy_every >= 32:
            redundancy_every = 8
        if coded_burst <= 1:
            coded_burst = 1
        code_degree = 8
        # Default target ≈ typical WAN NIC; override with --rate to match link.
        if rate_mbit <= 0:
            rate_mbit = 1000.0
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
    try_set_buffer(sock, socket.SO_SNDBUF, 64 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_RCVBUF, 8 * 1024 * 1024)
    sock.bind((host, port))
    sock.setblocking(False)
    print(f"Tetrys server listening on udp://{host}:{port}")
    print("waiting for client READY...")

    client_addr: tuple[str, int] | None = None
    enc = TetrysEncoder(
        EncoderConfig(
            max_window=max_window,
            max_coding_span=max(max_window * 4, 65536),
            redundancy_every=redundancy_every,
            coded_burst=coded_burst,
            payload_size=payload_size,
            code_degree=code_degree,
        )
    )
    enc_lock = threading.Lock()
    fin_from_client = threading.Event()
    stop_feedback = threading.Event()
    # Large initial flight — fill the pipe before first ACKs arrive.
    ack_progress = {
        "ack": 0,
        "plr": 0,
        "t": time.monotonic(),
        "rtt_us": 0.0,
        "echo": 0,
        "flight": max_window if wan else min(8192, max_window),
        "nack": 0,
    }

    # FASP-style: blast at --rate; soft delay bias only; loss → NACK repair.
    limiter: RateLimiter | None = None
    delay_cc: DelayRateController | None = None
    if rate_mbit > 0 or wan:
        max_bps = (rate_mbit if rate_mbit > 0 else 1000.0) * 1_000_000 / 8
        limiter = RateLimiter(max_bps, start_bps=max_bps)
        delay_cc = DelayRateController(limiter, payload_size=payload_size)
        print(
            f"pace BLAST start {max_bps / (1024 * 1024):.1f} "
            f"cap {max_bps / (1024 * 1024):.1f} MiB/s "
            f"(FASP-style; loss→NACK only)"
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
                    missing = pkt.missing_ids(limit=8192)
                    held = pkt.held_ids(limit=200_000) if wan else []
                    with enc_lock:
                        enc.apply_feedback(
                            pkt.cumulative_ack, pkt.plr_byte, missing, held
                        )
                        win_now = enc.window_size
                    prev_ack = ack_progress["ack"]
                    ack_progress["ack"] = pkt.cumulative_ack
                    ack_progress["plr"] = pkt.plr_byte
                    ack_progress["t"] = time.monotonic()
                    ack_progress["nack"] = len(missing)
                    delta = max(0, pkt.cumulative_ack - prev_ack)
                    if delay_cc is not None:
                        # PLR ignored for pacing (FASP); still recorded on encoder
                        delay_cc.on_loss(pkt.plr_byte)
                        if delta > 0:
                            delay_cc.on_ack(delta, pkt.plr_byte)
                        if pkt.echo_ts_us:
                            now_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
                            rtt = delay_cc.on_echo(pkt.echo_ts_us, now_us)
                            if rtt is not None:
                                ack_progress["rtt_us"] = rtt
                        ack_progress["echo"] = pkt.echo_ts_us
                        # Never shrink cwnd below current flight (avoids admit deadlock)
                        cwnd = delay_cc.target_cwnd_packets()
                        ack_progress["flight"] = min(
                            enc.cfg.max_window, max(cwnd, win_now, ack_progress["flight"])
                        )
                    elif delta > 0:
                        ack_progress["flight"] = min(
                            enc.cfg.max_window,
                            max(ack_progress["flight"] + max(delta, 1), 1024),
                        )
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
    # Large batches: keep blasting new data; repair rides alongside.
    batch = 4096 if wan else 2048
    file_offset = 0
    last_ack_seen = 0
    last_ack_advance_t = t0

    def _stamp(buf: bytearray, ts: int) -> None:
        if len(buf) < 4:
            return
        ptype = buf[2]
        if ptype == PKT_SOURCE and len(buf) >= SOURCE_HDR_SIZE:
            struct.pack_into("!I", buf, 8, ts)
        elif ptype == PKT_CODED and len(buf) >= CODED_HDR_SIZE:
            struct.pack_into("!I", buf, 16, ts)

    def send_batch(wires: list[bytes | bytearray | memoryview]) -> None:
        """Stamp in-place when possible, pace, then sendmmsg/sendto."""
        if not wires:
            return
        bufs: list[bytearray] = []
        for w in wires:
            if isinstance(w, bytearray):
                bufs.append(w)
            else:
                bufs.append(bytearray(w))
        total = sum(len(b) for b in bufs)
        if limiter is not None:
            limiter.consume(total)
        ts = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
        for buf in bufs:
            _stamp(buf, ts)
        assert client_addr is not None
        send_datagrams(sock, client_addr, bufs)
        if pace_us > 0:
            time.sleep(pace_us * len(bufs) / 1_000_000.0)

    def send_datagram(wire: bytes | bytearray | memoryview) -> None:
        send_batch([wire])

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

                    cur_ack = ack_progress["ack"]
                    now_loop = time.monotonic()
                    if cur_ack != last_ack_seen:
                        last_ack_seen = cur_ack
                        last_ack_advance_t = now_loop

                    # HOL holes → repair gets the pipe; new data only if room remains.
                    rtt_s = max(0.05, float(ack_progress["rtt_us"]) / 1_000_000.0)
                    plr = int(ack_progress["plr"])
                    with enc_lock:
                        win = enc.window_size
                        nack_pending = len(enc._nack_q)
                        stalled = (
                            wan
                            and win > 64
                            and (now_loop - last_ack_advance_t > rtt_s)
                        )
                        holey = wan and (stalled or nack_pending > 0 or plr >= 16)
                        if holey:
                            repair_n = 2048 if stalled else 1024
                        else:
                            repair_n = 128 if wan else 4
                        repairs = enc.pop_nack_retransmit(limit=repair_n)
                        if holey or win >= enc.cfg.max_window:
                            repairs.extend(
                                enc.retransmit_oldest(
                                    limit=max(128, repair_n - len(repairs))
                                )
                            )
                        # Modest coded repair over HOL (known+missing mix) — not a flood.
                        coded_repair: list[bytes] = []
                        if holey and enc.fec_enabled:
                            n_coded = 8 if stalled else 4
                            coded_repair = enc.emit_coded(n_coded)
                        room = enc.cfg.max_window - enc.window_size

                    flight_cap = max(int(ack_progress["flight"]), win)
                    ack_progress["flight"] = min(enc.cfg.max_window, flight_cap)
                    flight_room = max(0, flight_cap - win)
                    n_take = min(batch, max(room, 0), flight_room)
                    # While repairing holes, throttle new admits — keep pipe on frontier.
                    if holey and (repairs or coded_repair):
                        n_take = min(n_take, max(64, batch // 8))

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

                    wires: list[bytearray | bytes] = []
                    with enc_lock:
                        for chunk in chunks:
                            if not enc.can_accept():
                                break
                            wires.append(enc.add_source(chunk))
                            for cw in enc.maybe_coded():
                                wires.append(cw)

                    # Repair first whenever holes exist (unblocks cumack / window).
                    if holey:
                        send_batch(repairs)
                        send_batch(coded_repair)
                        send_batch(wires)
                    else:
                        send_batch(wires)
                        send_batch(repairs)
                    sent = len(wires) + len(repairs) + len(coded_repair)

                    if eof:
                        with enc_lock:
                            win = enc.window_size
                        if win == 0:
                            if not fin_sent:
                                fin = FinPacket(True, total_symbols).pack()
                                send_batch([fin, fin, fin])
                                fin_sent = True
                            if fin_from_client.wait(0.1):
                                elapsed = time.monotonic() - t0
                                _print_summary(enc, file_size, elapsed)
                                return 0
                            send_datagram(FinPacket(True, total_symbols).pack())
                        else:
                            with enc_lock:
                                tail = enc.retransmit_oldest(limit=256)
                                tail.extend(enc.pop_nack_retransmit(limit=128))
                                if enc.fec_enabled:
                                    tail.extend(enc.emit_coded(8))
                            send_batch(tail)
                            time.sleep(0.00005)
                    elif sent == 0:
                        with enc_lock:
                            tail = enc.retransmit_oldest(limit=256)
                            tail.extend(enc.pop_nack_retransmit(limit=128))
                            if enc.fec_enabled:
                                tail.extend(enc.emit_coded(8))
                        send_batch(tail)

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
                        mode = ""
                        bw_m = 0.0
                        peak_m = 0.0
                        if delay_cc is not None:
                            if delay_cc.srtt_us and delay_cc.base_rtt_us:
                                q_ms = max(0.0, delay_cc.srtt_us - delay_cc.base_rtt_us) / 1000.0
                            mode = f" {delay_cc.mode}"
                            bw_m = delay_cc.btlbw / (1024 * 1024)
                            peak_m = delay_cc.peak_bw / (1024 * 1024)
                            cwnd = delay_cc.target_cwnd_packets()
                            ack_progress["flight"] = min(
                                enc.cfg.max_window,
                                max(cwnd, st["window"], ack_progress["flight"]),
                            )
                        flight = int(ack_progress["flight"])
                        print(
                            f"progress {done}/{total_symbols} ({pct:.1f}%) "
                            f"win={st['window']}/{flight} coding={st.get('coding', st['window'])} "
                            f"ack={st['cumulative_ack']} "
                            f"coded={st['sent_coded']} burst={st['coded_burst']} "
                            f"nack={st['nack_q']} plr={st['plr_byte']} "
                            f"rtt={rtt_ms:.1f}ms q={q_ms:.1f}ms echo={int(ack_progress['echo'])} "
                            f"pace={pace:.1f}/{cap:.1f}MiB/s bw={bw_m:.1f}/{peak_m:.1f}{mode} "
                            f"app={rate:.1f} MiB/s"
                            f"{' HOL' if stalled else ''}"
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
    p.add_argument(
        "--window",
        type=int,
        default=None,
        help="elastic window (default: 16384 WAN / 8192 LAN); client Ready can cap lower",
    )
    p.add_argument(
        "--redundancy",
        type=int,
        default=32,
        help="coded every N source packets (0=off; WAN auto/default 8)",
    )
    p.add_argument(
        "--coded-burst",
        type=int,
        default=0,
        help="coded packets per redundancy tick (0=auto)",
    )
    p.add_argument("--pace-us", type=float, default=0.0)
    p.add_argument("--skip-hash", action="store_true")
    p.add_argument(
        "--wan",
        action="store_true",
        help="WAN: payload 1350, blast pacing, FEC+NACK/HOL repair",
    )
    p.add_argument(
        "--rate-mbit",
        "--rate",
        type=float,
        default=0.0,
        dest="rate_mbit",
        help="target UDP send rate in Mbit/s (alias: --rate). WAN default 1000",
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
