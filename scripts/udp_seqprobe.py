#!/usr/bin/env python3
"""One-way UDP sequence probe (iperf-like) with reorder / loss stats.

RFC 4737-style: a packet is reordered if it arrives after a higher seq.
This is path reorder, not Tetrys skip_done (late repair after decode).

  # sender (Russia, same as tetrys server): wait for client punch, blast back
  python scripts/udp_seqprobe.py send --wait-punch --port 7494 \\
      --rate-mbit 200 --size 1350 --seconds 10

  # receiver (Spain): punch server then count reorder on the data path
  python scripts/udp_seqprobe.py recv --punch 185.41.43.122:7494 --seconds 12
"""

from __future__ import annotations

import argparse
import socket
import struct
import sys
import time

MAGIC = b"SQP1"
PUNCH = b"PNCH"
_HDR = struct.Struct("!4sIQ")  # magic, seq, send_ns
HDR_SIZE = _HDR.size


def _sock() -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)
    return s


def run_send(
    host: str,
    port: int,
    *,
    rate_mbit: float,
    size: int,
    seconds: float,
    wait_punch: bool,
) -> int:
    size = max(size, HDR_SIZE)
    payload = b"\x00" * (size - HDR_SIZE)
    pps = (rate_mbit * 1_000_000 / 8.0) / size
    interval = 1.0 / max(pps, 1.0)
    sock = _sock()
    dest: tuple[str, int] | None = (host, port) if host else None
    if wait_punch:
        sock.bind(("0.0.0.0", port))
        sock.settimeout(0.5)
        print(f"send waiting for punch on udp://0.0.0.0:{port}")
        deadline = time.monotonic() + 60.0
        dest = None
        while dest is None and time.monotonic() < deadline:
            try:
                data, addr = sock.recvfrom(2048)
            except TimeoutError:
                continue
            if data.startswith(PUNCH) or data.startswith(MAGIC):
                dest = addr
                print(f"punched by {addr[0]}:{addr[1]}")
        if dest is None:
            print("no punch", file=sys.stderr)
            return 1
        sock.settimeout(None)
    assert dest is not None
    t0 = time.monotonic()
    next_t = t0
    seq = 0
    sent = 0
    try:
        while time.monotonic() - t0 < seconds:
            now = time.monotonic()
            if now < next_t:
                time.sleep(min(0.0005, next_t - now))
                continue
            pkt = _HDR.pack(MAGIC, seq & 0xFFFFFFFF, time.time_ns()) + payload
            try:
                sock.sendto(pkt, dest)
            except BlockingIOError:
                continue
            seq += 1
            sent += 1
            next_t += interval
            if next_t < time.monotonic() - 0.05:
                next_t = time.monotonic()
    finally:
        sock.close()
    elapsed = max(time.monotonic() - t0, 1e-6)
    mbit = sent * size * 8 / elapsed / 1_000_000
    print(
        f"send done pkts={sent} size={size} {elapsed:.2f}s "
        f"{mbit:.1f} Mbit/s target={rate_mbit:.1f} dest={dest[0]}:{dest[1]}"
    )
    return 0


def run_recv(
    port: int,
    *,
    seconds: float,
    host: str,
    punch_host: str | None,
    punch_port: int,
) -> int:
    sock = _sock()
    sock.bind((host, port))
    sock.settimeout(0.2)
    if punch_host:
        punch_pkt = PUNCH + b"\x00" * 32
        dest = (punch_host, punch_port)
        print(f"recv punching {punch_host}:{punch_port} from local :{port}")
        t_punch = time.monotonic()
        while time.monotonic() - t_punch < 8.0:
            sock.sendto(punch_pkt, dest)
            try:
                data, addr = sock.recvfrom(65535)
            except TimeoutError:
                continue
            if len(data) >= HDR_SIZE and data[:4] == MAGIC:
                # fall through into main loop with this first seq packet
                sock.settimeout(0.5)
                return _recv_loop(sock, seconds, first=(data, addr))
        print("no reply to punch", file=sys.stderr)
        sock.close()
        return 1
    print(f"recv listening udp://{host}:{port} for {seconds:.1f}s after first packet")
    return _recv_loop(sock, seconds, first=None)


def _recv_loop(
    sock: socket.socket,
    seconds: float,
    first: tuple[bytes, tuple[str, int]] | None,
) -> int:
    t_end: float | None = None
    recvd = 0
    unique = 0
    dups = 0
    reorder = 0
    max_seq = -1
    max_disp = 0
    bytes_rx = 0
    jitter_sum = 0.0
    jitter_n = 0
    last_owd_ns: int | None = None
    bins = {"1-10": 0, "11-100": 0, "101-1000": 0, "1001+": 0}
    seen_tail: set[int] = set()
    tail_keep = 200_000
    t_first = 0.0
    pending = first
    try:
        while True:
            now = time.monotonic()
            if t_end is not None and now >= t_end:
                break
            if pending is not None:
                data, _addr = pending
                pending = None
            else:
                try:
                    data, _addr = sock.recvfrom(65535)
                except TimeoutError:
                    continue
            if len(data) < HDR_SIZE or data[:4] != MAGIC:
                continue
            if t_end is None:
                t_first = now
                t_end = now + seconds
                print("first packet, clock started")
            _magic, seq, send_ns = _HDR.unpack_from(data)
            recvd += 1
            bytes_rx += len(data)
            recv_ns = time.time_ns()
            owd = recv_ns - send_ns
            if last_owd_ns is not None:
                jitter_sum += abs(owd - last_owd_ns)
                jitter_n += 1
            last_owd_ns = owd
            if seq in seen_tail:
                dups += 1
                continue
            seen_tail.add(seq)
            unique += 1
            if seq < max_seq:
                reorder += 1
                disp = max_seq - seq
                max_disp = max(max_disp, disp)
                if disp <= 10:
                    bins["1-10"] += 1
                elif disp <= 100:
                    bins["11-100"] += 1
                elif disp <= 1000:
                    bins["101-1000"] += 1
                else:
                    bins["1001+"] += 1
            else:
                max_seq = seq
            if len(seen_tail) > tail_keep:
                cutoff = max_seq - tail_keep
                seen_tail = {s for s in seen_tail if s >= cutoff}
    finally:
        sock.close()
    elapsed = max(time.monotonic() - t_first, 1e-6) if recvd else 1.0
    expected = max_seq + 1 if max_seq >= 0 else 0
    lost = max(0, expected - unique)
    loss_pct = 100.0 * lost / expected if expected else 0.0
    reo_pct = 100.0 * reorder / unique if unique else 0.0
    mbit = bytes_rx * 8 / elapsed / 1_000_000
    jitter_us = (jitter_sum / jitter_n / 1000.0) if jitter_n else 0.0
    print(
        f"recv pkts={recvd} unique={unique} dups={dups} "
        f"lost≈{lost} ({loss_pct:.2f}%) max_seq={max_seq}"
    )
    print(
        f"reorder={reorder} ({reo_pct:.3f}% of unique) "
        f"max_displacement={max_disp} pkts"
    )
    print(f"displacement bins: {bins}")
    print(f"rate={mbit:.1f} Mbit/s  mean |Δowd|={jitter_us:.0f} us  {elapsed:.2f}s")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="UDP seq probe: loss + reorder")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("send")
    sp.add_argument(
        "--host",
        default="",
        help="dest host; omit with --wait-punch (tetrys data path)",
    )
    sp.add_argument("--port", type=int, default=7494)
    sp.add_argument("--wait-punch", action="store_true")
    sp.add_argument("--rate-mbit", type=float, default=200.0)
    sp.add_argument("--size", type=int, default=1350)
    sp.add_argument("--seconds", type=float, default=10.0)
    rp = sub.add_parser("recv")
    rp.add_argument("--host", default="0.0.0.0")
    rp.add_argument("--port", type=int, default=0, help="0 = ephemeral local port")
    rp.add_argument("--punch", default="", help="host:port of sender (NAT hole punch)")
    rp.add_argument("--seconds", type=float, default=15.0)
    args = p.parse_args(argv)
    if args.cmd == "send":
        return run_send(
            args.host,
            args.port,
            rate_mbit=args.rate_mbit,
            size=args.size,
            seconds=args.seconds,
            wait_punch=args.wait_punch or not args.host,
        )
    punch_host = None
    punch_port = 7494
    if args.punch:
        if ":" in args.punch:
            punch_host, ps = args.punch.rsplit(":", 1)
            punch_port = int(ps)
        else:
            punch_host = args.punch
    return run_recv(
        args.port,
        seconds=args.seconds,
        host=args.host,
        punch_host=punch_host,
        punch_port=punch_port,
    )


if __name__ == "__main__":
    raise SystemExit(main())
