"""Tetrys NC file client — receives a file from a server over UDP."""

from __future__ import annotations

import argparse
import hashlib
import select
import socket
import struct
import sys
import time
from pathlib import Path

from .decoder import DecoderConfig, TetrysDecoder
from .netutil import try_set_buffer
from .packets import (
    MAGIC,
    PKT_CODED,
    PKT_FIN,
    PKT_META,
    PKT_SOURCE,
    SOURCE_HDR_SIZE,
    CodedPacket,
    FinPacket,
    MetaPacket,
    ReadyPacket,
)


def run_client(
    host: str,
    port: int,
    output: Path,
    max_window: int = 8192,
    feedback_every: int = 256,
    wan: bool = False,
) -> int:
    if wan:
        if max_window < 2048:
            max_window = 2048
        if feedback_every > 32:
            feedback_every = 16

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_RCVBUF, 4 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_SNDBUF, 1 * 1024 * 1024)
    sock.setblocking(False)
    server = (host, port)

    dec = TetrysDecoder(
        DecoderConfig(
            max_decode_window=max_window * 2,
            feedback_every_packets=feedback_every,
            delivered_cache=max_window * 2,
        )
    )

    print(f"connecting to udp://{host}:{port}")
    ready = ReadyPacket(max_window).pack()
    for _ in range(5):
        sock.sendto(ready, server)

    meta: MetaPacket | None = None
    t_wait = time.monotonic()
    while meta is None:
        if time.monotonic() - t_wait > 30:
            print("timeout waiting for META", file=sys.stderr)
            return 1
        r, _, _ = select.select([sock], [], [], 0.5)
        if not r:
            sock.sendto(ready, server)
            continue
        data, _addr = sock.recvfrom(65535)
        from .packets import parse_packet

        try:
            pkt = parse_packet(data)
        except ValueError:
            continue
        if isinstance(pkt, MetaPacket):
            meta = pkt
            print(
                f"META: name={meta.file_name} size={meta.file_size} "
                f"payload={meta.payload_size} sha256={meta.sha256_hex[:16] or 'n/a'}..."
            )

    assert meta is not None
    dec.payload_size = meta.payload_size
    total_symbols = (meta.file_size + meta.payload_size - 1) // meta.payload_size
    dec.total_symbols = total_symbols

    output.parent.mkdir(parents=True, exist_ok=True)
    out_path = output
    if out_path.is_dir():
        out_path = out_path / meta.file_name

    hasher = hashlib.sha256()
    verify = bool(meta.sha256_hex)
    bytes_written = 0
    t0 = time.monotonic()
    last_progress = t0
    fin_seen = False
    last_fb = 0.0
    # Last sender timestamp seen — echoed in feedback for delay-based CC
    last_echo_ts = 0

    def send_feedback() -> None:
        nonlocal last_fb
        fb = dec.build_feedback()
        fb.echo_ts_us = last_echo_ts
        sock.sendto(fb.pack(), server)
        last_fb = time.monotonic()

    with out_path.open("wb", buffering=8 * 1024 * 1024) as out:
        while True:
            timeout = 0.02 if (wan or dec.has_holes()) else 0.1
            r, _, _ = select.select([sock], [], [], timeout)
            now = time.monotonic()
            if not r:
                send_feedback()
                if fin_seen and dec.is_complete():
                    break
                if now - t0 > 3600:
                    print("transfer timeout", file=sys.stderr)
                    return 1
                continue

            processed = 0
            while True:
                try:
                    data, _addr = sock.recvfrom(65535)
                except BlockingIOError:
                    break

                if len(data) < 4 or data[0] != MAGIC:
                    continue
                ptype = data[2]

                delivered: list[tuple[int, bytes]] = []
                if ptype == PKT_SOURCE:
                    if len(data) >= SOURCE_HDR_SIZE:
                        sid, ts = struct.unpack_from("!II", data, 4)
                        # Always echo latest stamp (0 only if sender omitted it)
                        last_echo_ts = ts
                        delivered = dec.on_source_raw(sid, data[SOURCE_HDR_SIZE:])
                    else:
                        sid = struct.unpack_from("!I", data, 4)[0]
                        delivered = dec.on_source_raw(sid, data[8:])
                elif ptype == PKT_CODED:
                    coded = CodedPacket.unpack(data)
                    last_echo_ts = coded.send_ts_us
                    delivered = dec.on_coded(coded)
                elif ptype == PKT_FIN:
                    fin_seen = True
                    dec.total_symbols = struct.unpack_from("!I", data, 4)[0]
                    delivered = dec.pop_deliverable()
                elif ptype == PKT_META:
                    continue

                for _sid, payload in delivered:
                    remaining = meta.file_size - bytes_written
                    if remaining <= 0:
                        break
                    if remaining < len(payload):
                        payload = payload[:remaining]
                    out.write(payload)
                    if verify:
                        hasher.update(payload)
                    bytes_written += len(payload)

                processed += 1

            if processed and (
                dec.need_feedback()
                or (wan and now - last_fb > 0.05)
                or (dec.has_holes() and now - last_fb > 0.05)
            ):
                send_feedback()

            if now - last_progress >= 1.0:
                last_progress = now
                st = dec.stats()
                pct = 100.0 * bytes_written / meta.file_size if meta.file_size else 0
                rate = bytes_written / max(now - t0, 1e-6) / (1024 * 1024)
                print(
                    f"progress {bytes_written}/{meta.file_size} ({pct:.1f}%) "
                    f"deliver={st['next_deliver']}/{total_symbols} "
                    f"buf={st['buffered']} eq={st['equations']} "
                    f"recovered={st['recovered']} {rate:.1f} MiB/s"
                )

            if dec.is_complete() and bytes_written >= meta.file_size:
                break

    for _ in range(5):
        sock.sendto(FinPacket(True, total_symbols).pack(), server)
        time.sleep(0.02)

    elapsed = time.monotonic() - t0
    digest = hasher.hexdigest() if verify else ""
    ok = bytes_written == meta.file_size and (not verify or digest == meta.sha256_hex)
    st = dec.stats()
    goodput = bytes_written / max(elapsed, 1e-6) / (1024 * 1024)
    print(
        f"{'OK' if ok else 'FAIL'}: wrote {out_path} ({bytes_written} bytes) "
        f"in {elapsed:.2f}s ({goodput:.2f} MiB/s)"
    )
    if verify:
        print(f"sha256 local ={digest}")
        print(f"sha256 remote={meta.sha256_hex}")
    print(
        f"stats: source_rx={st['source_rx']} coded_rx={st['coded_rx']} "
        f"nc_recovered={st['recovered']}"
    )
    return 0 if ok else 2


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Tetrys NC UDP file client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--output", type=Path, default=Path("received.bin"))
    p.add_argument("--window", type=int, default=8192)
    p.add_argument("--feedback-every", type=int, default=256)
    p.add_argument(
        "--wan",
        action="store_true",
        help="lossy/long-RTT profile: smaller window, faster SACK/NACK feedback",
    )
    args = p.parse_args(argv)
    return run_client(
        args.host,
        args.port,
        args.output,
        max_window=args.window,
        feedback_every=args.feedback_every,
        wan=args.wan,
    )


if __name__ == "__main__":
    raise SystemExit(main())
