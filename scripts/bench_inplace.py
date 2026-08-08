#!/usr/bin/env python3
"""In-process localhost Tetrys transfer benchmark (server+client threads)."""

from __future__ import annotations

import argparse
import select
import socket
import struct
import threading
import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tetrys_nc.decoder import DecoderConfig, TetrysDecoder  # noqa: E402
from tetrys_nc.encoder import EncoderConfig, TetrysEncoder  # noqa: E402
from tetrys_nc.genfile import generate  # noqa: E402
from tetrys_nc.netutil import try_set_buffer  # noqa: E402
from tetrys_nc.packets import (  # noqa: E402
    MAGIC,
    PKT_CODED,
    PKT_FIN,
    PKT_SOURCE,
    PKT_WND_UPT,
    CodedPacket,
    FinPacket,
    WindowUpdatePacket,
    parse_packet,
)


def run_once(path: Path, payload: int, window: int, redundancy: int, port: int) -> dict:
    file_size = path.stat().st_size
    total_symbols = (file_size + payload - 1) // payload
    result: dict = {"ok": False, "miBs": 0.0, "seconds": 0.0}

    ready = threading.Event()
    done = threading.Event()

    def server() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try_set_buffer(sock, socket.SO_SNDBUF, 4 * 1024 * 1024)
        try_set_buffer(sock, socket.SO_RCVBUF, 1 * 1024 * 1024)
        sock.bind(("127.0.0.1", port))
        sock.setblocking(False)
        enc = TetrysEncoder(
            EncoderConfig(max_window=window, redundancy_every=redundancy, payload_size=payload)
        )
        # wait ready
        client = None
        deadline = time.monotonic() + 30
        while client is None and time.monotonic() < deadline:
            r, _, _ = select.select([sock], [], [], 0.1)
            if not r:
                continue
            data, addr = sock.recvfrom(65535)
            pkt = parse_packet(data)
            if isinstance(pkt, WindowUpdatePacket) or data[2] == 0x12:
                client = addr
                ready.set()
        if client is None:
            return

        import mmap

        with path.open("rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                offset = 0
                while offset < file_size:
                    # drain feedback
                    while True:
                        try:
                            data, addr = sock.recvfrom(65535)
                        except BlockingIOError:
                            break
                        if data[2] == PKT_WND_UPT:
                            pkt = WindowUpdatePacket.unpack(data)
                            enc.apply_feedback(pkt.cumulative_ack, pkt.plr_byte)

                    sent = 0
                    while enc.can_accept() and offset < file_size and sent < 1024:
                        end = min(offset + payload, file_size)
                        raw = mm[offset:end]
                        offset = end
                        chunk = bytes(raw) if len(raw) == payload else bytes(raw) + b"\x00" * (
                            payload - len(raw)
                        )
                        wire = enc.add_source(chunk)
                        while True:
                            try:
                                sock.sendto(wire, client)
                                break
                            except BlockingIOError:
                                select.select([], [sock], [], 0.005)
                        for cwire in enc.maybe_coded():
                            try:
                                sock.sendto(cwire, client)
                            except BlockingIOError:
                                pass
                        sent += 1

                    if sent == 0:
                        oldest = enc.oldest_id
                        if oldest is not None:
                            w = enc.pack_source_id(oldest)
                            if w:
                                try:
                                    sock.sendto(w, client)
                                except BlockingIOError:
                                    pass
                        select.select([sock], [], [], 0.001)

                # drain window
                while enc.window_size > 0 and not done.wait(0):
                    while True:
                        try:
                            data, _ = sock.recvfrom(65535)
                        except BlockingIOError:
                            break
                        if data[2] == PKT_WND_UPT:
                            pkt = WindowUpdatePacket.unpack(data)
                            enc.apply_feedback(pkt.cumulative_ack, pkt.plr_byte)
                        elif data[2] == PKT_FIN:
                            return
                    oldest = enc.oldest_id
                    if oldest is not None:
                        w = enc.pack_source_id(oldest)
                        if w:
                            try:
                                sock.sendto(w, client)
                            except BlockingIOError:
                                pass
                    c = enc.make_coded()
                    if c:
                        try:
                            sock.sendto(c.pack(), client)
                        except BlockingIOError:
                            pass
                    try:
                        sock.sendto(FinPacket(True, total_symbols).pack(), client)
                    except BlockingIOError:
                        pass
                    select.select([sock], [], [], 0.001)
                for _ in range(5):
                    sock.sendto(FinPacket(True, total_symbols).pack(), client)
            finally:
                mm.close()
        sock.close()

    def client() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try_set_buffer(sock, socket.SO_RCVBUF, 4 * 1024 * 1024)
        sock.setblocking(False)
        server = ("127.0.0.1", port)
        from tetrys_nc.packets import ReadyPacket

        for _ in range(20):
            sock.sendto(ReadyPacket(window).pack(), server)
            if ready.wait(0.05):
                break

        dec = TetrysDecoder(
            DecoderConfig(max_decode_window=window, feedback_every_packets=256, delivered_cache=window)
        )
        dec.total_symbols = total_symbols
        dec.payload_size = payload
        out = path.with_suffix(".recv.bin")
        written = 0
        t0 = time.monotonic()
        fin = False
        last_echo = 0
        with out.open("wb", buffering=8 * 1024 * 1024) as f:
            while written < file_size:
                r, _, _ = select.select([sock], [], [], 0.1)
                if not r:
                    fb = dec.build_feedback()
                    fb.echo_ts_us = last_echo
                    sock.sendto(fb.pack(), server)
                    continue
                while True:
                    try:
                        data, _ = sock.recvfrom(65535)
                    except BlockingIOError:
                        break
                    if len(data) < 4 or data[0] != MAGIC:
                        continue
                    ptype = data[2]
                    delivered = []
                    if ptype == PKT_SOURCE:
                        sid, ts = struct.unpack_from("!II", data, 4)
                        if ts:
                            last_echo = ts
                        delivered = dec.on_source_raw(sid, data[12:])
                    elif ptype == PKT_CODED:
                        coded = CodedPacket.unpack(data)
                        if coded.send_ts_us:
                            last_echo = coded.send_ts_us
                        delivered = dec.on_coded(coded)
                    elif ptype == PKT_FIN:
                        fin = True
                        delivered = dec.pop_deliverable()
                    for _, payload_b in delivered:
                        need = file_size - written
                        if need <= 0:
                            break
                        chunk = payload_b[:need]
                        f.write(chunk)
                        written += len(chunk)
                if dec.need_feedback():
                    fb = dec.build_feedback()
                    fb.echo_ts_us = last_echo
                    sock.sendto(fb.pack(), server)
                if fin and written >= file_size:
                    break
        elapsed = time.monotonic() - t0
        sock.sendto(FinPacket(True, total_symbols).pack(), server)
        sock.close()
        result["ok"] = written == file_size
        result["seconds"] = elapsed
        result["miBs"] = written / max(elapsed, 1e-9) / (1024 * 1024)
        result["written"] = written
        done.set()

    ts = threading.Thread(target=server, daemon=True)
    tc = threading.Thread(target=client, daemon=True)
    ts.start()
    time.sleep(0.05)
    tc.start()
    tc.join(timeout=600)
    ts.join(timeout=5)
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="64M")
    ap.add_argument("--out", type=Path, default=ROOT / "testdata" / "inplace_bench.txt")
    args = ap.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)

    from tetrys_nc.genfile import parse_size

    size = parse_size(args.size)
    src = args.out.parent / f"inplace_{args.size}.bin"
    print(f"generating {src} ({size})...")
    generate(src, size)

    configs = [
        (8192, 8192, 32),
        (32768, 8192, 32),
        (60000, 4096, 32),
        (32768, 8192, 0),
    ]
    lines = [f"=== inplace bench {args.size} ==="]
    best = (0.0, None)
    for payload, window, red in configs:
        port = 19000 + (payload % 1000) + red
        print(f"run payload={payload} window={window} red={red} ...")
        r = run_once(src, payload, window, red, port)
        line = (
            f"payload={payload} window={window} red={red} "
            f"ok={r.get('ok')} {r.get('miBs', 0):.1f} MiB/s in {r.get('seconds', 0):.2f}s"
        )
        print(line)
        lines.append(line)
        if r.get("ok") and r.get("miBs", 0) > best[0]:
            best = (r["miBs"], (payload, window, red))

    lines.append(f"BEST: {best[1]} @ {best[0]:.1f} MiB/s")
    args.out.write_text("\n".join(lines) + "\n")
    print(f"wrote {args.out}")
    return 0 if best[0] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
