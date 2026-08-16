"""Generation + RaptorQ random-access file transfer."""

from __future__ import annotations

import hashlib
import mmap
import os
import select
import socket
import sys
import threading
import time
from pathlib import Path

from .gen_raptor import GenDecoder, GenEncoder, repair_count, require_raptorq
from .netutil import send_datagrams, try_set_buffer
from .packets import (
    MAGIC,
    PKT_FIN,
    PKT_GEN,
    PKT_GEN_FB,
    PKT_META,
    PKT_READY,
    XFER_GEN,
    FinPacket,
    GenFeedbackPacket,
    GenPacket,
    MetaPacket,
    ReadyPacket,
    parse_packet,
)
from .ratectl import RateLimiter

# Keep recent encoders for fast drain/NACK (avoid re-encode from mmap).
_ENCODER_KEEP = 512
# Do not re-repair the same gen on every send-loop iteration.
_REPAIR_COOLDOWN_S = 0.10
# Start frontier repair when client lags this many gens behind send cursor.
_REPAIR_LAG = 48
# Soft cap on how far blast may run ahead of next_needed.
# 512 was too tight on a ~900 Mbit path and parked transfers in HOLD.
_INFLIGHT_MAX = 2048
# If next_needed unchanged this long, full-reblast that gen (thin repair failed).
_STUCK_S = 0.40
# Min gap between full reblasts of the same gen.
_FULL_REBLAST_COOLDOWN_S = 0.25


def _file_sha256(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def run_gen_server(
    host: str,
    port: int,
    file_path: Path,
    *,
    symbol_size: int = 1350,
    gen_k: int = 48,
    overhead_pct: int = 8,
    rate_mbit: float = 1500.0,
    ramp_s: float = 4.0,
    skip_hash: bool = True,
) -> int:
    require_raptorq()
    if not file_path.is_file():
        print(f"file not found: {file_path}", file=sys.stderr)
        return 1

    file_size = file_path.stat().st_size
    block_bytes = max(1, gen_k) * max(64, symbol_size)
    total_gens = (file_size + block_bytes - 1) // block_bytes if file_size else 0
    digest = "" if skip_hash else _file_sha256(file_path)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_SNDBUF, 32 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_RCVBUF, 8 * 1024 * 1024)
    sock.bind((host, port))
    sock.setblocking(False)

    print(
        f"file={file_path.name} size={file_size}"
        f"{' (hash skipped)' if skip_hash else ''}"
    )
    print(
        f"xfer=gen gens={total_gens} T={symbol_size} K~={gen_k} "
        f"block={block_bytes} overhead={overhead_pct}% "
        f"rate_mbit={rate_mbit}"
    )
    print(f"Gen RaptorQ server listening on udp://{host}:{port}")
    print("waiting for client READY...")

    client_addr: tuple[str, int] | None = None
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
            print(f"client ready from {addr}")

    assert client_addr is not None

    meta = MetaPacket(
        file_size,
        file_path.name,
        symbol_size,
        digest,
        xfer=XFER_GEN,
        gen_symbol_size=symbol_size,
        gen_k=gen_k,
        gen_overhead_pct=overhead_pct,
    ).pack()
    for _ in range(3):
        sock.sendto(meta, client_addr)

    max_bps = max(rate_mbit, 1.0) * 1_000_000 / 8
    limiter = RateLimiter(max_bps, start_bps=1.0 if ramp_s > 0 else max_bps)
    t_ramp0 = time.monotonic()
    if ramp_s > 0:
        print(f"pace ramp {ramp_s:.1f}s 0→{max_bps / (1024 * 1024):.1f} MiB/s")

    encoders: dict[int, GenEncoder] = {}
    repair_extra: dict[int, int] = {}
    last_repair_ts: dict[int, float] = {}
    last_full_ts: dict[int, float] = {}
    stop_fb = threading.Event()
    fb_lock = threading.Lock()
    fb_state = {
        "next_needed": 0,
        "completed": 0,
        "nacks": [],
        "echo": 0,
        "done": False,
    }

    def feedback_loop() -> None:
        while not stop_fb.is_set():
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
                if isinstance(pkt, GenFeedbackPacket):
                    with fb_lock:
                        fb_state["next_needed"] = pkt.next_needed_gen
                        fb_state["completed"] = pkt.completed_gens
                        fb_state["nacks"] = list(pkt.nack_gens)
                        fb_state["echo"] = pkt.echo_ts_us
                        if pkt.completed_gens >= total_gens and total_gens > 0:
                            fb_state["done"] = True
                elif isinstance(pkt, FinPacket):
                    with fb_lock:
                        fb_state["done"] = True
                elif isinstance(pkt, ReadyPacket):
                    sock.sendto(meta, client_addr)

    fb_thread = threading.Thread(target=feedback_loop, name="gen-fb", daemon=True)
    fb_thread.start()

    def pace_tick() -> None:
        if ramp_s > 0:
            elapsed = time.monotonic() - t_ramp0
            if elapsed < ramp_s:
                frac = (elapsed / ramp_s) ** 2
                limiter.set_rate(max(1.0, max_bps * frac))
            else:
                limiter.set_rate(max_bps)

    def send_batch(wires: list[bytes]) -> None:
        if not wires:
            return
        pace_tick()
        total = sum(len(w) for w in wires)
        limiter.consume(total)
        assert client_addr is not None
        send_datagrams(sock, client_addr, wires)

    def get_or_make_encoder(mm: mmap.mmap, gen_id: int) -> GenEncoder | None:
        if gen_id < 0 or gen_id >= total_gens:
            return None
        enc = encoders.get(gen_id)
        if enc is not None:
            return enc
        off = gen_id * block_bytes
        end = min(off + block_bytes, file_size)
        raw = bytes(mm[off:end])
        if len(raw) < block_bytes:
            raw = raw + b"\x00" * (block_bytes - len(raw))
        enc = GenEncoder(raw, symbol_size, overhead_pct)
        encoders[gen_id] = enc
        return enc

    def repair_one(mm: mmap.mmap, nid: int, *, now: float, full_once: bool = False) -> int:
        """Send a cooldown-gated repair (or one full reblast) for incomplete gen."""
        if (now - last_repair_ts.get(nid, 0.0)) < _REPAIR_COOLDOWN_S:
            return 0
        enc = get_or_make_encoder(mm, nid)
        if enc is None:
            return 0
        base_r = max(1, repair_count(gen_k, overhead_pct))
        rounds = repair_extra.get(nid, 0)
        if full_once:
            ts = int(now * 1_000_000) & 0xFFFFFFFF
            wires = [
                GenPacket(nid, i, blob, ts).pack()
                for i, blob in enumerate(enc.packets())
            ]
            send_batch(wires)
            repair_extra[nid] = max(rounds, 1) + 1
            last_repair_ts[nid] = now
            return len(wires)
        rounds = min(rounds + 1, 32)
        repair_extra[nid] = rounds
        new_pkts = enc.ensure_repair(enc.repair_budget + base_r)
        if not new_pkts:
            new_pkts = enc.packets()[-base_r:]
        else:
            new_pkts = new_pkts[:base_r]
        ts = int(now * 1_000_000) & 0xFFFFFFFF
        wires = [GenPacket(nid, 0, blob, ts).pack() for blob in new_pkts]
        send_batch(wires)
        last_repair_ts[nid] = now
        return len(wires)

    t0 = time.monotonic()
    bytes_sent_payload = 0
    gens_sent = 0
    repair_sent = 0
    last_progress = t0
    stuck_nn = -1
    stuck_nn_since = t0

    try:
        with file_path.open("rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                gen_id = 0
                while gen_id < total_gens:
                    with fb_lock:
                        if fb_state["done"]:
                            break
                        nacks = list(fb_state["nacks"])
                        next_needed = int(fb_state["next_needed"])
                        completed = int(fb_state["completed"])

                    now = time.monotonic()
                    if next_needed != stuck_nn:
                        stuck_nn = next_needed
                        stuck_nn_since = now
                    stuck = (now - stuck_nn_since) >= _STUCK_S
                    lag = gen_id - next_needed

                    # Only intervene mid-blast when the frontier is stuck.
                    # Continuous lag-based thin repair re-created the death spiral
                    # under loss; proactive FEC + drain covers the rest.
                    if (
                        stuck
                        and 0 <= next_needed < gen_id
                        and (now - last_full_ts.get(next_needed, 0.0))
                        >= _FULL_REBLAST_COOLDOWN_S
                    ):
                        last_repair_ts.pop(next_needed, None)
                        n = repair_one(mm, next_needed, now=now, full_once=True)
                        if n:
                            repair_sent += n
                            last_full_ts[next_needed] = now

                    # Soft backpressure only when far ahead of a stuck hole.
                    if lag >= _INFLIGHT_MAX and stuck:
                        if now - last_progress >= 1.0:
                            last_progress = now
                            rate = (
                                bytes_sent_payload
                                / max(now - t0, 1e-6)
                                / (1024 * 1024)
                            )
                            print(
                                f"progress {gen_id}/{total_gens} "
                                f"client_done={completed} "
                                f"lag={lag} "
                                f"repair_extra={repair_sent} "
                                f"HOLD "
                                f"app={rate:.1f} MiB/s"
                            )
                        time.sleep(0.002)
                        continue

                    off = gen_id * block_bytes
                    end = min(off + block_bytes, file_size)
                    raw = bytes(mm[off:end])
                    if len(raw) < block_bytes:
                        raw = raw + b"\x00" * (block_bytes - len(raw))

                    enc = GenEncoder(raw, symbol_size, overhead_pct)
                    encoders[gen_id] = enc
                    protected = {next_needed, *nacks[:16]}
                    keep_from = max(0, gen_id - _ENCODER_KEEP)
                    for old in [g for g in encoders if g < keep_from and g not in protected]:
                        encoders.pop(old, None)
                        if old not in protected:
                            repair_extra.pop(old, None)
                            last_repair_ts.pop(old, None)

                    ts = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
                    wires = [
                        GenPacket(gen_id, i, blob, ts).pack()
                        for i, blob in enumerate(enc.packets())
                    ]
                    for i in range(0, len(wires), 64):
                        send_batch(wires[i : i + 64])
                    gens_sent += 1
                    bytes_sent_payload += end - off
                    gen_id += 1

                    now = time.monotonic()
                    if now - last_progress >= 1.0:
                        last_progress = now
                        with fb_lock:
                            done_g = fb_state["completed"]
                            nn = int(fb_state["next_needed"])
                        rate = bytes_sent_payload / max(now - t0, 1e-6) / (1024 * 1024)
                        print(
                            f"progress {gen_id}/{total_gens} "
                            f"client_done={done_g} "
                            f"lag={gen_id - nn} "
                            f"repair_extra={repair_sent} "
                            f"app={rate:.1f} MiB/s"
                        )

                fin = FinPacket(True, total_gens).pack()
                for _ in range(3):
                    sock.sendto(fin, client_addr)

                drain_deadline = time.monotonic() + 300.0
                while time.monotonic() < drain_deadline:
                    with fb_lock:
                        if fb_state["done"] or fb_state["completed"] >= total_gens:
                            break
                        nacks = list(fb_state["nacks"])
                        next_needed = int(fb_state["next_needed"])
                    if not nacks:
                        sock.sendto(fin, client_addr)
                        time.sleep(0.02)
                        continue
                    now = time.monotonic()
                    ordered = sorted(
                        nacks[:24],
                        key=lambda g: (0 if g == next_needed else 1, abs(g - next_needed)),
                    )
                    for nid in ordered:
                        # First contact: full packet set once; then repair forever.
                        repair_sent += repair_one(
                            mm,
                            nid,
                            now=now,
                            full_once=repair_extra.get(nid, 0) == 0,
                        )
                    sock.sendto(fin, client_addr)
                    time.sleep(0.005)

            finally:
                mm.close()
    finally:
        stop_fb.set()
        fb_thread.join(timeout=1.0)

    elapsed = time.monotonic() - t0
    goodput = file_size / max(elapsed, 1e-6) / (1024 * 1024)
    with fb_lock:
        completed = fb_state["completed"]
    print(
        f"done in {elapsed:.2f}s — goodput {goodput:.2f} MiB/s — "
        f"gens_sent={gens_sent}/{total_gens} client_done={completed} "
        f"repair_pkts={repair_sent}"
    )
    return 0


def run_gen_client(
    host: str,
    port: int,
    output: Path,
    meta: MetaPacket,
    sock: socket.socket,
    server: tuple[str, int],
) -> int:
    require_raptorq()
    symbol_size = meta.gen_symbol_size or meta.payload_size or 1350
    gen_k = meta.gen_k or 48
    overhead_pct = meta.gen_overhead_pct or 8
    block_bytes = gen_k * symbol_size
    file_size = meta.file_size
    total_gens = (file_size + block_bytes - 1) // block_bytes if file_size else 0

    print(
        f"gen-xfer: gens={total_gens} T={symbol_size} K={gen_k} "
        f"block={block_bytes} overhead={overhead_pct}%"
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    out_path = output
    if out_path.is_dir():
        out_path = out_path / meta.file_name

    out_path.touch(exist_ok=True)
    fd = os.open(out_path, os.O_RDWR)
    try:
        os.ftruncate(fd, file_size)
    except OSError:
        pass

    decoders: dict[int, GenDecoder] = {}
    done_bits = bytearray((total_gens + 7) // 8) if total_gens else bytearray()
    completed = 0
    gens_recovered = 0
    write_queue: list[tuple[int, bytes]] = []
    last_echo = 0
    fin_seen = False
    t0 = time.monotonic()
    last_progress = t0
    last_fb = 0.0
    bytes_written = 0

    def bit_get(i: int) -> bool:
        if i < 0 or i >= total_gens:
            return True
        return bool(done_bits[i >> 3] & (1 << (i & 7)))

    def bit_set(i: int) -> None:
        nonlocal completed
        if i < 0 or i >= total_gens or bit_get(i):
            return
        done_bits[i >> 3] |= 1 << (i & 7)
        completed += 1

    def flush_writes() -> None:
        nonlocal bytes_written, write_queue
        for off, data in write_queue:
            if off >= file_size:
                continue
            chunk = data[: max(0, file_size - off)]
            os.pwrite(fd, chunk, off)
            bytes_written += len(chunk)
        write_queue.clear()

    def send_fb() -> None:
        nonlocal last_fb
        next_needed = total_gens
        nacks: list[int] = []
        horizon = 256 if fin_seen else 64
        for i in range(total_gens):
            if bit_get(i):
                continue
            if next_needed == total_gens:
                next_needed = i
            if i <= next_needed + horizon or i in decoders:
                nacks.append(i)
            if len(nacks) >= 64:
                break
        pkt = GenFeedbackPacket(next_needed, nacks, last_echo, completed)
        sock.sendto(pkt.pack(), server)
        last_fb = time.monotonic()

    try:
        while True:
            timeout = 0.02 if fin_seen else 0.05
            r, _, _ = select.select([sock], [], [], timeout)
            now = time.monotonic()
            fb_every = 0.05 if fin_seen else 0.1
            if not r:
                send_fb()
                if fin_seen and completed >= total_gens:
                    break
                if now - t0 > 3600:
                    print("transfer timeout", file=sys.stderr)
                    return 1
                continue

            while True:
                try:
                    data, _ = sock.recvfrom(65535)
                except BlockingIOError:
                    break
                if len(data) < 4 or data[0] != MAGIC:
                    continue
                ptype = data[2]
                if ptype == PKT_GEN:
                    try:
                        gp = GenPacket.unpack(data)
                    except ValueError:
                        continue
                    last_echo = gp.send_ts_us
                    gid = gp.gen_id
                    if bit_get(gid):
                        continue
                    rem = file_size - gid * block_bytes
                    if rem <= 0:
                        bit_set(gid)
                        continue
                    tlen = min(block_bytes, rem)
                    dec = decoders.get(gid)
                    if dec is None:
                        dec = GenDecoder(block_bytes, symbol_size)
                        decoders[gid] = dec
                    out = dec.add_packet(gp.payload)
                    if out is not None:
                        if dec.symbols_rx > gen_k + 1:
                            gens_recovered += 1
                        payload = out[:tlen]
                        write_queue.append((gid * block_bytes, payload))
                        bit_set(gid)
                        decoders.pop(gid, None)
                        if len(write_queue) >= 8:
                            flush_writes()
                elif ptype == PKT_FIN:
                    fin_seen = True
                elif ptype == PKT_META:
                    continue

            if write_queue:
                flush_writes()

            if now - last_fb > fb_every:
                send_fb()

            if now - last_progress >= 1.0:
                last_progress = now
                pct = 100.0 * completed / total_gens if total_gens else 100.0
                rate = (
                    completed * block_bytes / max(now - t0, 1e-6) / (1024 * 1024)
                )
                print(
                    f"progress {completed}/{total_gens} ({pct:.1f}%) "
                    f"gens_recovered≈{gens_recovered} "
                    f"decoders={len(decoders)} "
                    f"{rate:.1f} MiB/s"
                )

            if completed >= total_gens:
                flush_writes()
                break
    finally:
        flush_writes()
        os.close(fd)
        for _ in range(5):
            sock.sendto(FinPacket(True, total_gens).pack(), server)
            time.sleep(0.02)

    elapsed = time.monotonic() - t0
    ok = completed >= total_gens
    goodput = file_size / max(elapsed, 1e-6) / (1024 * 1024)
    print(
        f"{'OK' if ok else 'FAIL'}: wrote {out_path} ({file_size} bytes) "
        f"in {elapsed:.2f}s ({goodput:.2f} MiB/s)"
    )
    print(
        f"stats: gens_done={completed}/{total_gens} "
        f"gens_recovered≈{gens_recovered}"
    )
    return 0 if ok else 2
