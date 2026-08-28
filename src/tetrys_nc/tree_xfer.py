"""Tree transfer: same v2 RaptorQ blocks as a single file, payload from a virtual stream."""

from __future__ import annotations

import math
import os
import random
import select
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from .block_packets import (
    BlockDataV2,
    BlockFeedbackV2,
    BlockFinV2,
    BlockMetaV2,
    BlockReadyV2,
    OpenBlock,
    pack_data_packets,
    parse_v2_packet,
)
from .block_state import (
    AckPacer,
    BlockGeometry,
    REPAIR_AGE_S,
    REPAIR_COOLDOWN_S,
    REPAIR_INTERVAL_S,
    TAIL_REPAIR_COOLDOWN_S,
    RepairDebtController,
    SenderBlockState,
    SenderFeedbackState,
    ExtraRepairWindow,
    repair_tick_limits,
    select_repair_candidates,
)
from .block_xfer import _encode_workers, _pace_limits
from .gen_raptor import GenEncoder, GenReceiveSlot
from .netutil import recv_datagrams, send_datagrams, try_set_buffer
from .ratectl import RateLimiter
from .tree_stream import (
    STAGING_NAME,
    TREE_META_NAME,
    StreamSource,
    encode_index,
    layout_from_root,
    materialize_from_staging,
)

_FEEDBACK_S = 0.020
_SEND_CHUNK = 64


def _encode_payload(
    payload: bytes,
    symbol_size: int,
    overhead_pct: int,
    session_id: int,
    block_id: int,
) -> tuple[list[bytes], int, GenEncoder]:
    encoder = GenEncoder(payload, symbol_size, overhead_pct)
    stamp = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
    wires = pack_data_packets(session_id, block_id, encoder.packets(), 0, stamp)
    return wires, encoder.repair_budget, encoder


def _encode_block(
    source: StreamSource,
    block_id: int,
    block_bytes: int,
    symbol_size: int,
    overhead_pct: int,
    session_id: int,
) -> tuple[int, list[bytes], int, GenEncoder]:
    payload, _tlen = source.read_block(block_id, block_bytes)
    wires, budget, encoder = _encode_payload(
        payload, symbol_size, overhead_pct, session_id, block_id
    )
    return block_id, wires, budget, encoder


def run_tree_server(
    host: str,
    port: int,
    root: Path,
    *,
    symbol_size: int = 256,
    block_k: int = 64,
    initial_repair_pct: int = 14,
    active_bytes: int = 4 << 20,
    rate_mbit: float = 400.0,
) -> int:
    layout = layout_from_root(root)
    index = encode_index([(f.rel, f.size) for f in layout.files])
    source = StreamSource(layout, index)
    geometry = BlockGeometry(symbol_size, block_k, active_bytes)
    total_blocks = max(1, math.ceil(layout.total_bytes / geometry.block_bytes))
    min_bps, max_bps, start_bps = _pace_limits(rate_mbit)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_SNDBUF, 8 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_RCVBUF, 8 * 1024 * 1024)
    sock.bind((host, port))
    sock.setblocking(False)
    print(
        f"tree-stream server udp://{host}:{port} files={len(layout.files)} "
        f"bytes={layout.total_bytes} blocks={total_blocks} "
        f"K={block_k} T={symbol_size} fec={initial_repair_pct}%",
        flush=True,
    )

    client: tuple[str, int] | None = None
    session_id = 0
    deadline = time.monotonic() + 30.0
    while client is None and time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], 0.5)
        if not readable:
            continue
        try:
            raw, addr = sock.recvfrom(2048)
            packet = parse_v2_packet(raw)
        except (BlockingIOError, ValueError):
            continue
        if isinstance(packet, BlockReadyV2):
            client = addr
            session_id = packet.session_id
    if client is None:
        source.close()
        sock.close()
        raise TimeoutError("tree-stream server timed out waiting for READY")

    meta = BlockMetaV2(
        session_id,
        layout.total_bytes,
        TREE_META_NAME,
        symbol_size,
        block_k,
        initial_repair_pct,
        geometry.active_bytes,
        "",
    ).pack()
    for _ in range(8):
        sock.sendto(meta, client)

    feedback = SenderFeedbackState(session_id)
    stop = threading.Event()
    client_fin = threading.Event()
    pacer = AckPacer(min_bps, max_bps, start_bps, fec_frac=initial_repair_pct / 100.0)
    limiter = RateLimiter(max_bps, start_bps=start_bps, min_frac=min_bps / max_bps)

    def feedback_loop() -> None:
        while not stop.is_set():
            readable, _, _ = select.select([sock], [], [], 0.05)
            if not readable:
                continue
            while True:
                try:
                    raw, _ = sock.recvfrom(4096)
                except BlockingIOError:
                    break
                try:
                    packet = parse_v2_packet(raw)
                except ValueError:
                    continue
                if isinstance(packet, BlockFeedbackV2):
                    if feedback.apply(packet):
                        limiter.set_rate(
                            pacer.update(packet.unique_payload_bytes, time.monotonic())
                        )
                elif (
                    isinstance(packet, BlockFinV2)
                    and packet.session_id == session_id
                    and packet.ok
                ):
                    client_fin.set()

    fb_thread = threading.Thread(target=feedback_loop, daemon=True)
    fb_thread.start()

    active: dict[int, SenderBlockState] = {}
    enc_cache: dict[int, GenEncoder] = {}
    next_block = 0
    last_repair = 0.0
    extra_win = ExtraRepairWindow()
    repair_ctl = RepairDebtController(float(initial_repair_pct))
    use_pool = total_blocks >= 48
    encode_pool = ThreadPoolExecutor(max_workers=_encode_workers()) if use_pool else None
    inflight: dict[int, Future] = {}
    ready: dict[int, tuple[list[bytes], int, GenEncoder]] = {}
    prefetch = min(8, max(2, geometry.active_blocks))

    def send_wires(wires: list[bytes]) -> None:
        for pos in range(0, len(wires), _SEND_CHUNK):
            batch = wires[pos : pos + _SEND_CHUNK]
            limiter.consume(sum(map(len, batch)))
            send_datagrams(sock, client, batch, chunk=_SEND_CHUNK)

    def pump_ready() -> None:
        for bid in [b for b, fut in inflight.items() if fut.done()]:
            fut = inflight.pop(bid)
            _block_id, wires, budget, encoder = fut.result()
            ready[bid] = (wires, budget, encoder)

    def submit_ahead() -> None:
        pump_ready()
        bid = next_block
        queued = 0
        while bid < total_blocks and queued < prefetch:
            queued += 1
            if bid in active or bid in ready or bid in inflight:
                bid += 1
                continue
            inflight[bid] = encode_pool.submit(
                _encode_block,
                source,
                bid,
                geometry.block_bytes,
                symbol_size,
                repair_ctl.current,
                session_id,
            )
            bid += 1

    def repair_tick(opened, now: float) -> None:
        tail = next_block >= total_blocks
        candidates = select_repair_candidates(
            active,
            opened,
            now,
            block_k=block_k,
            tail=tail,
            age_s=REPAIR_AGE_S,
            cooldown_s=TAIL_REPAIR_COOLDOWN_S if tail else REPAIR_COOLDOWN_S,
        )
        total_need = sum(need for need, _age, _bid in candidates)
        budget, tick_s = repair_tick_limits(total_need, tail=tail)
        t0 = time.perf_counter()
        sent = 0
        for need, _age, block_id in candidates:
            if sent >= budget or (time.perf_counter() - t0) >= tick_s:
                break
            encoder = enc_cache.get(block_id)
            if encoder is None:
                continue
            state = active[block_id]
            n = min(need, budget - sent)
            prev = encoder.packet_count
            new_packets = encoder.ensure_repair(state.repair_emitted + n)
            if not new_packets:
                continue
            stamp = int(now * 1_000_000) & 0xFFFFFFFF
            send_wires(
                pack_data_packets(session_id, block_id, new_packets, prev, stamp)
            )
            state.repair_emitted += len(new_packets)
            state.last_repair_ts = now
            sent += len(new_packets)

    try:
        if use_pool:
            submit_ahead()
        while not client_fin.is_set():
            completed, opened, _unique, _decoded = feedback.snapshot()
            now = time.monotonic()
            for block_id in list(active):
                if block_id not in completed:
                    continue
                state = active.pop(block_id)
                extra_win.observe(state.repair_emitted > state.initial_repair)
                enc_cache.pop(block_id, None)
            pacer.repair_busy = extra_win.pressure()
            if use_pool:
                submit_ahead()

            admitted = False
            while next_block < total_blocks and len(active) < geometry.active_blocks:
                if use_pool:
                    pump_ready()
                    item = ready.pop(next_block, None)
                    if item is None:
                        submit_ahead()
                        break
                    wires, budget, encoder = item
                else:
                    payload, _tlen = source.read_block(next_block, geometry.block_bytes)
                    wires, budget, encoder = _encode_payload(
                        payload,
                        symbol_size,
                        repair_ctl.current,
                        session_id,
                        next_block,
                    )
                enc_cache[next_block] = encoder
                active[next_block] = SenderBlockState(
                    next_block,
                    initial_repair=budget,
                    repair_emitted=budget,
                    sent_at=now,
                )
                send_wires(wires)
                next_block += 1
                admitted = True
                if use_pool:
                    submit_ahead()

            tail = next_block >= total_blocks
            if now - last_repair >= REPAIR_INTERVAL_S and active:
                repair_tick(opened, now)
                last_repair = now
            elif tail and active:
                repair_tick(opened, now)
                last_repair = now

            if next_block >= total_blocks and not active:
                for _ in range(8):
                    sock.sendto(BlockFinV2(session_id, total_blocks).pack(), client)
                    time.sleep(0.005)
                grace = time.monotonic() + 2.0
                while not client_fin.is_set() and time.monotonic() < grace:
                    time.sleep(0.02)
                break
            if not admitted and not active:
                time.sleep(0.002)
    finally:
        stop.set()
        fb_thread.join(timeout=1.0)
        if encode_pool is not None:
            encode_pool.shutdown(wait=False)
        source.close()
        sock.close()
    return 0


def run_tree_client(
    host: str,
    port: int,
    output_dir: Path,
    *,
    wan: bool = False,
    active_bytes: int = 4 << 20,
    timeout_s: float = 180.0,
) -> int:
    del wan
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_RCVBUF, 32 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_SNDBUF, 8 * 1024 * 1024)
    sock.setblocking(False)
    server = (host, port)
    session_id = random.SystemRandom().randrange(1, 0xFFFFFFFF)
    ready = BlockReadyV2(session_id, active_bytes).pack()
    for _ in range(8):
        sock.sendto(ready, server)

    meta: BlockMetaV2 | None = None
    deadline = time.monotonic() + 30.0
    while meta is None and time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], 0.5)
        if not readable:
            sock.sendto(ready, server)
            continue
        try:
            raw, _ = sock.recvfrom(4096)
            packet = parse_v2_packet(raw)
        except (BlockingIOError, ValueError):
            continue
        if isinstance(packet, BlockMetaV2) and packet.session_id == session_id:
            meta = packet
    if meta is None:
        sock.close()
        raise TimeoutError("tree-stream client timed out waiting for META")
    if meta.file_name != TREE_META_NAME:
        sock.close()
        raise ValueError("not a tree-stream session")

    geometry = BlockGeometry(meta.symbol_size, meta.block_k, meta.active_bytes)
    total_blocks = max(1, math.ceil(max(0, meta.file_size) / geometry.block_bytes))
    output_dir.mkdir(parents=True, exist_ok=True)
    staging = output_dir / STAGING_NAME
    fd = os.open(staging, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)
    os.ftruncate(fd, meta.file_size)
    slots: dict[int, GenReceiveSlot] = {}
    done_blocks: set[int] = set()
    unique_payload_bytes = 0
    decoded_bytes = 0
    feedback_id = 0
    last_feedback = 0.0
    last_echo = 0
    t0 = time.monotonic()
    last_log = t0
    slot_seen: dict[int, float] = {}

    def send_feedback(force: bool = False) -> None:
        nonlocal feedback_id, last_feedback
        now = time.monotonic()
        if not force and now - last_feedback < _FEEDBACK_S:
            return
        feedback_id += 1
        opened = [
            OpenBlock(
                block_id,
                slot.symbols_rx,
                slot.decode_failed,
                min(255, int((now - slot_seen.get(block_id, now)) / 0.020)),
            )
            for block_id, slot in sorted(slots.items())
            if block_id not in done_blocks
        ][:64]
        packet = BlockFeedbackV2(
            session_id,
            feedback_id,
            unique_payload_bytes,
            decoded_bytes,
            last_echo,
            sorted(done_blocks),
            opened,
        )
        sock.sendto(packet.pack(), server)
        last_feedback = now

    try:
        while len(done_blocks) < total_blocks:
            readable, _, _ = select.select([sock], [], [], 0.02)
            if readable:
                try:
                    batch = recv_datagrams(sock, 64)
                except BlockingIOError:
                    batch = []
                for raw in batch:
                    try:
                        packet = parse_v2_packet(raw)
                    except ValueError:
                        continue
                    if getattr(packet, "session_id", None) != session_id:
                        continue
                    if isinstance(packet, BlockDataV2):
                        block_id = packet.block_id
                        if block_id >= total_blocks or block_id in done_blocks:
                            continue
                        off = block_id * geometry.block_bytes
                        tlen = min(geometry.block_bytes, meta.file_size - off)
                        slot = slots.get(block_id)
                        if slot is None:
                            slot = GenReceiveSlot(
                                block_id,
                                gen_k=meta.block_k,
                                symbol_size=meta.symbol_size,
                                block_bytes=geometry.block_bytes,
                                tlen=tlen,
                            )
                            slots[block_id] = slot
                            slot_seen[block_id] = time.monotonic()
                        before = slot.symbols_rx
                        decoded = slot.add_packet(packet.payload, packet.esi)
                        if slot.symbols_rx == before:
                            continue
                        unique_payload_bytes += len(raw)
                        last_echo = packet.send_ts_us
                        if decoded is not None:
                            os.pwrite(fd, decoded[:tlen], off)
                            decoded_bytes += tlen
                            done_blocks.add(block_id)
                            slot.close()
                            slots.pop(block_id, None)
                            slot_seen.pop(block_id, None)
                    elif isinstance(packet, BlockFinV2):
                        continue
            send_feedback()
            now = time.monotonic()
            if now - last_log >= 1.0:
                elapsed = max(now - t0, 1e-6)
                print(
                    f"tree progress {len(done_blocks)}/{total_blocks} "
                    f"({100.0 * len(done_blocks) / total_blocks:.1f}%) "
                    f"app={decoded_bytes / elapsed / 1048576:.1f}MiB/s",
                    flush=True,
                )
                last_log = now
            if time.monotonic() - t0 > timeout_s:
                raise TimeoutError("tree-stream client timed out")
        send_feedback(force=True)
        for _ in range(16):
            sock.sendto(BlockFinV2(session_id, total_blocks).pack(), server)
            time.sleep(0.005)
    finally:
        for slot in slots.values():
            slot.close()
        os.close(fd)
        sock.close()

    wire_s = max(time.monotonic() - t0, 1e-6)
    print(
        f"tree-stream wire: {meta.file_size} bytes blocks={len(done_blocks)} "
        f"in {wire_s:.2f}s ({meta.file_size / wire_s / 1048576:.2f} MiB/s)",
        flush=True,
    )
    t_mat = time.monotonic()
    nfiles = materialize_from_staging(staging, output_dir)
    staging.unlink(missing_ok=True)
    mat_s = max(time.monotonic() - t_mat, 1e-6)
    elapsed = max(time.monotonic() - t0, 1e-6)
    print(
        f"tree-stream tree: {nfiles} files in {mat_s:.2f}s",
        flush=True,
    )
    print(
        f"tree-stream OK: {nfiles} files {meta.file_size} bytes "
        f"blocks={len(done_blocks)} in {elapsed:.2f}s "
        f"({meta.file_size / wire_s / 1048576:.2f} MiB/s wire)",
        flush=True,
    )
    return 0
