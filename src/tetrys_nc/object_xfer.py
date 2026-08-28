"""Object-mux session on top of v2 RaptorQ blocks.

One UDP session, many files enqueued while it runs. File bytes are packed
into the same K×T blocks as single-file v2; OPEN/FIN are control datagrams.
"""

from __future__ import annotations

import os
import random
import select
import socket
import threading
import time
from collections import OrderedDict, deque
from pathlib import Path

from .block_packets import (
    MUX_META_NAME,
    BlockFeedbackV2,
    BlockFinV2,
    BlockMetaV2,
    BlockReadyV2,
    OpenBlock,
    ObjectFinV2,
    ObjectOpenV2,
    ObjectResetV2,
    BlockDataV2,
    pack_data_packets,
    parse_v2_packet,
)
from .block_state import (
    AckPacer,
    BlockGeometry,
    REPAIR_AGE_S,
    REPAIR_COOLDOWN_S,
    REPAIR_INTERVAL_S,
    RepairDebtController,
    SenderBlockState,
    SenderFeedbackState,
    ExtraRepairWindow,
    repair_tick_limits,
    select_repair_candidates,
)
from .block_xfer import _pace_limits
from .gen_raptor import GenEncoder, GenReceiveSlot
from .netutil import recv_datagrams, send_datagrams, try_set_buffer
from .object_frames import FRAME_HDR, BlockFill, ObjectCursor, unpack_block
from .object_pack import is_pack_name, unpack_files
from .ratectl import RateLimiter

_FEEDBACK_S = 0.020
_FLUSH_S = 0.008
_SEND_CHUNK = 64


class ObjectSession:
    """Thread-safe queue of named blobs. close() means no more puts."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: deque[ObjectCursor] = deque()
        self._next_id = 1
        self._closed = False

    def put(self, name: str, data: bytes) -> int:
        safe = Path(name).name
        if not safe or safe in {".", ".."}:
            raise ValueError("invalid object name")
        with self._lock:
            if self._closed:
                raise RuntimeError("session already closed")
            obj_id = self._next_id
            self._next_id += 1
            self._pending.append(ObjectCursor(obj_id, safe, data))
            return obj_id

    def close(self) -> None:
        with self._lock:
            self._closed = True

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def pop_pending(self) -> ObjectCursor | None:
        with self._lock:
            if not self._pending:
                return None
            return self._pending.popleft()

    def pending_empty(self) -> bool:
        with self._lock:
            return not self._pending

    def idle(self, current: ObjectCursor | None) -> bool:
        with self._lock:
            return self._closed and not self._pending and current is None


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


def _fill_block(
    geometry: BlockGeometry,
    session: ObjectSession,
    current: ObjectCursor | None,
    *,
    flush: bool,
    announce,
    finish,
) -> tuple[bytes | None, ObjectCursor | None]:
    del flush
    fill = BlockFill(geometry.block_bytes)
    while fill.space() > FRAME_HDR:
        if current is None:
            current = session.pop_pending()
        if current is None:
            break
        announce(current)
        took = fill.take(current)
        if current.done:
            finish(current)
            current = None
        if not took:
            break
    if not fill.chunks:
        return None, current
    return fill.packed(), current


def run_object_server(
    host: str,
    port: int,
    session: ObjectSession,
    *,
    symbol_size: int = 256,
    block_k: int = 64,
    initial_repair_pct: int = 14,
    active_bytes: int = 4 << 20,
    rate_mbit: float = 400.0,
) -> int:
    geometry = BlockGeometry(symbol_size, block_k, active_bytes)
    min_bps, max_bps, start_bps = _pace_limits(rate_mbit)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_SNDBUF, 8 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_RCVBUF, 8 * 1024 * 1024)
    sock.bind((host, port))
    sock.setblocking(False)
    print(
        f"object-mux server udp://{host}:{port} K={block_k} T={symbol_size} "
        f"fec={initial_repair_pct}%",
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
        sock.close()
        raise TimeoutError("object-mux server timed out waiting for READY")

    meta = BlockMetaV2(
        session_id,
        0,
        MUX_META_NAME,
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
    current: ObjectCursor | None = None
    next_block = 0
    last_repair = 0.0
    last_fill = time.monotonic()
    extra_win = ExtraRepairWindow()
    repair_ctl = RepairDebtController(float(initial_repair_pct))
    announced: set[int] = set()

    def send_ctrl(wire: bytes) -> None:
        sock.sendto(wire, client)

    def announce(cursor: ObjectCursor) -> None:
        if cursor.obj_id in announced:
            return
        send_ctrl(
            ObjectOpenV2(
                session_id, cursor.obj_id, len(cursor.data), cursor.name
            ).pack()
        )
        announced.add(cursor.obj_id)

    def finish(cursor: ObjectCursor) -> None:
        send_ctrl(ObjectFinV2(session_id, cursor.obj_id, len(cursor.data), cursor.name).pack())

    def send_wires(wires: list[bytes]) -> None:
        for pos in range(0, len(wires), _SEND_CHUNK):
            batch = wires[pos : pos + _SEND_CHUNK]
            limiter.consume(sum(map(len, batch)))
            send_datagrams(sock, client, batch, chunk=_SEND_CHUNK)

    def repair_tick(opened, now: float) -> None:
        tail = session.idle(current)
        candidates = select_repair_candidates(
            active,
            opened,
            now,
            block_k=block_k,
            tail=tail,
            age_s=REPAIR_AGE_S,
            cooldown_s=REPAIR_COOLDOWN_S,
        )
        total_need = sum(need for need, _age, _bid in candidates)
        budget, tick_s = repair_tick_limits(total_need, tail=tail)
        t0 = time.perf_counter()
        sent = 0
        for need, _age, block_id in candidates:
            if sent >= budget or (time.perf_counter() - t0) >= tick_s:
                break
            encoder = enc_cache[block_id]
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

            admitted = False
            while len(active) < geometry.active_blocks:
                flush = session.closed or (now - last_fill >= _FLUSH_S)
                payload, current = _fill_block(
                    geometry,
                    session,
                    current,
                    flush=flush,
                    announce=announce,
                    finish=finish,
                )
                if payload is None:
                    break
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
                last_fill = now
                admitted = True

            if now - last_repair >= REPAIR_INTERVAL_S and active:
                repair_tick(opened, now)
                last_repair = now

            if session.idle(current) and not active and next_block > 0:
                for _ in range(8):
                    sock.sendto(BlockFinV2(session_id, next_block).pack(), client)
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
        sock.close()
    return 0


def _safe_name(name: str) -> str:
    base = Path(name).name
    if not base or base in {".", ".."}:
        return f"obj_{abs(hash(name)) & 0xFFFF:x}"
    return base


def run_object_client(
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
        raise TimeoutError("object-mux client timed out waiting for META")
    if meta.file_name != MUX_META_NAME:
        sock.close()
        raise ValueError("not an object-mux session")

    geometry = BlockGeometry(meta.symbol_size, meta.block_k, meta.active_bytes)
    output_dir.mkdir(parents=True, exist_ok=True)
    names: dict[int, str] = {}
    sizes: dict[int, int] = {}
    written: dict[int, int] = {}
    fd_lru: OrderedDict[int, int] = OrderedDict()
    truncated: set[int] = set()
    finished: set[int] = set()
    slots: dict[int, GenReceiveSlot] = {}
    done_blocks: set[int] = set()
    unique_payload_bytes = 0
    decoded_bytes = 0
    feedback_id = 0
    last_feedback = 0.0
    last_echo = 0
    total_blocks: int | None = None
    t0 = time.monotonic()
    pending_chunks: dict[int, list] = {}

    def _fd_for(obj_id: int) -> int | None:
        if obj_id not in names:
            return None
        if obj_id in fd_lru:
            fd_lru.move_to_end(obj_id)
            return fd_lru[obj_id]
        while len(fd_lru) >= 64:
            _oid, old = fd_lru.popitem(last=False)
            os.close(old)
        path = output_dir / names[obj_id]
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
        if obj_id not in truncated:
            size = sizes.get(obj_id, 0)
            if size > 0:
                os.ftruncate(fd, size)
            truncated.add(obj_id)
        fd_lru[obj_id] = fd
        return fd

    def open_obj(obj_id: int, name: str, size: int) -> None:
        names[obj_id] = _safe_name(name)
        sizes[obj_id] = size
        written.setdefault(obj_id, 0)
        for chunk in pending_chunks.pop(obj_id, []):
            _write_chunk(chunk)

    def _write_chunk(chunk) -> None:
        nonlocal decoded_bytes
        fd = _fd_for(chunk.obj_id)
        if fd is None:
            pending_chunks.setdefault(chunk.obj_id, []).append(chunk)
            return
        os.pwrite(fd, chunk.data, chunk.offset)
        written[chunk.obj_id] = max(
            written.get(chunk.obj_id, 0), chunk.offset + len(chunk.data)
        )
        decoded_bytes += len(chunk.data)
        _close_if_complete(chunk.obj_id)

    def _close_if_complete(obj_id: int) -> None:
        size = sizes.get(obj_id)
        if obj_id not in finished or size is None:
            return
        if written.get(obj_id, 0) < size:
            return
        fd = fd_lru.pop(obj_id, None)
        if fd is not None:
            os.close(fd)

    def send_feedback(force: bool = False) -> None:
        nonlocal feedback_id, last_feedback
        now = time.monotonic()
        if not force and now - last_feedback < _FEEDBACK_S:
            return
        feedback_id += 1
        opened = [
            OpenBlock(block_id, slot.symbols_rx, False, 0)
            for block_id, slot in sorted(slots.items())
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

    def ingest_block(payload: bytes) -> None:
        for chunk in unpack_block(payload):
            if chunk.name:
                open_obj(chunk.obj_id, chunk.name, chunk.size)
            _write_chunk(chunk)
            size = sizes.get(chunk.obj_id)
            if size is not None and written.get(chunk.obj_id, 0) >= size:
                finished.add(chunk.obj_id)
                _close_if_complete(chunk.obj_id)

    try:
        while True:
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
                    if isinstance(packet, ObjectOpenV2):
                        if packet.obj_id not in names:
                            open_obj(packet.obj_id, packet.name, packet.size)
                    elif isinstance(packet, ObjectResetV2):
                        fd = fd_lru.pop(packet.obj_id, None)
                        if fd is not None:
                            os.close(fd)
                        name = names.get(packet.obj_id)
                        if name:
                            path = output_dir / name
                            if path.exists():
                                path.unlink()
                    elif isinstance(packet, ObjectFinV2):
                        name = packet.name or f"obj_{packet.obj_id}.bin"
                        if packet.obj_id not in names:
                            open_obj(packet.obj_id, name, packet.size)
                        else:
                            safe = _safe_name(name)
                            old = names[packet.obj_id]
                            if old != safe:
                                src = output_dir / old
                                dst = output_dir / safe
                                if src.exists() and src != dst:
                                    src.replace(dst)
                                names[packet.obj_id] = safe
                        sizes[packet.obj_id] = packet.size
                        finished.add(packet.obj_id)
                        _close_if_complete(packet.obj_id)
                    elif isinstance(packet, BlockMetaV2):
                        continue
                    elif isinstance(packet, BlockDataV2):
                        block_id = packet.block_id
                        if block_id in done_blocks:
                            continue
                        slot = slots.get(block_id)
                        if slot is None:
                            slot = GenReceiveSlot(
                                block_id,
                                gen_k=meta.block_k,
                                symbol_size=meta.symbol_size,
                                block_bytes=geometry.block_bytes,
                                tlen=geometry.block_bytes,
                            )
                            slots[block_id] = slot
                        before = slot.symbols_rx
                        decoded = slot.add_packet(packet.payload, packet.esi)
                        if slot.symbols_rx == before:
                            continue
                        unique_payload_bytes += len(raw)
                        last_echo = packet.send_ts_us
                        if decoded is not None:
                            ingest_block(decoded[: geometry.block_bytes])
                            done_blocks.add(block_id)
                            slot.close()
                            slots.pop(block_id, None)
                    elif isinstance(packet, BlockFinV2):
                        total_blocks = packet.total_blocks
            send_feedback()
            if (
                total_blocks is not None
                and total_blocks > 0
                and all(i in done_blocks for i in range(total_blocks))
                and not pending_chunks
            ):
                break
            if time.monotonic() - t0 > timeout_s:
                raise TimeoutError("object-mux client timed out")
        send_feedback(force=True)
        for _ in range(16):
            sock.sendto(BlockFinV2(session_id, total_blocks or 0).pack(), server)
            time.sleep(0.005)
    finally:
        for slot in slots.values():
            slot.close()
        for fd in fd_lru.values():
            os.close(fd)
        fd_lru.clear()
        sock.close()

    missing = [oid for oid, size in sizes.items() if written.get(oid, 0) < size]
    if missing:
        raise ValueError(f"incomplete objects {missing}")
    if pending_chunks:
        raise ValueError(f"unnamed object chunks {sorted(pending_chunks)}")
    for path in list(output_dir.iterdir()):
        if path.is_file() and is_pack_name(path.name):
            for fname, blob in unpack_files(path.read_bytes()):
                (output_dir / _safe_name(fname)).write_bytes(blob)
            path.unlink()
    nfiles = sum(1 for p in output_dir.iterdir() if p.is_file())
    print(
        f"object-mux OK: {nfiles} files in {output_dir} "
        f"blocks={len(done_blocks)}",
        flush=True,
    )
    return 0
