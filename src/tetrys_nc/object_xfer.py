"""Object-mux: many named blobs over one RaptorQ UDP session."""

from __future__ import annotations

import os
import random
import select
import socket
import sys
import threading
import time
from collections import OrderedDict, deque
from pathlib import Path

from .block_packets import (
    MUX_META_NAME,
    BlockData,
    BlockFeedback,
    BlockFin,
    BlockMeta,
    BlockReady,
    OpenBlock,
    ObjectFin,
    ObjectOpen,
    pack_data_packets,
    parse_packet,
)
from .block_state import (
    REPAIR_AGE_S,
    REPAIR_COOLDOWN_S,
    REPAIR_INTERVAL_S,
    BlockGeometry,
    SenderBlockState,
    SenderFeedbackState,
    repair_tick_limits,
    select_repair_candidates,
)
from .block_xfer import _pace_limits
from .gen_raptor import GenEncoder, GenReceiveSlot
from .netutil import recv_datagrams, send_datagrams, try_set_buffer
from .object_frames import FRAME_HDR, BlockFill, ObjectCursor, is_pack_name, split_for_session, unpack_block, unpack_files
from .ratectl import RateLimiter

_FEEDBACK_S = 0.020
_SEND_CHUNK = 64
_BAR_W = 22


def _bar(frac: float, width: int = _BAR_W) -> str:
    frac = 0.0 if frac < 0 else 1.0 if frac > 1 else frac
    filled = int(round(width * frac))
    return "█" * filled + "░" * (width - filled)


def _fmt_size(n: int) -> str:
    if n >= 1048576:
        return f"{n / 1048576:.1f}MiB"
    if n >= 1024:
        return f"{n / 1024:.1f}KiB"
    return f"{n}B"


def _fmt_rate(bps: float) -> str:
    return f"{max(0.0, bps) / 1048576:.1f}MiB/s"


def _file_bar_line(
    name: str, wrote: int, size: int, done: bool, rate_bps: float | None = None
) -> str:
    total = size or wrote
    frac = 1.0 if done else ((wrote / total) if total else 0.0)
    pct = 100.0 if done else 100.0 * frac
    shown = total if done else wrote
    line = (
        f"{name[:36]:<36} [{_bar(frac)}] {pct:5.1f}% "
        f"{_fmt_size(shown)}/{_fmt_size(size)}"
    )
    if rate_bps is not None:
        line += f"  {_fmt_rate(rate_bps)}"
    return line


def _redraw_file_bars(lines: list[str], prev_rows: int) -> int:
    n = len(lines)
    parts: list[str] = []
    if prev_rows:
        parts.append(f"\x1b[{prev_rows}A")
    for line in lines:
        parts.append(f"\x1b[2K{line}\n")
    extra = prev_rows - n
    if extra > 0:
        parts.extend("\x1b[2K\n" for _ in range(extra))
        parts.append(f"\x1b[{extra}A")
    sys.stdout.write("".join(parts))
    sys.stdout.flush()
    return n


def _safe_name(name: str) -> str:
    base = Path(name).name
    return base if base and base not in {".", ".."} else f"obj_{abs(hash(name)) & 0xFFFF:x}"


def _udp(host: str | None, port: int | None, *, snd: int, rcv: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_SNDBUF, snd)
    try_set_buffer(sock, socket.SO_RCVBUF, rcv)
    if host is not None and port is not None:
        sock.bind((host, port))
    sock.setblocking(False)
    return sock


def _burst(sock: socket.socket, addr, wire: bytes, n: int, pause: float = 0.005) -> None:
    for i in range(n):
        sock.sendto(wire, addr)
        if pause and i + 1 < n:
            time.sleep(pause)


class ObjectSession:
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
            return self._pending.popleft() if self._pending else None

    def idle(self, current: ObjectCursor | None) -> bool:
        with self._lock:
            return self._closed and not self._pending and current is None


def _fill_block(geometry: BlockGeometry, session: ObjectSession, current, announce, finish):
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
    return (fill.packed() if fill.chunks else None), current


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
    sock = _udp(host, port, snd=8 << 20, rcv=8 << 20)
    print(
        f"object-mux server udp://{host}:{port} K={block_k} T={symbol_size} "
        f"fec={initial_repair_pct}%",
        flush=True,
    )
    client, session_id, deadline = None, 0, time.monotonic() + 30.0
    while client is None and time.monotonic() < deadline:
        if not select.select([sock], [], [], 0.5)[0]:
            continue
        try:
            raw, addr = sock.recvfrom(2048)
            packet = parse_packet(raw)
        except (BlockingIOError, ValueError):
            continue
        if isinstance(packet, BlockReady):
            client, session_id = addr, packet.session_id
    if client is None:
        sock.close()
        raise TimeoutError("object-mux server timed out waiting for READY")

    meta = BlockMeta(
        session_id, 0, MUX_META_NAME, symbol_size, block_k,
        initial_repair_pct, geometry.active_bytes, "",
    ).pack()
    _burst(sock, client, meta, 8, 0.0)
    return run_object_session(
        sock, client, session_id, session,
        geometry=geometry,
        initial_repair_pct=initial_repair_pct,
        min_bps=min_bps,
        max_bps=max_bps,
        start_bps=start_bps,
        close_sock=True,
    )


def queue_disk_files(session: ObjectSession, files: list[tuple[str, Path]]) -> int:
    blobs = [(name, path.read_bytes()) for name, path in files]
    packed = split_for_session(blobs)
    for name, blob in packed:
        session.put(name, blob)
    session.close()
    return len(files)


def run_object_session(
    sock: socket.socket,
    client,
    session_id: int,
    session: ObjectSession,
    *,
    geometry: BlockGeometry,
    initial_repair_pct: int,
    min_bps: float,
    max_bps: float,
    start_bps: float,
    close_sock: bool = False,
) -> int:
    block_k = geometry.block_k
    symbol_size = geometry.symbol_size
    limiter = RateLimiter(max_bps, start_bps=start_bps, min_frac=min_bps / max_bps)
    feedback = SenderFeedbackState(session_id)
    stop, client_fin = threading.Event(), threading.Event()
    t0 = time.monotonic()

    def feedback_loop() -> None:
        while not stop.is_set():
            if not select.select([sock], [], [], 0.05)[0]:
                continue
            while True:
                try:
                    packet = parse_packet(sock.recvfrom(4096)[0])
                except BlockingIOError:
                    break
                except ValueError:
                    continue
                if isinstance(packet, BlockFeedback):
                    feedback.apply(packet)
                elif isinstance(packet, BlockFin) and packet.session_id == session_id and packet.ok:
                    client_fin.set()

    fb_thread = threading.Thread(target=feedback_loop, daemon=True)
    fb_thread.start()
    active: dict[int, SenderBlockState] = {}
    enc_cache: dict[int, GenEncoder] = {}
    current = None
    next_block = 0
    last_repair = 0.0
    announced: set[int] = set()

    def announce(cursor: ObjectCursor) -> None:
        if cursor.obj_id in announced:
            return
        sock.sendto(
            ObjectOpen(session_id, cursor.obj_id, len(cursor.data), cursor.name).pack(),
            client,
        )
        announced.add(cursor.obj_id)

    def finish(cursor: ObjectCursor) -> None:
        sock.sendto(
            ObjectFin(session_id, cursor.obj_id, len(cursor.data), cursor.name).pack(),
            client,
        )

    def send_wires(wires: list[bytes]) -> None:
        for i in range(0, len(wires), _SEND_CHUNK):
            batch = wires[i : i + _SEND_CHUNK]
            limiter.consume(sum(map(len, batch)))
            send_datagrams(sock, client, batch, chunk=_SEND_CHUNK)

    def repair_tick(opened, now: float) -> None:
        candidates = select_repair_candidates(
            active, opened, now, block_k=block_k, tail=session.idle(current),
            age_s=REPAIR_AGE_S, cooldown_s=REPAIR_COOLDOWN_S,
        )
        budget, tick_s = repair_tick_limits(sum(n for n, *_ in candidates), tail=session.idle(current))
        t0, sent = time.perf_counter(), 0
        for need, _age, block_id in candidates:
            if sent >= budget or (time.perf_counter() - t0) >= tick_s:
                break
            encoder, state = enc_cache[block_id], active[block_id]
            prev = encoder.packet_count
            new_packets = encoder.ensure_repair(state.repair_emitted + min(need, budget - sent))
            if not new_packets:
                continue
            stamp = int(now * 1_000_000) & 0xFFFFFFFF
            send_wires(pack_data_packets(session_id, block_id, new_packets, prev, stamp))
            state.repair_emitted += len(new_packets)
            state.last_repair_ts = now
            sent += len(new_packets)

    try:
        aborted = False
        while not client_fin.is_set():
            completed, opened, *_ = feedback.snapshot()
            now = time.monotonic()
            if feedback.client_lost(now, t0):
                aborted = True
                print(
                    f"abort — client silent mux blocks={next_block}",
                    flush=True,
                )
                break
            for block_id in [b for b in active if b in completed]:
                active.pop(block_id)
                enc_cache.pop(block_id, None)
            admitted = False
            while len(active) < geometry.active_blocks:
                payload, current = _fill_block(geometry, session, current, announce, finish)
                if payload is None:
                    break
                encoder = GenEncoder(payload, symbol_size, initial_repair_pct)
                stamp = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
                send_wires(pack_data_packets(session_id, next_block, encoder.packets(), 0, stamp))
                enc_cache[next_block] = encoder
                active[next_block] = SenderBlockState(
                    next_block, initial_repair=encoder.repair_budget,
                    repair_emitted=encoder.repair_budget, sent_at=now,
                )
                next_block += 1
                admitted = True
            if now - last_repair >= REPAIR_INTERVAL_S and active:
                repair_tick(opened, now)
                last_repair = now
            if session.idle(current) and not active and next_block > 0:
                _burst(sock, client, BlockFin(session_id, next_block).pack(), 8)
                grace = time.monotonic() + 2.0
                while not client_fin.is_set() and time.monotonic() < grace:
                    time.sleep(0.02)
                break
            if not admitted and not active:
                time.sleep(0.002)
    finally:
        stop.set()
        fb_thread.join(timeout=1.0)
        if close_sock:
            sock.close()
    elapsed = max(time.monotonic() - t0, 1e-6)
    if not aborted:
        print(
            f"done in {elapsed:.2f}s — mux blocks={next_block} "
            f"{next_block * geometry.block_bytes / elapsed / 1048576:.1f} MiB/s payload",
            flush=True,
        )
    return 0


class _Sink:
    def __init__(self, output_dir: Path) -> None:
        self.dir = output_dir
        self.names: dict[int, str] = {}
        self.sizes: dict[int, int] = {}
        self.written: dict[int, int] = {}
        self.finished: set[int] = set()
        self.pending: dict[int, list] = {}
        self._fds: OrderedDict[int, int] = OrderedDict()
        self._truncated: set[int] = set()
        self.decoded = 0
        self.just_finished: list[tuple[str, int]] = []

    def _mark_finished(self, obj_id: int) -> None:
        if obj_id in self.finished:
            return
        self.finished.add(obj_id)
        name = self.names.get(obj_id, f"obj_{obj_id}")
        self.just_finished.append((name, self.sizes.get(obj_id, 0)))

    def _fd(self, obj_id: int) -> int | None:
        if obj_id not in self.names:
            return None
        if obj_id in self._fds:
            self._fds.move_to_end(obj_id)
            return self._fds[obj_id]
        while len(self._fds) >= 64:
            os.close(self._fds.popitem(last=False)[1])
        fd = os.open(self.dir / self.names[obj_id], os.O_CREAT | os.O_RDWR, 0o644)
        if obj_id not in self._truncated:
            if self.sizes.get(obj_id, 0) > 0:
                os.ftruncate(fd, self.sizes[obj_id])
            self._truncated.add(obj_id)
        self._fds[obj_id] = fd
        return fd

    def _maybe_close(self, obj_id: int) -> None:
        size = self.sizes.get(obj_id)
        if obj_id not in self.finished or size is None or self.written.get(obj_id, 0) < size:
            return
        fd = self._fds.pop(obj_id, None)
        if fd is not None:
            os.close(fd)

    def open(self, obj_id: int, name: str, size: int) -> None:
        self.names[obj_id] = _safe_name(name)
        self.sizes[obj_id] = size
        self.written.setdefault(obj_id, 0)
        for chunk in self.pending.pop(obj_id, []):
            self.write(chunk)

    def write(self, chunk) -> None:
        fd = self._fd(chunk.obj_id)
        if fd is None:
            self.pending.setdefault(chunk.obj_id, []).append(chunk)
            return
        os.pwrite(fd, chunk.data, chunk.offset)
        self.written[chunk.obj_id] = max(
            self.written.get(chunk.obj_id, 0), chunk.offset + len(chunk.data)
        )
        self.decoded += len(chunk.data)
        size = self.sizes.get(chunk.obj_id)
        if size is not None and self.written.get(chunk.obj_id, 0) >= size:
            self._mark_finished(chunk.obj_id)
        self._maybe_close(chunk.obj_id)

    def fin(self, obj_id: int, name: str, size: int) -> None:
        name = name or f"obj_{obj_id}.bin"
        if obj_id not in self.names:
            self.open(obj_id, name, size)
        else:
            safe, old = _safe_name(name), self.names[obj_id]
            if old != safe:
                src, dst = self.dir / old, self.dir / safe
                if src.exists() and src != dst:
                    src.replace(dst)
                self.names[obj_id] = safe
        self.sizes[obj_id] = size
        if self.written.get(obj_id, 0) >= size:
            self._mark_finished(obj_id)
        else:
            self.finished.add(obj_id)
        self._maybe_close(obj_id)

    def ingest(self, payload: bytes) -> None:
        for chunk in unpack_block(payload):
            if chunk.name:
                self.open(chunk.obj_id, chunk.name, chunk.size)
            self.write(chunk)

    def all_bars(self) -> list[tuple[str, int, int, bool]]:
        rows: list[tuple[str, int, int, bool]] = []
        for oid, name in self.names.items():
            size = self.sizes.get(oid, 0)
            wrote = self.written.get(oid, 0)
            done = oid in self.finished or (size > 0 and wrote >= size)
            rows.append((name, wrote, size, done))
        return rows

    def explode_packs(self) -> None:
        for path in list(self.dir.iterdir()):
            if path.is_file() and is_pack_name(path.name):
                for fname, blob in unpack_files(path.read_bytes()):
                    (self.dir / _safe_name(fname)).write_bytes(blob)
                path.unlink()

    def close(self) -> None:
        for fd in self._fds.values():
            os.close(fd)
        self._fds.clear()

    def check(self) -> None:
        missing = [oid for oid, size in self.sizes.items() if self.written.get(oid, 0) < size]
        if missing:
            raise ValueError(f"incomplete objects {missing}")
        if self.pending:
            raise ValueError(f"unnamed object chunks {sorted(self.pending)}")


def run_object_client(
    host: str,
    port: int,
    output_dir: Path,
    *,
    wan: bool = False,
    active_bytes: int = 4 << 20,
    timeout_s: float = 180.0,
    file_progress: bool = False,
) -> int:
    del wan
    sock = _udp(None, None, snd=8 << 20, rcv=32 << 20)
    server = (host, port)
    session_id = random.SystemRandom().randrange(1, 0xFFFFFFFF)
    ready = BlockReady(session_id, active_bytes).pack()
    _burst(sock, server, ready, 8, 0.0)

    meta, deadline = None, time.monotonic() + 30.0
    while meta is None and time.monotonic() < deadline:
        if not select.select([sock], [], [], 0.5)[0]:
            sock.sendto(ready, server)
            continue
        try:
            packet = parse_packet(sock.recvfrom(4096)[0])
        except (BlockingIOError, ValueError):
            continue
        if isinstance(packet, BlockMeta) and packet.session_id == session_id:
            meta = packet
    if meta is None:
        sock.close()
        raise TimeoutError("object-mux client timed out waiting for META")
    if meta.file_name != MUX_META_NAME:
        sock.close()
        raise ValueError("not an object-mux session")
    return consume_object_stream(
        sock, server, session_id, meta, output_dir, timeout_s, close_sock=True,
        file_progress=file_progress,
    )


def consume_object_stream(
    sock: socket.socket,
    server,
    session_id: int,
    meta: BlockMeta,
    output_dir: Path,
    timeout_s: float,
    *,
    close_sock: bool = True,
    file_progress: bool = False,
) -> int:

    geometry = BlockGeometry(meta.symbol_size, meta.block_k, meta.active_bytes)
    output_dir.mkdir(parents=True, exist_ok=True)
    sink = _Sink(output_dir)
    slots: dict[int, GenReceiveSlot] = {}
    done: set[int] = set()
    unique = feedback_id = last_echo = 0
    last_fb = 0.0
    total_blocks: int | None = None
    t0 = time.monotonic()
    last_log = t0
    last_draw = 0.0
    bar_rows = 0
    tty = sys.stdout.isatty()
    rate_t = t0
    rate_b = 0
    inst_bps = 0.0

    def sample_rate(now: float, nbytes: int) -> float:
        nonlocal rate_t, rate_b, inst_bps
        dt = now - rate_t
        if dt >= 0.2:
            inst_bps = (nbytes - rate_b) / dt
            rate_t, rate_b = now, nbytes
        elif now > t0:
            inst_bps = nbytes / (now - t0)
        return inst_bps

    def send_feedback(force: bool = False) -> None:
        nonlocal feedback_id, last_fb
        now = time.monotonic()
        if not force and now - last_fb < _FEEDBACK_S:
            return
        feedback_id += 1
        opened = [
            OpenBlock(bid, slot.symbols_rx, False, 0) for bid, slot in sorted(slots.items())
        ][:64]
        sock.sendto(
            BlockFeedback(
                session_id, feedback_id, unique, sink.decoded, last_echo, sorted(done), opened
            ).pack(),
            server,
        )
        last_fb = now

    try:
        while True:
            if select.select([sock], [], [], 0.02)[0]:
                try:
                    batch = recv_datagrams(sock, 64)
                except BlockingIOError:
                    batch = []
                for raw in batch:
                    try:
                        packet = parse_packet(raw)
                    except ValueError:
                        continue
                    if getattr(packet, "session_id", None) != session_id:
                        continue
                    if isinstance(packet, ObjectOpen) and packet.obj_id not in sink.names:
                        sink.open(packet.obj_id, packet.name, packet.size)
                    elif isinstance(packet, ObjectFin):
                        sink.fin(packet.obj_id, packet.name, packet.size)
                    elif isinstance(packet, BlockData):
                        if packet.block_id in done:
                            continue
                        slot = slots.get(packet.block_id)
                        if slot is None:
                            slot = GenReceiveSlot(
                                packet.block_id,
                                gen_k=meta.block_k,
                                symbol_size=meta.symbol_size,
                                block_bytes=geometry.block_bytes,
                                tlen=geometry.block_bytes,
                            )
                            slots[packet.block_id] = slot
                        before = slot.symbols_rx
                        decoded = slot.add_packet(packet.payload, packet.esi)
                        if slot.symbols_rx == before:
                            continue
                        unique += len(raw)
                        last_echo = packet.send_ts_us
                        if decoded is not None:
                            sink.ingest(decoded[: geometry.block_bytes])
                            done.add(packet.block_id)
                            slot.close()
                            slots.pop(packet.block_id, None)
                    elif isinstance(packet, BlockFin):
                        total_blocks = packet.total_blocks
            send_feedback()
            now = time.monotonic()
            if file_progress:
                inst = sample_rate(now, sink.decoded)
                if tty and now - last_draw >= 0.05:
                    lines = [
                        _file_bar_line(name, wrote, size, done, inst)
                        for name, wrote, size, done in sink.all_bars()
                    ]
                    if lines:
                        bar_rows = _redraw_file_bars(lines, bar_rows)
                        last_draw = now
                elif not tty:
                    for name, size in sink.just_finished:
                        print(
                            _file_bar_line(name, size, size, True, inst),
                            flush=True,
                        )
                    sink.just_finished.clear()
            elif now - last_log >= 1.0:
                elapsed = max(now - t0, 1e-6)
                inst = sample_rate(now, sink.decoded)
                blk = (
                    f"{len(done)}/{total_blocks}"
                    if total_blocks is not None
                    else str(len(done))
                )
                print(
                    f"progress files={len(sink.finished)} blocks={blk} "
                    f"app={sink.decoded / elapsed / 1048576:.1f}MiB/s "
                    f"inst={_fmt_rate(inst)}",
                    flush=True,
                )
                last_log = now
            if (
                total_blocks
                and all(i in done for i in range(total_blocks))
                and not sink.pending
            ):
                break
            if time.monotonic() - t0 > timeout_s:
                raise TimeoutError("object-mux client timed out")
        send_feedback(force=True)
        _burst(sock, server, BlockFin(session_id, total_blocks or 0).pack(), 16)
        if file_progress and tty:
            inst = sample_rate(time.monotonic(), sink.decoded)
            lines = [
                _file_bar_line(name, wrote, size, done, inst)
                for name, wrote, size, done in sink.all_bars()
            ]
            if lines:
                _redraw_file_bars(lines, bar_rows)
    finally:
        for slot in slots.values():
            slot.close()
        sink.close()
        if close_sock:
            sock.close()
    sink.check()
    sink.explode_packs()
    nfiles = sum(1 for p in output_dir.iterdir() if p.is_file())
    nbytes = sum(p.stat().st_size for p in output_dir.iterdir() if p.is_file())
    elapsed = max(time.monotonic() - t0, 1e-6)
    print(
        f"done in {elapsed:.2f}s — goodput {nbytes / elapsed / 1048576:.2f} MiB/s — "
        f"files={nfiles} in {output_dir} blocks={len(done)} "
        f"decoded={sink.decoded / 1048576:.1f}MiB",
        flush=True,
    )
    return 0
