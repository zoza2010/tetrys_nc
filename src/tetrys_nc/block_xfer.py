"""RaptorQ block transfer over UDP."""

from __future__ import annotations

import hashlib
import mmap
import multiprocessing
import os
import random
import select
import socket
import sys
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from .block_packets import (
    MUX_META_NAME,
    BlockData,
    BlockFeedback,
    BlockFin,
    BlockMeta,
    BlockReady,
    OpenBlock,
    pack_data_packets,
    parse_packet,
    stamp_data_wires,
)
from .block_state import (
    BlockGeometry,
    REPAIR_AGE_S,
    REPAIR_COOLDOWN_S,
    REPAIR_INTERVAL_S,
    TAIL_REPAIR_COOLDOWN_S,
    TAIL_REPAIR_TICK_PER_BLOCK,
    RepairDebtController,
    SenderBlockState,
    SenderFeedbackState,
    WAN_ACTIVE_BYTES,
    WAN_BLOCK_K,
    WAN_INITIAL_REPAIR_PCT,
    WAN_PACE_CAP_MBIT,
    WAN_CC_CAP_MBIT,
    WAN_START_MBIT,
    WAN_SYMBOL_SIZE,
    ExtraRepairWindow,
    block_loss_frac,
    percentile,
    repair_tick_limits,
    select_repair_candidates,
)
from .blastcc import BlastCc, _SEED_FRAC
from .gen_raptor import GenEncoder, GenReceiveSlot
from .netutil import recv_datagrams, send_datagrams, take_send_stats, try_set_buffer
from .ratectl import RateLimiter

_FEEDBACK_S = 0.020
_TAIL_IDLE_S = 5.0
_ENCODER_CACHE = 64
_SEND_CHUNK = 64

_worker_mm: mmap.mmap | None = None
_worker_path: str | None = None
_worker_lock = threading.Lock()


@dataclass
class LoopTimers:
    encode_s: float = 0.0
    pack_s: float = 0.0
    pace_s: float = 0.0
    send_s: float = 0.0
    repair_s: float = 0.0
    wait_s: float = 0.0
    source_pkts: int = 0
    repair_pkts: int = 0
    source_bytes: int = 0
    repair_bytes: int = 0

    def take(self) -> LoopTimers:
        snap = LoopTimers(
            self.encode_s,
            self.pack_s,
            self.pace_s,
            self.send_s,
            self.repair_s,
            self.wait_s,
            self.source_pkts,
            self.repair_pkts,
            self.source_bytes,
            self.repair_bytes,
        )
        self.encode_s = self.pack_s = self.pace_s = 0.0
        self.send_s = self.repair_s = self.wait_s = 0.0
        self.source_pkts = self.repair_pkts = 0
        self.source_bytes = self.repair_bytes = 0
        return snap


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(4 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def _client_bar(frac: float, width: int = 22) -> str:
    frac = 0.0 if frac < 0 else 1.0 if frac > 1 else frac
    filled = int(round(width * frac))
    return "█" * filled + "░" * (width - filled)


def _client_rate(bps: float) -> str:
    return f"{max(0.0, bps) / 1048576:.1f}MiB/s"


def _client_size(n: int) -> str:
    if n >= 1048576:
        return f"{n / 1048576:.1f}MiB"
    if n >= 1024:
        return f"{n / 1024:.1f}KiB"
    return f"{n}B"


def _block_data(
    mm: mmap.mmap, file_size: int, block_id: int, geometry: BlockGeometry
) -> tuple[bytes, int]:
    off = block_id * geometry.block_bytes
    tlen = min(geometry.block_bytes, max(0, file_size - off))
    data = bytes(mm[off : off + tlen])
    if len(data) < geometry.block_bytes:
        data += bytes(geometry.block_bytes - len(data))
    return data, tlen


def _worker_open(path: str) -> mmap.mmap:
    global _worker_mm, _worker_path
    with _worker_lock:
        if _worker_mm is None or _worker_path != path:
            fh = open(path, "rb")
            _worker_mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
            _worker_path = path
        return _worker_mm


def encode_block_job(
    path: str,
    block_id: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    session_id: int,
) -> tuple[int, list[bytes], int, float]:
    """Worker: mmap offset → RaptorQ packets already wrapped as DATA wires."""
    mm = _worker_open(path)
    off = block_id * block_bytes
    tlen = min(block_bytes, max(0, file_size - off))
    data = bytes(mm[off : off + tlen])
    if len(data) < block_bytes:
        data += bytes(block_bytes - len(data))
    t0 = time.perf_counter()
    encoder = GenEncoder(data, symbol_size, overhead_pct)
    encode_s = time.perf_counter() - t0
    stamp = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
    wires = pack_data_packets(session_id, block_id, encoder.packets(), 0, stamp)
    return block_id, wires, encoder.repair_budget, encode_s


def rebuild_block_encoder(
    mm: mmap.mmap,
    file_size: int,
    block_id: int,
    geometry: BlockGeometry,
    repair_emitted: int,
) -> GenEncoder:
    """Deterministic encoder rebuild from mmap (cache-pressure fallback)."""
    data, _tlen = _block_data(mm, file_size, block_id, geometry)
    encoder = GenEncoder(data, geometry.symbol_size, 0)
    if repair_emitted > encoder.repair_budget:
        encoder.ensure_repair(repair_emitted)
    return encoder


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _rate_cc_enabled(flag: bool | None) -> bool:
    if flag is True:
        return True
    if flag is False:
        return False
    env = os.environ.get("TETRYS_CC", "").strip().lower()
    if env in {"0", "off", "false", "no"}:
        return False
    if env in {"1", "on", "true", "blast", "yes"}:
        return True
    return True


def _pace_limits(rate_mbit: float, *, cc: bool = False) -> tuple[float, float, float]:
    if cc:
        cap_mbit = max(
            rate_mbit,
            1.0,
            _env_float("TETRYS_CC_CAP_MBIT", WAN_CC_CAP_MBIT),
        )
        max_bps = cap_mbit * 1_000_000 / 8
        start_mbit = min(cap_mbit, max(rate_mbit, 1.0))
        start_bps = start_mbit * 1_000_000 / 8
        min_bps = min(start_bps, max(80_000_000 / 8, max_bps * 0.10))
        return min_bps, max_bps, start_bps
    cap_mbit = min(
        max(rate_mbit, 1.0),
        _env_float("TETRYS_PACE_CAP_MBIT", WAN_PACE_CAP_MBIT),
    )
    max_bps = cap_mbit * 1_000_000 / 8
    start_mbit = _env_float("TETRYS_START_MBIT", WAN_START_MBIT)
    start_bps = min(max_bps, start_mbit * 1_000_000 / 8)
    # Floor at start. 0.85×start (595 Mbit) was the lock that made 2 GiB
    # runs drop to ~60 MiB/s after a few noisy ACK samples.
    min_frac = min(1.0, max(0.50, _env_float("TETRYS_PACE_MIN_FRAC", 1.0)))
    min_bps = max(1_000_000.0, min(max_bps, start_bps * min_frac))
    return min_bps, max_bps, start_bps


def _encode_workers() -> int:
    env = os.environ.get("TETRYS_ENCODE_WORKERS", "").strip()
    if env:
        return max(1, int(env))
    cpus = os.cpu_count() or 4
    return min(4, max(2, cpus - 1))


def _make_encode_pool(workers: int):
    """Process pool on Linux; thread pool elsewhere (spawn from test threads)."""
    if sys.platform.startswith("linux"):
        try:
            ctx = multiprocessing.get_context("fork")
            return ProcessPoolExecutor(max_workers=workers, mp_context=ctx)
        except (OSError, ValueError, RuntimeError):
            pass
    return ThreadPoolExecutor(max_workers=workers)


def _wait_block_ready(
    sock: socket.socket,
    geometry: BlockGeometry,
    active_cap: int,
    timeout_s: float | None,
) -> tuple[tuple[str, int], int, str] | None:
    deadline = None if timeout_s is None else time.monotonic() + timeout_s
    while deadline is None or time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], 0.5)
        if not readable:
            continue
        try:
            raw, addr = sock.recvfrom(2048)
            packet = parse_packet(raw)
        except (BlockingIOError, ValueError):
            continue
        if isinstance(packet, BlockReady):
            geometry.active_bytes = min(
                active_cap,
                max(2 * geometry.block_bytes, packet.active_bytes),
            )
            return addr, packet.session_id, packet.rel_path
    return None


def _safe_path(root: Path, rel: str) -> Path | None:
    if not rel or "\x00" in rel:
        return None
    candidate = Path(rel)
    if candidate.is_absolute():
        return None
    base = root.resolve()
    try:
        path = (base / candidate).resolve()
        path.relative_to(base)
    except (OSError, ValueError):
        return None
    return path


def _safe_join(root: Path, rel: str) -> Path | None:
    path = _safe_path(root, rel)
    if path is None or not path.is_file():
        return None
    return path


def _resolve_ready(
    root: Path, rel: str
) -> tuple[str, Path | list[tuple[str, Path]]] | None:
    parts = [p.strip() for p in rel.replace("\r", "").split("\n") if p.strip()]
    if not parts:
        return None
    if len(parts) == 1:
        path = _safe_path(root, parts[0])
        if path is None:
            return None
        if path.is_file():
            return "file", path
        if path.is_dir():
            files = [
                (child.name, child)
                for child in sorted(path.iterdir())
                if child.is_file() and not child.name.startswith(".")
            ]
            return ("mux", files) if files else None
        return None
    files: list[tuple[str, Path]] = []
    for part in parts:
        path = _safe_join(root, part)
        if path is None:
            return None
        files.append((Path(part).name, path))
    return "mux", files


def run_block_server(
    host: str,
    port: int,
    root: Path,
    *,
    default_file: str = "",
    symbol_size: int = WAN_SYMBOL_SIZE,
    block_k: int = WAN_BLOCK_K,
    initial_repair_pct: int = WAN_INITIAL_REPAIR_PCT,
    active_bytes: int = WAN_ACTIVE_BYTES,
    rate_mbit: float = 1150.0,
    ramp_s: float = 0.0,
    skip_hash: bool = False,
    rate_cc: bool | None = None,
    once: bool = True,
) -> int:
    geometry = BlockGeometry(symbol_size, block_k, active_bytes)
    root = root.resolve()
    workers = _encode_workers()
    prefetch_depth = min(64, max(geometry.active_blocks, 32))
    cc_on = _rate_cc_enabled(rate_cc)
    min_bps, max_bps, start_bps = _pace_limits(rate_mbit, cc=cc_on)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_SNDBUF, 128 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_RCVBUF, 16 * 1024 * 1024)
    sock.bind((host, port))
    sock.setblocking(False)
    print(
        f"server udp://{host}:{port} root={root} "
        f"default={default_file or '-'} "
        f"K={block_k} T={symbol_size} "
        f"block={geometry.block_bytes / 1048576:.2f}MiB active={geometry.active_blocks} "
        f"start={start_bps * 8 / 1e6:.0f}Mbit min={min_bps * 8 / 1e6:.0f}Mbit "
        f"cap={max_bps * 8 / 1e6:.0f}Mbit "
        f"cc={'blast' if cc_on else 'off'} "
        f"fec={initial_repair_pct}% enc_workers={workers} prefetch={prefetch_depth}",
        flush=True,
    )

    encode_pool = _make_encode_pool(workers)
    active_cap = active_bytes
    try:
        while True:
            print("waiting for READY", flush=True)
            got = _wait_block_ready(
                sock, geometry, active_cap, 60.0 if once else None
            )
            if got is None:
                raise TimeoutError("server timed out waiting for READY")
            client, session_id, rel_path = got
            rel = rel_path or default_file
            asked = _resolve_ready(root, rel)
            if asked is None:
                print(f"reject READY path={rel!r}", flush=True)
                nak = BlockMeta(
                    session_id,
                    0,
                    f"!not found: {rel}",
                    symbol_size,
                    block_k,
                    initial_repair_pct,
                    geometry.active_bytes,
                    "",
                ).pack()
                for _ in range(8):
                    sock.sendto(nak, client)
                continue
            kind, target = asked
            if kind == "mux":
                from .object_xfer import ObjectSession, queue_disk_files, run_object_session

                files = target
                print(
                    f"serve mux files={len(files)} from {client[0]}:{client[1]}",
                    flush=True,
                )
                meta = BlockMeta(
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
                obj_session = ObjectSession()
                queue_disk_files(obj_session, files)
                run_object_session(
                    sock,
                    client,
                    session_id,
                    obj_session,
                    geometry=geometry,
                    initial_repair_pct=initial_repair_pct,
                    min_bps=min_bps,
                    max_bps=max_bps,
                    start_bps=start_bps,
                    close_sock=False,
                )
                if once:
                    break
                print("idle — waiting for READY", flush=True)
                continue
            file_path = target
            file_size = file_path.stat().st_size
            total_blocks = geometry.total_blocks(file_size)
            digest = "" if skip_hash else _hash_file(file_path)
            file_path_str = str(file_path)
            prefetch_depth = min(64, max(geometry.active_blocks, 32))
            print(
                f"serve {rel} size={file_size} blocks={total_blocks} "
                f"from {client[0]}:{client[1]}",
                flush=True,
            )

            meta = BlockMeta(
                session_id,
                file_size,
                rel.replace("\\", "/"),
                symbol_size,
                block_k,
                initial_repair_pct,
                geometry.active_bytes,
                digest,
            ).pack()
            for _ in range(8):
                sock.sendto(meta, client)

            feedback = SenderFeedbackState(session_id)
            stop = threading.Event()
            client_fin = threading.Event()
            limiter = RateLimiter(
                max_bps, start_bps=start_bps if not cc_on else start_bps * _SEED_FRAC,
                min_frac=min_bps / max_bps,
            )
            limiter.set_burst_s(_env_float("TETRYS_BURST_S", 0.008))
            cc = (
                BlastCc(max_bps=max_bps, start_bps=start_bps, min_bps=min_bps)
                if cc_on
                else None
            )

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
                            packet = parse_packet(raw)
                        except ValueError:
                            continue
                        if isinstance(packet, BlockFeedback):
                            feedback.apply(packet)
                        elif (
                            isinstance(packet, BlockFin)
                            and packet.session_id == session_id
                            and packet.ok
                        ):
                            client_fin.set()

            fb_thread = threading.Thread(target=feedback_loop, daemon=True)
            fb_thread.start()

            active: dict[int, SenderBlockState] = {}
            enc_cache: dict[int, GenEncoder] = {}
            enc_order: list[int] = []
            next_block = 0
            timers = LoopTimers()
            fec_floor = float(initial_repair_pct)
            repair_ctl = RepairDebtController(
                fec_floor,
                min_pct=fec_floor,
                max_pct=max(28.0, fec_floor),
            )
            t0 = time.monotonic()
            last_log = t0
            aborted = False
            tail_idle_start: float | None = None
            first_close = 0
            first_close_seen = 0
            extra_blocks = 0
            extra_win = ExtraRepairWindow()
            loss_samples: list[float] = []
            extra_frac_samples: list[float] = []
            last_unique = 0
            source_wire_total = 0
            repair_wire_total = 0
            ready: dict[int, tuple[list[bytes], int]] = {}
            inflight: dict[int, Future] = {}
            last_repair_loop = 0.0
            tail_started: float | None = None
            pace_samples: list[float] = []

            def send_wires(wires: list[bytes], *, repair: bool) -> None:
                nonlocal source_wire_total, repair_wire_total
                if ramp_s > 0 and cc is None:
                    elapsed = time.monotonic() - t0
                    if elapsed < ramp_s:
                        limiter.set_rate(max(min_bps, start_bps * max(0.05, elapsed / ramp_s)))
                for pos in range(0, len(wires), _SEND_CHUNK):
                    if cc is not None:
                        limiter.set_rate(cc.on_timer(time.monotonic()))
                    batch = wires[pos : pos + _SEND_CHUNK]
                    t_pace = time.perf_counter()
                    limiter.consume(sum(map(len, batch)))
                    timers.pace_s += time.perf_counter() - t_pace
                    stamp_data_wires(batch, int(time.monotonic() * 1_000_000) & 0xFFFFFFFF)
                    t_send = time.perf_counter()
                    send_datagrams(sock, client, batch, chunk=_SEND_CHUNK)
                    timers.send_s += time.perf_counter() - t_send
                amount = sum(map(len, wires))
                if repair:
                    timers.repair_bytes += amount
                    timers.repair_pkts += len(wires)
                    repair_wire_total += amount
                else:
                    timers.source_bytes += amount
                    timers.source_pkts += len(wires)
                    source_wire_total += amount

            def encoder_for(block_id: int) -> GenEncoder:
                encoder = enc_cache.get(block_id)
                if encoder is not None:
                    if block_id in enc_order:
                        enc_order.remove(block_id)
                    enc_order.append(block_id)
                    return encoder
                state = active[block_id]
                encoder = rebuild_block_encoder(
                    mm, file_size, block_id, geometry, state.repair_emitted
                )
                enc_cache[block_id] = encoder
                enc_order.append(block_id)
                cache_limit = max(_ENCODER_CACHE, geometry.active_blocks)
                while len(enc_order) > cache_limit:
                    drop = enc_order.pop(0)
                    if drop != block_id:
                        enc_cache.pop(drop, None)
                return encoder

            def reap_completed(completed: set[int], *, tail: bool) -> None:
                nonlocal first_close, first_close_seen, extra_blocks
                for block_id in list(active):
                    if block_id not in completed:
                        continue
                    state = active.pop(block_id)
                    extra = max(0, state.repair_emitted - state.initial_repair)
                    # Tail repair storms must not train primary FEC for later blocks.
                    if not tail:
                        repair_ctl.observe(extra, block_k)
                        extra_win.observe(extra > 0)
                        extra_frac_samples.append(extra / max(1, block_k))
                        if extra == 0:
                            loss_samples.append(0.0)
                        else:
                            frac = block_loss_frac(state, block_k)
                            if frac is not None:
                                loss_samples.append(frac)
                    first_close_seen += 1
                    if extra == 0:
                        first_close += 1
                    else:
                        extra_blocks += 1
                    enc_cache.pop(block_id, None)
                    if block_id in enc_order:
                        enc_order.remove(block_id)

            def repair_tick(opened: dict[int, OpenBlock], now: float, tail: bool) -> int:
                t_r = time.perf_counter()
                cooldown_s = TAIL_REPAIR_COOLDOWN_S if tail else REPAIR_COOLDOWN_S
                candidates = select_repair_candidates(
                    active,
                    opened,
                    now,
                    block_k=block_k,
                    tail=tail,
                    age_s=REPAIR_AGE_S,
                    cooldown_s=cooldown_s,
                )
                total_need = sum(need for need, _age, _bid in candidates)
                budget, tick_s = repair_tick_limits(total_need, tail=tail)
                per_block = TAIL_REPAIR_TICK_PER_BLOCK if tail else budget
                sent = 0
                for need, _age, block_id in candidates:
                    if sent >= budget or (time.perf_counter() - t_r) >= tick_s:
                        break
                    encoder = encoder_for(block_id)
                    state = active[block_id]
                    n = min(need, budget - sent, per_block)
                    previous_count = encoder.packet_count
                    t_pack = time.perf_counter()
                    new_packets = encoder.ensure_repair(state.repair_emitted + n)
                    if not new_packets:
                        continue
                    stamp = int(now * 1_000_000) & 0xFFFFFFFF
                    wires = pack_data_packets(
                        session_id, block_id, new_packets, previous_count, stamp
                    )
                    timers.pack_s += time.perf_counter() - t_pack
                    send_wires(wires, repair=True)
                    state.repair_emitted += len(new_packets)
                    state.last_repair_ts = now
                    sent += len(new_packets)
                timers.repair_s += time.perf_counter() - t_r
                return sent

            def pump_ready() -> None:
                finished = [bid for bid, fut in inflight.items() if fut.done()]
                for bid in finished:
                    fut = inflight.pop(bid)
                    try:
                        block_id, wires, budget, encode_s = fut.result()
                    except Exception:
                        t_enc = time.perf_counter()
                        block_id, wires, budget, encode_s = encode_block_job(
                            file_path_str,
                            bid,
                            geometry.block_bytes,
                            file_size,
                            symbol_size,
                            repair_ctl.current,
                            session_id,
                        )
                        encode_s += time.perf_counter() - t_enc
                    ready[block_id] = (wires, budget)
                    timers.encode_s += encode_s

            def submit_ahead() -> None:
                pump_ready()
                queued = 0
                bid = next_block
                while bid < total_blocks and queued < prefetch_depth:
                    if bid in active:
                        bid += 1
                        continue
                    queued += 1
                    if bid not in ready and bid not in inflight:
                        inflight[bid] = encode_pool.submit(
                            encode_block_job,
                            file_path_str,
                            bid,
                            geometry.block_bytes,
                            file_size,
                            symbol_size,
                            repair_ctl.current,
                            session_id,
                        )
                    bid += 1

            fh = file_path.open("rb")
            mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                submit_ahead()
                while not client_fin.is_set():
                            completed, opened, unique_rx, decoded, echo_ts, fb_id = (
                                feedback.snapshot()
                            )
                            now = time.monotonic()
                            if feedback.client_lost(now, t0):
                                aborted = True
                                print(
                                    f"abort — client silent "
                                    f"sent={next_block}/{total_blocks} "
                                    f"done={len(completed)}",
                                    flush=True,
                                )
                                break
                            if cc is not None:
                                limiter.set_rate(
                                    cc.on_feedback(
                                        now,
                                        feedback_id=fb_id,
                                        unique_bytes=unique_rx,
                                        decoded_bytes=decoded,
                                        echo_ts_us=echo_ts,
                                        extra_frac=extra_win.frac,
                                        window_full=len(active) >= geometry.active_blocks,
                                    )
                                )
                            tail = next_block >= total_blocks
                            if tail and tail_started is None:
                                tail_started = now
                            reap_completed(completed, tail=tail)
                            submit_ahead()

                            admitted = False
                            while (
                                next_block < total_blocks
                                and len(active) < geometry.active_blocks
                            ):
                                pump_ready()
                                item = ready.pop(next_block, None)
                                if item is None:
                                    submit_ahead()
                                    break
                                wires, budget = item
                                state = SenderBlockState(
                                    next_block,
                                    initial_repair=budget,
                                    repair_emitted=budget,
                                    sent_at=time.monotonic(),
                                )
                                active[next_block] = state
                                send_wires(wires, repair=False)
                                next_block += 1
                                admitted = True
                                submit_ahead()

                            # Repair is a side budget: never spin it while waiting on encode,
                            # and at most one tick per REPAIR_INTERVAL_S once the window is full.
                            window_full = len(active) >= geometry.active_blocks
                            if tail or (
                                window_full
                                and now - last_repair_loop >= REPAIR_INTERVAL_S
                            ):
                                repair_tick(opened, now, tail)
                                last_repair_loop = now

                            if now - last_log >= 1.0:
                                elapsed = max(now - t0, 1e-6)
                                snap = timers.take()
                                sys_s, blk_s, calls, blocks = take_send_stats()
                                inst_unique = unique_rx - last_unique
                                last_unique = unique_rx
                                close_pct = (
                                    100.0 * first_close / first_close_seen
                                    if first_close_seen
                                    else 0.0
                                )
                                pace_samples.append(limiter.rate * 8 / 1e6)
                                print(
                                    f"progress sent={next_block}/{total_blocks} "
                                    f"done={len(completed)} active={len(active)} "
                                    f"ready={len(ready)} inflight={len(inflight)} "
                                    f"open={len(opened)} fec={repair_ctl.current}% "
                                    f"close={close_pct:.0f}% "
                                    f"pace={limiter.rate * 8 / 1e6:.0f}Mbit "
                                    f"cc={cc.phase if cc is not None else 'off'} "
                                    f"xfrac={extra_win.frac * 100:.0f}% "
                                    f"loss_p50={((percentile(loss_samples, 50) or 0.0) * 100):.1f}% "
                                    f"ack={unique_rx / elapsed / 1048576:.1f} "
                                    f"inst={inst_unique / 1048576:.1f} "
                                    f"app={decoded / elapsed / 1048576:.1f}MiB/s "
                                    f"enc={snap.encode_s * 1e3:.0f}ms "
                                    f"pack={snap.pack_s * 1e3:.0f}ms "
                                    f"pace_wait={snap.pace_s * 1e3:.0f}ms "
                                    f"send={snap.send_s * 1e3:.0f}ms "
                                    f"repair={snap.repair_s * 1e3:.0f}ms "
                                    f"sys={sys_s * 1e3:.0f}ms calls={calls} blk={blocks} "
                                    f"src={snap.source_pkts}pkt "
                                    f"rpr={snap.repair_pkts}pkt "
                                    f"wire={(snap.source_bytes + snap.repair_bytes) / 1048576:.1f}MiB",
                                    flush=True,
                                )
                                last_log = now

                            if next_block >= total_blocks:
                                sock.sendto(BlockFin(session_id, total_blocks).pack(), client)
                            if len(completed) >= total_blocks:
                                for _ in range(16):
                                    sock.sendto(
                                        BlockFin(session_id, total_blocks).pack(), client
                                    )
                                break
                            if next_block >= total_blocks and not active:
                                if tail_idle_start is None:
                                    tail_idle_start = now
                                elif now - tail_idle_start > _TAIL_IDLE_S:
                                    break
                            else:
                                tail_idle_start = None
                            if not admitted and not tail:
                                t_wait = time.perf_counter()
                                time.sleep(0.001)
                                timers.wait_s += time.perf_counter() - t_wait
            finally:
                stop.set()
                for fut in list(inflight.values()):
                    fut.cancel()
                fb_thread.join(timeout=1.0)
                mm.close()
                fh.close()

            elapsed = max(time.monotonic() - t0, 1e-6)
            if aborted:
                if once:
                    break
                print("idle — waiting for READY", flush=True)
                continue
            close_pct = 100.0 * first_close / first_close_seen if first_close_seen else 0.0
            tail_s = (time.monotonic() - tail_started) if tail_started else 0.0
            pace_sorted = sorted(pace_samples)
            if pace_sorted:
                pace_p10 = pace_sorted[max(0, int(0.1 * (len(pace_sorted) - 1)))]
                pace_med = pace_sorted[len(pace_sorted) // 2]
                pace_max = pace_sorted[-1]
                pace_txt = f"pace_p10={pace_p10:.0f} med={pace_med:.0f} max={pace_max:.0f}Mbit"
            else:
                pace_txt = "pace_p10=n/a"
            loss_p50 = percentile(loss_samples, 50)
            loss_p90 = percentile(loss_samples, 90)
            loss_p99 = percentile(loss_samples, 99)
            extra_p50 = percentile(extra_frac_samples, 50)
            extra_p90 = percentile(extra_frac_samples, 90)
            def _pct(val: float | None) -> str:
                return "n/a" if val is None else f"{val * 100:.1f}%"
            print(
                f"done in {elapsed:.2f}s — goodput "
                f"{file_size / elapsed / 1048576:.2f} MiB/s — "
                f"source_wire={source_wire_total / 1048576:.1f}MiB "
                f"repair_wire={repair_wire_total / 1048576:.1f}MiB "
                f"first_close={close_pct:.0f}% extra_blocks={extra_blocks} "
                f"xfrac={extra_win.frac * 100:.0f}% "
                f"loss_p50={_pct(loss_p50)} p90={_pct(loss_p90)} p99={_pct(loss_p99)} "
                f"extra_p50={_pct(extra_p50)} p90={_pct(extra_p90)} "
                f"fec={repair_ctl.current}% tail={tail_s:.2f}s {pace_txt}",
                flush=True,
            )
            if once:
                break
            print("idle — waiting for READY", flush=True)
    except KeyboardInterrupt:
        print("server stop", flush=True)
    finally:
        encode_pool.shutdown(wait=False, cancel_futures=True)
        sock.close()
    return 0


def run_block_client(
    host: str,
    port: int,
    output: Path,
    *,
    remote: str = "",
    wan: bool = False,
    active_bytes: int = WAN_ACTIVE_BYTES,
    file_progress: bool = False,
) -> int:
    del wan
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_RCVBUF, 128 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_SNDBUF, 8 * 1024 * 1024)
    sock.setblocking(False)
    server = (host, port)
    session_id = random.SystemRandom().randrange(1, 0xFFFFFFFF)
    ready = BlockReady(session_id, active_bytes, remote).pack()
    for _ in range(8):
        sock.sendto(ready, server)

    meta: BlockMeta | None = None
    deadline = time.monotonic() + 30.0
    while meta is None and time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], 0.5)
        if not readable:
            sock.sendto(ready, server)
            continue
        try:
            raw, _ = sock.recvfrom(4096)
            packet = parse_packet(raw)
        except (BlockingIOError, ValueError):
            continue
        if isinstance(packet, BlockMeta) and packet.session_id == session_id:
            meta = packet
    if meta is None:
        sock.close()
        raise TimeoutError("client timed out waiting for META")
    if meta.file_name.startswith("!"):
        sock.close()
        raise FileNotFoundError(meta.file_name[1:].lstrip())
    if meta.file_name == MUX_META_NAME:
        from .object_xfer import consume_object_stream

        print(
            f"META mux K={meta.block_k} T={meta.symbol_size} fec={meta.initial_repair_pct}%",
            flush=True,
        )
        return consume_object_stream(
            sock, server, session_id, meta, output, timeout_s=180.0,
            close_sock=True, file_progress=file_progress,
        )

    geometry = BlockGeometry(meta.symbol_size, meta.block_k, meta.active_bytes)
    total_blocks = geometry.total_blocks(meta.file_size)
    print(
        f"META name={meta.file_name} size={meta.file_size} "
        f"blocks={total_blocks} K={meta.block_k} T={meta.symbol_size} "
        f"fec={meta.initial_repair_pct}%",
        flush=True,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(output, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)
    os.ftruncate(fd, meta.file_size)
    slots: dict[int, GenReceiveSlot] = {}
    done: set[int] = set()
    unique_payload_bytes = 0
    decoded_bytes = 0
    feedback_id = 0
    last_feedback = 0.0
    last_echo = 0
    t0 = time.monotonic()
    last_log = 0.0
    bar_shown = False
    rate_t = t0
    rate_b = 0
    inst_bps = 0.0
    fin_seen = False
    dup_esi = 0
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
            if block_id not in done
        ][:64]
        packet = BlockFeedback(
            session_id,
            feedback_id,
            unique_payload_bytes,
            decoded_bytes,
            last_echo,
            sorted(done),
            opened,
        )
        sock.sendto(packet.pack(), server)
        last_feedback = now

    try:
        while len(done) < total_blocks:
            readable, _, _ = select.select([sock], [], [], 0.01)
            if readable:
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
                    if isinstance(packet, BlockData):
                        block_id = packet.block_id
                        if block_id >= total_blocks or block_id in done:
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
                            dup_esi += 1
                            continue
                        unique_payload_bytes += len(raw)
                        last_echo = packet.send_ts_us
                        if decoded is not None:
                            os.pwrite(fd, decoded[:tlen], off)
                            decoded_bytes += tlen
                            done.add(block_id)
                            slot.close()
                            slots.pop(block_id, None)
                            slot_seen.pop(block_id, None)
                    elif isinstance(packet, BlockFin):
                        fin_seen = True
            send_feedback()
            now = time.monotonic()
            dt = now - rate_t
            if dt >= 0.2:
                inst_bps = (decoded_bytes - rate_b) / dt
                rate_t, rate_b = now, decoded_bytes
            elif now > t0:
                inst_bps = decoded_bytes / (now - t0)
            if file_progress:
                name = Path(meta.file_name).name or output.name
                line = (
                    f"{name[:36]:<36} [{_client_bar(decoded_bytes / max(1, meta.file_size))}] "
                    f"{100.0 * decoded_bytes / max(1, meta.file_size):5.1f}% "
                    f"{_client_size(decoded_bytes)}/{_client_size(meta.file_size)}  "
                    f"{len(done)}/{total_blocks}  {_client_rate(inst_bps)}"
                )
                if sys.stdout.isatty() and now - last_log >= 0.05:
                    if bar_shown:
                        sys.stdout.write("\x1b[1A\x1b[2K")
                    print(line, flush=True)
                    bar_shown = True
                    last_log = now
                elif not sys.stdout.isatty() and now - last_log >= 1.0:
                    print(line, flush=True)
                    last_log = now
            elif now - last_log >= 1.0:
                elapsed = max(now - t0, 1e-6)
                print(
                    f"progress {len(done)}/{total_blocks} "
                    f"({100.0 * len(done) / total_blocks:.1f}%) "
                    f"open={len(slots)} unique={unique_payload_bytes / elapsed / 1048576:.1f} "
                    f"app={decoded_bytes / elapsed / 1048576:.1f}MiB/s "
                    f"inst={_client_rate(inst_bps)} "
                    f"dup_esi={dup_esi}",
                    flush=True,
                )
                last_log = now
        send_feedback(force=True)
        if file_progress:
            name = Path(meta.file_name).name or output.name
            line = (
                f"{name[:36]:<36} [{_client_bar(1.0)}] 100.0% "
                f"{_client_size(meta.file_size)}/{_client_size(meta.file_size)}  "
                f"{total_blocks}/{total_blocks}  {_client_rate(inst_bps)}"
            )
            if sys.stdout.isatty() and bar_shown:
                sys.stdout.write("\x1b[1A\x1b[2K")
            print(line, flush=True)
        for _ in range(16):
            sock.sendto(BlockFin(session_id, total_blocks).pack(), server)
            time.sleep(0.005)
    finally:
        for slot in slots.values():
            slot.close()
        os.close(fd)
        sock.close()

    elapsed = max(time.monotonic() - t0, 1e-6)
    if meta.sha256_hex:
        got = _hash_file(output)
        if got != meta.sha256_hex:
            raise ValueError("output hash mismatch")
    print(
        f"OK: wrote {output} ({meta.file_size} bytes) in {elapsed:.2f}s "
        f"({meta.file_size / elapsed / 1048576:.2f} MiB/s) fin={fin_seen}",
        flush=True,
    )
    return 0
