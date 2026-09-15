"""RaptorQ block transfer over UDP."""

from __future__ import annotations

import hashlib
import mmap
import multiprocessing
import os
import queue
import random
import select
import socket
import sys
import threading
import time
from concurrent.futures import (
    CancelledError,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
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
    merge_open_feedback,
    pack_data_packets,
    parse_packet,
    stamp_data_wires,
)
from .block_state import (
    BlockGeometry,
    FEC_RAPTORQ_MARGIN,
    resolve_fec_cli,
    REPAIR_AGE_S,
    REPAIR_COOLDOWN_S,
    REPAIR_INTERVAL_S,
    TAIL_REPAIR_COOLDOWN_S,
    TAIL_REPAIR_TICK_PER_BLOCK,
    SenderBlockState,
    SenderFeedbackState,
    WAN_ACTIVE_BYTES,
    WAN_BLOCK_K,
    WAN_CC_CAP_MBIT,
    WAN_SYMBOL_SIZE,
    ExtraRepairWindow,
    apply_open_block,
    block_quartile,
    fmt_quartile_counts,
    fmt_quartile_pcts,
    ghost_flight_ready,
    make_block_sample,
    make_fec_controller,
    percentile,
    repair_tick_limits,
    select_repair_candidates,
)
from .blastcc import BlastCc, _SEED_FRAC
from .gen_raptor import GenEncoder, GenReceiveSlot, blast_repair_budget
from .netutil import recv_datagrams, send_datagrams, take_send_stats, try_set_buffer
from .ratectl import RateLimiter

_FEEDBACK_S = 0.020
_TAIL_IDLE_S = 5.0
_FIN_INTERVAL_S = 0.05
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


def _pace_limits(rate_mbit: float, *, cc: bool = False) -> tuple[float, float, float]:
    if cc:
        # Search cap is not a path guess. Always above 1 Gbit unless env is
        # even higher (default 10 Gbit). Start at the stall floor; BlastCc
        # climbs after the first RTT, then sits on measured delivery.
        cap_mbit = max(
            2000.0,
            _env_float("TETRYS_CC_CAP_MBIT", WAN_CC_CAP_MBIT),
        )
        max_bps = cap_mbit * 1_000_000 / 8
        min_bps = 8_000_000 / 8
        start_raw = os.environ.get("TETRYS_START_MBIT", "").strip()
        if start_raw:
            try:
                start_mbit = max(float(start_raw), 1.0)
            except ValueError:
                start_mbit = min_bps * 8 / 1_000_000
            start_bps = min(max_bps, start_mbit * 1_000_000 / 8)
        else:
            start_bps = min_bps
        return min_bps, max_bps, start_bps
    # `--rate` is a hard lock. Optional env may clip; 850 must not
    # silently override a higher requested lock.
    rate = max(float(rate_mbit), 1.0)
    raw_cap = os.environ.get("TETRYS_PACE_CAP_MBIT", "").strip()
    if raw_cap:
        rate = min(rate, max(float(raw_cap), 1.0))
    bps = rate * 1_000_000 / 8
    return bps, bps, bps


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


def _send_copies(sock: socket.socket, addr, payload: bytes, n: int = 8) -> None:
    for _ in range(n):
        sock.sendto(payload, addr)


class BlockSender:
    """One file blast: encode window, repair, pace, abort on silent client."""

    def __init__(
        self,
        sock: socket.socket,
        client,
        session_id: int,
        file_path: Path,
        geometry: BlockGeometry,
        *,
        initial_repair_pct: int,
        min_bps: float,
        max_bps: float,
        start_bps: float,
        cc_on: bool,
        encode_pool,
        prefetch_depth: int,
    ) -> None:
        self.sock = sock
        self.client = client
        self.session_id = session_id
        self.file_path = file_path
        self.file_path_str = str(file_path)
        self.file_size = file_path.stat().st_size
        self.geometry = geometry
        self.block_k = geometry.block_k
        self.symbol_size = geometry.symbol_size
        self.total_blocks = geometry.total_blocks(self.file_size)
        self.min_bps = min_bps
        self.encode_pool = encode_pool
        self.prefetch_depth = prefetch_depth
        self.feedback = SenderFeedbackState(session_id)
        self.stop = threading.Event()
        self.client_fin = threading.Event()
        pace = start_bps if not cc_on else start_bps * _SEED_FRAC
        self.limiter = RateLimiter(
            pace, burst_s=_env_float("TETRYS_BURST_S", 0.008)
        )
        self.cc = (
            BlastCc(
                max_bps=max_bps,
                start_bps=start_bps,
                min_bps=min_bps,
                active_bytes=geometry.active_bytes,
            )
            if cc_on
            else None
        )
        self.repair_ctl = make_fec_controller(int(initial_repair_pct))
        self.active: dict[int, SenderBlockState] = {}
        self.enc_cache: dict[int, GenEncoder] = {}
        self.enc_order: list[int] = []
        self.ready: dict[int, tuple[list[bytes], int]] = {}
        self.pending: dict[int, Future] = {}
        self._encoded: queue.SimpleQueue[tuple[int, Future]] = queue.SimpleQueue()
        self.next_block = 0
        self.timers = LoopTimers()
        self.t0 = 0.0
        self.aborted = False
        self.tail_idle_start: float | None = None
        self.tail_started: float | None = None
        self.last_fin_ts: float = 0.0
        self.first_close = 0
        self.first_close_seen = 0
        self.extra_blocks = 0
        self.close_q_hits = [0, 0, 0, 0]
        self.close_q_seen = [0, 0, 0, 0]
        self.extra_q = [0, 0, 0, 0]
        self.extra_lo: int | None = None
        self.extra_hi: int | None = None
        self.last_encode_pct = int(self.repair_ctl.current)
        self.extra_win = ExtraRepairWindow()
        self.loss_samples: list[float] = []
        self.extra_frac_samples: list[float] = []
        self.pace_samples: list[float] = []
        self.flight_loss_samples: list[float] = []
        self.late_frac_samples: list[float] = []
        self._wait_flight: dict[int, SenderBlockState] = {}
        self.last_unique = 0
        self.source_wire_total = 0
        self.repair_wire_total = 0
        self.last_repair_loop = 0.0
        self.last_log = 0.0
        self.mm: mmap.mmap | None = None

    def _feedback_loop(self) -> None:
        sock = self.sock
        while not self.stop.is_set():
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
                    self.feedback.apply(packet)
                elif (
                    isinstance(packet, BlockFin)
                    and packet.session_id == self.session_id
                    and packet.ok
                ):
                    self.client_fin.set()

    def _apply_cc(self) -> None:
        """Push the latest ACK into BlastCc. Admit can block the main loop."""
        cc = self.cc
        if cc is None:
            return
        _completed, _opened, unique_rx, decoded, echo_ts, fb_id = (
            self.feedback.snapshot()
        )
        self.limiter.set_rate(
            cc.on_feedback(
                time.monotonic(),
                feedback_id=fb_id,
                unique_bytes=unique_rx,
                decoded_bytes=decoded,
                echo_ts_us=echo_ts,
                extra_frac=self.extra_win.frac,
                window_full=len(self.active) >= self.geometry.active_blocks,
                sent_bytes=self.source_wire_total + self.repair_wire_total,
                source_bytes=self.source_wire_total,
                path_loss=self.repair_ctl.path_loss_q(50),
            )
        )
        for line in cc.pull_events():
            print(line, flush=True)

    def _send_wires(self, wires: list[bytes], *, repair: bool) -> None:
        limiter = self.limiter
        cc = self.cc
        timers = self.timers
        for pos in range(0, len(wires), _SEND_CHUNK):
            if cc is not None:
                self._apply_cc()
                limiter.set_rate(cc.on_timer(time.monotonic()))
            batch = wires[pos : pos + _SEND_CHUNK]
            t_pace = time.perf_counter()
            limiter.consume(sum(map(len, batch)))
            timers.pace_s += time.perf_counter() - t_pace
            stamp_data_wires(
                batch,
                int(time.monotonic() * 1_000_000) & 0xFFFFFFFF,
                self.repair_ctl.current,
            )
            t_send = time.perf_counter()
            send_datagrams(self.sock, self.client, batch, chunk=_SEND_CHUNK)
            timers.send_s += time.perf_counter() - t_send
        amount = sum(map(len, wires))
        if repair:
            timers.repair_bytes += amount
            timers.repair_pkts += len(wires)
            self.repair_wire_total += amount
        else:
            timers.source_bytes += amount
            timers.source_pkts += len(wires)
            self.source_wire_total += amount

    def _encoder_for(self, block_id: int) -> GenEncoder:
        encoder = self.enc_cache.get(block_id)
        if encoder is not None:
            if block_id in self.enc_order:
                self.enc_order.remove(block_id)
            self.enc_order.append(block_id)
            return encoder
        state = self.active[block_id]
        encoder = rebuild_block_encoder(
            self.mm, self.file_size, block_id, self.geometry, state.repair_emitted
        )
        self.enc_cache[block_id] = encoder
        self.enc_order.append(block_id)
        cache_limit = max(_ENCODER_CACHE, self.geometry.active_blocks)
        while len(self.enc_order) > cache_limit:
            drop = self.enc_order.pop(0)
            if drop != block_id:
                self.enc_cache.pop(drop, None)
        return encoder

    def _observe_fec(self, sample) -> None:
        self.repair_ctl.observe_block(sample)

    def _reap_completed(
        self,
        completed: set[int],
        opened: dict[int, OpenBlock],
        *,
        tail: bool,
    ) -> None:
        for block_id in list(self.active):
            if block_id not in completed:
                continue
            state = self.active.pop(block_id)
            extra = max(0, state.repair_emitted - state.initial_repair)
            item = opened.get(block_id)
            if item is not None:
                apply_open_block(state, item)
            self._wait_flight[block_id] = state
            self.first_close_seen += 1
            q = block_quartile(block_id, self.total_blocks)
            self.close_q_seen[q] += 1
            if extra == 0:
                self.first_close += 1
                self.close_q_hits[q] += 1
            else:
                self.extra_blocks += 1
                self.extra_q[q] += 1
                self.extra_lo = (
                    block_id if self.extra_lo is None else min(self.extra_lo, block_id)
                )
                self.extra_hi = (
                    block_id if self.extra_hi is None else max(self.extra_hi, block_id)
                )
            self.enc_cache.pop(block_id, None)
            if block_id in self.enc_order:
                self.enc_order.remove(block_id)
        self._flush_flight_samples(opened, tail=tail)

    def _flush_flight_samples(
        self, opened: dict[int, OpenBlock], *, tail: bool
    ) -> None:
        qdelay_high = self.cc is not None and self.cc.high_delay_n >= 1
        now = time.monotonic()
        for block_id, state in list(self._wait_flight.items()):
            item = opened.get(block_id)
            if item is not None:
                apply_open_block(state, item)
            ready = (
                state.rx_at_flight >= 0
                or ghost_flight_ready(item)
                or (tail and now - state.sent_at >= REPAIR_AGE_S)
            )
            if not ready and now - state.sent_at < REPAIR_AGE_S + 0.20:
                continue
            # Receiver freeze is first-flight rank. Do not replace it with
            # unique-at-decode after DIR filled the hole.
            if state.unique_at_age < 0:
                if state.rx_at_flight >= 0:
                    state.unique_at_age = state.rx_at_flight
                elif state.unique_rx > 0:
                    state.unique_at_age = state.unique_rx
                    if state.first_deficit < 0:
                        state.first_deficit = max(
                            0, self.block_k + FEC_RAPTORQ_MARGIN - state.unique_rx
                        )
                elif state.unique_rx < self.block_k:
                    state.unique_rx = self.block_k
            extra = max(0, state.repair_emitted - state.initial_repair)
            fec_tail = tail and block_id >= max(0, self.total_blocks - 2)
            sample = make_block_sample(
                state, self.block_k, tail=fec_tail, qdelay_high=qdelay_high
            )
            if sample.train:
                if not state.fec_sampled:
                    self._observe_fec(sample)
                    state.fec_sampled = True
                self.extra_win.observe(sample.dir_pressure())
                self.extra_frac_samples.append(extra / max(1, self.block_k))
                flight = max(1, self.block_k + max(0, state.initial_repair))
                if sample.first_flight_loss is not None:
                    self.flight_loss_samples.append(sample.first_flight_loss)
                    self.loss_samples.append(sample.first_flight_loss)
                self.late_frac_samples.append(sample.late_unique / flight)
            self._wait_flight.pop(block_id, None)

    def _repair_tick(
        self, opened: dict[int, OpenBlock], now: float, tail: bool, *, window_full: bool = False
    ) -> int:
        t_r = time.perf_counter()
        cooldown_s = TAIL_REPAIR_COOLDOWN_S if tail else REPAIR_COOLDOWN_S
        candidates = select_repair_candidates(
            self.active,
            opened,
            now,
            block_k=self.block_k,
            tail=tail,
            age_s=REPAIR_AGE_S,
            cooldown_s=cooldown_s,
            dir_pad_fn=lambda deficit: self.repair_ctl.dir_margin(
                deficit, self.block_k, tail=tail
            ),
            prefer_oldest=window_full and not tail,
        )
        total_need = sum(need for need, _age, _bid in candidates)
        budget, tick_s = repair_tick_limits(
            total_need, tail=tail, window_full=window_full and not tail
        )
        per_block = TAIL_REPAIR_TICK_PER_BLOCK if tail else budget
        sent = 0
        for need, _age, block_id in candidates:
            if sent >= budget or (time.perf_counter() - t_r) >= tick_s:
                break
            encoder = self._encoder_for(block_id)
            state = self.active[block_id]
            n = min(need, budget - sent, per_block)
            previous_count = encoder.packet_count
            t_pack = time.perf_counter()
            new_packets = encoder.ensure_repair(state.repair_emitted + n)
            if not new_packets:
                continue
            stamp = int(now * 1_000_000) & 0xFFFFFFFF
            wires = pack_data_packets(
                self.session_id, block_id, new_packets, previous_count, stamp
            )
            self.timers.pack_s += time.perf_counter() - t_pack
            self._send_wires(wires, repair=True)
            state.repair_emitted += len(new_packets)
            state.last_repair_ts = now
            state.repair_rounds += 1
            sent += len(new_packets)
            item = opened.get(block_id)
            if item is not None:
                apply_open_block(state, item)
            if not tail and not state.fec_sampled and state.rx_at_flight >= 0:
                qdelay_high = self.cc is not None and self.cc.high_delay_n >= 1
                sample = make_block_sample(
                    state, self.block_k, tail=False, qdelay_high=qdelay_high
                )
                if sample.train:
                    self._observe_fec(sample)
                    state.fec_sampled = True
        self.timers.repair_s += time.perf_counter() - t_r
        return sent

    def _has_decode_debt(self, opened: dict, now: float) -> bool:
        """Drip repair as soon as a block is old and not yet first-closed."""
        need = self.block_k + FEC_RAPTORQ_MARGIN
        for block_id, state in self.active.items():
            if now - state.sent_at < REPAIR_AGE_S:
                continue
            item = opened.get(block_id)
            rx = item.unique_esi if item is not None else state.unique_rx
            if rx < need:
                return True
        return False

    def _still_needed(self, bid: int) -> bool:
        return (
            self.next_block <= bid < self.total_blocks
            and bid not in self.active
            and bid not in self.ready
            and bid not in self.pending
        )

    def _take_encoded(self) -> None:
        while True:
            try:
                bid, done_fut = self._encoded.get_nowait()
            except queue.Empty:
                return
            if self.pending.get(bid) is not done_fut:
                continue
            self.pending.pop(bid, None)
            if done_fut.cancelled():
                if self._still_needed(bid):
                    self._submit_encode(bid)
                continue
            try:
                block_id, wires, budget, encode_s = done_fut.result()
            except CancelledError:
                if self._still_needed(bid):
                    self._submit_encode(bid)
                continue
            except Exception:
                t_enc = time.perf_counter()
                block_id, wires, budget, encode_s = encode_block_job(
                    self.file_path_str,
                    bid,
                    self.geometry.block_bytes,
                    self.file_size,
                    self.symbol_size,
                    self.repair_ctl.encode_pct(bid),
                    self.session_id,
                )
                encode_s += time.perf_counter() - t_enc
            # Keep whatever FEC the worker encoded. Re-encoding on a level
            # change empties ready and stalls admit (ready=0 inflight=64).
            self.ready[block_id] = (wires, budget)
            self.timers.encode_s += encode_s

    def _submit_encode(self, bid: int) -> None:
        pct = self.repair_ctl.encode_pct(bid)
        fut = self.encode_pool.submit(
            encode_block_job,
            self.file_path_str,
            bid,
            self.geometry.block_bytes,
            self.file_size,
            self.symbol_size,
            pct,
            self.session_id,
        )
        self.pending[bid] = fut
        fut.add_done_callback(
            lambda done, block_id=bid: self._encoded.put((block_id, done))
        )

    def _submit_ahead(self) -> None:
        self._take_encoded()
        queued = 0
        bid = self.next_block
        while bid < self.total_blocks and queued < self.prefetch_depth:
            if bid in self.active:
                bid += 1
                continue
            queued += 1
            if bid not in self.ready and bid not in self.pending:
                self._submit_encode(bid)
            bid += 1

    def _admit_source(self) -> bool:
        admitted = False
        while (
            self.next_block < self.total_blocks
            and len(self.active) < self.geometry.active_blocks
        ):
            self._take_encoded()
            item = self.ready.pop(self.next_block, None)
            if item is None:
                self._submit_ahead()
                return admitted
            wires, budget = item
            self.active[self.next_block] = SenderBlockState(
                self.next_block,
                initial_repair=budget,
                repair_emitted=budget,
                sent_at=time.monotonic(),
            )
            self.last_encode_pct = int(
                round(100.0 * budget / max(1, self.geometry.block_k))
            )
            self._send_wires(wires, repair=False)
            self.next_block += 1
            admitted = True
            self._submit_ahead()
        return admitted

    def _log_progress(
        self, now: float, completed: set[int], opened: dict, unique_rx: int, decoded: int
    ) -> None:
        elapsed = max(now - self.t0, 1e-6)
        snap = self.timers.take()
        sys_s, blk_s, calls, blocks = take_send_stats()
        inst_unique = unique_rx - self.last_unique
        self.last_unique = unique_rx
        close_pct = (
            100.0 * self.first_close / self.first_close_seen
            if self.first_close_seen
            else 0.0
        )
        self.pace_samples.append(self.limiter.rate * 8 / 1e6)
        cc = self.cc
        cc_extra = ""
        if cc is not None:
            cc_extra = (
                f"snd={cc.last_send_rate * 8 / 1e6:.0f} "
                f"unq={cc.last_delivery * 8 / 1e6:.0f} "
                f"good={cc.last_good * 8 / 1e6:.0f} "
                f"ov={int(cc._pipe_oversend())} "
            )
        print(
            f"progress sent={self.next_block}/{self.total_blocks} "
            f"done={len(completed)} active={len(self.active)} "
            f"ready={len(self.ready)} inflight={len(self.pending)} "
            f"open={len(opened)} fec={self.repair_ctl.current}% "
            f"enc_fec={self.last_encode_pct}% "
            f"why={self.repair_ctl.reason.replace(' ', '_')} "
            f"close={close_pct:.0f}% "
            f"close_q={fmt_quartile_pcts(self.close_q_hits, self.close_q_seen)} "
            f"extra_q={fmt_quartile_counts(self.extra_q)} "
            f"pace={self.limiter.rate * 8 / 1e6:.0f}Mbit "
            f"cc={cc.phase if cc is not None else 'off'} "
            f"{cc_extra}"
            f"xfrac={self.extra_win.frac * 100:.0f}% "
            f"loss_p50={((percentile(self.loss_samples, 50) or 0.0) * 100):.1f}% "
            f"path_p50={((self.repair_ctl.path_loss_q(50) or 0.0) * 100):.1f}% "
            f"path_p95={((self.repair_ctl.path_loss_q(95) or 0.0) * 100):.1f}% "
            f"cover_p50={((self.repair_ctl.path_loss_q(50, coverable=True) or 0.0) * 100):.1f}% "
            f"cover_p95={((self.repair_ctl.path_loss_q(95, coverable=True) or 0.0) * 100):.1f}% "
            f"hol={((self.repair_ctl.hol_frac() or 0.0) * 100):.0f}% "
            f"late_p50={((percentile(self.late_frac_samples, 50) or 0.0) * 100):.1f}% "
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

    def run(self) -> bool:
        self.t0 = time.monotonic()
        self.last_log = self.t0
        fb_thread = threading.Thread(target=self._feedback_loop, daemon=True)
        fb_thread.start()
        fh = self.file_path.open("rb")
        self.mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            self._submit_ahead()
            while not self.client_fin.is_set():
                completed, opened, unique_rx, decoded, echo_ts, fb_id = (
                    self.feedback.snapshot()
                )
                now = time.monotonic()
                if self.feedback.client_lost(now, self.t0):
                    self.aborted = True
                    print(
                        f"abort — client silent "
                        f"sent={self.next_block}/{self.total_blocks} "
                        f"done={len(completed)}",
                        flush=True,
                    )
                    break
                if self.cc is not None:
                    self._apply_cc()
                tail = self.next_block >= self.total_blocks
                if tail and self.tail_started is None:
                    self.tail_started = now
                self._reap_completed(completed, opened, tail=tail)
                self._submit_ahead()
                admitted = self._admit_source()

                window_full = len(self.active) >= self.geometry.active_blocks
                drip = tail or window_full or self._has_decode_debt(opened, now)
                if tail:
                    repair_gap = TAIL_REPAIR_COOLDOWN_S
                elif window_full:
                    repair_gap = 0.0
                else:
                    repair_gap = REPAIR_INTERVAL_S
                if drip and now - self.last_repair_loop >= repair_gap:
                    self._repair_tick(
                        opened, now, tail, window_full=window_full
                    )
                    self.last_repair_loop = now

                if now - self.last_log >= 1.0:
                    self._log_progress(now, completed, opened, unique_rx, decoded)
                    self.last_log = now

                if self.next_block >= self.total_blocks:
                    if now - self.last_fin_ts >= _FIN_INTERVAL_S:
                        self.sock.sendto(
                            BlockFin(self.session_id, self.total_blocks).pack(),
                            self.client,
                        )
                        self.last_fin_ts = now
                if len(completed) >= self.total_blocks:
                    self._flush_flight_samples(opened, tail=True)
                    fin = BlockFin(self.session_id, self.total_blocks).pack()
                    for _ in range(16):
                        self.sock.sendto(fin, self.client)
                    break
                if self.next_block >= self.total_blocks and not self.active:
                    if self.tail_idle_start is None:
                        self.tail_idle_start = now
                    elif now - self.tail_idle_start > _TAIL_IDLE_S:
                        break
                else:
                    self.tail_idle_start = None
                if not admitted:
                    t_wait = time.perf_counter()
                    time.sleep(0.001)
                    self.timers.wait_s += time.perf_counter() - t_wait
        finally:
            self.stop.set()
            for fut in list(self.pending.values()):
                fut.cancel()
            fb_thread.join(timeout=1.0)
            self.mm.close()
            fh.close()
            self.mm = None
        return self.aborted

    def print_done(self) -> None:
        elapsed = max(time.monotonic() - self.t0, 1e-6)
        close_pct = (
            100.0 * self.first_close / self.first_close_seen
            if self.first_close_seen
            else 0.0
        )
        tail_s = (time.monotonic() - self.tail_started) if self.tail_started else 0.0
        pace_sorted = sorted(self.pace_samples)
        if pace_sorted:
            pace_p10 = pace_sorted[max(0, int(0.1 * (len(pace_sorted) - 1)))]
            pace_med = pace_sorted[len(pace_sorted) // 2]
            pace_max = pace_sorted[-1]
            pace_txt = f"pace_p10={pace_p10:.0f} med={pace_med:.0f} max={pace_max:.0f}Mbit"
        else:
            pace_txt = "pace_p10=n/a"

        def _pct(val: float | None) -> str:
            return "n/a" if val is None else f"{val * 100:.1f}%"

        extra_span = ""
        if self.extra_lo is not None and self.extra_hi is not None:
            extra_span = f" extra_span={self.extra_lo}-{self.extra_hi}"
        print(
            f"done in {elapsed:.2f}s — goodput "
            f"{self.file_size / elapsed / 1048576:.2f} MiB/s — "
            f"source_wire={self.source_wire_total / 1048576:.1f}MiB "
            f"repair_wire={self.repair_wire_total / 1048576:.1f}MiB "
            f"first_close={close_pct:.0f}% extra_blocks={self.extra_blocks} "
            f"close_q={fmt_quartile_pcts(self.close_q_hits, self.close_q_seen)} "
            f"extra_q={fmt_quartile_counts(self.extra_q)}"
            f"{extra_span} "
            f"dir_rounds={self.repair_ctl.dir_rounds} "
            f"xfrac={self.extra_win.frac * 100:.0f}% "
            f"loss_p50={_pct(percentile(self.loss_samples, 50))} "
            f"p90={_pct(percentile(self.loss_samples, 90))} "
            f"p99={_pct(percentile(self.loss_samples, 99))} "
            f"flight_p95={_pct(percentile(self.flight_loss_samples, 95))} "
            f"path_p50={_pct(self.repair_ctl.path_loss_q(50))} "
            f"path_p95={_pct(self.repair_ctl.path_loss_q(95))} "
            f"cover_p50={_pct(self.repair_ctl.path_loss_q(50, coverable=True))} "
            f"cover_p95={_pct(self.repair_ctl.path_loss_q(95, coverable=True))} "
            f"hol={_pct(self.repair_ctl.hol_frac())} "
            f"late_p50={_pct(percentile(self.late_frac_samples, 50))} "
            f"extra_p50={_pct(percentile(self.extra_frac_samples, 50))} "
            f"p90={_pct(percentile(self.extra_frac_samples, 90))} "
            f"fec={self.repair_ctl.current}% enc_fec={self.last_encode_pct}% "
            f"why={self.repair_ctl.reason.replace(' ', '_')} "
            f"tail={tail_s:.2f}s {pace_txt}",
            flush=True,
        )


class BlockReceiver:
    def __init__(
        self,
        sock: socket.socket,
        server: tuple[str, int],
        session_id: int,
        meta: BlockMeta,
        output: Path,
        *,
        file_progress: bool,
    ) -> None:
        self.sock = sock
        self.server = server
        self.session_id = session_id
        self.meta = meta
        self.output = output
        self.file_progress = file_progress
        self.geometry = BlockGeometry(meta.symbol_size, meta.block_k, meta.active_bytes)
        self.total_blocks = self.geometry.total_blocks(meta.file_size)
        self.slots: dict[int, GenReceiveSlot] = {}
        self.done: set[int] = set()
        self.unique_payload_bytes = 0
        self.decoded_bytes = 0
        self.feedback_id = 0
        self.last_feedback = 0.0
        self.last_echo = 0
        self.t0 = 0.0
        self.last_log = 0.0
        self.bar_shown = False
        self.rate_t = 0.0
        self.rate_b = 0
        self.inst_bps = 0.0
        self.fin_seen = False
        self.dup_esi = 0
        self.slot_seen: dict[int, float] = {}
        self.fd = -1
        self.fec_pct = int(meta.initial_repair_pct)

    def _send_feedback(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_feedback < _FEEDBACK_S:
            return
        self.feedback_id += 1

        def _open(block_id: int, slot: GenReceiveSlot) -> OpenBlock:
            age_s = now - self.slot_seen.get(block_id, now)
            slot.maybe_freeze_flight(age_s, REPAIR_AGE_S)
            return OpenBlock(
                block_id,
                slot.symbols_rx,
                slot.decode_failed,
                min(255, int(age_s / 0.020)),
                slot.unique_at_flight,
            )

        incomplete = [
            _open(block_id, slot)
            for block_id, slot in sorted(self.slots.items())
            if block_id not in self.done
        ]
        ghosts = [
            _open(block_id, slot)
            for block_id, slot in sorted(self.slots.items())
            if block_id in self.done
        ]
        opened = merge_open_feedback(incomplete, ghosts)
        packet = BlockFeedback(
            self.session_id,
            self.feedback_id,
            self.unique_payload_bytes,
            self.decoded_bytes,
            self.last_echo,
            sorted(self.done),
            opened,
        )
        self.sock.sendto(packet.pack(), self.server)
        self.last_feedback = now
        for block_id in list(self.slots):
            if block_id not in self.done:
                continue
            if now - self.slot_seen.get(block_id, now) < REPAIR_AGE_S:
                continue
            slot = self.slots.pop(block_id, None)
            if slot is not None:
                slot.close()
            self.slot_seen.pop(block_id, None)

    def _on_data(self, packet: BlockData, raw: bytes) -> None:
        if packet.fec_pct > 0:
            self.fec_pct = int(packet.fec_pct)
        block_id = packet.block_id
        if block_id >= self.total_blocks:
            return
        off = block_id * self.geometry.block_bytes
        tlen = min(self.geometry.block_bytes, self.meta.file_size - off)
        slot = self.slots.get(block_id)
        if slot is None:
            if block_id in self.done:
                return
            slot = GenReceiveSlot(
                block_id,
                gen_k=self.meta.block_k,
                symbol_size=self.meta.symbol_size,
                block_bytes=self.geometry.block_bytes,
                tlen=tlen,
            )
            self.slots[block_id] = slot
            self.slot_seen[block_id] = time.monotonic()
        fec = int(packet.fec_pct or self.fec_pct or self.meta.initial_repair_pct)
        slot.arm_flight_limit(slot.gen_k + blast_repair_budget(slot.gen_k, fec))
        before = slot.symbols_rx
        decoded = slot.add_packet(packet.payload, packet.esi)
        if block_id in self.done:
            if slot.symbols_rx > before:
                self.unique_payload_bytes += len(raw)
            return
        if slot.symbols_rx == before:
            self.dup_esi += 1
            return
        self.unique_payload_bytes += len(raw)
        self.last_echo = packet.send_ts_us
        if decoded is not None:
            os.pwrite(self.fd, decoded[:tlen], off)
            self.decoded_bytes += tlen
            self.done.add(block_id)

    def _log(self, now: float) -> None:
        meta = self.meta
        dt = now - self.rate_t
        if dt >= 0.2:
            self.inst_bps = (self.decoded_bytes - self.rate_b) / dt
            self.rate_t, self.rate_b = now, self.decoded_bytes
        elif now > self.t0:
            self.inst_bps = self.decoded_bytes / (now - self.t0)
        if self.file_progress:
            name = Path(meta.file_name).name or self.output.name
            line = (
                f"{name[:36]:<36} [{_client_bar(self.decoded_bytes / max(1, meta.file_size))}] "
                f"{100.0 * self.decoded_bytes / max(1, meta.file_size):5.1f}% "
                f"{_client_size(self.decoded_bytes)}/{_client_size(meta.file_size)}  "
                f"{len(self.done)}/{self.total_blocks}  {_client_rate(self.inst_bps)} "
                f"fec={self.fec_pct}%"
            )
            if sys.stdout.isatty() and now - self.last_log >= 0.05:
                if self.bar_shown:
                    sys.stdout.write("\x1b[1A\x1b[2K")
                print(line, flush=True)
                self.bar_shown = True
                self.last_log = now
            elif not sys.stdout.isatty() and now - self.last_log >= 1.0:
                print(line, flush=True)
                self.last_log = now
        elif now - self.last_log >= 1.0:
            elapsed = max(now - self.t0, 1e-6)
            print(
                f"progress {len(self.done)}/{self.total_blocks} "
                f"({100.0 * len(self.done) / self.total_blocks:.1f}%) "
                f"open={len(self.slots)} unique={self.unique_payload_bytes / elapsed / 1048576:.1f} "
                f"app={self.decoded_bytes / elapsed / 1048576:.1f}MiB/s "
                f"inst={_client_rate(self.inst_bps)} "
                f"fec={self.fec_pct}% "
                f"dup_esi={self.dup_esi}",
                flush=True,
            )
            self.last_log = now

    def _print_complete_bar(self) -> None:
        if not self.file_progress:
            return
        name = Path(self.meta.file_name).name or self.output.name
        line = (
            f"{name[:36]:<36} [{_client_bar(1.0)}] 100.0% "
            f"{_client_size(self.meta.file_size)}/{_client_size(self.meta.file_size)}  "
            f"{self.total_blocks}/{self.total_blocks}  {_client_rate(self.inst_bps)}"
        )
        if sys.stdout.isatty() and self.bar_shown:
            sys.stdout.write("\x1b[1A\x1b[2K")
        print(line, flush=True)

    def run(self) -> int:
        meta = self.meta
        output = self.output
        print(
            f"META name={meta.file_name} size={meta.file_size} "
            f"blocks={self.total_blocks} K={meta.block_k} T={meta.symbol_size} "
            f"fec={meta.initial_repair_pct}% (start)",
            flush=True,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(output, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)
        os.ftruncate(self.fd, meta.file_size)
        self.t0 = time.monotonic()
        self.rate_t = self.t0
        sock = self.sock
        try:
            while len(self.done) < self.total_blocks:
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
                        if getattr(packet, "session_id", None) != self.session_id:
                            continue
                        if isinstance(packet, BlockData):
                            self._on_data(packet, raw)
                        elif isinstance(packet, BlockFin):
                            self.fin_seen = True
                self._send_feedback()
                self._log(time.monotonic())
            self._send_feedback(force=True)
            self._print_complete_bar()
            for _ in range(16):
                sock.sendto(BlockFin(self.session_id, self.total_blocks).pack(), self.server)
                time.sleep(0.005)
        finally:
            for slot in self.slots.values():
                slot.close()
            os.close(self.fd)
            sock.close()

        elapsed = max(time.monotonic() - self.t0, 1e-6)
        if meta.sha256_hex:
            got = _hash_file(output)
            if got != meta.sha256_hex:
                raise ValueError("output hash mismatch")
        print(
            f"OK: wrote {output} ({meta.file_size} bytes) in {elapsed:.2f}s "
            f"({meta.file_size / elapsed / 1048576:.2f} MiB/s) "
            f"fec={self.fec_pct}% fin={self.fin_seen}",
            flush=True,
        )
        return 0


def run_block_server(
    host: str,
    port: int,
    root: Path,
    *,
    default_file: str = "",
    symbol_size: int = WAN_SYMBOL_SIZE,
    block_k: int = WAN_BLOCK_K,
    initial_repair_pct: int | None = None,
    active_bytes: int = WAN_ACTIVE_BYTES,
    rate_mbit: float | None = None,
    skip_hash: bool = False,
    once: bool = True,
) -> int:
    geometry = BlockGeometry(symbol_size, block_k, active_bytes)
    root = root.resolve()
    workers = _encode_workers()
    prefetch_depth = min(64, max(geometry.active_blocks, 32))
    cc_on = rate_mbit is None
    min_bps, max_bps, start_bps = _pace_limits(
        0.0 if cc_on else rate_mbit, cc=cc_on
    )
    fec_mode, fec_start = resolve_fec_cli(initial_repair_pct)

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
        f"fec={fec_start}% "
        f"mode={fec_mode} "
        f"enc_workers={workers} prefetch={prefetch_depth}",
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
                    fec_start,
                    geometry.active_bytes,
                    "",
                ).pack()
                _send_copies(sock, client, nak)
                continue
            kind, target = asked
            if kind == "mux":
                from .object_xfer import ObjectSession, queue_disk_files, run_object_session

                files = target
                print(
                    f"serve mux files={len(files)} from {client[0]}:{client[1]}",
                    flush=True,
                )
                obj_session = ObjectSession()
                queue_disk_files(obj_session, files)
                meta = BlockMeta(
                    session_id,
                    obj_session.nbytes,
                    MUX_META_NAME,
                    symbol_size,
                    block_k,
                    fec_start,
                    geometry.active_bytes,
                    f"n={obj_session.nobj}",
                ).pack()
                _send_copies(sock, client, meta)
                run_object_session(
                    sock,
                    client,
                    session_id,
                    obj_session,
                    geometry=geometry,
                    initial_repair_pct=fec_start,
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
                fec_start,
                geometry.active_bytes,
                digest,
            ).pack()
            _send_copies(sock, client, meta)

            sender = BlockSender(
                sock,
                client,
                session_id,
                file_path,
                geometry,
                initial_repair_pct=fec_start,
                min_bps=min_bps,
                max_bps=max_bps,
                start_bps=start_bps,
                cc_on=cc_on,
                encode_pool=encode_pool,
                prefetch_depth=prefetch_depth,
            )
            aborted = sender.run()
            if aborted:
                if once:
                    break
                print("idle — waiting for READY", flush=True)
                continue
            sender.print_done()
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
    active_bytes: int = WAN_ACTIVE_BYTES,
    file_progress: bool = False,
) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_RCVBUF, 128 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_SNDBUF, 8 * 1024 * 1024)
    sock.setblocking(False)
    server = (host, port)
    session_id = random.SystemRandom().randrange(1, 0xFFFFFFFF)
    ready = BlockReady(session_id, active_bytes, remote).pack()
    _send_copies(sock, server, ready)

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

    return BlockReceiver(
        sock, server, session_id, meta, output, file_progress=file_progress
    ).run()
