"""Generation + RaptorQ random-access file transfer."""

from __future__ import annotations

import hashlib
import math
import mmap
import multiprocessing
import os
import queue
import select
import socket
import sys
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path

from .gen_raptor import (
    GenDecoder,
    GenEncoder,
    blast_repair_budget,
    fountain_blast_budget,
    repair_count,
    require_raptorq,
)
from .ratectl import RateLimiter
from .netutil import (
    recv_datagrams,
    send_datagrams,
    take_send_stats,
    try_set_buffer,
)
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

# Raptor encoders are heavy (hundreds of KiB to MiB each). Keep a bounded
# working set; old generations can be reconstructed from mmap for repair.
_ENCODER_KEEP = 128
# Do not re-repair the same gen on every send-loop iteration.
_REPAIR_COOLDOWN_S = 0.10
# Start frontier repair when client lags this many gens behind send cursor.
_REPAIR_LAG = 48
# If next_needed is unchanged this long, send deficit-sized fountain repair.
_STUCK_S = 0.40
# Min gap between deficit repair rounds of the same generation.
_STUCK_REPAIR_COOLDOWN_S = 0.25
# RaptorQ normally decodes near K; add a small rank margin and enough surplus
# for loss of the repair packets themselves.
_DECODE_MARGIN = 2
_REPAIR_SURPLUS = 0.25
# Bound incomplete source data rather than generation count so larger K does
# not recreate an unbounded repair tail. This is well above the path BDP.
_MAX_INFLIGHT_BYTES = 64 * 1024 * 1024
_MIN_INFLIGHT_GENS = 64
# Blast encode runs in separate PROCESSES: the raptorq binding holds the GIL
# for the whole native encode (measured: a sendto in the send thread waits up
# to ~12ms behind 3 encoding threads). Worker processes have their own GIL,
# so the send loop keeps the socket busy while children encode.
_ENCODE_WORKERS = 4
_ENCODE_PREFETCH = 16
# Hybrid pipeline: fountain only when client lags or inflight is high.
_PIPELINE_LAG_GENS = 64
_PIPELINE_INFLIGHT_FRAC = 3 / 4
# Fountain mode (overhead=0): per-gen repair in encode worker; cap top-up at inflight limit.
_FOUNTAIN_TARGET_OVERHEAD_PCT = 8
# Fountain tick every N systematic sends when hybrid path is stressed (not at hard cap).
_FOUNTAIN_EVERY_N = 4
# How many incomplete gens to fountain-repair per send-loop turn.
_REPAIR_PER_TURN = 2
_REPAIR_AT_CAP = 4
# One feedback round must stay a thin fountain top-up, never a full reblast.
_REPAIR_ROUND_MAX = 32
# While blast is blocked on inflight cap, spray new ESI into the frontier
# (send loop only). Rate is limited by the token bucket, not a hard pkts/s cap.
# Pipelined blast: K (+ bootstrap) then advance; async fountain fills gaps.
_FOUNTAIN_CAP_GENS = 3
_FOUNTAIN_CAP_COOLDOWN_S = 0.05
_FOUNTAIN_CAP_PER_GEN = 8
# Track fountain state only near the client frontier (not the whole inflight tail).
_FOUNTAIN_TRACK_MAX = 64
_FOUNTAIN_WINDOW = 48
# Bound in-flight async repair encodes (each holds wire batches in RAM).
_REPAIR_FUTURES_MAX = 12
# Circuit breaker when repair wire rate explodes (pkts/s in the 1s progress window).
_REPAIR_STORM_PKTS_S = 25_000
_REPAIR_STORM_BACKOFF_S = 0.75
# Bound repair_extra / cooldown dicts (one int/float per gen touched by repair).
_REPAIR_META_KEEP = 256
# Per-worker repair encoder cache (incremental ensure_repair, not full rebuild).
_REPAIR_ENC_CACHE_MAX = 48
# Client NACK horizon slack beyond byte-bounded inflight window.
_FEEDBACK_HORIZON_SLACK = 32
# Delivery-rate pacing: only back off when client goodput lags wire pace.
_ADAPT_PACE_MIN_FRAC = 0.10
_ADAPT_PACE_BACKOFF_FRAC = 0.82  # delivery < pace×this → slow down
_ADAPT_PACE_RECOVER_FRAC = 0.92  # delivery ≥ pace×this → ease toward --rate
_ADAPT_PACE_RECOVER_STEP = 1.12
_ADAPT_PACE_GOOD_SNAP = 3  # consecutive good samples before full --rate
_ADAPT_PACE_WARMUP_S = 6.0  # ignore bursty early decode after ramp
_ADAPT_PACE_BAD_SNAP = 3  # consecutive lagging samples before backoff


def compute_inflight_gen_limit(gen_k: int, symbol_size: int) -> int:
    """Byte-bounded pipeline window in generations (size-independent in bytes)."""
    block_bytes = max(1, gen_k) * max(64, symbol_size)
    return max(_MIN_INFLIGHT_GENS, _MAX_INFLIGHT_BYTES // block_bytes)


def client_feedback_horizon(inflight_gen_limit: int) -> int:
    """NACK coverage must match server inflight, not a fixed small constant."""
    return inflight_gen_limit + _FEEDBACK_HORIZON_SLACK


def should_pause_blast(
    incomplete: int,
    frontier_lag: int,
    *,
    inflight_gen_limit: int,
) -> bool:
    """Pause new data when occupancy or frontier lag hits the window."""
    return incomplete >= inflight_gen_limit or frontier_lag >= inflight_gen_limit


def repair_thread_limit(
    *,
    at_cap: bool,
    storm_active: bool,
    nack_count: int,
    frontier_lag: int,
) -> int:
    """Scale repair parallelism with backlog; storm keeps urgent path only."""
    if storm_active:
        return 1
    base = _REPAIR_AT_CAP if at_cap else _REPAIR_PER_TURN
    backlog = max(nack_count, frontier_lag // 64, 1)
    return min(16, max(base, backlog // 4))


def fountain_redundancy(overhead_pct: int) -> bool:
    """True when CLI overhead is 0 (repair via fountain bootstrap + cap top-up)."""
    return overhead_pct <= 0


def repair_round_size(gen_k: int, overhead_pct: int) -> int:
    """Default repair symbols per generation or fountain round."""
    pct = overhead_pct if overhead_pct > 0 else _FOUNTAIN_TARGET_OVERHEAD_PCT
    return max(1, repair_count(gen_k, pct))


def cap_fountain_send(
    gen_k: int,
    prior_extra: int,
    send_n: int | None,
    symbols_rx: int | None,
    *,
    decode_margin: int = _DECODE_MARGIN,
    round_max: int = _REPAIR_ROUND_MAX,
) -> int:
    """Cap fountain repair to per-gen budget; allow deficit top-up beyond."""
    budget = repair_round_size(gen_k, 0)
    if prior_extra >= budget:
        if symbols_rx is None:
            if prior_extra >= gen_k:
                return 0
            want = send_n if send_n is not None else 2
            return max(1, min(int(want), 4, round_max))
        if symbols_rx < gen_k + decode_margin:
            deficit = max(1, gen_k + decode_margin - symbols_rx)
            want = send_n if send_n is not None else deficit
            return max(1, min(int(want), round_max))
        return 0
    room = budget - prior_extra
    want = send_n if send_n is not None else room
    return max(1, min(int(want), room, round_max))


def pipeline_stressed(
    incomplete: int,
    lag: int,
    *,
    inflight_gen_limit: int,
) -> bool:
    """Hybrid mode: fountain pipeline only when client/path is under pressure."""
    return incomplete >= int(inflight_gen_limit * _PIPELINE_INFLIGHT_FRAC) or (
        lag > _PIPELINE_LAG_GENS
    )


def update_adaptive_pace_bps(
    *,
    delivery_bps: float,
    cur_bps: float,
    max_bps: float,
    good_streak: int,
    bad_streak: int = 0,
    min_frac: float = _ADAPT_PACE_MIN_FRAC,
    backoff_frac: float = _ADAPT_PACE_BACKOFF_FRAC,
    recover_frac: float = _ADAPT_PACE_RECOVER_FRAC,
    recover_step: float = _ADAPT_PACE_RECOVER_STEP,
    good_snap: int = _ADAPT_PACE_GOOD_SNAP,
) -> tuple[float, int, int]:
    """Backoff-only pacing: hold --rate unless delivery clearly lags."""
    floor = max_bps * min_frac
    if delivery_bps < cur_bps * backoff_frac:
        bad_streak += 1
        if bad_streak >= _ADAPT_PACE_BAD_SNAP:
            return max(floor, delivery_bps * 1.08), 0, 0
        return cur_bps, 0, bad_streak
    bad_streak = 0
    if delivery_bps >= max_bps * recover_frac:
        streak = good_streak + 1
        if streak >= good_snap:
            return max_bps, 0, 0
        if cur_bps < max_bps:
            return min(max_bps, cur_bps * recover_step), streak, 0
        return cur_bps, streak, 0
    return cur_bps, good_streak, 0


def should_fountain_tick(
    *,
    stressed: bool,
    at_cap: bool,
    tick_n: int,
    every_n: int = _FOUNTAIN_EVERY_N,
    storm_active: bool = False,
    fountain_mode: bool = False,
) -> bool:
    """Rate-limit fountain submits on the send loop when not at hard cap."""
    if storm_active:
        return False
    if fountain_mode:
        # 8 MiB / loss-half never hits inflight cap (1035); still need ESI.
        return at_cap or tick_n % every_n == 0
    return stressed and (at_cap or tick_n % every_n == 0)


def should_track_fountain_gen(
    gen_id: int,
    next_needed: int,
    *,
    at_cap: bool,
    window: int = _FOUNTAIN_WINDOW,
) -> bool:
    """Track gens near the frontier for hybrid async fountain repair."""
    if gen_id < next_needed:
        return False
    limit = window if at_cap else min(window // 2, _FOUNTAIN_CAP_GENS + 4)
    return gen_id < next_needed + limit


def cap_fountain_gens(
    fountain_gens: set[int],
    next_needed: int,
    *,
    max_track: int = _FOUNTAIN_TRACK_MAX,
) -> None:
    for gid in list(fountain_gens):
        if gid < next_needed:
            fountain_gens.discard(gid)
    while len(fountain_gens) > max_track:
        drop = min(fountain_gens)
        if drop < next_needed:
            fountain_gens.discard(drop)
            continue
        fountain_gens.discard(drop)


def prune_fountain_gens_set(
    fountain_gens: set[int],
    next_needed: int,
    nack_rx: dict[int, int],
    nacks: list[int],
    *,
    gen_k: int,
    sent_before: int,
    inflight_gen_limit: int,
    window: int = _FOUNTAIN_WINDOW,
    max_track: int = _FOUNTAIN_TRACK_MAX,
) -> None:
    """Drop stale fountain gens outside the active frontier / inflight window."""
    enough = gen_k + _DECODE_MARGIN
    hi = next_needed + window
    lo = max(0, sent_before - inflight_gen_limit)
    for gid in list(fountain_gens):
        if gid < next_needed or gid >= hi or gid < lo:
            fountain_gens.discard(gid)
            continue
        rx = nack_rx.get(gid)
        if rx is not None and rx >= enough and gid not in nacks:
            fountain_gens.discard(gid)
    cap_fountain_gens(fountain_gens, next_needed, max_track=max_track)


def repair_storm_detected(
    repair_pkts_per_s: float,
    fountain_count: int,
    *,
    storm_pkts_s: float = _REPAIR_STORM_PKTS_S,
    track_max: int = _FOUNTAIN_TRACK_MAX,
) -> bool:
    return repair_pkts_per_s >= storm_pkts_s or fountain_count > track_max * 2


def prune_repair_meta(
    repair_extra: dict[int, int],
    last_repair_ts: dict[int, float],
    last_full_ts: dict[int, float],
    next_needed: int,
    *,
    keep: int = _REPAIR_META_KEEP,
) -> None:
    """Prevent repair bookkeeping dicts from growing with every touched gen."""
    for meta in (repair_extra, last_repair_ts, last_full_ts):
        for gid in list(meta):
            if gid < next_needed:
                meta.pop(gid, None)
        while len(meta) > keep:
            meta.pop(min(meta), None)


def track_fountain_gen(
    fountain_gens: set[int],
    gen_id: int,
    next_needed: int,
    *,
    at_cap: bool,
) -> None:
    if not should_track_fountain_gen(gen_id, next_needed, at_cap=at_cap):
        return
    fountain_gens.add(gen_id)
    cap_fountain_gens(fountain_gens, next_needed)


def fountain_targets(
    next_needed: int,
    sent_before: int,
    nacks: list[int],
    nack_rx: dict[int, int],
    *,
    gen_k: int,
    limit: int,
    decode_margin: int = _DECODE_MARGIN,
    frontier_only: bool = False,
) -> list[int]:
    """Oldest likely-incomplete gens to fountain while blast is at cap."""
    if sent_before <= 0 or limit <= 0:
        return []
    enough = gen_k + decode_margin
    out: list[int] = []
    seen: set[int] = set()

    def add(gid: int) -> bool:
        if gid < 0 or gid >= sent_before or gid in seen:
            return len(out) >= limit
        rx = nack_rx.get(gid)
        if rx is not None and rx >= enough:
            return len(out) >= limit
        seen.add(gid)
        out.append(gid)
        return len(out) >= limit

    if add(next_needed):
        return out
    for gid in sorted(g for g in nacks if g != next_needed):
        if add(gid):
            return out
    if frontier_only:
        return out
    start = max(0, next_needed)
    for gid in range(start, sent_before):
        if add(gid):
            return out
    return out


class _Win:
    """1s window counters; lock-free from the send thread, locked for workers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.enc_s = 0.0
        self.repair_enc_s = 0.0
        self.qdrop = 0

    def add_enc(self, s: float) -> None:
        with self._lock:
            self.enc_s += s

    def add_repair_enc(self, s: float) -> None:
        with self._lock:
            self.repair_enc_s += s

    def add_qdrop(self) -> None:
        with self._lock:
            self.qdrop += 1

    def take_workers(self) -> tuple[float, float, int]:
        with self._lock:
            out = (self.enc_s, self.repair_enc_s, self.qdrop)
            self.enc_s = 0.0
            self.repair_enc_s = 0.0
            self.qdrop = 0
            return out


def _pct(part: float, wall: float) -> int:
    if wall <= 1e-9:
        return 0
    return int(round(100.0 * min(part, wall) / wall))


# Per-worker-process state for blast encoding (initialized lazily).
_worker_mm: mmap.mmap | None = None
_worker_path: str | None = None
_worker_repair_enc: dict[int, GenEncoder] = {}
_worker_repair_order: list[int] = []


def _worker_repair_encoder(
    gid: int,
    raw: bytes,
    symbol_size: int,
    overhead_pct: int,
) -> GenEncoder:
    """LRU GenEncoder per gid so ensure_repair() is incremental across rounds."""
    enc = _worker_repair_enc.get(gid)
    if enc is not None:
        if gid in _worker_repair_order:
            _worker_repair_order.remove(gid)
        _worker_repair_order.append(gid)
        return enc
    enc = GenEncoder(raw, symbol_size, overhead_pct, systematic_only=True)
    _worker_repair_enc[gid] = enc
    _worker_repair_order.append(gid)
    while len(_worker_repair_order) > _REPAIR_ENC_CACHE_MAX:
        drop = _worker_repair_order.pop(0)
        _worker_repair_enc.pop(drop, None)
    return enc


def _encode_gen_worker(
    path: str,
    gid: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    fountain_bootstrap: bool = False,
) -> tuple[float, int, list[bytes], int]:
    """Encode one generation; returns (cpu_s, src_bytes, wires, bootstrap_repair_pkts)."""
    global _worker_mm, _worker_path
    t_enc = time.monotonic()
    if _worker_mm is None or _worker_path != path:
        f = open(path, "rb")
        _worker_mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        _worker_path = path
    off = gid * block_bytes
    end = min(off + block_bytes, file_size)
    # Cold page cache turns each block read into a synchronous disk stall
    # (measured: 1.5ms/gen warm vs ~9ms/gen cold). Hint a few blocks ahead.
    try:
        ra_end = min(off + 8 * block_bytes, file_size)
        _worker_mm.madvise(mmap.MADV_WILLNEED, off, ra_end - off)
    except (AttributeError, OSError, ValueError):
        pass
    raw = bytes(_worker_mm[off:end])
    if len(raw) < block_bytes:
        raw = raw + b"\x00" * (block_bytes - len(raw))
    k_est = max(1, (len(raw) + symbol_size - 1) // symbol_size)
    enc = GenEncoder(raw, symbol_size, overhead_pct, systematic_only=True)
    bootstrap = blast_repair_budget(k_est, overhead_pct)
    if bootstrap <= 0 and fountain_bootstrap:
        bootstrap = fountain_blast_budget(k_est, _FOUNTAIN_TARGET_OVERHEAD_PCT)
    if bootstrap:
        enc.ensure_repair(bootstrap)
    ts = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
    wires = [
        GenPacket(gid, i, blob, ts).pack()
        for i, blob in enumerate(enc.packets())
    ]
    return time.monotonic() - t_enc, end - off, wires, bootstrap


def _repair_gen_worker(
    path: str,
    gid: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    gen_k: int,
    prior_extra: int,
    send_n: int,
    ts_us: int,
) -> tuple[float, int, list[bytes]]:
    """Encode a fountain tail in a worker process (same GIL isolation as blast)."""
    global _worker_mm, _worker_path
    t_enc = time.monotonic()
    if _worker_mm is None or _worker_path != path:
        f = open(path, "rb")
        _worker_mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        _worker_path = path
    off = gid * block_bytes
    end = min(off + block_bytes, file_size)
    raw = bytes(_worker_mm[off:end])
    if len(raw) < block_bytes:
        raw = raw + b"\x00" * (block_bytes - len(raw))
    enc = _worker_repair_encoder(gid, raw, symbol_size, overhead_pct)
    want = max(1, min(int(send_n), _REPAIR_ROUND_MAX))
    target_budget = prior_extra + want
    new_pkts = enc.ensure_repair(target_budget)
    if not new_pkts:
        return time.monotonic() - t_enc, 0, []
    new_pkts = new_pkts[-want:]
    first_esi = enc.packet_count - len(new_pkts)
    wires = [
        GenPacket(gid, first_esi + i, blob, ts_us).pack()
        for i, blob in enumerate(new_pkts)
    ]
    return time.monotonic() - t_enc, len(new_pkts), wires


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
    gen_k: int = 192,
    overhead_pct: int = 0,
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
    inflight_gen_limit = compute_inflight_gen_limit(gen_k, symbol_size)
    adapt_pace = os.environ.get("TETRYS_ADAPT_PACE", "").lower() in ("1", "true", "yes")
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
    fountain_mode = fountain_redundancy(overhead_pct)
    print(
        f"xfer=gen gens={total_gens} T={symbol_size} K~={gen_k} "
        f"block={block_bytes} overhead={overhead_pct}% "
        f"{'(fountain redundancy) ' if fountain_mode else ''}"
        f"cc=off "
        f"rate_mbit={rate_mbit} inflight_gens={inflight_gen_limit} "
        f"adapt_pace={'on' if adapt_pace else 'off'} "
        f"mode={'fountain' if fountain_mode else 'hybrid+async_fountain'} "
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
    for _ in range(16):
        sock.sendto(meta, client_addr)

    max_bps = max(rate_mbit, 1.0) * 1_000_000 / 8
    limiter = RateLimiter(
        max_bps,
        start_bps=1.0 if ramp_s > 0 else max_bps,
        min_frac=_ADAPT_PACE_MIN_FRAC if adapt_pace else 0.90,
    )
    t_ramp0 = time.monotonic()
    last_meta_ts = t_ramp0
    pace_state = {
        "bps": max_bps,
        "last_completed": -1,
        "last_ts": t_ramp0,
        "good_streak": 0,
        "bad_streak": 0,
    }
    pace_lock = threading.Lock()
    if ramp_s > 0:
        print(f"pace ramp {ramp_s:.1f}s 0→{max_bps / (1024 * 1024):.1f} MiB/s")

    enc_lock = threading.Lock()
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
        "nack_rx": {},
        "hol_miss_esi": [],
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
                    now = time.monotonic()
                    completed = pkt.completed_gens
                    with fb_lock:
                        fb_state["next_needed"] = pkt.next_needed_gen
                        fb_state["completed"] = completed
                        fb_state["nacks"] = list(pkt.nack_gens)
                        counts = pkt.nack_rx_counts or []
                        fb_state["nack_rx"] = dict(zip(pkt.nack_gens, counts))
                        fb_state["hol_miss_esi"] = list(pkt.hol_miss_esi or [])
                        fb_state["echo"] = pkt.echo_ts_us
                        if completed >= total_gens and total_gens > 0:
                            fb_state["done"] = True
                    if adapt_pace and (now - t_ramp0) >= ramp_s + _ADAPT_PACE_WARMUP_S:
                        with pace_lock:
                            last_c = pace_state["last_completed"]
                            last_ts = pace_state["last_ts"]
                            cur = pace_state["bps"]
                            if last_c >= 0 and completed > last_c:
                                dt = now - last_ts
                                if dt >= 0.12:
                                    delivery_bps = (completed - last_c) * block_bytes / dt
                                    cur, streak, bad = update_adaptive_pace_bps(
                                        delivery_bps=delivery_bps,
                                        cur_bps=cur,
                                        max_bps=max_bps,
                                        good_streak=pace_state["good_streak"],
                                        bad_streak=pace_state["bad_streak"],
                                    )
                                    pace_state["bps"] = cur
                                    pace_state["good_streak"] = streak
                                    pace_state["bad_streak"] = bad
                            pace_state["last_completed"] = completed
                            pace_state["last_ts"] = now
                            limiter.set_rate(pace_state["bps"])
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
                return
        if adapt_pace:
            with pace_lock:
                limiter.set_rate(pace_state["bps"])
        else:
            limiter.set_rate(max_bps)

    def send_batch(wires: list[bytes], *, repair: bool = False) -> None:
        nonlocal send_s, pace_s, wire_bytes, blast_pkts, repair_pkts_w
        if not wires:
            return
        pace_tick()
        total = sum(len(w) for w in wires)
        pace_s += limiter.consume(total)
        assert client_addr is not None
        t_send = time.monotonic()
        send_datagrams(sock, client_addr, wires)
        send_s += time.monotonic() - t_send
        wire_bytes += total
        if repair:
            repair_pkts_w += len(wires)
        else:
            blast_pkts += len(wires)

    def get_or_make_encoder(mm: mmap.mmap, gen_id: int) -> GenEncoder | None:
        if gen_id < 0 or gen_id >= total_gens:
            return None
        with enc_lock:
            enc = encoders.get(gen_id)
        if enc is not None:
            return enc
        off = gen_id * block_bytes
        end = min(off + block_bytes, file_size)
        raw = bytes(mm[off:end])
        if len(raw) < block_bytes:
            raw = raw + b"\x00" * (block_bytes - len(raw))
        enc = GenEncoder(raw, symbol_size, overhead_pct, systematic_only=True)
        with enc_lock:
            existing = encoders.get(gen_id)
            if existing is not None:
                return existing
            encoders[gen_id] = enc
        return enc

    def prune_encoders(protected: set[int]) -> None:
        """Bound native Raptor encoder memory while retaining active repairs."""
        target = _ENCODER_KEEP + len(protected)
        with enc_lock:
            if len(encoders) <= target:
                return
            for old in list(encoders):
                if len(encoders) <= target:
                    break
                if old in protected:
                    continue
                encoders.pop(old, None)

    encode_pool: ProcessPoolExecutor | None = None
    file_path_str = str(file_path)

    def repair_one(
        mm: mmap.mmap,
        nid: int,
        *,
        now: float,
        symbols_rx: int | None = None,
        cooldown_s: float = _REPAIR_COOLDOWN_S,
        send_n: int | None = None,
        source_esis: list[int] | None = None,
    ) -> list[bytes]:
        """Sync repair encode (repair thread fallback)."""
        if (now - last_repair_ts.get(nid, 0.0)) < cooldown_s:
            return []
        if source_esis:
            enc = get_or_make_encoder(mm, nid)
            if enc is not None:
                pkts = enc.packets()
                ts_us = int(now * 1_000_000) & 0xFFFFFFFF
                wires = [
                    GenPacket(nid, esi, pkts[esi], ts_us).pack()
                    for esi in source_esis[:32]
                    if 0 <= esi < len(pkts)
                ]
                if wires:
                    last_repair_ts[nid] = now
                    return wires
        if (
            symbols_rx is not None
            and symbols_rx >= gen_k + _DECODE_MARGIN
            and send_n is None
        ):
            return []
        if nid < 0 or nid >= total_gens:
            return []
        prior_extra = repair_extra.get(nid, 0)
        base_r = repair_round_size(gen_k, overhead_pct)
        if send_n is None:
            if symbols_rx is None or symbols_rx <= 0:
                send_n = base_r
            else:
                deficit = max(1, gen_k + _DECODE_MARGIN - symbols_rx)
                send_n = math.ceil(deficit * (1.0 + _REPAIR_SURPLUS))
        send_n = max(1, min(int(send_n), _REPAIR_ROUND_MAX))
        ts_us = int(now * 1_000_000) & 0xFFFFFFFF
        if encode_pool is not None:
            try:
                enc_cpu_s, sent, wires = encode_pool.submit(
                    _repair_gen_worker,
                    file_path_str,
                    nid,
                    block_bytes,
                    file_size,
                    symbol_size,
                    overhead_pct,
                    gen_k,
                    prior_extra,
                    send_n,
                    ts_us,
                ).result(timeout=30.0)
            except Exception:
                return []
            if not wires:
                return []
            win.add_repair_enc(enc_cpu_s)
            repair_extra[nid] = prior_extra + sent
            last_repair_ts[nid] = now
            return wires
        enc = get_or_make_encoder(mm, nid)
        if enc is None:
            return []
        t_enc = time.monotonic()
        target_budget = prior_extra + send_n
        new_pkts = enc.ensure_repair(target_budget)
        if not new_pkts:
            return []
        new_pkts = new_pkts[-send_n:]
        repair_extra[nid] = prior_extra + len(new_pkts)
        first_esi = enc.packet_count - len(new_pkts)
        wires = [
            GenPacket(nid, first_esi + i, blob, ts_us).pack()
            for i, blob in enumerate(new_pkts)
        ]
        last_repair_ts[nid] = now
        win.add_repair_enc(time.monotonic() - t_enc)
        return wires

    def repair_nacks(
        mm: mmap.mmap,
        nacks: list[int],
        nack_rx: dict[int, int],
        next_needed: int,
        *,
        now: float,
        limit: int,
        sent_before: int,
        hol_miss_esi: list[int] | None = None,
    ) -> list[bytes]:
        ordered = sorted(
            (g for g in nacks if 0 <= g < sent_before),
            key=lambda g: (0 if g == next_needed else 1, abs(g - next_needed)),
        )[:limit]
        wires: list[bytes] = []
        for nid in ordered:
            miss = hol_miss_esi if nid == next_needed else None
            wires.extend(
                repair_one(
                    mm,
                    nid,
                    now=now,
                    symbols_rx=nack_rx.get(nid),
                    source_esis=miss,
                )
            )
        if ordered:
            prune_encoders(set(ordered))
        return wires

    t0 = time.monotonic()
    bytes_sent_payload = 0
    gens_sent = 0
    repair_sent = 0
    last_progress = t0
    stuck_nn = -1
    stuck_nn_since = t0
    win = _Win()
    send_s = 0.0
    pace_s = 0.0
    cap_s = 0.0
    wait_enc_s = 0.0
    wire_bytes = 0
    blast_pkts = 0
    repair_pkts_w = 0

    try:
        with file_path.open("rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            # spawn: safe with the already-running feedback thread, and worker
            # processes encode without contending for this process's GIL.
            encode_pool = ProcessPoolExecutor(
                max_workers=_ENCODE_WORKERS,
                mp_context=multiprocessing.get_context("spawn"),
            )
            encode_futures: dict[
                int, Future[tuple[float, int, list[bytes], int]]
            ] = {}
            repair_q: queue.Queue[list[bytes]] = queue.Queue(maxsize=64)
            repair_fut_lock = threading.Lock()
            repair_futures: list[
                tuple[Future[tuple[float, int, list[bytes]]], int, int]
            ] = []
            stop_repair = threading.Event()
            cursor = {"gen_id": 0}
            fountain_gens: set[int] = set()
            fountain_lock = threading.Lock()
            fountain_tick_n = 0
            storm_state = {"until": 0.0}

            def repair_pending_count() -> int:
                with repair_fut_lock:
                    return len(repair_futures)

            def repair_submit_async(
                mm: mmap.mmap,
                nid: int,
                *,
                now: float,
                symbols_rx: int | None = None,
                cooldown_s: float = _REPAIR_COOLDOWN_S,
                send_n: int | None = None,
            ) -> bool:
                if now < storm_state["until"] and send_n is not None:
                    return False
                if (now - last_repair_ts.get(nid, 0.0)) < cooldown_s:
                    return False
                if (
                    symbols_rx is not None
                    and symbols_rx >= gen_k + _DECODE_MARGIN
                    and send_n is None
                ):
                    return False
                if nid < 0 or nid >= total_gens or encode_pool is None:
                    wires = repair_one(
                        mm,
                        nid,
                        now=now,
                        symbols_rx=symbols_rx,
                        cooldown_s=cooldown_s,
                        send_n=send_n,
                    )
                    if wires:
                        try:
                            repair_q.put_nowait(wires)
                        except queue.Full:
                            win.add_qdrop()
                    return bool(wires)
                prior_extra = repair_extra.get(nid, 0)
                base_r = repair_round_size(gen_k, overhead_pct)
                if send_n is None:
                    if symbols_rx is None or symbols_rx <= 0:
                        send_n = base_r
                    else:
                        deficit = max(1, gen_k + _DECODE_MARGIN - symbols_rx)
                        send_n = math.ceil(deficit * (1.0 + _REPAIR_SURPLUS))
                if fountain_mode:
                    send_n = cap_fountain_send(
                        gen_k,
                        prior_extra,
                        send_n,
                        symbols_rx,
                    )
                    if send_n <= 0:
                        return False
                send_n = max(1, min(int(send_n), _REPAIR_ROUND_MAX))
                ts_us = int(now * 1_000_000) & 0xFFFFFFFF
                with repair_fut_lock:
                    if len(repair_futures) >= _REPAIR_FUTURES_MAX:
                        return False
                fut = encode_pool.submit(
                    _repair_gen_worker,
                    file_path_str,
                    nid,
                    block_bytes,
                    file_size,
                    symbol_size,
                    overhead_pct,
                    gen_k,
                    prior_extra,
                    send_n,
                    ts_us,
                )
                with repair_fut_lock:
                    repair_futures.append((fut, nid, prior_extra))
                last_repair_ts[nid] = now
                return True

            def drain_repair_futures() -> int:
                n = 0
                pending: list[tuple[Future, int, int]] = []
                with repair_fut_lock:
                    for item in repair_futures:
                        pending.append(item)
                    repair_futures.clear()
                still: list[tuple[Future, int, int]] = []
                for fut, nid, prior_extra in pending:
                    if not fut.done():
                        still.append((fut, nid, prior_extra))
                        continue
                    try:
                        enc_cpu_s, sent, wires = fut.result()
                    except Exception:
                        continue
                    if not wires:
                        continue
                    win.add_repair_enc(enc_cpu_s)
                    repair_extra[nid] = prior_extra + sent
                    send_batch(wires, repair=True)
                    n += len(wires)
                if still:
                    with repair_fut_lock:
                        repair_futures.extend(still)
                return n

            def prune_fountain(
                next_needed: int,
                nack_rx: dict[int, int],
                nacks: list[int],
                sent_before: int,
            ) -> None:
                with fountain_lock:
                    prune_fountain_gens_set(
                        fountain_gens,
                        next_needed,
                        nack_rx,
                        nacks,
                        gen_k=gen_k,
                        sent_before=sent_before,
                        inflight_gen_limit=inflight_gen_limit,
                    )

            def fountain_send_tick(
                mm: mmap.mmap,
                *,
                now: float,
                next_needed: int,
                nacks: list[int],
                nack_rx: dict[int, int],
                sent_before: int,
                at_cap: bool,
            ) -> int:
                """Queue async fountain repair; returns number of submits."""
                if sent_before <= 0:
                    return 0
                prune_fountain(next_needed, nack_rx, nacks, sent_before)
                if not fountain_mode:
                    with fountain_lock:
                        if not fountain_gens:
                            return 0
                targets = fountain_targets(
                    next_needed,
                    sent_before,
                    nacks,
                    nack_rx,
                    gen_k=gen_k,
                    limit=_FOUNTAIN_CAP_GENS if not at_cap else _FOUNTAIN_CAP_GENS * 2,
                    frontier_only=not at_cap,
                )
                queued = 0
                for i, nid in enumerate(targets):
                    if repair_submit_async(
                        mm,
                        nid,
                        now=now,
                        symbols_rx=nack_rx.get(nid),
                        cooldown_s=_FOUNTAIN_CAP_COOLDOWN_S,
                        send_n=_FOUNTAIN_CAP_PER_GEN,
                    ):
                        queued += 1
                if targets:
                    prune_encoders(set(targets))
                return queued

            def fill_encode_queue(start: int) -> None:
                stop = min(total_gens, start + _ENCODE_PREFETCH)
                for gid in range(start, stop):
                    if gid in encode_futures:
                        continue
                    encode_futures[gid] = encode_pool.submit(
                        _encode_gen_worker,
                        file_path_str,
                        gid,
                        block_bytes,
                        file_size,
                        symbol_size,
                        overhead_pct,
                        fountain_mode,
                    )

            def repair_loop() -> None:
                while not stop_repair.is_set():
                    with fb_lock:
                        if fb_state["done"]:
                            break
                        nacks = list(fb_state["nacks"])
                        nack_rx = dict(fb_state["nack_rx"])
                        next_needed = int(fb_state["next_needed"])
                        completed = int(fb_state["completed"])
                        hol_miss = list(fb_state.get("hol_miss_esi") or [])
                    gid = cursor["gen_id"]
                    if gid <= 0:
                        time.sleep(0.005)
                        continue
                    incomplete = max(0, gid - completed)
                    at_cap = incomplete >= inflight_gen_limit
                    now = time.monotonic()
                    storm_active = now < storm_state["until"]
                    stuck = (now - stuck_nn_since) >= _STUCK_S and not storm_active
                    wires: list[bytes] = []
                    if (
                        stuck
                        and (gid - next_needed) >= _REPAIR_LAG
                        and 0 <= next_needed < gid
                        and (now - last_full_ts.get(next_needed, 0.0))
                        >= _STUCK_REPAIR_COOLDOWN_S
                    ):
                        last_repair_ts.pop(next_needed, None)
                        extra = repair_one(
                            mm,
                            next_needed,
                            now=now,
                            symbols_rx=nack_rx.get(next_needed),
                            source_esis=hol_miss or None,
                        )
                        if extra:
                            last_full_ts[next_needed] = now
                            wires.extend(extra)
                    if not nacks and 0 <= next_needed < gid:
                        nacks = [next_needed]
                    frontier_lag = max(0, gid - next_needed)
                    repair_limit = repair_thread_limit(
                        at_cap=at_cap,
                        storm_active=storm_active,
                        nack_count=len(nacks),
                        frontier_lag=frontier_lag,
                    )
                    if not storm_active or nacks:
                        wires.extend(
                            repair_nacks(
                                mm,
                                nacks if not storm_active else nacks[:1],
                                nack_rx,
                                next_needed,
                                now=now,
                                limit=repair_limit,
                                sent_before=gid,
                                hol_miss_esi=hol_miss or None,
                            )
                        )
                    if wires:
                        try:
                            repair_q.put(wires, timeout=0.05)
                        except queue.Full:
                            win.add_qdrop()
                    else:
                        time.sleep(0.004)

            repair_thread = threading.Thread(
                target=repair_loop, name="gen-repair", daemon=True
            )
            repair_thread.start()

            def drain_repairs() -> int:
                n = 0
                while True:
                    try:
                        batch = repair_q.get_nowait()
                    except queue.Empty:
                        break
                    send_batch(batch, repair=True)
                    n += len(batch)
                return n

            try:
                gen_id = 0
                fill_encode_queue(0)
                while gen_id < total_gens:
                    with fb_lock:
                        if fb_state["done"]:
                            break
                        next_needed = int(fb_state["next_needed"])
                        completed = int(fb_state["completed"])

                    now = time.monotonic()
                    if completed <= 0 and (now - last_meta_ts) >= 0.15:
                        sock.sendto(meta, client_addr)
                        last_meta_ts = now
                    if ramp_s <= 0 or (now - t_ramp0) >= ramp_s:
                        pace_tick()
                    if next_needed != stuck_nn:
                        stuck_nn = next_needed
                        stuck_nn_since = now
                        prune_repair_meta(
                            repair_extra,
                            last_repair_ts,
                            last_full_ts,
                            next_needed,
                        )
                    incomplete = max(0, gen_id - completed)
                    if incomplete >= (inflight_gen_limit * 3) // 4:
                        limiter.set_burst_s(0.002)
                    elif incomplete <= inflight_gen_limit // 2:
                        limiter.set_burst_s(0.016)
                    repair_sent += drain_repairs()
                    repair_sent += drain_repair_futures()
                    lag = gen_id - next_needed
                    stressed = pipeline_stressed(
                        incomplete, lag, inflight_gen_limit=inflight_gen_limit
                    )
                    at_cap = incomplete >= inflight_gen_limit
                    with fb_lock:
                        nacks = list(fb_state["nacks"])
                        nack_rx = dict(fb_state["nack_rx"])
                    storm_active = now < storm_state["until"]
                    if should_fountain_tick(
                        stressed=stressed,
                        at_cap=at_cap,
                        tick_n=fountain_tick_n,
                        storm_active=storm_active,
                        fountain_mode=fountain_mode,
                    ):
                        fountain_send_tick(
                            mm,
                            now=now,
                            next_needed=next_needed,
                            nacks=nacks,
                            nack_rx=nack_rx,
                            sent_before=gen_id,
                            at_cap=at_cap,
                        )
                    fountain_tick_n += 1
                    repair_sent += drain_repair_futures()
                    if should_pause_blast(
                        incomplete,
                        lag,
                        inflight_gen_limit=inflight_gen_limit,
                    ):
                        if repair_q.empty() and not repair_futures:
                            time.sleep(0.001)
                            cap_s += 0.001
                        continue

                    fut = encode_futures.get(gen_id)
                    if fut is None:
                        fill_encode_queue(gen_id)
                        time.sleep(0.0005)
                        wait_enc_s += 0.0005
                        continue
                    if not fut.done():
                        time.sleep(0.0005)
                        wait_enc_s += 0.0005
                        continue
                    enc_cpu_s, source_bytes, wires, bootstrap = fut.result()
                    win.add_enc(enc_cpu_s)
                    encode_futures.pop(gen_id, None)
                    fill_encode_queue(gen_id + 1)
                    prune_encoders({next_needed})

                    send_batch(wires)
                    if bootstrap:
                        repair_extra[gen_id] = bootstrap
                    if stressed:
                        with fountain_lock:
                            track_fountain_gen(
                                fountain_gens,
                                gen_id,
                                next_needed,
                                at_cap=at_cap,
                            )
                    gens_sent += 1
                    bytes_sent_payload += source_bytes
                    gen_id += 1
                    cursor["gen_id"] = gen_id

                    now = time.monotonic()
                    if now - last_progress >= 1.0:
                        dt = now - last_progress
                        last_progress = now
                        with fb_lock:
                            done_g = fb_state["completed"]
                            nn = int(fb_state["next_needed"])
                        rate = bytes_sent_payload / max(now - t0, 1e-6) / (1024 * 1024)
                        pace_mbit = limiter.rate * 8.0 / 1_000_000.0
                        cap_mbit = max_bps * 8.0 / 1_000_000.0
                        enc_s, renc_s, qdrop = win.take_workers()
                        sys_s, blk_s, sys_n, blk_n = take_send_stats()
                        parts = {
                            "send": send_s,
                            "pace": pace_s,
                            "cap": cap_s,
                            "wenc": wait_enc_s,
                        }
                        bottleneck = max(parts, key=parts.get)
                        wire_mib = wire_bytes / dt / (1024 * 1024)
                        repair_rate = repair_pkts_w / max(dt, 1e-6)
                        with fountain_lock:
                            fount_n = len(fountain_gens)
                        if repair_storm_detected(repair_rate, fount_n):
                            storm_state["until"] = now + _REPAIR_STORM_BACKOFF_S
                            if adapt_pace:
                                with pace_lock:
                                    floor = max_bps * _ADAPT_PACE_MIN_FRAC
                                    pace_state["bps"] = max(
                                        floor, pace_state["bps"] * 0.70
                                    )
                                    pace_state["good_streak"] = 0
                                    limiter.set_rate(pace_state["bps"])
                        storm_flag = "storm" if now < storm_state["until"] else (
                            "fountain" if fountain_mode else (
                                "stress" if stressed else "clean"
                            )
                        )
                        print(
                            f"progress {gen_id}/{total_gens} "
                            f"client_done={done_g} "
                            f"lag={gen_id - nn} "
                            f"incomplete={max(0, gen_id - int(done_g))}/"
                            f"{inflight_gen_limit} "
                            f"fountain={fount_n} "
                            f"pipe={storm_flag} "
                            f"repair_extra={repair_sent} "
                            f"rate={pace_mbit:.0f}/{cap_mbit:.0f}Mbit "
                            f"app={rate:.1f} MiB/s"
                        )
                        print(
                            f"  xfer wire={wire_mib:.1f} "
                            f"blast={blast_pkts} repair={repair_pkts_w} "
                            f"enc_q={len(encode_futures)} "
                            f"repair_fut={repair_pending_count()} "
                            f"meta={len(repair_extra)} "
                            f"qdrop={qdrop} "
                            f"enc_cpu={enc_s * 1000:.0f}ms "
                            f"renc={renc_s * 1000:.0f}ms "
                            f"send={_pct(send_s, dt)}% "
                            f"(sys={sys_s * 1000:.0f}ms/{sys_n} "
                            f"blk={blk_s * 1000:.0f}ms/{blk_n}) "
                            f"pace={_pct(pace_s, dt)}% "
                            f"cap={_pct(cap_s, dt)}% "
                            f"wenc={_pct(wait_enc_s, dt)}% "
                            f"limit={bottleneck}"
                        )
                        send_s = pace_s = cap_s = wait_enc_s = 0.0
                        wire_bytes = 0
                        blast_pkts = 0
                        repair_pkts_w = 0

                stop_repair.set()
                repair_thread.join(timeout=1.0)
                repair_sent += drain_repairs()
                repair_sent += drain_repair_futures()

                fin = FinPacket(True, total_gens).pack()
                for _ in range(16):
                    sock.sendto(fin, client_addr)

                drain_deadline = time.monotonic() + 300.0
                while time.monotonic() < drain_deadline:
                    with fb_lock:
                        if fb_state["done"] or fb_state["completed"] >= total_gens:
                            break
                        nacks = list(fb_state["nacks"])
                        nack_rx = dict(fb_state["nack_rx"])
                        next_needed = int(fb_state["next_needed"])
                        hol_miss = list(fb_state.get("hol_miss_esi") or [])
                    if not nacks:
                        if 0 <= next_needed < total_gens:
                            nacks = [next_needed]
                        else:
                            sock.sendto(fin, client_addr)
                            time.sleep(0.02)
                            continue
                    now = time.monotonic()
                    wires = repair_nacks(
                        mm,
                        nacks,
                        nack_rx,
                        next_needed,
                        now=now,
                        limit=24,
                        sent_before=total_gens,
                        hol_miss_esi=hol_miss or None,
                    )
                    if wires:
                        send_batch(wires, repair=True)
                        repair_sent += len(wires)
                    repair_sent += drain_repairs()
                    sock.sendto(fin, client_addr)
                    time.sleep(0.005)

            finally:
                stop_repair.set()
                repair_thread.join(timeout=1.0)
                encode_pool.shutdown(wait=False, cancel_futures=True)
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
    overhead_pct = meta.gen_overhead_pct
    block_bytes = gen_k * symbol_size
    file_size = meta.file_size
    total_gens = (file_size + block_bytes - 1) // block_bytes if file_size else 0
    inflight_gen_limit = compute_inflight_gen_limit(gen_k, symbol_size)
    feedback_horizon = client_feedback_horizon(inflight_gen_limit)

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
    next_needed_hint = 0
    max_gid_seen = -1
    gens_recovered = 0
    last_echo = 0
    fin_seen = False
    t0 = time.monotonic()
    last_progress = t0
    last_fb = 0.0
    bytes_written = 0
    rx_pkts = 0
    skip_done = 0
    dup_esi = 0
    wait_rx_s = 0.0
    recv_s = 0.0
    dec_s = 0.0
    write_s = 0.0
    write_inline = 0
    rx_bytes = 0
    # Unbounded: a 1s disk stall at 90 MiB/s is ~350 gens. A 128-deep queue
    # overflows and used to pwrite on the recv thread, dropping the UDP
    # socket and collapsing the rest of the transfer.
    write_q: queue.Queue[tuple[int, bytes] | None] = queue.Queue()
    write_lock = threading.Lock()
    _fadv_dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)

    def bit_get(i: int) -> bool:
        if i < 0 or i >= total_gens:
            return True
        return bool(done_bits[i >> 3] & (1 << (i & 7)))

    def bit_set(i: int) -> None:
        nonlocal completed, next_needed_hint
        if i < 0 or i >= total_gens or bit_get(i):
            return
        done_bits[i >> 3] |= 1 << (i & 7)
        completed += 1
        if i == next_needed_hint:
            while next_needed_hint < total_gens and bit_get(next_needed_hint):
                next_needed_hint += 1

    def pwrite_one(off: int, data: bytes) -> None:
        nonlocal bytes_written, write_s
        if off >= file_size:
            return
        chunk = data[: max(0, file_size - off)]
        t_w = time.monotonic()
        os.pwrite(fd, chunk, off)
        dt = time.monotonic() - t_w
        if _fadv_dontneed is not None:
            try:
                os.posix_fadvise(fd, off, len(chunk), _fadv_dontneed)
            except OSError:
                pass
        with write_lock:
            write_s += dt
            bytes_written += len(chunk)

    def write_loop() -> None:
        while True:
            item = write_q.get()
            if item is None:
                break
            pwrite_one(item[0], item[1])

    def enqueue_write(off: int, data: bytes) -> None:
        write_q.put_nowait((off, data))

    write_thread = threading.Thread(target=write_loop, name="gen-write", daemon=True)
    write_thread.start()

    def send_fb() -> None:
        nonlocal last_fb
        next_needed = next_needed_hint
        nacks: list[int] = []
        nack_rx: list[int] = []
        seen: set[int] = set()
        hi = min(total_gens, next_needed + feedback_horizon + 1)
        for i in range(next_needed, hi):
            if bit_get(i):
                continue
            if i not in decoders and i > max_gid_seen:
                continue
            nacks.append(i)
            seen.add(i)
            dec = decoders.get(i)
            nack_rx.append(dec.symbols_rx if dec is not None else 0)
            if len(nacks) >= 64:
                break
        if len(nacks) < 64:
            for i in sorted(decoders):
                if i in seen or bit_get(i):
                    continue
                nacks.append(i)
                seen.add(i)
                dec = decoders[i]
                nack_rx.append(dec.symbols_rx)
                if len(nacks) >= 64:
                    break
        hol_miss_esi: list[int] | None = None
        if next_needed < total_gens:
            dec = decoders.get(next_needed)
            if dec is not None and dec.done is None:
                miss = dec.missing_source_esi(gen_k, limit=32)
                if miss:
                    hol_miss_esi = miss
        pkt = GenFeedbackPacket(
            next_needed,
            nacks,
            last_echo,
            completed,
            nack_rx,
            hol_miss_esi,
        )
        sock.sendto(pkt.pack(), server)
        last_fb = time.monotonic()

    try:
        while True:
            timeout = 0.02 if fin_seen else 0.05
            t_sel = time.monotonic()
            r, _, _ = select.select([sock], [], [], timeout)
            now = time.monotonic()
            if fin_seen:
                fb_every = 0.05
            elif len(decoders) >= 16:
                fb_every = 0.02
            else:
                fb_every = 0.1
            if not r:
                wait_rx_s += now - t_sel
                send_fb()
                if fin_seen and completed >= total_gens:
                    break
                if now - t0 > 3600:
                    print("transfer timeout", file=sys.stderr)
                    return 1
                continue

            while True:
                try:
                    t_r = time.monotonic()
                    batch = recv_datagrams(sock, 64)
                    recv_s += time.monotonic() - t_r
                except BlockingIOError:
                    break
                for data in batch:
                    if len(data) < 4 or data[0] != MAGIC:
                        continue
                    ptype = data[2]
                    if ptype == PKT_GEN:
                        try:
                            gp = GenPacket.unpack(data)
                        except ValueError:
                            continue
                        rx_pkts += 1
                        rx_bytes += len(data)
                        last_echo = gp.send_ts_us
                        gid = gp.gen_id
                        max_gid_seen = max(max_gid_seen, gid)
                        if bit_get(gid):
                            skip_done += 1
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
                        t_dec = time.monotonic()
                        dups0 = dec.dup_esi
                        out = dec.add_packet(gp.payload, gp.esi)
                        dec_s += time.monotonic() - t_dec
                        dup_esi += dec.dup_esi - dups0
                        if out is not None:
                            if dec.symbols_rx > gen_k + 1:
                                gens_recovered += 1
                            payload = out[:tlen]
                            bit_set(gid)
                            decoders.pop(gid, None)
                            enqueue_write(gid * block_bytes, payload)
                    elif ptype == PKT_FIN:
                        fin_seen = True
                    elif ptype == PKT_META:
                        continue

            if now - last_fb > fb_every:
                send_fb()

            if now - last_progress >= 1.0:
                dt = now - last_progress
                last_progress = now
                pct = 100.0 * completed / total_gens if total_gens else 100.0
                rate = (
                    completed * block_bytes / max(now - t0, 1e-6) / (1024 * 1024)
                )
                rx_mib = rx_bytes / dt / (1024 * 1024)
                parts = {
                    "wait_rx": wait_rx_s,
                    "recv": recv_s,
                    "dec": dec_s,
                    "write": write_s,
                }
                bottleneck = max(parts, key=parts.get)
                print(
                    f"progress {completed}/{total_gens} ({pct:.1f}%) "
                    f"gens_recovered≈{gens_recovered} "
                    f"decoders={len(decoders)} "
                    f"{rate:.1f} MiB/s"
                )
                print(
                    f"  xfer rx={rx_mib:.1f} pkts={rx_pkts} "
                    f"skip_done={skip_done} dup_esi={dup_esi} "
                    f"wait_rx={_pct(wait_rx_s, dt)}% "
                    f"recv={_pct(recv_s, dt)}% "
                    f"dec={_pct(dec_s, dt)}% "
                    f"write={_pct(write_s, dt)}% "
                    f"wq={write_q.qsize()} winl={write_inline} "
                    f"limit={bottleneck}"
                )
                rx_pkts = skip_done = dup_esi = write_inline = 0
                wait_rx_s = recv_s = dec_s = 0.0
                with write_lock:
                    write_s = 0.0
                rx_bytes = 0

            if completed >= total_gens:
                break
    finally:
        write_q.put(None)
        write_thread.join(timeout=60.0)
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
