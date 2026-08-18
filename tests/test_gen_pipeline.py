"""Regression tests for gen pipeline / async fountain bottlenecks."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tetrys_nc.gen_raptor import require_raptorq
from tetrys_nc.gen_xfer import (
    _FOUNTAIN_EVERY_N,
    _FOUNTAIN_RATE_START,
    _FOUNTAIN_SCHEDULE_MAX,
    _FOUNTAIN_TRACK_MAX,
    _FOUNTAIN_WINDOW,
    _MAX_INFLIGHT_BYTES,
    _MIN_INFLIGHT_GENS,
    _REPAIR_META_KEEP,
    _encode_gen_worker,
    adapt_fountain_fraction,
    allocate_fountain_packets,
    can_schedule_fountain_repair,
    cap_fountain_gens,
    fountain_gen_weights,
    fountain_open_stats,
    gen_still_open,
    prune_fountain_gens_set,
    prune_repair_meta,
    repair_storm_detected,
    should_fountain_tick,
    should_track_fountain_gen,
    track_fountain_gen,
)
from tetrys_nc.packets import GenPacket


def test_inflight_gen_limit_from_bytes():
    # K=48, T=1350 → block=64800; 64 MiB / block ≈ 1035 gens.
    block = 48 * 1350
    limit = max(_MIN_INFLIGHT_GENS, _MAX_INFLIGHT_BYTES // block)
    assert limit > 1000


def test_inflight_gen_limit_respects_min_for_huge_blocks():
    block = 8 * 1024 * 1024
    limit = max(_MIN_INFLIGHT_GENS, _MAX_INFLIGHT_BYTES // block)
    assert limit == _MIN_INFLIGHT_GENS


def test_should_fountain_tick_streams_and_runs_every_turn_at_cap():
    for tick in range(8):
        assert should_fountain_tick(
            at_cap=True,
            tick_n=tick,
        )
    ticks = [
        should_fountain_tick(at_cap=False, tick_n=n, every_n=2)
        for n in range(12)
    ]
    assert ticks == [True, False, True, False, True, False, True, False, True, False, True, False]


def test_should_fountain_tick_disabled_during_storm():
    assert not should_fountain_tick(
        at_cap=True, tick_n=0, storm_active=True
    )


def test_cap_fountain_gens_drops_oldest():
    s = set(range(100, 200))
    cap_fountain_gens(s, next_needed=100, max_track=64)
    assert len(s) == 64
    assert min(s) == 136
    assert max(s) == 199


def test_prune_fountain_gens_set_window_and_inflight():
    s = {10, 50, 60, 80, 120}
    prune_fountain_gens_set(
        s,
        next_needed=50,
        nack_rx={60: 200},
        nacks=[],
        gen_k=192,
        sent_before=100,
        inflight_gen_limit=1035,
        window=48,
    )
    assert 10 not in s  # below sent_before - inflight (36)
    assert 60 not in s  # enough symbols
    assert 50 in s
    assert 80 in s
    assert 120 not in s  # beyond next_needed + window (98)


def test_should_track_fountain_gen_clean_vs_cap():
    assert not should_track_fountain_gen(500, 100, at_cap=False)
    assert should_track_fountain_gen(103, 100, at_cap=False)
    assert should_track_fountain_gen(140, 100, at_cap=True)
    assert not should_track_fountain_gen(200, 100, at_cap=True)


def test_adapt_fountain_fraction_rises_at_cap():
    lo = adapt_fountain_fraction(
        0.05, open_count=2, avg_deficit=0.0, gen_k=48, at_cap=False
    )
    hi = adapt_fountain_fraction(
        0.05,
        open_count=2,
        avg_deficit=0.0,
        gen_k=48,
        at_cap=True,
    )
    assert hi > lo


def test_fountain_gen_weights_prioritize_frontier():
    weights = fountain_gen_weights(
        10,
        40,
        nacks=[10, 12, 18],
        nack_rx={10: 20, 12: 200, 18: 5},
        gen_k=48,
        decode_margin=2,
    )
    assert 12 not in weights
    assert weights[10] > weights[18]


def test_allocate_fountain_packets_respects_budget():
    weights = {1: 8.0, 2: 4.0, 3: 1.0}
    got = allocate_fountain_packets(5, weights, round_max=16)
    assert sum(got.values()) <= 5
    assert 1 in got
    assert got[1] >= got.get(3, 0)


def test_can_schedule_fountain_repair_while_below_decode_threshold():
    seen: dict[int, int] = {}
    assert can_schedule_fountain_repair(7, 10, seen, gen_k=48)
    seen[7] = 10
    assert can_schedule_fountain_repair(7, 10, seen, gen_k=48)
    assert not can_schedule_fountain_repair(7, 50, seen, gen_k=48)
    assert can_schedule_fountain_repair(7, 12, seen, gen_k=48)
    assert not can_schedule_fountain_repair(7, None, {7: -1}, gen_k=48)


def test_gen_still_open_respects_frontier_and_rank():
    assert not gen_still_open(5, 6, {5: 10}, [5], gen_k=48)
    assert gen_still_open(6, 6, {6: 10}, [6], gen_k=48)
    assert not gen_still_open(6, 6, {6: 50}, [], gen_k=48, decode_margin=2)


def test_fountain_open_stats_counts_deficit():
    count, avg = fountain_open_stats(
        10,
        40,
        nacks=[10, 11],
        nack_rx={10: 20, 11: 45},
        gen_k=48,
        decode_margin=2,
    )
    assert count == 2
    assert avg > 0


def test_track_fountain_gen_respects_cap():
    s: set[int] = set()
    for gid in range(1000):
        track_fountain_gen(s, gid, next_needed=900, at_cap=True)
    assert len(s) <= _FOUNTAIN_TRACK_MAX
    assert min(s) >= 900


def test_repair_storm_detected():
    assert repair_storm_detected(30_000, 10)
    assert repair_storm_detected(100, _FOUNTAIN_TRACK_MAX * 2 + 1)
    assert not repair_storm_detected(1000, 10)


def test_prune_repair_meta_bounds_dicts():
    extra = {i: i for i in range(500)}
    ts: dict[int, float] = {i: 1.0 for i in range(500)}
    rx: dict[int, int] = {i: i for i in range(500)}
    prune_repair_meta(extra, ts, rx, next_needed=400, keep=_REPAIR_META_KEEP)
    assert all(k >= 400 for k in extra)
    assert len(extra) <= _REPAIR_META_KEEP


def test_sync_repair_in_send_loop_blocks_like_old_pipeline():
    """Model the old pipeline: repair_one().result() inside every send iteration."""

    def sync_send_loop(*, gens: int, repair_s: float, send_s: float) -> float:
        t0 = time.monotonic()
        for _ in range(gens):
            time.sleep(repair_s)
            time.sleep(send_s)
        return time.monotonic() - t0

    def async_send_loop(*, gens: int, repair_s: float, send_s: float) -> float:
        t0 = time.monotonic()
        pool = ThreadPoolExecutor(max_workers=4)
        pending: list = []
        try:
            for _ in range(gens):
                pending.append(pool.submit(time.sleep, repair_s))
                time.sleep(send_s)
                still = []
                for fut in pending:
                    if fut.done():
                        fut.result()
                    else:
                        still.append(fut)
                pending = still
            for fut in pending:
                fut.result()
        finally:
            pool.shutdown(wait=True)
        return time.monotonic() - t0

    gens = 24
    repair_s = 0.006
    send_s = 0.001
    sync_t = sync_send_loop(gens=gens, repair_s=repair_s, send_s=send_s)
    async_t = async_send_loop(gens=gens, repair_s=repair_s, send_s=send_s)
    # Old pattern pays full repair latency every iteration; async overlaps encode.
    assert async_t < sync_t * 0.55
    assert sync_t > gens * repair_s * 0.9


raptorq = pytest.importorskip("raptorq")


def test_encode_worker_is_systematic_only(tmp_path: Path):
    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    path = tmp_path / "block.bin"
    path.write_bytes(bytes((i * 7) & 0xFF for i in range(block)))

    enc_cpu_s, src_bytes, wires, bootstrap = _encode_gen_worker(
        str(path),
        0,
        block,
        block,
        T,
    )
    assert src_bytes == block
    assert enc_cpu_s >= 0.0
    assert bootstrap == 0
    assert len(wires) >= K

    parsed = [GenPacket.unpack(w) for w in wires]
    esis = {p.esi for p in parsed}
    assert len(esis) == len(wires)


def test_encode_worker_process_pool_parallel(tmp_path: Path):
    """Process pool should encode multiple gens without serializing on the GIL."""
    from concurrent.futures import ProcessPoolExecutor
    import multiprocessing

    require_raptorq()
    T = 512
    K = 16
    block = K * T
    path = tmp_path / "pool.bin"
    path.write_bytes(bytes((i * 3) & 0xFF for i in range(block * 3)))

    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=3, mp_context=ctx) as pool:
        futs = [
            pool.submit(_encode_gen_worker, str(path), gid, block, block * 3, T)
            for gid in range(3)
        ]
        out = [f.result(timeout=60.0) for f in futs]

    for enc_cpu_s, src_bytes, wires, bootstrap in out:
        assert enc_cpu_s >= 0.0
        assert src_bytes == block
        assert len(wires) >= K
        assert bootstrap == 0


def test_adaptive_fountain_ticks_less_often_off_cap():
    ticks = 200
    submits = sum(
        1
        for n in range(ticks)
        if should_fountain_tick(at_cap=False, tick_n=n, every_n=_FOUNTAIN_EVERY_N)
    )
    assert submits == ticks // _FOUNTAIN_EVERY_N
    assert submits < ticks
