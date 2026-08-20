"""Regression tests for gen pipeline / async fountain bottlenecks."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tetrys_nc.gen_raptor import GenEncoder, blast_repair_budget, fountain_blast_budget, require_raptorq
from tetrys_nc.gen_xfer import (
    _FOUNTAIN_CAP_GENS,
    _FOUNTAIN_EVERY_N,
    _FOUNTAIN_TRACK_MAX,
    _FOUNTAIN_WINDOW,
    _REPAIR_META_KEEP,
    _encode_gen_worker,
    cap_fountain_gens,
    cap_fountain_send,
    client_feedback_horizon,
    compute_inflight_gen_limit,
    fountain_redundancy,
    pipeline_stressed,
    prune_fountain_gens_set,
    prune_repair_meta,
    repair_round_size,
    repair_storm_detected,
    repair_thread_limit,
    should_fountain_tick,
    should_pause_blast,
    should_track_fountain_gen,
    track_fountain_gen,
    update_adaptive_pace_bps,
)
from tetrys_nc.packets import GenPacket


def test_compute_inflight_and_feedback_horizon():
    assert compute_inflight_gen_limit(48, 1350) == 1035
    assert compute_inflight_gen_limit(192, 1350) == 258
    assert client_feedback_horizon(1035) >= 1035


def test_should_pause_blast_occupancy_and_lag():
    cap = 1035
    assert not should_pause_blast(100, 200, inflight_gen_limit=cap)
    assert should_pause_blast(cap, 10, inflight_gen_limit=cap)
    assert should_pause_blast(10, cap, inflight_gen_limit=cap)


def test_repair_thread_limit_scales_with_backlog():
    assert repair_thread_limit(
        at_cap=True, storm_active=True, nack_count=64, frontier_lag=900
    ) == 1
    lim = repair_thread_limit(
        at_cap=True, storm_active=False, nack_count=64, frontier_lag=900
    )
    assert lim >= 8


def test_pipeline_stressed_clean_vs_pressure():
    cap = 258
    threshold = int(cap * 3 / 4)  # 193
    # Healthy path: small lag, inflight below 3/4 cap.
    assert not pipeline_stressed(40, 35, inflight_gen_limit=cap)
    assert not pipeline_stressed(threshold - 1, 45, inflight_gen_limit=cap)
    assert pipeline_stressed(threshold, 10, inflight_gen_limit=cap)
    # Client lag without full inflight.
    assert pipeline_stressed(100, 65, inflight_gen_limit=cap)
    assert not pipeline_stressed(100, 64, inflight_gen_limit=cap)


def test_should_fountain_tick_skips_clean_path():
    for tick in range(20):
        assert not should_fountain_tick(stressed=False, at_cap=False, tick_n=tick)
        assert not should_fountain_tick(stressed=False, at_cap=True, tick_n=tick)


def test_update_adaptive_pace_backoff_only():
    max_bps = 100_000_000.0
    # Healthy delivery: stay at max.
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=95_000_000.0,
        cur_bps=max_bps,
        max_bps=max_bps,
        good_streak=0,
    )
    assert bps == max_bps
    assert streak == 1
    assert bad == 0
    # One lagging sample: hold pace (bad streak not reached).
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=20_000_000.0,
        cur_bps=max_bps,
        max_bps=max_bps,
        good_streak=0,
    )
    assert bps == max_bps
    assert bad == 1
    # Sustained lag: back off.
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=20_000_000.0,
        cur_bps=max_bps,
        max_bps=max_bps,
        good_streak=0,
        bad_streak=2,
    )
    assert bps < max_bps
    assert bad == 0
    # Recovery ramps toward max after good samples.
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=95_000_000.0,
        cur_bps=50_000_000.0,
        max_bps=max_bps,
        good_streak=2,
    )
    assert bps == max_bps


def test_should_fountain_tick_fountain_mode():
    for tick in range(8):
        assert should_fountain_tick(
            stressed=False,
            at_cap=True,
            tick_n=tick,
            fountain_mode=True,
        )
    off = [
        should_fountain_tick(
            stressed=False,
            at_cap=False,
            tick_n=n,
            fountain_mode=True,
            every_n=4,
        )
        for n in range(8)
    ]
    assert off == [True, False, False, False, True, False, False, False]


def test_should_fountain_tick_stressed_throttles_off_cap():
    ticks = [
        should_fountain_tick(stressed=True, at_cap=False, tick_n=n, every_n=4)
        for n in range(12)
    ]
    assert ticks == [True, False, False, False, True, False, False, False, True, False, False, False]


def test_should_fountain_tick_stressed_at_cap_every_turn():
    for tick in range(8):
        assert should_fountain_tick(stressed=True, at_cap=True, tick_n=tick)


def test_should_fountain_tick_disabled_during_storm():
    assert not should_fountain_tick(
        stressed=True, at_cap=True, tick_n=0, storm_active=True
    )


def test_cap_fountain_gens_drops_oldest():
    s = set(range(100, 200))
    cap_fountain_gens(s, next_needed=100, max_track=64)
    assert len(s) == 64
    assert min(s) == 136
    assert max(s) == 199


def test_prune_fountain_gens_set_window_and_inflight():
    s = {10, 20, 50, 80, 120}
    prune_fountain_gens_set(
        s,
        next_needed=20,
        nack_rx={50: 200},
        nacks=[],
        gen_k=192,
        sent_before=100,
        inflight_gen_limit=258,
        window=48,
    )
    assert 10 not in s
    assert 50 not in s  # enough symbols
    assert 20 in s
    assert 80 not in s  # beyond next_needed + window (68)
    assert 120 not in s


def test_should_track_fountain_gen_clean_vs_cap():
    assert not should_track_fountain_gen(500, 100, at_cap=False)
    assert should_track_fountain_gen(103, 100, at_cap=False)
    assert should_track_fountain_gen(140, 100, at_cap=True)
    assert not should_track_fountain_gen(200, 100, at_cap=True)


def test_cap_fountain_send_budget_and_deficit():
    k = 192
    budget = repair_round_size(k, 0)
    assert cap_fountain_send(k, 0, budget, None) == budget
    assert cap_fountain_send(k, budget, budget, None) == 4
    assert cap_fountain_send(k, k, 8, None) == 0
    assert cap_fountain_send(k, budget, 8, 100) > 0


def test_fountain_redundancy_and_repair_round():
    assert fountain_redundancy(0)
    assert not fountain_redundancy(8)
    assert repair_round_size(192, 0) == repair_round_size(192, 8)


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
    full: dict[int, float] = {}
    prune_repair_meta(extra, ts, full, next_needed=400, keep=_REPAIR_META_KEEP)
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


def test_encode_worker_bootstrap_in_initial_blast(tmp_path: Path):
    """Bootstrap repair must ride in the first blast, not as a later repair flood."""
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
        8,
    )
    assert src_bytes == block
    assert enc_cpu_s >= 0.0
    assert bootstrap == blast_repair_budget(K, 8)
    assert bootstrap > 0
    assert len(wires) >= K + bootstrap

    parsed = [GenPacket.unpack(w) for w in wires]
    esis = {p.esi for p in parsed}
    assert len(esis) == len(wires)
    assert max(esis) >= K + bootstrap - 1


def test_encode_worker_fountain_mode_bootstrap(tmp_path: Path):
    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    path = tmp_path / "block0.bin"
    path.write_bytes(bytes((i * 11) & 0xFF for i in range(block)))

    _enc_cpu_s, src_bytes, wires, bootstrap = _encode_gen_worker(
        str(path),
        0,
        block,
        block,
        T,
        0,
        True,
    )
    assert src_bytes == block
    assert bootstrap == fountain_blast_budget(K, 8)
    assert bootstrap > 0
    assert len(wires) >= K + bootstrap


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
            pool.submit(_encode_gen_worker, str(path), gid, block, block * 3, T, 8)
            for gid in range(3)
        ]
        out = [f.result(timeout=60.0) for f in futs]

    for enc_cpu_s, src_bytes, wires, bootstrap in out:
        assert enc_cpu_s >= 0.0
        assert src_bytes == block
        assert len(wires) >= K
        assert bootstrap == blast_repair_budget(K, 8)


def test_repair_flood_regression_old_vs_throttled_fountain():
    """Old pipeline ticked fountain every send-loop turn; hybrid throttles it."""
    inflight_cap = 258
    ticks = 200
    old_submits = 0
    new_submits = 0
    for n in range(ticks):
        incomplete = 200
        lag = 80
        stressed = pipeline_stressed(incomplete, lag, inflight_gen_limit=inflight_cap)
        assert stressed
        at_cap = incomplete >= inflight_cap
        old_submits += _FOUNTAIN_CAP_GENS
        if should_fountain_tick(stressed=stressed, at_cap=at_cap, tick_n=n):
            new_submits += _FOUNTAIN_CAP_GENS
    assert old_submits == ticks * _FOUNTAIN_CAP_GENS
    assert new_submits == (ticks // _FOUNTAIN_EVERY_N) * _FOUNTAIN_CAP_GENS
    assert new_submits < old_submits // 3


def test_bootstrap_avoids_extra_repair_round_on_clean_gen():
    """Initial blast carries bootstrap repair; repair_extra starts non-zero."""
    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    data = bytes((i * 13) & 0xFF for i in range(block))
    enc = GenEncoder(data, T, 8, systematic_only=True)
    bootstrap = blast_repair_budget(K, 8)
    extra = enc.ensure_repair(bootstrap)
    assert bootstrap > 0
    assert len(enc.packets()) >= K + bootstrap
    assert len(extra) == bootstrap
