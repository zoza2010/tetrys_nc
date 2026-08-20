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
    _HOL_REPAIR_COOLDOWN_S,
    _OH_CLEAN_HOLD_S,
    _REORDER_HOLDOFF_S,
    _REPAIR_COOLDOWN_S,
    _REPAIR_META_KEEP,
    _encode_gen_worker,
    adaptive_blast_overhead_pct,
    build_feedback_miss_bitmap,
    cap_fountain_gens,
    cap_fountain_send,
    clear_gen_deficit,
    client_feedback_horizon,
    client_feedback_interval,
    compute_inflight_gen_limit,
    echo_rtt_s,
    fountain_redundancy,
    hol_repair_cooldown_s,
    note_gen_deficit,
    pipeline_stressed,
    prune_fountain_gens_set,
    prune_repair_meta,
    repair_holdoff_ready,
    repair_round_size,
    repair_pressure,
    repair_storm_detected,
    repair_thread_limit,
    should_fountain_tick,
    should_pause_blast,
    should_track_fountain_gen,
    should_yield_blast_to_repair,
    track_fountain_gen,
    update_adaptive_pace_bps,
    update_delay_pace_bps,
)
from tetrys_nc.packets import miss_bitmap_to_nacks
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
    soft = (cap * 3) // 4
    assert should_pause_blast(soft, soft // 2, inflight_gen_limit=cap)


def test_repair_pressure_and_yield():
    cap = 1000
    assert repair_pressure(100, 50, inflight_gen_limit=cap) == 0.1
    assert repair_pressure(900, 200, inflight_gen_limit=cap) == 0.9
    assert should_yield_blast_to_repair(0.9, repair_pending=False, nack_count=0)
    assert not should_yield_blast_to_repair(
        0.1, repair_pending=False, nack_count=0
    )
    assert should_yield_blast_to_repair(
        0.5, repair_pending=True, nack_count=20
    )


def test_hol_repair_uses_short_cooldown():
    assert hol_repair_cooldown_s(10, 10, hol_miss=True) == _HOL_REPAIR_COOLDOWN_S
    assert hol_repair_cooldown_s(10, 10, hol_miss=False) == _REPAIR_COOLDOWN_S
    assert hol_repair_cooldown_s(11, 10, hol_miss=True) == _REPAIR_COOLDOWN_S
    assert 0.0 < _HOL_REPAIR_COOLDOWN_S < _REPAIR_COOLDOWN_S


def test_reorder_holdoff_gates_repair_feedback():
    deficit: dict[int, float] = {}
    t0 = 100.0
    note_gen_deficit(deficit, 7, t0)
    assert not repair_holdoff_ready(deficit, 7, t0 + 0.002, holdoff_s=0.005)
    assert repair_holdoff_ready(deficit, 7, t0 + 0.006, holdoff_s=0.005)
    clear_gen_deficit(deficit, 7)
    assert not repair_holdoff_ready(deficit, 7, t0 + 1.0, holdoff_s=0.005)
    assert _REORDER_HOLDOFF_S == 0.005


def test_adaptive_blast_overhead_scales_with_pressure():
    cap = 1000
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=0,
        nack_count=0,
        incomplete=10,
        inflight_gen_limit=cap,
        clean_s=0.0,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=0,
        nack_count=0,
        incomplete=10,
        inflight_gen_limit=cap,
        clean_s=_OH_CLEAN_HOLD_S,
        floor_pct=8,
    ) == 8
    # Steady ~100-gen pipeline on a 1k window stays at base (pressure ~0.1).
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=100,
        nack_count=0,
        incomplete=100,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=200,
        nack_count=3,
        incomplete=200,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 15
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=500,
        nack_count=20,
        incomplete=500,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 20
    assert adaptive_blast_overhead_pct(
        base_pct=0,
        frontier_lag=100,
        nack_count=50,
        incomplete=500,
        inflight_gen_limit=cap,
    ) == 0


def test_echo_rtt_and_delay_pace():
    now = 1000.0
    echo = int((now - 0.100) * 1_000_000) & 0xFFFFFFFF
    rtt = echo_rtt_s(echo, now)
    assert rtt is not None
    assert abs(rtt - 0.100) < 0.002
    assert echo_rtt_s(0, now) is None
    # Stale / stall echo (> max RTT) ignored.
    stale = int((now - 3.000) * 1_000_000) & 0xFFFFFFFF
    assert echo_rtt_s(stale, now) is None
    max_bps = 100_000_000.0
    # Standing queue → back off.
    bps, min_rtt, q = update_delay_pace_bps(
        rtt_s=0.120,
        min_rtt_s=0.080,
        cur_bps=max_bps,
        max_bps=max_bps,
    )
    assert q == pytest.approx(0.040)
    assert bps < max_bps
    assert bps >= max_bps * 0.12
    # Empty queue while probing → climb faster.
    bps2, _, q2 = update_delay_pace_bps(
        rtt_s=0.081,
        min_rtt_s=0.080,
        cur_bps=30_000_000.0,
        max_bps=max_bps,
    )
    assert q2 < 0.005
    assert bps2 > 30_000_000.0 * 1.04
    assert min_rtt > 0
    # Near ceiling → gentler climb.
    bps3, _, _ = update_delay_pace_bps(
        rtt_s=0.081,
        min_rtt_s=0.080,
        cur_bps=80_000_000.0,
        max_bps=max_bps,
    )
    assert bps3 == pytest.approx(80_000_000.0 * 1.08)
    # Thresholds scale with base RTT: 40ms queue is acceptable on a 300ms path.
    long_bps, _, long_q = update_delay_pace_bps(
        rtt_s=0.340,
        min_rtt_s=0.300,
        cur_bps=30_000_000.0,
        max_bps=max_bps,
    )
    assert long_q == pytest.approx(0.040)
    assert long_bps > 30_000_000.0
    # The same queue is severe on a low-latency path.
    short_bps, _, _ = update_delay_pace_bps(
        rtt_s=0.050,
        min_rtt_s=0.010,
        cur_bps=80_000_000.0,
        max_bps=max_bps,
    )
    assert short_bps < 80_000_000.0


def test_client_feedback_interval_is_fast_while_open():
    assert client_feedback_interval(
        fin_seen=False,
        next_needed=0,
        total_gens=100,
        open_gens=1,
        nack_count=1,
    ) == 0.02
    assert client_feedback_interval(
        fin_seen=True,
        next_needed=100,
        total_gens=100,
        open_gens=0,
        nack_count=0,
    ) == 0.05


def test_repair_thread_limit_scales_with_backlog():
    assert repair_thread_limit(
        at_cap=True,
        storm_active=True,
        nack_count=64,
        frontier_lag=900,
        inflight_gen_limit=1035,
    ) >= 2
    assert repair_thread_limit(
        at_cap=True,
        storm_active=True,
        nack_count=2,
        frontier_lag=10,
        inflight_gen_limit=1035,
    ) == 1
    lim = repair_thread_limit(
        at_cap=True,
        storm_active=False,
        nack_count=64,
        frontier_lag=900,
        inflight_gen_limit=1035,
    )
    assert lim >= 8


def test_build_feedback_miss_bitmap_gap_aware():
    done = {0, 1, 3}
    decoders = {5: object()}

    def bit_get(i: int) -> bool:
        return i in done

    bitmap = build_feedback_miss_bitmap(
        next_needed=0,
        total_gens=20,
        horizon=16,
        bit_get=bit_get,
        decoders=decoders,
        max_gid_seen=7,
    )
    missing = miss_bitmap_to_nacks(0, bitmap)
    assert 2 in missing  # gap before max_gid_seen
    assert 5 in missing  # partial decoder
    assert 6 in missing  # gap before max_gid_seen
    assert 8 not in missing  # beyond max_gid_seen, still in flight


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


def test_should_fountain_tick_disabled_during_storm_unless_cap_stressed():
    assert not should_fountain_tick(
        stressed=False, at_cap=True, tick_n=0, storm_active=True
    )
    assert should_fountain_tick(
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
