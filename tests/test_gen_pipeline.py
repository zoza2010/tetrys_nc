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
    bbr_pacing_gain,
    bbr_still_startup,
    build_feedback_miss_bitmap,
    cap_fountain_gens,
    cap_fountain_send,
    clear_gen_deficit,
    client_feedback_horizon,
    client_feedback_interval,
    compute_inflight_gen_limit,
    delay_cc_may_probe,
    echo_rtt_s,
    fountain_redundancy,
    hol_hole_gens,
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
    smooth_delay_rtt_s,
    track_fountain_gen,
    update_adaptive_pace_bps,
    update_delay_pace_bps,
    update_delivery_guard_bps,
    update_delivery_rate_pace_bps,
    update_btlbw_bps,
)
from tetrys_nc.packets import miss_bitmap_to_nacks
from tetrys_nc.packets import GenPacket


def test_compute_inflight_and_feedback_horizon():
    # Default 64 MiB WAN window (not the ATP 8 MiB experiment).
    assert compute_inflight_gen_limit(48, 1350) == 1035
    assert compute_inflight_gen_limit(192, 1350) == 258
    assert compute_inflight_gen_limit(96, 1350) == 517
    assert client_feedback_horizon(1035) >= 1035


def test_should_pause_blast_occupancy_and_lag():
    cap = 1035
    assert not should_pause_blast(100, 200, inflight_gen_limit=cap)
    assert should_pause_blast(cap, 10, inflight_gen_limit=cap)
    # HOL hole: lag at the window with almost nothing unrecovered — keep blasting.
    assert not should_pause_blast(10, cap, inflight_gen_limit=cap)
    soft = (cap * 3) // 4
    assert should_pause_blast(soft, 0, inflight_gen_limit=cap)
    assert not should_pause_blast(soft - 1, cap, inflight_gen_limit=cap)


def test_repair_pressure_and_yield():
    cap = 1000
    assert repair_pressure(100, 50, inflight_gen_limit=cap) == 0.1
    assert repair_pressure(900, 200, inflight_gen_limit=cap) == 0.9
    # Lag-only HOL is not occupancy pressure.
    assert repair_pressure(2, 500, inflight_gen_limit=cap) == 0.002
    assert should_yield_blast_to_repair(0.9, repair_pending=False, nack_count=0)
    assert not should_yield_blast_to_repair(
        0.1, repair_pending=False, nack_count=0
    )
    assert should_yield_blast_to_repair(
        0.5, repair_pending=True, nack_count=20
    )
    assert should_yield_blast_to_repair(
        0.0, repair_pending=False, nack_count=0, hol_hole=64
    )
    assert hol_hole_gens(2, 497) == 495
    assert hol_hole_gens(80, 80) == 0


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
    # Clean path pays no FEC tax after the hold (ATP source-stream).
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=0,
        nack_count=0,
        incomplete=10,
        inflight_gen_limit=cap,
        clean_s=_OH_CLEAN_HOLD_S,
    ) == 0
    # Steady ~100-gen pipeline on a 1k window stays at base (pressure ~0.1).
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=100,
        nack_count=0,
        incomplete=100,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 10
    # HOL hole must not raise FEC: lag is large, occupancy is not.
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=500,
        nack_count=0,
        incomplete=2,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 10
    # Reorder NACK bitmap on a clean WAN is not loss.
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=80,
        nack_count=40,
        incomplete=40,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 10
    # 20% of a large window used to trip mid-FEC; with 8 MiB/64 gens that
    # same fraction is a healthy pipeline, so keep base until half-full.
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=200,
        nack_count=3,
        incomplete=200,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=500,
        nack_count=20,
        incomplete=500,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 15
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=800,
        nack_count=20,
        incomplete=800,
        inflight_gen_limit=cap,
        max_pct=20,
    ) == 20
    # 8 MiB WAN window: ~30/64 gens is BDP, not a loss signal.
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=30,
        nack_count=0,
        incomplete=30,
        inflight_gen_limit=64,
        max_pct=20,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=48,
        nack_count=0,
        incomplete=48,
        inflight_gen_limit=64,
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


def test_smooth_delay_rtt_ignores_isolated_spikes():
    ewma, spike, n = smooth_delay_rtt_s(0.075, 0.0)
    assert not spike and n == 0
    assert ewma == pytest.approx(0.075)
    # Today's WAN blip: 75 → 187 ms must not pull the filter.
    ewma2, spike2, n2 = smooth_delay_rtt_s(0.187, ewma)
    assert spike2 and n2 == 1
    assert ewma2 == pytest.approx(0.075)
    ewma3, spike3, n3 = smooth_delay_rtt_s(0.080, ewma2, n2)
    assert not spike3 and n3 == 0
    assert abs(ewma3 - 0.075) < 0.01
    # Sustained jump is real delay: after 3 outliers, mix in.
    ewma_s = 0.075
    streak = 0
    for _ in range(3):
        ewma_s, is_spike, streak = smooth_delay_rtt_s(0.187, ewma_s, streak)
    assert not is_spike
    assert ewma_s > 0.075
    assert ewma_s < 0.187
    # Modest standing queue is not a spike.
    slow, spike_slow, _ = smooth_delay_rtt_s(0.095, 0.075)
    assert not spike_slow
    assert 0.075 < slow < 0.095


def test_delivery_guard_cuts_overload_not_below_floor():
    floor = 25_000_000.0  # 200 Mbit
    # 900 Mbit pace, ~13 MiB/s goodput (today's 33% loss collapse).
    cur = 112_500_000.0
    new, why = update_delivery_guard_bps(
        delivery_bps=13.4 * 1024 * 1024,
        cur_bps=cur,
        overhead_pct=10,
        min_bps=floor,
    )
    assert why == "cut_hard"
    assert new == pytest.approx(cur * 0.55)
    assert new > floor
    # After two hard cuts we sit near 300 Mbit, not 8 Mbit.
    new2, _ = update_delivery_guard_bps(
        delivery_bps=13.4 * 1024 * 1024,
        cur_bps=new,
        overhead_pct=10,
        min_bps=floor,
    )
    new3, _ = update_delivery_guard_bps(
        delivery_bps=13.4 * 1024 * 1024,
        cur_bps=new2,
        overhead_pct=10,
        min_bps=floor,
    )
    mbit = new3 * 8 / 1_000_000
    assert 200 <= mbit <= 400
    # Healthy 400 Mbit slice: delivery matches expected app → hold.
    ok, why_ok = update_delivery_guard_bps(
        delivery_bps=43.0 * 1024 * 1024,
        cur_bps=50_000_000.0,
        overhead_pct=10,
        min_bps=floor,
    )
    assert why_ok == "ok"
    assert ok == 50_000_000.0
    # Clean path: limiter at 900 Mbit but wire/app already match → do not cut.
    hold, why_hold = update_delivery_guard_bps(
        delivery_bps=75.0 * 1024 * 1024,
        cur_bps=112_500_000.0,
        overhead_pct=10,
        min_bps=floor,
        wire_bps=82.0 * 1024 * 1024,
    )
    assert why_hold == "hold"
    assert hold == 112_500_000.0
    # Same 75 MiB/s vs the 900 Mbit *target* would look like a miss without wire.
    cut_pace, why_pace = update_delivery_guard_bps(
        delivery_bps=75.0 * 1024 * 1024,
        cur_bps=112_500_000.0,
        overhead_pct=10,
        min_bps=floor,
    )
    assert why_pace == "hold" or why_pace == "ok"
    # Paused at inflight cap: tiny wire must not hide a 2 MiB/s collapse.
    cap_cut, why_cap = update_delivery_guard_bps(
        delivery_bps=2.0 * 1024 * 1024,
        cur_bps=112_500_000.0,
        overhead_pct=20,
        min_bps=floor,
        wire_bps=4.0 * 1024 * 1024,
        window_full=True,
    )
    assert why_cap == "cut_hard"
    assert cap_cut == pytest.approx(112_500_000.0 * 0.55)


def test_delay_cc_blocks_probe_on_full_pipeline():
    cap = 517
    assert delay_cc_may_probe(70, cap)
    pause = (cap * 3) // 4
    assert delay_cc_may_probe(pause - 1, cap)
    assert not delay_cc_may_probe(pause, cap)
    assert not delay_cc_may_probe(387, cap)


def test_bbr_gain_and_delivery_rate_pace():
    t0 = 1000.0
    rt = 0.080
    assert bbr_pacing_gain(t0, rt, t0) == pytest.approx(1.25)
    assert bbr_pacing_gain(t0 + rt + 0.001, rt, t0) == pytest.approx(0.75)
    assert bbr_pacing_gain(t0 + 2.5 * rt, rt, t0) == pytest.approx(1.00)
    floor = 25_000_000.0
    cap = 112_500_000.0
    samples: list[tuple[float, float]] = []
    btlbw, samples = update_btlbw_bps(
        delivery_bps=40.0 * 1024 * 1024,
        samples=samples,
        now_s=t0,
        rtprop_s=rt,
    )
    assert btlbw == pytest.approx(40.0 * 1024 * 1024)
    paced = update_delivery_rate_pace_bps(
        btlbw_bps=btlbw, max_bps=cap, min_bps=floor, gain=1.25
    )
    assert paced == pytest.approx(min(cap, btlbw * 1.25))
    drained = update_delivery_rate_pace_bps(
        btlbw_bps=btlbw, max_bps=cap, min_bps=floor, gain=0.75
    )
    assert drained < paced
    # Drain must not rewrite BtlBw: cruise returns to the high watermark.
    cruise = update_delivery_rate_pace_bps(
        btlbw_bps=btlbw, max_bps=cap, min_bps=floor, gain=1.00
    )
    assert cruise == pytest.approx(btlbw)
    # STARTUP climbs off the 200 Mbit floor even if first samples were slow.
    climb = update_delivery_rate_pace_bps(
        btlbw_bps=20.0 * 1024 * 1024, max_bps=cap, min_bps=floor, gain=2.00
    )
    assert climb >= floor * 1.90


def test_btlbw_max_filter_forgets_a_bad_start():
    rt = 0.080
    samples: list[tuple[float, float]] = []
    slow = 20.0 * 1024 * 1024
    fast = 80.0 * 1024 * 1024
    btlbw, samples = update_btlbw_bps(
        delivery_bps=slow, samples=samples, now_s=0.0, rtprop_s=rt
    )
    assert btlbw == pytest.approx(slow)
    btlbw, samples = update_btlbw_bps(
        delivery_bps=fast, samples=samples, now_s=0.20, rtprop_s=rt
    )
    assert btlbw == pytest.approx(fast)
    # 10 RTprop is only 0.8s here; the WAN floor is 3s so a delay bump
    # cannot expire a good probe. After that floor the slow start is gone.
    btlbw, samples = update_btlbw_bps(
        delivery_bps=fast, samples=samples, now_s=3.20, rtprop_s=rt
    )
    assert btlbw == pytest.approx(fast)
    assert all(r >= fast * 0.99 for _, r in samples)


def test_bbr_startup_needs_sustained_growth():
    assert bbr_still_startup(
        startup=True, btlbw_bps=40e6, round_btlbw_bps=0.0
    )
    assert bbr_still_startup(
        startup=True, btlbw_bps=40e6, round_btlbw_bps=25e6
    )
    assert not bbr_still_startup(
        startup=True, btlbw_bps=26e6, round_btlbw_bps=25e6
    )
    assert not bbr_still_startup(
        startup=False, btlbw_bps=80e6, round_btlbw_bps=25e6
    )
    # Empty pipeline: keep STARTUP even if BtlBw looks flat.
    assert bbr_still_startup(
        startup=True,
        btlbw_bps=26e6,
        round_btlbw_bps=25e6,
        occupancy=10,
        inflight_gen_limit=64,
    )
    assert not bbr_still_startup(
        startup=True,
        btlbw_bps=26e6,
        round_btlbw_bps=25e6,
        occupancy=40,
        inflight_gen_limit=64,
    )


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
    # Client lag / BDP without high unrecovered occupancy is not stress.
    assert not pipeline_stressed(100, 65, inflight_gen_limit=cap)
    assert not pipeline_stressed(100, 200, inflight_gen_limit=cap)


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
