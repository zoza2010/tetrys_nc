"""Blast rate-search: delay/policer cuts; extra/DIR is not a rate signal."""

from __future__ import annotations

import pytest

from tetrys_nc.blastcc import (
    CRUISE,
    DRAIN,
    MEASURE,
    PROBE,
    STARTUP,
    BlastCc,
    BwFilter,
    rtt_from_echo,
)
from tetrys_nc.block_packets import pack_data_packets, parse_packet, stamp_data_wires

_CAP = 850_000_000 / 8
_START = _CAP


def _echo(now: float, rtt_s: float) -> int:
    return int((now - rtt_s) * 1_000_000) & 0xFFFFFFFF


def _cc() -> BlastCc:
    return BlastCc(max_bps=_CAP, start_bps=_START)


def _feed(
    cc: BlastCc,
    now: float,
    fb: int,
    unique: int,
    rtt: float,
    extra: float = 0.0,
    decoded: int | None = None,
    sent: int = 0,
    source: int = 0,
) -> float:
    return cc.on_feedback(
        now,
        feedback_id=fb,
        unique_bytes=unique,
        decoded_bytes=unique if decoded is None else decoded,
        echo_ts_us=_echo(now, rtt),
        extra_frac=extra,
        window_full=True,
        sent_bytes=sent,
        source_bytes=source,
    )


def test_seed_is_bbr_like_fraction_not_full_cap():
    cc = _cc()
    assert cc.phase == STARTUP
    assert cc.rate == pytest.approx(_START * 0.90)


def test_rtt_from_echo_wraps_32bit():
    now = 100.5
    assert rtt_from_echo(now, _echo(now, 0.08)) == pytest.approx(0.08)
    assert rtt_from_echo(now, 0) is None


def test_on_timer_waits_for_rtt_before_climb():
    cc = _cc()
    seed = cc.rate
    now = 1.0
    for _ in range(8):
        now += 0.12
        cc.on_timer(now)
    assert cc.rate == seed


def test_on_timer_climbs_during_blocked_send():
    cc = _cc()
    cc.min_rtt = 0.08
    seed = cc.rate
    now = 1.0
    for _ in range(8):
        now += 0.12
        cc.on_timer(now)
    assert cc.rate > seed
    assert cc.rate == pytest.approx(_CAP)


def test_startup_climbs_to_cap_without_delivery_plateau():
    cc = _cc()
    now = 10.0
    unique = 0
    for i in range(1, 20):
        now += 0.20
        unique += int(_CAP * 0.20)
        _feed(cc, now, i, unique, 0.080)
    assert cc.rate >= _CAP * 0.98
    assert cc.phase in (CRUISE, PROBE)


def test_single_jitter_does_not_drain():
    cc = _cc()
    now = 10.0
    unique = 0
    for i in range(1, 12):
        now += 0.05
        unique += int(_CAP * 0.05)
        rtt = 0.095 if i == 10 else 0.080
        _feed(cc, now, i, unique, rtt)
    assert cc.phase != DRAIN
    assert cc.rate > _START * 0.90


def test_sustained_queue_enters_drain_not_loss():
    cc = _cc()
    now = 10.0
    unique = 0
    for i in range(1, 10):
        now += 0.05
        unique += 2_000_000
        _feed(cc, now, i, unique, 0.080, extra=0.50)
    for i in range(10, 40):
        now += 0.05
        unique += 2_000_000
        _feed(cc, now, i, unique, 0.140, extra=0.50)
    assert cc.phase == DRAIN


def test_drain_exits_to_cruise_when_delay_falls():
    cc = _cc()
    cc.phase = DRAIN
    cc.drain_ts = 10.0
    cc.rate = _CAP * 0.7
    cc.last_good = _CAP * 0.7
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    now = 10.2
    for i in range(1, 5):
        now += 0.05
        _feed(cc, now, i, i * 1_000_000, 0.085)
    assert cc.phase == CRUISE


def test_extra_repair_without_delay_does_not_cut_cruise():
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = _CAP * 0.9
    cc.last_good = cc.rate
    cc.cruise_ts = 100.5
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.081
    now = 101.0
    held = cc.rate
    unique = 0
    for i, dt in enumerate((0.20, 0.20), start=1):
        now = 101.0 + (i - 1) * dt if i > 1 else 101.0
        unique += int(held * dt)
        _feed(cc, now, i, unique, 0.081, extra=0.02)
    assert cc.rate == held
    assert cc.phase == CRUISE


def test_busy_extra_repair_does_not_ratchet_cruise():
    """DIR/FEC pressure is not a full pipe. Lock-850 holds 95; extra cuts fell to 70."""
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = _CAP
    cc.last_good = cc.rate
    cc.cruise_ts = 100.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 101.0
    unique = 0
    for i in range(1, 8):
        now += 0.12
        unique += int(_CAP * 0.12)
        _feed(cc, now, i, unique, 0.081, extra=0.20)
    assert cc.rate == pytest.approx(_CAP, rel=0.02)
    assert cc.phase in (CRUISE, PROBE)


def test_probe_then_revert_if_filtered_delay_rises():
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = _CAP * 0.8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    now = 5.0
    unique = int(_CAP * 0.8 * 1.0)
    _feed(cc, now, 1, unique, 0.08)
    assert cc.phase == PROBE
    base = cc.probe_base
    fb = 2
    while now < cc.probe_until:
        now += 0.05
        unique += int(base * 0.05)
        _feed(cc, now, fb, unique, 0.16)
        fb += 1
    unique += int(base * 0.05)
    _feed(cc, cc.probe_until + 0.01, fb, unique, 0.16)
    assert cc.phase == CRUISE
    assert cc.rate == base


def test_busy_extra_repair_does_not_drain_startup():
    cc = _cc()
    now = 10.0
    unique = 0
    for i in range(1, 12):
        now += 0.20
        unique += int(_CAP * 0.20)
        _feed(cc, now, i, unique, 0.080, extra=0.20)
    assert cc.phase != DRAIN
    assert cc.rate >= _CAP * 0.98


def test_oversend_measures_plateau_without_slower_history():
    """First contact with a cap: unique stays put after a trial cut."""
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = _CAP
    cc.last_good = _CAP
    cc.cruise_ts = 10.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 10.0
    unique = 0
    thin = 200_000_000 / 8
    for i in range(1, 16):
        now += 0.12
        unique += int(thin * 0.12)
        _feed(cc, now, i, unique, 0.081)
    assert cc.rate < _CAP * 0.45
    assert cc.rate == pytest.approx(thin * 1.10, rel=0.30)
    assert cc.phase in (CRUISE, MEASURE, PROBE)


def test_source_done_still_measures_policer():
    """File fits in the window: source stops, unique plateaus, repair oversend."""
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = _CAP
    cc.last_good = _CAP
    cc.cruise_ts = 10.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 10.0
    unique = 0
    sent = 0
    frozen_src = 16 * 1048576
    thin = 90_000_000 / 8
    for i in range(1, 18):
        now += 0.12
        unique += int(thin * 0.12)
        sent += int(_CAP * 0.12)
        _feed(cc, now, i, unique, 0.081, sent=sent, source=frozen_src)
    assert cc.rate < _CAP * 0.45
    assert cc.rate == pytest.approx(thin * 1.10, rel=0.35)


def test_policer_cuts_when_faster_send_buys_no_delivery():
    """Shaper: unique stays put when send rises. iid loss would track send."""
    cc = _cc()
    cc.phase = CRUISE
    cc.cruise_ts = 10.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 10.0
    unique = 0
    thin = 200_000_000 / 8
    # Sit on the cap (no oversend) so the jump to 850 is the elasticity signal.
    cc.rate = thin * 1.05
    cc.last_good = cc.rate
    fb = 1
    for _ in range(6):
        now += 0.12
        unique += int(thin * 0.12)
        _feed(cc, now, fb, unique, 0.081)
        fb += 1
    cc.rate = _CAP
    cc.last_good = _CAP
    for _ in range(8):
        now += 0.12
        unique += int(thin * 0.12)
        _feed(cc, now, fb, unique, 0.081)
        fb += 1
    assert cc.rate < _CAP * 0.45
    assert cc.rate == pytest.approx(thin * 1.10, rel=0.25)


def test_lossy_fat_pipe_does_not_look_like_policer():
    """23% iid: unique is 0.77× send but still rises when send rises."""
    cc = _cc()
    cc.phase = CRUISE
    cc.cruise_ts = 10.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 10.0
    unique = 0
    fb = 1
    cc.rate = _CAP
    cc.last_good = _CAP
    for _ in range(14):
        now += 0.12
        unique += int(cc.rate * 0.77 * 0.12)
        _feed(cc, now, fb, unique, 0.081)
        fb += 1
    assert cc.rate == pytest.approx(_CAP, rel=0.08)


def test_unique_cliff_is_window_stall_not_a_cap():
    """HOL/uncoverable: unique was high, then stops. Do not ratchet to 8 Mbit."""
    cc = _cc()
    cc.phase = CRUISE
    cc.cruise_ts = 10.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 10.0
    unique = 0
    fb = 1
    cc.rate = _CAP
    cc.last_good = _CAP
    for _ in range(10):
        now += 0.12
        unique += int(_CAP * 0.90 * 0.12)
        _feed(cc, now, fb, unique, 0.081)
        fb += 1
    held = cc.rate
    for _ in range(12):
        now += 0.12
        unique += int(8_000_000 / 8 * 0.12)
        _feed(cc, now, fb, unique, 0.081)
        fb += 1
    assert cc.rate == pytest.approx(held, rel=0.12)
    assert cc.rate > _CAP * 0.70


def test_measure_restores_if_unique_holds_but_decode_is_stuck():
    """Uncoverable window: unique looks like a thin cap, decoded does not recover."""
    cc = _cc()
    cc.phase = CRUISE
    cc.cruise_ts = 10.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 10.0
    unique = 0
    fb = 1
    cc.rate = _CAP
    cc.last_good = _CAP
    thin = 200_000_000 / 8
    stuck = 0
    for _ in range(20):
        now += 0.12
        unique += int(thin * 0.12)
        _feed(cc, now, fb, unique, 0.081, decoded=stuck)
        fb += 1
    assert cc.last_good == pytest.approx(_CAP)
    assert cc.rate == pytest.approx(_CAP, rel=0.12)


def test_window_crawl_at_one_third_start_is_not_a_fat_plateau():
    """dirty-wan HOL: unique ~0.38×850 with no decode must not lock ~350 Mbit."""
    cc = _cc()
    cc.phase = CRUISE
    cc.cruise_ts = 10.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.min_rtt = 0.08
    now = 10.0
    unique = 0
    fb = 1
    cc.rate = _CAP
    cc.last_good = _CAP
    crawl = int(_CAP * 0.38)
    for _ in range(20):
        now += 0.12
        unique += int(crawl * 0.12)
        _feed(cc, now, fb, unique, 0.081, decoded=0)
        fb += 1
    assert cc.last_good == pytest.approx(_CAP)
    assert cc.rate == pytest.approx(_CAP, rel=0.12)


def test_cruise_ceiling_does_not_repin_to_start_after_cut():
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = 400_000_000 / 8
    cc.last_good = cc.rate
    cc.min_bps = 250_000_000 / 8
    cc.bw.observe(10 * 1048576)
    cc.bw.observe(11 * 1048576)
    cc.bw.observe(12 * 1048576)
    ceiling = cc._rate_ceiling()
    assert ceiling < _START
    assert ceiling >= cc.rate


def test_probe_reverts_if_faster_send_buys_no_delivery():
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = _CAP * 0.8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    now = 5.0
    unique = int(_CAP * 0.8)
    _feed(cc, now, 1, unique, 0.08)
    assert cc.phase == PROBE
    base = cc.probe_base
    fb = 2
    while now < cc.probe_until:
        now += 0.05
        unique += int(base * 0.05)
        _feed(cc, now, fb, unique, 0.081)
        fb += 1
    unique += int(base * 0.05)
    _feed(cc, cc.probe_until + 0.01, fb, unique, 0.081)
    assert cc.phase == CRUISE
    assert cc.rate == pytest.approx(base)


def test_search_seeds_at_stall_floor_not_channel_guess():
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    assert cc.rate == pytest.approx(start)
    assert cc.phase == STARTUP


def test_startup_climbs_above_start_toward_search_cap():
    start = 8_000_000 / 8
    cap = 10_000_000_000 / 8
    cc = BlastCc(max_bps=cap, start_bps=start, min_bps=start)
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    now = 1.0
    unique = 0
    for i in range(1, 24):
        now += 0.20
        unique += int(cc.rate * 0.20)
        _feed(cc, now, i, unique, 0.080)
    assert cc.rate > 850_000_000 / 8
    assert cc.rate <= cap


def test_startup_does_not_pin_ceiling_to_start():
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    # One step is allowed; the 10 Gbit search cap stays closed until unique.
    assert cc._rate_ceiling() > start
    assert cc._rate_ceiling() < start * 4


def test_startup_does_not_open_search_cap_before_unique():
    start = 8_000_000 / 8
    cap = 10_000_000_000 / 8
    cc = BlastCc(max_bps=cap, start_bps=start, min_bps=start)
    cc.min_rtt = 0.08
    now = 1.0
    for _ in range(8):
        now += 0.12
        cc.on_timer(now)
    assert cc.rate < 200_000_000 / 8


def test_startup_ceiling_tracks_unique_not_burst_times_gain():
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    burst = 900_000_000 / 8
    cc.bw.observe(burst * 0.90)
    cc.bw.observe(burst * 0.95)
    cc.bw.observe(burst)
    assert cc._startup_ceiling() < burst * 1.50
    assert cc._startup_ceiling() < 2_000_000_000 / 8


def test_overshoot_then_unique_cliff_is_still_oversend():
    """Spain: 2.8 Gbit blast, unique cliffs. That is overshoot, not HOL."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = STARTUP
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.rate = 2_800_000_000 / 8
    cc.last_send_rate = cc.rate
    cc.last_unique = 8 * 1048576
    cc.last_delivery = 40_000_000 / 8
    absorbed = 900_000_000 / 8
    cc.bw.observe(absorbed * 0.9)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    assert cc._pipe_oversend() is True


def test_probe_can_raise_above_start_without_channel_cap():
    start = 850_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start)
    cc.phase = CRUISE
    cc.rate = start
    cc.last_good = start
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    _feed(cc, 3.0, 1, 1_000_000, 0.08)
    assert cc.phase == PROBE
    assert cc.rate > start
    assert cc.rate <= start * 1.25


def test_bw_filter_ignores_single_spike():
    bw = BwFilter()
    bw.observe(100.0)
    bw.observe(110.0)
    bw.observe(10_000.0)
    assert bw.max_bw == pytest.approx(110.0)


def test_short_ack_interval_does_not_raise_bw():
    cc = _cc()
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    _feed(cc, 1.00, 1, 0, 0.08)
    _feed(cc, 1.01, 2, 50_000_000, 0.08)
    assert cc.bw.max_bw is None
    assert cc.last_ts == pytest.approx(1.00)


def test_short_acks_accumulate_until_min_rtt_window():
    cc = _cc()
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    _feed(cc, 1.00, 1, 0, 0.08)
    _feed(cc, 1.01, 2, 1_000_000, 0.08)
    _feed(cc, 1.05, 3, 4_000_000, 0.08)
    assert cc.last_ts == pytest.approx(1.05)
    assert cc.bw.samples


def test_stamp_overwrites_encode_age():
    wires = pack_data_packets(1, 2, [b"x" * 10], 0, 1)
    stamp_data_wires(wires, 99)
    pkt = parse_packet(bytes(wires[0]))
    assert pkt.send_ts_us == 99


def test_window_pause_high_rtt_does_not_drain_startup():
    """Spain run 4: encode pause + unique cliff looked like a standing queue."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = STARTUP
    now = 10.0
    unique = 0
    fat = 900_000_000 / 8
    for i in range(1, 12):
        now += 0.12
        unique += int(fat * 0.12)
        _feed(cc, now, i, unique, 0.080, sent=int(fat * (now - 10.0)))
    held = cc.rate
    assert held > 400_000_000 / 8
    paused_sent = int(fat * (now - 10.0))
    for i in range(12, 24):
        now += 0.12
        unique += int(fat * 0.12 * 0.20)
        _feed(cc, now, i, unique, 0.160, sent=paused_sent)
    assert cc.phase != DRAIN
    assert cc.rate > held * 0.70


def test_drain_does_not_compound_below_unique():
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    absorbed = 1_000_000_000 / 8
    cc.rate = 1_347_000_000 / 8
    cc.last_good = cc.rate
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    first = cc._drain_target()
    # Floor is filtered unique × 1.10, not another 0.75× off the limiter.
    assert first >= absorbed * 0.95 * 1.10 * 0.99
    cc.rate = first
    second = cc._drain_target()
    assert second >= first * 0.99


def test_probe_keeps_when_unique_rose_even_if_decode_flat():
    cc = _cc()
    cc.phase = CRUISE
    base = _CAP * 0.5
    cc.rate = base
    cc.last_good = base
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    now = 5.0
    unique = int(base)
    decoded = unique
    _feed(cc, now, 1, unique, 0.08, decoded=decoded)
    assert cc.phase == PROBE
    fb = 2
    while now < cc.probe_until:
        now += 0.05
        unique += int(cc.rate * 0.05)
        _feed(cc, now, fb, unique, 0.081, decoded=decoded)
        fb += 1
    unique += int(cc.rate * 0.05)
    _feed(cc, cc.probe_until + 0.01, fb, unique, 0.081, decoded=decoded)
    assert cc.phase == CRUISE
    assert cc.rate > base
