"""Blast rate-search: delay/policer cuts; extra/DIR is not a rate signal."""

from __future__ import annotations

import pytest

from tetrys_nc.blastcc import (
    CRUISE,
    DRAIN,
    MEASURE,
    PATH_DROPPER,
    PATH_HOL,
    PATH_IID,
    PATH_QUEUE,
    PROBE,
    STARTUP,
    BlastCc,
    BwFilter,
    _STARTUP_GAIN,
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
    path_loss: float | None = None,
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
        path_loss=path_loss,
    )


def test_seed_is_bbr_like_fraction_not_full_cap():
    cc = _cc()
    assert cc.phase == STARTUP
    assert cc.rate == pytest.approx(_START * 0.90)


def test_startup_gain_is_bbr_until_fat():
    """Empty pipe: 2.89× BtlBw. After unique tracks send, walk 1.25×."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.min_rtt = 0.08
    filling = 200_000_000 / 8
    cc.rate = filling * 2.0
    cc.last_send_rate = cc.rate
    cc.last_delivery = filling
    for x in (filling * 0.95, filling, filling * 1.02):
        cc.bw.observe(x)
    assert cc._fat_pipe() is False
    assert cc._startup_search_cap(cc.max_bps) >= filling * 2.5
    assert cc._startup_gain() == pytest.approx(_STARTUP_GAIN)


def test_fat_startup_ceiling_walks_not_jump():
    """Fat pipe walks 1.25×. Dirty stall 1.15×. No second 2.89× into a dropper."""
    start = 850_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.min_rtt = 0.08
    burst = 900_000_000 / 8
    cc.rate = burst
    cc.last_send_rate = burst
    cc.last_delivery = burst
    for x in (burst * 0.95, burst, burst * 1.02):
        cc.bw.observe(x)
    assert cc._fat_pipe() is True
    assert cc._startup_gain() == pytest.approx(1.25)
    cc.bw_stall_n = 3
    assert cc._startup_search_cap(cc.max_bps) <= burst * 1.30
    assert cc._raise_ceiling() <= burst * 1.30
    cc.phase = STARTUP
    cc.rate = burst * 1.12
    cc.last_send_rate = cc.rate
    cc.was_fat = True
    assert cc._clean_pipe() is False
    assert cc._startup_search_cap(cc.max_bps) <= burst * 1.20
    assert cc._raise_ceiling() <= burst * 1.20


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


def test_on_timer_does_not_invent_rate_before_unique():
    """Spain 8 MiB/s: 1.25^n without unique walked 8 → 2.7 Gbit."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.min_rtt = 0.08
    now = 1.0
    for _ in range(40):
        now += 0.08
        cc.on_timer(now)
    assert cc.rate < 20_000_000 / 8


def test_on_timer_climbs_after_unique_while_send_blocked():
    cc = _cc()
    cc.min_rtt = 0.08
    now = 1.0
    unique = 0
    for i in range(1, 5):
        now += 0.20
        unique += int(_CAP * 0.20)
        _feed(cc, now, i, unique, 0.080)
    for _ in range(8):
        now += 0.12
        cc.on_timer(now)
    assert cc.rate == pytest.approx(_CAP, rel=0.02)


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


def test_cruise_fills_when_one_block_in_flight():
    """Spain: cruise at 22 Mbit, open=1, 40s of probe_revert. Not C."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.rate = 22_000_000 / 8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    now = 5.0
    unique = 1_000_000
    for i in range(1, 8):
        now += 0.20
        unique += int(max(cc.rate, 1) * 0.20)
        _feed(cc, now, i, unique, 0.081)
    assert cc.rate > 80_000_000 / 8


def test_starved_pipe_does_not_qdelay_drain():
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.rate = 22_000_000 / 8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.04
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    now = 5.0
    unique = 1_000_000
    for i in range(1, 10):
        now += 0.12
        unique += 200_000
        _feed(cc, now, i, unique, 0.160)
    assert cc.phase != DRAIN


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
    assert cc.rate >= held
    assert cc.phase in (CRUISE, PROBE)


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
    assert cc.rate == pytest.approx(thin * 1.10, rel=0.35)


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


def test_measure_keeps_cut_if_unique_collapsed():
    """Spain: cut 1348→1099, unique cliffs, restore put 1348 back and died."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    absorbed = 999_000_000 / 8
    cc.rate = 1_348_000_000 / 8
    cc.last_good = start
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    cc.last_delivery = absorbed
    now = 10.0
    cc._start_measure(now)
    trial = cc.rate
    cc.last_delivery = 266_000_000 / 8
    cc.measure_until = now
    cc._finish_measure(now)
    assert cc.rate == pytest.approx(trial, rel=0.02)
    assert cc.rate < 1_200_000_000 / 8


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
    for _ in range(30):
        now += 0.12
        cc.on_timer(now)
    assert cc.rate < 20_000_000 / 8


def test_startup_ceiling_tracks_unique_not_burst_times_gain():
    """GRO/ACK 2 Gbit unique is not C. Ceiling follows filtered bw, 2.89×."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    burst = 900_000_000 / 8
    cc.bw.observe(burst * 0.90)
    cc.bw.observe(burst * 0.95)
    cc.bw.observe(burst)
    cc.last_delivery = 2_000_000_000 / 8
    cap = cc._startup_ceiling()
    assert cap <= burst * _STARTUP_GAIN * 1.02
    assert cap < (2_000_000_000 / 8) * _STARTUP_GAIN


def test_first_window_unique_is_not_oversend():
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = STARTUP
    cc.rate = 1_357_000_000 / 8
    cc.last_unique = 8 * 1048576
    cc.last_delivery = 13_000_000 / 8
    assert cc._pipe_oversend() is False


def test_wrecked_window_cuts_off_stale_gigabit_filter():
    """Spain: pace 1091, unq=50, bw=992, close=13%, ov stayed 0."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rate = 1_091_000_000 / 8
    cc.last_good = cc.rate
    cc.recv_lag = True
    cc.last_unique = 32 * 1048576
    cc.last_delivery = 50_000_000 / 8
    cc.cliff_n = 4
    absorbed = 992_000_000 / 8
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    assert cc._pipe_oversend() is True
    aimed = cc._aim_delivery()
    assert aimed < 300_000_000 / 8
    assert aimed > 100_000_000 / 8
    cc._cut_to_delivery(1.0)
    assert cc.rate < 300_000_000 / 8
    assert cc.rate > 100_000_000 / 8
    # After the cut, do not measure back to 8. Starved fill must climb.
    cc.rate = 20_000_000 / 8
    cc.last_delivery = 7_000_000 / 8
    cc.recv_lag = True
    assert cc._pipe_oversend() is False
    assert cc._pipe_starved() is True
    assert cc._rate_ceiling() > 80_000_000 / 8


def test_one_lag_spike_after_climb_does_not_cut_to_fill():
    """Spain: climbed 218→830, one unq=114 / lag, then measure slammed 200."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rate = 830_000_000 / 8
    cc.last_good = cc.rate
    cc.recv_lag = True
    cc.last_unique = 32 * 1048576
    cc.last_delivery = 114_000_000 / 8
    cc.cliff_n = 1
    absorbed = 861_000_000 / 8
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    assert cc._pipe_oversend() is False
    aimed = cc._aim_delivery()
    assert aimed > 400_000_000 / 8
    cc._cut_to_delivery(1.0)
    assert cc.rate > 400_000_000 / 8


def test_follow_pace_then_unique_cliff_is_oversend():
    """Spain after 2.7 Gbit cap: 1290 on ~900 unique, then cliff, ov stayed 0."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = STARTUP
    cc.min_rtt = 0.08
    absorbed = 947_000_000 / 8
    cc.rate = 1_290_000_000 / 8
    cc.last_send_rate = cc.rate
    cc.last_unique = 16 * 1048576
    cc.last_delivery = 200_000_000 / 8
    cc.bw.observe(absorbed * 0.9)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    assert cc._pipe_oversend() is True


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
    cc.last_unique = 16 * 1048576
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


def _mbit(bps: float) -> float:
    return bps * 8 / 1_000_000


def _search_cc() -> BlastCc:
    start = 8_000_000 / 8
    return BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)


def _simulate_path(
    cc: BlastCc,
    *,
    c_bps: float,
    rtt: float = 0.08,
    loss: float = 0.0,
    dt: float = 0.12,
    duration: float = 16.0,
    mode: str = "policer",
    hol_start: float | None = None,
    hol_dur: float = 2.0,
) -> list[float]:
    """Closed-loop path: delivery follows send against C / iid / HOL."""
    now = 1.0
    unique = 0
    decoded = 0
    sent = 0
    fb = 1
    rates: list[float] = []
    t0 = now
    while now - t0 < duration:
        now += dt
        send = max(cc.rate, 1.0)
        elapsed = now - t0
        hol = (
            hol_start is not None
            and hol_start <= elapsed < hol_start + hol_dur
        )
        if hol:
            delivered = min(send, c_bps) * 0.08
            rtt_now = rtt * 1.6
            dec = 0.0
        elif mode == "iid":
            delivered = send * (1.0 - loss)
            rtt_now = rtt
            dec = delivered
        elif mode == "soft":
            # Spain: ~0% at C, ~5% at 1.25×C (800→1000), queue empty.
            if send <= c_bps:
                delivered = send * (1.0 - loss)
            else:
                delivered = c_bps + (send - c_bps) * 0.74
            rtt_now = rtt
            dec = delivered
        elif mode == "queue":
            delivered = min(send, c_bps) * (1.0 - loss)
            excess = max(0.0, send / max(c_bps, 1.0) - 1.0)
            rtt_now = rtt + min(0.080, excess * rtt)
            dec = delivered
        else:
            delivered = min(send, c_bps) * (1.0 - loss)
            rtt_now = rtt
            dec = delivered
        unique += max(1, int(delivered * dt))
        decoded += max(0, int(dec * dt))
        sent += max(1, int(send * dt))
        if hol:
            # First-flight of already-trained blocks stays quiet; unique cliffs.
            path_loss = 0.0
        else:
            path_loss = 0.0 if send <= 0 else max(0.0, 1.0 - delivered / send)
        _feed(
            cc,
            now,
            fb,
            unique,
            rtt_now,
            decoded=decoded,
            sent=sent,
            source=sent,
            path_loss=path_loss,
        )
        fb += 1
        rates.append(cc.rate)
    return rates


def test_closed_loop_fat_pipe_settles_near_c_without_sawtooth():
    """Spain UDP C ~800 Mbit, ~0% loss. Must sit there, not 200↔800."""
    cc = _search_cc()
    c = 800_000_000 / 8
    rates = _simulate_path(cc, c_bps=c, mode="policer", duration=18.0)
    tail = rates[-max(8, len(rates) // 5) :]
    med = sorted(tail)[len(tail) // 2]
    lo, hi = min(tail), max(tail)
    assert _mbit(med) > 720.0, _mbit(med)
    assert _mbit(med) < 1100.0, _mbit(med)
    assert hi < lo * 2.0, (_mbit(lo), _mbit(hi))
    assert cc._raise_ceiling() < 1_200_000_000 / 8, _mbit(cc._raise_ceiling())


def test_loss_grew_on_dropper_knee_not_stable_iid():
    cc = _search_cc()
    fat = 900_000_000 / 8
    cc.send_samples = [
        (fat * 0.90, fat * 0.90),
        (fat * 0.95, fat * 0.95),
        (fat, fat),
    ]
    cc.knee_bps = fat
    cc.loss_knee_n = 2
    cc.phase = CRUISE
    cc.rate = 1_000_000_000 / 8
    cc.last_send_rate = cc.rate
    cc.last_delivery = 948_000_000 / 8
    cc.last_path_loss = 0.05
    cc.path_samples = [
        (fat * 0.90, 0.0),
        (fat * 0.95, 0.005),
        (fat, 0.01),
    ]
    assert cc._dropper_knee_sample() is True
    assert cc._loss_grew() is True
    knee = cc._knee_bps()
    assert knee is not None
    assert 850.0 < _mbit(knee) < 950.0
    iid = _search_cc()
    frac = 0.77
    iid.send_samples = [
        (400_000_000 / 8, 400_000_000 / 8 * frac),
        (500_000_000 / 8, 500_000_000 / 8 * frac),
        (600_000_000 / 8, 600_000_000 / 8 * frac),
    ]
    iid.rate = 700_000_000 / 8
    iid.last_send_rate = iid.rate
    iid.last_delivery = iid.rate * frac
    assert iid._dropper_knee_sample() is False
    assert iid._loss_grew() is False


def test_loss_grew_ignores_encode_lag_not_dropper():
    """Spain 2026-09-13: startup_loss_knee snd=764 unq=415 is lag, not C."""
    cc = _search_cc()
    cc.knee_bps = 800_000_000 / 8
    cc.was_fat = True
    cc.loss_knee_n = 8
    cc.rate = 910_000_000 / 8
    cc.last_send_rate = 764_000_000 / 8
    cc.last_delivery = 415_000_000 / 8
    for x in (700_000_000 / 8, 750_000_000 / 8, 799_000_000 / 8):
        cc.bw.observe(x)
    assert cc._dropper_knee_sample() is False
    assert cc._loss_grew() is False


def test_dropper_knee_ignores_unique_lag_when_first_flight_quiet():
    """WAN: snd=267 unq=240 path=0 is ACK lag, not C=40."""
    cc = _search_cc()
    cc.phase = STARTUP
    cc.was_fat = True
    cc.knee_bps = 40_000_000 / 8
    cc.rate = 40_000_000 / 8
    cc.last_send_rate = 267_000_000 / 8
    cc.last_delivery = 240_000_000 / 8
    cc.last_path_loss = 0.0
    cc.send_samples = [
        (30_000_000 / 8, 30_000_000 / 8),
        (35_000_000 / 8, 35_000_000 / 8),
        (40_000_000 / 8, 40_000_000 / 8),
    ]
    assert cc._dropper_knee_sample() is False
    assert cc._path_loss_grew() is False


def test_dropper_knee_needs_first_flight():
    """Fixed FEC used to leave path_loss None; unique lag is not C."""
    cc = _search_cc()
    cc.phase = STARTUP
    cc.rate = 66_000_000 / 8
    cc.last_send_rate = 210_000_000 / 8
    cc.last_delivery = 194_000_000 / 8
    cc.send_samples = [
        (30_000_000 / 8, 30_000_000 / 8),
        (40_000_000 / 8, 40_000_000 / 8),
        (50_000_000 / 8, 50_000_000 / 8),
    ]
    assert cc._dropper_knee_sample() is False
    assert cc._path_class() != PATH_DROPPER


def test_should_lock_c_when_unique_stalled_and_send_ahead():
    cc = _search_cc()
    c = 800_000_000 / 8
    cc.phase = STARTUP
    cc.was_fat = True
    cc.bw_stall_n = 3
    cc.rate = c * 1.15
    cc.last_send_rate = cc.rate
    cc.last_delivery = c
    for x in (c * 0.95, c * 0.98, c):
        cc.bw.observe(x)
    assert cc._clean_pipe() is False
    assert cc._should_lock_c() is True
    cc.last_delivery = cc.last_send_rate
    assert cc._clean_pipe() is True
    assert cc._should_lock_c() is True


def test_lock_knee_sits_on_last_clean_send_when_unique_fell():
    """Spain: bw=1017, unq=845, snd=1182. Sit on last 0.99 send, not unique."""
    cc = _search_cc()
    cc.phase = CRUISE
    cc.was_fat = True
    knee = 900_000_000 / 8
    cc.knee_bps = knee
    cc.rate = 1_170_000_000 / 8
    cc.last_send_rate = 1_182_000_000 / 8
    cc.last_delivery = 845_000_000 / 8
    cc.last_good = cc.rate
    for x in (1_000_000_000 / 8, 1_010_000_000 / 8, 1_017_000_000 / 8):
        cc.bw.observe(x)
    cc._lock_knee(0.0)
    assert cc.rate == pytest.approx(knee)
    assert cc.saw_loss_knee is True


def test_lock_knee_sits_on_clean_latch_when_unique_tracks_send():
    """Soft dropper: unique follows send to 1.4×C. Sit on last 0.99, not unique."""
    cc = _search_cc()
    cc.phase = CRUISE
    cc.was_fat = True
    knee = 779_000_000 / 8
    cc.knee_bps = knee
    cc.rate = 1_233_000_000 / 8
    cc.last_send_rate = cc.rate
    cc.last_delivery = 1_121_000_000 / 8
    cc.last_good = cc.rate
    u = cc.last_delivery
    for x in (u * 0.95, u * 0.98, u):
        cc.bw.observe(x)
    cc._lock_knee(0.0)
    assert cc.rate == pytest.approx(knee)
    assert cc.saw_loss_knee is True


def test_clean_pipe_uses_first_flight_not_unique_lag():
    """HOL unique dip with quiet first-flight is not a dirty path."""
    cc = _search_cc()
    send = 900_000_000 / 8
    cc.rate = send
    cc.last_send_rate = send
    cc.last_delivery = 760_000_000 / 8
    cc.last_path_loss = 0.01
    assert cc._clean_pipe() is True
    cc.last_path_loss = 0.05
    assert cc._clean_pipe() is False


def test_path_loss_grew_locks_last_clean_send():
    """First-flight 0% at 900 → 5% at 1000: lock 900, not unique 948."""
    cc = _search_cc()
    clean = 900_000_000 / 8
    cc.phase = CRUISE
    cc.was_fat = True
    cc.knee_bps = clean
    cc.rate = 1_000_000_000 / 8
    cc.last_send_rate = cc.rate
    cc.last_delivery = 948_000_000 / 8
    cc.last_path_loss = 0.05
    cc.path_samples = [
        (clean * 0.90, 0.0),
        (clean * 0.95, 0.005),
        (clean, 0.01),
    ]
    assert cc._path_loss_grew() is True
    cc.loss_knee_n = 2
    cc._lock_knee(0.0)
    assert cc.rate == pytest.approx(clean)


def test_flat_coverable_path_loss_is_fec_not_c_lock():
    """~20% iid at every rate: FEC, not a dropper knee."""
    cc = _search_cc()
    cc.phase = CRUISE
    cc.rate = 700_000_000 / 8
    cc.last_send_rate = cc.rate
    cc.last_delivery = cc.rate * 0.80
    cc.last_path_loss = 0.20
    cc.path_samples = [
        (400_000_000 / 8, 0.20),
        (500_000_000 / 8, 0.19),
        (600_000_000 / 8, 0.21),
    ]
    assert cc._path_loss_grew() is False
    assert cc._dropper_knee_sample() is False


def test_lock_knee_does_not_latch_dropper_fringe():
    """Spain 91 MiB/s: 1040 Mbit at 5% first-flight is not last-clean."""
    cc = _search_cc()
    cc.phase = CRUISE
    cc.was_fat = True
    clean = 900_000_000 / 8
    cc.knee_bps = clean
    cc.rate = 1_136_000_000 / 8
    cc.last_send_rate = 1_040_000_000 / 8
    cc.last_delivery = 1_030_000_000 / 8
    cc.last_path_loss = 0.05
    cc.last_good = cc.rate
    for x in (1_000_000_000 / 8, 1_001_000_000 / 8, 1_001_000_000 / 8):
        cc.bw.observe(x)
    cc._note_knee()
    assert cc.knee_bps == pytest.approx(clean)
    cc.last_send_rate = 1_329_000_000 / 8
    cc.last_delivery = 952_000_000 / 8
    cc._lock_knee(0.0)
    assert cc.rate == pytest.approx(clean)
    assert cc.rate < 1_000_000_000 / 8


def test_locked_ceiling_does_not_keep_dropper_walk():
    """After lock, clip must sit on the knee, not the 1039 Mbit walk."""
    cc = _search_cc()
    clean = 900_000_000 / 8
    cc.phase = CRUISE
    cc.saw_loss_knee = True
    cc.dropper_confirmed = True
    cc.knee_bps = clean
    cc.rate = 1_039_000_000 / 8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc._hold_locked_c()
    cc.rate = cc._clip(cc.rate)
    assert cc.rate == pytest.approx(clean)


def test_path_class_dropper_vs_iid_vs_hol():
    dropper = _search_cc()
    clean = 900_000_000 / 8
    dropper.rate = 1_000_000_000 / 8
    dropper.last_send_rate = dropper.rate
    dropper.last_delivery = 948_000_000 / 8
    dropper.last_path_loss = 0.05
    dropper.path_samples = [
        (clean * 0.90, 0.0),
        (clean * 0.95, 0.005),
        (clean, 0.01),
    ]
    assert dropper._path_class() == PATH_DROPPER

    iid = _search_cc()
    iid.rate = 700_000_000 / 8
    iid.last_send_rate = iid.rate
    iid.last_delivery = iid.rate * 0.80
    iid.last_path_loss = 0.20
    iid.path_samples = [
        (400_000_000 / 8, 0.20),
        (500_000_000 / 8, 0.19),
        (600_000_000 / 8, 0.21),
    ]
    iid.send_samples = [
        (400_000_000 / 8, 400_000_000 / 8 * 0.80),
        (500_000_000 / 8, 500_000_000 / 8 * 0.80),
        (600_000_000 / 8, 600_000_000 / 8 * 0.80),
    ]
    for x in (iid.last_delivery * 0.95, iid.last_delivery, iid.last_delivery):
        iid.bw.observe(x)
    assert iid._path_class() == PATH_IID

    hol = _search_cc()
    hol.rate = 900_000_000 / 8
    hol.last_send_rate = hol.rate
    hol.last_delivery = 50_000_000 / 8
    hol.last_path_loss = 0.0
    absorbed = 900_000_000 / 8
    for x in (absorbed * 0.95, absorbed, absorbed * 1.02):
        hol.bw.observe(x)
    assert hol._unique_cliff() is True
    assert hol._path_class() == PATH_HOL


def test_standing_queue_drains_after_dropper_lock():
    """qdelay stays a real-queue detector even after last-clean lock."""
    cc = _search_cc()
    knee = 900_000_000 / 8
    cc.phase = CRUISE
    cc.saw_loss_knee = True
    cc.knee_bps = knee
    cc.rate = knee
    cc.last_good = knee
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.last_delivery = knee
    cc.last_send_rate = knee
    cc.last_qdelay = 0.050
    cc.high_delay_n = 4
    assert cc._standing_queue() is True
    assert cc._path_class() == PATH_QUEUE


def test_lock_knee_discards_startup_leftover_latch():
    """WAN: 0.99 latch at 10 Mbit vs unique 260. Must not freeze at 10."""
    cc = _search_cc()
    cc.phase = CRUISE
    cc.was_fat = True
    cc.knee_bps = 10_000_000 / 8
    cc.rate = 10_000_000 / 8
    cc.last_good = cc.rate
    cc.last_send_rate = 276_000_000 / 8
    cc.last_delivery = 260_000_000 / 8
    cc.last_path_loss = 0.0
    for x in (200_000_000 / 8, 210_000_000 / 8, 221_000_000 / 8):
        cc.bw.observe(x)
    cc._lock_knee(0.0)
    assert cc.rate > 200_000_000 / 8
    assert cc.rate == pytest.approx(cc.last_send_rate)


def test_lock_knee_discards_mid_ramp_leftover_latch():
    """WAN static-8: 0.99 latch at 66 Mbit vs unique 194. Must keep climbing."""
    cc = _search_cc()
    cc.phase = CRUISE
    cc.was_fat = True
    cc.knee_bps = 66_000_000 / 8
    cc.rate = 66_000_000 / 8
    cc.last_good = cc.rate
    cc.last_send_rate = 210_000_000 / 8
    cc.last_delivery = 194_000_000 / 8
    cc.last_path_loss = 0.0
    for x in (180_000_000 / 8, 190_000_000 / 8, 194_000_000 / 8):
        cc.bw.observe(x)
    cc._lock_knee(0.0)
    assert cc.rate > 150_000_000 / 8
    assert cc.rate == pytest.approx(cc.last_send_rate)


def test_c_lock_holds_through_hol_unique_cliff():
    """After lock, HOL unique dip must not wrecked_cut toward ~50 MiB/s."""
    cc = _search_cc()
    knee = 857_000_000 / 8
    cc.phase = CRUISE
    cc.was_fat = True
    cc.saw_loss_knee = True
    cc.knee_bps = knee
    cc.rate = knee
    cc.last_good = knee
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.recv_lag = True
    cc.last_unique = 32 * 1048576
    cc.last_delivery = 50_000_000 / 8
    cc.last_send_rate = knee
    cc.cliff_n = 4
    for x in (knee * 0.95, knee, knee * 1.02):
        cc.bw.observe(x)
    cc._recover_wrecked(1.0)
    assert cc.saw_loss_knee is True
    assert cc.rate == pytest.approx(knee)
    cc._cut_to_delivery(1.2)
    assert cc.rate == pytest.approx(knee)
    cc.on_feedback(
        5.0,
        feedback_id=1,
        unique_bytes=int(50_000_000 / 8),
        decoded_bytes=0,
        echo_ts_us=_echo(5.0, 0.08),
        extra_frac=0.2,
        window_full=True,
        sent_bytes=int(knee),
        source_bytes=int(knee),
    )
    cc.on_feedback(
        5.20,
        feedback_id=2,
        unique_bytes=int(50_000_000 / 8 * 0.20),
        decoded_bytes=0,
        echo_ts_us=_echo(5.20, 0.08),
        extra_frac=0.2,
        window_full=True,
        sent_bytes=int(knee * 0.20),
        source_bytes=int(knee * 0.20),
    )
    assert cc.saw_loss_knee is True
    assert cc.rate == pytest.approx(knee)
    assert "wrecked_cut" not in " ".join(cc._events)


def test_soft_lock_climbs_when_first_flight_goes_quiet():
    """WAN 637 lock then path_p=0 must probe, not freeze pace_p10=med=max."""
    cc = _search_cc()
    knee = 637_000_000 / 8
    cc.phase = CRUISE
    cc.was_fat = True
    cc.saw_loss_knee = True
    cc.dropper_confirmed = False
    cc.knee_bps = knee
    cc.rate = knee
    cc.last_good = knee
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    cc.last_send_rate = knee
    cc.last_delivery = knee
    cc.last_path_loss = 0.0
    cc.recv_lag = False
    for x in (knee * 0.95, knee, knee * 1.02):
        cc.bw.observe(x)
    assert cc._may_search_past_lock() is True
    assert cc._dropper_frozen() is False
    _feed(
        cc,
        3.0,
        1,
        int(knee * 3.0),
        0.08,
        sent=int(knee * 3.0),
        source=int(knee * 3.0),
        path_loss=0.0,
        decoded=int(knee * 3.0),
    )
    _feed(
        cc,
        3.20,
        2,
        int(knee * 3.20),
        0.08,
        sent=int(knee * 3.20),
        source=int(knee * 3.20),
        path_loss=0.0,
        decoded=int(knee * 3.20),
    )
    assert cc.phase == PROBE, (cc.phase, cc.rate, cc._events)
    assert cc.rate > knee


def test_confirmed_dropper_does_not_probe_past_knee():
    cc = _search_cc()
    knee = 900_000_000 / 8
    cc.phase = CRUISE
    cc.was_fat = True
    cc.saw_loss_knee = True
    cc.dropper_confirmed = True
    cc.knee_bps = knee
    cc.rate = knee
    cc.last_good = knee
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 0.0
    cc.last_send_rate = knee
    cc.last_delivery = knee
    cc.last_path_loss = 0.0
    for x in (knee * 0.95, knee, knee * 1.02):
        cc.bw.observe(x)
    assert cc._may_search_past_lock() is False
    _feed(
        cc,
        3.0,
        1,
        int(knee * 3.0),
        0.08,
        sent=int(knee * 3.0),
        source=int(knee * 3.0),
        path_loss=0.0,
        decoded=int(knee * 3.0),
    )
    _feed(
        cc,
        3.20,
        2,
        int(knee * 3.20),
        0.08,
        sent=int(knee * 3.20),
        source=int(knee * 3.20),
        path_loss=0.0,
        decoded=int(knee * 3.20),
    )
    assert cc.phase == CRUISE
    assert cc.rate == pytest.approx(knee)


def test_fill_shelf_lock_is_not_c_even_if_dropper_confirmed():
    """WAN 2026-09-15: pace=218 good=218 path_p50=24%→0%. Two-block fill is not C."""
    cc = _search_cc()
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    fill = cc._starved_fill_bps()
    cc.phase = CRUISE
    cc.was_fat = True
    cc.saw_loss_knee = True
    cc.dropper_confirmed = True
    cc.knee_bps = fill
    cc.rate = fill
    cc.last_good = fill
    cc.cruise_ts = 0.0
    cc.last_step_ts = 0.0
    cc.last_unique = 32 * 1048576
    cc.last_send_rate = 0.0
    cc.last_delivery = fill
    cc.last_path_loss = 0.0
    cc.recv_lag = False
    for x in (fill * 0.95, fill, fill * 1.02):
        cc.bw.observe(x)
    assert cc._on_fill_shelf() is True
    assert cc._is_fill_knee() is True
    assert cc._locked_c() is False
    assert cc._may_search_past_lock() is True
    assert cc._hol_stall() is False
    now = 3.0
    unique = 32 * 1048576
    sent = unique
    fb = 1
    for _ in range(8):
        now += 0.16
        unique += int(cc.rate * 0.16)
        sent += int(cc.rate * 0.16)
        _feed(
            cc,
            now,
            fb,
            unique,
            0.08,
            sent=sent,
            source=sent,
            path_loss=0.0,
            decoded=unique,
        )
        fb += 1
    assert cc.rate > fill * 1.20, _mbit(cc.rate)


def test_closed_loop_fill_shelf_climbs_to_fat_policer():
    """Stuck at two-block fill with a confirmed knee; C is 800."""
    cc = _search_cc()
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    fill = 2 * 1048576 / 0.08
    cc.phase = CRUISE
    cc.rate = fill
    cc.last_good = fill
    cc.knee_bps = fill
    cc.saw_loss_knee = True
    cc.dropper_confirmed = True
    cc.was_fat = True
    cc.cruise_ts = 0.0
    cc.last_unique = 32 * 1048576
    c = 800_000_000 / 8
    rates = _simulate_path(cc, c_bps=c, mode="policer", duration=16.0)
    tail = rates[-max(8, len(rates) // 5) :]
    med = sorted(tail)[len(tail) // 2]
    assert _mbit(med) > 500.0, (_mbit(med), cc.dropper_confirmed)


def test_probe_abort_dropper_sits_on_probe_base_not_trickle_send():
    """WAN 2026-09-14: probe_abort dropper snd=384/unq=727 locked 384."""
    cc = _search_cc()
    base = 937_000_000 / 8
    cc.phase = PROBE
    cc.was_fat = True
    cc.saw_loss_knee = True
    cc.knee_bps = base
    cc.probe_base = base
    cc.rate = 1_171_000_000 / 8
    cc.last_good = base
    cc.last_send_rate = 384_000_000 / 8
    cc.last_delivery = 727_000_000 / 8
    cc.last_path_loss = 0.034
    for x in (1_000_000_000 / 8, 1_100_000_000 / 8, 1_131_000_000 / 8):
        cc.bw.observe(x)
    assert cc._c_lock_bps() == pytest.approx(base)
    cc._abort_probe_dropper(8.0)
    assert cc.rate == pytest.approx(base)
    assert cc.knee_bps == pytest.approx(base)
    assert cc.dropper_confirmed is True
    assert cc.phase == CRUISE


def test_c_lock_holds_rate_through_mid_transfer_hol():
    """Closed-loop: lock near C, then HOL. Stay near C, not ~400 Mbit."""
    cc = _search_cc()
    c = 800_000_000 / 8
    rates = _simulate_path(
        cc, c_bps=c, mode="soft", duration=16.0, hol_start=8.0, hol_dur=3.0
    )
    assert cc.saw_loss_knee is True, cc._events
    tail = rates[-8:]
    assert min(tail) > 600_000_000 / 8, (_mbit(min(tail)), cc._events)
    assert max(tail) < 1_050_000_000 / 8, _mbit(max(tail))


def test_hol_on_latched_knee_does_not_ratchet_to_50():
    """Mid-transfer unique ~400 Mbit is HOL, not C=400. Stay on 850."""
    cc = _search_cc()
    knee = 850_000_000 / 8
    cc.phase = CRUISE
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.knee_bps = knee
    cc.rate = 950_000_000 / 8
    cc.last_good = cc.rate
    cc.last_delivery = 400_000_000 / 8
    cc.last_send_rate = 200_000_000 / 8
    cc.recv_lag = True
    cc._recover_wrecked(1.0)
    assert cc.rate == pytest.approx(knee)
    assert cc.knee_bps == pytest.approx(knee)
    assert cc.saw_loss_knee is True
    assert cc.last_good >= knee


def test_fat_startup_stall_locks_and_does_not_probe():
    """Fat stall must freeze C immediately — no cruise +10% into the dropper."""
    cc = _search_cc()
    c = 800_000_000 / 8
    rates = _simulate_path(cc, c_bps=c, mode="soft", duration=12.0)
    assert cc.saw_loss_knee is True, cc._events
    tail = rates[-8:]
    assert max(tail) < 1_050_000_000 / 8, _mbit(max(tail))
    assert max(tail) < min(tail) * 1.12, (_mbit(min(tail)), _mbit(max(tail)))


def test_knee_not_latched_on_gro_burst():
    """Spain: snd=1108 at limiter 855 is ACK burst, not a higher C."""
    cc = _search_cc()
    rate = 855_000_000 / 8
    cc.rate = rate
    cc.last_send_rate = 1_108_000_000 / 8
    cc.last_delivery = cc.last_send_rate
    cc._note_knee()
    assert cc.knee_bps == 0.0
    cc.last_send_rate = rate
    cc.last_delivery = rate
    cc._note_knee()
    assert cc.knee_bps == pytest.approx(rate)


def test_dropper_knee_ignores_fat_qdelay():
    """Spain after loss knee: snd=1008/unq=999 drained 950→855 on OOO RTT."""
    cc = _search_cc()
    rate = 950_000_000 / 8
    cc.phase = CRUISE
    cc.rate = rate
    cc.last_good = rate
    cc.saw_loss_knee = True
    cc.was_fat = True
    cc.knee_bps = rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 10.0
    cc.last_step_ts = 10.0
    now = 10.0
    unique = 0
    sent = 0
    for i in range(1, 14):
        now += 0.12
        unique += int(rate * 0.12)
        sent += int(rate * 0.12)
        _feed(cc, now, i, unique, 0.140, sent=sent, source=sent)
    assert cc.phase != DRAIN
    assert cc.rate > rate * 0.95


def test_closed_loop_soft_knee_does_not_walk_into_five_percent():
    """Spain: 800M ~0%, 1000M ~5%, empty queue. Sit near 800, not 1.2 Gbit."""
    cc = _search_cc()
    c = 800_000_000 / 8
    rates = _simulate_path(cc, c_bps=c, mode="soft", duration=20.0)
    tail = rates[-max(8, len(rates) // 5) :]
    med = sorted(tail)[len(tail) // 2]
    assert _mbit(med) > 720.0, _mbit(med)
    assert _mbit(med) < 980.0, _mbit(med)
    tail_hi, tail_lo = max(tail), min(tail)
    assert tail_hi < tail_lo * 1.15, (_mbit(tail_lo), _mbit(tail_hi), cc.saw_loss_knee)


def test_closed_loop_iid_loss_does_not_lock_unique_times_headroom():
    """23% iid on an 800 Mbit cap: unique is 0.77×C. Sit on C, not unique×1.10."""
    cc = _search_cc()
    c = 800_000_000 / 8
    rates = _simulate_path(cc, c_bps=c, loss=0.23, mode="policer", duration=22.0)
    tail = rates[-max(8, len(rates) // 5) :]
    med = sorted(tail)[len(tail) // 2]
    assert _mbit(med) > 650.0, _mbit(med)
    assert max(tail) < min(tail) * 2.0, (_mbit(min(tail)), _mbit(max(tail)))


def test_closed_loop_spain_policer_sits_near_850():
    """Russia↔Spain: empty queue, policer ~850–950. Must not lock ~600."""
    cc = _search_cc()
    c = 850_000_000 / 8
    rates = _simulate_path(cc, c_bps=c, mode="soft", duration=22.0)
    tail = rates[-max(8, len(rates) // 5) :]
    med = sorted(tail)[len(tail) // 2]
    assert 750.0 < _mbit(med) < 980.0, _mbit(med)
    assert max(tail) < min(tail) * 1.20, (_mbit(min(tail)), _mbit(max(tail)))


def test_closed_loop_thin_policer_sits_near_shaper():
    cc = _search_cc()
    c = 90_000_000 / 8
    rates = _simulate_path(cc, c_bps=c, mode="policer", duration=16.0)
    tail = rates[-max(8, len(rates) // 5) :]
    med = sorted(tail)[len(tail) // 2]
    assert _mbit(med) < 180.0, _mbit(med)
    assert _mbit(med) > 50.0, _mbit(med)


def test_closed_loop_hol_dip_does_not_lock_trickle_as_c():
    """Fat 800, then 2s unique cliff. Must not stay at ~200 after recovery."""
    cc = _search_cc()
    c = 800_000_000 / 8
    rates = _simulate_path(
        cc,
        c_bps=c,
        mode="policer",
        duration=20.0,
        hol_start=8.0,
        hol_dur=2.0,
    )
    tail = rates[-12:]
    med = sorted(tail)[len(tail) // 2]
    assert _mbit(med) > 400.0, (_mbit(med), cc.phase)


def test_dropper_knee_ignores_stable_coverable_loss_after_fill_latch():
    """WAN 2026-09-14: knee=197 then path_p50=22% (FEC). Not a new C lock."""
    cc = _search_cc()
    fill = 197_000_000 / 8
    cc.knee_bps = fill
    cc.rate = 240_000_000 / 8
    cc.last_send_rate = cc.rate
    cc.last_delivery = 224_000_000 / 8
    cc.last_path_loss = 0.222
    cc.path_samples = [
        (fill, 0.221),
        (fill * 1.02, 0.224),
        (210_000_000 / 8, 0.221),
    ]
    for x in (cc.last_delivery * 0.95, cc.last_delivery, cc.last_delivery):
        cc.bw.observe(x)
    assert cc._dropper_knee_sample() is False
    assert cc._path_loss_grew() is False


def test_quiet_hol_after_fat_climb_does_not_sit_on_two_block_fill():
    """WAN: pace 1210 / unq=847 / path=0%, then snd=51. Hold fat, not 197."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = STARTUP
    fat = 847_000_000 / 8
    cc.rate = 1_210_000_000 / 8
    cc.last_good = start
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.last_path_loss = 0.0
    cc.recv_lag = True
    cc.last_unique = 32 * 1048576
    cc.last_delivery = 48_000_000 / 8
    cc.last_send_rate = 51_000_000 / 8
    cc.cliff_n = 4
    for x in (fat * 0.95, fat, fat * 1.02):
        cc.bw.observe(x)
    cc._recover_wrecked(1.0)
    assert cc.rate > 400_000_000 / 8, _mbit(cc.rate)
    assert cc.last_good > 400_000_000 / 8, _mbit(cc.last_good)


def test_stale_gigabit_filter_does_not_lock_after_window_wreck():
    """Spain 2026-09-12: startup 1385/unq=948, then close 100%→16%.

    last_good stuck at 1076, probe 1076↔1345, ov stayed 0 because 1076/956
    is only 1.13×. Must forget the burst and cut.
    """
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.rate = 1_076_000_000 / 8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 10.0
    cc.last_step_ts = 10.0
    absorbed = 956_000_000 / 8
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    now = 10.0
    unique = 32 * 1048576
    decoded = 0
    sent = 64 * 1048576
    trickle = 50_000_000 / 8
    for i in range(1, 14):
        now += 0.12
        unique += int(trickle * 0.12)
        sent += int(cc.rate * 0.12)
        _feed(
            cc,
            now,
            i,
            unique,
            0.081,
            decoded=decoded,
            sent=sent,
            source=sent,
        )
    assert cc.rate < 400_000_000 / 8, _mbit(cc.rate)
    assert cc.last_good < 500_000_000 / 8, _mbit(cc.last_good)
    assert cc.phase != MEASURE


def test_window_stall_send_pause_still_cuts_stale_limiter():
    """Spain 2026-09-12 #2: climbed back to 1053, snd fell to 50, ov=0."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.rate = 1_053_000_000 / 8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 10.0
    cc.last_step_ts = 10.0
    absorbed = 969_000_000 / 8
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    now = 10.0
    unique = 32 * 1048576
    decoded = 0
    sent = 64 * 1048576
    trickle = 50_000_000 / 8
    for i in range(1, 14):
        now += 0.12
        unique += int(trickle * 0.12)
        sent += int(trickle * 0.12)
        _feed(
            cc,
            now,
            i,
            unique,
            0.081,
            decoded=decoded,
            sent=sent,
            source=sent,
        )
    assert cc.rate < 400_000_000 / 8, _mbit(cc.rate)
    assert cc.last_good < 500_000_000 / 8, _mbit(cc.last_good)


def test_measure_restore_if_trial_is_far_below_filter():
    """Spain: climbed to 830, HOL unique 114, measure slammed ~200. Restore."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    absorbed = 861_000_000 / 8
    cc.rate = 830_000_000 / 8
    cc.last_good = cc.rate
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    cc.last_delivery = 114_000_000 / 8
    cc.recv_lag = True
    now = 10.0
    cc._start_measure(now)
    # Force the WAN slam: trial sat on trickle, not on the 861 Mbit filter.
    cc.rate = 200_000_000 / 8
    cc.last_delivery = 114_000_000 / 8
    cc.measure_until = now
    cc._finish_measure(now)
    assert cc.rate > 400_000_000 / 8


def test_startup_wrecked_cut_climbs_off_two_block_fill():
    """Spain 2026-09-13: wrecked_cut 1326→259, bw stayed 1016, unique~250
    matched send, close 87→96%. unique_cliff blocked probes; sat 259 / 29 MiB/s.
    After the cut, a fat 800 Mbit path must climb, not freeze on fill.
    """
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.rate = 259_000_000 / 8
    cc.last_good = start
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 10.0
    cc.last_step_ts = 10.0
    cc.measure_holdoff = 0.0
    absorbed = 1_016_000_000 / 8
    cc.bw.observe(absorbed * 0.90)
    cc.bw.observe(absorbed * 0.95)
    cc.bw.observe(absorbed)
    cc.bw_peak = absorbed
    cc.last_delivery = 235_000_000 / 8
    cc.last_send_rate = 259_000_000 / 8
    now = 10.0
    unique = 32 * 1048576
    sent = 64 * 1048576
    c = 800_000_000 / 8
    rates: list[float] = []
    for i in range(1, 90):
        now += 0.12
        send = max(cc.rate, 1.0)
        delivered = min(send, c)
        unique += max(1, int(delivered * 0.12))
        sent += max(1, int(send * 0.12))
        _feed(
            cc,
            now,
            i,
            unique,
            0.081,
            sent=sent,
            source=sent,
        )
        rates.append(cc.rate)
    tail = rates[-12:]
    med = sorted(tail)[len(tail) // 2]
    assert _mbit(med) > 500.0, (_mbit(med), cc.phase, _mbit(cc.rate))


def test_probe_abort_lag_does_not_raise_last_good():
    """Spain 2026-09-13: probe_abort lag sat last_good=695 then unique died."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = PROBE
    cc.rate = 695_000_000 / 8
    cc.probe_base = cc.rate
    cc.last_good = 556_000_000 / 8
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.probe_until = 20.0
    cc.probe_unique = 656_000_000 / 8
    cc.last_delivery = 295_000_000 / 8
    cc.last_decoded_rate = 0.0
    cc.recv_lag = True
    cc.last_unique = 32 * 1048576
    now = 10.0
    unique = 32 * 1048576
    decoded = 8 * 1048576
    sent = 64 * 1048576
    for i in range(1, 8):
        now += 0.12
        unique += int(90_000_000 / 8 * 0.12)
        _feed(
            cc,
            now,
            i,
            unique,
            0.081,
            decoded=decoded,
            sent=sent,
            source=sent,
        )
    assert cc.last_good <= 556_000_000 / 8 * 1.02, _mbit(cc.last_good)
    assert cc.rate < 650_000_000 / 8, _mbit(cc.rate)


def test_paused_window_trickle_does_not_forget_near_c():
    """Spain 2026-09-13: 695/unq=62/snd=257/open=63. Cut, then last_good must
    not pin the limiter at the failed probe (probe_abort take_rate=False).
    """
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = PROBE
    cc.rate = 695_000_000 / 8
    cc.probe_base = cc.rate
    cc.last_good = 556_000_000 / 8
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.probe_until = 20.0
    cc.probe_unique = 656_000_000 / 8
    cc.last_delivery = 62_000_000 / 8
    cc.last_decoded_rate = 0.0
    cc.recv_lag = True
    cc.last_unique = 32 * 1048576
    cc.last_send_rate = 257_000_000 / 8
    now = 10.0
    unique = 32 * 1048576
    decoded = 8 * 1048576
    sent = 64 * 1048576
    for i in range(1, 10):
        now += 0.12
        unique += int(62_000_000 / 8 * 0.12)
        _feed(
            cc,
            now,
            i,
            unique,
            0.081,
            decoded=decoded,
            sent=sent,
            source=sent,
        )
    assert cc.last_good <= 556_000_000 / 8 * 1.02, _mbit(cc.last_good)
    assert cc.rate <= cc.last_good * 1.05, (_mbit(cc.rate), _mbit(cc.last_good))


def test_stale_limiter_with_send_pause_cuts_when_bw_tracks_trickle():
    """Spain 2026-09-13: pace=777 unq=107 snd=68 bw≈trickle ov=0 close=27% FEC 4%."""
    start = 8_000_000 / 8
    cc = BlastCc(max_bps=10_000_000_000 / 8, start_bps=start, min_bps=start)
    cc.phase = CRUISE
    cc.rate = 777_000_000 / 8
    cc.last_good = cc.rate
    cc.min_rtt = 0.08
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.rtt.n = 20
    cc.cruise_ts = 10.0
    cc.last_step_ts = 10.0
    trickle = 107_000_000 / 8
    cc.bw.observe(trickle * 0.90)
    cc.bw.observe(trickle * 0.95)
    cc.bw.observe(trickle)
    cc.last_delivery = trickle
    cc.last_send_rate = 68_000_000 / 8
    cc.last_sent = 64 * 1048576
    cc.recv_lag = True
    cc.last_unique = 32 * 1048576
    now = 10.0
    unique = 32 * 1048576
    decoded = unique // 4
    sent = 64 * 1048576
    source = sent
    for i in range(1, 12):
        now += 0.12
        unique += int(trickle * 0.12)
        decoded += int(trickle * 0.12 * 0.3)
        sent += int(68_000_000 / 8 * 0.12)
        _feed(
            cc,
            now,
            i,
            unique,
            0.081,
            decoded=decoded,
            sent=sent,
            source=source,
        )
    assert _mbit(cc.rate) < 400.0, _mbit(cc.rate)
    assert _mbit(cc.last_good) < 400.0, _mbit(cc.last_good)
    assert cc.phase != MEASURE
