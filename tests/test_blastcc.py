"""Blast rate-search: filtered RTT, startup to cap, loss does not cut."""

from __future__ import annotations

import pytest

from tetrys_nc.blastcc import (
    CRUISE,
    DRAIN,
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
) -> float:
    return cc.on_feedback(
        now,
        feedback_id=fb,
        unique_bytes=unique,
        decoded_bytes=unique,
        echo_ts_us=_echo(now, rtt),
        extra_frac=extra,
        window_full=True,
    )


def test_seed_is_bbr_like_fraction_not_full_cap():
    cc = _cc()
    assert cc.phase == STARTUP
    assert cc.rate == pytest.approx(_START * 0.90)


def test_rtt_from_echo_wraps_32bit():
    now = 100.5
    assert rtt_from_echo(now, _echo(now, 0.08)) == pytest.approx(0.08)
    assert rtt_from_echo(now, 0) is None


def test_on_timer_climbs_during_blocked_send():
    cc = _cc()
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
    assert cc.phase == CRUISE
    assert cc.rate <= _CAP * 1.26


def test_single_jitter_does_not_drain():
    cc = _cc()
    now = 10.0
    unique = 0
    for i in range(1, 12):
        now += 0.05
        unique += 1_000_000
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
    _feed(cc, now, 1, 5_000_000, 0.081, extra=0.02)
    _feed(cc, now + 0.20, 2, 15_000_000, 0.081, extra=0.02)
    assert cc.rate == held
    assert cc.phase == CRUISE


def test_busy_extra_repair_cuts_cruise():
    cc = _cc()
    cc.phase = CRUISE
    cc.rate = _CAP
    cc.last_good = cc.rate
    cc.cruise_ts = 100.0
    cc.rtt.n = 20
    cc.rtt.min_rtt = 0.08
    cc.rtt.srtt = 0.08
    cc.last_delivery = 80 * 1048576
    _feed(cc, 101.0, 1, 5_000_000, 0.08, extra=0.20)
    _feed(cc, 101.2, 2, 15_000_000, 0.08, extra=0.20)
    assert cc.rate < _CAP


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
    _feed(cc, now, 1, 1, 0.08)
    assert cc.phase == PROBE
    base = cc.probe_base
    fb = 2
    while now < cc.probe_until:
        now += 0.05
        _feed(cc, now, fb, fb, 0.16)
        fb += 1
    _feed(cc, cc.probe_until + 0.01, fb, fb, 0.16)
    assert cc.phase == CRUISE
    assert cc.rate == base


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
    for bps in (start, start, start * 1.05):
        cc.bw.observe(bps)
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


def test_stamp_overwrites_encode_age():
    wires = pack_data_packets(1, 2, [b"x" * 10], 0, 1)
    stamp_data_wires(wires, 99)
    pkt = parse_packet(bytes(wires[0]))
    assert pkt.send_ts_us == 99
