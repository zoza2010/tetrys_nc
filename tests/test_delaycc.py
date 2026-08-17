from tetrys_nc.delaycc import DelayCc, rtt_from_echo_s


def test_rtt_from_echo():
    now = 100.0
    echo = int(now * 1_000_000 - 120_000) & 0xFFFFFFFF
    rtt = rtt_from_echo_s(echo, now)
    assert rtt is not None and 0.11 < rtt < 0.13
    assert rtt_from_echo_s(0, now) is None


def test_cc_decreases_on_queue():
    cc = DelayCc(max_bps=100_000_000, min_bps=25_000_000, interval_s=0.0)
    cc.qdelay_s = 0.050
    start = cc.rate
    cc.step(1.0)
    assert cc.rate < start
    assert cc.mode == "cut"


def test_cc_probes_when_empty():
    cc = DelayCc(max_bps=100_000_000, min_bps=25_000_000, interval_s=0.0)
    cc.rate = 50_000_000
    cc.qdelay_s = 0.001
    cc.step(1.0)
    assert cc.rate > 50_000_000


def test_cc_disabled_stays_at_max():
    cc = DelayCc(max_bps=100_000_000, min_bps=25_000_000, enabled=False, interval_s=0.0)
    cc.qdelay_s = 0.10
    cc.step(1.0)
    assert cc.rate == 100_000_000


def test_min_rtt_window_expires():
    cc = DelayCc(max_bps=100_000_000, min_bps=25_000_000)
    cc.note_echo(int((10.0 - 0.20) * 1_000_000) & 0xFFFFFFFF, 10.0)
    assert cc.min_rtt_s is not None and cc.min_rtt_s < 0.25
    later = 21.0
    cc.note_echo(int((later - 0.40) * 1_000_000) & 0xFFFFFFFF, later)
    assert cc.min_rtt_s is not None and cc.min_rtt_s > 0.30


def test_delivery_tracks_goodput():
    cc = DelayCc(max_bps=100_000_000, min_bps=10_000_000, interval_s=0.0)
    cc.note_delivery(1.0, 0, 1000)
    cc.note_delivery(1.5, 20_000, 1000)
    assert cc.bw_bps > 30_000_000
