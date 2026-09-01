"""Token-bucket sleep-debt: recover oversleep without enlarging the burst."""

from __future__ import annotations

import time

import pytest

from tetrys_nc.ratectl import RateLimiter

_CAP = 900_000_000 / 8  # 900 Mbit
_BATCH = int(_CAP * 0.010)  # 10 ms of wire


def test_sleep_debt_shortens_next_wait_without_growing_burst(monkeypatch):
    clock = [100.0]
    sleeps: list[float] = []

    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        clock[0] += s + 0.001  # 1 ms timer overshoot

    monkeypatch.setattr(time, "sleep", fake_sleep)

    lim = RateLimiter(_CAP, burst_s=0.016)
    burst0 = lim.burst
    lim.tokens = 0.0
    lim.updated = clock[0]

    lim.consume(_BATCH)
    assert sleeps, "first consume must sleep"
    first = sleeps[0]
    assert first == pytest.approx(0.010, rel=0.05)
    assert lim._sleep_debt == pytest.approx(0.001, abs=2e-4)
    assert lim.burst == burst0
    assert lim.tokens == 0.0

    clock[0] += 0.00005
    lim.consume(_BATCH)
    assert len(sleeps) == 2
    assert sleeps[1] == pytest.approx(first - 0.001, abs=2e-4)
    assert lim.burst == burst0


def test_large_debt_skips_sleep_instead_of_dumping_tokens(monkeypatch):
    clock = [0.0]
    sleeps: list[float] = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        time, "sleep", lambda s: sleeps.append(s) or clock.__setitem__(0, clock[0] + s)
    )

    lim = RateLimiter(_CAP, burst_s=0.016)
    lim.tokens = 0.0
    lim.updated = 0.0
    lim._sleep_debt = 0.020

    slept = lim.consume(_BATCH)
    assert slept == 0.0
    assert sleeps == []
    assert lim.tokens == 0.0
    assert lim.burst == _CAP * 0.016


def test_set_rate_clips_only_to_cap():
    lim = RateLimiter(_CAP, start_bps=_CAP)
    lim.set_rate(_CAP * 0.10)
    assert lim.rate == pytest.approx(_CAP * 0.10)
    lim.set_rate(_CAP * 2)
    assert lim.rate == pytest.approx(_CAP)
