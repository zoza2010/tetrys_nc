"""Long-transfer netem profiles: size-dependent path degradation."""

from __future__ import annotations

import time

import pytest

from tetrys_nc.netem_udp import PROFILES, Direction, PathSpec


def test_wan_long_profiles_exist():
    wan = PROFILES["wan-long"]
    assert wan.phase2_start_s >= 30.0
    assert wan.phase2_loss is not None
    local = PROFILES["wan-long-local"]
    assert 5.0 < local.phase2_start_s < 20.0


def test_phase2_loss_only_after_start():
    spec = PathSpec(
        loss=0.01,
        phase2_start_s=10.0,
        phase2_loss=0.50,
        seed=1,
    )
    t0 = 100.0
    down = Direction(spec, 1, is_down=True, t0=t0)
    drops_early = sum(
        1 for _ in range(200) if down.decide(t0 + 5.0, 100) is None
    )
    down2 = Direction(spec, 1, is_down=True, t0=t0)
    drops_late = sum(
        1 for _ in range(200) if down2.decide(t0 + 15.0, 100) is None
    )
    assert drops_late > drops_early * 3


def test_phase2_rate_slower_delivery():
    spec = PathSpec(
        delay_s=0.0,
        rate_mbit=1000.0,
        phase2_start_s=5.0,
        phase2_rate_mbit=50.0,
        seed=2,
    )
    t0 = 0.0
    down = Direction(spec, 2, is_down=True, t0=t0)
    assert down._rate_bps(t0 + 1.0) > down._rate_bps(t0 + 10.0) * 5


@pytest.mark.parametrize("profile", ["wan-long", "wan-long-dip"])
def test_short_transfer_finishes_before_phase2(profile: str):
    """8 MiB @ ~15 MiB/s finishes in <1s; phase2 at 36s must not affect it."""
    spec = PROFILES[profile]
    assert spec.phase2_start_s > 5.0