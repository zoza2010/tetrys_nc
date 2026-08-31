"""Netem profiles for the current WAN underfill (pace/wenc, not path starve).

2026-08-21 Russia↔Spain: --rate 900 (~107 MiB/s UDP) but wire 95–102 on a
good slice (limit=pace) and ~75 when encode waits (wenc≈60%). App goodput
then 79–87 or ~62. Distinct from profile ``starve`` (15 Mbit path cap).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from sim.netem_udp import PROFILES, Direction, PathSpec

from test_netem_wan_profiles import _run_through_netem

ROOT = Path(__file__).resolve().parents[1]
UNDERFILL_LIVE = ("wan-underfill", "wan-underfill-wenc")
_CEILING_MBIT = 900.0


def test_underfill_profiles_exist_and_are_not_starve():
    under = PROFILES["wan-underfill"]
    wenc = PROFILES["wan-underfill-wenc"]
    hol = PROFILES["wan-underfill-hol"]
    starve = PROFILES["starve"]
    fat = PROFILES["wan-fast"]

    assert under.rate_mbit < _CEILING_MBIT
    assert under.rate_mbit > 600.0
    assert under.loss < 0.03
    assert under.duty_on_s == 0.0
    assert under.blackout_dur_s == 0.0
    assert 0.030 <= under.delay_s <= 0.055

    assert wenc.duty_on_s > 0.0 and wenc.duty_off_s > 0.0
    on_frac = wenc.duty_on_s / (wenc.duty_on_s + wenc.duty_off_s)
    assert 0.50 <= on_frac <= 0.70
    assert wenc.duty_down
    assert wenc.rate_mbit > under.rate_mbit

    assert hol.blackout_dur_s > 0.0
    assert hol.blackout_down and not hol.blackout_up
    assert hol.rate_mbit == under.rate_mbit

    assert starve.rate_mbit < 30.0
    assert fat.rate_mbit >= _CEILING_MBIT
    ooo = PROFILES["wan-ooo"]
    assert ooo.reorder_p >= 0.40
    assert ooo.loss <= 0.01
    assert ooo.rate_mbit >= 850.0
    ooo_w = PROFILES["wan-ooo-wenc"]
    assert ooo_w.duty_off_s > ooo.duty_off_s
    assert ooo_w.reorder_p == ooo.reorder_p


def test_underfill_rate_slower_than_900_cap():
    full = Direction(PathSpec(delay_s=0.0, rate_mbit=900.0, seed=1), 1, is_down=True, t0=0.0)
    under = Direction(PathSpec(delay_s=0.0, rate_mbit=780.0, seed=1), 1, is_down=True, t0=0.0)
    assert under._rate_bps(0.0) / full._rate_bps(0.0) == pytest.approx(780 / 900)
    full._tokens = 0.0
    under._tokens = 0.0
    pkt = 1400
    tf = full.decide(0.0, pkt)
    tu = under.decide(0.0, pkt)
    assert tf is not None and tu is not None
    assert tu > tf
    assert tu / tf == pytest.approx(900 / 780, rel=0.02)


def test_duty_cycle_holds_data_during_off_not_loss():
    spec = PathSpec(delay_s=0.0, duty_on_s=0.010, duty_off_s=0.010, seed=1)
    d = Direction(spec, 1, is_down=True, t0=0.0)
    on = d.decide(0.004, 100)
    off = d.decide(0.015, 100)
    assert on is not None and off is not None
    assert on < 0.006
    assert off >= 0.019


def test_duty_cycle_does_not_delay_acks():
    spec = PathSpec(
        delay_s=0.0,
        duty_on_s=0.010,
        duty_off_s=0.010,
        duty_down=True,
        seed=1,
    )
    up = Direction(spec, 1, is_down=False, t0=0.0)
    got = up.decide(0.015, 40)
    assert got is not None
    assert got == pytest.approx(0.015, abs=1e-9)


def test_underfill_hol_blackout_drops_data_only():
    spec = PROFILES["wan-underfill-hol"]
    t0 = 10.0
    mid = t0 + spec.blackout_start_s + spec.blackout_dur_s / 2.0
    down = Direction(spec, spec.seed, is_down=True, t0=t0)
    up = Direction(spec, spec.seed + 1, is_down=False, t0=t0)
    n = 80
    assert sum(1 for _ in range(n) if down.decide(mid, 100) is None) == n
    down2 = Direction(spec, spec.seed, is_down=True, t0=t0)
    early_drops = sum(1 for _ in range(n) if down2.decide(t0 + 0.2, 100) is None)
    assert early_drops < n // 3
    ack_drops = sum(1 for _ in range(n) if up.decide(mid, 40) is None)
    assert ack_drops < n // 3


@pytest.mark.parametrize("profile", UNDERFILL_LIVE)
def test_underfill_profiles_transfer_8m(tmp_path: Path, profile: str) -> None:
    """Sender at the WAN ceiling (900) through a path that cannot fill it."""
    pytest.importorskip("raptorq")
    blob = ROOT / "testdata" / "blob_8m.bin"
    if not blob.is_file():
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "sim.genfile",
                "--output",
                str(blob),
                "--size",
                "8M",
            ],
            cwd=ROOT,
        )
    ports = {"wan-underfill": 17660, "wan-underfill-wenc": 17670}
    ok, srv, emu = _run_through_netem(
        tmp_path,
        blob,
        profile,
        srv_port=ports[profile],
        timeout=35,
        rate="900",
    )
    valid = "valid=True" in emu or (
        "queue_drop=0" in emu and "jumbo_drop=0" in emu
    )
    assert ok, f"{profile} did not complete\n{emu[-400:]}\n{srv[-400:]}"
    assert "done in" in srv, srv[-800:]
    assert valid, f"{profile} netem invalid\n{emu[-400:]}"
