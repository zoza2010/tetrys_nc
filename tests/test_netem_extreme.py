"""Extreme netem paths: policer (drop, no queue), flap, tail, jitter, ACK cap."""

from __future__ import annotations

import socket
import subprocess
import sys
from pathlib import Path

import pytest

from sim.netem_udp import PROFILES, Direction, PathSpec, UdpNetem

from test_netem_wan_profiles import _run_through_netem

ROOT = Path(__file__).resolve().parents[1]
LIVE = ("shaper", "flap", "tail-crush")


def test_extreme_profiles_exist():
    shaper = PROFILES["shaper"]
    assert shaper.rate_drop
    assert shaper.rate_down and not shaper.rate_up
    assert shaper.rate_mbit < 200.0
    assert shaper.loss == 0.0

    wan = PROFILES["shaper-wan"]
    assert wan.rate_drop and wan.rate_mbit > shaper.rate_mbit

    late = PROFILES["shaper-late"]
    assert late.rate_drop
    assert late.phase2_rate_mbit is not None
    assert late.phase2_rate_mbit < late.rate_mbit

    flap = PROFILES["flap"]
    assert flap.duty_drop
    assert flap.duty_on_s > 0 and flap.duty_off_s > 0

    crush = PROFILES["tail-crush"]
    assert crush.phase2_loss is not None
    assert crush.phase2_loss > 0.5

    jitter = PROFILES["jitter-storm"]
    assert jitter.loss == 0.0
    assert jitter.jitter_s > 0.02

    ack = PROFILES["ack-shaper"]
    assert ack.rate_drop and ack.rate_up and not ack.rate_down

    hol = PROFILES["reorder-hol"]
    assert hol.reorder_extra_s >= 0.15


def test_policer_drops_over_rate_without_extra_delay():
    """Shaper: no standing queue. Queue-rate would stretch deadlines."""
    now = 10.0
    pkt = 1400
    drop = Direction(
        PathSpec(delay_s=0.02, rate_mbit=8.0, rate_drop=True, seed=1),
        1,
        is_down=True,
        t0=now,
    )
    queue = Direction(
        PathSpec(delay_s=0.02, rate_mbit=8.0, rate_drop=False, seed=1),
        1,
        is_down=True,
        t0=now,
    )
    drop._tokens = 0.0
    queue._tokens = 0.0
    d_dead = [drop.decide(now, pkt) for _ in range(40)]
    q_dead = [queue.decide(now, pkt) for _ in range(40)]
    d_keep = [t for t in d_dead if t is not None]
    assert sum(1 for t in d_dead if t is None) >= 20
    assert all(t <= now + 0.022 for t in d_keep)
    assert all(t is not None for t in q_dead)


def test_policer_does_not_touch_acks():
    spec = PROFILES["shaper"]
    up = Direction(spec, spec.seed, is_down=False, t0=0.0)
    up._tokens = 0.0
    kept = sum(1 for _ in range(30) if up.decide(0.0, 80) is not None)
    assert kept == 30


def test_duty_drop_is_loss_not_hold():
    spec = PathSpec(
        delay_s=0.0,
        duty_on_s=0.010,
        duty_off_s=0.010,
        duty_drop=True,
        seed=1,
    )
    d = Direction(spec, 1, is_down=True, t0=0.0)
    assert d.decide(0.004, 100) is not None
    assert d.decide(0.015, 100) is None


def test_ack_shaper_drops_up_keeps_down():
    spec = PROFILES["ack-shaper"]
    down = Direction(spec, spec.seed, is_down=True, t0=0.0)
    up = Direction(spec, spec.seed + 1, is_down=False, t0=0.0)
    down._tokens = 0.0
    up._tokens = 0.0
    assert sum(1 for _ in range(20) if down.decide(0.0, 1400) is None) == 0
    assert sum(1 for _ in range(200) if up.decide(0.0, 80) is None) >= 10


def test_shaper_proxy_model_drops_not_queue():
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.bind(("127.0.0.1", 0))
    emu = UdpNetem(
        ("127.0.0.1", 0),
        srv.getsockname(),
        PathSpec(delay_s=0.01, rate_mbit=8.0, rate_drop=True, seed=1),
        queue_max=64,
    )
    cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dest = emu.sock.getsockname()
    for _ in range(80):
        cli.sendto(b"x" * 1400, dest)
        emu.step()
    for _ in range(40):
        emu.step()
    emu.close()
    srv.close()
    cli.close()
    assert emu.stats.model_drop > 0
    assert emu.stats.queue_drop == 0
    assert emu.stats.valid
    assert emu.stats.fwd > 0


def _ensure_blob_8m() -> Path:
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
    return blob


@pytest.mark.parametrize("profile", LIVE)
def test_extreme_profiles_transfer_8m(tmp_path: Path, profile: str) -> None:
    pytest.importorskip("raptorq")
    blob = _ensure_blob_8m()
    ports = {"shaper": 17810, "flap": 17820, "tail-crush": 17830}
    ok, srv, emu = _run_through_netem(
        tmp_path,
        blob,
        profile,
        srv_port=ports[profile],
        timeout=40,
        rate="200",
        extra_env={"TETRYS_FEC_MODE": "quantile"},
    )
    banner_only = "queue_drop=" not in emu
    valid = banner_only or "valid=True" in emu or (
        "queue_drop=0" in emu and "jumbo_drop=0" in emu
    )
    assert ok, f"{profile} did not complete\n{emu[-400:]}\n{srv[-400:]}"
    assert "done in" in srv, srv[-800:]
    assert valid, f"{profile} netem invalid\n{emu[-400:]}"
