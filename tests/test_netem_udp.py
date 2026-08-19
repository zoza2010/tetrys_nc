"""Loopback UDP netem: delay, loss, seed, queue vs model drops."""

from __future__ import annotations

import socket
import threading
import time

from tetrys_nc.netem_udp import Direction, PathSpec, UdpNetem, parse_hostport
from tetrys_nc.netem_udp import PROFILES


def test_parse_hostport():
    assert parse_hostport("127.0.0.1:7495") == ("127.0.0.1", 7495)
    assert parse_hostport("7495")[1] == 7495


def test_all_loss_is_model_drop():
    d = Direction(PathSpec(loss=1.0, seed=7), seed=7)
    now = time.monotonic()
    assert d.decide(now, 100) is None


def test_seed_reproducible_drop_pattern():
    spec = PathSpec(loss=0.3, seed=42)

    def seq() -> list[bool]:
        d = Direction(spec, seed=42)
        t = 1.0
        return [d.decide(t, 10) is None for _ in range(40)]

    assert seq() == seq()


def test_delay_is_at_least_configured():
    d = Direction(PathSpec(delay_s=0.05, seed=1), seed=1)
    now = 10.0
    got = d.decide(now, 80)
    assert got is not None
    assert got >= now + 0.049


def test_proxy_delivers_with_delay():
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.bind(("127.0.0.1", 0))
    srv.settimeout(1.0)
    emu = UdpNetem(
        ("127.0.0.1", 0),
        srv.getsockname(),
        PathSpec(delay_s=0.03, seed=1),
    )
    stop = False

    def loop() -> None:
        while not stop:
            emu.step()

    th = threading.Thread(target=loop, daemon=True)
    th.start()
    cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t0 = time.monotonic()
    cli.sendto(b"ping", emu.sock.getsockname())
    data, _ = srv.recvfrom(64)
    dt = time.monotonic() - t0
    stop = True
    th.join(timeout=1.0)
    emu.close()
    srv.close()
    cli.close()
    assert data == b"ping"
    assert dt >= 0.02
    assert emu.stats.valid
    assert emu.stats.fwd >= 1


def test_queue_overflow_is_not_model_loss():
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.bind(("127.0.0.1", 0))
    emu = UdpNetem(
        ("127.0.0.1", 0),
        srv.getsockname(),
        PathSpec(delay_s=1.0, seed=1),
        queue_max=8,
    )
    cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dest = emu.sock.getsockname()
    for i in range(20):
        cli.sendto(bytes([i]), dest)
        emu.step()
    emu.close()
    srv.close()
    cli.close()
    assert emu.stats.queue_drop > 0
    assert emu.stats.model_drop == 0
    assert not emu.stats.valid


def test_jumbo_gso_blob_is_dropped():
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.bind(("127.0.0.1", 0))
    emu = UdpNetem(("127.0.0.1", 0), srv.getsockname(), PathSpec())
    cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cli.sendto(b"x" * 5000, emu.sock.getsockname())
    emu.step()
    emu.close()
    srv.close()
    cli.close()
    assert emu.stats.jumbo_drop == 1
    assert emu.stats.fwd == 0
    assert not emu.stats.valid


def test_hol_blackout_drops_data_keeps_acks():
    spec = PathSpec(
        blackout_start_s=1.0,
        blackout_dur_s=1.0,
        blackout_down=True,
        blackout_up=False,
        seed=1,
    )
    t0 = 50.0
    down = Direction(spec, 1, is_down=True, t0=t0)
    up = Direction(spec, 2, is_down=False, t0=t0)
    assert down.decide(t0 + 0.2, 100) is not None
    assert down.decide(t0 + 1.5, 100) is None
    assert up.decide(t0 + 1.5, 100) is not None
    assert down.decide(t0 + 2.2, 100) is not None


def test_hol_stall_profile_exists():
    spec = PROFILES["hol-stall"]
    assert spec.blackout_dur_s > 0
    assert spec.blackout_down and not spec.blackout_up
    assert spec.loss_after is not None and spec.loss_after > spec.loss


def test_first_second_spike_loss():
    spec = PathSpec(first_s=1.0, first_loss=1.0, loss=0.0, seed=1)
    d = Direction(spec, 1, is_down=True, t0=0.0)
    assert d.decide(0.2, 10) is None
    assert d.decide(1.5, 10) is not None


def test_ack_delay_is_asymmetric():
    spec = PathSpec(delay_s=0.01, delay_up_s=0.2, seed=1)
    up = Direction(spec, 1, is_down=False, t0=0.0)
    down = Direction(spec, 2, is_down=True, t0=0.0)
    u = up.decide(1.0, 10)
    dn = down.decide(1.0, 10)
    assert u is not None and dn is not None
    assert (u - 1.0) > (dn - 1.0) + 0.1


def test_loss_after_blackout_only_on_down():
    spec = PathSpec(
        loss=0.0,
        blackout_start_s=0.0,
        blackout_dur_s=0.05,
        loss_after=1.0,
        seed=3,
    )
    t0 = 0.0
    down = Direction(spec, 3, is_down=True, t0=t0)
    up = Direction(spec, 4, is_down=False, t0=t0)
    assert down.decide(1.0, 40) is None
    assert up.decide(1.0, 40) is not None
