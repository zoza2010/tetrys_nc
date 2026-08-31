"""Replay today's WAN 1 GiB path shapes through loopback netem."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from sim.netem_udp import PROFILES

ROOT = Path(__file__).resolve().parents[1]
WAN_PROFILES = (
    "wan-slow",
    "wan-mid",
    "wan-good",
    "wan-dip",
    "wan-fast",
    "wan-ooo",
)


def _run_through_netem(
    tmp_path: Path,
    blob: Path,
    profile: str,
    *,
    srv_port: int,
    timeout: int,
    rate: str = "200",
) -> tuple[bool, str, str]:
    out = tmp_path / f"recv_{profile}.bin"
    srv_log = tmp_path / f"srv_{profile}.log"
    emu_log = tmp_path / f"emu_{profile}.log"
    env = os.environ.copy()
    env["TETRYS_GSO"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    py = [sys.executable, "-u", "-m", "tetrys_nc"]
    srv = subprocess.Popen(
        py
        + [
            "server",
            "--file",
            str(blob),
            "--port",
            str(srv_port),
            "--wan",
            "--skip-hash",
            "--rate",
            rate,
            "--ramp-s",
            "0.2",
            "--gen-k",
            "48",
        ],
        cwd=ROOT,
        env=env,
        stdout=srv_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    emu = subprocess.Popen(
        [sys.executable, "-u", "-m", "sim.netem_udp"]
        + [
            "--listen",
            f"127.0.0.1:{srv_port + 1}",
            "--forward",
            f"127.0.0.1:{srv_port}",
            "--profile",
            profile,
        ],
        cwd=ROOT,
        env=env,
        stdout=emu_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    time.sleep(0.4)
    try:
        cli = subprocess.run(
            py
            + [
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                str(srv_port + 1),
                "--wan",
                "--output",
                str(out),
            ],
            cwd=ROOT,
            env=env,
            timeout=timeout,
            capture_output=True,
            text=True,
        )
        ok = cli.returncode == 0 and "OK:" in ((cli.stdout or "") + (cli.stderr or ""))
        cli_txt = (cli.stdout or "") + (cli.stderr or "")
    except subprocess.TimeoutExpired:
        ok = False
        cli_txt = "TIMEOUT"
    finally:
        srv.terminate()
        emu.terminate()
        srv.wait(timeout=3)
        emu.wait(timeout=3)
    return ok, srv_log.read_text(errors="replace"), emu_log.read_text(errors="replace")


def test_wan_profile_names_match_batch():
    for name in WAN_PROFILES:
        assert name in PROFILES
    assert PROFILES["wan-slow"].rate_mbit < PROFILES["wan-fast"].rate_mbit
    assert PROFILES["wan-dip"].blackout_dur_s > 0
    assert PROFILES["wan-good"].loss < PROFILES["wan-slow"].loss
    assert PROFILES["wan-ooo"].reorder_p > PROFILES["wan-good"].reorder_p
    assert PROFILES["wan-ooo"].loss < PROFILES["wan-good"].loss


@pytest.mark.parametrize("profile", WAN_PROFILES)
def test_wan_profiles_transfer_8m(tmp_path: Path, profile: str) -> None:
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
    ports = {
        "wan-slow": 17610,
        "wan-mid": 17620,
        "wan-good": 17630,
        "wan-dip": 17640,
        "wan-fast": 17650,
        "wan-ooo": 17680,
    }
    port = ports[profile]
    ok, srv, emu = _run_through_netem(
        tmp_path, blob, profile, srv_port=port, timeout=35, rate="200"
    )
    valid = "valid=True" in emu or (
        "queue_drop=0" in emu and "jumbo_drop=0" in emu
    )
    assert ok, f"{profile} did not complete\n{emu[-400:]}\n{srv[-400:]}"
    assert "done in" in srv, srv[-800:]
    assert valid, f"{profile} netem invalid\n{emu[-400:]}"
