"""Replay the WAN HOL freeze through loopback netem + real fountain."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _progress_done(log: str) -> list[tuple[int, int]]:
    """(gens_sent, client_done) from server progress lines."""
    out: list[tuple[int, int]] = []
    for m in re.finditer(
        r"progress (\d+)/\d+ client_done=(\d+)", log
    ):
        out.append((int(m.group(1)), int(m.group(2))))
    return out


def _hol_freeze(samples: list[tuple[int, int]], min_ticks: int = 2) -> bool:
    """client_done stuck while gens_sent still climbs (WAN 1G/2G signature)."""
    if len(samples) < min_ticks + 1:
        return False
    stuck = 1
    for i in range(1, len(samples)):
        sent0, done0 = samples[i - 1]
        sent1, done1 = samples[i]
        if done1 == done0 and sent1 > sent0:
            stuck += 1
            if stuck >= min_ticks:
                return True
        else:
            stuck = 1
    return False


def test_hol_stall_profile_does_not_freeze_blast(tmp_path: Path) -> None:
    pytest.importorskip("raptorq")
    blob = tmp_path / "blob_32m.bin"
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "tetrys_nc",
            "genfile",
            "--output",
            str(blob),
            "--size",
            "32M",
        ],
        cwd=ROOT,
    )
    out = tmp_path / "recv.bin"
    srv_log = tmp_path / "srv.log"
    emu_log = tmp_path / "emu.log"
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
            "17594",
            "--wan",
            "--skip-hash",
            "--rate",
            "80",
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
        py
        + [
            "netem",
            "--listen",
            "127.0.0.1:17595",
            "--forward",
            "127.0.0.1:17594",
            "--profile",
            "hol-stall",
            "--seed",
            "1",
        ],
        cwd=ROOT,
        env=env,
        stdout=emu_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    time.sleep(0.5)
    try:
        cli = subprocess.run(
            py
            + [
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                "17595",
                "--wan",
                "--output",
                str(out),
            ],
            cwd=ROOT,
            env=env,
            timeout=45,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        cli_rc = -1
        cli_out = "TIMEOUT"
    else:
        cli_rc = cli.returncode
        cli_out = (cli.stdout or "") + (cli.stderr or "")
    finally:
        srv.terminate()
        emu.terminate()
        srv.wait(timeout=3)
        emu.wait(timeout=3)

    text = srv_log.read_text(errors="replace")
    samples = _progress_done(text)
    froze = _hol_freeze(samples)
    ok = "OK:" in cli_out and cli_rc == 0
    # Occupancy-only pause + 8 MiB window: HOL lag must not freeze blast.
    assert samples, f"no progress in server log:\n{text[-1500:]}"
    assert ok and not froze, (
        f"HOL stall profile must finish without freeze; "
        f"completed={ok} froze={froze} last={samples[-3:] if samples else None}\n"
        f"{cli_out[-500:]}"
    )
