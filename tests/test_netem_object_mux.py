"""Object-mux (packed micros) through userspace netem."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _make_mix(root: Path) -> tuple[Path, Path, dict[str, bytes]]:
    early = root / "early"
    late = root / "late"
    early.mkdir(parents=True)
    late.mkdir(parents=True)
    want: dict[str, bytes] = {}
    for i in range(80):
        blob = bytes([i % 256]) * (40 + i % 60)
        name = f"m{i:03d}.bin"
        (early / name).write_bytes(blob)
        want[name] = blob
    for i, name in enumerate(("big0.bin", "big1.bin")):
        blob = os.urandom(50_000 + i * 1000)
        (early / name).write_bytes(blob)
        want[name] = blob
    for i in range(20):
        blob = bytes([255 - i]) * (30 + i)
        name = f"lm{i:03d}.bin"
        (late / name).write_bytes(blob)
        want[name] = blob
    (late / "big_late.bin").write_bytes(os.urandom(40_000))
    want["big_late.bin"] = (late / "big_late.bin").read_bytes()
    return early, late, want


@pytest.mark.parametrize("profile", ["spain", "lossy"])
def test_object_mux_packed_through_netem(tmp_path: Path, profile: str) -> None:
    pytest.importorskip("raptorq")
    early, late, want = _make_mix(tmp_path / "src")
    out = tmp_path / "recv"
    srv_port = 17710 if profile == "spain" else 17720
    env = os.environ.copy()
    env["TETRYS_GSO"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    py = [sys.executable, "-u", "-m", "tetrys_nc"]
    srv_log = tmp_path / "srv.log"
    emu_log = tmp_path / "emu.log"
    srv = subprocess.Popen(
        py
        + [
            "objserver",
            "--host",
            "127.0.0.1",
            "--port",
            str(srv_port),
            "--early",
            str(early),
            "--late",
            str(late),
            "--late-delay",
            "0.15",
            "--fec",
            "28",
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
                "objclient",
                "--host",
                "127.0.0.1",
                "--port",
                str(srv_port + 1),
                "--output",
                str(out),
                "--timeout",
                "90",
            ],
            cwd=ROOT,
            env=env,
            timeout=100,
            capture_output=True,
            text=True,
        )
        ok = cli.returncode == 0 and "object-mux OK" in ((cli.stdout or "") + (cli.stderr or ""))
    except subprocess.TimeoutExpired:
        ok = False
        cli = None
    finally:
        srv.terminate()
        emu.terminate()
        srv.wait(timeout=3)
        emu.wait(timeout=3)
    emu_txt = emu_log.read_text(errors="replace")
    srv_txt = srv_log.read_text(errors="replace")
    cli_txt = "" if cli is None else ((cli.stdout or "") + (cli.stderr or ""))
    assert ok, f"{profile} mux failed\n{cli_txt[-600:]}\n{emu_txt[-400:]}\n{srv_txt[-400:]}"
    got = {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}
    assert got == want
    if "queue_drop=" in emu_txt:
        assert "queue_drop=0" in emu_txt
