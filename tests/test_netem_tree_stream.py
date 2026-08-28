"""Tree-stream through userspace netem vs v2 single-file on the same mix."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _make_tree(root: Path) -> dict[str, bytes]:
    root.mkdir(parents=True)
    want: dict[str, bytes] = {}
    (root / "sub").mkdir()
    for i in range(80):
        blob = bytes([i % 256]) * (40 + i % 60)
        name = f"m{i:03d}.bin"
        (root / name).write_bytes(blob)
        want[name] = blob
    for i, name in enumerate(("big0.bin", "big1.bin")):
        blob = os.urandom(50_000 + i * 1000)
        (root / "sub" / name).write_bytes(blob)
        want[f"sub/{name}"] = blob
    (root / "empty.dat").write_bytes(b"")
    want["empty.dat"] = b""
    (root / "sub" / "late.bin").write_bytes(os.urandom(40_000))
    want["sub/late.bin"] = (root / "sub" / "late.bin").read_bytes()
    return want


def _run_netem_pair(
    tmp_path: Path,
    profile: str,
    srv_args: list[str],
    cli_args: list[str],
    ok_token: str,
    srv_port: int,
) -> tuple[bool, str, str, str, float]:
    env = os.environ.copy()
    env["TETRYS_GSO"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    py = [sys.executable, "-u", "-m", "tetrys_nc"]
    srv_log = tmp_path / "srv.log"
    emu_log = tmp_path / "emu.log"
    srv = subprocess.Popen(
        py + srv_args,
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
    t0 = time.perf_counter()
    try:
        cli = subprocess.run(
            py + cli_args,
            cwd=ROOT,
            env=env,
            timeout=100,
            capture_output=True,
            text=True,
        )
        wall = time.perf_counter() - t0
        ok = cli.returncode == 0 and ok_token in ((cli.stdout or "") + (cli.stderr or ""))
    except subprocess.TimeoutExpired:
        wall = time.perf_counter() - t0
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
    return ok, cli_txt, srv_txt, emu_txt, wall


@pytest.mark.parametrize("profile", ["spain", "lossy"])
def test_tree_stream_through_netem(tmp_path: Path, profile: str) -> None:
    pytest.importorskip("raptorq")
    src = tmp_path / "src"
    want = _make_tree(src)
    out = tmp_path / "recv"
    srv_port = 17810 if profile == "spain" else 17820
    ok, cli_txt, srv_txt, emu_txt, _wall = _run_netem_pair(
        tmp_path,
        profile,
        [
            "treeserver",
            "--host",
            "127.0.0.1",
            "--port",
            str(srv_port),
            "--dir",
            str(src),
            "--fec",
            "28",
        ],
        [
            "treeclient",
            "--host",
            "127.0.0.1",
            "--port",
            str(srv_port + 1),
            "--output",
            str(out),
            "--timeout",
            "90",
        ],
        "tree-stream OK",
        srv_port,
    )
    assert ok, f"{profile} tree failed\n{cli_txt[-600:]}\n{emu_txt[-400:]}\n{srv_txt[-400:]}"
    got = {
        p.relative_to(out).as_posix(): p.read_bytes()
        for p in out.rglob("*")
        if p.is_file()
    }
    assert got == want
    if "queue_drop=" in emu_txt:
        assert "queue_drop=0" in emu_txt
