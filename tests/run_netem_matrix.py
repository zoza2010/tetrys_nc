"""Run 8 MiB fountain through every netem profile; print a table."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

from sim.netem_udp import PROFILES

ROOT = Path(__file__).resolve().parents[1]


def run_one(blob: Path, profile: str, port: int, timeout: int) -> dict:
    env = os.environ.copy()
    env["TETRYS_GSO"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    py = [sys.executable, "-u", "-m", "tetrys_nc"]
    srv_log = Path(f"/tmp/netem_mtx_srv_{profile}.log")
    emu_log = Path(f"/tmp/netem_mtx_emu_{profile}.log")
    out = Path(f"/tmp/netem_mtx_out_{profile}.bin")
    srv = subprocess.Popen(
        py
        + [
            "server",
            "--file",
            str(blob),
            "--port",
            str(port),
            "--wan",
            "--skip-hash",
            "--rate",
            "200",
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
            f"127.0.0.1:{port + 1}",
            "--forward",
            f"127.0.0.1:{port}",
            "--profile",
            profile,
        ],
        cwd=ROOT,
        env=env,
        stdout=emu_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    time.sleep(0.35)
    t0 = time.monotonic()
    try:
        cli = subprocess.run(
            py
            + [
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                str(port + 1),
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
        dt = time.monotonic() - t0
        text = (cli.stdout or "") + (cli.stderr or "")
        ok = cli.returncode == 0 and "OK:" in text
    except subprocess.TimeoutExpired:
        dt = time.monotonic() - t0
        ok = False
        text = "TIMEOUT"
    finally:
        srv.terminate()
        emu.terminate()
        try:
            srv.wait(timeout=2)
            emu.wait(timeout=2)
        except subprocess.TimeoutExpired:
            srv.kill()
            emu.kill()
    slog = srv_log.read_text(errors="replace")
    elog = emu_log.read_text(errors="replace")
    done = re.search(
        r"done in ([\d.]+)s — goodput ([\d.]+) MiB/s .* repair_pkts=(\d+)", slog
    )
    qdrop = 0
    mdrop = 0
    jumbo = 0
    valid = "?"
    for m in re.finditer(
        r"model_drop=(\d+) queue_drop=(\d+) jumbo_drop=(\d+) valid=(True|False)",
        elog,
    ):
        mdrop, qdrop, jumbo, valid = (
            int(m.group(1)),
            int(m.group(2)),
            int(m.group(3)),
            m.group(4),
        )
    return {
        "profile": profile,
        "ok": ok,
        "s": done.group(1) if done else f"{dt:.1f}t",
        "mbps": done.group(2) if done else "-",
        "repair": done.group(3) if done else "-",
        "mdrop": mdrop,
        "qdrop": qdrop,
        "jumbo": jumbo,
        "valid": valid,
        "note": "TIMEOUT" if text == "TIMEOUT" else ("OK" if ok else "FAIL"),
    }


def main() -> int:
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
    names = [n for n in sorted(PROFILES) if n != "none"]
    print(
        f"{'profile':<18} {'res':<8} {'s':>7} {'MiB/s':>7} {'repair':>8} "
        f"{'mdrop':>7} {'qdrop':>6} {'valid':<6}"
    )
    port = 17800
    rows = []
    for name in names:
        row = run_one(blob, name, port, timeout=25)
        rows.append(row)
        print(
            f"{row['profile']:<18} {row['note']:<8} {str(row['s']):>7} "
            f"{str(row['mbps']):>7} {str(row['repair']):>8} "
            f"{row['mdrop']:>7} {row['qdrop']:>6} {row['valid']:<6}"
        )
        port += 2
        time.sleep(0.15)
    ok_n = sum(1 for r in rows if r["ok"])
    print(f"done {ok_n}/{len(rows)} ok")
    return 0 if ok_n == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
