"""Compare goodput across file sizes on long-transfer netem profiles.

wan-long / wan-long-dip start clean, then degrade around t=36–40s so 8–64 MiB
finish in phase1 while 256 MiB+ hit the dip (mimics 2G WAN skew vs 1G).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SIZES = (
    ("8M", "blob_8m.bin", 30),
    ("64M", "blob_64m.bin", 120),
    ("256M", "blob_256m.bin", 300),
)
PROFILES = ("wan-good", "wan-long-local", "wan-long-dip-local")


def ensure_blob(name: str, size: str) -> Path:
    blob = ROOT / "testdata" / name
    if not blob.is_file():
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "sim.genfile",
                "--output",
                str(blob),
                "--size",
                size,
            ],
            cwd=ROOT,
        )
    return blob


def run_one(blob: Path, profile: str, port: int, timeout: int) -> dict:
    env = os.environ.copy()
    env["TETRYS_GSO"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    py = [sys.executable, "-u", "-m", "tetrys_nc"]
    srv_log = Path(f"/tmp/netem_sz_srv_{profile}_{blob.stem}.log")
    out = Path(f"/tmp/netem_sz_out_{profile}_{blob.stem}.bin")
    srv = subprocess.Popen(
        py
        + [
            "server",
            "--file",
            str(blob),
            "--port",
            str(port),
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
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
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
    except subprocess.TimeoutExpired:
        ok = False
    dt = time.monotonic() - t0
    srv.terminate()
    emu.terminate()
    try:
        srv.wait(timeout=2)
        emu.wait(timeout=2)
    except subprocess.TimeoutExpired:
        srv.kill()
        emu.kill()
    slog = srv_log.read_text(errors="replace")
    done = re.search(
        r"done in ([\d.]+)s — goodput ([\d.]+) MiB/s .* repair_pkts=(\d+)", slog
    )
    return {
        "ok": ok,
        "wall_s": f"{dt:.1f}",
        "goodput": done.group(2) if done else "-",
        "repair": done.group(3) if done else "-",
    }


def main() -> int:
    print(f"{'profile':<14} {'size':<6} {'res':<6} {'s':>6} {'MiB/s':>7} {'repair':>8}")
    port = 18600
    ok_n = 0
    total = 0
    for profile in PROFILES:
        for size_label, blob_name, timeout in SIZES:
            blob = ensure_blob(blob_name, size_label)
            row = run_one(blob, profile, port, timeout)
            total += 1
            if row["ok"]:
                ok_n += 1
            note = "OK" if row["ok"] else "FAIL"
            print(
                f"{profile:<14} {size_label:<6} {note:<6} {row['wall_s']:>6} "
                f"{row['goodput']:>7} {row['repair']:>8}"
            )
            port += 2
            time.sleep(0.1)
    print(f"done {ok_n}/{total} ok")
    return 0 if ok_n == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
