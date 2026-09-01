"""Measure WAN underfill: pacer microbench + 256 MiB through netem profiles."""

from __future__ import annotations

import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tetrys_nc.ratectl import RateLimiter  # noqa: E402

CAP_MBIT = 900.0
CAP_MIB = CAP_MBIT * 1_000_000 / 8 / (1024 * 1024)  # ~107.3


def bench_pacer(seconds: float = 2.0, batch: int = 1400 * 64) -> dict:
    lim = RateLimiter(CAP_MBIT * 1_000_000 / 8)
    n = 0
    slept = 0.0
    t0 = time.monotonic()
    while time.monotonic() - t0 < seconds:
        slept += lim.consume(batch)
        n += batch
    dt = time.monotonic() - t0
    mib = n / dt / (1024 * 1024)
    return {
        "mib": mib,
        "fill": mib / CAP_MIB,
        "pace_frac": slept / dt,
        "sleep_debt": lim._sleep_debt,
    }


def parse_xfer(log: str) -> dict:
    wires, apps, limits, pace_pct, wenc_pct, send_pct = [], [], [], [], [], []
    fecs, lags = [], []
    for m in re.finditer(
        r"fec=(\d+)% pace=(\d+)/(\d+)Mbit lag=(\d+) incomplete=(\d+)/(\d+).*app=([\d.]+)",
        log,
    ):
        fecs.append(int(m.group(1)))
        lags.append(int(m.group(4)))
        apps.append(float(m.group(7)))
    for m in re.finditer(
        r"xfer wire=([\d.]+) .* send=(\d+)% .* pace=(\d+)% .* cap=(\d+)% "
        r"wenc=(\d+)% limit=(\w+)",
        log,
    ):
        wires.append(float(m.group(1)))
        send_pct.append(int(m.group(2)))
        pace_pct.append(int(m.group(3)))
        wenc_pct.append(int(m.group(5)))
        limits.append(m.group(6))
    done = re.search(
        r"done in ([\d.]+)s — goodput ([\d.]+) MiB/s .* repair_pkts=(\d+)", log
    )
    skip = re.search(r"skip_done=(\d+)", log)

    def med(xs):
        return statistics.median(xs) if xs else None

    return {
        "goodput": float(done.group(2)) if done else None,
        "wall": float(done.group(1)) if done else None,
        "repair": int(done.group(3)) if done else None,
        "wire_med": med(wires),
        "app_med": med(apps),
        "fec_med": med(fecs),
        "lag_med": med(lags),
        "pace_pct": med(pace_pct),
        "wenc_pct": med(wenc_pct),
        "send_pct": med(send_pct),
        "limit": max(set(limits), key=limits.count) if limits else None,
        "n_samp": len(wires),
        "skip_done": skip.group(1) if skip else None,
    }


def run_xfer(
    blob: Path,
    *,
    profile: str | None,
    port: int,
    timeout: int = 90,
) -> tuple[dict, str]:
    env = os.environ.copy()
    env["TETRYS_GSO"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    py = [sys.executable, "-u", "-m", "tetrys_nc"]
    tag = f"{profile or 'loop'}_{port}"
    srv_log = Path(f"/tmp/ufill_srv_{tag}.log")
    emu_log = Path(f"/tmp/ufill_emu_{tag}.log")
    out = Path(f"/tmp/ufill_out_{tag}.bin")
    cli_port = port + 1 if profile else port
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
            "900",
            "--ramp-s",
            "0.2",
            "--gen-k",
            "96",
        ],
        cwd=ROOT,
        env=env,
        stdout=srv_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    emu = None
    if profile:
        emu = subprocess.Popen(
            [sys.executable, "-u", "-m", "sim.netem_udp"]
            + [
                "--listen",
                f"127.0.0.1:{cli_port}",
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
    time.sleep(0.4)
    try:
        cli = subprocess.run(
            py
            + [
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                str(cli_port),
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
    except subprocess.TimeoutExpired:
        ok = False
        cli = None
    finally:
        srv.terminate()
        if emu is not None:
            emu.terminate()
        srv.wait(timeout=3)
        if emu is not None:
            emu.wait(timeout=3)
    slog = srv_log.read_text(errors="replace")
    stats = parse_xfer(slog)
    stats["ok"] = ok
    stats["cli_skip"] = None
    if cli is not None:
        m = re.search(r"skip_done=(\d+)", (cli.stdout or "") + (cli.stderr or ""))
        stats["cli_skip"] = m.group(1) if m else None
        stats["cli_txt"] = ((cli.stdout or "") + (cli.stderr or ""))[-400:]
    try:
        out.unlink()
    except OSError:
        pass
    return stats, slog[-2500:]


def fmt(v, nd=1):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.{nd}f}"
    return str(v)


def main() -> int:
    print(f"cap {CAP_MBIT:.0f} Mbit = {CAP_MIB:.1f} MiB/s UDP  (10% FEC → app≤{CAP_MIB/1.10:.1f})")
    print("--- pacer microbench (no sockets) ---")
    for batch in (1400, 1400 * 16, 1400 * 64, 1400 * 106):
        r = bench_pacer(1.5, batch)
        print(
            f"  batch={batch:6d}B  {r['mib']:6.1f} MiB/s  "
            f"fill={r['fill']*100:5.1f}%  sleep={r['pace_frac']*100:5.1f}%"
        )

    blob = ROOT / "testdata" / "blob_256m.bin"
    if not blob.is_file():
        blob = ROOT / "testdata" / "blob_64m.bin"
    print(f"\n--- transfer {blob.name}  --wan --rate 900 --gen-k 96 ---")
    cases = [
        ("loopback", None),
        ("none", "none"),
        ("wan-fast", "wan-fast"),
        ("wan-underfill", "wan-underfill"),
        ("wan-underfill-wenc", "wan-underfill-wenc"),
        ("wan-good", "wan-good"),
    ]
    print(
        f"{'case':<22} {'ok':<4} {'app':>6} {'wire':>6} {'fill%':>6} "
        f"{'fec':>4} {'limit':<6} {'pace%':>5} {'wenc%':>5} {'send%':>5} "
        f"{'lag':>5} {'n':>3}"
    )
    port = 19100
    rc = 0
    for name, profile in cases:
        stats, tail = run_xfer(blob, profile=profile, port=port)
        port += 2
        fill = None
        if stats["wire_med"] is not None:
            fill = 100.0 * stats["wire_med"] / CAP_MIB
        print(
            f"{name:<22} {'Y' if stats['ok'] else 'N':<4} "
            f"{fmt(stats['goodput']):>6} {fmt(stats['wire_med']):>6} "
            f"{fmt(fill, 1):>6} {fmt(stats['fec_med'], 0):>4} "
            f"{str(stats['limit'] or '-'):<6} {fmt(stats['pace_pct'], 0):>5} "
            f"{fmt(stats['wenc_pct'], 0):>5} {fmt(stats['send_pct'], 0):>5} "
            f"{fmt(stats['lag_med'], 0):>5} {stats['n_samp']:>3}"
        )
        if not stats["ok"]:
            rc = 1
            print("   FAIL", tail[-300:].replace("\n", " | "))
        time.sleep(0.2)
    print("fill% = median wire / 107.3 MiB/s (900 Mbit). limit= from 1s samples.")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
