"""Local netem A/B: fixed-24 vs fixed-8 on clean, iid, burst."""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from tetrys_nc.block_state import parse_done_metrics  # noqa: E402

BLOB = ROOT / "testdata" / "blob_256m.bin"
MODES = (
    ("fixed-24", "24"),
    ("fixed-8", "8"),
)
PROFILES = ("clean-rtt", "lossy", "spain")
REPEATS = 2
RATE = "200"
GEN_K = "48"
TIMEOUT = 90
BASE_PORT = 18200


def _run(profile: str, overhead: str, port: int, work: Path) -> dict:
    out = work / "recv.bin"
    srv_log = work / "srv.log"
    emu_log = work / "emu.log"
    env = os.environ.copy()
    env["TETRYS_GSO"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("TETRYS_CC", "0")
    py = [sys.executable, "-u", "-m", "tetrys_nc"]
    srv_cmd = py + [
        "server",
        "--file",
        str(BLOB),
        "--port",
        str(port),
        "--skip-hash",
        "--rate",
        RATE,
        "--ramp-s",
        "0",
        "--gen-k",
        GEN_K,
    ]
    if overhead is not None:
        srv_cmd.extend(["--gen-overhead", overhead])
    srv = subprocess.Popen(
        srv_cmd,
        cwd=ROOT,
        env=env,
        stdout=srv_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    emu = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-m",
            "sim.netem_udp",
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
    ok = False
    cli_txt = ""
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
            timeout=TIMEOUT,
            capture_output=True,
            text=True,
        )
        cli_txt = (cli.stdout or "") + (cli.stderr or "")
        ok = cli.returncode == 0 and "OK:" in cli_txt
    except subprocess.TimeoutExpired:
        cli_txt = "TIMEOUT"
    finally:
        srv.terminate()
        emu.terminate()
        try:
            srv.wait(timeout=3)
        except subprocess.TimeoutExpired:
            srv.kill()
        try:
            emu.wait(timeout=3)
        except subprocess.TimeoutExpired:
            emu.kill()
    srv_txt = srv_log.read_text(errors="replace")
    metrics = parse_done_metrics(srv_txt)
    fec = None
    if metrics is not None:
        import re

        m = re.search(r"done in .* fec=(\d+)%", srv_txt)
        fec = int(m.group(1)) if m else None
    return {
        "ok": ok,
        "goodput": None if metrics is None else metrics.goodput_mib,
        "source_wire": None if metrics is None else metrics.source_wire_mib,
        "repair_wire": None if metrics is None else metrics.repair_wire_mib,
        "total_wire": None if metrics is None else metrics.total_wire_mib,
        "first_close": None if metrics is None else metrics.first_close_pct,
        "tail_s": None if metrics is None else metrics.tail_s,
        "pace_p10": None if metrics is None else metrics.pace_p10,
        "dir_rounds": None if metrics is None else metrics.dir_rounds,
        "fec_end": fec,
        "err": "" if ok else (cli_txt[-200:] + "\n" + srv_txt[-200:]),
    }


def _median(vals: list[float]) -> float | None:
    clean = [v for v in vals if v is not None]
    if not clean:
        return None
    return float(statistics.median(clean))


def _min(vals: list[float]) -> float | None:
    clean = [v for v in vals if v is not None]
    return None if not clean else float(min(clean))


def main() -> int:
    if not BLOB.is_file():
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "sim.genfile",
                "--output",
                str(BLOB),
                "--size",
                "32M",
            ],
            cwd=ROOT,
        )
    runs: list[dict] = []
    n = 0
    with tempfile.TemporaryDirectory(prefix="fec-ab-") as tmp:
        tmp_path = Path(tmp)
        for profile in PROFILES:
            for mode_name, overhead in MODES:
                samples = []
                for rep in range(REPEATS):
                    n += 1
                    port = BASE_PORT + n * 2
                    work = tmp_path / f"{profile}_{mode_name}_{rep}"
                    work.mkdir()
                    print(
                        f"[{n}/{len(PROFILES)*len(MODES)*REPEATS}] "
                        f"{profile} {mode_name} r{rep+1}",
                        flush=True,
                    )
                    got = _run(profile, overhead, port, work)
                    got.update(profile=profile, mode=mode_name, rep=rep + 1)
                    samples.append(got)
                    runs.append(got)
                    status = "ok" if got["ok"] else "FAIL"
                    print(
                        f"  {status} gp={got['goodput']} wire={got['total_wire']} "
                        f"close={got['first_close']} fec={got['fec_end']} "
                        f"dir={got['dir_rounds']} tail={got['tail_s']}",
                        flush=True,
                    )
    summary = []
    for profile in PROFILES:
        by_mode = {}
        for mode_name, _ in MODES:
            chunk = [
                r
                for r in runs
                if r["profile"] == profile and r["mode"] == mode_name
            ]
            by_mode[mode_name] = {
                "n_ok": sum(1 for r in chunk if r["ok"]),
                "n": len(chunk),
                "goodput_med": _median([r["goodput"] for r in chunk]),
                "goodput_min": _min([r["goodput"] for r in chunk]),
                "wire_med": _median([r["total_wire"] for r in chunk]),
                "source_med": _median([r["source_wire"] for r in chunk]),
                "repair_med": _median([r["repair_wire"] for r in chunk]),
                "close_med": _median([r["first_close"] for r in chunk]),
                "tail_med": _median([r["tail_s"] for r in chunk]),
                "dir_med": _median(
                    [float(r["dir_rounds"]) for r in chunk if r["dir_rounds"] is not None]
                ),
                "fec_end_med": _median(
                    [float(r["fec_end"]) for r in chunk if r["fec_end"] is not None]
                ),
            }
        summary.append({"profile": profile, "modes": by_mode})
    out = {
        "file": str(BLOB.name),
        "size_mib": BLOB.stat().st_size / 1048576,
        "k": int(GEN_K),
        "rate_mbit": int(RATE),
        "repeats": REPEATS,
        "runs": runs,
        "summary": summary,
    }
    dest = ROOT / "testdata" / "fec_ab_netem.json"
    dest.write_text(json.dumps(out, indent=2))
    print(json.dumps(summary, indent=2), flush=True)
    print(f"wrote {dest}", flush=True)
    return 0 if all(r["ok"] for r in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
