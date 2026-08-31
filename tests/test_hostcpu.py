"""Host CPU sampler and RaptorQ encode microbench."""

from __future__ import annotations

from pathlib import Path

import pytest

from sim.hostcpu import (
    CpuDelta,
    HostCpuSampler,
    cgroup_cpu_stat_path,
    parse_cgroup_cpu_stat,
    parse_proc_stat_cpu,
    parse_psi_cpu,
    read_avg_mhz,
)


PROC_STAT = """\
cpu  100 10 20 400 50 0 5 25 0 0
cpu0 50 5 10 200 25 0 2 12 0 0
intr 1
"""

PROC_STAT_2 = """\
cpu  200 20 40 500 70 0 10 125 0 0
cpu0 100 10 20 250 35 0 4 60 0 0
intr 2
"""

PSI = """\
some avg10=1.00 avg60=0.50 avg300=0.10 total=1000000
full avg10=0.10 avg60=0.05 avg300=0.01 total=100000
"""

PSI_2 = """\
some avg10=2.00 avg60=0.50 avg300=0.10 total=1040000
full avg10=0.20 avg60=0.05 avg300=0.01 total=101000
"""

CG = """\
usage_usec 5000000
user_usec 4000000
system_usec 1000000
nr_periods 10
nr_throttled 2
throttled_usec 250000
"""

CG_2 = """\
usage_usec 6000000
user_usec 4800000
system_usec 1200000
nr_periods 20
nr_throttled 5
throttled_usec 400000
"""


def test_parse_proc_stat_cpu_jiffies():
    got = parse_proc_stat_cpu(PROC_STAT)
    assert got is not None
    user, idle, steal, total = got
    assert user == 110
    assert idle == 450
    assert steal == 25
    # first 8 fields only; guest must not double-count
    assert total == 100 + 10 + 20 + 400 + 50 + 0 + 5 + 25


def test_parse_psi_and_cgroup():
    assert parse_psi_cpu(PSI) == (1_000_000, 100_000)
    assert parse_cgroup_cpu_stat(CG) == (250_000, 2)


def test_cgroup_v2_path(tmp_path: Path):
    cg = tmp_path / "fs" / "cgroup"
    leaf = cg / "user.slice" / "session.scope"
    leaf.mkdir(parents=True)
    (leaf / "cpu.stat").write_text(CG)
    found = cgroup_cpu_stat_path("0::/user.slice/session.scope\n", cg)
    assert found == leaf / "cpu.stat"


def test_read_avg_mhz(tmp_path: Path):
    cpu0 = tmp_path / "cpu0" / "cpufreq"
    cpu1 = tmp_path / "cpu1" / "cpufreq"
    cpu0.mkdir(parents=True)
    cpu1.mkdir(parents=True)
    (cpu0 / "scaling_cur_freq").write_text("2400000\n")
    (cpu1 / "scaling_cur_freq").write_text("2000000\n")
    assert read_avg_mhz(tmp_path) == pytest.approx(2200.0)


def test_sampler_deltas(tmp_path: Path):
    proc = tmp_path / "proc"
    sysfs = tmp_path / "sys"
    cpu_root = sysfs / "devices" / "system" / "cpu" / "cpu0" / "cpufreq"
    cg = sysfs / "fs" / "cgroup" / "user.slice"
    cpu_root.mkdir(parents=True)
    cg.mkdir(parents=True)
    (proc / "pressure").mkdir(parents=True)
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "cgroup").write_text("0::/user.slice\n")
    (proc / "stat").write_text(PROC_STAT)
    (proc / "pressure" / "cpu").write_text(PSI)
    (cg / "cpu.stat").write_text(CG)
    (cpu_root / "scaling_cur_freq").write_text("2199000\n")

    sampler = HostCpuSampler(proc=proc, sysfs=sysfs)
    sampler.prime()
    (proc / "stat").write_text(PROC_STAT_2)
    (proc / "pressure" / "cpu").write_text(PSI_2)
    (cg / "cpu.stat").write_text(CG_2)
    snap = sampler.sample()
    assert snap is not None
    assert snap.available
    # Δ: user 110→220, idle 450→570, steal 25→125, total 610→965
    assert snap.steal_pct == pytest.approx(100.0 * 100 / 355, abs=0.1)
    assert snap.usr_pct == pytest.approx(100.0 * 110 / 355, abs=0.1)
    assert snap.idle_pct == pytest.approx(100.0 * 120 / 355, abs=0.1)
    assert snap.psi_some_ms == pytest.approx(40.0)
    assert snap.psi_full_ms == pytest.approx(1.0)
    assert snap.throttle_ms == pytest.approx(150.0)
    assert snap.nr_throttled == 3
    assert snap.mhz == pytest.approx(2199.0)
    line = snap.format_line()
    assert line.startswith("  cpu steal=")
    assert "psi=40ms" in line
    assert "thr=150ms" in line
    assert "thrn=3" in line
    assert "mhz=2199" in line


def test_cpu_line_hides_zero_throttles():
    line = CpuDelta(
        wall_s=1.0,
        steal_pct=0.0,
        usr_pct=10.0,
        idle_pct=90.0,
        psi_some_ms=0.0,
        psi_full_ms=0.0,
        throttle_ms=0.0,
        nr_throttled=0,
        mhz=3000.0,
        load1=0.5,
    ).format_line()
    assert "thrn=" not in line
    assert "steal=0%" in line


def test_encbench_reports_gens_per_s():
    pytest.importorskip("raptorq")
    from sim.encbench import bench_encode

    got = bench_encode(k=16, symbol_size=64, overhead_pct=10, n=2, warmup=1)
    assert got.n == 2
    assert got.gens_per_s > 0
    assert got.ms_per_gen > 0
    assert "encbench probe" in got.format_line("probe")
    assert "gen/s" in got.format_line("probe")
