"""Host CPU capacity samples: steal, PSI stall, cgroup throttle, frequency.

Used on the WAN sender to tell hypervisor steal / quota throttle / in-VM
contention apart from our encode pool. All paths are optional: Darwin and
stripped containers just report nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import time


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text()
    except OSError:
        return None


def parse_proc_stat_cpu(text: str) -> tuple[int, int, int, int] | None:
    """Aggregate `cpu` line → (user+nice, idle+iowait, steal, total) jiffies.

    `total` is the first 8 fields (user..steal). guest/guest_nice are already
    inside user on Linux and must not be added again.
    """
    for line in text.splitlines():
        if not line.startswith("cpu "):
            continue
        nums = [int(x) for x in line.split()[1:]]
        if len(nums) < 4:
            return None
        user = nums[0] + (nums[1] if len(nums) > 1 else 0)
        idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
        steal = nums[7] if len(nums) > 7 else 0
        total = sum(nums[:8]) if len(nums) >= 8 else sum(nums)
        return user, idle, steal, total
    return None


def parse_psi_cpu(text: str) -> tuple[int, int] | None:
    """Return (`some total` µs, `full total` µs)."""
    some: int | None = None
    full = 0
    for line in text.splitlines():
        kind, _, rest = line.partition(" ")
        if kind not in {"some", "full"}:
            continue
        fields = dict(kv.split("=", 1) for kv in rest.split() if "=" in kv)
        try:
            total = int(fields["total"])
        except (KeyError, ValueError):
            continue
        if kind == "some":
            some = total
        else:
            full = total
    if some is None:
        return None
    return some, full


def parse_cgroup_cpu_stat(text: str) -> tuple[int, int] | None:
    """Return (`throttled_usec`, `nr_throttled`)."""
    thr: int | None = None
    nr = 0
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            val = int(parts[1])
        except ValueError:
            continue
        if parts[0] == "throttled_usec":
            thr = val
        elif parts[0] == "nr_throttled":
            nr = val
    if thr is None:
        return None
    return thr, nr


def cgroup_cpu_stat_path(proc_self_cgroup: str, sys_fs_cgroup: Path) -> Path | None:
    rel = ""
    for line in proc_self_cgroup.splitlines():
        if line.startswith("0::"):
            rel = line[3:].strip()
            break
        # cgroup v1:  `2:cpu,cpuacct:/slice`
        fields = line.split(":", 2)
        if len(fields) == 3 and (
            fields[1] == "cpu" or fields[1].startswith("cpu,") or "cpuacct" in fields[1]
        ):
            rel = fields[2].strip()
    candidates: list[Path] = []
    if rel:
        candidates.append(sys_fs_cgroup / rel.lstrip("/") / "cpu.stat")
        parent = (sys_fs_cgroup / rel.lstrip("/"))
        for _ in range(6):
            parent = parent.parent
            candidates.append(parent / "cpu.stat")
            if parent == sys_fs_cgroup:
                break
    candidates.append(sys_fs_cgroup / "cpu.stat")
    candidates.append(sys_fs_cgroup / "cpu,cpuacct" / "cpu.stat")
    seen: set[Path] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        if path.is_file():
            return path
    return None


def read_avg_mhz(cpufreq_root: Path) -> float | None:
    freqs: list[float] = []
    if not cpufreq_root.is_dir():
        return None
    for cpu_dir in sorted(cpufreq_root.glob("cpu[0-9]*")):
        for name in ("cpufreq/scaling_cur_freq", "cpufreq/cpuinfo_cur_freq"):
            raw = _read_text(cpu_dir / name)
            if not raw:
                continue
            try:
                khz = int(raw.strip().split()[0])
            except ValueError:
                continue
            if khz > 0:
                freqs.append(khz / 1000.0)
                break
    if not freqs:
        return None
    return sum(freqs) / len(freqs)


@dataclass(frozen=True)
class CpuDelta:
    wall_s: float
    steal_pct: float | None
    usr_pct: float | None
    idle_pct: float | None
    psi_some_ms: float | None
    psi_full_ms: float | None
    throttle_ms: float | None
    nr_throttled: int | None
    mhz: float | None
    load1: float | None

    @property
    def available(self) -> bool:
        return any(
            v is not None
            for v in (
                self.steal_pct,
                self.usr_pct,
                self.psi_some_ms,
                self.throttle_ms,
                self.mhz,
            )
        )

    def format_line(self) -> str:
        def pct(v: float | None) -> str:
            return "n/a" if v is None else f"{v:.0f}%"

        def ms(v: float | None) -> str:
            return "n/a" if v is None else f"{v:.0f}ms"

        mhz = "n/a" if self.mhz is None else f"{self.mhz:.0f}"
        load = "n/a" if self.load1 is None else f"{self.load1:.2f}"
        thrn = ""
        if self.nr_throttled:
            thrn = f" thrn={self.nr_throttled}"
        return (
            f"  cpu steal={pct(self.steal_pct)} usr={pct(self.usr_pct)} "
            f"idle={pct(self.idle_pct)} psi={ms(self.psi_some_ms)} "
            f"thr={ms(self.throttle_ms)}{thrn} mhz={mhz} load={load}"
        )


@dataclass
class _Raw:
    stat: tuple[int, int, int, int] | None
    psi: tuple[int, int] | None
    cg: tuple[int, int] | None
    mhz: float | None
    load1: float | None
    t: float


class HostCpuSampler:
    """Delta sampler. Call `prime()` once, then `sample()` about once a second."""

    def __init__(
        self,
        *,
        proc: Path | None = None,
        sysfs: Path | None = None,
    ) -> None:
        self._proc = proc if proc is not None else Path("/proc")
        self._sysfs = sysfs if sysfs is not None else Path("/sys")
        self._cg_stat: Path | None | bool = False
        self._prev: _Raw | None = None

    def _cgroup_stat_path(self) -> Path | None:
        if self._cg_stat is False:
            text = _read_text(self._proc / "self" / "cgroup") or ""
            self._cg_stat = cgroup_cpu_stat_path(text, self._sysfs / "fs" / "cgroup")
        return self._cg_stat if isinstance(self._cg_stat, Path) else None

    def _read_raw(self) -> _Raw:
        stat_txt = _read_text(self._proc / "stat")
        psi_txt = _read_text(self._proc / "pressure" / "cpu")
        cg_path = self._cgroup_stat_path()
        cg_txt = _read_text(cg_path) if cg_path is not None else None
        try:
            load1 = os.getloadavg()[0]
        except OSError:
            load1 = None
        return _Raw(
            stat=parse_proc_stat_cpu(stat_txt) if stat_txt else None,
            psi=parse_psi_cpu(psi_txt) if psi_txt else None,
            cg=parse_cgroup_cpu_stat(cg_txt) if cg_txt else None,
            mhz=read_avg_mhz(self._sysfs / "devices" / "system" / "cpu"),
            load1=load1,
            t=time.monotonic(),
        )

    def prime(self) -> None:
        self._prev = self._read_raw()

    def sample(self) -> CpuDelta | None:
        cur = self._read_raw()
        prev = self._prev
        self._prev = cur
        if prev is None:
            return None
        wall = max(cur.t - prev.t, 1e-6)
        steal = usr = idle = None
        if cur.stat is not None and prev.stat is not None:
            du = cur.stat[0] - prev.stat[0]
            di = cur.stat[1] - prev.stat[1]
            ds = cur.stat[2] - prev.stat[2]
            dtot = cur.stat[3] - prev.stat[3]
            if dtot > 0:
                steal = 100.0 * max(0, ds) / dtot
                usr = 100.0 * max(0, du) / dtot
                idle = 100.0 * max(0, di) / dtot
        psi_some = psi_full = None
        if cur.psi is not None and prev.psi is not None:
            # PSI `total` is microseconds of stall.
            psi_some = max(0, cur.psi[0] - prev.psi[0]) / 1000.0
            psi_full = max(0, cur.psi[1] - prev.psi[1]) / 1000.0
        thr_ms = nr_thr = None
        if cur.cg is not None and prev.cg is not None:
            thr_ms = max(0, cur.cg[0] - prev.cg[0]) / 1000.0
            nr_thr = max(0, cur.cg[1] - prev.cg[1])
        return CpuDelta(
            wall_s=wall,
            steal_pct=steal,
            usr_pct=usr,
            idle_pct=idle,
            psi_some_ms=psi_some,
            psi_full_ms=psi_full,
            throttle_ms=thr_ms,
            nr_throttled=nr_thr,
            mhz=cur.mhz,
            load1=cur.load1,
        )
