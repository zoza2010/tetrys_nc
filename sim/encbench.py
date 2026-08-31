"""Fixed RaptorQ encode microbenchmark.

Same construction as the WAN blast path: GenEncoder with systematic + FEC.
A dedicated probe so encode speed can be compared before / after a transfer
without the send loop or repair pool in the way.
"""

from __future__ import annotations

from dataclasses import dataclass
import argparse
import os
import sys
import time

from tetrys_nc.gen_raptor import GenEncoder, require_raptorq

from .hostcpu import HostCpuSampler


# WAN default. Keep this fixed across runs so gen/s is comparable.
DEFAULT_K = 96
DEFAULT_T = 1350
DEFAULT_FEC = 10


@dataclass(frozen=True)
class EncodeBench:
    k: int
    symbol_size: int
    overhead_pct: int
    n: int
    elapsed_s: float
    gens_per_s: float
    ms_per_gen: float
    bytes_per_gen: int

    def format_line(self, label: str) -> str:
        mib = self.bytes_per_gen * self.gens_per_s / (1024 * 1024)
        return (
            f"encbench {label} K={self.k} T={self.symbol_size} "
            f"fec={self.overhead_pct}% n={self.n}: "
            f"{self.gens_per_s:.1f} gen/s ({self.ms_per_gen:.1f} ms/gen, "
            f"{mib:.0f} MiB/s construct)"
        )


def bench_encode(
    *,
    k: int = DEFAULT_K,
    symbol_size: int = DEFAULT_T,
    overhead_pct: int = DEFAULT_FEC,
    n: int = 8,
    warmup: int = 1,
    data: bytes | None = None,
) -> EncodeBench:
    """Encode `n` generations after `warmup`. Isolated: no pool, no I/O."""
    require_raptorq()
    k = max(1, int(k))
    symbol_size = max(64, int(symbol_size))
    overhead_pct = max(0, int(overhead_pct))
    n = max(1, int(n))
    warmup = max(0, int(warmup))
    size = k * symbol_size
    if data is None:
        data = os.urandom(size)
    elif len(data) < size:
        data = data + bytes(size - len(data))
    else:
        data = data[:size]
    for _ in range(warmup):
        GenEncoder(data, symbol_size, overhead_pct)
    t0 = time.perf_counter()
    for _ in range(n):
        GenEncoder(data, symbol_size, overhead_pct)
    elapsed = max(time.perf_counter() - t0, 1e-9)
    return EncodeBench(
        k=k,
        symbol_size=symbol_size,
        overhead_pct=overhead_pct,
        n=n,
        elapsed_s=elapsed,
        gens_per_s=n / elapsed,
        ms_per_gen=elapsed / n * 1000.0,
        bytes_per_gen=size,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m sim.encbench",
        description=(
            "RaptorQ encode microbench + host CPU (steal/PSI/throttle). "
            "Run on the WAN sender while idle, then compare with the same "
            "lines during a transfer."
        ),
    )
    p.add_argument("--k", type=int, default=DEFAULT_K)
    p.add_argument("--symbol-size", type=int, default=DEFAULT_T)
    p.add_argument("--overhead", type=int, default=DEFAULT_FEC)
    p.add_argument("--n", type=int, default=16, help="encodes per sample")
    p.add_argument(
        "--seconds",
        type=float,
        default=0.0,
        help="loop this many seconds (0 = one sample and exit)",
    )
    args = p.parse_args(argv)

    print(
        "encbench: steal=hypervisor  psi=in-VM stall  "
        "thr=cgroup quota  slow bench + high steal → host; "
        "slow bench + psi/thr → VM; stable bench + high wenc → our pool"
    )
    cpu = HostCpuSampler()
    cpu.prime()
    deadline = time.monotonic() + max(0.0, args.seconds)
    sample_i = 0
    while True:
        sample_i += 1
        label = "idle" if args.seconds <= 0 else f"t{sample_i}"
        bench = bench_encode(
            k=args.k,
            symbol_size=args.symbol_size,
            overhead_pct=args.overhead,
            n=args.n,
            warmup=1 if sample_i == 1 else 0,
        )
        print(bench.format_line(label), flush=True)
        snap = cpu.sample()
        if snap is not None and snap.available:
            print(snap.format_line(), flush=True)
        if args.seconds <= 0 or time.monotonic() >= deadline:
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
