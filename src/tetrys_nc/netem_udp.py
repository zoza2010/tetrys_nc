"""Userspace UDP path emulator (netem-like), for real tetrys sockets.

Kernel `tc netem` is better at line rate but needs Linux + sudo. This proxy
sits on 127.0.0.1, applies delay/jitter/loss/GE/reorder with a seed, and
counts *model* drops separately from *queue* overflows (queue_drop > 0 ⇒
the run is not a valid path sample).

GSO: the kernel delivers a UDP_SEGMENT burst as one datagram to this process.
Set TETRYS_GSO=0 on the sender or jumbo packets are dropped (jumbo_drop).
"""

from __future__ import annotations

import argparse
import heapq
import random
import select
import socket
import time
from dataclasses import dataclass

_MAX_DGRAM = 65535
_JUMBO = 2048
_QUEUE_MAX = 4096


@dataclass(frozen=True)
class PathSpec:
    delay_s: float = 0.0
    jitter_s: float = 0.0
    loss: float = 0.0
    ge_p_gb: float = 0.0
    ge_p_bg: float = 0.30
    ge_loss_good: float = 0.0
    ge_loss_bad: float = 0.35
    reorder_p: float = 0.0
    reorder_extra_s: float = 0.020
    rate_mbit: float = 0.0
    seed: int = 1
    # WAN HOL recipe: after a healthy start, blackhole *data* (server→client)
    # while ACKs still flow, then leave residual data loss. Server keeps blasting
    # new gens; next_needed can freeze if fountain does not fill the hole.
    blackout_start_s: float = 0.0
    blackout_dur_s: float = 0.0
    blackout_down: bool = True
    blackout_up: bool = False
    loss_after: float | None = None
    first_s: float = 0.0
    first_loss: float = 0.0
    dup_p: float = 0.0
    delay_up_s: float | None = None
    loss_up: float | None = None


PROFILES: dict[str, PathSpec] = {
    "none": PathSpec(),
    "clean-rtt": PathSpec(delay_s=0.050, seed=1),
    "lossy": PathSpec(delay_s=0.050, loss=0.08, seed=1),
    "spain": PathSpec(
        delay_s=0.050,
        jitter_s=0.008,
        ge_p_gb=0.03,
        ge_p_bg=0.22,
        ge_loss_good=0.02,
        ge_loss_bad=0.28,
        reorder_p=0.04,
        reorder_extra_s=0.020,
        seed=1,
    ),
    # Afternoon WAN 1G/2G: wire ~10–28 then HOL freeze.
    "hol-stall": PathSpec(
        delay_s=0.050,
        jitter_s=0.005,
        loss=0.02,
        blackout_start_s=1.20,
        blackout_dur_s=2.50,
        blackout_down=True,
        blackout_up=False,
        loss_after=0.20,
        seed=1,
    ),
    # Five matched 1 GiB WAN runs (gen-raptorq fountain, 2026-08-19).
    # wan1: 15.4 MiB/s, wire avg 17.6 max 57, repair ~15k
    "wan-slow": PathSpec(
        delay_s=0.055,
        jitter_s=0.012,
        loss=0.07,
        rate_mbit=160.0,
        seed=1,
    ),
    # wan2: 40.2 MiB/s, wire avg 53.5 max 87, repair ~15k
    "wan-mid": PathSpec(
        delay_s=0.050,
        jitter_s=0.008,
        loss=0.04,
        rate_mbit=450.0,
        seed=2,
    ),
    # wan3: 75.1 MiB/s, wire avg 85 max 102, repair ~3.7k
    "wan-good": PathSpec(
        delay_s=0.048,
        jitter_s=0.006,
        loss=0.015,
        rate_mbit=850.0,
        seed=3,
    ),
    # wan4: 27.6 MiB/s, wire min 1.5 avg 37 max 72, repair ~42k (dip)
    "wan-dip": PathSpec(
        delay_s=0.050,
        jitter_s=0.010,
        loss=0.03,
        rate_mbit=400.0,
        blackout_start_s=1.50,
        blackout_dur_s=1.80,
        blackout_down=True,
        blackout_up=False,
        loss_after=0.12,
        seed=4,
    ),
    # wan5: 84.1 MiB/s, wire avg 98 max 106, repair ~3.4k
        "wan-fast": PathSpec(
        delay_s=0.042,
        jitter_s=0.004,
        loss=0.010,
        rate_mbit=1000.0,
        seed=5,
    ),
    "ack-blackout": PathSpec(
        delay_s=0.050,
        blackout_start_s=0.40,
        blackout_dur_s=1.50,
        blackout_down=False,
        blackout_up=True,
        seed=11,
    ),
    "both-blackout": PathSpec(
        delay_s=0.050,
        blackout_start_s=0.50,
        blackout_dur_s=0.80,
        blackout_down=True,
        blackout_up=True,
        loss_after=0.05,
        seed=12,
    ),
    "reorder-storm": PathSpec(
        delay_s=0.040,
        jitter_s=0.015,
        reorder_p=0.35,
        reorder_extra_s=0.080,
        loss=0.02,
        seed=13,
    ),
    "ge-sticky": PathSpec(
        delay_s=0.050,
        ge_p_gb=0.12,
        ge_p_bg=0.06,
        ge_loss_good=0.02,
        ge_loss_bad=0.55,
        seed=14,
    ),
    "loss-half": PathSpec(delay_s=0.050, loss=0.50, rate_mbit=80.0, seed=15),
    "rtt-fat": PathSpec(delay_s=0.180, jitter_s=0.040, loss=0.03, seed=16),
    "starve": PathSpec(delay_s=0.050, loss=0.02, rate_mbit=15.0, seed=17),
    "first-spike": PathSpec(
        delay_s=0.050,
        first_s=1.0,
        first_loss=0.18,
        loss=0.025,
        seed=18,
    ),
    "dup-light": PathSpec(delay_s=0.050, dup_p=0.08, loss=0.02, seed=19),
    "asymm-ack": PathSpec(
        delay_s=0.030,
        delay_up_s=0.220,
        loss=0.02,
        seed=20,
    ),
    "bloat": PathSpec(
        delay_s=0.080,
        jitter_s=0.050,
        rate_mbit=40.0,
        loss=0.03,
        seed=21,
    ),
    "loss-up": PathSpec(delay_s=0.050, loss=0.01, loss_up=0.25, seed=22),
    "zero-delay-lossy": PathSpec(delay_s=0.0, loss=0.12, seed=23),
    "dup-reorder": PathSpec(
        delay_s=0.050,
        dup_p=0.10,
        reorder_p=0.20,
        reorder_extra_s=0.040,
        seed=24,
    ),
}


@dataclass
class PathStats:
    rx: int = 0
    fwd: int = 0
    model_drop: int = 0
    queue_drop: int = 0
    jumbo_drop: int = 0

    @property
    def valid(self) -> bool:
        return self.queue_drop == 0 and self.jumbo_drop == 0


class Direction:
    """One-way seeded impairments + optional token-bucket rate."""

    def __init__(
        self,
        spec: PathSpec,
        seed: int,
        *,
        is_down: bool = False,
        t0: float | None = None,
    ) -> None:
        self.spec = spec
        self.rng = random.Random(seed)
        self.is_down = is_down
        self.t0 = time.monotonic() if t0 is None else t0
        self.ge_bad = False
        self._tokens = 0.0
        self._last = self.t0
        if spec.rate_mbit > 0:
            self._bps = spec.rate_mbit * 1_000_000 / 8
            self._tokens = self._bps * 0.05
        else:
            self._bps = 0.0

    def _loss_p(self, now: float) -> float:
        spec = self.spec
        elapsed = max(0.0, now - self.t0)
        if spec.first_s > 0 and elapsed < spec.first_s:
            if self.is_down:
                return spec.first_loss
        in_bo = (
            spec.blackout_dur_s > 0
            and spec.blackout_start_s <= elapsed < spec.blackout_start_s + spec.blackout_dur_s
        )
        if in_bo:
            if (self.is_down and spec.blackout_down) or (
                (not self.is_down) and spec.blackout_up
            ):
                return 1.0
        after = elapsed >= spec.blackout_start_s + spec.blackout_dur_s
        if after and spec.loss_after is not None and self.is_down:
            return spec.loss_after
        if not self.is_down and spec.loss_up is not None:
            return spec.loss_up
        if spec.ge_p_gb > 0:
            if self.ge_bad:
                if self.rng.random() < spec.ge_p_bg:
                    self.ge_bad = False
            elif self.rng.random() < spec.ge_p_gb:
                self.ge_bad = True
            return spec.ge_loss_bad if self.ge_bad else spec.ge_loss_good
        return spec.loss

    def decide(self, now: float, nbytes: int) -> float | None:
        """Return delivery deadline, or None to drop (model)."""
        if self.rng.random() < self._loss_p(now):
            return None
        delay = self.spec.delay_s
        if not self.is_down and self.spec.delay_up_s is not None:
            delay = self.spec.delay_up_s
        if self.spec.jitter_s > 0:
            delay = max(0.0, delay + self.rng.gauss(0.0, self.spec.jitter_s))
        if self.spec.reorder_p > 0 and self.rng.random() < self.spec.reorder_p:
            delay += self.spec.reorder_extra_s
        if self._bps > 0:
            elapsed = max(0.0, now - self._last)
            self._last = now
            self._tokens = min(self._bps * 0.25, self._tokens + elapsed * self._bps)
            need = float(nbytes)
            if self._tokens < need:
                extra = (need - self._tokens) / self._bps
                delay += extra
                self._tokens = 0.0
            else:
                self._tokens -= need
        return now + delay


class UdpNetem:
    def __init__(
        self,
        listen: tuple[str, int],
        forward: tuple[str, int],
        spec: PathSpec,
        *,
        queue_max: int = _QUEUE_MAX,
    ) -> None:
        if listen[0] not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("bind loopback only")
        self.listen = listen
        self.forward = forward
        self.spec = spec
        self.queue_max = queue_max
        t0 = time.monotonic()
        self.up = Direction(spec, spec.seed, is_down=False, t0=t0)
        self.down = Direction(spec, spec.seed + 1, is_down=True, t0=t0)
        self.stats = PathStats()
        self._seq = 0
        self._heap: list[tuple[float, int, bytes, tuple[str, int]]] = []
        self._client: tuple[str, int] | None = None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 * 1024 * 1024)
        self.sock.bind(listen)
        self.sock.setblocking(False)

    def close(self) -> None:
        self.sock.close()

    def _enqueue(self, data: bytes, dest: tuple[str, int], up: bool) -> None:
        self.stats.rx += 1
        if len(data) > _JUMBO:
            self.stats.jumbo_drop += 1
            return
        now = time.monotonic()
        dline = (self.up if up else self.down).decide(now, len(data))
        if dline is None:
            self.stats.model_drop += 1
            return
        if len(self._heap) >= self.queue_max:
            self.stats.queue_drop += 1
            return
        heapq.heappush(self._heap, (dline, self._seq, data, dest))
        self._seq += 1
        dirc = self.up if up else self.down
        if self.spec.dup_p > 0 and dirc.rng.random() < self.spec.dup_p:
            if len(self._heap) >= self.queue_max:
                self.stats.queue_drop += 1
            else:
                heapq.heappush(
                    self._heap, (dline + 0.001, self._seq, data, dest)
                )
                self._seq += 1

    def _drain(self, now: float) -> None:
        while self._heap and self._heap[0][0] <= now:
            _t, _s, data, dest = heapq.heappop(self._heap)
            try:
                self.sock.sendto(data, dest)
                self.stats.fwd += 1
            except OSError:
                self.stats.queue_drop += 1

    def step(self) -> None:
        now = time.monotonic()
        timeout = 0.05
        if self._heap:
            timeout = max(0.0, min(timeout, self._heap[0][0] - now))
        r, _, _ = select.select([self.sock], [], [], timeout)
        if r:
            try:
                data, addr = self.sock.recvfrom(_MAX_DGRAM)
            except BlockingIOError:
                data = b""
                addr = None
            if data and addr is not None:
                if addr == self.forward:
                    dest = self._client
                    if dest is not None:
                        self._enqueue(data, dest, up=False)
                else:
                    self._client = addr
                    self._enqueue(data, self.forward, up=True)
        self._drain(time.monotonic())

    def run(self, seconds: float | None = None) -> PathStats:
        t0 = time.monotonic()
        try:
            while seconds is None or (time.monotonic() - t0) < seconds:
                self.step()
        except KeyboardInterrupt:
            pass
        return self.stats


def parse_hostport(s: str) -> tuple[str, int]:
    host, _, port = s.rpartition(":")
    if not host:
        host = "127.0.0.1"
    return host, int(port)


def spec_from_args(args: argparse.Namespace) -> PathSpec:
    base = PROFILES[args.profile]
    return PathSpec(
        delay_s=args.delay_ms / 1000.0 if args.delay_ms is not None else base.delay_s,
        jitter_s=args.jitter_ms / 1000.0 if args.jitter_ms is not None else base.jitter_s,
        loss=args.loss if args.loss is not None else base.loss,
        ge_p_gb=base.ge_p_gb,
        ge_p_bg=base.ge_p_bg,
        ge_loss_good=base.ge_loss_good,
        ge_loss_bad=base.ge_loss_bad,
        reorder_p=base.reorder_p,
        reorder_extra_s=base.reorder_extra_s,
        rate_mbit=args.rate_mbit if args.rate_mbit is not None else base.rate_mbit,
        seed=args.seed if args.seed is not None else base.seed,
        blackout_start_s=base.blackout_start_s,
        blackout_dur_s=base.blackout_dur_s,
        blackout_down=base.blackout_down,
        blackout_up=base.blackout_up,
        loss_after=base.loss_after,
        first_s=base.first_s,
        first_loss=base.first_loss,
        dup_p=base.dup_p,
        delay_up_s=base.delay_up_s,
        loss_up=base.loss_up,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Loopback UDP WAN emulator")
    p.add_argument("--listen", default="127.0.0.1:7495")
    p.add_argument("--forward", default="127.0.0.1:7494")
    p.add_argument("--profile", choices=sorted(PROFILES), default="spain")
    p.add_argument("--delay-ms", type=float, default=None)
    p.add_argument("--jitter-ms", type=float, default=None)
    p.add_argument("--loss", type=float, default=None)
    p.add_argument("--rate-mbit", type=float, default=None)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--queue-max", type=int, default=_QUEUE_MAX)
    args = p.parse_args(argv)
    listen = parse_hostport(args.listen)
    forward = parse_hostport(args.forward)
    spec = spec_from_args(args)
    emu = UdpNetem(listen, forward, spec, queue_max=args.queue_max)
    print(
        f"netem {listen} -> {forward} profile={args.profile} "
        f"delay={spec.delay_s*1e3:.0f}ms loss={spec.loss} "
        f"ge={spec.ge_p_gb} seed={spec.seed}  (TETRYS_GSO=0 on sender)"
    )
    last = time.monotonic()
    try:
        while True:
            emu.step()
            now = time.monotonic()
            if now - last >= 1.0:
                s = emu.stats
                print(
                    f"  rx={s.rx} fwd={s.fwd} model_drop={s.model_drop} "
                    f"queue_drop={s.queue_drop} jumbo_drop={s.jumbo_drop} "
                    f"valid={s.valid}"
                )
                last = now
    except KeyboardInterrupt:
        print("stop", emu.stats)
    finally:
        emu.close()
    return 0
