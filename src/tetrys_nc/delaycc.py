"""Delay-based congestion control. Loss is ignored."""

from __future__ import annotations

from dataclasses import dataclass, field


def rtt_from_echo_s(echo_ts_us: int, now: float) -> float | None:
    """RTT from echoed send_ts_us (32-bit wrap)."""
    if echo_ts_us <= 0:
        return None
    now_us = int(now * 1_000_000) & 0xFFFFFFFF
    delta = (now_us - echo_ts_us) & 0xFFFFFFFF
    if delta <= 0 or delta > 5_000_000:
        return None
    return delta / 1_000_000.0


@dataclass
class DelayCc:
    """Cut send rate on standing queue; probe when qdelay is low."""

    max_bps: float
    min_bps: float
    qdelay_high_s: float = 0.080
    qdelay_low_s: float = 0.020
    decrease: float = 0.97
    increase: float = 1.03
    interval_s: float = 0.10
    enabled: bool = True
    rate: float = field(init=False)
    min_rtt_s: float | None = field(default=None, init=False)
    qdelay_s: float = field(default=0.0, init=False)
    last_update: float = field(default=0.0, init=False)
    last_completed: int = field(default=0, init=False)
    last_complete_t: float = field(default=0.0, init=False)
    bw_bps: float = field(default=0.0, init=False)
    rtt_s: float = field(default=0.0, init=False)
    rtt_ewma_s: float = field(default=0.0, init=False)
    mode: str = field(default="hold", init=False)
    _rtt_hist: list[tuple[float, float]] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.max_bps = max(1.0, self.max_bps)
        self.min_bps = min(self.max_bps, max(1.0, self.min_bps))
        self.rate = self.max_bps

    def note_echo(self, echo_ts_us: int, now: float) -> None:
        rtt = rtt_from_echo_s(echo_ts_us, now)
        if rtt is None:
            return
        self.rtt_s = rtt
        self._rtt_hist.append((now, rtt))
        cutoff = now - 10.0
        self._rtt_hist = [(t, r) for t, r in self._rtt_hist if t >= cutoff]
        self.min_rtt_s = min(r for _, r in self._rtt_hist)
        if self.rtt_ewma_s <= 0.0:
            self.rtt_ewma_s = rtt
        else:
            self.rtt_ewma_s = 0.95 * self.rtt_ewma_s + 0.05 * rtt
        self.qdelay_s = rtt - self.rtt_ewma_s

    def note_delivery(self, now: float, completed: int, block_bytes: int) -> None:
        if self.last_complete_t <= 0.0:
            self.last_completed = completed
            self.last_complete_t = now
            return
        dt = now - self.last_complete_t
        if dt < 0.20:
            return
        db = max(0, completed - self.last_completed) * block_bytes
        self.last_completed = completed
        self.last_complete_t = now
        if db > 0:
            inst = db / dt
            self.bw_bps = max(self.bw_bps * 0.85, inst)

    def step(self, now: float) -> float:
        if not self.enabled:
            self.rate = self.max_bps
            return self.rate
        if now - self.last_update < self.interval_s:
            return self.rate
        self.last_update = now
        # Spike vs smoothed RTT, not vs a lucky min sample.
        if self.qdelay_s >= 0.040:
            self.rate *= self.decrease
            self.mode = "cut"
        elif self.qdelay_s <= 0.005:
            self.rate *= self.increase
            self.mode = "probe"
        else:
            self.mode = "hold"
        self.rate = min(self.max_bps, max(self.min_bps, self.rate))
        return self.rate
