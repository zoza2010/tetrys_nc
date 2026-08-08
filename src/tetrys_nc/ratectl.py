"""FASP-inspired delay-based rate control with slow start (loss ≠ congestion)."""

from __future__ import annotations

import time


class RateLimiter:
    """Token-bucket pacing in bytes/sec with a hard ceiling."""

    def __init__(
        self,
        max_bps: float,
        start_bps: float | None = None,
        burst: float | None = None,
    ) -> None:
        self.max_rate = max(max_bps, 1.0)
        # Steady-state floor after MD — keep modest so false RTT can't pin us here
        self.min_rate = min(self.max_rate, max(2_500_000.0, self.max_rate * 0.05))
        if start_bps is None:
            # Open gently: ~3% of target, capped at ~80 Mbit
            start_bps = min(self.max_rate, max(1_250_000.0, self.max_rate * 0.03))
            start_bps = min(start_bps, 10_000_000.0)
        # Start may be below min_rate; set_rate() will enforce floor later
        self.rate = min(self.max_rate, max(start_bps, 1_000_000.0))
        self.burst = burst if burst is not None else max(self.rate * 0.02, 32_000.0)
        self.tokens = min(self.burst, 64_000.0)
        self.updated = time.monotonic()

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * 0.08, 65_000.0)

    def consume(self, nbytes: int) -> None:
        now = time.monotonic()
        elapsed = now - self.updated
        self.updated = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.tokens -= nbytes
        if self.tokens < 0:
            sleep_s = (-self.tokens) / self.rate
            time.sleep(sleep_s)
            self.updated = time.monotonic()
            self.tokens = 0.0


class DelayRateController:
    """
    Adjust send rate from RTT / queuing delay, not packet loss.

    Slow-start: climb while queueing delay stays near zero.
    Congestion avoidance: gentler increase; MD on queue growth (never on loss).
    """

    __slots__ = (
        "limiter",
        "base_rtt_us",
        "srtt_us",
        "last_rtt_us",
        "alpha",
        "samples",
        "slow_start",
        "warmup_left",
    )

    def __init__(self, limiter: RateLimiter, alpha: float = 0.125) -> None:
        self.limiter = limiter
        self.base_rtt_us: float | None = None
        self.srtt_us: float | None = None
        self.last_rtt_us: float = 0.0
        self.alpha = alpha
        self.samples = 0
        self.slow_start = True
        self.warmup_left = 8

    def _bump_rate(self, factor: float, additive: float = 0.0) -> None:
        rate = self.limiter.rate
        nxt = min(self.limiter.max_rate, max(rate * factor, rate + additive))
        if nxt < self.limiter.min_rate:
            self.limiter.rate = nxt
            self.limiter.burst = max(nxt * 0.05, 32_000.0)
        else:
            self.limiter.set_rate(nxt)
        if self.limiter.rate >= self.limiter.max_rate * 0.98:
            self.slow_start = False

    def on_ack(self, acked: int, plr_byte: int = 0) -> None:
        """
        Climb send rate from ACK progress when the path looks healthy.
        Primary climb signal when plr≈0 (delay signal is secondary).
        """
        if acked <= 0 or plr_byte >= 16:
            return
        queue_us = 0.0
        if self.srtt_us is not None and self.base_rtt_us is not None:
            queue_us = max(0.0, self.srtt_us - self.base_rtt_us)
        # Only hold back ACK-climb on severe standing queue
        if queue_us > 200_000:
            return
        if self.slow_start or self.samples == 0:
            self._bump_rate(1.12, additive=200_000.0)
        else:
            self._bump_rate(1.05, additive=100_000.0)

    def on_echo(self, echo_ts_us: int, now_us: int | None = None) -> float | None:
        if not echo_ts_us:
            return None
        if now_us is None:
            now_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
        rtt = (now_us - echo_ts_us) & 0xFFFFFFFF
        if rtt < 100 or rtt > 5_000_000:
            return None

        self.last_rtt_us = float(rtt)
        self.samples += 1
        if self.base_rtt_us is None or rtt < self.base_rtt_us:
            self.base_rtt_us = float(rtt)
        elif self.base_rtt_us is not None and rtt < self.base_rtt_us * 1.05:
            # Slowly track improving baseline (path variance)
            self.base_rtt_us = 0.95 * self.base_rtt_us + 0.05 * float(rtt)
        if self.srtt_us is None:
            self.srtt_us = float(rtt)
        else:
            a = self.alpha
            self.srtt_us = (1.0 - a) * self.srtt_us + a * float(rtt)

        inst_queue = max(0.0, float(rtt) - (self.base_rtt_us or float(rtt)))
        queue_us = max(0.0, self.srtt_us - self.base_rtt_us)

        if self.warmup_left > 0:
            self.warmup_left -= 1
            # Warmup: never MD — establish baseline only
            if inst_queue < 30_000:
                self._bump_rate(1.12, additive=150_000.0)
            return float(rtt)

        # Soft MD — high thresholds (WAN RTT often 100–200ms+jitter)
        if inst_queue > 250_000 or queue_us > 200_000:
            self.slow_start = False
            self.limiter.set_rate(max(self.limiter.min_rate, self.limiter.rate * 0.90))
        elif inst_queue > 150_000 or queue_us > 120_000:
            self.slow_start = False
            self.limiter.set_rate(max(self.limiter.min_rate, self.limiter.rate * 0.95))
        elif self.slow_start and queue_us < 40_000 and inst_queue < 50_000:
            self._bump_rate(1.15, additive=200_000.0)
        elif queue_us < 30_000 and inst_queue < 40_000:
            self._bump_rate(1.04, additive=80_000.0)
        return float(rtt)

    def stats(self) -> dict:
        return {
            "rtt_us": self.last_rtt_us,
            "srtt_us": self.srtt_us or 0.0,
            "base_rtt_us": self.base_rtt_us or 0.0,
            "queue_us": max(0.0, (self.srtt_us or 0.0) - (self.base_rtt_us or 0.0)),
            "samples": self.samples,
            "slow_start": self.slow_start,
            "rate": self.limiter.rate,
            "max_rate": self.limiter.max_rate,
        }
