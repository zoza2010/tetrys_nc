"""Aggressive fill-pipe rate control (loss → repair, not rate collapse)."""

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
        # High floor — never collapse to a crawl when filling the pipe
        self.min_rate = min(self.max_rate, max(self.max_rate * 0.35, 8_000_000.0))
        if start_bps is None:
            # Open near target immediately (user wants full channel)
            start_bps = self.max_rate
        self.rate = min(self.max_rate, max(start_bps, self.min_rate))
        self.burst = burst if burst is not None else max(self.rate * 0.1, 200_000.0)
        self.tokens = self.burst
        self.updated = time.monotonic()

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * 0.1, 200_000.0)

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
    Fill-pipe controller: stay near max_rate.

    Loss is repaired (NACK/coded), not treated as a reason to vacate the channel.
    Only extreme standing queue slightly eases off — and never below min_rate (~35% cap).
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
        self.slow_start = False  # already at/near target
        self.warmup_left = 0

    def _bump_rate(self, factor: float, additive: float = 0.0) -> None:
        rate = self.limiter.rate
        nxt = min(self.limiter.max_rate, max(rate * factor, rate + additive))
        self.limiter.set_rate(nxt)

    def on_loss(self, plr_byte: int) -> None:
        """Loss → keep sending; optional tiny ease only on extreme PLR."""
        if plr_byte < 200:  # ~78% — ignore normal hole bursts
            return
        # Still stay high: at most -10%
        self.limiter.set_rate(self.limiter.rate * 0.9)

    def on_ack(self, acked: int, plr_byte: int = 0) -> None:
        """Push back toward max whenever ACKs advance."""
        if acked <= 0:
            return
        # Always climb toward ceiling (ignore plr for rate)
        if self.limiter.rate < self.limiter.max_rate * 0.99:
            self._bump_rate(1.08, additive=500_000.0)

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
        if self.srtt_us is None:
            self.srtt_us = float(rtt)
        else:
            a = self.alpha
            self.srtt_us = (1.0 - a) * self.srtt_us + a * float(rtt)

        queue_us = max(0.0, self.srtt_us - (self.base_rtt_us or self.srtt_us))
        # Only ease if queue is enormous; still keep ≥ min_rate (high floor)
        if queue_us > 400_000:
            self.limiter.set_rate(self.limiter.rate * 0.95)
        else:
            self._bump_rate(1.02, additive=200_000.0)
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
