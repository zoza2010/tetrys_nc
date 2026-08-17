"""Token-bucket pacing for gen RaptorQ blast sends."""

from __future__ import annotations

import time


class RateLimiter:
    """Token-bucket pacing in bytes/sec with a hard ceiling (target rate)."""

    def __init__(
        self,
        max_bps: float,
        start_bps: float | None = None,
        burst: float | None = None,
    ) -> None:
        self.max_rate = max(max_bps, 1.0)
        # FASP-like: never collapse far below target — keep the pipe full.
        self.min_rate = min(self.max_rate, max(self.max_rate * 0.90, 8_000_000.0))
        if start_bps is None:
            start_bps = self.max_rate
        self.rate = min(self.max_rate, max(start_bps, self.min_rate))
        # A few milliseconds, not 250ms: large token bursts were harmless when
        # Python sendto was the bottleneck, but UDP GSO can dump them instantly
        # and overflow the path/receiver queues.
        self.burst = burst if burst is not None else max(self.rate * 0.002, 64_000.0)
        self.tokens = self.burst
        self.updated = time.monotonic()

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * 0.002, 64_000.0)

    def consume(self, nbytes: int) -> None:
        now = time.monotonic()
        elapsed = now - self.updated
        self.updated = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.tokens -= nbytes
        if self.tokens < 0:
            sleep_s = (-self.tokens) / self.rate
            # Cap single sleep — prefer short yields over multi-second stalls.
            if sleep_s > 0.05:
                sleep_s = 0.05
            time.sleep(sleep_s)
            self.updated = time.monotonic()
            self.tokens = 0.0
