"""Token-bucket pacing. Rate search lives in BlastCc; this only meters."""

from __future__ import annotations

import time

# Short enough not to dump into a shallow WAN queue (250ms did); long enough
# that we do not sleep after every 64-packet GSO burst (2ms starved ~1 Gbit).
_BURST_S = 0.008
# At CC floor (~80 Mbit) rate×burst_s is smaller than one send chunk.
_MIN_BURST = 256_000.0
_MAX_SLEEP_S = 0.05
_SPIN_AFTER_S = 0.0004


class RateLimiter:
    def __init__(
        self,
        max_bps: float,
        start_bps: float | None = None,
        burst_s: float = _BURST_S,
    ) -> None:
        self.max_rate = max(max_bps, 1.0)
        self.rate = min(self.max_rate, max(start_bps or self.max_rate, 1.0))
        self._burst_s = max(0.001, min(burst_s, 0.050))
        self.burst = max(self.rate * self._burst_s, _MIN_BURST)
        self.tokens = self.burst
        self.updated = time.monotonic()
        self._sleep_debt = 0.0

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, 1.0))
        self.burst = max(self.rate * self._burst_s, _MIN_BURST)

    def consume(self, nbytes: int) -> float:
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + max(0.0, now - self.updated) * self.rate)
        self.updated = now
        self.tokens -= nbytes
        if self.tokens >= 0:
            return 0.0

        need_s = min(_MAX_SLEEP_S, (-self.tokens) / self.rate)
        self.tokens = 0.0
        if self._sleep_debt >= need_s:
            self._sleep_debt = min(self._burst_s, self._sleep_debt - need_s)
            return 0.0
        need_s -= self._sleep_debt
        self._sleep_debt = 0.0

        t0 = time.monotonic()
        if need_s >= _SPIN_AFTER_S:
            time.sleep(need_s)
        deadline = t0 + need_s
        spins = 0
        while time.monotonic() < deadline and spins < 2_000_000:
            spins += 1
        now2 = time.monotonic()
        self.updated = now2
        if now2 > deadline:
            self._sleep_debt = min(self._burst_s, now2 - deadline)
        return need_s
