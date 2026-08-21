"""Token-bucket pacing for gen RaptorQ blast sends."""

from __future__ import annotations

import os
import time


def _oversleep_credit() -> float:
    try:
        v = float(os.environ.get("TETRYS_PACE_OVERSLEEP", "0") or "0")
    except ValueError:
        return 0.0
    return min(1.0, max(0.0, v))


class RateLimiter:
    """Token-bucket pacing in bytes/sec with a hard ceiling (target rate)."""

    def __init__(
        self,
        max_bps: float,
        start_bps: float | None = None,
        burst: float | None = None,
        min_frac: float = 0.90,
    ) -> None:
        self.max_rate = max(max_bps, 1.0)
        # Default floor is high so a fixed --rate blast stays full. Delay CC
        # passes a lower min_frac so it can start low and probe upward.
        abs_floor = min(1_000_000.0, self.max_rate * 0.05)
        self.min_rate = min(
            self.max_rate, max(self.max_rate * min_frac, abs_floor, 1_000_000.0)
        )
        if start_bps is None:
            start_bps = self.max_rate
        self.rate = min(self.max_rate, max(start_bps, self.min_rate))
        # ~16ms of the target rate: enough for one K=192 generation (~290 KiB)
        # so we do not sleep after every 64-packet GSO burst. 2ms was starving
        # the 1000 Mbit ceiling; 250ms used to overflow shallow WAN queues.
        self._burst_s = 0.016
        self.burst = burst if burst is not None else max(self.rate * self._burst_s, 256_000.0)
        self.tokens = self.burst
        self.updated = time.monotonic()
        self.oversleep_credit = _oversleep_credit()
        # Seconds already waited beyond the intended sleep. Applied to the
        # *next* sleep (shorter wait) instead of extra tokens, so catch-up
        # does not enlarge the burst into a policer.
        self._sleep_debt = 0.0

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * self._burst_s, 256_000.0)

    def set_burst_s(self, burst_s: float) -> None:
        self._burst_s = max(0.001, min(burst_s, 0.050))
        self.burst = max(self.rate * self._burst_s, 256_000.0)

    def consume(self, nbytes: int) -> float:
        """Spend tokens. Returns seconds slept waiting for the bucket."""
        now = time.monotonic()
        elapsed = max(0.0, now - self.updated)
        self.updated = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.tokens -= nbytes
        if self.tokens >= 0:
            return 0.0

        need_s = (-self.tokens) / self.rate
        if need_s > 0.05:
            need_s = 0.05

        debt = self._sleep_debt
        if debt > 0.0:
            if debt >= need_s:
                self._sleep_debt = min(self._burst_s, debt - need_s)
                self.tokens = 0.0
                return 0.0
            need_s -= debt
            self._sleep_debt = 0.0

        t0 = time.monotonic()
        deadline = t0 + need_s
        # Sleep the full wait when it is above timer granularity; spin only
        # sub-ms remainders. Bound the spin so a frozen test clock cannot hang.
        if need_s >= 0.0004:
            time.sleep(need_s)
        spins = 0
        while time.monotonic() < deadline and spins < 2_000_000:
            spins += 1
        now2 = time.monotonic()
        self.updated = now2
        overslept = now2 - deadline
        if overslept > 0.0:
            self._sleep_debt = min(self._burst_s, self._sleep_debt + overslept)
        self.tokens = 0.0
        if overslept > 0.0 and self.oversleep_credit > 0:
            self.tokens = min(
                self.burst, overslept * self.rate * self.oversleep_credit
            )
        return need_s
