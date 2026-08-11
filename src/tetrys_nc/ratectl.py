"""FASP-style blast pacing: fill --rate hard; delay only soft-biases; loss ≠ congestion."""

from __future__ import annotations

import time
from collections import deque


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
        # Large burst so we can fill shallow buffers / absorb ACK jitter.
        self.burst = burst if burst is not None else max(self.rate * 0.25, 2_000_000.0)
        self.tokens = self.burst
        self.updated = time.monotonic()

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * 0.25, 2_000_000.0)

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


class DelayRateController:
    """
    FASP-style controller:

    - Optional ease-in ramp ~0 → --rate over --ramp-s (slow start, then fast)
    - Then blast at target; standing queue → tiny soft bias (floor ≈ 90%)
    - Loss ignored for pacing (repair handles reliability)
    - Large cwnd so elastic window / flight never starve the pipe
    """

    MODE_BLAST = "Blast"
    MODE_SOFT = "Soft"
    MODE_RAMP = "Ramp"
    # Aliases kept for older log/tests
    MODE_STARTUP = MODE_BLAST
    MODE_DRAIN = MODE_SOFT
    MODE_PROBE_BW = MODE_BLAST

    __slots__ = (
        "limiter",
        "base_rtt_us",
        "srtt_us",
        "last_rtt_us",
        "alpha",
        "samples",
        "slow_start",
        "mode",
        "btlbw",
        "peak_bw",
        "payload_size",
        "ramp_s",
        "_delivered",
        "_sample_t",
        "_sample_delivered",
        "_bw_filter",
        "_rtt_warmup",
        "_min_rtt_stamp",
        "_t0",
        "_ramp_done",
        "_saved_min",
    )

    def __init__(
        self,
        limiter: RateLimiter,
        alpha: float = 0.125,
        payload_size: int = 1350,
        ramp_s: float = 0.0,
    ) -> None:
        self.limiter = limiter
        self.payload_size = max(1, payload_size)
        self.base_rtt_us: float | None = None
        self.srtt_us: float | None = None
        self.last_rtt_us: float = 0.0
        self.alpha = alpha
        self.samples = 0
        self.slow_start = False
        self.mode = self.MODE_BLAST
        self.btlbw = 0.0
        self.peak_bw = 0.0
        self.ramp_s = max(0.0, float(ramp_s))
        self._delivered = 0.0
        self._sample_t = time.monotonic()
        self._sample_delivered = 0.0
        self._bw_filter: deque[tuple[float, float]] = deque()
        self._rtt_warmup = 0
        self._min_rtt_stamp = time.monotonic()
        self._t0 = time.monotonic()
        self._saved_min = self.limiter.min_rate
        self._ramp_done = self.ramp_s <= 0.0
        if self._ramp_done:
            self.limiter.set_rate(self.limiter.max_rate)
        else:
            # Allow near-zero during ramp (below FASP floor)
            self.limiter.min_rate = 1.0
            self.limiter.set_rate(1.0)
            self.mode = self.MODE_RAMP
            self.slow_start = True

    def tick(self) -> None:
        """Advance ramp / pacing from the send loop (not only on ACK)."""
        self._update_pacing(time.monotonic())

    def on_loss(self, plr_byte: int) -> None:
        return

    def on_ack(self, acked: int, plr_byte: int = 0) -> None:
        if acked <= 0:
            return
        now = time.monotonic()
        self._delivered += acked * self.payload_size
        dt = now - self._sample_t
        if dt >= 0.05:
            delta = self._delivered - self._sample_delivered
            if delta > 0:
                self._offer_bw(now, delta / dt)
            self._sample_t = now
            self._sample_delivered = self._delivered
            self._update_pacing(now)

    def on_echo(self, echo_ts_us: int, now_us: int | None = None) -> float | None:
        if not echo_ts_us:
            return None
        if now_us is None:
            now_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
        rtt = (now_us - echo_ts_us) & 0xFFFFFFFF
        if rtt < 1_000 or rtt > 2_000_000:
            return None

        now = time.monotonic()
        self.last_rtt_us = float(rtt)
        self.samples += 1
        self._rtt_warmup += 1

        if self._rtt_warmup >= 4 and rtt < 250_000:
            if self.base_rtt_us is None or rtt < self.base_rtt_us:
                self.base_rtt_us = float(rtt)
                self._min_rtt_stamp = now
            elif now - self._min_rtt_stamp > 10.0:
                self.base_rtt_us = min(self.base_rtt_us * 1.02, float(rtt))
                self._min_rtt_stamp = now

        if self.srtt_us is None:
            self.srtt_us = float(rtt)
        else:
            a = self.alpha
            self.srtt_us = (1.0 - a) * self.srtt_us + a * float(rtt)

        self._update_pacing(now)
        return float(rtt)

    def target_cwnd_packets(self) -> int:
        """Oversized flight so sender never waits on ACK while pipe has room."""
        min_rtt = self.base_rtt_us or self.srtt_us or 40_000.0
        bw = max(self.limiter.max_rate, self.limiter.rate, self.btlbw, self.peak_bw)
        bdp = bw * (min_rtt / 1_000_000.0) / self.payload_size
        # High gain: keep multiple BDPs in flight (FASP-like aggressiveness).
        return max(16384, int(bdp * 6.0))

    def _offer_bw(self, now: float, sample_bps: float) -> None:
        if sample_bps <= 0:
            return
        self._bw_filter.append((now, sample_bps))
        min_rtt_s = (self.base_rtt_us or 40_000.0) / 1_000_000.0
        horizon = min(8.0, max(1.5, 8.0 * min_rtt_s))
        while self._bw_filter and now - self._bw_filter[0][0] > horizon:
            self._bw_filter.popleft()
        self.btlbw = max(s for _, s in self._bw_filter)
        if self.btlbw > self.peak_bw:
            self.peak_bw = self.btlbw

    def _queue_us(self) -> float:
        if not self.base_rtt_us or not self.srtt_us:
            return 0.0
        return max(0.0, self.srtt_us - self.base_rtt_us)

    def _ramp_frac(self, now: float) -> float:
        if self._ramp_done or self.ramp_s <= 0.0:
            return 1.0
        elapsed = now - self._t0
        if elapsed >= self.ramp_s:
            self._ramp_done = True
            self.slow_start = False
            self.limiter.min_rate = self._saved_min
            return 1.0
        # Ease-in cubic: slow at start, accelerates toward the end.
        t = max(0.0, min(1.0, elapsed / self.ramp_s))
        return t * t * t

    def _update_pacing(self, now: float) -> None:
        q_us = self._queue_us()
        frac = self._ramp_frac(now)
        # Ceiling climbs 0 → max during ramp; Soft bias applies on top.
        cap = self.limiter.max_rate * frac
        if frac < 1.0 and cap < 1.0:
            cap = 1.0

        if q_us > 120_000:
            self.mode = self.MODE_SOFT
            self.limiter.set_rate(cap * 0.92)
            return
        if q_us > 60_000:
            self.mode = self.MODE_SOFT
            self.limiter.set_rate(cap * 0.97)
            return

        if frac < 1.0:
            self.mode = self.MODE_RAMP
            self.slow_start = True
        else:
            self.mode = self.MODE_BLAST
            self.slow_start = False
        self.limiter.set_rate(cap)

    def stats(self) -> dict:
        return {
            "rtt_us": self.last_rtt_us,
            "srtt_us": self.srtt_us or 0.0,
            "base_rtt_us": self.base_rtt_us or 0.0,
            "queue_us": self._queue_us(),
            "samples": self.samples,
            "slow_start": self.slow_start,
            "mode": self.mode,
            "btlbw": self.btlbw,
            "peak_bw": self.peak_bw,
            "cwnd": self.target_cwnd_packets(),
            "rate": self.limiter.rate,
            "max_rate": self.limiter.max_rate,
            "ramp_s": self.ramp_s,
        }


BbrController = DelayRateController
