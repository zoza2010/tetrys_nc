"""Delay-based pacing (BBR-like): queue delay steers rate; loss ≠ congestion."""

from __future__ import annotations

import time
from collections import deque


class RateLimiter:
    """Token-bucket pacing in bytes/sec with a hard ceiling."""

    def __init__(
        self,
        max_bps: float,
        start_bps: float | None = None,
        burst: float | None = None,
    ) -> None:
        self.max_rate = max(max_bps, 1.0)
        # Allow real drain when RTT blows up (was 50% floor → stuck at 64 MiB/s)
        self.min_rate = min(self.max_rate, max(self.max_rate * 0.08, 4_000_000.0))
        if start_bps is None:
            start_bps = min(self.max_rate, max(self.max_rate * 0.35, 12_000_000.0))
        self.rate = min(self.max_rate, max(start_bps, self.min_rate))
        self.burst = burst if burst is not None else max(self.rate * 0.08, 200_000.0)
        self.tokens = self.burst
        self.updated = time.monotonic()

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * 0.08, 200_000.0)

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
    Delay-based controller (loss ignored for pacing):

    - Track min_RTT (skip first noisy echoes)
    - Estimate delivery rate from cumulative ACK
    - Standing queue (srtt − min_rtt) → cut toward delivery rate
    - Clear path → climb toward --rate
    """

    MODE_STARTUP = "Startup"
    MODE_DRAIN = "Drain"
    MODE_PROBE_BW = "ProbeBW"

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
        "_delivered",
        "_sample_t",
        "_sample_delivered",
        "_bw_filter",
        "_rtt_warmup",
        "_min_rtt_stamp",
    )

    def __init__(
        self,
        limiter: RateLimiter,
        alpha: float = 0.125,
        payload_size: int = 1350,
    ) -> None:
        self.limiter = limiter
        self.payload_size = max(1, payload_size)
        self.base_rtt_us: float | None = None
        self.srtt_us: float | None = None
        self.last_rtt_us: float = 0.0
        self.alpha = alpha
        self.samples = 0
        self.slow_start = True
        self.mode = self.MODE_STARTUP
        self.btlbw = 0.0
        self.peak_bw = 0.0
        self._delivered = 0.0
        self._sample_t = time.monotonic()
        self._sample_delivered = 0.0
        self._bw_filter: deque[tuple[float, float]] = deque()
        self._rtt_warmup = 0
        self._min_rtt_stamp = time.monotonic()

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

        # min_RTT: ignore first echoes; also ignore extreme spikes for baseline
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
        min_rtt = self.base_rtt_us or self.srtt_us or 80_000.0
        bw = max(self.limiter.rate, self.btlbw, self.peak_bw * 0.5)
        bdp = bw * (min_rtt / 1_000_000.0) / self.payload_size
        gain = 2.0 if self.mode != self.MODE_DRAIN else 1.25
        return max(2048, int(bdp * gain))

    def _offer_bw(self, now: float, sample_bps: float) -> None:
        if sample_bps <= 0:
            return
        self._bw_filter.append((now, sample_bps))
        min_rtt_s = (self.base_rtt_us or 80_000.0) / 1_000_000.0
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

    def _update_pacing(self, now: float) -> None:
        q_us = self._queue_us()
        bw = self.btlbw if self.btlbw > 0 else 0.0
        floor = self.limiter.min_rate

        if self.mode == self.MODE_STARTUP:
            self.slow_start = True
            if q_us > 25_000:
                self.mode = self.MODE_DRAIN
                self.slow_start = False
                target = max(floor, bw * 0.9) if bw > 0 else self.limiter.rate * 0.5
                self.limiter.set_rate(target)
                return
            self.limiter.set_rate(
                min(self.limiter.max_rate, max(self.limiter.rate * 1.3, self.limiter.rate + 2_000_000))
            )
            if self.limiter.rate >= self.limiter.max_rate * 0.95 and self.samples >= 6:
                self.mode = self.MODE_PROBE_BW
                self.slow_start = False
            return

        # Real drain: match delivery rate when we built a standing queue
        if q_us > 40_000:
            self.mode = self.MODE_DRAIN
            self.slow_start = False
            if bw > 0:
                target = bw * 0.85
            else:
                target = self.limiter.rate * 0.7
            self.limiter.set_rate(max(floor, min(self.limiter.max_rate, target)))
            return

        if q_us > 15_000:
            self.mode = self.MODE_PROBE_BW
            self.slow_start = False
            if bw > 0:
                target = min(self.limiter.rate, max(bw * 1.05, floor))
            else:
                target = self.limiter.rate * 0.95
            self.limiter.set_rate(max(floor, min(self.limiter.max_rate, target)))
            return

        # Clear path — climb (loss ignored)
        self.mode = self.MODE_PROBE_BW
        self.slow_start = False
        self.limiter.set_rate(
            min(
                self.limiter.max_rate,
                max(self.limiter.rate * 1.08, self.limiter.rate + 1_000_000.0),
            )
        )

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
        }


BbrController = DelayRateController
