"""BBR-style pacing for lossy WAN: delay + delivery-rate; loss ≠ congestion."""

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
        # Floor high enough to fill a long-RTT pipe; BBR must not crawl
        self.min_rate = min(self.max_rate, max(self.max_rate * 0.12, 8_000_000.0))
        if start_bps is None:
            start_bps = min(self.max_rate, max(self.max_rate * 0.5, 16_000_000.0))
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
    BBR-ish controller tuned for shallow-buffer WAN (iperf-like):

    - Loss/PLR never cuts pacing (repair is separate)
    - BtlBw from cumulative-ACK delivery rate (max filter + peak hold)
    - min_RTT ignores the first noisy samples (startup queue spike)
    - Drain only on clear standing queue (RTT ≫ min_RTT)
    - cwnd floored so a ~80ms path can carry hundreds of Mbit/s
    """

    MODE_STARTUP = "Startup"
    MODE_DRAIN = "Drain"
    MODE_PROBE_BW = "ProbeBW"

    # Milder than classic BBR — 0.75 drains killed goodput on this path
    _PROBE_GAINS = (1.25, 1.0, 1.0, 1.0, 0.9, 1.0, 1.0, 1.0)

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
        "_full_bw",
        "_full_bw_count",
        "_cycle_idx",
        "_cycle_t",
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
        self._full_bw = 0.0
        self._full_bw_count = 0
        self._cycle_idx = 0
        self._cycle_t = time.monotonic()
        self._rtt_warmup = 0  # skip first echoes for min_rtt
        self._min_rtt_stamp = time.monotonic()

    def on_loss(self, plr_byte: int) -> None:
        """Loss is NOT congestion — ignore for pacing."""
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
        # Discard absurd samples (startup stamp skew / wrap)
        if rtt < 1_000 or rtt > 2_000_000:
            return None

        now = time.monotonic()
        self.last_rtt_us = float(rtt)
        self.samples += 1
        self._rtt_warmup += 1

        # First echoes often include sender queue — don't poison min_rtt
        if self._rtt_warmup >= 4:
            if self.base_rtt_us is None or rtt < self.base_rtt_us:
                self.base_rtt_us = float(rtt)
                self._min_rtt_stamp = now
            elif now - self._min_rtt_stamp > 10.0:
                # slowly allow min_rtt to rise if path changed
                self.base_rtt_us = min(self.base_rtt_us * 1.05, float(rtt))
                self._min_rtt_stamp = now

        if self.srtt_us is None:
            self.srtt_us = float(rtt)
        else:
            a = self.alpha
            self.srtt_us = (1.0 - a) * self.srtt_us + a * float(rtt)

        self._update_pacing(now)
        return float(rtt)

    def target_cwnd_packets(self) -> int:
        """Inflight ≈ BDP×gain, floored for WAN BDP at a useful rate."""
        min_rtt = self.base_rtt_us or self.srtt_us or 80_000.0
        # Use peak/max pacing so cwnd doesn't collapse when ACK rate dips on HOL
        bw = max(self.btlbw, self.peak_bw * 0.85, self.limiter.rate)
        bdp = bw * (min_rtt / 1_000_000.0) / self.payload_size
        gain = 2.5 if self.mode == self.MODE_STARTUP else 2.0
        if self.mode == self.MODE_DRAIN:
            gain = 1.5
        # Floor: enough for ~200 Mbit @ 80ms ≈ 1500 pkts; aim ≥ half-cap BDP
        floor_bw = max(self.limiter.min_rate, self.limiter.max_rate * 0.35)
        floor = int(floor_bw * (min_rtt / 1_000_000.0) / self.payload_size * 2.0)
        floor = max(4096, floor)
        return max(floor, int(bdp * gain))

    def _offer_bw(self, now: float, sample_bps: float) -> None:
        if sample_bps <= 0:
            return
        self._bw_filter.append((now, sample_bps))
        min_rtt_s = (self.base_rtt_us or 80_000.0) / 1_000_000.0
        # Longer filter — HOL makes short windows too pessimistic
        horizon = min(15.0, max(2.0, 12.0 * min_rtt_s))
        while self._bw_filter and now - self._bw_filter[0][0] > horizon:
            self._bw_filter.popleft()
        self.btlbw = max(s for _, s in self._bw_filter)
        if self.btlbw > self.peak_bw:
            self.peak_bw = self.btlbw

    def _queue_us(self) -> float:
        if not self.base_rtt_us or not self.srtt_us:
            return 0.0
        return max(0.0, self.srtt_us - self.base_rtt_us)

    def _queue_ratio(self) -> float:
        if not self.base_rtt_us or not self.srtt_us:
            return 1.0
        return self.srtt_us / self.base_rtt_us

    def _pacing_floor(self) -> float:
        # Hold near peak once we've seen real throughput (iperf-class paths)
        if self.peak_bw > self.limiter.min_rate:
            return max(self.limiter.min_rate, self.peak_bw * 0.7)
        return self.limiter.min_rate

    def _update_pacing(self, now: float) -> None:
        q = self._queue_ratio()
        q_us = self._queue_us()
        bw = max(self.btlbw, self.peak_bw * 0.8) if self.peak_bw > 0 else self.btlbw
        floor = self._pacing_floor()

        if self.mode == self.MODE_STARTUP:
            self.slow_start = True
            # Climb hard toward cap (like filling the pipe in iperf)
            target = min(self.limiter.max_rate, max(self.limiter.rate * 1.35, (bw or 0) * 2.0))
            if target < self.limiter.rate:
                target = min(self.limiter.max_rate, self.limiter.rate * 1.2)
            self.limiter.set_rate(max(target, floor))

            growing = self.btlbw > self._full_bw * 1.2
            if growing:
                self._full_bw = self.btlbw
                self._full_bw_count = 0
            elif self.btlbw > 0:
                self._full_bw_count += 1

            # Exit only with clear queue OR BW plateau near a useful rate
            useful = self.btlbw >= self.limiter.max_rate * 0.25 or self.limiter.rate >= self.limiter.max_rate * 0.9
            heavy_queue = q > 1.5 and q_us > 40_000  # >40ms standing queue
            plateau = self._full_bw_count >= 5 and useful
            if heavy_queue or plateau:
                self.mode = self.MODE_DRAIN if heavy_queue else self.MODE_PROBE_BW
                self.slow_start = False
                if heavy_queue:
                    self.limiter.set_rate(max(floor, min(self.limiter.max_rate, bw * 0.9)))
                return
            return

        if self.mode == self.MODE_DRAIN:
            self.slow_start = False
            self.limiter.set_rate(max(floor, min(self.limiter.max_rate, bw * 0.85)))
            if q < 1.2 or q_us < 20_000:
                self.mode = self.MODE_PROBE_BW
                self._cycle_idx = 0
                self._cycle_t = now
            return

        # ProbeBW (no harsh ProbeRTT — it collapsed this path to ~2 MiB/s)
        self.slow_start = False
        min_rtt_s = (self.base_rtt_us or 80_000.0) / 1_000_000.0
        if now - self._cycle_t >= max(min_rtt_s, 0.1):
            self._cycle_idx = (self._cycle_idx + 1) % len(self._PROBE_GAINS)
            self._cycle_t = now
        gain = self._PROBE_GAINS[self._cycle_idx]
        # Delay-based ease only on real standing queue — not on loss
        if q > 1.5 and q_us > 40_000:
            gain = min(gain, 0.85)
            self.mode = self.MODE_DRAIN
        target = (bw * gain) if bw > 0 else self.limiter.rate
        # If we're below cap and queue is fine, keep probing up
        if q < 1.25 and q_us < 25_000 and self.limiter.rate < self.limiter.max_rate * 0.95:
            target = max(target, self.limiter.rate * 1.05, floor)
        self.limiter.set_rate(min(self.limiter.max_rate, max(target, floor)))

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
