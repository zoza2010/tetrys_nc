"""BBR-style pacing: delay + delivery-rate; loss is not a congestion signal."""

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
        # Low floor — BBR must be able to cut when the pipe is full
        self.min_rate = min(self.max_rate, max(self.max_rate * 0.02, 1_000_000.0))
        if start_bps is None:
            start_bps = min(self.max_rate, max(self.max_rate * 0.25, 8_000_000.0))
        self.rate = min(self.max_rate, max(start_bps, self.min_rate))
        self.burst = burst if burst is not None else max(self.rate * 0.05, 100_000.0)
        self.tokens = self.burst
        self.updated = time.monotonic()

    def set_rate(self, rate_bps: float) -> None:
        self.rate = min(self.max_rate, max(rate_bps, self.min_rate))
        self.burst = max(self.rate * 0.05, 100_000.0)

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
    Simplified BBR:

    - Estimate BtlBw from ACK delivery rate (max filter)
    - Track min RTT from packet echoes
    - Pace at BtlBw × gain; cwnd ≈ BDP × gain
    - Loss / PLR never cuts the rate (repair separately)
    - Queue (RTT ≫ min_RTT) drains with gain < 1
    """

    MODE_STARTUP = "Startup"
    MODE_DRAIN = "Drain"
    MODE_PROBE_BW = "ProbeBW"
    MODE_PROBE_RTT = "ProbeRTT"

    # ProbeBW gain cycle (BBR-ish)
    _PROBE_GAINS = (1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

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
        "payload_size",
        "_delivered",
        "_sample_t",
        "_sample_delivered",
        "_bw_filter",
        "_full_bw",
        "_full_bw_count",
        "_cycle_idx",
        "_cycle_t",
        "_probe_rtt_t",
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
        self._delivered = 0.0
        self._sample_t = time.monotonic()
        self._sample_delivered = 0.0
        # (time, bps) samples for max filter (~10 RTTs, min 1s)
        self._bw_filter: deque[tuple[float, float]] = deque()
        self._full_bw = 0.0
        self._full_bw_count = 0
        self._cycle_idx = 0
        self._cycle_t = time.monotonic()
        self._probe_rtt_t = time.monotonic()
        self._min_rtt_stamp = time.monotonic()

    def on_loss(self, plr_byte: int) -> None:
        """Loss is NOT congestion — ignore for pacing."""
        return

    def on_ack(self, acked: int, plr_byte: int = 0) -> None:
        """Update delivery-rate estimate from newly cumulative-acked symbols."""
        if acked <= 0:
            return
        now = time.monotonic()
        self._delivered += acked * self.payload_size
        dt = now - self._sample_t
        if dt >= 0.03:
            delta = self._delivered - self._sample_delivered
            if delta > 0:
                sample = delta / dt
                self._offer_bw(now, sample)
            self._sample_t = now
            self._sample_delivered = self._delivered
            self._update_pacing(now)

    def on_echo(self, echo_ts_us: int, now_us: int | None = None) -> float | None:
        if not echo_ts_us:
            return None
        if now_us is None:
            now_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
        rtt = (now_us - echo_ts_us) & 0xFFFFFFFF
        if rtt < 100 or rtt > 5_000_000:
            return None

        now = time.monotonic()
        self.last_rtt_us = float(rtt)
        self.samples += 1

        # Refresh min RTT every 10s (BBR ProbeRTT window)
        if self.base_rtt_us is None or rtt < self.base_rtt_us:
            self.base_rtt_us = float(rtt)
            self._min_rtt_stamp = now
        elif now - self._min_rtt_stamp > 10.0:
            # expire stale min; take current as new baseline
            self.base_rtt_us = float(rtt)
            self._min_rtt_stamp = now

        if self.srtt_us is None:
            self.srtt_us = float(rtt)
        else:
            a = self.alpha
            self.srtt_us = (1.0 - a) * self.srtt_us + a * float(rtt)

        self._update_pacing(now)
        return float(rtt)

    def target_cwnd_packets(self) -> int:
        """Inflight target ≈ BDP × cwnd_gain (packets)."""
        min_rtt = self.base_rtt_us or self.srtt_us
        bw = self.btlbw if self.btlbw > 0 else self.limiter.rate
        if not min_rtt or min_rtt <= 0:
            return 4096
        bdp = bw * (min_rtt / 1_000_000.0) / self.payload_size
        gain = 2.0 if self.mode == self.MODE_STARTUP else 1.5
        if self.mode == self.MODE_DRAIN:
            gain = 1.0
        if self.mode == self.MODE_PROBE_RTT:
            gain = 0.5
        return max(256, int(bdp * gain))

    def _offer_bw(self, now: float, sample_bps: float) -> None:
        if sample_bps <= 0:
            return
        self._bw_filter.append((now, sample_bps))
        # Keep ~10 RTTs of samples (floor 1s, cap 10s)
        min_rtt_s = (self.base_rtt_us or 80_000.0) / 1_000_000.0
        horizon = min(10.0, max(1.0, 10.0 * min_rtt_s))
        while self._bw_filter and now - self._bw_filter[0][0] > horizon:
            self._bw_filter.popleft()
        self.btlbw = max(s for _, s in self._bw_filter)

    def _queue_ratio(self) -> float:
        if not self.base_rtt_us or not self.srtt_us:
            return 1.0
        return self.srtt_us / self.base_rtt_us

    def _update_pacing(self, now: float) -> None:
        q = self._queue_ratio()
        bw = self.btlbw if self.btlbw > 0 else self.limiter.rate

        if self.mode == self.MODE_STARTUP:
            self.slow_start = True
            # Exit startup when BW plateaus OR standing queue appears
            if self.btlbw > 0:
                if self.btlbw > self._full_bw * 1.25:
                    self._full_bw = self.btlbw
                    self._full_bw_count = 0
                else:
                    self._full_bw_count += 1
            if q > 1.25 or self._full_bw_count >= 3:
                self.mode = self.MODE_DRAIN
                self.slow_start = False
                self.limiter.set_rate(min(self.limiter.max_rate, bw * 0.75))
                return
            # Startup gain ~2× estimated BW (capped)
            target = max(bw * 2.0, self.limiter.rate * 1.25) if bw > 0 else self.limiter.rate * 1.5
            self.limiter.set_rate(min(self.limiter.max_rate, target))
            return

        if self.mode == self.MODE_DRAIN:
            self.slow_start = False
            self.limiter.set_rate(min(self.limiter.max_rate, max(bw * 0.75, self.limiter.min_rate)))
            if q < 1.1:
                self.mode = self.MODE_PROBE_BW
                self._cycle_idx = 0
                self._cycle_t = now
            return

        # Periodic ProbeRTT: briefly cut inflight to refresh min_rtt
        if now - self._probe_rtt_t > 10.0 and self.mode == self.MODE_PROBE_BW:
            self.mode = self.MODE_PROBE_RTT
            self._probe_rtt_t = now
            self.limiter.set_rate(min(self.limiter.max_rate, max(bw * 0.5, self.limiter.min_rate)))
            return

        if self.mode == self.MODE_PROBE_RTT:
            if now - self._probe_rtt_t > max(0.2, (self.base_rtt_us or 80_000) / 1e6 * 2):
                self.mode = self.MODE_PROBE_BW
                self._cycle_t = now
            else:
                self.limiter.set_rate(min(self.limiter.max_rate, max(bw * 0.5, self.limiter.min_rate)))
            return

        # ProbeBW
        self.slow_start = False
        min_rtt_s = (self.base_rtt_us or 80_000.0) / 1_000_000.0
        if now - self._cycle_t >= max(min_rtt_s, 0.05):
            self._cycle_idx = (self._cycle_idx + 1) % len(self._PROBE_GAINS)
            self._cycle_t = now
        gain = self._PROBE_GAINS[self._cycle_idx]
        # Extra drain if queue grows (delay-based, not loss-based)
        if q > 1.2:
            gain = min(gain, 0.75)
        elif q > 1.5:
            gain = 0.5
        target = bw * gain if bw > 0 else self.limiter.rate
        self.limiter.set_rate(min(self.limiter.max_rate, max(target, self.limiter.min_rate)))

    def stats(self) -> dict:
        return {
            "rtt_us": self.last_rtt_us,
            "srtt_us": self.srtt_us or 0.0,
            "base_rtt_us": self.base_rtt_us or 0.0,
            "queue_us": max(0.0, (self.srtt_us or 0.0) - (self.base_rtt_us or 0.0)),
            "samples": self.samples,
            "slow_start": self.slow_start,
            "mode": self.mode,
            "btlbw": self.btlbw,
            "cwnd": self.target_cwnd_packets(),
            "rate": self.limiter.rate,
            "max_rate": self.limiter.max_rate,
        }


# Back-compat alias
BbrController = DelayRateController
