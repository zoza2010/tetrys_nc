"""Rate search: BBR-like phases, filtered delivery, no channel cap."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .block_state import WAN_ACTIVE_BYTES

STARTUP = "startup"
DRAIN = "drain"
CRUISE = "cruise"
PROBE = "probe"

# WAN jitter on Russia↔Spain is tens of ms; DC-like 15ms trips false drain.
_QDELAY_STOP_S = 0.040
_QDELAY_DRAIN_OK_S = 0.015
_QDELAY_CUT_S = 0.050
_QDELAY_HOLD = 4
_MIN_RTT_HOLD_S = 10.0
_MIN_RTT_SAMPLES = 8
_SRTT_ALPHA = 0.125
_STARTUP_GAIN = 2.0 / math.log(2)
_PROBE_GAIN = 1.10
_PROBE_WAIT_S = 1.0
_DRAIN_GAIN = 0.75
_DRAIN_MAX_CUTS = 3
_DRAIN_MAX_S = 1.5
_CRUISE_CUT = 0.93
_GOOD_FLOOR = 0.90
_SEED_FRAC = 0.90
_ABS_MIN_MBIT = 80.0
# Unique-byte rate is goodput, not wire; 1.25 sat below the 850 start and blocked probe.
_DELIVERY_HEADROOM = 1.42
_STEP_MAX = 1.25
_INFLIGHT_GAIN = 1.25
_BW_WINDOW = 16
_BW_MIN_SAMPLES = 3
_EXTRA_OK = 0.04
_EXTRA_CUT = 0.08
_DECODE_KEEPUP = 0.70


def rtt_from_echo(now_s: float, echo_us: int) -> float | None:
    if echo_us <= 0:
        return None
    now_us = int(now_s * 1_000_000) & 0xFFFFFFFF
    delta = (now_us - echo_us) & 0xFFFFFFFF
    if delta < 1_000 or delta > 2_000_000:
        return None
    return delta / 1_000_000.0


@dataclass
class RttFilter:
    """min_rtt window + SRTT; qdelay is 0 until enough samples."""

    min_rtt: float | None = None
    srtt: float | None = None
    n: int = 0
    _min_at: float = 0.0

    def observe(self, now: float, rtt: float | None) -> float:
        if rtt is None:
            if self.srtt is None or self.min_rtt is None or self.n < _MIN_RTT_SAMPLES:
                return 0.0
            return max(0.0, self.srtt - self.min_rtt)
        self.n += 1
        if self.min_rtt is None or rtt < self.min_rtt:
            self.min_rtt = rtt
            self._min_at = now
        elif now - self._min_at >= _MIN_RTT_HOLD_S:
            self.min_rtt = rtt if self.srtt is None else min(rtt, self.srtt)
            self._min_at = now
        if self.srtt is None:
            self.srtt = rtt
        else:
            self.srtt += _SRTT_ALPHA * (rtt - self.srtt)
        if self.n < _MIN_RTT_SAMPLES or self.min_rtt is None or self.srtt is None:
            return 0.0
        return max(0.0, self.srtt - self.min_rtt)


@dataclass
class BwFilter:
    """Delivery samples; max_bw is second-highest so one ACK spike is ignored."""

    samples: list[float] = field(default_factory=list)
    window: int = _BW_WINDOW

    def observe(self, bps: float) -> None:
        if bps <= 0:
            return
        self.samples.append(bps)
        if len(self.samples) > self.window:
            del self.samples[0]

    @property
    def max_bw(self) -> float | None:
        if len(self.samples) < _BW_MIN_SAMPLES:
            return None
        xs = sorted(self.samples)
        return xs[-2]


@dataclass
class BlastCc:
    max_bps: float
    start_bps: float
    min_bps: float = 0.0
    active_bytes: int = WAN_ACTIVE_BYTES
    rate: float = 0.0
    last_good: float = 0.0
    phase: str = STARTUP
    min_rtt: float | None = None
    last_unique: int = 0
    last_decoded: int = 0
    last_ts: float | None = None
    last_delivery: float = 0.0
    recv_lag: bool = False
    rtt: RttFilter = field(default_factory=RttFilter)
    bw: BwFilter = field(default_factory=BwFilter)
    high_delay_n: int = 0
    low_delay_n: int = 0
    last_step_ts: float = 0.0
    cruise_ts: float = 0.0
    drain_ts: float = 0.0
    drain_cuts: int = 0
    probe_until: float = 0.0
    probe_base: float = 0.0
    last_fb: int = -1

    def __post_init__(self) -> None:
        abs_min = _ABS_MIN_MBIT * 1_000_000 / 8
        self.min_bps = max(abs_min, self.min_bps)
        seed = max(self.min_bps, min(self.max_bps, self.start_bps * _SEED_FRAC))
        self.rate = seed
        self.last_good = seed

    def _inflight_ceiling(self) -> float:
        rtt = self.min_rtt if self.min_rtt and self.min_rtt > 0 else 0.08
        return max(self.min_bps, self.active_bytes / (rtt * _INFLIGHT_GAIN))

    def _rate_ceiling(self) -> float:
        hard = min(self.max_bps, self._inflight_ceiling())
        if self.phase == STARTUP:
            return min(hard, self.start_bps)
        bw = self.bw.max_bw
        if bw is None:
            return hard
        return min(hard, max(self.start_bps, bw * _DELIVERY_HEADROOM))

    def _clip(self, rate: float) -> float:
        floor = max(self.min_bps, self.last_good * _GOOD_FLOOR)
        return min(self._rate_ceiling(), max(floor, rate))

    def _nudge(self, gain: float) -> float:
        target = min(self.rate * gain, self.rate * _STEP_MAX)
        return self._clip(target)

    def _enter_cruise(self, now: float) -> None:
        self.phase = CRUISE
        self.last_good = max(self.last_good, self.rate)
        self.cruise_ts = now
        self.last_step_ts = now
        self.high_delay_n = 0
        self.low_delay_n = 0
        self.drain_cuts = 0

    def _step_s(self) -> float:
        if self.min_rtt is None:
            return 0.10
        return max(0.08, min(0.20, 1.25 * self.min_rtt))

    def on_timer(self, now: float) -> float:
        """Startup can climb while send_wires blocks the feedback snapshot."""
        if self.phase != STARTUP:
            return self.rate
        ceiling = self._rate_ceiling()
        if self.rate >= ceiling * 0.98:
            qdelay = 0.0
            if self.rtt.n >= _MIN_RTT_SAMPLES:
                qdelay = self.rtt.observe(now, None)
            if qdelay < _QDELAY_STOP_S:
                self._enter_cruise(now)
            return self.rate
        if now - self.last_step_ts >= self._step_s():
            self.last_step_ts = now
            self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
        return self.rate

    def _observe_delivery(
        self, now: float, unique_bytes: int, decoded_bytes: int
    ) -> None:
        self.recv_lag = False
        if self.last_ts is None:
            self.last_unique = unique_bytes
            self.last_decoded = decoded_bytes
            self.last_ts = now
            return
        dt = now - self.last_ts
        min_dt = 0.5 * (self.min_rtt or 0.08)
        if dt < min_dt:
            return
        du = unique_bytes - self.last_unique
        dd = decoded_bytes - self.last_decoded
        self.last_unique = unique_bytes
        self.last_decoded = decoded_bytes
        self.last_ts = now
        if du <= 0:
            return
        unique_rate = du / dt
        decoded_rate = max(0.0, dd) / dt if dd > 0 else 0.0
        self.last_delivery = unique_rate
        if decoded_rate <= 0 or decoded_rate < unique_rate * _DECODE_KEEPUP:
            self.recv_lag = True
            return
        self.bw.observe(min(unique_rate, decoded_rate))

    def on_feedback(
        self,
        now: float,
        *,
        feedback_id: int,
        unique_bytes: int,
        decoded_bytes: int,
        echo_ts_us: int,
        extra_frac: float,
        window_full: bool,
    ) -> float:
        if feedback_id <= self.last_fb:
            return self.rate
        self.last_fb = feedback_id
        raw_rtt = rtt_from_echo(now, echo_ts_us)
        qdelay = self.rtt.observe(now, raw_rtt)
        self.min_rtt = self.rtt.min_rtt
        self._observe_delivery(now, unique_bytes, decoded_bytes)
        recv_lag = bool(window_full and self.recv_lag)
        step_s = 0.16 if self.min_rtt is None else max(0.12, min(0.40, 2.0 * self.min_rtt))

        if qdelay >= _QDELAY_CUT_S:
            self.high_delay_n += 1
            self.low_delay_n = 0
        elif qdelay <= _QDELAY_DRAIN_OK_S:
            self.low_delay_n += 1
            self.high_delay_n = 0
        else:
            self.high_delay_n = 0

        if self.phase == STARTUP:
            ceiling = self._rate_ceiling()
            at_cap = self.rate >= ceiling * 0.98
            if qdelay >= _QDELAY_STOP_S:
                if self.high_delay_n >= _QDELAY_HOLD:
                    self.phase = DRAIN
                    self.drain_ts = now
                    self.drain_cuts = 1
                    self.rate = self._clip(self.rate * _DRAIN_GAIN)
                    self.last_step_ts = now
            elif at_cap:
                self._enter_cruise(now)
            elif now - self.last_step_ts >= step_s:
                self.last_step_ts = now
                self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
        elif self.phase == DRAIN:
            drained_long = now - self.drain_ts >= _DRAIN_MAX_S
            if self.low_delay_n >= 2 or drained_long:
                self._enter_cruise(now)
            elif now - self.last_step_ts >= step_s and self.drain_cuts < _DRAIN_MAX_CUTS:
                self.last_step_ts = now
                self.drain_cuts += 1
                self.rate = self._clip(self.rate * _DRAIN_GAIN)
        elif self.phase == CRUISE:
            cong = self.high_delay_n >= _QDELAY_HOLD
            if cong:
                self.phase = DRAIN
                self.drain_ts = now
                self.drain_cuts = 1
                self.rate = self._clip(self.rate * _CRUISE_CUT)
                self.last_step_ts = now
                self.high_delay_n = 0
            elif extra_frac >= _EXTRA_CUT:
                self.rate = self._clip(self.rate * _CRUISE_CUT)
                self.last_good = min(self.last_good, self.rate)
            elif (
                not recv_lag
                and extra_frac < _EXTRA_OK
                and qdelay < _QDELAY_STOP_S
                and now - self.cruise_ts >= _PROBE_WAIT_S
                and self.rate < self._rate_ceiling() * 0.98
            ):
                self.phase = PROBE
                self.probe_base = self.rate
                self.rate = min(self._rate_ceiling(), self._nudge(_PROBE_GAIN))
                self.probe_until = now + max(0.32, 4.0 * (self.min_rtt or 0.08))
        elif self.phase == PROBE:
            if extra_frac >= _EXTRA_OK or self.high_delay_n >= _QDELAY_HOLD or recv_lag:
                self.rate = self.probe_base
                self._enter_cruise(now)
            elif now >= self.probe_until:
                better = (
                    qdelay < _QDELAY_STOP_S
                    and self.high_delay_n == 0
                    and extra_frac < _EXTRA_OK
                    and not recv_lag
                )
                if better:
                    self.last_good = self.rate
                else:
                    self.rate = self.probe_base
                self._enter_cruise(now)

        self.rate = self._clip(self.rate)
        return self.rate
