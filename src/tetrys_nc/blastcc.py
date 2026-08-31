"""Rate search for RaptorQ blast: BBR-like startup, delay drain, no loss backoff."""

from __future__ import annotations

from dataclasses import dataclass, field

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
_STARTUP_GAIN = 1.25
_PROBE_GAIN = 1.03
_PROBE_WAIT_S = 2.0
_DRAIN_GAIN = 1.0 / _STARTUP_GAIN
_DRAIN_MAX_CUTS = 3
_DRAIN_MAX_S = 1.5
_CRUISE_CUT = 0.93
_GOOD_FLOOR = 0.90
_SEED_FRAC = 0.90
_ABS_MIN_MBIT = 80.0
# WAN at 984 Mbit was still first_close=100%; 1139+ blew extra-repair.
_DELIVERY_HEADROOM = 1.42
_EXTRA_OK = 0.04
_EXTRA_CUT = 0.08


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
class BlastCc:
    max_bps: float
    start_bps: float
    min_bps: float = 0.0
    rate: float = 0.0
    last_good: float = 0.0
    phase: str = STARTUP
    min_rtt: float | None = None
    last_unique: int = 0
    last_decoded: int = 0
    last_ts: float | None = None
    last_delivery: float = 0.0
    rtt: RttFilter = field(default_factory=RttFilter)
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

    def _clip(self, rate: float) -> float:
        floor = max(self.min_bps, self.last_good * _GOOD_FLOOR)
        return min(self.max_bps, max(floor, rate))

    def _climb_ceiling(self) -> float:
        """Startup only fills the hint; probes search up to safety cap."""
        return min(self.max_bps, self.start_bps)

    def _path_ceiling(self) -> float:
        if self.last_delivery <= 1_000_000.0:
            return self.max_bps
        return min(
            self.max_bps,
            max(self.start_bps, self.last_delivery * _DELIVERY_HEADROOM),
        )

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
        ceiling = self._climb_ceiling()
        if self.rate >= ceiling * 0.98:
            qdelay = 0.0
            if self.rtt.n >= _MIN_RTT_SAMPLES:
                qdelay = self.rtt.observe(now, None)
            if qdelay < _QDELAY_STOP_S:
                self._enter_cruise(now)
            return self.rate
        if now - self.last_step_ts >= self._step_s():
            self.last_step_ts = now
            self.rate = min(ceiling, self.rate * _STARTUP_GAIN)
        return self.rate

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

        dt = 0.0 if self.last_ts is None else max(1e-3, now - self.last_ts)
        delivery = 0.0
        decoded_rate = 0.0
        if self.last_ts is not None:
            delivery = max(0.0, unique_bytes - self.last_unique) / dt
            decoded_rate = max(0.0, decoded_bytes - self.last_decoded) / dt
        self.last_unique = unique_bytes
        self.last_decoded = decoded_bytes
        self.last_ts = now
        if delivery > 0:
            self.last_delivery = delivery

        recv_lag = window_full and decoded_rate > 0 and delivery > decoded_rate * 1.35
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
            ceiling = self._climb_ceiling()
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
                self.rate = min(ceiling, self.rate * _STARTUP_GAIN)
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
                and self.rate < self.max_bps * 0.98
            ):
                self.phase = PROBE
                self.probe_base = self.rate
                ceiling = self._path_ceiling()
                self.rate = min(self.max_bps, ceiling, self.rate * _PROBE_GAIN)
                self.probe_until = now + max(0.32, 4.0 * (self.min_rtt or 0.08))
        elif self.phase == PROBE:
            if extra_frac >= _EXTRA_OK or self.high_delay_n >= _QDELAY_HOLD:
                self.rate = self.probe_base
                self._enter_cruise(now)
            elif now >= self.probe_until:
                better = (
                    qdelay < _QDELAY_STOP_S
                    and self.high_delay_n == 0
                    and extra_frac < _EXTRA_OK
                )
                if better:
                    self.last_good = self.rate
                else:
                    self.rate = self.probe_base
                self._enter_cruise(now)

        if self.phase != STARTUP:
            self.rate = min(self.rate, self._path_ceiling())
        self.rate = min(self.max_bps, max(self.min_bps, self.rate))
        return self.rate
