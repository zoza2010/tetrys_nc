"""Rate search: BBR-like phases, filtered delivery, no channel cap."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .block_state import WAN_ACTIVE_BYTES

STARTUP = "startup"
DRAIN = "drain"
CRUISE = "cruise"
PROBE = "probe"
MEASURE = "measure"

# Queue trip as a fraction of measured min_rtt (80 ms WAN ⇒ 40/15/50 ms).
_QDELAY_STOP_FRAC = 0.50
_QDELAY_DRAIN_OK_FRAC = 0.1875
_QDELAY_CUT_FRAC = 0.625
_QDELAY_STOP_MIN_S = 0.004
_QDELAY_DRAIN_OK_MIN_S = 0.002
_QDELAY_CUT_MIN_S = 0.006
_QDELAY_HOLD = 4
_MIN_RTT_HOLD_S = 10.0
_MIN_RTT_SAMPLES = 8
_SRTT_ALPHA = 0.125
_STARTUP_GAIN = 2.0 / math.log(2)
# Once unique exists, send at most this times delivery. 2.88× a burst
# sample is how Spain jumped 900 Mbit → 2.8 Gbit and never recovered.
_STARTUP_FOLLOW = 1.35
_PROBE_GAIN = 1.10
_PROBE_WAIT_S = 1.0
_DRAIN_GAIN = 0.75
_DRAIN_MAX_CUTS = 3
_DRAIN_MAX_S = 1.5
_CRUISE_CUT = 0.93
_GOOD_FLOOR = 0.90
_SEED_FRAC = 0.90
# Stall floor only. Operating point comes from delivery.
_ABS_MIN_MBIT = 8.0
# Bootstrap RTT before the first echo. Not a path guess for rate.
_RTT_BOOTSTRAP_S = 0.08
# Unique ESI is delivered payload, not source goodput.
_DELIVERY_HEADROOM = 1.15
_STEP_MAX = 1.25
_INFLIGHT_GAIN = 1.25
_BW_WINDOW = 16
_BW_MIN_SAMPLES = 3
# Unique-ESI rate is delivered payload. Send much faster than that with a
# flat RTT is a policer. extra/DIR is FEC's job, not a rate cut.
_POLICER_GAIN = 1.20
_POLICER_AIM = 1.10
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
    last_decoded_rate: float = 0.0
    probe_decoded: float = 0.0
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
    last_extra: float = 0.0
    last_sent: int = 0
    last_send_rate: float = 0.0
    last_source: int = 0
    last_source_rate: float = 0.0
    have_source: bool = False
    send_samples: list[tuple[float, float]] = field(default_factory=list)
    measure_restore: float = 0.0
    measure_good: float = 0.0
    measure_delivery: float = 0.0
    measure_until: float = 0.0
    measure_holdoff: float = 0.0
    measure_need_decode: bool = False
    _events: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        abs_min = _ABS_MIN_MBIT * 1_000_000 / 8
        self.min_bps = max(abs_min, self.min_bps)
        seed = max(self.min_bps, min(self.max_bps, self.start_bps * _SEED_FRAC))
        self.rate = seed
        self.last_good = seed

    def _rtt_s(self) -> float:
        if self.min_rtt and self.min_rtt > 0:
            return self.min_rtt
        return _RTT_BOOTSTRAP_S

    def _qdelay_stop(self) -> float:
        return max(_QDELAY_STOP_MIN_S, _QDELAY_STOP_FRAC * self._rtt_s())

    def _qdelay_ok(self) -> float:
        return max(_QDELAY_DRAIN_OK_MIN_S, _QDELAY_DRAIN_OK_FRAC * self._rtt_s())

    def _qdelay_cut(self) -> float:
        return max(_QDELAY_CUT_MIN_S, _QDELAY_CUT_FRAC * self._rtt_s())

    def _inflight_ceiling(self) -> float:
        return max(self.min_bps, self.active_bytes / (self._rtt_s() * _INFLIGHT_GAIN))

    def _rate_ceiling(self) -> float:
        hard = min(self.max_bps, self._inflight_ceiling())
        if self.phase == STARTUP:
            return self._startup_search_cap(hard)
        if self._pipe_starved():
            # Do not pin to a 22 Mbit first unique. That is one block / RTT.
            return min(hard, max(self.rate * _STEP_MAX, self._starved_fill_bps()))
        bw = self.bw.max_bw
        if bw is None:
            return hard
        # Raise ceiling with delivery; never use a low unique-bw sample to
        # yank the rate down. Cuts are delay/policer.
        return min(
            hard,
            max(
                self.min_bps,
                self.rate,
                bw * _DELIVERY_HEADROOM,
            ),
        )

    def _startup_search_cap(self, hard: float) -> float:
        bw = self.bw.max_bw
        if bw is None:
            # Hold near start. rate × 1.25^n is how the timer walked
            # 8 → 2.7 Gbit before three unique samples (Spain 8 MiB/s).
            return min(hard, max(self.min_bps, self.start_bps * _STEP_MAX))
        # Follow the filter. A single GRO/ACK burst is not C.
        ref = bw
        if 0 < self.last_delivery <= bw * 1.50:
            ref = max(ref, self.last_delivery)
        return min(hard, max(self.min_bps, ref * _STARTUP_FOLLOW))

    def _startup_ceiling(self) -> float:
        return self._rate_ceiling()

    @property
    def fec_may_raise(self) -> bool:
        """FEC may raise only after the path is classified, not while hunting C."""
        return self.phase in (CRUISE, PROBE) and not self._pipe_oversend()

    def pull_events(self) -> list[str]:
        ev, self._events = self._events, []
        return ev

    def _emit(self, msg: str) -> None:
        mbit = self.rate * 8 / 1_000_000
        snd = self.last_send_rate * 8 / 1_000_000
        unq = self.last_delivery * 8 / 1_000_000
        bw = self.bw.max_bw
        bw_m = (bw * 8 / 1_000_000) if bw else 0.0
        self._events.append(
            f"cc_trace {msg} phase={self.phase} rate={mbit:.0f} "
            f"snd={snd:.0f} unq={unq:.0f} bw={bw_m:.0f} "
            f"good={self.last_good * 8 / 1_000_000:.0f} "
            f"lag={int(self.recv_lag)} ov={int(self._pipe_oversend())}"
        )

    def _clip(self, rate: float) -> float:
        # Trial cuts must actually sit on delivery. last_good * 0.90 would
        # yank a 450 Mbit measure back to 765 and hide the result.
        if self.phase == MEASURE:
            return min(self._rate_ceiling(), max(self.min_bps, rate))
        floor = max(self.min_bps, self.last_good * _GOOD_FLOOR)
        return min(self._rate_ceiling(), max(floor, rate))

    def _delivery_bps(self) -> float | None:
        return self.bw.max_bw

    def _absorbed_bps(self) -> float | None:
        # Filtered unique, not the instantaneous sample. A HOL/window stall
        # drops last_delivery toward 0; min() treated that as a 8 Mbit cap.
        return self._delivery_bps()

    def _send_ref(self) -> float:
        return self.rate

    def _unique_cliff(self) -> bool:
        absorbed = self._absorbed_bps()
        if absorbed is None or absorbed <= 0 or self.last_delivery <= 0:
            return False
        return self.last_delivery < absorbed * 0.50

    def _send_paused(self) -> bool:
        """Encode/window pause: limiter is high, flush is not."""
        if self.rate <= 0 or self.last_send_rate <= 0:
            return False
        return self.last_send_rate < self.rate * 0.30

    def _admit_stall(self) -> bool:
        """HOL/window pause looks like qdelay. It is not a standing queue."""
        return self._unique_cliff() or self._send_paused()

    def _pipe_starved(self) -> bool:
        """Less than two blocks in flight. Empty queue cannot be C."""
        if self.rate <= 0:
            return True
        return self.rate * self._rtt_s() < 2 * 1048576

    def _starved_fill_bps(self) -> float:
        """Rate that puts ~2 blocks in the pipe so the window can open."""
        return max(self.min_bps, 2 * 1048576 / self._rtt_s())

    def _drain_target(self) -> float:
        """One 0.75× step, but never below measured unique."""
        cut = self.rate * _DRAIN_GAIN
        aimed = self._aim_delivery()
        if 0 < aimed <= self.rate:
            return max(cut, aimed)
        return cut

    def _pipe_oversend(self) -> bool:
        """Send is well above unique arrival. Cap or iid — measure to tell."""
        # First-window unique (~one block) is not C. Measuring it locked
        # Spain at 51 Mbit / 13 MiB/s.
        if self.last_unique < 16 * 1048576:
            return False
        absorbed = self._absorbed_bps()
        if absorbed is None or absorbed <= 0:
            return False
        # Unique cliffed below the filter: HOL only if we are still at
        # follow. Send above follow then a cliff is the 1.3 Gbit Spain
        # overshoot (close 100% → 5%, ov stayed 0 under 1.50× slack).
        if self.last_delivery > 0 and self.last_delivery < absorbed * 0.50:
            return self._send_ref() >= absorbed * _STARTUP_FOLLOW
        if self.have_source and self.last_send_rate > 0:
            # Window stall: source admission stopped AND decode is not
            # keeping up. Source finishing a small file is not a stall.
            if (
                self.last_source_rate < self.last_send_rate * 0.30
                and self.recv_lag
            ):
                return False
        # Startup follows unique at 1.35×. 2.5× slack let 2.7 Gbit sit
        # on a 900 Mbit filter. 1.50 is just above follow.
        gain = 1.50 if self.phase == STARTUP else _POLICER_GAIN
        return self._send_ref() > absorbed * gain

    def _slower_pairs(self) -> list[tuple[float, float]]:
        return [
            (send, delivered)
            for send, delivered in self.send_samples
            if 0 < send < self.rate * 0.92
        ]

    def _loss_tracks_send(self) -> bool:
        """Unique/send stayed similar at a slower rate → iid loss, not a cap."""
        slower = self._slower_pairs()
        if len(slower) < 2 or self.last_delivery <= 0 or self.rate <= 0:
            return False
        ratio_now = self.last_delivery / self.rate
        ratio_old = max(delivered / send for send, delivered in slower)
        return ratio_now >= ratio_old * 0.92

    def _over_delivery(self, *, window_full: bool) -> bool:
        """Policer from elasticity: faster send did not raise unique."""
        del window_full
        if not self._pipe_oversend():
            return False
        slower = self._slower_pairs()
        if len(slower) < 2 or self.last_delivery <= 0:
            return False
        ratio_now = self.last_delivery / self.rate
        ratio_old = max(delivered / send for send, delivered in slower)
        return ratio_now < ratio_old * 0.92

    def _aim_delivery(self) -> float:
        absorbed = self._absorbed_bps()
        if absorbed is None or absorbed <= 0:
            return self.rate
        return max(self.min_bps, absorbed * _POLICER_AIM)

    def _cut_to_delivery(self, now: float, *, remember: bool = True) -> None:
        self.rate = min(self.rate, self._aim_delivery())
        if remember:
            self.last_good = min(self.last_good, self.rate)
        self.last_step_ts = now

    def _start_measure(self, now: float) -> None:
        """Drop to measured delivery; keep it only if unique does not fall."""
        self.phase = MEASURE
        self.measure_restore = self.rate
        self.measure_good = self.last_good
        self.measure_delivery = self._absorbed_bps() or self.last_delivery
        self._cut_to_delivery(now, remember=False)
        rtt = self._rtt_s()
        # Long enough for new blocks to first-close if the cut actually
        # lowered loss. 4 RTT is too short on a 64 MiB uncoverable window.
        self.measure_until = now + max(0.80, 8.0 * rtt)
        self._emit(f"measure_start restore={self.measure_restore * 8 / 1_000_000:.0f}")

    def _finish_measure(self, now: float) -> None:
        unique_held = (
            self.measure_delivery > 0
            and self.last_delivery > 0
            and self.last_delivery >= self.measure_delivery * 0.90
        )
        restore = self.measure_restore if self.measure_restore > 0 else self.rate
        ratio_before = (
            self.measure_delivery / restore if restore > 0 else 0.0
        )
        ratio_after = self.last_delivery / self.rate if self.rate > 0 else 0.0
        # Cap: unique stays, unique/send rises. iid: unique tracks the cut.
        lost_less = ratio_after >= ratio_before * 1.12
        decoded_ok = not self.recv_lag
        # Stale unique from the pre-cut send must not confirm a tiny trial.
        follows_trial = (
            self.last_delivery > 0
            and self.rate > 0
            and self.last_delivery <= self.rate * 1.25
            and self.last_delivery >= self.rate * 0.45
        )
        fat_plateau = (
            unique_held
            and lost_less
            and follows_trial
            and ratio_before >= 0.45
            and ratio_after >= 0.80
        )
        collapsed = (
            self.measure_delivery > 0
            and self.last_delivery > 0
            and self.last_delivery < self.measure_delivery * 0.70
        )
        keep = (
            (unique_held and lost_less and decoded_ok and follows_trial)
            or fat_plateau
            or collapsed
        )
        if keep:
            self.last_good = min(self.last_good, self.rate)
            self.measure_need_decode = False
            self._emit(
                f"measure_keep held={int(unique_held)} less={int(lost_less)} "
                f"follow={int(follows_trial)} fat={int(fat_plateau)} "
                f"collapse={int(collapsed)} "
                f"rb={ratio_before:.2f} ra={ratio_after:.2f}"
            )
        else:
            self.rate = self.measure_restore
            self.last_good = self.measure_good
            self.measure_holdoff = now + max(1.0, 8.0 * self._rtt_s())
            # Trial send is not a slower steady-state sample.
            self.send_samples.clear()
            if self.recv_lag:
                self.measure_need_decode = True
            self._emit(
                f"measure_restore held={int(unique_held)} less={int(lost_less)} "
                f"follow={int(follows_trial)} dec_ok={int(decoded_ok)} "
                f"rb={ratio_before:.2f} ra={ratio_after:.2f} "
                f"back={restore * 8 / 1_000_000:.0f}"
            )
        self._enter_cruise(now)

    def _nudge(self, gain: float) -> float:
        if self.phase == STARTUP:
            return self._clip(self.rate * gain)
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
        # No channel guess: wait for an echo before inventing a rate.
        if self.min_rtt is None:
            return self.rate
        if self.bw.max_bw is None:
            return self.rate
        if self._pipe_oversend():
            return self.rate
        ceiling = self._startup_ceiling()
        if self.rate >= ceiling * 0.98:
            return self.rate
        if now - self.last_step_ts >= self._step_s():
            self.last_step_ts = now
            self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
        return self.rate

    def _observe_delivery(
        self,
        now: float,
        unique_bytes: int,
        decoded_bytes: int,
        sent_bytes: int,
        source_bytes: int,
    ) -> None:
        if self.last_ts is None:
            self.last_unique = unique_bytes
            self.last_decoded = decoded_bytes
            self.last_sent = sent_bytes
            self.last_source = source_bytes
            self.last_ts = now
            return
        dt = now - self.last_ts
        min_dt = 0.5 * self._rtt_s()
        if dt < min_dt:
            return
        du = unique_bytes - self.last_unique
        dd = decoded_bytes - self.last_decoded
        ds = sent_bytes - self.last_sent
        dsrc = source_bytes - self.last_source
        self.last_unique = unique_bytes
        self.last_decoded = decoded_bytes
        self.last_sent = sent_bytes
        self.last_source = source_bytes
        self.last_ts = now
        if ds > 0:
            self.last_send_rate = ds / dt
        if source_bytes > 0:
            self.have_source = True
            self.last_source_rate = dsrc / dt if dsrc > 0 else 0.0
        if du <= 0:
            self.recv_lag = True
            return
        unique_rate = du / dt
        decoded_rate = max(0.0, dd) / dt if dd > 0 else 0.0
        self.last_delivery = unique_rate
        self.last_decoded_rate = decoded_rate
        self.recv_lag = (
            decoded_rate <= 0 or decoded_rate < unique_rate * _DECODE_KEEPUP
        )
        # A HOL/window stall cliffs unique; do not let that rewrite max_bw
        # into an 8 Mbit "cap". A real shaper is a stable low unique from
        # the first samples, so max_bw is already that level.
        cliff = (
            self.bw.max_bw is not None and unique_rate < self.bw.max_bw * 0.50
        )
        if cliff:
            return
        # Unique is path delivery even when decode lags. Confirming a cut
        # still needs decode to recover or a fat unique/send plateau.
        self.bw.observe(unique_rate)
        if self.phase != MEASURE:
            self.send_samples.append((self._send_ref(), unique_rate))
            if len(self.send_samples) > _BW_WINDOW:
                del self.send_samples[0]

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
        sent_bytes: int = 0,
        source_bytes: int = 0,
    ) -> float:
        if feedback_id <= self.last_fb:
            return self.rate
        self.last_fb = feedback_id
        raw_rtt = rtt_from_echo(now, echo_ts_us)
        qdelay = self.rtt.observe(now, raw_rtt)
        self.min_rtt = self.rtt.min_rtt
        self._observe_delivery(
            now, unique_bytes, decoded_bytes, sent_bytes, source_bytes
        )
        self.last_extra = extra_frac
        recv_lag = bool(window_full and self.recv_lag)
        step_s = 0.16 if self.min_rtt is None else max(0.12, min(0.40, 2.0 * self.min_rtt))
        settled = now - self.last_step_ts >= max(step_s, 2.0 * self._rtt_s())

        if self._admit_stall():
            # A paused window inflates RTT. Do not accumulate a drain.
            self.high_delay_n = 0
        elif qdelay >= self._qdelay_cut():
            self.high_delay_n += 1
            self.low_delay_n = 0
        elif qdelay <= self._qdelay_ok():
            self.low_delay_n += 1
            self.high_delay_n = 0
        else:
            self.high_delay_n = 0

        starved = self._pipe_starved()
        buffer_full = (
            qdelay >= self._qdelay_stop()
            and self.high_delay_n >= _QDELAY_HOLD
            and not self._admit_stall()
            and not starved
        )
        holding = now < self.measure_holdoff
        if not self.recv_lag:
            self.measure_need_decode = False
        policer = (not holding) and self._over_delivery(window_full=window_full)
        oversend = (
            (not holding)
            and not (self.measure_need_decode and self.recv_lag)
            and self._pipe_oversend()
            and not self._loss_tracks_send()
        )

        if self.phase == MEASURE:
            if now >= self.measure_until:
                self._finish_measure(now)
        elif self.phase == STARTUP:
            ceiling = self._startup_ceiling()
            hard = min(self.max_bps, self._inflight_ceiling())
            at_cap = self.rate >= hard * 0.98
            if buffer_full:
                self.phase = DRAIN
                self.drain_ts = now
                self.drain_cuts = 1
                self.rate = self._clip(self._drain_target())
                self.last_step_ts = now
                self._emit("startup_drain qdelay")
            elif oversend and (settled or self._unique_cliff()):
                self._start_measure(now)
            elif at_cap:
                self._emit("startup_at_cap")
                self._enter_cruise(now)
            elif now - self.last_step_ts >= step_s:
                self.last_step_ts = now
                self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
        elif self.phase == DRAIN:
            drained_long = now - self.drain_ts >= _DRAIN_MAX_S
            if policer:
                self._cut_to_delivery(now)
                self._enter_cruise(now)
            elif oversend:
                self._start_measure(now)
            elif self.low_delay_n >= 2 or drained_long:
                self._enter_cruise(now)
            elif now - self.last_step_ts >= step_s and self.drain_cuts < _DRAIN_MAX_CUTS:
                self.last_step_ts = now
                self.drain_cuts += 1
                self.rate = self._clip(self._drain_target())
        elif self.phase == CRUISE:
            if starved and not oversend:
                # Spain 22 Mbit / open=1: probe +10% and qdelay drain
                # sat for 40s. Jump toward two-block BDP, then follow.
                hard = min(self.max_bps, self._inflight_ceiling())
                ceiling = min(hard, max(self.rate * _STEP_MAX, self._starved_fill_bps()))
                if now - self.last_step_ts >= step_s and self.rate < ceiling * 0.98:
                    self.last_step_ts = now
                    self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
                    self._emit("cruise_fill")
            elif buffer_full:
                self.phase = DRAIN
                self.drain_ts = now
                self.drain_cuts = 1
                soft = self.rate * _CRUISE_CUT
                aimed = self._aim_delivery()
                if aimed > 0 and aimed < self.rate:
                    self.rate = self._clip(max(soft, aimed))
                else:
                    self.rate = self._clip(soft)
                self.last_step_ts = now
                self.high_delay_n = 0
                self._emit("cruise_drain qdelay")
            elif settled and policer:
                self._cut_to_delivery(now)
                self._emit("cruise_policer")
            elif settled and oversend:
                self._start_measure(now)
            elif (
                not recv_lag
                and not policer
                and not oversend
                and qdelay < self._qdelay_stop()
                and now - self.cruise_ts >= _PROBE_WAIT_S
                and self.rate < self._rate_ceiling() * 0.98
            ):
                self.phase = PROBE
                self.probe_base = self.rate
                self.probe_decoded = self.last_decoded_rate
                follow = (
                    self.last_delivery > 0
                    and self.last_delivery >= self.rate * 0.85
                )
                gain = _STEP_MAX if follow else _PROBE_GAIN
                self.rate = min(self._rate_ceiling(), self._nudge(gain))
                self.probe_until = now + max(0.32, 4.0 * self._rtt_s())
        elif self.phase == PROBE:
            unique_held = (
                self.last_delivery > 0
                and self.probe_base > 0
                and self.last_delivery >= self.probe_base * 0.95
            )
            lag_stuck = recv_lag and not unique_held and not starved
            if buffer_full or lag_stuck or policer or oversend:
                why = (
                    "qdelay"
                    if buffer_full
                    else "lag"
                    if lag_stuck
                    else "policer"
                    if policer
                    else "oversend"
                )
                self.rate = self.probe_base
                self._emit(f"probe_abort {why}")
                self._enter_cruise(now)
            elif now >= self.probe_until:
                decode_gained = (
                    self.probe_decoded > 0
                    and self.last_decoded_rate >= self.probe_decoded * 1.02
                )
                unique_gained = (
                    self.last_delivery > 0
                    and self.probe_base > 0
                    and self.last_delivery >= self.probe_base * 1.02
                )
                filling = starved and not oversend
                better = (
                    (decode_gained or unique_gained or filling)
                    and (filling or qdelay < self._qdelay_stop())
                    and (filling or self.high_delay_n == 0)
                    and not lag_stuck
                    and not self._over_delivery(window_full=window_full)
                )
                if better:
                    self.last_good = self.rate
                    self._emit("probe_keep")
                else:
                    self.rate = self.probe_base
                    self._emit("probe_revert no_gain")
                self._enter_cruise(now)

        self.rate = self._clip(self.rate)
        return self.rate
