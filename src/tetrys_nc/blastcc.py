"""Rate search: classify the path, then sit on C.

CC owns the wire rate. FEC is a pad, not a C signal. Unique ESI is
delivered payload, not UDP C.

Path class:
  queue   — standing RTT inflation with a live pipe → drain
  dropper — first-flight p grows with send, RTT flat → lock last-clean send
  policer — send rises, unique does not (token bucket / hard cap)
  iid     — p stable as send rises → FEC's job, do not cut
  hol     — unique cliff / send pause → hold, never lock the trickle
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .block_state import FEC_COVER_MAX, WAN_ACTIVE_BYTES, cover_loss_p

STARTUP = "startup"
DRAIN = "drain"
CRUISE = "cruise"
PROBE = "probe"
MEASURE = "measure"

PATH_UNKNOWN = "unknown"
PATH_IID = "iid"
PATH_DROPPER = "dropper"
PATH_POLICER = "policer"
PATH_QUEUE = "queue"
PATH_HOL = "hol"

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
_STARTUP_FOLLOW = 1.35
_STARTUP_TRACK = 1.50
_BW_GROW = 1.20
_BW_STALL = 3
_PROBE_GAIN = 1.10
_PROBE_WAIT_S = 1.0
_CLIMB_WAIT_S = 0.20
_CLIFF_HOLD = 4
_OVERSEND_HOLD = 3
_DRAIN_GAIN = 0.75
_DRAIN_MAX_CUTS = 3
_DRAIN_MAX_S = 1.5
_CRUISE_CUT = 0.93
_GOOD_FLOOR = 0.90
_SEED_FRAC = 0.90
_ABS_MIN_MBIT = 8.0
_RTT_BOOTSTRAP_S = 0.08
_DELIVERY_HEADROOM = 1.15
_CLEAN_RATIO = 0.97
_LOSS_NOISE = 0.02
_LOSS_KNEE = 0.03
_LOSS_KEEPUP = 0.85
_LOSS_HOLD = 2
_KNEE_GOOD_RATIO = 0.99
_STEP_MAX = 1.25
_INFLIGHT_GAIN = 1.25
_BW_WINDOW = 16
_BW_MIN_SAMPLES = 3
_IID_RATIO = 0.70
_COVERABLE_P = cover_loss_p(FEC_COVER_MAX)
_POLICER_GAIN = 1.20
_POLICER_AIM = 1.10
_DECODE_KEEPUP = 0.70
_BLOCK_BYTES = 1048576


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
    probe_unique: float = 0.0
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
    last_path_loss: float | None = None
    last_qdelay: float = 0.0
    path_kind: str = PATH_UNKNOWN
    last_sent: int = 0
    last_send_rate: float = 0.0
    last_source: int = 0
    last_source_rate: float = 0.0
    have_source: bool = False
    send_samples: list[tuple[float, float]] = field(default_factory=list)
    path_samples: list[tuple[float, float]] = field(default_factory=list)
    measure_restore: float = 0.0
    measure_good: float = 0.0
    measure_delivery: float = 0.0
    measure_until: float = 0.0
    measure_holdoff: float = 0.0
    measure_need_decode: bool = False
    cliff_n: int = 0
    oversend_n: int = 0
    bw_peak: float = 0.0
    bw_stall_n: int = 0
    was_fat: bool = False
    knee_bps: float = 0.0
    saw_loss_knee: bool = False
    dropper_confirmed: bool = False
    loss_knee_n: int = 0
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

    def _startup_gain(self) -> float:
        """2.89× while starved or unique has not tracked send. Then 1.25×.

        Unique=send from the first ACK is still an empty pipe (two-block
        fill is not C). A second 2.89× on a fat dropper has no queue.
        """
        self._fat_pipe()
        dirty = not self._clean_pipe()
        stalled = self.bw_stall_n >= _BW_STALL
        if dirty and self.was_fat and stalled:
            return _DELIVERY_HEADROOM
        if dirty and self._iid_like():
            return _STARTUP_TRACK
        if self._pipe_starved():
            return _STARTUP_GAIN
        if stalled or self.was_fat:
            return _STEP_MAX
        return _STARTUP_GAIN

    def _startup_search_cap(self, hard: float) -> float:
        bw = self.bw.max_bw
        if bw is None:
            return min(hard, max(self.min_bps, self.start_bps * _STEP_MAX))
        ref = bw
        if 0 < self.last_delivery <= bw * 1.50:
            ref = max(ref, self.last_delivery)
        return min(hard, max(self.min_bps, ref * self._startup_gain()))

    def _rate_ceiling(self) -> float:
        hard = min(self.max_bps, self._inflight_ceiling())
        if self.dropper_confirmed and self.knee_bps > 0 and not self._is_fill_knee():
            return min(hard, max(self.min_bps, self.knee_bps))
        if self.phase == STARTUP:
            cap = self._startup_search_cap(hard)
            if self._pipe_starved() and self.bw.max_bw is not None:
                if self.last_unique < 16 * _BLOCK_BYTES:
                    cap = max(cap, min(hard, self._starved_fill_bps()))
            return cap
        if self._pipe_starved():
            return min(hard, max(self.rate * _STEP_MAX, self._starved_fill_bps()))
        bw = self.bw.max_bw
        if bw is None:
            return hard
        cap = max(self.min_bps, self.rate, bw * _DELIVERY_HEADROOM)
        return min(hard, cap)

    def _startup_ceiling(self) -> float:
        return self._rate_ceiling()

    def _raise_ceiling(self) -> float:
        hard = min(self.max_bps, self._inflight_ceiling())
        if self._pipe_starved():
            return hard
        bw = self.bw.max_bw
        if bw is None:
            return hard
        if self.dropper_confirmed and not self._is_fill_knee():
            knee = self._knee_bps()
            cap = knee if knee is not None else bw
        else:
            cap = bw * self._startup_gain()
        return min(hard, max(self.min_bps, cap))

    def _note_bw(self, settled: bool) -> None:
        bw = self.bw.max_bw
        if bw is None or bw <= 0:
            return
        if self.bw_peak <= 0 or bw > self.bw_peak * _BW_GROW:
            self.bw_peak = max(self.bw_peak, bw)
            self.bw_stall_n = 0
        elif settled:
            self.bw_stall_n += 1

    def _startup_bw_stalled(self, settled: bool) -> bool:
        del settled
        bw = self.bw.max_bw
        if bw is None or self.bw_stall_n < _BW_STALL:
            return False
        return self.rate >= bw * 0.95

    def _fat_pipe(self) -> bool:
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        got = self.last_delivery
        if got <= 0 or send <= 0 or self._unique_cliff():
            return False
        ok = got >= send * 0.90
        if ok:
            self.was_fat = True
        return ok

    def _clean_pipe(self) -> bool:
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        got = self.last_delivery
        if send <= 0 or self._unique_cliff() or self._send_paused():
            return False
        if self.last_path_loss is not None:
            return self.last_path_loss < _LOSS_NOISE
        if got <= 0:
            return False
        return got >= send * _CLEAN_RATIO

    def _should_lock_c(self) -> bool:
        if not self.was_fat or self._unique_cliff() or self._send_paused():
            return False
        if self.last_extra >= 0.20 or self.bw_stall_n < _BW_STALL:
            return False
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        got = self.last_delivery
        if send <= 0 or got <= 0:
            return False
        if self.rate > 0 and got > self.rate * 1.10:
            return False
        if not self._clean_pipe() and self.knee_bps <= 0:
            bw = self.bw.max_bw
            if bw is None or got < bw * 0.90:
                return False
            if send > got * 1.40:
                return False
        return True

    def _locked_c(self) -> bool:
        return self.saw_loss_knee and self.knee_bps > 0 and not self._is_fill_knee()

    def _dropper_frozen(self) -> bool:
        return self.dropper_confirmed and self._locked_c()

    def _may_search_past_lock(self) -> bool:
        if self._on_fill_shelf() or self._is_fill_knee():
            return True
        if self._dropper_frozen():
            return False
        if not self._locked_c():
            return True
        if self._hol_stall() or self._unique_cliff() or self.path_kind == PATH_QUEUE:
            return False
        if self.last_path_loss is not None and self.last_path_loss >= _LOSS_KNEE:
            return False
        return True

    def _hold_locked_c(self) -> None:
        if not self._locked_c():
            return
        self.rate = self.knee_bps
        self.last_good = max(self.last_good, self.knee_bps)

    @property
    def fec_may_raise(self) -> bool:
        """Remote adaptive FEC: do not hunt C with repair on a dropper/policer."""
        if self._on_fill_shelf():
            return True
        if self._dropper_frozen() or self.path_kind == PATH_POLICER:
            return False
        return self.path_kind != PATH_HOL

    @property
    def fec_may_lower(self) -> bool:
        return self._clean_pipe() and not self._hol_stall()

    @property
    def fec_may_lower_floor(self) -> bool:
        return self.fec_may_lower

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
            f"knee={self.knee_bps * 8 / 1_000_000:.0f} "
            f"path={(self.last_path_loss or 0.0) * 100:.1f}% "
            f"kind={self.path_kind} "
            f"lag={int(self.recv_lag)} ov={int(self._pipe_oversend())}"
        )

    def _clip(self, rate: float) -> float:
        if self.phase == MEASURE:
            return min(self._rate_ceiling(), max(self.min_bps, rate))
        floor = max(self.min_bps, self.last_good * _GOOD_FLOOR)
        if self._locked_c() and not self._is_fill_knee():
            floor = max(floor, self.knee_bps)
        if self.phase == PROBE:
            return min(self._raise_ceiling(), max(floor, rate))
        if self.phase == STARTUP:
            return min(self._startup_ceiling(), max(floor, rate))
        return min(self._rate_ceiling(), max(floor, rate))

    def _delivery_bps(self) -> float | None:
        return self.bw.max_bw

    def _absorbed_bps(self) -> float | None:
        return self._delivery_bps()

    def _send_ref(self) -> float:
        return self.rate

    def _unique_cliff(self) -> bool:
        absorbed = self._absorbed_bps()
        if absorbed is None or absorbed <= 0 or self.last_delivery <= 0:
            return False
        return self.last_delivery < absorbed * 0.50

    def _limiter_stale(self) -> bool:
        if self.last_good < self._starved_fill_bps() * 1.5:
            return False
        if self.last_delivery <= 0 or self.last_delivery >= self.last_good * 0.35:
            return False
        return self._send_paused()

    def _send_paused(self) -> bool:
        if self.rate <= 0 or self.last_sent <= 0:
            return False
        if self.last_send_rate <= 0:
            return True
        return self.last_send_rate < self.rate * 0.30

    def _hol_stall(self) -> bool:
        if self._unique_cliff():
            return True
        if not self._send_paused():
            return False
        if self.rate > 0 and self.last_delivery >= self.rate * _IID_RATIO:
            return False
        return True

    def _admit_stall(self) -> bool:
        return self._hol_stall()

    def _standing_queue(self) -> bool:
        if self._hol_stall() or self._pipe_starved():
            return False
        if self.high_delay_n < _QDELAY_HOLD or self.last_qdelay < self._qdelay_stop():
            return False
        if self.rate > 0 and self.last_delivery > self.rate * 1.50:
            return False
        return self.last_delivery > 0

    def _path_class(self) -> str:
        if self._hol_stall() and (
            self.last_path_loss is None or self.last_path_loss < _LOSS_NOISE
        ):
            return PATH_HOL
        if self._standing_queue():
            return PATH_QUEUE
        if self._path_loss_grew() or self._dropper_knee_sample():
            return PATH_DROPPER
        if (
            self._pipe_oversend()
            and not self._iid_like()
            and not self._loss_tracks_send()
        ):
            return PATH_POLICER
        if self._iid_like() and not self._clean_pipe() and not self._path_loss_grew():
            return PATH_IID
        return PATH_UNKNOWN

    def _pipe_starved(self) -> bool:
        if self.rate <= 0:
            return True
        return self.rate * self._rtt_s() < 2 * _BLOCK_BYTES

    def _starved_fill_bps(self) -> float:
        return max(self.min_bps, 2 * _BLOCK_BYTES / self._rtt_s())

    def _on_fill_shelf(self) -> bool:
        """Two-block BDP (~210 Mbit at 80 ms). Unique cannot exceed send here."""
        fill = self._starved_fill_bps()
        return fill * 0.80 <= self.rate <= fill * 1.30

    def _is_fill_knee(self) -> bool:
        if self.knee_bps <= 0:
            return False
        fill = self._starved_fill_bps()
        return fill * 0.80 <= self.knee_bps <= fill * 1.30

    def _drain_target(self) -> float:
        cut = self.rate * _DRAIN_GAIN
        aimed = self._aim_delivery()
        if 0 < aimed <= self.rate:
            return max(cut, aimed)
        return cut

    def _iid_like(self) -> bool:
        absorbed = self._absorbed_bps()
        send = self._send_ref()
        if absorbed is None or absorbed <= 0 or send <= 0:
            return False
        if self._unique_cliff():
            return False
        return absorbed >= send * _IID_RATIO

    def _pipe_oversend(self) -> bool:
        if self.last_unique < 16 * _BLOCK_BYTES:
            return False
        absorbed = self._absorbed_bps()
        if absorbed is None or absorbed <= 0:
            return (
                self.last_delivery > 0
                and self.last_delivery < self.rate * 0.50
                and self.recv_lag
                and self.cliff_n >= _CLIFF_HOLD
            )
        if self._send_paused() and not (
            (self._unique_cliff() or self._limiter_stale())
            and self.recv_lag
            and self.cliff_n >= _CLIFF_HOLD
        ):
            return False
        if self.last_delivery > 0 and self.last_delivery < absorbed * 0.50:
            if self._send_ref() >= absorbed * _STARTUP_FOLLOW:
                return True
            return (
                self.recv_lag
                and self._send_ref() > self.last_delivery * 3.0
                and self._send_ref() >= absorbed * 0.90
                and self.cliff_n >= _CLIFF_HOLD
            )
        if self._limiter_stale() and self.recv_lag and self.cliff_n >= _CLIFF_HOLD:
            return True
        if self.have_source and self.last_send_rate > 0:
            if (
                self.last_source_rate < self.last_send_rate * 0.30
                and self.recv_lag
                and not self._limiter_stale()
            ):
                return False
        if self._iid_like():
            return False
        gain = 1.50 if self.phase == STARTUP else _POLICER_GAIN
        return self._send_ref() > absorbed * gain

    def _slower_pairs(self) -> list[tuple[float, float]]:
        return [
            (send, delivered)
            for send, delivered in self.send_samples
            if 0 < send < self.rate * 0.92
        ]

    def _loss_tracks_send(self) -> bool:
        slower = self._slower_pairs()
        if len(slower) < 2 or self.last_delivery <= 0 or self.rate <= 0:
            return False
        ratio_now = self.last_delivery / self.rate
        ratio_old = max(delivered / send for send, delivered in slower)
        return ratio_now >= ratio_old * 0.92

    @staticmethod
    def _sample_loss(send: float, delivered: float) -> float:
        if send <= 0:
            return 1.0
        return max(0.0, min(1.0, 1.0 - delivered / send))

    def _dropper_knee_sample(self) -> bool:
        if self._unique_cliff() or self._send_paused():
            return False
        if self.last_path_loss is None or self.last_path_loss < _LOSS_KNEE:
            return False
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        got = self.last_delivery
        if send <= 0 or got <= 0 or got < send * _LOSS_KEEPUP:
            return False
        loss_now = self._sample_loss(send, got)
        slower_p = [pl for s, pl in self.path_samples if 0 < s < send * 0.92]
        noisy_already = len(slower_p) >= 2 and min(slower_p) >= _LOSS_NOISE
        if noisy_already:
            return False
        if self.knee_bps > 0 and send > self.knee_bps * 1.05:
            if got < self.knee_bps * 0.90:
                return False
            return loss_now >= _LOSS_KNEE
        slower = self._slower_pairs()
        if len(slower) < 2:
            return False
        loss_old = min(self._sample_loss(s, d) for s, d in slower)
        if loss_now <= loss_old + _LOSS_NOISE:
            return False
        return loss_now >= _LOSS_KNEE and loss_now >= loss_old + _LOSS_KNEE

    def _path_loss_grew(self) -> bool:
        if self.last_path_loss is None or self._unique_cliff() or self._send_paused():
            return False
        p = self.last_path_loss
        if p < _LOSS_KNEE:
            return False
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        slower = [pl for s, pl in self.path_samples if 0 < s < send * 0.92]
        if len(slower) >= 2:
            old = min(slower)
            if old >= _LOSS_NOISE:
                return False
            return p >= _LOSS_KNEE
        if (
            self.knee_bps > 0
            and send > self.knee_bps * 1.05
            and p >= _LOSS_KNEE
            and p <= _COVERABLE_P
        ):
            return True
        return False

    def _note_loss_knee(self, now: float) -> None:
        if self.phase in (MEASURE, DRAIN):
            return
        hit = self._dropper_knee_sample() or self._path_loss_grew()
        if self.last_extra >= 0.20 and self.knee_bps <= 0:
            hit = self._path_loss_grew()
        if hit:
            self.loss_knee_n += 1
        else:
            self.loss_knee_n = 0
        if self.loss_knee_n >= _LOSS_HOLD:
            if self._on_fill_shelf():
                return
            if not self.saw_loss_knee:
                self._lock_knee(now)

    def _c_lock_bps(self) -> float | None:
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        delivery = self.last_delivery
        bw = self.bw.max_bw
        knee = self._last_clean_send()
        # Mid-ramp 0.99 latch (694) vs unique sitting on C (800): the latch
        # is leftover. Unique that followed send deep into a dropper
        # (1121 vs clean 779) is not C — keep last-clean.
        leftover = (
            knee is not None
            and delivery > 0
            and not self._unique_cliff()
            and knee < delivery
            and delivery <= knee * 1.20
        )
        quiet = self.last_path_loss is None or self.last_path_loss < _LOSS_KNEE
        if leftover and (quiet or knee < delivery * 0.90):
            knee = None
        if (
            knee is not None
            and delivery > 0
            and knee < delivery * 0.50
            and not self._unique_cliff()
            and quiet
        ):
            knee = None
        if knee is None or knee <= 0:
            dirty = (
                self.last_path_loss is not None and self.last_path_loss >= _LOSS_KNEE
            )
            if dirty:
                if leftover and delivery > 0:
                    return delivery
                return None
            tracking = (
                delivery > 0
                and send > 0
                and not self._unique_cliff()
                and delivery >= send * 0.90
            )
            if tracking and (self._clean_pipe() or delivery >= send * 0.90):
                knee = send
                if self.rate > 0 and self.rate >= send * 0.85:
                    knee = min(send, self.rate)
            elif self._clean_pipe() and send > 0:
                knee = min(send, self.rate) if self.rate > 0 else send
                if delivery > 0:
                    knee = min(knee, delivery * _POLICER_AIM)
            else:
                knee = bw
        if knee is None or knee <= 0:
            return None
        if (
            send > 0
            and knee > send * 1.10
            and not self._send_paused()
            and not self._unique_cliff()
            and not (delivery > 0 and send < delivery * 0.70)
        ):
            knee = send
        return self._sanitize_knee(knee)

    def _last_clean_send(self) -> float | None:
        if self.knee_bps > 0:
            return self.knee_bps
        best: float | None = None
        for send, got in self.send_samples:
            if send > 0 and got / send >= _KNEE_GOOD_RATIO:
                best = send if best is None else max(best, send)
        return best

    def _abort_probe_dropper(self, now: float) -> None:
        base = self.knee_bps if self.knee_bps > 0 else self.probe_base
        fill = self._starved_fill_bps()
        if base > 0 and base <= fill * 1.30:
            self.dropper_confirmed = False
            self.saw_loss_knee = False
            self.knee_bps = 0.0
            self.rate = max(base, fill)
            self._enter_cruise(now, take_rate=False)
            self._emit("probe_abort fill_knee")
            return
        if base > 0:
            prior = max(self.last_good, self.knee_bps)
            # Mid-ramp after amnesia: do not crown ~481 as C when we already
            # held a much fatter clean sample.
            if prior >= fill * 1.5 and base < prior * 0.85:
                self.rate = max(base, prior * _DRAIN_GAIN)
                self.last_good = max(self.last_good, prior)
                if self.knee_bps <= 0:
                    self.knee_bps = prior
                self.dropper_confirmed = False
                self._enter_cruise(now, take_rate=False)
                self._emit("probe_abort dropper_reject_low")
                return
            self.knee_bps = max(self.knee_bps, base)
            self.rate = base
            self.last_good = max(self.last_good, base)
            self.saw_loss_knee = True
            self.dropper_confirmed = True
            self._enter_cruise(now, take_rate=False)
            return
        self._lock_knee(now)
        self.dropper_confirmed = True

    def _lock_knee(self, now: float) -> None:
        knee = self._c_lock_bps()
        if knee is None or knee <= 0:
            return
        fill = self._starved_fill_bps()
        if knee <= fill * 1.30:
            if self.rate < knee:
                self.rate = knee
                self.last_good = max(self.last_good, knee)
            self.last_step_ts = now
            return
        self.knee_bps = self._sanitize_knee(knee)
        self.rate = self.knee_bps
        self.last_good = self.knee_bps
        self.saw_loss_knee = True
        self._enter_cruise(now, take_rate=False)
        self._emit("loss_knee_lock")

    def _loss_grew(self) -> bool:
        return self._dropper_knee_sample() and self.loss_knee_n >= _LOSS_HOLD

    def _knee_bps(self) -> float | None:
        if self.knee_bps > 0:
            return self.knee_bps
        return None

    def _delivery_cap_bps(self) -> float | None:
        """Proxy for path C: unique/bw plateau — never the limiter overshoot."""
        parts: list[float] = []
        bw = self.bw.max_bw
        if bw is not None and bw > 0:
            parts.append(bw)
        # Live unique during HOL/cliff is a trickle — do not use as C cap.
        if (
            self.last_delivery > 0
            and not self._unique_cliff()
            and not self.recv_lag
        ):
            parts.append(self.last_delivery)
        for send, got in self.send_samples:
            if send > 0 and got / send >= _KNEE_GOOD_RATIO:
                parts.append(min(send, got))
        if not parts:
            return None
        return max(parts)

    def _sanitize_knee(self, knee: float) -> float:
        """Clip a latched knee only when it clearly exceeds delivered C."""
        if knee <= 0:
            return knee
        cap = self._delivery_cap_bps()
        if cap is None or cap <= 0:
            return knee
        limit = cap * _POLICER_AIM
        if knee <= limit:
            return knee
        return max(self.min_bps, limit)

    def _note_knee(self) -> None:
        if self.saw_loss_knee:
            return
        send = self.last_send_rate
        got = self.last_delivery
        if send <= 0 or got <= 0 or self._unique_cliff():
            return
        if self.rate > 0 and send < self.rate * 0.50:
            return
        if self.rate > 0 and send > self.rate * 1.10:
            return
        if self.last_path_loss is not None and self.last_path_loss >= _LOSS_KNEE:
            return
        if got / send >= _KNEE_GOOD_RATIO:
            # Latch delivered C, not min(send, rate) which equals overshoot pace
            # when unique briefly tracks a 1.25× blast.
            latched = min(send, got, self.rate if self.rate > 0 else send)
            latched = self._sanitize_knee(latched)
            if latched > self._starved_fill_bps() * 1.30:
                self.knee_bps = max(self.knee_bps, latched)

    def _note_path_sample(self) -> None:
        if self.last_path_loss is None:
            return
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        if send <= 0:
            return
        self.path_samples.append((send, self.last_path_loss))
        if len(self.path_samples) > _BW_WINDOW:
            del self.path_samples[0]

    def _over_delivery(self, *, window_full: bool) -> bool:
        del window_full
        if self._loss_grew():
            return True
        if not self._pipe_oversend():
            return False
        slower = self._slower_pairs()
        if len(slower) < 2 or self.last_delivery <= 0:
            return False
        ratio_now = self.last_delivery / self.rate
        ratio_old = max(delivered / send for send, delivered in slower)
        return ratio_now < ratio_old * 0.92

    def _aim_delivery(self) -> float:
        if self._locked_c():
            return self.knee_bps
        if self._loss_grew():
            knee = self._knee_bps()
            if knee is not None:
                return max(self.min_bps, knee)
        absorbed = self._absorbed_bps()
        if self._unique_cliff() and self.last_delivery > 0 and self.recv_lag:
            live = max(self.min_bps, self.last_delivery * _POLICER_AIM)
            fill = self._starved_fill_bps()
            trickle = self.last_delivery < fill * 0.40
            if trickle:
                return max(live, fill)
            if self.last_good >= fill * 1.5:
                return max(live, self.last_good * _DRAIN_GAIN)
            return max(live, fill)
        if absorbed is None or absorbed <= 0:
            return self.rate
        return max(self.min_bps, absorbed * _POLICER_AIM)

    def _forget_stale_bw(self) -> None:
        self.bw.samples.clear()
        self.bw_peak = 0.0
        self.bw_stall_n = 0
        self.was_fat = False
        self.knee_bps = 0.0
        self.saw_loss_knee = False
        self.dropper_confirmed = False
        self.loss_knee_n = 0
        self.send_samples.clear()
        self.path_samples.clear()
        self.last_path_loss = None

    def _cut_to_delivery(self, now: float, *, remember: bool = True) -> None:
        if self._locked_c():
            self._hold_locked_c()
            self.last_step_ts = now
            return
        self.rate = min(self.rate, self._aim_delivery())
        if remember:
            # Soft knee / confirmed C: cut pace, do not erase memory.
            if self.knee_bps > 0:
                self.last_good = max(self.last_good, self.knee_bps)
            else:
                self.last_good = min(self.last_good, self.rate)
        self.last_step_ts = now

    def _recover_wrecked(self, now: float) -> None:
        if self._locked_c():
            self._hold_locked_c()
            self.last_step_ts = now
            self._emit("hold_knee")
            return
        fill = self._starved_fill_bps()
        held = max(self.last_good, self.bw.max_bw or 0.0, self.knee_bps)
        live = (
            max(self.min_bps, self.last_delivery * _POLICER_AIM)
            if self.last_delivery > 0
            else fill
        )
        trickle = self.last_delivery <= 0 or self.last_delivery < fill * 0.40
        paused_stale = (
            self._send_paused()
            and self.last_good >= fill * 1.5
            and self.last_delivery > 0
            and self.last_delivery < self.last_good * 0.35
        )
        if self.knee_bps > 0 and not trickle:
            sane = self._sanitize_knee(self.knee_bps)
            self.knee_bps = sane
            self.rate = sane
            self.last_good = max(self.last_good, sane)
            # Soft knee hold is not a dropper lock — allow later search.
            self.last_step_ts = now
            self._emit("hol_hold_knee")
            return
        # Soft knee or fat bw memory: HOL/lag cliff after overshoot must not
        # call forget (WAN: knee≈749, path still 8% → old hol_hold_fat skipped).
        if self.knee_bps > 0 or (trickle and held >= fill * 1.5 and self.was_fat):
            raw = self.knee_bps if self.knee_bps > 0 else held * _DRAIN_GAIN
            sane = self._sanitize_knee(max(raw, fill))
            if self.knee_bps > 0:
                self.knee_bps = min(self.knee_bps, max(sane, fill))
            aimed = max(fill, sane)
            self.rate = aimed
            self.last_good = max(self.last_good, aimed)
            self.last_step_ts = now
            self.measure_holdoff = now + max(0.80, 8.0 * self._rtt_s())
            self._emit("hol_hold_fat")
            return
        # HOL after a real climb with clean path (no soft knee yet).
        if (
            trickle
            and held >= fill * 1.5
            and self.last_path_loss is not None
            and self.last_path_loss < _LOSS_NOISE
        ):
            aimed = max(fill, held * _DRAIN_GAIN)
            self.rate = aimed
            self.last_good = max(self.last_good, aimed)
            self.last_step_ts = now
            self.measure_holdoff = now + max(0.80, 8.0 * self._rtt_s())
            self._emit("hol_hold_fat")
            return
        self._forget_stale_bw()
        if trickle or paused_stale:
            aimed = max(fill, live)
        elif self.last_good >= fill * 1.5:
            aimed = max(live, self.last_good * _DRAIN_GAIN)
        else:
            aimed = max(fill, live)
        self.rate = min(self.rate, aimed)
        self.last_good = min(self.last_good, self.rate)
        self.last_step_ts = now
        self.measure_holdoff = now + max(0.80, 8.0 * self._rtt_s())
        self._emit("wrecked_cut")

    def _start_measure(self, now: float) -> None:
        self.phase = MEASURE
        self.measure_restore = self.rate
        self.measure_good = self.last_good
        self.measure_delivery = self._absorbed_bps() or self.last_delivery
        self._cut_to_delivery(now, remember=False)
        self.measure_until = now + max(0.80, 8.0 * self._rtt_s())
        self._emit(f"measure_start restore={self.measure_restore * 8 / 1_000_000:.0f}")

    def _finish_measure(self, now: float) -> None:
        unique_held = (
            self.measure_delivery > 0
            and self.last_delivery > 0
            and self.last_delivery >= self.measure_delivery * 0.90
        )
        restore = self.measure_restore if self.measure_restore > 0 else self.rate
        ratio_before = self.measure_delivery / restore if restore > 0 else 0.0
        ratio_after = self.last_delivery / self.rate if self.rate > 0 else 0.0
        lost_less = ratio_after >= ratio_before * 1.12
        decoded_ok = not self.recv_lag
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
        keep_collapse = collapsed and self.rate >= self.measure_delivery * 0.70
        keep = (
            (unique_held and lost_less and decoded_ok and follows_trial)
            or fat_plateau
            or keep_collapse
        )
        if keep:
            self.last_good = min(self.last_good, self.rate)
            self.measure_need_decode = False
            self._emit("measure_keep")
        else:
            self.rate = self.measure_restore
            self.last_good = self.measure_good
            self.measure_holdoff = now + max(1.5, 12.0 * self._rtt_s())
            self.send_samples.clear()
            if self.recv_lag:
                self.measure_need_decode = True
            self._emit(f"measure_restore back={restore * 8 / 1_000_000:.0f}")
        self._enter_cruise(now)

    def _nudge(self, gain: float) -> float:
        if self.phase == STARTUP:
            return self._clip(self.rate * gain)
        target = min(self.rate * gain, self.rate * _STEP_MAX)
        return self._clip(target)

    def _enter_cruise(self, now: float, *, take_rate: bool = True) -> None:
        self.phase = CRUISE
        if take_rate:
            self.last_good = max(self.last_good, self.rate)
        self.cruise_ts = now
        self.last_step_ts = now
        self.high_delay_n = 0
        self.low_delay_n = 0
        self.drain_cuts = 0
        self.oversend_n = 0

    def _step_s(self) -> float:
        if self.min_rtt is None:
            return 0.10
        return max(0.08, min(0.20, 1.25 * self.min_rtt))

    def on_timer(self, now: float) -> float:
        if self.phase != STARTUP:
            return self.rate
        if self.min_rtt is None or self.bw.max_bw is None:
            return self.rate
        if self._pipe_oversend():
            return self.rate
        ceiling = self._startup_ceiling()
        if self.rate >= ceiling * 0.98:
            return self.rate
        if now - self.last_step_ts >= self._step_s():
            self.last_step_ts = now
            bw = self.bw.max_bw or self.rate
            if self.last_delivery > 0:
                bw = max(bw, self.last_delivery)
            aimed = bw * self._startup_gain()
            self.rate = min(ceiling, max(self.rate, aimed))
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
        if dt < 0.5 * self._rtt_s():
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
        self.last_send_rate = ds / dt if ds > 0 else 0.0
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
        cliff = self.bw.max_bw is not None and unique_rate < self.bw.max_bw * 0.50
        if cliff:
            return
        self._note_knee()
        self.bw.observe(unique_rate)
        if self.phase != MEASURE:
            send_ref = (
                self.last_send_rate if self.last_send_rate > 0 else self._send_ref()
            )
            self.send_samples.append((send_ref, unique_rate))
            if len(self.send_samples) > _BW_WINDOW:
                del self.send_samples[0]

    def _begin_drain(self, now: float, *, cruise: bool) -> None:
        self.phase = DRAIN
        self.drain_ts = now
        self.drain_cuts = 1
        if cruise:
            soft = self.rate * _CRUISE_CUT
            aimed = self._aim_delivery()
            if aimed > 0 and aimed < self.rate:
                self.rate = self._clip(max(soft, aimed))
            else:
                self.rate = self._clip(soft)
            self.high_delay_n = 0
            self._emit("cruise_drain qdelay")
        else:
            self.rate = self._clip(self._drain_target())
            self._emit("startup_drain qdelay")
        self.last_step_ts = now

    def _cut_policer(self, now: float, tag: str) -> None:
        self._cut_to_delivery(now)
        if self.phase == STARTUP:
            self._enter_cruise(now)
        self._emit(tag)

    def _tick_startup(
        self,
        now: float,
        *,
        buffer_full: bool,
        oversend_held: bool,
        settled: bool,
        policer: bool,
    ) -> None:
        ceiling = self._startup_ceiling()
        hard = min(self.max_bps, self._inflight_ceiling())
        if buffer_full:
            self._begin_drain(now, cruise=False)
        elif self.path_kind == PATH_DROPPER:
            self._lock_knee(now)
        elif oversend_held and (self._unique_cliff() or self._limiter_stale()):
            self._recover_wrecked(now)
            self._enter_cruise(now, take_rate=False)
            self._emit("startup_cliff_cut")
        elif oversend_held and (settled or self._unique_cliff()) and not self.recv_lag:
            self._cut_policer(now, "startup_policer")
        elif settled and policer:
            self._cut_policer(now, "startup_loss_knee")
        elif self.rate >= hard * 0.98:
            self._emit("startup_at_cap")
            self._enter_cruise(now)
        elif self._startup_bw_stalled(settled) and not self._pipe_starved():
            bw = self.bw.max_bw or self.rate
            if self._path_loss_grew() or self._dropper_knee_sample():
                self._emit("startup_stall")
                self._lock_knee(now)
            else:
                aimed = max(self.min_bps, bw if self.was_fat else bw * _POLICER_AIM)
                if self.rate > aimed:
                    self.rate = aimed
                # Remember delivered C (bw/knee), not limiter overshoot pace.
                held = max(self.knee_bps, bw)
                if held >= self._starved_fill_bps() * 1.5:
                    self.last_good = max(self.last_good, self._sanitize_knee(held))
                self._emit("startup_stall")
                self._enter_cruise(now, take_rate=False)
        elif now - self.last_step_ts >= (
            0.16 if self.min_rtt is None else max(0.12, min(0.40, 2.0 * self.min_rtt))
        ):
            self.last_step_ts = now
            ref = self.bw.max_bw or self.rate
            if self.last_delivery > 0:
                ref = max(ref, self.last_delivery)
            self.rate = min(ceiling, max(self.rate, ref * self._startup_gain()))

    def _tick_drain(
        self,
        now: float,
        *,
        oversend_held: bool,
        policer: bool,
        step_s: float,
    ) -> None:
        drained_long = now - self.drain_ts >= _DRAIN_MAX_S
        if policer:
            self._cut_to_delivery(now)
            self._enter_cruise(now)
        elif oversend_held and (self._unique_cliff() or self._limiter_stale()):
            self._recover_wrecked(now)
            self._enter_cruise(now, take_rate=False)
            self._emit("drain_cliff_cut")
        elif oversend_held and not self.recv_lag:
            self._cut_policer(now, "drain_policer")
        elif self.low_delay_n >= 2 or drained_long:
            self._enter_cruise(now)
        elif now - self.last_step_ts >= step_s and self.drain_cuts < _DRAIN_MAX_CUTS:
            self.last_step_ts = now
            self.drain_cuts += 1
            self.rate = self._clip(self._drain_target())

    def _tick_cruise(
        self,
        now: float,
        *,
        buffer_full: bool,
        oversend_held: bool,
        settled: bool,
        policer: bool,
        holding: bool,
        recv_lag: bool,
        qdelay: float,
        step_s: float,
    ) -> None:
        empty_pipe = (
            self.last_unique < 16 * _BLOCK_BYTES
            or self._unique_cliff()
            or (
                self.last_delivery > 0
                and self.last_delivery < self._starved_fill_bps() * 0.40
            )
        )
        if buffer_full:
            self._begin_drain(now, cruise=True)
        elif self._on_fill_shelf() and not self._pipe_oversend():
            hard = min(self.max_bps, self._inflight_ceiling())
            ceiling = min(hard, max(self.rate * _STEP_MAX, self._starved_fill_bps() * _STARTUP_GAIN))
            if now - self.last_step_ts >= step_s and self.rate < ceiling * 0.98:
                self.last_step_ts = now
                self.dropper_confirmed = False
                self.saw_loss_knee = False
                if self._is_fill_knee():
                    self.knee_bps = 0.0
                self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
                self._emit("cruise_fill")
        elif self._locked_c() and not self._may_search_past_lock():
            self._hold_locked_c()
        elif starved := (self._pipe_starved() and not self._pipe_oversend() and empty_pipe):
            del starved
            hard = min(self.max_bps, self._inflight_ceiling())
            ceiling = min(hard, max(self.rate * _STEP_MAX, self._starved_fill_bps()))
            if now - self.last_step_ts >= step_s and self.rate < ceiling * 0.98:
                self.last_step_ts = now
                self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
                self._emit("cruise_fill")
        elif (
            not self._on_fill_shelf()
            and (self.path_kind == PATH_DROPPER or self._path_loss_grew() or self._loss_grew())
        ):
            self._lock_knee(now)
        elif settled and policer:
            self._cut_to_delivery(now)
            self._emit("cruise_policer")
        elif oversend_held and (self._unique_cliff() or self._limiter_stale()):
            self._recover_wrecked(now)
            self._emit("cruise_cliff_cut")
        elif settled and oversend_held and not self.recv_lag:
            self._cut_policer(now, "cruise_policer")
        elif (
            self._may_search_past_lock()
            and self.path_kind not in (PATH_DROPPER, PATH_QUEUE)
            and not holding
            and not policer
            and not self._pipe_oversend()
            and qdelay < self._qdelay_stop()
            and self.rate < self._raise_ceiling() * 0.98
        ):
            follow = self.last_delivery > 0 and self.last_delivery >= self.rate * _IID_RATIO
            if self._unique_cliff() and not follow:
                return
            bw = self.bw.max_bw
            below_c = bw is not None and self.rate < bw * _STARTUP_FOLLOW
            plateau = (
                bw is not None
                and self.bw_peak > 0
                and bw <= self.bw_peak * 1.05
                and self.rate >= bw * 1.15
            )
            wait_s = _PROBE_WAIT_S if plateau else (
                _CLIMB_WAIT_S if follow or below_c else _PROBE_WAIT_S
            )
            if recv_lag or (self.recv_lag and not follow):
                return
            if now - self.cruise_ts < wait_s:
                return
            self.phase = PROBE
            self.probe_base = self.rate
            self.probe_decoded = self.last_decoded_rate
            self.probe_unique = self.last_delivery
            gain = _PROBE_GAIN if plateau or not (follow or below_c) else _STEP_MAX
            self.rate = min(self._raise_ceiling(), self._nudge(gain))
            self.probe_until = now + max(0.32, 4.0 * self._rtt_s())

    def _tick_probe(
        self,
        now: float,
        *,
        buffer_full: bool,
        oversend_held: bool,
        policer: bool,
        recv_lag: bool,
        qdelay: float,
        window_full: bool,
    ) -> None:
        unique_held = self.last_delivery > 0 and (
            (self.probe_unique > 0 and self.last_delivery >= self.probe_unique * 0.95)
            or (self.probe_base > 0 and self.last_delivery >= self.probe_base * 0.95)
        )
        starved = self._pipe_starved()
        lag_stuck = recv_lag and not unique_held and not starved
        dropper = self.path_kind == PATH_DROPPER or self._path_loss_grew()
        if buffer_full or lag_stuck or policer or oversend_held or dropper:
            why = (
                "qdelay" if buffer_full
                else "lag" if lag_stuck
                else "dropper" if dropper
                else "policer" if policer
                else "oversend"
            )
            if why == "dropper":
                self._emit(f"probe_abort {why}")
                self._abort_probe_dropper(now)
                return
            if why == "lag" and self.last_good > 0:
                fill = self._starved_fill_bps()
                self.rate = max(
                    fill,
                    min(
                        self.probe_base,
                        max(self.last_good, self.probe_base * _DRAIN_GAIN),
                    ),
                )
            else:
                self.rate = self.probe_base
            self.measure_holdoff = now + max(0.50, 4.0 * self._rtt_s())
            self._emit(f"probe_abort {why}")
            self._enter_cruise(now, take_rate=False)
            return
        if now < self.probe_until:
            return
        decode_gained = (
            self.probe_decoded > 0
            and self.last_decoded_rate >= self.probe_decoded * 1.02
        )
        unique_gained = self.last_delivery > 0 and (
            (self.probe_unique > 0 and self.last_delivery >= self.probe_unique * 1.02)
            or (self.probe_base > 0 and self.last_delivery >= self.probe_base * 1.02)
        )
        filling = starved and not self._pipe_oversend()
        follow_probe = (
            self.last_delivery > 0
            and self.probe_base > 0
            and self.last_delivery >= self.probe_base * _IID_RATIO
        )
        better = (
            (decode_gained or unique_gained or filling)
            and (filling or follow_probe)
            and (filling or qdelay < self._qdelay_stop())
            and (filling or self.high_delay_n == 0)
            and not lag_stuck
            and not self._over_delivery(window_full=window_full)
        )
        if better:
            knee = self._knee_bps()
            past_knee = (
                self.dropper_confirmed
                and knee is not None
                and self.rate > knee * 1.05
            )
            if past_knee:
                self.rate = self.probe_base
                self._emit("probe_revert knee")
                self._enter_cruise(now, take_rate=False)
            else:
                if not self.recv_lag:
                    self.last_good = self.rate
                self._emit("probe_keep")
                self._enter_cruise(now, take_rate=not self.recv_lag)
        else:
            self.rate = self.probe_base
            self._emit("probe_revert no_gain")
            self._enter_cruise(now, take_rate=False)

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
        path_loss: float | None = None,
    ) -> float:
        if feedback_id <= self.last_fb:
            return self.rate
        self.last_fb = feedback_id
        qdelay = self.rtt.observe(now, rtt_from_echo(now, echo_ts_us))
        self.min_rtt = self.rtt.min_rtt
        self.last_qdelay = qdelay
        if path_loss is not None:
            self.last_path_loss = max(0.0, min(1.0, float(path_loss)))
        self.last_extra = extra_frac
        self._observe_delivery(
            now, unique_bytes, decoded_bytes, sent_bytes, source_bytes
        )
        self._note_path_sample()
        self._fat_pipe()
        self._note_loss_knee(now)
        if (self._unique_cliff() or self._limiter_stale()) and self.recv_lag:
            self.cliff_n += 1
        else:
            self.cliff_n = 0
        recv_lag = bool(window_full and self.recv_lag)
        step_s = 0.16 if self.min_rtt is None else max(0.12, min(0.40, 2.0 * self.min_rtt))
        settled = now - self.last_step_ts >= max(step_s, 2.0 * self._rtt_s())
        self._note_bw(settled)

        if self._admit_stall():
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
        self.path_kind = self._path_class()
        buffer_full = self.path_kind == PATH_QUEUE or (
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
        if oversend:
            self.oversend_n += 1
        else:
            self.oversend_n = 0
        oversend_held = oversend and (
            self.oversend_n >= _OVERSEND_HOLD or self._unique_cliff()
        )

        if self.phase == MEASURE:
            if now >= self.measure_until:
                self._finish_measure(now)
        elif self.phase == STARTUP:
            self._tick_startup(
                now,
                buffer_full=buffer_full,
                oversend_held=oversend_held,
                settled=settled,
                policer=policer,
            )
        elif self.phase == DRAIN:
            self._tick_drain(
                now, oversend_held=oversend_held, policer=policer, step_s=step_s
            )
        elif self.phase == CRUISE:
            self._tick_cruise(
                now,
                buffer_full=buffer_full,
                oversend_held=oversend_held,
                settled=settled,
                policer=policer,
                holding=holding,
                recv_lag=recv_lag,
                qdelay=qdelay,
                step_s=step_s,
            )
        elif self.phase == PROBE:
            self._tick_probe(
                now,
                buffer_full=buffer_full,
                oversend_held=oversend_held,
                policer=policer,
                recv_lag=recv_lag,
                qdelay=qdelay,
                window_full=window_full,
            )

        self.rate = self._clip(self.rate)
        return self.rate
