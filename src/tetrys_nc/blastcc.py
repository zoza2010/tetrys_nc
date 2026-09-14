"""Rate search: path class first, then BBR-like phases.

CC owns the wire rate. FEC is a static first-flight pad plus drip until
decode — it must not hunt C. Unique ESI is delivered fountain symbols,
not UDP C.

Path class:
  queue    — standing RTT inflation with a live pipe (keep qdelay drain)
  dropper  — first-flight p grows with send, RTT flat; lock last-clean send
  policer  — send rises, unique does not
  iid      — p stable as send rises; FEC's job, not a rate cut
  hol      — unique cliff / send pause; do not lock that trickle as C
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
# BBR Startup: 2.89× BtlBw while unique has not yet tracked send (pipe
# filling from 8 Mbit). After a fat sample, walk 1.25× — a second 2.89×
# jump (557→1610) on a dropper has no queue to absorb it. 1.15× unique
# from the first fat sample crawled Spain to a unique-lock at 810.
_STARTUP_FOLLOW = 1.35
# Coverable iid: unique is ~0.77×send, so 1.35×unique barely climbs.
# Track a growing filter faster; stall-exit stops a 2 Gbit walk.
_STARTUP_TRACK = 1.50
_BW_GROW = 1.20
_BW_STALL = 3
_PROBE_GAIN = 1.10
_PROBE_WAIT_S = 1.0
# Unique follows and we are still below the filter: climb, do not wait 1s.
_CLIMB_WAIT_S = 0.20
_CLIFF_HOLD = 4
_OVERSEND_HOLD = 3
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
# Channel is holding this send (Spain 800–900 / ~0%). 0.90 still looks
# "fat" at iperf 1000M / 5% and walks into the dropper.
_CLEAN_RATIO = 0.97
# A dropper (Spain 800M ~0% → 1000M ~5%) does not build a queue, so
# qdelay stays 0. If loss grows with send, that is C — not "FEC's job".
_LOSS_NOISE = 0.02
_LOSS_KNEE = 0.03
# Dropper knee (iperf 1000M ~8%) still follows send. Encoder/window lag
# does not: Spain startup_loss_knee was snd=764 / unq=415.
_LOSS_KEEPUP = 0.85
_LOSS_HOLD = 2
# Latch the clean send (Spain 800–900 / ~0%). 0.97 walks the latch into
# the 3% fringe (WAN lock sat at 964 with path_p50=7%).
_KNEE_GOOD_RATIO = 0.99
_STEP_MAX = 1.25
_INFLIGHT_GAIN = 1.25
_BW_WINDOW = 16
_BW_MIN_SAMPLES = 3
# FEC max 32% covers ~24% path loss → unique/send ≈ 0.76. Slack for ACK
# noise. Below this, unique is too thin to be coverable iid.
_IID_RATIO = 0.70
_COVERABLE_P = cover_loss_p(FEC_COVER_MAX)
# Unique-ESI rate is delivered payload. Send much faster than that with a
# flat RTT is a policer. extra/DIR is FEC's job, not a rate cut.
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

    def _rate_ceiling(self) -> float:
        hard = min(self.max_bps, self._inflight_ceiling())
        # Confirmed dropper only. A soft lock (637 then path_p=0) must
        # still be allowed to climb; pinning the cap froze WAN at 156/637.
        if self.dropper_confirmed and self.knee_bps > 0:
            return min(hard, max(self.min_bps, self.knee_bps))
        if self.phase == STARTUP:
            cap = self._startup_search_cap(hard)
            # One-block unique (~22 Mbit) is not C. Once 16 MiB has landed,
            # unique is a real sample: do not jump to two-block fill over a
            # 90 Mbit shaper (netem pace_med stuck at 211).
            if self._pipe_starved() and self.bw.max_bw is not None:
                if self.last_unique < 16 * _BLOCK_BYTES:
                    cap = max(cap, min(hard, self._starved_fill_bps()))
            return cap
        if self._pipe_starved():
            # Do not pin to a 22 Mbit first unique. That is one block / RTT.
            return min(hard, max(self.rate * _STEP_MAX, self._starved_fill_bps()))
        bw = self.bw.max_bw
        if bw is None:
            return hard
        # Raise ceiling with delivery; never use a low unique-bw sample to
        # yank the rate down. Cuts are delay/policer/dropper.
        cap = max(
            self.min_bps,
            self.rate,
            bw * _DELIVERY_HEADROOM,
        )
        return min(hard, cap)

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
        follow = self._startup_gain()
        return min(hard, max(self.min_bps, ref * follow))

    def _startup_ceiling(self) -> float:
        return self._rate_ceiling()

    def _raise_ceiling(self) -> float:
        """How high probing may go. Clip ceiling includes self.rate so it
        cannot tell us we are below C. Inflight of the 64 MiB window is
        ~5 Gbit at 80 ms — that is not UDP C. Spain 2026-09-13 iid probes
        used that cap (max=1370) and HOL'd to 34 MiB/s; lock-900 is 83.
        """
        hard = min(self.max_bps, self._inflight_ceiling())
        if self._pipe_starved():
            return hard
        bw = self.bw.max_bw
        if bw is None:
            return hard
        if self.dropper_confirmed:
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

    def _startup_gain(self) -> float:
        """BBR 2.89× while BtlBw is still growing. One lagged unique sample
        after a step is not 'pipe full' — that 1.15× crawl sat at 50 Mbit.
        """
        self._fat_pipe()
        dirty = not self._clean_pipe()
        stalled = self.bw_stall_n >= _BW_STALL
        if dirty and self.was_fat and stalled:
            return _DELIVERY_HEADROOM
        if dirty and self._iid_like():
            return _STARTUP_TRACK
        if stalled:
            return _STEP_MAX
        return _STARTUP_GAIN

    def _startup_bw_stalled(self, settled: bool) -> bool:
        """Delivery stopped growing: C is in the filter, stop the 2 Gbit walk."""
        del settled
        bw = self.bw.max_bw
        if bw is None or self.bw_stall_n < _BW_STALL:
            return False
        return self.rate >= bw * 0.95

    def _fat_pipe(self) -> bool:
        """Live unique keeps up with flush. Use last_delivery, not max_bw."""
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        got = self.last_delivery
        if got <= 0 or send <= 0:
            return False
        if self._unique_cliff():
            return False
        ok = got >= send * 0.90
        if ok:
            self.was_fat = True
        return ok

    def _clean_pipe(self) -> bool:
        """Send is still in the ~0% region (lock-900), not iperf 1000/5%.

        Prefer first-flight path loss when the receiver has trained: unique
        lag/HOL looks like 15% ESI loss while first-flight is still ~0.
        """
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
        """Fat path, delivery filter stalled: that is C, including a clean
        0% plateau. Skipping a clean stall entered cruise and probed +10%
        into the dropper; HOL then wrecked_cut to ~50 MiB/s.
        """
        if not self.was_fat or self._unique_cliff() or self._send_paused():
            return False
        if self.last_extra >= 0.20:
            return False
        if self.bw_stall_n < _BW_STALL:
            return False
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        got = self.last_delivery
        if send <= 0 or got <= 0:
            return False
        # Unique already faster than the limiter: still filling from 8 Mbit.
        # WAN locked 179 while unq=227 / snd=264 (path 0%).
        if self.rate > 0 and got > self.rate * 1.10:
            return False
        if not self._clean_pipe() and self.knee_bps <= 0:
            bw = self.bw.max_bw
            # HOL/measure trickle: unique well behind send. A 1.15× hunt
            # above a stalled unique is the 0% C plateau.
            if bw is None or got < bw * 0.90:
                return False
            if send > got * 1.40:
                return False
        return True

    def _locked_c(self) -> bool:
        """Already sitting on measured C. HOL dips are not a new cap."""
        return self.saw_loss_knee and self.knee_bps > 0

    def _dropper_frozen(self) -> bool:
        """A probe already saw first-flight grow with send. Sit like `--rate`."""
        return self.dropper_confirmed and self._locked_c()

    def _may_search_past_lock(self) -> bool:
        """Soft lock plus a quiet pipe is a plateau, not C.

        WAN 2026-09-14: loss_knee_lock 637 / path=7.1% lag=1, then
        path_p50=0% / unique tracking, pace_p10=med=max=637 for the file.
        HOL unique cliff must still hold the latch (not wrecked_cut).
        """
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
        """`--rate` does not cut because unique cliffed for a round-trip."""
        if not self._locked_c():
            return
        self.rate = self.knee_bps
        self.last_good = max(self.last_good, self.knee_bps)

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
        # Trial cuts must actually sit on delivery. last_good * 0.90 would
        # yank a 450 Mbit measure back to 765 and hide the result.
        if self.phase == MEASURE:
            return min(self._rate_ceiling(), max(self.min_bps, rate))
        floor = max(self.min_bps, self.last_good * _GOOD_FLOOR)
        if self._locked_c():
            floor = max(floor, self.knee_bps)
        # Cruise clip includes self.rate so it cannot raise. Probe/startup
        # must use the search cap or iid never climbs off two-block fill.
        if self.phase == PROBE:
            return min(self._raise_ceiling(), max(floor, rate))
        if self.phase == STARTUP:
            return min(self._startup_ceiling(), max(floor, rate))
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

    def _limiter_stale(self) -> bool:
        """Limiter sits far above live unique while send already paused.

        Spain 2026-09-13: last_good=777 / unq=107 / snd=68 / ov=0. The bw
        filter had already tracked the trickle, so unique_cliff vs max_bw
        was false and FEC uncoverable walked to 4%.
        """
        if self.last_good < self._starved_fill_bps() * 1.5:
            return False
        if self.last_delivery <= 0:
            return False
        if self.last_delivery >= self.last_good * 0.35:
            return False
        return self._send_paused()

    def _send_paused(self) -> bool:
        """Encode/window pause: limiter is high, flush is not."""
        if self.rate <= 0 or self.last_sent <= 0:
            return False
        if self.last_send_rate <= 0:
            return True
        return self.last_send_rate < self.rate * 0.30

    def _hol_stall(self) -> bool:
        """HOL/window pause looks like qdelay. It is not a standing queue."""
        return self._unique_cliff() or self._send_paused()

    def _admit_stall(self) -> bool:
        return self._hol_stall()

    def _standing_queue(self) -> bool:
        """Persistent RTT inflation with a live pipe. Reorder + HOL is not C.

        A dropper (Spain) keeps qdelay ~0. If the path later becomes a
        real queue, drain even after a last-clean lock.
        """
        if self._hol_stall() or self._pipe_starved():
            return False
        if self.high_delay_n < _QDELAY_HOLD:
            return False
        if self.last_qdelay < self._qdelay_stop():
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
        """Less than two blocks in flight. Empty queue cannot be C."""
        if self.rate <= 0:
            return True
        return self.rate * self._rtt_s() < 2 * _BLOCK_BYTES

    def _starved_fill_bps(self) -> float:
        """Rate that puts ~2 blocks in the pipe so the window can open."""
        return max(self.min_bps, 2 * _BLOCK_BYTES / self._rtt_s())

    def _drain_target(self) -> float:
        """One 0.75× step, but never below measured unique."""
        cut = self.rate * _DRAIN_GAIN
        aimed = self._aim_delivery()
        if 0 < aimed <= self.rate:
            return max(cut, aimed)
        return cut

    def _iid_like(self) -> bool:
        """Unique is a large fraction of send: coverable loss, not a cap."""
        absorbed = self._absorbed_bps()
        send = self._send_ref()
        if absorbed is None or absorbed <= 0 or send <= 0:
            return False
        if self._unique_cliff():
            return False
        return absorbed >= send * _IID_RATIO

    def _pipe_oversend(self) -> bool:
        """Send is well above unique arrival. Cap or iid — measure to tell."""
        # First-window unique (~one block) is not C. Measuring it locked
        # Spain at 51 Mbit / 13 MiB/s.
        if self.last_unique < 16 * _BLOCK_BYTES:
            return False
        absorbed = self._absorbed_bps()
        if absorbed is None or absorbed <= 0:
            # Filter forgotten after a cut. Unique trickle vs the limiter
            # is still a wreck — Spain 2026-09-13 sat 695 / unq=62 / bw=0.
            return (
                self.last_delivery > 0
                and self.last_delivery < self.rate * 0.50
                and self.recv_lag
                and self.cliff_n >= _CLIFF_HOLD
            )
        # Encode pause is not a cap. A full window that cannot flush while
        # unique has cliffed under a gigabit limiter IS a wreck — Spain
        # climbed back to 1053, snd fell to 50, ov stayed 0, close 83%→16%.
        if self._send_paused() and not (
            (self._unique_cliff() or self._limiter_stale())
            and self.recv_lag
            and self.cliff_n >= _CLIFF_HOLD
        ):
            return False
        # Unique cliffed below the filter: HOL only if we are still at
        # follow. Send above follow then a cliff is the 1.3 Gbit Spain
        # overshoot (close 100% → 5%, ov stayed 0 under 1.50× slack).
        if self.last_delivery > 0 and self.last_delivery < absorbed * 0.50:
            if self._send_ref() >= absorbed * _STARTUP_FOLLOW:
                return True
            # 1091 / unq=50 for many ACKs is a wreck. One lag spike at
            # 830 after a climb (iperf C is 800) is HOL — hold the rate.
            # Spain 2026-09-12: limiter 1076 on bw=956 (1.13×) with unique
            # 50 never reached 1.35×, so ov stayed 0 and last_good stuck.
            return (
                self.recv_lag
                and self._send_ref() > self.last_delivery * 3.0
                and self._send_ref() >= absorbed * 0.90
                and self.cliff_n >= _CLIFF_HOLD
            )
        if (
            self._limiter_stale()
            and self.recv_lag
            and self.cliff_n >= _CLIFF_HOLD
        ):
            return True
        if self.have_source and self.last_send_rate > 0:
            # Window stall: source admission stopped AND decode is not
            # keeping up. Source finishing a small file is not a stall.
            if (
                self.last_source_rate < self.last_send_rate * 0.30
                and self.recv_lag
                and not self._limiter_stale()
            ):
                return False
        # Coverable iid: unique tracks send. Cutting to unique×1.10 is
        # how the search oscillated 800 ↔ 200 on a fat UDP path.
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
        """Unique/send stayed similar at a slower rate → iid loss, not a cap."""
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
        """One sample looks like C, not encode lag / HOL / stable iid.

        0% at 900 → 5–8% at 1000 still has unique ≈ send. A 2.88× startup
        step or GRO burst leaves unique at ~0.5× send — that is not C.
        """
        if self._unique_cliff() or self._send_paused():
            return False
        # Unique/send without first-flight is ACK lag (fixed FEC used to
        # skip path_loss: WAN locked 66 Mbit at snd=210/unq=194/path=None).
        if self.last_path_loss is None or self.last_path_loss < _LOSS_KNEE:
            return False
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        got = self.last_delivery
        if send <= 0 or got <= 0:
            return False
        if got < send * _LOSS_KEEPUP:
            return False
        loss_now = self._sample_loss(send, got)
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
        """First-flight p rose with send: dropper C, not HOL unique lag.

        Coverable iid (flat p at every rate, p ≲ 24%) is FEC's job.
        """
        if self.last_path_loss is None:
            return False
        if self._unique_cliff() or self._send_paused():
            return False
        p = self.last_path_loss
        if p < _LOSS_KNEE:
            return False
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        slower = [
            pl for s, pl in self.path_samples if 0 < s < send * 0.92
        ]
        if len(slower) >= 2:
            old = min(slower)
            # Already noisy at a slower send: coverable iid, FEC's job.
            # Only the 0% → 3%+ step is the dropper knee.
            if old >= _LOSS_NOISE:
                return False
            if p >= _LOSS_KNEE:
                return True
            return False
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
            # Window-busy HOL is not C. First-flight still may be.
            hit = self._path_loss_grew()
        if hit:
            self.loss_knee_n += 1
        else:
            self.loss_knee_n = 0
        if self.loss_knee_n >= _LOSS_HOLD:
            if not self.saw_loss_knee:
                self._lock_knee(now)
            self.saw_loss_knee = True

    def _c_lock_bps(self) -> float | None:
        """Sit on the last ~0% send, like `--rate 900`.

        Unique is recv after the dropper (Spain lock-810 / unq=810 vs
        lock-900). A HOL unique dip is not a lower C. Only use live
        unique when nothing clean was latched.
        """
        send = self.last_send_rate if self.last_send_rate > 0 else self.rate
        delivery = self.last_delivery
        bw = self.bw.max_bw
        knee = self._last_clean_send()
        # 8 Mbit / 66 Mbit 0.99 latch vs unique 194 is a ramp leftover, not C.
        if (
            knee is not None
            and delivery > 0
            and knee < delivery * 0.50
            and not self._unique_cliff()
            and (self.last_path_loss is None or self.last_path_loss < _LOSS_KNEE)
        ):
            knee = None
        if knee is None or knee <= 0:
            # Dirty first-flight: unique/delivery is the dropper recv, not C.
            if self.last_path_loss is not None and self.last_path_loss >= _LOSS_KNEE:
                return None
            tracking = (
                delivery > 0
                and send > 0
                and not self._unique_cliff()
                and delivery >= send * _IID_RATIO
            )
            if tracking and (self._clean_pipe() or delivery >= send * _IID_RATIO):
                knee = send
                if self.rate > 0 and self.rate >= send * 0.85:
                    knee = min(send, self.rate)
            elif self._clean_pipe() and send > 0:
                knee = min(send, self.rate) if self.rate > 0 else send
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
        return knee

    def _last_clean_send(self) -> float | None:
        if self.knee_bps > 0:
            return self.knee_bps
        best: float | None = None
        for send, got in self.send_samples:
            if send > 0 and got / send >= _KNEE_GOOD_RATIO:
                best = send if best is None else max(best, send)
        return best

    def _abort_probe_dropper(self, now: float) -> None:
        """Probe hit the dropper. Sit on the pre-probe shelf, not HOL send."""
        base = self.knee_bps if self.knee_bps > 0 else self.probe_base
        if base > 0:
            self.knee_bps = base
            self.rate = base
            self.last_good = max(self.last_good, base)
            self.saw_loss_knee = True
            self.dropper_confirmed = True
            self._enter_cruise(now, take_rate=False)
            return
        self._lock_knee(now)
        self.dropper_confirmed = True

    def _lock_knee(self, now: float) -> None:
        """Sit like `--rate`. Do not keep probing C."""
        knee = self._c_lock_bps()
        if knee is None or knee <= 0:
            return
        self.knee_bps = knee
        self.rate = knee
        self.last_good = knee
        self.saw_loss_knee = True
        self._enter_cruise(now, take_rate=False)
        self._emit("loss_knee_lock")

    def _loss_grew(self) -> bool:
        """Loss rose as send rose: past C even when the queue is empty.

        Stable 23% iid is the same at every rate — FEC. 0% at 900 Mbit → 5%
        at 1000 Mbit is the dropper knee. One lagged unique sample is not.
        """
        return self._dropper_knee_sample() and self.loss_knee_n >= _LOSS_HOLD

    def _knee_bps(self) -> float | None:
        """Highest flush that still had near-zero extra loss (lock-900)."""
        if self.knee_bps > 0:
            return self.knee_bps
        return None

    def _note_knee(self) -> None:
        if self.saw_loss_knee:
            return
        send = self.last_send_rate
        got = self.last_delivery
        if send <= 0 or got <= 0 or self._unique_cliff():
            return
        # Encode pause: unique can dwarf a tiny limiter sample and look
        # like 0% loss at 8 Mbit. Only latch while we are actually flushing.
        if self.rate > 0 and send < self.rate * 0.50:
            return
        # GRO/ACK burst (snd=1100 at limiter 855) is not a higher C.
        if self.rate > 0 and send > self.rate * 1.10:
            return
        if self.last_path_loss is not None and self.last_path_loss >= _LOSS_KNEE:
            return
        if got / send >= _KNEE_GOOD_RATIO:
            self.knee_bps = max(self.knee_bps, min(send, self.rate))

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
        """Policer from elasticity: faster send did not raise unique.

        Rising loss vs a slower send is enough: `_pipe_oversend` is false
        while unique/send is still 0.95 (coverable), which is how Spain
        walked 900 → 1.2 Gbit with an empty queue.
        """
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
        if (
            self._unique_cliff()
            and self.last_delivery > 0
            and self.recv_lag
        ):
            live = max(self.min_bps, self.last_delivery * _POLICER_AIM)
            fill = self._starved_fill_bps()
            # Dead window: unique below ~80 Mbit. A 114 Mbit dip at 830
            # is not C=200 (UDP iperf still delivers 800).
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
        """A HOL cliff is not C. Drop the burst filter so last_good can fall."""
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
            self.last_good = min(self.last_good, self.rate)
        self.last_step_ts = now

    def _recover_wrecked(self, now: float) -> None:
        """Window died under a stale gigabit filter. Always drop that
        burst sample. Trickle vs HOL only picks the cut: fill vs last_good×0.75.
        A 128 Mbit HOL dip at last_good=600 is not C=210.
        """
        if self._locked_c():
            self._hold_locked_c()
            self.last_step_ts = now
            self._emit("hold_knee")
            return
        fill = self._starved_fill_bps()
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
        # Mid-transfer HOL on a latched plateau (unique ~400 Mbit, not a
        # 50 Mbit dead window): sit on the knee. Forgetting C ratchets
        # last_good×0.75 down to ~50 MiB/s.
        if self.knee_bps > 0 and not trickle:
            self.rate = self.knee_bps
            self.last_good = max(self.last_good, self.knee_bps)
            self.saw_loss_knee = True
            self.last_step_ts = now
            self._emit("hol_hold_knee")
            return
        # Unique cliffed vs the burst filter: that sample is not C.
        # Spain 2026-09-13: unq=235 vs bw=1016 was not a trickle (fill~209),
        # so max_bw stayed 1016 and unique_cliff blocked every later probe.
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
        # Keep a collapse only if the trial itself sat near the pre-cut
        # unique (overshoot 1348→1099). A HOL dip 830→200 is not C.
        keep_collapse = collapsed and self.rate >= self.measure_delivery * 0.70
        keep = (
            (unique_held and lost_less and decoded_ok and follows_trial)
            or fat_plateau
            or keep_collapse
        )
        if keep:
            self.last_good = min(self.last_good, self.rate)
            self.measure_need_decode = False
            self._emit(
                f"measure_keep held={int(unique_held)} less={int(lost_less)} "
                f"follow={int(follows_trial)} fat={int(fat_plateau)} "
                f"collapse={int(keep_collapse)} "
                f"rb={ratio_before:.2f} ra={ratio_after:.2f}"
            )
        else:
            self.rate = self.measure_restore
            self.last_good = self.measure_good
            self.measure_holdoff = now + max(1.5, 12.0 * self._rtt_s())
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
            bw = self.bw.max_bw or self.rate
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
        else:
            self.last_send_rate = 0.0
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
        self._note_knee()
        self.bw.observe(unique_rate)
        if self.phase != MEASURE:
            send_ref = (
                self.last_send_rate if self.last_send_rate > 0 else self._send_ref()
            )
            self.send_samples.append((send_ref, unique_rate))
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
        path_loss: float | None = None,
    ) -> float:
        if feedback_id <= self.last_fb:
            return self.rate
        self.last_fb = feedback_id
        raw_rtt = rtt_from_echo(now, echo_ts_us)
        qdelay = self.rtt.observe(now, raw_rtt)
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
        if self._unique_cliff() and self.recv_lag:
            self.cliff_n += 1
        elif self._limiter_stale() and self.recv_lag:
            self.cliff_n += 1
        else:
            self.cliff_n = 0
        recv_lag = bool(window_full and self.recv_lag)
        step_s = 0.16 if self.min_rtt is None else max(0.12, min(0.40, 2.0 * self.min_rtt))
        settled = now - self.last_step_ts >= max(step_s, 2.0 * self._rtt_s())
        self._note_bw(settled)

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
            elif self.path_kind == PATH_DROPPER:
                self._lock_knee(now)
            elif oversend_held and (self._unique_cliff() or self._limiter_stale()):
                self._recover_wrecked(now)
                self._enter_cruise(now, take_rate=False)
                self._emit("startup_cliff_cut")
            elif oversend_held and (settled or self._unique_cliff()):
                self._start_measure(now)
            elif settled and policer:
                self._cut_to_delivery(now)
                self._enter_cruise(now)
                self._emit("startup_loss_knee")
            elif at_cap:
                self._emit("startup_at_cap")
                self._enter_cruise(now)
            elif self._startup_bw_stalled(settled) and not starved:
                bw = self.bw.max_bw or self.rate
                # Unique stall at 180 Mbit is still filling from 8, not C.
                # Freeze only when first-flight / dropper loss grew.
                if self._path_loss_grew() or self._dropper_knee_sample():
                    self._emit("startup_stall")
                    self._lock_knee(now)
                else:
                    if self.was_fat:
                        aimed = max(self.min_bps, bw)
                    else:
                        aimed = max(self.min_bps, bw * _POLICER_AIM)
                    if self.rate > aimed:
                        self.rate = aimed
                    self._emit("startup_stall")
                    self._enter_cruise(now, take_rate=not self.recv_lag)
            elif now - self.last_step_ts >= step_s:
                self.last_step_ts = now
                bw = self.bw.max_bw or self.rate
                aimed = bw * self._startup_gain()
                self.rate = min(ceiling, max(self.rate, aimed))
        elif self.phase == DRAIN:
            drained_long = now - self.drain_ts >= _DRAIN_MAX_S
            if policer:
                self._cut_to_delivery(now)
                self._enter_cruise(now)
            elif oversend_held and (self._unique_cliff() or self._limiter_stale()):
                self._recover_wrecked(now)
                self._enter_cruise(now, take_rate=False)
                self._emit("drain_cliff_cut")
            elif oversend_held:
                self._start_measure(now)
            elif self.low_delay_n >= 2 or drained_long:
                self._enter_cruise(now)
            elif now - self.last_step_ts >= step_s and self.drain_cuts < _DRAIN_MAX_CUTS:
                self.last_step_ts = now
                self.drain_cuts += 1
                self.rate = self._clip(self._drain_target())
        elif self.phase == CRUISE:
            empty_pipe = (
                self.last_unique < 16 * _BLOCK_BYTES
                or self._unique_cliff()
                or (
                    self.last_delivery > 0
                    and self.last_delivery < self._starved_fill_bps() * 0.40
                )
            )
            if buffer_full:
                # Queue can appear after a dropper lock (path change).
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
            elif self._locked_c() and not self._may_search_past_lock():
                # Confirmed dropper, or soft lock while HOL/dirty first-flight.
                # A quiet pipe after a false knee must fall through to fill/probe.
                self._hold_locked_c()
            elif starved and not oversend and empty_pipe:
                # Spain 22 Mbit / open=1: probe +10% and qdelay drain
                # sat for 40s. Jump toward two-block BDP, then follow.
                # A 90 Mbit shaper is also "starved" vs 2-block fill — that
                # is C, not an empty pipe. Only fill if unique is a trickle.
                hard = min(self.max_bps, self._inflight_ceiling())
                ceiling = min(hard, max(self.rate * _STEP_MAX, self._starved_fill_bps()))
                if now - self.last_step_ts >= step_s and self.rate < ceiling * 0.98:
                    self.last_step_ts = now
                    self.rate = min(ceiling, self._nudge(_STARTUP_GAIN))
                    self._emit("cruise_fill")
            elif self.path_kind == PATH_DROPPER or self._path_loss_grew() or self._loss_grew():
                self._lock_knee(now)
            elif settled and policer:
                self._cut_to_delivery(now)
                self._emit("cruise_policer")
            elif oversend_held and (self._unique_cliff() or self._limiter_stale()):
                self._recover_wrecked(now)
                self._emit("cruise_cliff_cut")
            elif settled and oversend_held:
                self._start_measure(now)
            elif (
                self._may_search_past_lock()
                and self.path_kind not in (PATH_DROPPER, PATH_QUEUE)
                and not holding
                and not policer
                and not oversend
                and qdelay < self._qdelay_stop()
                and self.rate < self._raise_ceiling() * 0.98
            ):
                follow = (
                    self.last_delivery > 0
                    and self.last_delivery >= self.rate * _IID_RATIO
                )
                # Stale max_bw (startup 1 Gbit, now sending 259) looks like
                # a unique cliff. Live unique tracking send is not a stall.
                if not self._unique_cliff() or follow:
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
                        pass
                    elif now - self.cruise_ts >= wait_s:
                        self.phase = PROBE
                        self.probe_base = self.rate
                        self.probe_decoded = self.last_decoded_rate
                        self.probe_unique = self.last_delivery
                        if plateau:
                            gain = _PROBE_GAIN
                        elif follow or below_c:
                            gain = _STEP_MAX
                        else:
                            gain = _PROBE_GAIN
                        self.rate = min(self._raise_ceiling(), self._nudge(gain))
                        self.probe_until = now + max(0.32, 4.0 * self._rtt_s())
        elif self.phase == PROBE:
            unique_held = self.last_delivery > 0 and (
                (
                    self.probe_unique > 0
                    and self.last_delivery >= self.probe_unique * 0.95
                )
                or (
                    self.probe_base > 0
                    and self.last_delivery >= self.probe_base * 0.95
                )
            )
            lag_stuck = recv_lag and not unique_held and not starved
            dropper = self.path_kind == PATH_DROPPER or self._path_loss_grew()
            if buffer_full or lag_stuck or policer or oversend_held or dropper:
                why = (
                    "qdelay"
                    if buffer_full
                    else "lag"
                    if lag_stuck
                    else "dropper"
                    if dropper
                    else "policer"
                    if policer
                    else "oversend"
                )
                if why == "dropper":
                    self._emit(f"probe_abort {why}")
                    self._abort_probe_dropper(now)
                else:
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
            elif now >= self.probe_until:
                decode_gained = (
                    self.probe_decoded > 0
                    and self.last_decoded_rate >= self.probe_decoded * 1.02
                )
                unique_gained = self.last_delivery > 0 and (
                    (
                        self.probe_unique > 0
                        and self.last_delivery >= self.probe_unique * 1.02
                    )
                    or (
                        self.probe_base > 0
                        and self.last_delivery >= self.probe_base * 1.02
                    )
                )
                filling = starved and not oversend
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
                    elif not self.recv_lag:
                        self.last_good = self.rate
                        self._emit("probe_keep")
                        self._enter_cruise(now, take_rate=not self.recv_lag)
                    else:
                        self._emit("probe_keep")
                        self._enter_cruise(now, take_rate=False)
                else:
                    self.rate = self.probe_base
                    self._emit("probe_revert no_gain")
                    self._enter_cruise(now, take_rate=False)

        self.rate = self._clip(self.rate)
        return self.rate
