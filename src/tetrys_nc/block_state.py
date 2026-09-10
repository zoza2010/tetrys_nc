"""Pure state and control helpers for block transfer."""

from __future__ import annotations

import math
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field

from .block_packets import BlockFeedback, OpenBlock

# Local bench at T=1350: K=384 ~1.6 ms encode / 0.26 ms decode;
# K=768 ~3.2 ms encode (~307 MiB/s) / 0.5 ms decode; K=1536 ~7.6 ms encode.
WAN_SYMBOL_SIZE = 1350
WAN_BLOCK_K = 768
WAN_ACTIVE_BYTES = 64 * 1024 * 1024
# Fixed 24% covers ~18% path loss (p/(1-p) ≈ 22%). Dirty-hour iperf at
# 850 Mbit saw ~23% loss → need ~30%; adaptive cover goes to 32%.
WAN_INITIAL_REPAIR_PCT = 24
# Fixed 850 Mbit beat 880/920 on this path: extra-repair stays in check.
WAN_START_MBIT = 850.0
# `--rate` locks. Omit `--rate` to search; cap is 10 Gbit, never the 850 lock.
WAN_PACE_CAP_MBIT = 850.0
WAN_CC_CAP_MBIT = 10000.0
# Sliding extra-repair fraction over recent non-tail completions.
EXTRA_FRAC_WINDOW = 32
EXTRA_FRAC_MIN_SAMPLES = 8
EXTRA_FRAC_BUSY = 0.12
# ~1.5 RTprop on the current Russia↔Spain path (~80 ms).
REPAIR_AGE_S = 0.12
REPAIR_COOLDOWN_S = 0.06
REPAIR_TICK_PKTS = 48
REPAIR_TICK_PKTS_MAX = 256
REPAIR_TICK_S = 0.012
REPAIR_INTERVAL_S = 0.050
TAIL_REPAIR_TICK_PKTS = 256
TAIL_REPAIR_TICK_PER_BLOCK = 48
TAIL_REPAIR_TICK_S = 0.040
TAIL_REPAIR_COOLDOWN_S = 0.020
FLIGHT_AGE_BUCKETS = max(1, int(REPAIR_AGE_S / 0.020))
# Stop blasting if the receiver vanished. After at least one ACK, 2s covers
# WAN loss bursts; never-heard waits longer for the first RTT.
CLIENT_GONE_S = 2.0
CLIENT_NEVER_S = 8.0
# Discrete adaptive FEC. Floor 4%; `--gen-overhead` locks a single level.
FEC_LEVELS = (4, 8, 12, 18, 24, 28, 32)
FEC_FLOOR_PCT = 4
FEC_MAX_PCT = 32
# Measurement cap above cover so a 50%+ blackout does not look "coverable".
FEC_NEED_CAP = 48
# 32% first-flight covers ~24% path loss. 24% left today's 23% hour
# in a DIR storm slower than bulk TCP (~23 MiB/s).
FEC_COVER_MAX = 32
FEC_QUANTILE = 95.0
# Up uses a bulk quantile so a 5% DIR tail does not walk 12→24 on a clean path.
# p95 stays the down floor (do not walk through a real first-flight need).
FEC_UP_QUANTILE = 75.0
FEC_WINDOW = 48
FEC_MIN_TRAIN = 8
FEC_CLEAN_DOWN = 24
FEC_CLEAN_DOWN_LOW = 48
FEC_SOFT_FLOOR = 18
# Omit `--gen-overhead` to search; cold start is 12%, not the old 24% lock.
FEC_COLD_PCT = 12
FEC_PROBE_PERIOD = 16
FEC_RAPTORQ_MARGIN = 2
DIR_LIGHT_SLACK = 8
# WAN A/B gates for quantile-only vs locked 24%.
ADAPTIVE_CLEAN_WIRE_MAX = 0.90
ADAPTIVE_WAN_GOODPUT_MIN = 0.97
ADAPTIVE_WAN_MIN_MIB = 75.0


@dataclass(slots=True)
class BlockGeometry:
    symbol_size: int = WAN_SYMBOL_SIZE
    block_k: int = WAN_BLOCK_K
    active_bytes: int = WAN_ACTIVE_BYTES

    @property
    def block_bytes(self) -> int:
        return self.symbol_size * self.block_k

    @property
    def active_blocks(self) -> int:
        return max(2, self.active_bytes // self.block_bytes)

    def total_blocks(self, file_size: int) -> int:
        return max(1, math.ceil(max(0, file_size) / self.block_bytes))


@dataclass(slots=True)
class SenderBlockState:
    block_id: int
    unique_rx: int = 0
    initial_repair: int = 0
    repair_emitted: int = 0
    sent_at: float = 0.0
    last_repair_ts: float = 0.0
    decode_failed: bool = False
    unique_at_age: int = -1
    first_deficit: int = -1
    repair_rounds: int = 0
    probe: bool = False
    fec_sampled: bool = False

    def repair_need(self, block_k: int, margin: int = 2, pad: int = 4) -> int:
        if self.decode_failed and self.unique_rx >= block_k + margin:
            return 8
        return max(0, block_k + margin - self.unique_rx) + (
            pad if self.unique_rx < block_k + margin else 0
        )


@dataclass
class SenderFeedbackState:
    """Idempotent feedback accumulator shared by receiver and send loop."""

    session_id: int
    feedback_id: int = -1
    unique_payload_bytes: int = 0
    decoded_file_bytes: int = 0
    echo_ts_us: int = 0
    completed: set[int] = field(default_factory=set)
    open_rx: dict[int, OpenBlock] = field(default_factory=dict)
    last_feedback_ts: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def apply(self, packet: BlockFeedback, now: float | None = None) -> bool:
        if packet.session_id != self.session_id:
            return False
        with self.lock:
            if packet.feedback_id <= self.feedback_id:
                return False
            self.feedback_id = packet.feedback_id
            self.unique_payload_bytes = max(
                self.unique_payload_bytes, packet.unique_payload_bytes
            )
            self.decoded_file_bytes = max(
                self.decoded_file_bytes, packet.decoded_file_bytes
            )
            self.echo_ts_us = packet.echo_ts_us & 0xFFFFFFFF
            self.completed.update(packet.done_blocks or [])
            incoming = {
                item.block_id: item for item in (packet.open_blocks or [])
            }
            self.open_rx.update(incoming)
            for block_id in list(self.open_rx):
                if block_id in self.completed and block_id not in incoming:
                    self.open_rx.pop(block_id, None)
            self.last_feedback_ts = time.monotonic() if now is None else now
        return True

    def client_lost(self, now: float, start_ts: float) -> bool:
        with self.lock:
            if self.feedback_id < 0:
                return now - start_ts > CLIENT_NEVER_S
            return now - self.last_feedback_ts > CLIENT_GONE_S

    def snapshot(
        self,
    ) -> tuple[set[int], dict[int, OpenBlock], int, int, int, int]:
        with self.lock:
            return (
                set(self.completed),
                dict(self.open_rx),
                self.unique_payload_bytes,
                self.decoded_file_bytes,
                self.echo_ts_us,
                self.feedback_id,
            )


def select_repair_candidates(
    states: dict[int, SenderBlockState],
    opened: dict[int, OpenBlock],
    now: float,
    *,
    block_k: int,
    tail: bool,
    age_s: float = REPAIR_AGE_S,
    cooldown_s: float = REPAIR_COOLDOWN_S,
    dir_pad: int = 4,
    dir_pad_fn=None,
) -> list[tuple[int, int, int]]:
    """Pick repair targets: smallest positive deficit, then oldest.

    Returns (need, -age_ms, block_id). Packet gaps and HOL frontiers are
    intentionally unused: only unique ESI / decode_failed matter.
    """
    candidates: list[tuple[int, int, int]] = []
    for block_id, state in states.items():
        item = opened.get(block_id)
        if item is not None:
            state.unique_rx = max(state.unique_rx, item.unique_esi)
            state.decode_failed = item.decode_failed
        age = now - state.sent_at
        failed = state.decode_failed or (item is not None and item.decode_failed)
        if age >= age_s or tail or failed:
            if state.unique_at_age < 0 and state.unique_rx > 0:
                state.unique_at_age = state.unique_rx
                state.first_deficit = max(
                    0, block_k + FEC_RAPTORQ_MARGIN - state.unique_rx
                )
        if not tail and age < age_s and not failed:
            continue
        if now - state.last_repair_ts < cooldown_s:
            continue
        deficit = max(0, block_k + FEC_RAPTORQ_MARGIN - state.unique_rx)
        pad = int(dir_pad_fn(deficit)) if dir_pad_fn is not None else dir_pad
        need = state.repair_need(block_k, margin=FEC_RAPTORQ_MARGIN, pad=pad)
        if item is None and age >= age_s:
            need = max(need, 8)
        if need > 0:
            candidates.append((need, -int(age * 1000), block_id))
    candidates.sort()
    return candidates


def ghost_flight_ready(item: OpenBlock | None) -> bool:
    """True when the receiver has held the slot for a full first-flight window."""
    return item is not None and item.age_bucket >= FLIGHT_AGE_BUCKETS


def block_loss_frac(state: SenderBlockState, block_k: int) -> float | None:
    """Loss vs initial flight (K + initial repair) at first repair age."""
    if state.unique_at_age < 0:
        return None
    flight = max(1, block_k + max(0, state.initial_repair))
    return max(0.0, min(1.0, 1.0 - state.unique_at_age / flight))


def needed_repair_pct(
    state: SenderBlockState, block_k: int, margin: int = FEC_RAPTORQ_MARGIN
) -> float | None:
    """FEC percent of K demanded by first-flight unique vs K+R0.

    Receiver unique at decode is ~K+margin and must not be used as the
    flight size. When unique_at_age is a real first-flight count, missing
    packets vs the initial blast are the FEC the block actually needed.
    """
    del margin
    extra = max(0, state.repair_emitted - state.initial_repair)
    if state.unique_at_age > 0:
        flight = max(1, block_k + max(0, state.initial_repair))
        missing = max(0, flight - state.unique_at_age)
        return min(float(FEC_NEED_CAP), 100.0 * missing / max(1, block_k))
    if extra <= 0 and not state.decode_failed:
        return 0.0
    first_def = state.first_deficit if state.first_deficit >= 0 else 0
    light = (
        state.repair_rounds <= 1
        and extra <= first_def + 4 + DIR_LIGHT_SLACK
        and not state.decode_failed
    )
    if light:
        return 0.0
    init_pct = 100.0 * max(0, state.initial_repair) / max(1, block_k)
    used = init_pct + 100.0 * extra / max(1, block_k)
    return min(float(FEC_NEED_CAP), used)


def late_unique(state: SenderBlockState) -> int:
    """Symbols that arrived after first repair age (reorder, not loss)."""
    if state.unique_at_age < 0:
        return 0
    return max(0, state.unique_rx - state.unique_at_age)


def fec_level_index(pct: float, *, min_pct: int = FEC_FLOOR_PCT, max_pct: int = FEC_MAX_PCT) -> int:
    lo = min(min_pct, max_pct)
    hi = max(min_pct, max_pct)
    clipped = min(hi, max(lo, pct))
    best = 0
    best_dist = abs(FEC_LEVELS[0] - clipped)
    for i, level in enumerate(FEC_LEVELS):
        if level < lo or level > hi:
            continue
        dist = abs(level - clipped)
        if dist < best_dist:
            best, best_dist = i, dist
    return best


def fec_level_ceil_index(
    pct: float, *, min_pct: int = FEC_FLOOR_PCT, max_pct: int = FEC_MAX_PCT
) -> int:
    """Smallest send level that still covers `pct` (round up, not nearest)."""
    lo = min(min_pct, max_pct)
    hi = max(min_pct, max_pct)
    clipped = min(hi, max(lo, pct))
    last = 0
    for i, level in enumerate(FEC_LEVELS):
        if level < lo or level > hi:
            continue
        last = i
        if level >= clipped:
            return i
    return last


@dataclass(slots=True)
class BlockLossSample:
    """Per-block telemetry used by FEC/DIR/CC. Tail and incomplete are not trained."""

    first_flight_loss: float | None
    needed_repair_pct: float | None
    deficit_at_age: int
    repair_rounds: int
    extra_symbols: int
    late_unique: int
    decode_failed: bool
    train: bool
    qdelay_high: bool = False
    rank_known: bool = False
    probe: bool = False

    def dir_pressure(self, dir_pad: int = 4) -> bool:
        """Repeat rounds / growing deficit / decode failure — not one light DIR."""
        if self.repair_rounds >= 2:
            return True
        if self.decode_failed and self.repair_rounds >= 1:
            return True
        budget = max(0, self.deficit_at_age) + max(0, dir_pad) + DIR_LIGHT_SLACK
        if self.repair_rounds <= 1 and self.extra_symbols <= budget:
            return False
        return self.extra_symbols > budget


def make_block_sample(
    state: SenderBlockState,
    block_k: int,
    *,
    tail: bool,
    qdelay_high: bool = False,
) -> BlockLossSample:
    extra = max(0, state.repair_emitted - state.initial_repair)
    rank_known = state.unique_at_age > 0
    need = needed_repair_pct(state, block_k)
    if not rank_known:
        need = 0.0
    return BlockLossSample(
        first_flight_loss=block_loss_frac(state, block_k),
        needed_repair_pct=need,
        deficit_at_age=state.first_deficit if state.first_deficit >= 0 else 0,
        repair_rounds=state.repair_rounds,
        extra_symbols=extra,
        late_unique=late_unique(state),
        decode_failed=state.decode_failed,
        train=bool(
            not tail
            and state.unique_rx > 0
            and (rank_known or (extra == 0 and state.unique_rx >= block_k))
        ),
        qdelay_high=qdelay_high,
        rank_known=rank_known,
        probe=state.probe,
    )


def dir_lightweight(sample: BlockLossSample, dir_pad: int = 4) -> bool:
    return not sample.dir_pressure(dir_pad)


def percentile(samples: list[float], p: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    idx = int(round((p / 100.0) * (len(ordered) - 1)))
    return ordered[min(len(ordered) - 1, max(0, idx))]


def repair_tick_limits(total_need: int, *, tail: bool) -> tuple[int, float]:
    """Packet budget and wall-time cap for one repair tick."""
    if tail:
        return TAIL_REPAIR_TICK_PKTS, TAIL_REPAIR_TICK_S
    budget = min(REPAIR_TICK_PKTS_MAX, max(REPAIR_TICK_PKTS, max(0, int(total_need))))
    tick_s = REPAIR_TICK_S if budget <= REPAIR_TICK_PKTS else max(REPAIR_TICK_S, 0.024)
    return budget, tick_s


@dataclass(slots=True)
class ExtraRepairWindow:
    """Recent CC-pressure completions. Light one-round DIR is not pressure."""

    extra_n: int = 0
    _pos: int = 0
    _ring: list[int] = field(default_factory=list)

    def observe(self, extra: bool) -> None:
        bit = 1 if extra else 0
        ring = self._ring
        if len(ring) < EXTRA_FRAC_WINDOW:
            ring.append(bit)
            self.extra_n += bit
            return
        old = ring[self._pos]
        ring[self._pos] = bit
        self.extra_n += bit - old
        self._pos = (self._pos + 1) % EXTRA_FRAC_WINDOW

    @property
    def frac(self) -> float:
        n = len(self._ring)
        if n < EXTRA_FRAC_MIN_SAMPLES:
            return 0.0
        return self.extra_n / n

    def pressure(self, tail: bool = False) -> bool:
        return (not tail) and self.frac >= EXTRA_FRAC_BUSY


@dataclass(slots=True)
class RepairDebtController:
    """Initial FEC learned from completed-block repair debt, never packet gaps."""

    pct: float = 20.0
    up_alpha: float = 0.04
    down_alpha: float = 0.16
    slew: float = 1.5
    min_pct: float = 12.0
    max_pct: float = 28.0
    debt_need: int = 4
    clean_need: int = 8
    debt_n: int = 0
    clean_n: int = 0

    def observe(self, extra_symbols: int, block_k: int) -> int:
        # Hold the configured floor. One dirty block or a tail storm must not
        # yank primary FEC; only a short streak of extra-repair completions.
        if extra_symbols <= 0:
            self.debt_n = 0
            self.clean_n += 1
            if self.clean_n >= self.clean_need and self.pct > self.min_pct:
                nxt = (1.0 - self.down_alpha) * self.pct + self.down_alpha * self.min_pct
                self.pct = max(self.min_pct, min(self.pct, nxt))
            return self.current
        self.clean_n = 0
        self.debt_n += 1
        if self.debt_n < self.debt_need:
            return self.current
        debt_pct = 100.0 * max(0, extra_symbols) / max(1, block_k)
        target = min(
            self.max_pct,
            max(self.min_pct, min(debt_pct, self.max_pct - 2.0) + 2.0),
        )
        if target <= self.pct:
            return self.current
        nxt = (1.0 - self.up_alpha) * self.pct + self.up_alpha * target
        self.pct = min(self.max_pct, max(self.min_pct, min(self.pct + self.slew, nxt)))
        return self.current

    @property
    def current(self) -> int:
        return int(round(min(self.max_pct, max(self.min_pct, self.pct))))


@dataclass
class QuantileFecController:
    """Discrete FEC from first-flight need. Slow down, bulk-undercover up.

    Down uses p95 so a quiet path can leave 24% but not walk through a real
    need. Up uses p75: DIR pad often closes a 23% miss in one or two rounds,
    so storm-only (repair_rounds ≥ 3) never left 12%. Isolated DIR / a 5%
    tail keep p75 at 0 and must not raise.
    """

    mode: str = "quantile"
    start_pct: int = WAN_INITIAL_REPAIR_PCT
    min_pct: int = FEC_FLOOR_PCT
    max_pct: int = FEC_MAX_PCT
    level_idx: int = 0
    clean_n: int = 0
    probe_fail_at: int = -1
    reason: str = "cold"
    dir_rounds: int = 0
    needed: deque[float] = field(default_factory=lambda: deque(maxlen=FEC_WINDOW))
    repair_loss: deque[float] = field(default_factory=lambda: deque(maxlen=FEC_WINDOW))

    def __post_init__(self) -> None:
        self.mode = (self.mode or "quantile").strip().lower()
        if self.mode not in ("fixed", "quantile"):
            self.mode = "quantile"
        if self.mode == "fixed":
            locked = int(self.start_pct)
            self.min_pct = self.max_pct = locked
            self.level_idx = fec_level_index(locked, min_pct=FEC_FLOOR_PCT, max_pct=FEC_MAX_PCT)
            self.reason = f"fixed {locked}"
            return
        self.min_pct = int(min(self.max_pct, max(FEC_LEVELS[0], self.min_pct)))
        self.max_pct = int(max(self.min_pct, min(FEC_MAX_PCT, self.max_pct)))
        self.level_idx = fec_level_index(
            self.start_pct, min_pct=self.min_pct, max_pct=self.max_pct
        )
        self.reason = f"cold start={self.current} mode={self.mode}"

    @property
    def current(self) -> int:
        if self.mode == "fixed":
            return int(self.start_pct)
        return int(FEC_LEVELS[self.level_idx])

    def encode_pct(self, block_id: int) -> int:
        """Most blocks at current; every Nth is a probe one level below."""
        if self.mode == "fixed" or self.level_idx <= 0:
            return self.current
        nxt = FEC_LEVELS[self.level_idx - 1]
        if nxt < self.min_pct:
            return self.current
        if block_id % FEC_PROBE_PERIOD == 0:
            return nxt
        return self.current

    def _cover_cap(self) -> int:
        return min(int(self.max_pct), FEC_COVER_MAX)

    def dir_margin(
        self,
        deficit: int,
        block_k: int,
        *,
        tail: bool = False,
        tick_left: int | None = None,
    ) -> int:
        """ΔR so a low FEC level still closes in one DIR round, not two."""
        del block_k
        if deficit <= 0:
            return 0
        p = percentile(list(self.repair_loss), 90) or 0.0
        p = min(0.50, max(0.0, p))
        if not self.repair_loss:
            delta = 4
        elif p < 0.03:
            delta = 0
        else:
            delta = int(math.ceil(deficit * p / max(1e-6, 1.0 - p)))
        cover = self._cover_cap()
        if self.mode != "fixed" and self.current < cover and p >= 0.03:
            delta = max(
                delta,
                int(math.ceil(deficit * (cover - self.current) / max(1, cover))),
            )
        cap = TAIL_REPAIR_TICK_PER_BLOCK if tail else REPAIR_TICK_PKTS
        if tick_left is not None:
            cap = min(cap, max(0, tick_left))
        return min(max(0, delta), cap)

    def _skip_outlier(self, needed_pct: float) -> bool:
        if len(self.needed) < FEC_MIN_TRAIN:
            return False
        p95 = percentile(list(self.needed), 95) or 0.0
        p99 = percentile(list(self.needed), 99) or 0.0
        return needed_pct > max(p99, 2.0 * max(p95, 1.0)) and needed_pct >= 40.0

    def _down_ready(self, sample: BlockLossSample) -> bool:
        """One level at a time; below 18% only with a real first-flight snapshot."""
        if self.level_idx <= 0:
            return False
        nxt = FEC_LEVELS[self.level_idx - 1]
        if nxt < self.min_pct:
            return False
        if nxt < FEC_SOFT_FLOOR and not sample.rank_known:
            return False
        if self.probe_fail_at == nxt:
            return False
        need = FEC_CLEAN_DOWN_LOW if nxt < FEC_SOFT_FLOOR else FEC_CLEAN_DOWN
        return self.clean_n >= need

    def _note_probe(self, sample: BlockLossSample) -> None:
        nxt_pct = FEC_LEVELS[self.level_idx - 1] if self.level_idx > 0 else self.current
        if sample.extra_symbols <= 0 and not sample.dir_pressure():
            if self.probe_fail_at == nxt_pct:
                self.probe_fail_at = -1
        else:
            self.probe_fail_at = nxt_pct

    def observe_block(self, sample: BlockLossSample) -> int:
        self.dir_rounds += max(0, sample.repair_rounds)
        if self.mode == "fixed":
            self.reason = f"fixed {self.current}"
            return self.current
        if not sample.train or sample.needed_repair_pct is None:
            return self.current
        needed_pct = float(sample.needed_repair_pct)
        if sample.probe:
            self._note_probe(sample)
            if sample.extra_symbols > 0 or sample.dir_pressure():
                self.reason = f"probe-fail {self.probe_fail_at}%"
                return self.current
        if self._skip_outlier(needed_pct):
            self.reason = f"skip-outlier need={needed_pct:.1f}"
            return self.current
        loss = 0.0 if sample.extra_symbols <= 0 else min(
            1.0, (sample.needed_repair_pct or 0.0) / 100.0
        )
        self.needed.append(needed_pct)
        self.repair_loss.append(loss)
        if len(self.needed) < FEC_MIN_TRAIN:
            self.reason = f"warm n={len(self.needed)} hold={self.current}"
            return self.current
        cover = self._cover_cap()
        coverable = [n for n in self.needed if n <= cover]
        src = coverable if len(coverable) >= FEC_MIN_TRAIN else list(self.needed)
        q = percentile(src, FEC_QUANTILE) or 0.0
        q_up = percentile(src, FEC_UP_QUANTILE) or 0.0
        # Two repair ticks on a delayed RTT look like a storm on small K.
        storm = sample.decode_failed or sample.repair_rounds >= 3
        if (
            sample.rank_known
            and storm
            and needed_pct <= cover
        ):
            q = max(q, needed_pct)
            q_up = max(q_up, needed_pct)
        target_idx = fec_level_index(q, min_pct=self.min_pct, max_pct=cover)
        up_idx = fec_level_ceil_index(q_up, min_pct=self.min_pct, max_pct=cover)
        saw_repair = (
            sample.extra_symbols > 0
            or sample.repair_rounds >= 1
            or sample.decode_failed
        )
        undercover = (
            sample.rank_known
            and q_up > self.current
            and saw_repair
        )
        if up_idx > self.level_idx:
            # Isolated DIR is delay/reorder (p75 stays ~0). Jump on a storm
            # or when most first-flights in the window needed more FEC.
            if (storm and sample.dir_pressure()) or undercover:
                self.level_idx = up_idx
                self.clean_n = 0
                self.probe_fail_at = -1
                why = "up-storm" if storm else "up-undercover"
                self.reason = (
                    f"{why} p{FEC_UP_QUANTILE:.0f}={q_up:.1f} -> {self.current}"
                )
                return self.current
        # Storm already zeroed clean_n on the way up. One-round DIR (pad,
        # reorder) must not freeze a high level after the path goes quiet.
        if storm and sample.dir_pressure():
            self.clean_n = 0
        else:
            self.clean_n += 1
        nxt = FEC_LEVELS[self.level_idx - 1] if self.level_idx > 0 else self.current
        if self._down_ready(sample) and nxt >= FEC_LEVELS[target_idx]:
            self.level_idx -= 1
            self.clean_n = 0
            self.probe_fail_at = -1
            self.reason = f"down p{FEC_QUANTILE:.0f}={q:.1f} -> {self.current}"
            return self.current
        self.reason = (
            f"hold p{FEC_QUANTILE:.0f}={q:.1f} {self.current}% "
            f"clean={self.clean_n}"
        )
        return self.current


def adaptive_start_pct(initial: int, mode: str, floor: int = FEC_FLOOR_PCT) -> int:
    """Fixed mode keeps the lock; adaptive cold-start is min(initial, 12%)."""
    if (mode or "").strip().lower() == "fixed":
        return int(initial)
    return max(int(floor), min(int(initial), FEC_COLD_PCT))


def resolve_fec_cli(
    overhead: int | None,
    *,
    env_mode: str | None = None,
) -> tuple[str, int]:
    """`--gen-overhead N` locks FEC; omit it to run adaptive quantile."""
    if overhead is not None:
        return "fixed", int(overhead)
    _ = env_mode
    return "quantile", FEC_COLD_PCT


def make_fec_controller(
    initial_repair_pct: int,
    *,
    mode: str | None = None,
    floor_pct: int | None = None,
    max_pct: int | None = None,
    clamp_cold: bool = False,
) -> QuantileFecController:
    chosen = (mode or "quantile").strip().lower()
    if chosen not in ("fixed", "quantile"):
        chosen = "quantile"
    if chosen == "fixed":
        return QuantileFecController(
            mode="fixed",
            start_pct=int(initial_repair_pct),
            min_pct=int(initial_repair_pct),
            max_pct=int(initial_repair_pct),
        )
    floor = FEC_FLOOR_PCT if floor_pct is None else int(floor_pct)
    top = FEC_MAX_PCT if max_pct is None else int(max_pct)
    start = int(initial_repair_pct)
    if clamp_cold:
        start = adaptive_start_pct(start, chosen, floor)
    return QuantileFecController(
        mode=chosen,
        start_pct=start,
        min_pct=floor,
        max_pct=top,
    )


@dataclass(slots=True)
class FecRunMetrics:
    goodput_mib: float
    source_wire_mib: float
    repair_wire_mib: float = 0.0
    first_close_pct: float = 0.0
    tail_s: float = 0.0
    pace_p10: float = 0.0
    pace_med: float = 0.0
    dir_rounds: int = 0

    @property
    def total_wire_mib(self) -> float:
        return self.source_wire_mib + self.repair_wire_mib


def parse_done_metrics(log: str) -> FecRunMetrics | None:
    """Parse BlockSender print_done line for matched A/B logs."""
    if "goodput " not in log or "source_wire=" not in log:
        return None

    def _find(pattern: str) -> str | None:
        m = re.search(pattern, log)
        return m.group(1) if m else None

    goodput = _find(r"goodput ([\d.]+) MiB/s")
    source = _find(r"source_wire=([\d.]+)MiB")
    repair = _find(r"repair_wire=([\d.]+)MiB")
    close = _find(r"first_close=([\d.]+)%")
    if goodput is None or source is None or repair is None:
        return None

    def _f(val: str | None, default: float = 0.0) -> float:
        if val is None or val == "n/a":
            return default
        return float(val)

    return FecRunMetrics(
        goodput_mib=float(goodput),
        source_wire_mib=float(source),
        repair_wire_mib=float(repair),
        first_close_pct=_f(close),
        dir_rounds=int(_find(r"dir_rounds=(\d+)") or 0),
        tail_s=_f(_find(r"tail=([\d.]+)s")),
        pace_p10=_f(_find(r"pace_p10=([\d.]+|n/a)")),
        pace_med=_f(_find(r"med=([\d.]+)")),
    )


def adaptive_gate_failures(
    profile: str,
    fixed24: FecRunMetrics,
    adaptive: FecRunMetrics,
    *,
    wan_min_mib: float = ADAPTIVE_WAN_MIN_MIB,
) -> list[str]:
    """Quantile-only acceptance vs locked 24%. Empty list means pass."""
    fails: list[str] = []
    kind = profile.strip().lower()
    clean = kind.startswith("clean")
    if clean:
        if adaptive.total_wire_mib > fixed24.total_wire_mib * ADAPTIVE_CLEAN_WIRE_MAX:
            fails.append("clean wire not >=10% below fixed-24")
    else:
        if adaptive.goodput_mib < fixed24.goodput_mib * ADAPTIVE_WAN_GOODPUT_MIN:
            fails.append("WAN/burst median goodput >3% worse than fixed-24")
        if adaptive.goodput_mib < wan_min_mib:
            fails.append(f"WAN min goodput below {wan_min_mib:.0f} MiB/s")
    if adaptive.tail_s > fixed24.tail_s * 1.25 + 0.50:
        fails.append("tail worse than baseline")
    if (
        fixed24.pace_p10 > 0
        and adaptive.pace_p10 > 0
        and adaptive.pace_p10 < fixed24.pace_p10 * 0.90
    ):
        fails.append("pace_p10 worse than baseline")
    return fails
