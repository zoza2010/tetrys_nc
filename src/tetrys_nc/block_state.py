"""Pure state and control helpers for block transfer."""

from __future__ import annotations

import math
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field

from .block_packets import MAX_OPEN_BLOCKS, BlockFeedback, OpenBlock

# Local bench at T=1350: K=384 ~1.6 ms encode / 0.26 ms decode;
# K=768 ~3.2 ms encode (~307 MiB/s) / 0.5 ms decode; K=1536 ~7.6 ms encode.
WAN_SYMBOL_SIZE = 1350
WAN_BLOCK_K = 768
WAN_ACTIVE_BYTES = 64 * 1024 * 1024
# Locked `--gen-overhead 24` covers ~18% path loss (p/(1-p)).
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
# Window-full HOL (Spain: pace=752, src=0, rpr=5120 ≈ 55 Mbit): drip
# 256 pkt / 50 ms starves the limiter. Blast repair; limiter paces.
HOL_REPAIR_TICK_PKTS = 2048
HOL_REPAIR_TICK_S = 0.20
HOL_EXTRA_FRAC = 0.40
TAIL_REPAIR_TICK_PKTS = 256
TAIL_REPAIR_TICK_PER_BLOCK = 48
TAIL_REPAIR_TICK_S = 0.040
TAIL_REPAIR_COOLDOWN_S = 0.020
FLIGHT_AGE_BUCKETS = max(1, int(REPAIR_AGE_S / 0.020))
# Stop blasting if the receiver vanished. After at least one ACK, 2s covers
# WAN loss bursts; never-heard waits longer for the first RTT.
CLIENT_GONE_S = 2.0
CLIENT_NEVER_S = 8.0
# First-flight pad is static (`--gen-overhead` or FEC_STATIC_PCT). DIR
# drips until decode. CC owns the wire rate.
FEC_NEED_CAP = 48
# 32% first-flight covers ~24% path loss (p/(1-p)). HOL cover cap.
FEC_COVER_MAX = 32
FEC_WINDOW = 48
FEC_STATIC_PCT = 8
FEC_RAPTORQ_MARGIN = 2
DIR_LIGHT_SLACK = 8


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
    rx_at_flight: int = -1
    first_deficit: int = -1
    repair_rounds: int = 0
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
            # The 80-open ACK rotates. Unbounded merge grew to 4471 and
            # the send loop looked stuck (Spain lock-921, open=4471).
            cap = MAX_OPEN_BLOCKS * 2
            if len(self.open_rx) > cap:
                stale = [bid for bid in self.open_rx if bid not in incoming]
                drop = len(self.open_rx) - cap
                for bid in stale[:drop]:
                    self.open_rx.pop(bid, None)
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
    prefer_oldest: bool = False,
) -> list[tuple[int, int, int]]:
    """Pick repair targets: smallest positive deficit, then oldest.

    `prefer_oldest` is the full-window HOL case: smallest-need first
    spends the 256-pkt drip on almost-done blocks and never feeds the
    frontier. Returns (need, -age_ms, block_id).
    """
    candidates: list[tuple[int, int, int]] = []
    for block_id, state in states.items():
        item = opened.get(block_id)
        if item is not None:
            apply_open_block(state, item)
        age = now - state.sent_at
        failed = state.decode_failed or (item is not None and item.decode_failed)
        if age >= age_s or tail or failed:
            if state.unique_at_age < 0 and state.unique_rx > 0:
                state.unique_at_age = state.unique_rx
                if state.first_deficit < 0:
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
        # `tail` is true as soon as the last source block is admitted —
        # often 64 in-flight. Dripping all of them is a repair storm.
        # Only the drained window (Spain 2070/2072, rpr=0) needs a nudge.
        if tail and len(states) <= 8:
            need = max(need, 8)
        if need > 0:
            candidates.append((need, -int(age * 1000), block_id))
    if prefer_oldest:
        candidates.sort(key=lambda t: (t[1], t[0], t[2]))
    else:
        candidates.sort()
    return candidates


def ghost_flight_ready(item: OpenBlock | None) -> bool:
    """True when the receiver has held the slot for a full first-flight window."""
    return item is not None and item.age_bucket >= FLIGHT_AGE_BUCKETS


def apply_open_block(state: SenderBlockState, item: OpenBlock) -> None:
    """Copy live unique and the receiver-local first-flight freeze."""
    state.unique_rx = max(state.unique_rx, item.unique_esi)
    state.decode_failed = item.decode_failed or state.decode_failed
    if item.unique_at_flight >= 0 and state.rx_at_flight < 0:
        state.rx_at_flight = item.unique_at_flight


def first_flight_unique(state: SenderBlockState) -> int:
    """Prefer receiver freeze; sender-clock unique_at_age is ACK-delayed."""
    if state.rx_at_flight >= 0:
        return state.rx_at_flight
    return state.unique_at_age


def cover_loss_p(cover_pct: int) -> float:
    """Packet drop `p` that `cover_pct` FEC first-closes: r/(100+r)."""
    r = max(0, int(cover_pct))
    return r / (100.0 + r)


def block_loss_frac(state: SenderBlockState, block_k: int) -> float | None:
    """Loss vs initial flight (K + initial repair) at first repair age."""
    got = first_flight_unique(state)
    if got < 0:
        return None
    flight = max(1, block_k + max(0, state.initial_repair))
    return max(0.0, min(1.0, 1.0 - got / flight))


def fec_pct_for_loss(loss: float) -> float:
    """Repair percent of K that first-closes packet loss `p`: p/(1-p).

    `needed_repair_pct` is missing/K against the already-padded flight, so a
    28% blast with 19% drop reports ~25% need and refuses to leave 28%.
    """
    p = min(0.49, max(0.0, float(loss)))
    return 100.0 * p / max(1e-9, 1.0 - p)


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
    got = first_flight_unique(state)
    if got > 0:
        flight = max(1, block_k + max(0, state.initial_repair))
        missing = max(0, flight - got)
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
    got = first_flight_unique(state)
    if got < 0:
        return 0
    return max(0, state.unique_rx - got)


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
    rank_known = first_flight_unique(state) > 0
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
    )


def dir_lightweight(sample: BlockLossSample, dir_pad: int = 4) -> bool:
    return not sample.dir_pressure(dir_pad)


def percentile(samples: list[float], p: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    idx = int(round((p / 100.0) * (len(ordered) - 1)))
    return ordered[min(len(ordered) - 1, max(0, idx))]


def repair_tick_limits(
    total_need: int, *, tail: bool, window_full: bool = False
) -> tuple[int, float]:
    """Packet budget and wall-time cap for one repair tick."""
    if tail:
        return TAIL_REPAIR_TICK_PKTS, TAIL_REPAIR_TICK_S
    if window_full:
        need = max(0, int(total_need))
        budget = min(HOL_REPAIR_TICK_PKTS, max(REPAIR_TICK_PKTS_MAX, need))
        return budget, HOL_REPAIR_TICK_S
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



@dataclass
class FecController:
    """Static first-flight pad. DIR drips until decode; CC owns wire rate."""

    start_pct: int = FEC_STATIC_PCT
    reason: str = "fixed"
    dir_rounds: int = 0
    path_loss: deque[float] = field(default_factory=lambda: deque(maxlen=FEC_WINDOW))
    repair_loss: deque[float] = field(default_factory=lambda: deque(maxlen=FEC_WINDOW))

    def __post_init__(self) -> None:
        self.start_pct = int(self.start_pct)
        self.reason = f"fixed {self.start_pct}"

    @property
    def current(self) -> int:
        return int(self.start_pct)

    def encode_pct(self, block_id: int) -> int:
        del block_id
        return self.current

    def _cover_cap(self) -> int:
        return FEC_COVER_MAX

    def dir_margin(
        self,
        deficit: int,
        block_k: int,
        *,
        tail: bool = False,
        tick_left: int | None = None,
    ) -> int:
        """ΔR so DIR can close a remaining deficit in one round."""
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
        cap = TAIL_REPAIR_TICK_PER_BLOCK if tail else REPAIR_TICK_PKTS
        if tick_left is not None:
            cap = min(cap, max(0, tick_left))
        return min(max(0, delta), cap)

    def path_loss_q(self, q: float, *, coverable: bool = False) -> float | None:
        src = list(self.path_loss)
        if coverable:
            cap_p = cover_loss_p(self._cover_cap())
            src = [x for x in src if x <= cap_p]
        if not src:
            return None
        return percentile(src, q)

    def hol_frac(self) -> float | None:
        """Share of first-flight samples above FEC cover (window HOL, not C)."""
        if not self.path_loss:
            return None
        cap_p = cover_loss_p(self._cover_cap())
        hol = sum(1 for x in self.path_loss if x > cap_p)
        return hol / len(self.path_loss)

    def observe_block(self, sample: BlockLossSample, **_kwargs) -> int:
        self.dir_rounds += max(0, sample.repair_rounds)
        if sample.first_flight_loss is not None:
            self.path_loss.append(float(sample.first_flight_loss))
        if sample.train and sample.needed_repair_pct is not None:
            loss = (
                0.0
                if sample.extra_symbols <= 0
                else min(1.0, float(sample.needed_repair_pct) / 100.0)
            )
            self.repair_loss.append(loss)
        self.reason = f"fixed {self.current}"
        return self.current


def resolve_fec_cli(overhead: int | None) -> tuple[str, int]:
    """`--gen-overhead N` locks FEC; omit it for a static first-flight pad."""
    if overhead is not None:
        return "fixed", int(overhead)
    return "fixed", FEC_STATIC_PCT


def make_fec_controller(initial_repair_pct: int, **_kwargs) -> FecController:
    return FecController(start_pct=int(initial_repair_pct))


def block_quartile(block_id: int, total: int) -> int:
    """0..3 bucket by block index so first_close miss is not a single scalar."""
    if total <= 0:
        return 0
    return min(3, max(0, (int(block_id) * 4) // int(total)))


def fmt_quartile_pcts(hits: list[int], seen: list[int]) -> str:
    parts: list[str] = []
    for hit, n in zip(hits, seen):
        if n <= 0:
            parts.append("-")
        else:
            parts.append(str(int(round(100.0 * hit / n))))
    return "/".join(parts)


def fmt_quartile_counts(counts: list[int]) -> str:
    return "/".join(str(int(n)) for n in counts)


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
