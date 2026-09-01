"""Pure state and control helpers for block transfer."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field

from .block_packets import BlockFeedback, OpenBlock

# Local bench at T=1350: K=384 ~1.6 ms encode / 0.26 ms decode;
# K=768 ~3.2 ms encode (~307 MiB/s) / 0.5 ms decode; K=1536 ~7.6 ms encode.
WAN_SYMBOL_SIZE = 1350
WAN_BLOCK_K = 768
WAN_ACTIVE_BYTES = 64 * 1024 * 1024
# 20% left dirty-run p90 loss uncovered (p/(1-p) ≈ 23% at 18% path loss).
WAN_INITIAL_REPAIR_PCT = 24
# Fixed 850 Mbit beat 880/920 on this path: extra-repair stays in check.
WAN_START_MBIT = 850.0
WAN_PACE_CAP_MBIT = 850.0
# Sanity clip for CC output (not a path estimate). --rate locks WAN_PACE_CAP.
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
# Stop blasting if the receiver vanished. After at least one ACK, 2s covers
# WAN loss bursts; never-heard waits longer for the first RTT.
CLIENT_GONE_S = 2.0
CLIENT_NEVER_S = 8.0


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
            for block_id in packet.done_blocks or []:
                self.open_rx.pop(block_id, None)
            self.open_rx = {
                item.block_id: item
                for item in (packet.open_blocks or [])
                if item.block_id not in self.completed
            }
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
            if state.unique_at_age < 0:
                state.unique_at_age = state.unique_rx
        if not tail and age < age_s and not failed:
            continue
        if now - state.last_repair_ts < cooldown_s:
            continue
        need = state.repair_need(block_k)
        if item is None and age >= age_s:
            need = max(need, 8)
        if need > 0:
            candidates.append((need, -int(age * 1000), block_id))
    candidates.sort()
    return candidates


def block_loss_frac(state: SenderBlockState, block_k: int) -> float | None:
    """Loss vs initial flight (K + initial repair) at first repair age."""
    if state.unique_at_age < 0:
        return None
    flight = max(1, block_k + max(0, state.initial_repair))
    return max(0.0, min(1.0, 1.0 - state.unique_at_age / flight))


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
    """Recent extra vs first-close completions. Mixes do not reset the signal."""

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
