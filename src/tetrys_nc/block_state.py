"""Pure state and control helpers for reorder-insensitive transfer v2."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field

from .block_packets import BlockFeedbackV2, OpenBlock

# Local bench at T=1350: K=384 ~1.6 ms encode / 0.26 ms decode;
# K=768 ~3.2 ms encode (~307 MiB/s) / 0.5 ms decode; K=1536 ~7.6 ms encode.
WAN_SYMBOL_SIZE = 1350
WAN_BLOCK_K = 768
WAN_ACTIVE_BYTES = 64 * 1024 * 1024
WAN_INITIAL_REPAIR_PCT = 20
# Path carries ~900 Mbit at ~0.12% loss (iperf); 1 Gbit already drops ~13%.
WAN_START_MBIT = 700.0
WAN_PACE_CAP_MBIT = 920.0
# ~3 RTprop on the current Russia↔Spain path (~80 ms).
REPAIR_AGE_S = 0.24
REPAIR_COOLDOWN_S = 0.06
REPAIR_TICK_PKTS = 48
REPAIR_TICK_PER_BLOCK = 16
REPAIR_TICK_S = 0.012
REPAIR_INTERVAL_S = 0.050
TAIL_REPAIR_TICK_PKTS = 256
TAIL_REPAIR_TICK_PER_BLOCK = 48
TAIL_REPAIR_TICK_S = 0.040
TAIL_REPAIR_COOLDOWN_S = 0.020


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
    completed: set[int] = field(default_factory=set)
    open_rx: dict[int, OpenBlock] = field(default_factory=dict)
    last_feedback_ts: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def apply(self, packet: BlockFeedbackV2, now: float | None = None) -> bool:
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

    def snapshot(self) -> tuple[set[int], dict[int, OpenBlock], int, int]:
        with self.lock:
            return (
                set(self.completed),
                dict(self.open_rx),
                self.unique_payload_bytes,
                self.decoded_file_bytes,
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


@dataclass(slots=True)
class AckPacer:
    """ACK-clock paced only by short-interval unique receive bytes."""

    min_bps: float
    max_bps: float
    offer_bps: float
    confirmed_bps: float = 0.0
    delivery_bps: float = 0.0
    ewma_bps: float = 0.0
    last_unique: int = 0
    last_ts: float = 0.0
    last_dt: float = 0.0
    last_sample_bps: float = 0.0
    startup: bool = True
    plateau_n: int = 0
    weak_n: int = 0
    weak_events: int = 0
    cut_events: int = 0
    held_repair_events: int = 0
    repair_busy: bool = False

    def _slew(self, target: float) -> float:
        hi = self.offer_bps * 1.10
        lo = self.offer_bps * 0.90
        return min(self.max_bps, max(self.min_bps, min(hi, max(lo, target))))

    def update(
        self, unique_bytes: int, now: float, repair_busy: bool | None = None
    ) -> float:
        if repair_busy is None:
            repair_busy = self.repair_busy
        # Unique=0 feedback only arms the clock after the first delivered byte.
        if unique_bytes <= 0:
            return self.offer_bps
        if self.last_ts <= 0.0:
            self.last_ts = now
            self.last_unique = unique_bytes
            return self.offer_bps
        dt = now - self.last_ts
        delta = unique_bytes - self.last_unique
        # Short delivery samples, independent of block completion / reorder.
        if dt < 0.10 or delta <= 0:
            return self.offer_bps
        sample = delta / dt
        self.delivery_bps = sample
        self.last_dt = dt
        self.last_sample_bps = sample
        if self.ewma_bps <= 0.0:
            self.ewma_bps = sample
        else:
            self.ewma_bps = 0.75 * self.ewma_bps + 0.25 * sample
        self.confirmed_bps = max(self.confirmed_bps, self.ewma_bps)
        # A blocked send loop stretches dt; do not treat that as low BtlBw.
        if dt > 0.50:
            self.last_ts = now
            self.last_unique = unique_bytes
            return self.offer_bps
        cruise = min(self.max_bps, max(self.min_bps, self.ewma_bps * 1.05))
        prev = self.offer_bps
        if self.startup:
            if sample >= self.offer_bps * 0.85:
                self.weak_n = 0
                self.plateau_n = 0
                target = min(self.max_bps, self.offer_bps * 1.06)
            elif sample >= self.offer_bps * 0.70:
                self.weak_n = 0
                self.plateau_n += 1
                target = self.offer_bps
                if self.plateau_n >= 4:
                    self.startup = False
            else:
                self.weak_n += 1
                self.plateau_n += 1
                target = self.offer_bps
                if self.weak_n >= 3:
                    self.startup = False
                    if repair_busy:
                        self.held_repair_events += 1
                        target = self.offer_bps
                    else:
                        target = cruise
            self.offer_bps = max(self._slew(target), self.min_bps)
            if self.offer_bps >= self.max_bps * 0.995:
                self.startup = False
        else:
            if sample < self.offer_bps * 0.75:
                if self.weak_n == 0:
                    self.weak_events += 1
                self.weak_n += 1
            else:
                self.weak_n = 0
            # Extra-repair makes unique ACK look weak; cutting then locks the floor.
            if repair_busy and cruise < self.offer_bps:
                self.held_repair_events += 1
                target = self.offer_bps
            elif cruise < self.offer_bps and self.weak_n < 3:
                target = self.offer_bps
            else:
                target = cruise
            self.offer_bps = self._slew(target)
        if self.offer_bps < prev * 0.995:
            self.cut_events += 1
        self.last_ts = now
        self.last_unique = unique_bytes
        return self.offer_bps


@dataclass(slots=True)
class RepairDebtController:
    """Initial FEC learned from completed-block repair debt, never packet gaps."""

    pct: float = 20.0
    up_alpha: float = 0.04
    down_alpha: float = 0.16
    slew: float = 1.5
    min_pct: float = 12.0
    max_pct: float = 22.0
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
