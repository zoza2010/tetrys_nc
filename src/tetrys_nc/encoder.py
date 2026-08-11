"""Tetrys encoder with elastic encoding window (RFC 9407 §4)."""

from __future__ import annotations

import struct
import time
from collections import OrderedDict, deque
from dataclasses import dataclass

from . import gf256
from .packets import MAGIC, VERSION, SOURCE_HDR_SIZE, CodedPacket


_SOURCE_PREFIX = struct.Struct("!BBBBII")


@dataclass(slots=True)
class EncoderConfig:
    max_window: int = 8192
    # 0 = disable periodic coding; else coded every N sources
    redundancy_every: int = 32
    # How many coded packets to emit each redundancy tick
    coded_burst: int = 1
    # How many oldest unacked symbols to mix into each coded packet
    code_degree: int = 8
    payload_size: int = 32768


class TetrysEncoder:
    def __init__(self, config: EncoderConfig | None = None) -> None:
        self.cfg = config or EncoderConfig()
        self._window: OrderedDict[int, bytes] = OrderedDict()
        self._next_source_id = 0
        self._next_coded_id = 1
        self._sources_since_coded = 0
        self._total_sent_source = 0
        self._total_sent_coded = 0
        self._redundancy_every = self.cfg.redundancy_every
        self._coded_burst = max(1, self.cfg.coded_burst)
        self._cumulative_ack = 0
        # NACK queue: age missing ids before repair (reorder hold on sender).
        # sid -> first time reported missing
        self._nack_pending: dict[int, float] = {}
        self._nack_q: deque[int] = deque()
        self._nack_set: set[int] = set()
        # sid -> last SOURCE retransmit time (cooldown against spam)
        self._rexmit_at: dict[int, float] = {}
        # sid -> first time this SOURCE was put on the wire
        self._first_sent_at: dict[int, float] = {}
        self._total_rexmit = 0
        self._unique_rexmit = 0
        self.last_plr_byte = 0
        self._send_ts_us = 0

    def stamp(self) -> int:
        """Refresh send timestamp (monotonic µs, uint32)."""
        self._send_ts_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
        return self._send_ts_us

    @property
    def send_ts_us(self) -> int:
        return self._send_ts_us

    @property
    def window_size(self) -> int:
        return len(self._window)

    @property
    def next_source_id(self) -> int:
        return self._next_source_id

    @property
    def oldest_id(self) -> int | None:
        if not self._window:
            return None
        return next(iter(self._window))

    @property
    def coded_burst(self) -> int:
        return self._coded_burst

    def can_accept(self) -> bool:
        return len(self._window) < self.cfg.max_window

    def add_source(self, payload: bytes) -> bytearray:
        """Pack SOURCE into an owned bytearray (mutable for late timestamp stamp)."""
        ps = self.cfg.payload_size
        if len(payload) > ps:
            raise ValueError("payload too large")
        if not self.can_accept():
            raise RuntimeError("elastic window full; wait for cumulative ACK")

        if len(payload) < ps:
            payload = bytes(payload) + b"\x00" * (ps - len(payload))
        elif not isinstance(payload, bytes):
            payload = bytes(payload)

        sid = self._next_source_id
        self._next_source_id += 1
        self._window[sid] = payload
        self._first_sent_at[sid] = time.monotonic()
        self._sources_since_coded += 1
        self._total_sent_source += 1

        ts = self.stamp()
        wire = bytearray(SOURCE_HDR_SIZE + ps)
        _SOURCE_PREFIX.pack_into(wire, 0, MAGIC, VERSION, 0, 0, sid, ts)
        wire[SOURCE_HDR_SIZE:] = payload
        return wire

    def maybe_coded(self) -> list[bytes]:
        """Return 0..N packed coded packets according to redundancy schedule."""
        if self._redundancy_every <= 0 or not self._window:
            return []
        if self._sources_since_coded < self._redundancy_every:
            return []
        self._sources_since_coded = 0
        return self.emit_coded(self._coded_burst)

    def emit_coded(self, n: int = 1) -> list[bytes]:
        """Force N coded packets over the oldest window (HOL repair)."""
        out: list[bytes] = []
        if n <= 0 or not self._window:
            return out
        for _ in range(n):
            pkt = self.make_coded(prefer_oldest=True)
            if pkt is None:
                break
            out.append(pkt.pack())
        return out

    def emit_repair_coded(self, limit: int = 8, cooldown: float = 0.0) -> list[bytes]:
        """
        On-demand Tetrys repair: coded packets over the HOL frontier instead of
        SOURCE retransmits. Marks tip symbols as recently repaired (cooldown).
        """
        if limit <= 0 or not self._window:
            return []
        now = time.monotonic()
        # Avoid flooding the same tip every loop iteration.
        oldest = next(iter(self._window))
        last = self._rexmit_at.get(oldest, 0.0)
        if last and cooldown > 0 and (now - last) < cooldown:
            return []
        out = self.emit_coded(limit)
        if out:
            # Cooldown tip symbols covered by the coded mix.
            for i, sid in enumerate(self._window.keys()):
                if i >= self.cfg.code_degree:
                    break
                self._rexmit_at[sid] = now
                self._nack_pending.pop(sid, None)
                self._drop_nack(sid)
        return out

    def make_coded(self, prefer_oldest: bool = True) -> CodedPacket | None:
        """
        Build coded packet over the elastic window.
        For WAN/HOL recovery we MUST mix the oldest unacked symbols
        (near the delivery frontier), not the newest ones.
        """
        if not self._window:
            return None
        items = list(self._window.items())
        degree = min(self.cfg.code_degree, len(items))
        if prefer_oldest:
            chosen = items[:degree]
        else:
            chosen = items[-degree:]
        first, last = chosen[0][0], chosen[-1][0]
        cid = self._next_coded_id
        self._next_coded_id += 1

        terms = [
            (gf256.vandermonde_coef(sid + 1, cid), data) for sid, data in chosen
        ]
        payload = gf256.linear_combine(terms, self.cfg.payload_size)

        self._total_sent_coded += 1
        return CodedPacket(cid, first, last, payload, send_ts_us=self.stamp())

    def apply_feedback(
        self,
        cumulative_ack: int,
        plr_byte: int = 0,
        missing_ids: list[int] | None = None,
        held_ids: list[int] | None = None,
    ) -> int:
        if cumulative_ack > self._cumulative_ack:
            self._cumulative_ack = cumulative_ack
        removed = 0
        while self._window and next(iter(self._window)) < self._cumulative_ack:
            sid, _ = self._window.popitem(last=False)
            self._forget_sid(sid)
            removed += 1

        # With FEC off: free SACKed symbols immediately so HOL holes don't pin
        # the entire elastic window (FASP-like — don't stall on already-delivered).
        if held_ids and self.cfg.redundancy_every <= 0:
            for sid in held_ids:
                if self._window.pop(sid, None) is not None:
                    self._forget_sid(sid)
                    removed += 1

        # Drop stale NACK entries (SACKed / cumacked / not in window)
        self._prune_nack_q()

        now = time.monotonic()
        if missing_ids:
            for sid in missing_ids:
                if sid in self._window:
                    # First sighting starts reorder timer; do not NACK yet
                    self._nack_pending.setdefault(sid, now)

        # Adaptive FEC around configured base. Loss → slightly denser coded over
        # the HOL frontier — NOT a SOURCE-retransmit flood, and never disables FEC.
        self.last_plr_byte = plr_byte
        plr = plr_byte * 100.0 / 256.0 if plr_byte > 0 else 0.0
        base_red = self.cfg.redundancy_every
        if base_red <= 0:
            self._redundancy_every = 0
            self._coded_burst = 1
        elif plr >= 25:
            # ~more FEC, still bounded (every 4 sources × burst 2 ≈ 50% coded)
            self._redundancy_every = max(4, base_red // 2)
            self._coded_burst = max(self.cfg.coded_burst, 2)
        elif plr >= 10:
            self._redundancy_every = max(6, base_red)
            self._coded_burst = max(self.cfg.coded_burst, 1)
        else:
            self._redundancy_every = base_red
            self._coded_burst = self.cfg.coded_burst
        return removed

    def _drop_nack(self, sid: int) -> None:
        self._nack_set.discard(sid)
        self._nack_pending.pop(sid, None)

    def _forget_sid(self, sid: int) -> None:
        self._drop_nack(sid)
        self._rexmit_at.pop(sid, None)
        self._first_sent_at.pop(sid, None)

    def _sent_age(self, sid: int, now: float) -> float:
        t0 = self._first_sent_at.get(sid)
        if t0 is None:
            return 1e9
        return now - t0

    def _note_rexmit(self, sid: int, now: float) -> None:
        if sid not in self._rexmit_at:
            self._unique_rexmit += 1
        self._rexmit_at[sid] = now
        self._total_rexmit += 1

    def _prune_nack_q(self) -> None:
        """Remove NACK ids we can no longer repair (not in window / already ACKed)."""
        if not self._nack_q and not self._nack_pending:
            return
        base = self._cumulative_ack
        kept: deque[int] = deque()
        self._nack_set.clear()
        for sid in self._nack_q:
            if sid < base or sid not in self._window:
                self._nack_pending.pop(sid, None)
                self._rexmit_at.pop(sid, None)
                self._first_sent_at.pop(sid, None)
                continue
            if sid not in self._nack_set:
                self._nack_set.add(sid)
                kept.append(sid)
        self._nack_q = kept
        for sid in list(self._nack_pending):
            if sid < base or sid not in self._window:
                self._nack_pending.pop(sid, None)

    def nack_ready_count(
        self,
        min_age: float = 0.0,
        far_age: float | None = None,
        frontier: int | None = None,
        cooldown: float = 0.0,
    ) -> int:
        """Count NACKs old enough to repair (first-send age + cooldown)."""
        now = time.monotonic()
        base = self._cumulative_ack
        far = min_age if far_age is None else far_age
        tip_frontier = 512 if frontier is None else frontier
        n = 0
        seen: set[int] = set()
        for sid in self._nack_q:
            if sid < base or sid not in self._window:
                continue
            dist = max(0, sid - base)
            if frontier is not None and dist >= frontier:
                continue
            need = min_age if dist < tip_frontier else far
            if self._sent_age(sid, now) < need:
                continue
            last = self._rexmit_at.get(sid, 0.0)
            if last and cooldown > 0 and (now - last) < cooldown:
                continue
            seen.add(sid)
            n += 1
        for sid in self._nack_pending:
            if sid in seen or sid not in self._window or sid < base:
                continue
            dist = max(0, sid - base)
            if frontier is not None and dist >= frontier:
                continue
            need = min_age if dist < tip_frontier else far
            if self._sent_age(sid, now) < need:
                continue
            last = self._rexmit_at.get(sid, 0.0)
            if last and cooldown > 0 and (now - last) < cooldown:
                continue
            n += 1
        return n

    def pop_nack_retransmit(
        self,
        limit: int = 8,
        min_age: float = 0.0,
        far_age: float | None = None,
        frontier: int = 512,
        cooldown: float = 0.0,
    ) -> list[bytearray]:
        """Pack SOURCE retransmits; gate on first-send age (OOO hold)."""
        out: list[bytearray] = []
        if limit <= 0:
            return out
        now = time.monotonic()
        base = self._cumulative_ack
        far = min_age if far_age is None else far_age

        ready: list[int] = []
        for sid in list(self._nack_pending):
            if sid < base or sid not in self._window:
                self._nack_pending.pop(sid, None)
                continue
            dist = max(0, sid - base)
            need = min_age if dist < frontier else far
            if self._sent_age(sid, now) < need:
                continue
            last = self._rexmit_at.get(sid, 0.0)
            if last and cooldown > 0 and (now - last) < cooldown:
                continue
            ready.append(sid)
        ready.sort()

        for sid in ready[: max(limit * 2, limit)]:
            self._nack_pending.pop(sid, None)
            if sid not in self._nack_set:
                self._nack_set.add(sid)
                self._nack_q.append(sid)

        queued = sorted(self._nack_q)
        self._nack_q.clear()
        self._nack_set.clear()
        leftover: list[int] = []
        for sid in queued:
            if sid < base or sid not in self._window:
                self._rexmit_at.pop(sid, None)
                continue
            dist = max(0, sid - base)
            need = min_age if dist < frontier else far
            if self._sent_age(sid, now) < need:
                leftover.append(sid)
                continue
            if len(out) >= limit:
                leftover.append(sid)
                continue
            last = self._rexmit_at.get(sid, 0.0)
            if last and cooldown > 0 and (now - last) < cooldown:
                leftover.append(sid)
                continue
            wire = self.pack_source_id(sid)
            if wire is not None:
                out.append(wire)
                self._note_rexmit(sid, now)
        for sid in leftover:
            if sid not in self._nack_set and sid in self._window and sid >= base:
                self._nack_set.add(sid)
                self._nack_q.append(sid)
        return out

    def retransmit_oldest(
        self,
        limit: int = 64,
        cooldown: float = 0.0,
        min_age: float = 0.0,
    ) -> list[bytearray]:
        """Retransmit oldest unacked SOURCE; requires min_age since first send."""
        out: list[bytearray] = []
        if limit <= 0 or not self._window:
            return out
        now = time.monotonic()
        for sid in list(self._window.keys()):
            if len(out) >= limit:
                break
            if min_age > 0 and self._sent_age(sid, now) < min_age:
                continue
            last = self._rexmit_at.get(sid, 0.0)
            if last and cooldown > 0 and (now - last) < cooldown:
                continue
            wire = self.pack_source_id(sid)
            if wire is not None:
                out.append(wire)
                self._note_rexmit(sid, now)
        return out

    def get_source(self, symbol_id: int) -> bytes | None:
        return self._window.get(symbol_id)

    def pack_source_id(self, symbol_id: int) -> bytearray | None:
        payload = self._window.get(symbol_id)
        if payload is None:
            return None
        wire = bytearray(SOURCE_HDR_SIZE + len(payload))
        _SOURCE_PREFIX.pack_into(wire, 0, MAGIC, VERSION, 0, 0, symbol_id, self.stamp())
        wire[SOURCE_HDR_SIZE:] = payload
        return wire

    def stats(self) -> dict:
        return {
            "window": len(self._window),
            "next_source_id": self._next_source_id,
            "sent_source": self._total_sent_source,
            "sent_coded": self._total_sent_coded,
            "redundancy_every": self._redundancy_every,
            "coded_burst": self._coded_burst,
            "cumulative_ack": self._cumulative_ack,
            "nack_q": len(self._nack_q) + len(self._nack_pending),
            "rexmit": self._total_rexmit,
            "rexmit_unique": self._unique_rexmit,
            "plr_byte": self.last_plr_byte,
        }
