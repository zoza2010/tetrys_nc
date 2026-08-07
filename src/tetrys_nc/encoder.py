"""Tetrys encoder with elastic encoding window (RFC 9407 §4)."""

from __future__ import annotations

import struct
from collections import OrderedDict, deque
from dataclasses import dataclass

from . import gf256
from .packets import MAGIC, VERSION, CodedPacket


_SOURCE_PREFIX = struct.Struct("!BBBBI")


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
        self._wire = bytearray(8 + self.cfg.payload_size)
        # NACK queue from receiver SACK holes
        self._nack_q: deque[int] = deque()
        self._nack_set: set[int] = set()
        self.last_plr_byte = 0

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

    def add_source(self, payload: bytes) -> memoryview:
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
        self._sources_since_coded += 1
        self._total_sent_source += 1

        wire = self._wire
        if len(wire) != 8 + ps:
            wire = self._wire = bytearray(8 + ps)
        _SOURCE_PREFIX.pack_into(wire, 0, MAGIC, VERSION, 0, 0, sid)
        wire[8:] = payload
        return memoryview(wire)

    def maybe_coded(self) -> list[bytes]:
        """Return 0..N packed coded packets according to redundancy schedule."""
        if self._redundancy_every <= 0 or not self._window:
            return []
        if self._sources_since_coded < self._redundancy_every:
            return []
        self._sources_since_coded = 0
        out: list[bytes] = []
        for _ in range(self._coded_burst):
            pkt = self.make_coded(prefer_oldest=True)
            if pkt is None:
                break
            out.append(pkt.pack())
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
        # Ensure contiguous span for first/last encoding vector
        # Window is always contiguous by construction.
        cid = self._next_coded_id
        self._next_coded_id += 1

        out = bytearray(self.cfg.payload_size)
        for sid, data in chosen:
            coef = gf256.vandermonde_coef(sid + 1, cid)
            gf256.mul_bytes(coef, data, out)

        self._total_sent_coded += 1
        return CodedPacket(cid, first, last, bytes(out))

    def apply_feedback(
        self,
        cumulative_ack: int,
        plr_byte: int = 0,
        missing_ids: list[int] | None = None,
    ) -> int:
        if cumulative_ack > self._cumulative_ack:
            self._cumulative_ack = cumulative_ack
        removed = 0
        while self._window and next(iter(self._window)) < self._cumulative_ack:
            sid, _ = self._window.popitem(last=False)
            self._nack_set.discard(sid)
            removed += 1

        if missing_ids:
            for sid in missing_ids:
                if sid in self._window and sid not in self._nack_set:
                    self._nack_set.add(sid)
                    self._nack_q.append(sid)

        self.last_plr_byte = plr_byte
        if plr_byte > 0:
            plr = plr_byte * 100.0 / 256.0
            # High loss → more coded packets per source (burst), every source
            if plr >= 40:
                self._redundancy_every = 1
                self._coded_burst = max(self.cfg.coded_burst, 4)
            elif plr >= 25:
                self._redundancy_every = 1
                self._coded_burst = max(self.cfg.coded_burst, 3)
            elif plr >= 12:
                self._redundancy_every = 1
                self._coded_burst = max(self.cfg.coded_burst, 2)
            elif plr >= 5:
                self._redundancy_every = 1
                self._coded_burst = max(self.cfg.coded_burst, 1)
            elif self.cfg.redundancy_every > 0:
                self._redundancy_every = self.cfg.redundancy_every
                self._coded_burst = self.cfg.coded_burst
        return removed

    def pop_nack_retransmit(self, limit: int = 8) -> list[bytes]:
        """Pack SOURCE packets for NACKed ids still in the window."""
        out: list[bytes] = []
        while self._nack_q and len(out) < limit:
            sid = self._nack_q.popleft()
            self._nack_set.discard(sid)
            wire = self.pack_source_id(sid)
            if wire is not None:
                out.append(wire)
        return out

    def get_source(self, symbol_id: int) -> bytes | None:
        return self._window.get(symbol_id)

    def pack_source_id(self, symbol_id: int) -> bytes | None:
        payload = self._window.get(symbol_id)
        if payload is None:
            return None
        return _SOURCE_PREFIX.pack(MAGIC, VERSION, 0, 0, symbol_id) + payload

    def stats(self) -> dict:
        return {
            "window": len(self._window),
            "next_source_id": self._next_source_id,
            "sent_source": self._total_sent_source,
            "sent_coded": self._total_sent_coded,
            "redundancy_every": self._redundancy_every,
            "coded_burst": self._coded_burst,
            "cumulative_ack": self._cumulative_ack,
            "nack_q": len(self._nack_q),
            "plr_byte": self.last_plr_byte,
        }
