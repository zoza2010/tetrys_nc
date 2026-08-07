"""Tetrys encoder with elastic encoding window (RFC 9407 §4)."""

from __future__ import annotations

import struct
from collections import OrderedDict
from dataclasses import dataclass

from . import gf256
from .packets import MAGIC, VERSION, CodedPacket


_SOURCE_PREFIX = struct.Struct("!BBBBI")


@dataclass(slots=True)
class EncoderConfig:
    max_window: int = 8192
    # 0 = disable repair coding; else coded every N sources
    redundancy_every: int = 32
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
        self._cumulative_ack = 0
        # Reusable wire buffer: header(8) + payload
        self._wire = bytearray(8 + self.cfg.payload_size)

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

    def can_accept(self) -> bool:
        return len(self._window) < self.cfg.max_window

    def add_source(self, payload: bytes) -> memoryview:
        """Add source symbol; returns wire view valid until the next add_source/pack."""
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
    def maybe_coded(self) -> bytes | None:
        if self._redundancy_every <= 0 or not self._window:
            return None
        if self._sources_since_coded < self._redundancy_every:
            return None
        self._sources_since_coded = 0
        pkt = self.make_coded()
        return None if pkt is None else pkt.pack()

    def make_coded(self) -> CodedPacket | None:
        if not self._window:
            return None
        items = list(self._window.items())[-self.cfg.code_degree :]
        first, last = items[0][0], items[-1][0]
        cid = self._next_coded_id
        self._next_coded_id += 1

        out = bytearray(self.cfg.payload_size)
        for sid, data in items:
            coef = gf256.vandermonde_coef(sid + 1, cid)
            gf256.mul_bytes(coef, data, out)

        self._total_sent_coded += 1
        return CodedPacket(cid, first, last, bytes(out))

    def apply_feedback(self, cumulative_ack: int, plr_byte: int = 0) -> int:
        if cumulative_ack > self._cumulative_ack:
            self._cumulative_ack = cumulative_ack
        removed = 0
        while self._window and next(iter(self._window)) < self._cumulative_ack:
            self._window.popitem(last=False)
            removed += 1

        # Enable/adapt coding only when losses are reported
        if plr_byte > 0:
            plr = plr_byte * 100.0 / 256.0
            if plr >= 10:
                self._redundancy_every = 2
            elif plr >= 5:
                self._redundancy_every = 4
            elif plr >= 2:
                self._redundancy_every = 8
            elif self.cfg.redundancy_every > 0:
                self._redundancy_every = self.cfg.redundancy_every
        return removed

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
            "cumulative_ack": self._cumulative_ack,
        }
