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
    # Cap on (next_id - cumack) while FEC keeps SACKed payloads for coding.
    max_coding_span: int = 0  # 0 → 4×max_window
    # 0 = disable periodic coding; else coded every N sources
    redundancy_every: int = 32
    # How many coded packets to emit each redundancy tick
    coded_burst: int = 1
    # How many oldest unacked symbols to mix into each coded packet
    code_degree: int = 8
    payload_size: int = 32768


class TetrysEncoder:
    """
    Dual accounting for FASP-like throughput + Tetrys FEC:

    - ``_payloads``: all symbols with id ≥ cumack (coding + retransmit memory)
    - ``_unsacked``: subset not yet SACKed by receiver (admission / flight)

    SACK frees admission slots without deleting coding payloads (when FEC on),
    so coded packets still see a contiguous HOL span with known+missing symbols.
    """

    def __init__(self, config: EncoderConfig | None = None) -> None:
        self.cfg = config or EncoderConfig()
        if self.cfg.max_coding_span <= 0:
            self.cfg.max_coding_span = max(self.cfg.max_window * 4, 32768)
        self._payloads: OrderedDict[int, bytes] = OrderedDict()
        self._unsacked: set[int] = set()
        self._next_source_id = 0
        self._next_coded_id = 1
        self._sources_since_coded = 0
        self._total_sent_source = 0
        self._total_sent_coded = 0
        self._redundancy_every = self.cfg.redundancy_every
        self._coded_burst = max(1, self.cfg.coded_burst)
        self._cumulative_ack = 0
        self._nack_q: deque[int] = deque()
        self._nack_set: set[int] = set()
        self.last_plr_byte = 0
        self._send_ts_us = 0

    def stamp(self) -> int:
        self._send_ts_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
        return self._send_ts_us

    @property
    def send_ts_us(self) -> int:
        return self._send_ts_us

    @property
    def window_size(self) -> int:
        """In-flight / admission occupancy (unsacked only)."""
        return len(self._unsacked)

    @property
    def coding_size(self) -> int:
        return len(self._payloads)

    @property
    def next_source_id(self) -> int:
        return self._next_source_id

    @property
    def oldest_id(self) -> int | None:
        if not self._payloads:
            return None
        return next(iter(self._payloads))

    @property
    def coded_burst(self) -> int:
        return self._coded_burst

    @property
    def fec_enabled(self) -> bool:
        return self.cfg.redundancy_every > 0

    def _coding_span(self) -> int:
        if not self._payloads:
            return 0
        return self._next_source_id - self._cumulative_ack

    def can_accept(self) -> bool:
        if len(self._unsacked) >= self.cfg.max_window:
            return False
        if self._coding_span() >= self.cfg.max_coding_span:
            return False
        return True

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
        self._payloads[sid] = payload
        self._unsacked.add(sid)
        self._sources_since_coded += 1
        self._total_sent_source += 1

        ts = self.stamp()
        wire = bytearray(SOURCE_HDR_SIZE + ps)
        _SOURCE_PREFIX.pack_into(wire, 0, MAGIC, VERSION, 0, 0, sid, ts)
        wire[SOURCE_HDR_SIZE:] = payload
        return wire

    def maybe_coded(self) -> list[bytes]:
        if self._redundancy_every <= 0 or not self._payloads:
            return []
        if self._sources_since_coded < self._redundancy_every:
            return []
        self._sources_since_coded = 0
        return self.emit_coded(self._coded_burst)

    def emit_coded(self, n: int = 1) -> list[bytes]:
        """Force N coded packets over the oldest coding span (HOL)."""
        out: list[bytes] = []
        if n <= 0 or not self._payloads:
            return out
        for _ in range(n):
            pkt = self.make_coded(prefer_oldest=True)
            if pkt is None:
                break
            out.append(pkt.pack())
        return out

    def make_coded(self, prefer_oldest: bool = True) -> CodedPacket | None:
        """
        Contiguous mix over ``_payloads`` (includes SACKed-but-not-cumack'd).
        Decoder subtracts symbols it already holds → equations on the holes.
        Skip degree-1 when that symbol is still unsacked (SOURCE retransmit is enough).
        """
        if not self._payloads:
            return None
        degree = min(self.cfg.code_degree, len(self._payloads))
        if prefer_oldest:
            start = next(iter(self._payloads))
            chosen: list[tuple[int, bytes]] = []
            for sid in range(start, start + degree):
                data = self._payloads.get(sid)
                if data is None:
                    break
                chosen.append((sid, data))
        else:
            items = list(self._payloads.items())[-degree:]
            chosen = items
        if not chosen:
            return None
        # Degree-1 over an unsacked symbol ≡ redundant SOURCE — don't waste rate.
        if len(chosen) == 1 and chosen[0][0] in self._unsacked:
            return None

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
        while self._payloads and next(iter(self._payloads)) < self._cumulative_ack:
            sid, _ = self._payloads.popitem(last=False)
            self._unsacked.discard(sid)
            self._nack_set.discard(sid)
            removed += 1

        # SACK: free admission slot; keep payload for FEC until cumack (if FEC on).
        if held_ids:
            for sid in held_ids:
                if sid in self._unsacked:
                    self._unsacked.discard(sid)
                    self._nack_set.discard(sid)
                    removed += 1
                if not self.fec_enabled and sid in self._payloads:
                    del self._payloads[sid]

        if missing_ids:
            for sid in missing_ids:
                if sid in self._payloads and sid not in self._nack_set:
                    self._nack_set.add(sid)
                    self._nack_q.append(sid)

        self.last_plr_byte = plr_byte
        plr = plr_byte * 100.0 / 256.0 if plr_byte > 0 else 0.0
        base_red = self.cfg.redundancy_every
        if base_red <= 0:
            self._redundancy_every = 0
            self._coded_burst = 1
        elif plr >= 25:
            self._redundancy_every = max(4, base_red // 2)
            self._coded_burst = max(self.cfg.coded_burst, 2)
        elif plr >= 10:
            self._redundancy_every = max(6, min(base_red, 8))
            self._coded_burst = max(self.cfg.coded_burst, 1)
        else:
            self._redundancy_every = base_red
            self._coded_burst = self.cfg.coded_burst
        return removed

    def pop_nack_retransmit(self, limit: int = 8) -> list[bytearray]:
        out: list[bytearray] = []
        while self._nack_q and len(out) < limit:
            sid = self._nack_q.popleft()
            self._nack_set.discard(sid)
            wire = self.pack_source_id(sid)
            if wire is not None:
                out.append(wire)
        return out

    def retransmit_oldest(self, limit: int = 64) -> list[bytearray]:
        """Retransmit oldest *unsacked* symbols (HOL holes + unconfirmed)."""
        out: list[bytearray] = []
        if limit <= 0 or not self._unsacked:
            return out
        for sid in self._payloads:
            if sid not in self._unsacked:
                continue
            wire = self.pack_source_id(sid)
            if wire is not None:
                out.append(wire)
                if len(out) >= limit:
                    break
        return out

    def get_source(self, symbol_id: int) -> bytes | None:
        return self._payloads.get(symbol_id)

    def pack_source_id(self, symbol_id: int) -> bytearray | None:
        payload = self._payloads.get(symbol_id)
        if payload is None:
            return None
        wire = bytearray(SOURCE_HDR_SIZE + len(payload))
        _SOURCE_PREFIX.pack_into(wire, 0, MAGIC, VERSION, 0, 0, symbol_id, self.stamp())
        wire[SOURCE_HDR_SIZE:] = payload
        return wire

    # Compat alias used by older tests / call sites
    @property
    def _window(self) -> OrderedDict[int, bytes]:
        return self._payloads

    def stats(self) -> dict:
        return {
            "window": len(self._unsacked),
            "coding": len(self._payloads),
            "next_source_id": self._next_source_id,
            "sent_source": self._total_sent_source,
            "sent_coded": self._total_sent_coded,
            "redundancy_every": self._redundancy_every,
            "coded_burst": self._coded_burst,
            "cumulative_ack": self._cumulative_ack,
            "nack_q": len(self._nack_q),
            "plr_byte": self.last_plr_byte,
        }
