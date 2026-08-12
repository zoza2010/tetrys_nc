"""Binary packet formats for Tetrys-over-UDP file transfer."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

try:
    import numpy as _np
except ImportError:  # pragma: no cover - numpy is the normal path
    _np: Any = None  # type: ignore[no-redef]

MAGIC = 0x54  # 'T'
VERSION = 1

PKT_SOURCE = 0x00
PKT_CODED = 0x01
PKT_WND_UPT = 0x02
PKT_META = 0x10
PKT_FIN = 0x11
PKT_READY = 0x12

# SOURCE: magic,ver,type,flags, sid, send_ts_us, payload
# CODED:  magic,ver,type,flags, cid, first, last, send_ts_us, payload
SOURCE_HDR_SIZE = 12
CODED_HDR_SIZE = 20


class PacketType(IntEnum):
    SOURCE = PKT_SOURCE
    CODED = PKT_CODED
    WND_UPT = PKT_WND_UPT
    META = PKT_META
    FIN = PKT_FIN
    READY = PKT_READY


_HDR = struct.Struct("!BBBB")
_SOURCE_PREFIX = struct.Struct("!BBBBII")
_CODED_PREFIX = struct.Struct("!BBBBIIII")


def pack_source(symbol_id: int, payload: bytes | bytearray, send_ts_us: int = 0) -> bytes:
    return (
        _SOURCE_PREFIX.pack(MAGIC, VERSION, PKT_SOURCE, 0, symbol_id, send_ts_us & 0xFFFFFFFF)
        + payload
    )


@dataclass(slots=True)
class SourcePacket:
    symbol_id: int
    payload: bytes
    send_ts_us: int = 0

    def pack(self) -> bytes:
        return pack_source(self.symbol_id, self.payload, self.send_ts_us)

    @classmethod
    def unpack(cls, data: bytes) -> SourcePacket:
        # New format: 12-byte header. Legacy 8-byte header still accepted.
        if len(data) >= 12:
            sid, ts = struct.unpack_from("!II", data, 4)
            # Heuristic: legacy packets have payload starting at 8; if the
            # "timestamp" looks like raw payload (high entropy) we still treat
            # bytes 8:12 as ts — wire is ours and always stamped now.
            return cls(sid, data[12:], ts)
        sid = struct.unpack_from("!I", data, 4)[0]
        return cls(sid, data[8:], 0)


@dataclass(slots=True)
class CodedPacket:
    coded_id: int
    first_source_id: int
    last_source_id: int
    payload: bytes
    send_ts_us: int = 0

    def pack(self) -> bytes:
        return (
            _CODED_PREFIX.pack(
                MAGIC,
                VERSION,
                PKT_CODED,
                0,
                self.coded_id,
                self.first_source_id,
                self.last_source_id,
                self.send_ts_us & 0xFFFFFFFF,
            )
            + self.payload
        )

    @classmethod
    def unpack(cls, data: bytes) -> CodedPacket:
        if len(data) >= 20:
            cid, first, last, ts = struct.unpack_from("!IIII", data, 4)
            return cls(cid, first, last, data[20:], ts)
        # Legacy 16-byte coded header
        cid, first, last = struct.unpack_from("!III", data, 4)
        return cls(cid, first, last, data[16:], 0)


@dataclass(slots=True)
class WindowUpdatePacket:
    """
    Feedback from decoder.
    cumulative_ack: symbols with id < cumulative_ack are fully delivered.
    sack: bit i set => symbol (cumulative_ack + i) is already held by receiver.
    sack_span: number of valid SACK bits (padding beyond span must be ignored).
    Missing bits in the SACK range are NACK targets for repair/retransmit.
    echo_ts_us: last sender timestamp echoed (for RTT / delay-based CC).
    """

    cumulative_ack: int
    nb_missing_src: int
    nb_not_used_coded: int
    plr_byte: int
    sack: bytes = b""
    echo_ts_us: int = 0
    sack_span: int = 0

    # flags bit0: trailer uint16 sack_span after sack bytes
    # flags bit1: extended word count (uint32 after 22-byte hdr; byte field=0)
    FLAG_SACK_SPAN = 0x01
    FLAG_SACK_EXT = 0x02

    def pack(self) -> bytes:
        sack = self.sack
        pad = (-len(sack)) % 4
        if pad:
            sack = sack + b"\x00" * pad
        sack_words = len(sack) // 4
        span = self.sack_span if self.sack_span > 0 else min(len(self.sack) * 8, sack_words * 32)
        flags = self.FLAG_SACK_SPAN
        if sack_words <= 255:
            return _HDR.pack(MAGIC, VERSION, PKT_WND_UPT, flags) + struct.pack(
                "!III BB I",
                self.cumulative_ack,
                self.nb_missing_src,
                self.nb_not_used_coded,
                self.plr_byte & 0xFF,
                sack_words & 0xFF,
                self.echo_ts_us & 0xFFFFFFFF,
            ) + sack + struct.pack("!H", min(span, 65535) & 0xFFFF)

        flags |= self.FLAG_SACK_EXT
        return _HDR.pack(MAGIC, VERSION, PKT_WND_UPT, flags) + struct.pack(
            "!III BB I",
            self.cumulative_ack,
            self.nb_missing_src,
            self.nb_not_used_coded,
            self.plr_byte & 0xFF,
            0,
            self.echo_ts_us & 0xFFFFFFFF,
        ) + struct.pack("!I", sack_words & 0xFFFFFFFF) + sack + struct.pack(
            "!H", min(span, 65535) & 0xFFFF
        )

    @classmethod
    def unpack(cls, data: bytes) -> WindowUpdatePacket:
        flags = data[3] if len(data) > 3 else 0
        # header4 + III BB I = 22, then optional ext words, sack, optional span
        if len(data) >= 22:
            cum, nb_miss, nb_coded, plr, sack_words, echo = struct.unpack_from(
                "!III BB I", data, 4
            )
            off = 22
            if flags & cls.FLAG_SACK_EXT:
                if len(data) < off + 4:
                    return cls(cum, nb_miss, nb_coded, plr, b"", echo, 0)
                sack_words = struct.unpack_from("!I", data, off)[0]
                off += 4
            sack_end = off + sack_words * 4
            sack = data[off:sack_end]
            span = 0
            if flags & cls.FLAG_SACK_SPAN and len(data) >= sack_end + 2:
                span = struct.unpack_from("!H", data, sack_end)[0]
            elif sack:
                span = len(sack) * 8
            return cls(cum, nb_miss, nb_coded, plr, sack, echo, span)
        if len(data) >= 18:
            cum, nb_miss, nb_coded, plr, sack_words = struct.unpack_from(
                "!III BB", data, 4
            )
            sack = data[18 : 18 + sack_words * 4]
            return cls(cum, nb_miss, nb_coded, plr, sack, 0, len(sack) * 8)
        cum, nb_miss, nb_coded, plr = struct.unpack_from("!III B", data, 4)
        return cls(cum, nb_miss, nb_coded, plr, b"", 0, 0)

    def _span(self) -> int:
        if self.sack_span > 0:
            return self.sack_span
        return len(self.sack) * 8

    def missing_ids(self, limit: int = 64) -> list[int]:
        """Symbol IDs in SACK range that receiver does NOT have."""
        span = self._span()
        # Runs in the sender's feedback thread and holds the GIL: a bit loop
        # over a 17k-bit span costs ~1.7ms and starves the send loop.
        if _np is not None and span > 512:
            bits = _np.unpackbits(
                _np.frombuffer(self.sack, dtype=_np.uint8),
                count=span,
                bitorder="little",
            )
            gaps = _np.flatnonzero(bits == 0)[:limit]
            return (gaps + self.cumulative_ack).tolist()
        out: list[int] = []
        for i, byte in enumerate(self.sack):
            for b in range(8):
                bit = i * 8 + b
                if bit >= span:
                    return out
                if not (byte & (1 << b)):
                    out.append(self.cumulative_ack + bit)
                    if len(out) >= limit:
                        return out
        return out

    def held_ids(self, limit: int = 100_000) -> list[int]:
        """Symbol IDs in SACK range that receiver already holds (bit set)."""
        out: list[int] = []
        span = self._span()
        for i, byte in enumerate(self.sack):
            for b in range(8):
                bit = i * 8 + b
                if bit >= span:
                    return out
                if byte & (1 << b):
                    out.append(self.cumulative_ack + bit)
                    if len(out) >= limit:
                        return out
        return out


@dataclass(slots=True)
class MetaPacket:
    file_size: int
    file_name: str
    payload_size: int
    sha256_hex: str

    def pack(self) -> bytes:
        name = self.file_name.encode("utf-8")[:255]
        digest = self.sha256_hex.encode("ascii")[:64]
        return (
            _HDR.pack(MAGIC, VERSION, PKT_META, 0)
            + struct.pack("!QIH B", self.file_size, self.payload_size, len(name), len(digest))
            + name
            + digest
        )

    @classmethod
    def unpack(cls, data: bytes) -> MetaPacket:
        file_size, payload_size, name_len, dig_len = struct.unpack_from("!QIH B", data, 4)
        off = 4 + 8 + 4 + 2 + 1
        name = data[off : off + name_len].decode("utf-8")
        off += name_len
        digest = data[off : off + dig_len].decode("ascii")
        return cls(file_size, name, payload_size, digest)


@dataclass(slots=True)
class FinPacket:
    ok: bool
    total_symbols: int

    def pack(self) -> bytes:
        return _HDR.pack(MAGIC, VERSION, PKT_FIN, 1 if self.ok else 0) + struct.pack(
            "!I", self.total_symbols
        )

    @classmethod
    def unpack(cls, data: bytes) -> FinPacket:
        flags = data[3]
        total = struct.unpack_from("!I", data, 4)[0]
        return cls(bool(flags & 1), total)


@dataclass(slots=True)
class ReadyPacket:
    max_window: int

    def pack(self) -> bytes:
        return _HDR.pack(MAGIC, VERSION, PKT_READY, 0) + struct.pack("!I", self.max_window)

    @classmethod
    def unpack(cls, data: bytes) -> ReadyPacket:
        return cls(struct.unpack_from("!I", data, 4)[0])


def parse_packet(data: bytes):
    if len(data) < 4 or data[0] != MAGIC:
        raise ValueError("invalid packet")
    ptype = data[2]
    if ptype == PKT_SOURCE:
        return SourcePacket.unpack(data)
    if ptype == PKT_CODED:
        return CodedPacket.unpack(data)
    if ptype == PKT_WND_UPT:
        return WindowUpdatePacket.unpack(data)
    if ptype == PKT_META:
        return MetaPacket.unpack(data)
    if ptype == PKT_FIN:
        return FinPacket.unpack(data)
    if ptype == PKT_READY:
        return ReadyPacket.unpack(data)
    raise ValueError(f"unknown packet type {ptype}")
