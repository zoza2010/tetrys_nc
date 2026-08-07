"""Binary packet formats for Tetrys-over-UDP file transfer."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

# Magic / version for our session framing
MAGIC = 0x54  # 'T'
VERSION = 1

PKT_SOURCE = 0x00
PKT_CODED = 0x01
PKT_WND_UPT = 0x02
PKT_META = 0x10
PKT_FIN = 0x11
PKT_READY = 0x12


class PacketType(IntEnum):
    SOURCE = PKT_SOURCE
    CODED = PKT_CODED
    WND_UPT = PKT_WND_UPT
    META = PKT_META
    FIN = PKT_FIN
    READY = PKT_READY


_HDR = struct.Struct("!BBBB")
_SOURCE_PREFIX = struct.Struct("!BBBBI")
_CODED_PREFIX = struct.Struct("!BBBBIII")


def pack_source(symbol_id: int, payload: bytes | bytearray) -> bytes:
    return _SOURCE_PREFIX.pack(MAGIC, VERSION, PKT_SOURCE, 0, symbol_id) + payload


@dataclass(slots=True)
class SourcePacket:
    symbol_id: int
    payload: bytes

    def pack(self) -> bytes:
        return pack_source(self.symbol_id, self.payload)

    @classmethod
    def unpack(cls, data: bytes) -> SourcePacket:
        sid = struct.unpack_from("!I", data, 4)[0]
        return cls(sid, data[8:])


@dataclass(slots=True)
class CodedPacket:
    """Coded symbol over a contiguous elastic window [first_source_id, last_source_id]."""

    coded_id: int
    first_source_id: int
    last_source_id: int
    payload: bytes

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
            )
            + self.payload
        )

    @classmethod
    def unpack(cls, data: bytes) -> CodedPacket:
        cid, first, last = struct.unpack_from("!III", data, 4)
        return cls(cid, first, last, data[16:])


@dataclass(slots=True)
class WindowUpdatePacket:
    """
    Feedback from decoder.
    cumulative_ack: all source symbols with id < cumulative_ack are safe to remove.
    Also carries PLR / buffer stats for adaptive redundancy.
    """

    cumulative_ack: int
    nb_missing_src: int
    nb_not_used_coded: int
    plr_byte: int

    def pack(self) -> bytes:
        return _HDR.pack(MAGIC, VERSION, PKT_WND_UPT, 0) + struct.pack(
            "!III B",
            self.cumulative_ack,
            self.nb_missing_src,
            self.nb_not_used_coded,
            self.plr_byte & 0xFF,
        )

    @classmethod
    def unpack(cls, data: bytes) -> WindowUpdatePacket:
        cum, nb_miss, nb_coded, plr = struct.unpack_from("!III B", data, 4)
        return cls(cum, nb_miss, nb_coded, plr)


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
