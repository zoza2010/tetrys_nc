"""Binary packet formats for gen RaptorQ UDP file transfer."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

MAGIC = 0x54  # 'T'
VERSION = 1

PKT_META = 0x10
PKT_FIN = 0x11
PKT_READY = 0x12
PKT_GEN = 0x20  # RaptorQ generation symbol (opaque rq packet)
PKT_GEN_FB = 0x21  # generation feedback / NACK

# GEN: hdr4 + gen_id4 + esi4 + send_ts4 = 16
GEN_HDR_SIZE = 16
FLAG_META_XFER = 0x08  # META always carries gen params trailer
FLAG_FB_RX_COUNTS = 0x01  # each NACK carries uint16 unique symbols received
FLAG_FB_HOL_MISS = 0x02  # trailing uint8 + uint16 source ESIs for next_needed
FLAG_FB_BITMAP = 0x04  # uint16 len + miss bitmap anchored at next_needed_gen
# Max miss bitmap bytes (1088 bits; covers K=48 inflight window + slack).
FEEDBACK_BITMAP_MAX_BYTES = 136
XFER_GEN = 1

_HDR = struct.Struct("!BBBB")


class PacketType(IntEnum):
    META = PKT_META
    FIN = PKT_FIN
    READY = PKT_READY
    GEN = PKT_GEN
    GEN_FB = PKT_GEN_FB


@dataclass(slots=True)
class GenPacket:
    """One RaptorQ EncodingPacket for a generation (payload = rq serialize())."""

    gen_id: int
    esi: int  # informational; rq blob is self-describing
    payload: bytes  # serialized raptorq EncodingPacket
    send_ts_us: int = 0

    def pack(self) -> bytes:
        return (
            _HDR.pack(MAGIC, VERSION, PKT_GEN, 0)
            + struct.pack(
                "!III",
                self.gen_id & 0xFFFFFFFF,
                self.esi & 0xFFFFFFFF,
                self.send_ts_us & 0xFFFFFFFF,
            )
            + self.payload
        )

    @classmethod
    def unpack(cls, data: bytes) -> GenPacket:
        if len(data) < GEN_HDR_SIZE:
            raise ValueError("gen packet too short")
        gen_id, esi, ts = struct.unpack_from("!III", data, 4)
        return cls(gen_id, esi, data[GEN_HDR_SIZE:], ts)


@dataclass(slots=True)
class GenFeedbackPacket:
    """Client feedback for generation transfer: next needed + NACK gen ids."""

    next_needed_gen: int
    nack_gens: list[int]
    echo_ts_us: int = 0
    completed_gens: int = 0
    # Aligned with nack_gens. 0 means no symbols received / no decoder yet.
    nack_rx_counts: list[int] | None = None
    # Missing systematic source ESIs for next_needed_gen (sparse retransmit).
    hol_miss_esi: list[int] | None = None
    # Bit i set => gen (next_needed_gen + i) needs repair (gap/partial).
    miss_bitmap: bytes | None = None

    def pack(self) -> bytes:
        nacks = self.nack_gens[:64]
        counts = (self.nack_rx_counts or [])[: len(nacks)]
        with_counts = len(counts) == len(nacks)
        hol = self.hol_miss_esi or []
        body = struct.pack(
            "!IIH",
            self.next_needed_gen & 0xFFFFFFFF,
            self.completed_gens & 0xFFFFFFFF,
            len(nacks),
        )
        if with_counts:
            for g, rx in zip(nacks, counts):
                body += struct.pack("!IH", g & 0xFFFFFFFF, min(0xFFFF, max(0, rx)))
        else:
            for g in nacks:
                body += struct.pack("!I", g & 0xFFFFFFFF)
        if hol:
            body += struct.pack("!B", min(32, len(hol)))
            for esi in hol[:32]:
                body += struct.pack("!H", esi & 0xFFFF)
        bitmap = self.miss_bitmap or b""
        if bitmap:
            body += struct.pack("!H", len(bitmap))
            body += bitmap
        body += struct.pack("!I", self.echo_ts_us & 0xFFFFFFFF)
        flags = (FLAG_FB_RX_COUNTS if with_counts else 0) | (
            FLAG_FB_HOL_MISS if hol else 0
        ) | (FLAG_FB_BITMAP if bitmap else 0)
        return _HDR.pack(MAGIC, VERSION, PKT_GEN_FB, flags) + body

    @classmethod
    def unpack(cls, data: bytes) -> GenFeedbackPacket:
        if len(data) < 4 + 4 + 4 + 2:
            raise ValueError("gen feedback too short")
        next_needed, completed, n = struct.unpack_from("!IIH", data, 4)
        off = 4 + 10
        nacks: list[int] = []
        counts: list[int] = []
        with_counts = bool(data[3] & FLAG_FB_RX_COUNTS)
        with_hol = bool(data[3] & FLAG_FB_HOL_MISS)
        with_bitmap = bool(data[3] & FLAG_FB_BITMAP)
        item_size = 6 if with_counts else 4
        if len(data) < off + n * item_size:
            raise ValueError("gen feedback NACK list truncated")
        for _ in range(n):
            if with_counts:
                gen_id, rx = struct.unpack_from("!IH", data, off)
                nacks.append(gen_id)
                counts.append(rx)
            else:
                nacks.append(struct.unpack_from("!I", data, off)[0])
            off += item_size
        hol: list[int] = []
        if with_hol:
            if len(data) < off + 1:
                raise ValueError("gen feedback HOL miss truncated")
            hol_n = data[off]
            off += 1
            if len(data) < off + hol_n * 2:
                raise ValueError("gen feedback HOL miss list truncated")
            for _ in range(hol_n):
                hol.append(struct.unpack_from("!H", data, off)[0])
                off += 2
        bitmap = b""
        if with_bitmap:
            if len(data) < off + 2:
                raise ValueError("gen feedback bitmap truncated")
            blen = struct.unpack_from("!H", data, off)[0]
            off += 2
            if len(data) < off + blen:
                raise ValueError("gen feedback bitmap payload truncated")
            bitmap = bytes(data[off : off + blen])
            off += blen
        echo = struct.unpack_from("!I", data, off)[0] if len(data) >= off + 4 else 0
        return cls(
            next_needed,
            nacks,
            echo,
            completed,
            counts if with_counts else None,
            hol if hol else None,
            bitmap if bitmap else None,
        )


def miss_bitmap_to_nacks(next_needed: int, bitmap: bytes) -> list[int]:
    """Expand a miss bitmap anchored at next_needed into gen ids."""
    out: list[int] = []
    for off in range(len(bitmap) * 8):
        if bitmap[off >> 3] & (1 << (off & 7)):
            out.append(next_needed + off)
    return out


def merge_feedback_nacks(
    pkt: GenFeedbackPacket,
) -> tuple[list[int], dict[int, int]]:
    """Merge explicit NACK list with optional miss bitmap."""
    nacks = list(pkt.nack_gens)
    counts = pkt.nack_rx_counts or []
    nack_rx = dict(zip(pkt.nack_gens, counts))
    if pkt.miss_bitmap:
        for gid in miss_bitmap_to_nacks(pkt.next_needed_gen, pkt.miss_bitmap):
            if gid not in nack_rx:
                nacks.append(gid)
                nack_rx[gid] = 0
    return nacks, nack_rx


@dataclass(slots=True)
class MetaPacket:
    file_size: int
    file_name: str
    payload_size: int
    sha256_hex: str
    xfer: int = XFER_GEN
    gen_symbol_size: int = 0
    gen_k: int = 0
    gen_overhead_pct: int = 0

    def pack(self) -> bytes:
        name = self.file_name.encode("utf-8")[:255]
        digest = self.sha256_hex.encode("ascii")[:64]
        body = (
            _HDR.pack(MAGIC, VERSION, PKT_META, FLAG_META_XFER)
            + struct.pack("!QIH B", self.file_size, self.payload_size, len(name), len(digest))
            + name
            + digest
            + struct.pack(
                "!BHHB",
                XFER_GEN & 0xFF,
                self.gen_symbol_size & 0xFFFF,
                self.gen_k & 0xFFFF,
                self.gen_overhead_pct & 0xFF,
            )
        )
        return body

    @classmethod
    def unpack(cls, data: bytes) -> MetaPacket:
        flags = data[3]
        file_size, payload_size, name_len, dig_len = struct.unpack_from("!QIH B", data, 4)
        off = 4 + 8 + 4 + 2 + 1
        name = data[off : off + name_len].decode("utf-8")
        off += name_len
        digest = data[off : off + dig_len].decode("ascii")
        off += dig_len
        gen_symbol_size = 0
        gen_k = 0
        gen_overhead_pct = 0
        if flags & FLAG_META_XFER and len(data) >= off + 6:
            _xfer, gen_symbol_size, gen_k, gen_overhead_pct = struct.unpack_from(
                "!BHHB", data, off
            )
        return cls(
            file_size,
            name,
            payload_size,
            digest,
            XFER_GEN,
            gen_symbol_size,
            gen_k,
            gen_overhead_pct,
        )


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
    if ptype == PKT_META:
        return MetaPacket.unpack(data)
    if ptype == PKT_FIN:
        return FinPacket.unpack(data)
    if ptype == PKT_READY:
        return ReadyPacket.unpack(data)
    if ptype == PKT_GEN:
        return GenPacket.unpack(data)
    if ptype == PKT_GEN_FB:
        return GenFeedbackPacket.unpack(data)
    raise ValueError(f"unknown packet type {ptype}")
