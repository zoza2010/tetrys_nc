"""Block transfer wire protocol."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

MAGIC = 0x54
VERSION = 2
MAX_DATAGRAM = 1400
MAX_DONE_RANGES = 48
MAX_OPEN_BLOCKS = 64
MAX_RANGE_SPAN = 1_000_000

_HDR = struct.Struct("!BBBBI")
_DATA = struct.Struct("!III")
_FB_BASE = struct.Struct("!IQQIHH")
_RANGE = struct.Struct("!II")
_OPEN = struct.Struct("!IHBB")


class BlockPacketType(IntEnum):
    META = 0x30
    READY = 0x31
    DATA = 0x32
    FEEDBACK = 0x33
    FIN = 0x34
    OBJ_OPEN = 0x35
    OBJ_FIN = 0x36


@dataclass(slots=True, frozen=True)
class OpenBlock:
    block_id: int
    unique_esi: int
    decode_failed: bool = False
    age_bucket: int = 0


@dataclass(slots=True)
class BlockReady:
    session_id: int
    active_bytes: int
    rel_path: str = ""

    def pack(self) -> bytes:
        name = self.rel_path.encode("utf-8")[:1200]
        return (
            _HDR.pack(
                MAGIC, VERSION, BlockPacketType.READY, 0, self.session_id & 0xFFFFFFFF
            )
            + struct.pack("!IH", self.active_bytes & 0xFFFFFFFF, len(name))
            + name
        )

    @classmethod
    def unpack(cls, data: bytes) -> BlockReady:
        _require(data, 12, BlockPacketType.READY)
        session = struct.unpack_from("!I", data, 4)[0]
        active = struct.unpack_from("!I", data, 8)[0]
        if len(data) <= 12:
            return cls(session, active, "")
        if len(data) == 13 + data[12]:
            path = data[13 : 13 + data[12]].decode("utf-8")
            return cls(session, active, path)
        nlen = struct.unpack_from("!H", data, 12)[0]
        if len(data) < 14 + nlen:
            raise ValueError("READY path truncated")
        path = data[14 : 14 + nlen].decode("utf-8")
        return cls(session, active, path)


@dataclass(slots=True)
class BlockMeta:
    session_id: int
    file_size: int
    file_name: str
    symbol_size: int
    block_k: int
    initial_repair_pct: int
    active_bytes: int
    sha256_hex: str = ""

    def pack(self) -> bytes:
        name = self.file_name.encode("utf-8")[:255]
        digest = self.sha256_hex.encode("ascii")[:64]
        body = struct.pack(
            "!QHHBI BB",
            self.file_size,
            self.symbol_size,
            self.block_k,
            self.initial_repair_pct,
            self.active_bytes,
            len(name),
            len(digest),
        )
        flags = 1 if self.file_size == 0 and self.file_name.startswith("!") else 0
        return (
            _HDR.pack(MAGIC, VERSION, BlockPacketType.META, flags, self.session_id)
            + body
            + name
            + digest
        )

    @classmethod
    def unpack(cls, data: bytes) -> BlockMeta:
        _require(data, 27, BlockPacketType.META)
        session = struct.unpack_from("!I", data, 4)[0]
        file_size, symbol_size, block_k, fec, active_bytes, nlen, dlen = (
            struct.unpack_from("!QHHBI BB", data, 8)
        )
        off = 27
        if len(data) < off + nlen + dlen:
            raise ValueError("META strings truncated")
        name = data[off : off + nlen].decode("utf-8")
        off += nlen
        digest = data[off : off + dlen].decode("ascii")
        return cls(session, file_size, name, symbol_size, block_k, fec, active_bytes, digest)


@dataclass(slots=True)
class BlockData:
    session_id: int
    block_id: int
    esi: int
    payload: bytes
    send_ts_us: int = 0

    def pack(self) -> bytes:
        return (
            _HDR.pack(MAGIC, VERSION, BlockPacketType.DATA, 0, self.session_id)
            + _DATA.pack(self.block_id, self.esi, self.send_ts_us & 0xFFFFFFFF)
            + self.payload
        )

    @classmethod
    def unpack(cls, data: bytes) -> BlockData:
        _require(data, 20, BlockPacketType.DATA)
        session = struct.unpack_from("!I", data, 4)[0]
        block_id, esi, stamp = _DATA.unpack_from(data, 8)
        return cls(session, block_id, esi, data[20:], stamp)


def pack_data_packets(
    session_id: int,
    block_id: int,
    payloads: list[bytes],
    first_esi: int = 0,
    send_ts_us: int = 0,
) -> list[bytes]:
    """Pack DATA datagrams without allocating BlockData per symbol."""
    sid = session_id & 0xFFFFFFFF
    bid = block_id & 0xFFFFFFFF
    stamp = send_ts_us & 0xFFFFFFFF
    out: list[bytes] = []
    for i, payload in enumerate(payloads):
        buf = bytearray(20 + len(payload))
        _HDR.pack_into(buf, 0, MAGIC, VERSION, BlockPacketType.DATA, 0, sid)
        _DATA.pack_into(buf, 8, bid, (first_esi + i) & 0xFFFFFFFF, stamp)
        buf[20:] = payload
        out.append(buf)
    return out


def stamp_data_wires(wires: list[bytes], send_ts_us: int) -> None:
    """Overwrite DATA send_ts in place so echo RTT is not encode/prefetch age."""
    packed = struct.pack("!I", send_ts_us & 0xFFFFFFFF)
    for i, wire in enumerate(wires):
        if len(wire) < 20:
            continue
        if isinstance(wire, bytearray):
            wire[16:20] = packed
            continue
        buf = bytearray(wire)
        buf[16:20] = packed
        wires[i] = buf


@dataclass(slots=True)
class BlockFeedback:
    session_id: int
    feedback_id: int
    unique_payload_bytes: int
    decoded_file_bytes: int
    echo_ts_us: int = 0
    done_blocks: list[int] | None = None
    open_blocks: list[OpenBlock] | None = None

    def pack(self) -> bytes:
        ranges = block_ids_to_ranges(
            self.done_blocks or [],
            limit=MAX_DONE_RANGES,
            rotate=self.feedback_id,
        )
        opened = list(self.open_blocks or [])[:MAX_OPEN_BLOCKS]
        body = _FB_BASE.pack(
            self.feedback_id & 0xFFFFFFFF,
            self.unique_payload_bytes & 0xFFFFFFFFFFFFFFFF,
            self.decoded_file_bytes & 0xFFFFFFFFFFFFFFFF,
            self.echo_ts_us & 0xFFFFFFFF,
            len(ranges),
            len(opened),
        )
        body += b"".join(
            _RANGE.pack(start & 0xFFFFFFFF, count & 0xFFFFFFFF)
            for start, count in ranges
        )
        body += b"".join(
            _OPEN.pack(
                item.block_id & 0xFFFFFFFF,
                min(0xFFFF, max(0, item.unique_esi)),
                min(255, max(0, item.age_bucket)),
                1 if item.decode_failed else 0,
            )
            for item in opened
        )
        out = _HDR.pack(
            MAGIC, VERSION, BlockPacketType.FEEDBACK, 0, self.session_id
        ) + body
        if len(out) > MAX_DATAGRAM:
            raise ValueError("feedback exceeds one datagram")
        return out

    @classmethod
    def unpack(cls, data: bytes) -> BlockFeedback:
        _require(data, 36, BlockPacketType.FEEDBACK)
        session = struct.unpack_from("!I", data, 4)[0]
        feedback_id, unique, decoded, echo, nranges, nopen = _FB_BASE.unpack_from(
            data, 8
        )
        if nranges > MAX_DONE_RANGES or nopen > MAX_OPEN_BLOCKS:
            raise ValueError("feedback count out of bounds")
        off = 36
        need = off + nranges * _RANGE.size + nopen * _OPEN.size
        if len(data) < need:
            raise ValueError("feedback truncated")
        ranges: list[tuple[int, int]] = []
        for _ in range(nranges):
            start, count = _RANGE.unpack_from(data, off)
            if count <= 0 or count > MAX_RANGE_SPAN:
                raise ValueError("done range too large")
            ranges.append((start, count))
            off += _RANGE.size
        opened: list[OpenBlock] = []
        for _ in range(nopen):
            block_id, rx, age, flags = _OPEN.unpack_from(data, off)
            opened.append(OpenBlock(block_id, rx, bool(flags & 1), age))
            off += _OPEN.size
        return cls(
            session,
            feedback_id,
            unique,
            decoded,
            echo,
            ranges_to_block_ids(ranges),
            opened,
        )


@dataclass(slots=True)
class BlockFin:
    session_id: int
    total_blocks: int
    ok: bool = True

    def pack(self) -> bytes:
        return _HDR.pack(
            MAGIC, VERSION, BlockPacketType.FIN, 1 if self.ok else 0, self.session_id
        ) + struct.pack("!I", self.total_blocks)

    @classmethod
    def unpack(cls, data: bytes) -> BlockFin:
        _require(data, 12, BlockPacketType.FIN)
        return cls(
            struct.unpack_from("!I", data, 4)[0],
            struct.unpack_from("!I", data, 8)[0],
            bool(data[3] & 1),
        )


MUX_META_NAME = "__objects__"
_OBJ_NAMED = struct.Struct("!IQB")


def _pack_obj_named(kind: BlockPacketType, session_id: int, obj_id: int, size: int, name: str) -> bytes:
    raw = name.encode("utf-8")[:255]
    return (
        _HDR.pack(MAGIC, VERSION, kind, 0, session_id)
        + _OBJ_NAMED.pack(obj_id & 0xFFFFFFFF, size & 0xFFFFFFFFFFFFFFFF, len(raw))
        + raw
    )


def _unpack_obj_named(cls, data: bytes, kind: BlockPacketType):
    _require(data, 21, kind)
    session = struct.unpack_from("!I", data, 4)[0]
    obj_id, size, nlen = _OBJ_NAMED.unpack_from(data, 8)
    if len(data) < 21 + nlen:
        raise ValueError(f"{kind.name} truncated")
    return cls(session, obj_id, size, data[21 : 21 + nlen].decode("utf-8"))


@dataclass(slots=True)
class ObjectOpen:
    session_id: int
    obj_id: int
    size: int
    name: str

    def pack(self) -> bytes:
        return _pack_obj_named(
            BlockPacketType.OBJ_OPEN, self.session_id, self.obj_id, self.size, self.name
        )

    @classmethod
    def unpack(cls, data: bytes) -> ObjectOpen:
        return _unpack_obj_named(cls, data, BlockPacketType.OBJ_OPEN)


@dataclass(slots=True)
class ObjectFin:
    session_id: int
    obj_id: int
    size: int
    name: str = ""

    def pack(self) -> bytes:
        return _pack_obj_named(
            BlockPacketType.OBJ_FIN, self.session_id, self.obj_id, self.size, self.name
        )

    @classmethod
    def unpack(cls, data: bytes) -> ObjectFin:
        return _unpack_obj_named(cls, data, BlockPacketType.OBJ_FIN)


def parse_packet(data: bytes):
    if len(data) < 8 or data[0] != MAGIC or data[1] != VERSION:
        raise ValueError("not a tetrys packet")
    try:
        kind = BlockPacketType(data[2])
    except ValueError as exc:
        raise ValueError(f"unknown packet type {data[2]}") from exc
    cls = {
        BlockPacketType.META: BlockMeta,
        BlockPacketType.READY: BlockReady,
        BlockPacketType.DATA: BlockData,
        BlockPacketType.FEEDBACK: BlockFeedback,
        BlockPacketType.FIN: BlockFin,
        BlockPacketType.OBJ_OPEN: ObjectOpen,
        BlockPacketType.OBJ_FIN: ObjectFin,
    }[kind]
    return cls.unpack(data)


def block_ids_to_ranges(
    ids: list[int],
    *,
    limit: int = MAX_DONE_RANGES,
    rotate: int = 0,
) -> list[tuple[int, int]]:
    """Compact completed block IDs into inclusive-count ranges."""
    if not ids:
        return []
    ordered = sorted(set(ids))
    ranges: list[tuple[int, int]] = []
    start = prev = ordered[0]
    for block_id in ordered[1:]:
        if block_id == prev + 1:
            prev = block_id
            continue
        ranges.append((start, prev - start + 1))
        start = prev = block_id
    ranges.append((start, prev - start + 1))
    if len(ranges) <= limit:
        return ranges
    offset = (rotate * limit) % len(ranges)
    out: list[tuple[int, int]] = []
    idx = offset
    for _ in range(limit):
        out.append(ranges[idx])
        idx = (idx + 1) % len(ranges)
    return out


def ranges_to_block_ids(ranges: list[tuple[int, int]]) -> list[int]:
    out: list[int] = []
    for start, count in ranges:
        if count <= 0 or count > MAX_RANGE_SPAN:
            raise ValueError("done range too large")
        out.extend(range(start, start + count))
    return out


def _require(data: bytes, size: int, kind: BlockPacketType) -> None:
    if (
        len(data) < size
        or data[0] != MAGIC
        or data[1] != VERSION
        or data[2] != int(kind)
    ):
        raise ValueError(f"invalid {kind.name} packet")
