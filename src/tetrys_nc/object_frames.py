"""Pack many objects into a fixed-size RaptorQ block payload."""

from __future__ import annotations

import struct
from dataclasses import dataclass

# obj_id, byte offset, payload length, name length (0 on continuation).
_FRAME = struct.Struct("!IQIB")
FRAME_HDR = _FRAME.size
_SIZE = struct.Struct("!Q")


def _name_bytes(name: str) -> bytes:
    return name.encode("utf-8")[:255]


def frame_overhead(name: str = "") -> int:
    extra = (_SIZE.size + len(_name_bytes(name))) if name else 0
    return FRAME_HDR + extra


@dataclass(slots=True)
class ObjectChunk:
    obj_id: int
    offset: int
    data: bytes
    name: str = ""
    size: int = 0


def pack_block(block_bytes: int, chunks: list[ObjectChunk]) -> bytes:
    """Serialize chunks into a padded block. Remaining space is zeros."""
    if block_bytes <= FRAME_HDR:
        raise ValueError("block too small for an object frame")
    buf = bytearray(block_bytes)
    pos = 0
    for chunk in chunks:
        if not chunk.data:
            continue
        name = _name_bytes(chunk.name) if chunk.offset == 0 and chunk.name else b""
        need = FRAME_HDR + (0 if not name else _SIZE.size + len(name)) + len(chunk.data)
        if pos + need > block_bytes:
            raise ValueError("chunks overflow block")
        _FRAME.pack_into(
            buf, pos, chunk.obj_id, chunk.offset, len(chunk.data), len(name)
        )
        pos += FRAME_HDR
        if name:
            _SIZE.pack_into(buf, pos, chunk.size & 0xFFFFFFFFFFFFFFFF)
            pos += _SIZE.size
            buf[pos : pos + len(name)] = name
            pos += len(name)
        buf[pos : pos + len(chunk.data)] = chunk.data
        pos += len(chunk.data)
    return bytes(buf)


def unpack_block(block: bytes) -> list[ObjectChunk]:
    """Parse frames until padding (length 0 or truncated header)."""
    out: list[ObjectChunk] = []
    pos = 0
    n = len(block)
    while pos + FRAME_HDR <= n:
        obj_id, offset, length, nlen = _FRAME.unpack_from(block, pos)
        if length == 0 and obj_id == 0:
            break
        pos += FRAME_HDR
        name = ""
        size = 0
        if nlen:
            if pos + _SIZE.size + nlen > n:
                break
            size = _SIZE.unpack_from(block, pos)[0]
            pos += _SIZE.size
            name = block[pos : pos + nlen].decode("utf-8")
            pos += nlen
        if length <= 0 or pos + length > n:
            break
        out.append(
            ObjectChunk(obj_id, offset, bytes(block[pos : pos + length]), name, size)
        )
        pos += length
    return out


class ObjectCursor:
    def __init__(self, obj_id: int, name: str, data: bytes) -> None:
        self.obj_id = obj_id
        self.name = name
        self.data = data
        self.pos = 0
        self.announced = False

    @property
    def remaining(self) -> int:
        return len(self.data) - self.pos

    @property
    def done(self) -> bool:
        return self.pos >= len(self.data)


class BlockFill:
    """Fill one block from a sequence of object cursors."""

    def __init__(self, block_bytes: int) -> None:
        self.block_bytes = block_bytes
        self.chunks: list[ObjectChunk] = []
        self._pos = 0
        self.opened: list[ObjectCursor] = []
        self.finished: list[ObjectCursor] = []

    def space(self) -> int:
        return self.block_bytes - self._pos

    def take(self, cursor: ObjectCursor) -> bool:
        """Append as much of cursor as fits. Return True if any bytes taken."""
        room = self.space()
        first = cursor.pos == 0
        hdr = frame_overhead(cursor.name if first else "")
        if room <= hdr or cursor.done:
            return False
        n = min(cursor.remaining, room - hdr)
        if n <= 0:
            return False
        if not cursor.announced:
            self.opened.append(cursor)
            cursor.announced = True
        chunk = cursor.data[cursor.pos : cursor.pos + n]
        self.chunks.append(
            ObjectChunk(
                cursor.obj_id,
                cursor.pos,
                chunk,
                cursor.name if first else "",
                len(cursor.data) if first else 0,
            )
        )
        cursor.pos += n
        self._pos += hdr + n
        if cursor.done:
            self.finished.append(cursor)
        return True

    def packed(self) -> bytes:
        return pack_block(self.block_bytes, self.chunks)
