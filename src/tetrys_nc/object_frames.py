"""Object frames inside a RaptorQ block, plus packed-micro containers."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path

_FRAME = struct.Struct("!IQIB")
FRAME_HDR = _FRAME.size
_SIZE = struct.Struct("!Q")
PACK_MAGIC = b"TNCK"
PACK_NAME_PREFIX = "__pack_"
_HDR = struct.Struct("!I")
_ENT = struct.Struct("!HQ")
SMALL_MAX = 256 * 1024
PACK_MAX = 4 << 20


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
        _FRAME.pack_into(buf, pos, chunk.obj_id, chunk.offset, len(chunk.data), len(name))
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
    out: list[ObjectChunk] = []
    pos, n = 0, len(block)
    while pos + FRAME_HDR <= n:
        obj_id, offset, length, nlen = _FRAME.unpack_from(block, pos)
        if length == 0 and obj_id == 0:
            break
        pos += FRAME_HDR
        name, size = "", 0
        if nlen:
            if pos + _SIZE.size + nlen > n:
                break
            size = _SIZE.unpack_from(block, pos)[0]
            pos += _SIZE.size
            name = block[pos : pos + nlen].decode("utf-8")
            pos += nlen
        if length <= 0 or pos + length > n:
            break
        out.append(ObjectChunk(obj_id, offset, bytes(block[pos : pos + length]), name, size))
        pos += length
    return out


class ObjectCursor:
    def __init__(self, obj_id: int, name: str, data: bytes) -> None:
        self.obj_id = obj_id
        self.name = name
        self.data = data
        self.pos = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.pos

    @property
    def done(self) -> bool:
        return self.pos >= len(self.data)


class BlockFill:
    def __init__(self, block_bytes: int) -> None:
        self.block_bytes = block_bytes
        self.chunks: list[ObjectChunk] = []
        self._pos = 0

    def space(self) -> int:
        return self.block_bytes - self._pos

    def take(self, cursor: ObjectCursor) -> bool:
        first = cursor.pos == 0
        hdr = frame_overhead(cursor.name if first else "")
        room = self.space()
        if room <= hdr or cursor.done:
            return False
        n = min(cursor.remaining, room - hdr)
        if n <= 0:
            return False
        self.chunks.append(
            ObjectChunk(
                cursor.obj_id,
                cursor.pos,
                cursor.data[cursor.pos : cursor.pos + n],
                cursor.name if first else "",
                len(cursor.data) if first else 0,
            )
        )
        cursor.pos += n
        self._pos += hdr + n
        return True

    def packed(self) -> bytes:
        return pack_block(self.block_bytes, self.chunks)


def pack_files(files: list[tuple[str, bytes]]) -> bytes:
    buf = bytearray(PACK_MAGIC) + _HDR.pack(len(files))
    for name, data in files:
        raw = Path(name).name.encode("utf-8")[:255]
        buf += _ENT.pack(len(raw), len(data)) + raw + data
    return bytes(buf)


def unpack_files(blob: bytes) -> list[tuple[str, bytes]]:
    if not blob.startswith(PACK_MAGIC):
        raise ValueError("not a tetrys object pack")
    pos = len(PACK_MAGIC)
    (count,) = _HDR.unpack_from(blob, pos)
    pos += _HDR.size
    out: list[tuple[str, bytes]] = []
    for _ in range(count):
        nlen, size = _ENT.unpack_from(blob, pos)
        pos += _ENT.size
        name = blob[pos : pos + nlen].decode("utf-8")
        pos += nlen
        out.append((name, blob[pos : pos + size]))
        pos += size
    return out


def is_pack_name(name: str) -> bool:
    return Path(name).name.startswith(PACK_NAME_PREFIX)


def split_for_session(
    files: list[tuple[str, bytes]],
    *,
    small_max: int = SMALL_MAX,
    pack_max: int = PACK_MAX,
    pack_start: int = 0,
) -> list[tuple[str, bytes]]:
    objects: list[tuple[str, bytes]] = []
    batch: list[tuple[str, bytes]] = []
    batch_bytes = 0
    pack_i = pack_start

    def flush() -> None:
        nonlocal pack_i, batch_bytes
        if not batch:
            return
        objects.append((f"{PACK_NAME_PREFIX}{pack_i:04d}.tnck", pack_files(batch)))
        pack_i += 1
        batch.clear()
        batch_bytes = 0

    for name, data in files:
        if len(data) > small_max:
            flush()
            objects.append((name, data))
            continue
        extra = 2 + 8 + min(255, len(Path(name).name.encode("utf-8"))) + len(data)
        if batch and batch_bytes + extra > pack_max:
            flush()
        batch.append((name, data))
        batch_bytes += extra
    flush()
    return objects
