"""Pack many tiny files into one mux object (ATP-style packed tree)."""

from __future__ import annotations

import struct
from pathlib import Path

PACK_MAGIC = b"TNCK"
PACK_NAME_PREFIX = "__pack_"
_HDR = struct.Struct("!I")  # file count
_ENT = struct.Struct("!HQ")  # name length, payload size

# Files larger than this stay as their own mux objects.
SMALL_MAX = 256 * 1024
# Flush a pack when it would exceed this (a few WAN blocks).
PACK_MAX = 4 << 20


def pack_files(files: list[tuple[str, bytes]]) -> bytes:
    buf = bytearray(PACK_MAGIC)
    buf += _HDR.pack(len(files))
    for name, data in files:
        raw = Path(name).name.encode("utf-8")[:255]
        buf += _ENT.pack(len(raw), len(data))
        buf += raw
        buf += data
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
        data = blob[pos : pos + size]
        pos += size
        out.append((name, data))
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
    """Turn a file list into mux objects: packs of micros + leftover larges."""
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
