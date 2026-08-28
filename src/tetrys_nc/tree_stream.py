"""Virtual bundle stream: arbitrary files as one sequential byte range.

Wire payload is: binary index | file bytes in index order | padding to block.
Source files stay on disk; encode/decode never load the whole tree into RAM.
"""

from __future__ import annotations

import mmap
import os
import struct
import sys
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

MAGIC = b"TNTR"
VERSION = 1
TREE_META_NAME = "__tree__"
STAGING_NAME = ".tetrys_stream"
INDEX_ALIGN = 4096
_COPY_CHUNK = 1 << 20
_HDR = struct.Struct("!4sHI")  # magic, version, nfiles
_ENT = struct.Struct("!QH")  # size, path_len
_FD_LRU = 64


class TreePathError(ValueError):
    pass


def rel_posix(root: Path, path: Path) -> str:
    rel = path.relative_to(root).as_posix()
    if not rel or rel.startswith("/") or rel == "." or ".." in Path(rel).parts:
        raise TreePathError(f"unsafe path {path}")
    return rel


def safe_join(root: Path, rel: str) -> Path:
    if not rel or rel.startswith("/") or ".." in Path(rel).parts:
        raise TreePathError(f"unsafe relative path {rel!r}")
    out = (root / rel).resolve()
    base = root.resolve()
    if out != base and base not in out.parents:
        raise TreePathError(f"path escapes root: {rel}")
    return out


@dataclass(slots=True)
class FileExtent:
    rel: str
    size: int
    stream_off: int
    src: Path | None = None


@dataclass(slots=True)
class TreeLayout:
    files: list[FileExtent]
    index_bytes: int
    payload_bytes: int

    @property
    def total_bytes(self) -> int:
        return self.index_bytes + self.payload_bytes


def pad_index(index: bytes) -> bytes:
    n = max(INDEX_ALIGN, -(-len(index) // INDEX_ALIGN) * INDEX_ALIGN)
    if len(index) == n:
        return index
    return index + bytes(n - len(index))


def aligned_index_bytes(parsed_end: int) -> int:
    return max(INDEX_ALIGN, -(-parsed_end // INDEX_ALIGN) * INDEX_ALIGN)


def encode_index(files: list[tuple[str, int]]) -> bytes:
    buf = bytearray(_HDR.pack(MAGIC, VERSION, len(files)))
    for rel, size in files:
        raw = rel.encode("utf-8")
        if len(raw) > 65535:
            raise TreePathError(f"path too long: {rel}")
        buf += _ENT.pack(size & 0xFFFFFFFFFFFFFFFF, len(raw))
        buf += raw
    return pad_index(bytes(buf))


def decode_index(blob: bytes) -> tuple[list[tuple[str, int]], int]:
    if len(blob) < _HDR.size or blob[:4] != MAGIC:
        raise ValueError("not a tetrys tree stream")
    magic, ver, nfiles = _HDR.unpack_from(blob, 0)
    if ver != VERSION:
        raise ValueError(f"unsupported tree version {ver}")
    pos = _HDR.size
    files: list[tuple[str, int]] = []
    for _ in range(nfiles):
        if pos + _ENT.size > len(blob):
            raise ValueError("truncated tree index")
        size, nlen = _ENT.unpack_from(blob, pos)
        pos += _ENT.size
        if pos + nlen > len(blob):
            raise ValueError("truncated tree path")
        rel = blob[pos : pos + nlen].decode("utf-8")
        pos += nlen
        files.append((rel, size))
    return files, pos


def layout_from_root(root: Path) -> TreeLayout:
    root = root.resolve()
    found: list[tuple[str, int, Path]] = []
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if not path.is_file():
            continue
        rel = rel_posix(root, path)
        found.append((rel, path.stat().st_size, path.resolve()))
    index = encode_index([(rel, size) for rel, size, _ in found])
    files: list[FileExtent] = []
    off = len(index)
    payload = 0
    for rel, size, src in found:
        files.append(FileExtent(rel, size, off, src))
        off += size
        payload += size
    return TreeLayout(files, len(index), payload)


def layout_from_index(index: bytes) -> TreeLayout:
    pairs, parsed_end = decode_index(index)
    index_len = aligned_index_bytes(parsed_end)
    if len(index) < index_len:
        raise ValueError("truncated aligned tree index")
    files: list[FileExtent] = []
    off = index_len
    payload = 0
    for rel, size in pairs:
        files.append(FileExtent(rel, size, off, None))
        off += size
        payload += size
    return TreeLayout(files, index_len, payload)


def _bisect_extent(files: list[FileExtent], stream_off: int) -> int:
    lo, hi = 0, len(files)
    while lo < hi:
        mid = (lo + hi) // 2
        ext = files[mid]
        if stream_off < ext.stream_off:
            hi = mid
        elif stream_off >= ext.stream_off + ext.size:
            lo = mid + 1
        else:
            return mid
    return lo


class StreamSource:
    """Read a stream range from source files via pread."""

    def __init__(self, layout: TreeLayout, index: bytes) -> None:
        if len(index) != layout.index_bytes:
            raise ValueError("index length mismatch")
        self.layout = layout
        self.index = index
        self._fds: OrderedDict[str, int] = OrderedDict()
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            for fd in self._fds.values():
                os.close(fd)
            self._fds.clear()

    def _fd(self, ext: FileExtent) -> int:
        if ext.src is None:
            raise RuntimeError("source path missing")
        key = str(ext.src)
        with self._lock:
            if key in self._fds:
                self._fds.move_to_end(key)
                return self._fds[key]
            while len(self._fds) >= _FD_LRU:
                _, old = self._fds.popitem(last=False)
                os.close(old)
            fd = os.open(ext.src, os.O_RDONLY)
            self._fds[key] = fd
            return fd

    def read_block(self, block_id: int, block_bytes: int) -> tuple[bytes, int]:
        off = block_id * block_bytes
        tlen = min(block_bytes, max(0, self.layout.total_bytes - off))
        if tlen <= 0:
            return bytes(block_bytes), 0
        buf = bytearray(block_bytes)
        self._read_into(memoryview(buf)[:tlen], off)
        return bytes(buf), tlen

    def _read_into(self, dest: memoryview, stream_off: int) -> None:
        pos = 0
        remain = len(dest)
        off = stream_off
        files = self.layout.files
        index_end = self.layout.index_bytes
        while remain > 0:
            if off < index_end:
                n = min(remain, index_end - off)
                dest[pos : pos + n] = self.index[off : off + n]
                pos += n
                off += n
                remain -= n
                continue
            if not files:
                dest[pos : pos + remain] = b"\x00" * remain
                return
            idx = _bisect_extent(files, off)
            if idx >= len(files):
                dest[pos : pos + remain] = b"\x00" * remain
                return
            ext = files[idx]
            if off < ext.stream_off:
                gap = min(remain, ext.stream_off - off)
                dest[pos : pos + gap] = b"\x00" * gap
                pos += gap
                off += gap
                remain -= gap
                continue
            local = off - ext.stream_off
            n = min(remain, ext.size - local)
            if n <= 0:
                dest[pos : pos + remain] = b"\x00" * remain
                return
            got = os.pread(self._fd(ext), n, local)
            dest[pos : pos + len(got)] = got
            if len(got) < n:
                dest[pos + len(got) : pos + n] = b"\x00" * (n - len(got))
            pos += n
            off += n
            remain -= n


class StreamSink:
    """Write stream ranges into destination files once the index prefix is known."""

    def __init__(self, dest: Path) -> None:
        self.dest = dest
        self.layout: TreeLayout | None = None
        self._pending: dict[int, bytes] = {}
        self._index_buf = bytearray()
        self._fds: OrderedDict[str, int] = OrderedDict()
        self._created: set[str] = set()

    def close(self) -> None:
        for fd in self._fds.values():
            os.close(fd)
        self._fds.clear()

    def ingest(self, stream_off: int, payload: bytes) -> None:
        if self.layout is None:
            end = stream_off + len(payload)
            if end > len(self._index_buf):
                self._index_buf.extend(b"\x00" * (end - len(self._index_buf)))
            self._index_buf[stream_off : stream_off + len(payload)] = payload
            self._pending[stream_off] = payload
            if not self._try_index():
                return
            extras = list(self._pending.items())
            self._pending.clear()
            for off, blob in extras:
                self._write_range(off, blob)
            return
        self._write_range(stream_off, payload)

    def _try_index(self) -> bool:
        if len(self._index_buf) < _HDR.size:
            return False
        try:
            _pairs, parsed_end = decode_index(bytes(self._index_buf))
        except ValueError:
            return False
        index_len = aligned_index_bytes(parsed_end)
        if len(self._index_buf) < index_len:
            return False
        self.layout = layout_from_index(bytes(self._index_buf[:index_len]))
        self.dest.mkdir(parents=True, exist_ok=True)
        for ext in self.layout.files:
            if ext.size == 0:
                path = safe_join(self.dest, ext.rel)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
                self._created.add(ext.rel)
        return True

    def _fd(self, ext: FileExtent) -> int:
        if ext.rel in self._fds:
            self._fds.move_to_end(ext.rel)
            return self._fds[ext.rel]
        while len(self._fds) >= _FD_LRU:
            _, old = self._fds.popitem(last=False)
            os.close(old)
        path = safe_join(self.dest, ext.rel)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
        if ext.rel not in self._created:
            if ext.size > 0:
                os.ftruncate(fd, ext.size)
            self._created.add(ext.rel)
        self._fds[ext.rel] = fd
        return fd

    def _write_range(self, stream_off: int, payload: bytes) -> None:
        assert self.layout is not None
        pos = 0
        off = stream_off
        remain = len(payload)
        index_end = self.layout.index_bytes
        files = self.layout.files
        while remain > 0:
            if off < index_end:
                skip = min(remain, index_end - off)
                pos += skip
                off += skip
                remain -= skip
                continue
            if not files:
                return
            idx = _bisect_extent(files, off)
            if idx >= len(files):
                return
            ext = files[idx]
            if off < ext.stream_off:
                gap = min(remain, ext.stream_off - off)
                pos += gap
                off += gap
                remain -= gap
                continue
            local = off - ext.stream_off
            if ext.size <= 0:
                return
            n = min(remain, ext.size - local)
            if n <= 0:
                return
            os.pwrite(self._fd(ext), payload[pos : pos + n], local)
            pos += n
            off += n
            remain -= n


def _drop_prefix(path: Path, prefix: int) -> None:
    """Remove a leading prefix from a file, preferably without a second copy."""
    size = path.stat().st_size
    if prefix <= 0:
        return
    if prefix >= size:
        os.truncate(path, 0)
        return
    payload = size - prefix
    fd = os.open(path, os.O_RDWR)
    try:
        collapse = getattr(os, "FALLOC_FL_COLLAPSE_RANGE", None)
        if (
            collapse is not None
            and sys.platform.startswith("linux")
            and prefix % INDEX_ALIGN == 0
        ):
            try:
                os.fallocate(fd, collapse, 0, prefix)
                return
            except OSError:
                pass
        chunk = _COPY_CHUNK
        remaining = payload
        while remaining > 0:
            take = min(chunk, remaining)
            src = prefix + remaining - take
            dst = remaining - take
            data = os.pread(fd, take, src)
            os.pwrite(fd, data, dst)
            remaining -= take
        os.ftruncate(fd, payload)
    finally:
        os.close(fd)


def materialize_from_staging(staging: Path, dest: Path) -> int:
    """Split a received stream file into the destination tree."""
    dest.mkdir(parents=True, exist_ok=True)
    with staging.open("rb") as fh:
        mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            pairs, parsed_end = decode_index(mm)
            index_len = aligned_index_bytes(parsed_end)
        finally:
            mm.close()
    nonempty = [(rel, size) for rel, size in pairs if size > 0]
    empties = [rel for rel, size in pairs if size <= 0]
    for rel in empties:
        path = safe_join(dest, rel)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    if len(nonempty) == 1:
        rel, size = nonempty[0]
        path = safe_join(dest, rel)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        staging.replace(path)
        _drop_prefix(path, index_len)
        if path.stat().st_size != size:
            os.truncate(path, size)
        return len(pairs)
    with staging.open("rb") as fh:
        mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            off = index_len
            for rel, size in pairs:
                if size <= 0:
                    continue
                path = safe_join(dest, rel)
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("wb") as out:
                    remain = size
                    pos = off
                    while remain > 0:
                        take = min(_COPY_CHUNK, remain)
                        out.write(mm[pos : pos + take])
                        pos += take
                        remain -= take
                off += size
        finally:
            mm.close()
    staging.unlink(missing_ok=True)
    return len(pairs)
