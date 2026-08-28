from __future__ import annotations

import os
import socket
import threading
from pathlib import Path

import pytest

pytest.importorskip("raptorq")

from tetrys_nc.tree_stream import (
    StreamSink,
    StreamSource,
    aligned_index_bytes,
    decode_index,
    encode_index,
    layout_from_root,
    materialize_from_staging,
)
from tetrys_nc.tree_xfer import run_tree_client, run_tree_server


def _free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _write_tree(root: Path) -> dict[str, bytes]:
    want: dict[str, bytes] = {}
    (root / "sub").mkdir()
    files = {
        "empty.dat": b"",
        "a.bin": b"hello",
        "sub/b.bin": os.urandom(80_000),
        "sub/c.bin": bytes(range(256)) * 40,
        "z-late-name.txt": b"tail",
    }
    for rel, data in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        want[rel] = data
    real = root / "real_link_target.bin"
    real.write_bytes(b"linked")
    (root / "via_link.bin").symlink_to(real.name)
    want["via_link.bin"] = b"linked"
    want["real_link_target.bin"] = b"linked"
    return want


def test_index_roundtrip_and_source_blocks(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    want = _write_tree(src)
    layout = layout_from_root(src)
    index = encode_index([(f.rel, f.size) for f in layout.files])
    pairs, index_len = decode_index(index)
    assert index_len <= len(index)
    assert len(index) % 4096 == 0
    assert {rel for rel, _size in pairs} == set(want)
    assert any(size == 0 for _rel, size in pairs)
    source = StreamSource(layout, index)
    try:
        block, tlen = source.read_block(0, 16_384)
        assert tlen == min(16_384, layout.total_bytes)
        assert block[:4] == b"TNTR"
        assembled = bytearray()
        bid = 0
        while bid * 16_384 < layout.total_bytes:
            payload, n = source.read_block(bid, 16_384)
            assembled += payload[:n]
            bid += 1
        got_pairs, got_len = decode_index(bytes(assembled))
        off = aligned_index_bytes(got_len)
        restored: dict[str, bytes] = {}
        for rel, size in got_pairs:
            restored[rel] = bytes(assembled[off : off + size])
            off += size
        assert restored == want
    finally:
        source.close()


def test_materialize_from_staging_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    want = _write_tree(src)
    layout = layout_from_root(src)
    index = encode_index([(f.rel, f.size) for f in layout.files])
    source = StreamSource(layout, index)
    staging = tmp_path / "stream.bin"
    try:
        assembled = bytearray()
        bid = 0
        while bid * 8192 < layout.total_bytes:
            payload, n = source.read_block(bid, 8192)
            assembled += payload[:n]
            bid += 1
        staging.write_bytes(assembled)
    finally:
        source.close()
    dst = tmp_path / "dst"
    n = materialize_from_staging(staging, dst)
    assert n == len(want)
    got = {
        p.relative_to(dst).as_posix(): p.read_bytes()
        for p in dst.rglob("*")
        if p.is_file()
    }
    assert got == want


def test_materialize_single_file_drops_index_prefix(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    blob = os.urandom(50_000)
    (src / "only.bin").write_bytes(blob)
    layout = layout_from_root(src)
    index = encode_index([(f.rel, f.size) for f in layout.files])
    source = StreamSource(layout, index)
    staging = tmp_path / "stream.bin"
    try:
        assembled = bytearray()
        bid = 0
        while bid * 8192 < layout.total_bytes:
            payload, n = source.read_block(bid, 8192)
            assembled += payload[:n]
            bid += 1
        staging.write_bytes(assembled)
    finally:
        source.close()
    dst = tmp_path / "dst"
    materialize_from_staging(staging, dst)
    assert (dst / "only.bin").read_bytes() == blob
    assert not staging.exists()


def test_sink_out_of_order_blocks(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    want = _write_tree(src)
    layout = layout_from_root(src)
    index = encode_index([(f.rel, f.size) for f in layout.files])
    source = StreamSource(layout, index)
    sink = StreamSink(dst)
    try:
        b1, n1 = source.read_block(1, 4096)
        sink.ingest(4096, b1[:n1])
        b0, n0 = source.read_block(0, 4096)
        sink.ingest(0, b0[:n0])
        bid = 2
        while bid * 4096 < layout.total_bytes:
            payload, n = source.read_block(bid, 4096)
            sink.ingest(bid * 4096, payload[:n])
            bid += 1
    finally:
        source.close()
        sink.close()
    got = {
        p.relative_to(dst).as_posix(): p.read_bytes()
        for p in dst.rglob("*")
        if p.is_file()
    }
    assert got == want


def test_tree_stream_loopback(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    want = _write_tree(src)
    port = _free_udp_port()
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_tree_server(
                "127.0.0.1",
                port,
                src,
                symbol_size=256,
                block_k=64,
                initial_repair_pct=14,
                active_bytes=2 << 20,
                rate_mbit=400,
            )
        except BaseException as exc:
            errors.append(exc)

    def client() -> None:
        try:
            run_tree_client("127.0.0.1", port, dst, active_bytes=2 << 20)
        except BaseException as exc:
            errors.append(exc)

    st = threading.Thread(target=server)
    st.start()
    threading.Event().wait(0.05)
    ct = threading.Thread(target=client)
    ct.start()
    ct.join(timeout=30)
    st.join(timeout=30)
    assert not errors, errors[0]
    got = {
        p.relative_to(dst).as_posix(): p.read_bytes()
        for p in dst.rglob("*")
        if p.is_file()
    }
    assert got == want
