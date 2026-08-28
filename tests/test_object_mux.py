from __future__ import annotations

import os
import socket
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("raptorq")

from tetrys_nc.block_packets import (
    ObjectFinV2,
    ObjectOpenV2,
    ObjectResetV2,
    parse_v2_packet,
)
from tetrys_nc.object_frames import BlockFill, ObjectChunk, ObjectCursor, pack_block, unpack_block
from tetrys_nc.object_pack import pack_files, split_for_session, unpack_files
from tetrys_nc.object_xfer import ObjectSession, run_object_client, run_object_server


def _free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_object_control_packets_roundtrip():
    packets = [
        ObjectOpenV2(9, 3, 12, "note.txt"),
        ObjectFinV2(9, 3, 12, "note.txt"),
        ObjectResetV2(9, 3),
    ]
    for packet in packets:
        assert parse_v2_packet(packet.pack()) == packet


def test_pack_unpack_two_objects_in_one_block():
    chunks = [
        ObjectChunk(1, 0, b"hello", "a.txt", 5),
        ObjectChunk(2, 0, b"world!!", "b.txt", 7),
    ]
    block = pack_block(256, chunks)
    assert len(block) == 256
    got = unpack_block(block)
    assert [(c.obj_id, c.offset, c.data, c.name, c.size) for c in got] == [
        (1, 0, b"hello", "a.txt", 5),
        (2, 0, b"world!!", "b.txt", 7),
    ]


def test_block_fill_splits_large_object():
    cursor = ObjectCursor(1, "big.bin", b"x" * 80)
    fill = BlockFill(64)
    assert fill.take(cursor) is True
    assert not cursor.done
    rest = BlockFill(80)
    assert rest.take(cursor) is True
    assert cursor.done
    a = unpack_block(fill.packed())
    b = unpack_block(rest.packed())
    assert a[0].offset == 0
    assert a[0].name == "big.bin"
    assert a[0].size == 80
    assert b[0].offset == len(a[0].data)
    assert b[0].name == ""
    assert a[0].data + b[0].data == b"x" * 80


def test_session_put_after_close_raises():
    session = ObjectSession()
    session.put("a.bin", b"aa")
    session.close()
    with pytest.raises(RuntimeError):
        session.put("b.bin", b"bb")


def test_session_rejects_path_escape():
    session = ObjectSession()
    with pytest.raises(ValueError):
        session.put("..", b"x")


def test_object_mux_loopback_and_late_enqueue(tmp_path: Path):
    session = ObjectSession()
    out = tmp_path / "recv"
    port = _free_udp_port()
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_object_server(
                "127.0.0.1",
                port,
                session,
                symbol_size=256,
                block_k=32,
                initial_repair_pct=14,
                active_bytes=1 << 20,
                rate_mbit=200,
            )
        except BaseException as exc:
            errors.append(exc)

    def client() -> None:
        try:
            run_object_client("127.0.0.1", port, out, active_bytes=1 << 20)
        except BaseException as exc:
            errors.append(exc)

    st = threading.Thread(target=server)
    st.start()
    time.sleep(0.05)
    ct = threading.Thread(target=client)
    ct.start()
    time.sleep(0.08)
    early = {f"a{i}.bin": os.urandom(400 + i) for i in range(3)}
    for name, blob in early.items():
        session.put(name, blob)
    time.sleep(0.05)
    late = {"late.bin": b"queued-while-running", "z.dat": os.urandom(1500)}
    for name, blob in late.items():
        session.put(name, blob)
    session.close()
    ct.join(timeout=20)
    st.join(timeout=20)
    assert not errors, errors[0]
    assert ct.is_alive() is False
    assert st.is_alive() is False
    want = {**early, **late}
    got = {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}
    assert got == want


def test_object_mux_many_micros_and_two_large(tmp_path: Path):
    session = ObjectSession()
    out = tmp_path / "recv"
    port = _free_udp_port()
    errors: list[BaseException] = []
    micros = {f"m{i:04d}.bin": bytes([i % 256]) * (40 + i % 80) for i in range(120)}
    larges = {
        "big0.bin": os.urandom(80_000),
        "big1.bin": os.urandom(120_000),
    }

    def server() -> None:
        try:
            run_object_server(
                "127.0.0.1",
                port,
                session,
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
            run_object_client("127.0.0.1", port, out, active_bytes=2 << 20)
        except BaseException as exc:
            errors.append(exc)

    st = threading.Thread(target=server)
    st.start()
    time.sleep(0.05)
    ct = threading.Thread(target=client)
    ct.start()
    time.sleep(0.05)
    for name, blob in micros.items():
        session.put(name, blob)
    time.sleep(0.02)
    for name, blob in larges.items():
        session.put(name, blob)
    session.close()
    ct.join(timeout=30)
    st.join(timeout=30)
    assert not errors, errors[0]
    want = {**micros, **larges}
    got = {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}
    assert got == want


def test_tree_pack_roundtrip():
    files = [(f"m{i}.bin", bytes([i]) * (10 + i)) for i in range(20)]
    blob = pack_files(files)
    assert unpack_files(blob) == files


def test_split_packs_micros_keeps_larges():
    micros = [(f"m{i}.bin", b"x" * 50) for i in range(40)]
    big = ("big.bin", os.urandom(300_000))
    objects = split_for_session(micros + [big], small_max=256_000, pack_max=2000)
    names = [n for n, _ in objects]
    assert "big.bin" in names
    assert any(n.startswith("__pack_") for n in names)
    restored: dict[str, bytes] = {}
    for name, data in objects:
        if name.startswith("__pack_"):
            restored.update(unpack_files(data))
        else:
            restored[name] = data
    want = {n: d for n, d in micros}
    want["big.bin"] = big[1]
    assert restored == want


def test_split_pack_start_avoids_name_collision():
    a = split_for_session([("a.bin", b"x" * 10)], pack_max=100)
    b = split_for_session([("b.bin", b"y" * 10)], pack_max=100, pack_start=1)
    assert a[0][0] != b[0][0]


def test_object_mux_packed_micros_loopback(tmp_path: Path):
    session = ObjectSession()
    out = tmp_path / "recv"
    port = _free_udp_port()
    errors: list[BaseException] = []
    micros = {f"m{i:04d}.bin": bytes([i % 256]) * (40 + i % 80) for i in range(80)}
    larges = {"big0.bin": os.urandom(90_000)}
    objects = split_for_session(
        [*micros.items(), *larges.items()],
        small_max=256_000,
        pack_max=50_000,
    )

    def server() -> None:
        try:
            run_object_server(
                "127.0.0.1",
                port,
                session,
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
            run_object_client("127.0.0.1", port, out, active_bytes=2 << 20)
        except BaseException as exc:
            errors.append(exc)

    st = threading.Thread(target=server)
    st.start()
    time.sleep(0.05)
    ct = threading.Thread(target=client)
    ct.start()
    time.sleep(0.05)
    for name, blob in objects:
        session.put(name, blob)
    session.close()
    ct.join(timeout=30)
    st.join(timeout=30)
    assert not errors, errors[0]
    got = {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}
    assert got == {**micros, **larges}
