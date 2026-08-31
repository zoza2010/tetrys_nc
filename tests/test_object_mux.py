from __future__ import annotations

import os
import socket
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("raptorq")

from tetrys_nc.block_packets import ObjectFin, ObjectOpen, parse_packet
from tetrys_nc.object_frames import (
    BlockFill,
    ObjectChunk,
    ObjectCursor,
    pack_block,
    pack_files,
    split_for_session,
    unpack_block,
    unpack_files,
)
from tetrys_nc.object_xfer import ObjectSession, run_object_client, run_object_server


def _free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _loopback(tmp_path: Path, objects: dict[str, bytes] | list[tuple[str, bytes]], **kw):
    session = ObjectSession()
    out = tmp_path / "recv"
    port = _free_udp_port()
    errors: list[BaseException] = []
    items = objects.items() if isinstance(objects, dict) else objects

    def server() -> None:
        try:
            run_object_server("127.0.0.1", port, session, **kw)
        except BaseException as exc:
            errors.append(exc)

    def client() -> None:
        try:
            run_object_client("127.0.0.1", port, out, active_bytes=kw.get("active_bytes", 1 << 20))
        except BaseException as exc:
            errors.append(exc)

    st = threading.Thread(target=server)
    st.start()
    time.sleep(0.05)
    ct = threading.Thread(target=client)
    ct.start()
    time.sleep(0.05)
    for name, blob in items:
        session.put(name, blob)
    session.close()
    ct.join(timeout=30)
    st.join(timeout=30)
    assert not errors, errors[0]
    return {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}


def test_object_control_packets_roundtrip():
    for packet in (
        ObjectOpen(9, 3, 12, "note.txt"),
        ObjectFin(9, 3, 12, "note.txt"),
    ):
        assert parse_packet(packet.pack()) == packet


def test_pack_unpack_two_objects_in_one_block():
    chunks = [
        ObjectChunk(1, 0, b"hello", "a.txt", 5),
        ObjectChunk(2, 0, b"world!!", "b.txt", 7),
    ]
    got = unpack_block(pack_block(256, chunks))
    assert [(c.obj_id, c.offset, c.data, c.name, c.size) for c in got] == [
        (1, 0, b"hello", "a.txt", 5),
        (2, 0, b"world!!", "b.txt", 7),
    ]


def test_block_fill_splits_large_object():
    cursor = ObjectCursor(1, "big.bin", b"x" * 80)
    fill, rest = BlockFill(64), BlockFill(80)
    assert fill.take(cursor) and not cursor.done
    assert rest.take(cursor) and cursor.done
    a, b = unpack_block(fill.packed()), unpack_block(rest.packed())
    assert a[0].name == "big.bin" and a[0].size == 80 and b[0].name == ""
    assert a[0].data + b[0].data == b"x" * 80


def test_session_put_after_close_raises():
    session = ObjectSession()
    session.put("a.bin", b"aa")
    session.close()
    with pytest.raises(RuntimeError):
        session.put("b.bin", b"bb")


def test_session_rejects_path_escape():
    with pytest.raises(ValueError):
        ObjectSession().put("..", b"x")


def test_object_mux_loopback_and_late_enqueue(tmp_path: Path):
    session = ObjectSession()
    out = tmp_path / "recv"
    port = _free_udp_port()
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_object_server(
                "127.0.0.1", port, session, symbol_size=256, block_k=32,
                initial_repair_pct=14, active_bytes=1 << 20, rate_mbit=200,
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
    got = {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}
    assert got == {**early, **late}


def test_object_mux_many_micros_and_two_large(tmp_path: Path):
    micros = {f"m{i:04d}.bin": bytes([i % 256]) * (40 + i % 80) for i in range(120)}
    larges = {"big0.bin": os.urandom(80_000), "big1.bin": os.urandom(120_000)}
    got = _loopback(
        tmp_path, {**micros, **larges},
        symbol_size=256, block_k=64, initial_repair_pct=14, active_bytes=2 << 20, rate_mbit=400,
    )
    assert got == {**micros, **larges}


def test_tree_pack_roundtrip():
    files = [(f"m{i}.bin", bytes([i]) * (10 + i)) for i in range(20)]
    assert unpack_files(pack_files(files)) == files


def test_split_packs_micros_keeps_larges():
    micros = [(f"m{i}.bin", b"x" * 50) for i in range(40)]
    big = ("big.bin", os.urandom(300_000))
    objects = split_for_session(micros + [big], small_max=256_000, pack_max=2000)
    names = [n for n, _ in objects]
    assert "big.bin" in names and any(n.startswith("__pack_") for n in names)
    restored: dict[str, bytes] = {}
    for name, data in objects:
        restored.update(unpack_files(data) if name.startswith("__pack_") else {name: data})
    assert restored == {n: d for n, d in micros} | {"big.bin": big[1]}


def test_mux_progress_is_group_plus_current():
    from tetrys_nc.object_xfer import mux_progress_lines

    total, current = mux_progress_lines(
        decoded=1500,
        expected_bytes=3000,
        finished=1,
        expected_files=3,
        current=("b.bin", 400, 1000, False),
        inst_bps=2 * 1048576,
    )
    assert total.startswith("total")
    assert "50.0%" in total
    assert "1/3 files" in total
    assert "2.0MiB/s" in total
    assert current.startswith("b.bin")
    assert "current" not in current
    assert "+1" not in current
    packed = mux_progress_lines(
        decoded=50,
        expected_bytes=100,
        finished=0,
        expected_files=1,
        current=("__pack_0", 50, 100, False),
    )
    assert packed[1].startswith("pack_0")
    later = mux_progress_lines(
        decoded=2000,
        expected_bytes=3000,
        finished=2,
        expected_files=3,
        current=("c.bin", 500, 1000, False),
    )
    assert "66.7%" in later[0]
    assert "2/3 files" in later[0]
    assert later[1].startswith("c.bin")


def test_current_progress_stays_on_oldest_incomplete(tmp_path: Path):
    from tetrys_nc.object_xfer import _Sink

    sink = _Sink(tmp_path)
    sink.open(1, "a.bin", 1000)
    sink.open(2, "b.bin", 1000)
    sink.written[1] = 400
    sink.written[2] = 50
    name, wrote, size, done = sink.current_progress()
    assert name == "a.bin"
    assert wrote == 400
    sink.written[2] = 900
    name, wrote, _, _ = sink.current_progress()
    assert name == "a.bin" and wrote == 400
    sink.written[1] = 300
    name, wrote, _, _ = sink.current_progress()
    assert name == "a.bin" and wrote == 400
    sink.finished.add(1)
    sink.written[1] = 1000
    name, wrote, _, _ = sink.current_progress()
    assert name == "b.bin" and wrote == 900


def test_split_pack_start_avoids_name_collision():
    a = split_for_session([("a.bin", b"x" * 10)], pack_max=100)
    b = split_for_session([("b.bin", b"y" * 10)], pack_max=100, pack_start=1)
    assert a[0][0] != b[0][0]


def test_object_mux_packed_micros_loopback(tmp_path: Path):
    micros = {f"m{i:04d}.bin": bytes([i % 256]) * (40 + i % 80) for i in range(80)}
    larges = {"big0.bin": os.urandom(90_000)}
    objects = split_for_session([*micros.items(), *larges.items()], small_max=256_000, pack_max=50_000)
    got = _loopback(
        tmp_path, objects,
        symbol_size=256, block_k=64, initial_repair_pct=14, active_bytes=2 << 20, rate_mbit=400,
    )
    assert got == {**micros, **larges}
