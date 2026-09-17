from __future__ import annotations

import os
import socket
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("raptorq")

from tetrys_nc.block_packets import VfsEntry, _pack_list_entries, parse_packet
from tetrys_nc.block_xfer import (
    _collect_tree_files,
    _safe_dest,
    run_block_client,
    run_block_server,
    run_block_upload_client,
    run_vfs_list,
    run_vfs_mkdir,
    run_vfs_unlink,
)
from tetrys_nc.fm_core import (
    LocalEntry,
    build_local_manifest,
    copy_local_to_remote,
    copy_remote_to_local,
    list_local,
)


def _free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _server_kwargs(**extra):
    kw = dict(
        symbol_size=256,
        block_k=64,
        initial_repair_pct=14,
        active_bytes=4 << 20,
        rate_mbit=400,
        skip_hash=True,
        allow_upload=True,
        once=False,
    )
    kw.update(extra)
    return kw


def _start_server(root: Path, port: int, **extra):
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_block_server("127.0.0.1", port, root, **_server_kwargs(**extra))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=server, daemon=True)
    thread.start()
    time.sleep(0.05)
    return thread, errors


def _wait_exists(path: Path, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise AssertionError(f"missing {path}")


def test_list_entries_split_across_datagrams():
    entries = [VfsEntry(f"f{i:04d}.bin", False, i, 1_700_000_000) for i in range(200)]
    wires = _pack_list_entries(3, entries)
    assert len(wires) > 1
    got: list[VfsEntry] = []
    for i, wire in enumerate(wires):
        packet = parse_packet(wire)
        assert packet.session_id == 3
        assert packet.seq == i
        assert packet.last == (i + 1 == len(wires))
        got.extend(packet.entries)
    assert [item.name for item in got] == [item.name for item in entries]


def test_loopback_vfs_list_mkdir_rm(tmp_path: Path):
    root = tmp_path / "srv"
    nested = root / "keep"
    nested.mkdir(parents=True)
    (root / "a.bin").write_bytes(b"aaa")
    (nested / "b.bin").write_bytes(b"bbbb")
    port = _free_udp_port()
    thread, errors = _start_server(root, port)

    listing = {item.name: item for item in run_vfs_list("127.0.0.1", port, "")}
    assert "a.bin" in listing and not listing["a.bin"].is_dir
    assert listing["a.bin"].size == 3
    assert "keep" in listing and listing["keep"].is_dir
    assert "keep/b.bin" not in listing
    sub = run_vfs_list("127.0.0.1", port, "keep")
    assert {item.name for item in sub} == {"b.bin"}

    run_vfs_mkdir("127.0.0.1", port, "inbox")
    assert (root / "inbox").is_dir()
    names = {item.name for item in run_vfs_list("127.0.0.1", port, "")}
    assert "inbox" in names

    with pytest.raises(OSError, match="exists"):
        run_vfs_mkdir("127.0.0.1", port, "inbox")
    with pytest.raises(FileNotFoundError):
        run_vfs_list("127.0.0.1", port, "missing")
    with pytest.raises(FileNotFoundError):
        run_vfs_list("127.0.0.1", port, "../etc")
    assert _safe_dest(root, "../etc") is None

    run_vfs_unlink("127.0.0.1", port, "a.bin")
    assert not (root / "a.bin").exists()
    with pytest.raises(OSError):
        run_vfs_unlink("127.0.0.1", port, "keep")
    run_vfs_unlink("127.0.0.1", port, "keep/b.bin")
    run_vfs_unlink("127.0.0.1", port, "keep")
    assert not (root / "keep").exists()
    assert not errors


def test_loopback_vfs_mkdir_rejected_when_disabled(tmp_path: Path):
    root = tmp_path / "srv"
    root.mkdir()
    port = _free_udp_port()
    _start_server(root, port, allow_upload=False)
    listing = run_vfs_list("127.0.0.1", port, "")
    assert listing == []
    with pytest.raises(OSError, match="uploads disabled"):
        run_vfs_mkdir("127.0.0.1", port, "inbox")
    assert not (root / "inbox").exists()


def test_loopback_vfs_copyin_copyout(tmp_path: Path):
    root = tmp_path / "srv"
    root.mkdir()
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    payload = os.urandom(2 * 64 * 256 + 11)
    src.write_bytes(payload)
    port = _free_udp_port()
    kw = _server_kwargs()
    client_kw = {k: v for k, v in kw.items() if k not in {"allow_upload", "once"}}
    _start_server(root, port)

    run_vfs_mkdir("127.0.0.1", port, "inbox")
    seen_up: list[str] = []
    run_block_upload_client(
        "127.0.0.1",
        port,
        src,
        remote="inbox/stored.bin",
        progress=seen_up.append,
        **client_kw,
    )
    _wait_exists(root / "inbox" / "stored.bin")
    names = {item.name for item in run_vfs_list("127.0.0.1", port, "inbox")}
    assert "stored.bin" in names
    assert any("100.0%" in line for line in seen_up)
    seen_down: list[str] = []
    run_block_client(
        "127.0.0.1",
        port,
        dst,
        remote="inbox/stored.bin",
        active_bytes=4 << 20,
        progress=seen_down.append,
    )
    assert dst.read_bytes() == payload
    assert any("100.0%" in line for line in seen_down)
    run_vfs_unlink("127.0.0.1", port, "inbox/stored.bin")
    assert "stored.bin" not in {
        item.name for item in run_vfs_list("127.0.0.1", port, "inbox")
    }


def test_collect_tree_files_recursive(tmp_path: Path):
    root = tmp_path / "tree"
    (root / "sub" / "deep").mkdir(parents=True)
    (root / "a.txt").write_bytes(b"a")
    (root / "sub" / "b.txt").write_bytes(b"bb")
    (root / "sub" / "deep" / "c.txt").write_bytes(b"ccc")
    (root / ".skip").write_bytes(b"x")
    got = dict(_collect_tree_files(root))
    assert got.keys() == {"a.txt", "sub/b.txt", "sub/deep/c.txt"}


def test_loopback_recursive_mux_download(tmp_path: Path):
    root = tmp_path / "srv"
    tree = root / "tree"
    (tree / "sub").mkdir(parents=True)
    blobs = {
        "a.txt": b"alpha",
        "sub/b.txt": b"bravo-bravo",
        "sub/c.bin": os.urandom(900),
    }
    for rel, data in blobs.items():
        path = tree / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    out = tmp_path / "out"
    port = _free_udp_port()
    _start_server(root, port)
    assert run_block_client("127.0.0.1", port, out, remote="tree") == 0
    assert {p.relative_to(out).as_posix(): p.read_bytes() for p in out.rglob("*") if p.is_file()} == blobs


def test_loopback_recursive_mux_upload(tmp_path: Path):
    root = tmp_path / "srv"
    root.mkdir()
    local = tmp_path / "local"
    (local / "nested").mkdir(parents=True)
    (local / "one.txt").write_bytes(b"one")
    (local / "nested" / "two.txt").write_bytes(b"two-two")
    port = _free_udp_port()
    kw = _server_kwargs()
    client_kw = {k: v for k, v in kw.items() if k not in {"allow_upload", "once"}}
    _start_server(root, port)
    assert (
        run_block_upload_client(
            "127.0.0.1", port, local, remote="up", **client_kw
        )
        == 0
    )
    assert (root / "up" / "one.txt").read_bytes() == b"one"
    assert (root / "up" / "nested" / "two.txt").read_bytes() == b"two-two"


def test_fm_recursive_local_and_remote_rm(tmp_path: Path):
    from tetrys_nc.fm_core import local_rm, remote_rm

    local = tmp_path / "tree"
    (local / "sub").mkdir(parents=True)
    (local / "a.txt").write_bytes(b"a")
    (local / "sub" / "b.txt").write_bytes(b"b")
    local_rm(local)
    assert not local.exists()

    root = tmp_path / "srv"
    nested = root / "drop" / "deep"
    nested.mkdir(parents=True)
    (root / "drop" / "x.bin").write_bytes(b"x")
    (nested / "y.bin").write_bytes(b"y")
    port = _free_udp_port()
    _start_server(root, port)
    remote_rm("127.0.0.1", port, "drop")
    assert not (root / "drop").exists()


def test_fm_core_local_manifest_and_copy(tmp_path: Path):
    root = tmp_path / "srv"
    root.mkdir()
    local = tmp_path / "local"
    (local / "dir" / "nested").mkdir(parents=True)
    (local / "plain.txt").write_bytes(b"p")
    (local / "dir" / "a.txt").write_bytes(b"aa")
    (local / "dir" / "nested" / "b.txt").write_bytes(b"bbb")
    entries = list_local(local)
    names = {item.name for item in entries}
    assert names == {"dir", "plain.txt"}
    assert any(isinstance(item, LocalEntry) and item.is_dir for item in entries)

    manifest = build_local_manifest(local, ["plain.txt", "dir"])
    assert {rel for rel, _ in manifest} == {
        "plain.txt",
        "dir/a.txt",
        "dir/nested/b.txt",
    }

    port = _free_udp_port()
    kw = _server_kwargs()
    client_kw = {k: v for k, v in kw.items() if k not in {"allow_upload", "once"}}
    _start_server(root, port)
    assert (
        copy_local_to_remote(
            "127.0.0.1",
            port,
            local,
            ["dir"],
            remote_dir="inbox",
            **client_kw,
        )
        == 0
    )
    assert (root / "inbox" / "dir" / "a.txt").read_bytes() == b"aa"
    assert (root / "inbox" / "dir" / "nested" / "b.txt").read_bytes() == b"bbb"

    out = tmp_path / "down"
    assert copy_remote_to_local("127.0.0.1", port, ["inbox/dir"], out) == 0
    assert (out / "dir" / "a.txt").read_bytes() == b"aa"
    assert (out / "dir" / "nested" / "b.txt").read_bytes() == b"bbb"


def test_loopback_vfs_nat_punch(tmp_path: Path):
    root = tmp_path / "srv"
    root.mkdir()
    (root / "a.bin").write_bytes(b"punch")
    srv_port = _free_udp_port()
    cli_port = _free_udp_port()
    _start_server(root, srv_port, punch_peer=("127.0.0.1", cli_port))
    names = {
        item.name
        for item in run_vfs_list(
            "127.0.0.1",
            srv_port,
            "",
            bind_port=cli_port,
            wait_punch=True,
        )
    }
    assert "a.bin" in names
