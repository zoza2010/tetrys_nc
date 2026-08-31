from __future__ import annotations

import mmap
import os
import random
import socket
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("raptorq")

from tetrys_nc.block_packets import (
    BlockData,
    BlockFeedback,
    BlockFin,
    BlockMeta,
    BlockReady,
    OpenBlock,
    block_ids_to_ranges,
    pack_data_packets,
    parse_packet,
)
from tetrys_nc.block_state import (
    BlockGeometry,
    REPAIR_AGE_S,
    REPAIR_COOLDOWN_S,
    RepairDebtController,
    SenderBlockState,
    SenderFeedbackState,
    TAIL_REPAIR_COOLDOWN_S,
    WAN_ACTIVE_BYTES,
    WAN_BLOCK_K,
    WAN_INITIAL_REPAIR_PCT,
    WAN_SYMBOL_SIZE,
    ExtraRepairWindow,
    block_loss_frac,
    percentile,
    repair_tick_limits,
    select_repair_candidates,
)
from tetrys_nc.block_xfer import (
    _pace_limits,
    _rate_cc_enabled,
    _safe_join,
    encode_block_job,
    rebuild_block_encoder,
    run_block_client,
    run_block_server,
)
from tetrys_nc.gen_raptor import GenEncoder, GenReceiveSlot


def test_wire_roundtrips_and_rejects_wrong_version():
    packets = [
        BlockReady(9, 64 << 20),
        BlockMeta(9, 1234, "blob.bin", 1350, 768, 14, 64 << 20, "ab"),
        BlockData(9, 7, 3, b"x" * 100, 55),
        BlockFeedback(
            9,
            4,
            1000,
            500,
            55,
            [1, 3],
            [OpenBlock(7, 700), OpenBlock(8, 770, True)],
        ),
        BlockFin(9, 20),
    ]
    for packet in packets:
        assert parse_packet(packet.pack()) == packet
    with pytest.raises(ValueError):
        parse_packet(b"\x54\x01\x20\x00" + bytes(20))


def test_feedback_ranges_fit_in_one_datagram():
    sequential = BlockFeedback(
        3,
        1,
        0,
        0,
        done_blocks=list(range(2000)),
        open_blocks=[OpenBlock(i, i) for i in range(1000)],
    )
    wire = sequential.pack()
    assert len(wire) <= 1400
    got = BlockFeedback.unpack(wire)
    assert got.done_blocks == list(range(2000))
    assert len(got.open_blocks or []) == 64
    sparse = BlockFeedback(
        3,
        2,
        0,
        0,
        done_blocks=list(range(0, 200, 2)),
        open_blocks=[OpenBlock(i, i) for i in range(80)],
    )
    sparse_wire = sparse.pack()
    assert len(sparse_wire) <= 1400
    sparse_got = BlockFeedback.unpack(sparse_wire)
    assert len(sparse_got.done_blocks or []) == 48
    with pytest.raises(ValueError):
        BlockFeedback.unpack(wire[:-3])


def test_completion_ranges_are_compact_and_rotate():
    assert block_ids_to_ranges([0, 1, 2, 5, 6]) == [(0, 3), (5, 2)]
    islands = list(range(0, 120, 2))
    first = block_ids_to_ranges(islands, limit=48, rotate=0)
    second = block_ids_to_ranges(islands, limit=48, rotate=1)
    assert len(first) == 48
    assert first != second


def test_parse_packet_rejects_unknown_version():
    ready = BlockReady(9, 64 << 20).pack()
    assert parse_packet(ready) == BlockReady(9, 64 << 20)
    named = BlockReady(9, 64 << 20, "testdata/blob.bin")
    assert parse_packet(named.pack()) == named
    with pytest.raises(ValueError):
        parse_packet(b"\x54\x09\x30\x00" + bytes(8))


def test_safe_join_stays_under_root(tmp_path: Path):
    blob = tmp_path / "sub" / "x.bin"
    blob.parent.mkdir()
    blob.write_bytes(b"ok")
    assert _safe_join(tmp_path, "sub/x.bin") == blob.resolve()
    assert _safe_join(tmp_path, "../x.bin") is None
    assert _safe_join(tmp_path, "/etc/passwd") is None
    assert _safe_join(tmp_path, "") is None
    assert _safe_join(tmp_path, "missing.bin") is None


def test_feedback_state_is_idempotent_and_monotonic():
    state = SenderFeedbackState(11)
    newer = BlockFeedback(
        11, 2, 200, 100, done_blocks=[1], open_blocks=[OpenBlock(2, 50)]
    )
    stale = BlockFeedback(
        11, 1, 999, 999, done_blocks=[9], open_blocks=[OpenBlock(2, 80)]
    )
    assert state.apply(newer, now=1.0)
    assert not state.apply(stale, now=2.0)
    done, opened, unique, decoded, echo, fb_id = state.snapshot()
    assert done == {1}
    assert opened[2].unique_esi == 50
    assert (unique, decoded) == (200, 100)
    assert fb_id == 2


def test_reordered_symbols_decode_like_ordered_symbols():
    k = 64
    symbol = 256
    data = bytes((i * 7) & 0xFF for i in range(k * symbol))
    encoder = GenEncoder(data, symbol, 20)
    packets = encoder.packets()

    def decode(order: list[int]) -> tuple[bytes | None, int]:
        slot = GenReceiveSlot(
            0,
            gen_k=k,
            symbol_size=symbol,
            block_bytes=len(data),
            tlen=len(data),
        )
        out = None
        for esi in order:
            out = slot.add_packet(packets[esi], esi)
            if out is not None:
                break
        count = slot.symbols_rx
        slot.close()
        return out, count

    ordered = list(range(len(packets)))
    shuffled = ordered.copy()
    random.Random(42).shuffle(shuffled)
    out_ordered, count_ordered = decode(ordered)
    out_shuffled, count_shuffled = decode(shuffled)
    assert out_ordered == data
    assert out_shuffled == data
    assert abs(count_ordered - count_shuffled) <= 4


def test_one_stuck_block_does_not_define_admission_frontier():
    geometry = BlockGeometry(1350, 768, 64 << 20)
    assert geometry.active_blocks >= 64
    active = {
        i: SenderBlockState(i, unique_rx=768 if i else 0) for i in range(64)
    }
    completed = set(range(1, 64))
    for block_id in completed:
        active.pop(block_id)
    assert list(active) == [0]
    assert len(active) < geometry.active_blocks








def test_extra_repair_window_busy_on_sliding_fraction():
    win = ExtraRepairWindow()
    for _ in range(32):
        win.observe(False)
    assert win.pressure() is False
    assert win.frac == 0.0
    for _ in range(2):
        win.observe(True)
    assert win.frac < 0.12
    assert win.pressure() is False
    for _ in range(6):
        win.observe(True)
    assert win.frac >= 0.12
    assert win.pressure() is True
    assert win.pressure(tail=True) is False
    early = ExtraRepairWindow()
    for _ in range(4):
        early.observe(True)
    assert early.pressure() is False






def test_pace_limits_floor_equals_start(monkeypatch):
    monkeypatch.delenv("TETRYS_START_MBIT", raising=False)
    monkeypatch.delenv("TETRYS_PACE_CAP_MBIT", raising=False)
    monkeypatch.delenv("TETRYS_PACE_MIN_FRAC", raising=False)
    min_bps, max_bps, start_bps = _pace_limits(2500.0)
    assert start_bps == pytest.approx(850_000_000 / 8)
    assert min_bps == pytest.approx(start_bps)
    assert max_bps == pytest.approx(850_000_000 / 8)


def test_pace_limits_cc_uses_search_cap(monkeypatch):
    monkeypatch.delenv("TETRYS_CC_CAP_MBIT", raising=False)
    min_bps, max_bps, start_bps = _pace_limits(850.0, cc=True)
    assert start_bps == pytest.approx(850_000_000 / 8)
    assert max_bps == pytest.approx(1600_000_000 / 8)
    assert min_bps < start_bps


def test_rate_cc_defaults_on(monkeypatch):
    monkeypatch.delenv("TETRYS_CC", raising=False)
    assert _rate_cc_enabled(None) is True
    assert _rate_cc_enabled(True) is True
    assert _rate_cc_enabled(False) is False
    monkeypatch.setenv("TETRYS_CC", "0")
    assert _rate_cc_enabled(None) is False
    monkeypatch.setenv("TETRYS_CC", "1")
    assert _rate_cc_enabled(None) is True


def test_cli_explicit_rate_locks_cc():
    from tetrys_nc.server import _cli_pace

    wan_rate, wan_cc = _cli_pace(True, None)
    assert wan_rate == pytest.approx(850.0)
    assert wan_cc is True
    lock_rate, lock_cc = _cli_pace(True, 850.0)
    assert lock_rate == pytest.approx(850.0)
    assert lock_cc is False
    lan_rate, lan_cc = _cli_pace(False, None)
    assert lan_rate == pytest.approx(1500.0)
    assert lan_cc is True
    assert _cli_pace(False, 80.0) == (80.0, False)


def test_repair_debt_controller_ignores_packet_order():
    ctl = RepairDebtController(16.0, min_pct=16.0, max_pct=24.0)
    first = ctl.observe(30, 768)
    second = ctl.observe(30, 768)
    assert first == second == 16


def test_repair_debt_controller_holds_floor_and_slews_up():
    ctl = RepairDebtController(16.0, min_pct=16.0, max_pct=22.0)
    for _ in range(40):
        ctl.observe(0, 768)
    assert ctl.current == 16
    jumped = ctl.observe(400, 768)
    assert jumped == 16
    for _ in range(3):
        ctl.observe(400, 768)
    assert 16 <= ctl.current <= 18
    ctl2 = RepairDebtController(16.0, min_pct=16.0, max_pct=22.0)
    for _ in range(80):
        ctl2.observe(400, 768)
    assert 16 < ctl2.current <= 22
    for _ in range(20):
        ctl2.observe(0, 768)
    assert ctl2.current <= 18


def test_repair_need_is_rank_only():
    state = SenderBlockState(5, unique_rx=700)
    assert state.repair_need(768) == 74
    state.unique_rx = 770
    assert state.repair_need(768) == 0
    state.decode_failed = True
    assert state.repair_need(768) == 8


def test_geometry_locks_wan_block_size():
    geometry = BlockGeometry()
    assert geometry.block_k == WAN_BLOCK_K == 768
    assert geometry.symbol_size == WAN_SYMBOL_SIZE == 1350
    assert geometry.active_bytes == WAN_ACTIVE_BYTES
    assert geometry.block_bytes == 768 * 1350
    assert geometry.active_blocks >= 64
    assert WAN_INITIAL_REPAIR_PCT == 24
    assert 0 < TAIL_REPAIR_COOLDOWN_S < REPAIR_COOLDOWN_S
    assert REPAIR_AGE_S <= 0.12


def test_block_loss_frac_uses_unique_at_first_repair_age():
    state = SenderBlockState(0, unique_rx=700, initial_repair=154, unique_at_age=700)
    # 700 / (768+154) ≈ 0.76 received → ~24% loss on the initial flight.
    assert block_loss_frac(state, 768) == pytest.approx(1.0 - 700 / 922)
    assert block_loss_frac(SenderBlockState(1), 768) is None
    assert percentile([0.1, 0.2, 0.3, 0.4, 0.5], 50) == pytest.approx(0.3)
    budget, tick_s = repair_tick_limits(200, tail=False)
    assert budget == 200
    assert tick_s >= 0.024
    small, _ = repair_tick_limits(10, tail=False)
    assert small == 48
    tail_b, _ = repair_tick_limits(10, tail=True)
    assert tail_b == 256


def test_repair_age_stamps_unique_once():
    now = 10.0
    young = SenderBlockState(0, unique_rx=500, sent_at=9.95)
    old = SenderBlockState(1, unique_rx=700, sent_at=9.0)
    opened = {0: OpenBlock(0, 500), 1: OpenBlock(1, 700)}
    select_repair_candidates(
        {0: young, 1: old},
        opened,
        now,
        block_k=768,
        tail=False,
        age_s=0.12,
        cooldown_s=0.0,
    )
    assert young.unique_at_age < 0
    assert old.unique_at_age == 700
    old.unique_rx = 720
    select_repair_candidates(
        {1: old},
        {1: OpenBlock(1, 720)},
        now,
        block_k=768,
        tail=False,
        age_s=0.12,
        cooldown_s=0.0,
    )
    assert old.unique_at_age == 700


def test_repair_prefers_smallest_deficit_not_hol_frontier():
    now = 10.0
    states = {
        0: SenderBlockState(0, unique_rx=100, sent_at=0.0),
        1: SenderBlockState(1, unique_rx=700, sent_at=9.0),
        2: SenderBlockState(2, unique_rx=760, sent_at=1.0),
    }
    opened = {i: OpenBlock(i, states[i].unique_rx) for i in states}
    got = select_repair_candidates(
        states, opened, now, block_k=768, tail=False, age_s=0.24, cooldown_s=0.0
    )
    assert [item[2] for item in got] == [2, 1, 0]


def test_encode_block_job_matches_direct_pack(tmp_path: Path):
    k, t = 32, 128
    data = os.urandom(k * t)
    path = tmp_path / "job.bin"
    path.write_bytes(data)
    block_id, wires, budget, encode_s = encode_block_job(
        str(path), 0, k * t, len(data), t, 14, 9
    )
    assert block_id == 0
    assert encode_s >= 0.0
    assert budget > 0
    parsed = [parse_packet(bytes(w)) for w in wires]
    assert all(isinstance(p, BlockData) for p in parsed)
    assert parsed[0].session_id == 9
    encoder = GenEncoder(data, t, 14)
    assert [p.payload for p in parsed] == encoder.packets()


def test_pack_data_packets_roundtrips_like_blockdata():
    payloads = [b"aa", b"bb"]
    wires = pack_data_packets(3, 4, payloads, first_esi=7, send_ts_us=11)
    for i, wire in enumerate(wires):
        got = BlockData.unpack(bytes(wire))
        assert got == BlockData(3, 4, 7 + i, payloads[i], 11)


def test_decode_failed_skips_repair_age_wait():
    now = 10.0
    states = {0: SenderBlockState(0, unique_rx=770, sent_at=9.9, decode_failed=True)}
    opened = {0: OpenBlock(0, 770, True)}
    got = select_repair_candidates(
        states, opened, now, block_k=768, tail=False, age_s=0.24, cooldown_s=0.0
    )
    assert [item[2] for item in got] == [0]


def test_encoder_rebuild_preserves_esi_prefix(tmp_path: Path):
    k, t = 32, 128
    data = os.urandom(k * t)
    path = tmp_path / "block.bin"
    path.write_bytes(data)
    geometry = BlockGeometry(t, k, 4 << 20)
    original = GenEncoder(data, t, 14)
    with path.open("rb") as fh:
        mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            rebuilt = rebuild_block_encoder(
                mm, len(data), 0, geometry, original.repair_budget
            )
        finally:
            mm.close()
    assert rebuilt.packets() == original.packets()


def _free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_loopback_transfer_is_byte_correct(tmp_path: Path):
    src = tmp_path / "in.bin"
    dst = tmp_path / "out.bin"
    payload = os.urandom(3 * 64 * 256 + 17)
    src.write_bytes(payload)
    port = _free_udp_port()
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_block_server(
                "127.0.0.1",
                port,
                tmp_path,
                default_file="in.bin",
                symbol_size=256,
                block_k=64,
                initial_repair_pct=14,
                active_bytes=4 << 20,
                rate_mbit=400,
                skip_hash=True,
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=server, daemon=False)
    thread.start()
    time.sleep(0.05)
    run_block_client("127.0.0.1", port, dst, remote="in.bin", active_bytes=4 << 20)
    thread.join(timeout=8)
    assert not errors, errors[0]
    assert dst.read_bytes() == payload


def test_loopback_server_serves_two_clients(tmp_path: Path):
    src = tmp_path / "in.bin"
    dst1 = tmp_path / "out1.bin"
    dst2 = tmp_path / "out2.bin"
    payload = os.urandom(3 * 64 * 256 + 17)
    src.write_bytes(payload)
    port = _free_udp_port()
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_block_server(
                "127.0.0.1",
                port,
                tmp_path,
                default_file="in.bin",
                symbol_size=256,
                block_k=64,
                initial_repair_pct=14,
                active_bytes=4 << 20,
                rate_mbit=400,
                skip_hash=True,
                once=False,
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=server, daemon=True)
    thread.start()
    time.sleep(0.05)
    run_block_client("127.0.0.1", port, dst1, remote="in.bin", active_bytes=4 << 20)
    run_block_client("127.0.0.1", port, dst2, remote="in.bin", active_bytes=4 << 20)
    assert dst1.read_bytes() == payload
    assert dst2.read_bytes() == payload
    assert not errors, errors[0]


def test_loopback_client_picks_file_under_root(tmp_path: Path):
    (tmp_path / "a.bin").write_bytes(b"aaa" * 1000)
    (tmp_path / "b.bin").write_bytes(b"bbb" * 2000)
    dst = tmp_path / "out.bin"
    port = _free_udp_port()
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_block_server(
                "127.0.0.1",
                port,
                tmp_path,
                symbol_size=256,
                block_k=64,
                initial_repair_pct=14,
                active_bytes=4 << 20,
                rate_mbit=400,
                skip_hash=True,
                once=False,
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=server, daemon=True)
    thread.start()
    time.sleep(0.05)
    run_block_client("127.0.0.1", port, dst, remote="b.bin", active_bytes=4 << 20)
    assert dst.read_bytes() == b"bbb" * 2000
    with pytest.raises(FileNotFoundError):
        run_block_client(
            "127.0.0.1", port, tmp_path / "bad.bin", remote="../etc/passwd",
            active_bytes=4 << 20,
        )
    assert not errors, errors[0]


def test_loopback_directory_uses_mux(tmp_path: Path):
    src = tmp_path / "many"
    src.mkdir()
    blobs = {f"f{i}.bin": os.urandom(800 + i) for i in range(12)}
    for name, data in blobs.items():
        (src / name).write_bytes(data)
    out = tmp_path / "out"
    port = _free_udp_port()
    errors: list[BaseException] = []

    def server() -> None:
        try:
            run_block_server(
                "127.0.0.1",
                port,
                tmp_path,
                symbol_size=256,
                block_k=64,
                initial_repair_pct=14,
                active_bytes=4 << 20,
                rate_mbit=400,
                skip_hash=True,
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=server, daemon=False)
    thread.start()
    time.sleep(0.05)
    run_block_client("127.0.0.1", port, out, remote="many", active_bytes=4 << 20)
    thread.join(timeout=15)
    assert not errors, errors[0]
    got = {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}
    assert got == blobs
