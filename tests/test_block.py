from __future__ import annotations

import mmap
import os
import random
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

pytest.importorskip("raptorq")

from tetrys_nc.block_packets import (
    BlockData,
    BlockFeedback,
    BlockFin,
    BlockMeta,
    BlockReady,
    MAX_GHOST_OPEN,
    MAX_OPEN_BLOCKS,
    OpenBlock,
    block_ids_to_ranges,
    merge_open_feedback,
    pack_data_packets,
    parse_packet,
    stamp_data_wires,
)
from tetrys_nc.block_state import (
    BlockGeometry,
    FLIGHT_AGE_BUCKETS,
    REPAIR_AGE_S,
    REPAIR_COOLDOWN_S,
    SenderBlockState,
    SenderFeedbackState,
    TAIL_REPAIR_COOLDOWN_S,
    WAN_ACTIVE_BYTES,
    WAN_BLOCK_K,
    WAN_INITIAL_REPAIR_PCT,
    WAN_SYMBOL_SIZE,
    ExtraRepairWindow,
    block_quartile,
    fmt_quartile_counts,
    fmt_quartile_pcts,
    resolve_fec_cli,
    block_loss_frac,
    dir_lightweight,
    fec_pct_for_loss,
    first_flight_unique,
    ghost_flight_ready,
    late_unique,
    make_block_sample,
    make_fec_controller,
    needed_repair_pct,
    percentile,
    repair_tick_limits,
    select_repair_candidates,
)
from tetrys_nc.block_xfer import (
    BlockSender,
    _pace_limits,
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
    assert len(got.open_blocks or []) == MAX_OPEN_BLOCKS
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


def test_feedback_keeps_ghost_open_on_completed_block():
    state = SenderFeedbackState(3)
    assert state.apply(
        BlockFeedback(
            3,
            1,
            100,
            50,
            done_blocks=[7],
            open_blocks=[OpenBlock(7, 920), OpenBlock(8, 400)],
        ),
        now=1.0,
    )
    done, opened, *_rest = state.snapshot()
    assert done == {7}
    assert opened[7].unique_esi == 920
    assert opened[8].unique_esi == 400
    assert state.apply(
        BlockFeedback(3, 2, 120, 80, done_blocks=[7], open_blocks=[OpenBlock(8, 410)]),
        now=1.2,
    )
    _done, opened, *_rest = state.snapshot()
    assert 7 not in opened
    assert opened[8].unique_esi == 410


def test_feedback_open_rx_does_not_grow_unbounded():
    state = SenderFeedbackState(3)
    for i in range(1, 400):
        assert state.apply(
            BlockFeedback(
                3,
                i,
                i,
                0,
                open_blocks=[OpenBlock(i, 10), OpenBlock(i + 1, 11)],
            ),
            now=float(i),
        )
    _done, opened, *_rest = state.snapshot()
    assert len(opened) <= MAX_OPEN_BLOCKS * 2


def test_ghost_flight_ready_waits_full_repair_age():
    assert FLIGHT_AGE_BUCKETS == 6
    assert ghost_flight_ready(None) is False
    assert ghost_flight_ready(OpenBlock(1, 900, age_bucket=5)) is False
    assert ghost_flight_ready(OpenBlock(1, 900, age_bucket=6)) is True


def test_merge_open_feedback_keeps_ghosts_when_window_is_full():
    incomplete = [OpenBlock(i, 400) for i in range(64)]
    ghosts = [OpenBlock(1000 + i, 900, age_bucket=6) for i in range(12)]
    opened = merge_open_feedback(incomplete, ghosts)
    ghost_ids = {item.block_id for item in opened if item.block_id >= 1000}
    assert ghost_ids == {1000 + i for i in range(12)}
    assert len(opened) == 64 + 12
    assert MAX_OPEN_BLOCKS == 80
    assert MAX_GHOST_OPEN == 16


def test_merge_open_feedback_keeps_newest_when_incomplete_overflows():
    incomplete = [OpenBlock(i, 400) for i in range(80)]
    ghosts = [OpenBlock(1000 + i, 900, age_bucket=6) for i in range(16)]
    opened = merge_open_feedback(incomplete, ghosts)
    ids = {item.block_id for item in opened}
    assert {1000 + i for i in range(16)} <= ids
    assert {0, 1, 2} <= ids
    assert {77, 78, 79} <= ids
    assert 70 not in ids
    assert len(opened) == MAX_OPEN_BLOCKS


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


def test_receive_slot_keeps_counting_after_decode():
    k = 32
    symbol = 64
    data = bytes((i * 3) & 0xFF for i in range(k * symbol))
    encoder = GenEncoder(data, symbol, 24)
    packets = encoder.packets()
    slot = GenReceiveSlot(
        0, gen_k=k, symbol_size=symbol, block_bytes=len(data), tlen=len(data)
    )
    out = None
    for esi, pkt in enumerate(packets):
        got = slot.add_packet(pkt, esi)
        if got is not None:
            out = got
            decoded_at = slot.symbols_rx
            break
    assert out == data
    assert decoded_at < len(packets)
    for esi in range(decoded_at, len(packets)):
        slot.add_packet(packets[esi], esi)
    assert slot.symbols_rx == len(packets)
    assert slot.decode_failed is False
    slot.close()


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






def test_pace_limits_locked_rate_is_not_clipped_to_850(monkeypatch):
    monkeypatch.delenv("TETRYS_START_MBIT", raising=False)
    monkeypatch.delenv("TETRYS_PACE_CAP_MBIT", raising=False)
    min_bps, max_bps, start_bps = _pace_limits(900.0)
    assert start_bps == pytest.approx(900_000_000 / 8)
    assert min_bps == pytest.approx(start_bps)
    assert max_bps == pytest.approx(900_000_000 / 8)
    hi_min, hi_max, hi_start = _pace_limits(2500.0)
    assert hi_start == pytest.approx(2500_000_000 / 8)
    assert hi_min == hi_max == hi_start


def test_pace_limits_env_cap_still_clips(monkeypatch):
    monkeypatch.setenv("TETRYS_PACE_CAP_MBIT", "850")
    min_bps, max_bps, start_bps = _pace_limits(2500.0)
    assert max_bps == pytest.approx(850_000_000 / 8)
    assert start_bps == pytest.approx(850_000_000 / 8)
    assert min_bps == pytest.approx(start_bps)


def test_pace_limits_cc_uses_search_cap(monkeypatch):
    monkeypatch.delenv("TETRYS_CC_CAP_MBIT", raising=False)
    monkeypatch.delenv("TETRYS_START_MBIT", raising=False)
    min_bps, max_bps, start_bps = _pace_limits(850.0, cc=True)
    assert start_bps == pytest.approx(8_000_000 / 8)
    assert max_bps == pytest.approx(10000_000_000 / 8)
    assert min_bps == pytest.approx(8_000_000 / 8)
    assert start_bps == min_bps


def test_pace_limits_cc_cap_is_above_one_gigabit(monkeypatch):
    monkeypatch.delenv("TETRYS_CC_CAP_MBIT", raising=False)
    monkeypatch.delenv("TETRYS_START_MBIT", raising=False)
    min_bps, max_bps, start_bps = _pace_limits(850.0, cc=True)
    assert start_bps == pytest.approx(8_000_000 / 8)
    assert max_bps == pytest.approx(10000_000_000 / 8)
    assert max_bps * 8 / 1e6 > 1000.0
    assert min_bps == pytest.approx(8_000_000 / 8)
    monkeypatch.setenv("TETRYS_CC_CAP_MBIT", "850")
    _, capped, _ = _pace_limits(850.0, cc=True)
    assert capped * 8 / 1e6 >= 2000.0


def test_pace_limits_cc_start_env_is_optional(monkeypatch):
    monkeypatch.setenv("TETRYS_START_MBIT", "850")
    min_bps, max_bps, start_bps = _pace_limits(0.0, cc=True)
    assert start_bps == pytest.approx(850_000_000 / 8)
    assert min_bps == pytest.approx(8_000_000 / 8)
    assert max_bps > start_bps


def test_feedback_client_lost_after_silence():
    st = SenderFeedbackState(1)
    assert st.client_lost(10.0, 9.5) is False
    assert st.client_lost(18.0, 9.5) is True
    st.apply(BlockFeedback(1, 1, 1000, 500), now=10.0)
    assert st.client_lost(11.5, 9.5) is False
    assert st.client_lost(12.1, 9.5) is True


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
    hol_b, hol_s = repair_tick_limits(400, tail=False, window_full=True)
    assert hol_b >= 256
    assert hol_s >= 0.10


def test_window_full_repair_prefers_oldest_hol():
    """Spain HOL: smallest-need first spent the drip on almost-done blocks."""
    now = 10.0
    young = SenderBlockState(1, unique_rx=760, sent_at=9.7)
    old = SenderBlockState(0, unique_rx=200, sent_at=8.0)
    opened = {0: OpenBlock(0, 200), 1: OpenBlock(1, 760)}
    oldest = select_repair_candidates(
        {0: old, 1: young},
        opened,
        now,
        block_k=768,
        tail=False,
        age_s=0.12,
        cooldown_s=0.0,
        prefer_oldest=True,
    )
    assert oldest[0][2] == 0
    nearest = select_repair_candidates(
        {0: old, 1: young},
        opened,
        now,
        block_k=768,
        tail=False,
        age_s=0.12,
        cooldown_s=0.0,
        prefer_oldest=False,
    )
    assert nearest[0][2] == 1


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
    assert old.first_deficit == 768 + 2 - 700
    silent = SenderBlockState(2, unique_rx=0, sent_at=9.0)
    select_repair_candidates(
        {2: silent},
        {2: OpenBlock(2, 0)},
        now,
        block_k=768,
        tail=False,
        age_s=0.12,
        cooldown_s=0.0,
    )
    assert silent.unique_at_age < 0
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


def test_flush_keeps_first_flight_unique(tmp_path: Path):
    geo = BlockGeometry(symbol_size=32, block_k=8, active_bytes=32 * 8 * 4)
    blob = tmp_path / "blob.bin"
    blob.write_bytes(os.urandom(geo.block_bytes * 2))
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        sender = BlockSender(
            sock,
            ("127.0.0.1", 9),
            1,
            blob,
            geo,
            initial_repair_pct=24,
            min_bps=1e6,
            max_bps=1e6,
            start_bps=1e6,
            cc_on=False,
            encode_pool=pool,
            prefetch_depth=1,
        )
        state = SenderBlockState(
            0,
            unique_rx=12,
            initial_repair=2,
            repair_emitted=10,
            sent_at=time.monotonic() - 1.0,
            unique_at_age=5,
            first_deficit=5,
            repair_rounds=1,
        )
        sender._wait_flight[0] = state
        opened = {0: OpenBlock(0, 12, age_bucket=FLIGHT_AGE_BUCKETS)}
        sender._flush_flight_samples(opened, tail=False)
        assert state.unique_at_age == 5
        assert 0 not in sender._wait_flight
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        sock.close()


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


def test_stamp_data_wires_overwrites_live_fec():
    wires = pack_data_packets(3, 4, [b"aa"], send_ts_us=11)
    stamp_data_wires(wires, 99, fec_pct=32)
    got = BlockData.unpack(bytes(wires[0]))
    assert got.send_ts_us == 99
    assert got.fec_pct == 32


def test_decode_failed_skips_repair_age_wait():
    now = 10.0
    states = {0: SenderBlockState(0, unique_rx=770, sent_at=9.9, decode_failed=True)}
    opened = {0: OpenBlock(0, 770, True)}
    got = select_repair_candidates(
        states, opened, now, block_k=768, tail=False, age_s=0.24, cooldown_s=0.0
    )
    assert [item[2] for item in got] == [0]


def test_tail_drips_repair_when_unique_already_at_rank():
    """Spain CC lock-921: unique 770, no DONE, rpr=0 until the process was killed."""
    now = 10.0
    state = SenderBlockState(2070, unique_rx=770, sent_at=1.0)
    opened = {2070: OpenBlock(2070, 770)}
    quiet = select_repair_candidates(
        {2070: state},
        opened,
        now,
        block_k=768,
        tail=False,
        age_s=0.12,
        cooldown_s=0.0,
    )
    assert quiet == []
    got = select_repair_candidates(
        {2070: state},
        opened,
        now,
        block_k=768,
        tail=True,
        age_s=0.12,
        cooldown_s=0.0,
    )
    assert got and got[0][2] == 2070
    assert got[0][0] >= 8


def test_tail_does_not_drip_a_full_in_flight_window():
    """All source admitted is `tail`, but 32 in-flight blocks must not storm."""
    now = 10.0
    states = {i: SenderBlockState(i, unique_rx=770, sent_at=1.0) for i in range(32)}
    opened = {i: OpenBlock(i, 770) for i in states}
    got = select_repair_candidates(
        states,
        opened,
        now,
        block_k=768,
        tail=True,
        age_s=0.12,
        cooldown_s=0.0,
    )
    assert got == []


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


def test_server_stops_when_client_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tetrys_nc.block_state.CLIENT_NEVER_S", 0.35)
    monkeypatch.setattr("tetrys_nc.block_state.CLIENT_GONE_S", 0.25)
    src = tmp_path / "in.bin"
    src.write_bytes(os.urandom(4 * 1024 * 1024))
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

    thread = threading.Thread(target=server, daemon=True)
    thread.start()
    time.sleep(0.05)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(BlockReady(7, 4 << 20, "in.bin").pack(), ("127.0.0.1", port))
    time.sleep(0.05)
    sock.close()
    thread.join(timeout=3.0)
    assert not errors, errors[0]
    assert not thread.is_alive()


def _fec_state(
    unique_at_age: int,
    *,
    k: int = 768,
    initial_repair: int = 184,
    extra: int = 0,
    rounds: int = 0,
    unique_rx: int | None = None,
    decode_failed: bool = False,
) -> SenderBlockState:
    got = unique_at_age if unique_rx is None else unique_rx
    return SenderBlockState(
        0,
        unique_rx=got,
        initial_repair=initial_repair,
        repair_emitted=initial_repair + extra,
        unique_at_age=unique_at_age,
        first_deficit=max(0, k + 2 - unique_at_age) if unique_at_age >= 0 else -1,
        repair_rounds=rounds,
        decode_failed=decode_failed,
    )


def test_late_unique_is_reorder_not_loss():
    state = _fec_state(700, unique_rx=740, initial_repair=184)
    assert late_unique(state) == 40
    assert block_loss_frac(state, 768) == pytest.approx(1.0 - 700 / 952)
    sample = make_block_sample(state, 768, tail=False)
    assert sample.late_unique == 40
    assert sample.first_flight_loss == pytest.approx(1.0 - 700 / 952)


def test_openblock_carries_unique_at_flight():
    item = OpenBlock(7, 700, unique_at_flight=680)
    got = parse_packet(
        BlockFeedback(9, 1, 0, 0, open_blocks=[item]).pack()
    )
    assert got.open_blocks[0].unique_at_flight == 680
    assert got.open_blocks[0].unique_esi == 700
    empty = parse_packet(
        BlockFeedback(9, 2, 0, 0, open_blocks=[OpenBlock(8, 10)]).pack()
    )
    assert empty.open_blocks[0].unique_at_flight == -1


def test_block_loss_frac_prefers_receiver_flight():
    state = SenderBlockState(
        0, unique_rx=900, initial_repair=184, unique_at_age=700
    )
    state.rx_at_flight = 850
    assert first_flight_unique(state) == 850
    assert block_loss_frac(state, 768) == pytest.approx(1.0 - 850 / 952)
    assert late_unique(state) == 50


def test_slot_freezes_first_flight_not_dir_esi():
    slot = GenReceiveSlot(0, gen_k=8, symbol_size=32, block_bytes=256, tlen=256)
    slot._decoded = b"done"
    slot.arm_flight_limit(10)
    for esi in range(10):
        slot.add_packet(b"x" * 32, esi)
    slot.add_packet(b"x" * 32, 15)
    assert slot.unique_flight == 10
    assert slot.symbols_rx == 11
    slot.maybe_freeze_flight(0.05, 0.12)
    assert slot.unique_at_flight < 0
    slot.maybe_freeze_flight(0.12, 0.12)
    assert slot.unique_at_flight == 10


def test_coverable_path_loss_ignores_hol():
    ctl = make_fec_controller(24)
    ctl.path_loss.extend([0.10, 0.13, 0.87, 0.12, 0.91, 0.11])
    assert (ctl.path_loss_q(95) or 0.0) > 0.8
    assert (ctl.path_loss_q(50, coverable=True) or 0.0) < 0.15
    assert ctl.hol_frac() == pytest.approx(2.0 / 6.0)


def test_block_sample_skips_tail_and_incomplete():
    incomplete = _fec_state(-1, unique_rx=400)
    incomplete.unique_at_age = -1
    incomplete.first_deficit = -1
    assert make_block_sample(incomplete, 768, tail=False).train is False
    closed = _fec_state(920)
    assert make_block_sample(closed, 768, tail=True).train is False
    assert make_block_sample(closed, 768, tail=False).train is True
    early_close = _fec_state(-1, unique_rx=770, extra=0)
    early_close.unique_at_age = -1
    early_close.first_deficit = -1
    assert make_block_sample(early_close, 768, tail=False).train is True
    blackout = _fec_state(0)
    assert make_block_sample(blackout, 768, tail=False).train is False


def test_dir_light_round_is_not_cc_pressure():
    light = make_block_sample(
        _fec_state(700, extra=80, rounds=1), 768, tail=False
    )
    assert dir_lightweight(light) is True
    assert light.dir_pressure() is False
    storm = make_block_sample(
        _fec_state(700, extra=400, rounds=3), 768, tail=False
    )
    assert storm.dir_pressure() is True
    failed = make_block_sample(
        _fec_state(770, extra=20, rounds=1, decode_failed=True), 768, tail=False
    )
    assert failed.dir_pressure() is True


def test_fixed_fec_keeps_requested_percent():
    ctl = make_fec_controller(14)
    assert ctl.current == 14
    dirty = make_block_sample(_fec_state(500, extra=400, rounds=4), 768, tail=False)
    for _ in range(40):
        ctl.observe_block(dirty)
    assert ctl.current == 14


def test_fixed_fec_still_records_first_flight_for_cc():
    """BlastCc classifies dropper vs HOL from path_loss_q, even when FEC is static."""
    ctl = make_fec_controller(8)
    clean = make_block_sample(_fec_state(940), 768, tail=False)
    for _ in range(8):
        ctl.observe_block(clean)
    assert ctl.current == 8
    p50 = ctl.path_loss_q(50)
    assert p50 is not None
    assert p50 < 0.05


def test_needed_repair_uses_first_flight_not_decode_rank():
    lucky = _fec_state(770, initial_repair=184, extra=0)
    assert needed_repair_pct(lucky, 768) == pytest.approx(100.0 * (952 - 770) / 768)
    full = _fec_state(950, initial_repair=184, extra=0)
    assert needed_repair_pct(full, 768) < 1.0
    censored = _fec_state(-1, unique_rx=770, extra=0)
    censored.unique_at_age = -1
    assert needed_repair_pct(censored, 768) == 0.0


def test_gen_overhead_locks_fec_omit_is_static_pad():
    assert resolve_fec_cli(24) == ("fixed", 24)
    assert resolve_fec_cli(8) == ("fixed", 8)
    assert resolve_fec_cli(None) == ("fixed", 8)
    locked = make_fec_controller(24)
    assert locked.current == 24
    for _ in range(40):
        locked.observe_block(make_block_sample(_fec_state(940), 768, tail=False))
    assert locked.current == 24


def test_select_repair_uses_dir_pad():
    now = 10.0
    state = SenderBlockState(1, unique_rx=700, sent_at=9.0)
    opened = {1: OpenBlock(1, 700)}
    with_pad = select_repair_candidates(
        {1: state},
        opened,
        now,
        block_k=768,
        tail=False,
        age_s=0.12,
        cooldown_s=0.0,
        dir_pad_fn=lambda deficit: 20,
    )
    assert with_pad[0][0] == state.repair_need(768, pad=20)


def test_dir_margin_is_zero_on_clean_and_capped_on_loss():
    ctl = make_fec_controller(24)
    for _ in range(10):
        ctl.observe_block(make_block_sample(_fec_state(940), 768, tail=False))
    assert ctl.dir_margin(40, 768) == 0
    lossy = make_fec_controller(24)
    dirty = _fec_state(620, initial_repair=184, extra=400, rounds=2)
    for _ in range(10):
        lossy.observe_block(make_block_sample(dirty, 768, tail=False))
    pad = lossy.dir_margin(40, 768, tick_left=10)
    assert 0 < pad <= 10
    tail_pad = lossy.dir_margin(400, 768, tail=True)
    assert tail_pad <= 48


def test_fec_pct_for_loss_maps_packet_drop_to_repair():
    assert fec_pct_for_loss(0.0) == pytest.approx(0.0)
    assert fec_pct_for_loss(0.10) == pytest.approx(100.0 / 9.0)
    assert fec_pct_for_loss(0.20) == pytest.approx(25.0)
    assert fec_pct_for_loss(0.2424) == pytest.approx(32.0, rel=0.03)


def test_first_close_still_records_path_loss():
    k = 768
    r0 = 246
    unique = int((k + r0) * 0.92)
    sample = make_block_sample(
        _fec_state(unique, extra=0, initial_repair=r0), k, tail=False
    )
    assert sample.extra_symbols == 0
    assert sample.first_flight_loss is not None
    assert sample.first_flight_loss > 0.05
    ctl = make_fec_controller(32)
    ctl.observe_block(sample)
    assert ctl.path_loss[-1] == pytest.approx(sample.first_flight_loss)


def test_block_quartile_and_fmt():
    assert block_quartile(0, 2072) == 0
    assert block_quartile(518, 2072) == 1
    assert block_quartile(1036, 2072) == 2
    assert block_quartile(2071, 2072) == 3
    assert fmt_quartile_pcts([10, 8, 0, 0], [10, 10, 0, 0]) == "100/80/-/-"
    assert fmt_quartile_counts([0, 5, 12, 20]) == "0/5/12/20"


def test_parse_done_metrics_from_sender_log():
    from tetrys_nc.block_state import parse_done_metrics

    log = (
        "done in 26.10s — goodput 78.90 MiB/s — source_wire=2587.1MiB "
        "repair_wire=12.4MiB first_close=98% extra_blocks=4 dir_rounds=7 "
        "xfrac=3% loss_p50=1.0% p90=2.0% p99=4.0% flight_p95=2.5% "
        "extra_p50=0.5% p90=1.0% fec=12% why=hold_p95=6.1_12%_clean=4 "
        "tail=0.80s pace_p10=850 med=850 max=850Mbit"
    )
    got = parse_done_metrics(log)
    assert got is not None
    assert got.goodput_mib == pytest.approx(78.90)
    assert got.source_wire_mib == pytest.approx(2587.1)
    assert got.repair_wire_mib == pytest.approx(12.4)
    assert got.first_close_pct == pytest.approx(98.0)
    assert got.dir_rounds == 7
    assert got.tail_s == pytest.approx(0.80)
    assert got.pace_p10 == pytest.approx(850.0)
    assert got.pace_med == pytest.approx(850.0)
