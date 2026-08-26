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
    BlockDataV2,
    BlockFeedbackV2,
    BlockFinV2,
    BlockMetaV2,
    BlockReadyV2,
    OpenBlock,
    block_ids_to_ranges,
    pack_data_packets,
    parse_v2_packet,
)
from tetrys_nc.block_state import (
    AckPacer,
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
    encode_block_job,
    rebuild_block_encoder,
    run_block_client,
    run_block_server,
)
from tetrys_nc.gen_raptor import GenEncoder, GenReceiveSlot
from tetrys_nc.packets import parse_packet


def test_v2_wire_roundtrips_and_rejects_v1():
    packets = [
        BlockReadyV2(9, 64 << 20),
        BlockMetaV2(9, 1234, "blob.bin", 1350, 768, 14, 64 << 20, "ab"),
        BlockDataV2(9, 7, 3, b"x" * 100, 55),
        BlockFeedbackV2(
            9,
            4,
            1000,
            500,
            55,
            [1, 3],
            [OpenBlock(7, 700), OpenBlock(8, 770, True)],
        ),
        BlockFinV2(9, 20),
    ]
    for packet in packets:
        assert parse_v2_packet(packet.pack()) == packet
    with pytest.raises(ValueError):
        parse_v2_packet(b"\x54\x01\x20\x00" + bytes(20))


def test_v2_feedback_ranges_fit_in_one_datagram():
    sequential = BlockFeedbackV2(
        3,
        1,
        0,
        0,
        done_blocks=list(range(2000)),
        open_blocks=[OpenBlock(i, i) for i in range(1000)],
    )
    wire = sequential.pack()
    assert len(wire) <= 1400
    got = BlockFeedbackV2.unpack(wire)
    assert got.done_blocks == list(range(2000))
    assert len(got.open_blocks or []) == 64
    sparse = BlockFeedbackV2(
        3,
        2,
        0,
        0,
        done_blocks=list(range(0, 200, 2)),
        open_blocks=[OpenBlock(i, i) for i in range(80)],
    )
    sparse_wire = sparse.pack()
    assert len(sparse_wire) <= 1400
    sparse_got = BlockFeedbackV2.unpack(sparse_wire)
    assert len(sparse_got.done_blocks or []) == 48
    with pytest.raises(ValueError):
        BlockFeedbackV2.unpack(wire[:-3])


def test_completion_ranges_are_compact_and_rotate():
    assert block_ids_to_ranges([0, 1, 2, 5, 6]) == [(0, 3), (5, 2)]
    islands = list(range(0, 120, 2))
    first = block_ids_to_ranges(islands, limit=48, rotate=0)
    second = block_ids_to_ranges(islands, limit=48, rotate=1)
    assert len(first) == 48
    assert first != second


def test_parse_packet_dispatches_v2_and_rejects_unknown_version():
    ready = BlockReadyV2(9, 64 << 20).pack()
    assert parse_packet(ready) == BlockReadyV2(9, 64 << 20)
    with pytest.raises(ValueError):
        parse_packet(b"\x54\x09\x30\x00" + bytes(8))


def test_feedback_state_is_idempotent_and_monotonic():
    state = SenderFeedbackState(11)
    newer = BlockFeedbackV2(
        11, 2, 200, 100, done_blocks=[1], open_blocks=[OpenBlock(2, 50)]
    )
    stale = BlockFeedbackV2(
        11, 1, 999, 999, done_blocks=[9], open_blocks=[OpenBlock(2, 80)]
    )
    assert state.apply(newer, now=1.0)
    assert not state.apply(stale, now=2.0)
    done, opened, unique, decoded = state.snapshot()
    assert done == {1}
    assert opened[2].unique_esi == 50
    assert (unique, decoded) == (200, 100)


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


def test_ack_pacer_uses_unique_bytes_not_decoded_bytes():
    pacer = AckPacer(25_000_000, 143_750_000, 27_500_000)
    assert pacer.update(0, 1.0) == 27_500_000
    assert pacer.update(1_000_000, 1.0) == 27_500_000
    raised = pacer.update(8_000_000, 1.25)
    assert raised > 27_500_000


def test_ack_pacer_ignores_empty_and_stretched_samples():
    start = 87_500_000  # 700 Mbit
    pacer = AckPacer(start * 0.85, 115_000_000, start)
    assert pacer.update(0, 1.00) == start
    assert pacer.update(1_000_000, 1.00) == start
    # dt=0.8s would look like ~10 Mbit if treated as BtlBw.
    assert pacer.update(2_000_000, 1.80) == start
    assert pacer.startup is True
    climbed = pacer.update(12_000_000, 1.95)
    assert climbed >= start
    assert pacer.startup is True


def test_ack_pacer_holds_through_one_weak_sample():
    start = 87_500_000
    pacer = AckPacer(start * 0.85, 115_000_000, start)
    pacer.update(1_000_000, 1.00)
    # Strong sample: 14 MB / 0.15s ≈ 93 MB/s > start.
    pacer.update(1_000_000 + 14_000_000, 1.15)
    pacer.startup = False
    held = pacer.offer_bps
    # Weak unique without repair is app-limited; BtlBw max-filter holds.
    once = pacer.update(pacer.last_unique + 2_000_000, pacer.last_ts + 0.15)
    assert once >= held * 0.99
    twice = pacer.update(pacer.last_unique + 2_000_000, pacer.last_ts + 0.15)
    assert twice >= held * 0.99
    for _ in range(5):
        pacer.update(pacer.last_unique + 2_000_000, pacer.last_ts + 0.15)
    assert pacer.offer_bps >= held * 0.90


def test_ack_pacer_holds_while_repair_busy():
    start = 87_500_000
    pacer = AckPacer(start, 115_000_000, start)
    pacer.update(1_000_000, 1.00)
    pacer.update(1_000_000 + 14_000_000, 1.15)
    pacer.startup = False
    held = pacer.offer_bps
    for _ in range(2):
        got = pacer.update(
            pacer.last_unique + 2_000_000,
            pacer.last_ts + 0.15,
            repair_busy=True,
        )
    assert got >= held * 0.99
    assert pacer.held_repair_events >= 1
    assert pacer.backoff_events == 0


def test_ack_pacer_backoff_on_sustained_repair():
    start = 87_500_000
    pacer = AckPacer(start, 115_000_000, start, fec_frac=0.20)
    pacer.update(1_000_000, 1.00)
    for _ in range(12):
        delta = max(1, int(pacer.offer_bps * 0.80 * 0.15))
        pacer.update(pacer.last_unique + delta, pacer.last_ts + 0.15)
    held = pacer.offer_bps
    assert held > start
    assert pacer.btlbw_bps > 0
    for _ in range(8):
        pacer.update(
            pacer.last_unique + 2_000_000,
            pacer.last_ts + 0.15,
            repair_busy=True,
        )
    assert pacer.backoff_events >= 1
    assert pacer.offer_bps < held
    assert pacer.offer_bps >= start


def test_ack_pacer_backoff_cools_without_flooring():
    start = 87_500_000
    pacer = AckPacer(start, 115_000_000, start, fec_frac=0.20)
    pacer.update(1_000_000, 1.00)
    for _ in range(12):
        delta = max(1, int(pacer.offer_bps * 0.80 * 0.15))
        pacer.update(pacer.last_unique + delta, pacer.last_ts + 0.15)
    held = pacer.offer_bps
    for _ in range(40):
        pacer.update(
            pacer.last_unique + 2_000_000,
            pacer.last_ts + 0.15,
            repair_busy=True,
        )
    assert pacer.backoff_events >= 1
    assert pacer.backoff_events <= 4
    assert pacer.offer_bps < held
    assert pacer.offer_bps > start * 1.05
    pacer.update(pacer.last_unique + 12_000_000, pacer.last_ts + 0.15)
    assert pacer.mode != "probe"


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


def test_ack_pacer_cruises_near_btlbw_not_hard_cap():
    start = 87_500_000
    cap = 115_000_000
    pacer = AckPacer(start, cap, start, fec_frac=0.20)
    pacer.update(1_000_000, 1.00)
    for _ in range(24):
        delta = max(1, int(pacer.offer_bps * 0.80 * 0.15))
        pacer.update(pacer.last_unique + delta, pacer.last_ts + 0.15)
    assert pacer.btlbw_bps > 0
    assert pacer.mode in ("cruise", "probe")
    # Cruise 0.97×BtlBw; probe may briefly go to 1.03×. Never need the hard cap
    # unless BtlBw itself is the cap.
    assert pacer.offer_bps <= max(pacer.btlbw_bps * 1.04, pacer.min_bps)


def test_ack_pacer_climbs_when_unique_tracks_offer():
    start = 87_500_000
    pacer = AckPacer(start, 115_000_000, start, fec_frac=0.20)
    pacer.update(1_000_000, 1.00)
    for _ in range(12):
        delta = max(1, int(pacer.offer_bps * 0.80 * 0.15))
        pacer.update(pacer.last_unique + delta, pacer.last_ts + 0.15)
    assert pacer.offer_bps > start * 1.10
    assert pacer.btlbw_bps > start


def test_ack_pacer_stays_near_start_when_unique_is_start_payload():
    start = 87_500_000  # 700 Mbit send; 20% FEC → ~70 MB/s unique
    pacer = AckPacer(start, 115_000_000, start, fec_frac=0.20)
    pacer.update(1_000_000, 1.00)
    for _ in range(16):
        pacer.update(pacer.last_unique + 10_500_000, pacer.last_ts + 0.15)
    assert pacer.offer_bps <= start * 1.08
    assert pacer.btlbw_bps <= start * 1.05


def test_ack_pacer_floor_is_start():
    start = 87_500_000
    pacer = AckPacer(start, 115_000_000, start)
    pacer.update(1_000_000, 1.00)
    pacer.update(1_000_000 + 14_000_000, 1.15)
    pacer.startup = False
    for _ in range(12):
        pacer.update(pacer.last_unique + 2_000_000, pacer.last_ts + 0.15)
    assert pacer.offer_bps >= start


def test_pace_limits_floor_equals_start(monkeypatch):
    monkeypatch.delenv("TETRYS_START_MBIT", raising=False)
    monkeypatch.delenv("TETRYS_PACE_CAP_MBIT", raising=False)
    monkeypatch.delenv("TETRYS_PACE_MIN_FRAC", raising=False)
    min_bps, max_bps, start_bps = _pace_limits(2500.0)
    assert start_bps == pytest.approx(850_000_000 / 8)
    assert min_bps == pytest.approx(start_bps)
    assert max_bps == pytest.approx(850_000_000 / 8)


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
    assert WAN_INITIAL_REPAIR_PCT == 20
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
    parsed = [parse_v2_packet(bytes(w)) for w in wires]
    assert all(isinstance(p, BlockDataV2) for p in parsed)
    assert parsed[0].session_id == 9
    encoder = GenEncoder(data, t, 14)
    assert [p.payload for p in parsed] == encoder.packets()


def test_pack_data_packets_roundtrips_like_blockdata():
    payloads = [b"aa", b"bb"]
    wires = pack_data_packets(3, 4, payloads, first_esi=7, send_ts_us=11)
    for i, wire in enumerate(wires):
        got = BlockDataV2.unpack(bytes(wire))
        assert got == BlockDataV2(3, 4, 7 + i, payloads[i], 11)


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


def test_v2_loopback_transfer_is_byte_correct(tmp_path: Path):
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
                src,
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
    run_block_client("127.0.0.1", port, dst, active_bytes=4 << 20)
    thread.join(timeout=8)
    assert not errors, errors[0]
    assert dst.read_bytes() == payload
