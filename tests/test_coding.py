"""Unit tests for GF(256) and Tetrys encode/decode roundtrip."""

from __future__ import annotations

import time

from tetrys_nc import gf256
from tetrys_nc.decoder import TetrysDecoder
from tetrys_nc.encoder import EncoderConfig, TetrysEncoder
from tetrys_nc.packets import SOURCE_HDR_SIZE, CodedPacket, WindowUpdatePacket, parse_packet
from tetrys_nc.ratectl import DelayRateController, RateLimiter


def test_gf_mul_div():
    for a in (1, 2, 17, 200, 255):
        for b in (1, 3, 9, 128, 255):
            assert gf256.div(gf256.mul(a, b), b) == a


def test_gf_mul_bytes_matches_scalar():
    data = bytes(range(256)) * 8
    for coef in (0, 1, 2, 17, 200):
        out_a = bytearray(len(data))
        out_b = bytearray(len(data))
        gf256.mul_bytes(coef, data, out_a)
        if coef == 0:
            pass
        elif coef == 1:
            for i, b in enumerate(data):
                out_b[i] ^= b
        else:
            table = gf256.MUL_TABLE[coef]
            for i, b in enumerate(data):
                out_b[i] ^= table[b]
        assert out_a == out_b


def test_linear_combine_matches_mul_bytes():
    ps = 1350
    a = bytes((i * 3) % 256 for i in range(ps))
    b = bytes((i * 7) % 256 for i in range(ps))
    terms = [(17, a), (200, b), (1, a)]
    got = gf256.linear_combine(terms, ps)
    ref = bytearray(ps)
    for coef, data in terms:
        gf256.mul_bytes(coef, data, ref)
    assert got == bytes(ref)
    assert gf256.backend() in ("numpy", "python")


def test_roundtrip_no_loss():
    enc = TetrysEncoder(
        EncoderConfig(max_window=64, redundancy_every=3, payload_size=64, code_degree=8)
    )
    dec = TetrysDecoder()
    dec.payload_size = 64
    messages = [bytes([(i + j) % 256 for j in range(64)]) for i in range(40)]

    delivered = {}
    for msg in messages:
        wire = enc.add_source(msg)
        sid = int.from_bytes(wire[4:8], "big")
        for s, data in dec.on_source_raw(sid, bytes(wire[SOURCE_HDR_SIZE:])):
            delivered[s] = data
        for coded_wire in enc.maybe_coded():
            coded = parse_packet(coded_wire)
            assert isinstance(coded, CodedPacket)
            for s, data in dec.on_coded(coded):
                delivered[s] = data
        fb = dec.build_feedback()
        fb.echo_ts_us = int.from_bytes(wire[8:12], "big")
        enc.apply_feedback(fb.cumulative_ack, fb.plr_byte, fb.missing_ids())

    for s, data in dec.pop_deliverable():
        delivered[s] = data

    assert len(delivered) == 40
    for i, msg in enumerate(messages):
        assert delivered[i] == msg


def test_recover_oldest_coded_with_holes():
    """Drop early packets; coded over OLDEST window must repair HOL."""
    enc = TetrysEncoder(
        EncoderConfig(max_window=128, redundancy_every=2, payload_size=32, code_degree=16)
    )
    dec = TetrysDecoder()
    dec.payload_size = 32
    n = 30
    messages = [bytes([i] * 32) for i in range(n)]
    drop = {3, 7, 11, 15}

    delivered = {}
    for i, msg in enumerate(messages):
        wire = enc.add_source(msg)
        if i not in drop:
            sid = int.from_bytes(wire[4:8], "big")
            for s, data in dec.on_source_raw(sid, bytes(wire[SOURCE_HDR_SIZE:])):
                delivered[s] = data
        for coded_wire in enc.maybe_coded():
            coded = parse_packet(coded_wire)
            assert isinstance(coded, CodedPacket)
            for s, data in dec.on_coded(coded):
                delivered[s] = data
        fb = dec.build_feedback()
        enc.apply_feedback(fb.cumulative_ack, fb.plr_byte, fb.missing_ids())

    for _ in range(40):
        c = enc.make_coded(prefer_oldest=True)
        if c:
            for s, data in dec.on_coded(c):
                delivered[s] = data
        for s, data in dec.pop_deliverable():
            delivered[s] = data
        fb = dec.build_feedback()
        enc.apply_feedback(fb.cumulative_ack, fb.plr_byte, fb.missing_ids())
        for wire in enc.pop_nack_retransmit(limit=8):
            sid = int.from_bytes(wire[4:8], "big")
            for s, data in dec.on_source_raw(sid, wire[SOURCE_HDR_SIZE:]):
                delivered[s] = data
        if len(delivered) == n:
            break

    for s, data in dec.pop_deliverable():
        delivered[s] = data

    assert dec.stats()["recovered"] > 0 or len(delivered) == n
    for i, msg in enumerate(messages):
        assert i in delivered, f"missing symbol {i}"
        assert delivered[i] == msg


def test_sack_missing_ids():
    dec = TetrysDecoder()
    dec.total_symbols = 100
    dec.on_source_raw(0, b"\x00" * 8)
    dec.on_source_raw(2, b"\x02" * 8)
    dec.on_source_raw(3, b"\x03" * 8)
    assert dec.next_deliver == 1
    assert dec.highest_seen == 3
    fb = dec.build_feedback(sack_bits=32)
    assert fb.cumulative_ack == 1
    missing = fb.missing_ids(limit=16)
    assert 1 in missing
    assert fb.nb_missing_src == 1
    assert fb.plr_byte > 0


def test_plr_zero_when_in_order():
    dec = TetrysDecoder()
    dec.total_symbols = 1000
    for i in range(50):
        dec.on_source_raw(i, bytes([i % 256]) * 8)
    fb = dec.build_feedback(sack_bits=256)
    assert fb.cumulative_ack == 50
    assert fb.nb_missing_src == 0
    assert fb.plr_byte == 0


def test_window_update_echo_roundtrip():
    pkt = WindowUpdatePacket(
        10, 2, 0, 5, sack=b"\x01\x00\x00\x00", echo_ts_us=123456, sack_span=8
    )
    wire = pkt.pack()
    got = WindowUpdatePacket.unpack(wire)
    assert got.cumulative_ack == 10
    assert got.echo_ts_us == 123456
    assert got.plr_byte == 5
    assert got.sack_span == 8
    assert got.sack[0] & 1
    assert 10 not in got.missing_ids()
    assert 10 in got.held_ids()


def test_sack_release_unpins_window():
    """WAN/no-FEC: SACKed symbols must leave the encoder window."""
    enc = TetrysEncoder(
        EncoderConfig(max_window=32, redundancy_every=0, payload_size=8)
    )
    for i in range(20):
        enc.add_source(bytes([i]) * 8)
    assert enc.window_size == 20
    # Receiver delivered 0..4, holds 6..10, missing 5
    held = list(range(6, 11))
    missing = [5]
    enc.apply_feedback(5, plr_byte=40, missing_ids=missing, held_ids=held)
    assert enc.window_size == 20 - 5 - 5  # freed cumack 0..4 and held 6..10
    assert 5 in enc._window
    assert 5 in enc._nack_set
    assert 11 in enc._window


def test_nack_reorder_delay_and_horizon():
    enc = TetrysEncoder(
        EncoderConfig(
            max_window=64,
            redundancy_every=0,
            payload_size=8,
            nack_reorder_ms=50,
            nack_horizon=10,
            nack_hol_urgent=2,  # only cum_ack, cum_ack+1 are immediate
        )
    )
    for i in range(40):
        enc.add_source(bytes([i]) * 8)

    # Hole at 12 (outside urgent=2, inside horizon=10) and 25 (beyond horizon).
    enc.apply_feedback(5, missing_ids=[12, 25], held_ids=list(range(6, 12)))
    assert 12 not in enc._nack_set
    assert 12 in enc._nack_pending
    assert 25 not in enc._nack_pending
    assert enc.pop_nack_retransmit(limit=8) == []

    # HOL-urgent hole is immediate.
    enc2 = TetrysEncoder(
        EncoderConfig(
            max_window=64,
            redundancy_every=0,
            payload_size=8,
            nack_reorder_ms=50,
            nack_horizon=32,
            nack_hol_urgent=8,
        )
    )
    for i in range(20):
        enc2.add_source(bytes([i]) * 8)
    enc2.apply_feedback(3, missing_ids=[3, 4, 12], held_ids=list(range(5, 11)))
    assert 3 in enc2._nack_set and 4 in enc2._nack_set
    assert 12 in enc2._nack_pending
    assert 12 not in enc2._nack_set

    # Age the pending timestamp and promote.
    enc._nack_pending[12] = time.monotonic() - 0.06
    assert enc.promote_nacks() == 1
    assert 12 in enc._nack_set
    wires = enc.pop_nack_retransmit(limit=8)
    assert len(wires) == 1
    # Non-HOL re-arms delay after send.
    assert 12 in enc._nack_pending
    assert 12 not in enc._nack_set

    # Held cancels pending.
    enc.apply_feedback(5, missing_ids=[], held_ids=[12])
    assert 12 not in enc._nack_pending
    assert 12 not in enc._window


def test_blast_starts_at_cap():
    lim = RateLimiter(50_000_000.0)
    assert lim.rate == lim.max_rate
    assert lim.min_rate >= lim.max_rate * 0.90


def test_loss_does_not_collapse_rate():
    lim = RateLimiter(50_000_000.0, start_bps=50_000_000.0)
    cc = DelayRateController(lim, payload_size=1350)
    before = lim.rate
    cc.on_loss(120)
    assert lim.rate == before
    cc.on_loss(220)
    assert lim.rate == before


def test_soft_bias_keeps_near_cap():
    lim = RateLimiter(50_000_000.0, start_bps=50_000_000.0)
    cc = DelayRateController(lim, payload_size=1000)
    import time

    for _ in range(4):
        time.sleep(0.055)
        cc.on_ack(800, plr_byte=40)
    # Establish min_rtt then a large standing queue
    cc.base_rtt_us = 80_000.0
    cc.srtt_us = 220_000.0  # 140ms standing queue
    cc._update_pacing(time.monotonic())
    assert cc.mode == DelayRateController.MODE_SOFT
    # Soft bias only — still ≥90% of target (FASP floor)
    assert lim.rate >= 50_000_000.0 * 0.90
    assert lim.rate <= 50_000_000.0
