"""Unit tests for GF(256) and Tetrys encode/decode roundtrip."""

from __future__ import annotations

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
        # scalar reference
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
    pkt = WindowUpdatePacket(10, 2, 0, 5, sack=b"\x01\x00\x00\x00", echo_ts_us=123456)
    wire = pkt.pack()
    got = WindowUpdatePacket.unpack(wire)
    assert got.cumulative_ack == 10
    assert got.echo_ts_us == 123456
    assert got.plr_byte == 5
    assert got.sack[0] & 1  # bit0 set => id 10 held
    assert 10 not in got.missing_ids()


def test_slow_start_begins_below_max():
    lim = RateLimiter(100_000_000.0)  # 100 MB/s ceiling
    assert lim.rate < lim.max_rate
    assert lim.rate <= 10_000_000.0  # start capped (~80 Mbit)


def test_delay_cc_ignores_loss_raises_on_clear_path():
    lim = RateLimiter(10_000_000.0, start_bps=5_000_000.0)
    cc = DelayRateController(lim)
    cc.warmup_left = 0
    send_ts = 1_000_000
    assert cc.on_echo(send_ts, send_ts + 20_000) == 20_000.0
    assert cc.base_rtt_us == 20_000.0
    before = lim.rate
    cc.on_echo(send_ts + 1000, send_ts + 1000 + 25_000)
    assert lim.rate >= before
    peaked = lim.rate
    # Severe queue (>250ms inst) should cut
    cc.on_echo(send_ts + 2000, send_ts + 2000 + 400_000)
    assert lim.rate < peaked


def test_ack_climb_without_rtt_echo():
    lim = RateLimiter(50_000_000.0, start_bps=1_400_000.0)
    cc = DelayRateController(lim)
    start = lim.rate
    for _ in range(20):
        cc.on_ack(64, plr_byte=0)
    assert lim.rate > start * 2
    assert lim.rate <= lim.max_rate
