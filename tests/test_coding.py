"""Unit tests for GF(256) and Tetrys encode/decode roundtrip."""

from __future__ import annotations

from tetrys_nc import gf256
from tetrys_nc.decoder import TetrysDecoder
from tetrys_nc.encoder import EncoderConfig, TetrysEncoder
from tetrys_nc.packets import CodedPacket, parse_packet


def test_gf_mul_div():
    for a in (1, 2, 17, 200, 255):
        for b in (1, 3, 9, 128, 255):
            assert gf256.div(gf256.mul(a, b), b) == a


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
        for s, data in dec.on_source_raw(sid, wire[8:]):
            delivered[s] = data
        coded_wire = enc.maybe_coded()
        if coded_wire:
            coded = parse_packet(coded_wire)
            assert isinstance(coded, CodedPacket)
            for s, data in dec.on_coded(coded):
                delivered[s] = data
        fb = dec.build_feedback()
        enc.apply_feedback(fb.cumulative_ack, fb.plr_byte)

    for s, data in dec.pop_deliverable():
        delivered[s] = data

    assert len(delivered) == 40
    for i, msg in enumerate(messages):
        assert delivered[i] == msg


def test_recover_with_coded():
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
            for s, data in dec.on_source_raw(sid, wire[8:]):
                delivered[s] = data
        coded_wire = enc.maybe_coded()
        if coded_wire:
            coded = parse_packet(coded_wire)
            assert isinstance(coded, CodedPacket)
            for s, data in dec.on_coded(coded):
                delivered[s] = data
        if i % 5 == 4:
            c = enc.make_coded()
            if c:
                for s, data in dec.on_coded(c):
                    delivered[s] = data
        fb = dec.build_feedback()
        enc.apply_feedback(fb.cumulative_ack, fb.plr_byte)

    for _ in range(40):
        c = enc.make_coded()
        if c:
            for s, data in dec.on_coded(c):
                delivered[s] = data
        for s, data in dec.pop_deliverable():
            delivered[s] = data
        fb = dec.build_feedback()
        enc.apply_feedback(fb.cumulative_ack, fb.plr_byte)
        if len(delivered) == n:
            break

    for s, data in dec.pop_deliverable():
        delivered[s] = data

    assert dec.stats()["recovered"] > 0
    for i, msg in enumerate(messages):
        assert i in delivered, f"missing symbol {i}"
        assert delivered[i] == msg
