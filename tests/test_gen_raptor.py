"""Unit tests for generation RaptorQ helpers (skipped if raptorq missing)."""

from __future__ import annotations

import random

import pytest

raptorq = pytest.importorskip("raptorq")

from tetrys_nc.gen_raptor import GenDecoder, GenEncoder, blast_repair_budget, repair_count
from tetrys_nc.gen_xfer import fountain_targets
from tetrys_nc.packets import (
    XFER_GEN,
    GenFeedbackPacket,
    GenPacket,
    MetaPacket,
    parse_packet,
)


def test_fountain_targets_prefers_frontier_and_skips_full_rank():
    got = fountain_targets(
        10,
        40,
        nacks=[12, 18, 10],
        nack_rx={10: 80, 12: 200, 18: 20},
        gen_k=192,
        limit=4,
        decode_margin=2,
    )
    # 12 already has K+margin symbols; 10 is the frontier even if nacked.
    assert got[0] == 10
    assert 12 not in got
    assert 18 in got
    assert len(got) <= 4


def test_fountain_targets_walks_window_without_nacks():
    got = fountain_targets(5, 20, nacks=[], nack_rx={}, gen_k=192, limit=3)
    assert got == [5, 6, 7]


def test_repair_count():
    assert repair_count(48, 8) == 4
    assert repair_count(10, 8) >= 1
    assert repair_count(48, 0) == 0
    assert blast_repair_budget(48, 0) == repair_count(48, 4)
    assert blast_repair_budget(48, 8) == repair_count(48, 8)


def test_fountain_targets_frontier_only():
    got = fountain_targets(
        10,
        300,
        nacks=[],
        nack_rx={},
        gen_k=192,
        limit=8,
        frontier_only=True,
    )
    assert got == [10]


def test_gen_roundtrip():
    T = 1350
    K = 48
    data = bytes([(i * 3) & 0xFF for i in range(K * T)])
    enc = GenEncoder(data, T, overhead_pct=8)
    pkts = enc.packets()
    assert len(pkts) >= K
    dec = GenDecoder(len(data), T)
    out = None
    for blob in pkts:
        out = dec.add_packet(blob)
        if out is not None:
            break
    assert out == data


def test_gen_with_loss():
    T = 1350
    K = 48
    data = bytes([(i * 5) & 0xFF for i in range(K * T)])
    enc = GenEncoder(data, T, overhead_pct=8)
    pkts = list(enc.packets())
    rng = random.Random(42)
    # Drop ~5%
    keep = [p for p in pkts if rng.random() > 0.05]
    dec = GenDecoder(len(data), T)
    out = None
    for blob in keep:
        out = dec.add_packet(blob)
        if out is not None:
            break
    if out is None:
        # need extras
        more = enc.ensure_repair(repair_count(K, 8) * 3)
        for blob in more:
            out = dec.add_packet(blob)
            if out is not None:
                break
    assert out == data


def test_gen_packet_wire():
    gp = GenPacket(7, 3, b"\x01" * 100, send_ts_us=99)
    got = GenPacket.unpack(gp.pack())
    assert got.gen_id == 7 and got.esi == 3 and got.payload == gp.payload
    assert isinstance(parse_packet(gp.pack()), GenPacket)


def test_gen_feedback_wire():
    fb = GenFeedbackPacket(
        2,
        [2, 5, 9],
        echo_ts_us=1,
        completed_gens=2,
        nack_rx_counts=[45, 12, 0],
    )
    got = GenFeedbackPacket.unpack(fb.pack())
    assert got.next_needed_gen == 2
    assert got.nack_gens == [2, 5, 9]
    assert got.completed_gens == 2
    assert got.nack_rx_counts == [45, 12, 0]


def test_gen_feedback_legacy_without_counts():
    fb = GenFeedbackPacket(2, [2, 5], echo_ts_us=1, completed_gens=1)
    got = GenFeedbackPacket.unpack(fb.pack())
    assert got.nack_gens == [2, 5]
    assert got.nack_rx_counts is None


def test_decoder_deduplicates_outer_esi():
    T = 64
    K = 8
    data = bytes(range(T)) * K
    enc = GenEncoder(data, T, overhead_pct=2)
    pkt = enc.packets()[0]
    dec = GenDecoder(len(data), T)
    assert dec.add_packet(pkt, esi=0) is None
    assert dec.add_packet(pkt, esi=0) is None
    assert dec.symbols_rx == 1
    assert dec.dup_esi == 1


def test_meta_gen_roundtrip():
    m = MetaPacket(
        1000,
        "f.bin",
        1350,
        "ab" * 32,
        xfer=XFER_GEN,
        gen_symbol_size=1350,
        gen_k=48,
        gen_overhead_pct=8,
    )
    got = MetaPacket.unpack(m.pack())
    assert got.xfer == XFER_GEN
    assert got.gen_k == 48 and got.gen_symbol_size == 1350
    # META always packs gen trailer
    plain = MetaPacket(100, "x", 64, "")
    got2 = MetaPacket.unpack(plain.pack())
    assert got2.xfer == XFER_GEN
