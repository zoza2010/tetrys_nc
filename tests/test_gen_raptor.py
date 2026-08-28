"""Unit tests for generation RaptorQ helpers (skipped if raptorq missing)."""

from __future__ import annotations

import random
from pathlib import Path

import pytest

raptorq = pytest.importorskip("raptorq")

from tetrys_nc.gen_raptor import GenDecoder, GenEncoder, GenReceiveSlot, blast_repair_budget, repair_count


def test_repair_count():
    assert repair_count(48, 8) == 4
    assert repair_count(10, 8) >= 1
    assert repair_count(48, 0) == 0
    assert blast_repair_budget(48, 0) == 0
    assert blast_repair_budget(48, 8) == repair_count(48, 8)


def test_systematic_only_smaller_than_full_blast():
    T, K = 1350, 48
    data = bytes([(i * 3) & 0xFF for i in range(K * T)])
    full = GenEncoder(data, T, overhead_pct=8, systematic_only=False)
    sys_only = GenEncoder(data, T, overhead_pct=8, systematic_only=True)
    assert len(sys_only.packets()) >= K
    assert len(sys_only.packets()) < len(full.packets())


def test_gen_receive_slot_systematic_spool(tmp_path: Path):
    T, K = 512, 16
    data = bytes((i * 3) & 0xFF for i in range(K * T))
    enc = GenEncoder(data, T, overhead_pct=8, systematic_only=True)
    pkts = enc.packets()
    spool = tmp_path / "spool"
    slot = GenReceiveSlot(0, gen_k=K, symbol_size=T, block_bytes=len(data), tlen=len(data), spool_dir=spool)
    out = None
    for esi in range(K):
        out = slot.add_packet(pkts[esi], esi)
    slot.close()
    assert out == data
    assert not list(spool.glob("*.pkts"))


def test_gen_receive_slot_repair_spool(tmp_path: Path):
    T, K = 512, 16
    data = bytes((i * 7) & 0xFF for i in range(K * T))
    enc = GenEncoder(data, T, overhead_pct=8, systematic_only=False)
    pkts = enc.packets()
    drop = {3, 7}
    spool = tmp_path / "spool"
    slot = GenReceiveSlot(0, gen_k=K, symbol_size=T, block_bytes=len(data), tlen=len(data), spool_dir=spool)
    out = None
    for esi, blob in enumerate(pkts):
        if esi in drop and esi < K:
            continue
        out = slot.add_packet(blob, esi)
        if out is not None:
            break
    slot.close()
    assert out == data


def test_gen_roundtrip():
    T, K = 1350, 48
    data = bytes([(i * 3) & 0xFF for i in range(K * T)])
    enc = GenEncoder(data, T, overhead_pct=8)
    dec = GenDecoder(len(data), T)
    out = None
    for blob in enc.packets():
        out = dec.add_packet(blob)
        if out is not None:
            break
    assert out == data


def test_gen_with_loss():
    T, K = 1350, 48
    data = bytes([(i * 5) & 0xFF for i in range(K * T)])
    enc = GenEncoder(data, T, overhead_pct=8)
    rng = random.Random(42)
    keep = [p for p in enc.packets() if rng.random() > 0.05]
    dec = GenDecoder(len(data), T)
    out = None
    for blob in keep:
        out = dec.add_packet(blob)
        if out is not None:
            break
    if out is None:
        for blob in enc.ensure_repair(repair_count(K, 8) * 3):
            out = dec.add_packet(blob)
            if out is not None:
                break
    assert out == data


def test_decoder_missing_source_esi():
    T, K = 64, 8
    data = bytes(range(T)) * K
    enc = GenEncoder(data, T, overhead_pct=2)
    dec = GenDecoder(len(data), T)
    dec.add_packet(enc.packets()[0], esi=0)
    dec.add_packet(enc.packets()[2], esi=2)
    assert dec.missing_source_esi(K) == [1, 3, 4, 5, 6, 7]


def test_decoder_deduplicates_outer_esi():
    T, K = 64, 8
    data = bytes(range(T)) * K
    enc = GenEncoder(data, T, overhead_pct=2)
    pkt = enc.packets()[0]
    dec = GenDecoder(len(data), T)
    assert dec.add_packet(pkt, esi=0) is None
    assert dec.add_packet(pkt, esi=0) is None
    assert dec.symbols_rx == 1
    assert dec.dup_esi == 1
