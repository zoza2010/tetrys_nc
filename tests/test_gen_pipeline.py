"""Regression tests for gen pipeline / async fountain bottlenecks."""

from __future__ import annotations

import mmap
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tetrys_nc.gen_raptor import GenEncoder, blast_repair_budget, fountain_blast_budget, require_raptorq
from tetrys_nc.gen_xfer import (
    _FOUNTAIN_CAP_GENS,
    _FOUNTAIN_EVERY_N,
    _FOUNTAIN_TRACK_MAX,
    _FOUNTAIN_WINDOW,
    _HOL_REPAIR_COOLDOWN_S,
    _OH_CLEAN_HOLD_S,
    _REORDER_HOLDOFF_S,
    _REPAIR_COOLDOWN_S,
    _REPAIR_META_KEEP,
    encode_batch_start,
    encode_read_gens_from_env,
    disk_queue_adapt_bytes,
    disk_queue_adapt_from_env,
    disk_queue_drop_consumed,
    disk_direct_from_env,
    disk_queue_mib_from_env,
    disk_queue_max_mib_from_env,
    disk_queue_note_read,
    disk_queue_pop_blob,
    disk_queue_pread,
    open_disk_queue_fd,
    disk_feed_cap_bps,
    drain_encode_out_queue,
    _encode_blob_worker_stream,
    _encode_gen_worker,
    _encode_gens_worker,
    _encode_gens_worker_stream,
    adaptive_blast_overhead_pct,
    blast_fec_miss_frac,
    adaptive_inflight_mib,
    bbr_pacing_gain,
    bbr_still_startup,
    bdp_bytes,
    build_feedback_miss_bitmap,
    cap_fountain_gens,
    cap_fountain_send,
    clear_gen_deficit,
    client_feedback_horizon,
    client_feedback_interval,
    compute_inflight_gen_limit,
    delay_cc_may_probe,
    drain_empty_round_max,
    hol_pause_should_hold,
    hol_should_pause_blast,
    drain_epoch_is_stale,
    drain_never_seen_frontier,
    drain_repair_send_n,
    drain_scoreboard_targets,
    echo_rtt_s,
    even_spread,
    fountain_redundancy,
    fountain_targets,
    format_close_rounds,
    gen_rank_deficit,
    hol_blocks_tail_repair,
    hol_hole_gens,
    hol_repair_cooldown_s,
    hol_resend_pad_n,
    nack_close_rounds,
    note_close_round,
    note_gen_deficit,
    order_repair_nacks,
    pipeline_stressed,
    prune_fountain_gens_set,
    prune_repair_meta,
    readahead_bytes_from_env,
    readahead_next_slice,
    run_file_disk_queue,
    run_file_readahead,
    repair_holdoff_ready,
    repair_extra_n,
    repair_round_size,
    repair_overhead_pct,
    repair_pressure,
    repair_send_n,
    repair_storm_detected,
    repair_thread_limit,
    select_repair_feedback_gens,
    should_fountain_tick,
    should_pause_blast,
    should_track_fountain_gen,
    should_yield_blast_to_repair,
    smooth_delay_rtt_s,
    track_fountain_gen,
    update_adaptive_pace_bps,
    update_delay_pace_bps,
    update_delivery_guard_bps,
    update_delivery_rate_pace_bps,
    update_btlbw_bps,
)
from tetrys_nc.packets import miss_bitmap_to_nacks
from tetrys_nc.packets import GenPacket


def test_compute_inflight_and_feedback_horizon():
    # Default 64 MiB WAN window (not the ATP 8 MiB experiment).
    assert compute_inflight_gen_limit(48, 1350) == 1035
    assert compute_inflight_gen_limit(192, 1350) == 258
    assert compute_inflight_gen_limit(96, 1350) == 517
    assert client_feedback_horizon(1035) >= 1035
    assert compute_inflight_gen_limit(96, 1350, inflight_mib=8) == 64
    assert compute_inflight_gen_limit(96, 1350, inflight_mib=16) == 129


def test_readahead_next_slice_skips_consumed_and_caps_eof():
    block = 1000
    size = 50_000
    ahead = 8_000
    assert readahead_next_slice(
        gen_id=0,
        block_bytes=block,
        file_size=size,
        primed=0,
        ahead_bytes=ahead,
        chunk_bytes=4096,
    ) == (0, 4096)
    assert (
        readahead_next_slice(
            gen_id=0,
            block_bytes=block,
            file_size=size,
            primed=8_000,
            ahead_bytes=ahead,
            chunk_bytes=4096,
        )
        is None
    )
    # Blast already past primed: skip consumed prefix.
    assert readahead_next_slice(
        gen_id=10,
        block_bytes=block,
        file_size=size,
        primed=0,
        ahead_bytes=ahead,
        chunk_bytes=4096,
    ) == (10_000, 4096)
    assert readahead_next_slice(
        gen_id=49,
        block_bytes=block,
        file_size=size,
        primed=49_000,
        ahead_bytes=ahead,
        chunk_bytes=8192,
    ) == (49_000, 1_000)
    assert (
        readahead_next_slice(
            gen_id=0,
            block_bytes=block,
            file_size=size,
            primed=0,
            ahead_bytes=0,
        )
        is None
    )


def test_readahead_bytes_from_env(monkeypatch):
    monkeypatch.delenv("TETRYS_READAHEAD_MIB", raising=False)
    assert readahead_bytes_from_env() == 0
    monkeypatch.setenv("TETRYS_READAHEAD_MIB", "128")
    assert readahead_bytes_from_env() == 128 * 1024 * 1024
    monkeypatch.setenv("TETRYS_READAHEAD_MIB", "0")
    assert readahead_bytes_from_env() == 0
    monkeypatch.setenv("TETRYS_READAHEAD_MIB", "nope")
    assert readahead_bytes_from_env() == 0 * 1024 * 1024


def test_encode_read_gens_from_env(monkeypatch):
    monkeypatch.delenv("TETRYS_ENCODE_READ_GENS", raising=False)
    assert encode_read_gens_from_env() == 16
    monkeypatch.setenv("TETRYS_ENCODE_READ_GENS", "32")
    assert encode_read_gens_from_env() == 32
    monkeypatch.setenv("TETRYS_ENCODE_READ_GENS", "0")
    assert encode_read_gens_from_env() == 1
    monkeypatch.setenv("TETRYS_ENCODE_READ_GENS", "nope")
    assert encode_read_gens_from_env() == 16
    assert encode_batch_start(0, 16) == 0
    assert encode_batch_start(16, 16) == 16
    assert encode_batch_start(17, 16) == 16
    assert encode_batch_start(31, 16) == 16
    assert encode_batch_start(32, 16) == 32


def test_disk_queue_mib_from_env(monkeypatch):
    monkeypatch.delenv("TETRYS_DISK_QUEUE_MIB", raising=False)
    assert disk_queue_mib_from_env() == 64 * 1024 * 1024
    monkeypatch.setenv("TETRYS_DISK_QUEUE_MIB", "32")
    assert disk_queue_mib_from_env() == 32 * 1024 * 1024
    monkeypatch.setenv("TETRYS_DISK_QUEUE_MIB", "0")
    assert disk_queue_mib_from_env() == 0
    monkeypatch.setenv("TETRYS_DISK_QUEUE_MIB", "nope")
    assert disk_queue_mib_from_env() == 64 * 1024 * 1024


def test_disk_queue_max_mib_from_env(monkeypatch):
    lo = 64 * 1024 * 1024
    monkeypatch.delenv("TETRYS_DISK_QUEUE_MAX_MIB", raising=False)
    assert disk_queue_max_mib_from_env(lo) == 64 * 1024 * 1024
    monkeypatch.setenv("TETRYS_DISK_QUEUE_MAX_MIB", "96")
    assert disk_queue_max_mib_from_env(lo) == 96 * 1024 * 1024
    # Never shrink the floor the user asked for.
    monkeypatch.setenv("TETRYS_DISK_QUEUE_MAX_MIB", "16")
    assert disk_queue_max_mib_from_env(lo) == lo
    monkeypatch.setenv("TETRYS_DISK_QUEUE_MAX_MIB", "nope")
    assert disk_queue_max_mib_from_env(lo) == 64 * 1024 * 1024


def test_disk_queue_adapt_from_env(monkeypatch):
    monkeypatch.delenv("TETRYS_DISK_QUEUE_ADAPT", raising=False)
    assert disk_queue_adapt_from_env() is False
    monkeypatch.setenv("TETRYS_DISK_QUEUE_ADAPT", "0")
    assert disk_queue_adapt_from_env() is False
    monkeypatch.setenv("TETRYS_DISK_QUEUE_ADAPT", "false")
    assert disk_queue_adapt_from_env() is False
    monkeypatch.setenv("TETRYS_DISK_QUEUE_ADAPT", "1")
    assert disk_queue_adapt_from_env() is True


def test_disk_queue_adapt_grows_when_disk_is_fast_and_hungry():
    lo = 32 * 1024 * 1024
    hi = 64 * 1024 * 1024
    step = 16 * 1024 * 1024
    send = 90 * 1024 * 1024
    # Fast pread + empty queue → grow one step.
    assert (
        disk_queue_adapt_bytes(
            current_bytes=lo,
            min_bytes=lo,
            max_bytes=hi,
            queued_frac=0.10,
            disk_bps=120 * 1024 * 1024,
            send_bps=send,
            available_bytes=4000 * 1024 * 1024,
        )
        == lo + step
    )
    # Slow disk that cannot keep up: stay at the floor even if hungry.
    assert (
        disk_queue_adapt_bytes(
            current_bytes=lo,
            min_bytes=lo,
            max_bytes=hi,
            queued_frac=0.05,
            disk_bps=20 * 1024 * 1024,
            send_bps=send,
            available_bytes=4000 * 1024 * 1024,
        )
        == lo
    )
    # Fast but already half-full: do not keep inflating.
    assert (
        disk_queue_adapt_bytes(
            current_bytes=lo + step,
            min_bytes=lo,
            max_bytes=hi,
            queued_frac=0.40,
            disk_bps=120 * 1024 * 1024,
            send_bps=send,
            available_bytes=4000 * 1024 * 1024,
        )
        == lo + step
    )
    # Slow after a grow: step back so RAM returns to page cache.
    assert (
        disk_queue_adapt_bytes(
            current_bytes=hi,
            min_bytes=lo,
            max_bytes=hi,
            queued_frac=0.80,
            disk_bps=20 * 1024 * 1024,
            send_bps=send,
            available_bytes=4000 * 1024 * 1024,
        )
        == hi - step
    )
    # Ballooned VM: collapse to the floor.
    assert (
        disk_queue_adapt_bytes(
            current_bytes=hi,
            min_bytes=lo,
            max_bytes=hi,
            queued_frac=0.05,
            disk_bps=200 * 1024 * 1024,
            send_bps=send,
            available_bytes=500 * 1024 * 1024,
        )
        == lo
    )
    # Unknown MemAvailable still allows a grow (non-Linux / test hosts).
    assert (
        disk_queue_adapt_bytes(
            current_bytes=lo,
            min_bytes=lo,
            max_bytes=hi,
            queued_frac=0.10,
            disk_bps=120 * 1024 * 1024,
            send_bps=send,
            available_bytes=None,
        )
        == lo + step
    )


def test_disk_feed_cap_hysteresis_and_ewma():
    max_bps = 100_000_000.0
    disk = 20_000_000.0
    # Full queue: network CC owns pace even if we know disk is slower.
    cap, capping = disk_feed_cap_bps(
        queued_frac=0.90,
        disk_bps=disk,
        max_bps=max_bps,
        min_bps=1_000_000.0,
    )
    assert not capping
    assert cap == max_bps
    # Empty queue: cap at disk × headroom.
    cap, capping = disk_feed_cap_bps(
        queued_frac=0.10,
        disk_bps=disk,
        max_bps=max_bps,
        min_bps=1_000_000.0,
    )
    assert capping
    assert 20_000_000.0 < cap < 22_000_000.0
    # Mid-queue without latch: do not cap yet.
    cap, capping = disk_feed_cap_bps(
        queued_frac=0.40,
        disk_bps=disk,
        max_bps=max_bps,
        min_bps=1_000_000.0,
        capping=False,
    )
    assert not capping
    assert cap == max_bps
    # Latch holds until the queue is mostly full again.
    cap, capping = disk_feed_cap_bps(
        queued_frac=0.40,
        disk_bps=disk,
        max_bps=max_bps,
        min_bps=1_000_000.0,
        capping=True,
    )
    assert capping
    assert cap < 22_000_000.0
    cap, capping = disk_feed_cap_bps(
        queued_frac=0.60,
        disk_bps=disk,
        max_bps=max_bps,
        min_bps=1_000_000.0,
        capping=True,
    )
    assert not capping
    assert cap == max_bps
    # Fast disk / warm cache: never cap.
    cap, capping = disk_feed_cap_bps(
        queued_frac=0.05,
        disk_bps=90_000_000.0,
        max_bps=max_bps,
        min_bps=1_000_000.0,
    )
    assert not capping
    assert cap == max_bps
    # No sample yet → do not clamp to zero.
    cap, capping = disk_feed_cap_bps(
        queued_frac=0.0,
        disk_bps=0.0,
        max_bps=max_bps,
        min_bps=1_000_000.0,
    )
    assert not capping
    assert cap == max_bps

    st: dict = {}
    disk_queue_note_read(st, 4_000_000, 1.0, elapsed=0.10)
    assert st["rate_bps"] == pytest.approx(40_000_000.0, rel=0.01)
    # Idle pause after queue-full wait must not smear into MiB/s.
    before = st["rate_bps"]
    disk_queue_note_read(st, 4_000_000, 4.0, elapsed=2.0)
    assert st["rate_bps"] == before
    disk_queue_note_read(st, 4_000_000, 4.1, elapsed=0.20)
    assert st["rate_bps"] > 25_000_000.0

    wall: dict = {}
    disk_queue_note_read(wall, 4_000_000, 1.0)
    assert wall.get("rate_bps", 0.0) == 0.0
    disk_queue_note_read(wall, 4_000_000, 1.02)
    assert wall.get("rate_bps", 0.0) == 0.0
    disk_queue_note_read(wall, 4_000_000, 1.20)
    assert wall["rate_bps"] == pytest.approx(8_000_000.0 / 0.20, rel=0.01)
    # Multi-second gap is idle, not a 2 MiB/s disk.
    prev = wall["rate_bps"]
    disk_queue_note_read(wall, 4_000_000, 4.0)
    assert wall["rate_bps"] == prev


def test_run_file_disk_queue_fills_sequential_batches(tmp_path: Path):
    block = 1000
    batch_n = 4
    n_gens = 12
    data = os.urandom(block * n_gens)
    p = tmp_path / "diskq.bin"
    p.write_bytes(data)
    cond = threading.Condition()
    state: dict = {"blobs": {}, "queued": 0, "off": 0, "err": None}
    cursor = {"gen_id": 0}
    stop = threading.Event()
    t = threading.Thread(
        target=run_file_disk_queue,
        kwargs={
            "path": str(p),
            "file_size": len(data),
            "block_bytes": block,
            "batch_n": batch_n,
            "cursor": cursor,
            "stop": stop,
            "cond": cond,
            "state": state,
            "max_bytes": len(data),
            "chunk_bytes": 8 * block,
        },
        daemon=True,
    )
    t.start()
    with cond:
        ready = cond.wait_for(lambda: len(state["blobs"]) >= 3, timeout=2.0)
    assert ready
    b0 = disk_queue_pop_blob(state, cond, 0)
    assert b0 == data[0 : 4 * block]
    b1 = disk_queue_pop_blob(state, cond, 4)
    assert b1 == data[4 * block : 8 * block]
    stop.set()
    with cond:
        cond.notify_all()
    t.join(timeout=2.0)
    assert state["err"] is None


def test_disk_direct_from_env(monkeypatch):
    monkeypatch.delenv("TETRYS_DISK_DIRECT", raising=False)
    assert disk_direct_from_env() is True
    monkeypatch.setenv("TETRYS_DISK_DIRECT", "0")
    assert disk_direct_from_env() is False
    monkeypatch.setenv("TETRYS_DISK_DIRECT", "1")
    assert disk_direct_from_env() is True


def test_disk_queue_pread_roundtrip(tmp_path: Path):
    data = os.urandom(12_000)
    p = tmp_path / "pread.bin"
    p.write_bytes(data)
    fd, direct = open_disk_queue_fd(str(p))
    try:
        scratch = None
        if direct:
            scratch = mmap.mmap(-1, 16 * 1024)
        try:
            got = disk_queue_pread(
                fd, 4000, 8000, scratch=scratch, direct=direct
            )
        finally:
            if scratch is not None:
                scratch.close()
    finally:
        os.close(fd)
    assert got == data[8000:12000]


def test_run_file_readahead_reads_ahead(tmp_path):
    path = tmp_path / "blob.bin"
    data = os.urandom(64 * 1024)
    path.write_bytes(data)
    cursor = {"gen_id": 0}
    stop = threading.Event()
    run_file_readahead(
        str(path),
        file_size=len(data),
        block_bytes=4096,
        cursor=cursor,
        stop=stop,
        ahead_bytes=len(data),
    )
    assert path.read_bytes() == data


def test_adaptive_inflight_tracks_bdp():
    # 900 Mbit × 80 ms ≈ 8.58 MiB BDP; gain 8 → cap 64.
    wan_bps = 900_000_000 / 8.0
    assert bdp_bytes(wan_bps, 0.080) == pytest.approx(8.58 * 1024 * 1024, rel=0.02)
    assert (
        adaptive_inflight_mib(
            btlbw_bps=wan_bps,
            min_rtt_s=0.080,
            current_mib=64.0,
            mix_up=1.0,
            mix_down=1.0,
        )
        == 64.0
    )
    # Narrower path: 200 Mbit × 80 ms ≈ 1.91 MiB; gain 8 → ~15 MiB.
    slow_bps = 200_000_000 / 8.0
    got = adaptive_inflight_mib(
        btlbw_bps=slow_bps,
        min_rtt_s=0.080,
        current_mib=64.0,
        mix_up=1.0,
        mix_down=1.0,
    )
    assert 14.0 <= got <= 17.0
    # No samples yet: keep the current window.
    assert (
        adaptive_inflight_mib(
            btlbw_bps=0.0, min_rtt_s=0.0, current_mib=64.0
        )
        == 64.0
    )
    # Do not shrink under live occupancy (would pause blast).
    held = adaptive_inflight_mib(
        btlbw_bps=slow_bps,
        min_rtt_s=0.080,
        current_mib=64.0,
        occupancy_bytes=30 * 1024 * 1024,
        mix_up=1.0,
        mix_down=1.0,
    )
    assert held >= 30 / 0.70 - 0.5
    # Mix toward the target rather than jumping in one sample.
    stepped = adaptive_inflight_mib(
        btlbw_bps=slow_bps,
        min_rtt_s=0.080,
        current_mib=64.0,
        mix_down=0.15,
    )
    assert got < stepped < 64.0


def test_should_pause_blast_occupancy_and_lag():
    cap = 1035
    assert not should_pause_blast(100, 200, inflight_gen_limit=cap)
    assert should_pause_blast(cap, 10, inflight_gen_limit=cap)
    # HOL hole: lag at the window with almost nothing unrecovered — keep blasting.
    assert not should_pause_blast(10, cap, inflight_gen_limit=cap)
    soft = (cap * 3) // 4
    assert should_pause_blast(soft, 0, inflight_gen_limit=cap)
    assert not should_pause_blast(soft - 1, cap, inflight_gen_limit=cap)


def test_repair_pressure_and_yield():
    cap = 1000
    assert repair_pressure(100, 50, inflight_gen_limit=cap) == 0.1
    assert repair_pressure(900, 200, inflight_gen_limit=cap) == 0.9
    # Lag-only HOL is not occupancy pressure.
    assert repair_pressure(2, 500, inflight_gen_limit=cap) == 0.002
    assert should_yield_blast_to_repair(0.9, repair_pending=False, nack_count=0)
    assert not should_yield_blast_to_repair(
        0.1, repair_pending=False, nack_count=0
    )
    assert should_yield_blast_to_repair(
        0.5, repair_pending=True, nack_count=20
    )
    assert should_yield_blast_to_repair(
        0.0, repair_pending=False, nack_count=0, hol_hole=64
    )
    assert hol_hole_gens(2, 497) == 495
    assert hol_hole_gens(80, 80) == 0
    # Occupancy pause still ignores lag; HOL pause is a separate brake.
    assert not hol_should_pause_blast(63)
    assert hol_should_pause_blast(64)
    assert hol_should_pause_blast(13000)
    # Enter only when stuck; stay until the hole shrinks (hysteresis).
    assert not hol_pause_should_hold(active=False, hol_hole=200, stuck=False)
    assert hol_pause_should_hold(active=False, hol_hole=200, stuck=True)
    assert hol_pause_should_hold(active=True, hol_hole=40, stuck=False)
    assert not hol_pause_should_hold(active=True, hol_hole=31, stuck=False)


def test_hol_repair_uses_short_cooldown():
    assert hol_repair_cooldown_s(10, 10, hol_miss=True) == _HOL_REPAIR_COOLDOWN_S
    assert hol_repair_cooldown_s(10, 10, hol_miss=False) == _REPAIR_COOLDOWN_S
    assert hol_repair_cooldown_s(11, 10, hol_miss=True) == _REPAIR_COOLDOWN_S
    assert 0.0 < _HOL_REPAIR_COOLDOWN_S < _REPAIR_COOLDOWN_S


def test_reorder_holdoff_gates_repair_feedback():
    deficit: dict[int, float] = {}
    t0 = 100.0
    note_gen_deficit(deficit, 7, t0)
    assert not repair_holdoff_ready(deficit, 7, t0 + 0.002, holdoff_s=0.005)
    assert repair_holdoff_ready(deficit, 7, t0 + 0.006, holdoff_s=0.005)
    clear_gen_deficit(deficit, 7)
    assert not repair_holdoff_ready(deficit, 7, t0 + 1.0, holdoff_s=0.005)
    assert _REORDER_HOLDOFF_S == 0.005


def test_adaptive_blast_overhead_scales_with_pressure():
    cap = 1000
    # Occupancy / HOL / clean-hold must not move FEC: those are BDP, not loss.
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=0,
        nack_count=0,
        incomplete=10,
        inflight_gen_limit=cap,
        clean_s=0.0,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=0,
        nack_count=0,
        incomplete=10,
        inflight_gen_limit=cap,
        clean_s=_OH_CLEAN_HOLD_S,
        floor_pct=8,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=0,
        nack_count=0,
        incomplete=10,
        inflight_gen_limit=cap,
        clean_s=_OH_CLEAN_HOLD_S,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=100,
        nack_count=0,
        incomplete=100,
        inflight_gen_limit=cap,
        max_pct=32,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=500,
        nack_count=0,
        incomplete=2,
        inflight_gen_limit=cap,
        max_pct=32,
    ) == 10
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=80,
        nack_count=40,
        incomplete=40,
        inflight_gen_limit=cap,
        max_pct=32,
        miss_frac=0.0,
    ) == 10
    # Full window without rank misses stays at base (was wrongly 20%).
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=48,
        nack_count=0,
        incomplete=48,
        inflight_gen_limit=64,
        max_pct=32,
    ) == 10
    # Manual 30% is a floor: adapt must not cut it after a 'clean' window.
    assert adaptive_blast_overhead_pct(
        base_pct=30,
        frontier_lag=10,
        nack_count=0,
        incomplete=10,
        inflight_gen_limit=cap,
        clean_s=_OH_CLEAN_HOLD_S,
        max_pct=32,
    ) == 30
    # Rank-deficient aged NACKs raise toward ceil. A short HOL NACK list is
    # the usual feedback shape, so miss_frac alone is the signal.
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=80,
        nack_count=5,
        incomplete=80,
        inflight_gen_limit=cap,
        max_pct=32,
        miss_frac=0.12,
    ) == 21
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=80,
        nack_count=12,
        incomplete=80,
        inflight_gen_limit=cap,
        max_pct=32,
        miss_frac=0.40,
    ) == 32
    assert adaptive_blast_overhead_pct(
        base_pct=10,
        frontier_lag=80,
        nack_count=1,
        incomplete=80,
        inflight_gen_limit=cap,
        max_pct=32,
        miss_frac=1.0,
    ) == 32
    assert adaptive_blast_overhead_pct(
        base_pct=0,
        frontier_lag=100,
        nack_count=50,
        incomplete=500,
        inflight_gen_limit=cap,
        miss_frac=1.0,
    ) == 0


def test_blast_fec_miss_frac_uses_rank_not_occupancy():
    k = 96
    rx = {0: 40, 1: 50, 2: 90, 3: 98}
    assert blast_fec_miss_frac(rx, [0, 1, 2, 3], gen_k=k) == 0.75
    assert blast_fec_miss_frac(rx, [], gen_k=k) == 0.0
    assert blast_fec_miss_frac({}, [7, 8], gen_k=k) == 1.0
    assert blast_fec_miss_frac({7: 98, 8: 99}, [7, 8], gen_k=k) == 0.0


def test_echo_rtt_and_delay_pace():
    now = 1000.0
    echo = int((now - 0.100) * 1_000_000) & 0xFFFFFFFF
    rtt = echo_rtt_s(echo, now)
    assert rtt is not None
    assert abs(rtt - 0.100) < 0.002
    assert echo_rtt_s(0, now) is None
    # Stale / stall echo (> max RTT) ignored.
    stale = int((now - 3.000) * 1_000_000) & 0xFFFFFFFF
    assert echo_rtt_s(stale, now) is None
    max_bps = 100_000_000.0
    # Standing queue → back off.
    bps, min_rtt, q = update_delay_pace_bps(
        rtt_s=0.120,
        min_rtt_s=0.080,
        cur_bps=max_bps,
        max_bps=max_bps,
    )
    assert q == pytest.approx(0.040)
    assert bps < max_bps
    assert bps >= max_bps * 0.12
    # Empty queue while probing → climb faster.
    bps2, _, q2 = update_delay_pace_bps(
        rtt_s=0.081,
        min_rtt_s=0.080,
        cur_bps=30_000_000.0,
        max_bps=max_bps,
    )
    assert q2 < 0.005
    assert bps2 > 30_000_000.0 * 1.04
    assert min_rtt > 0
    # Near ceiling → gentler climb.
    bps3, _, _ = update_delay_pace_bps(
        rtt_s=0.081,
        min_rtt_s=0.080,
        cur_bps=80_000_000.0,
        max_bps=max_bps,
    )
    assert bps3 == pytest.approx(80_000_000.0 * 1.08)
    # Thresholds scale with base RTT: 40ms queue is acceptable on a 300ms path.
    long_bps, _, long_q = update_delay_pace_bps(
        rtt_s=0.340,
        min_rtt_s=0.300,
        cur_bps=30_000_000.0,
        max_bps=max_bps,
    )
    assert long_q == pytest.approx(0.040)
    assert long_bps > 30_000_000.0
    # The same queue is severe on a low-latency path.
    short_bps, _, _ = update_delay_pace_bps(
        rtt_s=0.050,
        min_rtt_s=0.010,
        cur_bps=80_000_000.0,
        max_bps=max_bps,
    )
    assert short_bps < 80_000_000.0


def test_smooth_delay_rtt_ignores_isolated_spikes():
    ewma, spike, n = smooth_delay_rtt_s(0.075, 0.0)
    assert not spike and n == 0
    assert ewma == pytest.approx(0.075)
    # Today's WAN blip: 75 → 187 ms must not pull the filter.
    ewma2, spike2, n2 = smooth_delay_rtt_s(0.187, ewma)
    assert spike2 and n2 == 1
    assert ewma2 == pytest.approx(0.075)
    ewma3, spike3, n3 = smooth_delay_rtt_s(0.080, ewma2, n2)
    assert not spike3 and n3 == 0
    assert abs(ewma3 - 0.075) < 0.01
    # Sustained jump is real delay: after 3 outliers, mix in.
    ewma_s = 0.075
    streak = 0
    for _ in range(3):
        ewma_s, is_spike, streak = smooth_delay_rtt_s(0.187, ewma_s, streak)
    assert not is_spike
    assert ewma_s > 0.075
    assert ewma_s < 0.187
    # Modest standing queue is not a spike.
    slow, spike_slow, _ = smooth_delay_rtt_s(0.095, 0.075)
    assert not spike_slow
    assert 0.075 < slow < 0.095


def test_delivery_guard_cuts_overload_not_below_floor():
    floor = 25_000_000.0  # 200 Mbit
    # 900 Mbit pace, ~13 MiB/s goodput (today's 33% loss collapse).
    cur = 112_500_000.0
    new, why = update_delivery_guard_bps(
        delivery_bps=13.4 * 1024 * 1024,
        cur_bps=cur,
        overhead_pct=10,
        min_bps=floor,
    )
    assert why == "cut_hard"
    assert new == pytest.approx(cur * 0.55)
    assert new > floor
    # After two hard cuts we sit near 300 Mbit, not 8 Mbit.
    new2, _ = update_delivery_guard_bps(
        delivery_bps=13.4 * 1024 * 1024,
        cur_bps=new,
        overhead_pct=10,
        min_bps=floor,
    )
    new3, _ = update_delivery_guard_bps(
        delivery_bps=13.4 * 1024 * 1024,
        cur_bps=new2,
        overhead_pct=10,
        min_bps=floor,
    )
    mbit = new3 * 8 / 1_000_000
    assert 200 <= mbit <= 400
    # Healthy 400 Mbit slice: delivery matches expected app → hold.
    ok, why_ok = update_delivery_guard_bps(
        delivery_bps=43.0 * 1024 * 1024,
        cur_bps=50_000_000.0,
        overhead_pct=10,
        min_bps=floor,
    )
    assert why_ok == "ok"
    assert ok == 50_000_000.0
    # Clean path: limiter at 900 Mbit but wire/app already match → do not cut.
    hold, why_hold = update_delivery_guard_bps(
        delivery_bps=75.0 * 1024 * 1024,
        cur_bps=112_500_000.0,
        overhead_pct=10,
        min_bps=floor,
        wire_bps=82.0 * 1024 * 1024,
    )
    assert why_hold == "hold"
    assert hold == 112_500_000.0
    # Same 75 MiB/s vs the 900 Mbit *target* would look like a miss without wire.
    cut_pace, why_pace = update_delivery_guard_bps(
        delivery_bps=75.0 * 1024 * 1024,
        cur_bps=112_500_000.0,
        overhead_pct=10,
        min_bps=floor,
    )
    assert why_pace == "hold" or why_pace == "ok"
    # Paused at inflight cap: tiny wire must not hide a 2 MiB/s collapse.
    cap_cut, why_cap = update_delivery_guard_bps(
        delivery_bps=2.0 * 1024 * 1024,
        cur_bps=112_500_000.0,
        overhead_pct=20,
        min_bps=floor,
        wire_bps=4.0 * 1024 * 1024,
        window_full=True,
    )
    assert why_cap == "cut_hard"
    assert cap_cut == pytest.approx(112_500_000.0 * 0.55)


def test_delay_cc_blocks_probe_on_full_pipeline():
    cap = 517
    assert delay_cc_may_probe(70, cap)
    pause = (cap * 3) // 4
    assert delay_cc_may_probe(pause - 1, cap)
    assert not delay_cc_may_probe(pause, cap)
    assert not delay_cc_may_probe(387, cap)


def test_bbr_gain_and_delivery_rate_pace():
    t0 = 1000.0
    rt = 0.080
    assert bbr_pacing_gain(t0, rt, t0) == pytest.approx(1.25)
    assert bbr_pacing_gain(t0 + rt + 0.001, rt, t0) == pytest.approx(0.75)
    assert bbr_pacing_gain(t0 + 2.5 * rt, rt, t0) == pytest.approx(1.00)
    floor = 25_000_000.0
    cap = 112_500_000.0
    samples: list[tuple[float, float]] = []
    btlbw, samples = update_btlbw_bps(
        delivery_bps=40.0 * 1024 * 1024,
        samples=samples,
        now_s=t0,
        rtprop_s=rt,
    )
    assert btlbw == pytest.approx(40.0 * 1024 * 1024)
    paced = update_delivery_rate_pace_bps(
        btlbw_bps=btlbw, max_bps=cap, min_bps=floor, gain=1.25
    )
    assert paced == pytest.approx(min(cap, btlbw * 1.25))
    drained = update_delivery_rate_pace_bps(
        btlbw_bps=btlbw, max_bps=cap, min_bps=floor, gain=0.75
    )
    assert drained < paced
    # Drain must not rewrite BtlBw: cruise returns to the high watermark.
    cruise = update_delivery_rate_pace_bps(
        btlbw_bps=btlbw, max_bps=cap, min_bps=floor, gain=1.00
    )
    assert cruise == pytest.approx(btlbw)
    # STARTUP climbs off the 200 Mbit floor even if first samples were slow.
    climb = update_delivery_rate_pace_bps(
        btlbw_bps=20.0 * 1024 * 1024, max_bps=cap, min_bps=floor, gain=2.00
    )
    assert climb >= floor * 1.90


def test_btlbw_max_filter_forgets_a_bad_start():
    rt = 0.080
    samples: list[tuple[float, float]] = []
    slow = 20.0 * 1024 * 1024
    fast = 80.0 * 1024 * 1024
    btlbw, samples = update_btlbw_bps(
        delivery_bps=slow, samples=samples, now_s=0.0, rtprop_s=rt
    )
    assert btlbw == pytest.approx(slow)
    btlbw, samples = update_btlbw_bps(
        delivery_bps=fast, samples=samples, now_s=0.20, rtprop_s=rt
    )
    assert btlbw == pytest.approx(fast)
    # 10 RTprop is only 0.8s here; the WAN floor is 3s so a delay bump
    # cannot expire a good probe. After that floor the slow start is gone.
    btlbw, samples = update_btlbw_bps(
        delivery_bps=fast, samples=samples, now_s=3.20, rtprop_s=rt
    )
    assert btlbw == pytest.approx(fast)
    assert all(r >= fast * 0.99 for _, r in samples)


def test_bbr_startup_needs_sustained_growth():
    assert bbr_still_startup(
        startup=True, btlbw_bps=40e6, round_btlbw_bps=0.0
    )
    assert bbr_still_startup(
        startup=True, btlbw_bps=40e6, round_btlbw_bps=25e6
    )
    assert not bbr_still_startup(
        startup=True, btlbw_bps=26e6, round_btlbw_bps=25e6
    )
    assert not bbr_still_startup(
        startup=False, btlbw_bps=80e6, round_btlbw_bps=25e6
    )
    # Empty pipeline: keep STARTUP even if BtlBw looks flat.
    assert bbr_still_startup(
        startup=True,
        btlbw_bps=26e6,
        round_btlbw_bps=25e6,
        occupancy=10,
        inflight_gen_limit=64,
    )
    assert not bbr_still_startup(
        startup=True,
        btlbw_bps=26e6,
        round_btlbw_bps=25e6,
        occupancy=40,
        inflight_gen_limit=64,
    )


def test_client_feedback_interval_is_fast_while_open():
    assert client_feedback_interval(
        fin_seen=False,
        next_needed=0,
        total_gens=100,
        open_gens=1,
        nack_count=1,
    ) == 0.02
    assert client_feedback_interval(
        fin_seen=True,
        next_needed=100,
        total_gens=100,
        open_gens=0,
        nack_count=0,
    ) == 0.05
    assert client_feedback_interval(
        fin_seen=True,
        next_needed=90,
        total_gens=100,
        open_gens=3,
        nack_count=3,
    ) == 0.02


def test_repair_thread_limit_stays_thin():
    assert repair_thread_limit(
        at_cap=True,
        storm_active=True,
        nack_count=64,
        frontier_lag=900,
        inflight_gen_limit=1035,
    ) == 2
    assert repair_thread_limit(
        at_cap=True,
        storm_active=True,
        nack_count=2,
        frontier_lag=10,
        inflight_gen_limit=1035,
    ) == 1
    lim = repair_thread_limit(
        at_cap=True,
        storm_active=False,
        nack_count=64,
        frontier_lag=900,
        inflight_gen_limit=1035,
    )
    assert lim == 8
    assert repair_thread_limit(
        at_cap=False,
        storm_active=False,
        nack_count=64,
        frontier_lag=900,
        inflight_gen_limit=1035,
    ) == 4


def test_even_spread_covers_window():
    items = list(range(10, 40))
    got = even_spread(items, 6)
    assert len(got) == 6
    assert got[0] == 10
    assert max(got) >= 34
    shifted = even_spread(items, 6, offset=5)
    assert shifted != got
    assert even_spread(items, 50) == items
    assert even_spread([], 4) == []


def test_repair_send_n_deficit_and_probe():
    k = 96
    # Unknown rank: absolute pad + probe, not 2 fountain tokens.
    assert repair_send_n(None, k, hol=True) == 6
    assert repair_send_n(k + 2, k) == 0
    # deficit 4 (enough=98, rx=94) + abs pad 4 → 8, not 10% of 4.
    assert repair_extra_n(4, overhead_pct=10) == 4
    assert repair_extra_n(4, overhead_pct=20) == 4
    assert repair_extra_n(40, overhead_pct=20) == 8
    assert repair_send_n(94, k, hol=False) == 8
    assert repair_send_n(94, k, hol=True) == 8
    assert repair_send_n(94, k, hol=False, overhead_pct=10) == 8
    assert repair_send_n(94, k, hol=True, overhead_pct=10) == 8
    assert repair_send_n(97, k, hol=False, overhead_pct=10) == 5
    assert repair_send_n(94, k, hol=False, overhead_pct=20) == 8
    assert repair_send_n(94, k, hol=True, close=True, overhead_pct=10) == 8
    assert repair_overhead_pct(10) == 10
    assert repair_overhead_pct(0) == 8
    assert hol_resend_pad_n(0, 10) == 0
    assert hol_resend_pad_n(1, 10) == 4
    assert hol_resend_pad_n(4, 10) == 4
    assert hol_resend_pad_n(10, 20) == 4
    assert hol_resend_pad_n(40, 20) == 8
    hist = [0, 0, 0, 0, 0]
    note_close_round(hist, 0)
    note_close_round(hist, 1)
    note_close_round(hist, 1)
    note_close_round(hist, 3)
    note_close_round(hist, 9)
    assert hist == [2, 0, 1, 0, 1]
    assert "r1=2" in format_close_rounds(hist, label="nack_close")
    assert "n=4" in format_close_rounds(hist, label="nack_close")
    # 20ms ACK ticks over one 80ms RTT must not look like r5+.
    assert nack_close_rounds(0.10, 0.081) == 1
    assert nack_close_rounds(0.14, 0.081) == 2
    assert nack_close_rounds(7 * 0.02, 0.081) == 2
    assert nack_close_rounds(0.0, None) == 1
    assert repair_send_n(0, k, hol=True) == 16
    assert repair_send_n(0, k, hol=False) == 16
    assert gen_rank_deficit(None, k) is None
    assert gen_rank_deficit(90, k) == 8
    assert hol_blocks_tail_repair(20, k)
    assert not hol_blocks_tail_repair(94, k)
    assert not hol_blocks_tail_repair(None, k)


def test_select_repair_feedback_gens_hol_then_cheap():
    k = 192
    open_rx = {g: 50 for g in range(10, 40)}
    open_rx[10] = 20  # deep HOL
    open_rx[25] = 190  # near-complete
    open_rx[22] = 10  # largest deficit
    got = select_repair_feedback_gens(10, open_rx, gen_k=k, limit=8)
    assert got[0] == 10
    assert 25 in got
    assert max(got) >= 28
    open_rx[10] = 190  # HOL almost closed
    cheap = select_repair_feedback_gens(10, open_rx, gen_k=k, limit=8)
    assert cheap[0] == 10
    assert 25 in cheap


def test_order_repair_nacks_blocks_tail_on_deep_hol():
    nacks = list(range(10, 40))
    nack_rx = {g: 50 for g in nacks}
    nack_rx[10] = 20
    nack_rx[25] = 190
    got = order_repair_nacks(
        nacks, 10, nack_rx, gen_k=192, limit=8, sent_before=80
    )
    assert got[0] == 10
    assert 25 in got
    assert max(got) >= 28
    loose = order_repair_nacks(
        nacks, 10, nack_rx, gen_k=192, limit=4, sent_before=80
    )
    assert loose[0] == 10
    assert len(loose) == 4


def test_fountain_targets_hol_first_no_window_walk():
    assert fountain_targets(5, 20, nacks=[], nack_rx={}, gen_k=192, limit=3) == [5]
    nacks = list(range(10, 30))
    nack_rx = {g: 50 for g in nacks}
    nack_rx[10] = 20
    nack_rx[25] = 190
    nack_rx[22] = 10
    stuck = fountain_targets(
        10, 40, nacks=nacks, nack_rx=nack_rx, gen_k=192, limit=8
    )
    assert stuck[0] == 10
    assert 25 in stuck
    assert max(stuck) >= 24
    nack_rx[10] = 192
    open_hol = fountain_targets(
        10, 40, nacks=nacks, nack_rx=nack_rx, gen_k=192, limit=8
    )
    assert open_hol[0] == 10
    assert 25 in open_hol


def test_build_feedback_miss_bitmap_gap_aware():
    done = {0, 1, 3}
    decoders = {5: object()}

    def bit_get(i: int) -> bool:
        return i in done

    bitmap = build_feedback_miss_bitmap(
        next_needed=0,
        total_gens=20,
        horizon=16,
        bit_get=bit_get,
        decoders=decoders,
        max_gid_seen=7,
    )
    missing = miss_bitmap_to_nacks(0, bitmap)
    assert 2 in missing  # gap before max_gid_seen
    assert 5 in missing  # partial decoder
    assert 6 in missing  # gap before max_gid_seen
    assert 8 not in missing  # beyond max_gid_seen, still in flight


def test_build_feedback_miss_bitmap_include_unseen_after_fin():
    done = {0, 1, 3}
    decoders = {5: object()}

    def bit_get(i: int) -> bool:
        return i in done

    bitmap = build_feedback_miss_bitmap(
        next_needed=0,
        total_gens=20,
        horizon=16,
        bit_get=bit_get,
        decoders=decoders,
        max_gid_seen=7,
        include_unseen=True,
    )
    missing = miss_bitmap_to_nacks(0, bitmap)
    assert 2 in missing
    assert 8 in missing  # never-seen tail after FIN
    assert 15 in missing
    assert 0 not in missing
    assert 1 not in missing
    assert 3 not in missing


def test_drain_scoreboard_targets_bitmap_and_never_seen():
    # HOL + bitmap holes in window; never-seen fills the rest of the window.
    targets = drain_scoreboard_targets(
        next_needed=100,
        total_gens=200,
        miss_nacks=[100, 103, 180],
        never_seen=110,
        window=16,
    )
    assert targets[0] == 100
    assert 103 in targets
    assert 180 not in targets  # outside window
    assert 110 in targets
    assert 115 in targets
    assert max(targets) == 115
    # Complete file: nothing to drain.
    assert drain_scoreboard_targets(
        next_needed=200,
        total_gens=200,
        miss_nacks=[199],
        never_seen=200,
    ) == []
    # Legacy FB without never_seen still repairs HOL + nacks.
    legacy = drain_scoreboard_targets(
        next_needed=50,
        total_gens=80,
        miss_nacks=[50, 52],
        never_seen=None,
        window=8,
    )
    assert legacy == [50, 52]


def test_drain_epoch_is_stale_and_never_seen_frontier():
    assert not drain_epoch_is_stale(0, 4)
    assert not drain_epoch_is_stale(5, 0)
    assert not drain_epoch_is_stale(5, 5)
    assert drain_epoch_is_stale(3, 5)
    assert not drain_epoch_is_stale(6, 5)
    assert drain_epoch_is_stale(0xFFFF, 1)
    assert not drain_epoch_is_stale(1, 0xFFFF)
    assert drain_never_seen_frontier(10, 9, 100) == 10
    assert drain_never_seen_frontier(10, 40, 100) == 41
    assert drain_never_seen_frontier(99, 200, 100) == 100


def test_drain_repair_send_n_closes_empty_gen_in_one_round():
    cap = drain_empty_round_max(96)
    assert cap == 96 + 2 + 4
    assert drain_repair_send_n(None, 96, empty=True) == cap
    assert drain_repair_send_n(0, 96, empty=False) == cap
    # Partial hole stays a patch, not a full reblast.
    n = drain_repair_send_n(90, 96, empty=False)
    assert 1 <= n < cap
    assert n >= (96 - 90)


def test_pipeline_stressed_clean_vs_pressure():
    cap = 258
    threshold = int(cap * 3 / 4)  # 193
    # Healthy path: small lag, inflight below 3/4 cap.
    assert not pipeline_stressed(40, 35, inflight_gen_limit=cap)
    assert not pipeline_stressed(threshold - 1, 45, inflight_gen_limit=cap)
    assert pipeline_stressed(threshold, 10, inflight_gen_limit=cap)
    # Client lag / BDP without high unrecovered occupancy is not stress.
    assert not pipeline_stressed(100, 65, inflight_gen_limit=cap)
    assert not pipeline_stressed(100, 200, inflight_gen_limit=cap)


def test_should_fountain_tick_skips_clean_path():
    for tick in range(20):
        assert not should_fountain_tick(stressed=False, at_cap=False, tick_n=tick)
        assert not should_fountain_tick(stressed=False, at_cap=True, tick_n=tick)
        # Reorder NACKs on a thin pipeline must not start fountain.
        assert not should_fountain_tick(
            stressed=False,
            at_cap=False,
            tick_n=tick,
            pressure=0.10,
            nack_count=64,
        )


def test_should_fountain_tick_on_occupancy_nacks():
    ticks = [
        should_fountain_tick(
            stressed=False,
            at_cap=False,
            tick_n=n,
            every_n=4,
            pressure=0.15,
            nack_count=40,
        )
        for n in range(8)
    ]
    assert ticks == [True, False, False, False, True, False, False, False]
    assert should_fountain_tick(
        stressed=False,
        at_cap=True,
        tick_n=1,
        pressure=0.15,
        nack_count=40,
    )
    assert not should_fountain_tick(
        stressed=False,
        at_cap=True,
        tick_n=1,
        pressure=0.10,
        nack_count=40,
    )


def test_update_adaptive_pace_backoff_only():
    max_bps = 100_000_000.0
    # Healthy delivery: stay at max.
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=95_000_000.0,
        cur_bps=max_bps,
        max_bps=max_bps,
        good_streak=0,
    )
    assert bps == max_bps
    assert streak == 1
    assert bad == 0
    # One lagging sample: hold pace (bad streak not reached).
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=20_000_000.0,
        cur_bps=max_bps,
        max_bps=max_bps,
        good_streak=0,
    )
    assert bps == max_bps
    assert bad == 1
    # Sustained lag: back off.
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=20_000_000.0,
        cur_bps=max_bps,
        max_bps=max_bps,
        good_streak=0,
        bad_streak=2,
    )
    assert bps < max_bps
    assert bad == 0
    # Recovery ramps toward max after good samples.
    bps, streak, bad = update_adaptive_pace_bps(
        delivery_bps=95_000_000.0,
        cur_bps=50_000_000.0,
        max_bps=max_bps,
        good_streak=2,
    )
    assert bps == max_bps


def test_should_fountain_tick_fountain_mode():
    for tick in range(8):
        assert should_fountain_tick(
            stressed=False,
            at_cap=True,
            tick_n=tick,
            fountain_mode=True,
        )
    off = [
        should_fountain_tick(
            stressed=False,
            at_cap=False,
            tick_n=n,
            fountain_mode=True,
            every_n=4,
        )
        for n in range(8)
    ]
    assert off == [True, False, False, False, True, False, False, False]


def test_should_fountain_tick_stressed_throttles_off_cap():
    ticks = [
        should_fountain_tick(stressed=True, at_cap=False, tick_n=n, every_n=4)
        for n in range(12)
    ]
    assert ticks == [True, False, False, False, True, False, False, False, True, False, False, False]


def test_should_fountain_tick_stressed_at_cap_every_turn():
    for tick in range(8):
        assert should_fountain_tick(stressed=True, at_cap=True, tick_n=tick)


def test_should_fountain_tick_disabled_during_storm_unless_cap_stressed():
    assert not should_fountain_tick(
        stressed=False, at_cap=True, tick_n=0, storm_active=True
    )
    assert should_fountain_tick(
        stressed=True, at_cap=True, tick_n=0, storm_active=True
    )


def test_cap_fountain_gens_drops_oldest():
    s = set(range(100, 200))
    cap_fountain_gens(s, next_needed=100, max_track=64)
    assert len(s) == 64
    assert min(s) == 136
    assert max(s) == 199


def test_prune_fountain_gens_set_window_and_inflight():
    s = {10, 20, 50, 80, 120}
    prune_fountain_gens_set(
        s,
        next_needed=20,
        nack_rx={50: 200},
        nacks=[],
        gen_k=192,
        sent_before=100,
        inflight_gen_limit=258,
        window=48,
    )
    assert 10 not in s
    assert 50 not in s  # enough symbols
    assert 20 in s
    assert 80 not in s  # beyond next_needed + window (68)
    assert 120 not in s


def test_should_track_fountain_gen_clean_vs_cap():
    assert not should_track_fountain_gen(500, 100, at_cap=False)
    assert should_track_fountain_gen(103, 100, at_cap=False)
    assert should_track_fountain_gen(140, 100, at_cap=True)
    assert not should_track_fountain_gen(200, 100, at_cap=True)


def test_cap_fountain_send_budget_and_deficit():
    k = 192
    budget = repair_round_size(k, 0)
    assert cap_fountain_send(k, 0, budget, None) == budget
    assert cap_fountain_send(k, budget, budget, None) == 4
    assert cap_fountain_send(k, k, 8, None) == 0
    assert cap_fountain_send(k, budget, 8, 100) > 0


def test_fountain_redundancy_and_repair_round():
    assert fountain_redundancy(0)
    assert not fountain_redundancy(8)
    assert repair_round_size(192, 0) == repair_round_size(192, 8)


def test_track_fountain_gen_respects_cap():
    s: set[int] = set()
    for gid in range(1000):
        track_fountain_gen(s, gid, next_needed=900, at_cap=True)
    assert len(s) <= _FOUNTAIN_TRACK_MAX
    assert min(s) >= 900


def test_repair_storm_detected():
    assert repair_storm_detected(30_000, 10)
    assert repair_storm_detected(100, _FOUNTAIN_TRACK_MAX * 2 + 1)
    assert not repair_storm_detected(1000, 10)


def test_prune_repair_meta_bounds_dicts():
    extra = {i: i for i in range(500)}
    ts: dict[int, float] = {i: 1.0 for i in range(500)}
    full: dict[int, float] = {}
    prune_repair_meta(extra, ts, full, next_needed=400, keep=_REPAIR_META_KEEP)
    assert all(k >= 400 for k in extra)
    assert len(extra) <= _REPAIR_META_KEEP


def test_sync_repair_in_send_loop_blocks_like_old_pipeline():
    """Model the old pipeline: repair_one().result() inside every send iteration."""

    def sync_send_loop(*, gens: int, repair_s: float, send_s: float) -> float:
        t0 = time.monotonic()
        for _ in range(gens):
            time.sleep(repair_s)
            time.sleep(send_s)
        return time.monotonic() - t0

    def async_send_loop(*, gens: int, repair_s: float, send_s: float) -> float:
        t0 = time.monotonic()
        pool = ThreadPoolExecutor(max_workers=4)
        pending: list = []
        try:
            for _ in range(gens):
                pending.append(pool.submit(time.sleep, repair_s))
                time.sleep(send_s)
                still = []
                for fut in pending:
                    if fut.done():
                        fut.result()
                    else:
                        still.append(fut)
                pending = still
            for fut in pending:
                fut.result()
        finally:
            pool.shutdown(wait=True)
        return time.monotonic() - t0

    gens = 24
    repair_s = 0.006
    send_s = 0.001
    sync_t = sync_send_loop(gens=gens, repair_s=repair_s, send_s=send_s)
    async_t = async_send_loop(gens=gens, repair_s=repair_s, send_s=send_s)
    # Old pattern pays full repair latency every iteration; async overlaps encode.
    assert async_t < sync_t * 0.55
    assert sync_t > gens * repair_s * 0.9


raptorq = pytest.importorskip("raptorq")


def test_encode_worker_bootstrap_in_initial_blast(tmp_path: Path):
    """Bootstrap repair must ride in the first blast, not as a later repair flood."""
    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    path = tmp_path / "block.bin"
    path.write_bytes(bytes((i * 7) & 0xFF for i in range(block)))

    enc_cpu_s, src_bytes, wires, bootstrap = _encode_gen_worker(
        str(path),
        0,
        block,
        block,
        T,
        8,
    )
    assert src_bytes == block
    assert enc_cpu_s >= 0.0
    assert bootstrap == blast_repair_budget(K, 8)
    assert bootstrap > 0
    assert len(wires) >= K + bootstrap

    parsed = [GenPacket.unpack(w) for w in wires]
    esis = {p.esi for p in parsed}
    assert len(esis) == len(wires)
    assert max(esis) >= K + bootstrap - 1


def test_encode_gens_worker_reads_consecutive_blocks(tmp_path: Path):
    """One worker task should mmap a multi-gen slice, not one gen per submit."""
    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    n = 3
    path = tmp_path / "blocks.bin"
    path.write_bytes(bytes((i * 5) & 0xFF for i in range(block * n)))

    out = _encode_gens_worker(str(path), 0, n, block, block * n, T, 8, False)
    assert len(out) == n
    for i, (enc_cpu_s, src_bytes, wires, bootstrap) in enumerate(out):
        assert src_bytes == block
        assert enc_cpu_s >= 0.0
        assert bootstrap == blast_repair_budget(K, 8)
        assert len(wires) >= K + bootstrap
        parsed = [GenPacket.unpack(w) for w in wires]
        assert all(p.gen_id == i for p in parsed)


def test_drain_encode_out_queue_moves_inflight_to_ready():
    src: queue.Queue = queue.Queue()
    ready: dict = {}
    inflight = {0, 1, 2}
    src.put((1, (0.1, 10, [b"a"], 0)))
    src.put((2, (0.2, 20, [b"b"], 1)))
    n = drain_encode_out_queue(src, ready, inflight)
    assert n == 2
    assert set(ready) == {1, 2}
    assert inflight == {0}


def test_encode_gens_worker_stream_puts_each_gen(tmp_path: Path):
    """Batch read must publish gen 0 before the remaining gens are encoded."""
    import tetrys_nc.gen_xfer as gx

    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    n = 3
    path = tmp_path / "stream.bin"
    path.write_bytes(bytes((i * 9) & 0xFF for i in range(block * n)))
    q: queue.Queue = queue.Queue()
    old = gx._encode_out_q
    gx._encode_out_q = q
    try:
        got = _encode_gens_worker_stream(
            str(path), 0, n, block, block * n, T, 8, False
        )
    finally:
        gx._encode_out_q = old
    assert got == n
    gids = []
    while not q.empty():
        gid, item = q.get_nowait()
        gids.append(gid)
        enc_cpu_s, src_bytes, wires, bootstrap = item
        assert src_bytes == block
        assert enc_cpu_s >= 0.0
        assert bootstrap == blast_repair_budget(K, 8)
        assert len(wires) >= K + bootstrap
        parsed = [GenPacket.unpack(w) for w in wires]
        assert all(p.gen_id == gid for p in parsed)
    assert gids == [0, 1, 2]


def test_encode_blob_worker_stream_does_not_need_path(tmp_path: Path):
    import tetrys_nc.gen_xfer as gx

    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    n = 3
    blob = bytes((i * 13) & 0xFF for i in range(block * n))
    q: queue.Queue = queue.Queue()
    old = gx._encode_out_q
    gx._encode_out_q = q
    try:
        got = _encode_blob_worker_stream(blob, 0, n, block, block * n, T, 8, False)
    finally:
        gx._encode_out_q = old
    assert got == n
    assert [q.get_nowait()[0] for _ in range(n)] == [0, 1, 2]


def test_encode_worker_fountain_mode_bootstrap(tmp_path: Path):
    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    path = tmp_path / "block0.bin"
    path.write_bytes(bytes((i * 11) & 0xFF for i in range(block)))

    _enc_cpu_s, src_bytes, wires, bootstrap = _encode_gen_worker(
        str(path),
        0,
        block,
        block,
        T,
        0,
        True,
    )
    assert src_bytes == block
    assert bootstrap == fountain_blast_budget(K, 8)
    assert bootstrap > 0
    assert len(wires) >= K + bootstrap


def test_encode_worker_process_pool_parallel(tmp_path: Path):
    """Process pool should encode multiple gens without serializing on the GIL."""
    from concurrent.futures import ProcessPoolExecutor
    import multiprocessing

    require_raptorq()
    T = 512
    K = 16
    block = K * T
    path = tmp_path / "pool.bin"
    path.write_bytes(bytes((i * 3) & 0xFF for i in range(block * 3)))

    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=3, mp_context=ctx) as pool:
        futs = [
            pool.submit(_encode_gen_worker, str(path), gid, block, block * 3, T, 8)
            for gid in range(3)
        ]
        out = [f.result(timeout=60.0) for f in futs]

    for enc_cpu_s, src_bytes, wires, bootstrap in out:
        assert enc_cpu_s >= 0.0
        assert src_bytes == block
        assert len(wires) >= K
        assert bootstrap == blast_repair_budget(K, 8)


def test_repair_flood_regression_old_vs_throttled_fountain():
    """Old pipeline ticked fountain every send-loop turn; hybrid throttles it."""
    inflight_cap = 258
    ticks = 200
    old_submits = 0
    new_submits = 0
    for n in range(ticks):
        incomplete = 200
        lag = 80
        stressed = pipeline_stressed(incomplete, lag, inflight_gen_limit=inflight_cap)
        assert stressed
        at_cap = incomplete >= inflight_cap
        old_submits += _FOUNTAIN_CAP_GENS
        if should_fountain_tick(stressed=stressed, at_cap=at_cap, tick_n=n):
            new_submits += _FOUNTAIN_CAP_GENS
    assert old_submits == ticks * _FOUNTAIN_CAP_GENS
    assert new_submits == (ticks // _FOUNTAIN_EVERY_N) * _FOUNTAIN_CAP_GENS
    assert new_submits < old_submits // 3


def test_bootstrap_avoids_extra_repair_round_on_clean_gen():
    """Initial blast carries bootstrap repair; repair_extra starts non-zero."""
    require_raptorq()
    T = 1350
    K = 48
    block = K * T
    data = bytes((i * 13) & 0xFF for i in range(block))
    enc = GenEncoder(data, T, 8, systematic_only=True)
    bootstrap = blast_repair_budget(K, 8)
    extra = enc.ensure_repair(bootstrap)
    assert bootstrap > 0
    assert len(enc.packets()) >= K + bootstrap
    assert len(extra) == bootstrap
