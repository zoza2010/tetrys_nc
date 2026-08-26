"""v1 generation transfer — unused by CLI.

The live server/client path is ``block_xfer`` (protocol v2). This module
stays for unit tests of shared helpers and historical WAN diagnostics.
Do not add new transfer logic here.
"""

from __future__ import annotations

import hashlib
import math
import mmap
import multiprocessing
import os
import queue
import select
import shutil
import socket
import sys
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path

from .encbench import bench_encode
from .gen_raptor import (
    GenDecoder,
    GenEncoder,
    GenReceiveSlot,
    blast_repair_budget,
    disk_spool_enabled,
    fountain_blast_budget,
    repair_count,
    require_raptorq,
)
from .hostcpu import HostCpuSampler
from .ratectl import RateLimiter
from .netutil import (
    recv_datagrams,
    send_datagrams,
    take_send_stats,
    try_set_buffer,
)
from .packets import (
    MAGIC,
    PKT_FIN,
    PKT_GEN,
    PKT_GEN_FB,
    PKT_META,
    PKT_READY,
    XFER_GEN,
    FEEDBACK_BITMAP_MAX_BYTES,
    GEN_HDR_SIZE,
    FinPacket,
    GenFeedbackPacket,
    GenPacket,
    MetaPacket,
    ReadyPacket,
    merge_feedback_nacks,
    miss_bitmap_to_nacks,
    parse_packet,
    stamp_gen_wire,
)
from .ratectl import RateLimiter

# Raptor encoders are heavy (hundreds of KiB to MiB each). Keep a bounded
# working set; old generations can be reconstructed from mmap for repair.
_ENCODER_KEEP = 128
# Do not re-repair the same gen on every send-loop iteration.
_REPAIR_COOLDOWN_S = 0.10
# Sparse HOL source resend: faster than a full RTT, never zero (that floods).
_HOL_REPAIR_COOLDOWN_S = 0.05
# Wait for late/reordered packets before NACK / hol_miss (path OOO is often <10ms).
# WAN sweep: 5ms ≈81 MiB/s, 8ms ≈54, 12ms ≈71, 20ms stalled/disk issues.
_REORDER_HOLDOFF_S = 0.005
# Start frontier repair when client lags this many gens behind send cursor.
_REPAIR_LAG = 48
# If next_needed is unchanged this long, send deficit-sized fountain repair.
_STUCK_S = 0.20
# Min gap between deficit repair rounds of the same generation.
_STUCK_REPAIR_COOLDOWN_S = 0.12
# RaptorQ normally decodes near K; add a small rank margin and enough surplus
# for loss of the repair packets themselves.
_DECODE_MARGIN = 2
_REPAIR_SURPLUS = max(
    0.0, min(1.0, float(os.environ.get("TETRYS_REPAIR_SURPLUS", "0.25")))
)
# Small NACK patches cannot reuse blast FEC percent: 10% of 4 symbols is one
# packet, and one burst loss costs another RTT. Always add this many extras.
_REPAIR_ABS_PAD = 4
# Bound incomplete source data rather than generation count so larger K does
# not recreate an unbounded repair tail. Occupancy already tracks ~1 BDP
# (data in flight + feedback delay), so a 1×BDP window pauses blast.
# Live window is gain×BDP, floored at 8 MiB and capped at 64 MiB.
_MAX_INFLIGHT_MIB = 64.0
_MIN_INFLIGHT_MIB = 8.0
_INFLIGHT_BDP_GAIN = 8.0
_INFLIGHT_MIX_UP = 0.35
_INFLIGHT_MIX_DOWN = 0.15
_MIN_INFLIGHT_GENS = 1
# Blast encode runs in separate PROCESSES: the raptorq binding holds the GIL
# for the whole native encode (measured: a sendto in the send thread waits up
# to ~12ms behind 3 encoding threads). Worker processes have their own GIL,
# so the send loop keeps the socket busy while children encode.
# Leave one core for the send/repair loop; override with TETRYS_ENCODE_WORKERS.
# WAN 6-core A/B: 6 workers starved send (2G 59 vs 76 MiB/s at 4).
_ENCODE_WORKERS = 4
_ENCODE_PREFETCH = 16
# One worker reads this many gens in a single mmap slice (~2 MiB at
# WAN K=96/T=1350), then streams each encoded gen to the send loop.
# Override with TETRYS_ENCODE_READ_GENS.
_ENCODE_READ_GENS = 16
# One sequential pread thread feeds this many MiB of source gens into RAM.
# Encode workers consume those blobs and must not mmap the blast path.
# 0 disables (old per-worker mmap). Override with TETRYS_DISK_QUEUE_MIB.
# Fixed 64 MiB: adaptive 32→64 left the reader hungry (diskq=0–21/32, wait_rx).
_DISK_QUEUE_MIB = 64
# Opt-in ceiling if TETRYS_DISK_QUEUE_ADAPT=1. Default adapt is off.
_DISK_QUEUE_MAX_MIB = 64
_DISK_QUEUE_ADAPT_STEP_MIB = 16
_DISK_QUEUE_ADAPT_MIN_AVAIL_MIB = 1800
_DISK_QUEUE_ADAPT_FAST_FRAC = 0.50
_DISK_QUEUE_ADAPT_FAST_MIN_BPS = 80.0 * 1024 * 1024
_DISK_QUEUE_ADAPT_EVERY_S = 1.0
_DISK_READ_CHUNK = 4 * 1024 * 1024
# O_DIRECT bypasses the ballooned page cache. Buffered 2G reads on this VM
# were ~38 MB/s while the new SSD does ~540 MB/s with iflag=direct.
_DISK_DIRECT_ALIGN = 4096
# Cap send pace to the sequential reader while the RAM queue is draining.
# Full queue → network CC owns the rate (warm cache / fast disk).
_DISK_PACE_HUNGRY_FRAC = 0.25
_DISK_PACE_FULL_FRAC = 0.55
_DISK_PACE_HEADROOM = 1.05
_DISK_PACE_RATE_DT_S = 0.05
_DISK_PACE_IDLE_S = 0.80
_DISK_PACE_FAST_FRAC = 0.85
# Optional extra page-cache window (MiB). 0 = disk-queue thread is the reader.
_READAHEAD_MIB = 0
_READAHEAD_CHUNK = 2 * 1024 * 1024
_READAHEAD_WORKER_GENS = 64
# Repair must not share the blast encode pool. A HOL hole (lag≫occupancy)
# used to submit fountain into the same 4 workers; send then sat in wenc
# and the client starved at wait_rx (~32 MiB/s instead of ~75).
_REPAIR_WORKERS = 1
# Hybrid pipeline: fountain only when unrecovered occupancy is high.
# Fountain mode (overhead=0): per-gen repair in encode worker; cap top-up at inflight limit.
_FOUNTAIN_TARGET_OVERHEAD_PCT = 8
# Fountain tick every N systematic sends when hybrid path is stressed (not at hard cap).
_FOUNTAIN_EVERY_N = 4
# How many incomplete gens to fountain-repair per send-loop turn.
_REPAIR_PER_TURN = 4
_REPAIR_AT_CAP = 8
# One feedback round must stay a thin fountain top-up, never a full reblast.
_REPAIR_ROUND_MAX = 32
# While blast is blocked on inflight cap, spray new ESI into the frontier
# (send loop only). Rate is limited by the token bucket, not a hard pkts/s cap.
# Pipelined blast: K (+ bootstrap) then advance; async fountain fills gaps.
_FOUNTAIN_CAP_GENS = 6
_FOUNTAIN_CAP_COOLDOWN_S = 0.05
_FOUNTAIN_CAP_PER_GEN = 8
# Start spreading repair before occupancy hits the 75% pause (last 1G grew
# 200 open gens during an 80 MiB/s blast, then the drain tail collapsed).
_FOUNTAIN_PRESSURE = 0.15
# Close gens that are 1..4 symbols short first; a deep HOL hole must not
# spray the already-decoded tail (that stole blast on WAN).
_FOUNTAIN_NEAR_COMPLETE = 4
_FOUNTAIN_HOL_KEEP = 8
_FOUNTAIN_PROBE = 2
_FOUNTAIN_SEND_MAX = 8
_FOUNTAIN_TAIL_MAX = 4
_FOUNTAIN_EMPTY_HOL = 16
_FOUNTAIN_EMPTY_TAIL = 16
# Track fountain state only near the client frontier (not the whole inflight tail).
_FOUNTAIN_TRACK_MAX = 64
_FOUNTAIN_WINDOW = 48
# Bound in-flight async repair encodes (each holds wire batches in RAM).
_REPAIR_FUTURES_MAX = 12
# Circuit breaker when repair wire rate explodes (pkts/s in the 1s progress window).
_REPAIR_STORM_PKTS_S = 25_000
_REPAIR_STORM_BACKOFF_S = 0.75
# Bound repair_extra / cooldown dicts (one int/float per gen touched by repair).
_REPAIR_META_KEEP = 256
# Per-worker repair encoder cache (incremental ensure_repair, not full rebuild).
_REPAIR_ENC_CACHE_MAX = 48
# Client NACK horizon slack beyond byte-bounded inflight window.
_FEEDBACK_HORIZON_SLACK = 32
# Fast client feedback while the frontier is stalled or any gen is open.
_FB_EVERY_S = 0.02
# After FIN, keep the fast interval while holes remain; 50ms only when idle.
_FB_EVERY_FIN_S = 0.05
# Post-FIN drain: repair a sliding window of confirmed holes / never-seen
# gens instead of spraying already-complete gens one HOL at a time.
_DRAIN_WINDOW = 48
_DRAIN_TURN_GENS = 12
_DRAIN_COOLDOWN_S = 0.08
# Mid-blast HOL share also requires next_needed stuck >= _STUCK_S.
# Size alone is WAN reorder inside the 64 MiB window.
_HOL_PAUSE_GENS = 64
# Stay in HOL-share until the hole shrinks; otherwise a 3-gen HOL crawl
# resets stuck_nn and blast resumes into a bigger hole.
_HOL_RESUME_GENS = 32
# Keep blasting while repairing HOL. 32 pkts is ~25% of one K=96+30% gen
# (~125 wire packets). A full pause starved the client at wait_rx=99%.
_HOL_SHARE_TURN_PKTS = 32
_HOL_SHARE_TURN_GENS = 4
# Delivery-rate pacing: only back off when client goodput lags wire pace.
_ADAPT_PACE_MIN_FRAC = 0.10
_ADAPT_PACE_BACKOFF_FRAC = 0.82  # delivery < pace×this → slow down
_ADAPT_PACE_RECOVER_FRAC = 0.92  # delivery ≥ pace×this → ease toward --rate
_ADAPT_PACE_RECOVER_STEP = 1.12
_ADAPT_PACE_GOOD_SNAP = 3  # consecutive good samples before full --rate
_ADAPT_PACE_WARMUP_S = 6.0  # ignore bursty early decode after ramp
_ADAPT_PACE_BAD_SNAP = 3  # consecutive lagging samples before backoff
# Delay-based pacing from echoed send timestamps (same clock → RTT).
# Start well below the --rate ceiling and climb only on confirmed clean RTT.
# Floor stays high: a policer shows no standing queue, so the old 0.5% floor
# collapsed to ~8 Mbit when 900 Mbit was dropped (~33% loss). 200 Mbit is
# still under today's clean 400 Mbit knee and above the 300 Mbit goal.
_DELAY_CC_START_MBIT = 220.0
_DELAY_CC_MIN_MBIT = 200.0
_DELAY_CC_MIN_FRAC = 0.20
_DELAY_CC_TARGET_QUEUE_MIN_S = 0.006
_DELAY_CC_TARGET_QUEUE_RTT_FRAC = 0.25
_DELAY_CC_HIGH_QUEUE_MIN_S = 0.020
_DELAY_CC_HIGH_QUEUE_RTT_FRAC = 0.50
_DELAY_CC_DOWN = 0.92
_DELAY_CC_DOWN_HARD = 0.82
_DELAY_CC_UP = 1.08
_DELAY_CC_UP_PROBE = 1.25
_DELAY_CC_UP_PROBE_BELOW_FRAC = 0.50
_DELAY_CC_WARMUP_S = 0.5
_DELAY_CC_UPDATE_S = 0.10
_DELAY_CC_CLEAN_SAMPLES = 2
_DELAY_CC_CONGESTION_SAMPLES = 3
_DELAY_CC_BACKOFF_HOLD_S = 0.8
# Delivery vs expected app: policers drop without inflating RTT.
_DELAY_CC_DELIVERY_WARMUP_S = 2.0
_DELAY_CC_DELIVERY_BAD = 0.50
_DELAY_CC_DELIVERY_OK = 0.78
_DELAY_CC_DELIVERY_CUT = 0.70
_DELAY_CC_DELIVERY_CUT_HARD = 0.55
# Two seconds supports long-distance links but rejects old feedback after stalls.
_DELAY_CC_MAX_RTT_S = 2.0
# Slowly forget optimistic min_rtt so a lucky early sample cannot stick forever.
_DELAY_CC_MIN_RTT_GROW = 1.0008
# EWMA + spike gate: a single 75→187 ms echo must not slam pace to the floor.
_DELAY_CC_RTT_ALPHA = 0.20
_DELAY_CC_SPIKE_MULT = 1.75
_DELAY_CC_SPIKE_MIN_S = 0.040
_DELAY_CC_SPIKE_CONFIRM = 3
# FEC eases back to 4% when the window miss rate is low — never strip
# blast repair entirely. Occupancy "clean" is BDP, not a loss-free path.
# miss is packet shortfall / (K+margin × recent window), not NACK-list share.
_OH_CLEAN_HOLD_S = 4.0
_OH_FLOOR_PCT = 4
_OH_CEIL_PCT = 32
# Leave the 4% floor on a small window miss. 8% was deaf once the floor
# dropped (64 NACKs / 540 gens ≈ 2–3% even with 200+ open gens).
_OH_MISS_MID = 0.02
_OH_MISS_HI = 0.25
# Incomplete gens inside this fraction of the blast window are in-flight BDP,
# not loss. Extra occupancy means first-pass FEC is too thin.
_OH_INFLIGHT_SLACK_FRAC = 0.30
# Unknown path: blast the first window at ceil, then follow measured miss.
_OH_START_HOLD_S = 2.0
# First-pass packet-loss FEC. Rank deficit is only a boost; BBR ignores this.
_LOSS_WINDOW = 4096
_LOSS_RIGHT_MARGIN = 64
_LOSS_GRACE_MIN_S = 0.050
_LOSS_GRACE_RTT_S = 0.080
_LOSS_GRACE_RTT_MULT = 3.0
_LOSS_GRACE_MAX_S = 0.50
_LOSS_SAMPLE_MIN = 256
_LOSS_BURST_MIN = 128
_LOSS_ALPHA_UP = 0.50
_LOSS_ALPHA_DOWN = 0.15
_LOSS_ALPHA_SLOW = 0.08
_LOSS_PAD_PCT = 3
_LOSS_SLEW_UP = 12
_LOSS_SLEW_DOWN = 2
_LOSS_CLEAN_WINDOWS = 3
_LOSS_BURST_P = 0.15
_LOSS_DEFICIT_BOOST_MID = 4
_LOSS_DEFICIT_BOOST_HI = 8
# BBR-style delivery-rate pacing (ATP PROBE_BW): don't outrun drain.
_BBR_PROBE_GAIN = 1.25
_BBR_DRAIN_GAIN = 0.75
_BBR_CRUISE_GAIN = 1.00
# TCP BBR STARTUP is 2×; tetrys HOL cannot survive that on WAN. Climb with
# the same 1.25× used in PROBE_BW — BtlBw still grows, just without a
# multi-BDP repair hole.
_BBR_STARTUP_GAIN = 1.25
_BBR_CYCLE_UNITS = 8  # 1 probe + 1 drain + 6 cruise, in RTprop
_BBR_BTLBW_RTPROP = 10.0  # BBR default, but never shorter than _BBR_BTLBW_MIN_S
_BBR_BTLBW_MIN_S = 3.0  # WAN: one delay bump must not expire a good probe
_BBR_STARTUP_GROW = 1.25  # still filling the pipe if BtlBw grew this much
_BBR_STARTUP_FLAT = 3  # exit STARTUP after this many ungrown RTprop rounds
_BBR_SAMPLE_KEEP = 64
# Encode/send warmup is not BtlBw. Skip delivery samples until this elapses.
_BBR_SAMPLE_WARMUP_S = _DELAY_CC_DELIVERY_WARMUP_S
# delivery << pace with an empty window is sender-limited, not a policer.
_BBR_COLD_DELIV_FRAC = 0.45
# A plateau on the 200 Mbit CC floor is still cold start unless the window is full.
_BBR_STARTUP_FLOOR_MULT = 1.5
_BBR_STARTUP_MAX_MBIT = 1150.0
_BBR_STARTUP_ROUND_MIN_S = 0.25
_BBR_STARTUP_CONFIRM = 0.75
_BBR_STARTUP_BAD = 0.68
_BBR_STARTUP_BAD_ROUNDS = 2
_BBR_STARTUP_HOL_ROUNDS = 2
_BBR_STARTUP_FLAT_GROW = 1.10
_BBR_STARTUP_SLOW_MBIT = 800.0
_BBR_STARTUP_SLOW_GAIN = 1.12
_BBR_STARTUP_DELIVERY_HEADROOM = 1.35
_BBR_STARTUP_MIN_BYTES = 1 * 1024 * 1024
_BBR_DELIVERY_WIRE_SLACK = 1.08
# Occupancy fraction that is a real decode/repair stall, not generation HOL
# from random-access RaptorQ (later gens done, next_needed still lagging).
_BBR_STALL_OCC_FRAC = 0.75


def clamp_inflight_mib(mib: float, *, cap_mib: float | None = None) -> float:
    """Keep the unrecovered window between ~1 BDP floor and the byte cap."""
    cap = _MAX_INFLIGHT_MIB if cap_mib is None else float(cap_mib)
    cap = min(_MAX_INFLIGHT_MIB, max(_MIN_INFLIGHT_MIB, cap))
    return min(cap, max(_MIN_INFLIGHT_MIB, float(mib)))


def max_inflight_bytes(inflight_mib: float | None = None) -> int:
    """Byte cap for unrecovered gens. Override with TETRYS_INFLIGHT_MIB."""
    if inflight_mib is None:
        raw = os.environ.get("TETRYS_INFLIGHT_MIB", str(_MAX_INFLIGHT_MIB))
        try:
            mib = float(raw or str(_MAX_INFLIGHT_MIB))
        except ValueError:
            mib = _MAX_INFLIGHT_MIB
    else:
        mib = float(inflight_mib)
    return int(clamp_inflight_mib(mib) * 1024 * 1024)


def compute_inflight_gen_limit(
    gen_k: int,
    symbol_size: int,
    *,
    inflight_mib: float | None = None,
) -> int:
    """Byte-bounded pipeline window in generations (size-independent in bytes)."""
    block_bytes = max(1, gen_k) * max(64, symbol_size)
    return max(_MIN_INFLIGHT_GENS, max_inflight_bytes(inflight_mib) // block_bytes)


def bdp_bytes(btlbw_bps: float, min_rtt_s: float) -> float:
    """Bottleneck bandwidth × min RTT. Zero until both samples exist."""
    if btlbw_bps <= 1.0 or min_rtt_s <= 0.0:
        return 0.0
    return float(btlbw_bps) * float(min_rtt_s)


def adaptive_inflight_mib(
    *,
    btlbw_bps: float,
    min_rtt_s: float,
    current_mib: float,
    occupancy_bytes: float = 0.0,
    gain: float = _INFLIGHT_BDP_GAIN,
    cap_mib: float | None = None,
    mix_up: float = _INFLIGHT_MIX_UP,
    mix_down: float = _INFLIGHT_MIX_DOWN,
) -> float:
    """Track gain×BDP. Occupancy is already ~1 BDP, so gain=1 pauses blast.

    Shrinks only while there is pause headroom; never under live occupancy.
    Until BtlBw and min_rtt exist, keep the current window.
    """
    cap = _MAX_INFLIGHT_MIB if cap_mib is None else float(cap_mib)
    cur = clamp_inflight_mib(current_mib, cap_mib=cap)
    bdp = bdp_bytes(btlbw_bps, min_rtt_s)
    if bdp <= 0.0:
        return cur
    target = gain * bdp / (1024.0 * 1024.0)
    occ_mib = max(0.0, float(occupancy_bytes) / (1024.0 * 1024.0))
    # Pause is 75% of the window; keep occupancy under ~70% after a shrink.
    if occ_mib > 0.0:
        target = max(target, occ_mib / 0.70)
    target = clamp_inflight_mib(target, cap_mib=cap)
    if target >= cur:
        mixed = cur * (1.0 - mix_up) + target * mix_up
    else:
        mixed = cur * (1.0 - mix_down) + target * mix_down
    return clamp_inflight_mib(mixed, cap_mib=cap)


def client_feedback_horizon(inflight_gen_limit: int) -> int:
    """NACK coverage must match server inflight, not a fixed small constant."""
    return inflight_gen_limit + _FEEDBACK_HORIZON_SLACK


def build_feedback_miss_bitmap(
    *,
    next_needed: int,
    total_gens: int,
    horizon: int,
    bit_get,
    decoders: dict[int, object],
    max_gid_seen: int,
    include_gid=None,
    include_unseen: bool = False,
) -> bytes:
    """Compact miss map for [next_needed, next_needed+horizon); gap-aware.

    Unseen gens past max_gid_seen stay off during blast (still in flight).
    After FIN they are holes and must be advertised.
    """
    if next_needed >= total_gens or horizon <= 0:
        return b""
    window = min(horizon + 1, total_gens - next_needed)
    nbytes = min(FEEDBACK_BITMAP_MAX_BYTES, (window + 7) // 8)
    if nbytes <= 0:
        return b""
    buf = bytearray(nbytes)
    max_bits = nbytes * 8
    for off in range(min(window, max_bits)):
        gid = next_needed + off
        if bit_get(gid):
            continue
        unseen = gid not in decoders and gid > max_gid_seen
        if unseen and not include_unseen:
            continue
        if include_gid is not None and not (unseen and include_unseen):
            if not include_gid(gid):
                continue
        buf[off >> 3] |= 1 << (off & 7)
    return bytes(buf)


def drain_epoch_is_stale(epoch: int, last: int) -> bool:
    """True when `epoch` is older than `last` on a uint16 wrap-around clock."""
    ep = int(epoch) & 0xFFFF
    seen = int(last) & 0xFFFF
    if ep <= 0 or seen <= 0 or ep == seen:
        return False
    return ((seen - ep) & 0xFFFF) < 0x8000


def drain_never_seen_frontier(next_needed: int, max_gid_seen: int, total_gens: int) -> int:
    """First gen the client has never received, clipped to the file."""
    if total_gens <= 0:
        return 0
    ns = max(int(next_needed), int(max_gid_seen) + 1)
    return min(int(total_gens), max(0, ns))


def drain_scoreboard_targets(
    *,
    next_needed: int,
    total_gens: int,
    miss_nacks: list[int],
    never_seen: int | None,
    window: int = _DRAIN_WINDOW,
) -> list[int]:
    """Gens to repair after FIN: HOL window of bitmap holes + never-seen tail."""
    nn = int(next_needed)
    total = int(total_gens)
    if nn < 0 or nn >= total:
        return []
    win = max(1, int(window))
    hi = min(total, nn + win)
    want: set[int] = set()
    if nn < hi:
        want.add(nn)
    for raw in miss_nacks:
        gid = int(raw)
        if nn <= gid < hi:
            want.add(gid)
    if never_seen is not None:
        ns = int(never_seen)
        if 0 <= ns < total:
            lo = max(nn, min(ns, hi))
            want.update(range(lo, hi))
    return sorted(want)[:win]


def drain_empty_round_max(gen_k: int) -> int:
    """One-RTT close of an empty K-symbol gen (not the blast 32-packet cap)."""
    return max(1, int(gen_k) + _DECODE_MARGIN + _REPAIR_ABS_PAD)


def drain_repair_send_n(
    symbols_rx: int | None,
    gen_k: int,
    *,
    empty: bool,
) -> int:
    """Drain repair size: full K+pad for never-seen, deficit+pad for partial."""
    cap = drain_empty_round_max(gen_k)
    if empty or symbols_rx is None or int(symbols_rx) <= 0:
        return cap
    n = repair_send_n(
        int(symbols_rx),
        gen_k,
        hol=True,
        close=True,
        round_max=cap,
    )
    return max(1, int(n))


def hol_hole_gens(incomplete: int, frontier_lag: int) -> int:
    """Gens already done past next_needed. Large value is a HOL hole, not BDP."""
    return max(0, int(frontier_lag) - int(incomplete))


def hol_should_pause_blast(
    hol_hole: int,
    *,
    pause_gens: int | None = None,
    inflight_gen_limit: int = 0,
) -> bool:
    """Pause blast when later gens decoded past a stuck HOL (not BDP occupancy)."""
    floor = _HOL_PAUSE_GENS if pause_gens is None else max(1, int(pause_gens))
    if inflight_gen_limit > 0:
        floor = max(floor, int(inflight_gen_limit))
    return int(hol_hole) >= floor


def hol_pause_should_hold(
    *,
    active: bool,
    hol_hole: int,
    stuck: bool,
    pause_gens: int | None = None,
    resume_gens: int | None = None,
    occupancy: int = 0,
    inflight_gen_limit: int = 0,
) -> bool:
    """Enter on stuck+hole; stay until the hole falls under the resume floor.

    Occupancy below 75% of the window is reorder/random-access, not a reason
    to stop first-pass blast.
    """
    cap = max(0, int(inflight_gen_limit))
    if cap > 0 and int(occupancy) < (cap * 3) // 4:
        return False
    if active:
        floor = _HOL_RESUME_GENS if resume_gens is None else max(0, int(resume_gens))
        return int(hol_hole) >= floor
    return bool(stuck) and hol_should_pause_blast(hol_hole, pause_gens=pause_gens)


def hol_share_cap_n(
    n: int,
    *,
    max_pkts: int = _HOL_SHARE_TURN_PKTS,
) -> int:
    """Bound HOL repair so it cannot eat the whole send turn."""
    return max(0, min(int(n), max(0, int(max_pkts))))


def should_pause_blast(
    incomplete: int,
    frontier_lag: int,
    *,
    inflight_gen_limit: int,
) -> bool:
    """Pause new data when unrecovered occupancy fills the window.

    Frontier lag alone is a HOL hole (later gens already decoded). Stopping
    blast there freezes the rest of the file instead of repairing the hole.
    """
    del frontier_lag
    if incomplete >= inflight_gen_limit:
        return True
    return incomplete >= (inflight_gen_limit * 3) // 4


def pipeline_is_stalled(
    *,
    occupancy: int,
    inflight_gen_limit: int,
    occ_frac: float = _BBR_STALL_OCC_FRAC,
) -> bool:
    """True when unrecovered gens fill the window.

    ``sent - next_needed`` is decode-ahead from random-access generations, not
    a full pipeline. STARTUP/BBR must not treat that lag as a stall.
    """
    cap = max(1, int(inflight_gen_limit))
    return int(occupancy) >= max(1, int(cap * max(0.0, float(occ_frac))))


def delay_cc_congestion_pressure(
    occupancy: int, inflight_gen_limit: int
) -> bool:
    """True when unrecovered gens fill the window, not decode-ahead BDP.

    Occupancy of 20–60% is a normal WAN decode pipeline. Treating that as a
    standing queue selected 0.75 drain and cut pace ~40% while the path was
    still delivering.
    """
    cap = max(1, int(inflight_gen_limit))
    return int(occupancy) >= (cap * 3) // 4


def delay_cc_may_probe(occupancy: int, inflight_gen_limit: int) -> bool:
    """Climb only while blast is not paused on occupancy."""
    return not delay_cc_congestion_pressure(occupancy, inflight_gen_limit)


def delay_cc_may_rebase_min_rtt(
    *,
    is_spike: bool,
    pipeline_clean: bool,
    queue_s: float,
    ewma_s: float,
    min_rtt_s: float,
    max_mult: float = 2.0,
    min_queue_s: float = _DELAY_CC_TARGET_QUEUE_MIN_S,
) -> bool:
    """Rebase min_rtt for small baseline drift, not a stall echo.

    A 80→656 ms jump with lag=1 is HOL/feedback stall. Treating it as the
    new RTprop zeros queue_s (rtt=656/0ms) and lets delay-CC slam pace.
    """
    if is_spike or not pipeline_clean:
        return False
    if float(queue_s) < float(min_queue_s):
        return False
    min_r = max(1e-6, float(min_rtt_s))
    if float(ewma_s) > min_r * max(1.0, float(max_mult)):
        return False
    return True


def repair_pressure(
    incomplete: int,
    frontier_lag: int,
    *,
    inflight_gen_limit: int,
) -> float:
    """0..1 occupancy pressure. Lag-only HOL must not look like congestion."""
    del frontier_lag
    cap = max(1, inflight_gen_limit)
    return min(1.0, incomplete / cap)


def blast_fec_window_gens(
    *,
    nack_count: int,
    incomplete: int,
    frontier_lag: int,
    inflight_gen_limit: int,
    sent_gens: int,
) -> int:
    """Recent gens that could have reported, capped at the blast window.

    Do not use len(nacks): that list is already the failures.
    """
    recent = max(int(incomplete), int(frontier_lag), int(sent_gens), 1)
    cap = max(int(inflight_gen_limit), 1)
    return max(int(nack_count), min(cap, recent))


def blast_fec_open_frac(
    incomplete: int,
    inflight_gen_limit: int,
    *,
    slack_frac: float = _OH_INFLIGHT_SLACK_FRAC,
) -> float:
    """Unrecovered gens past ~1 BDP. Clean in-flight must not look like loss."""
    cap = max(1, int(inflight_gen_limit))
    slack = max(0, int(cap * min(1.0, max(0.0, float(slack_frac)))))
    extra = max(0, int(incomplete) - slack)
    return min(1.0, extra / cap)


def blast_fec_miss_frac(
    nack_rx: dict[int, int],
    nacks: list[int],
    *,
    gen_k: int,
    decode_margin: int = _DECODE_MARGIN,
    window_gens: int = 0,
) -> float:
    """First-pass packet miss over the recent window, not over the NACK list.

    Each NACK contributes (K+margin−rx) / (K+margin). Completed gens in the
    window contribute 0. Empty nacks → 0 (keep base overhead).
    """
    if not nacks:
        return 0.0
    need = max(1, int(gen_k) + int(decode_margin))
    win = max(int(window_gens), len(nacks), 1)
    shortfall = 0
    for gid in nacks:
        rx = nack_rx.get(int(gid))
        if rx is None:
            shortfall += need
        else:
            shortfall += max(0, need - max(0, int(rx)))
    return min(1.0, shortfall / (need * win))


def adaptive_blast_overhead_pct(
    *,
    base_pct: int,
    frontier_lag: int,
    nack_count: int,
    incomplete: int,
    inflight_gen_limit: int,
    clean_s: float = 0.0,
    floor_pct: int = _OH_FLOOR_PCT,
    max_pct: int | None = None,
    clean_hold_s: float = _OH_CLEAN_HOLD_S,
    miss_frac: float = 0.0,
    start_pct: int | None = None,
    elapsed_s: float = 0.0,
    sent_gens: int = 0,
    start_hold_s: float = 0.0,
    start_hold_gens: int = 0,
) -> int:
    """Start at ceil, then follow window miss down to 4%.

    Occupancy and HOL lag are BDP / reorder, not loss. miss_frac is packet
    shortfall over the recent window. --gen-overhead is the CLI default when
    adapt is off, not a live floor.
    """
    del frontier_lag, incomplete, inflight_gen_limit, clean_s, clean_hold_s
    del nack_count
    if base_pct <= 0:
        return 0
    lo = max(0, int(floor_pct))
    hi = max(base_pct, max_pct if max_pct is not None else _OH_CEIL_PCT)
    hi = max(lo, hi)
    miss = min(1.0, max(0.0, float(miss_frac)))
    if miss >= _OH_MISS_HI:
        measured = hi
    elif miss >= _OH_MISS_MID:
        measured = max(lo, (lo + hi) // 2)
    else:
        measured = lo
    holding = (start_hold_s > 0 and elapsed_s < start_hold_s) or (
        start_hold_gens > 0 and sent_gens < start_hold_gens
    )
    if holding:
        prior = max(base_pct, start_pct if start_pct is not None else hi)
        return max(measured, prior)
    return measured


def packet_loss_fec_from_env() -> bool:
    raw = os.environ.get("TETRYS_PACKET_LOSS_FEC", "1")
    return raw.lower() not in ("0", "false", "no")


def seq_ahead(a: int, b: int) -> int:
    """Unsigned 32-bit distance a−b."""
    return (int(a) - int(b)) & 0xFFFFFFFF


def seq_lt(a: int, b: int) -> bool:
    """TCP-style sequence comparison."""
    d = seq_ahead(b, a)
    return 0 < d < 0x80000000


def blast_loss_grace_s(
    rtt_s: float = 0.0,
    *,
    late_frac: float = 0.0,
    min_s: float = _LOSS_GRACE_MIN_S,
    rtt_mult: float = _LOSS_GRACE_RTT_MULT,
    default_rtt_s: float = _LOSS_GRACE_RTT_S,
    max_s: float = _LOSS_GRACE_MAX_S,
) -> float:
    """Reorder grace: 3×RTprop, stretched if late arrivals keep arriving."""
    rtt = float(rtt_s) if rtt_s > 0.0 else float(default_rtt_s)
    grace = max(float(min_s), float(rtt_mult) * rtt)
    if late_frac > 0.02:
        grace *= 1.25
    return min(float(max_s), grace)


def blast_loss_sample_p(rx_unique: int, lost_matured: int) -> float | None:
    n = max(0, int(rx_unique)) + max(0, int(lost_matured))
    if n <= 0:
        return None
    return max(0, int(lost_matured)) / n


def blast_loss_confidence_margin(sample_n: int) -> float:
    n = max(0, int(sample_n))
    if n <= 0:
        return 1.0
    return min(1.0, 1.0 / math.sqrt(n))


def update_blast_loss_ewma(
    p_fast: float,
    p_slow: float,
    sample: float,
    *,
    alpha_up: float = _LOSS_ALPHA_UP,
    alpha_down: float = _LOSS_ALPHA_DOWN,
    alpha_slow: float = _LOSS_ALPHA_SLOW,
) -> tuple[float, float, float]:
    """Fast-attack / slow-decay loss filter. Working p is max(fast, slow)."""
    s = min(1.0, max(0.0, float(sample)))
    fast = min(1.0, max(0.0, float(p_fast)))
    slow = min(1.0, max(0.0, float(p_slow)))
    if fast <= 0.0 and slow <= 0.0:
        fast = slow = s
    elif s > fast:
        fast = (1.0 - alpha_up) * fast + alpha_up * s
    else:
        fast = (1.0 - alpha_down) * fast + alpha_down * s
    slow = (1.0 - alpha_slow) * slow + alpha_slow * s
    return fast, slow, max(fast, slow)


def blast_loss_overhead_pct(
    p: float,
    *,
    floor_pct: int = _OH_FLOOR_PCT,
    max_pct: int = _OH_CEIL_PCT,
    pad_pct: int = _LOSS_PAD_PCT,
    sample_n: int = 0,
) -> int:
    """Map loss probability to FEC. FEC packets are also lost: p/(1-p)."""
    lo = max(0, int(floor_pct))
    hi = max(lo, int(max_pct))
    prob = min(0.90, max(0.0, float(p)))
    if int(sample_n) <= 0 and prob <= 0.0:
        return lo
    required = 0.0 if prob <= 0.0 else prob / max(1e-9, 1.0 - prob)
    oh = required + max(0, int(pad_pct)) / 100.0
    return min(hi, max(lo, int(math.ceil(oh * 100.0 - 1e-9))))


def take_loss_sample(
    prev: dict,
    report: dict,
) -> dict | None:
    """Delta a cumulative matured-loss report. Lost/stale feedback is a no-op."""
    seq_end = int(report.get("seq_end") or 0) & 0xFFFFFFFF
    seq_begin = int(report.get("seq_begin") or 0) & 0xFFFFFFFF
    rx = int(report.get("rx") or 0) & 0xFFFFFFFF
    lost = int(report.get("lost") or 0) & 0xFFFFFFFF
    if seq_end == seq_begin and rx == 0 and lost == 0:
        return None
    last_end = prev.get("seq_end")
    if last_end is not None:
        last_end = int(last_end) & 0xFFFFFFFF
        if last_end == seq_end:
            return None
        if not seq_lt(last_end, seq_end):
            return None
        d_rx = (rx - int(prev.get("rx") or 0)) & 0xFFFFFFFF
        d_lost = (lost - int(prev.get("lost") or 0)) & 0xFFFFFFFF
    else:
        d_rx = rx
        d_lost = lost
    n = d_rx + d_lost
    if n <= 0:
        prev["seq_end"] = seq_end
        prev["rx"] = rx
        prev["lost"] = lost
        return None
    p = d_lost / n
    prev["seq_end"] = seq_end
    prev["seq_begin"] = seq_begin
    prev["rx"] = rx
    prev["lost"] = lost
    prev["late"] = int(report.get("late") or 0)
    prev["pending"] = int(report.get("pending") or 0)
    prev["epoch"] = int(report.get("epoch") or 0)
    return {
        "rx": d_rx,
        "lost": d_lost,
        "n": n,
        "p": p,
        "late": int(report.get("late") or 0),
        "pending": int(report.get("pending") or 0),
    }


def accumulate_loss_sample(
    state: dict,
    sample: dict | None,
    *,
    window_n: int = _LOSS_SAMPLE_MIN,
    burst_n: int = _LOSS_BURST_MIN,
    burst_p: float = _LOSS_BURST_P,
) -> dict | None:
    """Emit fixed matured windows; tiny feedback deltas cannot move FEC."""
    if sample is None:
        return None
    state["rx"] = int(state.get("rx") or 0) + max(0, int(sample.get("rx") or 0))
    state["lost"] = int(state.get("lost") or 0) + max(
        0, int(sample.get("lost") or 0)
    )
    state["late"] = int(sample.get("late") or 0)
    state["pending"] = int(sample.get("pending") or 0)
    n = int(state["rx"]) + int(state["lost"])
    p = int(state["lost"]) / n if n > 0 else 0.0
    burst = n >= max(1, int(burst_n)) and p >= float(burst_p)
    if n < max(1, int(window_n)) and not burst:
        return None
    out = {
        "rx": int(state["rx"]),
        "lost": int(state["lost"]),
        "n": n,
        "p": p,
        "late": int(state["late"]),
        "pending": int(state["pending"]),
    }
    state["rx"] = 0
    state["lost"] = 0
    return out


def apply_packet_loss_fec(
    *,
    cur_pct: int,
    sample: dict | None,
    rank_miss: float = 0.0,
    p_fast: float = 0.0,
    p_slow: float = 0.0,
    clean_windows: int = 0,
    floor_pct: int = _OH_FLOOR_PCT,
    max_pct: int = _OH_CEIL_PCT,
    start_pct: int | None = None,
    elapsed_s: float = 0.0,
    sent_gens: int = 0,
    start_hold_s: float = 0.0,
    start_hold_gens: int = 0,
) -> tuple[int, float, float, float, int, int, int]:
    """Return (pct, p_fast, p_slow, p, fec_base, deficit_boost, clean_windows)."""
    lo = max(0, int(floor_pct))
    hi = max(lo, int(max_pct))
    holding = (start_hold_s > 0 and elapsed_s < start_hold_s) or (
        start_hold_gens > 0 and sent_gens < start_hold_gens
    )
    p = max(float(p_fast), float(p_slow))
    burst_boost = 0
    n = int(sample["n"]) if sample is not None else 0
    p_hat = float(sample["p"]) if sample is not None else 0.0
    if sample is not None and n > 0:
        margin = blast_loss_confidence_margin(n)
        p_use = float(p_hat)
        if p_hat > 0.0:
            p_use = min(1.0, p_hat + (margin if n < _LOSS_SAMPLE_MIN else 0.25 * margin))
        # A 128-packet reorder hole is not 40–100% path loss. Cap short
        # windows so one burst cannot pin EWMA at the 32% FEC ceil.
        if n < _LOSS_SAMPLE_MIN:
            p_use = min(p_use, 0.25)
        burst = n >= _LOSS_BURST_MIN and p_hat >= max(
            _LOSS_BURST_P, float(p_fast) * 2.0 if p_fast > 0.0 else _LOSS_BURST_P
        )
        if n >= _LOSS_SAMPLE_MIN or burst:
            p_fast, p_slow, p = update_blast_loss_ewma(p_fast, p_slow, p_use)
        elif n >= _LOSS_BURST_MIN and p_use > p_fast:
            p_fast, p_slow, p = update_blast_loss_ewma(p_fast, p_slow, p_use)
        if burst:
            burst_boost = max(0, hi - lo) // 4
            clean_windows = 0
        elif p_hat < 0.02 and n >= _LOSS_SAMPLE_MIN:
            clean_windows = int(clean_windows) + 1
        else:
            clean_windows = 0
    miss = min(1.0, max(0.0, float(rank_miss)))
    confident_loss = sample is not None and n >= _LOSS_SAMPLE_MIN
    if confident_loss:
        # Rank deficit on a random-access client is HOL lag, not extra loss.
        deficit_boost = 0
    elif miss >= _OH_MISS_HI:
        deficit_boost = _LOSS_DEFICIT_BOOST_HI
    elif miss >= _OH_MISS_MID:
        deficit_boost = _LOSS_DEFICIT_BOOST_MID
    else:
        deficit_boost = 0
    fec_base = blast_loss_overhead_pct(
        p, floor_pct=lo, max_pct=hi, sample_n=n if sample is not None else 0
    )
    target = min(hi, fec_base + deficit_boost + burst_boost)
    cur = min(hi, max(lo, int(cur_pct)))
    if target > cur:
        new_pct = min(target, cur + _LOSS_SLEW_UP)
        clean_windows = 0
    elif target < cur:
        # Path loss here is ~6%, so p<2% "clean" windows almost never
        # happen. Follow target anyway; extra clean windows slew faster.
        step = _LOSS_SLEW_DOWN
        if int(clean_windows) >= _LOSS_CLEAN_WINDOWS:
            step = _LOSS_SLEW_DOWN * 2
        new_pct = max(target, cur - step)
    else:
        new_pct = cur
    # Start-hold is for an unknown path. A confident sample is the path.
    if holding and not confident_loss:
        prior = start_pct if start_pct is not None else hi
        new_pct = max(new_pct, int(prior))
    new_pct = min(hi, max(lo, int(new_pct)))
    return new_pct, p_fast, p_slow, p, fec_base, deficit_boost, int(clean_windows)


class BlastLossTracker:
    """Sliding first-pass sequence bitmap. Gaps start pending, then mature."""

    def __init__(self, window: int = _LOSS_WINDOW) -> None:
        self.window = max(64, int(window))
        self.bits = bytearray((self.window + 7) // 8)
        self.hole_ts = [0.0] * self.window
        self.base = 0
        self.started = False
        self.max_seen = 0
        self.max_seen_ts = 0.0
        self.rx_unique = 0
        self.lost = 0
        self.late = 0
        self.epoch = 1
        self.seq_begin = 0
        self.grace_s = blast_loss_grace_s()

    def _slot(self, seq: int) -> int:
        return int(seq) % self.window

    def _get(self, seq: int) -> bool:
        i = self._slot(seq)
        return bool(self.bits[i >> 3] & (1 << (i & 7)))

    def _set(self, seq: int) -> None:
        i = self._slot(seq)
        self.bits[i >> 3] |= 1 << (i & 7)

    def _clear(self, seq: int) -> None:
        i = self._slot(seq)
        self.bits[i >> 3] &= ~(1 << (i & 7))
        self.hole_ts[i] = 0.0

    def _advance_base(self) -> None:
        self._clear(self.base)
        self.base = (self.base + 1) & 0xFFFFFFFF

    def _force_advance(self, new_base: int) -> None:
        new_base = int(new_base) & 0xFFFFFFFF
        while seq_lt(self.base, new_base):
            if self._get(self.base):
                self.rx_unique += 1
            else:
                self.lost += 1
            self._advance_base()

    def on_packet(self, blast_seq: int | None, now: float) -> None:
        if blast_seq is None:
            return
        seq = int(blast_seq) & 0xFFFFFFFF
        if not self.started:
            self.started = True
            self.base = seq
            self.seq_begin = seq
            self.max_seen = seq
            self.max_seen_ts = float(now)
            self._set(seq)
            return
        if seq_lt(seq, self.base):
            self.late += 1
            return
        dist = seq_ahead(seq, self.base)
        if dist >= self.window:
            self._force_advance((seq - self.window + 1) & 0xFFFFFFFF)
            if seq_lt(seq, self.base):
                self.late += 1
                return
        if self._get(seq):
            return
        self._set(seq)
        if seq_lt(self.max_seen, seq):
            s = (self.max_seen + 1) & 0xFFFFFFFF
            while seq_lt(s, seq):
                if not self._get(s):
                    i = self._slot(s)
                    if self.hole_ts[i] <= 0.0:
                        self.hole_ts[i] = float(now)
                s = (s + 1) & 0xFFFFFFFF
            self.max_seen = seq
            self.max_seen_ts = float(now)

    def mature(self, now: float, *, right_margin: int = _LOSS_RIGHT_MARGIN) -> int:
        """Declare received/lost up to the reorder frontier. Return pending holes."""
        if not self.started:
            return 0
        margin = max(1, int(right_margin))
        end = (self.max_seen + 1) & 0xFFFFFFFF
        while seq_lt(self.base, end):
            if self._get(self.base):
                self.rx_unique += 1
                self._advance_base()
                continue
            if seq_ahead(self.max_seen, self.base) < margin:
                break
            opened = self.hole_ts[self._slot(self.base)]
            if opened <= 0.0:
                opened = self.max_seen_ts
            if float(now) - opened < self.grace_s:
                break
            self.lost += 1
            self._advance_base()
        pending = 0
        s = self.base
        guard = 0
        while seq_lt(s, end):
            if not self._get(s):
                pending += 1
            s = (s + 1) & 0xFFFFFFFF
            guard += 1
            if guard > self.window:
                break
        return pending

    def adapt_grace(self, rtt_s: float = 0.0) -> None:
        n = self.rx_unique + self.lost
        late_frac = (self.late / n) if n > 0 else 0.0
        self.grace_s = blast_loss_grace_s(rtt_s, late_frac=late_frac)

    def report(self, pending: int) -> dict:
        return {
            "epoch": self.epoch,
            "seq_begin": self.seq_begin,
            "seq_end": self.base,
            "rx": self.rx_unique,
            "lost": self.lost,
            "late": self.late,
            "pending": max(0, int(pending)),
        }


def echo_rtt_s(echo_ts_us: int, now_s: float) -> float | None:
    """RTT from echoed server send_ts_us; None if missing/stale."""
    if echo_ts_us <= 0:
        return None
    now_us = int(now_s * 1_000_000.0) & 0xFFFFFFFF
    delta = (now_us - (echo_ts_us & 0xFFFFFFFF)) & 0xFFFFFFFF
    if delta == 0 or delta > int(_DELAY_CC_MAX_RTT_S * 1_000_000):
        return None
    return delta / 1_000_000.0


def smooth_delay_rtt_s(
    sample_s: float,
    ewma_s: float,
    spike_streak: int = 0,
    *,
    alpha: float = _DELAY_CC_RTT_ALPHA,
    spike_mult: float = _DELAY_CC_SPIKE_MULT,
    spike_min_s: float = _DELAY_CC_SPIKE_MIN_S,
    spike_confirm: int = _DELAY_CC_SPIKE_CONFIRM,
) -> tuple[float, bool, int]:
    """EWMA RTT. Isolated spikes do not move the filter or count as congestion.

    Returns (ewma_s, is_spike, spike_streak). After ``spike_confirm`` consecutive
    outliers the rise is treated as real delay and mixed in with ``alpha``.
    """
    if sample_s <= 0.0:
        return ewma_s, False, 0
    if ewma_s <= 0.0:
        return sample_s, False, 0
    outlier = (
        sample_s >= ewma_s * spike_mult
        and (sample_s - ewma_s) >= spike_min_s
    )
    if outlier:
        streak = int(spike_streak) + 1
        if streak < max(1, spike_confirm):
            return ewma_s, True, streak
        mixed = ewma_s * (1.0 - alpha) + sample_s * alpha
        return mixed, False, streak
    mixed = ewma_s * (1.0 - alpha) + sample_s * alpha
    return mixed, False, 0


def bbr_pacing_gain(
    now_s: float,
    rtprop_s: float,
    cycle_t0: float,
    *,
    probe: float = _BBR_PROBE_GAIN,
    drain: float = _BBR_DRAIN_GAIN,
    cruise: float = _BBR_CRUISE_GAIN,
    units: int = _BBR_CYCLE_UNITS,
) -> float:
    """PROBE_BW-style gain: 1.25× then cruise. One unit = RTprop.

    Cyclic 0.75 drain is for emptying a standing queue after a probe. This
    WAN is a lossy policer: drain just yields bandwidth and never builds
    BtlBw. RTT+occupancy still applies ``drain`` via the congested path.
    """
    del drain
    rtprop = max(0.020, float(rtprop_s) if rtprop_s > 0.0 else 0.080)
    period = max(1, int(units)) * rtprop
    pos = (max(0.0, now_s - cycle_t0) % period) / rtprop
    if pos < 1.0:
        return probe
    return cruise


def update_btlbw_bps(
    *,
    delivery_bps: float,
    samples: list[tuple[float, float]],
    now_s: float,
    rtprop_s: float,
    window_rtprop: float = _BBR_BTLBW_RTPROP,
    keep: int = _BBR_SAMPLE_KEEP,
) -> tuple[float, list[tuple[float, float]]]:
    """Windowed max delivery (BBR BtlBw). A slow start expires instead of pinning."""
    out = list(samples)
    if delivery_bps > 1.0:
        out.append((float(now_s), float(delivery_bps)))
    rtprop = max(0.020, float(rtprop_s) if rtprop_s > 0.0 else 0.080)
    window_s = max(_BBR_BTLBW_MIN_S, max(1.0, float(window_rtprop)) * rtprop)
    cutoff = float(now_s) - window_s
    out = [(t, r) for t, r in out if t >= cutoff]
    if len(out) > keep:
        out = out[-keep:]
    if not out:
        return 0.0, out
    return max(r for _, r in out), out


def bbr_delivery_is_path_sample(
    *,
    delivery_bps: float,
    cur_bps: float,
    min_bps: float,
    elapsed_s: float,
    occupancy: int = 0,
    inflight_gen_limit: int = 0,
    warmup_s: float = _BBR_SAMPLE_WARMUP_S,
    cold_frac: float = _BBR_COLD_DELIV_FRAC,
) -> bool:
    """True when delivery is a bottleneck sample, not encode/send warmup.

    FEC overhead is not a speed signal: a 20% blast can still run ~80 MiB/s.
    Occupancy here only distinguishes a full window (real slow path) from an
    empty sender-limited start. It is not used to stay in STARTUP on a
    fast client that keeps the decode pipeline empty.
    """
    if elapsed_s < max(0.0, float(warmup_s)):
        return False
    floor = max(1.0, float(min_bps))
    cap = max(0, int(inflight_gen_limit))
    window_full = cap > 0 and int(occupancy) >= (cap * 3) // 4
    if delivery_bps < floor * 0.90:
        return window_full
    if cur_bps > floor * 1.15 and delivery_bps < cur_bps * max(0.05, float(cold_frac)):
        return window_full
    return True


def bbr_startup_climb_bps(
    *,
    cur_bps: float,
    max_bps: float,
    min_bps: float,
    climb: float = _BBR_STARTUP_GAIN,
) -> float:
    """STARTUP probe from the current offer, not from a cold 200 Mbit BtlBw."""
    floor = max(1.0, float(min_bps))
    return min(float(max_bps), max(float(cur_bps), floor) * max(1.0, float(climb)))


def bbr_delivery_round(
    *,
    delivered_bytes: int,
    blast_wire_bytes: int,
    dt_s: float,
    offer_bps: float,
    overhead_pct: int = 0,
    min_bytes: int = _BBR_STARTUP_MIN_BYTES,
    wire_slack: float = _BBR_DELIVERY_WIRE_SLACK,
) -> dict:
    """Validate an ACK-clocked delivery round against bytes actually blasted."""
    dt = max(1e-6, float(dt_s))
    delivered = max(0, int(delivered_bytes))
    wire = max(0, int(blast_wire_bytes))
    delivery_bps = delivered / dt
    wire_bps = wire / dt
    oh = 1.0 + max(0, int(overhead_pct)) / 100.0
    app_offer_bps = wire_bps / oh
    fill = wire_bps / max(1.0, float(offer_bps))
    ratio = delivery_bps / max(1.0, app_offer_bps)
    enough = delivered >= max(1, int(min_bytes)) and wire >= max(1, int(min_bytes))
    valid = enough and wire_bps > 1.0
    capped_delivery = min(
        delivery_bps,
        max(app_offer_bps, float(offer_bps) / oh) * max(1.0, float(wire_slack)),
    )
    reason = "ok"
    if not enough:
        reason = "small"
        valid = False
    elif fill < 0.70:
        valid = False
        reason = "sender"
    elif delivery_bps > app_offer_bps * max(1.0, float(wire_slack)) and wire_bps > 1.0:
        reason = "capped"
    return {
        "valid": valid,
        "reason": reason,
        "delivery_bps": capped_delivery,
        "startup_bps": capped_delivery * oh,
        "raw_delivery_bps": delivery_bps,
        "wire_bps": wire_bps,
        "app_offer_bps": app_offer_bps,
        "fill": fill,
        "ratio": ratio,
    }


def bbr_startup_step(
    *,
    offer_bps: float,
    confirmed_bps: float,
    prior_delivery_bps: float,
    sample: dict,
    max_bps: float,
    startup_max_bps: float,
    stalled: bool = False,
    congested: bool = False,
    bad_rounds: int = 0,
    flat_rounds: int = 0,
    hol_rounds: int = 0,
    confirm_ratio: float = _BBR_STARTUP_CONFIRM,
    bad_ratio: float = _BBR_STARTUP_BAD,
) -> dict:
    """Advance STARTUP once per complete delivery round."""
    offer = max(1.0, float(offer_bps))
    confirmed = max(0.0, float(confirmed_bps))
    prior = max(0.0, float(prior_delivery_bps))
    if stalled:
        hol = int(hol_rounds) + 1
        if hol >= _BBR_STARTUP_HOL_ROUNDS and confirmed > 1.0:
            return {
                "startup": False,
                "offer_bps": confirmed,
                "confirmed_bps": confirmed,
                "delivery_bps": prior,
                "bad_rounds": int(bad_rounds),
                "flat_rounds": int(flat_rounds),
                "hol_rounds": hol,
                "phase": "exit",
                "reason": "hol",
            }
        return {
            "startup": True,
            "offer_bps": offer,
            "confirmed_bps": confirmed,
            "delivery_bps": prior,
            "bad_rounds": int(bad_rounds),
            "flat_rounds": int(flat_rounds),
            "hol_rounds": hol,
            "phase": "hold",
            "reason": "hol",
        }
    if not bool(sample.get("valid")):
        reason = str(sample.get("reason") or "invalid")
        # fill<0.70 is encode/send/wenc, not a path ceiling. Freezing the
        # offer at 220 until a full-pipe sample arrives wastes the climb
        # after the sender recovers.
        if reason == "sender" and not congested:
            cap = min(
                max(1.0, float(max_bps)), max(1.0, float(startup_max_bps))
            )
            slow_at = _BBR_STARTUP_SLOW_MBIT * 1_000_000 / 8
            gain = (
                _BBR_STARTUP_SLOW_GAIN
                if offer >= slow_at
                else _BBR_STARTUP_GAIN
            )
            return {
                "startup": True,
                "offer_bps": min(cap, offer * gain),
                "confirmed_bps": confirmed,
                "delivery_bps": prior,
                "bad_rounds": int(bad_rounds),
                "flat_rounds": int(flat_rounds),
                "hol_rounds": 0,
                "phase": "warmup",
                "reason": "sender",
            }
        return {
            "startup": True,
            "offer_bps": offer,
            "confirmed_bps": confirmed,
            "delivery_bps": prior,
            "bad_rounds": int(bad_rounds),
            "flat_rounds": int(flat_rounds),
            "hol_rounds": 0,
            "phase": "warmup",
            "reason": reason,
        }
    delivery = max(
        0.0,
        float(sample.get("startup_bps") or sample.get("delivery_bps") or 0.0),
    )
    ratio = max(0.0, float(sample.get("ratio") or 0.0))
    bad = int(bad_rounds)
    flat = int(flat_rounds)
    if congested or ratio < float(bad_ratio):
        bad += 1
    else:
        bad = 0
    grew = prior <= 1.0 or delivery >= prior * _BBR_STARTUP_FLAT_GROW
    confirmed_now = ratio >= float(confirm_ratio) and not congested
    if confirmed_now:
        confirmed = max(confirmed, offer)
        grew = True
    flat = 0 if grew else flat + 1
    exit_reason = ""
    if bad >= _BBR_STARTUP_BAD_ROUNDS:
        exit_reason = "under"
    elif flat >= _BBR_STARTUP_FLAT:
        exit_reason = "flat"
    cap = min(max(1.0, float(max_bps)), max(1.0, float(startup_max_bps)))
    if offer >= cap * 0.995 and (not grew or confirmed_now):
        exit_reason = "cap"
    if exit_reason:
        cruise = max(confirmed, delivery)
        return {
            "startup": False,
            "offer_bps": min(cap, max(1.0, cruise)),
            "confirmed_bps": max(confirmed, delivery),
            "delivery_bps": delivery,
            "bad_rounds": bad,
            "flat_rounds": flat,
            "hol_rounds": 0,
            "phase": "exit",
            "reason": exit_reason,
        }
    if not confirmed_now:
        return {
            "startup": True,
            "offer_bps": offer,
            "confirmed_bps": confirmed,
            "delivery_bps": delivery,
            "bad_rounds": bad,
            "flat_rounds": flat,
            "hol_rounds": 0,
            "phase": "probe",
            "reason": "unconfirmed",
        }
    slow_at = _BBR_STARTUP_SLOW_MBIT * 1_000_000 / 8
    gain = _BBR_STARTUP_SLOW_GAIN if offer >= slow_at else _BBR_STARTUP_GAIN
    next_offer = min(cap, offer * gain)
    # Headroom only caps an oversized jump, never a confirmed 1.25× step.
    if delivery > offer:
        next_offer = min(next_offer, delivery * _BBR_STARTUP_DELIVERY_HEADROOM)
    return {
        "startup": True,
        "offer_bps": max(offer, next_offer),
        "confirmed_bps": confirmed,
        "delivery_bps": delivery,
        "bad_rounds": bad,
        "flat_rounds": flat,
        "hol_rounds": 0,
        "phase": "probe",
        "reason": "confirmed",
    }


def bbr_still_startup(
    *,
    startup: bool,
    btlbw_bps: float,
    round_btlbw_bps: float,
    occupancy: int = 0,
    inflight_gen_limit: int = 0,
    grow: float = _BBR_STARTUP_GROW,
    min_bps: float = 0.0,
    elapsed_s: float = 0.0,
    warmup_s: float = 0.0,
    path_sample: bool = True,
    floor_mult: float = _BBR_STARTUP_FLOOR_MULT,
) -> bool:
    """Stay in STARTUP while BtlBw is still climbing.

    Occupancy is decode lag, not wire inflight: a client that keeps up looks
    like an empty pipeline even when the path is already the bottleneck.
    PROBE_BW (1.25×) is how we keep looking for more after a plateau.

    A plateau on the CC floor with an empty window is warmup, not BtlBw.
    A full window at that floor is a real slow path and may exit.
    """
    if not startup:
        return False
    if elapsed_s < max(0.0, float(warmup_s)):
        return True
    if not path_sample:
        return True
    floor = max(0.0, float(min_bps))
    if floor > 0.0 and btlbw_bps < floor * max(1.0, float(floor_mult)):
        cap = max(0, int(inflight_gen_limit))
        window_full = cap > 0 and int(occupancy) >= (cap * 3) // 4
        if not window_full:
            return True
    if round_btlbw_bps <= 1.0:
        return True
    return btlbw_bps >= round_btlbw_bps * max(1.0, float(grow))


def bbr_startup_advance(
    *,
    still_growing: bool,
    btlbw_bps: float,
    round_btlbw_bps: float,
    flat_rounds: int,
    flat_need: int = _BBR_STARTUP_FLAT,
) -> tuple[bool, int, float]:
    """One RTprop STARTUP check. Return (startup, flat_rounds, round_btlbw)."""
    if still_growing:
        return True, 0, float(btlbw_bps)
    flat = int(flat_rounds) + 1
    return flat < max(1, int(flat_need)), flat, float(round_btlbw_bps)


def update_delivery_rate_pace_bps(
    *,
    btlbw_bps: float,
    max_bps: float,
    min_bps: float,
    gain: float,
    overhead_pct: int = 0,
) -> float:
    """Pace = BtlBw × gain × (1+FEC). --rate is a safety cap, not the setpoint."""
    floor = max(1.0, min_bps)
    bw = max(float(btlbw_bps), floor)
    oh = 1.0 + max(0, int(overhead_pct)) / 100.0
    target = bw * max(0.50, float(gain)) * oh
    return min(max_bps, max(floor, target))


def update_delay_pace_bps(
    *,
    rtt_s: float,
    min_rtt_s: float,
    cur_bps: float,
    max_bps: float,
    min_frac: float = _DELAY_CC_MIN_FRAC,
    target_queue_s: float | None = None,
    high_queue_s: float | None = None,
    down: float = _DELAY_CC_DOWN,
    down_hard: float = _DELAY_CC_DOWN_HARD,
    up: float = _DELAY_CC_UP,
    up_probe: float = _DELAY_CC_UP_PROBE,
    min_rtt_grow: float = _DELAY_CC_MIN_RTT_GROW,
) -> tuple[float, float, float]:
    """Delay-based pacing: probe up from a low start, back off on standing queue.

    Returns (new_bps, new_min_rtt_s, queue_delay_s).
    """
    floor = max_bps * min_frac
    if rtt_s <= 0 or rtt_s > _DELAY_CC_MAX_RTT_S:
        return cur_bps, min_rtt_s, 0.0
    if min_rtt_s <= 0 or rtt_s < min_rtt_s:
        min_rtt_s = rtt_s
    if target_queue_s is None:
        target_queue_s = max(
            _DELAY_CC_TARGET_QUEUE_MIN_S,
            min_rtt_s * _DELAY_CC_TARGET_QUEUE_RTT_FRAC,
        )
    if high_queue_s is None:
        high_queue_s = max(
            _DELAY_CC_HIGH_QUEUE_MIN_S,
            min_rtt_s * _DELAY_CC_HIGH_QUEUE_RTT_FRAC,
        )
    queue_s = max(0.0, rtt_s - min_rtt_s)
    if queue_s >= high_queue_s:
        # Age min_rtt only under sustained delay so lows can expire.
        min_rtt_s = min(rtt_s, min_rtt_s * min_rtt_grow)
        queue_s = max(0.0, rtt_s - min_rtt_s)
        return max(floor, cur_bps * down_hard), min_rtt_s, queue_s
    if queue_s >= target_queue_s:
        return max(floor, cur_bps * down), min_rtt_s, queue_s
    if queue_s <= target_queue_s * 0.70:
        # Fast probe only while far below the cap. 1.40× from mid-range
        # overshoots a ~400 Mbit policer that still looks RTT-clean.
        step = (
            up_probe if cur_bps < max_bps * _DELAY_CC_UP_PROBE_BELOW_FRAC else up
        )
        return min(max_bps, max(cur_bps, floor) * step), min_rtt_s, queue_s
    return cur_bps, min_rtt_s, queue_s


def update_delivery_guard_bps(
    *,
    delivery_bps: float,
    cur_bps: float,
    overhead_pct: int,
    min_bps: float,
    wire_bps: float = 0.0,
    fill_frac: float = 0.80,
    window_full: bool = False,
    bad_frac: float = _DELAY_CC_DELIVERY_BAD,
    ok_frac: float = _DELAY_CC_DELIVERY_OK,
    cut: float = _DELAY_CC_DELIVERY_CUT,
    cut_hard: float = _DELAY_CC_DELIVERY_CUT_HARD,
) -> tuple[float, str]:
    """Cut pace when goodput lags bytes actually put on the wire.

    Compare to send volume, not the limiter target: a 900 Mbit ceiling with
    80 MiB/s wire is encode/pacer limited, not a policer. When the inflight
    window is full we are paused, wire collapses, and that fill check would
    never fire — then compare to the limiter target instead.
    """
    if (
        (not window_full)
        and wire_bps > 0.0
        and wire_bps < cur_bps * fill_frac
    ):
        return cur_bps, "hold"
    offer = cur_bps if window_full else (wire_bps if wire_bps > 0.0 else cur_bps)
    oh = max(0, int(overhead_pct)) / 100.0
    expected_app = offer / (1.0 + oh)
    if expected_app < 1.0 or delivery_bps <= 0.0:
        return cur_bps, "hold"
    ratio = delivery_bps / expected_app
    floor = max(1.0, min_bps)
    if ratio < 0.35:
        return max(floor, cur_bps * cut_hard), "cut_hard"
    if ratio < bad_frac:
        return max(floor, cur_bps * cut), "cut"
    if ratio >= ok_frac:
        return cur_bps, "ok"
    return cur_bps, "hold"


def should_yield_blast_to_repair(
    pressure: float,
    *,
    repair_pending: bool,
    nack_count: int,
    hol_hole: int = 0,
) -> bool:
    """Dual-queue: under occupancy, drain repair before blast.

    A decode-ahead HOL hole without occupancy is reorder/random-access, not a
    reason to starve first-pass.
    """
    del hol_hole
    if pressure >= 0.85:
        return True
    if pressure >= 0.45 and (repair_pending or nack_count > 12):
        return True
    if pressure >= 0.25 and repair_pending and nack_count > 48:
        return True
    return False


def hol_repair_cooldown_s(
    nid: int,
    next_needed: int,
    *,
    hol_miss: bool = False,
) -> float:
    """HOL sparse resend is quicker; other gens keep the full cooldown."""
    if nid == next_needed and hol_miss:
        return _HOL_REPAIR_COOLDOWN_S
    return _REPAIR_COOLDOWN_S


def reorder_holdoff_s() -> float:
    raw = os.environ.get("TETRYS_REORDER_HOLDOFF_MS")
    if raw is None or raw == "":
        return _REORDER_HOLDOFF_S
    try:
        return max(0.0, float(raw) / 1000.0)
    except ValueError:
        return _REORDER_HOLDOFF_S


def note_gen_deficit(deficit_since: dict[int, float], gid: int, now: float) -> None:
    if gid >= 0 and gid not in deficit_since:
        deficit_since[gid] = now


def clear_gen_deficit(deficit_since: dict[int, float], gid: int) -> None:
    deficit_since.pop(gid, None)


def repair_holdoff_ready(
    deficit_since: dict[int, float],
    gid: int,
    now: float,
    *,
    holdoff_s: float | None = None,
) -> bool:
    """True once an incomplete gen has been open longer than the reorder window."""
    first = deficit_since.get(gid)
    if first is None:
        return False
    wait = _REORDER_HOLDOFF_S if holdoff_s is None else holdoff_s
    return (now - first) >= wait


def client_feedback_interval(
    *,
    fin_seen: bool,
    next_needed: int,
    total_gens: int,
    open_gens: int,
    nack_count: int,
) -> float:
    still_open = next_needed < total_gens or open_gens > 0 or nack_count > 0
    if fin_seen and not still_open:
        return _FB_EVERY_FIN_S
    return _FB_EVERY_S


def repair_thread_limit(
    *,
    at_cap: bool,
    storm_active: bool,
    nack_count: int,
    frontier_lag: int,
    inflight_gen_limit: int,
) -> int:
    """Thin repair parallelism. 16-way at cap starved blast on WAN."""
    del nack_count
    if storm_active:
        if frontier_lag >= max(64, inflight_gen_limit // 4):
            return 2 if at_cap else 1
        return 1
    if at_cap:
        return _REPAIR_AT_CAP
    return _REPAIR_PER_TURN


def fountain_redundancy(overhead_pct: int) -> bool:
    """True when CLI overhead is 0 (repair via fountain bootstrap + cap top-up)."""
    return overhead_pct <= 0


def repair_round_size(gen_k: int, overhead_pct: int) -> int:
    """Default repair symbols per generation or fountain round."""
    pct = overhead_pct if overhead_pct > 0 else _FOUNTAIN_TARGET_OVERHEAD_PCT
    return max(1, repair_count(gen_k, pct))


def cap_fountain_send(
    gen_k: int,
    prior_extra: int,
    send_n: int | None,
    symbols_rx: int | None,
    *,
    decode_margin: int = _DECODE_MARGIN,
    round_max: int = _REPAIR_ROUND_MAX,
) -> int:
    """Cap fountain repair to per-gen budget; allow deficit top-up beyond."""
    budget = repair_round_size(gen_k, 0)
    if prior_extra >= budget:
        if symbols_rx is None:
            if prior_extra >= gen_k:
                return 0
            want = send_n if send_n is not None else 2
            return max(1, min(int(want), 4, round_max))
        if symbols_rx < gen_k + decode_margin:
            deficit = max(1, gen_k + decode_margin - symbols_rx)
            want = send_n if send_n is not None else deficit
            return max(1, min(int(want), round_max))
        return 0
    room = budget - prior_extra
    want = send_n if send_n is not None else room
    return max(1, min(int(want), room, round_max))


def pipeline_stressed(
    incomplete: int,
    lag: int,
    *,
    inflight_gen_limit: int,
) -> bool:
    """Fountain pipeline when unrecovered occupancy hits the blast pause."""
    return should_pause_blast(
        incomplete, lag, inflight_gen_limit=inflight_gen_limit
    )


def update_adaptive_pace_bps(
    *,
    delivery_bps: float,
    cur_bps: float,
    max_bps: float,
    good_streak: int,
    bad_streak: int = 0,
    min_frac: float = _ADAPT_PACE_MIN_FRAC,
    backoff_frac: float = _ADAPT_PACE_BACKOFF_FRAC,
    recover_frac: float = _ADAPT_PACE_RECOVER_FRAC,
    recover_step: float = _ADAPT_PACE_RECOVER_STEP,
    good_snap: int = _ADAPT_PACE_GOOD_SNAP,
) -> tuple[float, int, int]:
    """Backoff-only pacing: hold --rate unless delivery clearly lags."""
    floor = max_bps * min_frac
    if delivery_bps < cur_bps * backoff_frac:
        bad_streak += 1
        if bad_streak >= _ADAPT_PACE_BAD_SNAP:
            return max(floor, delivery_bps * 1.08), 0, 0
        return cur_bps, 0, bad_streak
    bad_streak = 0
    if delivery_bps >= max_bps * recover_frac:
        streak = good_streak + 1
        if streak >= good_snap:
            return max_bps, 0, 0
        if cur_bps < max_bps:
            return min(max_bps, cur_bps * recover_step), streak, 0
        return cur_bps, streak, 0
    return cur_bps, good_streak, 0


def should_fountain_tick(
    *,
    stressed: bool,
    at_cap: bool,
    tick_n: int,
    every_n: int = _FOUNTAIN_EVERY_N,
    storm_active: bool = False,
    fountain_mode: bool = False,
    pressure: float = 0.0,
    nack_count: int = 0,
) -> bool:
    """Fountain on pause/stress, or when occupancy+NACKs say gens need filling.

    A blackout leaves occupancy high but below the 75% pause; waiting for
    pause means the send loop never top-ups and drain times out.
    Clean WAN pressure stays ~0.1, so this does not spray on reorder NACKs.
    """
    if storm_active:
        return at_cap and stressed
    if fountain_mode:
        # 8 MiB / loss-half never hits inflight cap (1035); still need ESI.
        return at_cap or tick_n % every_n == 0
    need = stressed or (pressure >= _FOUNTAIN_PRESSURE and nack_count >= 8)
    return need and (at_cap or tick_n % every_n == 0)


def should_track_fountain_gen(
    gen_id: int,
    next_needed: int,
    *,
    at_cap: bool,
    window: int = _FOUNTAIN_WINDOW,
) -> bool:
    """Track gens near the frontier for hybrid async fountain repair."""
    if gen_id < next_needed:
        return False
    limit = window if at_cap else min(window // 2, _FOUNTAIN_CAP_GENS + 4)
    return gen_id < next_needed + limit


def cap_fountain_gens(
    fountain_gens: set[int],
    next_needed: int,
    *,
    max_track: int = _FOUNTAIN_TRACK_MAX,
) -> None:
    for gid in list(fountain_gens):
        if gid < next_needed:
            fountain_gens.discard(gid)
    while len(fountain_gens) > max_track:
        drop = min(fountain_gens)
        if drop < next_needed:
            fountain_gens.discard(drop)
            continue
        fountain_gens.discard(drop)


def prune_fountain_gens_set(
    fountain_gens: set[int],
    next_needed: int,
    nack_rx: dict[int, int],
    nacks: list[int],
    *,
    gen_k: int,
    sent_before: int,
    inflight_gen_limit: int,
    window: int = _FOUNTAIN_WINDOW,
    max_track: int = _FOUNTAIN_TRACK_MAX,
) -> None:
    """Drop stale fountain gens outside the active frontier / inflight window."""
    enough = gen_k + _DECODE_MARGIN
    hi = next_needed + window
    lo = max(0, sent_before - inflight_gen_limit)
    for gid in list(fountain_gens):
        if gid < next_needed or gid >= hi or gid < lo:
            fountain_gens.discard(gid)
            continue
        rx = nack_rx.get(gid)
        if rx is not None and rx >= enough and gid not in nacks:
            fountain_gens.discard(gid)
    cap_fountain_gens(fountain_gens, next_needed, max_track=max_track)


def repair_storm_detected(
    repair_pkts_per_s: float,
    fountain_count: int,
    *,
    storm_pkts_s: float = _REPAIR_STORM_PKTS_S,
    track_max: int = _FOUNTAIN_TRACK_MAX,
) -> bool:
    return repair_pkts_per_s >= storm_pkts_s or fountain_count > track_max * 2


def prune_repair_meta(
    repair_extra: dict[int, int],
    last_repair_ts: dict[int, float],
    last_full_ts: dict[int, float],
    next_needed: int,
    *,
    keep: int = _REPAIR_META_KEEP,
) -> None:
    """Prevent repair bookkeeping dicts from growing with every touched gen.

    Never evict next_needed: a HOL hole larger than `keep` used to drop the
    stuck gen's repair_extra, so the worker cache looked 'already full' and
    drain sent 0 forever.
    """
    hol = int(next_needed)
    for meta in (repair_extra, last_repair_ts, last_full_ts):
        for gid in list(meta):
            if gid < hol:
                meta.pop(gid, None)
        while len(meta) > keep:
            candidates = [g for g in list(meta) if g != hol]
            if not candidates:
                break
            meta.pop(min(candidates), None)


def track_fountain_gen(
    fountain_gens: set[int],
    gen_id: int,
    next_needed: int,
    *,
    at_cap: bool,
) -> None:
    if not should_track_fountain_gen(gen_id, next_needed, at_cap=at_cap):
        return
    fountain_gens.add(gen_id)
    cap_fountain_gens(fountain_gens, next_needed)


def even_spread(items: list[int], k: int, *, offset: int = 0) -> list[int]:
    """Pick k ids evenly across a sorted window so repair is not HOL-clustered."""
    if k <= 0 or not items:
        return []
    n = len(items)
    if n <= k:
        return list(items)
    out: list[int] = []
    seen: set[int] = set()
    for i in range(k):
        idx = (int(offset) + (i * n) // k) % n
        gid = items[idx]
        if gid in seen:
            continue
        seen.add(gid)
        out.append(gid)
    if len(out) < k:
        for gid in items:
            if gid in seen:
                continue
            out.append(gid)
            if len(out) >= k:
                break
    return out


def gen_rank_deficit(
    symbols_rx: int | None,
    gen_k: int,
    *,
    decode_margin: int = _DECODE_MARGIN,
) -> int | None:
    """Packets still needed for likely decode. None = unknown rank."""
    if symbols_rx is None:
        return None
    return max(0, int(gen_k) + int(decode_margin) - max(0, int(symbols_rx)))


def hol_blocks_tail_repair(
    symbols_rx: int | None,
    gen_k: int,
    *,
    decode_margin: int = _DECODE_MARGIN,
    near_complete: int = _FOUNTAIN_NEAR_COMPLETE,
) -> bool:
    """True when next_needed is far from rank; do not spray the tail."""
    d = gen_rank_deficit(symbols_rx, gen_k, decode_margin=decode_margin)
    return d is not None and d > near_complete


def repair_overhead_pct(overhead_pct: int) -> int:
    """Pad repair with the live FEC calculator, not a fixed surplus.

    `overhead_pct=0` (fountain) uses the same 8% bootstrap as the blast path.
    """
    pct = int(overhead_pct)
    if pct > 0:
        return pct
    return _FOUNTAIN_TARGET_OVERHEAD_PCT


def hol_resend_pad_n(miss_n: int, overhead_pct: int) -> int:
    """Fountain extras after sparse HOL ESI resend."""
    n = max(0, int(miss_n))
    if n <= 0:
        return 0
    return repair_extra_n(n, overhead_pct=overhead_pct)


def repair_extra_n(
    deficit: int,
    *,
    overhead_pct: int | None = None,
    surplus: float = 0.0,
    abs_pad: int = _REPAIR_ABS_PAD,
) -> int:
    """Extras on top of rank deficit: absolute pad wins on small holes."""
    d = max(0, int(deficit))
    if d <= 0:
        return 0
    extra = max(0, int(abs_pad))
    if overhead_pct is not None:
        pad = repair_overhead_pct(overhead_pct) / 100.0
        pct_extra = math.ceil(d * pad) if pad > 0 else 0
        if pct_extra == 0 and pad > 0:
            pct_extra = 1
        extra = max(extra, int(pct_extra))
    elif surplus > 0:
        extra = max(extra, int(math.ceil(d * max(0.0, float(surplus)))))
    return extra


def note_close_round(hist: list[int], rounds: int) -> None:
    """Bucket a closed hole by how many NACK/repair rounds it took."""
    if rounds <= 0 or not hist:
        return
    idx = min(int(rounds), len(hist)) - 1
    hist[idx] += 1


def format_close_rounds(hist: list[int], *, label: str) -> str:
    names = ("r1", "r2", "r3", "r4", "r5+")
    n = min(len(hist), len(names))
    body = " ".join(f"{names[i]}={int(hist[i])}" for i in range(n))
    return f"{label} {body} n={sum(hist[:n])}"


def nack_close_rounds(elapsed_s: float, rtt_s: float | None) -> int:
    """How many RTTs a NACK'd gen stayed open. Not feedback-tick count.

    A hole closed by the first repair is typically ~1 RTT (~80ms) plus
    holdoff; that used to land in r5+ because ACK is every 20ms.
    """
    rtt = float(rtt_s) if rtt_s is not None and rtt_s >= 0.02 else 0.08
    if elapsed_s <= 0:
        return 1
    return max(1, int(elapsed_s / rtt + 0.5))


def repair_send_n(
    symbols_rx: int | None,
    gen_k: int,
    *,
    hol: bool = False,
    surplus: float = _REPAIR_SURPLUS,
    decode_margin: int = _DECODE_MARGIN,
    round_max: int | None = None,
    probe: int | None = None,
    overhead_pct: int | None = None,
    close: bool = False,
) -> int:
    """Deficit plus an absolute pad so a small patch survives burst loss.

    Blast FEC percent is the wrong scale for a 4-symbol hole (10% → 1 packet).
    """
    deficit = gen_rank_deficit(symbols_rx, gen_k, decode_margin=decode_margin)
    if deficit is None:
        cap = int(round_max) if round_max is not None else _FOUNTAIN_SEND_MAX
        want = _REPAIR_ABS_PAD + _FOUNTAIN_PROBE if probe is None else int(probe)
        return max(1, min(int(want), max(1, cap)))
    if deficit <= 0:
        return 0
    if close:
        cap = int(round_max) if round_max is not None else _REPAIR_ROUND_MAX
    elif round_max is not None:
        cap = max(1, int(round_max))
    elif deficit > max(1, int(gen_k)) // 2:
        cap = _FOUNTAIN_EMPTY_HOL if hol else _FOUNTAIN_EMPTY_TAIL
    elif hol:
        cap = _FOUNTAIN_SEND_MAX
    else:
        cap = _FOUNTAIN_TAIL_MAX
    extra = repair_extra_n(
        deficit,
        overhead_pct=overhead_pct,
        surplus=0.0 if overhead_pct is not None else surplus,
    )
    n = deficit + extra
    empty_cap = _FOUNTAIN_EMPTY_HOL if hol or close else _FOUNTAIN_EMPTY_TAIL
    cap = max(cap, min(int(n), empty_cap))
    if close:
        cap = max(cap, _REPAIR_ROUND_MAX)
    return max(1, min(int(n), cap))


def select_repair_feedback_gens(
    next_needed: int,
    open_rx: dict[int, int],
    *,
    gen_k: int,
    limit: int,
    decode_margin: int = _DECODE_MARGIN,
    hol_keep: int = _FOUNTAIN_HOL_KEEP,
    near_complete: int = _FOUNTAIN_NEAR_COMPLETE,
    offset: int = 0,
) -> list[int]:
    """HOL + cheap closes + even sample of the rest of the open window."""
    del hol_keep
    if limit <= 0:
        return []
    enough = gen_k + decode_margin
    out: list[int] = []
    seen: set[int] = set()

    def add(gid: int) -> bool:
        if gid < 0 or gid in seen:
            return len(out) >= limit
        rx = open_rx.get(gid)
        if rx is None or rx >= enough:
            return len(out) >= limit
        seen.add(gid)
        out.append(gid)
        return len(out) >= limit

    if add(int(next_needed)):
        return out
    near: list[tuple[int, int]] = []
    rest: list[int] = []
    for gid, rx in open_rx.items():
        if gid in seen:
            continue
        d = gen_rank_deficit(rx, gen_k, decode_margin=decode_margin)
        if d is None or d <= 0:
            continue
        if d <= near_complete:
            near.append((d, gid))
        else:
            rest.append(gid)
    near.sort()
    for _, gid in near:
        if add(gid):
            return out
    rest.sort()
    for gid in even_spread(rest, limit - len(out), offset=offset):
        if add(gid):
            return out
    return out


def order_repair_nacks(
    nacks: list[int],
    next_needed: int,
    nack_rx: dict[int, int],
    *,
    gen_k: int,
    limit: int,
    sent_before: int,
    hol_keep: int = _FOUNTAIN_HOL_KEEP,
    near_complete: int = _FOUNTAIN_NEAR_COMPLETE,
    decode_margin: int = _DECODE_MARGIN,
    offset: int = 0,
) -> list[int]:
    """One HOL slot, cheap closes, then an even slice of the open window."""
    del hol_keep
    if limit <= 0 or sent_before <= 0:
        return []
    cands = [g for g in nacks if 0 <= g < sent_before]
    if not cands:
        return []
    out: list[int] = []
    seen: set[int] = set()

    def add(gid: int) -> bool:
        if gid in seen:
            return len(out) >= limit
        seen.add(gid)
        out.append(gid)
        return len(out) >= limit

    if next_needed in cands and add(next_needed):
        return out
    near: list[int] = []
    rest: list[int] = []
    for gid in cands:
        if gid in seen:
            continue
        d = gen_rank_deficit(
            nack_rx.get(gid), gen_k, decode_margin=decode_margin
        )
        if d is None or d <= 0:
            continue
        if d <= near_complete:
            near.append(gid)
        else:
            rest.append(gid)
    near.sort(
        key=lambda g: gen_rank_deficit(
            nack_rx.get(g), gen_k, decode_margin=decode_margin
        )
        or 0
    )
    for gid in near:
        if add(gid):
            return out
    rest.sort()
    for gid in even_spread(rest, limit - len(out), offset=offset):
        if add(gid):
            return out
    return out


def fountain_targets(
    next_needed: int,
    sent_before: int,
    nacks: list[int],
    nack_rx: dict[int, int],
    *,
    gen_k: int,
    limit: int,
    decode_margin: int = _DECODE_MARGIN,
    frontier_only: bool = False,
    hol_keep: int = _FOUNTAIN_HOL_KEEP,
    near_complete: int = _FOUNTAIN_NEAR_COMPLETE,
    offset: int = 0,
) -> list[int]:
    """HOL first, cheap closes, even spread. Never walk the decoded tail."""
    del hol_keep
    if sent_before <= 0 or limit <= 0:
        return []
    enough = gen_k + decode_margin
    out: list[int] = []
    seen: set[int] = set()

    def add(gid: int, *, allow_unknown: bool = False) -> bool:
        if gid < 0 or gid >= sent_before or gid in seen:
            return len(out) >= limit
        rx = nack_rx.get(gid)
        if rx is not None and rx >= enough:
            return len(out) >= limit
        if rx is None and not allow_unknown:
            return len(out) >= limit
        seen.add(gid)
        out.append(gid)
        return len(out) >= limit

    if add(next_needed, allow_unknown=True):
        return out
    if frontier_only:
        return out
    near: list[tuple[int, int]] = []
    rest: list[int] = []
    for gid in nacks:
        if gid in seen or gid < 0 or gid >= sent_before:
            continue
        rx = nack_rx.get(gid)
        if rx is None:
            continue
        d = max(0, enough - rx)
        if d <= 0:
            continue
        if d <= near_complete:
            near.append((d, gid))
        else:
            rest.append(gid)
    near.sort()
    for _, gid in near:
        if add(gid):
            return out
    rest.sort()
    for gid in even_spread(rest, limit - len(out), offset=offset):
        if add(gid):
            return out
    return out


class _Win:
    """1s window counters; lock-free from the send thread, locked for workers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.enc_s = 0.0
        self.repair_enc_s = 0.0
        self.qdrop = 0

    def add_enc(self, s: float) -> None:
        with self._lock:
            self.enc_s += s

    def add_repair_enc(self, s: float) -> None:
        with self._lock:
            self.repair_enc_s += s

    def add_qdrop(self) -> None:
        with self._lock:
            self.qdrop += 1

    def take_workers(self) -> tuple[float, float, int]:
        with self._lock:
            out = (self.enc_s, self.repair_enc_s, self.qdrop)
            self.enc_s = 0.0
            self.repair_enc_s = 0.0
            self.qdrop = 0
            return out


def _pct(part: float, wall: float) -> int:
    if wall <= 1e-9:
        return 0
    return int(round(100.0 * min(part, wall) / wall))


def encode_read_gens_from_env(default: int = _ENCODE_READ_GENS) -> int:
    raw = os.environ.get("TETRYS_ENCODE_READ_GENS")
    if raw is None or raw == "":
        n = default
    else:
        try:
            n = int(raw)
        except ValueError:
            n = default
    return max(1, min(128, n))


def encode_batch_start(gid: int, batch_n: int) -> int:
    n = max(1, int(batch_n))
    return (max(0, int(gid)) // n) * n


def disk_queue_mib_from_env(default_mib: int = _DISK_QUEUE_MIB) -> int:
    raw = os.environ.get("TETRYS_DISK_QUEUE_MIB")
    if raw is None or raw == "":
        mib = default_mib
    else:
        try:
            mib = int(raw)
        except ValueError:
            mib = default_mib
    return max(0, min(512, mib)) * 1024 * 1024


def disk_queue_max_mib_from_env(
    min_mib_bytes: int,
    default_max_mib: int = _DISK_QUEUE_MAX_MIB,
) -> int:
    raw = os.environ.get("TETRYS_DISK_QUEUE_MAX_MIB")
    if raw is None or raw == "":
        mib = default_max_mib
    else:
        try:
            mib = int(raw)
        except ValueError:
            mib = default_max_mib
    hi = max(0, min(512, mib)) * 1024 * 1024
    return max(int(min_mib_bytes), hi)


def disk_queue_adapt_from_env() -> bool:
    raw = os.environ.get("TETRYS_DISK_QUEUE_ADAPT", "0")
    return raw.lower() in ("1", "true", "yes")


def mem_available_bytes() -> int | None:
    """Linux MemAvailable; None if unknown (caller may still grow)."""
    try:
        with open("/proc/meminfo", encoding="ascii") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def disk_queue_adapt_bytes(
    *,
    current_bytes: int,
    min_bytes: int,
    max_bytes: int,
    queued_frac: float,
    disk_bps: float,
    send_bps: float,
    available_bytes: int | None = None,
    hungry_frac: float = _DISK_PACE_HUNGRY_FRAC,
    step_bytes: int = _DISK_QUEUE_ADAPT_STEP_MIB * 1024 * 1024,
    min_avail_bytes: int = _DISK_QUEUE_ADAPT_MIN_AVAIL_MIB * 1024 * 1024,
    fast_frac: float = _DISK_QUEUE_ADAPT_FAST_FRAC,
    fast_min_bps: float = _DISK_QUEUE_ADAPT_FAST_MIN_BPS,
) -> int:
    """Grow prefetch when disk can feed the path; shrink when it cannot.

    A hungry queue on a fast disk/cache means 32 MiB is not enough lookahead.
    A hungry queue on a slow disk means extra RAM would steal page cache.
    """
    lo = max(0, int(min_bytes))
    hi = max(lo, int(max_bytes))
    if hi <= 0:
        return 0
    cur = max(lo, min(hi, int(current_bytes)))
    q = min(1.0, max(0.0, float(queued_frac)))
    disk = max(0.0, float(disk_bps))
    send = max(1.0, float(send_bps))
    step = max(1, int(step_bytes))
    ram_ok = available_bytes is None or int(available_bytes) >= int(min_avail_bytes)
    fast = disk >= max(send * float(fast_frac), float(fast_min_bps))
    if not ram_ok:
        return lo
    # Slow disk: drain extra prefetch so RAM stays in page cache.
    if cur > lo and not fast:
        return max(lo, cur - step)
    if fast and q < hungry_frac and cur < hi:
        return min(hi, cur + step)
    return cur


def disk_queue_pop_blob(
    state: dict,
    cond: threading.Condition,
    b0: int,
) -> bytes | None:
    """Take one sequential batch blob for encode; None if not primed yet."""
    with cond:
        blob = state["blobs"].pop(int(b0), None)
        if blob is None:
            return None
        state["queued"] = max(0, int(state["queued"]) - len(blob))
        cond.notify_all()
        return blob


def disk_queue_note_read(
    state: dict,
    n: int,
    now: float,
    *,
    elapsed: float | None = None,
) -> None:
    """Update sequential-read EWMA from the last pread, not wall-clock gaps.

    Queue-full waits and repair stalls must not smear into MiB/s: a 4 MiB
    read after a 2s pause is still a fast disk, not 2 MiB/s.
    """
    n = max(0, int(n))
    if n <= 0:
        return
    state["read_bytes"] = int(state.get("read_bytes") or 0) + n
    if elapsed is not None:
        el = float(elapsed)
        if el <= 0.0 or el >= _DISK_PACE_IDLE_S:
            return
        inst = n / el
        prev = float(state.get("rate_bps") or 0.0)
        state["rate_bps"] = inst if prev <= 0.0 else (0.55 * prev + 0.45 * inst)
        state["rate_ts"] = float(now)
        state["rate_at"] = int(state["read_bytes"])
        return
    prev_ts = float(state.get("rate_ts") or 0.0)
    if prev_ts <= 0.0:
        state["rate_ts"] = float(now)
        state["rate_at"] = int(state["read_bytes"])
        return
    dt = float(now) - prev_ts
    if dt < _DISK_PACE_RATE_DT_S:
        return
    if dt >= _DISK_PACE_IDLE_S:
        state["rate_ts"] = float(now)
        state["rate_at"] = int(state["read_bytes"])
        return
    delta = int(state["read_bytes"]) - int(state.get("rate_at") or 0)
    inst = delta / dt
    prev = float(state.get("rate_bps") or 0.0)
    state["rate_bps"] = inst if prev <= 0.0 else (0.55 * prev + 0.45 * inst)
    state["rate_ts"] = float(now)
    state["rate_at"] = int(state["read_bytes"])


def disk_feed_cap_bps(
    *,
    queued_frac: float,
    disk_bps: float,
    max_bps: float,
    min_bps: float,
    capping: bool = False,
    hungry_frac: float = _DISK_PACE_HUNGRY_FRAC,
    full_frac: float = _DISK_PACE_FULL_FRAC,
    headroom: float = _DISK_PACE_HEADROOM,
    fast_frac: float = _DISK_PACE_FAST_FRAC,
) -> tuple[float, bool]:
    """Ceiling on send rate from RAM-queue occupancy.

    Full queue means the reader is ahead of send — network CC owns pace.
    A draining queue means --rate would blast holes; cap at the read EWMA.
    Hysteresis (hungry→full) avoids flapping around the threshold.
    Does not rewrite BtlBw; the caller applies this as a send-time min().
    """
    ceiling = max(1.0, float(max_bps))
    floor = max(1.0, float(min_bps))
    q = min(1.0, max(0.0, float(queued_frac)))
    if capping:
        hold = q < full_frac
    else:
        hold = q < hungry_frac
    if not hold or disk_bps <= 0.0 or disk_bps >= ceiling * fast_frac:
        return ceiling, False
    cap = min(ceiling, max(floor, float(disk_bps) * headroom))
    return cap, True


def disk_queue_drop_consumed(
    state: dict,
    *,
    gen_id: int,
    batch_n: int,
    block_bytes: int,
) -> None:
    """Drop batches the send cursor has already passed."""
    del batch_n
    gid = max(0, int(gen_id))
    drop: list[int] = []
    for b0, blob in state["blobs"].items():
        n = max(1, (len(blob) + int(block_bytes) - 1) // int(block_bytes))
        if int(b0) + n <= gid:
            drop.append(int(b0))
    for b0 in drop:
        blob = state["blobs"].pop(b0, None)
        if blob is not None:
            state["queued"] = max(0, int(state["queued"]) - len(blob))


def disk_direct_from_env() -> bool:
    raw = os.environ.get("TETRYS_DISK_DIRECT", "1")
    return raw.lower() not in ("0", "false", "no")


def open_disk_queue_fd(path: str) -> tuple[int, bool]:
    """Prefer O_DIRECT so sequential reads skip the ballooned page cache."""
    flags = os.O_RDONLY
    o_direct = getattr(os, "O_DIRECT", 0)
    if disk_direct_from_env() and o_direct:
        try:
            return os.open(path, flags | o_direct), True
        except OSError:
            pass
    return os.open(path, flags), False


def disk_queue_pread(
    fd: int,
    n: int,
    off: int,
    *,
    scratch: mmap.mmap | None = None,
    direct: bool = False,
) -> bytes:
    """Read ``n`` bytes at ``off``. Direct I/O uses an aligned scratch mmap."""
    n = max(0, int(n))
    off = max(0, int(off))
    if n <= 0:
        return b""
    if not direct or scratch is None or not hasattr(os, "preadv"):
        return os.pread(fd, n, off)
    align = _DISK_DIRECT_ALIGN
    off_a = off - (off % align)
    skip = off - off_a
    want_a = (skip + n + align - 1) // align * align
    if want_a > len(scratch):
        raise OSError(22, "direct pread scratch too small")
    got = os.preadv(fd, [memoryview(scratch)[:want_a]], off_a)
    if got <= skip:
        return b""
    return bytes(scratch[skip : min(got, skip + n)])


def run_file_disk_queue(
    path: str,
    *,
    file_size: int,
    block_bytes: int,
    batch_n: int,
    cursor: dict,
    stop: threading.Event,
    cond: threading.Condition,
    state: dict,
    max_bytes: int,
    chunk_bytes: int = _DISK_READ_CHUNK,
) -> None:
    """Single sequential pread into a bounded RAM queue of encode batches."""
    if max_bytes <= 0 or file_size <= 0 or block_bytes <= 0:
        return
    with cond:
        state.setdefault("max_bytes", int(max_bytes))
    batch_n = max(1, int(batch_n))
    batch_bytes = batch_n * int(block_bytes)
    read_n = max(int(chunk_bytes), batch_bytes)
    read_n = max(batch_bytes, (read_n // batch_bytes) * batch_bytes)
    try:
        fd, direct = open_disk_queue_fd(path)
    except OSError as exc:
        with cond:
            state["err"] = str(exc)
            cond.notify_all()
        return
    scratch: mmap.mmap | None = None
    if direct:
        try:
            scratch = mmap.mmap(-1, read_n + 2 * _DISK_DIRECT_ALIGN)
        except OSError:
            try:
                os.close(fd)
            except OSError:
                pass
            fd, direct = os.open(path, os.O_RDONLY), False
    try:
        fadv = getattr(os, "posix_fadvise", None)
        sequential = getattr(os, "POSIX_FADV_SEQUENTIAL", None)
        if fadv is not None and sequential is not None and not direct:
            try:
                fadv(fd, 0, 0, sequential)
            except OSError:
                pass
        while not stop.is_set():
            with cond:
                disk_queue_drop_consumed(
                    state,
                    gen_id=int(cursor.get("gen_id", 0)),
                    batch_n=batch_n,
                    block_bytes=block_bytes,
                )
                off = int(state["off"])
                if off >= file_size:
                    cond.wait(0.05)
                    continue
                cap = int(state.get("max_bytes") or max_bytes)
                if cap <= 0:
                    cond.wait(0.05)
                    continue
                if int(state["queued"]) >= cap:
                    cond.wait(0.05)
                    continue
            need = min(read_n, file_size - off)
            t_read0 = time.monotonic()
            try:
                chunk = disk_queue_pread(
                    fd, need, off, scratch=scratch, direct=direct
                )
            except OSError:
                if direct:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                    try:
                        fd = os.open(path, os.O_RDONLY)
                    except OSError:
                        stop.wait(0.05)
                        continue
                    direct = False
                    if scratch is not None:
                        try:
                            scratch.close()
                        except OSError:
                            pass
                        scratch = None
                    continue
                stop.wait(0.05)
                continue
            t_read1 = time.monotonic()
            if not chunk:
                with cond:
                    state["off"] = max(off, file_size)
                    cond.notify_all()
                stop.wait(0.05)
                continue
            pos = 0
            with cond:
                while pos < len(chunk):
                    b0 = (off + pos) // block_bytes
                    take = min(batch_bytes, len(chunk) - pos)
                    if b0 not in state["blobs"]:
                        sl = bytes(chunk[pos : pos + take])
                        state["blobs"][b0] = sl
                        state["queued"] = int(state["queued"]) + len(sl)
                    pos += take
                state["off"] = off + len(chunk)
                disk_queue_note_read(
                    state,
                    len(chunk),
                    t_read1,
                    elapsed=t_read1 - t_read0,
                )
                cond.notify_all()
    finally:
        if scratch is not None:
            try:
                scratch.close()
            except OSError:
                pass
        try:
            os.close(fd)
        except OSError:
            pass


def readahead_bytes_from_env(default_mib: int = _READAHEAD_MIB) -> int:
    raw = os.environ.get("TETRYS_READAHEAD_MIB")
    if raw is None or raw == "":
        mib = default_mib
    else:
        try:
            mib = int(raw)
        except ValueError:
            mib = default_mib
    return max(0, min(512, mib)) * 1024 * 1024


def readahead_next_slice(
    *,
    gen_id: int,
    block_bytes: int,
    file_size: int,
    primed: int,
    ahead_bytes: int,
    chunk_bytes: int = _READAHEAD_CHUNK,
) -> tuple[int, int] | None:
    """Next (offset, length) to pull into page cache, or None if far enough ahead."""
    if ahead_bytes <= 0 or file_size <= 0 or block_bytes <= 0 or chunk_bytes <= 0:
        return None
    blast_off = max(0, int(gen_id)) * int(block_bytes)
    if blast_off > file_size:
        blast_off = file_size
    target = min(file_size, blast_off + int(ahead_bytes))
    start = max(int(primed), blast_off)
    if start >= target:
        return None
    n = min(int(chunk_bytes), target - start)
    if n <= 0:
        return None
    return start, n


def run_file_readahead(
    path: str,
    *,
    file_size: int,
    block_bytes: int,
    cursor: dict,
    stop: threading.Event,
    ahead_bytes: int,
) -> None:
    """Sequential pread so blast encode faults hit warm cache, not the disk."""
    if ahead_bytes <= 0 or file_size <= 0:
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        fadv = getattr(os, "posix_fadvise", None)
        willneed = getattr(os, "POSIX_FADV_WILLNEED", None)
        sequential = getattr(os, "POSIX_FADV_SEQUENTIAL", None)
        if fadv is not None and sequential is not None:
            try:
                fadv(fd, 0, 0, sequential)
            except OSError:
                pass
        primed = 0
        while not stop.is_set():
            sl = readahead_next_slice(
                gen_id=int(cursor.get("gen_id", 0)),
                block_bytes=block_bytes,
                file_size=file_size,
                primed=primed,
                ahead_bytes=ahead_bytes,
            )
            if sl is None:
                if primed >= file_size:
                    break
                stop.wait(0.02)
                continue
            off, n = sl
            if fadv is not None and willneed is not None:
                try:
                    fadv(fd, off, n, willneed)
                except OSError:
                    pass
            try:
                got = os.pread(fd, n, off)
            except OSError:
                stop.wait(0.05)
                continue
            if not got:
                primed = max(primed, off + n)
                continue
            primed = off + len(got)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


# Per-worker-process state for blast encoding (initialized lazily).
_worker_mm: mmap.mmap | None = None
_worker_path: str | None = None
_worker_repair_enc: dict[int, GenEncoder] = {}
_worker_repair_order: list[int] = []
# Set by ProcessPoolExecutor initializer so workers can stream gens.
_encode_out_q: queue.Queue | None = None


def _encode_pool_init(out_q: queue.Queue) -> None:
    global _encode_out_q
    _encode_out_q = out_q


def drain_encode_out_queue(
    src: queue.Queue,
    ready: dict[int, tuple[float, int, list[bytes], int]],
    inflight: set[int],
    max_n: int = 64,
) -> int:
    """Move streamed encode results from the worker queue into `ready`."""
    n = 0
    while n < max_n:
        try:
            gid, item = src.get_nowait()
        except (queue.Empty, EOFError, OSError):
            break
        gid_i = int(gid)
        ready[gid_i] = item
        inflight.discard(gid_i)
        n += 1
    return n


def _worker_repair_encoder(
    gid: int,
    raw: bytes,
    symbol_size: int,
    overhead_pct: int,
) -> GenEncoder:
    """LRU GenEncoder per gid so ensure_repair() is incremental across rounds."""
    enc = _worker_repair_enc.get(gid)
    if enc is not None:
        if gid in _worker_repair_order:
            _worker_repair_order.remove(gid)
        _worker_repair_order.append(gid)
        return enc
    enc = GenEncoder(raw, symbol_size, overhead_pct, systematic_only=True)
    _worker_repair_enc[gid] = enc
    _worker_repair_order.append(gid)
    while len(_worker_repair_order) > _REPAIR_ENC_CACHE_MAX:
        drop = _worker_repair_order.pop(0)
        _worker_repair_enc.pop(drop, None)
    return enc


def _open_worker_mmap(path: str) -> mmap.mmap:
    global _worker_mm, _worker_path
    if _worker_mm is None or _worker_path != path:
        f = open(path, "rb")
        _worker_mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        _worker_path = path
    assert _worker_mm is not None
    return _worker_mm


def _read_gen_slice(
    path: str,
    start_gid: int,
    n_gens: int,
    block_bytes: int,
    file_size: int,
) -> tuple[bytes, int, float]:
    t0 = time.monotonic()
    mm = _open_worker_mmap(path)
    off = start_gid * block_bytes
    end = min(off + n_gens * block_bytes, file_size)
    try:
        mm.madvise(mmap.MADV_WILLNEED, off, end - off)
    except (AttributeError, OSError, ValueError):
        pass
    blob = bytes(mm[off:end])
    return blob, off, time.monotonic() - t0


def _encode_gen_from_slice(
    blob: bytes,
    off: int,
    i: int,
    start_gid: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    fountain_bootstrap: bool,
    read_s: float,
) -> tuple[float, int, list[bytes], int]:
    t_enc = time.monotonic()
    gid = start_gid + i
    sl = i * block_bytes
    src_end = min(off + sl + block_bytes, file_size)
    src_bytes = max(0, src_end - (off + sl))
    raw = blob[sl : sl + block_bytes]
    if len(raw) < block_bytes:
        raw = raw + b"\x00" * (block_bytes - len(raw))
    k_est = max(1, (len(raw) + symbol_size - 1) // symbol_size)
    enc = GenEncoder(raw, symbol_size, overhead_pct, systematic_only=True)
    bootstrap = blast_repair_budget(k_est, overhead_pct)
    if bootstrap <= 0 and fountain_bootstrap:
        bootstrap = fountain_blast_budget(k_est, _FOUNTAIN_TARGET_OVERHEAD_PCT)
    if bootstrap:
        enc.ensure_repair(bootstrap)
    ts = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
    wires = [
        GenPacket(gid, j, pkt, ts).pack()
        for j, pkt in enumerate(enc.packets())
    ]
    cpu_s = time.monotonic() - t_enc
    if i == 0:
        cpu_s += read_s
    return cpu_s, src_bytes, wires, bootstrap


def _encode_gens_worker(
    path: str,
    start_gid: int,
    n_gens: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    fountain_bootstrap: bool = False,
) -> list[tuple[float, int, list[bytes], int]]:
    """Read `n_gens` consecutive blocks in one slice, then encode each."""
    n_gens = max(1, int(n_gens))
    start_gid = max(0, int(start_gid))
    blob, off, read_s = _read_gen_slice(
        path, start_gid, n_gens, block_bytes, file_size
    )
    return [
        _encode_gen_from_slice(
            blob,
            off,
            i,
            start_gid,
            block_bytes,
            file_size,
            symbol_size,
            overhead_pct,
            fountain_bootstrap,
            read_s,
        )
        for i in range(n_gens)
    ]


def _encode_gens_worker_stream(
    path: str,
    start_gid: int,
    n_gens: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    fountain_bootstrap: bool = False,
) -> int:
    """Read a batch, then put each encoded gen on `_encode_out_q` immediately."""
    n_gens = max(1, int(n_gens))
    start_gid = max(0, int(start_gid))
    blob, off, read_s = _read_gen_slice(
        path, start_gid, n_gens, block_bytes, file_size
    )
    q = _encode_out_q
    for i in range(n_gens):
        item = _encode_gen_from_slice(
            blob,
            off,
            i,
            start_gid,
            block_bytes,
            file_size,
            symbol_size,
            overhead_pct,
            fountain_bootstrap,
            read_s,
        )
        if q is not None:
            q.put((start_gid + i, item))
    return n_gens


def _encode_blob_worker_stream(
    blob: bytes,
    start_gid: int,
    n_gens: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    fountain_bootstrap: bool = False,
) -> int:
    """Encode an already-read sequential slice; workers do not touch the disk."""
    n_gens = max(1, int(n_gens))
    start_gid = max(0, int(start_gid))
    off = start_gid * block_bytes
    q = _encode_out_q
    for i in range(n_gens):
        item = _encode_gen_from_slice(
            blob,
            off,
            i,
            start_gid,
            block_bytes,
            file_size,
            symbol_size,
            overhead_pct,
            fountain_bootstrap,
            0.0,
        )
        if q is not None:
            q.put((start_gid + i, item))
    return n_gens


def _encode_gen_worker(
    path: str,
    gid: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    fountain_bootstrap: bool = False,
) -> tuple[float, int, list[bytes], int]:
    """Encode one generation; returns (cpu_s, src_bytes, wires, bootstrap_repair_pkts)."""
    return _encode_gens_worker(
        path,
        gid,
        1,
        block_bytes,
        file_size,
        symbol_size,
        overhead_pct,
        fountain_bootstrap,
    )[0]


def _repair_gen_worker(
    path: str,
    gid: int,
    block_bytes: int,
    file_size: int,
    symbol_size: int,
    overhead_pct: int,
    gen_k: int,
    prior_extra: int,
    send_n: int,
    ts_us: int,
    round_max: int | None = None,
) -> tuple[float, int, list[bytes]]:
    """Encode a fountain tail in a worker process (same GIL isolation as blast)."""
    global _worker_mm, _worker_path
    t_enc = time.monotonic()
    if _worker_mm is None or _worker_path != path:
        f = open(path, "rb")
        _worker_mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        _worker_path = path
    off = gid * block_bytes
    end = min(off + block_bytes, file_size)
    raw = bytes(_worker_mm[off:end])
    if len(raw) < block_bytes:
        raw = raw + b"\x00" * (block_bytes - len(raw))
    enc = _worker_repair_encoder(gid, raw, symbol_size, overhead_pct)
    cap = _REPAIR_ROUND_MAX if round_max is None else max(1, int(round_max))
    want = max(1, min(int(send_n), cap))
    # prior_extra can lag the cached encoder (prune forgot HOL, or a
    # fresh parent dict). Always grow past the live budget so drain
    # cannot get ensure_repair() → [].
    target_budget = max(int(prior_extra), int(enc.repair_budget)) + want
    new_pkts = enc.ensure_repair(target_budget)
    if not new_pkts:
        have = enc.packets()
        if not have:
            return time.monotonic() - t_enc, 0, []
        start = int(prior_extra) % len(have)
        wrapped = [have[(start + i) % len(have)] for i in range(want)]
        wires = [
            GenPacket(gid, (start + i) % len(have), blob, ts_us).pack()
            for i, blob in enumerate(wrapped)
        ]
        return time.monotonic() - t_enc, 0, wires
    new_pkts = new_pkts[-want:]
    first_esi = enc.packet_count - len(new_pkts)
    wires = [
        GenPacket(gid, first_esi + i, blob, ts_us).pack()
        for i, blob in enumerate(new_pkts)
    ]
    return time.monotonic() - t_enc, len(new_pkts), wires


def _file_sha256(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def run_gen_server(
    host: str,
    port: int,
    file_path: Path,
    *,
    symbol_size: int = 1350,
    gen_k: int = 192,
    overhead_pct: int = 0,
    rate_mbit: float = 1500.0,
    ramp_s: float = 4.0,
    skip_hash: bool = True,
) -> int:
    require_raptorq()
    if not file_path.is_file():
        print(f"file not found: {file_path}", file=sys.stderr)
        return 1

    file_size = file_path.stat().st_size
    block_bytes = max(1, gen_k) * max(64, symbol_size)
    total_gens = (file_size + block_bytes - 1) // block_bytes if file_size else 0
    inflight_gen_limit = compute_inflight_gen_limit(gen_k, symbol_size)
    window_mib = max_inflight_bytes() / (1024 * 1024)
    inflight_cap_mib = window_mib
    adapt_inflight = os.environ.get("TETRYS_ADAPT_INFLIGHT", "1").lower() not in (
        "0",
        "false",
        "no",
    )
    adapt_pace = os.environ.get("TETRYS_ADAPT_PACE", "").lower() in ("1", "true", "yes")
    delay_cc = os.environ.get("TETRYS_DELAY_CC", "1").lower() not in (
        "0",
        "false",
        "no",
    )
    adapt_overhead = os.environ.get("TETRYS_ADAPT_OVERHEAD", "1").lower() not in (
        "0",
        "false",
        "no",
    ) and overhead_pct > 0
    packet_loss_fec = packet_loss_fec_from_env() and adapt_overhead
    server_synth_nack = os.environ.get("TETRYS_SERVER_SYNTH_NACK", "1").lower() not in (
        "0",
        "false",
        "no",
    )
    try:
        delay_start_mbit = float(
            os.environ.get("TETRYS_DELAY_START_MBIT", str(_DELAY_CC_START_MBIT))
            or str(_DELAY_CC_START_MBIT)
        )
    except ValueError:
        delay_start_mbit = _DELAY_CC_START_MBIT
    delay_start_mbit = min(rate_mbit, max(1.0, delay_start_mbit))
    try:
        delay_min_mbit = float(
            os.environ.get("TETRYS_DELAY_MIN_MBIT", str(_DELAY_CC_MIN_MBIT))
            or str(_DELAY_CC_MIN_MBIT)
        )
    except ValueError:
        delay_min_mbit = _DELAY_CC_MIN_MBIT
    delay_min_mbit = min(rate_mbit, max(1.0, delay_min_mbit))
    if delay_start_mbit < delay_min_mbit:
        delay_start_mbit = delay_min_mbit
    try:
        startup_max_mbit = float(
            os.environ.get(
                "TETRYS_BBR_STARTUP_MAX_MBIT", str(_BBR_STARTUP_MAX_MBIT)
            )
            or str(_BBR_STARTUP_MAX_MBIT)
        )
    except ValueError:
        startup_max_mbit = _BBR_STARTUP_MAX_MBIT
    startup_max_mbit = min(rate_mbit, max(delay_start_mbit, startup_max_mbit))
    startup_max_bps = startup_max_mbit * 1_000_000 / 8
    try:
        delay_up_probe = float(
            os.environ.get("TETRYS_DELAY_UP", str(_DELAY_CC_UP_PROBE))
            or str(_DELAY_CC_UP_PROBE)
        )
    except ValueError:
        delay_up_probe = _DELAY_CC_UP_PROBE
    delay_up_probe = min(2.0, max(1.01, delay_up_probe))
    try:
        delay_clean_n = int(
            os.environ.get("TETRYS_DELAY_CLEAN", str(_DELAY_CC_CLEAN_SAMPLES))
            or str(_DELAY_CC_CLEAN_SAMPLES)
        )
    except ValueError:
        delay_clean_n = _DELAY_CC_CLEAN_SAMPLES
    delay_clean_n = min(8, max(1, delay_clean_n))
    try:
        encode_workers = int(
            os.environ.get("TETRYS_ENCODE_WORKERS", str(_ENCODE_WORKERS))
            or str(_ENCODE_WORKERS)
        )
    except ValueError:
        encode_workers = _ENCODE_WORKERS
    encode_workers = min(16, max(1, encode_workers))
    encode_read_gens = encode_read_gens_from_env()
    encode_prefetch = max(
        _ENCODE_PREFETCH,
        encode_workers * encode_read_gens,
    )
    readahead_bytes = readahead_bytes_from_env()
    disk_queue_bytes = disk_queue_mib_from_env()
    disk_queue_max_bytes = disk_queue_max_mib_from_env(disk_queue_bytes)
    disk_queue_adapt = (
        disk_queue_bytes > 0
        and disk_queue_max_bytes > disk_queue_bytes
        and disk_queue_adapt_from_env()
    )
    disk_pace_on = disk_queue_bytes > 0 and os.environ.get(
        "TETRYS_DISK_PACE", "1"
    ).lower() not in ("0", "false", "no")
    disk_direct_on = disk_queue_bytes > 0 and disk_direct_from_env()
    try:
        repair_workers = int(
            os.environ.get("TETRYS_REPAIR_WORKERS", str(_REPAIR_WORKERS))
            or str(_REPAIR_WORKERS)
        )
    except ValueError:
        repair_workers = _REPAIR_WORKERS
    repair_workers = min(4, max(1, repair_workers))
    digest = "" if skip_hash else _file_sha256(file_path)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_SNDBUF, 32 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_RCVBUF, 8 * 1024 * 1024)
    sock.bind((host, port))
    sock.setblocking(False)

    print(
        f"file={file_path.name} size={file_size}"
        f"{' (hash skipped)' if skip_hash else ''}"
    )
    fountain_mode = fountain_redundancy(overhead_pct)
    print(
        f"xfer=gen gens={total_gens} T={symbol_size} K~={gen_k} "
        f"block={block_bytes} overhead={overhead_pct}% "
        f"{'(fountain redundancy) ' if fountain_mode else ''}"
        f"cc=off "
        f"rate_mbit={rate_mbit} inflight_gens={inflight_gen_limit} "
        f"inflight_mib={window_mib:.0f} "
        f"adapt_win={'on' if adapt_inflight else 'off'} "
        f"adapt_pace={'on' if adapt_pace else 'off'} "
        f"delay_cc={'on' if delay_cc else 'off'} "
        f"adapt_oh={'on' if adapt_overhead else 'off'} "
        f"loss_fec={'on' if packet_loss_fec else 'off'} "
        f"synth_nack={'on' if server_synth_nack else 'off'} "
        f"enc_workers={encode_workers} repair_workers={repair_workers} "
        f"enc_read={encode_read_gens}gens stream=1 "
        f"disk_q={disk_queue_bytes // (1024 * 1024)}"
        f"{('–' + str(disk_queue_max_bytes // (1024 * 1024))) if disk_queue_adapt else ''}"
        f"MiB "
        f"disk_pace={'on' if disk_pace_on else 'off'} "
        f"disk_direct={'on' if disk_direct_on else 'off'} "
        f"readahead={readahead_bytes // (1024 * 1024)}MiB "
        f"mode={'fountain' if fountain_mode else 'hybrid+async_fountain'} "
    )
    print(f"Gen RaptorQ server listening on udp://{host}:{port}")
    print(
        "cpu: steal=hypervisor  psi=in-VM stall  thr=cgroup quota  "
        "mhz=freq | slow encbench+steal→host; +psi/thr→VM; "
        "stable encbench+high wenc→our pool"
    )
    host_cpu = HostCpuSampler()
    host_cpu.prime()
    print("waiting for client READY...")

    client_addr: tuple[str, int] | None = None
    deadline = time.monotonic() + 600
    while client_addr is None:
        if time.monotonic() > deadline:
            print("timeout waiting for client", file=sys.stderr)
            return 1
        r, _, _ = select.select([sock], [], [], 1.0)
        if not r:
            continue
        data, addr = sock.recvfrom(65535)
        try:
            pkt = parse_packet(data)
        except ValueError:
            continue
        if isinstance(pkt, ReadyPacket):
            client_addr = addr
            print(f"client ready from {addr}")
            try:
                print(bench_encode(n=8, warmup=1).format_line("before"), flush=True)
            except Exception as exc:  # pragma: no cover
                print(f"encbench before failed: {exc}", file=sys.stderr)
            snap = host_cpu.sample()
            if snap is not None and snap.available:
                print(snap.format_line(), flush=True)
            host_cpu.prime()

    assert client_addr is not None

    meta = MetaPacket(
        file_size,
        file_path.name,
        symbol_size,
        digest,
        xfer=XFER_GEN,
        gen_symbol_size=symbol_size,
        gen_k=gen_k,
        gen_overhead_pct=overhead_pct,
    ).pack()
    for _ in range(16):
        sock.sendto(meta, client_addr)

    max_bps = max(rate_mbit, 1.0) * 1_000_000 / 8
    delay_min_bps = delay_min_mbit * 1_000_000 / 8
    if delay_cc:
        start_bps = min(max_bps, delay_start_mbit * 1_000_000 / 8)
        # Floor is an absolute min (200 Mbit), not 20% of the safety ceiling.
        # Otherwise --rate 2500 would pin the floor at 500 and block BBR.
        pace_min_frac = max(delay_min_bps, 1_000_000.0) / max_bps
        # Probe owns the climb — skip the time-based ease-in to --rate.
        effective_ramp_s = 0.0
    elif adapt_pace:
        start_bps = max_bps
        pace_min_frac = _ADAPT_PACE_MIN_FRAC
        effective_ramp_s = ramp_s
    else:
        start_bps = 1.0 if ramp_s > 0 else max_bps
        pace_min_frac = 0.90
        effective_ramp_s = ramp_s
    limiter = RateLimiter(
        max_bps,
        start_bps=start_bps,
        min_frac=pace_min_frac,
    )
    disk_pace = {
        "on": False,
        "max_bytes": 0,
        "min_bytes": 0,
        "hi_bytes": 0,
        "adapt": False,
        "adapt_ts": 0.0,
        "capping": False,
        "cap_bps": 0.0,
        "state": None,
        "cond": None,
        "orig_min": limiter.min_rate,
    }
    t_ramp0 = time.monotonic()
    last_meta_ts = t_ramp0
    send_totals = {"wire": 0, "blast": 0, "repair": 0}
    pace_state = {
        "bps": start_bps if delay_cc or adapt_pace else max_bps,
        "last_completed": -1,
        "last_ts": t_ramp0,
        "last_blast": 0,
        "last_wire": 0,
        "good_streak": 0,
        "bad_streak": 0,
        "min_rtt_s": 0.0,
        "rtt_s": 0.0,
        "rtt_ewma_s": 0.0,
        "spike_streak": 0,
        "queue_s": 0.0,
        "last_cc_ts": 0.0,
        "clean_streak": 0,
        "congestion_streak": 0,
        "hold_until": 0.0,
        "deliv_ewma": 0.0,
        "btlbw": 0.0,
        "btlbw_samples": [],
        "btlbw_round": 0.0,
        "bbr_startup": True,
        "startup_check_ts": t_ramp0,
        "startup_flat": 0,
        "startup_bad": 0,
        "startup_hol": 0,
        "startup_phase": "warmup",
        "startup_reason": "init",
        "startup_offer": start_bps,
        "startup_confirmed": 0.0,
        "startup_delivery": 0.0,
        "startup_ratio": 0.0,
        "round_completed": 0,
        "round_wire": 0,
        "round_ts": t_ramp0,
        "sample_reason": "warmup",
        "confirmed_btlbw": 0.0,
        "confirmed_cruise": 0.0,
        "gain": _BBR_STARTUP_GAIN,
        "guard_completed": -1,
        "guard_ts": t_ramp0,
        "floor_since": 0.0,
        "bbr_t0": t_ramp0,
    }
    pace_lock = threading.Lock()
    send_frontier = {"gen_id": 0, "blast_seq": 0}

    def refresh_inflight(
        occupancy: int,
        *,
        btlbw: float | None = None,
        min_rtt: float | None = None,
    ) -> None:
        nonlocal inflight_gen_limit, window_mib
        if not adapt_inflight:
            return
        if btlbw is None or min_rtt is None:
            with pace_lock:
                btlbw = float(pace_state["btlbw"])
                min_rtt = float(pace_state["min_rtt_s"])
        window_mib = adaptive_inflight_mib(
            btlbw_bps=float(btlbw),
            min_rtt_s=float(min_rtt),
            current_mib=window_mib,
            occupancy_bytes=max(0, int(occupancy)) * block_bytes,
            cap_mib=inflight_cap_mib,
        )
        inflight_gen_limit = compute_inflight_gen_limit(
            gen_k, symbol_size, inflight_mib=window_mib
        )

    if delay_cc:
        print(
            f"delay probe "
            f"{start_bps * 8 / 1_000_000:.0f}→{startup_max_mbit:.0f} Mbit "
            f"min={delay_min_mbit:.0f} "
            f"(fec start {_OH_CEIL_PCT}%→{_OH_FLOOR_PCT}%, ceil {_OH_CEIL_PCT}%, "
            f"up={delay_up_probe:.2f} clean={delay_clean_n} bbr=on)"
        )
    elif effective_ramp_s > 0:
        print(
            f"pace ramp {effective_ramp_s:.1f}s "
            f"0→{max_bps / (1024 * 1024):.1f} MiB/s"
        )

    enc_lock = threading.Lock()
    encoders: dict[int, GenEncoder] = {}
    repair_extra: dict[int, int] = {}
    last_repair_ts: dict[int, float] = {}
    last_full_ts: dict[int, float] = {}
    repair_rounds: dict[int, int] = {}
    repair_close_hist = [0, 0, 0, 0, 0]
    oh_start = (
        max(overhead_pct, _OH_CEIL_PCT)
        if adapt_overhead and overhead_pct > 0
        else overhead_pct
    )
    overhead_state = {
        "pct": oh_start,
        "miss": 0.0,
        "p_fast": 0.0,
        "p_slow": 0.0,
        "p": 0.0,
        "fec_base": oh_start,
        "fec_boost": 0,
        "clean_windows": 0,
        "loss_rx": 0,
        "loss_lost": 0,
        "loss_late": 0,
        "loss_pending": 0,
        "loss_ingest": {},
        "loss_accum": {},
        "loss_window_n": 0,
    }
    stop_fb = threading.Event()
    fb_lock = threading.Lock()
    fb_state = {
        "next_needed": 0,
        "completed": 0,
        "nacks": [],
        "nack_rx": {},
        "hol_miss_esi": [],
        "miss_bitmap": b"",
        "never_seen": None,
        "epoch": 0,
        "echo": 0,
        "done": False,
        "loss": {},
    }

    def feedback_loop() -> None:
        while not stop_fb.is_set():
            r, _, _ = select.select([sock], [], [], 0.05)
            if not r:
                continue
            while True:
                try:
                    data, addr = sock.recvfrom(65535)
                except BlockingIOError:
                    break
                if addr != client_addr:
                    continue
                try:
                    pkt = parse_packet(data)
                except ValueError:
                    continue
                if isinstance(pkt, GenFeedbackPacket):
                    now = time.monotonic()
                    completed = pkt.completed_gens
                    nacks, nack_rx = merge_feedback_nacks(pkt)
                    epoch = int(pkt.drain_epoch or 0)
                    with fb_lock:
                        last_ep = int(fb_state.get("epoch") or 0)
                        stale = drain_epoch_is_stale(epoch, last_ep)
                        fb_state["next_needed"] = max(
                            int(fb_state["next_needed"]), pkt.next_needed_gen
                        )
                        fb_state["completed"] = max(
                            int(fb_state["completed"]), completed
                        )
                        fb_state["echo"] = pkt.echo_ts_us
                        if completed >= total_gens and total_gens > 0:
                            fb_state["done"] = True
                        if not stale:
                            if epoch > 0:
                                fb_state["epoch"] = epoch
                            fb_state["nacks"] = nacks
                            fb_state["nack_rx"] = nack_rx
                            fb_state["hol_miss_esi"] = list(pkt.hol_miss_esi or [])
                            fb_state["miss_bitmap"] = pkt.miss_bitmap or b""
                            fb_state["never_seen"] = pkt.never_seen
                        fb_state["loss"] = {
                            "epoch": int(pkt.loss_epoch or 0),
                            "seq_begin": int(pkt.loss_seq_begin or 0),
                            "seq_end": int(pkt.loss_seq_end or 0),
                            "rx": int(pkt.loss_rx_unique or 0),
                            "lost": int(pkt.loss_lost or 0),
                            "late": int(pkt.loss_late or 0),
                            "pending": int(pkt.loss_pending or 0),
                        }
                    if delay_cc and (now - t_ramp0) >= _DELAY_CC_WARMUP_S:
                        rtt = echo_rtt_s(pkt.echo_ts_us, now)
                        if rtt is not None:
                            # Window-full HOL/protocol stall ≠ path congestion:
                            # keep RTT stats but do not slam the probe rate.
                            frontier_lag = max(
                                0, send_frontier["gen_id"] - pkt.next_needed_gen
                            )
                            occupancy = max(
                                0, send_frontier["gen_id"] - completed
                            )
                            stalled = pipeline_is_stalled(
                                occupancy=occupancy,
                                inflight_gen_limit=inflight_gen_limit,
                            )
                            with pace_lock:
                                ewma, is_spike, spike_n = smooth_delay_rtt_s(
                                    rtt,
                                    pace_state["rtt_ewma_s"],
                                    int(pace_state["spike_streak"]),
                                )
                                pace_state["rtt_ewma_s"] = ewma
                                pace_state["spike_streak"] = spike_n
                                pace_state["rtt_s"] = ewma
                                if pace_state["min_rtt_s"] <= 0 or (
                                    (not is_spike) and rtt < pace_state["min_rtt_s"]
                                ):
                                    pace_state["min_rtt_s"] = rtt
                                pace_state["queue_s"] = max(
                                    0.0, ewma - pace_state["min_rtt_s"]
                                )
                                # At a low pace with a healthy frontier, persistent
                                # delay is baseline drift/jitter, not self-queueing.
                                # Rebase gradually so the controller can recover
                                # without teaching itself through a real backlog.
                                pipeline_clean = occupancy < (
                                    inflight_gen_limit * 3
                                ) // 20
                                if delay_cc_may_rebase_min_rtt(
                                    is_spike=bool(is_spike),
                                    pipeline_clean=pipeline_clean,
                                    queue_s=float(pace_state["queue_s"]),
                                    ewma_s=ewma,
                                    min_rtt_s=float(pace_state["min_rtt_s"]),
                                ):
                                    pace_state["min_rtt_s"] = min(
                                        ewma,
                                        pace_state["min_rtt_s"] * 0.85 + ewma * 0.15,
                                    )
                                    # Small jitter, not a stall: keep probing.
                                    pace_state["queue_s"] = 0.0
                                target_q = max(
                                    _DELAY_CC_TARGET_QUEUE_MIN_S,
                                    pace_state["min_rtt_s"]
                                    * _DELAY_CC_TARGET_QUEUE_RTT_FRAC,
                                )
                                queue_s = pace_state["queue_s"]
                                if is_spike:
                                    pass  # keep streaks; one echo must not look like a queue
                                elif queue_s <= target_q * 0.70:
                                    pace_state["clean_streak"] += 1
                                    pace_state["congestion_streak"] = 0
                                elif queue_s >= target_q:
                                    pace_state["clean_streak"] = 0
                                    pace_state["congestion_streak"] += 1
                                else:
                                    pace_state["clean_streak"] = 0
                                    pace_state["congestion_streak"] = 0
                                congestion_pressure = delay_cc_congestion_pressure(
                                    occupancy, inflight_gen_limit
                                )
                                congested = (
                                    congestion_pressure
                                    and pace_state["congestion_streak"]
                                    >= _DELAY_CC_CONGESTION_SAMPLES
                                )
                                last_c = int(pace_state["last_completed"])
                                last_ts = float(pace_state["last_ts"])
                                floor = max_bps * pace_min_frac
                                cur_bps = float(pace_state["bps"])
                                if last_c < 0:
                                    pace_state["last_completed"] = completed
                                    pace_state["last_ts"] = now
                                    pace_state["last_blast"] = int(
                                        send_totals["blast"]
                                    )
                                    pace_state["last_wire"] = int(
                                        send_totals["wire"]
                                    )
                                elif (
                                    completed > last_c
                                    and (now - last_ts)
                                    >= max(
                                        _BBR_STARTUP_ROUND_MIN_S,
                                        float(pace_state["min_rtt_s"]),
                                    )
                                ):
                                    round_dt = max(now - last_ts, 1e-6)
                                    delivered_bytes = (
                                        completed - last_c
                                    ) * block_bytes
                                    blast_wire_bytes = max(
                                        0,
                                        int(send_totals["wire"])
                                        - int(pace_state["last_wire"]),
                                    )
                                    sample = bbr_delivery_round(
                                        delivered_bytes=delivered_bytes,
                                        blast_wire_bytes=blast_wire_bytes,
                                        dt_s=round_dt,
                                        offer_bps=cur_bps,
                                        overhead_pct=int(overhead_state["pct"]),
                                    )
                                    delivery_bps = float(sample["delivery_bps"])
                                    rtprop = float(pace_state["min_rtt_s"])
                                    elapsed_s = now - t_ramp0
                                    path_sample = (
                                        not stalled
                                        and bool(sample["valid"])
                                        and bbr_delivery_is_path_sample(
                                            delivery_bps=delivery_bps,
                                            cur_bps=cur_bps,
                                            min_bps=floor,
                                            elapsed_s=elapsed_s,
                                            occupancy=occupancy,
                                            inflight_gen_limit=inflight_gen_limit,
                                        )
                                    )
                                    if stalled:
                                        # HOL/decode lag is not a bandwidth
                                        # sample. Freeze BtlBw so a repair hole
                                        # cannot expire the path estimate.
                                        btlbw = float(pace_state["btlbw"])
                                        pace_state["deliv_ewma"] = delivery_bps
                                    else:
                                        # Warmup / send-limited delivery is
                                        # not BtlBw. Age the window only.
                                        btlbw, samples = update_btlbw_bps(
                                            delivery_bps=(
                                                delivery_bps if path_sample else 0.0
                                            ),
                                            samples=list(pace_state["btlbw_samples"]),
                                            now_s=now,
                                            rtprop_s=rtprop,
                                        )
                                        pace_state["btlbw_samples"] = samples
                                        pace_state["deliv_ewma"] = delivery_bps
                                        if path_sample:
                                            confirmed_bw = float(
                                                pace_state["confirmed_btlbw"]
                                            )
                                            if (
                                                confirmed_bw <= 1.0
                                                or delivery_bps
                                                >= confirmed_bw * 1.05
                                            ):
                                                confirmed_bw = max(
                                                    confirmed_bw, delivery_bps
                                                )
                                                pace_state[
                                                    "confirmed_btlbw"
                                                ] = confirmed_bw
                                            btlbw = max(btlbw, confirmed_bw)
                                        pace_state["btlbw"] = btlbw
                                    startup = bool(pace_state["bbr_startup"])
                                    if startup:
                                        step = bbr_startup_step(
                                            offer_bps=float(
                                                pace_state["startup_offer"]
                                            ),
                                            confirmed_bps=float(
                                                pace_state["startup_confirmed"]
                                            ),
                                            prior_delivery_bps=float(
                                                pace_state["startup_delivery"]
                                            ),
                                            sample=sample,
                                            max_bps=max_bps,
                                            startup_max_bps=startup_max_bps,
                                            stalled=stalled,
                                            congested=congested,
                                            bad_rounds=int(
                                                pace_state["startup_bad"]
                                            ),
                                            flat_rounds=int(
                                                pace_state["startup_flat"]
                                            ),
                                            hol_rounds=int(
                                                pace_state["startup_hol"]
                                            ),
                                        )
                                        pace_state["bbr_startup"] = bool(
                                            step["startup"]
                                        )
                                        pace_state["startup_offer"] = float(
                                            step["offer_bps"]
                                        )
                                        pace_state["startup_confirmed"] = float(
                                            step["confirmed_bps"]
                                        )
                                        pace_state["startup_delivery"] = float(
                                            step["delivery_bps"]
                                        )
                                        pace_state["startup_bad"] = int(
                                            step["bad_rounds"]
                                        )
                                        pace_state["startup_flat"] = int(
                                            step["flat_rounds"]
                                        )
                                        pace_state["startup_hol"] = int(
                                            step["hol_rounds"]
                                        )
                                        pace_state["startup_phase"] = str(
                                            step["phase"]
                                        )
                                        pace_state["startup_reason"] = str(
                                            step["reason"]
                                        )
                                        pace_state["startup_ratio"] = float(
                                            sample.get("ratio") or 0.0
                                        )
                                        pace_state["sample_reason"] = str(
                                            sample.get("reason") or "unknown"
                                        )
                                        cur = float(step["offer_bps"])
                                        if not bool(step["startup"]):
                                            pace_state["bbr_t0"] = now
                                            pace_state["confirmed_cruise"] = max(
                                                float(
                                                    pace_state[
                                                        "confirmed_cruise"
                                                    ]
                                                ),
                                                float(step["offer_bps"]),
                                            )
                                            pace_state[
                                                "confirmed_btlbw"
                                            ] = max(
                                                float(
                                                    pace_state[
                                                        "confirmed_btlbw"
                                                    ]
                                                ),
                                                delivery_bps,
                                            )
                                            pace_state["btlbw"] = max(
                                                float(pace_state["btlbw"]),
                                                float(
                                                    pace_state[
                                                        "confirmed_btlbw"
                                                    ]
                                                ),
                                            )
                                        startup = bool(step["startup"])
                                    # Never drain on RTT during STARTUP: the
                                    # first echo is warmup, not a standing queue.
                                    if startup:
                                        gain = _BBR_STARTUP_GAIN
                                    elif congested:
                                        gain = _BBR_DRAIN_GAIN
                                    elif stalled:
                                        # Don't 1.25×-probe into a HOL hole.
                                        gain = _BBR_CRUISE_GAIN
                                    else:
                                        gain = bbr_pacing_gain(
                                            now,
                                            rtprop,
                                            float(pace_state["bbr_t0"]),
                                        )
                                        if (
                                            not delay_cc_may_probe(
                                                occupancy, inflight_gen_limit
                                            )
                                            and gain > 1.0
                                        ):
                                            gain = _BBR_CRUISE_GAIN
                                    # BtlBw is app delivery; a 4% wire pad covers
                                    # floor FEC. Live 32% would 1.25× AND 1.32×.
                                    # FEC% is not a goodput signal.
                                    oh_pct = min(
                                        int(overhead_state["pct"]), _OH_FLOOR_PCT
                                    )
                                    if not startup:
                                        btlbw = max(
                                            float(pace_state["btlbw"]),
                                            float(
                                                pace_state[
                                                    "confirmed_btlbw"
                                                ]
                                            ),
                                        )
                                        cur = update_delivery_rate_pace_bps(
                                            btlbw_bps=btlbw,
                                            max_bps=max_bps,
                                            min_bps=floor,
                                            gain=gain,
                                            overhead_pct=oh_pct,
                                        )
                                        if not congested:
                                            cur = max(
                                                cur,
                                                float(
                                                    pace_state[
                                                        "confirmed_cruise"
                                                    ]
                                                ),
                                            )
                                    pace_state["gain"] = gain
                                    pace_state["bps"] = cur
                                    pace_state["last_completed"] = completed
                                    pace_state["last_ts"] = now
                                    pace_state["last_blast"] = int(
                                        send_totals["blast"]
                                    )
                                    pace_state["last_wire"] = int(
                                        send_totals["wire"]
                                    )
                                    pace_state["last_cc_ts"] = now
                                    if congested and not startup:
                                        pace_state["clean_streak"] = 0
                                        pace_state["hold_until"] = (
                                            now + _DELAY_CC_BACKOFF_HOLD_S
                                        )
                                    limiter.set_rate(cur)
                                    refresh_inflight(
                                        occupancy,
                                        btlbw=btlbw,
                                        min_rtt=rtprop,
                                    )
                                # BBR owns the setpoint. Extra feedback between
                                # delivery samples must not run delay-CC 0.82×
                                # down to the 200 Mbit floor (pace=200 with
                                # btlbw=368 / g=1.00). Congested RTT still
                                # selects drain gain on the next BBR tick.
                    elif adapt_pace and (now - t_ramp0) >= effective_ramp_s + _ADAPT_PACE_WARMUP_S:
                        with pace_lock:
                            last_c = pace_state["last_completed"]
                            last_ts = pace_state["last_ts"]
                            cur = pace_state["bps"]
                            if last_c >= 0 and completed > last_c:
                                dt = now - last_ts
                                if dt >= 0.12:
                                    delivery_bps = (completed - last_c) * block_bytes / dt
                                    cur, streak, bad = update_adaptive_pace_bps(
                                        delivery_bps=delivery_bps,
                                        cur_bps=cur,
                                        max_bps=max_bps,
                                        good_streak=pace_state["good_streak"],
                                        bad_streak=pace_state["bad_streak"],
                                    )
                                    pace_state["bps"] = cur
                                    pace_state["good_streak"] = streak
                                    pace_state["bad_streak"] = bad
                            pace_state["last_completed"] = completed
                            pace_state["last_ts"] = now
                            limiter.set_rate(pace_state["bps"])
                elif isinstance(pkt, FinPacket):
                    with fb_lock:
                        fb_state["done"] = True
                elif isinstance(pkt, ReadyPacket):
                    sock.sendto(meta, client_addr)

    fb_thread = threading.Thread(target=feedback_loop, name="gen-fb", daemon=True)
    fb_thread.start()

    def maybe_adapt_disk_queue() -> None:
        if not disk_pace["adapt"]:
            return
        st = disk_pace["state"]
        cond = disk_pace["cond"]
        if st is None:
            return
        now = time.monotonic()
        if now - float(disk_pace["adapt_ts"] or 0.0) < _DISK_QUEUE_ADAPT_EVERY_S:
            return
        disk_pace["adapt_ts"] = now
        lo = int(disk_pace["min_bytes"] or 0)
        hi = int(disk_pace["hi_bytes"] or lo)
        cur = int(st.get("max_bytes") or lo)
        queued = int(st.get("queued") or 0)
        new = disk_queue_adapt_bytes(
            current_bytes=cur,
            min_bytes=lo,
            max_bytes=hi,
            queued_frac=queued / max(cur, 1),
            disk_bps=float(st.get("rate_bps") or 0.0),
            send_bps=float(limiter.rate),
            available_bytes=mem_available_bytes(),
        )
        if new == cur:
            return
        disk_pace["max_bytes"] = new
        if cond is None:
            st["max_bytes"] = new
            return
        with cond:
            st["max_bytes"] = new
            cond.notify_all()

    def apply_disk_feed_cap() -> None:
        maybe_adapt_disk_queue()
        if not disk_pace["on"]:
            return
        st = disk_pace["state"]
        max_q = int((st or {}).get("max_bytes") or disk_pace["max_bytes"] or 0)
        if st is None or max_q <= 0:
            limiter.min_rate = float(disk_pace["orig_min"])
            disk_pace["capping"] = False
            disk_pace["cap_bps"] = 0.0
            return
        queued = int(st.get("queued") or 0)
        disk_bps = float(st.get("rate_bps") or 0.0)
        cap, capping = disk_feed_cap_bps(
            queued_frac=queued / max_q,
            disk_bps=disk_bps,
            max_bps=max_bps,
            min_bps=1_000_000.0,
            capping=bool(disk_pace["capping"]),
        )
        disk_pace["capping"] = capping
        if capping:
            limiter.min_rate = min(
                float(disk_pace["orig_min"]), max(1_000_000.0, cap)
            )
            limiter.set_rate(min(limiter.rate, cap))
            disk_pace["cap_bps"] = limiter.rate
        else:
            limiter.min_rate = float(disk_pace["orig_min"])
            disk_pace["cap_bps"] = 0.0

    def pace_tick() -> None:
        if not disk_pace["capping"]:
            limiter.min_rate = float(disk_pace["orig_min"])
        if effective_ramp_s > 0:
            elapsed = time.monotonic() - t_ramp0
            if elapsed < effective_ramp_s:
                frac = (elapsed / effective_ramp_s) ** 2
                limiter.set_rate(max(1.0, max_bps * frac))
                apply_disk_feed_cap()
                return
        if delay_cc or adapt_pace:
            with pace_lock:
                limiter.set_rate(pace_state["bps"])
        else:
            limiter.set_rate(max_bps)
        apply_disk_feed_cap()

    def send_batch(wires: list[bytes], *, repair: bool = False) -> None:
        nonlocal send_s, pace_s, wire_bytes, blast_pkts, repair_pkts_w
        if not wires:
            return
        pace_tick()
        # Stamp send_ts at wire time (not encode time) so delay-CC sees path queue.
        # First-pass packets also get a monotonic blast_seq for loss accounting.
        ts_us = int(time.monotonic() * 1_000_000) & 0xFFFFFFFF
        stamped: list[bytes | bytearray] = []
        for w in wires:
            seq = None
            if not repair and len(w) >= GEN_HDR_SIZE:
                seq = int(send_frontier.get("blast_seq") or 0)
                send_frontier["blast_seq"] = (seq + 1) & 0xFFFFFFFF
            stamped.append(
                stamp_gen_wire(w, send_ts_us=ts_us, blast_seq=seq)
                if len(w) >= GEN_HDR_SIZE
                else w
            )
        total = sum(len(w) for w in stamped)
        pace_s += limiter.consume(total)
        assert client_addr is not None
        t_send = time.monotonic()
        send_datagrams(sock, client_addr, stamped)
        send_s += time.monotonic() - t_send
        wire_bytes += total
        send_totals["wire"] += total
        if repair:
            send_totals["repair"] += total
            repair_pkts_w += len(stamped)
        else:
            send_totals["blast"] += total
            blast_pkts += len(stamped)

    def get_or_make_encoder(mm: mmap.mmap, gen_id: int) -> GenEncoder | None:
        if gen_id < 0 or gen_id >= total_gens:
            return None
        with enc_lock:
            enc = encoders.get(gen_id)
        if enc is not None:
            return enc
        off = gen_id * block_bytes
        end = min(off + block_bytes, file_size)
        raw = bytes(mm[off:end])
        if len(raw) < block_bytes:
            raw = raw + b"\x00" * (block_bytes - len(raw))
        enc = GenEncoder(raw, symbol_size, overhead_pct, systematic_only=True)
        with enc_lock:
            existing = encoders.get(gen_id)
            if existing is not None:
                return existing
            encoders[gen_id] = enc
        return enc

    def prune_encoders(protected: set[int]) -> None:
        """Bound native Raptor encoder memory while retaining active repairs."""
        target = _ENCODER_KEEP + len(protected)
        with enc_lock:
            if len(encoders) <= target:
                return
            for old in list(encoders):
                if len(encoders) <= target:
                    break
                if old in protected:
                    continue
                encoders.pop(old, None)

    encode_pool: ProcessPoolExecutor | None = None
    repair_pool: ProcessPoolExecutor | None = None
    file_path_str = str(file_path)

    def repair_one(
        mm: mmap.mmap,
        nid: int,
        *,
        now: float,
        symbols_rx: int | None = None,
        cooldown_s: float = _REPAIR_COOLDOWN_S,
        send_n: int | None = None,
        source_esis: list[int] | None = None,
        hol: bool = False,
        close: bool = False,
        round_max: int | None = None,
    ) -> list[bytes]:
        """Sync repair encode (repair thread fallback)."""
        if (now - last_repair_ts.get(nid, 0.0)) < cooldown_s:
            return []
        esi_wires: list[bytes] = []
        if source_esis:
            enc = get_or_make_encoder(mm, nid)
            if enc is not None:
                pkts = enc.packets()
                ts_us = int(now * 1_000_000) & 0xFFFFFFFF
                esi_wires = [
                    GenPacket(nid, esi, pkts[esi], ts_us).pack()
                    for esi in source_esis[:32]
                    if 0 <= esi < len(pkts)
                ]
        if (
            not esi_wires
            and symbols_rx is not None
            and symbols_rx >= gen_k + _DECODE_MARGIN
            and send_n is None
        ):
            return []
        if nid < 0 or nid >= total_gens:
            return esi_wires
        prior_extra = repair_extra.get(nid, 0)
        if send_n is None:
            if esi_wires:
                send_n = hol_resend_pad_n(
                    len(esi_wires), int(overhead_state["pct"])
                )
                if send_n <= 0:
                    last_repair_ts[nid] = now
                    repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
                    return esi_wires
            else:
                send_n = repair_send_n(
                    symbols_rx,
                    gen_k,
                    hol=hol,
                    overhead_pct=int(overhead_state["pct"]),
                    close=close,
                )
                if send_n <= 0:
                    return []
        cap = _REPAIR_ROUND_MAX if round_max is None else max(1, int(round_max))
        send_n = max(1, min(int(send_n), cap))
        ts_us = int(now * 1_000_000) & 0xFFFFFFFF
        pool = repair_pool if repair_pool is not None else encode_pool
        if pool is not None:
            try:
                enc_cpu_s, sent, wires = pool.submit(
                    _repair_gen_worker,
                    file_path_str,
                    nid,
                    block_bytes,
                    file_size,
                    symbol_size,
                    overhead_pct,
                    gen_k,
                    prior_extra,
                    send_n,
                    ts_us,
                    cap,
                ).result(timeout=30.0)
            except Exception:
                if esi_wires:
                    last_repair_ts[nid] = now
                    repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
                return esi_wires
            if not wires:
                if esi_wires:
                    last_repair_ts[nid] = now
                    repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
                return esi_wires
            win.add_repair_enc(enc_cpu_s)
            repair_extra[nid] = prior_extra + sent
            last_repair_ts[nid] = now
            repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
            return esi_wires + wires
        enc = get_or_make_encoder(mm, nid)
        if enc is None:
            if esi_wires:
                last_repair_ts[nid] = now
                repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
            return esi_wires
        t_enc = time.monotonic()
        target_budget = prior_extra + send_n
        new_pkts = enc.ensure_repair(target_budget)
        if not new_pkts:
            if esi_wires:
                last_repair_ts[nid] = now
                repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
            return esi_wires
        new_pkts = new_pkts[-send_n:]
        repair_extra[nid] = prior_extra + len(new_pkts)
        first_esi = enc.packet_count - len(new_pkts)
        wires = [
            GenPacket(nid, first_esi + i, blob, ts_us).pack()
            for i, blob in enumerate(new_pkts)
        ]
        last_repair_ts[nid] = now
        repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
        win.add_repair_enc(time.monotonic() - t_enc)
        return esi_wires + wires

    def repair_nacks(
        mm: mmap.mmap,
        nacks: list[int],
        nack_rx: dict[int, int],
        next_needed: int,
        *,
        now: float,
        limit: int,
        sent_before: int,
        hol_miss_esi: list[int] | None = None,
        cooldown_s: float | None = None,
        close: bool = False,
    ) -> list[bytes]:
        ordered = order_repair_nacks(
            nacks,
            next_needed,
            nack_rx,
            gen_k=gen_k,
            limit=limit,
            sent_before=sent_before,
        )
        wires: list[bytes] = []
        for nid in ordered:
            miss = hol_miss_esi if nid == next_needed else None
            is_hol = nid == next_needed
            cd = (
                float(cooldown_s)
                if cooldown_s is not None
                else hol_repair_cooldown_s(
                    nid, next_needed, hol_miss=bool(miss)
                )
            )
            wires.extend(
                repair_one(
                    mm,
                    nid,
                    now=now,
                    symbols_rx=nack_rx.get(nid),
                    source_esis=miss,
                    cooldown_s=cd,
                    hol=is_hol,
                    close=close,
                )
            )
        if ordered:
            prune_encoders(set(ordered))
        return wires

    def repair_hol_window(
        mm: mmap.mmap,
        *,
        now: float,
        next_needed: int,
        nacks: list[int],
        nack_rx: dict[int, int],
        miss_bm: bytes,
        hol_miss: list[int] | None,
        sent_before: int,
        max_pkts: int = _HOL_SHARE_TURN_PKTS,
        turn_gens: int = _HOL_SHARE_TURN_GENS,
    ) -> list[bytes]:
        """Close the HOL window from bitmap+NACKs; do not spray the decoded tail."""
        if next_needed < 0 or next_needed >= sent_before:
            return []
        miss_nacks = list(nacks)
        if miss_bm:
            miss_nacks.extend(miss_bitmap_to_nacks(next_needed, miss_bm))
        targets = drain_scoreboard_targets(
            next_needed=next_needed,
            total_gens=min(total_gens, sent_before),
            miss_nacks=miss_nacks,
            never_seen=None,
        )
        drain_cap = drain_empty_round_max(gen_k)
        room = hol_share_cap_n(max_pkts)
        wires: list[bytes] = []
        turn = targets[: max(1, int(turn_gens))]
        used: list[int] = []
        for gid in turn:
            if room <= 0:
                break
            rx = nack_rx.get(gid)
            empty = rx is None or int(rx) <= 0
            send_n = min(drain_repair_send_n(rx, gen_k, empty=empty), room)
            is_hol = gid == next_needed
            chunk = repair_one(
                mm,
                gid,
                now=now,
                symbols_rx=rx,
                send_n=send_n,
                source_esis=hol_miss if is_hol else None,
                cooldown_s=_DRAIN_COOLDOWN_S,
                hol=is_hol,
                close=True,
                round_max=min(drain_cap, room),
            )
            if chunk:
                wires.extend(chunk)
                room -= len(chunk)
                used.append(gid)
        if used:
            prune_encoders(set(used))
        return wires[: hol_share_cap_n(max_pkts)]

    t0 = time.monotonic()
    bytes_sent_payload = 0
    gens_sent = 0
    repair_sent = 0
    last_progress = t0
    stuck_nn = -1
    stuck_nn_since = t0
    hol_pause_active = False
    hol_pause_repair_acc = 0
    win = _Win()
    send_s = 0.0
    pace_s = 0.0
    cap_s = 0.0
    wait_enc_s = 0.0
    wire_bytes = 0
    blast_pkts = 0
    blast_gens = 0
    repair_pkts_w = 0

    try:
        with file_path.open("rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            fadv = getattr(os, "posix_fadvise", None)
            sequential = getattr(os, "POSIX_FADV_SEQUENTIAL", None)
            if fadv is not None and sequential is not None:
                try:
                    fadv(f.fileno(), 0, 0, sequential)
                except OSError:
                    pass
            # spawn: safe with the already-running feedback thread, and worker
            # processes encode without contending for this process's GIL.
            mp_ctx = multiprocessing.get_context("spawn")
            encode_out_q: queue.Queue = mp_ctx.Queue(maxsize=256)
            encode_pool = ProcessPoolExecutor(
                max_workers=encode_workers,
                mp_context=mp_ctx,
                initializer=_encode_pool_init,
                initargs=(encode_out_q,),
            )
            repair_pool = ProcessPoolExecutor(
                max_workers=repair_workers,
                mp_context=multiprocessing.get_context("spawn"),
            )
            encode_ready: dict[int, tuple[float, int, list[bytes], int]] = {}
            encode_inflight: set[int] = set()
            encode_batch_futs: dict[int, Future[int]] = {}
            repair_q: queue.Queue[list[bytes]] = queue.Queue(maxsize=64)
            repair_fut_lock = threading.Lock()
            repair_futures: list[
                tuple[Future[tuple[float, int, list[bytes]]], int, int]
            ] = []
            stop_repair = threading.Event()
            cursor = {"gen_id": 0}
            stop_readahead = threading.Event()
            stop_disk = threading.Event()
            disk_cond = threading.Condition()
            disk_state: dict = {
                "blobs": {},
                "queued": 0,
                "off": 0,
                "err": None,
                "read_bytes": 0,
                "rate_bps": 0.0,
                "rate_ts": 0.0,
                "rate_at": 0,
                "max_bytes": disk_queue_bytes,
            }
            readahead_thread = threading.Thread(
                target=run_file_readahead,
                kwargs={
                    "path": file_path_str,
                    "file_size": file_size,
                    "block_bytes": block_bytes,
                    "cursor": cursor,
                    "stop": stop_readahead,
                    "ahead_bytes": readahead_bytes,
                },
                name="gen-readahead",
                daemon=True,
            )
            disk_thread = threading.Thread(
                target=run_file_disk_queue,
                kwargs={
                    "path": file_path_str,
                    "file_size": file_size,
                    "block_bytes": block_bytes,
                    "batch_n": encode_read_gens,
                    "cursor": cursor,
                    "stop": stop_disk,
                    "cond": disk_cond,
                    "state": disk_state,
                    "max_bytes": disk_queue_bytes,
                },
                name="gen-disk-q",
                daemon=True,
            )
            if readahead_bytes > 0 and disk_queue_bytes <= 0:
                readahead_thread.start()
            if disk_queue_bytes > 0:
                disk_thread.start()
                disk_pace["on"] = disk_pace_on
                disk_pace["max_bytes"] = disk_queue_bytes
                disk_pace["min_bytes"] = disk_queue_bytes
                disk_pace["hi_bytes"] = disk_queue_max_bytes
                disk_pace["adapt"] = disk_queue_adapt
                disk_pace["state"] = disk_state
                disk_pace["cond"] = disk_cond
                with disk_cond:
                    disk_cond.wait_for(
                        lambda: bool(disk_state["blobs"]) or disk_state["err"],
                        timeout=2.0,
                    )
            fountain_gens: set[int] = set()
            fountain_lock = threading.Lock()
            fountain_tick_n = 0
            repair_spread_n = 0
            storm_state = {"until": 0.0}
            repair_pending_gids: set[int] = set()

            def repair_pending_count() -> int:
                with repair_fut_lock:
                    return len(repair_futures)

            def repair_submit_async(
                mm: mmap.mmap,
                nid: int,
                *,
                now: float,
                symbols_rx: int | None = None,
                cooldown_s: float = _REPAIR_COOLDOWN_S,
                send_n: int | None = None,
                allow_in_storm: bool = False,
                hol: bool = False,
            ) -> bool:
                if (
                    now < storm_state["until"]
                    and send_n is not None
                    and not allow_in_storm
                ):
                    return False
                if nid in repair_pending_gids:
                    return False
                with fb_lock:
                    nn_now = int(fb_state["next_needed"])
                if nid < nn_now:
                    return False
                if cooldown_s > 0 and (now - last_repair_ts.get(nid, 0.0)) < cooldown_s:
                    return False
                if (
                    symbols_rx is not None
                    and symbols_rx >= gen_k + _DECODE_MARGIN
                    and send_n is None
                ):
                    return False
                if nid < 0 or nid >= total_gens or (
                    repair_pool is None and encode_pool is None
                ):
                    wires = repair_one(
                        mm,
                        nid,
                        now=now,
                        symbols_rx=symbols_rx,
                        cooldown_s=cooldown_s,
                        send_n=send_n,
                        hol=hol,
                    )
                    if wires:
                        try:
                            repair_q.put_nowait(wires)
                        except queue.Full:
                            win.add_qdrop()
                    return bool(wires)
                prior_extra = repair_extra.get(nid, 0)
                if send_n is None:
                    send_n = repair_send_n(
                        symbols_rx,
                        gen_k,
                        hol=hol,
                        overhead_pct=int(overhead_state["pct"]),
                    )
                    if send_n <= 0:
                        return False
                if fountain_mode:
                    send_n = cap_fountain_send(
                        gen_k,
                        prior_extra,
                        send_n,
                        symbols_rx,
                    )
                    if send_n <= 0:
                        return False
                send_n = max(1, min(int(send_n), _REPAIR_ROUND_MAX))
                ts_us = int(now * 1_000_000) & 0xFFFFFFFF
                with repair_fut_lock:
                    if len(repair_futures) >= _REPAIR_FUTURES_MAX:
                        return False
                    repair_pending_gids.add(nid)
                fut = (repair_pool or encode_pool).submit(
                    _repair_gen_worker,
                    file_path_str,
                    nid,
                    block_bytes,
                    file_size,
                    symbol_size,
                    overhead_pct,
                    gen_k,
                    prior_extra,
                    send_n,
                    ts_us,
                )
                with repair_fut_lock:
                    repair_futures.append((fut, nid, prior_extra))
                last_repair_ts[nid] = now
                return True

            def drain_repair_futures() -> int:
                n = 0
                pending: list[tuple[Future, int, int]] = []
                with repair_fut_lock:
                    for item in repair_futures:
                        pending.append(item)
                    repair_futures.clear()
                still: list[tuple[Future, int, int]] = []
                for fut, nid, prior_extra in pending:
                    if not fut.done():
                        still.append((fut, nid, prior_extra))
                        continue
                    repair_pending_gids.discard(nid)
                    try:
                        enc_cpu_s, sent, wires = fut.result()
                    except Exception:
                        continue
                    with fb_lock:
                        nn_now = int(fb_state["next_needed"])
                    if nid < nn_now:
                        continue
                    if not wires:
                        continue
                    win.add_repair_enc(enc_cpu_s)
                    repair_extra[nid] = prior_extra + sent
                    repair_rounds[nid] = repair_rounds.get(nid, 0) + 1
                    send_batch(wires, repair=True)
                    n += len(wires)
                if still:
                    with repair_fut_lock:
                        repair_futures.extend(still)
                return n

            def prune_fountain(
                next_needed: int,
                nack_rx: dict[int, int],
                nacks: list[int],
                sent_before: int,
            ) -> None:
                with fountain_lock:
                    prune_fountain_gens_set(
                        fountain_gens,
                        next_needed,
                        nack_rx,
                        nacks,
                        gen_k=gen_k,
                        sent_before=sent_before,
                        inflight_gen_limit=inflight_gen_limit,
                    )

            def fountain_send_tick(
                mm: mmap.mmap,
                *,
                now: float,
                next_needed: int,
                nacks: list[int],
                nack_rx: dict[int, int],
                sent_before: int,
                at_cap: bool,
                pressure: float = 0.0,
            ) -> int:
                """Queue async fountain repair; returns number of submits."""
                if sent_before <= 0:
                    return 0
                prune_fountain(next_needed, nack_rx, nacks, sent_before)
                if not fountain_mode:
                    with fountain_lock:
                        if not fountain_gens and not at_cap and pressure < _FOUNTAIN_PRESSURE:
                            return 0
                targets = fountain_targets(
                    next_needed,
                    sent_before,
                    nacks,
                    nack_rx,
                    gen_k=gen_k,
                    limit=_FOUNTAIN_CAP_GENS,
                    frontier_only=(not at_cap) and pressure < _FOUNTAIN_PRESSURE,
                    offset=fountain_tick_n,
                )
                queued = 0
                for nid in targets:
                    send_n = repair_send_n(
                        nack_rx.get(nid),
                        gen_k,
                        hol=(nid == next_needed),
                        overhead_pct=int(overhead_state["pct"]),
                    )
                    if send_n <= 0:
                        continue
                    if repair_submit_async(
                        mm,
                        nid,
                        now=now,
                        symbols_rx=nack_rx.get(nid),
                        cooldown_s=_FOUNTAIN_CAP_COOLDOWN_S,
                        send_n=send_n,
                        hol=(nid == next_needed),
                    ):
                        queued += 1
                if targets:
                    prune_encoders(set(targets))
                return queued

            def drain_encode_stream() -> None:
                for b0, fut in list(encode_batch_futs.items()):
                    if not fut.done():
                        continue
                    encode_batch_futs.pop(b0, None)
                    exc = fut.exception()
                    if exc is not None:
                        raise RuntimeError(f"encode batch {b0} failed") from exc
                drain_encode_out_queue(
                    encode_out_q, encode_ready, encode_inflight, max_n=256
                )

            def fill_encode_queue(start: int) -> None:
                stop = min(total_gens, start + encode_prefetch)
                pct = int(overhead_state["pct"])
                gid = max(0, start)
                while gid < stop:
                    b0 = encode_batch_start(gid, encode_read_gens)
                    n = min(encode_read_gens, total_gens - b0)
                    covered = False
                    for i in range(n):
                        g = b0 + i
                        if g in encode_inflight or g in encode_ready:
                            covered = True
                            break
                    if not covered:
                        if disk_queue_bytes > 0:
                            blob = disk_queue_pop_blob(disk_state, disk_cond, b0)
                            if blob is None:
                                break
                            fut = encode_pool.submit(
                                _encode_blob_worker_stream,
                                blob,
                                b0,
                                n,
                                block_bytes,
                                file_size,
                                symbol_size,
                                pct,
                                fountain_mode,
                            )
                        else:
                            fut = encode_pool.submit(
                                _encode_gens_worker_stream,
                                file_path_str,
                                b0,
                                n,
                                block_bytes,
                                file_size,
                                symbol_size,
                                pct,
                                fountain_mode,
                            )
                        encode_batch_futs[b0] = fut
                        for i in range(n):
                            encode_inflight.add(b0 + i)
                    gid = b0 + n

            def repair_loop() -> None:
                nonlocal repair_spread_n
                while not stop_repair.is_set():
                    with fb_lock:
                        if fb_state["done"]:
                            break
                        nacks = list(fb_state["nacks"])
                        nack_rx = dict(fb_state["nack_rx"])
                        next_needed = int(fb_state["next_needed"])
                        completed = int(fb_state["completed"])
                        hol_miss = list(fb_state.get("hol_miss_esi") or [])
                    gid = cursor["gen_id"]
                    if gid <= 0:
                        time.sleep(0.005)
                        continue
                    incomplete = max(0, gid - completed)
                    at_cap = should_pause_blast(
                        incomplete, 0, inflight_gen_limit=inflight_gen_limit
                    )
                    now = time.monotonic()
                    storm_active = now < storm_state["until"]
                    stuck = (now - stuck_nn_since) >= _STUCK_S and not storm_active
                    wires: list[bytes] = []
                    if (
                        stuck
                        and (gid - next_needed) >= _REPAIR_LAG
                        and 0 <= next_needed < gid
                        and (now - last_full_ts.get(next_needed, 0.0))
                        >= _STUCK_REPAIR_COOLDOWN_S
                    ):
                        last_repair_ts.pop(next_needed, None)
                        rx = nack_rx.get(next_needed)
                        extra = repair_one(
                            mm,
                            next_needed,
                            now=now,
                            symbols_rx=rx,
                            send_n=drain_repair_send_n(
                                rx, gen_k, empty=rx is None or int(rx) <= 0
                            ),
                            source_esis=hol_miss or None,
                            hol=True,
                            close=True,
                            round_max=drain_empty_round_max(gen_k),
                        )
                        if extra:
                            last_full_ts[next_needed] = now
                            wires.extend(extra)
                    if (
                        server_synth_nack
                        and not nacks
                        and 0 <= next_needed < gid
                        and (
                            at_cap
                            or stuck
                            or incomplete >= max(1, inflight_gen_limit // 8)
                        )
                    ):
                        nacks = [next_needed]
                    frontier_lag = max(0, gid - next_needed)
                    repair_limit = repair_thread_limit(
                        at_cap=at_cap,
                        storm_active=storm_active,
                        nack_count=len(nacks),
                        frontier_lag=frontier_lag,
                        inflight_gen_limit=inflight_gen_limit,
                    )
                    if hol_miss and 0 <= next_needed < gid:
                        extra = repair_one(
                            mm,
                            next_needed,
                            now=now,
                            symbols_rx=nack_rx.get(next_needed),
                            source_esis=hol_miss,
                            hol=True,
                        )
                        if extra:
                            wires.extend(extra)
                    ordered = order_repair_nacks(
                        nacks if not storm_active else nacks[:1],
                        next_needed,
                        nack_rx,
                        gen_k=gen_k,
                        limit=repair_limit,
                        sent_before=gid,
                        offset=repair_spread_n,
                    )
                    repair_spread_n += 1
                    if not storm_active or nacks:
                        for nid in ordered:
                            if nid == next_needed and hol_miss:
                                continue
                            repair_submit_async(
                                mm,
                                nid,
                                now=now,
                                symbols_rx=nack_rx.get(nid),
                                cooldown_s=hol_repair_cooldown_s(
                                    nid, next_needed, hol_miss=False
                                ),
                                hol=(nid == next_needed),
                            )
                    if wires:
                        try:
                            repair_q.put(wires, timeout=0.05)
                        except queue.Full:
                            win.add_qdrop()
                    else:
                        time.sleep(0.004)

            repair_thread = threading.Thread(
                target=repair_loop, name="gen-repair", daemon=True
            )
            repair_thread.start()

            def drain_repairs() -> int:
                n = 0
                while True:
                    try:
                        batch = repair_q.get_nowait()
                    except queue.Empty:
                        break
                    send_batch(batch, repair=True)
                    n += len(batch)
                return n

            try:
                gen_id = 0
                fill_encode_queue(0)
                while gen_id < total_gens:
                    drain_encode_stream()
                    with fb_lock:
                        if fb_state["done"]:
                            break
                        next_needed = int(fb_state["next_needed"])
                        completed = int(fb_state["completed"])

                    now = time.monotonic()
                    if completed <= 0 and (now - last_meta_ts) >= 0.15:
                        sock.sendto(meta, client_addr)
                        last_meta_ts = now
                    if effective_ramp_s <= 0 or (now - t_ramp0) >= effective_ramp_s:
                        pace_tick()
                    if next_needed != stuck_nn:
                        stuck_nn = next_needed
                        stuck_nn_since = now
                        for gid in list(repair_rounds):
                            if gid < next_needed:
                                note_close_round(
                                    repair_close_hist, repair_rounds.pop(gid)
                                )
                        prune_repair_meta(
                            repair_extra,
                            last_repair_ts,
                            last_full_ts,
                            next_needed,
                        )
                    incomplete = max(0, gen_id - completed)
                    lag = gen_id - next_needed
                    if adapt_overhead:
                        with fb_lock:
                            nacks_oh = list(fb_state["nacks"])
                            nack_rx_oh = dict(fb_state["nack_rx"])
                            loss_rep = dict(fb_state.get("loss") or {})
                        rank_miss = blast_fec_miss_frac(
                            nack_rx_oh,
                            nacks_oh,
                            gen_k=gen_k,
                            window_gens=blast_fec_window_gens(
                                nack_count=len(nacks_oh),
                                incomplete=incomplete,
                                frontier_lag=lag,
                                inflight_gen_limit=inflight_gen_limit,
                                sent_gens=gen_id,
                            ),
                        )
                        if packet_loss_fec:
                            delta = take_loss_sample(
                                overhead_state["loss_ingest"], loss_rep
                            )
                            sample = accumulate_loss_sample(
                                overhead_state["loss_accum"], delta
                            )
                            overhead_state["loss_window_n"] = (
                                int(sample["n"]) if sample is not None else 0
                            )
                            (
                                overhead_state["pct"],
                                overhead_state["p_fast"],
                                overhead_state["p_slow"],
                                overhead_state["p"],
                                overhead_state["fec_base"],
                                overhead_state["fec_boost"],
                                overhead_state["clean_windows"],
                            ) = apply_packet_loss_fec(
                                cur_pct=int(overhead_state["pct"]),
                                sample=sample,
                                rank_miss=rank_miss,
                                p_fast=float(overhead_state["p_fast"]),
                                p_slow=float(overhead_state["p_slow"]),
                                clean_windows=int(
                                    overhead_state.get("clean_windows") or 0
                                ),
                                floor_pct=_OH_FLOOR_PCT,
                                max_pct=max(overhead_pct, _OH_CEIL_PCT),
                                start_pct=_OH_CEIL_PCT,
                                elapsed_s=now - t0,
                                sent_gens=gen_id,
                                start_hold_s=_OH_START_HOLD_S,
                                start_hold_gens=inflight_gen_limit,
                            )
                            if sample is not None:
                                overhead_state["miss"] = float(sample["p"])
                                overhead_state["loss_late"] = int(sample["late"])
                                overhead_state["loss_pending"] = int(
                                    sample["pending"]
                                )
                            elif rank_miss >= _OH_MISS_MID:
                                overhead_state["miss"] = 0.7 * float(
                                    overhead_state.get("miss") or 0.0
                                ) + 0.3 * rank_miss
                        else:
                            raw_miss = max(
                                rank_miss,
                                blast_fec_open_frac(
                                    incomplete, inflight_gen_limit
                                ),
                            )
                            prev_miss = float(overhead_state.get("miss") or 0.0)
                            if nacks_oh or raw_miss >= _OH_MISS_MID:
                                miss_oh = 0.7 * prev_miss + 0.3 * raw_miss
                            else:
                                miss_oh = prev_miss * 0.92
                            overhead_state["miss"] = miss_oh
                            overhead_state["pct"] = adaptive_blast_overhead_pct(
                                base_pct=overhead_pct,
                                frontier_lag=lag,
                                nack_count=len(nacks_oh),
                                incomplete=incomplete,
                                inflight_gen_limit=inflight_gen_limit,
                                miss_frac=miss_oh,
                                max_pct=max(overhead_pct, _OH_CEIL_PCT),
                                floor_pct=_OH_FLOOR_PCT,
                                start_pct=_OH_CEIL_PCT,
                                elapsed_s=now - t0,
                                sent_gens=gen_id,
                                start_hold_s=_OH_START_HOLD_S,
                                start_hold_gens=inflight_gen_limit,
                            )
                    if incomplete >= (inflight_gen_limit * 3) // 4:
                        limiter.set_burst_s(0.002)
                    elif incomplete <= inflight_gen_limit // 2:
                        limiter.set_burst_s(0.016)
                    repair_sent += drain_repairs()
                    repair_sent += drain_repair_futures()
                    lag = gen_id - next_needed
                    stressed = pipeline_stressed(
                        incomplete, lag, inflight_gen_limit=inflight_gen_limit
                    )
                    at_cap = should_pause_blast(
                        incomplete, lag, inflight_gen_limit=inflight_gen_limit
                    )
                    with fb_lock:
                        nacks = list(fb_state["nacks"])
                        nack_rx = dict(fb_state["nack_rx"])
                        miss_bm = fb_state.get("miss_bitmap") or b""
                        hol_miss_fb = list(fb_state.get("hol_miss_esi") or [])
                    storm_active = now < storm_state["until"]
                    pressure = repair_pressure(
                        incomplete,
                        lag,
                        inflight_gen_limit=inflight_gen_limit,
                    )
                    hol_hole = hol_hole_gens(incomplete, lag)
                    stuck_hol = (now - stuck_nn_since) >= _STUCK_S
                    want_pause = hol_pause_should_hold(
                        active=hol_pause_active,
                        hol_hole=hol_hole,
                        stuck=stuck_hol,
                        occupancy=incomplete,
                        inflight_gen_limit=inflight_gen_limit,
                    ) and gen_id > next_needed
                    if want_pause and not hol_pause_active:
                        last_repair_ts.pop(next_needed, None)
                    hol_pause_active = want_pause
                    if hol_pause_active:
                        extra = repair_hol_window(
                            mm,
                            now=now,
                            next_needed=next_needed,
                            nacks=nacks,
                            nack_rx=nack_rx,
                            miss_bm=miss_bm,
                            hol_miss=hol_miss_fb or None,
                            sent_before=gen_id,
                        )
                        if extra:
                            send_batch(extra, repair=True)
                            repair_sent += len(extra)
                            hol_pause_repair_acc += len(extra)
                        repair_sent += drain_repairs()
                        repair_sent += drain_repair_futures()
                        now_p = time.monotonic()
                        if now_p - last_progress >= 1.0:
                            last_progress = now_p
                            print(
                                f"hol-share next={next_needed} hole={hol_hole} "
                                f"done={completed}/{total_gens} sent={gen_id} "
                                f"repair={hol_pause_repair_acc}",
                                flush=True,
                            )
                            hol_pause_repair_acc = 0
                    if (not hol_pause_active) and should_fountain_tick(
                        stressed=stressed,
                        at_cap=at_cap,
                        tick_n=fountain_tick_n,
                        storm_active=storm_active,
                        fountain_mode=fountain_mode,
                        pressure=pressure,
                        nack_count=len(nacks),
                    ):
                        fountain_send_tick(
                            mm,
                            now=now,
                            next_needed=next_needed,
                            nacks=nacks,
                            nack_rx=nack_rx,
                            sent_before=gen_id,
                            at_cap=at_cap,
                            pressure=pressure,
                        )
                    fountain_tick_n += 1
                    repair_sent += drain_repair_futures()
                    repair_pending = (
                        not repair_q.empty() or repair_pending_count() > 0
                    )
                    if (not hol_pause_active) and should_yield_blast_to_repair(
                        pressure,
                        repair_pending=repair_pending,
                        nack_count=len(nacks),
                        hol_hole=hol_hole_gens(incomplete, lag),
                    ):
                        if not storm_active:
                            fountain_send_tick(
                                mm,
                                now=now,
                                next_needed=next_needed,
                                nacks=nacks,
                                nack_rx=nack_rx,
                                sent_before=gen_id,
                                at_cap=at_cap,
                                pressure=pressure,
                            )
                        repair_sent += drain_repairs()
                        repair_sent += drain_repair_futures()
                    if should_pause_blast(
                        incomplete,
                        lag,
                        inflight_gen_limit=inflight_gen_limit,
                    ):
                        if repair_q.empty() and not repair_futures:
                            time.sleep(0.001)
                            cap_s += 0.001
                        continue

                    if gen_id not in encode_ready:
                        fill_encode_queue(gen_id)
                        drain_encode_stream()
                        if gen_id not in encode_ready:
                            time.sleep(0.0005)
                            wait_enc_s += 0.0005
                            continue
                    enc_cpu_s, source_bytes, wires, bootstrap = encode_ready.pop(
                        gen_id
                    )
                    win.add_enc(enc_cpu_s)
                    fill_encode_queue(gen_id + 1)
                    prune_encoders({next_needed})

                    send_batch(wires)
                    if bootstrap:
                        repair_extra[gen_id] = bootstrap
                    if stressed:
                        with fountain_lock:
                            track_fountain_gen(
                                fountain_gens,
                                gen_id,
                                next_needed,
                                at_cap=at_cap,
                            )
                    gens_sent += 1
                    blast_gens += 1
                    bytes_sent_payload += source_bytes
                    gen_id += 1
                    cursor["gen_id"] = gen_id
                    send_frontier["gen_id"] = gen_id

                    now = time.monotonic()
                    if now - last_progress >= 1.0:
                        dt = now - last_progress
                        last_progress = now
                        with fb_lock:
                            done_g = fb_state["completed"]
                            nn = int(fb_state["next_needed"])
                        refresh_inflight(max(0, gen_id - int(done_g)))
                        rate = bytes_sent_payload / max(now - t0, 1e-6) / (1024 * 1024)
                        pace_mbit = limiter.rate * 8.0 / 1_000_000.0
                        cap_mbit = max_bps * 8.0 / 1_000_000.0
                        enc_s, renc_s, qdrop = win.take_workers()
                        sys_s, blk_s, sys_n, blk_n = take_send_stats()
                        parts = {
                            "send": send_s,
                            "pace": pace_s,
                            "cap": cap_s,
                            "wenc": wait_enc_s,
                        }
                        bottleneck = max(parts, key=parts.get)
                        disk_mib = 0.0
                        disk_feed_mib = 0.0
                        disk_cap_mib = disk_queue_bytes // (1024 * 1024)
                        if disk_pace["on"] and disk_pace["state"] is not None:
                            disk_mib = int(disk_pace["state"].get("queued") or 0) / (
                                1024 * 1024
                            )
                            disk_feed_mib = float(
                                disk_pace["state"].get("rate_bps") or 0.0
                            ) / (1024 * 1024)
                            disk_cap_mib = int(
                                disk_pace["state"].get("max_bytes")
                                or disk_pace["max_bytes"]
                                or disk_queue_bytes
                            ) // (1024 * 1024)
                            if disk_pace["capping"]:
                                bottleneck = "disk"
                        wire_mib = wire_bytes / dt / (1024 * 1024)
                        repair_rate = repair_pkts_w / max(dt, 1e-6)
                        deliv_mib = 0.0
                        if delay_cc and (now - t_ramp0) >= _DELAY_CC_DELIVERY_WARMUP_S:
                            with pace_lock:
                                last_c = int(pace_state["guard_completed"])
                                last_gts = float(pace_state["guard_ts"])
                                gdt = now - last_gts
                                if last_c < 0:
                                    pace_state["guard_completed"] = int(done_g)
                                    pace_state["guard_ts"] = now
                                elif gdt >= 0.8:
                                    delivery_bps = (
                                        max(0, int(done_g) - last_c) * block_bytes / gdt
                                    )
                                    deliv_mib = delivery_bps / (1024 * 1024)
                                    # BBR already paces from delivery. Skip the
                                    # old goodput guard: a HOL-full window is
                                    # protocol stall, not a policer.
                                    pace_state["guard_completed"] = int(done_g)
                                    pace_state["guard_ts"] = now
                        with fountain_lock:
                            fount_n = len(fountain_gens)
                        # Repair traffic alone is not congestion. Reorder/HOL can
                        # generate many repairs while the receive frontier stays
                        # healthy; only back pace off when the pipeline agrees.
                        if repair_storm_detected(repair_rate, fount_n) and stressed:
                            storm_state["until"] = now + _REPAIR_STORM_BACKOFF_S
                            # Repair load is not a path-capacity sample. Keep the
                            # confirmed BBR cruise; scheduling already yields
                            # blast turns to targeted repair.
                        storm_flag = "storm" if now < storm_state["until"] else (
                            "fountain" if fountain_mode else (
                                "stress" if stressed else "clean"
                            )
                        )
                        with pace_lock:
                            rtt_ms = pace_state["rtt_s"] * 1000.0
                            q_ms = pace_state["queue_s"] * 1000.0
                            oh_pct = int(overhead_state["pct"])
                            oh_miss = float(overhead_state.get("miss") or 0.0)
                            p_fast = float(overhead_state.get("p_fast") or 0.0)
                            p_slow = float(overhead_state.get("p_slow") or 0.0)
                            fec_base = int(overhead_state.get("fec_base") or oh_pct)
                            fec_boost = int(overhead_state.get("fec_boost") or 0)
                            loss_late = int(overhead_state.get("loss_late") or 0)
                            loss_pending = int(
                                overhead_state.get("loss_pending") or 0
                            )
                            loss_window_n = int(
                                overhead_state.get("loss_window_n") or 0
                            )
                            btlbw_mbit = float(pace_state["btlbw"]) * 8.0 / 1_000_000.0
                            startup_flag = "start" if pace_state["bbr_startup"] else "bw"
                            startup_phase = str(pace_state["startup_phase"])
                            startup_reason = str(pace_state["startup_reason"])
                            startup_offer = (
                                float(pace_state["startup_offer"])
                                * 8.0
                                / 1_000_000.0
                            )
                            startup_confirmed = (
                                float(pace_state["startup_confirmed"])
                                * 8.0
                                / 1_000_000.0
                            )
                            startup_ratio = float(pace_state["startup_ratio"])
                            startup_bad = int(pace_state["startup_bad"])
                            startup_flat = int(pace_state["startup_flat"])
                            startup_hol = int(pace_state["startup_hol"])
                            sample_reason = str(pace_state["sample_reason"])
                            bbr_gain = float(pace_state.get("gain") or 0.0)
                        for gid in list(repair_rounds):
                            if gid < nn:
                                note_close_round(
                                    repair_close_hist, repair_rounds.pop(gid)
                                )
                        print(
                            f"progress {gen_id}/{total_gens} "
                            f"client_done={done_g} "
                            f"fec={oh_pct}% "
                            f"miss={oh_miss:.3f} "
                            f"loss={oh_miss:.3f}/{p_fast:.3f}/{p_slow:.3f} "
                            f"loss_n={loss_window_n} "
                            f"late={loss_late} pend={loss_pending} "
                            f"fec_base={fec_base} fec_boost={fec_boost} "
                            f"pace={pace_mbit:.0f}/{cap_mbit:.0f}Mbit "
                            f"btlbw={btlbw_mbit:.0f}Mbit "
                            f"bbr={startup_flag} "
                            f"startup={startup_phase}/{startup_reason} "
                            f"offer={startup_offer:.0f} confirmed={startup_confirmed:.0f} "
                            f"ratio={startup_ratio:.2f} "
                            f"bad={startup_bad} flat={startup_flat} hol={startup_hol} "
                            f"sample={sample_reason} "
                            f"g={bbr_gain:.2f} "
                            f"lag={gen_id - nn} "
                            f"incomplete={max(0, gen_id - int(done_g))}/"
                            f"{inflight_gen_limit} "
                            f"win={window_mib:.0f}MiB "
                            f"fountain={fount_n} "
                            f"pipe={storm_flag} "
                            f"rtt={rtt_ms:.0f}/{q_ms:.0f}ms "
                            f"repair_extra={repair_sent} "
                            f"{format_close_rounds(repair_close_hist, label='repair_close')} "
                            f"deliv={deliv_mib:.1f} "
                            f"app={rate:.1f} MiB/s"
                        )
                        print(
                            f"  xfer wire={wire_mib:.1f} "
                            f"blast={blast_pkts} gens={blast_gens} "
                            f"repair={repair_pkts_w} "
                            f"enc_q={len(encode_inflight)} "
                            f"repair_fut={repair_pending_count()} "
                            f"meta={len(repair_extra)} "
                            f"qdrop={qdrop} "
                            f"enc_cpu={enc_s * 1000:.0f}ms "
                            f"ms/gen={enc_s / max(blast_gens, 1) * 1000:.1f} "
                            f"renc={renc_s * 1000:.0f}ms "
                            f"send={_pct(send_s, dt)}% "
                            f"(sys={sys_s * 1000:.0f}ms/{sys_n} "
                            f"blk={blk_s * 1000:.0f}ms/{blk_n}) "
                            f"pace={_pct(pace_s, dt)}% "
                            f"cap={_pct(cap_s, dt)}% "
                            f"wenc={_pct(wait_enc_s, dt)}% "
                            f"diskq={disk_mib:.0f}/{disk_cap_mib}MiB "
                            f"feed={disk_feed_mib:.0f} "
                            f"limit={bottleneck}"
                        )
                        snap = host_cpu.sample()
                        if snap is not None and snap.available:
                            print(snap.format_line(), flush=True)
                        send_s = pace_s = cap_s = wait_enc_s = 0.0
                        wire_bytes = 0
                        blast_pkts = 0
                        blast_gens = 0
                        repair_pkts_w = 0

                stop_repair.set()
                repair_thread.join(timeout=1.0)
                repair_sent += drain_repairs()
                repair_sent += drain_repair_futures()

                fin = FinPacket(True, total_gens).pack()
                for _ in range(16):
                    sock.sendto(fin, client_addr)

                drain_deadline = time.monotonic() + 300.0
                drain_t_log = 0.0
                while time.monotonic() < drain_deadline:
                    with fb_lock:
                        if fb_state["done"] or fb_state["completed"] >= total_gens:
                            break
                        if int(fb_state["next_needed"]) >= total_gens:
                            break
                        nacks = list(fb_state["nacks"])
                        nack_rx = dict(fb_state["nack_rx"])
                        next_needed = int(fb_state["next_needed"])
                        hol_miss = list(fb_state.get("hol_miss_esi") or [])
                        never_seen = fb_state.get("never_seen")
                        miss_bm = fb_state.get("miss_bitmap") or b""
                        done_g = int(fb_state["completed"])
                    now = time.monotonic()
                    miss_nacks = list(nacks)
                    if miss_bm:
                        miss_nacks.extend(miss_bitmap_to_nacks(next_needed, miss_bm))
                    targets = drain_scoreboard_targets(
                        next_needed=next_needed,
                        total_gens=total_gens,
                        miss_nacks=miss_nacks,
                        never_seen=never_seen,
                    )
                    if not targets:
                        sock.sendto(fin, client_addr)
                        time.sleep(0.02)
                        continue
                    drain_cap = drain_empty_round_max(gen_k)
                    ns = total_gens if never_seen is None else int(never_seen)
                    wires: list[bytes] = []
                    empty_n = 0
                    turn = targets[:_DRAIN_TURN_GENS]
                    for gid in turn:
                        rx = nack_rx.get(gid)
                        empty = rx is None or int(rx) <= 0 or gid >= ns
                        if empty:
                            empty_n += 1
                        send_n = drain_repair_send_n(rx, gen_k, empty=empty)
                        is_hol = gid == next_needed
                        wires.extend(
                            repair_one(
                                mm,
                                gid,
                                now=now,
                                symbols_rx=rx,
                                send_n=send_n,
                                source_esis=hol_miss if is_hol else None,
                                cooldown_s=_DRAIN_COOLDOWN_S,
                                hol=is_hol,
                                close=True,
                                round_max=drain_cap,
                            )
                        )
                    if turn:
                        prune_encoders(set(turn))
                    if wires:
                        send_batch(wires, repair=True)
                        repair_sent += len(wires)
                    if now - drain_t_log >= 1.0:
                        print(
                            f"drain next={next_needed} "
                            f"done={done_g}/{total_gens} "
                            f"targets={len(targets)} empty={empty_n} "
                            f"sent={len(wires)} never_seen={never_seen}",
                            flush=True,
                        )
                        drain_t_log = now
                    sock.sendto(fin, client_addr)
                    time.sleep(0.005)

            finally:
                stop_repair.set()
                stop_readahead.set()
                stop_disk.set()
                with disk_cond:
                    disk_cond.notify_all()
                repair_thread.join(timeout=1.0)
                if readahead_thread.ident is not None:
                    readahead_thread.join(timeout=1.0)
                if disk_thread.ident is not None:
                    disk_thread.join(timeout=1.0)
                encode_pool.shutdown(wait=False, cancel_futures=True)
                try:
                    encode_out_q.cancel_join_thread()
                except Exception:
                    pass
                if repair_pool is not None:
                    repair_pool.shutdown(wait=False, cancel_futures=True)
                mm.close()
    finally:
        stop_fb.set()
        fb_thread.join(timeout=1.0)

    elapsed = time.monotonic() - t0
    goodput = file_size / max(elapsed, 1e-6) / (1024 * 1024)
    with fb_lock:
        completed = fb_state["completed"]
        nn_done = int(fb_state["next_needed"])
    for gid in list(repair_rounds):
        if gid < nn_done:
            note_close_round(repair_close_hist, repair_rounds.pop(gid))
    print(
        f"done in {elapsed:.2f}s — goodput {goodput:.2f} MiB/s — "
        f"gens_sent={gens_sent}/{total_gens} client_done={completed} "
        f"repair_pkts={repair_sent} "
        f"{format_close_rounds(repair_close_hist, label='repair_close')}"
    )
    try:
        print(bench_encode(n=8, warmup=1).format_line("after"), flush=True)
    except Exception as exc:  # pragma: no cover
        print(f"encbench after failed: {exc}", file=sys.stderr)
    snap = host_cpu.sample()
    if snap is not None and snap.available:
        print(snap.format_line(), flush=True)
    return 0


def run_gen_client(
    host: str,
    port: int,
    output: Path,
    meta: MetaPacket,
    sock: socket.socket,
    server: tuple[str, int],
) -> int:
    require_raptorq()
    symbol_size = meta.gen_symbol_size or meta.payload_size or 1350
    gen_k = meta.gen_k or 48
    overhead_pct = meta.gen_overhead_pct
    block_bytes = gen_k * symbol_size
    file_size = meta.file_size
    total_gens = (file_size + block_bytes - 1) // block_bytes if file_size else 0
    inflight_gen_limit = compute_inflight_gen_limit(gen_k, symbol_size)
    feedback_horizon = client_feedback_horizon(inflight_gen_limit)
    holdoff_s = reorder_holdoff_s()

    print(
        f"gen-xfer: gens={total_gens} T={symbol_size} K={gen_k} "
        f"block={block_bytes} overhead={overhead_pct}% "
        f"disk_spool={'ram' if disk_spool_enabled() else 'off'} "
        f"reorder_holdoff_ms={holdoff_s * 1000:.0f}"
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    out_path = output
    if out_path.is_dir():
        out_path = out_path / meta.file_name

    out_path.touch(exist_ok=True)
    fd = os.open(out_path, os.O_RDWR)
    try:
        os.ftruncate(fd, file_size)
    except OSError:
        pass

    use_disk_spool = disk_spool_enabled()
    spool_dir = out_path.parent / f".{out_path.name}.spool"

    decoders: dict[int, GenDecoder] = {}
    slots: dict[int, GenReceiveSlot] = {}
    active_gens = slots if use_disk_spool else decoders
    # Decoded but not yet on disk: late packets must not reopen the generation.
    pending_write: set[int] = set()
    # First time we observed an incomplete gen (reorder holdoff clock).
    deficit_since: dict[int, float] = {}
    nack_since: dict[int, float] = {}
    nack_close_hist = [0, 0, 0, 0, 0]
    done_bits = bytearray((total_gens + 7) // 8) if total_gens else bytearray()
    completed = 0
    next_needed_hint = 0
    max_gid_seen = -1
    gens_recovered = 0
    last_echo = 0
    fin_seen = False
    drain_epoch = 0
    t0 = time.monotonic()
    last_progress = t0
    last_fb = 0.0
    bytes_written = 0
    rx_pkts = 0
    skip_done = 0
    dup_esi = 0
    wait_rx_s = 0.0
    recv_s = 0.0
    dec_s = 0.0
    write_s = 0.0
    write_inline = 0
    rx_bytes = 0
    blast_loss = BlastLossTracker()
    # Async pwrite; protocol treats the gen as done as soon as it decodes.
    write_q: queue.Queue[tuple[int, int, bytes] | None] = queue.Queue()
    write_lock = threading.Lock()
    write_fail: dict[str, OSError | None] = {"err": None}
    _fadv_dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)

    def bit_get(i: int) -> bool:
        if i < 0 or i >= total_gens:
            return True
        return bool(done_bits[i >> 3] & (1 << (i & 7)))

    def bit_set(i: int) -> None:
        nonlocal completed, next_needed_hint
        if i < 0 or i >= total_gens or bit_get(i):
            return
        done_bits[i >> 3] |= 1 << (i & 7)
        completed += 1
        clear_gen_deficit(deficit_since, i)
        first = nack_since.pop(i, None)
        if first is not None:
            rtt = echo_rtt_s(last_echo, time.monotonic())
            note_close_round(
                nack_close_hist,
                nack_close_rounds(time.monotonic() - first, rtt),
            )
        if i == next_needed_hint:
            while next_needed_hint < total_gens and bit_get(next_needed_hint):
                clear_gen_deficit(deficit_since, next_needed_hint)
                next_needed_hint += 1

    def pwrite_one(off: int, data: bytes) -> None:
        nonlocal bytes_written, write_s
        if off >= file_size:
            return
        chunk = data[: max(0, file_size - off)]
        t_w = time.monotonic()
        os.pwrite(fd, chunk, off)
        dt = time.monotonic() - t_w
        if _fadv_dontneed is not None:
            try:
                os.posix_fadvise(fd, off, len(chunk), _fadv_dontneed)
            except OSError:
                pass
        with write_lock:
            write_s += dt
            bytes_written += len(chunk)

    def write_loop() -> None:
        while True:
            item = write_q.get()
            if item is None:
                break
            gid, off, data = item
            try:
                pwrite_one(off, data)
            except OSError as e:
                write_fail["err"] = e
                print(f"disk write failed gen={gid}: {e}", file=sys.stderr)
            finally:
                pending_write.discard(gid)

    def enqueue_write(gid: int, off: int, data: bytes) -> None:
        pending_write.add(gid)
        bit_set(gid)
        write_q.put_nowait((gid, off, data))

    write_thread = threading.Thread(target=write_loop, name="gen-write", daemon=True)
    write_thread.start()

    def send_fb() -> None:
        nonlocal last_fb, drain_epoch
        now_fb = time.monotonic()
        next_needed = next_needed_hint
        # Later gens imply a hole/reorder on the frontier.
        if next_needed < total_gens and max_gid_seen > next_needed:
            note_gen_deficit(deficit_since, next_needed, now_fb)
            for gid in list(active_gens):
                if gid > next_needed and not bit_get(gid):
                    note_gen_deficit(deficit_since, gid, now_fb)
        if fin_seen:
            for gid in list(active_gens):
                if not bit_get(gid):
                    note_gen_deficit(deficit_since, gid, now_fb)
        nacks: list[int] = []
        nack_rx: list[int] = []
        open_rx: dict[int, int] = {}

        def aged(gid: int) -> bool:
            return repair_holdoff_ready(
                deficit_since, gid, now_fb, holdoff_s=holdoff_s
            )

        if next_needed < total_gens and (fin_seen or aged(next_needed)):
            slot = active_gens.get(next_needed)
            open_rx[next_needed] = slot.symbols_rx if slot is not None else 0
        for i, slot in active_gens.items():
            if i in open_rx or bit_get(i) or (not fin_seen and not aged(i)):
                continue
            open_rx[i] = slot.symbols_rx
        if fin_seen and next_needed < total_gens:
            hi = min(total_gens, next_needed + _DRAIN_WINDOW)
            for gid in range(next_needed, hi):
                if gid in open_rx or bit_get(gid) or gid in pending_write:
                    continue
                slot = active_gens.get(gid)
                open_rx[gid] = slot.symbols_rx if slot is not None else 0
        for gid in select_repair_feedback_gens(
            next_needed,
            open_rx,
            gen_k=gen_k,
            limit=64,
        ):
            nacks.append(gid)
            nack_rx.append(open_rx[gid])
            nack_since.setdefault(gid, now_fb)
        horizon = feedback_horizon
        if fin_seen:
            remain = max(0, total_gens - next_needed)
            horizon = min(FEEDBACK_BITMAP_MAX_BYTES * 8, max(feedback_horizon, remain))
        miss_bitmap = build_feedback_miss_bitmap(
            next_needed=next_needed,
            total_gens=total_gens,
            horizon=horizon,
            bit_get=bit_get,
            decoders=active_gens,
            max_gid_seen=max_gid_seen,
            include_gid=None if fin_seen else aged,
            include_unseen=fin_seen,
        )
        hol_miss_esi: list[int] | None = None
        if next_needed < total_gens and (fin_seen or aged(next_needed)):
            slot = active_gens.get(next_needed)
            if slot is not None:
                miss = slot.missing_source_esi(gen_k, limit=32)
                if miss:
                    hol_miss_esi = miss
        epoch = 0
        never_seen = None
        if fin_seen:
            drain_epoch = drain_epoch + 1
            if drain_epoch > 0xFFFF:
                drain_epoch = 1
            epoch = drain_epoch
            never_seen = drain_never_seen_frontier(
                next_needed, max_gid_seen, total_gens
            )
        pending = blast_loss.mature(now_fb)
        blast_loss.adapt_grace()
        loss_rep = blast_loss.report(pending)
        pkt = GenFeedbackPacket(
            next_needed,
            nacks,
            last_echo,
            completed,
            nack_rx,
            hol_miss_esi,
            miss_bitmap,
            epoch,
            never_seen,
            int(loss_rep["epoch"]),
            int(loss_rep["seq_begin"]),
            int(loss_rep["seq_end"]),
            int(loss_rep["rx"]),
            int(loss_rep["lost"]),
            int(loss_rep["late"]),
            int(loss_rep["pending"]),
        )
        sock.sendto(pkt.pack(), server)
        if fin_seen and completed >= total_gens:
            sock.sendto(FinPacket(True, total_gens).pack(), server)
        last_fb = now_fb

    try:
        while True:
            if write_fail["err"] is not None:
                print(f"abort: {write_fail['err']}", file=sys.stderr)
                return 1
            timeout = 0.02 if fin_seen else 0.05
            t_sel = time.monotonic()
            r, _, _ = select.select([sock], [], [], timeout)
            now = time.monotonic()
            fb_every = client_feedback_interval(
                fin_seen=fin_seen,
                next_needed=next_needed_hint,
                total_gens=total_gens,
                open_gens=len(active_gens),
                nack_count=len(active_gens),
            )
            if not r:
                wait_rx_s += now - t_sel
                send_fb()
                if fin_seen and completed >= total_gens:
                    break
                if now - t0 > 3600:
                    print("transfer timeout", file=sys.stderr)
                    return 1
                continue

            while True:
                try:
                    t_r = time.monotonic()
                    batch = recv_datagrams(sock, 64)
                    recv_s += time.monotonic() - t_r
                except BlockingIOError:
                    break
                for data in batch:
                    if len(data) < 4 or data[0] != MAGIC:
                        continue
                    ptype = data[2]
                    if ptype == PKT_GEN:
                        try:
                            gp = GenPacket.unpack(data)
                        except ValueError:
                            continue
                        rx_pkts += 1
                        rx_bytes += len(data)
                        last_echo = gp.send_ts_us
                        blast_loss.on_packet(gp.blast_seq, time.monotonic())
                        gid = gp.gen_id
                        max_gid_seen = max(max_gid_seen, gid)
                        if bit_get(gid) or gid in pending_write:
                            skip_done += 1
                            continue
                        rem = file_size - gid * block_bytes
                        if rem <= 0:
                            bit_set(gid)
                            continue
                        tlen = min(block_bytes, rem)
                        if use_disk_spool:
                            slot = slots.get(gid)
                            if slot is None:
                                slot = GenReceiveSlot(
                                    gid,
                                    gen_k=gen_k,
                                    symbol_size=symbol_size,
                                    block_bytes=block_bytes,
                                    tlen=tlen,
                                    spool_dir=spool_dir,
                                )
                                slots[gid] = slot
                            t_dec = time.monotonic()
                            dups0 = slot.dup_esi
                            out = slot.add_packet(gp.payload, gp.esi)
                            dec_s += time.monotonic() - t_dec
                            dup_esi += slot.dup_esi - dups0
                            if out is not None:
                                if slot.symbols_rx > gen_k + 1:
                                    gens_recovered += 1
                                slots.pop(gid, None)
                                slot.close()
                                clear_gen_deficit(deficit_since, gid)
                                enqueue_write(gid, gid * block_bytes, out[:tlen])
                            else:
                                now_rx = time.monotonic()
                                if gid > next_needed_hint:
                                    note_gen_deficit(
                                        deficit_since, next_needed_hint, now_rx
                                    )
                                    note_gen_deficit(deficit_since, gid, now_rx)
                                else:
                                    hole = slot.missing_source_esi(gen_k, limit=1)
                                    if hole and gp.esi > hole[0]:
                                        note_gen_deficit(deficit_since, gid, now_rx)
                        else:
                            dec = decoders.get(gid)
                            if dec is None:
                                dec = GenDecoder(block_bytes, symbol_size)
                                decoders[gid] = dec
                            t_dec = time.monotonic()
                            dups0 = dec.dup_esi
                            out = dec.add_packet(gp.payload, gp.esi)
                            dec_s += time.monotonic() - t_dec
                            dup_esi += dec.dup_esi - dups0
                            if out is not None:
                                if dec.symbols_rx > gen_k + 1:
                                    gens_recovered += 1
                                decoders.pop(gid, None)
                                clear_gen_deficit(deficit_since, gid)
                                enqueue_write(gid, gid * block_bytes, out[:tlen])
                            else:
                                now_rx = time.monotonic()
                                if gid > next_needed_hint:
                                    note_gen_deficit(
                                        deficit_since, next_needed_hint, now_rx
                                    )
                                    note_gen_deficit(deficit_since, gid, now_rx)
                                else:
                                    hole = dec.missing_source_esi(gen_k, limit=1)
                                    if hole and gp.esi > hole[0]:
                                        note_gen_deficit(deficit_since, gid, now_rx)
                    elif ptype == PKT_FIN:
                        fin_seen = True
                    elif ptype == PKT_META:
                        continue

            if now - last_fb > fb_every:
                send_fb()

            if now - last_progress >= 1.0:
                dt = now - last_progress
                last_progress = now
                pct = 100.0 * completed / total_gens if total_gens else 100.0
                rate = (
                    completed * block_bytes / max(now - t0, 1e-6) / (1024 * 1024)
                )
                rx_mib = rx_bytes / dt / (1024 * 1024)
                parts = {
                    "wait_rx": wait_rx_s,
                    "recv": recv_s,
                    "dec": dec_s,
                    "write": write_s,
                }
                bottleneck = max(parts, key=parts.get)
                print(
                    f"progress {completed}/{total_gens} ({pct:.1f}%) "
                    f"gens_recovered≈{gens_recovered} "
                    f"open_gens={len(active_gens)} "
                    f"{format_close_rounds(nack_close_hist, label='nack_rtt')} "
                    f"{rate:.1f} MiB/s"
                )
                print(
                    f"  xfer rx={rx_mib:.1f} pkts={rx_pkts} "
                    f"skip_done={skip_done} dup_esi={dup_esi} "
                    f"loss_rx={blast_loss.rx_unique} loss_lost={blast_loss.lost} "
                    f"late={blast_loss.late} "
                    f"wait_rx={_pct(wait_rx_s, dt)}% "
                    f"recv={_pct(recv_s, dt)}% "
                    f"dec={_pct(dec_s, dt)}% "
                    f"write={_pct(write_s, dt)}% "
                    f"wq={write_q.qsize()} winl={write_inline} "
                    f"limit={bottleneck}"
                )
                rx_pkts = skip_done = dup_esi = write_inline = 0
                wait_rx_s = recv_s = dec_s = 0.0
                with write_lock:
                    write_s = 0.0
                rx_bytes = 0

            if completed >= total_gens:
                break
    finally:
        write_q.put(None)
        write_thread.join(timeout=60.0)
        os.close(fd)
        if use_disk_spool:
            for slot in slots.values():
                slot.close()
            shutil.rmtree(spool_dir, ignore_errors=True)
        for _ in range(5):
            sock.sendto(FinPacket(True, total_gens).pack(), server)
            time.sleep(0.02)

    elapsed = time.monotonic() - t0
    ok = completed >= total_gens
    goodput = file_size / max(elapsed, 1e-6) / (1024 * 1024)
    print(
        f"{'OK' if ok else 'FAIL'}: wrote {out_path} ({file_size} bytes) "
        f"in {elapsed:.2f}s ({goodput:.2f} MiB/s)"
    )
    print(
        f"stats: gens_done={completed}/{total_gens} "
        f"gens_recovered≈{gens_recovered} "
        f"{format_close_rounds(nack_close_hist, label='nack_rtt')}"
    )
    return 0 if ok else 2
