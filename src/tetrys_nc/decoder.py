"""Tetrys decoder with Gaussian elimination over GF(256)."""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field

from . import gf256
from .packets import CodedPacket, WindowUpdatePacket


@dataclass(slots=True)
class DecoderConfig:
    max_decode_window: int = 4096
    feedback_every_packets: int = 256
    delivered_cache: int = 4096
    # Don't treat gaps as loss until this old (reorder hold). 0 = immediate.
    reorder_hold_s: float = 0.0


@dataclass
class TetrysDecoder:
    """Systematic Tetrys decoder with in-order delivery."""

    cfg: DecoderConfig = field(default_factory=DecoderConfig)
    _symbols: dict[int, bytes] = field(default_factory=dict)
    _delivered: OrderedDict[int, bytes] = field(default_factory=OrderedDict)
    next_deliver: int = 0
    _equations: list[tuple[dict[int, int], bytearray]] = field(default_factory=list)
    _packets_since_feedback: int = 0
    _total_source_rx: int = 0
    _total_coded_rx: int = 0
    _total_recovered: int = 0
    # Highest source id observed (received or referenced by a coded packet)
    highest_seen: int = -1
    total_symbols: int | None = None
    payload_size: int = 32768
    # sid -> monotonic time when gap was first observed (reorder vs loss)
    _gap_open_at: dict[int, float] = field(default_factory=dict)
    # GF decode cost for progress lines (reset by take_decode_stats).
    _dec_ns: int = 0
    _dec_n: int = 0

    def _known(self, sid: int) -> bytes | None:
        got = self._symbols.get(sid)
        if got is not None:
            return got
        return self._delivered.get(sid)

    def _note_seen(self, sid: int) -> None:
        now = time.monotonic()
        if sid > self.highest_seen:
            # Ids between old high-water and new sid may be reorder gaps.
            start = max(self.next_deliver, self.highest_seen + 1)
            for g in range(start, sid):
                if g not in self._symbols:
                    self._gap_open_at.setdefault(g, now)
            self.highest_seen = sid

    def _mark_gap_range(self) -> None:
        """Ensure open HOL gaps have timestamps (for PLR / NACK aging)."""
        if self.highest_seen < self.next_deliver:
            return
        now = time.monotonic()
        for g in range(self.next_deliver, self.highest_seen + 1):
            if g not in self._symbols:
                self._gap_open_at.setdefault(g, now)

    def on_source_raw(self, sid: int, payload: bytes | memoryview | bytearray) -> list[tuple[int, bytes]]:
        """Hot path entry without allocating SourcePacket."""
        self._packets_since_feedback += 1
        self._total_source_rx += 1
        self._note_seen(sid)
        self._gap_open_at.pop(sid, None)
        if sid < self.next_deliver or sid in self._symbols:
            return []

        # Own the bytes — callers may pass a view into a reused buffer.
        payload_b = payload if isinstance(payload, bytes) else bytes(payload)

        # Lossless in-order path: no cache, no equations
        if sid == self.next_deliver and not self._equations and not self._symbols:
            self.next_deliver = sid + 1
            return [(sid, payload_b)]

        self._symbols[sid] = payload_b
        if self._equations:
            t0 = time.perf_counter_ns()
            self._reduce_equations_with(sid, payload_b)
            self._solve()
            self._dec_ns += time.perf_counter_ns() - t0
            self._dec_n += 1
        return self.pop_deliverable()

    def on_coded(self, pkt: CodedPacket) -> list[tuple[int, bytes]]:
        self._packets_since_feedback += 1
        self._total_coded_rx += 1
        self._note_seen(pkt.last_source_id)

        t0 = time.perf_counter_ns()
        coefs: dict[int, int] = {}
        payload = bytearray(pkt.payload)
        for sid in range(pkt.first_source_id, pkt.last_source_id + 1):
            coef = gf256.vandermonde_coef(sid + 1, pkt.coded_id)
            if coef == 0:
                continue
            known = self._known(sid)
            if known is not None:
                gf256.mul_bytes(coef, known, payload)
            elif sid < self.next_deliver:
                # Need delivered cache for late coded packets
                self._dec_ns += time.perf_counter_ns() - t0
                self._dec_n += 1
                return []
            else:
                coefs[sid] = coef

        if not coefs:
            self._dec_ns += time.perf_counter_ns() - t0
            self._dec_n += 1
            return []

        self._equations.append((coefs, payload))
        if len(self._equations) > self.cfg.max_decode_window:
            self._equations = self._equations[-self.cfg.max_decode_window :]
        self._solve()
        self._dec_ns += time.perf_counter_ns() - t0
        self._dec_n += 1
        return self.pop_deliverable()

    def take_decode_stats(self) -> tuple[float, int, float]:
        """Decode CPU since last call: (ms total, op count, µs/op)."""
        ns, n = self._dec_ns, self._dec_n
        self._dec_ns = 0
        self._dec_n = 0
        ms = ns / 1e6
        us = (ns / n / 1e3) if n else 0.0
        return ms, n, us

    def _cache_delivered(self, sid: int, data: bytes) -> None:
        self._delivered[sid] = data
        while len(self._delivered) > self.cfg.delivered_cache:
            self._delivered.popitem(last=False)

    def _reduce_equations_with(self, sid: int, data: bytes) -> None:
        remaining: list[tuple[dict[int, int], bytearray]] = []
        for coefs, payload in self._equations:
            if sid in coefs:
                gf256.mul_bytes(coefs.pop(sid), data, payload)
            if coefs:
                remaining.append((coefs, payload))
        self._equations = remaining

    def _solve(self) -> None:
        for _ in range(64):
            progress = False
            still: list[tuple[dict[int, int], bytearray]] = []
            for coefs, payload in self._equations:
                for sid in list(coefs.keys()):
                    known = self._known(sid)
                    if known is not None:
                        gf256.mul_bytes(coefs.pop(sid), known, payload)
                if not coefs:
                    continue
                if len(coefs) == 1:
                    sid, coef = next(iter(coefs.items()))
                    if coef != 0 and self._known(sid) is None:
                        self._symbols[sid] = bytes(
                            gf256.scale_bytes(gf256.inv(coef), payload)
                        )
                        self._gap_open_at.pop(sid, None)
                        self._total_recovered += 1
                        progress = True
                    continue
                still.append((coefs, payload))
            self._equations = still
            if progress:
                continue

            if len(self._equations) < 2:
                break
            self._equations.sort(key=lambda e: (len(e[0]), min(e[0])))
            n = len(self._equations)
            changed = False
            for i in range(n):
                if not self._equations[i][0]:
                    continue
                pivot = min(self._equations[i][0])
                c_i = self._equations[i][0][pivot]
                for j in range(i + 1, n):
                    if pivot not in self._equations[j][0]:
                        continue
                    factor = gf256.div(self._equations[j][0][pivot], c_i)
                    new_coefs = dict(self._equations[j][0])
                    for sid, c in self._equations[i][0].items():
                        v = gf256.add(new_coefs.get(sid, 0), gf256.mul(factor, c))
                        if v == 0:
                            new_coefs.pop(sid, None)
                        else:
                            new_coefs[sid] = v
                    scaled = gf256.scale_bytes(factor, self._equations[i][1])
                    new_payload = bytearray(self._equations[j][1])
                    gf256.xor_bytes(new_payload, scaled)
                    self._equations[j] = (new_coefs, new_payload)
                    changed = True
            self._equations = [e for e in self._equations if e[0]]
            if not changed:
                break

    def pop_deliverable(self) -> list[tuple[int, bytes]]:
        out: list[tuple[int, bytes]] = []
        while self.next_deliver in self._symbols:
            data = self._symbols.pop(self.next_deliver)
            self._gap_open_at.pop(self.next_deliver, None)
            # Cache only when coding/recovery is active
            if self._equations or self._total_recovered:
                self._cache_delivered(self.next_deliver, data)
            out.append((self.next_deliver, data))
            self.next_deliver += 1
        return out

    def has_holes(self) -> bool:
        """True if we hold future symbols while waiting on next_deliver."""
        return bool(self._symbols) or bool(self._equations)

    def need_feedback(self) -> bool:
        hold = self.cfg.reorder_hold_s
        if self.has_holes():
            # With reorder hold: don't scream every 8 packets — let gaps age
            thresh = 64 if hold > 0 else 8
            if self._packets_since_feedback >= thresh:
                return True
        return self._packets_since_feedback >= self.cfg.feedback_every_packets

    def build_feedback(self, sack_bits: int = 256) -> WindowUpdatePacket:
        """
        SACK/PLR only cover symbols we have *evidence* were sent
        (up to highest_seen).

        With reorder_hold_s > 0: young gaps are reported as held so the sender
        does not NACK yet (iperf shows ~16% OOO on this path). After the hold,
        the bit stays clear → NACK/repair. True losses still repair after hold.
        """
        self._packets_since_feedback = 0
        self._mark_gap_range()
        now = time.monotonic()
        hold = self.cfg.reorder_hold_s
        base = self.next_deliver
        observed_end = max(self.highest_seen, base - 1)
        span = max(0, observed_end - base + 1)
        sack_bits = min(max(sack_bits, 0), 65535)
        span = min(span, sack_bits)
        if self.total_symbols is not None:
            span = min(span, max(0, self.total_symbols - base))

        n_bytes = (span + 7) // 8 if span > 0 else 0
        sack = bytearray(n_bytes)
        missing = 0
        aged_missing = 0
        for i in range(span):
            sid = base + i
            if sid in self._symbols:
                sack[i // 8] |= 1 << (i % 8)
            else:
                missing += 1
                t0 = self._gap_open_at.setdefault(sid, now)
                # Tip ages at half hold (~300ms @ 0.6) — match server first-send gate.
                # Old 0.25*hold (~150ms) screamed HOL loss under 30–40% OOO.
                if hold > 0 and i < 512:
                    hold_eff = min(hold, max(0.25, hold * 0.50))
                else:
                    hold_eff = hold
                if hold <= 0 or (now - t0) >= hold_eff:
                    aged_missing += 1
                # bit stays 0 — SACK accurate; sender ages NACK before repair

        # PLR from aged holes only (reorder looks like ~0% until hold expires)
        nack_count = aged_missing if hold > 0 else missing
        denom = max(span, 1)
        plr_pct = min(100.0, 100.0 * nack_count / denom) if span > 0 else 0.0
        plr_byte = int(plr_pct * 256 / 100)
        return WindowUpdatePacket(
            cumulative_ack=self.next_deliver,
            nb_missing_src=nack_count,
            nb_not_used_coded=len(self._equations),
            plr_byte=plr_byte,
            sack=bytes(sack),
            echo_ts_us=0,
            sack_span=span,
        )

    def is_complete(self) -> bool:
        if self.total_symbols is None:
            return False
        return self.next_deliver >= self.total_symbols

    def stats(self) -> dict:
        return {
            "next_deliver": self.next_deliver,
            "buffered": len(self._symbols),
            "equations": len(self._equations),
            "source_rx": self._total_source_rx,
            "coded_rx": self._total_coded_rx,
            "recovered": self._total_recovered,
            "gaps": len(self._gap_open_at),
        }
