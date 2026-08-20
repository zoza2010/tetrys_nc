"""RaptorQ helpers for generation-based transfer."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path


def require_raptorq():
    try:
        from raptorq import Decoder, Encoder  # type: ignore
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "raptorq is required (pip/uv install 'raptorq>=2')"
        ) from e
    return Encoder, Decoder


def repair_count(k: int, overhead_pct: int) -> int:
    """Extra repair packets beyond systematic (~overhead_pct of K). 0 = none."""
    if k <= 0 or overhead_pct <= 0:
        return 0
    r = int(math.ceil(k * overhead_pct / 100.0))
    return max(1, r)


def blast_repair_budget(k: int, overhead_pct: int) -> int:
    """Repair symbols bundled into the initial systematic blast."""
    if k <= 0 or overhead_pct <= 0:
        return 0
    return repair_count(k, overhead_pct)


def fountain_blast_budget(k: int, target_pct: int = 8) -> int:
    """Per-gen repair bundled with systematic when overhead_pct=0 (fountain mode)."""
    if k <= 0 or target_pct <= 0:
        return 0
    return repair_count(k, target_pct)


@dataclass
class GenEncoder:
    """Encode one file generation; can emit more repair on demand."""

    data: bytes
    symbol_size: int
    overhead_pct: int
    systematic_only: bool = False
    _encoder: object = field(init=False, repr=False)
    _packets: list[bytes] = field(default_factory=list, repr=False)
    _repair_budget: int = 0

    def __post_init__(self) -> None:
        Encoder, _ = require_raptorq()
        self._encoder = Encoder.with_defaults(self.data, self.symbol_size)
        k_est = max(1, (len(self.data) + self.symbol_size - 1) // self.symbol_size)
        if self.systematic_only:
            self._repair_budget = 0
        else:
            self._repair_budget = blast_repair_budget(k_est, self.overhead_pct)
        self._packets = list(self._encoder.get_encoded_packets(self._repair_budget))

    @property
    def packet_count(self) -> int:
        return len(self._packets)

    def packets(self) -> list[bytes]:
        return self._packets

    @property
    def repair_budget(self) -> int:
        return self._repair_budget

    def ensure_repair(self, min_total_repair: int) -> list[bytes]:
        """Grow repair budget; return only newly added serialized packets."""
        if min_total_repair <= self._repair_budget:
            return []
        prev = len(self._packets)
        self._repair_budget = min_total_repair
        # Prefix of get_encoded_packets(n) is stable as n grows.
        self._packets = list(self._encoder.get_encoded_packets(self._repair_budget))
        return self._packets[prev:]


@dataclass
class GenDecoder:
    """Decode one generation from serialized EncodingPackets."""

    transfer_length: int
    symbol_size: int
    _decoder: object = field(init=False, repr=False)
    done: bytes | None = None
    symbols_rx: int = 0
    dup_esi: int = 0
    _seen_esi: set[int] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        _, Decoder = require_raptorq()
        self._decoder = Decoder.with_defaults(self.transfer_length, self.symbol_size)

    def add_packet(self, rq_blob: bytes, esi: int | None = None) -> bytes | None:
        if self.done is not None:
            return self.done
        if esi is not None:
            if esi in self._seen_esi:
                self.dup_esi += 1
                return None
            self._seen_esi.add(esi)
        self.symbols_rx += 1
        out = self._decoder.decode(rq_blob)
        if out is not None:
            self.done = bytes(out)
        return self.done

    def missing_source_esi(self, source_k: int, *, limit: int = 24) -> list[int]:
        """Systematic ESIs in [0, source_k) not yet received."""
        if self.done is not None or source_k <= 0:
            return []
        out: list[int] = []
        for esi in range(source_k):
            if esi not in self._seen_esi:
                out.append(esi)
                if len(out) >= limit:
                    break
        return out


def disk_spool_enabled() -> bool:
    """Legacy name: keep GenReceiveSlot (RAM-buffered) path; disk I/O is off."""
    return os.environ.get("TETRYS_DISK_SPOOL", "1").lower() not in (
        "0",
        "false",
        "no",
    )


def _source_full_mask(gen_k: int) -> int:
    if gen_k <= 0:
        return 0
    return (1 << gen_k) - 1


class GenReceiveSlot:
    """Generation receive with in-memory packet buffer.

    Disk spool was the decode bottleneck (flush/seek/replay per gen). Packets
    stay in RAM; open gens are already bounded by the server inflight window.
    """

    __slots__ = (
        "gid",
        "gen_k",
        "symbol_size",
        "block_bytes",
        "tlen",
        "symbols_rx",
        "dup_esi",
        "_seen",
        "_source_mask",
        "_has_repair",
        "_decoder",
        "_decoder_ready",
        "_pkts",
    )

    def __init__(
        self,
        gid: int,
        *,
        gen_k: int,
        symbol_size: int,
        block_bytes: int,
        tlen: int,
        spool_dir: Path | None = None,
    ) -> None:
        del spool_dir  # API compat; no longer used
        self.gid = gid
        self.gen_k = gen_k
        self.symbol_size = symbol_size
        self.block_bytes = block_bytes
        self.tlen = tlen
        self.symbols_rx = 0
        self.dup_esi = 0
        self._seen: set[int] = set()
        self._source_mask = 0
        self._has_repair = False
        self._decoder: GenDecoder | None = None
        self._decoder_ready = False
        self._pkts: list[tuple[int, bytes]] = []

    def close(self) -> None:
        self._pkts.clear()
        self._decoder = None
        self._decoder_ready = False

    def missing_source_esi(self, source_k: int, *, limit: int = 24) -> list[int]:
        if source_k <= 0:
            return []
        if self._decoder is not None and self._decoder.done is None:
            return self._decoder.missing_source_esi(source_k, limit=limit)
        out: list[int] = []
        for esi in range(source_k):
            if not (self._source_mask & (1 << esi)):
                out.append(esi)
                if len(out) >= limit:
                    break
        return out

    def add_packet(self, rq_blob: bytes, esi: int) -> bytes | None:
        if esi in self._seen:
            self.dup_esi += 1
            return None
        self._seen.add(esi)
        self.symbols_rx += 1
        self._pkts.append((esi, rq_blob))
        if esi < self.gen_k:
            self._source_mask |= 1 << esi
        else:
            self._has_repair = True

        full = _source_full_mask(self.gen_k)
        if self._source_mask == full and not self._has_repair:
            return self._finalize_all_systematic()

        if self._has_repair:
            if not self._decoder_ready:
                self._build_decoder_from_memory()
                self._decoder_ready = True
            else:
                assert self._decoder is not None
                out = self._decoder.add_packet(rq_blob, esi)
                if out is not None:
                    return out[: self.tlen]
            if self._decoder is not None and self._decoder.done is not None:
                return self._decoder.done[: self.tlen]
        return None

    def _finalize_all_systematic(self) -> bytes | None:
        dec = GenDecoder(self.block_bytes, self.symbol_size)
        for esi, blob in self._pkts:
            if esi < self.gen_k:
                dec.add_packet(blob, esi)
        if dec.done is None:
            return None
        return dec.done[: self.tlen]

    def _build_decoder_from_memory(self) -> None:
        dec = GenDecoder(self.block_bytes, self.symbol_size)
        for esi, blob in self._pkts:
            dec.add_packet(blob, esi)
        self._decoder = dec
