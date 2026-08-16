"""RaptorQ helpers for generation-based transfer (optional `raptorq` package)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field


def require_raptorq():
    try:
        from raptorq import Decoder, Encoder  # type: ignore
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "raptorq is required for --xfer gen (pip/uv install 'raptorq>=2')"
        ) from e
    return Encoder, Decoder


def repair_count(k: int, overhead_pct: int) -> int:
    """Number of repair packets for ~overhead_pct of K (at least 1 if K>0)."""
    if k <= 0:
        return 0
    r = int(math.ceil(k * max(0, overhead_pct) / 100.0))
    return max(1, r)


@dataclass
class GenEncoder:
    """Encode one file generation; can emit more repair on demand."""

    data: bytes
    symbol_size: int
    overhead_pct: int
    _encoder: object = field(init=False, repr=False)
    _packets: list[bytes] = field(default_factory=list, repr=False)
    _repair_budget: int = 0

    def __post_init__(self) -> None:
        Encoder, _ = require_raptorq()
        self._encoder = Encoder.with_defaults(self.data, self.symbol_size)
        # Initial systematic + repair. Packet count ≈ K + R.
        k_est = max(1, (len(self.data) + self.symbol_size - 1) // self.symbol_size)
        self._repair_budget = repair_count(k_est, self.overhead_pct)
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

    def __post_init__(self) -> None:
        _, Decoder = require_raptorq()
        self._decoder = Decoder.with_defaults(self.transfer_length, self.symbol_size)

    def add_packet(self, rq_blob: bytes) -> bytes | None:
        if self.done is not None:
            return self.done
        self.symbols_rx += 1
        out = self._decoder.decode(rq_blob)
        if out is not None:
            self.done = bytes(out)
        return self.done
