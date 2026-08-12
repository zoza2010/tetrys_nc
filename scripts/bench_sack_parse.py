"""SACK parsing runs in the feedback thread and holds the GIL. How much does it cost?"""

from __future__ import annotations

import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tetrys_nc.packets import WindowUpdatePacket  # noqa: E402

REPS = 200


def make_pkt(span: int, loss: float) -> WindowUpdatePacket:
    rng = np.random.default_rng(3)
    bits = (rng.random(span) > loss).astype(np.uint8)
    sack = np.packbits(bits, bitorder="little").tobytes()
    return WindowUpdatePacket(
        cumulative_ack=1_000_000,
        nb_missing_src=int((bits == 0).sum()),
        nb_not_used_coded=0,
        plr_byte=0,
        sack=sack,
        echo_ts_us=0,
        sack_span=span,
    )


def np_held(pkt: WindowUpdatePacket) -> list[int]:
    span = pkt.sack_span
    bits = np.unpackbits(
        np.frombuffer(pkt.sack, dtype=np.uint8), count=span, bitorder="little"
    )
    return (np.flatnonzero(bits) + pkt.cumulative_ack).tolist()


def np_missing(pkt: WindowUpdatePacket) -> list[int]:
    span = pkt.sack_span
    bits = np.unpackbits(
        np.frombuffer(pkt.sack, dtype=np.uint8), count=span, bitorder="little"
    )
    return (np.flatnonzero(bits == 0) + pkt.cumulative_ack).tolist()


def timeit(name: str, fn) -> None:
    fn()
    t = time.perf_counter()
    for _ in range(REPS):
        fn()
    print(f"{name:<40s} {(time.perf_counter() - t) / REPS * 1000:8.3f} ms")


def main() -> int:
    for span in (2000, 17000):
        pkt = make_pkt(span, 0.004)
        print(f"\n--- span={span} bits, ~0.4% missing ---")
        assert np_held(pkt) == pkt.held_ids(limit=200_000)
        assert np_missing(pkt) == pkt.missing_ids(limit=8192)
        timeit("held_ids   (pure python bit loop)", lambda: pkt.held_ids(limit=200_000))
        timeit("missing_ids(pure python bit loop)", lambda: pkt.missing_ids(limit=8192))
        timeit("held_ids   (numpy unpackbits)", lambda: np_held(pkt))
        timeit("missing_ids(numpy unpackbits)", lambda: np_missing(pkt))

    # End-to-end feedback handling, as the server's feedback thread runs it.
    pkt = make_pkt(17000, 0.004)

    t = time.perf_counter()
    for _ in range(REPS):
        pkt.held_ids(limit=200_000)
        pkt.missing_ids(limit=8192)
    old = (time.perf_counter() - t) / REPS

    enc = build_encoder(pkt)
    t = time.perf_counter()
    for _ in range(REPS):
        enc.apply_feedback(
            pkt.cumulative_ack,
            pkt.plr_byte,
            pkt.missing_ids(limit=8192),
            sack=pkt.sack,
            sack_span=pkt.sack_span,
        )
    new = (time.perf_counter() - t) / REPS

    print(f"\nold: parse per feedback      {old * 1000:8.3f} ms"
          f"  -> {old * 500 * 100:5.0f}% of a core at 500 fb/s")
    print(f"new: parse + free per feedback{new * 1000:8.3f} ms"
          f"  -> {new * 500 * 100:5.0f}% of a core at 500 fb/s")
    print(f"window left: {enc.window_size}")
    return 0


def build_encoder(pkt: WindowUpdatePacket):
    """Encoder holding exactly the SACK range, so the first free does real work."""
    from tetrys_nc.encoder import EncoderConfig, TetrysEncoder

    enc = TetrysEncoder(
        EncoderConfig(payload_size=64, max_window=200000, redundancy_every=0)
    )
    enc._next_source_id = pkt.cumulative_ack
    for sid in range(pkt.cumulative_ack, pkt.cumulative_ack + pkt.sack_span):
        enc._window[sid] = b"\x00" * 64
    return enc


if __name__ == "__main__":
    raise SystemExit(main())
