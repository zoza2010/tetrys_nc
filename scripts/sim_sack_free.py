"""Sim: SACK-freed (holey) encoder window must still emit decodable coded repair."""

from __future__ import annotations

import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tetrys_nc.decoder import DecoderConfig, TetrysDecoder  # noqa: E402
from tetrys_nc.encoder import EncoderConfig, TetrysEncoder  # noqa: E402
from tetrys_nc.packets import CodedPacket, SourcePacket, parse_packet  # noqa: E402

PAYLOAD = 1350
N = 4000
LOSS = 0.005


def main() -> int:
    rnd = random.Random(7)
    enc = TetrysEncoder(
        EncoderConfig(
            payload_size=PAYLOAD,
            max_window=20000,
            redundancy_every=32,
            code_degree=32,
            adaptive_fec=False,
        )
    )
    dec = TetrysDecoder(DecoderConfig(max_decode_window=20000), payload_size=PAYLOAD)

    truth: dict[int, bytes] = {}
    got: dict[int, bytes] = {}
    lost = 0

    def feed(wire: bytes) -> None:
        pkt = parse_packet(bytes(wire))
        if isinstance(pkt, SourcePacket):
            out = dec.on_source_raw(pkt.symbol_id, pkt.payload)
        elif isinstance(pkt, CodedPacket):
            out = dec.on_coded(pkt)
        else:
            return
        for sid, data in out:
            got[sid] = bytes(data)

    for i in range(N):
        payload = bytes([(i * 31 + j) & 0xFF for j in range(PAYLOAD)])
        truth[i] = payload
        wire = enc.add_source(payload)
        for pkt_bytes in [wire, *enc.maybe_coded()]:
            if rnd.random() < LOSS:
                lost += 1
                continue
            feed(pkt_bytes)

        # Feed back SACK every 64 packets, as the client does.
        if i % 64 == 63:
            fb = dec.build_feedback(sack_bits=65535)
            enc.apply_feedback(
                fb.cumulative_ack,
                fb.plr_byte,
                fb.missing_ids(limit=8192),
                fb.held_ids(limit=200_000),
            )
            # Holey window: coded repair over the oldest run must stay decodable.
            for pkt_bytes in enc.emit_repair_coded(limit=2):
                feed(pkt_bytes)

    st = dec.stats()
    bad = [sid for sid in sorted(got) if got[sid] != truth[sid]]
    missing = [sid for sid in range(dec.next_deliver) if sid not in got]

    print(f"lost={lost} delivered={dec.next_deliver}/{N} recovered={st['recovered']}")
    print(f"window_after={enc.window_size} corrupt={len(bad)} gaps={len(missing)}")
    if bad:
        print(f"FAIL corrupt symbols: {bad[:10]}")
        return 1
    if missing:
        print(f"FAIL undelivered below frontier: {missing[:10]}")
        return 1
    if enc.window_size > 512:
        print("FAIL window not freed by SACK")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
