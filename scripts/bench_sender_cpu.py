"""Measure the sender's CPU ceiling: encode path vs socket path, no WAN."""

from __future__ import annotations

import os
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tetrys_nc.encoder import EncoderConfig, TetrysEncoder  # noqa: E402
from tetrys_nc.netutil import send_datagrams  # noqa: E402

PAYLOAD = 1350
N = 40000


def mibs(n_pkts: int, secs: float) -> float:
    return n_pkts * PAYLOAD / secs / (1024 * 1024)


def new_enc(redundancy: int) -> TetrysEncoder:
    return TetrysEncoder(
        EncoderConfig(
            payload_size=PAYLOAD,
            max_window=200000,
            redundancy_every=redundancy,
            code_degree=32,
            adaptive_fec=False,
        )
    )


def bench_source_only() -> float:
    enc = new_enc(0)
    chunk = bytes(PAYLOAD)
    t = time.perf_counter()
    for _ in range(N):
        enc.add_source(chunk)
    return time.perf_counter() - t


def bench_source_plus_coded() -> tuple[float, int]:
    enc = new_enc(32)
    chunk = bytes(PAYLOAD)
    coded = 0
    t = time.perf_counter()
    for _ in range(N):
        enc.add_source(chunk)
        coded += len(enc.maybe_coded())
    return time.perf_counter() - t, coded


def bench_coded_only(degree: int) -> float:
    enc = new_enc(0)
    chunk = bytes(PAYLOAD)
    for _ in range(degree + 64):
        enc.add_source(chunk)
    reps = 2000
    t = time.perf_counter()
    for _ in range(reps):
        enc.make_coded(prefer_oldest=False, degree=degree)
    return (time.perf_counter() - t) / reps


def bench_socket() -> float:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 << 20)
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sink.bind(("127.0.0.1", 0))
    sink.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
    addr = sink.getsockname()
    batch = [bytearray(PAYLOAD + 16) for _ in range(96)]
    sent = 0
    t = time.perf_counter()
    while sent < N:
        send_datagrams(sock, addr, batch)
        sent += len(batch)
    el = time.perf_counter() - t
    sock.close()
    sink.close()
    return el


def main() -> int:
    el_src = bench_source_only()
    print(f"add_source only:      {mibs(N, el_src):8.1f} MiB/s  ({el_src / N * 1e6:.2f} us/pkt)")

    el_mix, coded = bench_source_plus_coded()
    print(
        f"add_source + FEC 1/32:{mibs(N, el_mix):8.1f} MiB/s  "
        f"({el_mix / N * 1e6:.2f} us/pkt, {coded} coded)"
    )

    for degree in (8, 16, 32):
        us = bench_coded_only(degree) * 1e6
        print(f"make_coded degree={degree:<3d}  {us:8.1f} us/coded")

    el_sock = bench_socket()
    print(f"sendmmsg loopback:    {mibs(N, el_sock):8.1f} MiB/s  ({el_sock / N * 1e6:.2f} us/pkt)")

    total_us = (el_mix + el_sock) / N * 1e6
    print(f"\ncombined encode+send: {N * PAYLOAD / (el_mix + el_sock) / (1024 * 1024):8.1f} MiB/s"
          f"  ({total_us:.2f} us/pkt)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
