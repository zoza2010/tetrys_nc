"""Gen RaptorQ UDP file client entrypoint."""

from __future__ import annotations

import argparse
import select
import socket
import sys
import time
from pathlib import Path

from .netutil import try_set_buffer
from .packets import MetaPacket, ReadyPacket, parse_packet


def run_client(host: str, port: int, output: Path, wan: bool = False) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try_set_buffer(sock, socket.SO_RCVBUF, 128 * 1024 * 1024)
    try_set_buffer(sock, socket.SO_SNDBUF, 8 * 1024 * 1024)
    sock.setblocking(False)
    server = (host, port)

    print(f"connecting to udp://{host}:{port}" + (" (wan buffers)" if wan else ""))
    # max_window is unused by gen path but kept on the wire for READY.
    ready = ReadyPacket(65536 if wan else 8192).pack()
    for _ in range(5):
        sock.sendto(ready, server)

    meta: MetaPacket | None = None
    t_wait = time.monotonic()
    while meta is None:
        if time.monotonic() - t_wait > 30:
            print("timeout waiting for META", file=sys.stderr)
            return 1
        r, _, _ = select.select([sock], [], [], 0.5)
        if not r:
            sock.sendto(ready, server)
            continue
        data, _addr = sock.recvfrom(65535)
        try:
            pkt = parse_packet(data)
        except ValueError:
            continue
        if isinstance(pkt, MetaPacket):
            meta = pkt
            print(
                f"META: name={meta.file_name} size={meta.file_size} "
                f"payload={meta.payload_size} sha256={meta.sha256_hex[:16] or 'n/a'}... "
                f"xfer=gen K={meta.gen_k}"
            )

    assert meta is not None
    from .gen_xfer import run_gen_client

    return run_gen_client(host, port, output, meta, sock, server)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gen RaptorQ UDP file client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--output", type=Path, default=Path("received.bin"))
    p.add_argument(
        "--wan",
        action="store_true",
        help="WAN: enlarge socket buffers",
    )
    args = p.parse_args(argv)
    return run_client(args.host, args.port, args.output, wan=args.wan)


if __name__ == "__main__":
    raise SystemExit(main())
