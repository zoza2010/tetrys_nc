#!/usr/bin/env python3
"""Minimal TCP file transfer for WAN goodput comparison with tetrys_nc UDP."""

from __future__ import annotations

import argparse
import hashlib
import socket
import struct
import sys
import time
from pathlib import Path

_HDR = struct.Struct("!Q")  # uint64 file size
_CHUNK = 1024 * 1024
_RCVBUF = 128 * 1024 * 1024
_SNDBUF = 8 * 1024 * 1024


def _set_buffers(sock: socket.socket) -> None:
    for opt, val in ((socket.SO_RCVBUF, _RCVBUF), (socket.SO_SNDBUF, _SNDBUF)):
        try:
            sock.setsockopt(socket.SOL_SOCKET, opt, val)
        except OSError:
            pass


def run_server(host: str, port: int, file_path: Path) -> int:
    if not file_path.is_file():
        print(f"file not found: {file_path}", file=sys.stderr)
        return 1
    size = file_path.stat().st_size
    ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    _set_buffers(ls)
    ls.bind((host, port))
    ls.listen(1)
    print(f"tcp-xfer server listening on tcp://{host}:{port}")
    print(f"file={file_path.name} size={size}")
    print("waiting for client...")
    conn, addr = ls.accept()
    with conn:
        _set_buffers(conn)
        print(f"client connected from {addr}")
        t0 = time.monotonic()
        conn.sendall(_HDR.pack(size))
        sent = 0
        with file_path.open("rb") as f:
            while sent < size:
                chunk = f.read(min(_CHUNK, size - sent))
                if not chunk:
                    break
                conn.sendall(chunk)
                sent += len(chunk)
        elapsed = time.monotonic() - t0
    ls.close()
    rate = sent / max(elapsed, 1e-6) / (1024 * 1024)
    print(
        f"done in {elapsed:.2f}s — goodput {rate:.2f} MiB/s — "
        f"bytes_sent={sent}/{size}"
    )
    return 0 if sent == size else 1


def run_client(host: str, port: int, output: Path, *, verify: bool) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _set_buffers(sock)
    print(f"connecting to tcp://{host}:{port}")
    t0 = time.monotonic()
    sock.connect((host, port))
    hdr = _recv_exact(sock, _HDR.size)
    size = _HDR.unpack(hdr)[0]
    print(f"META: size={size}")
    h = hashlib.sha256() if verify else None
    received = 0
    with output.open("wb") as out:
        while received < size:
            want = min(_CHUNK, size - received)
            chunk = _recv_exact(sock, want)
            out.write(chunk)
            if h is not None:
                h.update(chunk)
            received += len(chunk)
    sock.close()
    elapsed = time.monotonic() - t0
    rate = received / max(elapsed, 1e-6) / (1024 * 1024)
    digest = h.hexdigest() if h is not None else "n/a"
    print(
        f"OK: wrote {output} ({received} bytes) in {elapsed:.2f}s "
        f"({rate:.2f} MiB/s) sha256={digest[:16]}..."
    )
    return 0 if received == size else 1


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("connection closed early")
        buf.extend(part)
    return bytes(buf)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Simple TCP file transfer benchmark")
    sub = p.add_subparsers(dest="cmd", required=True)

    ps = sub.add_parser("server", help="send one file to the first connected client")
    ps.add_argument("--host", default="0.0.0.0")
    ps.add_argument("--port", type=int, default=7494)
    ps.add_argument("--file", type=Path, required=True)

    pc = sub.add_parser("client", help="receive one file")
    pc.add_argument("--host", default="127.0.0.1")
    pc.add_argument("--port", type=int, default=7494)
    pc.add_argument("--output", type=Path, required=True)
    pc.add_argument(
        "--verify",
        action="store_true",
        help="compute sha256 while receiving (slightly slower)",
    )

    args = p.parse_args(argv)
    if args.cmd == "server":
        return run_server(args.host, args.port, args.file)
    return run_client(args.host, args.port, args.output, verify=args.verify)


if __name__ == "__main__":
    raise SystemExit(main())
