"""Socket helpers resilient to OS buffer limits (esp. macOS errno 55)."""

from __future__ import annotations

import socket
from typing import Sequence


def try_set_buffer(sock: socket.socket, option: int, size: int) -> int:
    """
    Request a socket buffer size, falling back to smaller values on failure.
    Returns the size that was accepted (best effort), or 0 if all attempts failed.
    """
    for candidate in (size, 4 * 1024 * 1024, 1024 * 1024, 256 * 1024, 64 * 1024):
        if candidate > size:
            continue
        try:
            sock.setsockopt(socket.SOL_SOCKET, option, candidate)
            return candidate
        except OSError:
            continue
    return 0


def send_datagrams(
    sock: socket.socket,
    addr: tuple[str, int],
    buffers: Sequence[bytes | bytearray | memoryview],
    *,
    chunk: int = 256,
) -> int:
    """
    Send many UDP datagrams efficiently.

    Uses sendmmsg on Linux (one syscall per chunk); falls back to sendto elsewhere.
    Returns number of datagrams sent.
    """
    if not buffers:
        return 0
    sendmmsg = getattr(sock, "sendmmsg", None)
    sent = 0
    n = len(buffers)
    i = 0
    while i < n:
        end = min(i + chunk, n)
        batch = buffers[i:end]
        if sendmmsg is not None:
            # [(data, ancdata, flags, addr), ...]
            msgs = [(buf, [], 0, addr) for buf in batch]
            pos = 0
            while pos < len(msgs):
                try:
                    nsent = sendmmsg(msgs[pos:])
                    if nsent <= 0:
                        break
                    pos += nsent
                    sent += nsent
                except BlockingIOError:
                    import select

                    select.select([], [sock], [], 0.0005)
                except OSError:
                    # Fall back per-datagram for this chunk
                    for buf in batch[pos:]:
                        _sendto_retry(sock, buf, addr)
                        sent += 1
                    pos = len(msgs)
                    break
        else:
            for buf in batch:
                _sendto_retry(sock, buf, addr)
                sent += 1
        i = end
    return sent


def _sendto_retry(
    sock: socket.socket,
    buf: bytes | bytearray | memoryview,
    addr: tuple[str, int],
) -> None:
    import select

    while True:
        try:
            sock.sendto(buf, addr)
            return
        except BlockingIOError:
            select.select([], [sock], [], 0.0005)
