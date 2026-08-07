"""Socket helpers resilient to OS buffer limits (esp. macOS errno 55)."""

from __future__ import annotations

import socket


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
