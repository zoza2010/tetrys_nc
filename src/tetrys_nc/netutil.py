"""Socket helpers resilient to OS buffer limits (esp. macOS errno 55)."""

from __future__ import annotations

import socket
import struct
from typing import Sequence

# Linux UDP GSO constants (linux/udp.h). Python does not expose sendmmsg on
# normal socket objects, but sendmsg + UDP_SEGMENT gives us the same syscall
# amortization and lets the kernel/NIC split one super-packet into datagrams.
_SOL_UDP = 17
_UDP_SEGMENT = 103
# Keep GSO bursts modest. A 64-segment super-packet at 1350 B creates an
# ~0.7ms wire burst; 16 amortizes syscalls without overwhelming shallow queues.
_GSO_MAX_SEGMENTS = 16
_UDP_MAX_PAYLOAD = 65535
_gso_by_fd: dict[int, bool] = {}


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
    if hasattr(sock, "sendmsg") and _gso_by_fd.get(sock.fileno(), True):
        try:
            return _send_gso(sock, addr, buffers)
        except (OSError, ValueError):
            # Unsupported kernel/qdisc or an unexpected packet shape. Remember
            # this fd and use the portable path for the rest of the session.
            _gso_by_fd[sock.fileno()] = False

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


def _send_gso(
    sock: socket.socket,
    addr: tuple[str, int],
    buffers: Sequence[bytes | bytearray | memoryview],
) -> int:
    """Send equal-sized consecutive datagrams using Linux UDP_SEGMENT."""
    sent = 0
    i = 0
    n = len(buffers)
    while i < n:
        seg_size = len(buffers[i])
        if seg_size <= 0 or seg_size > _UDP_MAX_PAYLOAD:
            raise ValueError("invalid UDP segment size")
        max_segments = min(_GSO_MAX_SEGMENTS, _UDP_MAX_PAYLOAD // seg_size)
        end = i + 1
        while (
            end < n
            and end - i < max_segments
            and len(buffers[end]) == seg_size
        ):
            end += 1
        if end - i == 1:
            _sendto_retry(sock, buffers[i], addr)
        else:
            payload = b"".join(buffers[i:end])
            ancillary = [(_SOL_UDP, _UDP_SEGMENT, struct.pack("H", seg_size))]
            while True:
                try:
                    written = sock.sendmsg([payload], ancillary, 0, addr)
                    if written != len(payload):
                        raise OSError("short UDP GSO send")
                    break
                except BlockingIOError:
                    import select

                    select.select([], [sock], [], 0.0005)
        sent += end - i
        i = end
    _gso_by_fd[sock.fileno()] = True
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
