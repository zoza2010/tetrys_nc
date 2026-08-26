"""Socket helpers resilient to OS buffer limits (esp. macOS errno 55)."""

from __future__ import annotations

import ctypes
import os
import socket
import struct
import sys
import time
from typing import Sequence

# Linux UDP GSO constants (linux/udp.h). Python does not expose sendmmsg on
# normal socket objects, but sendmsg + UDP_SEGMENT gives us the same syscall
# amortization and lets the kernel/NIC split one super-packet into datagrams.
_SOL_UDP = 17
_UDP_SEGMENT = 103
# Keep GSO bursts modest. A 64-segment super-packet at 1350 B creates an
# ~0.7ms wire burst; 16 amortizes syscalls without overwhelming shallow queues.
# TETRYS_GSO=0 disables GSO: under memory pressure (e.g. VMware balloon) the
# kernel's high-order contiguous allocation for a 22KB super-packet can take
# ~600us in reclaim/compaction while plain 1350B skbs stay fast.
# macOS/BSD accept sendmsg+UDP_SEGMENT without splitting, so default off there.
_DEFAULT_GSO = "32" if sys.platform.startswith("linux") else "0"
_GSO_MAX_SEGMENTS = int(os.environ.get("TETRYS_GSO", _DEFAULT_GSO) or "0")
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


_MSG_DONTWAIT = 0x40
_recv_mmsg: _RecvMmsg | None | bool = False

# Send-path introspection (single-writer from the send thread).
send_stats = {
    "syscall_s": 0.0,  # time inside sendmsg/sendto syscalls
    "block_s": 0.0,  # time waiting in select after BlockingIOError
    "calls": 0,  # syscall count
    "blocks": 0,  # BlockingIOError count
}


def take_send_stats() -> tuple[float, float, int, int]:
    out = (
        send_stats["syscall_s"],
        send_stats["block_s"],
        send_stats["calls"],
        send_stats["blocks"],
    )
    send_stats["syscall_s"] = 0.0
    send_stats["block_s"] = 0.0
    send_stats["calls"] = 0
    send_stats["blocks"] = 0
    return out


class _RecvMmsg:
    """Linux recvmmsg: many datagrams per syscall. Disabled if libc/layout fails."""

    def __init__(self, n: int = 64, bufsize: int = 2048) -> None:
        class Iovec(ctypes.Structure):
            _fields_ = [("iov_base", ctypes.c_void_p), ("iov_len", ctypes.c_size_t)]

        class Msghdr(ctypes.Structure):
            _fields_ = [
                ("msg_name", ctypes.c_void_p),
                ("msg_namelen", ctypes.c_uint32),
                ("msg_iov", ctypes.POINTER(Iovec)),
                ("msg_iovlen", ctypes.c_size_t),
                ("msg_control", ctypes.c_void_p),
                ("msg_controllen", ctypes.c_size_t),
                ("msg_flags", ctypes.c_int),
            ]

        class Mmsghdr(ctypes.Structure):
            _fields_ = [("msg_hdr", Msghdr), ("msg_len", ctypes.c_uint)]

        libc = ctypes.CDLL(None, use_errno=True)
        recvmmsg = libc.recvmmsg
        recvmmsg.argtypes = [
            ctypes.c_int,
            ctypes.POINTER(Mmsghdr),
            ctypes.c_uint,
            ctypes.c_int,
            ctypes.c_void_p,
        ]
        recvmmsg.restype = ctypes.c_int
        self._recvmmsg = recvmmsg
        self.n = n
        self.bufsize = bufsize
        self._bufs = [(ctypes.c_char * bufsize)() for _ in range(n)]
        self._iov = (Iovec * n)()
        self._msgs = (Mmsghdr * n)()
        for i in range(n):
            self._iov[i].iov_base = ctypes.cast(self._bufs[i], ctypes.c_void_p)
            self._iov[i].iov_len = bufsize
            self._msgs[i].msg_hdr.msg_iov = ctypes.pointer(self._iov[i])
            self._msgs[i].msg_hdr.msg_iovlen = 1

    def recv(self, sock: socket.socket, maxn: int) -> list[bytes]:
        n = min(maxn, self.n)
        got = self._recvmmsg(
            sock.fileno(), self._msgs, n, _MSG_DONTWAIT, None
        )
        if got < 0:
            err = ctypes.get_errno()
            if err in (11, 35):  # EAGAIN / EWOULDBLOCK
                raise BlockingIOError
            raise OSError(err, "recvmmsg")
        return [bytes(self._bufs[i][: self._msgs[i].msg_len]) for i in range(got)]


def recv_datagrams(sock: socket.socket, maxn: int = 64) -> list[bytes]:
    """
    Receive up to maxn datagrams. Uses recvmmsg on Linux; recvfrom loop elsewhere.
    Raises BlockingIOError if no datagram is ready (caller should stop the drain).
    """
    global _recv_mmsg
    if _recv_mmsg is False:
        try:
            _recv_mmsg = _RecvMmsg()
        except (OSError, AttributeError, TypeError, ValueError):
            _recv_mmsg = None
    if isinstance(_recv_mmsg, _RecvMmsg):
        try:
            batch = _recv_mmsg.recv(sock, maxn)
        except BlockingIOError:
            raise
        except OSError:
            _recv_mmsg = None
        else:
            if not batch:
                raise BlockingIOError
            return batch
    out: list[bytes] = []
    try:
        data, _ = sock.recvfrom(65535)
        out.append(data)
    except BlockingIOError:
        raise
    for _ in range(maxn - 1):
        try:
            data, _ = sock.recvfrom(65535)
        except BlockingIOError:
            break
        out.append(data)
    return out


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
    if (
        _GSO_MAX_SEGMENTS > 1
        and hasattr(sock, "sendmsg")
        and _gso_by_fd.get(sock.fileno(), True)
    ):
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
                    t0 = time.monotonic()
                    nsent = sendmmsg(msgs[pos:])
                    send_stats["syscall_s"] += time.monotonic() - t0
                    send_stats["calls"] += 1
                    if nsent <= 0:
                        break
                    pos += nsent
                    sent += nsent
                except BlockingIOError:
                    send_stats["calls"] += 1
                    send_stats["blocks"] += 1
                    import select

                    t1 = time.monotonic()
                    select.select([], [sock], [], 0.0005)
                    send_stats["block_s"] += time.monotonic() - t1
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
                t0 = time.monotonic()
                try:
                    written = sock.sendmsg([payload], ancillary, 0, addr)
                    send_stats["syscall_s"] += time.monotonic() - t0
                    send_stats["calls"] += 1
                    if written != len(payload):
                        raise OSError("short UDP GSO send")
                    break
                except BlockingIOError:
                    send_stats["syscall_s"] += time.monotonic() - t0
                    send_stats["calls"] += 1
                    send_stats["blocks"] += 1
                    import select

                    t1 = time.monotonic()
                    select.select([], [sock], [], 0.0005)
                    send_stats["block_s"] += time.monotonic() - t1
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
        t0 = time.monotonic()
        try:
            sock.sendto(buf, addr)
            send_stats["syscall_s"] += time.monotonic() - t0
            send_stats["calls"] += 1
            return
        except BlockingIOError:
            send_stats["syscall_s"] += time.monotonic() - t0
            send_stats["calls"] += 1
            send_stats["blocks"] += 1
            t1 = time.monotonic()
            select.select([], [sock], [], 0.0005)
            send_stats["block_s"] += time.monotonic() - t1
