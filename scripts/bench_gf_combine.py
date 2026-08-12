"""Where do the 222 us of a degree-32 coded packet go? Time each strategy."""

from __future__ import annotations

import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tetrys_nc import gf256  # noqa: E402

K = 32
LEN = 1350
REPS = 3000

rng = np.random.default_rng(1)
payloads = [rng.integers(0, 256, LEN, dtype=np.uint8).tobytes() for _ in range(K)]
coefs = [gf256.vandermonde_coef(i + 1, 12345) for i in range(K)]
MUL = gf256.MUL_TABLE_NP
ROWS = gf256.MUL_ROWS
assert MUL is not None and ROWS is not None
MUL_FLAT = MUL.reshape(-1)
coef_arr = np.array(coefs, dtype=np.uint8)
coef_off = coef_arr.astype(np.int32)[:, None] * 256


def timeit(name: str, fn) -> bytes:
    fn()
    t = time.perf_counter()
    for _ in range(REPS):
        res = fn()
    el = (time.perf_counter() - t) / REPS * 1e6
    print(f"{name:<34s} {el:8.1f} us")
    return res


def current():
    out = np.zeros(LEN, dtype=np.uint8)
    for coef, data in zip(coefs, payloads):
        d = np.frombuffer(data, dtype=np.uint8)
        if coef == 1:
            np.bitwise_xor(out, d, out=out)
        else:
            np.bitwise_xor(out, ROWS[coef][d], out=out)
    return out.tobytes()


def join_only():
    return b"".join(payloads)


def batched_2d():
    block = np.frombuffer(b"".join(payloads), dtype=np.uint8).reshape(K, LEN)
    return np.bitwise_xor.reduce(MUL[coef_arr[:, None], block], axis=0).tobytes()


def batched_flat():
    block = np.frombuffer(b"".join(payloads), dtype=np.uint8).reshape(K, LEN)
    idx = block.astype(np.int32)
    idx += coef_off
    return np.bitwise_xor.reduce(MUL_FLAT.take(idx), axis=0).tobytes()


def xor_only_floor():
    block = np.frombuffer(b"".join(payloads), dtype=np.uint8).reshape(K, LEN)
    return np.bitwise_xor.reduce(block, axis=0).tobytes()


def grouped_by_coef():
    """Distributive law: XOR symbols sharing a coefficient, then one gather each."""
    groups: dict[int, list[bytes]] = {}
    for coef, data in zip(coefs, payloads):
        groups.setdefault(coef, []).append(data)
    out = np.zeros(LEN, dtype=np.uint8)
    for coef, datas in groups.items():
        if len(datas) == 1:
            acc = np.frombuffer(datas[0], dtype=np.uint8)
        else:
            stack = np.frombuffer(b"".join(datas), dtype=np.uint8).reshape(-1, LEN)
            acc = np.bitwise_xor.reduce(stack, axis=0)
        np.bitwise_xor(out, ROWS[coef][acc] if coef != 1 else acc, out=out)
    return out.tobytes()


def main() -> int:
    print(f"K={K} len={LEN} distinct_coefs={len(set(coefs))}\n")
    ref = timeit("current (per-term loop)", current)
    timeit("  join only (memcpy floor)", join_only)
    timeit("  xor-only reduce (no gather)", xor_only_floor)
    a = timeit("batched 2-D gather", batched_2d)
    b = timeit("batched flat take", batched_flat)
    c = timeit("grouped by coefficient", grouped_by_coef)
    for name, got in (("batched_2d", a), ("batched_flat", b), ("grouped", c)):
        print(f"{name} matches reference: {got == ref}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
