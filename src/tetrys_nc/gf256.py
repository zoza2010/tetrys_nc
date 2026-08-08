"""GF(2^8) arithmetic with irreducible polynomial x^8 + x^4 + x^3 + x^2 + 1 (RFC 5510 / RFC 9407).

Hot-path mul/xor uses NumPy (prebuilt wheels) when available; pure-Python fallback otherwise.
No custom compiled extensions.
"""

from __future__ import annotations

from typing import Any

# Primitive polynomial: 0x11D = x^8 + x^4 + x^3 + x^2 + 1
_POLY = 0x11D

EXP = [0] * 512
LOG = [0] * 256
MUL_TABLE: list[bytearray] = [bytearray(256) for _ in range(256)]

_NP: Any = None
MUL_TABLE_NP = None  # type: ignore[var-annotated]
# Cache row views: MUL_ROWS[c] is contiguous uint8[256] for coef c
MUL_ROWS: list[Any] | None = None
_BACKEND = "python"


def _init_tables() -> None:
    x = 1
    for i in range(255):
        EXP[i] = x
        LOG[x] = i
        x <<= 1
        if x & 0x100:
            x ^= _POLY
    for i in range(255, 512):
        EXP[i] = EXP[i - 255]

    for a in range(256):
        row = MUL_TABLE[a]
        if a == 0:
            continue
        if a == 1:
            for b in range(256):
                row[b] = b
            continue
        log_a = LOG[a]
        for b in range(1, 256):
            row[b] = EXP[log_a + LOG[b]]


def _init_numpy() -> None:
    global _NP, MUL_TABLE_NP, MUL_ROWS, _BACKEND
    try:
        import numpy as np
    except ImportError:
        return
    _NP = np
    tbl = np.empty((256, 256), dtype=np.uint8)
    for a in range(256):
        tbl[a] = np.frombuffer(MUL_TABLE[a], dtype=np.uint8)
    MUL_TABLE_NP = tbl
    # Contiguous copies — faster fancy-index source than strided views
    MUL_ROWS = [np.ascontiguousarray(tbl[a]) for a in range(256)]
    _BACKEND = "numpy"


_init_tables()
_init_numpy()


def add(a: int, b: int) -> int:
    return a ^ b


def mul(a: int, b: int) -> int:
    return MUL_TABLE[a][b]


def div(a: int, b: int) -> int:
    if b == 0:
        raise ZeroDivisionError("division by zero in GF(256)")
    if a == 0:
        return 0
    return EXP[LOG[a] - LOG[b] + 255]


def inv(a: int) -> int:
    if a == 0:
        raise ZeroDivisionError("inverse of zero in GF(256)")
    return EXP[255 - LOG[a]]


def pow_alpha(exp: int) -> int:
    """Return α^exp where α is a primitive element."""
    return EXP[exp % 255]


def vandermonde_coef(source_id: int, coded_id: int) -> int:
    """
    CCGI=0b01: Vandermonde coefficient over GF(2^8).
    coef = α^((source_id * coded_id) % 256), with α^0 = 1 when product is 0.
    """
    e = (source_id * coded_id) % 256
    if e == 0:
        return 1
    return EXP[e % 255]


def _as_u8(data: bytes | bytearray | memoryview, n: int):
    assert _NP is not None
    if isinstance(data, _NP.ndarray):
        return data.ravel()[:n]
    return _NP.frombuffer(data, dtype=_NP.uint8, count=n)


def mul_bytes(coef: int, data: bytes | bytearray | memoryview, out: bytearray) -> None:
    """out[i] ^= coef * data[i] for all i (in-place accumulate)."""
    if coef == 0:
        return
    n = len(data)
    if n == 0:
        return

    if MUL_ROWS is not None and _NP is not None and n >= 32:
        d = _as_u8(data, n)
        o = _NP.frombuffer(out, dtype=_NP.uint8, count=n)
        if coef == 1:
            _NP.bitwise_xor(o, d, out=o)
        else:
            # Row lookup then xor — NumPy C loops, no Python per-byte
            _NP.bitwise_xor(o, MUL_ROWS[coef][d], out=o)
        return

    if coef == 1:
        ox = out
        for i in range(n):
            ox[i] ^= data[i]
        return
    table = MUL_TABLE[coef]
    ox = out
    for i in range(n):
        ox[i] ^= table[data[i]]


def scale_bytes(coef: int, data: bytes | bytearray | memoryview) -> bytearray:
    """Return coef * data as a new bytearray."""
    n = len(data)
    if coef == 0:
        return bytearray(n)
    if coef == 1:
        return bytearray(data)

    if MUL_ROWS is not None and _NP is not None and n >= 32:
        d = _as_u8(data, n)
        return bytearray(MUL_ROWS[coef][d].tobytes())

    out = bytearray(n)
    table = MUL_TABLE[coef]
    for i in range(n):
        out[i] = table[data[i]]
    return out


def xor_bytes(dst: bytearray, src: bytes | bytearray | memoryview) -> None:
    """dst[i] ^= src[i] (vectorized when NumPy is available)."""
    n = len(src)
    if n == 0:
        return
    if _NP is not None and n >= 32:
        d = _NP.frombuffer(dst, dtype=_NP.uint8, count=n)
        s = _as_u8(src, n)
        _NP.bitwise_xor(d, s, out=d)
        return
    for i in range(n):
        dst[i] ^= src[i]


def linear_combine(
    terms: list[tuple[int, bytes | bytearray | memoryview]],
    length: int,
) -> bytes:
    """
    Fast Σ coef_i * data_i over GF(256) via NumPy.
    Used by the encoder coded-packet path.
    """
    if not terms:
        return b"\x00" * length
    if MUL_ROWS is not None and _NP is not None and length >= 32:
        out = _NP.zeros(length, dtype=_NP.uint8)
        for coef, data in terms:
            if coef == 0:
                continue
            d = _as_u8(data, length)
            if coef == 1:
                _NP.bitwise_xor(out, d, out=out)
            else:
                _NP.bitwise_xor(out, MUL_ROWS[coef][d], out=out)
        return out.tobytes()
    out = bytearray(length)
    for coef, data in terms:
        mul_bytes(coef, data, out)
    return bytes(out)


def using_numpy() -> bool:
    return MUL_TABLE_NP is not None


def backend() -> str:
    return _BACKEND
