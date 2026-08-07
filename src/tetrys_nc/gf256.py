"""GF(2^8) arithmetic with irreducible polynomial x^8 + x^4 + x^3 + x^2 + 1 (RFC 5510 / RFC 9407)."""

from __future__ import annotations

# Primitive polynomial: 0x11D = x^8 + x^4 + x^3 + x^2 + 1
_POLY = 0x11D

EXP = [0] * 512
LOG = [0] * 256
# MUL_TABLE[a][b] = a * b in GF(256) — 64KiB, makes payload coding much faster
MUL_TABLE: list[bytearray] = [bytearray(256) for _ in range(256)]


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


_init_tables()


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


def mul_bytes(coef: int, data: bytes | bytearray, out: bytearray) -> None:
    """out[i] ^= coef * data[i] for all i (in-place accumulate)."""
    if coef == 0:
        return
    n = len(data)
    if coef == 1:
        # Local binds help a bit in tight Python loops
        ox = out
        for i in range(n):
            ox[i] ^= data[i]
        return
    table = MUL_TABLE[coef]
    ox = out
    for i in range(n):
        ox[i] ^= table[data[i]]


def scale_bytes(coef: int, data: bytes | bytearray) -> bytearray:
    """Return coef * data as a new bytearray."""
    out = bytearray(len(data))
    if coef == 0:
        return out
    if coef == 1:
        out[:] = data
        return out
    table = MUL_TABLE[coef]
    for i, b in enumerate(data):
        out[i] = table[b]
    return out
