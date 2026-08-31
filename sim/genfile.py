"""Generate a deterministic test file of a given size."""

from __future__ import annotations

import argparse
import struct
from pathlib import Path


def parse_size(s: str) -> int:
    s = s.strip().upper()
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    if s[-1] in units:
        return int(float(s[:-1]) * units[s[-1]])
    return int(s)


def generate(path: Path, size: int, seed: int = 0xC0FFEE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    chunk = 4 * 1024 * 1024
    # xorshift-ish deterministic stream
    state = seed & 0xFFFFFFFF
    written = 0
    with path.open("wb") as f:
        while written < size:
            n = min(chunk, size - written)
            buf = bytearray(n)
            i = 0
            while i + 4 <= n:
                state ^= (state << 13) & 0xFFFFFFFF
                state ^= (state >> 17) & 0xFFFFFFFF
                state ^= (state << 5) & 0xFFFFFFFF
                struct.pack_into("<I", buf, i, state)
                i += 4
            while i < n:
                state ^= (state << 13) & 0xFFFFFFFF
                state ^= (state >> 17) & 0xFFFFFFFF
                state ^= (state << 5) & 0xFFFFFFFF
                buf[i] = state & 0xFF
                i += 1
            f.write(buf)
            written += n
            if written % (64 * 1024 * 1024) == 0 or written == size:
                print(f"wrote {written}/{size} ({100.0 * written / size:.1f}%)")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Generate test file for Tetrys transfer")
    p.add_argument("--output", type=Path, default=Path("testdata/blob_1g.bin"))
    p.add_argument("--size", default="1G")
    p.add_argument("--seed", type=int, default=0xC0FFEE)
    args = p.parse_args(argv)
    size = parse_size(args.size)
    print(f"generating {args.output} ({size} bytes)...")
    generate(args.output, size, args.seed)
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
