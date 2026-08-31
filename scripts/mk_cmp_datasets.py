#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
from pathlib import Path

ROOT = Path.home() / "quic_tests/tetrys_nc/testdata"


def wipe(p: Path) -> None:
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True)


def concat(src: Path, dest: Path) -> None:
    files = sorted(x for x in src.iterdir() if x.is_file())
    with dest.open("wb") as out:
        for f in files:
            out.write(f.read_bytes())
    print(dest.name, dest.stat().st_size)


def main() -> None:
    p = ROOT / "cmp_1000small"
    wipe(p)
    for i in range(1000):
        (p / f"s{i:04d}.bin").write_bytes(bytes([i % 251]) * (12 * 1024 + (i % 4000)))
    print("1000small", sum(f.stat().st_size for f in p.iterdir()), "n", 1000)

    p = ROOT / "cmp_100med"
    wipe(p)
    for i in range(100):
        (p / f"m{i:03d}.bin").write_bytes(os.urandom(250 * 1024 + i * 100))
    print("100med", sum(f.stat().st_size for f in p.iterdir()), "n", 100)

    p = ROOT / "cmp_4large"
    wipe(p)
    blob8 = ROOT / "blob_8m.bin"
    data = blob8.read_bytes() if blob8.exists() else os.urandom(8 << 20)
    # 200 x 1 MiB: each is above SMALL_MAX so mux shows one progress bar per file.
    chunk = data[: 1 << 20] if len(data) >= (1 << 20) else os.urandom(1 << 20)
    for i in range(200):
        (p / f"f{i:03d}.bin").write_bytes(chunk)
    print("200x1MiB", 200 * (1 << 20), "n", 200)

    concat(ROOT / "cmp_1000small", ROOT / "blob_cmp_1000.bin")
    concat(ROOT / "cmp_100med", ROOT / "blob_cmp_100.bin")
    concat(ROOT / "cmp_4large", ROOT / "blob_cmp_4large.bin")

    t64 = ROOT / "tree64"
    t64.mkdir(exist_ok=True)
    link = t64 / "blob_64m.bin"
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(Path("..") / "blob_64m.bin")
    print("tree64 link ok")


if __name__ == "__main__":
    main()
