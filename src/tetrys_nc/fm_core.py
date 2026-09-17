"""Headless file-manager operations: list + one-mux copy of selections."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .block_packets import VfsEntry
from .block_state import WAN_BLOCK_K, WAN_SYMBOL_SIZE
from .block_xfer import (
    _collect_tree_files,
    _skip_vfs_name,
    run_block_client,
    run_block_upload_client,
    run_vfs_list,
    run_vfs_mkdir,
    run_vfs_unlink,
)


@dataclass(slots=True)
class LocalEntry:
    name: str
    is_dir: bool
    size: int
    mtime: int


def list_local(cwd: Path) -> list[LocalEntry]:
    cwd = cwd.resolve()
    if not cwd.is_dir():
        raise NotADirectoryError(cwd)
    out: list[LocalEntry] = []
    for child in sorted(cwd.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
        if _skip_vfs_name(child.name):
            continue
        try:
            st = child.stat()
        except OSError:
            continue
        is_dir = child.is_dir() and not child.is_symlink()
        if not is_dir and not child.is_file():
            continue
        out.append(
            LocalEntry(
                child.name,
                is_dir,
                0 if is_dir else st.st_size,
                int(st.st_mtime),
            )
        )
    return out


def list_remote(
    host: str,
    port: int,
    rel: str = "",
    **nat,
) -> list[VfsEntry]:
    return run_vfs_list(host, port, rel, **nat)


def build_local_manifest(
    cwd: Path, names: list[str]
) -> list[tuple[str, Path]]:
    """Expand selected names under cwd into recursive (relpath, path) pairs."""
    cwd = cwd.resolve()
    files: list[tuple[str, Path]] = []
    seen: set[str] = set()
    for name in names:
        rel = name.replace("\\", "/").strip("/")
        if not rel or rel in {".", ".."} or ".." in rel.split("/"):
            raise ValueError(f"invalid selection {name!r}")
        path = (cwd / rel).resolve()
        try:
            path.relative_to(cwd)
        except ValueError as exc:
            raise ValueError(f"selection escapes cwd: {name}") from exc
        if path.is_file():
            if rel not in seen:
                files.append((rel, path))
                seen.add(rel)
        elif path.is_dir():
            for child_rel, child in _collect_tree_files(path):
                key = f"{rel}/{child_rel}"
                if key not in seen:
                    files.append((key, child))
                    seen.add(key)
        else:
            raise FileNotFoundError(path)
    if not files:
        raise FileNotFoundError("selection has no files")
    return files


def copy_local_to_remote(
    host: str,
    port: int,
    cwd: Path,
    names: list[str],
    *,
    remote_dir: str = "",
    progress: Callable[[str], None] | None = None,
    should_abort: Callable[[], bool] | None = None,
    symbol_size: int = WAN_SYMBOL_SIZE,
    block_k: int = WAN_BLOCK_K,
    initial_repair_pct: int | None = None,
    rate_mbit: float | None = None,
    skip_hash: bool = True,
    **nat,
) -> int:
    files = build_local_manifest(cwd, names)
    remote = remote_dir.replace("\\", "/").strip("/")
    if len(files) == 1 and "/" not in files[0][0] and Path(cwd, names[0]).is_file():
        return run_block_upload_client(
            host,
            port,
            files[0][1],
            remote=f"{remote}/{files[0][0]}".strip("/") if remote else files[0][0],
            progress=progress,
            should_abort=should_abort,
            symbol_size=symbol_size,
            block_k=block_k,
            initial_repair_pct=initial_repair_pct,
            rate_mbit=rate_mbit,
            skip_hash=skip_hash,
            **nat,
        )
    return run_block_upload_client(
        host,
        port,
        files=files,
        remote=remote or "upload",
        progress=progress,
        should_abort=should_abort,
        symbol_size=symbol_size,
        block_k=block_k,
        initial_repair_pct=initial_repair_pct,
        rate_mbit=rate_mbit,
        skip_hash=skip_hash,
        **nat,
    )


def copy_remote_to_local(
    host: str,
    port: int,
    remotes: list[str],
    local_dir: Path,
    *,
    progress: Callable[[str], None] | None = None,
    should_abort: Callable[[], bool] | None = None,
    **nat,
) -> int:
    if not remotes:
        raise FileNotFoundError("nothing selected")
    local_dir = local_dir.resolve()
    local_dir.mkdir(parents=True, exist_ok=True)
    if len(remotes) == 1:
        remote = remotes[0].replace("\\", "/").strip("/")
        name = Path(remote).name or "download"
        dest = local_dir / name
        return run_block_client(
            host,
            port,
            dest,
            remote=remote,
            progress=progress,
            should_abort=should_abort,
            **nat,
        )
    joined = "\n".join(
        item.replace("\\", "/").strip("/") for item in remotes if item.strip()
    )
    return run_block_client(
        host,
        port,
        local_dir,
        remote=joined,
        progress=progress,
        should_abort=should_abort,
        **nat,
    )


def remote_mkdir(host: str, port: int, rel: str, **nat) -> None:
    run_vfs_mkdir(host, port, rel, **nat)


def remote_rm(host: str, port: int, rel: str, **nat) -> None:
    """Remove a file or recursively remove a directory on the server."""
    rel = rel.replace("\\", "/").strip("/")
    if not rel:
        raise ValueError("refusing to delete remote root")
    parent, _, name = rel.rpartition("/")
    listing = list_remote(host, port, parent, **nat)
    match = next((item for item in listing if item.name == name), None)
    if match is not None and match.is_dir:
        for item in list_remote(host, port, rel, **nat):
            remote_rm(host, port, f"{rel}/{item.name}", **nat)
    run_vfs_unlink(host, port, rel, **nat)


def local_rm(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()
