"""RaptorQ UDP file client."""

from __future__ import annotations

import argparse
from pathlib import Path

from .block_xfer import run_block_client


def run_client(
    host: str,
    port: int,
    output: Path,
    remote: str = "",
    file_progress: bool = False,
) -> int:
    print(f"connecting to udp://{host}:{port}")
    return run_block_client(
        host, port, output, remote=remote, file_progress=file_progress
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gen RaptorQ UDP file client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument(
        "--file",
        action="append",
        default=None,
        dest="files",
        help="remote path relative to server --dir; repeat or pass a directory to mux",
    )
    p.add_argument("--output", type=Path, default=None)
    p.add_argument(
        "--progress",
        action="store_true",
        help="TTY bars: mux shows group total plus the in-flight file",
    )
    args = p.parse_args(argv)
    files = args.files or []
    remote = "\n".join(files)
    output = args.output
    if output is None:
        if len(files) == 1 and "/" not in files[0].rstrip("/") and not files[0].endswith("\\"):
            output = Path(Path(files[0]).name)
        elif len(files) > 1:
            output = Path("recv")
        elif files:
            name = Path(files[0].rstrip("/"))
            output = Path(name.name)
        else:
            output = Path("received.bin")
    return run_client(
        args.host, args.port, output, remote=remote, file_progress=args.progress
    )


if __name__ == "__main__":
    raise SystemExit(main())
