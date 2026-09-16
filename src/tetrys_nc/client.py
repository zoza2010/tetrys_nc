"""RaptorQ UDP file client."""

from __future__ import annotations

import argparse
from pathlib import Path

from .block_state import WAN_BLOCK_K, WAN_SYMBOL_SIZE
from .block_xfer import run_block_client, run_block_upload_client


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
        help="download: remote path relative to server --dir. "
        "upload: local path; repeat or pass a directory to mux",
    )
    p.add_argument("--output", type=Path, default=None)
    p.add_argument(
        "--upload",
        action="store_true",
        help="send local --file to the server instead of downloading",
    )
    p.add_argument(
        "--remote",
        default="",
        help="upload destination relative to server --dir",
    )
    p.add_argument(
        "--progress",
        action="store_true",
        help="TTY bars: mux shows group total plus the in-flight file",
    )
    p.add_argument("--skip-hash", action="store_true")
    p.add_argument(
        "--payload-size",
        type=int,
        default=WAN_SYMBOL_SIZE,
        help=f"upload symbol size T (default {WAN_SYMBOL_SIZE})",
    )
    p.add_argument(
        "--rate-mbit",
        "--rate",
        type=float,
        default=None,
        dest="rate_mbit",
        help="upload: lock UDP send rate in Mbit/s (disables rate search)",
    )
    p.add_argument("--gen-k", type=int, default=WAN_BLOCK_K)
    p.add_argument(
        "--gen-overhead",
        type=int,
        default=None,
        help="upload: lock RaptorQ first-flight repair percent",
    )
    args = p.parse_args(argv)
    files = args.files or []
    if args.upload:
        if not files:
            p.error("--upload requires --file")
        sources = [Path(item) for item in files]
        print(f"uploading to udp://{args.host}:{args.port}")
        return run_block_upload_client(
            args.host,
            args.port,
            sources,
            remote=args.remote,
            symbol_size=args.payload_size,
            block_k=args.gen_k,
            initial_repair_pct=args.gen_overhead,
            rate_mbit=args.rate_mbit,
            skip_hash=args.skip_hash,
        )
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
