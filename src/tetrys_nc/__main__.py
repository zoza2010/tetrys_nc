"""python -m tetrys_nc {server|client|objserver|objclient} ..."""

from __future__ import annotations

import sys


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in {"-h", "--help"}:
        print(
            "usage:\n"
            "  python -m tetrys_nc server --file PATH [--port 9000] [--wan] [--gen-k 768]\n"
            "  python -m tetrys_nc client --output PATH [--host HOST --port 9000]\n"
            "  python -m tetrys_nc objserver --early DIR [--late DIR] [--wan] [--port 7494]\n"
            "  python -m tetrys_nc objclient --output DIR [--host HOST --port 7494] [--wan]\n"
        )
        return 0
    cmd = sys.argv[1]
    argv = sys.argv[2:]
    if cmd == "server":
        from .server import main as server_main

        return server_main(argv)
    if cmd == "client":
        from .client import main as client_main

        return client_main(argv)
    if cmd == "objserver":
        from .object_cli import main_objserver

        return main_objserver(argv)
    if cmd == "objclient":
        from .object_cli import main_objclient

        return main_objclient(argv)
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
