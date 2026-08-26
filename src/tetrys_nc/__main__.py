"""python -m tetrys_nc {server|client|genfile} ..."""

from __future__ import annotations

import sys


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in {"-h", "--help"}:
        print(
            "usage:\n"
            "  python -m tetrys_nc server --file PATH [--port 9000] [--wan] [--gen-k 768]\n"
            "  python -m tetrys_nc client --output PATH [--host HOST --port 9000]\n"
            "  python -m tetrys_nc genfile --output PATH [--size 1G]\n"
            "  python -m tetrys_nc netem --listen 127.0.0.1:7495 --forward 127.0.0.1:7494 --profile spain\n"
            "  python -m tetrys_nc encbench [--k 96] [--seconds 8]\n"
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
    if cmd == "genfile":
        from .genfile import main as gen_main

        return gen_main(argv)
    if cmd == "netem":
        from .netem_udp import main as netem_main

        return netem_main(argv)
    if cmd == "encbench":
        from .encbench import main as encbench_main

        return encbench_main(argv)
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
