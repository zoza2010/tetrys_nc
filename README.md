# tetrys_nc — RaptorQ UDP file transfer

Block transfer (`server` / `client`) and object-mux (`objserver` / `objclient`) over UDP.

```bash
uv sync --group dev
uv run python -m sim.genfile --output testdata/blob_1g.bin --size 1G

uv run python -m tetrys_nc server --dir testdata --port 7494 --skip-hash
uv run python -m tetrys_nc client --host 127.0.0.1 --port 7494 --file blob_1g.bin --output testdata/recv.bin
uv run python -m tetrys_nc client --host 127.0.0.1 --port 7494 --file cmp_1000small --output testdata/recvdir

uv run python -m tetrys_nc objserver --early DIR [--late DIR] --port 7494
uv run python -m tetrys_nc objclient --output DIR --host HOST --port 7494
```

WAN defaults: T=1350, K=768, 64 MiB window, 24% FEC, 850 Mbit pace.

Console file manager (`tetrys-fm`) browses local + remote panels and copies a
selection as **one recursive object-mux** (not one UDP session per file).
Server needs `--allow-upload` for uploads / mkdir / rm:

```bash
uv run python -m tetrys_nc server --dir testdata --port 7494 --skip-hash --allow-upload
uv run tetrys-fm --host 127.0.0.1 --port 7494 --local .
# keys: Tab panels, Ins mark, F5 copy, F7 mkdir, F8 rm, Esc cancel, q quit

# WAN (Spain client → Russia server): always absolute --dir so testdata with Maya ISO is the root.
# On Russia: bash scripts/wan_server.sh
#   → --dir /home/sysops/quic_tests/tetrys_nc/testdata --gen-overhead 20 --allow-upload

uv run python -m tetrys_nc client --host HOST --port 7494 --list
uv run python -m tetrys_nc client --host HOST --port 7494 --mkdir inbox
uv run python -m tetrys_nc client --host HOST --port 7494 --upload --file local.bin --remote inbox/local.bin
```

Directory download/upload also mux recursively via the plain client:

```bash
uv run python -m tetrys_nc client --host HOST --port 7494 --file tree --output recv_tree
uv run python -m tetrys_nc client --host HOST --port 7494 --upload --file tree --remote tree
```

Loopback emulator, encode bench, and test-blob generator live in `sim/`:

```bash
uv run python -m sim.netem_udp --listen 127.0.0.1:7495 --forward 127.0.0.1:7494 --profile spain
uv run python -m sim.encbench --k 96 --seconds 8
uv run python -m sim.genfile --output testdata/blob_1g.bin --size 1G
```

```bash
uv run pytest -q
```
