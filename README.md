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

Loopback emulator, encode bench, and test-blob generator live in `sim/`:

```bash
uv run python -m sim.netem_udp --listen 127.0.0.1:7495 --forward 127.0.0.1:7494 --profile spain
uv run python -m sim.encbench --k 96 --seconds 8
uv run python -m sim.genfile --output testdata/blob_1g.bin --size 1G
```

```bash
uv run pytest -q
```
