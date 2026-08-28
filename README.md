# tetrys_nc — RaptorQ UDP file transfer

v2 block transfer (`server` / `client`) and object-mux (`objserver` / `objclient`) over UDP.

```bash
uv sync --group dev
uv run python -m tetrys_nc genfile --output testdata/blob_1g.bin --size 1G

uv run python -m tetrys_nc server --file testdata/blob_1g.bin --port 7494 --wan --skip-hash
uv run python -m tetrys_nc client --host 127.0.0.1 --port 7494 --wan --output testdata/recv.bin

uv run python -m tetrys_nc objserver --early DIR [--late DIR] --wan --port 7494
uv run python -m tetrys_nc objclient --output DIR --host HOST --port 7494 --wan
```

WAN defaults: T=1350, K=768, 64 MiB window, 24% FEC, 850 Mbit pace.

```bash
uv run pytest -q
```
