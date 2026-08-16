# tetrys_nc — Generation RaptorQ UDP file transfer

Клиент и сервер для надёжной передачи файла поверх **UDP** с **generation RaptorQ**: независимые поколения (~K символов × T байт), systematic + repair, random-access запись на клиенте.

Имя пакета/CLI — `tetrys_nc` (совместимость с уже развёрнутыми VM); семантика — только gen transfer.

## Быстрый старт

```bash
# установка
uv sync --group dev

# тестовый файл 1 ГиБ
uv run python -m tetrys_nc genfile --output testdata/blob_1g.bin --size 1G

# терминал 1 — сервер (отправитель)
uv run python -m tetrys_nc server --file testdata/blob_1g.bin --port 9000 \
  --gen-k 48 --gen-overhead 8 --rate 1000 --ramp-s 2

# терминал 2 — клиент (получатель)
uv run python -m tetrys_nc client --host 127.0.0.1 --port 9000 --output testdata/received_1g.bin
```

Клиент сверяет SHA-256 с сервером (если сервер не с `--skip-hash`) и печатает `OK` / `FAIL`.

## Как это работает

```
App (file) → GenEncoder (RaptorQ) → UDP → GenDecoder → App (file)
                 ↑ PKT_GEN_FB (NACK gens) ↓
```

1. **META** — размер файла, имя, SHA-256, параметры gen (`T`, `K`, overhead).
2. **PKT_GEN** — один RaptorQ encoding packet для generation `gen_id`.
3. **PKT_GEN_FB** — `next_needed_gen` + NACK list незавершённых поколений.
4. **RateLimiter** — blast pacing по `--rate` / `--ramp-s`.

## Опции

```text
server:
  --file PATH          файл для отправки
  --port PORT          UDP порт (default 9000)
  --wan                T=1350; default --rate 1000
  --rate / --rate-mbit целевая скорость UDP (Mbit/s)
  --ramp-s S           разгон 0→rate (default 2)
  --gen-k N            символов на поколение (default 48)
  --gen-overhead PCT   repair overhead % (default 8)
  --skip-hash          не считать SHA-256 на сервере

client:
  --host HOST --port PORT
  --output PATH
  --wan                большие буферы сокета
```

## Тесты

```bash
uv run pytest -q
```

## WAN

```bash
# сервер
uv run python -m tetrys_nc server --file testdata/blob_1g.bin --port 7494 --wan --skip-hash \
  --rate 1000 --ramp-s 2 --gen-k 48 --gen-overhead 2

# клиент
uv run python -m tetrys_nc client --host tintrack-cloud.a-vfx.com --port 7494 --wan \
  --output testdata/received_1g.bin
```

Ставьте `--rate` по результату `iperf3 -u` на том же пути.
