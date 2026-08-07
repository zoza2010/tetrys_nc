# Tetrys NC — UDP file transfer with on-the-fly network coding

Клиент и сервер для надёжной передачи файла поверх **UDP** с алгоритмом **Tetrys** ([RFC 9407](https://www.rfc-editor.org/rfc/rfc9407.html)): elastic encoding window, систематические source-пакеты + coded repair-пакеты над GF(2⁸), feedback (SACK / window update).

Это замена TCP на уровне приложения: нет TCP, только UDP + Tetrys.

## Быстрый старт

```bash
# установка
uv sync --group dev

# тестовый файл 1 ГиБ
uv run python -m tetrys_nc genfile --output testdata/blob_1g.bin --size 1G

# терминал 1 — сервер (отправитель)
uv run python -m tetrys_nc server --file testdata/blob_1g.bin --port 9000

# терминал 2 — клиент (получатель)
uv run python -m tetrys_nc client --host 127.0.0.1 --port 9000 --output testdata/received_1g.bin
```

Клиент сверяет SHA-256 с сервером и печатает `OK` / `FAIL`.

## Как это работает

```
App (file) → Tetrys Encoder → UDP → Tetrys Decoder → App (file)
                 ↑ feedback (window update / SACK) ↓
```

1. **Source packet** — исходный символ с ID (систематическая передача).
2. **Coded packet** — линейная комбинация неподтверждённых символов из elastic window; коэффициенты Vandermonde GF(256) (`CCGI=0b01`).
3. **Window update** — клиент периодически шлёт SACK; сервер вычищает ACKed символы из окна и подстраивает частоту redundancy.

Параметры по умолчанию (оптимизированы под localhost / быстрый тест 1 ГиБ):
payload **32768** B, window **8192**, redundancy **0** (repair включается по PLR).
Для WAN/MTU 1500: `--payload-size 1400 --redundancy 8`.

## Опции

```text
server:
  --file PATH          файл для отправки
  --port PORT          UDP порт (default 9000)
  --window N           размер elastic window
  --redundancy N       coded packet каждые N source
  --payload-size N     размер символа (байты)
  --pace-us US         пауза после каждого source (throttle)

client:
  --host HOST --port PORT
  --output PATH
  --window N
  --feedback-every N   как часто слать window update
```

## Тесты и бенчмарк

```bash
uv run pytest -q

# сравнение конфигураций на localhost (64MiB)
uv run python scripts/bench_inplace.py --size 64M

# полный прогон (pytest + 64M + 1G)
bash scripts/run_bench.sh
```

## WAN (потери, большой RTT)

Без pacing UDP забивает путь → ещё больше потерь; coded по «хвосту» окна не лечит HOL-дыру в начале.

```bash
# сервер (в Испании)
uv run python -m tetrys_nc server --file testdata/blob_1g.bin --port 7494 --wan --skip-hash

# клиент (в РФ)
uv run python -m tetrys_nc client --host tintrack-cloud.a-vfx.com --port 7494 --wan --output testdata/received_1g.bin
```

`--wan` стартует **легко** (coded каждые 4 source, degree=12) — иначе pure-Python GF становится bottleneck (~0.2 MiB/s при plr=0).  
При росте PLR автоматически усиливается до 1 source + 2..4 coded.

Принудительно густо (если потери реально огромные):
```bash
--wan --redundancy 1 --coded-burst 3 --rate-mbit 200
```

## Замечания

- Реализация следует идеям RFC 9407 (elastic window, on-the-fly coding, feedback).
- Localhost: большие payload; WAN: обязательно `--wan`.
