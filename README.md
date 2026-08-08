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
3. **Window update** — клиент периодически шлёт SACK + echo timestamp; сервер вычищает ACKed символы и чинит дыры (NACK retransmit / coded).
4. **FASP-style rate control** — blast at `--rate`; standing queue only soft-biases (≥90% of target); потери **не** режут скорость, только усиливают repair.
5. **GF(2⁸)** — NumPy (готовые wheels, без своей компиляции) на hot path.

Параметры по умолчанию (оптимизированы под localhost / быстрый тест 1 ГиБ):
payload **32768** B, window **8192**, redundancy **32**.
Для WAN/MTU 1500: `--wan` (или `--payload-size 1400 --rate 800`).

## Опции

```text
server:
  --file PATH          файл для отправки
  --port PORT          UDP порт (default 9000)
  --window N           размер elastic window
  --redundancy N       coded packet каждые N source
  --payload-size N     размер символа (байты)
  --pace-us US         пауза после каждого source (throttle)
  --rate / --rate-mbit целевая скорость UDP (Mbit/s)

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

Как FASP: забиваем канал до `--rate`, loss → NACK retransmit (не fairness / не обвал rate).

```bash
# сервер (в Испании) — --rate ≈ ёмкости линка (iperf3 UDP)
uv run python -m tetrys_nc server --file testdata/blob_1g.bin --port 7494 --wan --rate 800 --skip-hash

# клиент (в РФ)
uv run python -m tetrys_nc client --host tintrack-cloud.a-vfx.com --port 7494 --wan --output testdata/received_1g.bin
```

`--wan`: payload 1350, **redundancy=0**, window 65536, **BLAST** на `--rate`, **`--reorder-ms 20`** (delayed NACK vs OOOrder).
Потери → NACK после reorder timer. В логе `nack=Q+P`.

```bash
--wan --rate 600 --reorder-ms 20
```

## Замечания

- Реализация следует идеям RFC 9407 (elastic window, on-the-fly coding, feedback).
- Rate control — FASP-подобно: blast to target, soft delay bias, loss ≠ congestion.
- Localhost: большие payload; WAN: обязательно `--wan` на **обоих** концах.
- Ставьте `--rate` по результату `iperf3 -u` на том же пути.