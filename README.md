# MatchPulse

Lambda-style football match analytics platform (MSc Big Data Processing and Applications).

- **Speed path:** a replay producer streams real StatsBomb match events into Kafka at match
  speed → Spark Structured Streaming → Redis → Grafana (live xG, momentum, alerts).
- **Batch path:** StatsBomb JSON + Kaggle European Soccer Database → Spark → Delta Lake on
  MinIO (bronze / silver / gold) → Streamlit (season stats, shot maps, league trends).

Everything runs locally with one `docker compose up`.

```
 StatsBomb JSON ──► replay-producer ──► Kafka ──► Spark Streaming ──► Redis ──► Grafana
                                        │  match-events / match-control      │
                                        │                                    └──► match-alerts
 StatsBomb + Kaggle ──► Spark batch ──► Delta Lake on MinIO (bronze/silver/gold) ──► Streamlit
```

## Team and ownership

| Area | Folder | Owner |
|---|---|---|
| Kafka setup, replay producer, Streamlit, workflows, contracts | `producer/`, `dashboards/streamlit/`, `scripts/`, `docs/` | David |
| Historical data, Delta Lake on MinIO, Spark batch | `batch/` | Mihai |
| Spark Structured Streaming, Redis, Grafana, anomaly detection | `streaming/`, `dashboards/grafana/` | Dayana |

## Prerequisites

- Docker Desktop (macOS Apple Silicon or Intel) or Docker Engine on Windows + WSL2,
  with Docker Compose v2. Every image used here supports both `linux/arm64` and `linux/amd64`.
- At least 6 GB RAM allocated to Docker once Spark is added.
- Python 3.11+ for running scripts from the host (optional; everything can also run in Docker).
- `make` (macOS/Linux/WSL). Windows users without `make` can use the equivalent `scripts/*.sh`.

## Quick start

```bash
cp .env.example .env     # optional: change ports, image tags, data folder
make up                  # Kafka + Kafka UI + MinIO + Streamlit, waits until healthy
make health              # OK/FAIL per service
make fetch-data          # StatsBomb data into data/ (competitions, match lists, 13 demo matches)
make replay-match MATCH=3788741 SPEED=60    # one match into Kafka (~2 min at 60x)
make test-replay MATCH=3788741              # PASS/FAIL: complete and in order?
make replay-many         # 3 matches in parallel, throughput log
make dev-fixtures        # sample gold tables in MinIO, until Mihai's batch output exists
```

Then open Kafka UI (http://localhost:8080) and Streamlit (http://localhost:8501).
`make help` lists every target. Without `make` (Windows/WSL), run `bash scripts/<target>.sh`,
see [`scripts/README.md`](scripts/README.md). The demo walkthrough is in
[`docs/demo-script.md`](docs/demo-script.md).

Stop with `make down`; `scripts/down.sh -v` also deletes Kafka and MinIO data.

## Services and ports

| Service | URL / address | Owner | Notes |
|---|---|---|---|
| Kafka (from containers) | `kafka:9092` | David | use this inside Docker Compose |
| Kafka (from your host) | `localhost:9094` | David | use this for scripts run in your terminal |
| Kafka UI | http://localhost:8080 | David | browse topics and messages |
| Streamlit | http://localhost:8501 | David | historical analytics (gold tables) |
| MinIO S3 API / console | http://localhost:9000 / http://localhost:9001 | Mihai | `minioadmin` / `minioadmin`, bucket `lake` |
| replay-producer | (on demand) | David | `make replay-match`, not started by `make up` |

## Kafka topics

| Topic | Partitions | Key | Written by | Purpose |
|---|---|---|---|---|
| `match-events` | 6 | `match_id` | replay producer | every match event ([schema](docs/event-schema.md)) |
| `match-control` | 1 | `match_id` | replay producer | kick_off / half_time / second_half_start / full_time |
| `match-alerts` | 3 | `match_id` | streaming (Dayana) | anomaly and momentum alerts |

## Data

Put raw data under `data/` (gitignored); see [`data/README.md`](data/README.md).

## Contracts

- [`docs/event-schema.md`](docs/event-schema.md): Kafka message formats
- [`docs/gold-contract.md`](docs/gold-contract.md): gold Delta tables (proposal)

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `OSError: [Errno 35] Resource deadlock avoided` in a container | macOS iCloud "Desktop & Documents" + "Optimise Mac Storage" turned files in `data/` into cloud placeholders; Docker cannot download them | The `make` targets download such files first. To avoid it entirely, keep the repo (or just `DATA_DIR_HOST` in `.env`) outside `~/Documents` / `~/Desktop` |
| `CERTIFICATE_VERIFY_FAILED` running `scripts/fetch_statsbomb.py` on the host | python.org Python on macOS has no CA certificates | Use `make fetch-data` (runs in Docker), or run *Install Certificates.command* from the Python folder in Applications |
| Streamlit says a gold table is not available | No gold tables in MinIO yet | `make dev-fixtures` (sample data) or Mihai's `make run-batch` |
| Producer log shows `GETPID ... Coordinator load in progress: retrying` | Brand-new Kafka still initialising | Harmless, it retries by itself |
| `make health` shows FAIL | that service is down | `docker logs <service>`, then `docker compose up -d <service>` |
| Port already in use | another program uses 8080/8501/9000/9094 | change the port in `.env` |
