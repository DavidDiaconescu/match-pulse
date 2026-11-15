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
cp .env.example .env          # optional: change ports or image tags here
docker compose up -d          # Kafka (KRaft) + Kafka UI + topic init
docker compose ps             # kafka should be "healthy", kafka-init "exited (0)"
```

Then open Kafka UI at http://localhost:8080. You should see three topics:
`match-events`, `match-control` and `match-alerts`.

Stop everything with `docker compose down`, or `docker compose down -v` to also delete Kafka data.

## Services and ports

| Service | URL / address | Notes |
|---|---|---|
| Kafka (from containers) | `kafka:9092` | use this inside Docker Compose |
| Kafka (from your host) | `localhost:9094` | use this for scripts run in your terminal |
| Kafka UI | http://localhost:8080 | browse topics and messages |

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
