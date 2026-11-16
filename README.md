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

## Tutorial de pornire (pentru Mihai și Dayana)

Pașii de mai jos pornesc tot proiectul pe laptopul tău, de la zero. Durează ~15 minute
prima dată (se descarcă imaginile Docker), apoi ~1 minut.

### Pasul 1: instalează programele necesare (o singură dată)

**Mac**
1. Instalează [Docker Desktop](https://www.docker.com/products/docker-desktop/) și pornește-l
   (iconița cu balena trebuie să apară sus, lângă ceas).
2. `git` și `make` le ai deja; dacă terminalul spune că lipsesc, rulează `xcode-select --install`.

**Windows**
1. Instalează WSL2 cu Ubuntu: deschide PowerShell ca Administrator și rulează `wsl --install`,
   apoi repornește calculatorul și setează un user/parolă în Ubuntu.
2. Instalează [Docker Desktop](https://www.docker.com/products/docker-desktop/). În
   *Settings → Resources → WSL integration*, bifează **Ubuntu**.
3. Deschide **Ubuntu** (din meniul Start) și rulează: `sudo apt update && sudo apt install -y git make`.
4. **Toate comenzile de mai jos se rulează în terminalul Ubuntu**, nu în PowerShell/CMD.

**Ambele:** în Docker Desktop → *Settings → Resources*, dă-i Docker-ului cel puțin **6 GB RAM**
(Spark are nevoie de memorie).

Verifică instalarea:
```bash
docker --version          # trebuie să afișeze o versiune
docker compose version    # trebuie să fie v2.x
```

### Pasul 2: descarcă proiectul

```bash
cd ~                      # pe Windows: rămâi în folderul Ubuntu, NU în /mnt/c/...
git clone <URL-ul repo-ului de pe GitHub> match-pulse
cd match-pulse
```

> Pe Mac, nu pune proiectul în `Documents` sau `Desktop` dacă ai iCloud activ (vezi
> Troubleshooting). Pe Windows, proiectul în `/mnt/c/...` merge foarte încet; ține-l în `~`.

### Pasul 3: pornește platforma

```bash
make up          # pornește Kafka, Kafka UI, MinIO, Streamlit (prima dată durează câteva minute)
make health      # trebuie să vezi OK la fiecare serviciu
```

Deschide în browser:
- Kafka UI: http://localhost:8080 (topic-urile `match-events`, `match-control`, `match-alerts`)
- MinIO: http://localhost:9001 (user `minioadmin`, parolă `minioadmin`)
- Streamlit: http://localhost:8501

### Pasul 4: descarcă datele și trimite un meci în Kafka

```bash
make fetch-data                              # datele StatsBomb în data/ (o singură dată)
make replay-match MATCH=3788741 SPEED=60     # Turcia–Italia, ~2 minute; lasă-l să termine
make test-replay MATCH=3788741               # trebuie să afișeze RESULT: PASS
```

În timp ce rulează replay-ul, în Kafka UI → *Topics → match-events → Messages* vezi
mesajele cum apar. Formatul lor este descris în [`docs/event-schema.md`](docs/event-schema.md).

### Pasul 5: datele de test pentru Streamlit

```bash
make dev-fixtures     # scrie tabele gold de test în MinIO
```

Reîncarcă http://localhost:8501: paginile *Team profile*, *Player shot map* și *League trends*
arată date (cu bannerul „DEV DATA" până când există tabelele reale ale lui Mihai).

### Pasul 6: oprește tot

```bash
make down                 # oprește containerele, datele rămân
bash scripts/down.sh -v   # oprește și ȘTERGE datele din Kafka și MinIO (o luăm de la zero)
```

### Ce te interesează pe tine

| Cine | De unde citești / unde scrii | Ce comandă te ajută |
|---|---|---|
| **Dayana** (streaming) | din containere: Kafka la `kafka:9092`; de pe laptop: `localhost:9094`. Citești `match-events` + `match-control`, scrii în `match-alerts` | `make replay-match` / `make replay-many` ca să ai date care curg |
| **Mihai** (batch) | datele brute sunt în `data/` (montat ca `/data` în containere); MinIO la `minio:9000` (din containere) sau `localhost:9000`, bucket `lake`; tabelele gold merg în `s3://lake/gold/<tabel>` | `make load-history`, `make run-batch` apelează `batch/load_history.sh` și `batch/run_batch.sh` (le creezi tu) |

Serviciile voastre (Spark, Redis, Grafana) le adăugați în secțiunea voastră din
`docker-compose.yml`; `make health` le verifică automat. `make help` listează toate comenzile.

### Dacă ceva nu merge

- `Cannot connect to the Docker daemon` → Docker Desktop nu e pornit; pornește-l și așteaptă ~30 s.
- `make health` arată FAIL → `docker compose logs <serviciu>` și trimite-mi output-ul.
- `port is already allocated` → alt program folosește portul; `cp .env.example .env` și schimbă portul acolo.
- Pe Windows, `bash: ...\r: command not found` → ai clonat din Windows, nu din Ubuntu; clonează din nou din terminalul Ubuntu.
- Mai multe în tabelul *Troubleshooting* de mai jos.

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
