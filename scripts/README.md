# scripts/

**Owner:** shared (maintained by David)

Each `make` target calls one of these scripts, so Windows/WSL users without `make` can run
`bash scripts/<name>.sh` instead. All scripts run from the repo root, wherever you call them from.

| Script | make target | Purpose |
|---|---|---|
| `up.sh` | `make up` | start the stack (build images if needed), wait until healthy, print URLs |
| `down.sh` | `make down` | stop the stack; `scripts/down.sh -v` also deletes Kafka and MinIO data |
| `topics.sh` | `make topics` | (re)create the Kafka topics and list them |
| `fetch-data.sh` | `make fetch-data` | download StatsBomb data into `data/` (runs `fetch_statsbomb.py` in Docker) |
| `replay-match.sh <id> [speed]` | `make replay-match MATCH=.. SPEED=..` | replay one match |
| `replay-many.sh [speed] [file]` | `make replay-many` | replay `producer/matches/demo.txt` in parallel, throughput log |
| `test-replay.sh <id> [--wait]` | `make test-replay MATCH=..` | verify a replay in Kafka (PASS/FAIL) |
| `test.sh` | `make test` | producer unit tests (pytest) in Docker |
| `dev-fixtures.sh` | `make dev-fixtures` | sample gold tables in MinIO for Streamlit |
| `load-history.sh` | `make load-history` | calls Mihai's `batch/load_history.sh` ("not implemented yet" until it exists) |
| `run-batch.sh` | `make run-batch` | calls Mihai's `batch/run_batch.sh` ("not implemented yet" until it exists) |
| `healthcheck.sh` | `make health` | OK/FAIL per compose service; exit code 1 if anything fails |
| `create-topics.sh` | (used by `kafka-init`) | creates the topics inside the Kafka container |
| `fetch_statsbomb.py` | (used by `fetch-data.sh`) | standard-library downloader for StatsBomb Open Data |
| `_common.sh` | (sourced by all) | `set -euo pipefail`, go to the repo root, macOS iCloud fix |
