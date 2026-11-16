# MatchPulse shortcuts. Every target just calls a script in scripts/, so Windows/WSL
# users without make can run the same thing: e.g. `make replay-match MATCH=3788741`
# is `bash scripts/replay-match.sh 3788741 60`.

MATCH ?= 3788741
SPEED ?= 60
MATCHES_FILE ?= matches/demo.txt

.DEFAULT_GOAL := help
.PHONY: help up down topics fetch-data replay-match replay-many test-replay test \
        dev-fixtures load-history run-batch health

help: ## show this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'

up: ## start the whole stack and wait until it is healthy
	@scripts/up.sh

down: ## stop the stack (keeps data; `scripts/down.sh -v` deletes it)
	@scripts/down.sh

topics: ## (re)create the Kafka topics and list them
	@scripts/topics.sh

fetch-data: ## download StatsBomb competitions, match lists and demo matches into data/
	@scripts/fetch-data.sh

replay-match: ## replay one match: make replay-match MATCH=3788741 SPEED=60
	@scripts/replay-match.sh $(MATCH) $(SPEED)

replay-many: ## replay the demo matches in parallel with a throughput log (SPEED=60)
	@scripts/replay-many.sh $(SPEED) $(MATCHES_FILE)

test-replay: ## check a replayed match in Kafka: make test-replay MATCH=3788741
	@scripts/test-replay.sh $(MATCH)

test: ## run the producer unit tests (pytest, in Docker)
	@scripts/test.sh

dev-fixtures: ## build SAMPLE gold tables in MinIO for the Streamlit app
	@scripts/dev-fixtures.sh

load-history: ## Mihai: load historical data into bronze (batch/load_history.sh)
	@scripts/load-history.sh

run-batch: ## Mihai: build silver and gold (batch/run_batch.sh)
	@scripts/run-batch.sh

health: ## check every service and print OK/FAIL
	@scripts/healthcheck.sh
