#!/usr/bin/env bash
# Run the producer unit tests (pytest) inside the producer image.
source "$(dirname "$0")/_common.sh"
docker compose build -q replay-producer
docker compose run --rm --no-deps --entrypoint python replay-producer -m pytest -q -p no:cacheprovider
