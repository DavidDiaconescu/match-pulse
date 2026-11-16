#!/usr/bin/env bash
# Download StatsBomb Open Data (competitions, match lists, demo match events) into data/.
# Runs inside the producer image, so it works without a local Python setup.
source "$(dirname "$0")/_common.sh"
docker compose run --rm --build --no-deps --entrypoint python replay-producer /scripts/fetch_statsbomb.py --demo "$@"
