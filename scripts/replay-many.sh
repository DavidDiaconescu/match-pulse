#!/usr/bin/env bash
# Replay several matches in parallel (one thread each) with a throughput log.
# Usage: scripts/replay-many.sh [speed] [matches file inside producer/]
source "$(dirname "$0")/_common.sh"
ensure_local_data
SPEED="${1:-60}"
FILE="${2:-matches/demo.txt}"
docker compose run --rm --build replay-producer --matches "$FILE" --speed "$SPEED"
