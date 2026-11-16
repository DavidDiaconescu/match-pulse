#!/usr/bin/env bash
# Replay one match into Kafka.   Usage: scripts/replay-match.sh <match_id> [speed]
source "$(dirname "$0")/_common.sh"
ensure_local_data
MATCH="${1:?usage: scripts/replay-match.sh <match_id> [speed]}"
SPEED="${2:-60}"
docker compose run --rm --build replay-producer --matches "$MATCH" --speed "$SPEED"
