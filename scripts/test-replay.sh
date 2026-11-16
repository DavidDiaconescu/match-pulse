#!/usr/bin/env bash
# Check that the latest completed replay of a match is complete and in order.
# Usage: scripts/test-replay.sh <match_id> [--wait]
source "$(dirname "$0")/_common.sh"
ensure_local_data
MATCH="${1:?usage: scripts/test-replay.sh <match_id> [--wait]}"
shift
docker compose run --rm --build --entrypoint python replay-producer test_consumer.py --match "$MATCH" "$@"
