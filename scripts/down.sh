#!/usr/bin/env bash
# Stop the stack. Extra arguments are passed on, e.g. `scripts/down.sh -v` also deletes data volumes.
source "$(dirname "$0")/_common.sh"
docker compose --profile tools down "$@"
