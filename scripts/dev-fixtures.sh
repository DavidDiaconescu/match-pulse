#!/usr/bin/env bash
# Build SAMPLE gold tables (dev data) in MinIO from the local StatsBomb files.
source "$(dirname "$0")/_common.sh"
ensure_local_data
if [ ! -f data/statsbomb/competitions.json ]; then
  echo "No StatsBomb data yet, downloading it first..."
  scripts/fetch-data.sh
fi
docker compose run --rm --build streamlit python dev_fixtures.py
