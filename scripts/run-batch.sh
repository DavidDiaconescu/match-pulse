#!/usr/bin/env bash
# Build silver and gold tables: delegates to Mihai's batch/run_batch.sh.
source "$(dirname "$0")/_common.sh"
if [ -x batch/run_batch.sh ]; then
  exec batch/run_batch.sh "$@"
fi
echo "run-batch: not implemented yet."
echo "  Owner: Mihai. Expected entry point: batch/run_batch.sh (see batch/README.md)."
echo "  For sample gold tables in the meantime: make dev-fixtures"
