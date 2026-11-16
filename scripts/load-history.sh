#!/usr/bin/env bash
# Load historical data into the bronze layer: delegates to Mihai's batch/load_history.sh.
source "$(dirname "$0")/_common.sh"
if [ -x batch/load_history.sh ]; then
  exec batch/load_history.sh "$@"
fi
echo "load-history: not implemented yet."
echo "  Owner: Mihai. Expected entry point: batch/load_history.sh (see batch/README.md)."
