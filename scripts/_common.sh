#!/usr/bin/env bash
# Sourced by the other scripts: stop on errors and run from the repo root,
# so every script works no matter which folder it is called from.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# macOS + iCloud "Desktop & Documents" sync with "Optimise Mac Storage" can replace
# files by cloud placeholders ("dataless"). macOS apps download them on open, but
# Docker containers cannot and fail with "Resource deadlock avoided" (errno 35).
# Reading the placeholders once on the host forces the download.
ensure_local_data() {
  if [ "$(uname)" = "Darwin" ] && [ -d "${DATA_DIR_HOST:-data}" ]; then
    find "${DATA_DIR_HOST:-data}" -type f -flags dataless -exec cat {} + > /dev/null 2>&1 || true
  fi
}
