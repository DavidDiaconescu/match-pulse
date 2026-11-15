# scripts/

**Owner:** shared (maintained by David)

Helper shell scripts used by Docker Compose and by teammates on Windows/WSL who
don't use `make`.

| Script | Purpose |
|---|---|
| `create-topics.sh` | Run by the `kafka-init` container at startup: waits for the broker, then creates `match-events`, `match-control` and `match-alerts` (safe to re-run) |
