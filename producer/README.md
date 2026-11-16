# producer/

**Owner:** David (person 1)

Replay producer: streams real StatsBomb Open Data match events into Kafka at match
speed, simulating a live feed. Message formats: [`docs/event-schema.md`](../docs/event-schema.md).

## Files

| File | What it does |
|---|---|
| `replay.py` | CLI and Kafka sending: replays one or more matches, logs progress and throughput |
| `schedule.py` | Pure logic: event order `(period, index)`, waits between events, half-time breaks, control signals |
| `transform.py` | Pure logic: raw StatsBomb event → `match-events` message |
| `loader.py` | Reads `data/statsbomb/events/<id>.json`, downloads it with `statsbombpy` if missing, finds home/away teams |
| `test_consumer.py` | Reads a replayed match back from Kafka and checks count, order, signals and schema (PASS/FAIL) |
| `matches/demo.txt` | Match ids for `make replay-many` |
| `tests/` | pytest unit tests with a 19-event hand-made fixture match (`tests/fixtures/9999.json`) |

## How a replay works

1. Load the match's events and sort them by `(period, index)`.
2. Build a schedule: before each event, wait for the gap in the period clock since the
   latest event so far. Before each new period, wait for the break (15 min at half-time,
   5 min before extra time, ...). Control signals go at the start and end of every period.
3. For each step, sleep `wait / speed` seconds, then send it. Events go to `match-events`;
   `kick_off`, `half_time`, `second_half_start` and `full_time` go to `match-control`. The key
   is always `match_id`, so each match stays in order within one partition.
4. Before `full_time`, the producer flushes, so every event is delivered before the
   end-of-match signal.

Each replay gets a new `replay_id` (UUID), so two replays of the same match never mix.

## Usage

With Docker (no local Python needed):

```bash
docker compose run --rm replay-producer --matches 3788741 --speed 60
docker compose run --rm replay-producer --matches matches/demo.txt --speed 60
docker compose run --rm --entrypoint python replay-producer test_consumer.py --match 3788741
docker compose run --rm --entrypoint python replay-producer -m pytest -q
```

From the host (Python 3.11+):

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r producer/requirements.txt
python producer/replay.py --matches 3788741 --speed 60          # uses localhost:9094
python producer/test_consumer.py --match 3788741
cd producer && pytest -q
```

| Option | Meaning |
|---|---|
| `--matches` | one or more match ids and/or files with one id per line (`#` starts a comment) |
| `--speed` | `1` = real time, `60` = one match minute per second, `0` = as fast as possible (default `60`) |
| `--bootstrap` | Kafka address; default `$KAFKA_BOOTSTRAP`, else `localhost:9094` |
| `--skip-breaks` | don't wait during half-time and the other breaks |

Ctrl+C (or `docker stop`) stops after the current message, flushes, and prints a summary.
