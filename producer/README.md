# producer/

**Owner:** David (person 1)

Replay producer that streams real StatsBomb Open Data match events into Kafka at
match speed, simulating a live feed.

- Reads one StatsBomb events JSON file per match from `data/` (or fetches it with
  `statsbombpy` if missing).
- Emits events to topic `match-events` (key = `match_id`) in the format defined in
  [`docs/event-schema.md`](../docs/event-schema.md).
- Emits lifecycle signals (kick_off, half_time, second_half_start, full_time) to
  topic `match-control`.

Status: built in Phase 2.
