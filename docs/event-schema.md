# Kafka message contract: `match-events` and `match-control`

**Owner:** David (producer). **Consumers:** Dayana (streaming), anyone reading the topics.
**Schema version:** 1

Machine-readable versions:
[`schemas/match-event.schema.json`](schemas/match-event.schema.json),
[`schemas/match-control.schema.json`](schemas/match-control.schema.json) (JSON Schema draft 2020-12).

If you need a change, ask first. Adding a new **optional** field is non-breaking. Renaming,
removing or changing the type of a field is breaking and bumps `schema_version`.

---

## 1. Topics

| Topic | Partitions | Message key | Value | Written by |
|---|---|---|---|---|
| `match-events` | 6 | `match_id` (UTF-8 string) | one match event, JSON (§2) | replay producer |
| `match-control` | 1 | `match_id` (UTF-8 string) | one lifecycle signal, JSON (§3) | replay producer |
| `match-alerts` | 3 | `match_id` (UTF-8 string) | defined by Dayana | streaming job |

Bootstrap servers: `kafka:9092` inside Docker Compose, `localhost:9094` from the host.

**Encoding:** values are UTF-8 JSON objects, one per message, no envelope. Player and team
names keep their original Unicode characters (e.g. `"Uğurcan Çakır"`).

### Ordering guarantees

- Every message for a match uses the same key (`match_id`), so it goes to the same partition.
  Kafka preserves order within a partition, so **consumers see one match's events in the
  order the producer sent them.** The producer sends with idempotence enabled, so retries
  cannot reorder or duplicate messages.
- The send order is: sort by `period`, then `index` (StatsBomb's own sequence number).
  We do **not** sort by `timestamp`, because the source has occasional wrong timestamps (§5).
- Different matches can share a partition (6 partitions, many matches). Always group or filter
  by `match_id`, never by partition.
- **There is no ordering between `match-events` and `match-control`**: they are separate
  topics. The producer sends `kick_off` before a match's first event and `full_time` after
  its last event has been delivered, but a consumer reading both topics can still see them
  interleaved differently. For exact positions inside the match, use the `Half Start` /
  `Half End` events in `match-events`.

---

## 2. `match-events` value

Example: a real shot from Turkey vs Italy, Euro 2020 (StatsBomb match `3788741`):

```json
{
  "schema_version": 1,
  "match_id": "3788741",
  "event_id": "fc81639c-6e60-49d3-b29a-82b2b8c5746d",
  "replay_id": "806b8853-bd76-49b0-ac41-cb0fe2e2b1ea",
  "index": 106,
  "period": 1,
  "timestamp": "00:02:09.222",
  "clock_seconds": 129.222,
  "minute": 2,
  "second": 9,
  "type": "Shot",
  "team": "Italy",
  "team_id": 914,
  "player": "Ciro Immobile",
  "player_id": 7788,
  "position": "Center Forward",
  "possession": 8,
  "possession_team": "Italy",
  "play_pattern": "From Throw In",
  "location": [114.6, 50.1],
  "end_location": [120.0, 44.8, 0.2],
  "under_pressure": false,
  "xg": 0.05223599,
  "outcome": "Off T",
  "is_goal": false,
  "card": null,
  "emitted_at": "2026-11-15T19:54:01.123Z"
}
```

### Field reference and mapping from raw StatsBomb

"Raw" refers to one object in StatsBomb's `data/events/<match_id>.json`.
**Required** fields are always present (the value may still be `null` where the type says so).

| Field | Type | Req. | Raw StatsBomb source | Notes |
|---|---|---|---|---|
| `schema_version` | int | yes | (none, set by producer) | currently `1` |
| `match_id` | string | yes | file name `<match_id>.json` | StatsBomb ids are integers; we send them as **strings**, same as the key |
| `event_id` | string (UUID) | yes | `id` | unique across all matches; use it to de-duplicate |
| `replay_id` | string (UUID) | no | (none, set by producer) | one id per replay run of a match, the same on its events and control signals. Replaying a match twice gives two `replay_id`s, so consumers can keep the runs apart |
| `index` | int | yes | `index` | StatsBomb's sequence number in the match, starting at 1 |
| `period` | int | yes | `period` | 1, 2 = halves; 3, 4 = extra time; 5 = penalty shoot-out |
| `timestamp` | string | yes | `timestamp` | **period clock** `HH:MM:SS.mmm`; resets to `00:00:00.000` at the start of each period |
| `clock_seconds` | number | yes | derived from `timestamp` | seconds since the start of the period (e.g. `"00:02:09.222"` → `129.222`) |
| `minute` | int | yes | `minute` | **match** minute, which does not reset: period 2 starts at `45`, period 3 at `90` |
| `second` | int | yes | `second` | 0–59, second within `minute` |
| `type` | string | yes | `type.name` | e.g. `Pass`, `Shot`, `Carry`, `Pressure`, `Ball Receipt*` (the `*` is part of StatsBomb's name) |
| `team` | string | yes | `team.name` | team that performed the event |
| `team_id` | int | yes | `team.id` | |
| `player` | string \| null | yes | `player.name` | `null` for team-level events: `Half Start`, `Half End`, `Starting XI`, `Tactical Shift`, `Own Goal For` |
| `player_id` | int \| null | yes | `player.id` | `null` when `player` is `null` |
| `position` | string \| null | yes | `position.name` | player's position at the time of the event |
| `possession` | int | yes | `possession` | possession sequence number within the match |
| `possession_team` | string | yes | `possession_team.name` | team in possession (can differ from `team`, e.g. for a `Pressure`) |
| `play_pattern` | string | yes | `play_pattern.name` | e.g. `Regular Play`, `From Corner`, `From Throw In` |
| `location` | [x, y] \| null | yes | `location` | pitch coordinates (§4); `null` for e.g. `Substitution`, `Half Start`, `Half End`, `Starting XI` |
| `end_location` | array \| null | yes | `pass.end_location`, `carry.end_location`, `shot.end_location`, `goalkeeper.end_location` | `[x, y]`, or `[x, y, z]` for shots (z = height in yards); `null` for other types |
| `under_pressure` | bool | yes | `under_pressure` | raw field is missing when false; we always send `true`/`false` |
| `xg` | number \| null | yes | `shot.statsbomb_xg` | **shots only**; `null` for every other type. Range 0–1 |
| `outcome` | string \| null | yes | `<sub-object>.outcome.name` (table below) | `null` if the event type has no outcome |
| `is_goal` | bool | yes | derived | `true` when `type = "Shot"` and `outcome = "Goal"`, **or** `type = "Own Goal For"`. Summing `is_goal` per `team` gives the score |
| `card` | string \| null | yes | `foul_committed.card.name`, `bad_behaviour.card.name` | `Yellow Card`, `Second Yellow`, `Red Card`, otherwise `null` |
| `emitted_at` | string | yes | (none, set by producer) | **wall-clock** time the producer sent the message, ISO-8601 UTC with milliseconds, e.g. `2026-11-15T19:54:01.123Z`. Use it for latency and processing-time windows; use `period` + `clock_seconds` for match time |

### Where `outcome` comes from

In raw StatsBomb the outcome sits inside a type-specific sub-object, and the sub-object's
name does not always follow from the type name, so the producer uses this explicit mapping:

| `type` | Raw sub-object | Example `outcome` values |
|---|---|---|
| `Pass` | `pass` | `Incomplete`, `Out`, `Pass Offside`, `Injury Clearance`, `Unknown`, **`Complete`** (*) |
| `Ball Receipt*` | `ball_receipt` | `Incomplete`, **`Complete`** (*) |
| `Shot` | `shot` | `Goal`, `Saved`, `Off T`, `Blocked`, `Wayward`, `Post`, `Saved Off Target`, `Saved to Post` |
| `Duel` | `duel` | `Won`, `Lost In Play`, `Lost Out`, `Success In Play`, `Success Out` |
| `Dribble` | `dribble` | `Complete`, `Incomplete` |
| `Interception` | `interception` | `Won`, `Lost In Play`, `Success In Play`, ... |
| `Goal Keeper` | `goalkeeper` | `Success`, `Claim`, `Clear`, `In Play Safe`, ... |
| `50/50` | `50_50` | `Won`, `Lost`, `Success To Team`, ... |
| `Substitution` | `substitution` | `Tactical`, `Injury` |
| all other types | (none) | `null` |

(*) StatsBomb **leaves out** `outcome` for successful passes and ball receipts. The producer
fills these in as `"Complete"` so consumers don't have to know this rule. For all other
types, a missing outcome is sent as `null`.

### Raw fields deliberately not forwarded

`duration`, `related_events`, `tactics` (lineups), `shot.freeze_frame` and the remaining
type-specific details (pass height, body part, technique, ...) are not sent, to keep messages
small. The batch layer reads them from the original JSON. If the streaming job needs one of
them, ask: adding an optional field is non-breaking.

---

## 3. `match-control` value

One message per lifecycle signal, keyed by `match_id`.

```json
{
  "schema_version": 1,
  "match_id": "3788741",
  "replay_id": "806b8853-bd76-49b0-ac41-cb0fe2e2b1ea",
  "signal": "half_time",
  "period": 1,
  "minute": 45,
  "second": 55,
  "home_team": "Turkey",
  "away_team": "Italy",
  "events_emitted": 2003,
  "emitted_at": "2026-11-15T20:39:56.004Z"
}
```

| Field | Type | Req. | Meaning |
|---|---|---|---|
| `schema_version` | int | yes | `1` |
| `match_id` | string | yes | same as the key |
| `replay_id` | string (UUID) | no | same `replay_id` as the replay's events |
| `signal` | string | yes | one of `kick_off`, `half_time`, `second_half_start`, `full_time` |
| `period` | int | yes | the period that is starting (`kick_off`, `second_half_start`) or ending (`half_time`, `full_time`) |
| `minute`, `second` | int | yes | match clock when the signal applies (taken from the `Half Start` / `Half End` event) |
| `home_team`, `away_team` | string \| null | yes | from StatsBomb match metadata when available, otherwise `null` (the events file alone does not say which team is at home) |
| `events_emitted` | int | yes | number of `match-events` messages already sent for this match. On `full_time` it is the **total**, which lets a consumer check it received everything |
| `emitted_at` | string | yes | wall-clock ISO-8601 UTC with milliseconds |

When each signal is sent:

| Signal | Sent | Raw StatsBomb anchor |
|---|---|---|
| `kick_off` | before the first event of period 1 | `Half Start`, period 1 |
| `half_time` | after the last event of period 1 | `Half End`, period 1 |
| `second_half_start` | before the first event of period 2 | `Half Start`, period 2 |
| `full_time` | after the last event of the match (once all events are delivered) | last `Half End` |

**Extra time (periods 3–5):** `half_time` is sent at the end of **every** period except the
last, `second_half_start` at the start of every period after the first, and `period` says
which period. `full_time` is always sent exactly once, at the very end.

StatsBomb records `Half Start` and `Half End` once **per team** (so twice per period). The
producer sends one control signal per period, not one per raw event. The raw events
themselves are still forwarded to `match-events`.

---

## 4. Pitch coordinates

StatsBomb coordinates are in yards on a **120 × 80** pitch. The origin `(0, 0)` is the top-left
corner, `x` increases towards the opponent's goal, and `y` increases downwards. Every event is
recorded **from the perspective of the team performing it**: that team always attacks
left → right (towards `x = 120`). The goal being attacked is centred at `(120, 40)`, posts at
`y = 36` and `y = 44`.

---

## 5. Known data issues

- **Occasional bad timestamps in the source.** In match `3788741`, event `index` 2001 (a
  `Ball Receipt*` completing the pass at `00:45:53.614`) has `timestamp = "00:00:00.189"` and
  `minute = 0`. Across the 13 Euro 2020 matches we downloaded there are 10 such backward
  jumps. **Decision:** the producer sends events in `(period, index)` order, so the event stays
  where it belongs, and forwards its timestamp unchanged (we don't invent data). For pacing,
  the producer measures each wait from the latest clock seen so far in the period, so a wrong
  timestamp causes neither a negative nor a huge wait. **Consumers must not assume
  `clock_seconds` never goes backwards within a period**; use `minute`/`second` of
  neighbouring events, or ignore such events for time-based windows.
- **Same-timestamp ties are common** (~30% of consecutive events share a timestamp, e.g. a
  `Pass` and its `Ball Receipt*`). `index` breaks the tie.
