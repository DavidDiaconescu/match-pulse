# MatchPulse demo script (David's part, ~10 minutes)

What the audience should take away: **real match data flows through Kafka as a live
feed, in order and complete (proven by a test), it scales to several matches at once, and
the batch side turns the same data into season analytics.**

Commands run from the repo root. `make <target>` is the same as `bash scripts/<target>.sh`
(Windows/WSL users without make).

---

## Before the demo (T-30 min, not shown)

| # | Do | Expected |
|---|---|---|
| 1 | Start Docker Desktop (≥ 6 GB RAM) | whale icon steady |
| 2 | `make fetch-data` (only if `data/statsbomb/` is empty) | "events ...: already present" or "downloaded" |
| 3 | `make up` | all containers healthy, URLs printed |
| 4 | `make dev-fixtures` (only needed once per MinIO volume; skip if Mihai's gold tables exist) | 4 tables written |
| 5 | `make health` | "All checks passed." |
| 6 | `make test` | `26 passed` |
| 7 | Open browser tabs: Kafka UI http://localhost:8080 → Topics; Streamlit http://localhost:8501 (page 1 already loaded once, so the cache is warm) | pages load |
| 8 | Two terminal windows side by side, font size large | |

Optional clean slate (empty topics): `scripts/down.sh -v && make up && make dev-fixtures`.
This also wipes MinIO, which is why dev-fixtures runs again.

---

## 1. Architecture (1 min): README diagram

**Say:** "MatchPulse is a Lambda architecture. Speed path: a replay producer streams real
StatsBomb events into Kafka at match speed; Dayana's Spark Streaming job turns them into live
xG and alerts in Redis and Grafana. Batch path: Mihai's Spark jobs build a Delta Lake on MinIO;
my Streamlit app reads its gold tables. My part: Kafka, the replay producer, the contracts
between us, the workflows and Streamlit."

**Show:** the diagram in `README.md`, then `docs/event-schema.md` §2 (one message, field mapping).

## 2. Everything is up (30 s)

```bash
make health
```

**Expected:** one green OK per service, "All checks passed."
**Fallback:** a FAIL line tells you which service; `docker compose up -d <service>`, or
`docker logs <service>`. If Kafka is the problem, `make down && make up` (about 30 s).

## 3. Live replay of one match (2.5 min)

Terminal 1:
```bash
make replay-match MATCH=3788741 SPEED=60
```
Terminal 2 (right away, it waits for the match to finish):
```bash
scripts/test-replay.sh 3788741 --wait
```

**Say while it runs:** "Turkey vs Italy, Euro 2020 opening match, 3,803 events. 60x speed:
one match minute per second, so the 15-minute half-time takes 15 seconds. The gaps between
messages are the real gaps between events."

**Show in Kafka UI** → Topics → `match-events` → Messages (live mode):
- the **key** is the match id, so every message of this match goes to the same partition (order)
- open a `Shot` message: `xg`, `outcome`, `location`, `emitted_at`
- `match-control`: `kick_off`, later `half_time` (log line `half_time (P1 45:55)` after about 46 s)

**Expected after ~110 s:** terminal 1 `done: 3803 events sent ... 0 failed`; terminal 2 a
report with 7 × PASS and `RESULT: PASS` (count = JSON file, no duplicates, same order,
control signals in order, JSON Schema valid).

**Fallback:** if the replay is slow to start, Kafka is still warming up (a `GETPID ... retrying`
line is harmless). If `--wait` times out, run `make test-replay MATCH=3788741` after the
replay ends (it checks the latest completed replay).

## 4. Scale: three matches at once (1.5 min)

```bash
make replay-many
```

**Say:** "One thread per match, all sharing one idempotent Kafka producer. Every 5 seconds
it logs throughput: events/second, total events, progress per match."

**Show:** the `throughput:` lines (~100 events/s total at 60x, 0 during half-time).
After ~30 s press **Ctrl+C**: "Graceful stop: it finishes the current message, flushes, and
prints a summary: nothing is lost or half-sent."

**Fallback:** if a match fails, the others continue and the failure is listed at the end.
`make replay-many SPEED=600` gets a full run in ~15 s.

## 5. Batch analytics in Streamlit (3 min): http://localhost:8501

1. **Home:** the four gold tables with row counts. "It reads Delta tables directly from MinIO
   with delta-rs, no Spark cluster needed for a dashboard." Point out the 🧪 dev-data
   label: "Until Mihai's batch output lands, these are sample tables with the same contract,
   built from StatsBomb data. When his tables arrive, nothing in the app changes."
2. **Team profile:** opens on Italy, Euro 2020: 5W 2D 0L, 13 goals from 10.1 xG ("they
   finished better than expected"), home vs away, comparison with the tournament average.
   Then switch to *Premier League 2015/2016 → Leicester City*: position 1, 81 points.
3. **Player shot map:** *UEFA Euro 2020 → England → Harry Kane*: 16 shots, 4 goals,
   3.35 xG. "Each circle is a shot, size = xG; the big hollow one on the penalty spot is the
   saved penalty against Denmark, and the filled one next to it is the rebound goal." Penalty
   shoot-out kicks are excluded, as in official statistics.
4. **League trends:** select Premier League, La Liga, Serie A, Ligue 1: 2.6 goals per match,
   44% home wins vs 29% away wins: "home advantage, visible in the data."

**Fallback:** red "table not available" message → `make dev-fixtures`, then press **C** in
the app (clear cache) and reload. If Streamlit is down: `docker compose up -d streamlit`.

## 6. Wrap-up (30 s)

**Say:** "Next steps: Dayana's streaming job consumes these same topics, and its alerts go to
`match-alerts`, which already exists. Mihai's gold tables replace the dev fixtures through the
contract in `docs/gold-contract.md`. All of it starts with one `make up`."

---

## Timing summary

| Step | Minutes |
|---|---|
| 1. Architecture | 1 |
| 2. Health | 0.5 |
| 3. Live replay + test | 2.5 |
| 4. Parallel replay | 1.5 |
| 5. Streamlit | 3 |
| 6. Wrap-up | 0.5 |
| Buffer for questions/fallbacks | 1 |
| **Total** | **10** |

## If everything breaks

`make down && make up && make health`: about 1 minute. Keep screenshots of each Streamlit
page and of a PASS test report in `docs/` as a last resort.
