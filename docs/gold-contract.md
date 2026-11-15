# Gold layer contract: tables read by the Streamlit app

> **PROPOSAL – to agree with Mihai.**
> Drafted by David from what the Streamlit pages need. Mihai owns the gold layer and has the
> final say on names, types and grain. Once agreed, remove this banner and treat changes as
> breaking (tell David before renaming or removing a column).

**Producer:** Mihai (Spark batch, `batch/`). **Consumer:** David (`dashboards/streamlit/`,
reading with the `deltalake` Python package, without Spark).

---

## 1. Location and access

| Item | Proposed value |
|---|---|
| Format | Delta Lake |
| Object store | MinIO (S3 API), bucket `lake` (env `LAKE_BUCKET`) |
| Table path | `s3://lake/gold/<table_name>` |
| Endpoint | `http://minio:9000` inside Docker, `http://localhost:9000` from the host |

How Streamlit reads a table (delta-rs storage options):

```python
from deltalake import DeltaTable

storage_options = {
    "AWS_ENDPOINT_URL": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",            # MinIO runs without TLS locally
}
df = DeltaTable("s3://lake/gold/team_season_stats", storage_options=storage_options).to_pandas()
```

The app reads tables only, never writes. Please keep Spark's default table features (no
deletion vectors, no column mapping) unless we have checked that delta-rs can read them.

## 2. Conventions (all tables)

- Column names are `snake_case`. Types below are **Spark SQL** types; Arrow/pandas types follow
  from them.
- `source` says which dataset a row came from: `"kaggle_esd"` (Kaggle European Soccer
  Database, 2008/09–2015/16, 11 leagues) or `"statsbomb"` (StatsBomb Open Data, selected
  competitions).
- `season` is a string. For club seasons it uses `"YYYY/YYYY"` (e.g. `"2015/2016"`), the format
  both datasets already use. For tournaments it is the year (e.g. `"2020"` for Euro 2020).
- `competition` is a human-readable name (e.g. `"England Premier League"`, `"UEFA Euro"`).
- IDs are strings (`team_id`, `player_id`, `match_id`) so ids from both datasets fit one column.
  They are only unique **within** a `source`.
- Every table has `updated_at TIMESTAMP` (when the batch job wrote the row).
- Nullable = `yes` means the value is legitimately missing for some rows, usually because one
  of the two sources doesn't have it.

---

## 3. `team_season_stats`

**Grain:** one row per `(source, competition, season, team_id)`.
**Partitioned by:** `source`.
**Used by:** page 1 (Team profile by season).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `source` | STRING | no | `kaggle_esd` or `statsbomb` |
| `competition` | STRING | no | league or tournament name |
| `country` | STRING | yes | e.g. `England`; `null` for international tournaments |
| `season` | STRING | no | see conventions |
| `team_id` | STRING | no | team id in the source dataset |
| `team_name` | STRING | no | display name |
| `matches_played` | INT | no | |
| `wins` | INT | no | |
| `draws` | INT | no | |
| `losses` | INT | no | |
| `points` | INT | no | 3 per win, 1 per draw |
| `goals_for` | INT | no | |
| `goals_against` | INT | no | |
| `goal_difference` | INT | no | `goals_for - goals_against` |
| `home_matches` | INT | no | |
| `home_wins` | INT | no | |
| `home_goals_for` | INT | no | |
| `home_goals_against` | INT | no | |
| `away_matches` | INT | no | |
| `away_wins` | INT | no | |
| `away_goals_for` | INT | no | |
| `away_goals_against` | INT | no | |
| `league_position` | INT | yes | final table position; `null` for tournaments |
| `shots_for` | INT | yes | StatsBomb only |
| `shots_against` | INT | yes | StatsBomb only |
| `xg_for` | DOUBLE | yes | sum of `shot.statsbomb_xg`; StatsBomb only |
| `xg_against` | DOUBLE | yes | StatsBomb only |
| `updated_at` | TIMESTAMP | no | |

## 4. `player_season_stats`

**Grain:** one row per `(source, competition, season, team_id, player_id)`. A player who
changed team mid-season has one row per team.
**Partitioned by:** none (small table).
**Used by:** page 2 (Player page: KPI cards next to the shot map).
**Source:** StatsBomb events. Kaggle's per-match goal data is embedded XML and is not needed in v1.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `source` | STRING | no | `statsbomb` |
| `competition` | STRING | no | |
| `season` | STRING | no | |
| `team_id` | STRING | no | |
| `team_name` | STRING | no | |
| `player_id` | STRING | no | StatsBomb `player.id` as a string |
| `player_name` | STRING | no | |
| `primary_position` | STRING | yes | most frequent `position.name` |
| `matches_played` | INT | no | matches with at least one event by the player |
| `minutes_played` | INT | yes | from lineups and substitutions, if Mihai computes it |
| `goals` | INT | no | shots with outcome `Goal` (excludes own goals) |
| `non_penalty_goals` | INT | no | |
| `shots` | INT | no | |
| `shots_on_target` | INT | no | outcome in `Goal`, `Saved`, `Saved to Post` |
| `xg` | DOUBLE | no | sum of `shot.statsbomb_xg` |
| `non_penalty_xg` | DOUBLE | no | excludes `shot.type.name = "Penalty"` |
| `assists` | INT | no | passes with `pass.goal_assist = true` |
| `key_passes` | INT | no | passes with `pass.shot_assist = true` or `goal_assist = true` |
| `passes_attempted` | INT | no | |
| `passes_completed` | INT | no | passes with no `pass.outcome` |
| `xg_per_90` | DOUBLE | yes | `xg / minutes_played * 90`; `null` if minutes unknown |
| `updated_at` | TIMESTAMP | no | |

## 5. `league_trends`

**Grain:** one row per `(source, competition, season)`.
**Partitioned by:** none.
**Used by:** page 3 (League trends: home vs away, goals per season, league selector).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `source` | STRING | no | mainly `kaggle_esd` |
| `competition` | STRING | no | |
| `country` | STRING | yes | |
| `season` | STRING | no | |
| `matches` | INT | no | |
| `total_goals` | INT | no | |
| `avg_goals_per_match` | DOUBLE | no | |
| `avg_home_goals` | DOUBLE | no | |
| `avg_away_goals` | DOUBLE | no | |
| `home_win_pct` | DOUBLE | no | share of matches, 0–1 (not 0–100) |
| `draw_pct` | DOUBLE | no | 0–1 |
| `away_win_pct` | DOUBLE | no | 0–1; the three pct columns sum to 1 |
| `updated_at` | TIMESTAMP | no | |

## 6. `shots` (shot-level, for shot maps)

**Grain:** one row per StatsBomb `Shot` event.
**Partitioned by:** `competition`, `season`.
**Used by:** page 2 (shot map on a pitch; filter by `player_id`, plus `season`/`competition`).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `source` | STRING | no | `statsbomb` |
| `competition` | STRING | no | |
| `season` | STRING | no | |
| `match_id` | STRING | no | |
| `match_date` | DATE | no | from StatsBomb matches metadata |
| `event_id` | STRING | no | StatsBomb event UUID (same as `event_id` on Kafka) |
| `team_id` | STRING | no | |
| `team_name` | STRING | no | |
| `opponent_name` | STRING | no | |
| `player_id` | STRING | no | |
| `player_name` | STRING | no | |
| `period` | INT | no | |
| `minute` | INT | no | |
| `second` | INT | no | |
| `x` | DOUBLE | no | `location[0]`, StatsBomb 120×80 yards, shooter attacks towards x = 120 |
| `y` | DOUBLE | no | `location[1]` |
| `end_x` | DOUBLE | no | `shot.end_location[0]` |
| `end_y` | DOUBLE | no | `shot.end_location[1]` |
| `end_z` | DOUBLE | yes | `shot.end_location[2]` (height), missing for some shots |
| `xg` | DOUBLE | no | `shot.statsbomb_xg` |
| `outcome` | STRING | no | `shot.outcome.name` |
| `is_goal` | BOOLEAN | no | `outcome = "Goal"` |
| `shot_type` | STRING | no | `shot.type.name`: `Open Play`, `Penalty`, `Free Kick`, `Corner` |
| `body_part` | STRING | no | `shot.body_part.name` |
| `technique` | STRING | no | `shot.technique.name` |
| `play_pattern` | STRING | no | `play_pattern.name` |
| `under_pressure` | BOOLEAN | no | missing in raw = `false` |
| `updated_at` | TIMESTAMP | no | |

Coordinates are kept **as StatsBomb provides them**, with no flipping. `mplsoccer`'s
`Pitch(pitch_type="statsbomb")` uses the same system, so the app draws them directly.

---

## 7. Open questions for Mihai

1. **Team names across datasets.** Kaggle and StatsBomb can spell the same club differently
   (e.g. `"FC Barcelona"` vs `"Barcelona"`). v1 proposal: no cross-source mapping, and the
   app always filters by `source`. Do we want a `dim_team` mapping table later?
2. **Which StatsBomb competitions** does the batch layer load? The app's season and
   competition selectors are filled from whatever is in the tables, so any choice works.
   Shot maps need enough matches per player to be interesting (e.g. a full La Liga season).
3. **`minutes_played`**: computing it from lineups and substitution events is extra work.
   It's fine to leave it `null` in v1; the app then hides per-90 numbers.
4. **Paths and bucket name**: OK with `s3://lake/gold/<table>`?
5. **Refresh**: full overwrite per batch run is fine for the app (it caches for a few minutes).
