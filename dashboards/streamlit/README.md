# dashboards/streamlit/

**Owner:** David (person 1)

Streamlit app for historical analytics: http://localhost:8501. It reads the gold Delta
tables from MinIO with the `deltalake` Python package (delta-rs), without Spark.
Table contract: [`docs/gold-contract.md`](../../docs/gold-contract.md).

| File | What it does |
|---|---|
| `app.py` | Home page: what the app is, status and row count of each gold table |
| `pages/1_Team_profile.py` | Team season: W/D/L, goals vs xG, home vs away, vs competition average, table |
| `pages/2_Player_shot_map.py` | Player KPIs and shot map (mplsoccer, marker size = xG), shot list |
| `pages/3_League_trends.py` | League selector; goals per match by season, home vs away goals, result split |
| `data_access.py` | **The only module that knows table paths and S3 settings.** Change paths here |
| `common.py` | Shared UI helpers: load a table (with a dev-data banner / friendly error), selectors |
| `dev_fixtures.py` | Builds **sample** gold tables (dev data) from local StatsBomb files |

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `GOLD_URI` | `s3://lake/gold` | base path of the gold tables (can also be a local folder) |
| `S3_ENDPOINT` | `http://localhost:9000` (`http://minio:9000` in Docker) | MinIO S3 API |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` | MinIO credentials |

## Dev data

Until Mihai's gold tables exist, `make dev-fixtures` builds them from StatsBomb Open Data:
- **results** (`team_season_stats`, `league_trends`) from the match lists of the 23
  competition-seasons that are almost complete in the open data (e.g. the 2015/16 Premier
  League, La Liga, Serie A and Ligue 1, four FA WSL seasons, World Cups, Euros). Partial
  seasons such as "only Barcelona's matches" are skipped because their tables would be wrong.
- **shots, player stats, xG** from the downloaded event files (all 13 matches of Italy and
  England at Euro 2020, plus any other replayed match). xG and player stats are only filled in
  for teams whose every match is downloaded. Penalty shoot-outs are excluded.

Tables written by `dev_fixtures.py` carry "DEV FIXTURE" in their Delta description; the app
then shows a 🧪 **DEV DATA** banner. When Mihai writes the real tables to the same paths, the
banner disappears and nothing else changes.

## Run

```bash
make up              # starts MinIO + Streamlit (and Kafka)
make dev-fixtures    # sample gold tables, only needed once per MinIO volume
open http://localhost:8501
```

Locally without Docker (MinIO must be running):
```bash
pip install -r dashboards/streamlit/requirements.txt
cd dashboards/streamlit && streamlit run app.py
```

After rebuilding tables, press **C** in the app (clear cache) or wait 5 minutes.
