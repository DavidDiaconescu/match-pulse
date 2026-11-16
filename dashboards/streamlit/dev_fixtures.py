"""Build SAMPLE gold tables (DEV DATA) from local StatsBomb files.

Mihai's real gold tables may not exist yet. This script builds small tables with the
exact columns and types of docs/gold-contract.md, so the Streamlit app can be developed
and demoed. Every table it writes is marked "DEV FIXTURE" in its Delta description,
and the app shows a warning banner when it reads one.

Inputs (download first with `make fetch-data`):
  data/statsbomb/competitions.json, data/statsbomb/matches/*/*.json   -> results
  data/statsbomb/events/<match_id>.json                               -> shots, players, xG

Run:
  docker compose run --rm streamlit python dev_fixtures.py     (writes to MinIO)
  GOLD_URI=/tmp/gold python dev_fixtures.py                    (writes to a local folder)
"""
from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import write_deltalake

from data_access import DEV_MARKER, storage_options, table_uri

# $DATA_DIR is /data inside Docker; on the host fall back to <repo>/data.
DATA_DIR = Path(os.getenv("DATA_DIR") or Path(__file__).resolve().parents[2] / "data") / "statsbomb"
SOURCE = "statsbomb"

# StatsBomb Open Data mostly covers a few teams per season (e.g. only Barcelona's
# La Liga matches). Season tables from such partial data would be wrong, so we only
# use competition-seasons that are (almost) complete:
MIN_LEAGUE_COVERAGE = 0.6     # league: at least 60% of a double round-robin
MIN_TOURNAMENT_MATCHES = 30   # international tournament: at least 30 matches

# --- Schemas: exactly the columns and types of docs/gold-contract.md ---------------
TS = pa.timestamp("us", tz="UTC")
SCHEMAS: dict[str, pa.Schema] = {
    "team_season_stats": pa.schema([
        ("source", pa.string()), ("competition", pa.string()), ("country", pa.string()),
        ("season", pa.string()), ("team_id", pa.string()), ("team_name", pa.string()),
        ("matches_played", pa.int32()), ("wins", pa.int32()), ("draws", pa.int32()),
        ("losses", pa.int32()), ("points", pa.int32()), ("goals_for", pa.int32()),
        ("goals_against", pa.int32()), ("goal_difference", pa.int32()),
        ("home_matches", pa.int32()), ("home_wins", pa.int32()), ("home_goals_for", pa.int32()),
        ("home_goals_against", pa.int32()), ("away_matches", pa.int32()), ("away_wins", pa.int32()),
        ("away_goals_for", pa.int32()), ("away_goals_against", pa.int32()),
        ("league_position", pa.int32()), ("shots_for", pa.int32()), ("shots_against", pa.int32()),
        ("xg_for", pa.float64()), ("xg_against", pa.float64()), ("updated_at", TS),
    ]),
    "player_season_stats": pa.schema([
        ("source", pa.string()), ("competition", pa.string()), ("season", pa.string()),
        ("team_id", pa.string()), ("team_name", pa.string()), ("player_id", pa.string()),
        ("player_name", pa.string()), ("primary_position", pa.string()),
        ("matches_played", pa.int32()), ("minutes_played", pa.int32()), ("goals", pa.int32()),
        ("non_penalty_goals", pa.int32()), ("shots", pa.int32()), ("shots_on_target", pa.int32()),
        ("xg", pa.float64()), ("non_penalty_xg", pa.float64()), ("assists", pa.int32()),
        ("key_passes", pa.int32()), ("passes_attempted", pa.int32()), ("passes_completed", pa.int32()),
        ("xg_per_90", pa.float64()), ("updated_at", TS),
    ]),
    "league_trends": pa.schema([
        ("source", pa.string()), ("competition", pa.string()), ("country", pa.string()),
        ("season", pa.string()), ("matches", pa.int32()), ("total_goals", pa.int32()),
        ("avg_goals_per_match", pa.float64()), ("avg_home_goals", pa.float64()),
        ("avg_away_goals", pa.float64()), ("home_win_pct", pa.float64()),
        ("draw_pct", pa.float64()), ("away_win_pct", pa.float64()), ("updated_at", TS),
    ]),
    "shots": pa.schema([
        ("source", pa.string()), ("competition", pa.string()), ("season", pa.string()),
        ("match_id", pa.string()), ("match_date", pa.date32()), ("event_id", pa.string()),
        ("team_id", pa.string()), ("team_name", pa.string()), ("opponent_name", pa.string()),
        ("player_id", pa.string()), ("player_name", pa.string()), ("period", pa.int32()),
        ("minute", pa.int32()), ("second", pa.int32()), ("x", pa.float64()), ("y", pa.float64()),
        ("end_x", pa.float64()), ("end_y", pa.float64()), ("end_z", pa.float64()),
        ("xg", pa.float64()), ("outcome", pa.string()), ("is_goal", pa.bool_()),
        ("shot_type", pa.string()), ("body_part", pa.string()), ("technique", pa.string()),
        ("play_pattern", pa.string()), ("under_pressure", pa.bool_()), ("updated_at", TS),
    ]),
}
PARTITIONS = {"team_season_stats": ["source"], "shots": ["competition", "season"]}
ON_TARGET = {"Goal", "Saved", "Saved to Post"}


# --- Loading -----------------------------------------------------------------------
def load_matches() -> list[dict[str, Any]]:
    """All matches of all competition-seasons, each with its competition info attached."""
    competitions = json.loads((DATA_DIR / "competitions.json").read_text(encoding="utf-8"))
    matches = []
    for comp in competitions:
        path = DATA_DIR / "matches" / str(comp["competition_id"]) / f"{comp['season_id']}.json"
        if not path.exists():
            continue
        for match in json.loads(path.read_text(encoding="utf-8")):
            match["_comp"] = comp
            matches.append(match)
    return matches


def season_key(match: dict[str, Any]) -> tuple[str, str]:
    return match["_comp"]["competition_name"], match["_comp"]["season_name"]


def is_well_covered(season_matches: list[dict[str, Any]]) -> bool:
    comp = season_matches[0]["_comp"]
    if comp["competition_international"]:
        return len(season_matches) >= MIN_TOURNAMENT_MATCHES
    teams = {m["home_team"]["home_team_id"] for m in season_matches} | {m["away_team"]["away_team_id"] for m in season_matches}
    full_season = len(teams) * (len(teams) - 1)  # everyone plays everyone home and away
    return len(season_matches) >= MIN_LEAGUE_COVERAGE * full_season


def load_events(match_ids: set[int]) -> dict[int, list[dict[str, Any]]]:
    """Events per match, WITHOUT the penalty shoot-out (period 5): shoot-out kicks are
    not goals, shots or xG in football statistics (e.g. Kane: 4 Euro 2020 goals, not 5)."""
    events = {}
    for path in (DATA_DIR / "events").glob("*.json"):
        if path.stem.isdigit() and int(path.stem) in match_ids:
            raw = json.loads(path.read_text(encoding="utf-8"))
            events[int(path.stem)] = [e for e in raw if e["period"] < 5]
    return events


# --- Table builders ------------------------------------------------------------------
def build_league_trends(seasons: dict[tuple[str, str], list[dict]], now: datetime) -> list[dict]:
    rows = []
    for (competition, season), matches in seasons.items():
        comp = matches[0]["_comp"]
        n = len(matches)
        home_goals = sum(m["home_score"] for m in matches)
        away_goals = sum(m["away_score"] for m in matches)
        home_wins = sum(m["home_score"] > m["away_score"] for m in matches)
        draws = sum(m["home_score"] == m["away_score"] for m in matches)
        rows.append({
            "source": SOURCE, "competition": competition,
            "country": None if comp["competition_international"] else comp["country_name"],
            "season": season, "matches": n, "total_goals": home_goals + away_goals,
            "avg_goals_per_match": (home_goals + away_goals) / n,
            "avg_home_goals": home_goals / n, "avg_away_goals": away_goals / n,
            "home_win_pct": home_wins / n, "draw_pct": draws / n,
            "away_win_pct": (n - home_wins - draws) / n, "updated_at": now,
        })
    return rows


def build_team_season_stats(seasons: dict[tuple[str, str], list[dict]],
                            events: dict[int, list[dict]], now: datetime) -> list[dict]:
    rows = []
    for (competition, season), matches in seasons.items():
        comp = matches[0]["_comp"]
        teams: dict[int, dict[str, Any]] = {}
        for m in matches:
            # Look at each match twice: once from the home side, once from the away side.
            for side, other in (("home", "away"), ("away", "home")):
                team_id = m[f"{side}_team"][f"{side}_team_id"]
                gf, ga = m[f"{side}_score"], m[f"{other}_score"]
                t = teams.setdefault(team_id, {"name": m[f"{side}_team"][f"{side}_team_name"],
                                               "match_ids": set(), "n": Counter()})
                t["match_ids"].add(m["match_id"])
                n = t["n"]  # the counters for this team
                n["matches_played"] += 1
                n["wins"] += gf > ga
                n["draws"] += gf == ga
                n["losses"] += gf < ga
                n["goals_for"] += gf
                n["goals_against"] += ga
                n[f"{side}_matches"] += 1
                n[f"{side}_wins"] += gf > ga
                n[f"{side}_goals_for"] += gf
                n[f"{side}_goals_against"] += ga

        season_rows = []
        for team_id, t in teams.items():
            n = t["n"]
            # xG and shots only if we have the events of EVERY match of this team,
            # otherwise the totals would silently be too low.
            covered = all(mid in events for mid in t["match_ids"])
            shots_for = shots_against = xg_for = xg_against = None
            if covered:
                shots = [e for mid in t["match_ids"] for e in events[mid] if e["type"]["name"] == "Shot"]
                own = [s for s in shots if s["team"]["id"] == team_id]
                opp = [s for s in shots if s["team"]["id"] != team_id]
                shots_for, shots_against = len(own), len(opp)
                xg_for = round(sum(s["shot"]["statsbomb_xg"] for s in own), 4)
                xg_against = round(sum(s["shot"]["statsbomb_xg"] for s in opp), 4)
            season_rows.append({
                "source": SOURCE, "competition": competition,
                "country": None if comp["competition_international"] else comp["country_name"],
                "season": season, "team_id": str(team_id), "team_name": t["name"],
                **{k: n[k] for k in ("matches_played", "wins", "draws", "losses", "goals_for", "goals_against",
                                     "home_matches", "home_wins", "home_goals_for", "home_goals_against",
                                     "away_matches", "away_wins", "away_goals_for", "away_goals_against")},
                "points": 3 * n["wins"] + n["draws"],
                "goal_difference": n["goals_for"] - n["goals_against"],
                "league_position": None,
                "shots_for": shots_for, "shots_against": shots_against,
                "xg_for": xg_for, "xg_against": xg_against, "updated_at": now,
            })

        if not comp["competition_international"]:
            # League table order: points, then goal difference, then goals scored.
            season_rows.sort(key=lambda r: (r["points"], r["goal_difference"], r["goals_for"]), reverse=True)
            for position, row in enumerate(season_rows, start=1):
                row["league_position"] = position
        rows.extend(season_rows)
    return rows


def minutes_played(match_events: list[dict]) -> dict[int, int]:
    """Minutes per player in one match: starters from minute 0, substitutes from when they
    come on, until they go off or the match ends. (Red cards are ignored: dev data.)"""
    end = max(e["minute"] for e in match_events)
    on: dict[int, int] = {}
    off: dict[int, int] = {}
    for e in match_events:
        if e["type"]["name"] == "Starting XI":
            for slot in e["tactics"]["lineup"]:
                on[slot["player"]["id"]] = 0
        elif e["type"]["name"] == "Substitution":
            off[e["player"]["id"]] = e["minute"]
            on[e["substitution"]["replacement"]["id"]] = e["minute"]
    return {pid: off.get(pid, end) - start for pid, start in on.items()}


def build_shots_and_players(matches_by_id: dict[int, dict], events: dict[int, list[dict]],
                            full_teams: set[tuple[str, str, int]], now: datetime) -> tuple[list[dict], list[dict]]:
    shots: list[dict] = []
    players: dict[tuple, dict[str, Any]] = {}

    for match_id, match_events in events.items():
        m = matches_by_id[match_id]
        competition, season = season_key(m)
        names = {m["home_team"]["home_team_id"]: m["home_team"]["home_team_name"],
                 m["away_team"]["away_team_id"]: m["away_team"]["away_team_name"]}
        minutes = minutes_played(match_events)

        for e in match_events:
            if "player" not in e:
                continue
            team_id = e["team"]["id"]
            event_type = e["type"]["name"]
            opponent = next(name for tid, name in names.items() if tid != team_id)

            if event_type == "Shot":
                shot = e["shot"]
                end = shot["end_location"] + [None] * (3 - len(shot["end_location"]))
                shots.append({
                    "source": SOURCE, "competition": competition, "season": season,
                    "match_id": str(match_id), "match_date": date.fromisoformat(m["match_date"]),
                    "event_id": e["id"], "team_id": str(team_id), "team_name": e["team"]["name"],
                    "opponent_name": opponent, "player_id": str(e["player"]["id"]),
                    "player_name": e["player"]["name"], "period": e["period"], "minute": e["minute"],
                    "second": e["second"], "x": e["location"][0], "y": e["location"][1],
                    "end_x": end[0], "end_y": end[1], "end_z": end[2], "xg": shot["statsbomb_xg"],
                    "outcome": shot["outcome"]["name"], "is_goal": shot["outcome"]["name"] == "Goal",
                    "shot_type": shot["type"]["name"], "body_part": shot["body_part"]["name"],
                    "technique": shot["technique"]["name"], "play_pattern": e["play_pattern"]["name"],
                    "under_pressure": bool(e.get("under_pressure", False)), "updated_at": now,
                })

            # Player season stats only for teams whose every match we have.
            if (competition, season, team_id) not in full_teams:
                continue
            key = (competition, season, team_id, e["player"]["id"])
            p = players.setdefault(key, {"name": e["player"]["name"], "team": e["team"]["name"],
                                         "positions": Counter(), "matches": set(), "minutes": {}, "c": Counter()})
            p["matches"].add(match_id)
            p["minutes"][match_id] = minutes.get(e["player"]["id"], 0)
            if "position" in e:
                p["positions"][e["position"]["name"]] += 1
            c = p["c"]
            if event_type == "Shot":
                penalty = e["shot"]["type"]["name"] == "Penalty"
                goal = e["shot"]["outcome"]["name"] == "Goal"
                c["shots"] += 1
                c["goals"] += goal
                c["non_penalty_goals"] += goal and not penalty
                c["shots_on_target"] += e["shot"]["outcome"]["name"] in ON_TARGET
                c["xg"] += e["shot"]["statsbomb_xg"]
                c["non_penalty_xg"] += 0 if penalty else e["shot"]["statsbomb_xg"]
            elif event_type == "Pass":
                p_ = e["pass"]
                c["passes_attempted"] += 1
                c["passes_completed"] += "outcome" not in p_   # no outcome = completed
                c["assists"] += bool(p_.get("goal_assist"))
                c["key_passes"] += bool(p_.get("shot_assist") or p_.get("goal_assist"))

    player_rows = []
    for (competition, season, team_id, player_id), p in players.items():
        c = p["c"]
        mins = sum(p["minutes"].values())
        player_rows.append({
            "source": SOURCE, "competition": competition, "season": season,
            "team_id": str(team_id), "team_name": p["team"], "player_id": str(player_id),
            "player_name": p["name"],
            "primary_position": p["positions"].most_common(1)[0][0] if p["positions"] else None,
            "matches_played": len(p["matches"]), "minutes_played": mins,
            **{k: int(c[k]) for k in ("goals", "non_penalty_goals", "shots", "shots_on_target", "assists",
                                      "key_passes", "passes_attempted", "passes_completed")},
            "xg": round(c["xg"], 4), "non_penalty_xg": round(c["non_penalty_xg"], 4),
            "xg_per_90": round(c["xg"] / mins * 90, 4) if mins else None, "updated_at": now,
        })
    return shots, player_rows


# --- Writing -----------------------------------------------------------------------
def write_table(name: str, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows, schema=SCHEMAS[name])  # fails loudly if a type is wrong
    options = storage_options()
    if options:
        # Single writer, so the S3 "unsafe rename" mode is fine (no concurrent commits).
        options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    write_deltalake(
        table_uri(name), table, mode="overwrite", schema_mode="overwrite",
        partition_by=PARTITIONS.get(name),
        description=f"{DEV_MARKER}: sample data built by dev_fixtures.py from StatsBomb Open Data",
        storage_options=options,
    )
    print(f"  wrote {name:<20} {table.num_rows:>6} rows -> {table_uri(name)}")


def main() -> None:
    now = datetime.now(timezone.utc)
    matches = load_matches()
    if not matches:
        raise SystemExit(f"No match lists in {DATA_DIR}. Run `make fetch-data` first.")
    matches_by_id = {m["match_id"]: m for m in matches}

    by_season: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for m in matches:
        by_season[season_key(m)].append(m)
    seasons = {k: v for k, v in by_season.items() if is_well_covered(v)}

    events = load_events(set(matches_by_id))
    team_rows = build_team_season_stats(seasons, events, now)
    full_teams = {(r["competition"], r["season"], int(r["team_id"])) for r in team_rows if r["xg_for"] is not None}
    shots, players = build_shots_and_players(matches_by_id, events, full_teams, now)

    print(f"DEV FIXTURES: {len(seasons)} well-covered competition-seasons, {len(events)} matches with events")
    write_table("team_season_stats", team_rows)
    write_table("league_trends", build_league_trends(seasons, now))
    write_table("shots", shots)
    write_table("player_season_stats", players)


if __name__ == "__main__":
    main()
