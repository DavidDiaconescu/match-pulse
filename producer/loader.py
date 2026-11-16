"""Load StatsBomb data from data/statsbomb/, downloading with statsbombpy if missing."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Inside Docker, data/ is mounted at /data. On the host it is <repo>/data.
DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))


@dataclass
class MatchData:
    match_id: str
    events: list[dict[str, Any]]
    home_team: str | None = None
    away_team: str | None = None


def read_json(path: Path) -> Any:
    """Read a JSON file, with a clear message for the macOS + iCloud placeholder problem."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        if exc.errno == 35:  # "Resource deadlock avoided" through Docker Desktop on a Mac
            raise OSError(exc.errno, f"{path} is not stored locally on the Mac (iCloud "
                          "'Optimise Mac Storage'?). Run the make/scripts command again (it downloads "
                          "such files first) or set DATA_DIR_HOST outside iCloud.") from exc
        raise


def events_path(match_id: str, data_dir: Path = DEFAULT_DATA_DIR) -> Path:
    return data_dir / "statsbomb" / "events" / f"{match_id}.json"


def fetch_events_with_statsbombpy(match_id: str, target: Path) -> list[dict[str, Any]]:
    """Download a match's events from StatsBomb Open Data and cache them at `target`."""
    from statsbombpy import sb  # imported here: only needed when the file is missing

    log.info("match %s: %s not found, downloading with statsbombpy", match_id, target)
    by_id = sb.events(match_id=int(match_id), fmt="dict")
    # statsbombpy returns {event_id: event} and adds a "match_id" field to each event.
    # Remove it so the cached file is identical to the original open-data file.
    events = sorted(by_id.values(), key=lambda e: e["index"])
    for event in events:
        event.pop("match_id", None)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(events, ensure_ascii=False), encoding="utf-8")
    return events


def find_teams(match_id: str, data_dir: Path = DEFAULT_DATA_DIR) -> tuple[str | None, str | None]:
    """Look up home/away team names in the downloaded match lists, if any."""
    for matches_file in (data_dir / "statsbomb" / "matches").glob("*/*.json"):
        for match in read_json(matches_file):
            if str(match["match_id"]) == match_id:
                return match["home_team"]["home_team_name"], match["away_team"]["away_team_name"]
    return None, None


def load_match(match_id: str, data_dir: Path = DEFAULT_DATA_DIR) -> MatchData:
    path = events_path(match_id, data_dir)
    if path.exists():
        events = read_json(path)
    else:
        events = fetch_events_with_statsbombpy(match_id, path)
    home, away = find_teams(match_id, data_dir)
    return MatchData(match_id=match_id, events=events, home_team=home, away_team=away)


def parse_match_args(values: list[str]) -> list[str]:
    """--matches accepts match ids and/or paths to text files with one id per line.

    In files, blank lines and anything after '#' are ignored.
    """
    match_ids: list[str] = []
    for value in values:
        if value.isdigit():
            match_ids.append(value)
            continue
        path = Path(value)
        if not path.exists():
            raise ValueError(f"'{value}' is neither a match id nor an existing file")
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                if not line.isdigit():
                    raise ValueError(f"{path}: '{line}' is not a match id")
                match_ids.append(line)
    if not match_ids:
        raise ValueError("no match ids given")
    return match_ids
