"""Turn one raw StatsBomb event into a `match-events` message (docs/event-schema.md).

Pure functions only (no Kafka, no files), so they are easy to unit-test.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = 1

# In raw StatsBomb, details such as the outcome live in a sub-object whose name
# depends on the event type. The names don't follow a simple rule
# ("Ball Receipt*" -> "ball_receipt", "Goal Keeper" -> "goalkeeper"), so we list them.
OUTCOME_SUB_OBJECT: dict[str, str] = {
    "Pass": "pass",
    "Ball Receipt*": "ball_receipt",
    "Shot": "shot",
    "Duel": "duel",
    "Dribble": "dribble",
    "Interception": "interception",
    "Goal Keeper": "goalkeeper",
    "50/50": "50_50",
    "Substitution": "substitution",
}

# Event types that have an end location, and the sub-object holding it.
END_LOCATION_SUB_OBJECT: dict[str, str] = {
    "Pass": "pass",
    "Carry": "carry",
    "Shot": "shot",
    "Goal Keeper": "goalkeeper",
}

# StatsBomb omits the outcome when these succeed; we write "Complete" explicitly.
OUTCOME_COMPLETE_WHEN_MISSING = {"Pass", "Ball Receipt*"}


def clock_to_seconds(timestamp: str) -> float:
    """Convert the period clock "HH:MM:SS.mmm" to seconds, e.g. "00:02:09.222" -> 129.222."""
    hours, minutes, seconds = timestamp.split(":")
    return round(int(hours) * 3600 + int(minutes) * 60 + float(seconds), 3)


def utc_now_iso() -> str:
    """Current wall-clock time as ISO-8601 UTC with milliseconds, e.g. 2026-11-15T19:54:01.123Z."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _name(obj: dict[str, Any] | None) -> str | None:
    """StatsBomb references look like {"id": 914, "name": "Italy"}; return the name or None."""
    return obj.get("name") if obj else None


def _outcome(raw: dict[str, Any], event_type: str) -> str | None:
    sub_key = OUTCOME_SUB_OBJECT.get(event_type)
    if sub_key is None:
        return None
    outcome = _name(raw.get(sub_key, {}).get("outcome"))
    if outcome is None and event_type in OUTCOME_COMPLETE_WHEN_MISSING:
        return "Complete"
    return outcome


def _end_location(raw: dict[str, Any], event_type: str) -> list[float] | None:
    sub_key = END_LOCATION_SUB_OBJECT.get(event_type)
    if sub_key is None:
        return None
    return raw.get(sub_key, {}).get("end_location")


def _card(raw: dict[str, Any]) -> str | None:
    # Cards are recorded on fouls, or on "Bad Behaviour" events (e.g. dissent).
    card = raw.get("foul_committed", {}).get("card") or raw.get("bad_behaviour", {}).get("card")
    return _name(card)


def transform_event(
    raw: dict[str, Any],
    match_id: str,
    replay_id: str | None = None,
    emitted_at: str | None = None,
) -> dict[str, Any]:
    """Map one raw StatsBomb event to the match-events contract (schema_version 1)."""
    event_type = raw["type"]["name"]
    outcome = _outcome(raw, event_type)
    player = raw.get("player")

    message: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "match_id": match_id,
        "event_id": raw["id"],
        "index": raw["index"],
        "period": raw["period"],
        "timestamp": raw["timestamp"],
        "clock_seconds": clock_to_seconds(raw["timestamp"]),
        "minute": raw["minute"],
        "second": raw["second"],
        "type": event_type,
        "team": raw["team"]["name"],
        "team_id": raw["team"]["id"],
        "player": _name(player),
        "player_id": player["id"] if player else None,
        "position": _name(raw.get("position")),
        "possession": raw["possession"],
        "possession_team": raw["possession_team"]["name"],
        "play_pattern": raw["play_pattern"]["name"],
        "location": raw.get("location"),
        "end_location": _end_location(raw, event_type),
        # The raw field is only present when true.
        "under_pressure": bool(raw.get("under_pressure", False)),
        "xg": raw["shot"]["statsbomb_xg"] if event_type == "Shot" else None,
        "outcome": outcome,
        # "Own Goal For" is the event on the team that benefits from an own goal.
        "is_goal": (event_type == "Shot" and outcome == "Goal") or event_type == "Own Goal For",
        "card": _card(raw),
        "emitted_at": emitted_at or utc_now_iso(),
    }
    if replay_id is not None:
        message["replay_id"] = replay_id
    return message
