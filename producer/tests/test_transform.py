import json

import pytest
from jsonschema import Draft202012Validator

from test_consumer import SCHEMA_DIR  # docs/schemas locally, /app/schemas in Docker
from transform import clock_to_seconds, transform_event

SCHEMA = SCHEMA_DIR / "match-event.schema.json"


@pytest.mark.parametrize("clock, expected", [
    ("00:00:00.000", 0.0),
    ("00:02:09.222", 129.222),
    ("00:45:30.000", 2730.0),
    ("01:00:00.500", 3600.5),
])
def test_clock_to_seconds(clock, expected):
    assert clock_to_seconds(clock) == expected


def test_shot_maps_all_fields(by_index):
    msg = transform_event(by_index[7], "9999", replay_id="r1", emitted_at="2026-01-01T00:00:00.000Z")
    assert msg["match_id"] == "9999"
    assert msg["event_id"] == by_index[7]["id"]
    assert msg["type"] == "Shot"
    assert msg["team"] == "Home FC" and msg["team_id"] == 1
    assert msg["player"] == "Ana Striker" and msg["player_id"] == 10
    assert msg["location"] == [108.0, 40.0]
    assert msg["end_location"] == [120.0, 39.0, 0.5]
    assert msg["xg"] == 0.76                      # <- shot.statsbomb_xg
    assert msg["outcome"] == "Goal"               # <- shot.outcome.name
    assert msg["is_goal"] is True
    assert msg["under_pressure"] is True
    assert msg["clock_seconds"] == 600.0
    assert msg["replay_id"] == "r1"
    assert msg["emitted_at"] == "2026-01-01T00:00:00.000Z"


def test_xg_only_on_shots(mini_match):
    for raw in mini_match:
        msg = transform_event(raw, "9999")
        assert (msg["xg"] is not None) == (raw["type"]["name"] == "Shot")


def test_missing_pass_and_receipt_outcome_means_complete(by_index):
    assert transform_event(by_index[5], "9999")["outcome"] == "Complete"   # Pass
    assert transform_event(by_index[6], "9999")["outcome"] == "Complete"   # Ball Receipt*
    assert transform_event(by_index[8], "9999")["outcome"] == "Incomplete"


def test_missing_outcome_on_other_types_is_null(by_index):
    assert transform_event(by_index[17], "9999")["outcome"] is None        # Duel without outcome
    assert transform_event(by_index[3], "9999")["outcome"] is None         # Half Start


def test_team_events_have_no_player_or_location(by_index):
    msg = transform_event(by_index[1], "9999")                             # Starting XI
    assert msg["player"] is None and msg["player_id"] is None
    assert msg["location"] is None and msg["end_location"] is None


def test_card(by_index):
    assert transform_event(by_index[9], "9999")["card"] == "Yellow Card"
    assert transform_event(by_index[5], "9999")["card"] is None


def test_goals_give_the_score(mini_match):
    goals = [m["team"] for m in (transform_event(e, "9999") for e in mini_match) if m["is_goal"]]
    # Home FC scored; the own goal by Home FC counts for Away United ("Own Goal For").
    assert sorted(goals) == ["Away United", "Home FC"]


def test_emitted_at_is_iso_utc(by_index):
    emitted = transform_event(by_index[5], "9999")["emitted_at"]
    assert emitted.endswith("Z") and "T" in emitted and len(emitted) == 24


def test_every_fixture_event_matches_json_schema(mini_match):
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()))
    for raw in mini_match:
        errors = [e.message for e in validator.iter_errors(transform_event(raw, "9999", replay_id="r1"))]
        assert errors == [], f"index {raw['index']}: {errors}"
