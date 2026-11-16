import pytest

from loader import find_teams, load_match, parse_match_args


def test_parse_ids_and_files(tmp_path):
    match_file = tmp_path / "matches.txt"
    match_file.write_text("# demo matches\n111\n\n222  # with comment\n")
    assert parse_match_args(["999", str(match_file)]) == ["999", "111", "222"]


@pytest.mark.parametrize("bad", [["not-a-file.txt"], []])
def test_parse_rejects_bad_input(bad):
    with pytest.raises(ValueError):
        parse_match_args(bad)


def test_load_match_from_local_file(tmp_path, mini_match):
    import json
    events_dir = tmp_path / "statsbomb" / "events"
    events_dir.mkdir(parents=True)
    (events_dir / "9999.json").write_text(json.dumps(mini_match))
    matches_dir = tmp_path / "statsbomb" / "matches" / "1"
    matches_dir.mkdir(parents=True)
    (matches_dir / "1.json").write_text(json.dumps([{
        "match_id": 9999,
        "home_team": {"home_team_name": "Home FC"},
        "away_team": {"away_team_name": "Away United"},
    }]))

    match = load_match("9999", data_dir=tmp_path)
    assert len(match.events) == 19
    assert (match.home_team, match.away_team) == ("Home FC", "Away United")
    assert find_teams("12345", data_dir=tmp_path) == (None, None)
