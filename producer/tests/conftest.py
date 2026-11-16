import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def mini_match() -> list[dict]:
    """A hand-made 19-event match (id 9999): Home FC 1-1 Away United.

    Contains: lineups, a pass + receipt without outcome, a goal, a ball receipt with a
    wrong (too early) timestamp, a yellow card, an own goal, a duel without outcome.
    """
    return json.loads((FIXTURES / "9999.json").read_text())


@pytest.fixture
def by_index(mini_match) -> dict[int, dict]:
    return {e["index"]: e for e in mini_match}
