#!/usr/bin/env python3
"""Download StatsBomb Open Data files into data/statsbomb/.

Uses only the Python standard library, so it runs on any machine with Python 3.11+
(no virtualenv needed).

What it downloads:
  data/statsbomb/competitions.json                 list of all open competitions/seasons
  data/statsbomb/matches/<comp>/<season>.json      match list + final scores (all, small)
  data/statsbomb/events/<match_id>.json            events of the matches given with --events

Examples:
  python3 scripts/fetch_statsbomb.py                      # competitions + all match lists
  python3 scripts/fetch_statsbomb.py --events 3788741     # ... plus one match's events
  python3 scripts/fetch_statsbomb.py --demo               # ... plus the demo/fixture matches
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
# $DATA_DIR is set inside Docker (/data); on the host we use <repo>/data.
DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent.parent / "data")) / "statsbomb"

# Euro 2020 (competition 55, season 43): every match of the two finalists, Italy and
# England (13 matches). The final, 3795506, went to extra time and penalties.
# Used for the replay demo and for the Streamlit dev fixtures.
DEMO_EVENT_MATCHES = [
    # Italy
    3788741, 3788754, 3788766, 3794685, 3795107, 3795220,
    # England
    3788745, 3788759, 3788772, 3794688, 3795187, 3795221,
    # Final: Italy 1-1 England (Italy won on penalties)
    3795506,
]


def download(url: str, target: Path, retries: int = 3) -> bool:
    """Download url to target unless the file already exists. Returns True if downloaded."""
    if target.exists():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                data = response.read()
            # Write to a temp file first so an interrupted download never leaves a
            # half-written JSON file behind.
            tmp = target.with_suffix(".part")
            tmp.write_bytes(data)
            tmp.replace(target)
            return True
        except OSError as exc:  # urllib errors are subclasses of OSError
            if attempt == retries:
                raise RuntimeError(f"failed to download {url}: {exc}") from exc
            time.sleep(2 * attempt)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--events", nargs="*", type=int, default=[], help="match ids whose events to download")
    parser.add_argument("--demo", action="store_true", help="also download the demo/fixture matches")
    args = parser.parse_args()

    competitions_file = DATA_DIR / "competitions.json"
    download(f"{BASE_URL}/competitions.json", competitions_file)
    competitions = json.loads(competitions_file.read_text(encoding="utf-8"))
    print(f"competitions.json: {len(competitions)} competition-seasons")

    new = 0
    for comp in competitions:
        path = f"matches/{comp['competition_id']}/{comp['season_id']}.json"
        new += download(f"{BASE_URL}/{path}", DATA_DIR / path)
    print(f"match lists: {len(competitions)} files ({new} newly downloaded)")

    match_ids = list(args.events) + (DEMO_EVENT_MATCHES if args.demo else [])
    for match_id in match_ids:
        target = DATA_DIR / "events" / f"{match_id}.json"
        status = "downloaded" if download(f"{BASE_URL}/events/{match_id}.json", target) else "already present"
        print(f"events {match_id}: {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
