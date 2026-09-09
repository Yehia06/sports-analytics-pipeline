"""
data_acquisition.py
--------------------
Downloads StatsBomb Open Data (competitions, matches, events, lineups) for a
configurable set of competitions/seasons and saves the raw JSON to disk.

StatsBomb open-data repo: https://github.com/statsbomb/open-data
All files are served as static JSON from raw.githubusercontent.com — no API
key required.

Usage:
    python data_acquisition.py                       # downloads default set
    python data_acquisition.py --comp 11 --season 90  # single comp/season
    python data_acquisition.py --list                # just print available comps

Default set (matches the assignment brief: La Liga, Euro 2020, Champions League):
    La Liga            competition_id=11
    Champions League    competition_id=16
    UEFA Euro 2020       competition_id=55, season_id=43
"""

import argparse
import json
import os
import time
from pathlib import Path
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Default competitions to pull, per the brief.
# You can widen/narrow this list freely.
DEFAULT_TARGETS = [
    {"name": "La Liga", "competition_id": 11, "season_id": 90},   # 2020/2021 (most complete recent season)
    # NOTE: StatsBomb's free open data only publishes the FINAL match for each
    # Champions League season (not the full tournament). We pull several
    # seasons' finals to get a reasonable sample of CL matches.
    {"name": "Champions League 2017/18", "competition_id": 16, "season_id": 1},
    {"name": "Champions League 2016/17", "competition_id": 16, "season_id": 2},
    {"name": "Champions League 2015/16", "competition_id": 16, "season_id": 27},
    {"name": "Champions League 2014/15", "competition_id": 16, "season_id": 26},
    {"name": "UEFA Euro 2020", "competition_id": 55, "season_id": 43},
]


def _get_json(url: str, retries: int = 3, backoff: float = 1.5):
    for attempt in range(retries):
        try:
            with urlopen(url, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 404:
                return None  # some matches have no lineups/360 data etc, that's fine
            print(f"  HTTP error {e.code} on {url}, retry {attempt+1}/{retries}")
        except URLError as e:
            print(f"  Network error on {url}: {e}, retry {attempt+1}/{retries}")
        time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url} after {retries} retries")


def fetch_competitions():
    print("Fetching competitions.json ...")
    data = _get_json(f"{BASE_URL}/competitions.json")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with open(RAW_DIR / "competitions.json", "w") as f:
        json.dump(data, f)
    return data


def fetch_matches(competition_id: int, season_id: int):
    url = f"{BASE_URL}/matches/{competition_id}/{season_id}.json"
    data = _get_json(url)
    out_dir = RAW_DIR / "matches" / str(competition_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / f"{season_id}.json", "w") as f:
        json.dump(data, f)
    return data or []


def fetch_match_events(match_id: int):
    url = f"{BASE_URL}/events/{match_id}.json"
    data = _get_json(url)
    out_dir = RAW_DIR / "events"
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / f"{match_id}.json", "w") as f:
        json.dump(data, f)
    return data or []


def fetch_lineups(match_id: int):
    url = f"{BASE_URL}/lineups/{match_id}.json"
    data = _get_json(url)
    if data is None:
        return []
    out_dir = RAW_DIR / "lineups"
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / f"{match_id}.json", "w") as f:
        json.dump(data, f)
    return data


def run(targets, max_matches_per_comp=None, fetch_lineups_too=True):
    competitions = fetch_competitions()
    comp_lookup = {(c["competition_id"], c["season_id"]): c for c in competitions}

    summary = []
    for t in targets:
        cid, sid = t["competition_id"], t["season_id"]
        label = comp_lookup.get((cid, sid), {}).get("competition_name", t.get("name", cid))
        print(f"\n=== {label} (competition_id={cid}, season_id={sid}) ===")
        matches = fetch_matches(cid, sid)
        print(f"  {len(matches)} matches found")

        if max_matches_per_comp:
            matches = matches[:max_matches_per_comp]
            print(f"  Limiting to first {len(matches)} matches for this run")

        n_ok = 0
        for m in matches:
            mid = m["match_id"]
            try:
                events = fetch_match_events(mid)
                if fetch_lineups_too:
                    fetch_lineups(mid)
                n_ok += 1
            except Exception as e:
                print(f"  ! failed match {mid}: {e}")
        print(f"  Downloaded events for {n_ok}/{len(matches)} matches")
        summary.append({"competition": label, "competition_id": cid, "season_id": sid,
                         "matches_downloaded": n_ok})

    print("\n=== Summary ===")
    for s in summary:
        print(s)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--comp", type=int, help="single competition_id to fetch")
    ap.add_argument("--season", type=int, help="single season_id to fetch (used with --comp)")
    ap.add_argument("--list", action="store_true", help="list available competitions and exit")
    ap.add_argument("--max-matches", type=int, default=None,
                     help="cap number of matches per competition (useful for a quick test run)")
    ap.add_argument("--no-lineups", action="store_true", help="skip lineup downloads")
    args = ap.parse_args()

    if args.list:
        comps = fetch_competitions()
        for c in comps:
            print(c["competition_id"], c["season_id"], c["competition_name"], c["season_name"])
        raise SystemExit(0)

    if args.comp:
        targets = [{"name": str(args.comp), "competition_id": args.comp,
                     "season_id": args.season or 1}]
    else:
        targets = DEFAULT_TARGETS

    run(targets, max_matches_per_comp=args.max_matches, fetch_lineups_too=not args.no_lineups)
