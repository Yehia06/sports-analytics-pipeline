"""
data_cleaning.py
-----------------
Reads the raw StatsBomb event JSON downloaded by data_acquisition.py and
flattens it into clean, tabular records — one table per Kafka topic the
pipeline needs:

    match-events            all events (generic envelope)
    shot-events              only Shot events, with xG / outcome / body part
    pass-events               only Pass events, with recipient / length / outcome
    pressure-events            only Pressure events
    win-probability-updates      NOT produced here (Person 4 model output) — a
                              placeholder table with the join keys is written so
                              downstream owners can see the expected shape.

Each row also gets a synthetic `event_epoch_seconds` column: a per-match
elapsed-seconds-from-kickoff value derived from `minute`/`second`/`period`.
This is what the Kafka producer uses to pace replay in "realistic live" mode.

Output: Parquet files under data/clean/<topic>/<match_id>.parquet
(also writes a combined data/clean/<topic>/all.parquet per topic)

Usage:
    python data_cleaning.py
"""

import json
from pathlib import Path
import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
CLEAN_DIR = Path(__file__).resolve().parent.parent / "data" / "clean"

# StatsBomb periods: 1 = first half (kickoff at 0'), 2 = second half (kickoff at 45'),
# 3/4 = extra time, 5 = penalties. We just use elapsed minute*60+second as a
# simple monotonic-enough clock for replay pacing; good enough for a demo pipeline.


def _base_fields(e: dict, match_id: int) -> dict:
    loc = e.get("location") or [None, None]
    return {
        "match_id": match_id,
        "event_id": e.get("id"),
        "index": e.get("index"),
        "period": e.get("period"),
        "minute": e.get("minute"),
        "second": e.get("second"),
        "timestamp": e.get("timestamp"),
        "event_epoch_seconds": (e.get("minute") or 0) * 60 + (e.get("second") or 0),
        "type": e.get("type", {}).get("name"),
        "team_id": e.get("team", {}).get("id"),
        "team_name": e.get("team", {}).get("name"),
        "player_id": (e.get("player") or {}).get("id"),
        "player_name": (e.get("player") or {}).get("name"),
        "position": (e.get("position") or {}).get("name"),
        "possession": e.get("possession"),
        "possession_team": (e.get("possession_team") or {}).get("name"),
        "location_x": loc[0],
        "location_y": loc[1],
        "play_pattern": (e.get("play_pattern") or {}).get("name"),
    }


def clean_match(match_id: int) -> dict:
    with open(RAW_DIR / "events" / f"{match_id}.json") as f:
        events = json.load(f)

    rows_all, rows_shot, rows_pass, rows_pressure = [], [], [], []

    for e in events:
        base = _base_fields(e, match_id)
        etype = base["type"]
        rows_all.append(base)

        if etype == "Shot":
            s = e.get("shot", {})
            end = s.get("end_location") or [None, None, None]
            rows_shot.append({
                **base,
                "shot_xg": s.get("statsbomb_xg"),
                "shot_outcome": (s.get("outcome") or {}).get("name"),
                "shot_body_part": (s.get("body_part") or {}).get("name"),
                "shot_technique": (s.get("technique") or {}).get("name"),
                "shot_type": (s.get("type") or {}).get("name"),
                "shot_end_x": end[0], "shot_end_y": end[1],
                "shot_end_z": end[2] if len(end) > 2 else None,
                "is_goal": (s.get("outcome") or {}).get("name") == "Goal",
            })

        elif etype == "Pass":
            p = e.get("pass", {})
            end = p.get("end_location") or [None, None]
            rows_pass.append({
                **base,
                "pass_recipient_id": (p.get("recipient") or {}).get("id"),
                "pass_recipient_name": (p.get("recipient") or {}).get("name"),
                "pass_length": p.get("length"),
                "pass_angle": p.get("angle"),
                "pass_height": (p.get("height") or {}).get("name"),
                "pass_outcome": (p.get("outcome") or {}).get("name", "Complete"),
                "pass_end_x": end[0], "pass_end_y": end[1],
                "is_key_pass": bool(p.get("shot_assist") or p.get("goal_assist")),
            })

        elif etype == "Pressure":
            rows_pressure.append({
                **base,
                "duration": e.get("duration"),
            })

    return {
        "all": pd.DataFrame(rows_all),
        "shot": pd.DataFrame(rows_shot),
        "pass": pd.DataFrame(rows_pass),
        "pressure": pd.DataFrame(rows_pressure),
    }


def run():
    events_dir = RAW_DIR / "events"
    match_ids = [int(p.stem) for p in events_dir.glob("*.json")]
    print(f"Cleaning {len(match_ids)} matches ...")

    topic_map = {
        "all": "match-events",
        "shot": "shot-events",
        "pass": "pass-events",
        "pressure": "pressure-events",
    }
    combined = {k: [] for k in topic_map}

    for mid in match_ids:
        cleaned = clean_match(mid)
        for key, df in cleaned.items():
            if df.empty:
                continue
            out_dir = CLEAN_DIR / topic_map[key]
            out_dir.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out_dir / f"{mid}.parquet", index=False)
            combined[key].append(df)
        print(f"  match {mid}: {len(cleaned['all'])} events "
              f"({len(cleaned['shot'])} shots, {len(cleaned['pass'])} passes, "
              f"{len(cleaned['pressure'])} pressures)")

    for key, dfs in combined.items():
        if not dfs:
            continue
        full = pd.concat(dfs, ignore_index=True)
        out_path = CLEAN_DIR / topic_map[key] / "all.parquet"
        full.to_parquet(out_path, index=False)
        print(f"Wrote combined {topic_map[key]}/all.parquet -> {len(full)} rows")

    # placeholder shape for win-probability-updates (produced later by Person 4's
    # model + Person 2's streaming job, not by ingestion — documented here so the
    # contract is clear from day one)
    wp_dir = CLEAN_DIR / "win-probability-updates"
    wp_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=[
        "match_id", "event_epoch_seconds", "minute", "team_id",
        "home_win_prob", "draw_prob", "away_win_prob", "model_version"
    ]).to_parquet(wp_dir / "SCHEMA_ONLY.parquet", index=False)
    print("Wrote win-probability-updates schema placeholder (populated at inference time).")


if __name__ == "__main__":
    run()
