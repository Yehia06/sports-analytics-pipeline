import os
import gc
import requests
import pandas as pd
from tqdm import tqdm

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
CLEAN_DIR = "data/clean"
TOPICS = ["match-events", "pass-events", "pressure-events", "shot-events"]

for topic in TOPICS:
    os.makedirs(os.path.join(CLEAN_DIR, topic), exist_ok=True)

# 1. Fetch competition catalog
comps = requests.get(f"{BASE_URL}/competitions.json").json()
# 11: La Liga, 16: Champions League, 55: UEFA Euro
TARGET_COMPS = [11, 16, 55]

# 2. Collect match IDs up to 325
match_ids = []
for comp in comps:
    if comp["competition_id"] in TARGET_COMPS:
        url = f"{BASE_URL}/matches/{comp['competition_id']}/{comp['season_id']}.json"
        res = requests.get(url)
        if res.status_code == 200:
            for m in res.json():
                match_ids.append(m["match_id"])
                if len(match_ids) >= 325:
                    break
    if len(match_ids) >= 325:
        break

print(f"Targeting {len(match_ids)} matches. Downloading and partitioning...")

# 3. Stream match by match to keep RAM usage low (< 300MB)
for m_id in tqdm(match_ids, desc="Processing matches"):
    out_path = f"{CLEAN_DIR}/match-events/{m_id}.parquet"
    if os.path.exists(out_path):
        continue  # Skip already downloaded matches

    ev_res = requests.get(f"{BASE_URL}/events/{m_id}.json")
    if ev_res.status_code != 200:
        continue

    raw = ev_res.json()
    df = pd.json_normalize(raw)

    # Save full events
    df.to_parquet(out_path, index=False)

    # Partition specific event topics
    if "type.name" in df.columns:
        passes = df[df["type.name"] == "Pass"]
        if not passes.empty:
            passes.to_parquet(f"{CLEAN_DIR}/pass-events/{m_id}.parquet", index=False)

        pressures = df[df["type.name"] == "Pressure"]
        if not pressures.empty:
            pressures.to_parquet(f"{CLEAN_DIR}/pressure-events/{m_id}.parquet", index=False)

        shots = df[df["type.name"] == "Shot"]
        if not shots.empty:
            shots.to_parquet(f"{CLEAN_DIR}/shot-events/{m_id}.parquet", index=False)

    del df, raw
    gc.collect()

print("All matches downloaded and partitioned successfully!")