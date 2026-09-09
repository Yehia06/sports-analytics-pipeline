import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:password@localhost:5432/sports_analytics")

df = pd.read_parquet("data/clean/match-events/3825907.parquet")

shots = df[df["type.name"] == "Shot"].copy()
shots["xg"] = shots["shot.statsbomb_xg"].fillna(0.0) if "shot.statsbomb_xg" in shots.columns else 0.0
shots["zone"] = shots["location"].apply(
    lambda loc: "Box" if loc is not None and len(loc) == 2 and loc[0] >= 102 else "Outside Box"
)

shots_agg = shots.groupby(["team.name", "zone"]).agg(
    total_shots=("id", "count"),
    total_xg=("xg", "sum")
).reset_index().rename(columns={"team.name": "team_name"})
shots_agg["match_id"] = 3825907

shots_agg.to_sql("agg_shots_by_zone", engine, if_exists="append", index=False)
print("agg_shots_by_zone saved successfully.")

momentum = df.groupby(["team.name", "minute"]).agg(
    action_count=("id", "count")
).reset_index().rename(columns={"team.name": "team_name", "minute": "minute_bucket"})
momentum["match_id"] = 3825907

momentum.to_sql("agg_team_momentum", engine, if_exists="append", index=False)
print("agg_team_momentum saved successfully.")