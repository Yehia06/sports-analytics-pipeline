import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:password@localhost:5432/sports_analytics")

# Populate win_probability_history
win_prob_df = pd.DataFrame([
    {"match_id": 3825907, "minute": 15, "home_team": "Atlético Madrid", "away_team": "Sporting Gijón", "home_win_prob": 0.52, "draw_prob": 0.30, "away_win_prob": 0.18},
    {"match_id": 3825907, "minute": 30, "home_team": "Atlético Madrid", "away_team": "Sporting Gijón", "home_win_prob": 0.58, "draw_prob": 0.28, "away_win_prob": 0.14},
    {"match_id": 3825907, "minute": 45, "home_team": "Atlético Madrid", "away_team": "Sporting Gijón", "home_win_prob": 0.45, "draw_prob": 0.33, "away_win_prob": 0.22},
    {"match_id": 3825907, "minute": 60, "home_team": "Atlético Madrid", "away_team": "Sporting Gijón", "home_win_prob": 0.40, "draw_prob": 0.30, "away_win_prob": 0.30},
    {"match_id": 3825907, "minute": 75, "home_team": "Atlético Madrid", "away_team": "Sporting Gijón", "home_win_prob": 0.35, "draw_prob": 0.25, "away_win_prob": 0.40},
    {"match_id": 3825907, "minute": 90, "home_team": "Atlético Madrid", "away_team": "Sporting Gijón", "home_win_prob": 0.15, "draw_prob": 0.15, "away_win_prob": 0.70},
])
win_prob_df.to_sql("win_probability_history", engine, if_exists="append", index=False)

# Populate live_predictions sample for shot map
shots_sample = pd.DataFrame([
    {"event_id": "shot-1", "match_id": 3825907, "timestamp": "00:12:04", "minute": 12, "second": 4, "event_type": "Shot", "team_name": "Atlético Madrid", "player_name": "Antoine Griezmann", "loc_x": 105.4, "loc_y": 38.2, "dist_to_goal": 14.7, "predicted_xg": 0.32},
    {"event_id": "shot-2", "match_id": 3825907, "timestamp": "00:34:19", "minute": 34, "second": 19, "event_type": "Shot", "team_name": "Sporting Gijón", "player_name": "Antonio Sanabria", "loc_x": 112.1, "loc_y": 42.0, "dist_to_goal": 8.1, "predicted_xg": 0.65},
    {"event_id": "shot-3", "match_id": 3825907, "timestamp": "00:67:51", "minute": 67, "second": 51, "event_type": "Shot", "team_name": "Sporting Gijón", "player_name": "Carlos Castro", "loc_x": 114.5, "loc_y": 39.5, "dist_to_goal": 5.5, "predicted_xg": 0.78},
    {"event_id": "shot-4", "match_id": 3825907, "timestamp": "00:81:10", "minute": 81, "second": 10, "event_type": "Shot", "team_name": "Atlético Madrid", "player_name": "Koke", "loc_x": 96.0, "loc_y": 55.0, "dist_to_goal": 28.3, "predicted_xg": 0.05}
])
shots_sample.to_sql("live_predictions", engine, if_exists="append", index=False)
print("Visualization seed records stored successfully.")