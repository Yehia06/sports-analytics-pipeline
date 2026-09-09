CREATE TABLE IF NOT EXISTS live_predictions (
    event_id VARCHAR(100) PRIMARY KEY,
    match_id BIGINT,
    timestamp VARCHAR(50),
    minute INT,
    second INT,
    event_type VARCHAR(50),
    team_name VARCHAR(100),
    player_name VARCHAR(100),
    loc_x DOUBLE PRECISION,
    loc_y DOUBLE PRECISION,
    dist_to_goal DOUBLE PRECISION,
    predicted_xg DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS win_probability_history (
    match_id BIGINT,
    minute INT,
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    home_win_prob DOUBLE PRECISION,
    draw_prob DOUBLE PRECISION,
    away_win_prob DOUBLE PRECISION,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agg_shots_by_zone (
    match_id BIGINT,
    team_name VARCHAR(100),
    zone VARCHAR(50),
    total_shots INT,
    total_xg DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS agg_team_momentum (
    match_id BIGINT,
    team_name VARCHAR(100),
    minute_bucket INT,
    action_count INT
);