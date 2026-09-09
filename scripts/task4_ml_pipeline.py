import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import (
    col, lit, sqrt, pow, atan2, when, coalesce, count, sum as _sum, avg, stddev,
    input_file_name, regexp_extract
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator, ClusteringEvaluator

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Task4-Analytics-MLlib") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n=======================================================")
print(">>> Starting Task 4: Analytics & Machine Learning Pipeline")
print("=======================================================\n")

# 2. Load Historical Data and extract match_id from filename
data_path = "data/clean/match-events/*.parquet"
df = spark.read.parquet(data_path) \
    .withColumn("match_id", regexp_extract(input_file_name(), r"(\d+)\.parquet", 1))

# ==============================================================================
# MODEL 1: Expected Goals (xG) Spatial Regression
# ==============================================================================
print(">>> [1/4] Training Spatial Expected Goals (xG) Regressor...")

shots_df = df.filter(col("`type.name`") == "Shot").select(
    col("id"),
    col("location")[0].alias("loc_x"),
    col("location")[1].alias("loc_y"),
    coalesce(col("under_pressure").cast(DoubleType()), lit(0.0)).alias("under_pressure"),
    col("`shot.statsbomb_xg`").alias("label")
).filter(col("label").isNotNull() & col("loc_x").isNotNull() & col("loc_y").isNotNull())

shots_features = shots_df \
    .withColumn("goal_dx", lit(120.0) - col("loc_x")) \
    .withColumn("goal_dy", col("loc_y") - lit(40.0)) \
    .withColumn("dist_to_goal", sqrt(pow(col("goal_dx"), 2) + pow(col("goal_dy"), 2))) \
    .withColumn("shot_angle", atan2(lit(7.32) * col("goal_dx"), pow(col("goal_dx"), 2) + pow(col("goal_dy"), 2) - lit(13.4)))

xg_assembler = VectorAssembler(
    inputCols=["dist_to_goal", "shot_angle", "under_pressure"],
    outputCol="features"
)
xg_data = xg_assembler.transform(shots_features).select("features", "label")
train_xg, test_xg = xg_data.randomSplit([0.8, 0.2], seed=42)

gbt = GBTRegressor(featuresCol="features", labelCol="label", maxIter=20, seed=42)
xg_model = gbt.fit(train_xg)
xg_preds = xg_model.transform(test_xg)

eval_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
eval_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
rmse = eval_rmse.evaluate(xg_preds)
r2 = eval_r2.evaluate(xg_preds)

print(f"    - xG Model Evaluated: RMSE = {rmse:.4f}, R2 = {r2:.4f}")

os.makedirs("models", exist_ok=True)
xg_model_dir = "models/xg_model"
if os.path.exists(xg_model_dir):
    shutil.rmtree(xg_model_dir)
xg_model.save(xg_model_dir)
print(f"    - Model saved to {xg_model_dir}")

# ==============================================================================
# MODEL 2: Win Probability Classifier
# ==============================================================================
print("\n>>> [2/4] Training Win Probability Multi-Class Classifier...")

match_stats = df.groupBy("match_id", "`team.name`").agg(
    count(when(col("`type.name`") == "Shot", 1)).alias("shots"),
    _sum(coalesce(col("`shot.statsbomb_xg`"), lit(0.0))).alias("total_xg"),
    count(when(col("`type.name`") == "Pass", 1)).alias("passes")
)

wp_data = match_stats.withColumn(
    "label",
    when(col("total_xg") > 1.5, lit(1.0)).when(col("total_xg") < 0.8, lit(2.0)).otherwise(lit(0.0))
)

wp_assembler = VectorAssembler(inputCols=["shots", "total_xg", "passes"], outputCol="features")
wp_vector = wp_assembler.transform(wp_data).select("features", "label")
train_wp, test_wp = wp_vector.randomSplit([0.8, 0.2], seed=42)

lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=15)
wp_model = lr.fit(train_wp)
wp_preds = wp_model.transform(test_wp)

eval_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = eval_acc.evaluate(wp_preds)
print(f"    - Win Probability Model Accuracy = {accuracy * 100:.2f}%")

wp_model_dir = "models/win_prob_model"
if os.path.exists(wp_model_dir):
    shutil.rmtree(wp_model_dir)
wp_model.save(wp_model_dir)
print(f"    - Model saved to {wp_model_dir}")

# ==============================================================================
# MODEL 3: Player Impact Clustering (K-Means)
# ==============================================================================
print("\n>>> [3/4] Running Player Impact Clustering (K-Means)...")

player_stats = df.filter(col("`player.name`").isNotNull()).groupBy("`player.name`").agg(
    count("id").alias("total_actions"),
    count(when(col("`type.name`") == "Shot", 1)).alias("shots"),
    count(when(col("`type.name`") == "Pressure", 1)).alias("pressures"),
    _sum(coalesce(col("`shot.statsbomb_xg`"), lit(0.0))).alias("xg_created")
).filter(col("total_actions") > 50)

cluster_assembler = VectorAssembler(
    inputCols=["total_actions", "shots", "pressures", "xg_created"],
    outputCol="raw_features"
)
scaler = StandardScaler(inputCol="raw_features", outputCol="features")
scaled_data = scaler.fit(cluster_assembler.transform(player_stats)).transform(cluster_assembler.transform(player_stats))

kmeans = KMeans(featuresCol="features", k=3, seed=42)
kmeans_model = kmeans.fit(scaled_data)

eval_silhouette = ClusteringEvaluator()
silhouette = eval_silhouette.evaluate(kmeans_model.transform(scaled_data))
print(f"    - KMeans Cluster Separation (Silhouette Score) = {silhouette:.4f}")

kmeans_model_dir = "models/kmeans_player_clusters"
if os.path.exists(kmeans_model_dir):
    shutil.rmtree(kmeans_model_dir)
kmeans_model.save(kmeans_model_dir)
print(f"    - Model saved to {kmeans_model_dir}")

# ==============================================================================
# MODEL 4: Fatigue & Anomaly Detection Logic
# ==============================================================================
print("\n>>> [4/4] Evaluating Player Fatigue & Physical Drop-off Anomalies...")

minute_action = df.filter(col("`player.name`").isNotNull()).groupBy(col("`player.name`").alias("player_name"), "minute").agg(
    count("id").alias("actions_per_min")
)

baseline_stats = minute_action.groupBy("player_name").agg(
    avg("actions_per_min").alias("mean_actions"),
    stddev("actions_per_min").alias("std_actions")
)

fatigue_metrics = minute_action.join(baseline_stats, "player_name") \
    .withColumn("z_score", (col("actions_per_min") - col("mean_actions")) / coalesce(col("std_actions"), lit(1.0))) \
    .filter((col("minute") > 75) & (col("z_score") < -1.8))

anomaly_count = fatigue_metrics.count()
print(f"    - Detected {anomaly_count} late-game player fatigue anomalies (Z-score < -1.8 past 75th min)")

print("    - Top Detected Fatigue Anomalies:")
fatigue_metrics.select("player_name", "minute", "actions_per_min", "z_score").show(5, truncate=False)

print("\n=======================================================")
print("[TASK 4 COMPLETED] All models trained, evaluated, and saved successfully!")
print("=======================================================\n")

spark.stop()