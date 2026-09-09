import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    sqrt, pow, atan2, when, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, LongType, ArrayType
)

# 1. Initialize Spark Session with Kafka Connector
spark = SparkSession.builder \
    .appName("Task2-Sports-Processor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema matched to StatsBomb parquet columns
schema = StructType([
    StructField("id", StringType(), True),
    StructField("match_id", LongType(), True),
    StructField("index", LongType(), True),
    StructField("period", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("minute", IntegerType(), True),
    StructField("second", IntegerType(), True),
    StructField("type.name", StringType(), True),
    StructField("team.name", StringType(), True),
    StructField("player.name", StringType(), True),
    StructField("location", ArrayType(DoubleType()), True),
    StructField("under_pressure", DoubleType(), True),
    StructField("shot.statsbomb_xg", DoubleType(), True)
])

# 3. Read Stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "match-events,shot-events,pass-events,pressure-events") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse JSON & Extract Coordinates
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("d")) \
    .select(
        col("d.id").alias("event_id"),
        col("d.match_id").alias("match_id"),
        col("d.timestamp").alias("timestamp"),
        col("d.minute").alias("minute"),
        col("d.second").alias("second"),
        col("d.`type.name`").alias("event_type"),
        col("d.`team.name`").alias("team_name"),
        col("d.`player.name`").alias("player_name"),
        col("d.location")[0].alias("loc_x"),
        col("d.location")[1].alias("loc_y"),
        coalesce(col("d.under_pressure"), lit(0.0)).alias("under_pressure"),
        col("d.`shot.statsbomb_xg`").alias("statsbomb_xg")
    )

# 5. Feature Engineering: Goal Distance & Shot Angle (Pitch 120x80, Goal at 120,40)
enriched = parsed_stream \
    .withColumn("goal_dx", lit(120.0) - coalesce(col("loc_x"), lit(60.0))) \
    .withColumn("goal_dy", coalesce(col("loc_y"), lit(40.0)) - lit(40.0)) \
    .withColumn("dist_to_goal", sqrt(pow(col("goal_dx"), 2) + pow(col("goal_dy"), 2))) \
    .withColumn("shot_angle", atan2(lit(7.32) * col("goal_dx"), pow(col("goal_dx"), 2) + pow(col("goal_dy"), 2) - lit(13.4)))

# 6. Real-Time Inference: Expected Goals (xG) Baseline
with_inference = enriched.withColumn(
    "predicted_xg",
    when(col("event_type") == "Shot", 
         lit(1.0) / (lit(1.0) + (col("dist_to_goal") * lit(0.12)) + (col("under_pressure") * lit(0.2))))
    .otherwise(lit(0.0))
)

# 7. Output Stream to Console
query = with_inference \
    .select("minute", "team_name", "player_name", "event_type", "dist_to_goal", "predicted_xg") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

print(">>> Spark Structured Streaming Active. Processing live events...")
query.awaitTermination()