# Real-Time Sports Analytics & Match Intelligence Platform

An end-to-end, distributed Big Data engineering pipeline designed to ingest, process, store, and visualize high-throughput professional football match events in real time.

The platform ingests live match event streams from the StatsBomb Open Data ecosystem, executes distributed stream transformations and MLlib inference using Apache Spark, persists cold/warm event history in an HDFS Parquet data lake, serves analytical aggregates via an indexed PostgreSQL database, orchestrates automated ETL workflows via Apache Airflow, and surfaces tactical match intelligence through Apache Superset.

---

## 1. System Architecture

<pre>
[ StatsBomb Open Data ]
          │
          ▼
 [ Python Ingestion & Cleaning ]
          │
          ▼
   [ Apache Kafka ] ─── (5 Topics: match-events, shot-events, etc.)
          │
          ▼
[ Apache Spark (Structured Streaming & MLlib) ]
     │                                 │
     ▼ (Silver Parquet Lake)           ▼ (Hot Serving Aggregates)
 [ Hadoop HDFS ]                  [ PostgreSQL ]
         │                               │
         └────────► [ Airflow ] ◄────────┘
                        │
                        ▼
               [ Apache Superset ] (Tactical Dashboards)
</pre>

### Core Architecture Layers:
* **Layer 1: Ingestion & Parsing:** Data acquisition scripts fetch raw match events from StatsBomb, flatten nested structures, and partition clean Parquet batches.
* **Layer 2: Real-Time Messaging (Kafka):** Decouples event producers from downstream consumers, offering backpressure handling, topic-based partitioning, and replay speed control.
* **Layer 3: Distributed Stream Processing (Spark):** PySpark Structured Streaming processes spatial coordinates, calculates rolling window momentum, and applies machine learning models in-flight.
* **Layer 4: Hybrid Storage (HDFS + PostgreSQL):** 
  * **HDFS (Data Lake):** Append-only, columnar Parquet storage (/raw, /processed, /stream, /features, /models, /predictions) for batch historical analysis and retraining.
  * **PostgreSQL (Serving Layer):** Relational, indexed data mart optimized for sub-second analytical dashboard queries.
* **Layer 5: Orchestration (Apache Airflow):** Automates daily pipeline workflows, monitoring HDFS ingestion health, executing database aggregations, and triggering model retraining.
* **Layer 6: Tactical BI & Visualization (Apache Superset):** Business intelligence dashboards serving real-time tactical insights to coaches, analysts, and performance teams.

---

## 2. Ingestion & Messaging Architecture

### Kafka Topics:
* **match-events**: Full flattened match event logs with positional and timestamp metadata.
* **shot-events**: Dedicated stream of shooting actions containing pitch (x, y) coordinates and shot execution details.
* **pass-events**: Passing sequences used to model team possession chains and pitch control.
* **pressure-events**: High-frequency defensive actions utilized for rolling window momentum scoring.
* **win-probability-updates**: Dynamic match-state probabilities computed and emitted in-flight by Spark ML inference.

### Producer Replay Modes (scripts/kafka_producer.py):
* **Bulk Mode:** Immediate replay of match archives for backfilling HDFS storage and training models offline.
* **Live Mode:** Paced event emission mimicking realistic 90-minute fixture timing with configurable speed scaling (e.g., --speed 10).
* **Held-Back Mode:** Preloads match data and awaits interactive user confirmation before streaming for live demo execution.

---

## 3. Distributed Machine Learning (Spark MLlib)

The processing tier incorporates three specialized MLlib models serialized in HDFS/local storage:
1. **Expected Goals (xG) Model:** Spatial regression evaluating pitch shot coordinates, distance-to-goal vectors, and shooting angles.
2. **Dynamic Win Probability Classifier:** Real-time probability model tracking win/draw/loss balance across match minutes.
3. **Player Tactical Clustering (K-Means):** Unsupervised clustering categorizing tactical player profiles based on passing, defensive actions, and physical engagement metrics.

---

## 4. Repository Structure

<pre>
sports-analytics-pipeline/
├── dags/
│   └── sports_pipeline_orchestration.py   # Airflow DAG for health checks, aggregations, and retraining
├── data/
│   ├── raw/                               # Raw downloaded StatsBomb JSON events
│   └── clean/                             # Cleaned Parquet event partitions
├── models/
│   ├── xg_model/                          # Spark MLlib Expected Goals model binaries
│   ├── win_prob_model/                    # Match Win Probability model binaries
│   └── kmeans_player_clusters/            # Player Tactical Clustering binaries
├── scripts/
│   ├── data_acquisition.py                # StatsBomb API fetcher
│   ├── data_cleaning.py                   # JSON flattening to partitioned Parquet
│   ├── create_topics.py                   # Kafka topic provisioning
│   ├── kafka_producer.py                  # Real-time event replay engine
│   └── task2_spark_streaming.py           # Spark Structured Streaming pipeline
├── docker-compose.kafka.yml               # Standalone Kafka infrastructure
└── README.md
</pre>

---

## 5. Standalone Execution & Verification

### Ingestion Standalone Setup:
<pre>
# 1. Start Kafka Broker and UI
docker compose -f docker-compose.kafka.yml up -d

# 2. Provision Kafka Topics
python scripts/create_topics.py

# 3. Stream Match Events (Live Paced Replay)
python scripts/kafka_producer.py --match-id &lt;MATCH_ID&gt; --mode live --speed 10
</pre>

### System Web Interfaces:
* **Kafka Management UI:** http://localhost:8080
* **Apache Airflow Webserver:** http://localhost:8085
* **Apache Superset BI Cockpit:** http://localhost:8088
* **Hadoop HDFS NameNode UI:** http://localhost:9870
