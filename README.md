# Person 1 — Data Engineering: Sources & Ingestion

## What's already done for you
I ran this pipeline myself against the **real StatsBomb Open Data** repo to prove
it works, and everything up to "send to Kafka" is fully built and tested:

- `scripts/data_acquisition.py` — downloads competitions/matches/events/lineups
  from StatsBomb's public GitHub repo. **Tested live**: downloaded 7 real matches
  (La Liga, Champions League finals, Euro 2020) successfully.
- `scripts/data_cleaning.py` — flattens raw JSON into clean Parquet tables split
  by event type (matching your Kafka topics). **Tested live**: produced
  26,042 match-events / 200 shot-events / 7,486 pass-events / 2,137
  pressure-events rows from those 7 matches, with no errors.
- `scripts/kafka_producer.py` — replays cleaned events into Kafka, in bulk or
  paced "live" mode with a configurable speed multiplier, plus a "held-back"
  match mode. Compiles clean; logic is complete.
- `scripts/create_topics.py` — creates all 5 required Kafka topics.
- `docker-compose.kafka.yml` — a local Kafka broker so you can test the producer
  standalone before it's wired into Person 5's full docker-compose.

**The one thing I genuinely cannot do for you:** I don't have a persistent
environment where I can run a live Kafka broker and keep it running for you to
connect to — I only have a sandboxed container with no Docker and no open
network to Docker Hub. So the download + cleaning steps below you can basically
skip re-checking (I already proved they work) — you just need to actually run
them yourself to get the full dataset on your machine, then stand up Kafka and
run the producer against it. Below is the **exact** sequence.

---

## Step-by-step: run this on your own machine

### 1. Prerequisites
- Python 3.9+
- Docker Desktop (or Docker Engine + Compose) installed and running
- ~2–5 GB free disk space (StatsBomb data + Parquet output)

### 2. Get the project files and install dependencies
```bash
cd person1                     # the folder I've packaged for you
python3 -m venv venv
source venv/bin/activate       # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Download the StatsBomb data
By default this pulls La Liga 2020/21, four Champions League final matches
(2014/15–2017/18 — StatsBomb's free tier only publishes the final of each CL
season, not the full tournament, so we pull several seasons' finals to get a
decent sample), and UEFA Euro 2020.

```bash
# Quick test run — just 3 matches per competition, finishes in ~1 minute
python scripts/data_acquisition.py --max-matches 3

# Full run — every match in the default competitions (hundreds of matches,
# will take a while and use a few GB)
python scripts/data_acquisition.py
```
Optional: see everything available and pick your own competitions —
```bash
python scripts/data_acquisition.py --list
python scripts/data_acquisition.py --comp 43 --season 106   # example: pick your own
```
Raw JSON lands in `data/raw/`.

### 4. Clean the data
```bash
python scripts/data_cleaning.py
```
This reads everything in `data/raw/events/` and writes Parquet files to
`data/clean/<topic-name>/<match_id>.parquet`, plus a combined `all.parquet`
per topic. You'll see per-match counts printed as it runs.

### 5. Start Kafka locally
```bash
docker compose -f docker-compose.kafka.yml up -d
```
Wait ~15–20 seconds for the broker to be ready. Check it's up:
```bash
docker ps                      # you should see zookeeper, kafka, kafka-ui running
```
Open http://localhost:8080 to browse topics/messages visually (kafka-ui).

### 6. Create the Kafka topics
```bash
python scripts/create_topics.py
```
Expected output: confirmation that `match-events`, `shot-events`,
`pass-events`, `pressure-events`, and `win-probability-updates` were created.

### 7. Run the producer
Pick a `match_id` from your cleaned data (list them with):
```bash
python -c "from pathlib import Path; print([p.stem for p in Path('data/clean/match-events').glob('*.parquet') if p.stem != 'all'])"
```

Then replay it:
```bash
# Fast bulk replay — fires all events immediately (good for backfilling
# historical data so Person 3's storage layer and Person 4's model training
# have something to consume right away)
python scripts/kafka_producer.py --match-id <MATCH_ID> --mode bulk

# Realistic live pace, real-time speed (a 90-minute match takes ~90 minutes
# to stream — good for a final demo)
python scripts/kafka_producer.py --match-id <MATCH_ID> --mode live --speed 1

# Realistic pace but sped up 20x (a ~95 min match streams in ~5 min — good
# for rehearsing the demo without waiting the full match length)
python scripts/kafka_producer.py --match-id <MATCH_ID> --mode live --speed 20

# Replay every match you've downloaded, in bulk (seed all historical data at once)
python scripts/kafka_producer.py --all --mode bulk

# Simulate a "held-back" live match — the producer loads it and waits for you
# to press ENTER before streaming, so you can kick off a live demo on command
python scripts/kafka_producer.py --match-id <MATCH_ID> --mode live --speed 10 --held-back
```

### 8. Verify it's working
- In kafka-ui (http://localhost:8080) click into `match-events` → Messages,
  you should see JSON events flowing in as the producer runs.
- Or from the command line:
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic match-events --from-beginning --max-messages 5
```

### 9. When you're done testing
```bash
docker compose -f docker-compose.kafka.yml down
```
(Person 5 will later fold Kafka into the full project `docker-compose.yml`
alongside Spark/HDFS/PostgreSQL/Superset/Airflow — this standalone file was
just so you could build/test in isolation without waiting on anyone else.)

---

## What to hand off to your teammates
- **Person 2** needs: the 5 topic names (already created) and the JSON event
  shape (see any `.parquet` file's columns, or just consume a few messages from
  `match-events` — every field on a StatsBomb event, flattened, plus
  `event_epoch_seconds` for time-based windowing).
- **Person 3** needs: nothing from you directly yet, but be ready to point your
  HDFS `/raw` folder-structure at wherever you decide to persist `data/raw/`
  and `data/clean/` long-term (or just let Person 2's Spark job write straight
  to HDFS from Kafka — that's the more typical architecture, worth a quick
  sync with them).
- **Person 5** needs: `docker-compose.kafka.yml` as a reference to merge into
  the full project compose file.

## Notes / things worth flagging to your team
- StatsBomb's free Champions League data is finals-only per season (not full
  tournaments) — I've documented this in the script; if your team wants more
  CL match volume, either accept fewer CL matches or lean more heavily on
  La Liga/Euro 2020 which have full-competition coverage.
- The `win-probability-updates` topic is created but not written to by this
  layer — it's populated later by Person 2's streaming job calling Person 4's
  trained model. I wrote a schema placeholder (`data/clean/win-probability-updates/SCHEMA_ONLY.parquet`)
  so the expected column contract is visible to everyone from day one.
