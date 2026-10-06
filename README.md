# NYC Taxi Analytics — batch and historical event replay

A local-first rebuild of the concepts in Darshil Parmar's Uber ETL tutorial, extended with Kafka ingestion, validation, durable deduplication, incremental export, and a dashboard. Sample data is synthetic, not measured NYC results. This is a runnable first milestone, not a completed cloud deployment.

## What is implemented

- CSV batch loader with transactional SQLite storage and five-minute SQL summaries.
- Python Kafka producer with configurable replay speed, duplicate injection and invalid-record injection.
- Python Kafka consumer: durable storage before synchronous offset commit; reruns use persistent event IDs.
- Rejected-record table with reasons and raw payloads (local quarantine, not a separate Kafka topic).
- Dashboard with trip counts, recorded totals, pickup-area ranking and event-time window chart.
- Optional BigQuery staging-to-MERGE exporter; a Mage custom-block adapter.
- Offline tests and a 120-row synthetic fixture.

Kafka uses a Python consumer in this first milestone. Spark, Debezium, GCS ingestion and a packaged Mage service are future extensions. BigQuery export is a manual snapshot sync, not continuous streaming into BigQuery.

## Quick start — Python only

Install Python 3.11 or 3.12. Open a terminal in this extracted folder.

```sh
python pipeline.py batch --csv data/sample.csv
python dashboard.py
```

Open http://localhost:8080. Expect 120 unique trips. Stop with Ctrl+C. Rerunning the batch increments duplicate-delivery metrics but leaves trip counts and monetary totals unchanged.

## Streaming demo — Docker Desktop + Python

Docker Desktop must be running (on Windows enable its WSL2 backend). Python runs on your laptop; Kafka runs in Docker. No GCP account is needed.

Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
docker compose up -d --wait
```

If PowerShell activation is blocked, use `.\.venv\Scripts\python.exe` in place of `python` without activating. macOS/Linux:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
docker compose up -d --wait
```

In terminal 1 (use your virtual environment):

```sh
python pipeline.py consume --db output/stream.db
```

In terminal 2:

```sh
python dashboard.py --db output/stream.db --port 8081
```

In terminal 3:

```sh
python pipeline.py produce --csv data/sample.csv --interval 0.25 --duplicate-every 10 --bad-every 15
```

Open http://localhost:8081. With a new database/topic, expect 112 valid unique trips, 8 unique rejected events and 8 valid duplicate deliveries. Rejected duplicate deliveries also increment the rejection-delivery metric. The dashboard shows unique rejected events. Invalid-record injection changes payloads but preserves IDs; use this as a separate experiment from clean reconciliation.

For recovery: stop only the consumer with Ctrl+C during replay, let the producer finish, then restart the consumer with the same group and database. It processes outstanding events. Kafka offsets and records persist in a Docker volume. Stop services with `docker compose down`; do not remove the volume unless intentionally resetting the experiment.

## Clean batch/stream reconciliation

Use fresh local databases and a fresh Kafka topic for an independent experiment. Keep the same source CSV on both paths.

```sh
python pipeline.py batch --csv data/sample.csv --db output/batch-clean.db
python pipeline.py consume --topic taxi.clean.v1 --group taxi-clean-v1 --db output/stream-clean.db
```

In another terminal:

```sh
python pipeline.py produce --csv data/sample.csv --topic taxi.clean.v1 --interval 0
```

After the consumer catches up, stop it and run:

```sh
python reconcile.py --batch output/batch-clean.db --stream output/stream-clean.db
```

This compares every stored trip and every five-minute summary, not just counts. Rerun the producer to demonstrate deduplication. To rerun from Kafka history into a fresh database use a **new consumer group**; an existing group has already committed offsets.

## Original dataset

The source tutorial calls the project Uber but describes the data as NYC TLC taxi records. Download its `data/uber_data.csv` from the source repository and save it as `data/uber_data.csv`:

https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project/blob/main/data/uber_data.csv

Then substitute that path in batch and producer commands. Expected fields: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `total_amount`, `payment_type`, and either `PULocationID` or both `pickup_latitude` and `pickup_longitude`. This milestone supports CSV, not Parquet. Coordinate-based records use approximate 0.01-degree grids, not official zones. Zone IDs are accepted but no zone-name lookup is included.

Source timestamps are interpreted as NYC local wall time without an offset; original historical timestamps are preserved. These are completed-trip records, not a live GPS feed. Reports group by pickup time even though the records contain drop-off/fare information.

## Data semantics and limitations

- Trip identity is SHA-256(source file bytes + row position). Exact same file copies and replays retain IDs; genuinely identical rows at different positions are retained as separate records. Reordered, edited or reformatted files have different IDs. Cross-file business deduplication is not implemented because these records lack a guaranteed unique source trip ID.
- Transport is at least once. SQLite primary keys make storage idempotent for these replay IDs; this is not a claim of end-to-end exactly-once processing.
- Summaries query all stored events, so late records revise historical windows. There is no watermark or final-window policy yet.
- Negative distance, non-finite values, invalid timestamps/payment types and reversed trip times are quarantined. Negative recorded totals are preserved as possible adjustments. Validation is illustrative and should be extended against the dataset dictionary.
- No automatic update/delete propagation for an existing trip ID. SQLite is a single-machine demo sink. Kafka is a single broker with no high availability, local loopback only.
- Delivery counts are diagnostic; duplicates legitimately increase these metrics. Stored trips and summary counts do not increase on duplicate delivery.
- Dollar totals use floating point for the demo; use decimal/numeric monetary types for accounting.

## Optional BigQuery sync

Create a dataset first. Authenticate with Application Default Credentials using your own Google Cloud account; do not commit credential files. Install `requirements-cloud.txt`.

```sh
python -m pip install -r requirements-cloud.txt
gcloud auth application-default login
python bigquery_export.py --db output/taxi.db --dataset YOUR_PROJECT.YOUR_DATASET
```

The exporter creates `trips`, uploads a snapshot in chunks to an expiring staging table, and MERGEs missing IDs. It preserves existing records; it does not update them. It deletes staging afterward. Serialize exports to the same target; concurrent MERGEs are outside this demo's guarantees. Cloud API calls can incur charges. Use Looker Studio with your BigQuery table to reproduce cloud reporting. Neither credentials nor a cloud deployment is included.

## Mage integration

Keep using Mage for scheduled batch orchestration. Install it in your preferred supported environment. Make `pipeline.py` importable and use `mage-files/rebuild_batch.py` as a custom block. Set `TAXI_CSV` and `TAXI_DB` to absolute paths. The adapter runs the local batch stage; scheduling BigQuery sync is a separate step. This package does not include a Mage project metadata export or a Mage Docker service.

## Tests

```sh
python -m unittest discover -s tests -v
```

Five offline tests pass in the build environment: durable rerun deduplication, validation/quarantine, reverse-order aggregation, source-path-independent identity and zone-based locations. Dashboard HTTP and CLI export were smoke-tested. Kafka/Docker, Mage and BigQuery were not executed in the build environment. Run the reconciliation/recovery demos before claiming those integrations as verified.

## Next milestones

1. Verify Kafka locally and record actual throughput/recovery results.
2. Add Spark Structured Streaming as a second consumer with event-time watermarks, checkpointing and Parquet/GCS output. Reconcile against this reference sink.
3. Package Mage orchestration and deploy incremental BigQuery reporting.
4. Add PostgreSQL/Debezium only when demonstrating operational inserts, updates and deletes.

## Attribution

See `reference/PROVENANCE.md`. The rebuild uses independently written implementation code; upstream tutorial files are linked rather than bundled. Present it as a tutorial-derived extension and describe your own contributions accurately.
