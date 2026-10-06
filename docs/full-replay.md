# NYC taxi full Kafka replay

Replace your project's `pipeline.py` with the supplied updated file. Keep your
existing `output/batch-green-full-v2.db`: it already contains the corrected full
batch baseline. No changes to the dashboard or database schema are required.
The existing project environment needs `pyarrow` and `confluent-kafka`.

## Changes

- Batch and consumer write up to 1,000 records in each SQLite transaction.
- Delivery metrics are updated once per transaction, rather than once per row.
- Kafka offsets for every partition in a fetched batch are committed synchronously
  only after the entire SQLite transaction succeeds. A failed database write
  rolls back the batch without acknowledging it. A Kafka commit failure stops
  the consumer; restarting can replay saved records, whose IDs prevent duplicate
  trip insertion. This is at-least-once delivery with idempotent storage.
- Progress prints approximately every five seconds, and at completion. Startup
  file hashing happens before the first producer/batch progress count.
- Producer progress counts queued deliveries. Only the final `Replay delivered`
  message confirms delivery callbacks and flush succeeded.
- Payment code 0 and missing payment values retain your uploaded validation.
- `--input` and `--csv` still both work. Producer's default delay remains 0.25
  seconds; use `--interval 0` for this full replay.
- Malformed JSON, invalid UTF-8, and Kafka tombstones are stored as rejections.

## 1. Start Kafka

From your project folder in PowerShell, with Docker Desktop running:

```powershell
docker compose up -d --wait
```

## 2. Terminal 1: start the full-run consumer

This uses a separate topic, group, and stream database from your 2,000-row test.
On the first run, the new stream database should not already contain unrelated data.

```powershell
python pipeline.py consume --topic taxi.full-202608-v1 --group taxi-full-202608-v1 --db output/stream-full-202608.db --batch-size 1000 --progress-every 5
```

The consumer may initially wait for the topic, which the producer creates.
Keep this terminal running while both producers finish.

## 3. Terminal 2: replay yellow, then green

Run these sequentially. Do not include `--limit`, `--bad-every`, or
`--duplicate-every` for the baseline replay.

```powershell
python pipeline.py produce --input data/yellow_tripdata_2026-08.parquet --topic taxi.full-202608-v1 --interval 0 --progress-every 5
python pipeline.py produce --input data/green_tripdata_2026-08.parquet --topic taxi.full-202608-v1 --interval 0 --progress-every 5
```

Both commands must finish with `Replay delivered`.
If a run is interrupted, restarting a producer reads its file from the start;
existing IDs prevent duplicate trip insertion. Stop/restart the consumer using
the same group and stream database to resume. Do not pair an old committed group
with an empty replacement database: its offsets would skip previously saved data.

## 4. Wait for the consumer to catch up

In Terminal 2:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group taxi-full-202608-v1
```

After both producers succeed, wait until `LAG` is 0 for every partition.
Then stop Terminal 1 with Ctrl+C. Run reconciliation:

```powershell
python reconcile.py --batch output/batch-green-full-v2.db --stream output/stream-full-202608.db
```

Expected: `PASS: trip records and five-minute summaries match`.
On a first clean replay, the stream database should have 3,377,400 accepted trips
and 3 rejected records (`dropoff precedes pickup`). The stream delivery metrics
will differ from the repaired batch database's historical rejection/recovery
metrics; reconciliation compares trip records and summaries, not that history.

## 5. Verify database counts

Paste this entire block into PowerShell:

```powershell
@'
import sqlite3
c = sqlite3.connect('file:output/stream-full-202608.db?mode=ro', uri=True)
print('Trips:', c.execute('SELECT COUNT(*) FROM trips').fetchone()[0])
print('Rejections:', c.execute('SELECT reason, COUNT(*) FROM rejected GROUP BY reason').fetchall())
print('Metrics:', c.execute('SELECT name, value FROM metrics ORDER BY name').fetchall())
c.close()
'@ | python -
```

## Validation performed before delivery

Six local tests passed: yellow/green payment normalization; duplicate IDs and
summary stability; whole-batch rollback; malformed messages and offsets across
multiple partitions; database persistence before a simulated Kafka commit
failure and safe replay; CSV limits and partial final batches. Python compilation
also passed. Broker behavior and full-dataset throughput require your local run;
a live Kafka broker and Parquet dependencies were unavailable in the test environment.
