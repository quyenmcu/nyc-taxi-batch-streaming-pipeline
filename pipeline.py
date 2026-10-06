"""Local batch and Kafka event-replay pipeline; Python 3.11+."""
import argparse
import csv
from collections import Counter
from contextlib import closing
from itertools import islice
import hashlib
import json
import math
import sqlite3
import time
from datetime import datetime
from pathlib import Path

DEFAULT_DB = 'output/taxi.db'


def connect(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS trips (
      trip_id TEXT PRIMARY KEY, pickup TEXT NOT NULL, dropoff TEXT NOT NULL,
      pickup_area TEXT NOT NULL, distance REAL NOT NULL, total REAL NOT NULL,
      payment_type INTEGER , payload TEXT NOT NULL, processed_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS rejected (
      event_id TEXT PRIMARY KEY, reason TEXT NOT NULL, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS metrics (
      name TEXT PRIMARY KEY, value INTEGER NOT NULL);
    CREATE VIEW IF NOT EXISTS five_minute_summary AS
      SELECT substr(pickup,1,14) || printf('%02d',CAST(substr(pickup,15,2) AS INTEGER)/5*5)
        || ':00' AS window_start, pickup_area,
        count(*) AS trips, sum(total) AS recorded_total, avg(distance) AS average_distance
      FROM trips GROUP BY window_start,pickup_area;
    ''')
    return db


def normalize(row):
    if not isinstance(row, dict):
        raise ValueError('event must be a JSON object')

    def stamp(key):
        text = str(row[key]).strip()

        try:
            value = datetime.fromisoformat(text.replace('Z', '+00:00'))
        except ValueError:
            value = None
            for fmt in ('%m/%d/%Y %H:%M', '%m/%d/%Y %H:%M:%S'):
                try:
                    value = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue

            if value is None:
                raise ValueError(f'Unsupported timestamp: {text}')

        if value.tzinfo is not None:
            raise ValueError(
                'source timestamps must use NYC local time without an offset'
            )

        return value.isoformat(sep=' ', timespec='seconds')

    if 'tpep_pickup_datetime' in row:
        pickup = stamp('tpep_pickup_datetime')
        dropoff = stamp('tpep_dropoff_datetime')
    elif 'lpep_pickup_datetime' in row:
        pickup = stamp('lpep_pickup_datetime')
        dropoff = stamp('lpep_dropoff_datetime')
    else:
        raise ValueError('missing taxi pickup/dropoff timestamp columns')

    if dropoff < pickup:
        raise ValueError('dropoff precedes pickup')

    distance = float(row['trip_distance'])
    total = float(row['total_amount'])

    if not math.isfinite(distance) or distance < 0:
        raise ValueError('invalid trip distance')

    if not math.isfinite(total):
        raise ValueError('non-finite total amount')

    payment_raw = row.get('payment_type')

    if payment_raw in (None, ''):
        payment = None
    else:
        payment = float(payment_raw)
        if (
            not math.isfinite(payment)
            or not payment.is_integer()
            or not 0 <= payment <= 6
        ):
            raise ValueError('invalid payment type')
        payment = int(payment)

    if row.get('PULocationID') not in (None, ''):
        area = 'zone:' + str(int(row['PULocationID']))
    else:
        lat = float(row['pickup_latitude'])
        lon = float(row['pickup_longitude'])

        if (
            not math.isfinite(lat)
            or not math.isfinite(lon)
            or not -90 <= lat <= 90
            or not -180 <= lon <= 180
        ):
            raise ValueError('invalid pickup coordinates')

        area = f'grid:{lat:.2f},{lon:.2f}'

    return pickup, dropoff, area, distance, total,payment


def event_id(row, source, index):
    # Identity is source-file digest + row position, NOT a claim of a TLC trip ID.
    return hashlib.sha256(f'{source}:{index}'.encode()).hexdigest()


def events(path):
    path = Path(path)

    digest = hashlib.sha256()
    with path.open('rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            digest.update(chunk)

    source_digest = digest.hexdigest()

    if path.suffix.lower() == '.parquet':
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError(
                'Parquet support requires: python -m pip install pyarrow'
            ) from exc

        parquet = pq.ParquetFile(path)
        index = 0

        for batch in parquet.iter_batches(batch_size=1000):
            for raw_row in batch.to_pylist():
                # Convert timestamp objects into JSON-compatible strings.
                row = {
                    key: (
                        value.isoformat()
                        if isinstance(value, datetime)
                        else (
                            None
                            if isinstance(value, float) 
                            and not math.isfinite(value)
                            else value
                        )
                    )
                    for key, value in raw_row.items()
                }

                yield {
                    'trip_id': event_id(row, source_digest, index),
                    'trip': row,
                }
                index += 1

    elif path.suffix.lower() == '.csv':
        with path.open(newline='', encoding='utf-8-sig') as file:
            for index, row in enumerate(csv.DictReader(file)):
                yield {
                    'trip_id': event_id(row, source_digest, index),
                    'trip': row,
                }

    else:
        raise ValueError('Input file must be .csv or .parquet')


def _process_one(db, envelope):
    """Write inside the caller's transaction; never commit here."""
    try:
        raw = json.dumps(envelope, sort_keys=True, allow_nan=False)
    except (ValueError, TypeError, OverflowError) as exc:
        raw = repr(envelope)
        identity = hashlib.sha256(raw.encode()).hexdigest()
        db.execute('INSERT OR IGNORE INTO rejected VALUES (?,?,?)',
                   (identity, str(exc), raw))
        return 'rejected'
    identity = hashlib.sha256(raw.encode()).hexdigest()
    try:
        if not isinstance(envelope, dict):
            raise ValueError('envelope must be an object')
        trip_id = envelope['trip_id']
        if not isinstance(trip_id, str) or not trip_id:
            raise ValueError('missing trip ID')
        identity = trip_id
        row = envelope['trip']
        values = normalize(row)
    except (ValueError, KeyError, TypeError, OverflowError) as exc:
        db.execute('INSERT OR IGNORE INTO rejected VALUES (?,?,?)',
                   (identity, str(exc), raw))
        return 'rejected'
    # Only an existing trip ID is ignored. Other database errors abort the batch.
    cursor = db.execute(
        'INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?) '
        'ON CONFLICT(trip_id) DO NOTHING',
        (trip_id, *values, json.dumps(row, sort_keys=True, allow_nan=False),
         time.time()))
    return 'inserted' if cursor.rowcount else 'duplicate'


def increment(db, name, amount=1):
    db.execute('INSERT INTO metrics VALUES (?,?) ON CONFLICT(name) '
               'DO UPDATE SET value=value+excluded.value', (name, amount))


def _record_counts(db, counts):
    for result, amount in counts.items():
        increment(db, result + '_deliveries', amount)


def process_many(db, envelopes):
    counts = Counter()
    with db:
        for envelope in envelopes:
            counts[_process_one(db, envelope)] += 1
        _record_counts(db, counts)
    return counts


def process(db, envelope):
    """Compatibility entry point for callers that process one event."""
    return next(iter(process_many(db, [envelope])))


class Progress:
    def __init__(self, label, every=5):
        self.label = label
        self.every = every
        self.started = self.last = time.monotonic()
        self.counts = Counter()

    def update(self, counts):
        self.counts.update(counts)
        self.report()

    def report(self, force=False):
        now = time.monotonic()
        if not force and now - self.last < self.every:
            return
        elapsed = max(now - self.started, 0.001)
        processed = sum(self.counts.values())
        print(json.dumps({
            'stage': self.label, 'processed': processed,
            **dict(self.counts), 'elapsed_seconds': round(elapsed, 1),
            'records_per_second': round(processed / elapsed, 1),
        }), flush=True)
        self.last = now


def batch(path, db_path, limit=0, batch_size=1000, progress_every=5):
    progress = Progress('batch', progress_every)
    print(f'Batch starting: {path}; batch_size={batch_size}', flush=True)
    source = events(path)
    if limit:
        source = islice(source, limit)
    with closing(connect(db_path)) as db:
        while True:
            chunk = list(islice(source, batch_size))
            if not chunk:
                break
            progress.update(process_many(db, chunk))
    progress.report(force=True)
    print(json.dumps(dict(progress.counts)), flush=True)
    return dict(progress.counts)


def producer(args):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({'bootstrap.servers': args.broker})
    futures = admin.create_topics([NewTopic(args.topic, 1, 1)])
    for future in futures.values():
        try:
            future.result()
        except Exception as exc:
            if 'TOPIC_ALREADY_EXISTS' not in str(exc):
                raise
    producer = Producer({'bootstrap.servers': args.broker, 'enable.idempotence': True})
    errors = []
    progress = Progress('produce', args.progress_every)
    print(f'Replay starting: {args.csv}', flush=True)
    def delivered(error, message):
        if error:
            errors.append(str(error))
    for index, event in enumerate(events(args.csv), 1):
        if args.bad_every and index % args.bad_every == 0:
            event['trip']['trip_distance'] = '-1'
        for _ in range(2 if args.duplicate_every and index % args.duplicate_every == 0 else 1):
            payload = json.dumps(event, allow_nan=False)
            while True:
                try:
                    producer.produce(args.topic, key=event['trip_id'], value=payload, on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
            progress.update({'queued_deliveries': 1})
            if errors:
                raise RuntimeError(f'Delivery failed: {errors[0]}')
        if args.interval:
            time.sleep(args.interval)
        if args.limit and index >= args.limit:
            break
    pending = producer.flush(30)
    if pending or errors:
        raise RuntimeError(f'undelivered={pending}, errors={errors}')
    progress.report(force=True)
    print('Replay delivered. Original event timestamps preserved.', flush=True)


def process_messages(db, messages):
    """Persist a fetched Kafka batch atomically and return its next offsets."""
    counts = Counter()
    offsets = {}
    with db:
        for message in messages:
            if message.error():
                raise RuntimeError(message.error())
            payload = message.value()
            try:
                if payload is None:
                    raise ValueError('Kafka tombstone has no event payload')
                envelope = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
                identity = f'{message.topic()}:{message.partition()}:{message.offset()}'
                raw = (payload.decode('utf-8', errors='replace')
                       if isinstance(payload, bytes) else str(payload))
                db.execute('INSERT OR IGNORE INTO rejected VALUES (?,?,?)',
                           (identity, str(exc), raw))
                result = 'rejected'
            else:
                result = _process_one(db, envelope)
            counts[result] += 1
            key = (message.topic(), message.partition())
            offsets[key] = max(offsets.get(key, 0), message.offset() + 1)
        _record_counts(db, counts)
    return counts, offsets


def consumer(args):
    from confluent_kafka import Consumer, TopicPartition
    client = Consumer({
        'bootstrap.servers': args.broker, 'group.id': args.group,
        'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
        'enable.auto.offset.store': False,
    })
    db = None
    progress = Progress('consume', args.progress_every)
    try:
        db = connect(args.db)
        client.subscribe([args.topic])
        print(f'Consumer running: {args.topic}; group={args.group}; '
              f'batch_size={args.batch_size}. Ctrl+C to stop.', flush=True)
        while True:
            messages = client.consume(num_messages=args.batch_size, timeout=1.0)
            if not messages:
                progress.report()
                continue
            counts, offsets = process_messages(db, messages)
            # SQLite has committed the entire fetched batch BEFORE Kafka commits.
            # A crash/commit failure causes replay; trip IDs prevent double counting.
            committed = client.commit(offsets=[
                TopicPartition(topic, partition, offset)
                for (topic, partition), offset in offsets.items()
            ], asynchronous=False)
            for partition in committed or []:
                if partition.error:
                    raise RuntimeError(f'Kafka offset commit failed: {partition.error}')
            progress.update(counts)
    except KeyboardInterrupt:
        print('Consumer stopped. Restart with the same group and database to resume.',
              flush=True)
    finally:
        # No unpersisted offsets are acknowledged during shutdown.
        try:
            client.close()
        finally:
            if db is not None:
                db.close()
            progress.report(force=True)


def export_csv(db_path, destination):
    Path(destination).parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as db, open(destination, 'w', newline='', encoding='utf-8') as file:
        cursor = db.execute('SELECT trip_id,pickup,dropoff,pickup_area,distance,total,payment_type FROM trips ORDER BY trip_id')
        writer = csv.writer(file)
        writer.writerow([item[0] for item in cursor.description])
        writer.writerows(cursor)
    print(destination)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('batch', 'produce'):
        p = sub.add_parser(name)
        p.add_argument('--input', '--csv',
            dest='csv',
            default='data/sample.csv',
            help='Path to a CSV or Parquet file',)
        p.add_argument('--progress-every', type=float, default=5)
        if name == 'batch':
            p.add_argument('--db', default=DEFAULT_DB)
            p.add_argument('--batch-size', type=int, default=1000)
            p.add_argument('--limit', type=int, default=0)
        else:
            p.add_argument('--broker', default='localhost:9092')
            p.add_argument('--topic', default='taxi.trips')
            p.add_argument('--interval', type=float, default=0.25)
            p.add_argument('--limit', type=int, default=0)
            p.add_argument('--duplicate-every', type=int, default=0)
            p.add_argument('--bad-every', type=int, default=0)
    p = sub.add_parser('consume')
    p.add_argument('--broker', default='localhost:9092')
    p.add_argument('--topic', default='taxi.trips')
    p.add_argument('--group', default='taxi-local-v1')
    p.add_argument('--db', default='output/stream.db')
    p.add_argument('--batch-size', type=int, default=1000)
    p.add_argument('--progress-every', type=float, default=5)
    p = sub.add_parser('export')
    p.add_argument('--db', default=DEFAULT_DB)
    p.add_argument('--out', default='output/trips.csv')
    args = parser.parse_args()
    if hasattr(args, 'batch_size') and not 1 <= args.batch_size <= 100000:
        parser.error('batch-size must be between 1 and 100000')
    if hasattr(args, 'progress_every') and (
            not math.isfinite(args.progress_every) or args.progress_every <= 0):
        parser.error('progress-every must be finite and positive')
    if args.command == 'batch':
        if args.limit < 0:
            parser.error('limit must be nonnegative')
        batch(args.csv, args.db, args.limit, args.batch_size, args.progress_every)
    elif args.command == 'produce':
        if not math.isfinite(args.interval) or args.interval < 0:
            parser.error('interval must be finite and nonnegative')
        if min(args.limit, args.duplicate_every, args.bad_every) < 0:
            parser.error('limit and injection frequencies must be nonnegative')
        producer(args)
    elif args.command == 'consume':
        consumer(args)
    else:
        export_csv(args.db, args.out)

if __name__ == '__main__':
    main()
