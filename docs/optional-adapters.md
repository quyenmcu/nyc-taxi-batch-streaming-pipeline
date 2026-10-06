# Optional adapters

These files are retained from the author's upload. They were not part of the
verified 3.38-million-record local Kafka/SQLite run.

## BigQuery snapshot export

Install the separate dependency set and use your own GCP credentials:

```powershell
python -m pip install -r requirements-cloud.txt
gcloud auth application-default login
python bigquery_export.py --db output/stream-full-202608.db --dataset YOUR_PROJECT.YOUR_DATASET
```

Create the dataset first. The exporter reads a snapshot, uploads it in chunks to
an expiring staging table, and MERGEs missing trip IDs into the target table. It
does not update or delete existing target records and is not continuous Kafka
streaming into BigQuery. Serialize exports to one target; concurrent MERGEs are
outside this demo's guarantees. Cloud usage may incur charges.

## Mage custom block

Install Mage in a supported environment, make the repository's `pipeline.py`
importable, and use `rebuild_batch.py` as a custom block. Set `TAXI_CSV` to an
absolute CSV or Parquet path and `TAXI_DB` to the destination SQLite path. Scheduling
the cloud exporter is a separate step. Mage project metadata and a Mage Docker
service are not included.
