# Reference sources reviewed on 2026-10-01

Darshil Parmar — Uber ETL pipeline:
https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project

Reviewed source blobs:
- mage-files/extract.py: 85a30f52129e2feaff99bf99e1eb5c0ed7e1aea7
- mage-files/transform.py: fdfda4c8bdc50a206fefe723264d0b0932cefb29
- mage-files/load.py: fc5aa74fcaed3b694375c3917cfd273925ccd840

Nguyen — NYC taxi batch and streaming reference:
https://github.com/trannhatnguyen2/NYC_Taxi_Data_Pipeline

Inspiration: historical data replay and a distinct streaming path. This milestone does not copy its Debezium/Spark/MinIO stack.

The referenced Darshil root directory did not include a LICENSE file in the listing reviewed. Upstream files are not redistributed in this archive; links and source hashes document provenance. Implementation files in this package were independently written. The author later uploaded green_tripdata_2026-08.parquet to this repository; the yellow input is not bundled. The 120-row sample is synthetic.



## Current verified scope (6 October 2026)

The local yellow/green Parquet run now includes batched Kafka replay, nullable payment
values, Flex Fare code 0, cached date/type filters, and full reconciliation.
See [current provenance](docs/provenance.md) and [measured results](docs/validation.md).
Optional BigQuery and Mage adapters remain separate from the verified local path.
