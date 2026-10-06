# Sources and implementation scope

## Tutorial references

- Darshil Parmar: https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project
  — initial NYC taxi ETL and analytics inspiration. Its README describes GCP storage,
  Compute Engine, Mage, BigQuery, and Looker Studio.
- Nguyen: https://github.com/trannhatnguyen2/NYC_Taxi_Data_Pipeline
  — reference for a separate streaming path and use of yellow/green taxi records.

These projects are credited as references. Their implementation files are not
bundled in this repository. This local project does not reproduce either upstream
cloud architecture or claim authorship of those tutorial designs.

## Dataset and zone definitions

- NYC TLC trip records: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Yellow dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
- Green dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
- Zone lookup: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

The bundled zone lookup is a TLC reference table. The author uploaded a small green Parquet file at the repository root; the yellow
file is not bundled. The root green file was retained during repository cleanup. `data/sample.csv` is independently generated synthetic data, not measured
NYC results. Large-dataset counts in the README are the author's reported and
screenshot-supported local run, not a dataset redistributed by this repository.

## Local contribution

The project implementation was developed with AI assistance and tested iteratively
on the author's Windows computer. It adds batch/Kafka parity, transactional replay
storage, error quarantine, batched acknowledgments, explicit payment semantics,
read-only cached analytics, and visible date-outlier handling. No cloud deployment,
Spark processing, Debezium CDC, or live taxi telemetry is claimed.
