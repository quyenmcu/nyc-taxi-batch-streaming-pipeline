# Full-dataset NYC taxi dashboard

Save the supplied Python file as `dashboard.py` in your existing project folder,
replacing the earlier dashboard file. Keep your current pipeline and databases.
No additional Python packages are required: the dashboard uses the standard library.

## Start from PowerShell

```powershell
python dashboard.py --db output/stream-full-202608.db --port 8080
```

Open **http://localhost:8080** in your browser. If a dashboard is already running
on port 8080, stop that terminal with Ctrl+C first, or use `--port 8081` and open
http://localhost:8081.

The default database is now `output/stream-full-202608.db`. The first scan happens
in the background: the page will show `Preparing snapshot` until it finishes.
The terminal reports scan progress every 250,000 trips. Subsequent page refreshes
and taxi filters reuse the completed snapshot.

## Expected results

The default view now selects August 2026 only: **3,377,371 accepted trips**.
A notice identifies **29 trips outside August**. They remain in the database.
Select **All source dates** to see the full reconciled totals below.

For your verified full replay (all source dates):

| Selection | Accepted trips |
| --- | ---: |
| All | 3,377,400 |
| Yellow | 3,336,713 |
| Green | 40,687 |

The whole database has three rejected events with reason `dropoff precedes pickup`.
Duplicate deliveries may be zero before a deliberate replay test.

## Views

- All/yellow/green/unknown taxi filters.
- August-only and all-source-date views, with an explicit outlier count.
- Unique trips and recorded total for the selection.
- Daily trips (latest 90 populated dates) or latest 48 populated five-minute windows.
- Ten pickup areas with the most trips, including recorded totals.
- Payment counts, with missing values separate from Flex Fare code 0.
- Whole-database unique rejections, reasons, and duplicate delivery counter.
- Snapshot timestamp and refresh state.

Dates come from the source's NYC local pickup timestamps. The dashboard does not
silently discard source date outliers. They are excluded only when the month
view is selected and remain available under All source dates. Daily charts use equal spacing for populated
dates; five-minute charts use equal spacing for populated windows.
Recorded total is the sum of the source `total_amount`, including any negative
values accepted by the pipeline; it is not a profit calculation.

## Refresh behavior

The page checks the cached results every five seconds. A background monitor checks
for source database changes every 60 seconds and rebuilds only when it detects a
change. Click **Refresh data** to request an immediate new scan. During a rebuild,
the previous completed snapshot remains visible. Its timestamp identifies the data
being shown. A new snapshot replaces all taxi-filter results together.

All source database access uses SQLite read-only connections. The dashboard does
not add indexes, tables, or columns, or modify trip data. Snapshots live in memory
and are rebuilt when the dashboard restarts. Both date scopes are aggregated in the same scan.
For another source month, use `--month YYYY-MM`.

## Zone names

Keep your existing `taxi_zone_lookup.csv` beside `dashboard.py` for TLC zone names.
You can also specify another path:

```powershell
python dashboard.py --db output/stream-full-202608.db --zones data/taxi_zone_lookup.csv
```

If no lookup file is present, the dashboard shows zone IDs and remains usable.

## Validation

Four automated tests passed: equivalence to the previous SQL dashboard for all
filters; read-only source access; background update after a source change; missing
and empty database handling. Python compilation and JavaScript syntax checks passed.
Local HTTP tests verified page loading, cached filters, manual refresh, and invalid
filter errors. A synthetic 100,000-row snapshot built in 0.36 seconds, with cached
HTTP responses averaging 0.91 milliseconds in the test environment. This is not a
benchmark of your 3.38-million-row database; check its initial scan time locally.
