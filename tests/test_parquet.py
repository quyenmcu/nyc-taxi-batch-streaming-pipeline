import json,tempfile,unittest
from datetime import datetime
from pathlib import Path
import pipeline as p
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError: pa=pq=None
@unittest.skipIf(pa is None,'Install requirements.txt for Parquet tests')
class ParquetTests(unittest.TestCase):
    def test_timestamp_serialization_nulls_and_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'green.parquet'
            table=pa.table({'lpep_pickup_datetime':[datetime(2026,8,1,12)]*2,'lpep_dropoff_datetime':[datetime(2026,8,1,13)]*2,'trip_distance':[1.0,2.0],'total_amount':[10.0,20.0],'payment_type':[None,0],'PULocationID':[1,2],'optional_field':[float('nan'),1.0]})
            pq.write_table(table,path);rows=list(p.events(path))
            self.assertEqual(rows[0]['trip']['lpep_pickup_datetime'],'2026-08-01T12:00:00')
            self.assertIsNone(rows[0]['trip']['optional_field']);json.dumps(rows,allow_nan=False)
            copy=Path(tmp)/'copy.parquet';copy.write_bytes(path.read_bytes())
            self.assertEqual(rows,list(p.events(copy)))
            self.assertEqual(p.batch(path,Path(tmp)/'batch.db'),{'inserted':2})
