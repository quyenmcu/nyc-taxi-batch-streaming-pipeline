import copy
import tempfile
import unittest
from pathlib import Path
from pipeline import batch, connect, events, process
from dashboard import stats

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=str(Path(self.tmp.name)/'taxi.db')
        self.events=list(events('data/sample.csv'))
    def tearDown(self):self.tmp.cleanup()
    def test_batch_rerun_and_restart(self):
        self.assertEqual(batch('data/sample.csv',self.db),{'inserted':120})
        self.assertEqual(batch('data/sample.csv',self.db),{'duplicate':120})
        self.assertEqual(stats(self.db)['trips'],120)
    def test_invalid_and_malformed_are_quarantined(self):
        with connect(self.db) as db:
            bad=copy.deepcopy(self.events[0]);bad['trip']['trip_distance']='NaN'
            self.assertEqual(process(db,bad),'rejected')
            self.assertEqual(process(db,{'oops':1}),'rejected')
            self.assertEqual(process(db,[]),'rejected')
        self.assertEqual(stats(self.db)['rejected'],3)
    def test_out_of_order_and_duplicate_totals(self):
        with connect(self.db) as db:
            for e in reversed(self.events):process(db,e)
            for e in self.events:process(db,e)
            total=db.execute('SELECT sum(trips),sum(recorded_total) FROM five_minute_summary').fetchone()
        self.assertEqual(total[0],120)
        self.assertAlmostEqual(total[1],sum(float(e['trip']['total_amount']) for e in self.events))
    def test_identity_is_independent_of_file_path(self):
        other=Path(self.tmp.name)/'copy.csv';other.write_bytes(Path('data/sample.csv').read_bytes())
        self.assertEqual(self.events,list(events(other)))
    def test_coordinates_and_zone_ids(self):
        with connect(self.db) as db:
            event=copy.deepcopy(self.events[0]);event['trip']['PULocationID']='161'
            self.assertEqual(process(db,event),'inserted')
            self.assertEqual(db.execute('SELECT pickup_area FROM trips').fetchone()[0],'zone:161')

if __name__=='__main__':unittest.main()
