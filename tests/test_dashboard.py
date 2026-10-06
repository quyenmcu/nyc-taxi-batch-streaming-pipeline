import sqlite3,tempfile,time,unittest
from pathlib import Path
import dashboard as d
import pipeline as p
from test_transactions import event

class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'taxi.db';db=p.connect(self.path)
        p.process_many(db,[event(1),event(2,'green',None),event(3,day='2008-12-31'),event(4,day='2026-09-01')]);db.close()
    def tearDown(self): self.tmp.cleanup()
    def test_month_boundaries_payments_and_readonly(self):
        before=self.path.read_bytes();result=d.build_snapshot(self.path)
        self.assertEqual(result['all']['trips'],4);self.assertEqual(result['all']['total'],50)
        month=result['month']['all']
        self.assertEqual(month['trips'],2);self.assertEqual(month['total'],25)
        self.assertEqual(month['outside_month'],2);self.assertEqual(month['first'],'2026-08-01 12:00:00')
        self.assertEqual(month['payments'],[['Flex Fare',1],['Missing payment type',1]])
        self.assertEqual(result['month']['green']['trips'],1)
        self.assertEqual(before,self.path.read_bytes())
    def test_snapshot_matches_sql(self):
        result=d.build_snapshot(self.path);db=d.open_readonly(self.path)
        try:
            self.assertEqual(result['all']['windows'],db.execute(f'SELECT {d.WINDOW_SQL} AS win,count(*) FROM trips GROUP BY win ORDER BY win').fetchall())
            self.assertEqual(result['all']['types'],{**{'yellow':0,'green':0,'unknown':0}, **dict(db.execute(f'SELECT {d.TAXI_TYPE_SQL} AS kind,count(*) FROM trips GROUP BY kind'))})
        finally: db.close()
    def test_background_update(self):
        cache=d.SnapshotCache(self.path,check_every=.02);cache.start()
        try:
            deadline=time.monotonic()+5
            while cache.get('all')[0] is None and time.monotonic()<deadline: time.sleep(.01)
            self.assertEqual(cache.get('all')[0]['trips'],4)
            db=p.connect(self.path);p.process(db,event(5));db.close()
            deadline=time.monotonic()+5
            while cache.get('all')[0]['trips']!=5 and time.monotonic()<deadline: time.sleep(.01)
            self.assertEqual(cache.get('all')[0]['trips'],5)
            self.assertEqual(cache.get('all','month')[0]['trips'],3)
            with self.assertRaises(ValueError): cache.get('bad')
            with self.assertRaises(ValueError): cache.get('all','bad')
        finally: cache.close()
    def test_missing_and_empty(self):
        missing=Path(self.tmp.name)/'missing.db'
        with self.assertRaises(sqlite3.OperationalError): d.build_snapshot(missing)
        self.assertFalse(missing.exists())
        empty=Path(self.tmp.name)/'empty.db';p.connect(empty).close()
        self.assertEqual(d.build_snapshot(empty)['all']['trips'],0)
