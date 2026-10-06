import contextlib,io,tempfile,unittest
from pathlib import Path
import pipeline as p
from reconcile import compare
from test_transactions import event
class ReconcileTests(unittest.TestCase):
    def test_match_and_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            a,b=Path(tmp)/'a.db',Path(tmp)/'b.db'
            for path in (a,b):
                db=p.connect(path);p.process_many(db,[event(1),event(2,'green',None)]);db.close()
            output=io.StringIO()
            with contextlib.redirect_stdout(output): compare(a,b)
            self.assertIn('PASS:',output.getvalue())
            db=p.connect(b);db.execute('UPDATE trips SET total=100 WHERE trip_id=?',('1',));db.commit();db.close()
            with self.assertRaises(SystemExit): compare(a,b)
