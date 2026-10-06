import json, sqlite3, sys, tempfile, types, unittest
from pathlib import Path
from unittest.mock import patch
import pipeline as p


def event(identity, kind='yellow', payment=0, day='2026-08-01'):
    prefix = 'lpep' if kind == 'green' else 'tpep'
    return {'trip_id': str(identity), 'trip': {
        prefix+'_pickup_datetime': day+' 12:00:00',
        prefix+'_dropoff_datetime': day+' 12:15:00',
        'trip_distance':1.5,'total_amount':12.5,'payment_type':payment,'PULocationID':1}}

class Message:
    def __init__(self, payload, partition=0, offset=0, error=None):
        self.payload,self.part,self.pos,self.err=payload,partition,offset,error
    def value(self): return self.payload
    def topic(self): return 'taxi.test'
    def partition(self): return self.part
    def offset(self): return self.pos
    def error(self): return self.err

class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'test.db';self.db=p.connect(self.path)
    def tearDown(self): self.db.close();self.tmp.cleanup()
    def test_yellow_green_missing_payment_and_duplicates(self):
        rows=[event(1),event(2,'green',None),event(3,payment=7),event(1)]
        self.assertEqual(dict(p.process_many(self.db,rows)),{'inserted':2,'rejected':1,'duplicate':1})
        before=self.db.execute('SELECT * FROM five_minute_summary').fetchall()
        p.process_many(self.db,rows)
        self.assertEqual(before,self.db.execute('SELECT * FROM five_minute_summary').fetchall())
        self.assertEqual(dict(self.db.execute('SELECT * FROM metrics')),{'inserted_deliveries':2,'rejected_deliveries':2,'duplicate_deliveries':4})
    def test_database_error_rolls_back_whole_batch(self):
        self.db.execute("CREATE TRIGGER fail_trip BEFORE INSERT ON trips WHEN NEW.trip_id='2' BEGIN SELECT RAISE(ABORT,'test failure'); END")
        with self.assertRaises(sqlite3.IntegrityError): p.process_many(self.db,[event(1),event(2)])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM trips').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM metrics').fetchone()[0],0)
    def test_malformed_messages_and_partition_offsets(self):
        msgs=[Message(json.dumps(event(1)).encode(),0,3),Message(b'{bad',1,8),Message(None,1,9),Message(b'\xff',0,4),Message(b'[]',0,5)]
        counts,offsets=p.process_messages(self.db,msgs)
        self.assertEqual(dict(counts),{'inserted':1,'rejected':4})
        self.assertEqual(offsets,{('taxi.test',0):6,('taxi.test',1):10})
        counts,_=p.process_messages(self.db,msgs)
        self.assertEqual(dict(counts),{'duplicate':1,'rejected':4})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM rejected').fetchone()[0],4)
    def test_message_error_rolls_back(self):
        with self.assertRaises(RuntimeError): p.process_messages(self.db,[Message(json.dumps(event(1)).encode()),Message(b'',error='broker error')])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM trips').fetchone()[0],0)
    def test_commit_failure_after_persistence_and_safe_replay(self):
        observations=[];path=self.path
        class Partition:
            def __init__(self,topic,partition,offset): self.topic,self.partition,self.offset,self.error=topic,partition,offset,None
        class Client:
            def __init__(self,config): self.config=config
            def subscribe(self,topics): pass
            def consume(self,**kwargs): return [Message(json.dumps(event(1)).encode(),0,10)]
            def commit(self,offsets,asynchronous):
                with sqlite3.connect(path) as reader: observations.append(reader.execute('SELECT COUNT(*) FROM trips').fetchone()[0])
                observations.append(offsets[0].offset)
                raise RuntimeError('simulated commit failure')
            def close(self): observations.append('closed')
        args=types.SimpleNamespace(broker='local',group='test',topic='taxi.test',db=str(path),progress_every=5,batch_size=1000)
        with patch.dict(sys.modules,{'confluent_kafka':types.SimpleNamespace(Consumer=Client,TopicPartition=Partition)}):
            with self.assertRaisesRegex(RuntimeError,'commit failure'): p.consumer(args)
        self.assertEqual(observations,[1,11,'closed'])
        counts,_=p.process_messages(self.db,[Message(json.dumps(event(1)).encode(),0,10)])
        self.assertEqual(dict(counts),{'duplicate':1})
    def test_partial_final_batch_and_limit(self):
        out=Path(self.tmp.name)/'limited.db'
        self.assertEqual(p.batch('data/sample.csv',out,limit=5,batch_size=3),{'inserted':5})
        self.assertEqual(p.batch('data/sample.csv',out,limit=7,batch_size=3),{'duplicate':5,'inserted':2})
