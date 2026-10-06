import argparse
import sqlite3
from contextlib import closing
from pathlib import Path

def compare(left,right):
    with closing(sqlite3.connect(Path(left).resolve().as_uri()+'?mode=ro',uri=True)) as a, closing(sqlite3.connect(Path(right).resolve().as_uri()+'?mode=ro',uri=True)) as b:
        a.execute('BEGIN')
        b.execute('BEGIN')
        trips='SELECT trip_id,pickup,dropoff,pickup_area,distance,total,payment_type FROM trips ORDER BY trip_id'
        windows='SELECT * FROM five_minute_summary ORDER BY window_start,pickup_area'
        for label,query in [('trips',trips),('windows',windows)]:
            ca,cb=a.execute(query),b.execute(query)
            while True:
                x,y=ca.fetchmany(1000),cb.fetchmany(1000)
                if x!=y:raise SystemExit(label+' differ')
                if not x:break
    print('PASS: trip records and five-minute summaries match')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--batch',required=True);p.add_argument('--stream',required=True);a=p.parse_args();compare(a.batch,a.stream)
