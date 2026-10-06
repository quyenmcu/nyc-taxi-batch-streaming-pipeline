"""Optional snapshot-to-staging export with trip-ID MERGE. Requires ADC."""
import argparse
import re
import uuid
from datetime import datetime,timedelta,timezone
from pipeline import connect

def main():
    from google.cloud import bigquery
    p=argparse.ArgumentParser();p.add_argument('--db',default='output/taxi.db');p.add_argument('--dataset',required=True,help='project.dataset');a=p.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9-]*\.[A-Za-z_][A-Za-z0-9_]*',a.dataset):
        p.error('expected project.dataset')
    client=bigquery.Client(project=a.dataset.split('.')[0])
    target=a.dataset+'.trips';stage=a.dataset+'.stage_'+uuid.uuid4().hex
    schema=[bigquery.SchemaField(n,t) for n,t in [('trip_id','STRING'),('pickup','DATETIME'),('dropoff','DATETIME'),('pickup_area','STRING'),('distance','FLOAT'),('total','FLOAT'),('payment_type','INTEGER')]]
    client.create_table(bigquery.Table(target,schema=schema),exists_ok=True)
    table=bigquery.Table(stage,schema=schema);table.expires=datetime.now(timezone.utc)+timedelta(hours=1)
    client.create_table(table)
    try:
        # Chunked staging upload avoids retaining the entire dataset in memory.
        with connect(a.db) as db:
            cur=db.execute('SELECT trip_id,pickup,dropoff,pickup_area,distance,total,payment_type FROM trips')
            names=[x[0] for x in cur.description]
            while rows:=cur.fetchmany(10000):
                client.load_table_from_json([dict(zip(names,row)) for row in rows],stage,
                    job_config=bigquery.LoadJobConfig(schema=schema,write_disposition='WRITE_APPEND')).result()
        client.query(f'''MERGE `{target}` T USING `{stage}` S ON T.trip_id=S.trip_id
          WHEN NOT MATCHED THEN INSERT (trip_id,pickup,dropoff,pickup_area,distance,total,payment_type)
          VALUES (S.trip_id,S.pickup,S.dropoff,S.pickup_area,S.distance,S.total,S.payment_type)''').result()
        print('Merged into '+target)
    finally:
        client.delete_table(stage,not_found_ok=True)

if __name__=='__main__':main()
