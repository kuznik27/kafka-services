from kafka import KafkaConsumer
from io import BytesIO
import fastavro
import psycopg2

import time

topic2consume = "tr_data"
tr_schema    = fastavro.schema.load_schema('trdata.avsc')

consumer = KafkaConsumer(
	topic2consume,
	#bootstrap_servers=['localhost:9092'],
	bootstrap_servers=['10.200.242.91:6063'],
    group_id='trdata2psql',
	value_deserializer=lambda v: deserialize(tr_schema, v)
	)
	
def deserialize(schema, data):
    bytes_reader = BytesIO(data)
    return fastavro.schemaless_reader(bytes_reader, schema)

conn_params = "dbname=block_st user=nyuli password=Nf67Thr() host=10.200.242.91"

for message in consumer:
    with psycopg2.connect(conn_params) as conn:
        for i in message.value['items']:
            sql = "INSERT INTO tr_tab(tr_timestamp, tr_id, tr_value) VALUES('" + str(i['timestamp']) + "','" + str(i['id']) + "','" + str(i['value']) + "');"
            try:
                with conn.cursor() as cur:
                    #print(str(i['timestamp']) + ", " + str(i['elem_id']) + ", " + str(i['value']))                
                    cur.execute(sql)
            except Exception as e:
                #print(str(e))
                print(str(time.ctime(int(time.time()))), "Problem in insert: ", sql)
                conn.rollback()