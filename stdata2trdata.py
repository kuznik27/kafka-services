from kafka import KafkaConsumer, KafkaProducer
from io import BytesIO
import fastavro

topic2consume = "st_data"
topic2produce = "tr_data"

sensor_schema = fastavro.schema.load_schema('stdata.avsc')
tr_schema    = fastavro.schema.load_schema('trdata.avsc')

def deserialize(schema, data):
    bytes_reader = BytesIO(data)
    return fastavro.schemaless_reader(bytes_reader, schema)

def serialize(schema, data):
    bytes_writer = BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, data)
    return bytes_writer.getvalue()

consumer = KafkaConsumer(
    topic2consume,
    bootstrap_servers=['10.200.242.91:6063'],
    value_deserializer = lambda v: deserialize(sensor_schema, v))

producer = KafkaProducer(bootstrap_servers='10.200.242.91:6063',
                         value_serializer=lambda v: serialize(tr_schema, v),
                         api_version=(0, 10, 1))
while(True):
    try:
        for message in consumer:
            s = {}
            for i in message.value['items']:
                producer.send(topic2produce, i)
    except Exception as e:
        print("Error : ", e, " (Cannot deserialize message from topic)")
