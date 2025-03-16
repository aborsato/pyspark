from kafka import KafkaProducer
from avro.schema import Parse
from avro.io import DatumWriter, BinaryEncoder
import io
import time
import random

# Define the Avro schema
schema_str = """
{
  "type": "record",
  "name": "SensorData",
  "fields": [
    {"name": "temperature", "type": "int"},
    {"name": "humidity", "type": "int"}
  ]
}
"""
schema = Parse(schema_str)

def avro_serializer(event):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(event, encoder)
    return bytes_writer.getvalue()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=avro_serializer
)

if __name__ == '__main__':
    while True:
        event = {'temperature': random.randint(20, 30), 'humidity': random.randint(60, 80)}
        print(f'Publishing message: {event}')
        producer.send('avro', value=event)
        time.sleep(1)