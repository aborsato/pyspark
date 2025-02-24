from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

if __name__ == '__main__':
    while True:
        event = {'temperature': random.randint(20, 30), 'humidity': random.randint(60, 80)}
        print(f'Publishing message: {event}')
        producer.send('iot', value=event)
        time.sleep(1)