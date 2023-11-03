from kafka.producer import KafkaProducer
from time import sleep
import json
from json import dumps
topic = 'news'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))
with open(r"data.json", "r") as f:
    data = json.load(f)
for i in data:
    print(i)
    producer.send(topic, value=i)