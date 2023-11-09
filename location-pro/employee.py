from kafka.producer import KafkaProducer
from time import sleep
import json
from json import dumps
topic = 'employee'
producer = KafkaProducer(bootstrap_servers=['localhost:9092','localhost:9093','localhost:9094'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))
with open(r"meta_data.json", "r") as f:
    data = json.load(f)
for i in data:
    print(i)
    producer.send(topic, value=i)