import json
from kafka import KafkaConsumer

topic = 'employee'
consumer = KafkaConsumer(
    bootstrap_servers=["localhost:9092", 'localhost:9093', 'localhost:9094'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="earliest"
)
consumer.subscribe(topic)
print("Waiting for location details.....")
while True:
    if consumer:
        print("")
        print("*" * 50)
        for message in consumer:
            if message.value['employee_id']==102:
                emp = message.value['employee_id']
                longitude = message.value['longitude']
                latitude = message.value['latitude']
                print({'Employee_ID':emp, "Longitude":longitude, "Latitude":latitude})
