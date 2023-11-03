import json
from kafka import KafkaConsumer
us_news = "US_news"
consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset = "latest"
)
consumer.subscribe(us_news)
print("Waiting for Raw news...")
while True:
    if consumer:
        print("Your Today's News About US")
        print("*"*50)
        for message in consumer:
            consumed_message = message.value['headline']
            print(consumed_message)


