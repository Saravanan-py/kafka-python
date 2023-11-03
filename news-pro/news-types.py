import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
us_news = "US_news"
world_news = "WORLD_news"
consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="latest"
)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))
consumer.subscribe("news")
print("Waiting for Raw news...")
while True:
    for message in consumer:
        consumed_message = message.value
        if consumed_message['category'] == 'U.S. NEWS':
            producer.send(us_news, value=consumed_message)
        if consumed_message['category'] == 'WORLD NEWS':
            producer.send(world_news, value=consumed_message)
        print("News Sended Successfully...")
