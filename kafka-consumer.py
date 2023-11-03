from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('news_us', bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), group_id="mygroup",
                         auto_offset_reset='latest')
for message in consumer:
    print(message.value)
