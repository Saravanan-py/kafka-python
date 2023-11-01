from kafka import KafkaConsumer
import json
consumer = KafkaConsumer ('mytopic5',bootstrap_servers = ['localhost:9092'],
                          value_deserializer=lambda m: json.loads(m.decode('utf-8')),group_id='demo12',auto_offset_reset='latest')
# consumer = KafkaConsumer ('mytopic5',bootstrap_servers = ['localhost:9092'],
#                           value_deserializer=lambda m: json.loads(m.decode('utf-8')),group_id='demo123',auto_offset_reset='earliest')
for message in consumer:
    print(message.offset)
    print(message.value)