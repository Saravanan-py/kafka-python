from kafka import KafkaConsumer
import json
consumer = KafkaConsumer ('mytopic6',bootstrap_servers = ['localhost:9092'],
                          value_deserializer=lambda m: json.loads(m.decode('utf-8')),group_id='demo112215sgtrjwrykvjh',auto_offset_reset='earliest')
# consumer = KafkaConsumer ('mytopic5',bootstrap_servers = ['localhost:9092'],
#                           value_deserializer=lambda m: json.loads(m.decode('utf-8')),group_id='demo123',auto_offset_reset='earliest')
for message in consumer:
    print(message.offset)
    print(message.value)
    print(message.partition)

