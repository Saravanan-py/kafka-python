from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

from kafka import KafkaConsumer
from kafka import TopicPartition, OffsetAndMetadata
import kafka
import json


class MyConsumerRebalanceListener(kafka.ConsumerRebalanceListener):

    def on_partitions_revoked(self, revoked):
        print("Partitions %s revoked" % revoked)
        print('*' * 50)

    def on_partitions_assigned(self, assigned):
        print("Partitions %s assigned" % assigned)
        print('*' * 50)


def serialize_data(x):
    return json.loads(x.decode('utf-8'))



consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         value_deserializer=serialize_data,
                         group_id='demo112215sgtrjwrykvjh', auto_offset_reset='latest',
                         enable_auto_commit=False, partition_assignment_strategy=[RangePartitionAssignor])

listener = MyConsumerRebalanceListener()
consumer.subscribe('sample2', listener=listener)

for message in consumer:
    print(message)
    print("The value is : {}".format(message.value))
    tp = TopicPartition(message.topic, message.partition)
    om = OffsetAndMetadata(message.offset + 1, message.timestamp)
    consumer.commit({tp: om})
