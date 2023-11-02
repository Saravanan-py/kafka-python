from kafka.producer import KafkaProducer
from time import sleep
from json import dumps


# producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer= lambda x: dumps(x).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))
topic = 'mytopic6'

for i in range(1000):
    message = {"number":i}
    producer.send(topic, value=message)
    print(message)
    sleep(3)
# Close the producer
producer.flush()
producer.close()
#
# # Synchronous send
# from time import sleep
# from json import dumps
# from kafka import KafkaProducer
#
# topic_name='mytopic2'
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
#
#
# for e in range(100):
#     data = {'number' : e}
#     print(data)
#     try:
#         record_metadata =producer.send(topic_name, value=data).get(timeout=10)
#         print(record_metadata.topic)
#         print(record_metadata.partition)
#         print(record_metadata.offset)
#         sleep(0.5)
#     except Exception as e:
#         print(e)
#
# producer.flush()
# producer.close()
#
# # Asynchronous send
# from time import sleep
# from json import dumps
# from kafka import KafkaProducer
#
# topic_name='mytopic2'
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
#
#
# def on_send_success(record_metadata,message):
#     print("""Successfully produced "{}" to topic {} and partition {} at offset {}""".format(message,record_metadata.topic,record_metadata.partition,record_metadata.offset))
#     print()
#
#
# def on_send_error(excp,message):
#     print('Failed to write the message "{}" , error : {}'.format(message,excp))
#
# for e in range(100):
#     data = {'number' : e}
#     record_metadata =producer.send(topic_name, value=data).add_callback(on_send_success,message=data).add_errback(on_send_error,message=data)
#     print("Sent the message {} using send method".format(data))
#     print()
#     sleep(0.5)
#
#
# producer.flush()
# producer.close()


#Consumer_Group
# from kafka.producer import KafkaProducer
# from time import sleep
# from json import dumps
#
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer= lambda x: dumps(x).encode('utf-8'))
# # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
# #                          value_serializer=lambda x: dumps(x).encode('utf-8'))
# topic = 'mytopic4'
#
# while True:
#     message = input("Enter the message you want to send: ")
#     partition_no = int(input("Enter the partition No: "))
#     producer.send(topic, value=message, partition=partition_no)
#
