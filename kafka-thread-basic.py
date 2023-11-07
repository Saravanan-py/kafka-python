# import threading
# from kafka import KafkaProducer
# import json
#
# def produce_data(producer, data):
#     producer.send('sample', value=data)
#
#
# # Create multiple producer threads
# num_threads = 5
# data_list = ['data1', 'data2', 'data3', 'data4', 'data5']
#
# for i in range(num_threads):
#     producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
#     data = data_list[i]
#
#     # Create a thread for each producer
#     thread = threading.Thread(target=produce_data, args=(producer, data))
#     thread.start()

from kafka import KafkaProducer
import json
import asyncio

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
kafka_topic = 'sample'


async def process_files(json_files):
    for file in json_files:
        with open(file, 'r') as a:
            data = json.load(a)
            producer.send(kafka_topic, value=data)
            print(data)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    json_files = ['data.json', 'data2.json']  # List of JSON file paths
    loop.run_until_complete(process_files(json_files))
    loop.close()
