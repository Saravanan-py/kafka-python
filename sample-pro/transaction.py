import json

from kafka import KafkaConsumer
from kafka import KafkaProducer
consumer = KafkaConsumer(
    'order_details',
    bootstrap_servers="localhost:9092"
)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))
print("Listening Customer Order...")
while True:
    for message in consumer:
        print("Ongoing transaction..")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)
        user_id = consumed_message["user_id"]
        total_cost = consumed_message["total_cost"]
        product_name = consumed_message['product_name']
        available_products = ["SG Bat", "TNCA Ball", "DSC Gloves", "SG Pads", "SHREY Helmet"]
        if product_name in available_products:
            data = {
                "customer_id": user_id,
                "customer_email": f"{user_id}@gmail.com",
                "total_cost": total_cost,
                "product_name": product_name,
                "status":"success",
                "reason":"product is available"
            }
            print("Successful transaction..")
            producer.send('order_confirmed', value = data)
        else:
            data = {
                "status":"failure",
                "reason":"product is unavailable",
                "product_name":None,
                "total_cost":0,
                "customer_email":None
            }
            print("Unsuccessful transaction..")
            producer.send('order_confirmed', value = data)