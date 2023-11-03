import json
import random
import time
from kafka import KafkaProducer
order_limit = 20
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))
print("Welcome to Vivya Corp Sports")
print("Will generate your order in 10 seconds")
time.sleep(10)
products = ["SG Bat", "TNCA Ball", "DSC Gloves", "SG Pads", "SHREY Helmet", "Iphone", "Samsung"]
prices = [340, 500, 1000, 2000, 2300]
for i in range(order_limit):
    data = {
        "order_id": "ORD" + str(i),
        "user_id": f"USER_{i}",
        "product_name": random.choice(products),
        "total_cost": random.choice(prices)
    }

    producer.send("order_details", value=data)
    print(f"Order Details has been sended successfully")
    time.sleep(0.5)
