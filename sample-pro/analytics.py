import json
from kafka import KafkaConsumer
consumer = KafkaConsumer(
    "order_confirmed",
    bootstrap_servers="localhost:9092"
)

total_orders_count = 0
total_revenue = 0
print("Listening Confirmation of Retailers...")
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message["total_cost"])
        total_orders_count += 1
        total_revenue += total_cost
        print(f"Orders so far today: {total_orders_count}")
        print(f"Revenue so far today: {total_revenue}")
        product_name = consumed_message['product_name']
        if product_name:
            print(f"Product Names are: {product_name}")
        else:
            print("Product Unavailable")
