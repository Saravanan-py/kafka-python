import json

from kafka import KafkaConsumer
consumer = KafkaConsumer(
    "order_confirmed",
    bootstrap_servers="localhost:9092",value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
emails_sent_so_far = set()
print("Listening Retailers confirmation...")
while True:
    for message in consumer:
        consumed_message = message.value
        customer_email = consumed_message["customer_email"]
        if customer_email:
            print(f"Sending email to {customer_email} ")
            emails_sent_so_far.add(customer_email)
            print(f"So far emails sent to {len(emails_sent_so_far)} unique emails")
        else:
            print(f"Order Unavailable")
