from confluent_kafka import Producer
import json

producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent | partition={msg.partition()} | offset={msg.offset()}")

orders = [
    {"order_id": "001", "customer": "Arjun",  "item": "Biryani", "amount": 250},
    {"order_id": "002", "customer": "Priya",  "item": "Pizza",   "amount": 400},
    {"order_id": "003", "customer": "Rahul",  "item": "Burger",  "amount": 150},
    {"order_id": "004", "customer": "Sneha",  "item": "Dosa",    "amount": 120},
    {"order_id": "005", "customer": "Karan",  "item": "Idli",    "amount": 80},
]

for order in orders:
    producer.produce(
        topic='orders',
        key=order['customer'],
        value=json.dumps(order).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)

producer.flush()
print("Done.")