from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-processor-v3',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

consumer.subscribe(['orders'])
print("Waiting for orders... (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        key = msg.key().decode('utf-8') if msg.key() else None
        order = json.loads(msg.value().decode('utf-8'))

        print(f"Partition: {msg.partition()} | "
              f"Offset: {msg.offset()} | "
              f"Key: {key} | "
              f"Order: {order['order_id']} - {order['customer']}")

        consumer.commit(message=msg)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Consumer closed cleanly.")