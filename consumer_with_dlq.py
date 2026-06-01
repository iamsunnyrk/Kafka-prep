from confluent_kafka import Consumer, Producer
import json
import random

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-processor-dlq',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

dlq_producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

consumer.subscribe(['orders'])

def process_order(order):
    # simulate 30% failure rate
    if random.random() < 0.3:
        raise Exception(f"Processing failed for order {order['order_id']}")
    print(f"Processed order {order['order_id']} ✓")

def send_to_dlq(message, error):
    dlq_payload = {
        'original_message': json.loads(message.value().decode('utf-8')),
        'error': str(error),
        'partition': message.partition(),
        'offset': message.offset()
    }
    dlq_producer.produce(
        topic='orders-dlq',
        value=json.dumps(dlq_payload).encode('utf-8')
    )
    dlq_producer.flush()
    print(f"Sent to DLQ: {dlq_payload['original_message']['order_id']} "
          f"| Error: {error}")

MAX_RETRIES = 3

print("Consumer with DLQ running... (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        # if msg.error():
        #     print(f"Consumer error: {msg.error()}")
        #     continue
        
        order = json.loads(msg.value().decode('utf-8'))
        
        #retry logic
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                process_order(order)
                success = True
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    import time
                    time.sleep(0.5 * (attempt + 1))  # backoff
        
        if not success:
            send_to_dlq(msg, f"Failed after {MAX_RETRIES} attempts")
            
        consumer.commit(message=msg)
    
except KeyboardInterrupt:
    print("\nStopping...")
finally:
    consumer.close()