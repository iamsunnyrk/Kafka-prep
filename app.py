from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time, random


# Define metrics
orders_processed = Counter(
    'orders_processed_total',
    'Total number of orders processed',
    ['status'] # label - success or failure
)

orders_in_queue = Gauge(
    'orders_in_queue',
    'Current number of orders waiting in queue'
)

processing_time = Histogram(
    'order_processing_seconds',
    'Time spent processing an order',
    buckets=[0.1, 0.5, 1.0, 2.0, 3.0]
)

def process_order():
    orders_in_queue.set(random.randint(0,100))
    
    start = time.time()
    time.sleep(random.uniform(0.1, 1.0))
    duration = time.time() - start
    
    processing_time.observe(duration)
    
    if random.random() > 0.2:
        orders_processed.labels(status='success').inc()
    else:
        orders_processed.labels(status='failure').inc()
    
if __name__ == '__main__':
    start_http_server(8000)
    print("Metrics available at http://localhost:8000/metrics")
    
    while True:
        process_order()
        time.sleep(0.5)
        