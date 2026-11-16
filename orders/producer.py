import json
import os
from confluent_kafka import Producer

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
producer = Producer({'bootstrap.servers': KAFKA_SERVERS})

def publish_order_created(order):
    payload = {
        'order_id': order.id,
        'customer': order.customer,
        'product': order.product,
        'quantity': order.quantity,
    }

    producer.produce(
        topic='order-created',
        key=str(order.id).encode('utf-8'),
        value=json.dumps(payload).encode('utf-8'),
    )
    producer.flush()
