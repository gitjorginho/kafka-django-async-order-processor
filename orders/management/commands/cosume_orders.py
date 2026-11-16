import json
import os
import time
from django.core.management.base import BaseCommand
from confluent_kafka import Consumer, KafkaException
from orders.models import Order

class Command(BaseCommand):
    help = 'Consumes order-created events from Kafka and dispatches orders.'

    def handle(self, *args, **options):
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        conf = {
            'bootstrap.servers': kafka_servers,
            'group.id': 'order-dispatch-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

        consumer = Consumer(conf)
        consumer.subscribe(['order-created'])

        self.stdout.write(self.style.SUCCESS('Dispatch Worker started. Listening to topic: order-created...'))

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode('utf-8'))
                order_id = event['order_id']

                self.stdout.write(f"Processing dispatch for Order #{order_id} ({event['product']})...")
                
                # Simula o tempo de processamento da expedição / integração logística
                time.sleep(3)

                Order.objects.filter(id=order_id).update(status='DISPATCHED')
                
                # Commit manual do offset apenas após persistência bem-sucedida
                consumer.commit(asynchronous=False)
                self.stdout.write(self.style.SUCCESS(f"Order #{order_id} successfully DISPATCHED!"))

        except KeyboardInterrupt:
            self.stdout.write('Stopping worker...')
        finally:
            consumer.close()
