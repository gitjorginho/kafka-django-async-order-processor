import json
import os
import time
from django.core.management.base import BaseCommand
from confluent_kafka import Consumer, KafkaError, KafkaException
from orders.models import Order

class Command(BaseCommand):
    help = 'Consumes order-created events from Kafka and persists dispatched orders.'

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
                    if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        time.sleep(2)
                        continue
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode('utf-8'))
                order_uuid = event['uuid']

                self.stdout.write(f"Processing dispatch for Order UUID {order_uuid} ({event['product']})...")
                
                # Simula o tempo de processamento da expedição
                time.sleep(3)

                # Idempotência: cria no banco apenas se esse UUID ainda não existir
                order, created = Order.objects.get_or_create(
                    uuid=order_uuid,
                    defaults={
                        'customer': event['customer'],
                        'product': event['product'],
                        'quantity': event['quantity'],
                        'status': 'DISPATCHED',
                    }
                )
                
                consumer.commit(asynchronous=False)
                
                if created:
                    self.stdout.write(self.style.SUCCESS(f"Order #{order.id} ({order_uuid}) persisted and DISPATCHED!"))
                else:
                    self.stdout.write(self.style.WARNING(f"Duplicate event ignored for Order UUID {order_uuid}."))

        except KeyboardInterrupt:
            self.stdout.write('Stopping worker...')
        finally:
            consumer.close()
