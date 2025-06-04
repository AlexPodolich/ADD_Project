import pika
import os
import argparse
from backend.dictionary import QueueName
from backend.dictionary import Exchange

def parse_args():
    parser = argparse.ArgumentParser(description="Setup RabbitMQ queues for ADD project.")
    parser.add_argument('--host', type=str, default=os.getenv('RABBITMQ_HOST', 'localhost'), help='RabbitMQ host (default: localhost or RABBITMQ_HOST env)')
    return parser.parse_args()

QUEUES = [
    {'name': QueueName.PROCESS.value, 'durable': False, 'auto_delete': False},
    {'name': QueueName.AI_MODEL.value, 'durable': False, 'auto_delete': False},
    {'name': QueueName.UPLOAD.value, 'durable': False, 'auto_delete': False},
]

def setup_queues(host):
    print(f"Connecting to RabbitMQ at {host}...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()

    channel.exchange_declare(exchange=Exchange.ADD_DIRECT.value, exchange_type='direct', durable=True)
    for q in QUEUES:
        print(f"Declaring queue: {q['name']}")
        channel.queue_declare(queue=q['name'], durable=q['durable'], auto_delete=q['auto_delete'])
        channel.queue_bind(exchange=Exchange.ADD_DIRECT.value, queue=q['name'], routing_key=q['name'])
    connection.close()
    print("All queues and exchange declared successfully.")

if __name__ == "__main__":
    args = parse_args()
    setup_queues(args.host)