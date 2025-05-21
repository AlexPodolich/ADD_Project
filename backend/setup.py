
import pika
import os
from .dictionary import QueueName

# RabbitMQ connection parameters
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

# List of required queues and their parameters
QUEUES = [
    {'name': QueueName.PROCESS.value, 'durable': False, 'auto_delete': False},
    {'name': QueueName.AI_MODEL.value, 'durable': False, 'auto_delete': False},
    {'name': QueueName.UPLOAD.value, 'durable': False, 'auto_delete': False},
]

def setup_queues():
    print(f"Connecting to RabbitMQ at {RABBITMQ_HOST}...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    for q in QUEUES:
        print(f"Declaring queue: {q['name']}")
        channel.queue_declare(queue=q['name'], durable=q['durable'], auto_delete=q['auto_delete'])
    connection.close()
    print("All queues declared successfully.")

if __name__ == "__main__":
    setup_queues()