import os
import csv
import json
import pika
from datetime import datetime
import time

# File path for the dataset
CSV_FILE_PATH = './data/google_play_store_dataset.csv'

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

def send_to_uploader(file_path):
    """Send the dataset file path to the uploader via RabbitMQ"""
    for _ in range(10):
        try:
            print("Producer connecting to RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600, blocked_connection_timeout=300)
            )
            channel = connection.channel()
            channel.queue_declare(queue='upload_queue', durable=False, auto_delete=False)
            channel.basic_publish(
                exchange='',
                routing_key='upload_queue',
                body=json.dumps({'action': 'producer_uploader_sendRawData', 'file_path': file_path})
            )
            print("File path sent to uploader queue.")
            connection.close()
            return
        except Exception as e:
            print(f"Failed to send file path to uploader queue: {e}")
            time.sleep(5)
    raise Exception("Could not connect to RabbitMQ after several attempts")

def send_to_processor(file_path):
    """Send the dataset file path to the processor via RabbitMQ"""
    for _ in range(10):
        try:
            print("Producer connecting to RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600, blocked_connection_timeout=300)
            )
            channel = connection.channel()
            channel.queue_declare(queue='process_queue', durable=False, auto_delete=False)
            channel.basic_publish(
                exchange='',
                routing_key='process_queue',
                body=json.dumps({'action': 'producer_processor_sendRawData', 'file_path': file_path})
            )
            print("File path sent to processor queue.")
            connection.close()
            return
        except Exception as e:
            print(f"Failed to send file path to processor queue: {e}")
            time.sleep(5)
    raise Exception("Could not connect to RabbitMQ after several attempts")

if __name__ == "__main__":
    if os.path.exists(CSV_FILE_PATH):
        print(f"Dataset found at {CSV_FILE_PATH}. Sending to uploader and processor...")
        send_to_uploader(CSV_FILE_PATH)
        send_to_processor(CSV_FILE_PATH)
    else:
        print(f"Dataset file not found at {CSV_FILE_PATH}. Please check the file path.")