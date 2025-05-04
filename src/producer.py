import os
import csv
import json
import pika
from datetime import datetime

# File path for the dataset
CSV_FILE_PATH = './data/google_play_store_dataset.csv'

def send_to_uploader(file_path):
    """Send the dataset file path to the uploader via RabbitMQ"""
    try:
        print("Producer connecting to RabbitMQ...")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', heartbeat=600, blocked_connection_timeout=300)
        )
        channel = connection.channel()

        # Declare queue for uploader
        channel.queue_declare(queue='upload_queue', durable=False, auto_delete=False)

        # Send file path to uploader
        channel.basic_publish(
            exchange='',
            routing_key='upload_queue',
            body=json.dumps({'action': 'producer_uploader_sendRawData', 'file_path': file_path})
        )
        print("File path sent to uploader queue.")

        connection.close()
    except Exception as e:
        print(f"Failed to send file path to uploader queue: {e}")
        raise

def send_to_processor(file_path):
    """Send the dataset file path to the processor via RabbitMQ"""
    try:
        print("Producer connecting to RabbitMQ...")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', heartbeat=600, blocked_connection_timeout=300)
        )
        channel = connection.channel()

        # Declare queue for processor
        channel.queue_declare(queue='process_queue', durable=False, auto_delete=False)

        # Send file path to processor
        channel.basic_publish(
            exchange='',
            routing_key='process_queue',
            body=json.dumps({'action': 'producer_processor_sendRawData', 'file_path': file_path})
        )
        print("File path sent to processor queue.")

        connection.close()
    except Exception as e:
        print(f"Failed to send file path to processor queue: {e}")
        raise

if __name__ == "__main__":
    if os.path.exists(CSV_FILE_PATH):
        print(f"Dataset found at {CSV_FILE_PATH}. Sending to uploader and processor...")
        send_to_uploader(CSV_FILE_PATH)
        send_to_processor(CSV_FILE_PATH)
    else:
        print(f"Dataset file not found at {CSV_FILE_PATH}. Please check the file path.")