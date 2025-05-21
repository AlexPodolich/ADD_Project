

import os
import json
import pika
from backend.dictionary import QueueName, Action, DataColumn


# File path for the dataset
from backend.dictionary import FilePath
CSV_FILE_PATH = FilePath.DATASET.value

def send_to_uploader(file_path):
    """Send the dataset file path to the uploader via RabbitMQ"""
    try:
        print("Producer connecting to RabbitMQ...")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', heartbeat=600, blocked_connection_timeout=300)
        )
        channel = connection.channel()

        # Declare queue for uploader
        message = {
            Action.PRODUCER_UPLOADER_SEND_RAW.name.lower(): Action.PRODUCER_UPLOADER_SEND_RAW.value,
            DataColumn.FILE_PATH.value if hasattr(DataColumn, 'FILE_PATH') else 'file_path': file_path
        }
        channel.basic_publish(
            exchange='',
            routing_key=QueueName.UPLOAD.value,
            body=json.dumps(message)
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
        message = {
            Action.PRODUCER_PROCESSOR_SEND_RAW.name.lower(): Action.PRODUCER_PROCESSOR_SEND_RAW.value,
            DataColumn.FILE_PATH.value if hasattr(DataColumn, 'FILE_PATH') else 'file_path': file_path
        }
        channel.basic_publish(
            exchange='',
            routing_key=QueueName.PROCESS.value,
            body=json.dumps(message)
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