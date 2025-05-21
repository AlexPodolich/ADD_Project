

import os
import json
import pika
from backend.dictionary import QueueName, Action, DataColumn
import argparse

# File path for the dataset
from backend.dictionary import FilePath
CSV_FILE_PATH = FilePath.DATASET.value

def send_data_uploader_processor(file_path):
    """Send the dataset file path to both the uploader and processor via RabbitMQ"""
    try:
        print("Producer connecting to RabbitMQ...")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', heartbeat=600, blocked_connection_timeout=300)
        )
        channel = connection.channel()

        # Message for uploader
        uploader_message = {
            Action.PRODUCER_UPLOADER_SEND_RAW.name.lower(): Action.PRODUCER_UPLOADER_SEND_RAW.value,
            DataColumn.FILE_PATH.value: file_path
        }

        from backend.dictionary import Exchange
        channel.basic_publish(
            exchange=Exchange.ADD_DIRECT.value,
            routing_key=QueueName.UPLOAD.value,
            body=json.dumps(uploader_message)
        )
        print("File path sent to uploader queue.")

        # Message for processor
        processor_message = {
            Action.PRODUCER_PROCESSOR_SEND_RAW.name.lower(): Action.PRODUCER_PROCESSOR_SEND_RAW.value,
            DataColumn.FILE_PATH.value: file_path
        }
        channel.basic_publish(
            exchange=Exchange.ADD_DIRECT.value,
            routing_key=QueueName.PROCESS.value,
            body=json.dumps(processor_message)
        )
        print("File path sent to processor queue.")

        connection.close()
    except Exception as e:
        print(f"Failed to send file path to uploader or processor queue: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send dataset file path to uploader and processor queues.")
    parser.add_argument('--file', type=str, default=CSV_FILE_PATH, help='Path to the dataset file (default: from FilePath enum)')
    args = parser.parse_args()
    file_path = args.file
    if os.path.exists(file_path):
        print(f"Dataset found at {file_path}. Sending to uploader and processor...")
        send_data_uploader_processor(file_path)
    else:
        print(f"Dataset file not found at {file_path}. Please check the file path.")