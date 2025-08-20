import pandas as pd
from datetime import datetime
import pika
import json
import time
from .dictionary import QueueName, Action
import os
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')


def send_to_uploader(file_path):
    """Send cleaned data file path to uploader via RabbitMQ"""
    for _ in range(10):
        try:
            print("Trying to send message to RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()
            
            message = {
                'action': Action.PROCESSOR_UPLOADER_UPLOAD_CLEANED.value,
                'file_path': file_path,
                'timestamp': datetime.now().isoformat()
            }
            channel.basic_publish(
                exchange='',
                routing_key=QueueName.UPLOAD.value,
                body=json.dumps(message)
            )
            
            print("Cleaned data upload command sent")
            connection.close()
        
        except Exception as e:
            print(f"Failed to send message to RabbitMQ: {e}")
            raise

def send_to_aimodel(file_path):
    """Send cleaned data file path to aimodel via RabbitMQ"""
    for _ in range(10):
        try:
            print("Trying to send message to RabbitMQ for aimodel...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            
            channel = connection.channel()

            message = {
                'action': Action.PROCESSOR_AIMODEL_TRAIN_MODEL.value,
                'file_path': file_path,
                'timestamp': datetime.now().isoformat()
            }
            channel.basic_publish(
                exchange='',
                routing_key=QueueName.AI_MODEL.value,
                body=json.dumps(message)
            )

            print("Cleaned data training command sent to aimodel")
            connection.close()

        except Exception as e:
            print(f"Failed to send message to RabbitMQ for aimodel: {e}")
            raise

def process_message(ch, method, properties, body):
    """Process received message from RabbitMQ"""
    try:
        message = json.loads(body)
        action = message.get('action')
        file_path = message.get('file_path')

        print(f"Received message - Action: {action}, File path: {file_path}")

        if action == Action.PRODUCER_PROCESSOR_SEND_RAW.value:
            process_data(file_path)
        else:
            print(f"Unknown action: {action}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")

def clean_size(size_str):
    """Convert size string to numeric MB value"""
    if pd.isna(size_str) or size_str == 'Varies with device':
        return None
    if isinstance(size_str, (int, float)):
        return size_str

    size = float(size_str.replace('M', '').replace('k', '').replace(',', '').replace('+', ''))
    if 'k' in str(size_str).lower():
        size = size / 1024
    return size

def clean_installs(install_str):
    """Convert install string to numeric value"""
    if pd.isna(install_str) or install_str == 'Free':
        return 0
    return int(install_str.replace(',', '').replace('+', '').strip())

def clean_price(price_str):
    """Convert price string to numeric value"""
    if pd.isna(price_str) or price_str == 'Free' or not isinstance(price_str, str):
        return 0.0
    try:
        return float(price_str.replace('$', '').strip())
    except ValueError:
        return 0.0


def process_data(file_path):
    """Process the data received from the producer"""
    print(f"Processing data from file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        # Perform data cleaning and processing
        df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
        df['Reviews'] = pd.to_numeric(df['Reviews'], errors='coerce')
        df['Size'] = df['Size'].apply(clean_size)
        df['Installs'] = df['Installs'].apply(clean_installs)
        df['Price'] = df['Price'].apply(clean_price)

        # Handle missing values
        df['Rating'] = df['Rating'].fillna(df['Rating'].mean())
        df['Size'] = df['Size'].fillna(df['Size'].median())
        df['Reviews'] = df['Reviews'].fillna(0)
        df['Type'] = df['Type'].fillna('Free')

        # Clean text columns
        df['Category'] = df['Category'].str.strip()
        df['Type'] = df['Type'].str.strip()
        df['Content Rating'] = df['Content Rating'].str.strip()

        df['Last Updated'] = pd.to_datetime(df['Last Updated'], format='mixed', errors='coerce')
        df['Last Updated'] = df['Last Updated'].fillna(pd.Timestamp.min)

        df['Genres'] = df['Genres'].str.split(';')

        output_path = './data/cleaned_google_dataset.csv'
        df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")

        send_to_uploader(output_path)

        send_to_aimodel(output_path)

    except Exception as e:
        print(f"Error processing data: {e}")

def start_listening():
    """Start listening for messages from RabbitMQ"""
    for _ in range(10):
        try:
            print("Starting RabbitMQ listener...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            
            channel = connection.channel()

            channel.queue_declare(
                queue='process_queue',
                durable=False,
                auto_delete=False
            )

            channel.basic_consume(
                queue=QueueName.PROCESS.value,
                on_message_callback=process_message
            )

            print("Waiting for messages...")
            channel.start_consuming()

        except Exception as e:
            print(f"Error starting RabbitMQ listener: {e}")
            raise

if __name__ == "__main__":
    start_listening()