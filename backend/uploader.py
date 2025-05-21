import csv
import psycopg2
from dotenv import load_dotenv
import os
import json
import pandas as pd
import pika
from psycopg2.extras import execute_batch

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'user': os.getenv("user"),
    'password': os.getenv("password"),
    'host': os.getenv("host"),
    'port': os.getenv("port"),
    'dbname': os.getenv("dbname")
}

CSV_FILE_PATH = "./data/google_play_store_dataset.csv"
BATCH_SIZE = 1000  # Optimal batch size for performance

def create_connection():
    """Create and return a new database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

def upload_raw_data(file_path):
    """Upload raw data to the database based on the file path"""
    print("Uploading raw data...")
    try:
        # Debug: Print the file path
        print("Received file path:", file_path)

        # Check if file path is empty
        if not file_path.strip():
            print("Received empty file path. Skipping processing.")
            return

        # Read CSV file into DataFrame
        df = pd.read_csv(file_path)

        if df.empty:
            print("Received file is valid but results in an empty DataFrame. Skipping processing.")
            return

        # Upload raw data to the database
        conn = create_connection()
        cursor = conn.cursor()

        # Clear existing data
        print("Clearing existing data...")
        cursor.execute("TRUNCATE TABLE raw_apps RESTART IDENTITY")
        conn.commit()

        # Prepare insert query
        insert_query = """
            INSERT INTO raw_apps (
                app, category, rating, reviews, size, installs, type,
                price, content_rating, genres, last_updated,
                current_ver, android_ver
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Insert data into the database
        batch = []
        total_rows = 0
        for index, row in df.iterrows():
            prepared_row = (
                row['App'], row['Category'], row['Rating'], row['Reviews'],
                row['Size'], row['Installs'], row['Type'], row['Price'],
                row['Content Rating'], row['Genres'], row['Last Updated'],
                row['Current Ver'], row['Android Ver']
            )
            batch.append(prepared_row)
            total_rows += 1

            # Insert batch when full
            if len(batch) >= BATCH_SIZE:
                execute_batch(cursor, insert_query, batch)
                conn.commit()
                batch = []
                print(f"Uploaded {total_rows} rows so far...")

        # Insert remaining rows
        if batch:
            execute_batch(cursor, insert_query, batch)
            conn.commit()
            print(f"Uploaded {total_rows} rows so far...")

        print("Raw data uploaded successfully.")

    except pd.errors.EmptyDataError:
        print("Pandas encountered an EmptyDataError. The file might be invalid or empty.")
    except Exception as e:
        print(f"Error uploading raw data: {e}")

def upload_cleaned_data(file_path):
    """Upload cleaned data to the database based on the file path"""
    print("Uploading cleaned data...")
    try:
        # Debug: Print the file path
        print("Received file path:", file_path)

        # Check if file path is empty
        if not file_path.strip():
            print("Received empty file path. Skipping processing.")
            return

        # Read CSV file into DataFrame
        df = pd.read_csv(file_path)

        if df.empty:
            print("Received file is valid but results in an empty DataFrame. Skipping processing.")
            return

        # Upload cleaned data to the database
        conn = create_connection()
        cursor = conn.cursor()

        # Clear existing data
        print("Clearing existing data from cleaned_apps table...")
        cursor.execute("TRUNCATE TABLE cleaned_apps RESTART IDENTITY")
        conn.commit()

        # Prepare insert query
        insert_query = """
            INSERT INTO cleaned_apps (
                app, category, rating, reviews, size, installs, type,
                price, content_rating, genres, last_updated,
                current_ver, android_ver
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Insert data into the database
        batch = []
        for _, row in df.iterrows():
            prepared_row = (
                row['App'], row['Category'], row['Rating'], row['Reviews'],
                row['Size'], row['Installs'], row['Type'], row['Price'],
                row['Content Rating'], row['Genres'], row['Last Updated'],
                row['Current Ver'], row['Android Ver']
            )
            batch.append(prepared_row)

        execute_batch(cursor, insert_query, batch)
        conn.commit()
        print("Cleaned data uploaded successfully.")

    except pd.errors.EmptyDataError:
        print("Pandas encountered an EmptyDataError. The file might be invalid or empty.")
    except Exception as e:
        print(f"Error uploading cleaned data: {e}")

def upload_prediction(prediction_data):
    """Upload prediction to the prediction_history table"""
    print("Uploading prediction...")
    try:
        # Upload prediction to the database
        conn = create_connection()
        cursor = conn.cursor()

        # Prepare insert query
        insert_query = """
            INSERT INTO prediction_history (
                category, size, type, price, content_rating, genres,
                predicted_rating, predicted_installs, predicted_reviews,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """

        # Extract data from prediction
        input_features = prediction_data['Input Features']
        predictions = prediction_data['Predictions']
        
        print("Input Features:", input_features)
        print("Predictions:", predictions)
        prepared_row = (
            input_features['category'],
            input_features['app_size'],
            input_features['app_type'],
            input_features['price'],
            input_features['content_rating'],
            input_features['genres'],
            predictions['Rating'],
            predictions['Installs'],
            predictions['Reviews']
        )

        # Insert data
        cursor.execute(insert_query, prepared_row)
        conn.commit()
        print("Prediction uploaded successfully.")

    except Exception as e:
        print(f"Error uploading prediction: {e}")
        raise

def process_message(ch, method, properties, body):
    """Process received message from RabbitMQ"""
    try:
        message = json.loads(body)
        action = message.get('action')
        
        print(f"Received message - Action: {action}")

        if action == 'producer_uploader_sendRawData':
            file_path = message.get('file_path')
            upload_raw_data(file_path)
        elif action == 'processor_uploader_upload_cleaned':
            file_path = message.get('file_path')
            upload_cleaned_data(file_path)
        elif action == 'aimodel_uploader_uploadprediction':
            prediction_data = message.get('prediction_data')
            upload_prediction(prediction_data)
        else:
            print(f"Unknown action: {action}")

        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")

def start_listening():
    """Start listening for messages from RabbitMQ"""
    connection = None
    try:
        # Create connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        channel = connection.channel()
        
        # Declare queue
        channel.queue_declare(queue='upload_queue')
        
        print("Uploader service is listening for messages...")
        
        # Set up consumer
        channel.basic_consume(
            queue='upload_queue',
            on_message_callback=process_message
        )
        
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print("\nGracefully shutting down the uploader service...")
            channel.stop_consuming()
            
    except Exception as e:
        print(f"Error in RabbitMQ connection: {e}")
    finally:
        if connection and not connection.is_closed:
            connection.close()
            print("Connection closed")

if __name__ == "__main__":
    print("Starting uploader service...")
    try:
        start_listening()
    except KeyboardInterrupt:
        print("\nUploader service stopped")