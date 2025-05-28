import csv
import psycopg2
from dotenv import load_dotenv
import os
import json
import pandas as pd
import pika
from psycopg2.extras import execute_batch
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

# Database configuration
DB_USER = os.environ.get('user')
DB_PASSWORD = os.environ.get('password')
DB_HOST = os.environ.get('host')
DB_PORT = os.environ.get('port', '5432')
DB_NAME = os.environ.get('dbname')

# Validate required environment variables
if not DB_HOST:
    raise ValueError("host environment variable is not set")
if not DB_USER:
    raise ValueError("user environment variable is not set")
if not DB_PASSWORD:
    raise ValueError("password environment variable is not set")
if not DB_NAME:
    raise ValueError("dbname environment variable is not set")

# Log environment variables (without sensitive data)
logger.info(f"host: {DB_HOST}")
logger.info(f"port: {DB_PORT}")
logger.info(f"user: {DB_USER}")
logger.info(f"dbname: {DB_NAME}")

# Construct database connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Log the connection string (without password)
safe_conn_string = DATABASE_URL.replace(DB_PASSWORD, '****')
logger.info(f"Database connection string: {safe_conn_string}")

CSV_FILE_PATH = "./data/google_play_store_dataset.csv"
BATCH_SIZE = 1000  # Optimal batch size for performance

def create_connection():
    """Create and return a new database connection"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"[UPLOADER] Attempting to connect to database (attempt {attempt + 1}/{max_retries})...")
            conn = psycopg2.connect(
                DATABASE_URL,
                sslmode='require'
            )
            conn.autocommit = False
            logger.info("[UPLOADER] Successfully connected to database")
            return conn
        except Exception as e:
            logger.error(f"[UPLOADER] Database connection failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"[UPLOADER] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("[UPLOADER] Max retries reached. Could not connect to database.")
                raise

def upload_raw_data(file_path):
    """Upload raw data to the database based on the file path"""
    logger.info("Starting raw data upload...")
    try:
        # Debug: Print the file path
        logger.info(f"Received file path: {file_path}")

        # Check if file path is empty
        if not file_path.strip():
            logger.info("Received empty file path. Skipping processing.")
            return

        # Read CSV file into DataFrame
        df = pd.read_csv(file_path)

        if df.empty:
            logger.info("Received file is valid but results in an empty DataFrame. Skipping processing.")
            return

        # Upload raw data to the database
        conn = create_connection()
        cursor = conn.cursor()

        # Clear existing data
        logger.info("Clearing existing data...")
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
                logger.info(f"Uploaded {total_rows} rows so far...")

        # Insert remaining rows
        if batch:
            execute_batch(cursor, insert_query, batch)
            conn.commit()
            logger.info(f"Uploaded {total_rows} rows so far...")

        logger.info("Raw data uploaded successfully.")

    except pd.errors.EmptyDataError:
        logger.error("Pandas encountered an EmptyDataError. The file might be invalid or empty.")
    except Exception as e:
        logger.error(f"Error uploading raw data: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def upload_cleaned_data(file_path):
    """Upload cleaned data to the database based on the file path"""
    logger.info("Starting cleaned data upload...")
    try:
        # Debug: Print the file path
        logger.info(f"Received file path: {file_path}")

        # Check if file path is empty
        if not file_path.strip():
            logger.info("Received empty file path. Skipping processing.")
            return

        # Read CSV file into DataFrame
        df = pd.read_csv(file_path)

        if df.empty:
            logger.info("Received file is valid but results in an empty DataFrame. Skipping processing.")
            return

        # Upload cleaned data to the database
        conn = create_connection()
        cursor = conn.cursor()

        # Clear existing data
        logger.info("Clearing existing data from cleaned_apps table...")
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
        logger.info("Cleaned data uploaded successfully.")

    except pd.errors.EmptyDataError:
        logger.error("Pandas encountered an EmptyDataError. The file might be invalid or empty.")
    except Exception as e:
        logger.error(f"Error uploading cleaned data: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def upload_prediction(prediction_data):
    """Upload prediction to the prediction_history table"""
    logger.info("[UPLOADER] Starting prediction upload...")
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
        
        logger.info(f"[UPLOADER] Input Features: {input_features}")
        logger.info(f"[UPLOADER] Predictions: {predictions}")
        
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
        logger.info("[UPLOADER] Prediction uploaded successfully")

    except Exception as e:
        logger.error(f"[UPLOADER] Error uploading prediction: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def process_message(ch, method, properties, body):
    """Process received message from RabbitMQ"""
    try:
        message = json.loads(body)
        action = message.get('action')
        
        logger.info(f"[UPLOADER] Received message - Action: {action}")

        if action == 'producer_uploader_sendRawData':
            file_path = message.get('file_path')
            logger.info(f"[UPLOADER] Processing raw data from: {file_path}")
            upload_raw_data(file_path)
        elif action == 'processor_uploader_upload_cleaned':
            file_path = message.get('file_path')
            logger.info(f"[UPLOADER] Processing cleaned data from: {file_path}")
            upload_cleaned_data(file_path)
        elif action == 'aimodel_uploader_uploadprediction':
            prediction_data = message.get('prediction_data')
            logger.info(f"[UPLOADER] Processing prediction data: {prediction_data}")
            upload_prediction(prediction_data)
        else:
            logger.info(f"[UPLOADER] Unknown action: {action}")

        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"[UPLOADER] Successfully processed message with action: {action}")

    except Exception as e:
        logger.error(f"[UPLOADER] Error processing message: {e}")
        logger.error(f"[UPLOADER] Message content: {body}")

def start_listening():
    """Start listening for messages from RabbitMQ"""
    connection = None
    for _ in range(10):
        try:
            logger.info("[UPLOADER] Attempting to connect to RabbitMQ...")
            # Create connection
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(RABBITMQ_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue='upload_queue')
            logger.info("[UPLOADER] Successfully connected to RabbitMQ")
            logger.info("[UPLOADER] Service is listening for messages...")
            channel.basic_consume(
                queue='upload_queue',
                on_message_callback=process_message
            )
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                logger.info("\n[UPLOADER] Gracefully shutting down the uploader service...")
                channel.stop_consuming()
            return
        except Exception as e:
            logger.error(f"[UPLOADER] Error in RabbitMQ connection: {e}")
            time.sleep(5)
    if connection and not connection.is_closed:
        connection.close()
        logger.info("[UPLOADER] Connection closed")

if __name__ == "__main__":
    logger.info("[UPLOADER] Starting uploader service...")
    try:
        start_listening()
    except KeyboardInterrupt:
        logger.info("\n[UPLOADER] Uploader service stopped")