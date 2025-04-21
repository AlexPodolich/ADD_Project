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

def upload_raw_data():
    """Upload raw CSV data without any transformations"""
    conn = None
    cursor = None
    total_rows = 0
    
    try:
        conn = create_connection()
        cursor = conn.cursor()
        
        # 1. Clear existing data
        print("Clearing existing data...")
        cursor.execute("TRUNCATE TABLE raw_apps RESTART IDENTITY")
        conn.commit()
        
        # 2. Prepare insert query
        insert_query = """
            INSERT INTO raw_apps (
                app, category, rating, reviews, size, installs, type,
                price, content_rating, genres, last_updated,
                current_ver, android_ver
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        print("Starting CSV data upload...")
        batch = []
        
        # 3. Process CSV file
        with open(CSV_FILE_PATH, encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                total_rows += 1
                
                # Prepare raw data exactly as-is from CSV
                prepared_row = (
                    row['App'],
                    row['Category'],
                    row['Rating'],
                    row['Reviews'],
                    row['Size'],
                    row['Installs'],
                    row['Type'],
                    row['Price'],
                    row['Content Rating'],
                    row['Genres'],
                    row['Last Updated'],
                    row['Current Ver'],
                    row['Android Ver']
                )
                
                batch.append(prepared_row)
                
                # Insert batch when full
                if len(batch) >= BATCH_SIZE:
                    execute_batch(cursor, insert_query, batch)
                    conn.commit()
                    batch = []
                    print(f"Processed {total_rows} rows...")
            
            # Insert remaining rows
            if batch:
                execute_batch(cursor, insert_query, batch)
                conn.commit()
        
        print("Data upload completed successfully")
        
    except Exception as e:
        print(f"Upload failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    print(f"\nTotal rows processed: {total_rows}")


def upload_cleaned_data(file_path):
    """Upload cleaned CSV data to the database"""
    conn = None
    cursor = None
    total_rows = 0
    
    try:
        conn = create_connection()
        cursor = conn.cursor()
        
        # Clear existing data
        print("Clearing existing data from cleaned_apps table...")
        cursor.execute("TRUNCATE TABLE cleaned_apps RESTART IDENTITY")
        conn.commit()
        
        # Read the cleaned CSV file
        df = pd.read_csv(file_path)
        
        # Prepare insert query
        insert_query = """
            INSERT INTO cleaned_apps (
                app, category, rating, reviews, size, installs, type,
                price, content_rating, genres, last_updated,
                current_ver, android_ver
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        print("Starting cleaned data upload...")
        batch = []
        
        # Process DataFrame rows
        for _, row in df.iterrows():
            total_rows += 1
            
            # Convert genres list back to string if needed
            genres = ';'.join(row['Genres']) if isinstance(row['Genres'], list) else row['Genres']
            
            prepared_row = (
                row['App'],
                row['Category'],
                row['Rating'],
                row['Reviews'],
                row['Size'],
                row['Installs'],
                row['Type'],
                row['Price'],
                row['Content Rating'],
                genres,
                row['Last Updated'],
                row['Current Ver'],
                row['Android Ver']
            )
            
            batch.append(prepared_row)
            
            # Insert batch when full
            if len(batch) >= BATCH_SIZE:
                execute_batch(cursor, insert_query, batch)
                conn.commit()
                batch = []
                print(f"Processed {total_rows} rows...")
        
        # Insert remaining rows
        if batch:
            execute_batch(cursor, insert_query, batch)
            conn.commit()
        
        print("Cleaned data upload completed successfully")
        
    except Exception as e:
        print(f"Upload failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    print(f"\nTotal rows processed: {total_rows}")

def process_message(ch, method, properties, body):
    """Process received message from RabbitMQ"""
    try:
        message = json.loads(body)
        action = message.get('action')
        file_path = message.get('file_path')
        
        print(f"Received message - Action: {action}, File path: {file_path}")
        
        if action == 'upload_raw':
            upload_raw_data()
        elif action == 'upload_cleaned':
            upload_cleaned_data(file_path)
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