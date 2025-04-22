import os
import pandas as pd
import numpy as np
from datetime import datetime
import pika
import json


def clean_size(size_str):
    """Convert size string to numeric MB value"""
    if pd.isna(size_str) or size_str == 'Varies with device':
        return None
    if isinstance(size_str, (int, float)):
        return size_str
    # Remove +, M, k, and commas from the string
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

def clean_data():
    """Clean and process the Google Play Store dataset"""
    print("Starting data cleaning process...")
    
    # Read the CSV file
    input_path = './data/google_play_store_dataset.csv'
    df = pd.read_csv(input_path)
    
    # Clean numeric columns
    df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
    df['Reviews'] = pd.to_numeric(df['Reviews'], errors='coerce')
    df['Size'] = df['Size'].apply(clean_size)
    df['Installs'] = df['Installs'].apply(clean_installs)
    df['Price'] = df['Price'].apply(clean_price)
    
    # Handle missing values without inplace
    df['Rating'] = df['Rating'].fillna(df['Rating'].mean())
    df['Size'] = df['Size'].fillna(df['Size'].median())
    df['Reviews'] = df['Reviews'].fillna(0)
    df['Type'] = df['Type'].fillna('Free')
    
    # Clean text columns
    df['Category'] = df['Category'].str.strip()
    df['Type'] = df['Type'].str.strip()
    df['Content Rating'] = df['Content Rating'].str.strip()
    
    # Convert date
    df['Last Updated'] = pd.to_datetime(df['Last Updated'], format='mixed', errors='coerce')
    df['Last Updated'] = df['Last Updated'].fillna(pd.Timestamp.min)
    
    # Split genres
    df['Genres'] = df['Genres'].str.split(';')
    
    # Save cleaned dataset
    output_path = './data/cleaned_google_dataset.csv'
    df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")
    print("Data cleaning process completed.")
    
    # Send to uploader via RabbitMQ
    send_to_uploader(output_path)

def send_to_uploader(file_path):
    """Send cleaned data file path to uploader via RabbitMQ"""
    try:
        print("Trying to send message to RabbitMQ...")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                heartbeat=600,
                blocked_connection_timeout=300
            )
        )
        channel = connection.channel()
        
        # Declare queue with consistent settings
        channel.queue_declare(
            queue='upload_queue',
            durable=False,  # Changed to match uploader
            auto_delete=False
        )
        

        #name action
        # processor-uploader-upload_cleaned
        message = {
            'action': 'upload_cleaned',
            'file_path': file_path,
            'timestamp': datetime.now().isoformat()
        }
        
        channel.basic_publish(
            exchange='',
            routing_key='upload_queue',
            body=json.dumps(message)
        )
        
        print("Cleaned data upload command sent")
        connection.close()
        
    except Exception as e:
        print(f"Failed to send message to RabbitMQ: {e}")
        raise

if __name__ == "__main__":
    clean_data()