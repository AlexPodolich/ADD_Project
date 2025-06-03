
import os
import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor
import pickle
import pika
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import logging
import random
import time
from .dictionary import QueueName, Action

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)
app = Flask(__name__)
CORS(app, origins=["http://localhost:3000", "http://localhost:6543"]) # Adjust port if needed
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')

def prepare_data(df):
    """Prepare features and target variables"""
    X = df[['Category', 'Size', 'Type', 'Price', 'Content Rating', 'Genres']].copy()
    y = df[['Rating', 'Installs', 'Reviews']].copy()
    
    # Clip Rating values between 1 and 5
    y.loc[:, 'Rating'] = y['Rating'].clip(1.0, 5.0)
    
    # Convert categorical variables to dummy variables
    X = pd.get_dummies(X, columns=['Category', 'Type', 'Content Rating', 'Genres'])
    
    return X, y

def train_model(data_path):
    print("Loading data...")
    data = pd.read_csv(data_path)
    
    # Prepare features
    X, y = prepare_data(data)
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train model
    print("Training model...")
    model = MultiOutputRegressor(RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1))
    model.fit(X_train, y_train)
    
    # Evaluate
    predictions = model.predict(X_test)
    print("\nModel Performance:")
    metrics = ['Rating', 'Installs', 'Reviews']
    for i, metric in enumerate(metrics):
        error = np.mean(np.abs(y_test.iloc[:, i] - predictions[:, i]))
        print(f"{metric} Mean Absolute Error: {error:.2f}")
    
    # Save model with fixed filename in ml_training directory
    print("\nSaving model...")
    model_data = {
        'model': model,
        'feature_columns': X.columns.tolist()
    }
    model_path = os.path.join(os.path.dirname(__file__), 'trained_model.pkl')
    with open(model_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    print("Training completed!")
    return model, X.columns.tolist()

def load_model(model_path=None):
    """Load a trained model from file"""
    try:
        if model_path is None:
            model_path = os.path.join(os.path.dirname(__file__), 'trained_model.pkl')
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
            return model_data['model'], model_data['feature_columns']
    except Exception as e:
        print(f"Error loading model: {e}")
        return None, None

def send_to_uploader(prediction_data):
    """Send prediction to uploader via RabbitMQ"""
    for _ in range(10):
        try:
            print("\n[DEBUG] Attempting to send prediction to uploader...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()

            message = {
                'action': Action.AIMODEL_UPLOADER_UPLOAD_PREDICTION.value,
                'prediction_data': prediction_data,
            }

            channel.basic_publish(
                exchange='',
                routing_key=QueueName.UPLOAD.value,
                body=json.dumps(message)
            )
            
            print("[DEBUG] Prediction sent to uploader queue successfully")
            connection.close()

        except Exception as e:
            print(f"[DEBUG] Error sending prediction to uploader: {e}")

def process_message(ch, method, properties, body):
    """Process received message from RabbitMQ"""
    try:
        message = json.loads(body)
        action = message.get('action')
        file_path = message.get('file_path')

        print(f"[DEBUG] Received message - Action: {action}, File path: {file_path}")

        if action == Action.PROCESSOR_AIMODEL_TRAIN_MODEL.value:
            # Train the model
            print("[DEBUG] Starting model training...")
            model, feature_columns = train_model(file_path)
            print("[DEBUG] Model training completed")
        else:
            print(f"[DEBUG] Unknown action: {action}")

        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[DEBUG] Error processing message: {e}")
        # Consider whether to acknowledge message in case of error
        ch.basic_ack(delivery_tag=method.delivery_tag)

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

            # Set up consumer
            channel.basic_consume(
                queue=QueueName.AI_MODEL.value,
                on_message_callback=process_message
            )

            print("Waiting for messages...")
            channel.start_consuming()

        except Exception as e:
            print(f"Error starting RabbitMQ listener: {e}")
            raise

@app.route('/predict', methods=['POST'])
def predictAndSend():
    try:
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400

        input_data = request.get_json()
        logging.info(f"Received input data: {input_data}")
        
        # Wczytaj model
        model, feature_columns = load_model()
        if model is None:
            return jsonify({"error": "Model not found"}), 500
        
        # Walidacja danych wejściowych
        required_fields = ['category', 'app_size', 'app_type', 'price', 'content_rating', 'genres']
        if not all(field in input_data for field in required_fields):
            return jsonify({"error": "Missing required fields"}), 400

        df = pd.DataFrame([{
            'Category': input_data['category'],
            'Size': input_data['app_size'],
            'Type': input_data['app_type'],
            'Price': float(input_data['price']),
            'Content Rating': input_data['content_rating'],
            'Genres': input_data['genres']
        }])

        df_encoded = pd.get_dummies(df, columns=['Category', 'Type', 'Content Rating', 'Genres'])
        final_df = pd.DataFrame(0, index=df_encoded.index, columns=feature_columns)
        common_cols = df_encoded.columns.intersection(feature_columns)
        final_df[common_cols] = df_encoded[common_cols]

        predictions = model.predict(final_df)
        predictions[0][0] = np.clip(predictions[0][0], 1.0, 5.0)

        result = {
            'Input Features': input_data,
            'Predictions': {
                'Rating': float(predictions[0][0]),
                'Installs': int(predictions[0][1]),
                'Reviews': int(predictions[0][2])
            }
        }

        send_to_uploader(result)
        return jsonify(result)

    except Exception as e:
        logging.error(f"Prediction error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    from threading import Thread
    flask_api_port = FLASK_API_PORT = int(os.getenv('FLASK_API_PORT'))
    logging.info(f"Starting Flask API server on port {flask_api_port}...")
    Thread(target=start_listening).start()
    app.run(debug=True, port=flask_api_port, host='0.0.0.0') 