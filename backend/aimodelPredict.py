import os
import json
import pandas as pd
import numpy as np
import pickle
import pika
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import logging
from .dictionary import QueueName, Action, Exchange
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor

logging.basicConfig(level=logging.INFO)
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)
app = Flask(__name__)
CORS(app, origins=["http://localhost:3000", "http://localhost:6543"])
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')


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
            exchange=Exchange.ADD_DIRECT.value,
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
            print("[DEBUG] Received training action in predict service (ignored)")
        else:
            print(f"[DEBUG] Unknown action: {action}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[DEBUG] Error processing message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

def start_listening():
    """Start listening for messages from RabbitMQ"""
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
        model, feature_columns = load_model()
        if model is None:
            return jsonify({"error": "Model not found"}), 500
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
    flask_api_port = 5001
    logging.info(f"Starting Flask API server on port {flask_api_port}...")
    Thread(target=start_listening).start()
    app.run(debug=True, port=flask_api_port, host='0.0.0.0')
