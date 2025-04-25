import os
import json
import pika
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import logging
import random

logging.basicConfig(level=logging.INFO)

# Load .env variables (primarily for potential RabbitMQ connection details if not localhost)
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

app = Flask(__name__)
# Allow requests from our frontend development server
CORS(app, origins=["http://localhost:3000", "http://localhost:6543"]) # Adjust port if needed

# RabbitMQ Configuration (assuming localhost default)
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
UPLOAD_QUEUE = 'upload_queue'

def publish_to_rabbitmq(message_body):
    """Publishes a message to the RabbitMQ upload queue."""
    connection = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=UPLOAD_QUEUE, durable=False) # Declare queue (idempotent)

        channel.basic_publish(
            exchange='',
            routing_key=UPLOAD_QUEUE,
            body=json.dumps(message_body),
            properties=pika.BasicProperties(
                # delivery_mode=2,  # Optional: make message persistent
            )
        )
        logging.info(f"Sent message to RabbitMQ queue '{UPLOAD_QUEUE}': {message_body['action']}")
    except Exception as e:
        logging.error(f"Failed to publish message to RabbitMQ: {e}")
        # Depending on requirements, you might want to raise this error
        # or handle it (e.g., return an error status to the frontend)
    finally:
        if connection and connection.is_open:
            connection.close()

@app.route('/predict', methods=['POST'])
def handle_prediction():
    """Receives input data, generates fake prediction, sends to RabbitMQ, returns result."""
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    input_data = request.get_json()
    logging.info(f"Received prediction request: {input_data}")

    # --- Basic Input Validation (Example) ---
    required_fields = ['category', 'app_size', 'app_type', 'price', 'content_rating', 'genres']
    if not all(field in input_data for field in required_fields):
        return jsonify({"error": "Missing required fields in input data"}), 400

    # --- Generate Fake Predictions ---
    # (Replace with actual ML model call in the future)
    predicted_installs = f"~{random.randint(1000, 5000000).toLocaleString() if hasattr(random.randint(1,2), 'toLocaleString') else random.randint(1000, 5000000)}"
    predicted_reviews = f"~{random.randint(100, 100000).toLocaleString() if hasattr(random.randint(1,2), 'toLocaleString') else random.randint(100, 100000)}"
    predicted_rating = f"{(random.random() * 4 + 1):.1f} / 5.0"

    # Combine input data with predictions
    result_data = {
        **input_data,
        "predicted_installs": predicted_installs,
        "predicted_reviews": predicted_reviews,
        "predicted_rating": predicted_rating
    }

    # --- Send data to Uploader via RabbitMQ ---
    rabbitmq_message = {
        "action": "save_prediction",
        "data": result_data
    }
    publish_to_rabbitmq(rabbitmq_message)

    # --- Return combined result immediately to frontend ---
    # (We don't wait for RabbitMQ processing for faster UI response)
    logging.info(f"Returning prediction result: {result_data}")
    return jsonify(result_data), 200

if __name__ == '__main__':
    # Remove reliance on PORT env variable for Flask
    # Hardcode the port for the Flask API server
    flask_api_port = 5001 
    logging.info(f"Starting Flask API server on port {flask_api_port}...")
    # Use the hardcoded port variable here
    app.run(debug=True, port=flask_api_port, host='0.0.0.0') 