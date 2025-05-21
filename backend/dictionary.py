
# Enums for string constants used in the ADD project
from enum import Enum, auto

# File path constants
class FilePath(str, Enum):
    DATASET = './data/google_play_store_dataset.csv'

# Queues used for RabbitMQ communication
class QueueName(str, Enum):
    PROCESS = "process_queue"
    AI_MODEL = "ai_model_queue"
    UPLOAD = "upload_queue"

# Exchanges for RabbitMQ communication
class Exchange(str, Enum):
    # Direct exchange for routing messages is used when the producer and consumer use the same queue and the same routing key
    ADD_DIRECT = "add_direct"
    #the one with '.name.' syntax
    ADD_TOPIC = "add_topic"

# Action strings for message routing between services
class Action(str, Enum):
    # Producer to Uploader
    PRODUCER_UPLOADER_SEND_RAW = "producer_uploader_sendRawData"
    # Producer to Processor
    PRODUCER_PROCESSOR_SEND_RAW = "producer_processor_sendRawData"
    # Processor to Uploader
    PROCESSOR_UPLOADER_UPLOAD_CLEANED = "processor_uploader_upload_cleaned"
    # Processor to Aimodel
    PROCESSOR_AIMODEL_TRAIN_MODEL = "processor_aimodel_trainmodel"
    # Aimodel to Uploader
    AIMODEL_UPLOADER_UPLOAD_PREDICTION = "aimodel_uploader_uploadprediction"

# Environment variable keys
class EnvVar(str, Enum):
    RABBITMQ_HOST = "RABBITMQ_HOST"
    DB_USER = "user"
    DB_PASSWORD = "password"
    DB_HOST = "host"
    DB_PORT = "port"
    DB_NAME = "dbname"

# Data column names (for reference, if needed in code)
class DataColumn(str, Enum):
    # Raw and cleaned data columns (CSV/database)
    APP = "App"
    CATEGORY = "Category"
    RATING = "Rating"
    REVIEWS = "Reviews"
    SIZE = "Size"
    INSTALLS = "Installs"
    TYPE = "Type"
    PRICE = "Price"
    CONTENT_RATING = "Content Rating"
    GENRES = "Genres"
    LAST_UPDATED = "Last Updated"
    CURRENT_VER = "Current Ver"
    ANDROID_VER = "Android Ver"
    FILE_PATH = "file_path"  # For message keys

class DbColumn(str, Enum):
    # Database table columns for raw_apps and cleaned_apps
    APP = "app"
    CATEGORY = "category"
    RATING = "rating"
    REVIEWS = "reviews"
    SIZE = "size"
    INSTALLS = "installs"
    TYPE = "type"
    PRICE = "price"
    CONTENT_RATING = "content_rating"
    GENRES = "genres"
    LAST_UPDATED = "last_updated"
    CURRENT_VER = "current_ver"
    ANDROID_VER = "android_ver"

class PredictionColumn(str, Enum):
    CATEGORY = "category"
    SIZE = "app_size"
    TYPE = "app_type"
    PRICE = "price"
    CONTENT_RATING = "content_rating"
    GENRES = "genres"
    RATING = "Rating"
    INSTALLS = "Installs"
    REVIEWS = "Reviews"

# Example usage:
#   from dictionary import QueueName, Action
#   queue = QueueName.PROCESS
#   action = Action.PRODUCER_UPLOADER_SEND_RAW
