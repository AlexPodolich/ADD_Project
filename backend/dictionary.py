from enum import Enum, auto

class FilePath(str, Enum):
    DATASET = './data/google_play_store_dataset.csv'
    DATASET_CLEANED = './data/google_play_store_dataset_cleaned.csv'

class QueueName(str, Enum):
    PROCESS = "process_queue"
    AI_MODEL = "ai_model_queue"
    UPLOAD = "upload_queue"

class Exchange(str, Enum):
    ADD_DIRECT = "add_direct"
    ADD_TOPIC = "add_topic"

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

class EnvVar(str, Enum):
    RABBITMQ_HOST = "RABBITMQ_HOST"
    DB_USER = "user"
    DB_PASSWORD = "password"
    DB_HOST = "host"
    DB_PORT = "port"
    DB_NAME = "dbname"

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
    FILE_PATH = "file_path" 

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

class FlaskAPI(str, Enum):
    FLASK_API_PORT = 5000
