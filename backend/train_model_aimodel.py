import os
import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor
from backend.dictionary import FilePath
import pickle
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def prepare_data(df):
    """Prepare features and target variables"""
    X = df[['Category', 'Size', 'Type', 'Price', 'Content Rating', 'Genres']].copy()
    y = df[['Rating', 'Installs', 'Reviews']].copy()
    
    y.loc[:, 'Rating'] = y['Rating'].clip(1.0, 5.0)

    X = pd.get_dummies(X, columns=['Category', 'Type', 'Content Rating', 'Genres'])
    
    return X, y

def train_model(data_path):
    print("Loading data...")
    data = pd.read_csv(data_path)
    
    X, y = prepare_data(data)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print("Training model...")
    model = MultiOutputRegressor(RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1))
    model.fit(X_train, y_train)
    
    predictions = model.predict(X_test)
    print("\nModel Performance:")
    metrics = ['Rating', 'Installs', 'Reviews']
    for i, metric in enumerate(metrics):
        error = np.mean(np.abs(y_test.iloc[:, i] - predictions[:, i]))
        print(f"{metric} Mean Absolute Error: {error:.2f}")
    
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

if __name__ == "__main__":
    data_path = os.getenv(FilePath.DATASET_CLEANED, '.\data\cleaned_google_dataset.csv')
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Dataset file not found at {data_path}")
    
    model, feature_columns = train_model(data_path)
    
    print("Model and feature columns saved successfully.")
    print(f"Model: {model}")
    print(f"Feature Columns: {feature_columns}")