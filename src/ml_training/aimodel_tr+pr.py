import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor
import pickle
from datetime import datetime

def prepare_data(df):
    """Prepare features and target variables"""
    X = df[['Category', 'Size', 'Type', 'Price', 'Content Rating', 'Genres']].copy()
    y = df[['Rating', 'Installs', 'Reviews']].copy()
    
    # Clip Rating values between 1 and 5
    y.loc[:, 'Rating'] = y['Rating'].clip(1.0, 5.0)
    
    # Convert categorical variables to dummy variables
    X = pd.get_dummies(X, columns=['Category', 'Type', 'Content Rating', 'Genres'])
    
    return X, y

def train_and_evaluate(X, y):
    """Train model and evaluate performance"""
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
    
    return model, X.columns.tolist()

def main():
    # Load data
    print("Loading data...")
    data = pd.read_csv('data/cleaned_google_dataset.csv')
    
    # Prepare features
    X, y = prepare_data(data)
    
    # Train and evaluate
    model, feature_columns = train_and_evaluate(X, y)
    
    # Save model
    print("\nSaving model...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f'model_{timestamp}.pkl', 'wb') as f:
        pickle.dump({'model': model, 'feature_columns': feature_columns}, f)
    
    print("Done!")

if __name__ == "__main__":
    main()
