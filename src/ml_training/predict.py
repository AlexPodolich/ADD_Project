import pandas as pd
import pickle
import os
import numpy as np

def load_model(model_path='model_20250425_175157.pkl'):
    """Load the trained model from a pickle file"""
    try:
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
            return model_data['model'], model_data['feature_columns']
    except Exception as e:
        print(f"Error loading model: {e}")
        return None, None

def predict(input_data, model, feature_columns):
    """Make predictions using the trained model"""
    try:
        # Convert input to DataFrame
        df = pd.DataFrame([input_data])
        
        # Encode categorical variables
        df_encoded = pd.get_dummies(df, columns=['Category', 'Type', 'Content Rating', 'Genres'])
        
        # Create a new DataFrame with all required columns initialized to 0
        final_df = pd.DataFrame(0, index=df_encoded.index, columns=feature_columns)
        
        # Update the values for columns that exist in df_encoded
        common_cols = df_encoded.columns.intersection(feature_columns)
        final_df[common_cols] = df_encoded[common_cols]
        
        # Make predictions
        predictions = model.predict(final_df)
        
        # Clip rating predictions between 1.0 and 5.0
        predictions[0][0] = np.clip(predictions[0][0], 1.0, 5.0)
        
        # Create result dictionary
        result = {
            'Input Features': input_data,
            'Predictions': {
                'Rating': predictions[0][0],
                'Installs': predictions[0][1],
                'Reviews': predictions[0][2]
            }
        }
        
        return result
    except Exception as e:
        print(f"Error making prediction: {e}")
        return None

def main():
    # Load the model
    model, feature_columns = load_model()
    if model is None:
        return
    
    # Premade example data for testing
    example_data = {
        'Category': 'GAME',
        'Size': 50.0,  # MB
        'Type': 'Free',
        'Price': 0.0,
        'Content Rating': 'Everyone',
        'Genres': 'Action,Adventure'
    }
    
    # Make prediction
    result = predict(example_data, model, feature_columns)
    
    if result:
        print("\nInput Features:")
        for key, value in result['Input Features'].items():
            print(f"{key}: {value}")
            
        print("\nPredictions:")
        for key, value in result['Predictions'].items():
            print(f"{key}: {value}")

if __name__ == "__main__":
    main()