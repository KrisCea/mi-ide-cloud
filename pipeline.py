import pandas as pd
import time

def run_pipeline():
    # Step 1: Load data
    print("Loading data...")
    data = pd.read_csv('data.csv')
    
    # Step 2: Preprocess data
    print("Preprocessing data...")
    data = data.dropna()
    
    # Step 3: Train model
    print("Training model...")
    time.sleep(2)  # Simulate time-consuming training process
    
    # Step 4: Evaluate model
    print("Evaluating model...")
    time.sleep(1)  # Simulate evaluation process
    
    print("Pipeline completed successfully.")