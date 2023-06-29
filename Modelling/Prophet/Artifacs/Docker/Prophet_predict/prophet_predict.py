import pandas as pd
import os
from prophet import Prophet
import numpy as np
import pickle

def load_and_predict(region, data_type, periods):
    """
    Load a trained Prophet model for a given region and data type, make predictions, and store the results.

    Args:
    region (str): The region code.
    data_type (str): 'Demand' or 'Net generation'.
    periods (int): Number of periods for which to make predictions.

    Returns:
    pd.DataFrame: A dataframe containing the forecast.
    """
    # Load the trained Prophet model
    with open(f"/workspace/VoltWise/Modelling/Prophet/Pickle_files/{region}_{data_type}_prophet_model.pkl", "rb") as file:
        model = pickle.load(file)

    # Create future dataframe and make predictions
    future = model.make_future_dataframe(periods=periods, freq='D')
    forecast = model.predict(future)

    # Save the forecast as a CSV file
    forecast.to_csv(f"/workspace/VoltWise/Modelling/Prophet/predictions/{region}_{data_type}_forecast.csv", index=False)

    return forecast


# Set the number of periods for which to make predictions
periods = 6 * 30  # Predict the next 6 months, assuming 30 days per month

# Make predictions for each region and data type
for region in regions:
    for data_type in data_types:
        forecast = load_and_predict(region, data_type, periods)