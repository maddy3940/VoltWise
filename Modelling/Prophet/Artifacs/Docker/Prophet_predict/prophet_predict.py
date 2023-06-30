import pandas as pd
import os
from prophet import Prophet
import numpy as np
import pickle
import boto3
import io 
import pyarrow

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

    # Read from an S3 bucket
    bucket_name = 'models-prophet'
    key = f'{region}_{data_type}_model.pkl'
    print(f'Forecasting for {region} {data_type}')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    pickle_data = response['Body'].read()
    model = pickle.loads(pickle_data)

    # Create future dataframe and make predictions
    future = model.make_future_dataframe(periods=periods, freq='D')
    forecast = model.predict(future)

    selected_columns = ['ds', 'yhat']

    # Select the desired columns using indexing operator []
    forecast = forecast[selected_columns]

    forecast.rename(columns={'ds':'date_time','yhat':'value'},inplace=True)
    
    # Save the forecast as a CSV file
    print(f'Writing forecast for {region} {data_type} to S3')
    write_to_s3(forecast, 'forecasts-eia', 'prophet/',f'{region}_{data_type}_forecast.parquet')

    return forecast

class IgnoreCloseBytesIO(io.BytesIO):
    def close(self):
        # Ignore close operations
        pass

def write_to_s3(df, bucket_name, key, filename):
    # Convert the DataFrame to Parquet format
    output_stream = IgnoreCloseBytesIO()
    df.to_parquet(output_stream, engine='pyarrow')

    # IMPORTANT: Seek to the start of the stream before reading
    output_stream.seek(0)

    # Write the Parquet data to the S3 bucket
    s3_client.put_object(Body=output_stream.getvalue(), Bucket=bucket_name, Key=key + filename)


# Specify the access keys
access_key_id = ''
secret_access_key = ''
regions = ['CAL', 'CAR', 'CENT', 'FLA', 'MIDA', 'MIDW', 'NE', 'NY', 'SE', 'SW', 'TEX']
data_types = ['demand', 'generation']

# Create an S3 client
s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

# Set the number of periods for which to make predictions
periods = 6 * 30  # Predict the next 6 months, assuming 30 days per month

# Make predictions for each region and data type
for region in regions:
    for data_type in data_types:
        forecast = load_and_predict(region, data_type, periods)