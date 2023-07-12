import pandas as pd
import os
from prophet import Prophet
import numpy as np
import pickle
import boto3
import io
from io import BytesIO

def load_data():
    # Read from an S3 bucket
    bucket_name = 'ecc-eia-data'
    key = 'raw_data/daily-data/'

    response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=key)
    
    # Extract the file names
    file_names = [os.path.basename(obj['Key']) for obj in response['Contents']]

    region_data = {}
    for filename in file_names:
        if filename.endswith('.parquet'):
            # Extract the region name from the file name
            region = filename.split('.')[0]
            df = read_from_s3(bucket_name,key+f'{region}.parquet')
            # Store the data in the dictionary with the region name as the key
            region_data[region] = df

    return region_data

def preprocess_data(data, region):
    # Filter data for the specified region
    region_data = data[region]

    # region_data['Date'] = pd.to_datetime(region_data['Date'], format='%Y-%m-%d')
    region_data['Timestamp'] = region_data['Date']

    # Prepare separate datasets for demand and net generation
    demand_data = region_data[['Timestamp', 'Demand']]
    demand_data.columns = ['ds', 'y']

    # Drop rows with missing values
    demand_data = demand_data.dropna()

    generation_data = region_data[['Timestamp', 'Net generation']]
    generation_data.columns = ['ds', 'y']

    # Drop rows with missing values
    generation_data = generation_data.dropna()

    return demand_data, generation_data

# Read function
def read_from_s3(bucket_name,key):
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read()
    df = pd.read_parquet(io.BytesIO(data))
    return df

def train_prophet_model(region, data_type, data):
    """
    Train a Prophet model for a given region and data type (demand or net generation).

    Args:
    region (str): The region code.
    data_type (str): 'Demand' or 'Net generation'.
    data (pd.DataFrame): The preprocessed data for the region, with columns 'ds' and 'y'.

    Returns:
    None
    """
    # Create and fit the Prophet model
    model = Prophet()
    model.fit(data)
    model_bytes  = pickle.dumps(model)

    # Specify the bucket name and object key
    bucket_name = 'models-prophet'
    object_key = f'{region}_{data_type}_model.pkl'

    # Write the pickle file to S3 bucket
    s3_client.put_object(Body=model_bytes, Bucket=bucket_name, Key=object_key)

    # Print the confirmation message
    print(f"Prophet model for {region} {data_type} has been stored in S3 bucket.")


# Specify the access keys
access_key_id = os.environ.get("access_key_id ")
secret_access_key = os.environ.get("secret_access_key")

print("Access id",access_key_id)
print("Keys",secret_access_key)
# Create an S3 client
s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

region_data= load_data()            
regions = list(region_data.keys())
print("Loaded Data")

# Preprocess data for each region and store it in a dictionary
preprocessed_data = {}
for region in regions:
    demand_data, generation_data = preprocess_data(region_data.copy(), region)
    preprocessed_data[region] = {'demand': demand_data, 'generation': generation_data}

print("Processed Raw Data into Demand and Generation for each region")

data_types = ['demand', 'generation']

for region in regions:
    for data_type in data_types:
        region_preprocessed_data = preprocessed_data[region][data_type]
        train_prophet_model(region, data_type, region_preprocessed_data)

print("Trained Prophet Models for each region and data type")

