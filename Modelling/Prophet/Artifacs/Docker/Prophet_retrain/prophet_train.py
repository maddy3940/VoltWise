import pandas as pd
import os
from prophet import Prophet
import numpy as np
import pickle
import boto3

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

def load_data():
    directory= "/workspace/VoltWise/Data_Ingestion/daily_data/"

    region_data = {}
    for filename in os.listdir(directory):
        if filename.endswith('.parquet'):
            # Extract the region name from the file name
            region = filename.split('.')[0]

            # Read the contents of the file into a variable
            filepath = os.path.join(directory, filename)
            df = pd.read_parquet(filepath)
            
            # Store the data in the dictionary with the region name as the key
            region_data[region] = df
    
    return region_data



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

    # Save the model as a pickle file
    model_directory = f"/workspace/VoltWise/Modelling/Prophet/Pickle_files"
    if not os.path.exists(model_directory):
        os.makedirs(model_directory)

    with open(f"{model_directory}/{region}_{data_type}_prophet_model.pkl", "wb") as file:
        pickle.dump(model, file)

region_data= load_data()            
regions = list(region_data.keys())
data_types = ['demand', 'generation']


# Preprocess data for each region and store it in a dictionary
preprocessed_data = {}
for region in regions:
    demand_data, generation_data = preprocess_data(region_data.copy(), region)
    preprocessed_data[region] = {'demand': demand_data, 'generation': generation_data}
    demand_data.to_csv(f'/workspace/VoltWise/Data_Ingestion/preprocessed_daily_data/{region}_demand.parquet', index=False)
    generation_data.to_csv(f'/workspace/VoltWise/Data_Ingestion/preprocessed_daily_data/{region}_generation.parquet', index=False)

data_types = ['demand', 'generation']

for region in regions:
    for data_type in data_types:
        region_preprocessed_data = preprocessed_data[region][data_type]
        train_prophet_model(region, data_type, region_preprocessed_data)