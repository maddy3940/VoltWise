import json
import pandas as pd
import boto3
import io

# global object
s3 = boto3.client('s3')

# Pushed using CI/CD
def get_forecast_data_from_s3(model, region, category, max_date):
    # Set the S3 bucket and file name
    bucket_name = 'forecasts-eia'
    
    if model == "arima":
        key = f'{model}/{region}_{category}_forecast.csv'
         # Read the CSV file from S3 into a Pandas DataFrame
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(obj['Body'])
        df['date_time'] = pd.to_datetime(df['date_time'])
    else:
        key = f'{model}/{region}_{category}_forecast.parquet'
        buffer = io.BytesIO()
        s3.download_fileobj(bucket_name, key, buffer)
        # Reset buffer position to the beginning
        buffer.seek(0)
        # Load the Parquet file from the buffer into a DataFrame
        df = pd.read_parquet(buffer, engine='pyarrow')
    
    df = df[df['date_time']>max_date]
    
    return df
    
def get_historic_data_from_s3(region):
    bucket_name = 'ecc-eia-data'
    key = f'historic-graph-data/{region}.parquet'
    
    buffer = io.BytesIO()
    s3.download_fileobj(bucket_name, key, buffer)

    # Reset buffer position to the beginning
    buffer.seek(0)

    # Load the Parquet file from the buffer into a DataFrame
    df = pd.read_parquet(buffer, engine='pyarrow')
    
    max_date = max(df['Date'])
    
    return df,max_date
    
def aggregate_data(df, frequency):
    
    df.loc[:, 'date_time'] = pd.to_datetime(df['date_time'])
    
    # Aggregate by frequency and reset the index
    df = df.groupby(pd.Grouper(key='date_time', freq=frequency)).sum().reset_index()
    df['value'] = df['value'].round(2)
    
    return df
        
def trim_data(df, time):
    trimmed_df = pd.DataFrame()
    if time == "1-month":
         # Trim the DataFrame to the first month
        trimmed_df = df.loc[df['date_time'] < df['date_time'].min() + pd.DateOffset(months=1)].copy()
    
    elif time == "3-months":
        # Trim the DataFrame to the first month
        trimmed_df = df.loc[df['date_time'] < df['date_time'].min() + pd.DateOffset(months=3)].copy()
    
    elif time == "6-months":
        # Trim the DataFrame to the first month
        trimmed_df = df.loc[df['date_time'] < df['date_time'].min() + pd.DateOffset(months=6)].copy()
    
    elif time == "1-year":
        # Trim the DataFrame to the first month
        # trimmed_df = df.loc[df['date_time'] < df['date_time'].min() + pd.DateOffset(years=1)].copy()
        trimmed_df = df
    
    return trimmed_df
    
def lambda_handler(event, context):
    
    response = {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET',
            'Access-Control-Allow-Headers': 'Content-Type'
        }
    }
    
    # print(event)
    
    if event['httpMethod'] == 'OPTIONS':
        print("options request")
        return response
    
    model = event['queryStringParameters']['model']
    region = event['queryStringParameters']['region']
    time = event['queryStringParameters']['time']
    frequency = event['queryStringParameters']['frequency']
    
    print("model:", model)
    print("region:", region)
    print("time:", time)
    print("frequency:", frequency)
    
    
    # Retrieving and processing Historic data from S3
    historic_df, max_date = get_historic_data_from_s3(region)
    
    # Creating two different dataframes for Historic demand and generation data
    historic_df['date_time'] = historic_df.apply(lambda row: pd.to_datetime(row['Date']), axis=1)

    historic_demand_df = historic_df[['date_time', 'Demand']]
    historic_demand_df.rename(columns = {'Demand': 'value'}, inplace=True)
    
    historic_generation_df = historic_df[['date_time', 'Net generation']]
    historic_generation_df.rename(columns = {'Net generation': 'value'}, inplace=True)
    
    # Aggregate demand and generation data
    aggregate_historic_demand_df = aggregate_data(historic_demand_df, frequency)
    aggregate_historic_generation_df = aggregate_data(historic_generation_df, frequency)
    
    
    
    # # Convert date_time to epoch time in milliseconds for the trimmed DataFrames
    aggregate_historic_demand_df.loc[:, 'epoch_time_ms'] = (aggregate_historic_demand_df['date_time'].astype(int) / 10**6).astype(int)
    aggregate_historic_generation_df.loc[:, 'epoch_time_ms'] = (aggregate_historic_generation_df['date_time'].astype(int) / 10**6).astype(int)
    
    # Convert DataFrames to list of lists with epoch time as integers
    historic_demand_data = aggregate_historic_demand_df[['epoch_time_ms', 'value']].astype({'epoch_time_ms': int}).values.tolist()
    historic_generation_data = aggregate_historic_generation_df[['epoch_time_ms', 'value']].astype({'epoch_time_ms': int}).values.tolist()
    
    
    
    # Retrieving and processing Forecast data from S3
    demand_df = get_forecast_data_from_s3(model, region, "demand", max_date)
    category = "netgen" if model == "arima" else "generation"
    generation_df = get_forecast_data_from_s3(model, region, category, max_date)
    
  
    # Aggregate demand and generation data
    aggregate_demand_df = aggregate_data(demand_df, frequency)
    aggregate_generation_df = aggregate_data(generation_df, frequency)
    
    # Trim demand and generation data
    trimmed_demand_df = trim_data(aggregate_demand_df, time)
    trimmed_generation_df = trim_data(aggregate_generation_df, time)

    # Convert date_time to epoch time in milliseconds for the trimmed DataFrames
    trimmed_demand_df.loc[:, 'epoch_time_ms'] = (trimmed_demand_df['date_time'].astype(int) / 10**6).astype(int) 
    trimmed_generation_df.loc[:, 'epoch_time_ms'] = (trimmed_generation_df['date_time'].astype(int) / 10**6).astype(int)

    # Convert the trimmed DataFrames to list of lists with epoch time as integers
    forecast_demand_data = trimmed_demand_df[['epoch_time_ms', 'value']].astype({'epoch_time_ms': int}).values.tolist()
    forecast_generation_data = trimmed_generation_df[['epoch_time_ms', 'value']].astype({'epoch_time_ms': int}).values.tolist()
    
    
    
    
    
    
    # result = {
    #     'forecast_demand_data':forecast_demand_data,
    #     'forecast_generation_data':forecast_generation_data,
    #     'historic_demand_data':historic_demand_data,
    #     'historic_generation_data':historic_generation_data
    # }
    # result = {
    #     # 'forecast_demand_data':forecast_demand_data,
    #     # 'forecast_generation_data':forecast_generation_data,
    #     'historic_demand_data':historic_demand_data,
    #     'historic_generation_data':historic_generation_data
    # }
    
    
    
    result = {
        'model' : model,
        'region' : region,
        'time' : time,
        'frequency' : frequency,
        'historic_demand_data': historic_demand_data,
        'historic_generation_data': historic_generation_data,
        'forecast_demand_data': forecast_demand_data,
        'forecast_generation_data': forecast_generation_data
        
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(result)
    }
