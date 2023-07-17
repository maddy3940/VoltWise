import pandas as pd
import json
import boto3
from io import BytesIO

# Pushed via CI/CD - shubham
# global object–
s3 = boto3.client('s3')


def get_historic_data_from_s3(region):
    print("inside def get_historic_data_from_s3(region):")
    bucket_name = 'ecc-eia-data'
    key = f'raw_data/daily-data/{region}.parquet'
    
    buffer = BytesIO()
    s3.download_fileobj(bucket_name, key, buffer)

    # Reset buffer position to the beginning
    buffer.seek(0)

    # Load the Parquet file from the buffer into a DataFrame
    df = pd.read_parquet(buffer, engine='pyarrow')
    

    # df[df.columns[0]] = pd.to_datetime(df['Date'])
    
    return df


def aggregate_data_by_month(df, year):
    print("inside aggregate_data_by_month")
    # Filter the data frame for the given year
    df_year = df[df['Date'].dt.year == year]
    
    # Extract month from the 'Date' column
    df_year['Month'] = df_year['Date'].dt.month
    
    # Aggregate demand and net generation by month
    aggregated_data = df_year.groupby('Month').agg({'Demand': 'sum', 'Net generation': 'sum'}).reset_index()
    
    # total demand and net generation for the year
    total_demand_given_year = df_year['Demand'].sum()
    total_generation_given_year = df_year['Net generation'].sum()
    
    #total import
    total_energy_import = df_year['Import'].sum()
    
    #total import
    total_energy_export = df_year['Export'].sum()
    
    
    return aggregated_data, total_demand_given_year, total_generation_given_year, total_energy_import, total_energy_export


def average_demand_net_generation(df, year):
    print("inside average_demand_net_generation")
    df_year = df[df['Date'].dt.year == year]
    df_year['Month'] = df_year['Date'].dt.month
    
    
    averaged_data = df_year.groupby('Month').agg({'Demand': 'mean', 'Net generation': 'mean'}).reset_index()
    
    return averaged_data
    

def calculate_generation_demand_ratio(aggregated_data):
    print("inside calculate_generation_demand_ratio")
    # Calculate generation-to-demand ratio
    aggregated_data['Generation_to_demand_ratio'] = aggregated_data['Aggregate_Generation'] / aggregated_data['Aggregate_Demand']
    
    return aggregated_data

def percent_change_by_month(df):
    print("inside percent_change_by_month")
    # Calculate percent change for demand and net generation
    df['Demand_pct_change'] = df['Demand'].pct_change(periods=1) * 100
    df['Net_generation_pct_change'] = df['Net generation'].pct_change(periods=1) * 100
    
    return df





def percent_change_aggregated(df, year):
    print("inside percent_change_aggregated")
    
    # aggregated data for given year
    aggregated_data_given_year, total_demand_given_year, total_generation_given_year, total_energy_import, total_energy_export = aggregate_data_by_month(df, year)
    
    # percent change for each month of given year
    aggregate_percent_change_given_year = percent_change_by_month(aggregated_data_given_year)
    
    if year == 2015:
        return aggregate_percent_change_given_year.fillna(0), total_demand_given_year, total_generation_given_year , 0, 0, total_energy_import, total_energy_export
    
    # Filter the data frame for the previous year
    df_prev_year = df[df['Date'].dt.year == year - 1]
    
    
    # Aggregate demand and net generation for December of the previous year
    dec_prev_year_demand = df_prev_year[df_prev_year['Date'].dt.month == 12]['Demand'].sum()
    dec_prev_year_generation = df_prev_year[df_prev_year['Date'].dt.month == 12]['Net generation'].sum()
    
    #Demand and generation for January of given year
    jan_given_year_demand = aggregate_percent_change_given_year.loc[aggregate_percent_change_given_year['Month'] == 1, 'Demand']
    jan_given_year_generation = aggregate_percent_change_given_year.loc[aggregate_percent_change_given_year['Month'] == 1, 'Net generation']
    
    # Calculate percent change for January of the given year compared to December of the previous year
    aggregate_percent_change_given_year.loc[aggregate_percent_change_given_year['Month'] == 1, 'Demand_pct_change'] = ( jan_given_year_demand - dec_prev_year_demand) / dec_prev_year_demand * 100
    aggregate_percent_change_given_year.loc[aggregate_percent_change_given_year['Month'] == 1, 'Net_generation_pct_change'] = (jan_given_year_generation - dec_prev_year_generation) / dec_prev_year_generation * 100
    
     # Aggregate demand and net generation for the previous year
    total_demand_prev_year = df_prev_year['Demand'].sum()
    total_generation_prev_year = df_prev_year['Net generation'].sum()
   
    # Calculate percent change in demand and net generation between the two years
    percent_change_demand_given_year = (total_demand_given_year - total_demand_prev_year) / total_demand_prev_year  * 100
    percent_change_generation_given_year = (total_generation_given_year - total_generation_prev_year) / total_generation_prev_year *100
    
    return aggregate_percent_change_given_year, total_demand_given_year, total_generation_given_year, percent_change_demand_given_year, percent_change_generation_given_year, total_energy_import, total_energy_export

def aggregate_data_by_quarter(df):
    print("inside aggregate_data_by_quarter")
    # Group the data by quarter and aggregate the demand and net generation
    aggregated_data = df.groupby(df['Month'].apply(lambda x: (x-1)//3 + 1)).agg({'Demand': 'sum', 'Net generation': 'sum'}).reset_index()
    aggregated_data.rename(columns={"Month": "Quarter"}, inplace=True)
    
    return aggregated_data


def get_data_list_by_month(df, key):
    print("inside get_data_list_by_month")
    aggregate_df = df[key].tolist()
    prefix_padding_index = df['Month'][0]
    prefix_padding_list = [0 for i in range(1,prefix_padding_index)]
    postfix_padding_index = df['Month'][len(df['Month']) - 1]
    postfix_padding_list = [0 for i in range(postfix_padding_index, 12)]
    aggregate_data_list_by_month = prefix_padding_list + aggregate_df + postfix_padding_list
    return aggregate_data_list_by_month

def get_data_list_by_quarter(df, key):
    print("inside get_data_list_by_quarter")
    aggregate_df = df[key].tolist()
    prefix_padding_index = df['Quarter'][0]
    prefix_padding_list = [0 for i in range(1,prefix_padding_index)]
    postfix_padding_index = df['Quarter'][len(df['Quarter']) - 1]
    postfix_padding_list = [0 for i in range(postfix_padding_index, 4)]
    aggregate_data_list_by_quarter = prefix_padding_list + aggregate_df + postfix_padding_list
    return aggregate_data_list_by_quarter






def lambda_handler(event, context):
    
    
    response = {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET',
            'Access-Control-Allow-Headers': 'Content-Type'
        }
    }
    
    
    if event['httpMethod'] == 'OPTIONS':
        print("options request")
        return response
        
    
    region = event['queryStringParameters']['region']
    year = int(event['queryStringParameters']['year'])
    
    
    
    # get historic data from 2015 to current year
    historic_df = get_historic_data_from_s3(region)
    
    print("region", region)
    print("year", year)
    
    
    aggregate_percent_change_given_year, total_demand_given_year, total_generation_given_year, percent_change_demand_given_year, percent_change_generation_given_year, total_energy_import, total_energy_export = percent_change_aggregated(historic_df, year)
    
    print("total_demand_current_year: ",total_demand_given_year)
    print("total_demand_current_year_pc: ",percent_change_demand_given_year)
    
    total_demand_current_month = aggregate_percent_change_given_year['Demand'][len(aggregate_percent_change_given_year['Demand']) - 1]
    total_demand_current_month_pc = aggregate_percent_change_given_year['Demand_pct_change'][len(aggregate_percent_change_given_year['Demand_pct_change']) - 1]
    
    total_generation_current_month = aggregate_percent_change_given_year['Net generation'][len(aggregate_percent_change_given_year['Net generation']) - 1]
    total_generation_current_month_pc = aggregate_percent_change_given_year['Net_generation_pct_change'][len(aggregate_percent_change_given_year['Net_generation_pct_change']) - 1]
    
    average_demand_generation = average_demand_net_generation(historic_df, year)
    
    aggregate_data_quarter = aggregate_data_by_quarter(aggregate_percent_change_given_year)
    
    
    monthly_data_df = aggregate_percent_change_given_year.rename(columns={"Demand": "Aggregate_Demand", "Net generation": "Aggregate_Generation"})
    
    monthly_data_df['Average_Demand'] = average_demand_generation['Demand']
    monthly_data_df['Average_Generation'] = average_demand_generation['Net generation']
    
    monthly_data_df = calculate_generation_demand_ratio(monthly_data_df)
    
    
    aggregate_demand_list_by_month = get_data_list_by_month(monthly_data_df, 'Aggregate_Demand')
    aggregate_generation_list_by_month = get_data_list_by_month(monthly_data_df, 'Aggregate_Generation')
    average_demand_list_by_month = get_data_list_by_month(monthly_data_df, 'Average_Demand')
    average_generation_list_by_month = get_data_list_by_month(monthly_data_df, 'Average_Generation')
    
    table_records = monthly_data_df.to_dict('records')
    
    
    aggregate_demand_list_by_quarter = get_data_list_by_quarter(aggregate_data_quarter, 'Demand')
    
    aggregate_generation_list_by_quarter = get_data_list_by_quarter(aggregate_data_quarter, 'Net generation')
    
    
    
    monthly_graph_data = {
        "aggregateDemand": aggregate_demand_list_by_month,
        "aggregateGeneration": aggregate_generation_list_by_month,
        "averageDemand": average_demand_list_by_month,
        "averageGeneration": average_generation_list_by_month
    }
    
    aggregateGenerationGivenYear = {
        "value": total_generation_given_year,
        "percentChange": percent_change_generation_given_year
    }
    
    aggregateDemandGivenYear = {
        "value": total_demand_given_year,
        "percentChange": percent_change_demand_given_year
    }
    
    
    aggregateDemandCurrentMonth = {
        "value": total_demand_current_month,
        "percentChange": total_demand_current_month_pc
    }
    
    aggregateGenerationCurrentMonth = {
        "value": total_generation_current_month,
        "percentChange": total_generation_current_month_pc
    }
    
    quarterly_graph_data = {
        "aggregateDemand": aggregate_demand_list_by_quarter,
        "aggregateGeneration": aggregate_generation_list_by_quarter
    }
    
    
    
    
    
    
    
    
    
    
    result = {
        "region": region,
        "year": year,
        "monthlyGraph" : monthly_graph_data,
        "quarterlyGraph" : quarterly_graph_data,
        "tableData" : table_records,
        "aggregateDemandGivenYear" : aggregateDemandGivenYear,
        "aggregateGenerationGivenYear": aggregateGenerationGivenYear,
        "aggregateDemandCurrentMonth" : aggregateDemandCurrentMonth,
        "aggregateGenerationCurrentMonth": aggregateGenerationCurrentMonth,
        "totalEnergyImport": total_energy_import, 
        "totalEnergyExport": total_energy_export
        
        
    }
    
 
    
    
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(result)
    }
