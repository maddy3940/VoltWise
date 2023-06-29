import json
import boto3
import requests
import pandas as pd
import datetime


def fetch_data(url):

    # call API
    r = requests.get(url)
    op = r.json()
    
    # json to data frame
    df = pd.json_normalize(op['response']['data'])

    df = df.pivot(columns="type-name", values='value', index = ['period', 'respondent'] )
    
    df.reset_index(inplace=True)
    
    # organize data as needed
    df.drop(columns=['Day-ahead demand forecast','Total interchange'], inplace=True)
    df[['Date', 'Hour']] = df['period'].str.split('T',expand=True)
    df = df.drop('period', axis = 1)
    df.rename(columns={'respondent':'region'}, inplace=True)
    df = df[['Date','Hour','region','Demand','Net generation']]
    # segregate data based on regions

    final_dict = segregate_to_region(df)

    return final_dict
    
    
def segregate_to_region(df):
    data = {}
    for r in regions:
        data[r] = df[df['region']==r].reset_index().drop(columns=['index'])
        
        if len(data[r])<96:
            temp = data_correction(r,data[r])
            temp.reset_index(inplace=True)
            temp.drop(columns=['index'],inplace=True)
            data[r] = temp
        print(f'Got demand data for region {r} and its count is {len(data[r])}')
    return data

def data_correction(r,df):
    hour = ['{:02d}'.format(i) for i in range(24)]
    uq_date = df['Date'].unique().tolist()
    dfs=[]
    for dt in uq_date:
        uq_hour = df[df['Date']==dt]['Hour'].unique().tolist()
        missing_hr = [x for x in hour if x not in uq_hour]
        if len(missing_hr)!=0:
            new_df=generate_data(df[(df['Date']==dt)],missing_hr,r,dt)

            dfs.append(new_df)
        else:
            dfs.append(df[(df['Date']==dt)])
    print(uq_date)
    dfs = pd.concat(dfs)

    return dfs

def generate_data(df,missing_hr,r,dt):
    
    demand_median = df['Demand'].median()
    net_gen_median = df['Net generation'].median()
    
    new_data=[]
    for hr in missing_hr:
        new_data.append((dt,hr,r,demand_median,net_gen_median))
        
    df_missing = pd.DataFrame(new_data,columns=['Date', 'Hour', 'region','Demand','Net generation'])
    df_new = pd.concat([df,df_missing])
    df_new = df_new.sort_values(['Date','Hour'])
    
    return df_new

def get_new_dates(end_date):
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    start_date =  (end_date - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
  
    return start_date
    
def update_raw_data(new_data):
    for k in regions:
        
        key = f'raw_data/{k}.parquet'
        
        key_daily = f'raw_data_daily/{k}.csv'

        historic_region_data[k]=pd.read_parquet('s3://{}/{}'.format(bucket_name, key),use_pandas_metadata=True)
        
        
        ct_0 = len(historic_region_data[k])
        new_data[k]['new_hash_time'] = new_data[k]['Date']+new_data[k]['Hour']
        historic_region_data[k]['new_hash_time'] = historic_region_data[k]['Date']+historic_region_data[k]['Hour'].astype(str)

        # Set the 'id' column as the index of both DataFrames
        new_data[k].set_index('new_hash_time', inplace=True)
        historic_region_data[k].set_index('new_hash_time', inplace=True)

        # Update rows in df that have matching indexes in new_data
        historic_region_data[k].update(new_data[k])

        # Append rows to df that have non-matching indexes in new_data
        historic_region_data[k] = pd.concat([historic_region_data[k], new_data[k].loc[~new_data[k].index.isin(historic_region_data[k].index)]])

        # Reset the index of the resulting DataFrame
        historic_region_data[k].reset_index(inplace=True)

        historic_region_data[k].drop(columns=['new_hash_time'],inplace=True)

        ct_1 = len(historic_region_data[k])
        
        
        historic_region_data[k].to_parquet('s3://{}/{}'.format(bucket_name, key))  

        print(f'Added {ct_1-ct_0} hourly new records for region {k} to raw_data folder') 

        # Converting hourly data to daily data and saving it to different bucket

        historic_region_data[k].drop(columns=['Hour','region'],inplace=True)

        historic_region_data[k] = historic_region_data[k].groupby(historic_region_data[k]['Date'])[['Demand', 'Net generation']].sum().reset_index()

        historic_region_data[k]['Date'] = pd.to_datetime(historic_region_data[k]['Date'])

        # Save to new bucket containing daily data
        historic_region_data[k].to_csv('s3://{}/{}'.format(bucket_name, key_daily),index=False)

        print(f'Added {ct_1-ct_0} daily new records for region {k} to raw_data_daily folder')

end_date = (datetime.datetime.now()- datetime.timedelta(days=1)).strftime('%Y-%m-%d')
start_date = get_new_dates(end_date)
regions = ['CAL', 'CAR', 'CENT', 'FLA', 'MIDA', 'MIDW', 'NE', 'NY', 'SE', 'SW', 'TEX']
historic_region_data={}


# Initialize a Boto3 S3 client
s3 = boto3.client('s3')

bucket_name = 'ecc-eia-data'


def lambda_handler(event, context):

   
    url= f"https://api.eia.gov/v2/electricity/rto/region-data/data/?\
frequency=hourly&data[0]=value&facets[respondent][]=CAL\
&facets[respondent][]=CAR&facets[respondent][]=CENT&facets[respondent][]=FLA&facets[respondent][]=MIDA\
&facets[respondent][]=MIDW&facets[respondent][]=NE&facets[respondent][]=NY&facets[respondent][]=SE&\
facets[respondent][]=SW&facets[respondent][]=TEX&\
start={start_date}T00&end={end_date}T23\
&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000\
&api_key=2Ztw7IK10RqAv0oag9T2o2FOV8YZgRpapfTEvhwH"
    
    print(url)
    
    print(f'Now new fetching data from {start_date} to {end_date}')
    new_data=fetch_data(url)
    
    print(f'Got new demand data from date {start_date} to {end_date} now checking for cdc with existing data for these dates')
    
    update_raw_data(new_data)
    
    print(f'Updated new data from date {start_date} to {end_date} for all regions')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
