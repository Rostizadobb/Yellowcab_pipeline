from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=azureyellowcab;AccountKey=r/A3SvJIIzCnK+7RXvM+Pa8hrwE8MFAt6jwAwibQlap03V5oRbyD52911/AdxQN91srwByCo/LnC+ASt03RZLg==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
SOURCE_FILE_PATH = '/opt/airflow/dags/files'

def file_download():
    try:
        container_client = blob_service_client.get_container_client('cabfiles')
        blob_list = container_client.list_blobs()        
        for filename in blob_list:
            get_file(filename.name)
            container_client.delete_blob(filename.name)
        return None
    except Exception as e:
        print("An error occurred:", e)
        return None

def get_file(filename):
    try:
        blob_client = blob_service_client.get_blob_client(
            container='cabfiles', blob=filename)
        with open(file=os.path.join(SOURCE_FILE_PATH, filename), mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())
        return None
    except Exception as e:
        print(e)
        return None

def load_files():
    try:
        for filename in os.listdir(SOURCE_FILE_PATH):
            print('enter for loop')
            if filename.endswith(".parquet"):
                print('enter if loop')
                file_upload(filename)
        return None
    except Exception as e:
        print(e)
        return None

def file_upload(filename):
    print('enter file_upload function')
    filepath = os.path.join(SOURCE_FILE_PATH, filename)
    print('read filepath')
    parquet_file = pq.ParquetFile(filepath)
    for i in parquet_file.iter_batches(batch_size=10000,columns=['tpep_pickup_datetime','tpep_dropoff_datetime']):
        df = i.to_pandas()
        upload_to_db(df)
    print('upload compleated')
    return None

def upload_to_db(df):
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/postgres')
    with engine.connect() as conn:
        df.to_sql(
            name="yellowcab_data", 
            con=conn,  
            if_exists="append", 
            index=False 
        )
        print('enden upload_to_db')
    return None

def calculation_file():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/postgres')
    df = pd.read_sql_query("WITH last_date AS (SELECT MAX(tpep_pickup_datetime) AS last_start_time FROM yellowcab_data) SELECT tpep_pickup_datetime, tpep_dropoff_datetime FROM yellowcab_data CROSS JOIN last_date WHERE tpep_pickup_datetime >= last_start_time - INTERVAL '45 days';",con=engine)
    df['duration'] = df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
    average_duration = df['duration'].mean()
    first_timestamp = df['tpep_pickup_datetime'].min()
    last_timestamp = df['tpep_pickup_datetime'].max()
    print("Average trip duration for the month: ", average_duration)
    print("First timestamp:", first_timestamp)
    print("Last timestamp:", last_timestamp)
    response_filepath, filename = generate_file(average_duration, first_timestamp, last_timestamp)
    send_file_to_azure(response_filepath, filename)
    return None

def generate_file(average_duration, first_timestamp, last_timestamp):
    filename = str(first_timestamp.year) + '-' + str(first_timestamp.month) + '.txt'
    filepath = os.path.join(SOURCE_FILE_PATH, filename)
    av_trip = "Average trip duration for the month: " + str(average_duration)
    first_time = "First timestamp: " + str(first_timestamp)
    last_time = "Last timestamp: " + str(last_timestamp)
    with open(filepath, "w") as f:
        f.write(av_trip)
        f.write(first_time)
        f.write(last_time)
    return filepath, filename

def send_file_to_azure(filepath, filename):
    container = "results"
    blob_client = blob_service_client.get_blob_client(container=container, blob=filename)
    with open(filepath, "rb") as data:
        blob_client.upload_blob(data)
    os.remove(filepath)
    return None