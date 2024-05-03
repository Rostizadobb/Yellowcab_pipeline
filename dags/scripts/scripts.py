from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import scripts.utils.FileUtils as utils
import logging
blob_service_client = BlobServiceClient.from_connection_string(utils.azure_conn)
logger = logging.getLogger(__name__)

def file_download():
    logger.info("Downloading files")
    try:
        container_client = blob_service_client.get_container_client('cabfiles')
        blob_list = container_client.list_blobs()        
        for filename in blob_list:
            get_file(filename.name)
            container_client.delete_blob(filename.name)
        logger.info("download completed")
        return None
    except Exception as e:
        logger.info("An error occurred, file not downloaded:", e)
        return None

def get_file(filename):
    logger.info("file detected on blob: ", filename)
    try:
        blob_client = blob_service_client.get_blob_client(
            container='cabfiles', blob=filename)
        with open(file=os.path.join(utils.source_path, filename), mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())
        return None
    except Exception as e:
        print(e)
        return None

def load_files():
    logger.info("starting upload to DB")
    try:
        for filename in os.listdir(utils.source_path):
            if filename.endswith(".parquet"):
                file_upload(filename)
        logger.info("DB upload completed")
        return None
        
    except Exception as e:
        print(e)
        return None

def file_upload(filename):
    logger.info("uploading file: ", filename)
    filepath = os.path.join(utils.source_path, filename)
    parquet_file = pq.ParquetFile(filepath)
    for i in parquet_file.iter_batches(batch_size=10000,columns=['tpep_pickup_datetime','tpep_dropoff_datetime']):
        #NOTE: We can use a dask dataframe as well to scale to distributed systems! 
        df = i.to_pandas()
        upload_to_db(df)
    os.remove(filepath)
    return None

def upload_to_db(df):
    engine = create_engine(utils.sql_conn)
    with engine.connect() as conn:
        df.to_sql(
            name="yellowcab_data", 
            con=conn,  
            if_exists="append", 
            index=False 
        )
    return None

def calculation_file():
    logger.info("Start average calculation")
    engine = create_engine(utils.sql_conn)
    df = pd.read_sql_query (utils.sql_query,con=engine)
    logger.info("calculation completed")
    average_duration = df['average_duration'].iloc[0]
    first_timestamp = df['max_pickup_datetime'].iloc[0]
    last_timestamp = df['min_pickup_datetime'].iloc[0]
    response_filepath, filename = generate_file(average_duration, first_timestamp, last_timestamp)
    send_file_to_azure(response_filepath, filename)
    return None

def generate_file(average_duration, first_timestamp, last_timestamp):
    logger.info("generating file")
    filename = str(first_timestamp.year) + '-' + str(first_timestamp.month) + '.txt'
    filepath = os.path.join(utils.source_path, filename)
    av_trip = "Average trip duration for the month: " + str(average_duration)
    first_time = "First timestamp: " + str(first_timestamp)
    last_time = "Last timestamp: " + str(last_timestamp)
    with open(filepath, "w") as f:
        f.write(av_trip)
        f.write(first_time)
        f.write(last_time)
    return filepath, filename

def send_file_to_azure(filepath, filename):
    logger.info("uploading file to blob storage")
    container = "results"
    blob_client = blob_service_client.get_blob_client(container=container, blob=filename)
    with open(filepath, "rb") as data:
        blob_client.upload_blob(data)
    os.remove(filepath)
    logger.info("Upload completed")
    return None
