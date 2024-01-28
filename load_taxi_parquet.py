import pandas as pd
from dotenv import load_dotenv
import os
import duckdb
import fsspec 
import s3fs

load_dotenv(override=True)
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

df = pd.read_parquet('s3://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet', 
                     storage_options = {
                         "key": aws_access_key_id,
                         "secret": aws_secret_access_key
                    })

conn = duckdb.connect('duckdb/nyctaxi.duckdb')

conn.execute(f"CREATE OR REPLACE TABLE yellow_trip_data AS SELECT * FROM 's3://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet' (storage_options => json_object('key', {aws_access_key_id}, 'secret', {aws_secret_access_key}))")

conn.close()


