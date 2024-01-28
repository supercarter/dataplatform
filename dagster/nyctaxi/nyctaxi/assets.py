import pandas as pd
from dotenv import load_dotenv
import os
import duckdb
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue

load_dotenv(override=True)
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

@asset
def read_nyc_taxi_raw() -> MaterializeResult:
    df = pd.read_parquet('s3://nyc-tlc/trip data/yellow_tripdata_2023-03.parquet', 
                         storage_options = {
                             "key": aws_access_key_id,
                             "secret": aws_secret_access_key
                        })
    df.to_parquet('../../data/TAXI.parquet')

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(deps=[read_nyc_taxi_raw])
def parquet_to_duckdb(context: AssetExecutionContext) -> MaterializeResult: 

    context.log.info('Connecting to duckdb...')
    conn = duckdb.connect('../../duckdb/nyctaxi.duckdb')
    context.log.info('Connected.')

    context.log.info('Loading parquet to duckdb table...')
    conn.execute(f"CREATE OR REPLACE TABLE yellow_trip_data AS SELECT * FROM read_parquet('../../data/TAXI.parquet')")
    context.log.info('Successful.')
    conn.close()
