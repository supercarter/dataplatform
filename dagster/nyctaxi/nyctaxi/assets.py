import pandas as pd
from dotenv import load_dotenv
import os
import duckdb
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue, AssetIn
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource, dbt_assets

from .constants import dbt_manifest_path

load_dotenv(override=True)
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

data_lake_location = '../../data_lake'

@asset
def get_next_month(duckdb: DuckDBResource) -> dict:
    with duckdb.get_connection() as conn:
        # its not straight foward to just add 1 to the next month for a variety of reasons
        # this query is a simple way to get the next month
        query = """
                    SELECT    date_part('year', next_month) as yyyy
		                    , date_part( 'month', next_month) as mm 
                    FROM (
		                    SELECT date_add(make_date(cast(yyyy as bigint), cast(mm as bigint), cast(01 as bigint)), INTERVAL 1 MONTH) as next_month 
		                    FROM stage.yellow_trip 
		                    ORDER BY yyyy desc, mm desc 
                            )
                """
        result = conn.execute(query).fetchone()
        return {'year': str(int(result[0])), 'month': f"{int(result[1]):02d}"}

@asset(ins={"last_month": AssetIn('get_next_month')})
def read_nyc_taxi_raw(last_month) -> MaterializeResult:
    
    year = last_month['year'] # '2023'
    month = last_month['month'] # '01'
    s3_location = f's3://nyc-tlc/trip data/yellow_tripdata_{year}-{month}.parquet'
    filename = s3_location.split('/')[-1]
    partition_path = f'yyyy={year}/mm={month}'

    df = pd.read_parquet(s3_location, 
                         storage_options = {
                             "key": aws_access_key_id,
                             "secret": aws_secret_access_key
                        })
    
    df['file'] = filename

    if not os.path.exists(f'{data_lake_location}/{partition_path}'):
        os.makedirs(f'{data_lake_location}/{partition_path}')

    df.to_parquet(f'{data_lake_location}/{partition_path}/{filename}')

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(deps=[read_nyc_taxi_raw])
def parquet_to_duckdb(context: AssetExecutionContext, duckdb: DuckDBResource) -> None: 


    yellow_trip_data_table = 'stage.yellow_trip'
    context.log.info('Connecting to duckdb...')
    with duckdb.get_connection() as conn: 
        context.log.info('Loading parquet to duckdb table...')
        conn.execute('CREATE SCHEMA IF NOT EXISTS stage')
        conn.execute(f"CREATE OR REPLACE TABLE {yellow_trip_data_table} AS SELECT * FROM read_parquet('{data_lake_location}/yyyy=*/mm=*/*.parquet')")
        context.log.info('Successful.')

"""
@dbt_assets(manifest = dbt_manifest_path)
def taxi_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
"""
