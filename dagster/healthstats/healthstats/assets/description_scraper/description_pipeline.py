from .dict_encoding_scraper import NHANES_Descriptions_Downloader
from dagster import asset, MaterializeResult, AssetExecutionContext
from dagster_duckdb import DuckDBResource
import pandas as pd
import os

data_lake_location = '/Users/riley/dataplatform/data_lake'

nhanes_year = 2017
partition_path = f'NHANES_{nhanes_year}-{nhanes_year+1}'
loc = f'{data_lake_location}/{partition_path}'


@asset
def read_nhanes_descriptions_and_encodings(context: AssetExecutionContext) -> MaterializeResult:

    os.makedirs(loc, exist_ok=True)

    scraper = NHANES_Descriptions_Downloader(BASE_YEAR=nhanes_year, target=loc)
    datatypes = scraper.return_datatypes()
    # Scrape data
    for datatype in datatypes:
        context.log.info(f'Extracting {datatype} descriptions and encodings...')
        scraper.extract_datatype(datatype)
    # Reset indices for both descriptions and encodings
    scraper.descDf.reset_index(inplace=True)
    scraper.encDf.reset_index(inplace=True)
    # Save files
    scraper.descDf.to_parquet(loc + '/descriptions.parquet')
    scraper.encDf.to_parquet(loc + '/encodings.parquet')
    context.log.info(f'Finished and saved descriptions file and encodings file as parquets.')
            
    return MaterializeResult(
        metadata={
        }
    )

@asset(deps=[read_nhanes_descriptions_and_encodings])
def desc_parquet_to_duckdb(context: AssetExecutionContext, duckdb: DuckDBResource) -> None: 

    nhanes_descriptions_table = 'stage.descriptions'
    nhanes_encodings_table = 'stage.encodings'
    context.log.info('Connecting to duckdb...')
    with duckdb.get_connection() as conn: 
        context.log.info('Loading descriptions.parquet to duckdb table...')
        conn.execute('CREATE SCHEMA IF NOT EXISTS stage')
        conn.execute(f"CREATE OR REPLACE TABLE {nhanes_descriptions_table} AS SELECT * FROM read_parquet('{loc}/descriptions.parquet')")
        context.log.info('Successful.')

        context.log.info('Loading encodings.parquet to duckdb table...')
        conn.execute('CREATE SCHEMA IF NOT EXISTS stage')
        conn.execute(f"CREATE OR REPLACE TABLE {nhanes_encodings_table} AS SELECT * FROM read_parquet('{loc}/encodings.parquet')")
        context.log.info('Successful.')