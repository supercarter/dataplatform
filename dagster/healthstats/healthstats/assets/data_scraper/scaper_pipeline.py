from .scraper import NHANESDataDownloader
from .pd_combine_dupes import combine_dupes
from dagster import asset, MaterializeResult, AssetExecutionContext
from dagster_duckdb import DuckDBResource
import os

data_lake_location = '/Users/riley/dataplatform/data_lake'
nhanes_year = 2017
partition_path = f'NHANES_{nhanes_year}-{nhanes_year+1}'
loc = f'{data_lake_location}/{partition_path}'


@asset
def read_nhanes_raw(context: AssetExecutionContext) -> MaterializeResult:
    os.makedirs(loc, exist_ok=True)

    scraper = NHANESDataDownloader(BASE_YEAR=nhanes_year, target=loc)
    data_urls = scraper.find_data_urls()
    # Scrape data
    for datatype, data_url in data_urls.items():
        context.log.info(f'Extracting {datatype} data...')
        scraper.extract_and_convert_xpt(data_url, datatype)
    # Remove duplicate columns then save the dataframe as parquet
    scraper.df = combine_dupes(scraper.df)
    scraper.df.to_parquet(loc + "/data.parquet", index=True)
    context.log.info(f'Finished and saved nhanes data as data.parquet.')
    return MaterializeResult(
        metadata={
        }
    )


@asset(deps=[read_nhanes_raw])
def data_parquet_to_duckdb(context: AssetExecutionContext, duckdb: DuckDBResource) -> None: 

    nhanes_data_table = 'stage.data'
    context.log.info('Connecting to duckdb...')
    with duckdb.get_connection() as conn: 
        context.log.info('Loading parquet to duckdb table...')
        conn.execute('CREATE SCHEMA IF NOT EXISTS stage')
        conn.execute(f"CREATE OR REPLACE TABLE {nhanes_data_table} AS SELECT * FROM read_parquet('{loc}/data.parquet')")
        context.log.info('Successful.')
