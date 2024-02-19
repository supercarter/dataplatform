from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource

from .assets import (
    data_scraper,
    description_scraper
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        'duckdb': DuckDBResource(
            database="/Users/riley/dataplatform/duckdb/healthstats.duckdb"
    )
    }
)
