from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from pathlib import Path
import os
from .constants import dbt_manifest_path

from . import assets

all_assets = load_assets_from_modules([assets])    

nyc_taxi_job = define_asset_job('nyc_taxi_job', selection=AssetSelection.all())

nyc_taxi_schedule = ScheduleDefinition(
    job=nyc_taxi_job,
    cron_schedule="0 * * * *"
)

defs = Definitions(
    assets=all_assets,
    resources={
        'duckdb': DuckDBResource(
            database="../../duckdb/nyctaxi.duckdb"
        ),
        "dbt": DbtCliResource(
            project_dir = "C:/Users/carte/august/dataplatform/dagster/nyctaxi/dbt_nyc_taxi"
        )
    }, 
    schedules=[nyc_taxi_schedule]
)





