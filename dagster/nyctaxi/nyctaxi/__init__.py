from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)


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
            database="duckdb/nyctaxi.duckdb"
        )
    }
    schedules=[nyc_taxi_schedule]
)


