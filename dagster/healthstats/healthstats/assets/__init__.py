from dagster import load_assets_from_package_module

from . import (
    data_scraper,
    description_scraper
)

data_scraper_assets = load_assets_from_package_module(data_scraper)
description_scraper_assets = load_assets_from_package_module(description_scraper)

