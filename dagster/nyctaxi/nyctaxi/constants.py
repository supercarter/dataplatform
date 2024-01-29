import os
from pathlib import Path

from dagster_dbt import DbtCliResource


dbt_project_dir = Path(__file__).joinpath("..","..","dbt_nyc_taxi").resolve()

dbt = DbtCliResource(project_dir = os.fspath(dbt_project_dir))


if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")