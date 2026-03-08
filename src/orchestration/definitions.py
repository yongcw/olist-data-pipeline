import os
from pathlib import Path
from dagster import Definitions, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# Absolute pathing for WSL stability
CURRENT_DIR = Path(__file__).parent.absolute()
DBT_PROJECT_DIR = (CURRENT_DIR.parent / "dbt_olist").resolve()
MANIFEST_PATH = (DBT_PROJECT_DIR / "target" / "manifest.json").resolve()

dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

@dbt_assets(manifest=os.fspath(MANIFEST_PATH))
def olist_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

defs = Definitions(
    assets=[olist_dbt_assets],
    resources={"dbt": dbt_resource},
)