# src/economic_curtailment_thresholds/definitions.py
import dagster as dg
from dagster_duckdb_pandas import DuckDBPandasIOManager
from economic_curtailment_thresholds.assets import smartsheet_assets, pi_webapi_assets
from economic_curtailment_thresholds.resources.smartsheet_resource import SmartsheetResource
from economic_curtailment_thresholds.resources.pi_webapi_resource import PIWebAPIResource
from economic_curtailment_thresholds.resources.duckdb_resource import database_resource

# Load assets
smartsheet_ec_assets = dg.load_assets_from_modules([smartsheet_assets])
pi_webapi_ec_assets = dg.load_assets_from_modules([pi_webapi_assets])

# Configure IO managers
smartsheet_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
    schema="smartsheet"
)

pi_webapi_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
    schema="pi_data"
)

defs = dg.Definitions(
    assets=[*smartsheet_ec_assets, *pi_webapi_ec_assets],
    resources={
        "smartsheet_client": SmartsheetResource(),
        "pi_webapi_client": PIWebAPIResource(),
        "database": database_resource,
        "io_manager": pi_webapi_io_manager,  # Default IO manager
    }
)