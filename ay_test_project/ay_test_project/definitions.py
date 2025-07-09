import dagster as dg
from dagster_pandas import PandasColumn
from dagster_duckdb_pandas import DuckDBPandasIOManager
from ay_test_project.assets import smart_sheets_economic_curtailment, pi_webapi_integration
from ay_test_project.resources.smartsheet_resource import SmartsheetResource
from ay_test_project.resources.pi_webapi_resource import PIWebAPIResource
from ay_test_project.resources.duckdb_resource import database_resource


smartsheet_ec_assets=dg.load_assets_from_modules([smart_sheets_economic_curtailment])
pi_webapi_assets = dg.load_assets_from_modules([pi_webapi_integration])

# Configure the IO manager for PI Web API assets
pi_webapi_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
    schema="pi_data"
)

# Configure separate IO manager for smartsheet assets if needed in the future
smartsheet_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",  # Same as manual DuckDB
    schema="smartsheet"
)

defs=dg.Definitions(
    assets=[*smartsheet_ec_assets, *pi_webapi_assets],
    resources={
        "smartsheet_client": SmartsheetResource(),
        "pi_webapi_client": PIWebAPIResource(),
        "database": database_resource,
        "io_manager": pi_webapi_io_manager,  # Default for PI Web API assets
        "smartsheet_io_manager": smartsheet_io_manager  # Available for future use
    }
)