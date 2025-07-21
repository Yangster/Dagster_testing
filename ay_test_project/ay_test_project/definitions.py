import dagster as dg
from dagster_pandas import PandasColumn
from dagster_duckdb_pandas import DuckDBPandasIOManager
from ay_test_project.assets import smart_sheets_economic_curtailment, pi_webapi_integration, turbine_data_extraction
from ay_test_project.resources.smartsheet_resource import SmartsheetResource
from ay_test_project.resources.pi_webapi_resource import PIWebAPIResource
from ay_test_project.resources.duckdb_resource import database_resource


smartsheet_ec_assets=dg.load_assets_from_modules([smart_sheets_economic_curtailment])
pi_webapi_assets = dg.load_assets_from_modules([pi_webapi_integration])
turbine_data_assets = dg.load_assets_from_modules([turbine_data_extraction])

# Configure the IO manager for PI Web API assets
pi_webapi_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
    schema="pi_data"
)

# Alternative: Create a separate IO manager for intermediate assets that should be replaced
intermediate_io_manager = DuckDBPandasIOManager(
    database="data/staging/pi_webapi.duckdb",
    schema="staging",
    store_table_type="replace"  # Always recreate the table
)


# Configure separate IO manager for smartsheet assets if needed in the future
smartsheet_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",  # Same as manual DuckDB
    schema="smartsheet"
)

# Configure IO manager for turbine data assets
turbine_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
    schema="turbine"
)

defs=dg.Definitions(
    assets=[*smartsheet_ec_assets, *pi_webapi_assets, *turbine_data_assets],
    resources={
        "smartsheet_client": SmartsheetResource(),
        "pi_webapi_client": PIWebAPIResource(),
        "database": database_resource,
        "io_manager": pi_webapi_io_manager,  # Default for PI Web API assets
        "intermediate_io_manager": intermediate_io_manager,  # For intermediate processing
        "smartsheet_io_manager": smartsheet_io_manager,  # Available for future use
        "turbine_io_manager": turbine_io_manager  # For turbine data assets
    }
)