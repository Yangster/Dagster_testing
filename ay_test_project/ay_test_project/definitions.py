# ay_test_project/ay_test_project/definitions.py
import dagster as dg
from dagster_pandas import PandasColumn
from dagster_duckdb_pandas import DuckDBPandasIOManager
from ay_test_project.assets import smart_sheets_economic_curtailment, pi_webapi_integration, turbine_data_extraction

# Import new insurance claims assets
from ay_test_project.assets import insurance_claim_tracking
# Import jobs and schedules
from ay_test_project.jobs.insurance_claims_job import (
    insurance_claims_job,
    daily_insurance_claims_schedule,
    frequent_insurance_claims_schedule
)
# Import resources
from ay_test_project.resources.smartsheet_resource import SmartsheetResource
from ay_test_project.resources.pi_webapi_resource import PIWebAPIResource
from ay_test_project.resources.quickbase_resource import QuickBaseResource
from ay_test_project.resources.duckdb_resource import database_resource

# Load asset groups
smartsheet_ec_assets = dg.load_assets_from_modules([smart_sheets_economic_curtailment])
pi_webapi_assets = dg.load_assets_from_modules([pi_webapi_integration])
turbine_data_assets = dg.load_assets_from_modules([turbine_data_extraction])
insurance_claims_assets = dg.load_assets_from_modules([insurance_claim_tracking])

# Configure the IO manager for PI Web API assets
pi_webapi_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
    schema="pi_data"
)

# Alternative: Create a separate IO manager for intermediate assets that should be replaced
intermediate_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
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

# IO manager for insurance claims assets
insurance_io_manager = DuckDBPandasIOManager(
    database="data/staging/data.duckdb",
    schema="insurance"
)

# Define all assets - grouping handled by individual asset decorators
all_assets = [
    *smartsheet_ec_assets,
    *pi_webapi_assets, 
    *turbine_data_assets,
    *insurance_claims_assets
]

# Define all jobs
all_jobs = [
    insurance_claims_job
]

# Define all schedules
all_schedules = [
    daily_insurance_claims_schedule,
    frequent_insurance_claims_schedule
]

defs=dg.Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources={
        # Client resources
        "smartsheet_client": SmartsheetResource(),
        "pi_webapi_client": PIWebAPIResource(),
        "qb_client": QuickBaseResource(),
        "database": database_resource,
        # IO managers - default and specialized
        "io_manager": pi_webapi_io_manager,  # Default IO manager
        "intermediate_io_manager": intermediate_io_manager,  # For intermediate processing
        "smartsheet_io_manager": smartsheet_io_manager,  # For smartsheet assets
        "turbine_io_manager": turbine_io_manager,  # For turbine data assets
        "insurance_io_manager": insurance_io_manager,  # For insurance claims assets
    },
    asset_checks=None,  # Can add data quality checks later
)