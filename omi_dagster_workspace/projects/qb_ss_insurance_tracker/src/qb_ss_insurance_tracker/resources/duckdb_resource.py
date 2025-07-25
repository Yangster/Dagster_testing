# src/qb_ss_insurance_tracker/resources/duckdb_resource.py
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

# Database resource for connections
database_resource = DuckDBResource(
    database="data/insurance_tracker/tracker.duckdb"
)

# IO Manager for asset storage
duckdb_io_manager = DuckDBPandasIOManager(
    database="data/insurance_tracker/tracker.duckdb",
    schema="public"
)