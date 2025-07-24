# src/economic_curtailment_thresholds/resources/duckdb_resource.py
from dagster_duckdb import DuckDBResource

database_resource = DuckDBResource(
    database="data/staging/data.duckdb"
)