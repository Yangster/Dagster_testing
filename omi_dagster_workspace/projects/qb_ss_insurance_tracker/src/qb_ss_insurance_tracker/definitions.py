# src/qb_ss_insurance_tracker/definitions.py
# originfally pointed to defs folder ,but not neccesary
# from pathlib import Path

# from dagster import definitions, load_from_defs_folder


# @definitions
# def defs():
#     return load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)


import dagster as dg
from dagster import ScheduleDefinition
from qb_ss_insurance_tracker.assets import quickbase_assets, smartsheet_assets
from qb_ss_insurance_tracker.resources.quickbase_resource import QuickBaseResource
from qb_ss_insurance_tracker.resources.smartsheet_resource import SmartsheetResource
from qb_ss_insurance_tracker.resources.config_resource import FieldMappingResource
from qb_ss_insurance_tracker.resources.duckdb_resource import database_resource, duckdb_io_manager

# Load assets from modules
qb_assets = dg.load_assets_from_modules([quickbase_assets])
ss_assets = dg.load_assets_from_modules([smartsheet_assets])

# Schedule - Daily at 7 AM (matching your original Airflow schedule)
daily_insurance_sync = ScheduleDefinition(
    name="daily_insurance_tracker_sync",
    cron_schedule="0 7 * * *",  # Daily at 7 AM
    target="*",  # Run all assets
    execution_timezone="America/Denver",
    description="Daily sync of insurance claims from QuickBase to Smartsheet"
)

# Main definitions - replaces the @definitions decorator pattern
defs = dg.Definitions(
    assets=[*qb_assets, *ss_assets],
    schedules=[daily_insurance_sync],
    resources={
        "quickbase_client": QuickBaseResource(),
        "smartsheet_client": SmartsheetResource(),
        "field_mapping": FieldMappingResource(),
        "database": database_resource,
        "duckdb_io_manager": duckdb_io_manager,
    }
)