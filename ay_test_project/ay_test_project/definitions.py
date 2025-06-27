import dagster as dg
from ay_test_project.assets import smart_sheets_economic_curtailment, pi_webapi_integration
from ay_test_project.resources.smartsheet_resource import SmartsheetResource
from ay_test_project.resources.pi_webapi_resource import PIWebAPIResource
from ay_test_project.resources.duckdb_resource import database_resource


smartsheet_ec_assets=dg.load_assets_from_modules([smart_sheets_economic_curtailment])
pi_webapi_assets = dg.load_assets_from_modules([pi_webapi_integration])

defs=dg.Definitions(
    assets=[*smartsheet_ec_assets, *pi_webapi_assets],
    resources={
        "smartsheet_client": SmartsheetResource(),
        "pi_webapi_client": PIWebAPIResource(),
        "database": database_resource
    }
)