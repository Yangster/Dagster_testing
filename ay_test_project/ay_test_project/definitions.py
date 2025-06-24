import dagster as dg
from ay_test_project.assets import smart_sheets_economic_curtailment, pi_webapi_integration

smartsheet_ec_assets=dg.load_assets_from_modules([smart_sheets_economic_curtailment])
pi_webapi_assets = dg.load_assets_from_modules([pi_webapi_integration])

defs=dg.Definitions(
    assets=[*smartsheet_ec_assets, *pi_webapi_assets]
)