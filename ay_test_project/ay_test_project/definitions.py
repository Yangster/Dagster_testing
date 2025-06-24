import dagster as dg
from ay_test_project.assets import smart_sheets_economic_curtailment

smartsheet_ec_assets=dg.load_assets_from_modules([smart_sheets_economic_curtailment])

defs=dg.Definitions(
    assets=[*smartsheet_ec_assets]
)