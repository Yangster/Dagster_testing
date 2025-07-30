# src/economic_curtailment_thresholds/assets/pi_webapi_assets.py
import pandas as pd
import requests
from requests_kerberos import HTTPKerberosAuth, DISABLED
from pathlib import Path
import datetime as dt
from dagster import asset, AssetExecutionContext, Config, MaterializeResult, MetadataValue, AssetIn
from typing import Optional
from economic_curtailment_thresholds.resources.pi_webapi_resource import PIWebAPIResource
from dagster_duckdb import DuckDBResource


class PIWebAPIConfig(Config):
    """Configuration for PI Web API connection"""
    table_name: str = "Economic_Curtailment_Prices_Solar"


def _get_upstream_table_name(context: AssetExecutionContext, upstream_asset_key: str) -> str:
    """Get table name from upstream asset metadata"""
    from dagster import AssetKey
    
    upstream_key = AssetKey([upstream_asset_key])
    materialization = context.instance.get_latest_materialization_event(upstream_key)
    
    if not materialization:
        raise ValueError(f"No materialization found for upstream asset: {upstream_asset_key}")
    
    metadata = materialization.dagster_event.event_specific_data.materialization.metadata
    
    if "table_name" in metadata:
        return metadata["table_name"].value
    else:
        raise ValueError(f"No table_name found in metadata for asset: {upstream_asset_key}")


@asset(
    description="PI Web API table WebID for economic curtailment data",
    group_name="pi_webapi",
    io_manager_key="io_manager",
    kind='duckdb'
)
def pi_table_webid(
    context: AssetExecutionContext,
    config: PIWebAPIConfig,
    pi_webapi_client: PIWebAPIResource
    ) -> pd.DataFrame:
    """Find and return the WebID for the PI Web API table"""
    
    endpoint = f"/assetdatabases/{pi_webapi_client.database_webid}/tables"
    response = pi_webapi_client.make_request("GET", endpoint)
    
    if response.status_code not in (200, 207):
        raise ValueError(f"Failed to get tables: {response.status_code}")
    
    tables = response.json().get("Items", [])
    
    for table in tables:
        if table.get("Name") == config.table_name:
            table_webid = table.get("WebId")
            context.log.info(f"Found table '{config.table_name}' with WebID: {table_webid}")
            
            # Return WebID as a single-row DataFrame
            df_webid = pd.DataFrame({
                'table_webid': [table_webid],
                'table_name': [config.table_name],
                'retrieved_at': [pd.Timestamp.now()]
            })
            
            context.add_output_metadata({
                "table_webid": table_webid,
                "table_name": config.table_name,
                "num_records": len(df_webid)
            })
            
            return df_webid
    
    raise ValueError(f"Table '{config.table_name}' not found")


@asset(
    description="Existing economic curtailment data from PI Web API",
    group_name="pi_webapi",
    ins={"pi_table_webid": AssetIn()},
    io_manager_key="io_manager",
    kind="duckdb"
)
def existing_pi_data(
    context: AssetExecutionContext,
    pi_webapi_client: PIWebAPIResource,
    pi_table_webid: pd.DataFrame
) -> pd.DataFrame:
    """Extract existing data from PI Web API table"""
    
    # Get WebID from upstream asset
    table_webid = pi_table_webid['table_webid'].iloc[0]
    
    endpoint = f"/tables/{table_webid}/data"
    response = pi_webapi_client.make_request("GET", endpoint)
    
    if response.status_code not in (200, 207):
        raise ValueError(f"Failed to get table data: {response.status_code}")
    
    table_data = response.json()
    rows_list = table_data.get("Rows", [])
    
    df_pi = pd.DataFrame(rows_list)
    if not df_pi.empty and 'PriceStartDate' in df_pi.columns:
        df_pi['PriceStartDate'] = pd.to_datetime(df_pi['PriceStartDate'])
    
    context.log.info(f"Retrieved {len(df_pi)} existing records from PI Web API")
    
    context.add_output_metadata({
        "num_records": len(df_pi),
        "num_columns": len(df_pi.columns) if not df_pi.empty else 0,
        "table_webid": table_webid
    })
    
    return df_pi


@asset(
    description="Bridge asset to load smartsheet data for PI Web API processing",
    group_name="pi_webapi",
    deps=["processed_curtailment_data"],
    io_manager_key="io_manager",
    kind="duckdb"
)
def smartsheet_data_bridge(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> pd.DataFrame:
    """Bridge asset to load processed smartsheet data from DuckDB storage"""
    
    with database.get_connection() as conn:
        # Read from the smartsheet schema where data is stored
        df = conn.execute("SELECT * FROM smartsheet.processed_smartsheet_data").fetch_df()
    
    context.log.info(f"Loaded {len(df)} records from smartsheet DuckDB for PI Web API processing")
    
    # Ensure PriceStartDate is datetime
    if 'PriceStartDate' in df.columns:
        df['PriceStartDate'] = pd.to_datetime(df['PriceStartDate'])
    
    context.add_output_metadata({
        "num_records": len(df),
        "num_columns": len(df.columns),
        "source_table": "smartsheet.processed_smartsheet_data"
    })
    
    return df


@asset(
    description="New records identified for PI Web API update",
    group_name="pi_webapi",
    ins={
        "smartsheet_data_bridge": AssetIn(),
        "existing_pi_data": AssetIn()
    },
    io_manager_key="io_manager",
    kind="duckdb"
)
def new_pi_records(
    context: AssetExecutionContext,
    smartsheet_data_bridge: pd.DataFrame,
    existing_pi_data: pd.DataFrame
) -> pd.DataFrame:
    """Identify new records that need to be added to PI Web API"""
    
    # Load processed Smartsheet data via bridge asset
    df_ss = smartsheet_data_bridge.copy()
    if 'PriceStartDate' in df_ss.columns:
        df_ss['PriceStartDate'] = pd.to_datetime(df_ss['PriceStartDate'])
    
    # Load existing PI data
    df_pi = existing_pi_data.copy()
    
    # Merge to find new records
    if df_pi.empty:
        # If no existing data, all Smartsheet data is new
        df_new = df_ss.copy()
        context.log.info(f"No existing PI data found. All {len(df_new)} records are new.")
    else:
        # Merge to find new records
        df_merged = pd.merge(
            df_ss, df_pi,
            on=['GlobalPlantcode', 'Subplant', 'PriceStartDate'],
            indicator=True, how='left'
        )
        
        # Get candidates for new records
        df_new_candidates = df_merged[df_merged['_merge'] != 'both'].copy()
        if not df_new_candidates.empty:
            df_new_candidates = df_new_candidates[['GlobalPlantcode', 'Subplant', 'PriceStartDate', 'EconomicThresholdPrice_x']]
            df_new_candidates = df_new_candidates.rename(columns={'EconomicThresholdPrice_x': 'EconomicThresholdPrice'})
        else:
            df_new_candidates = pd.DataFrame(columns=['GlobalPlantcode', 'Subplant', 'PriceStartDate', 'EconomicThresholdPrice'])
        
        # Filter for meaningful price changes
        df_new = _filter_price_changes(df_new_candidates, df_pi, context)
    
    if len(df_new) > 0:
        # Add reference data if we have existing PI data
        if not df_pi.empty and all(col in df_pi.columns for col in ['ElementCode', 'PlantName', 'PlantCode']):
            df_references = df_pi[['ElementCode', 'PlantName', 'GlobalPlantcode', 'PlantCode', 'Subplant']] \
                           .drop_duplicates().reset_index(drop=True)
            
            df_final = pd.merge(df_new, df_references, on=['GlobalPlantcode', 'Subplant'], how='left')
            df_final = df_final[['ElementCode', 'PlantName', 'GlobalPlantcode', 'PlantCode', 
                               'Subplant', 'PriceStartDate', 'EconomicThresholdPrice']]
        else:
            # If no reference data available, use what we have
            df_final = df_new.copy()
        
        context.log.info(f"Identified {len(df_final)} new records for PI Web API")
        
        context.add_output_metadata({
            "num_new_records": len(df_final),
            "price_range": f"${df_final['EconomicThresholdPrice'].min():.2f} - ${df_final['EconomicThresholdPrice'].max():.2f}" if 'EconomicThresholdPrice' in df_final.columns else "N/A",
        })
        
        return df_final
    else:
        # Return empty DataFrame with expected columns
        empty_columns = ['GlobalPlantcode', 'Subplant', 'PriceStartDate', 'EconomicThresholdPrice']
        df_empty = pd.DataFrame(columns=empty_columns)
        
        context.log.info("No new records identified")
        
        context.add_output_metadata({
            "num_new_records": 0,
        })
        
        return df_empty


@asset(
    description="PI Web API table updated with new records",
    group_name="pi_webapi",
    ins={
        "new_pi_records": AssetIn(),
        "pi_table_webid": AssetIn(),
        "existing_pi_data": AssetIn()
    },
    io_manager_key="io_manager"
)
def updated_pi_table(
    context: AssetExecutionContext,
    pi_webapi_client: PIWebAPIResource,
    new_pi_records: pd.DataFrame,
    pi_table_webid: pd.DataFrame,
    existing_pi_data: pd.DataFrame
) -> pd.DataFrame:
    """Update PI Web API table with new records"""
    
     # Handle empty case explicitly
    if len(new_pi_records) == 0:
        context.log.info("No new records to add to PI Web API - table is already up to date")
        context.add_output_metadata({
            "records_added": 0,
            "update_status": "No updates needed - table current",
            "action_taken": "Skipped update operation"
        })
        return existing_pi_data.copy()
    
    # Get WebID
    table_webid = pi_table_webid['table_webid'].iloc[0]

    # Update PI Web API table
    success = _update_pi_table(table_webid, new_pi_records, pi_webapi_client, context)
    
    if success:
        context.log.info(f"Successfully added {len(new_pi_records)} records to PI Web API")
        
        # Return updated dataset (combine existing + new)
        if not existing_pi_data.empty:
            updated_data = pd.concat([existing_pi_data, new_pi_records], ignore_index=True)
        else:
            updated_data = new_pi_records.copy()
        
        context.add_output_metadata({
            "records_added": len(new_pi_records),
            "total_records": len(updated_data),
            "update_status": "Success",
            "table_webid": table_webid
        })
        
        return updated_data
    else:
        raise ValueError("Failed to update PI Web API table")


def _filter_price_changes(df_new, df_pi, context, price_tolerance=0.001):
    """Filter records for meaningful price changes"""

    if df_new.empty or df_pi.empty:
        return df_new
    
    # Get latest prices by plant/subplant
    df_pi_sorted = df_pi.sort_values(['GlobalPlantcode', 'Subplant', 'PriceStartDate'])
    latest_prices = df_pi_sorted.groupby(['GlobalPlantcode', 'Subplant']).tail(1)
    latest_prices = latest_prices[['GlobalPlantcode', 'Subplant', 'EconomicThresholdPrice']]
    latest_prices = latest_prices.rename(columns={'EconomicThresholdPrice': 'LatestPrice'})
    
    # Merge and compare
    df_comparison = pd.merge(df_new, latest_prices, on=['GlobalPlantcode', 'Subplant'], how='left')
    
    # New plant/subplant combinations
    new_combinations = df_comparison[df_comparison['LatestPrice'].isna()]
    
    # Existing combinations with price changes
    existing_combinations = df_comparison[~df_comparison['LatestPrice'].isna()]
    price_changed = existing_combinations[
        abs(existing_combinations['EconomicThresholdPrice'] - existing_combinations['LatestPrice']) > price_tolerance
    ]
    
    # Combine results
    if len(new_combinations) > 0 and len(price_changed) > 0:
        filtered = pd.concat([new_combinations, price_changed], ignore_index=True)
    elif len(new_combinations) > 0:
        filtered = new_combinations
    elif len(price_changed) > 0:
        filtered = price_changed
    else:
        filtered = pd.DataFrame()
    
    if len(filtered) > 0:
        filtered = filtered[df_new.columns]
    
    context.log.info(f"Filtered {len(df_new)} candidates to {len(filtered)} meaningful changes")
    
    return filtered


def _update_pi_table(table_webid, df_new, pi_webapi_client: PIWebAPIResource, context):
    """Update PI Web API table with new records"""
    
    endpoint = f"/tables/{table_webid}/data"
    
    try:
        # Get current table data
        response = pi_webapi_client.make_request("GET", endpoint)
        
        if response.status_code not in (200, 207):
            context.log.error(f"Failed to get current table data: {response.status_code}")
            return False
        
        table_data = response.json()
        columns_dict = table_data.get("Columns", {})
        current_rows = table_data.get("Rows", [])
        
        # Prepare new rows
        df_to_add = df_new.copy()
        df_to_add['PriceStartDate'] = df_to_add['PriceStartDate'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        new_rows_list = []
        for _, row in df_to_add.iterrows():
            row_dict = row.to_dict()
            for key, value in row_dict.items():
                if pd.isna(value):
                    row_dict[key] = None
            new_rows_list.append(row_dict)
        
        # Update table
        updated_data = {
            "Columns": columns_dict,
            "Rows": current_rows + new_rows_list
        }
        
        update_response = pi_webapi_client.make_request("PUT", endpoint, json=updated_data)
        
        if update_response.status_code in (200, 204, 207):
            context.log.info(f"Successfully updated PI Web API table")
            return True
        else:
            context.log.error(f"Failed to update table: {update_response.status_code}")
            return False
            
    except Exception as e:
        context.log.error(f"Error updating table: {e}")
        return False