# ay_test_project/ay_test_project/assets/pi_webapi_integration.py
import pandas as pd
import requests
from requests_kerberos import HTTPKerberosAuth, DISABLED
from pathlib import Path
import datetime as dt
from dagster import asset, AssetExecutionContext, Config, MaterializeResult, MetadataValue
from typing import Optional


class PIWebAPIConfig(Config):
    """Configuration for PI Web API connection"""
    database_webid: str = "F1RDfBX_hedyUEKNiDBi2ij-swoeupCWwgO0Sod5PaUcAa2wRUdQTkEtQkEtQUZcRUdQTkE"
    base_url: str = "https://egpna-pi-web.enelint.global/piwebapi"
    table_name: str = "Economic_Curtailment_Prices_Solar"


def _get_upstream_file_path(context: AssetExecutionContext, upstream_asset_key: str) -> Path:
    """Get file path from upstream asset metadata"""
    from dagster import AssetKey
    
    upstream_key = AssetKey([upstream_asset_key])
    materialization = context.instance.get_latest_materialization_event(upstream_key)
    
    if not materialization:
        raise ValueError(f"No materialization found for upstream asset: {upstream_asset_key}")
    
    metadata = materialization.dagster_event.event_specific_data.materialization.metadata
    
    if "file_path" in metadata:
        return Path(metadata["file_path"].value)
    else:
        raise ValueError(f"No file_path found in metadata for asset: {upstream_asset_key}")


@asset(
    description="PI Web API table WebID for economic curtailment data",
    group_name="pi_webapi"
)
def pi_table_webid(
    context: AssetExecutionContext,
    config: PIWebAPIConfig
) -> MaterializeResult:
    """Find and return the WebID for the PI Web API table"""
    
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=DISABLED)
    headers = {
        "Host": "egpna-pi-web",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
    }
    
    query_url = f"{config.base_url}/assetdatabases/{config.database_webid}/tables"
    
    response = requests.get(query_url, auth=kerberos_auth, headers=headers, verify=True)
    
    if response.status_code not in (200, 207):
        raise ValueError(f"Failed to get tables: {response.status_code}")
    
    tables = response.json().get("Items", [])
    
    for table in tables:
        if table.get("Name") == config.table_name:
            table_webid = table.get("WebId")
            context.log.info(f"Found table '{config.table_name}' with WebID: {table_webid}")
            
            # Save WebID to file for downstream assets
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            
            webid_file = data_dir / "pi_table_webid.txt"
            webid_file.write_text(table_webid)
            
            return MaterializeResult(
                metadata={
                    "table_webid": table_webid,
                    "table_name": config.table_name,
                    "file_path": str(webid_file)
                }
            )
    
    raise ValueError(f"Table '{config.table_name}' not found")


@asset(
    description="Existing economic curtailment data from PI Web API",
    group_name="pi_webapi",
    deps=['pi_table_webid']
)
def existing_pi_data(
    context: AssetExecutionContext,
    config: PIWebAPIConfig
) -> MaterializeResult:
    """Extract existing data from PI Web API table"""
    
    # Get WebID from upstream asset
    webid_file_path = _get_upstream_file_path(context, "pi_table_webid")
    table_webid = webid_file_path.read_text().strip()
    
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=DISABLED)
    headers = {
        "Host": "egpna-pi-web",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
    }
    
    query_url = f"{config.base_url}/tables/{table_webid}/data"
    
    response = requests.get(query_url, auth=kerberos_auth, headers=headers, verify=True)
    
    if response.status_code not in (200, 207):
        raise ValueError(f"Failed to get table data: {response.status_code}")
    
    table_data = response.json()
    columns_dict = table_data.get("Columns", {})
    rows_list = table_data.get("Rows", [])
    
    df_pi = pd.DataFrame(rows_list)
    df_pi['PriceStartDate'] = pd.to_datetime(df_pi['PriceStartDate'])
    
    # Save data
    data_dir = Path("data")
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = data_dir / f"existing_pi_data_{timestamp}.csv"
    df_pi.to_csv(filepath, index=False)
    
    context.log.info(f"Retrieved {len(df_pi)} existing records from PI Web API")
    
    return MaterializeResult(
        metadata={
            "num_records": len(df_pi),
            "num_columns": len(df_pi.columns),
            "file_path": str(filepath),
            "file_size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
        }
    )


@asset(
    description="New records identified for PI Web API update",
    group_name="pi_webapi",
    deps=['processed_curtailment_data', 'existing_pi_data']
)
def new_pi_records(
    context: AssetExecutionContext
) -> MaterializeResult:
    """Identify new records that need to be added to PI Web API"""
    
    # Load processed Smartsheet data
    ss_file_path = _get_upstream_file_path(context, "processed_curtailment_data")
    df_ss = pd.read_csv(ss_file_path, parse_dates=['PriceStartDate'])
    
    # Load existing PI data
    pi_file_path = _get_upstream_file_path(context, "existing_pi_data")
    df_pi = pd.read_csv(pi_file_path, parse_dates=['PriceStartDate'])
    
    # Merge to find new records
    df_merged = pd.merge(
        df_ss, df_pi,
        on=['GlobalPlantcode', 'Subplant', 'PriceStartDate'],
        indicator=True, how='left'
    )
    
    # Get candidates for new records
    df_new_candidates = df_merged[df_merged['_merge'] != 'both'].copy()
    df_new_candidates = df_new_candidates[['GlobalPlantcode', 'Subplant', 'PriceStartDate', 'EconomicThresholdPrice_x']]
    df_new_candidates = df_new_candidates.rename(columns={'EconomicThresholdPrice_x': 'EconomicThresholdPrice'})
    
    # Filter for meaningful price changes
    df_new = _filter_price_changes(df_new_candidates, df_pi, context)
    
    if len(df_new) > 0:
        # Add reference data
        df_references = df_pi[['ElementCode', 'PlantName', 'GlobalPlantcode', 'PlantCode', 'Subplant']] \
                       .drop_duplicates().reset_index(drop=True)
        
        df_final = pd.merge(df_new, df_references, on=['GlobalPlantcode', 'Subplant'], how='left')
        df_final = df_final[['ElementCode', 'PlantName', 'GlobalPlantcode', 'PlantCode', 
                           'Subplant', 'PriceStartDate', 'EconomicThresholdPrice']]
        
        # Save new records
        data_dir = Path("data")
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = data_dir / f"new_pi_records_{timestamp}.csv"
        df_final.to_csv(filepath, index=False)
        
        context.log.info(f"Identified {len(df_final)} new records for PI Web API")
        
        return MaterializeResult(
            metadata={
                "num_new_records": len(df_final),
                "file_path": str(filepath),
                "price_range": f"${df_final['EconomicThresholdPrice'].min():.2f} - ${df_final['EconomicThresholdPrice'].max():.2f}",
            }
        )
    else:
        # Create empty file for consistency
        data_dir = Path("data")
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = data_dir / f"new_pi_records_{timestamp}.csv"
        pd.DataFrame().to_csv(filepath, index=False)
        
        context.log.info("No new records identified")
        
        return MaterializeResult(
            metadata={
                "num_new_records": 0,
                "file_path": str(filepath),
            }
        )


@asset(
    description="PI Web API table updated with new records",
    group_name="pi_webapi",
    deps=['new_pi_records', 'pi_table_webid', 'existing_pi_data']
)
def updated_pi_table(
    context: AssetExecutionContext,
    config: PIWebAPIConfig
) -> MaterializeResult:
    """Update PI Web API table with new records"""
    
    # Load new records
    new_records_path = _get_upstream_file_path(context, "new_pi_records")
    df_new = pd.read_csv(new_records_path, parse_dates=['PriceStartDate'])
    
    if len(df_new) == 0:
        context.log.info("No new records to add to PI Web API")
        return MaterializeResult(
            metadata={
                "records_added": 0,
                "update_status": "No updates needed"
            }
        )
    
    # Get WebID
    webid_file_path = _get_upstream_file_path(context, "pi_table_webid")
    table_webid = webid_file_path.read_text().strip()
    
    # Update PI Web API table
    success = _update_pi_table(table_webid, df_new, config, context)
    
    if success:
        context.log.info(f"Successfully added {len(df_new)} records to PI Web API")
        return MaterializeResult(
            metadata={
                "records_added": len(df_new),
                "update_status": "Success",
                "table_webid": table_webid
            }
        )
    else:
        raise ValueError("Failed to update PI Web API table")


def _filter_price_changes(df_new, df_pi, context, price_tolerance=0.001):
    """Filter records for meaningful price changes"""
    
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


def _update_pi_table(table_webid, df_new, config, context):
    """Update PI Web API table with new records"""
    
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=DISABLED)
    headers = {
        "Host": "egpna-pi-web",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
    }
    
    query_url = f"{config.base_url}/tables/{table_webid}/data"
    
    try:
        # Get current table data
        response = requests.get(query_url, auth=kerberos_auth, headers=headers, verify=True)
        
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
        
        update_response = requests.put(
            query_url,
            auth=kerberos_auth,
            headers=headers,
            json=updated_data,
            verify=True
        )
        
        if update_response.status_code in (200, 204, 207):
            context.log.info(f"Successfully updated PI Web API table")
            return True
        else:
            context.log.error(f"Failed to update table: {update_response.status_code}")
            return False
            
    except Exception as e:
        context.log.error(f"Error updating table: {e}")
        return False