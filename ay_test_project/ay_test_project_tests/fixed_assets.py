import pandas as pd
import smartsheet
import datetime as dt
import os
import re
from pathlib import Path
from dagster import asset, AssetExecutionContext, Config, MaterializeResult, MetadataValue
from typing import Optional


class SmartsheetConfig(Config):
    """Configuration for Smartsheet connection"""
    sheet_name: str = "Economic Curtailment Thresholds"
    bearer_token_env_var: str = "SMARTSHEETS_TOKEN"


class DataConfig(Config):
    """Configuration for data storage"""
    data_dir: str = "data"  # Directory to store CSV files


def _get_upstream_file_path(context: AssetExecutionContext, upstream_asset_key: str) -> Path:
    """
    Get the file path from upstream asset's metadata.
    
    Args:
        context: The current asset execution context
        upstream_asset_key: The key of the upstream asset (e.g., "raw_smartsheet_data")
    
    Returns:
        Path to the file created by the upstream asset
    """
    # Get the materialization record for the upstream asset
    from dagster import AssetKey
    
    upstream_key = AssetKey([upstream_asset_key])
    materialization = context.instance.get_latest_materialization_event(upstream_key)
    
    if not materialization:
        raise ValueError(f"No materialization found for upstream asset: {upstream_asset_key}")
    
    # Extract file path from metadata
    metadata = materialization.dagster_event.event_specific_data.materialization.metadata
    
    if "file_path" in metadata:
        file_path = metadata["file_path"].value
        return Path(file_path)
    else:
        raise ValueError(f"No file_path found in metadata for asset: {upstream_asset_key}")
    

@asset(
    description="Raw economic curtailment threshold data extracted from Smartsheet",
    group_name="economic_curtailment"
)
def raw_smartsheet_data(
    context: AssetExecutionContext, 
    config: SmartsheetConfig
) -> MaterializeResult:
    """
    Extract raw economic curtailment threshold data from Smartsheet and save to CSV.
    
    Returns:
        MaterializeResult with metadata about the saved file
    """
    context.log.info(f"Starting extraction from sheet: {config.sheet_name}")
    
    # Get bearer token from environment
    bearer = os.environ.get(config.bearer_token_env_var)
    if not bearer:
        raise ValueError(f"Environment variable {config.bearer_token_env_var} not found")
    
    # Create Smartsheet client with SSL configuration
    import ssl
    import urllib3
    
    # Multiple approaches to handle SSL issues in corporate environments
    try:
        # Approach 1: Update CA bundle path if available
        ca_bundle_path = os.environ.get('REQUESTS_CA_BUNDLE') or os.environ.get('CURL_CA_BUNDLE')
        if ca_bundle_path and os.path.exists(ca_bundle_path):
            context.log.info(f"Using CA bundle: {ca_bundle_path}")
            ss_client = smartsheet.Smartsheet(f"Bearer {bearer}")
        else:
            # Approach 2: Disable SSL warnings and verification for corporate networks
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # Create client
            ss_client = smartsheet.Smartsheet(f"Bearer {bearer}")
            
            # Configure session to bypass SSL verification
            if hasattr(ss_client, '_session'):
                ss_client._session.verify = False
                context.log.info("SSL verification disabled for Smartsheet client")
            
    except Exception as ssl_error:
        context.log.warning(f"SSL configuration failed: {ssl_error}")
        # Fallback: create client normally and hope for the best
        ss_client = smartsheet.Smartsheet(f"Bearer {bearer}")
    
    # Get all sheets and find the target sheet
    context.log.info("Retrieving sheet list from Smartsheet...")
    response = ss_client.Sheets.list_sheets(include_all=True)
    all_sheets = response.data
    
    sheet_found = False
    sheet_id = None
    for sheet in all_sheets:
        if sheet.name == config.sheet_name:
            sheet_id = sheet.id
            sheet_found = True
            break
    
    if not sheet_found:
        available_sheets = [sheet.name for sheet in all_sheets]
        raise ValueError(f"Sheet '{config.sheet_name}' not found. Available sheets: {available_sheets}")
    
    context.log.info(f"Found sheet with ID: {sheet_id}")
    
    # Get sheet details
    sheet = ss_client.Sheets.get_sheet(sheet_id)
    
    # Process sheet data
    columns = {col.id: col.title for col in sheet.columns}
    rows = []
    
    for row in sheet.rows:
        row_data = {}
        row_data['_row_id'] = row.id
        
        for cell in row.cells:
            column_name = columns.get(cell.column_id, f"Column_{cell.column_id}")
            cell_value = getattr(cell, 'value', None)
            row_data[column_name] = cell_value
            
        rows.append(row_data)
    
    df_raw = pd.DataFrame(rows)
    context.log.info(f"Extracted {len(df_raw)} rows and {len(df_raw.columns)} columns from Smartsheet")
    
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Save to CSV with timestamp
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = data_dir / f"raw_smartsheet_data_{timestamp}.csv"
    df_raw.to_csv(filepath, index=False)
    
    context.log.info(f"Saved raw data to {filepath}")
    
    return MaterializeResult(
        metadata={
            "num_records": len(df_raw),
            "num_columns": len(df_raw.columns),
            "file_path": str(filepath),
            "file_size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
            "columns": MetadataValue.json(list(df_raw.columns)),
        }
    )


@asset(
    description="Cleaned smartsheet data with standardized column names",
    group_name="economic_curtailment",
    deps=['raw_smartsheet_data']
)
def cleaned_smartsheet_data(
    context: AssetExecutionContext
) -> MaterializeResult:
    """
    Clean and standardize the raw Smartsheet data.
    
    Returns:
        MaterializeResult with metadata about the cleaned data file
    """
    context.log.info("Starting data cleaning process...")
    # Get the file path from upstream asset's metadata
    raw_file_path = _get_upstream_file_path(context, "raw_smartsheet_data")

    # Load the raw data from the CSV file
    df_raw = pd.read_csv(raw_file_path)
    context.log.info(f"Loaded {len(df_raw)} rows from {raw_file_path}")
    
    df_cleaned = df_raw.copy()
    
    # Rename date columns
    df_cleaned = _rename_date_columns(df_cleaned, context)
    
    # Rename standard columns
    column_renames = {
        'Before 12/16/24': '10/1/24', 
        'Plant Code': 'GlobalPlantcode'
    }
    
    df_cleaned = df_cleaned.rename(columns=column_renames)
    
    # Log the renaming operations
    for old_name, new_name in column_renames.items():
        if old_name in df_raw.columns:
            context.log.info(f"Renamed column '{old_name}' to '{new_name}'")
    
    # Save cleaned data
    data_dir = Path("data")
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = data_dir / f"cleaned_smartsheet_data_{timestamp}.csv"
    df_cleaned.to_csv(filepath, index=False)
    
    context.log.info(f"Cleaning completed. Saved to {filepath}")
    
    return MaterializeResult(
        metadata={
            "num_records": len(df_cleaned),
            "num_columns": len(df_cleaned.columns),
            "file_path": str(filepath),
            "file_size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
            "columns": MetadataValue.json(list(df_cleaned.columns)),
        }
    )


@asset(
    description="Economic curtailment data transformed from wide to long format",
    group_name="economic_curtailment",
    deps=['cleaned_smartsheet_data']
)
def transformed_curtailment_data(
    context: AssetExecutionContext
) -> MaterializeResult:
    """
    Transform cleaned data from wide format (dates as columns) to long format.
    
    Returns:
        MaterializeResult with metadata about the transformed data file
    """
    context.log.info("Starting data transformation to long format...")

    # Get the file path from upstream asset's metadata
    cleaned_file_path = _get_upstream_file_path(context, "cleaned_smartsheet_data")
    context.log.info(f"Loading cleaned data from: {cleaned_file_path}")
    df_cleaned = pd.read_csv(cleaned_file_path)
    context.log.info(f"Loaded {len(df_cleaned)} rows from upstream asset")
    
    # Transform to long format
    df_transformed = _unpivot_price_data(df_cleaned, context)
    
    # Convert Subplant to integer if it exists
    if 'Subplant' in df_transformed.columns:
        df_transformed['Subplant'] = df_transformed['Subplant'].astype(int)
        context.log.info("Converted Subplant column to integer")
    
    # Save transformed data
    data_dir = Path("data")
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = data_dir / f"transformed_curtailment_data_{timestamp}.csv"
    df_transformed.to_csv(filepath, index=False)
    
    context.log.info(f"Transformation completed. Saved to {filepath}")
    
    return MaterializeResult(
        metadata={
            "num_records": len(df_transformed),
            "num_columns": len(df_transformed.columns),
            "file_path": str(filepath),
            "file_size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
            "columns": MetadataValue.json(list(df_transformed.columns)),
            "date_range": f"{df_transformed['PriceStartDate'].min()} to {df_transformed['PriceStartDate'].max()}" if 'PriceStartDate' in df_transformed.columns else "N/A",
        }
    )


@asset(
    description="Final processed economic curtailment data ready for downstream use",
    group_name="economic_curtailment",
    deps=['transformed_curtailment_data']
)
def processed_curtailment_data(
    context: AssetExecutionContext
) -> MaterializeResult:
    """
    Final processing step to select and organize columns for downstream use.
    
    Returns:
        MaterializeResult with metadata about the final processed data file
    """
    context.log.info("Starting final data processing...")
    
    # Get the file path from upstream asset's metadata
    transformed_file_path = _get_upstream_file_path(context, "transformed_curtailment_data")
    context.log.info(f"Loading transformed data from: {transformed_file_path}")
    
    df_transformed = pd.read_csv(transformed_file_path, parse_dates=['PriceStartDate'])
    context.log.info(f"Loaded {len(df_transformed)} rows from upstream asset")
    
    # Define the final columns we want to keep
    final_columns = [
        'Internal Name', 
        'GlobalPlantcode', 
        'Subplant', 
        'PriceStartDate', 
        'EconomicThresholdPrice'
    ]
    
    # Check which columns actually exist
    available_columns = [col for col in final_columns if col in df_transformed.columns]
    missing_columns = [col for col in final_columns if col not in df_transformed.columns]
    
    if missing_columns:
        context.log.warning(f"Missing expected columns: {missing_columns}")
        context.log.info(f"Available columns: {list(df_transformed.columns)}")
    
    # Select available columns
    df_final = df_transformed[available_columns].copy()
    
    # Remove any rows with null critical values
    initial_rows = len(df_final)
    if 'EconomicThresholdPrice' in df_final.columns:
        df_final = df_final.dropna(subset=['EconomicThresholdPrice'])
        context.log.info(f"Removed {initial_rows - len(df_final)} rows with null prices")
    
    # Save final data
    data_dir = Path("data")
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = data_dir / f"processed_curtailment_data_{timestamp}.csv"
    df_final.to_csv(filepath, index=False)
    
    context.log.info(f"Final processing completed. Saved to {filepath}")
    
    # Calculate some summary statistics for metadata
    price_stats = {}
    if 'EconomicThresholdPrice' in df_final.columns:
        price_stats = {
            "min_price": float(df_final['EconomicThresholdPrice'].min()),
            "max_price": float(df_final['EconomicThresholdPrice'].max()),
            "avg_price": float(df_final['EconomicThresholdPrice'].mean()),
        }
    
    unique_plants = df_final['GlobalPlantcode'].nunique() if 'GlobalPlantcode' in df_final.columns else 0
    
    return MaterializeResult(
        metadata={
            "num_records": len(df_final),
            "num_columns": len(df_final.columns),
            "file_path": str(filepath),
            "file_size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
            "columns": MetadataValue.json(list(df_final.columns)),
            "unique_plants": unique_plants,
            **price_stats,
        }
    )


# Helper functions (kept as private functions)
def _rename_date_columns(df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
    """
    Rename columns containing date information to a consistent format.
    """
    rename_dict = {}
    
    # Common date patterns to match
    date_patterns = [
        r'\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b',  # MM/DD/YY or MM/DD/YYYY
        r'\b(\d{1,2})-(\d{1,2})-(\d{2,4})\b',  # MM-DD-YY or MM-DD-YYYY
        r'\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})\b', # MM.DD.YY or MM.DD.YYYY
    ]
    
    # Loop through all columns in the DataFrame
    for col in df.columns:
        # Skip standard columns that shouldn't be renamed
        if col in ['_row_id', 'QENEL', 'Market Entity', 'Internal Name', 
                   'GlobalPlantcode', 'Plant Code', 'Subplant']:
            continue
            
        # Try to find a date pattern in the column name
        date_found = None
        for pattern in date_patterns:
            match = re.search(pattern, col)
            if match:
                month, day, year = match.groups()
                
                # Handle 2-digit vs 4-digit years
                if len(year) == 2:
                    year_int = int(year)
                    if year_int <= 30:
                        year = f"20{year}"
                    else:
                        year = f"19{year}"
                
                # Format consistently as MM/DD/YY
                try:
                    # Validate the date
                    dt.datetime.strptime(f"{month}/{day}/{year}", "%m/%d/%Y")
                    # Convert to 2-digit year format for consistency
                    year_2digit = year[-2:]
                    date_found = f"{month}/{day}/{year_2digit}"
                    break
                except ValueError:
                    continue
        
        # If we found a valid date, add it to rename dictionary
        if date_found:
            rename_dict[col] = date_found
            context.log.info(f"Renaming column '{col}' to '{date_found}'")
    
    return df.rename(columns=rename_dict)


def _identify_date_columns(df: pd.DataFrame) -> list:
    """
    Identify which columns contain date information after renaming.
    """
    date_columns = []
    
    # Standard non-date columns to exclude
    id_columns = ['_row_id', 'QENEL', 'Market Entity', 'Internal Name', 
                  'GlobalPlantcode', 'Plant Code', 'Subplant']
    
    for col in df.columns:
        if col in id_columns:
            continue
            
        # Check if column name matches date pattern
        date_patterns = [
            r'^\d{1,2}/\d{1,2}/\d{2}$',  # MM/DD/YY
            r'^\d{1,2}-\d{1,2}-\d{2}$',  # MM-DD-YY
            r'^\d{1,2}\.\d{1,2}\.\d{2}$', # MM.DD.YY
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, col):
                date_columns.append(col)
                break
    
    return date_columns


def _unpivot_price_data(df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
    """
    Transform price data from wide to long format.
    """
    # List of columns to keep as identifiers
    id_columns = ['_row_id', 'QENEL', 'Market Entity', 'Internal Name', 
                  'GlobalPlantcode', 'Subplant']
    
    # Dynamically find date columns
    date_columns = _identify_date_columns(df)
    
    if not date_columns:
        context.log.warning("No date columns found for unpivoting")
        return df
    
    context.log.info(f"Found {len(date_columns)} date columns: {date_columns}")
    
    # Unpivot the DataFrame
    df_unpivoted = pd.melt(
        df,
        id_vars=id_columns,
        value_vars=date_columns,
        var_name='PriceStartDate',
        value_name='EconomicThresholdPrice'
    )
    
    # Convert the date strings to datetime objects
    df_unpivoted['PriceStartDate'] = pd.to_datetime(
        df_unpivoted['PriceStartDate'], 
        format='%m/%d/%y', 
        errors='coerce'
    )
    
    # Handle any dates that couldn't be parsed
    mask_failed = df_unpivoted['PriceStartDate'].isna()
    if mask_failed.any():
        context.log.warning(f"{mask_failed.sum()} dates could not be parsed with standard format")
    
    # Set time to midnight
    df_unpivoted['PriceStartDate'] = df_unpivoted['PriceStartDate'].dt.normalize()
    
    # Remove rows where date parsing failed
    df_unpivoted = df_unpivoted.dropna(subset=['PriceStartDate'])
    
    return df_unpivoted