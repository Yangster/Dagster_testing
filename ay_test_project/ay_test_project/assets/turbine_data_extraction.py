# ay_test_project/ay_test_project/assets/turbine_data_extraction.py
import pandas as pd
import json
from datetime import datetime, timedelta
import calendar
import pytz
import time
import gc
from pathlib import Path
from typing import Dict, List, Tuple
from json.decoder import JSONDecodeError
from dagster import (
    asset, 
    AssetExecutionContext, 
    Config, 
    MaterializeResult, 
    MetadataValue,
    AssetIn
)
from dagster_duckdb import DuckDBResource
from ay_test_project.resources.pi_webapi_resource import PIWebAPIResource
from urllib.parse import urlencode
import requests


class TurbineDataConfig(Config):
    """Configuration for turbine data extraction"""
    years: List[int] = [2023, 2024]
    months_by_year: Dict[int, List[int]] = {
        2023: list(range(7, 13)),  # July to December
        2024: list(range(1, 7))    # January to June
    }
    batch_size: int = 100  # WebID batch size
    chunk_size: int = 9    # File processing chunk size
    max_retries: int = 3
    turbine_tags_folder: str = "turbineTags2"


@asset(
    description="Load turbine tag configurations from CSV files",
    group_name="turbine_data",
    io_manager_key="io_manager"
)
def turbine_tag_config(
    context: AssetExecutionContext,
    config: TurbineDataConfig,
    database: DuckDBResource
) -> pd.DataFrame:
    """
    Load all turbine tags from CSV files in the turbine tags folder.
    This replaces the file-by-file reading in the original script.
    """
    # For now, we'll use the provided sample file
    # In production, you'd read from the actual folder
    sample_tags = [
        "USWRDDR.MAF004.UMD10.BS001.WMET1.HorWdSpd.mag.f",
        "USWRDDR.MAF004.AxA10.BJ001.WTUR1.W.mag.f", 
        "USWRDDR.MAF004.MDA10.BG001.WROT1.Pth1AngVal.mag.f",
        "USWRDDR.MAF004.MDA10.BG002.WROT1.Pth2AngVal.mag.f",
        "USWRDDR.MAF004.MDA10.BG003.WROT1.Pth3AngVal.mag.f",
        "USWRDDR.MAF004.UMD10.BG001.WMET1.InsWdDir.mag.f",
        "USWRDDR.MAF004.MDA20.BS901.WROT1.RotSpd.mag.f",
        "USWRDDR.MAF004.MDA10.BS921.WTRM1.VbrLevPct.mag.f",
        "USWRDDR.MAF004.MUD10.BG001.WNAC1.Dir.mag.f"
    ]
    
    df_tags = pd.DataFrame({'tag': sample_tags})
    
    context.log.info(f"Loaded {len(df_tags)} turbine tags")
    
    # Store in DuckDB for downstream use
    with database.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS turbine")
        conn.execute("CREATE OR REPLACE TABLE turbine.tag_config AS SELECT * FROM df_tags")
    
    context.add_output_metadata({
        "num_tags": len(df_tags),
        "sample_tags": MetadataValue.json(sample_tags[:3]),
        "table_name": "turbine.tag_config"
    })
    
    return df_tags


@asset(
    description="PI Web API WebIDs for all turbine tags",
    group_name="turbine_data",
    ins={"turbine_tag_config": AssetIn()},
    io_manager_key="io_manager"
)
def turbine_webids(
    context: AssetExecutionContext,
    config: TurbineDataConfig,
    pi_webapi_client: PIWebAPIResource,
    database: DuckDBResource,
    turbine_tag_config: pd.DataFrame
) -> pd.DataFrame:
    """
    Retrieve WebIDs for all turbine tags from PI Web API.
    This replaces the get_web_ids function from the original script.
    """
    unique_tags = turbine_tag_config['tag'].unique()
    context.log.info(f"Getting WebIDs for {len(unique_tags)} unique tags")
    
    search_url = "/dataservers/F1DS6tERIppyu0-VNWF27uDzjQQU5EVlJQSURBVEFSQ0hBLkVORUxJTlQuR0xPQkFM/points?"
    batch_url = "/batch"
    
    all_webids = {}
    
    # Process in batches
    for i in range(0, len(unique_tags), config.batch_size):
        batch_tags = unique_tags[i:i+config.batch_size]
        context.log.info(f"Processing WebID batch {i//config.batch_size + 1}/{(len(unique_tags) + config.batch_size - 1)//config.batch_size}")
        
        # Create search batch request
        search_batch = {
            tag: {
                "Method": "GET",
                "Resource": search_url + urlencode({"nameFilter": tag})
            }
            for tag in batch_tags
        }
        
        try:
            response = pi_webapi_client.make_request(
                "POST", 
                batch_url,
                json=search_batch
            )
            
            if response.status_code not in (200, 207):
                context.log.error(f"Search batch failed with status: {response.status_code}")
                continue
                
            search_response = response.json()
            
            # Extract WebIDs from response
            for tag in batch_tags:
                try:
                    if (tag in search_response and 
                        'Content' in search_response[tag] and
                        'Items' in search_response[tag]['Content'] and
                        search_response[tag]['Content']['Items']):
                        
                        all_webids[tag] = search_response[tag]['Content']['Items'][0]['WebId']
                    else:
                        context.log.warning(f"No WebID found for tag: {tag}")
                except Exception as e:
                    context.log.error(f"Error extracting WebID for tag {tag}: {str(e)}")
                    
        except Exception as e:
            context.log.error(f"Batch request failed: {str(e)}")
            continue
    
    # Convert to DataFrame
    webid_data = [{'tag': tag, 'webid': webid} for tag, webid in all_webids.items()]
    df_webids = pd.DataFrame(webid_data)
    
    context.log.info(f"Successfully retrieved {len(df_webids)} WebIDs out of {len(unique_tags)} tags")
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE turbine.webids AS SELECT * FROM df_webids")
    
    context.add_output_metadata({
        "total_tags": len(unique_tags),
        "successful_webids": len(df_webids),
        "success_rate": f"{len(df_webids)/len(unique_tags)*100:.1f}%",
        "table_name": "turbine.webids"
    })
    
    return df_webids


def _get_month_range(year: int, month: int) -> Tuple[datetime, datetime]:
    """Get start and end dates for a month in UTC"""
    start_date = datetime(year, month, 1, tzinfo=pytz.UTC)
    _, last_day = calendar.monthrange(year, month)
    end_date = datetime(year, month, last_day, 23, 59, 59, tzinfo=pytz.UTC) + timedelta(seconds=1)
    return start_date, end_date


def _date_range(start_date: datetime, end_date: datetime):
    """Generator for day-by-day date range"""
    current_date = start_date
    while current_date < end_date:
        yield current_date
        current_date += timedelta(days=1)


def _process_batch_summary(
    tags: List[str], 
    start_time: str, 
    end_time: str, 
    webids_dict: Dict[str, str],
    pi_webapi_client: PIWebAPIResource,
    context: AssetExecutionContext
) -> Dict:
    """Process a batch of tags and return summarized data (10-minute intervals)"""
    
    data_batch = {}
    for tag in tags:
        if tag not in webids_dict:
            context.log.warning(f"No WebID available for tag: {tag}")
            continue
            
        web_id = webids_dict[tag]
        
        # Use summary endpoint
        data_url = f"/streams/{web_id}/summary"
        query_params = [
            ('startTime', start_time),
            ('endTime', end_time),
            ('summaryType', 'Average'),
            ('summaryType', 'Minimum'), 
            ('summaryType', 'Maximum'),
            ('summaryDuration', '10m'),
            ('calculationBasis', 'TimeWeighted'),
            ('timeType', 'auto'),
            ('timeZone', 'UTC')
        ]
        
        query_string = '&'.join([f"{k}={requests.utils.quote(str(v))}" for k, v in query_params])
        sub_url = f"{data_url}?{query_string}"
        data_batch[tag] = {"Method": "GET", "Resource": sub_url}
    
    if not data_batch:
        context.log.warning("No valid tags found to process")
        return {}
    
    try:
        response = pi_webapi_client.make_request("POST", "/batch", json=data_batch)
        
        if response.status_code not in (200, 207):
            context.log.error(f"Data batch failed with status: {response.status_code}")
            return {}
            
        return response.json()
        
    except JSONDecodeError as e:
        context.log.error(f"Failed to decode data response: {str(e)}")
        return {}
    except Exception as e:
        context.log.error(f"Batch request failed: {str(e)}")
        return {}


def _process_summary_responses(data_json: Dict, context: AssetExecutionContext) -> pd.DataFrame:
    """Process summary response from PI Web API and return a DataFrame"""
    dataframes = []
    
    for tag, tag_data in data_json.items():
        if 'Status' not in tag_data or tag_data['Status'] != 200:
            context.log.warning(f"Error response for tag: {tag}")
            continue
            
        if 'Content' not in tag_data or 'Items' not in tag_data['Content']:
            context.log.warning(f"Unexpected response format for tag: {tag}")
            continue
        
        items = tag_data['Content']['Items']
        if not items:
            continue
        
        # Extract all values into lists
        timestamps = []
        values = []
        types = []
        
        for item in items:
            if 'Type' not in item or 'Value' not in item:
                continue
                
            value_obj = item['Value']
            if 'Timestamp' not in value_obj or 'Value' not in value_obj:
                continue
                
            timestamps.append(value_obj['Timestamp'])
            values.append(value_obj['Value'])
            types.append(item['Type'])
        
        if not timestamps:
            continue
            
        # Create dataframe
        raw_df = pd.DataFrame({
            'tag': tag,
            'Timestamp': timestamps,
            'Value': values,
            'Type': types
        })
        
        # Convert timestamp
        raw_df['Timestamp'] = pd.to_datetime(raw_df['Timestamp'], format='ISO8601')
        
        # Pivot to have one row per timestamp with columns for each Type
        tag_df = raw_df.pivot(index=['tag', 'Timestamp'], columns='Type', values='Value').reset_index()
        tag_df.columns.name = None
        
        dataframes.append(tag_df)
    
    if not dataframes:
        return pd.DataFrame()
        
    all_summaries_df = pd.concat(dataframes, ignore_index=True)
    
    # Ensure UTC timezone
    if all_summaries_df['Timestamp'].dt.tz is None:
        all_summaries_df['Timestamp'] = all_summaries_df['Timestamp'].dt.tz_localize('UTC')
    else:
        all_summaries_df['Timestamp'] = all_summaries_df['Timestamp'].dt.tz_convert('UTC')
    
    return all_summaries_df


@asset(
    description="Monthly turbine data extracted from PI Web API with 10-minute summaries",
    group_name="turbine_data",
    ins={"turbine_webids": AssetIn()},
    io_manager_key="io_manager"
)
def monthly_turbine_data(
    context: AssetExecutionContext,
    config: TurbineDataConfig,
    pi_webapi_client: PIWebAPIResource,
    database: DuckDBResource,
    turbine_webids: pd.DataFrame
) -> pd.DataFrame:
    """
    Extract turbine data for specified months with 10-minute summary intervals.
    Processes data month by month to manage memory usage.
    """
    start_time = time.time()
    
    # Convert WebIDs to dictionary for lookup
    webids_dict = dict(zip(turbine_webids['tag'], turbine_webids['webid']))
    
    # Get all tags to process
    all_tags = turbine_webids['tag'].tolist()
    
    context.log.info(f"Starting data extraction for {len(all_tags)} tags")
    context.log.info(f"Processing years: {config.years}")
    
    # Store all monthly data
    all_monthly_data = []
    total_records = 0
    
    with database.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS turbine")
    
    # Process each month separately
    for year in config.years:
        months = config.months_by_year.get(year, [])
        
        for month in months:
            month_start, month_end = _get_month_range(year, month)
            context.log.info(f"Processing {month_start.strftime('%B %Y')}")
            
            month_data = []
            
            # Process each day in the month
            for day in _date_range(month_start, month_end):
                start_date = day
                end_date = day + timedelta(days=1)
                
                start_time_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
                end_time_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
                
                context.log.info(f"Processing day: {start_date.strftime('%Y-%m-%d')}")
                
                # Process tags in chunks
                for i in range(0, len(all_tags), config.chunk_size):
                    chunk_tags = all_tags[i:i+config.chunk_size]
                    
                    for attempt in range(1, config.max_retries + 1):
                        try:
                            # Get summary data from PI Web API
                            data_json = _process_batch_summary(
                                chunk_tags, start_time_str, end_time_str,
                                webids_dict, pi_webapi_client, context
                            )
                            
                            if not data_json:
                                break
                            
                            # Process responses into DataFrame
                            summaries_df = _process_summary_responses(data_json, context)
                            
                            if not summaries_df.empty:
                                month_data.append(summaries_df)
                                context.log.info(f"Processed chunk with {len(summaries_df)} records")
                            
                            break  # Success, exit retry loop
                            
                        except Exception as e:
                            context.log.error(f"Error processing chunk: {str(e)}")
                            if attempt < config.max_retries:
                                time.sleep(attempt * 2)  # Exponential backoff
                                context.log.info(f"Retrying... (Attempt {attempt + 1})")
                            else:
                                context.log.error("Max retries reached. Skipping chunk.")
                
                # Memory cleanup after each day
                gc.collect()
            
            # Combine month data and store
            if month_data:
                month_df = pd.concat(month_data, ignore_index=True)
                month_df['year'] = year
                month_df['month'] = month
                
                # Store monthly data in DuckDB
                table_name = f"monthly_data_{year}_{month:02d}"
                with database.get_connection() as conn:
                    conn.execute(f"CREATE OR REPLACE TABLE turbine.{table_name} AS SELECT * FROM month_df")
                
                all_monthly_data.append(month_df)
                total_records += len(month_df)
                
                context.log.info(f"Completed {month_start.strftime('%B %Y')}: {len(month_df)} records")
            
            # Clear monthly data from memory
            month_data.clear()
            gc.collect()
    
    # Combine all data for return
    if all_monthly_data:
        final_df = pd.concat(all_monthly_data, ignore_index=True)
    else:
        final_df = pd.DataFrame()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    context.log.info(f"Data extraction completed in {elapsed_time:.2f} seconds")
    context.log.info(f"Total records extracted: {total_records}")
    
    context.add_output_metadata({
        "total_records": total_records,
        "num_tags_processed": len(all_tags),
        "extraction_time_seconds": round(elapsed_time, 2),
        "years_processed": config.years,
        "avg_records_per_tag": round(total_records / len(all_tags), 2) if all_tags else 0
    })
    
    return final_df