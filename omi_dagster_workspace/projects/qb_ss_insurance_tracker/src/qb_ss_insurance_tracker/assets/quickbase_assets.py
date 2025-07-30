# src/qb_ss_insurance_tracker/assets/quickbase_assets.py
import pandas as pd
import requests
import datetime as dt
import time
from typing import Dict, List, Any
from collections import OrderedDict
from dagster import asset, AssetExecutionContext, Config, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource
from qb_ss_insurance_tracker.resources.quickbase_resource import QuickBaseResource
from qb_ss_insurance_tracker.resources.config_resource import FieldMappingResource
from qb_ss_insurance_tracker.utils import format_qb_data, retry_operation

# Constants from your original code
OUTAGE_TABLE = "bjfqxt7qp"
QUERY_STRING = "({'516'.EX.'yes'}AND{'440'.OAF.'11-01-2024'})AND({'Total Downtime'.GT.'720'}OR({'Technology'.EX.'Wind'}AND{'Estimated cost to repair / return to service'.GT.'216000'})OR(({'Technology'.EX.'Solar'}OR{'Technology'.EX.'BESS'})AND{'Estimated cost to repair / return to service'.GT.'160000'}))"

class QuickBaseConfig(Config):
    """Configuration for QuickBase operations"""
    include_manual_claims: bool = True
    manual_claim_ids: List[str] = ['24537','24353','24353','18458','19488','20388','20651','19469','18199','20741','16265','21934','20424','20430','22060','22868','22300','23715','23555','23554','24117','23115','24364','24365','24861','25350','24846','25002','24898','23909','21313','25147','24924','24538','25185','24463','22300','25729','19722','19170','19155','18870','19315','20362','18970','21351','21345','21597','21476','22576','22585','22860','24791']

@asset(
    description="QB field schema and mapping configuration",
    group_name="quickbase_extraction",
    io_manager_key="duckdb_io_manager"
)
def qb_field_schema(
    context: AssetExecutionContext,
    quickbase_client: QuickBaseResource,
    field_mapping: FieldMappingResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Get QuickBase field schema and create field mapping lookup"""
    
    context.log.info("Retrieving QuickBase field schema...")
    
    # Get field mapping
    mapping = field_mapping.load_field_mapping()
    qb_field_mapping = [field['qb_field'] for field in mapping]
    
    # Get QB schema via API
    try:
        response = quickbase_client.make_request(
            'GET', 
            f'/fields?tableId={OUTAGE_TABLE}',
            timeout=30
        )
        
        if response.status_code != 200:
            raise ValueError(f"Failed to get QB schema: {response.status_code} - {response.text}")
            
        schema_data = response.json()
        
        # Filter to relevant fields
        relevant_fields = [field for field in schema_data if field['label'] in qb_field_mapping]
        
        # Create field ID lookup - ENSURE STRING KEYS like the working code
        field_lookup = {'515': "Unit # - Longitude"}  # Special case from original code
        for field in relevant_fields:
            field_lookup[str(field['id'])] = field['label']  # Convert to string!
        
        # Create DataFrame for storage
        df_schema = pd.DataFrame([
            {
                'field_id': fid,  # Store as string
                'field_label': label,
                'field_type': 'coordinate' if 'Longitude' in label else 'standard',
                'extracted_at': pd.Timestamp.now()
            }
            for fid, label in field_lookup.items()
        ])
        
        # Store in DuckDB
        with database.get_connection() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS quickbase")
            conn.execute("CREATE OR REPLACE TABLE quickbase.field_schema AS SELECT * FROM df_schema")
        
        context.log.info(f"Retrieved schema for {len(df_schema)} QB fields")
        
        return MaterializeResult(
            metadata={
                "num_fields": len(df_schema),
                "table_name": "quickbase.field_schema",
                "qb_table_id": OUTAGE_TABLE,
                "fields": MetadataValue.json(list(field_lookup.values()))
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to retrieve QB schema: {e}")
        raise

@asset(
    description="Open insurance claims from QuickBase query",
    group_name="quickbase_extraction",
    io_manager_key="duckdb_io_manager"
)
def qb_open_claims(
    context: AssetExecutionContext,
    config: QuickBaseConfig,
    quickbase_client: QuickBaseResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Extract list of open insurance claims from QuickBase"""
    
    context.log.info("Querying QuickBase for open insurance claims...")
    
    try:
        # Get QB client for legacy query
        qb_client = quickbase_client.get_client()
        
        # Execute query to get open claims
        qb_response = qb_client.doquery(
            query=QUERY_STRING, 
            fields=[3], 
            database=OUTAGE_TABLE
        )['record']
        
        # Extract claim IDs
        open_claim_fids = []
        for claim in qb_response:
            claim_fid = claim['f']['#text']
            if claim_fid not in open_claim_fids:
                open_claim_fids.append(claim_fid)
            else:
                context.log.warning(f"#{claim_fid} is a duplicate claim - removing duplicate")
        
        # Add manual claims if configured
        all_claim_fids = open_claim_fids.copy()
        if config.include_manual_claims:
            for manual_id in config.manual_claim_ids:
                if manual_id not in all_claim_fids:
                    all_claim_fids.append(manual_id)
        
        # Create DataFrame
        df_claims = pd.DataFrame([
            {
                'claim_id': claim_id,
                'source': 'query' if claim_id in open_claim_fids else 'manual',
                'extracted_at': pd.Timestamp.now()
            }
            for claim_id in all_claim_fids
        ])
        
        # Store in DuckDB
        with database.get_connection() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS quickbase")
            conn.execute("CREATE OR REPLACE TABLE quickbase.open_claims AS SELECT * FROM df_claims")
        
        context.log.info(f"Found {len(open_claim_fids)} open claims + {len(config.manual_claim_ids)} manual claims = {len(all_claim_fids)} total")
        
        return MaterializeResult(
            metadata={
                "total_claims": len(all_claim_fids),
                "query_claims": len(open_claim_fids),
                "manual_claims": len(config.manual_claim_ids) if config.include_manual_claims else 0,
                "table_name": "quickbase.open_claims",
                "query_string": QUERY_STRING
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to get open claims: {e}")
        raise

@asset(
    description="Detailed claim data extracted from QuickBase",
    group_name="quickbase_extraction", 
    deps=["qb_open_claims", "qb_field_schema"],
    io_manager_key="duckdb_io_manager"
)
def qb_claim_details(
    context: AssetExecutionContext,
    quickbase_client: QuickBaseResource,
    field_mapping: FieldMappingResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Extract detailed data for each claim from QuickBase"""
    
    context.log.info("Extracting detailed claim data from QuickBase...")
    
    # Load claims and field schema from DuckDB
    with database.get_connection() as conn:
        df_claims = conn.execute("SELECT * FROM quickbase.open_claims").fetch_df()
        df_schema = conn.execute("SELECT * FROM quickbase.field_schema").fetch_df()
    
    # Create field ID lookup - ENSURE STRING KEYS
    field_lookup = {}
    for _, row in df_schema.iterrows():
        field_lookup[str(row['field_id'])] = row['field_label']  # Ensure string keys
    
    context.log.info(f"Field lookup created with {len(field_lookup)} fields: {list(field_lookup.keys())[:5]}...")
    
    # Get field mapping
    mapping = field_mapping.load_field_mapping()
    
    claim_details = []
    failed_claims = []
    
    total_claims = len(df_claims)
    context.log.info(f"Processing {total_claims} claims...")
    
    for idx, row in df_claims.iterrows():
        claim_id = row['claim_id']
        context.log.info(f"Processing claim {idx + 1}/{total_claims}: #{claim_id}")
        
        try:
            # Query individual claim data
            claim_data = _query_single_claim(claim_id, quickbase_client, field_lookup, context)
            
            if claim_data:
                # Format the data
                formatted_data = format_qb_data(mapping, claim_data)
                
                # Add metadata
                formatted_data['claim_id'] = claim_id
                formatted_data['extracted_at'] = pd.Timestamp.now()
                formatted_data['source'] = row['source']
                
                claim_details.append(formatted_data)
            else:
                failed_claims.append(claim_id)
                
        except Exception as e:
            context.log.error(f"Failed to process claim #{claim_id}: {e}")
            failed_claims.append(claim_id)
            continue
    
    # Create DataFrame
    if claim_details:
        df_details = pd.DataFrame(claim_details)
    else:
        # Create empty DataFrame with expected columns
        sample_columns = ['claim_id', 'extracted_at', 'source'] + [f['ss_field'] for f in mapping]
        df_details = pd.DataFrame(columns=sample_columns)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE quickbase.claim_details AS SELECT * FROM df_details")
    
    success_count = len(claim_details)
    failure_count = len(failed_claims)
    
    context.log.info(f"Successfully processed {success_count}/{total_claims} claims")
    if failed_claims:
        context.log.warning(f"Failed claims: {failed_claims}")
    
    return MaterializeResult(
        metadata={
            "successful_claims": success_count,
            "failed_claims": failure_count,
            "total_claims": total_claims,
            "table_name": "quickbase.claim_details",
            "failed_claim_ids": MetadataValue.json(failed_claims) if failed_claims else None,
            "columns": MetadataValue.json(list(df_details.columns)) if not df_details.empty else None
        }
    )

def _query_single_claim(claim_id: str, quickbase_client: QuickBaseResource, field_lookup: Dict[str, str], context: AssetExecutionContext) -> Dict[str, Any]:
    """Query a single claim from QuickBase with retry logic"""
    
    def query_operation():
        # FIX: Use the same query format as the working code
        query_string = "{'Record ID#'.EX.'" + claim_id + "'}"  # Remove extra quotes!
        query_body = {
            "from": OUTAGE_TABLE,
            "where": query_string,
            "select": list(field_lookup.keys())
        }
        
        context.log.debug(f"Query body for claim #{claim_id}: {query_body}")
        
        response = quickbase_client.make_request(
            'POST', 
            '/records/query',
            json=query_body,
            timeout=30
        )
        
        if response.status_code != 200:
            context.log.error(f"QB API returned {response.status_code} for claim #{claim_id}: {response.text}")
            raise ValueError(f"QB API returned {response.status_code}")
        
        return response.json()['data']
    
    # Use retry logic from utils
    raw_data = retry_operation(query_operation, retries=3, delay=2)
    
    if not raw_data:
        context.log.error(f"Failed to get data for claim #{claim_id} after retries")
        return None
    
    # Handle multiple records returned (QB sometimes returns multiple for cost matches)
    if isinstance(raw_data, list):
        context.log.info(f"QB returned {len(raw_data)} records for claim #{claim_id}")
        
        # Find the record that actually matches our claim ID
        target_record = None
        for record in raw_data:
            if str(record.get('3', {}).get('value', '')) == claim_id:
                target_record = record
                break
        
        if not target_record:
            context.log.warning(f"No exact match found for claim #{claim_id}")
            return None
            
        raw_data = target_record
    
    # Convert QB field IDs to labels
    outage_data = {}
    for field_id, value_obj in raw_data.items():
        if field_id in field_lookup:
            field_label = field_lookup[field_id]
            field_value = value_obj.get('value') if isinstance(value_obj, dict) else value_obj
            outage_data[field_label] = field_value
    
    return outage_data