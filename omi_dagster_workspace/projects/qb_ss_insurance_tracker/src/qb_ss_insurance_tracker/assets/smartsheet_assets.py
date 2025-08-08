# src/qb_ss_insurance_tracker/assets/smartsheet_assets.py
import pandas as pd
import requests
import smartsheet
import time
import math
import json
from typing import Dict, List, Any
from collections import defaultdict
from dagster import asset, AssetExecutionContext, Config, MaterializeResult, MetadataValue, AssetIn
from dagster_duckdb import DuckDBResource
from qb_ss_insurance_tracker.resources.smartsheet_resource import SmartsheetResource
from qb_ss_insurance_tracker.resources.config_resource import FieldMappingResource
from qb_ss_insurance_tracker.utils import get_sheet_column_mapping, extract_claim_row_mapping, make_json_serializable
from qb_ss_insurance_tracker.processors.smartsheet_processors import (
    SmartsheetBatchProcessor,
    TemplateProcessor,
    ColumnFormulaManager
)

class SmartsheetConfig(Config):
    """Configuration for Smartsheet operations"""
    main_sheet_name: str = "O&M Insurance Claim Tracker"
    # main_sheet_name: str = "TESTING - Insurance Claim Tracker New" #changed for testing
    template_sheet_name: str = "O&M Insurance Claim Task Tracker Template"
    archive_sheet_name: str = "O&M Insurance Claim ARCHIVE-Below Deductible/Other"
    max_batch_size: int = 100

@asset(
    description="Smartsheet URLs and basic metadata",
    group_name="smartsheet_extraction",
    io_manager_key="duckdb_io_manager"
)
def smartsheet_urls(
    context: AssetExecutionContext,
    config: SmartsheetConfig,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Get Smartsheet URLs and IDs for all relevant sheets"""
    
    context.log.info("Retrieving Smartsheet URLs...")
    
    ss_client = smartsheet_client.get_client()
    
    # Get all sheets
    response = ss_client.Sheets.list_sheets(include_all=True)
    all_sheets = response.data
    
    # Find target sheets
    sheet_info = []
    target_sheets = {
        'main': config.main_sheet_name,
        'template': config.template_sheet_name,
        'archive': config.archive_sheet_name
    }
    
    for sheet_type, sheet_name in target_sheets.items():
        found = False
        for sheet in all_sheets:
            if sheet.name == sheet_name:
                sheet_info.append({
                    'sheet_type': sheet_type,
                    'sheet_name': sheet_name,
                    'sheet_id': str(sheet.id),
                    'sheet_url': f"https://api.smartsheet.com/2.0/sheets/{sheet.id}",
                    'retrieved_at': pd.Timestamp.now()
                })
                found = True
                break
        
        if not found:
            raise ValueError(f"Sheet '{sheet_name}' not found")
    
    df_urls = pd.DataFrame(sheet_info)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS smartsheet")
        conn.execute("CREATE OR REPLACE TABLE smartsheet.sheet_urls AS SELECT * FROM df_urls")
    
    context.log.info(f"Retrieved {len(df_urls)} sheet URLs")
    
    return MaterializeResult(
        metadata={
            "num_sheets": len(df_urls),
            "table_name": "smartsheet.sheet_urls",
            "sheets": MetadataValue.json(make_json_serializable(df_urls.to_dict('records')))
        }
    )

@asset(
    description="Main smartsheet data and structure",
    group_name="smartsheet_extraction",
    deps=["smartsheet_urls"],
    io_manager_key="duckdb_io_manager"
)
def main_sheet_data(
    context: AssetExecutionContext,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Extract data from main insurance claim tracker sheet - FIXED JSON storage"""
    
    context.log.info("Extracting main sheet data...")
    
    # Get main sheet URL
    with database.get_connection() as conn:
        sheet_urls = conn.execute("SELECT * FROM smartsheet.sheet_urls WHERE sheet_type = 'main'").fetch_df()
    
    if sheet_urls.empty:
        raise ValueError("Main sheet URL not found")
    
    sheet_url = sheet_urls.iloc[0]['sheet_url']
    
    # Get sheet data via API
    headers = smartsheet_client.get_headers()
    response = requests.get(sheet_url, headers=headers, verify=False)
    
    if response.status_code != 200:
        raise ValueError(f"Failed to get sheet data: {response.status_code}")
    
    sheet_data = response.json()  # This is already a Python dict
    
    # Extract and store sheet structure
    columns_data = []
    for col in sheet_data['columns']:
        columns_data.append({
            'column_id': col['id'],
            'column_title': col['title'],
            'column_type': col.get('type', 'TEXT_NUMBER'),
            'column_index': col.get('index', 0)
        })
    
    df_columns = pd.DataFrame(columns_data)
    
    # Extract and store row data
    rows_data = []
    for row in sheet_data['rows']:
        row_info = {
            'row_id': row['id'],
            'row_number': row.get('rowNumber', 0),
            'parent_id': row.get('parentId'),
            'indent_level': row.get('indent', 0),
            'extracted_at': pd.Timestamp.now()
        }
        
        # Extract cell values
        for cell in row.get('cells', []):
            col_title = next((c['column_title'] for c in columns_data if c['column_id'] == cell['columnId']), f"col_{cell['columnId']}")
            row_info[f"cell_{col_title}"] = cell.get('displayValue', cell.get('value'))
        
        rows_data.append(row_info)
    
    df_rows = pd.DataFrame(rows_data)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE smartsheet.main_sheet_columns AS SELECT * FROM df_columns")
        conn.execute("CREATE OR REPLACE TABLE smartsheet.main_sheet_rows AS SELECT * FROM df_rows")
        
        # Store raw sheet data as proper JSON string
        sheet_json = json.dumps(sheet_data)  # Convert to proper JSON string
        conn.execute("""
            CREATE OR REPLACE TABLE smartsheet.main_sheet_raw AS 
            SELECT ? as sheet_data, ? as extracted_at
        """, [sheet_json, pd.Timestamp.now()])
    
    context.log.info(f"Extracted {len(df_columns)} columns and {len(df_rows)} rows from main sheet")
    
    return MaterializeResult(
        metadata={
            "num_columns": len(df_columns),
            "num_rows": len(df_rows),
            "table_name": "smartsheet.main_sheet_rows",
            "sheet_id": sheet_data.get('id'),
            "columns": MetadataValue.json(list(df_columns['column_title']))
        }
    )



@asset(
    description="Template sheet data for subtask creation",
    group_name="smartsheet_extraction",
    deps=["smartsheet_urls"],
    io_manager_key="duckdb_io_manager"
)
def template_sheet_data(
    context: AssetExecutionContext,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Extract template sheet data for creating subtasks - FIXED CONNECTION HANDLING"""
    
    context.log.info("Extracting template sheet data...")
    
    # Get template sheet URL
    with database.get_connection() as conn:
        sheet_urls = conn.execute("SELECT * FROM smartsheet.sheet_urls WHERE sheet_type = 'template'").fetch_df()
    
    if sheet_urls.empty:
        raise ValueError("Template sheet URL not found")
    
    sheet_url = sheet_urls.iloc[0]['sheet_url']
    
    # Get sheet data
    headers = smartsheet_client.get_headers()
    response = requests.get(sheet_url, headers=headers, verify=False)
    
    if response.status_code != 200:
        raise ValueError(f"Failed to get template data: {response.status_code}")
    
    template_data = response.json()  # This is already a Python dict
    
    # Process template rows for analysis first
    template_rows = []
    for idx, row in enumerate(template_data['rows'], 1):
        row_info = {
            'template_row_index': idx,
            'row_id': row['id'],
            'indent_level': row.get('indent', 0),
            'num_cells': len(row.get('cells', [])),
            'extracted_at': pd.Timestamp.now()
        }
        template_rows.append(row_info)
    
    df_template = pd.DataFrame(template_rows)
    
    # Also get main sheet data for raw storage
    with database.get_connection() as conn:
        main_sheet_urls = conn.execute("SELECT * FROM smartsheet.sheet_urls WHERE sheet_type = 'main'").fetch_df()
    
    main_data = None
    if not main_sheet_urls.empty:
        main_url = main_sheet_urls.iloc[0]['sheet_url']
        main_response = requests.get(main_url, headers=headers, verify=False)
        if main_response.status_code == 200:
            main_data = main_response.json()
    
    # Store everything in a single connection context
    with database.get_connection() as conn:
        # Store template raw data as proper JSON string
        template_json = json.dumps(template_data)
        conn.execute("""
            CREATE OR REPLACE TABLE smartsheet.template_sheet_raw AS 
            SELECT ? as template_data, ? as extracted_at
        """, [template_json, pd.Timestamp.now()])
        
        # Store main sheet raw data if we got it
        if main_data:
            main_json = json.dumps(main_data)
            conn.execute("""
                CREATE OR REPLACE TABLE smartsheet.main_sheet_raw AS 
                SELECT ? as sheet_data, ? as extracted_at
            """, [main_json, pd.Timestamp.now()])
        
        # Store processed template data
        conn.execute("CREATE OR REPLACE TABLE smartsheet.template_sheet_data AS SELECT * FROM df_template")
    
    context.log.info(f"Extracted {len(df_template)} template rows")
    
    return MaterializeResult(
        metadata={
            "num_template_rows": len(df_template),
            "table_name": "smartsheet.template_sheet_data",
            "template_id": template_data.get('id'),
            "num_columns": len(template_data.get('columns', [])),
            "main_sheet_data_stored": main_data is not None
        }
    )


@asset(
    description="Archive sheet data for filtering",
    group_name="smartsheet_extraction", 
    deps=["smartsheet_urls"],
    io_manager_key="duckdb_io_manager"
)
def archive_sheet_claims(
    context: AssetExecutionContext,
    smartsheet_client: SmartsheetResource,
    field_mapping: FieldMappingResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Extract claim IDs from archive sheet to filter out archived claims"""
    
    context.log.info("Extracting archived claim IDs...")
    
    # Get archive sheet URL
    with database.get_connection() as conn:
        sheet_urls = conn.execute("SELECT * FROM smartsheet.sheet_urls WHERE sheet_type = 'archive'").fetch_df()
    
    if sheet_urls.empty:
        raise ValueError("Archive sheet URL not found")
    
    sheet_url = sheet_urls.iloc[0]['sheet_url']
    
    # Get sheet data
    headers = smartsheet_client.get_headers()
    response = requests.get(sheet_url, headers=headers, verify=False)
    
    if response.status_code != 200:
        raise ValueError(f"Failed to get archive data: {response.status_code}")
    
    archive_data = response.json()
    
    # Find the record ID column
    mapping = field_mapping.load_field_mapping()
    record_id_field = "Record # in Outage Management (QB)*"
    
    record_id_col_id = None
    for col in archive_data['columns']:
        if col['title'] == record_id_field:
            record_id_col_id = col['id']
            break
    
    if not record_id_col_id:
        context.log.warning(f"Record ID column '{record_id_field}' not found in archive sheet")
        df_archived = pd.DataFrame(columns=['claim_id', 'archived_at'])
    else:
        # Extract claim IDs from archive
        archived_claims = []
        for row in archive_data['rows']:
            for cell in row['cells']:
                if cell['columnId'] == record_id_col_id and len(cell) > 1:
                    claim_id = cell['displayValue']
                    if claim_id:
                        archived_claims.append({
                            'claim_id': str(claim_id),
                            'archived_at': pd.Timestamp.now(),
                            'row_id': row['id']
                        })
        
        df_archived = pd.DataFrame(archived_claims).drop_duplicates(subset=['claim_id'])
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE smartsheet.archived_claims AS SELECT * FROM df_archived")
    
    context.log.info(f"Found {len(df_archived)} archived claims")
    
    return MaterializeResult(
        metadata={
            "num_archived_claims": len(df_archived),
            "table_name": "smartsheet.archived_claims",
            "record_id_column": record_id_field
        }
    )

@asset(
    description="Current claims being tracked in main sheet",
    group_name="smartsheet_extraction",
    deps=["main_sheet_data"],
    io_manager_key="duckdb_io_manager"
)
def current_tracked_claims(
    context: AssetExecutionContext,
    field_mapping: FieldMappingResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Extract claim IDs currently tracked in the main sheet"""
    
    context.log.info("Extracting currently tracked claims...")
    
    # Load main sheet data
    with database.get_connection() as conn:
        df_columns = conn.execute("SELECT * FROM smartsheet.main_sheet_columns").fetch_df()
        df_rows = conn.execute("SELECT * FROM smartsheet.main_sheet_rows").fetch_df()
    
    # Find record ID column
    record_id_field = "Record # in Outage Management (QB)*"
    record_id_col_id = None
    
    for _, col in df_columns.iterrows():
        if col['column_title'] == record_id_field:
            record_id_col_id = col['column_id']
            break
    
    if not record_id_col_id:
        context.log.warning(f"Record ID column '{record_id_field}' not found")
        df_tracked = pd.DataFrame(columns=['claim_id', 'row_id', 'tracked_at'])
    else:
        # Extract tracked claims
        tracked_claims = []
        cell_col_name = f"cell_{record_id_field}"
        
        for _, row in df_rows.iterrows():
            if cell_col_name in row and pd.notna(row[cell_col_name]) and row[cell_col_name] != '':
                claim_id = str(row[cell_col_name])
                tracked_claims.append({
                    'claim_id': claim_id,
                    'row_id': row['row_id'],
                    'tracked_at': pd.Timestamp.now(),
                    'parent_id': row.get('parent_id'),
                    'indent_level': row.get('indent_level', 0)
                })
        
        df_tracked = pd.DataFrame(tracked_claims).drop_duplicates(subset=['claim_id'])
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE smartsheet.current_tracked_claims AS SELECT * FROM df_tracked")
    
    context.log.info(f"Found {len(df_tracked)} currently tracked claims")
    
    return MaterializeResult(
        metadata={
            "num_tracked_claims": len(df_tracked),
            "table_name": "smartsheet.current_tracked_claims",
            "record_id_column": record_id_field
        }
    )

@asset(
    description="Claims categorized for update vs add operations",
    group_name="data_processing",
    deps=["qb_claim_details", "current_tracked_claims", "archive_sheet_claims"],
    io_manager_key="duckdb_io_manager"
)
def claims_for_processing(
    context: AssetExecutionContext,
    database: DuckDBResource,
    field_mapping: FieldMappingResource
) -> MaterializeResult:
    """Categorize claims into those needing updates vs new additions"""
    
    context.log.info("Categorizing claims for processing...")
    
    # Load all required data
    with database.get_connection() as conn:
        df_qb_claims = conn.execute("SELECT * FROM quickbase.claim_details").fetch_df()
        df_tracked = conn.execute("SELECT * FROM smartsheet.current_tracked_claims").fetch_df()
        df_archived = conn.execute("SELECT * FROM smartsheet.archived_claims").fetch_df()
    
    # Filter out archived claims
    archived_claim_ids = set(df_archived['claim_id'].astype(str)) if not df_archived.empty else set()
    qb_claims_active = df_qb_claims[~df_qb_claims['claim_id'].astype(str).isin(archived_claim_ids)].copy()
    
    # Get currently tracked claim IDs
    tracked_claim_ids = set(df_tracked['claim_id'].astype(str)) if not df_tracked.empty else set()
    

    # ADD: Get field mapping for transformation
    # mapping = field_mapping.load_field_mapping()
    # Categorize claims
    update_claims = []
    add_claims = []
    
    for _, qb_claim in qb_claims_active.iterrows():
        claim_id = str(qb_claim['claim_id'])
        # Convert row to dict and apply field mapping
        qb_dict = qb_claim.to_dict()
        mapped_dict = field_mapping.map_qb_to_ss(qb_dict)  # QB→SS field names
        
        if claim_id in tracked_claim_ids:
            # Existing claim - needs update
            row_info = df_tracked[df_tracked['claim_id'] == claim_id].iloc[0]
            mapped_dict['sheet_row_id'] = row_info['row_id']
            mapped_dict['operation'] = 'update'
            update_claims.append(mapped_dict)
        else:
            # New claim - needs addition  
            mapped_dict['operation'] = 'add'
            add_claims.append(mapped_dict)
    
    # Create DataFrames with proper handling for empty cases
    if update_claims:
        df_updates = pd.DataFrame(update_claims)
    else:
        # Create empty DataFrame with Smartsheet field names
        sample_ss_fields = [f['ss_field'] for f in field_mapping.load_field_mapping()]
        sample_columns = sample_ss_fields + ['sheet_row_id', 'operation', 'claim_id', 'extracted_at', 'source']
        df_updates = pd.DataFrame(columns=sample_columns)
    
    if add_claims:
        df_adds = pd.DataFrame(add_claims)
    else:
        # Create empty DataFrame with Smartsheet field names
        sample_ss_fields = [f['ss_field'] for f in field_mapping.load_field_mapping()]
        sample_columns = sample_ss_fields + ['operation', 'claim_id', 'extracted_at', 'source']
        df_adds = pd.DataFrame(columns=sample_columns)
    
    # Store in DuckDB with proper table creation
    with database.get_connection() as conn:
        # Create tables with proper column definitions
        if not df_updates.empty:
            conn.execute("CREATE OR REPLACE TABLE smartsheet.claims_for_update AS SELECT * FROM df_updates")
        else:
            # Create empty table with proper schema
            context.log.info("Creating empty claims_for_update table with schema")
            columns_sql = ", ".join([f'"{col}" VARCHAR' for col in df_updates.columns])
            conn.execute(f"CREATE OR REPLACE TABLE smartsheet.claims_for_update ({columns_sql})")
        
        if not df_adds.empty:
            conn.execute("CREATE OR REPLACE TABLE smartsheet.claims_for_add AS SELECT * FROM df_adds")
        else:
            # Create empty table with proper schema
            context.log.info("Creating empty claims_for_add table with schema")
            columns_sql = ", ".join([f'"{col}" VARCHAR' for col in df_adds.columns])
            conn.execute(f"CREATE OR REPLACE TABLE smartsheet.claims_for_add ({columns_sql})")
    
    context.log.info(f"Categorized claims: {len(df_updates)} for update, {len(df_adds)} for addition")
    
    return MaterializeResult(
        metadata={
            "claims_for_update": len(df_updates),
            "claims_for_add": len(df_adds),
            "total_active_claims": len(qb_claims_active),
            "archived_claims_filtered": len(archived_claim_ids),
            "update_table": "smartsheet.claims_for_update",
            "add_table": "smartsheet.claims_for_add"
        }
    )

@asset(
    description="Batch update operations executed on Smartsheet",
    group_name="smartsheet_operations",
    deps=["claims_for_processing", "main_sheet_data"],
    io_manager_key="duckdb_io_manager"
)
def batch_update_results(
    context: AssetExecutionContext,
    config: SmartsheetConfig,
    smartsheet_client: SmartsheetResource,
    field_mapping: FieldMappingResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Execute batch updates on existing Smartsheet rows"""
    
    context.log.info("Executing batch updates on Smartsheet...")
    
    # Load update claims and sheet structure
    with database.get_connection() as conn:
        df_updates = conn.execute("SELECT * FROM smartsheet.claims_for_update").fetch_df()
        df_columns = conn.execute("SELECT * FROM smartsheet.main_sheet_columns").fetch_df()
        sheet_urls = conn.execute("SELECT * FROM smartsheet.sheet_urls WHERE sheet_type = 'main'").fetch_df()
    
    if df_updates.empty:
        context.log.info("No claims require updates")
        df_results = pd.DataFrame(columns=['claim_id', 'operation', 'status', 'batch_id', 'processed_at'])
    else:
        # Get sheet ID
        sheet_id = sheet_urls.iloc[0]['sheet_id']
        
        # Create column mapping
        mapping = field_mapping.load_field_mapping()
        column_mapping = {}
        for _, col in df_columns.iterrows():
            for field in mapping:
                if col['column_title'] == field['ss_field']:
                    column_mapping[field['ss_field']] = col['column_id']
        
        # Execute batch updates
        ss_client = smartsheet_client.get_client()
        batch_processor = SmartsheetBatchProcessor(
            ss_client=ss_client,
            sheet_id=int(sheet_id),
            max_batch_size=config.max_batch_size
        )
        
        # Disable formulas before updates
        context.log.info("Disabling column formulas...")
        # formula_jail = _disable_column_formulas(ss_client, sheet_id, mapping, context)
        formula_manager = ColumnFormulaManager(ss_client, sheet_id)
        field_names_to_check = [field['ss_field'] for field in mapping]
        formula_manager.disable_formulas(field_names_to_check)
        
        try:
            # Process updates
            results = batch_processor.process_update_batches(df_updates.to_dict('records'), column_mapping)
            
            # Create results DataFrame
            update_results = []
            for i, (_, claim) in enumerate(df_updates.iterrows()):
                batch_id = i // config.max_batch_size
                status = 'success' if i < results.get('successful', 0) else 'failed'
                
                update_results.append({
                    'claim_id': claim["Record # in Outage Management (QB)*"],
                    'operation': 'update',
                    'status': status,
                    'batch_id': batch_id,
                    'sheet_row_id': claim.get('sheet_row_id'),
                    'processed_at': pd.Timestamp.now()
                })
            
            df_results = pd.DataFrame(update_results)
            
        finally:
            # Re-enable formulas
            context.log.info("Re-enabling column formulas...")
            # _enable_column_formulas(ss_client, sheet_id, formula_jail, context)
            formula_manager.enable_formulas()
    
    # Store results
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE smartsheet.batch_update_results AS SELECT * FROM df_results")
    
    successful = len(df_results[df_results['status'] == 'success']) if not df_results.empty else 0
    failed = len(df_results[df_results['status'] == 'failed']) if not df_results.empty else 0
    
    context.log.info(f"Batch updates completed: {successful} successful, {failed} failed")
    
    return MaterializeResult(
        metadata={
            "successful_updates": successful,
            "failed_updates": failed,
            "total_updates": len(df_results),
            "table_name": "smartsheet.batch_update_results"
        }
    )

@asset(
    description="Batch add operations executed on Smartsheet",
    group_name="smartsheet_operations", 
    deps=["claims_for_processing", "main_sheet_data"],
    io_manager_key="duckdb_io_manager"
)
def batch_add_results(
    context: AssetExecutionContext,
    config: SmartsheetConfig,
    smartsheet_client: SmartsheetResource,
    field_mapping: FieldMappingResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Execute batch additions of new parent rows to Smartsheet"""
    
    context.log.info("Executing batch additions on Smartsheet...")
    
    # Load add claims and sheet structure
    with database.get_connection() as conn:
        df_adds = conn.execute("SELECT * FROM smartsheet.claims_for_add").fetch_df()
        df_columns = conn.execute("SELECT * FROM smartsheet.main_sheet_columns").fetch_df()
        sheet_urls = conn.execute("SELECT * FROM smartsheet.sheet_urls WHERE sheet_type = 'main'").fetch_df()
    
    if df_adds.empty:
        context.log.info("No new claims to add")
        df_results = pd.DataFrame(columns=['claim_id', 'operation', 'status', 'batch_id', 'new_row_id', 'processed_at'])
    else:
        # Get sheet ID
        sheet_id = sheet_urls[sheet_urls.sheet_type=='main']['sheet_id']
        
        # Create column mapping
        mapping = field_mapping.load_field_mapping()
        column_mapping = {}
        for _, col in df_columns.iterrows():
            for field in mapping:
                if col['column_title'] == field['ss_field']:
                    column_mapping[field['ss_field']] = col['column_id']
            # Add formula column
            if col['column_title'] == "Claim Name / Task Name":
                column_mapping["Claim Name / Task Name"] = col['column_id']
        
        # Execute batch additions
        ss_client = smartsheet_client.get_client()
        batch_processor = SmartsheetBatchProcessor(
            ss_client=ss_client,
            sheet_id=int(sheet_id),
            max_batch_size=config.max_batch_size
        )
        
        # Process additions
        results = batch_processor.process_add_batches(df_adds.to_dict('records'), column_mapping)
        
        # Create results DataFrame
        add_results = []
        new_row_ids = results.get('new_row_ids', [])
        
        for i, (_, claim) in enumerate(df_adds.iterrows()):
            batch_id = i // config.max_batch_size
            status = 'success' if i < results.get('successful', 0) else 'failed'
            new_row_id = new_row_ids[i] if i < len(new_row_ids) else None
            
            add_results.append({
                'claim_id': claim["Record # in Outage Management (QB)*"],
                'operation': 'add',
                'status': status,
                'batch_id': batch_id,
                'new_row_id': new_row_id,
                'processed_at': pd.Timestamp.now()
            })
        
        df_results = pd.DataFrame(add_results)
    
    # Store results
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE smartsheet.batch_add_results AS SELECT * FROM df_results")
    
    successful = len(df_results[df_results['status'] == 'success']) if not df_results.empty else 0
    failed = len(df_results[df_results['status'] == 'failed']) if not df_results.empty else 0
    
    context.log.info(f"Batch additions completed: {successful} successful, {failed} failed")
    
    return MaterializeResult(
        metadata={
            "successful_additions": successful,
            "failed_additions": failed,
            "total_additions": len(df_results),
            "table_name": "smartsheet.batch_add_results",
            "new_row_ids": MetadataValue.json(df_results[df_results['status'] == 'success']['new_row_id'].tolist()) if not df_results.empty else None
        }
    )


@asset(
    description="Subtasks added to new parent rows",
    group_name="smartsheet_operations",
    deps=["batch_add_results", "template_sheet_data"],
    io_manager_key="duckdb_io_manager"
)
def subtask_creation_results(
    context: AssetExecutionContext,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource
) -> MaterializeResult:
    """Add subtasks to newly created parent rows using fixed JSON parsing"""
    
    context.log.info("Adding subtasks to new parent rows...")
    start_time = time.time()
    
    # Load data from DuckDB
    with database.get_connection() as conn:
        df_add_results = conn.execute(
            "SELECT * FROM smartsheet.batch_add_results WHERE status = 'success'"
        ).fetch_df()
        
        if df_add_results.empty:
            context.log.info("No new parent rows to process")
            # Create empty result
            df_empty = pd.DataFrame(columns=[
                'parent_claim_id', 'parent_row_id', 'subtasks_added', 
                'status', 'processed_at'
            ])
            conn.execute(
                "CREATE OR REPLACE TABLE smartsheet.subtask_creation_results AS SELECT * FROM df_empty"
            )
            return MaterializeResult(
                metadata={
                    "total_subtasks_added": 0,
                    "successful_parents": 0,
                    "table_name": "smartsheet.subtask_creation_results"
                }
            )
        
        # Get sheet IDs
        sheet_info = conn.execute(
            "SELECT sheet_id FROM smartsheet.sheet_urls WHERE sheet_type = 'main'"
        ).fetch_df()
        template_info = conn.execute(
            "SELECT sheet_id FROM smartsheet.sheet_urls WHERE sheet_type = 'template'"
        ).fetch_df()
    
    # Initialize SDK client
    ss_client = smartsheet_client.get_client()
    main_sheet_id = int(sheet_info.iloc[0]['sheet_id'])
    template_sheet_id = int(template_info.iloc[0]['sheet_id'])
    
    # USE THE NEW TemplateProcessor
    template_processor = TemplateProcessor(ss_client, template_sheet_id, main_sheet_id)
    
    # Process each new parent row
    results = []
    for _, row in df_add_results.iterrows():
        if pd.notna(row['new_row_id']):
            parent_row_id = int(row['new_row_id'])
            claim_id = row['claim_id']
            
            context.log.info(f"Adding subtasks for claim {claim_id}")
            
            # Add subtasks using the clean TemplateProcessor
            subtasks_added = template_processor.apply_to_parent(
                parent_row_id, context
            )
            
            results.append({
                'parent_claim_id': claim_id,
                'parent_row_id': parent_row_id,
                'subtasks_added': subtasks_added,
                'status': 'success' if subtasks_added > 0 else 'failed',
                'processed_at': pd.Timestamp.now()
            })
    
    # Store and return results
    df_results = pd.DataFrame(results)
    with database.get_connection() as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE smartsheet.subtask_creation_results AS SELECT * FROM df_results"
        )
    
    elapsed = time.time() - start_time
    total_subtasks = df_results['subtasks_added'].sum()
    
    context.log.info(
        f"Added {total_subtasks} subtasks in {elapsed:.1f} seconds "
        f"({elapsed/len(results):.1f}s per claim)"
    )
    
    return MaterializeResult(
        metadata={
            "total_subtasks_added": int(total_subtasks),
            "successful_parents": len(df_results[df_results['status'] == 'success']),
            "processing_time_seconds": elapsed,
            "table_name": "smartsheet.subtask_creation_results"
        }
    )


@asset(
    description="Final processing summary and performance metrics",
    group_name="reporting",
    deps=["batch_update_results", "batch_add_results", "subtask_creation_results"],
    io_manager_key="duckdb_io_manager"
)
def processing_summary(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> MaterializeResult:
    """Generate final processing summary with performance metrics"""
    
    context.log.info("Generating processing summary...")
    
    # Load all results
    with database.get_connection() as conn:
        # Check if tables exist and load data
        try:
            df_updates = conn.execute("SELECT * FROM smartsheet.batch_update_results").fetch_df()
        except Exception as e:
            context.log.warning(f"No batch_update_results table found: {e}")
            df_updates = pd.DataFrame()
            
        try:
            df_adds = conn.execute("SELECT * FROM smartsheet.batch_add_results").fetch_df()
        except Exception as e:
            context.log.warning(f"No batch_add_results table found: {e}")
            df_adds = pd.DataFrame()
            
        try:
            df_subtasks = conn.execute("SELECT * FROM smartsheet.subtask_creation_results").fetch_df()
        except Exception as e:
            context.log.warning(f"No subtask_creation_results table found: {e}")
            df_subtasks = pd.DataFrame()
        
        # Get total counts from source data
        try:
            total_qb_claims = conn.execute("SELECT COUNT(*) as count FROM quickbase.claim_details").fetch_df().iloc[0]['count']
        except Exception:
            total_qb_claims = 0
            
        try:
            total_archived = conn.execute("SELECT COUNT(*) as count FROM smartsheet.archived_claims").fetch_df().iloc[0]['count']
        except Exception:
            total_archived = 0
    
    # Calculate summary metrics with safe handling for empty DataFrames
    summary = {
        'processing_date': pd.Timestamp.now(),
        'total_qb_claims': int(total_qb_claims),
        'total_archived_claims': int(total_archived),
        'claims_processed_for_update': len(df_updates),
        'claims_processed_for_add': len(df_adds),
        'successful_updates': len(df_updates[df_updates['status'] == 'success']) if not df_updates.empty else 0,
        'failed_updates': len(df_updates[df_updates['status'] == 'failed']) if not df_updates.empty else 0,
        'successful_additions': len(df_adds[df_adds['status'] == 'success']) if not df_adds.empty else 0,
        'failed_additions': len(df_adds[df_adds['status'] == 'failed']) if not df_adds.empty else 0,
        'total_subtasks_added': int(df_subtasks['subtasks_added'].sum()) if not df_subtasks.empty else 0,
        'parents_with_subtasks': len(df_subtasks[df_subtasks['status'] == 'success']) if not df_subtasks.empty else 0,
    }
    
    # Calculate performance metrics with safe handling
    if not df_updates.empty and 'batch_id' in df_updates.columns:
        update_batches = df_updates['batch_id'].nunique()
        summary['update_batches_processed'] = update_batches
    else:
        summary['update_batches_processed'] = 0
    
    if not df_adds.empty and 'batch_id' in df_adds.columns:
        add_batches = df_adds['batch_id'].nunique()
        summary['add_batches_processed'] = add_batches
    else:
        summary['add_batches_processed'] = 0
    
    # Create summary DataFrame
    df_summary = pd.DataFrame([summary])
    
    # Store summary with schema creation
    with database.get_connection() as conn:
        # Create reporting schema if it doesn't exist
        conn.execute("CREATE SCHEMA IF NOT EXISTS reporting")
        
        # Store the summary
        conn.execute("CREATE OR REPLACE TABLE reporting.processing_summary AS SELECT * FROM df_summary")
    
    # Log summary
    context.log.info("=== PROCESSING SUMMARY ===")
    context.log.info(f"Total QB Claims: {summary['total_qb_claims']}")
    context.log.info(f"Updates: {summary['successful_updates']}/{summary['claims_processed_for_update']} successful")
    context.log.info(f"Additions: {summary['successful_additions']}/{summary['claims_processed_for_add']} successful")
    context.log.info(f"Subtasks: {summary['total_subtasks_added']} added across {summary['parents_with_subtasks']} parents")
    
    return MaterializeResult(
        metadata={
            **{k: int(v) if isinstance(v, (int, float)) else str(v) for k, v in summary.items()},
            "table_name": "reporting.processing_summary"
        }
    )
