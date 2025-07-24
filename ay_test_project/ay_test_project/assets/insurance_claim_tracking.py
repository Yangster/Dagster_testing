# ay_test_project/ay_test_project/assets/insurance_claim_tracking.py
import pandas as pd
import time
import math
from datetime import datetime
from typing import Dict, List, Tuple
from dagster import (
    asset, 
    AssetExecutionContext, 
    Config, 
    MaterializeResult, 
    MetadataValue,
    AssetIn,
    DagsterType
)
from dagster_duckdb import DuckDBResource
from ay_test_project.resources.quickbase_resource import QuickBaseResource
from ay_test_project.resources.smartsheet_resource import SmartsheetResource
from ay_test_project.utils.field_mappings import load_field_mapping


class InsuranceClaimConfig(Config):
    """Configuration for insurance claim processing"""
    main_sheet_name: str = "O&M Insurance Claim Tracker"
    template_sheet_name: str = "O&M Insurance Claim Task Tracker Template"
    archive_sheet_name: str = "O&M Insurance Claim ARCHIVE-Below Deductible/Other"
    max_batch_size: int = 100
    max_retries: int = 3
    # Manual claims to include (from your original script)
    manual_claims: List[str] = [
        '24537','24353','24353','18458','19488','20388','20651','19469',
        '18199','20741','16265','21934','20424','20430','22060','22868',
        '22300','23715','23555','23554','24117','23115','24364','24365',
        '24861','25350','24846','25002','24898','23909','21313','25147',
        '24924','24538','25185','24463','22300','25729','19722','19170',
        '19155','18870','19315','20362','18970','21351','21345','21597',
        '21476','22576','22585','22860','24791'
    ]


@asset(
    description="Field mapping configuration for QB to Smartsheet data transformation",
    group_name="insurance_claims",
    io_manager_key="io_manager"
)
def field_mapping_config(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> pd.DataFrame:
    """
    Load and store field mapping configuration that maps QuickBase fields to Smartsheet fields.
    """
    context.log.info("Loading field mapping configuration...")
    
    # Load the field mapping from your config
    field_mapping = load_field_mapping()
    
    # Convert to DataFrame for storage
    df_mapping = pd.DataFrame(field_mapping)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS insurance")
        conn.execute("CREATE OR REPLACE TABLE insurance.field_mapping AS SELECT * FROM df_mapping")
    
    context.log.info(f"Loaded {len(df_mapping)} field mappings")
    
    context.add_output_metadata({
        "num_mappings": len(df_mapping),
        "qb_fields": MetadataValue.json(df_mapping['qb_field'].tolist()),
        "ss_fields": MetadataValue.json(df_mapping['ss_field'].tolist()),
    })
    
    return df_mapping


@asset(
    description="Active QuickBase claims that meet insurance criteria",
    group_name="insurance_claims",
    io_manager_key="io_manager"
)
def active_qb_claims(
    context: AssetExecutionContext,
    config: InsuranceClaimConfig,
    qb_client: QuickBaseResource,
    database: DuckDBResource
) -> pd.DataFrame:
    """
    Extract all active claims from QuickBase that meet insurance thresholds.
    """
    context.log.info("Fetching active claims from QuickBase...")
    
    # Get open claims using your existing logic
    qb_client_instance = qb_client.get_client()
    open_claims = qb_client.get_open_claims(qb_client_instance)
    
    # Combine with manual claims
    all_claims = list(set(config.manual_claims + open_claims))
    
    context.log.info(f"Found {len(open_claims)} open claims from QB")
    context.log.info(f"Including {len(config.manual_claims)} manual claims")
    context.log.info(f"Total unique claims: {len(all_claims)}")
    
    # Create DataFrame
    df_claims = pd.DataFrame({
        'claim_id': all_claims,
        'source': ['manual' if claim in config.manual_claims else 'qb_query' for claim in all_claims],
        'extracted_at': datetime.now()
    })
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS insurance")
        conn.execute("CREATE OR REPLACE TABLE insurance.active_qb_claims AS SELECT * FROM df_claims")
    
    context.add_output_metadata({
        "total_claims": len(all_claims),
        "manual_claims": len([c for c in all_claims if c in config.manual_claims]),
        "qb_query_claims": len([c for c in all_claims if c not in config.manual_claims]),
    })
    
    return df_claims


@asset(
    description="Current Smartsheet claim data and metadata",
    group_name="insurance_claims",
    io_manager_key="io_manager"
)
def current_smartsheet_data(
    context: AssetExecutionContext,
    config: InsuranceClaimConfig,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource
) -> pd.DataFrame:
    """
    Extract current claim data from Smartsheet to determine what needs updating.
    """
    context.log.info("Fetching current Smartsheet data...")
    
    ss_client = smartsheet_client.get_client()
    
    # Get sheet URLs
    main_sheet_url = smartsheet_client.get_sheet_url(ss_client, config.main_sheet_name)
    archive_sheet_url = smartsheet_client.get_sheet_url(ss_client, config.archive_sheet_name)
    
    # Get sheet data
    main_sheet_data = smartsheet_client.get_sheet_data(main_sheet_url)
    archive_sheet_data = smartsheet_client.get_sheet_data(archive_sheet_url)
    
    # Extract claim IDs and row mappings
    field_mapping = load_field_mapping()
    sheet_record_id_col_name = "Record # in Outage Management (QB)*"
    
    # Get column IDs
    main_col_ids = smartsheet_client.get_col_ids(field_mapping, main_sheet_data)
    archive_col_ids = smartsheet_client.get_col_ids(field_mapping, archive_sheet_data)
    
    record_col_id = main_col_ids[sheet_record_id_col_name]
    archive_record_col_id = archive_col_ids[sheet_record_id_col_name]
    
    # Get relevant row IDs
    main_claim_lookup = smartsheet_client.get_relevant_row_ids(
        main_sheet_data['rows'], record_col_id
    )
    archive_claim_lookup = smartsheet_client.get_relevant_row_ids(
        archive_sheet_data['rows'], archive_record_col_id
    )
    
    # Create combined DataFrame
    main_claims = [{'claim_id': claim_id, 'sheet_row_id': row_id, 'sheet_type': 'main'} 
                   for claim_id, row_id in main_claim_lookup.items()]
    archive_claims = [{'claim_id': claim_id, 'sheet_row_id': row_id, 'sheet_type': 'archive'} 
                      for claim_id, row_id in archive_claim_lookup.items()]
    
    all_sheet_claims = main_claims + archive_claims
    df_sheet_claims = pd.DataFrame(all_sheet_claims)
    
    if df_sheet_claims.empty:
        df_sheet_claims = pd.DataFrame(columns=['claim_id', 'sheet_row_id', 'sheet_type'])
    
    # Store metadata for downstream use
    metadata_df = pd.DataFrame([{
        'main_sheet_id': main_sheet_url[-16:],
        'main_sheet_url': main_sheet_url,
        'archive_sheet_url': archive_sheet_url,
        'total_main_claims': len(main_claims),
        'total_archive_claims': len(archive_claims),
        'extracted_at': datetime.now()
    }])
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS insurance")
        conn.execute("CREATE OR REPLACE TABLE insurance.current_sheet_claims AS SELECT * FROM df_sheet_claims")
        conn.execute("CREATE OR REPLACE TABLE insurance.sheet_metadata AS SELECT * FROM metadata_df")
    
    context.log.info(f"Found {len(main_claims)} claims in main sheet")
    context.log.info(f"Found {len(archive_claims)} claims in archive sheet")
    
    context.add_output_metadata({
        "main_sheet_claims": len(main_claims),
        "archive_sheet_claims": len(archive_claims),
        "total_tracked_claims": len(all_sheet_claims),
        "main_sheet_id": main_sheet_url[-16:],
    })
    
    return df_sheet_claims


@asset(
    description="QuickBase data for all active claims formatted for Smartsheet",
    group_name="insurance_claims",
    ins={
        "active_qb_claims": AssetIn(),
        "current_smartsheet_data": AssetIn(),
        "field_mapping_config": AssetIn()
    },
    io_manager_key="io_manager"
)
def qb_claim_data(
    context: AssetExecutionContext,
    config: InsuranceClaimConfig,
    qb_client: QuickBaseResource,
    database: DuckDBResource,
    active_qb_claims: pd.DataFrame,
    current_smartsheet_data: pd.DataFrame,
    field_mapping_config: pd.DataFrame
) -> pd.DataFrame:
    """
    Extract and format QuickBase data for all active claims.
    """
    context.log.info("Extracting QuickBase data for all active claims...")
    
    # Get archived claims to exclude
    archived_claims = set(current_smartsheet_data[
        current_smartsheet_data['sheet_type'] == 'archive'
    ]['claim_id'].tolist())
    
    # Filter out archived claims
    claims_to_process = active_qb_claims[
        ~active_qb_claims['claim_id'].isin(archived_claims)
    ]['claim_id'].tolist()
    
    context.log.info(f"Processing {len(claims_to_process)} claims (excluding {len(archived_claims)} archived)")
    
    # Extract QB data for each claim
    qb_data_list = []
    field_mapping = field_mapping_config.to_dict('records')
    
    for i, claim_id in enumerate(claims_to_process, 1):
        context.log.info(f"Processing claim {i}/{len(claims_to_process)}: #{claim_id}")
        
        try:
            # Get QB data
            qb_data = qb_client.outage_tracker_query(claim_id)
            if qb_data is None:
                context.log.warning(f"No QB data found for claim #{claim_id}")
                continue
            
            # Format for Smartsheet
            formatted_data = qb_client.format_qb_data(field_mapping, qb_data)
            formatted_data['claim_id'] = claim_id
            formatted_data['processed_at'] = datetime.now()
            
            qb_data_list.append(formatted_data)
            
        except Exception as e:
            context.log.error(f"Error processing claim #{claim_id}: {str(e)}")
            continue
    
    # Create DataFrame
    if qb_data_list:
        df_qb_data = pd.DataFrame(qb_data_list)
    else:
        # Create empty DataFrame with expected columns
        df_qb_data = pd.DataFrame(columns=['claim_id', 'processed_at'])
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE insurance.qb_claim_data AS SELECT * FROM df_qb_data")
    
    context.log.info(f"Successfully processed {len(qb_data_list)} claims")
    
    context.add_output_metadata({
        "total_claims_processed": len(qb_data_list),
        "total_claims_requested": len(claims_to_process),
        "success_rate": f"{len(qb_data_list)/len(claims_to_process)*100:.1f}%" if claims_to_process else "0%",
        "archived_claims_excluded": len(archived_claims),
    })
    
    return df_qb_data


@asset(
    description="Claims categorized as updates vs new additions",
    group_name="insurance_claims",
    ins={
        "qb_claim_data": AssetIn(),
        "current_smartsheet_data": AssetIn()
    },
    io_manager_key="io_manager"
)
def claim_processing_plan(
    context: AssetExecutionContext,
    database: DuckDBResource,
    qb_claim_data: pd.DataFrame,
    current_smartsheet_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Categorize claims into updates (existing) vs additions (new) and prepare processing plan.
    """
    context.log.info("Creating claim processing plan...")
    
    if qb_claim_data.empty:
        context.log.info("No QB claim data to process")
        return pd.DataFrame(columns=['claim_id', 'action_type', 'sheet_row_id'])
    
    # Get current main sheet claims
    main_sheet_claims = set(current_smartsheet_data[
        current_smartsheet_data['sheet_type'] == 'main'
    ]['claim_id'].tolist())
    
    # Create claim lookup for sheet row IDs
    sheet_lookup = dict(zip(
        current_smartsheet_data['claim_id'],
        current_smartsheet_data['sheet_row_id']
    ))
    
    # Categorize claims
    processing_plan = []
    
    for _, row in qb_claim_data.iterrows():
        claim_id = row['claim_id']
        
        if claim_id in main_sheet_claims:
            # Existing claim - needs update
            processing_plan.append({
                'claim_id': claim_id,
                'action_type': 'update',
                'sheet_row_id': sheet_lookup.get(claim_id),
                'qb_data_available': True
            })
        else:
            # New claim - needs addition
            processing_plan.append({
                'claim_id': claim_id,
                'action_type': 'add',
                'sheet_row_id': None,
                'qb_data_available': True
            })
    
    df_plan = pd.DataFrame(processing_plan)
    
    # Count actions
    update_count = len(df_plan[df_plan['action_type'] == 'update'])
    add_count = len(df_plan[df_plan['action_type'] == 'add'])
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE insurance.claim_processing_plan AS SELECT * FROM df_plan")
    
    context.log.info(f"Processing plan: {update_count} updates, {add_count} additions")
    
    context.add_output_metadata({
        "total_claims": len(df_plan),
        "claims_to_update": update_count,
        "claims_to_add": add_count,
        "existing_claims_in_sheet": len(main_sheet_claims),
    })
    
    return df_plan


@asset(
    description="Batch update results for existing claims in Smartsheet",
    group_name="insurance_claims",
    ins={
        "claim_processing_plan": AssetIn(),
        "qb_claim_data": AssetIn(),
        "field_mapping_config": AssetIn()
    },
    io_manager_key="io_manager"
)
def updated_smartsheet_claims(
    context: AssetExecutionContext,
    config: InsuranceClaimConfig,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource,
    claim_processing_plan: pd.DataFrame,
    qb_claim_data: pd.DataFrame,
    field_mapping_config: pd.DataFrame
) -> pd.DataFrame:
    """
    Perform batch updates for existing claims in Smartsheet.
    """
    context.log.info("Processing Smartsheet claim updates...")
    
    # Filter for update actions
    updates_plan = claim_processing_plan[claim_processing_plan['action_type'] == 'update']
    
    if updates_plan.empty:
        context.log.info("No claims to update")
        return pd.DataFrame(columns=['claim_id', 'action_type', 'status', 'processed_at'])
    
    context.log.info(f"Processing {len(updates_plan)} claim updates...")
    
    # Get sheet metadata
    with database.get_connection() as conn:
        sheet_metadata = conn.execute("SELECT * FROM insurance.sheet_metadata").fetch_df()
    
    sheet_id = sheet_metadata['main_sheet_id'].iloc[0]
    
    # Prepare update records
    update_records = []
    qb_data_dict = {row['claim_id']: row for _, row in qb_claim_data.iterrows()}
    
    for _, plan_row in updates_plan.iterrows():
        claim_id = plan_row['claim_id']
        sheet_row_id = plan_row['sheet_row_id']
        
        if claim_id in qb_data_dict:
            qb_record = qb_data_dict[claim_id].to_dict()
            
            # Remove formula field for updates and add sheet row ID
            if "Claim Name / Task Name" in qb_record:
                del qb_record["Claim Name / Task Name"]
            
            qb_record["Sheet Row ID"] = sheet_row_id
            update_records.append(qb_record)
    
    # Perform batch updates using your existing batch processor
    results = []
    
    if update_records:
        try:
            ss_client = smartsheet_client.get_client()
            
            # Get sheet data for column mapping
            sheet_url = sheet_metadata['main_sheet_url'].iloc[0]
            sheet_data = smartsheet_client.get_sheet_data(sheet_url)
            field_mapping = field_mapping_config.to_dict('records')
            relevant_sheet_cols = smartsheet_client.get_col_ids(field_mapping, sheet_data)
            
            # Use your existing batch processor
            batch_processor = smartsheet_client.create_batch_processor(
                ss_client, sheet_id, config.max_batch_size, config.max_retries
            )
            
            update_results = batch_processor.process_update_batches(update_records, relevant_sheet_cols)
            
            # Record results
            for _, plan_row in updates_plan.iterrows():
                results.append({
                    'claim_id': plan_row['claim_id'],
                    'action_type': 'update',
                    'status': 'success' if update_results['successful'] > 0 else 'failed',
                    'processed_at': datetime.now()
                })
            
            context.log.info(f"Update results: {update_results['successful']}/{update_results['total_records']} successful")
            
        except Exception as e:
            context.log.error(f"Batch update failed: {str(e)}")
            for _, plan_row in updates_plan.iterrows():
                results.append({
                    'claim_id': plan_row['claim_id'],
                    'action_type': 'update',
                    'status': 'failed',
                    'processed_at': datetime.now()
                })
    
    df_results = pd.DataFrame(results)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE insurance.update_results AS SELECT * FROM df_results")
    
    successful_updates = len(df_results[df_results['status'] == 'success'])
    
    context.add_output_metadata({
        "total_updates_attempted": len(df_results),
        "successful_updates": successful_updates,
        "failed_updates": len(df_results) - successful_updates,
        "success_rate": f"{successful_updates/len(df_results)*100:.1f}%" if df_results else "0%",
    })
    
    return df_results

# Continuation of insurance_claim_tracking.py

@asset(
    description="Batch addition results for new claims in Smartsheet",
    group_name="insurance_claims",
    ins={
        "claim_processing_plan": AssetIn(),
        "qb_claim_data": AssetIn(),
        "field_mapping_config": AssetIn(),
        "updated_smartsheet_claims": AssetIn()  # Ensure updates complete first
    },
    io_manager_key="io_manager"
)
def new_smartsheet_claims(
    context: AssetExecutionContext,
    config: InsuranceClaimConfig,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource,
    claim_processing_plan: pd.DataFrame,
    qb_claim_data: pd.DataFrame,
    field_mapping_config: pd.DataFrame,
    updated_smartsheet_claims: pd.DataFrame
) -> pd.DataFrame:
    """
    Add new claims to Smartsheet as parent rows.
    """
    context.log.info("Processing new claims for Smartsheet...")
    
    # Filter for addition actions
    additions_plan = claim_processing_plan[claim_processing_plan['action_type'] == 'add']
    
    if additions_plan.empty:
        context.log.info("No new claims to add")
        return pd.DataFrame(columns=['claim_id', 'action_type', 'status', 'new_row_id', 'processed_at'])
    
    context.log.info(f"Processing {len(additions_plan)} new claims...")
    
    # Get sheet metadata
    with database.get_connection() as conn:
        sheet_metadata = conn.execute("SELECT * FROM insurance.sheet_metadata").fetch_df()
    
    sheet_id = sheet_metadata['main_sheet_id'].iloc[0]
    sheet_url = sheet_metadata['main_sheet_url'].iloc[0]
    
    # Prepare addition records
    add_records = []
    qb_data_dict = {row['claim_id']: row for _, row in qb_claim_data.iterrows()}
    
    for _, plan_row in additions_plan.iterrows():
        claim_id = plan_row['claim_id']
        
        if claim_id in qb_data_dict:
            qb_record = qb_data_dict[claim_id].to_dict()
            # Keep the formula field for new additions
            add_records.append(qb_record)
    
    # Perform batch additions
    results = []
    new_row_ids = []
    
    if add_records:
        try:
            ss_client = smartsheet_client.get_client()
            
            # Get sheet data for column mapping
            sheet_data = smartsheet_client.get_sheet_data(sheet_url)
            field_mapping = field_mapping_config.to_dict('records')
            relevant_sheet_cols = smartsheet_client.get_col_ids(field_mapping, sheet_data)
            
            # Disable formulas during batch operations
            formula_jail = smartsheet_client.column_formula_holding(ss_client, sheet_data, field_mapping)
            
            # Use batch processor
            batch_processor = smartsheet_client.create_batch_processor(
                ss_client, sheet_id, config.max_batch_size, config.max_retries
            )
            
            add_results = batch_processor.process_add_batches(add_records, relevant_sheet_cols)
            
            # Re-enable formulas
            sheet_data_fresh = smartsheet_client.get_sheet_data(sheet_url)
            smartsheet_client.column_formula_release(ss_client, sheet_data_fresh, formula_jail)
            
            # Extract new row IDs for subtask creation
            new_row_ids = add_results.get("new_row_ids", [])
            
            # Record results
            for i, (_, plan_row) in enumerate(additions_plan.iterrows()):
                new_row_id = new_row_ids[i] if i < len(new_row_ids) else None
                results.append({
                    'claim_id': plan_row['claim_id'],
                    'action_type': 'add',
                    'status': 'success' if i < add_results['successful'] else 'failed',
                    'new_row_id': new_row_id,
                    'processed_at': datetime.now()
                })
            
            context.log.info(f"Addition results: {add_results['successful']}/{add_results['total_records']} successful")
            
        except Exception as e:
            context.log.error(f"Batch addition failed: {str(e)}")
            for _, plan_row in additions_plan.iterrows():
                results.append({
                    'claim_id': plan_row['claim_id'],
                    'action_type': 'add',
                    'status': 'failed',
                    'new_row_id': None,
                    'processed_at': datetime.now()
                })
    
    df_results = pd.DataFrame(results)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE insurance.addition_results AS SELECT * FROM df_results")
    
    successful_additions = len(df_results[df_results['status'] == 'success'])
    
    context.add_output_metadata({
        "total_additions_attempted": len(df_results),
        "successful_additions": successful_additions,
        "failed_additions": len(df_results) - successful_additions,
        "success_rate": f"{successful_additions/len(df_results)*100:.1f}%" if len(df_results) > 0 else "0%",
        "new_row_ids_created": len([r for r in df_results['new_row_id'] if r is not None]),
    })
    
    return df_results


@asset(
    description="Template sheet data for creating subtasks",
    group_name="insurance_claims",
    io_manager_key="io_manager"
)
def template_sheet_data(
    context: AssetExecutionContext,
    config: InsuranceClaimConfig,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource
) -> pd.DataFrame:
    """
    Load template sheet data for creating subtasks under new claims.
    """
    context.log.info("Loading template sheet data...")
    
    ss_client = smartsheet_client.get_client()
    template_sheet_url = smartsheet_client.get_sheet_url(ss_client, config.template_sheet_name)
    template_sheet_data = smartsheet_client.get_sheet_data(template_sheet_url)
    
    # Extract template rows and convert to format suitable for storage
    template_rows = []
    
    for row in template_sheet_data['rows']:
        row_data = {
            'template_row_id': row['id'],
            'row_number': row.get('rowNumber', 0),
            'cells_json': str(row.get('cells', [])),  # Store as string for DuckDB
            'parent_id': row.get('parentId'),
            'sibling_id': row.get('siblingId'),
        }
        template_rows.append(row_data)
    
    df_template = pd.DataFrame(template_rows)
    
    # Store metadata about template structure
    template_metadata = pd.DataFrame([{
        'template_name': config.template_sheet_name,
        'template_url': template_sheet_url,
        'template_id': template_sheet_url[-16:],
        'total_rows': len(template_rows),
        'columns_json': str(template_sheet_data.get('columns', [])),
        'extracted_at': datetime.now()
    }])
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS insurance")
        conn.execute("CREATE OR REPLACE TABLE insurance.template_rows AS SELECT * FROM df_template")
        conn.execute("CREATE OR REPLACE TABLE insurance.template_metadata AS SELECT * FROM template_metadata")
    
    context.log.info(f"Loaded {len(template_rows)} template rows")
    
    context.add_output_metadata({
        "template_rows": len(template_rows),
        "template_name": config.template_sheet_name,
        "template_id": template_sheet_url[-16:],
    })
    
    return df_template


@asset(
    description="Subtasks added to new claims in Smartsheet",
    group_name="insurance_claims",
    ins={
        "new_smartsheet_claims": AssetIn(),
        "template_sheet_data": AssetIn()
    },
    io_manager_key="io_manager"
)
def claim_subtasks(
    context: AssetExecutionContext,
    config: InsuranceClaimConfig,
    smartsheet_client: SmartsheetResource,
    database: DuckDBResource,
    new_smartsheet_claims: pd.DataFrame,
    template_sheet_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Add subtasks to newly created claims using the template structure.
    """
    context.log.info("Adding subtasks to new claims...")
    
    # Get successful new claims with row IDs
    successful_claims = new_smartsheet_claims[
        (new_smartsheet_claims['status'] == 'success') & 
        (new_smartsheet_claims['new_row_id'].notna())
    ]
    
    if successful_claims.empty:
        context.log.info("No successful new claims found for subtask creation")
        return pd.DataFrame(columns=['claim_id', 'subtasks_added', 'status', 'processed_at'])
    
    context.log.info(f"Adding subtasks for {len(successful_claims)} new claims...")
    
    # Get sheet and template metadata
    with database.get_connection() as conn:
        sheet_metadata = conn.execute("SELECT * FROM insurance.sheet_metadata").fetch_df()
        template_metadata = conn.execute("SELECT * FROM insurance.template_metadata").fetch_df()
    
    sheet_url = sheet_metadata['main_sheet_url'].iloc[0]
    
    # Get template and main sheet data
    ss_client = smartsheet_client.get_client()
    main_sheet_data = smartsheet_client.get_sheet_data(sheet_url)
    
    # Reconstruct template sheet data from stored data
    template_sheet_url = template_metadata['template_url'].iloc[0]
    template_sheet_data_full = smartsheet_client.get_sheet_data(template_sheet_url)
    
    # Prepare parent row info for subtask creation
    parent_rows_info = []
    for _, claim_row in successful_claims.iterrows():
        parent_rows_info.append({
            "claim_id": claim_row['claim_id'],
            "parent_row_id": claim_row['new_row_id']
        })
    
    # Add subtasks using your existing function
    results = []
    
    try:
        successful_subtasks = smartsheet_client.add_subtasks_with_grouping(
            ss_client, sheet_url, parent_rows_info, 
            template_sheet_data_full, main_sheet_data
        )
        
        # Record results for each claim
        subtasks_per_claim = successful_subtasks // len(parent_rows_info) if parent_rows_info else 0
        
        for _, claim_row in successful_claims.iterrows():
            results.append({
                'claim_id': claim_row['claim_id'],
                'subtasks_added': subtasks_per_claim,  # Approximate
                'status': 'success',
                'processed_at': datetime.now()
            })
        
        context.log.info(f"Successfully added {successful_subtasks} subtask rows total")
        
    except Exception as e:
        context.log.error(f"Subtask creation failed: {str(e)}")
        for _, claim_row in successful_claims.iterrows():
            results.append({
                'claim_id': claim_row['claim_id'],
                'subtasks_added': 0,
                'status': 'failed',
                'processed_at': datetime.now()
            })
    
    df_results = pd.DataFrame(results)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE insurance.subtask_results AS SELECT * FROM df_results")
    
    total_subtasks = df_results['subtasks_added'].sum()
    successful_claims_count = len(df_results[df_results['status'] == 'success'])
    
    context.add_output_metadata({
        "claims_processed": len(df_results),
        "successful_subtask_creation": successful_claims_count,
        "total_subtasks_added": int(total_subtasks),
        "avg_subtasks_per_claim": round(total_subtasks / len(df_results), 1) if len(df_results) > 0 else 0,
    })
    
    return df_results


@asset(
    description="Final processing summary and performance metrics",
    group_name="insurance_claims",
    ins={
        "updated_smartsheet_claims": AssetIn(),
        "new_smartsheet_claims": AssetIn(),
        "claim_subtasks": AssetIn()
    },
    io_manager_key="io_manager"
)
def processing_summary(
    context: AssetExecutionContext,
    database: DuckDBResource,
    updated_smartsheet_claims: pd.DataFrame,
    new_smartsheet_claims: pd.DataFrame,
    claim_subtasks: pd.DataFrame
) -> MaterializeResult:
    """
    Create final processing summary with performance metrics and status.
    """
    context.log.info("Generating final processing summary...")
    
    # Calculate summary statistics
    total_updates = len(updated_smartsheet_claims)
    successful_updates = len(updated_smartsheet_claims[updated_smartsheet_claims['status'] == 'success'])
    
    total_additions = len(new_smartsheet_claims)
    successful_additions = len(new_smartsheet_claims[new_smartsheet_claims['status'] == 'success'])
    
    total_subtask_operations = len(claim_subtasks)
    successful_subtasks = len(claim_subtasks[claim_subtasks['status'] == 'success'])
    total_subtasks_added = claim_subtasks['subtasks_added'].sum()
    
    # Create summary DataFrame
    summary_data = [{
        'processing_date': datetime.now(),
        'total_claims_processed': total_updates + total_additions,
        'claims_updated': total_updates,
        'successful_updates': successful_updates,
        'claims_added': total_additions,
        'successful_additions': successful_additions,
        'subtask_operations': total_subtask_operations,
        'successful_subtask_operations': successful_subtasks,
        'total_subtasks_added': int(total_subtasks_added),
        'overall_success_rate': round(
            (successful_updates + successful_additions) / (total_updates + total_additions) * 100, 2
        ) if (total_updates + total_additions) > 0 else 0
    }]
    
    df_summary = pd.DataFrame(summary_data)
    
    # Store in DuckDB
    with database.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE insurance.processing_summary AS SELECT * FROM df_summary")
    
    # Log final results
    context.log.info("=== PROCESSING COMPLETE ===")
    context.log.info(f"Total claims processed: {total_updates + total_additions}")
    context.log.info(f"Updates: {successful_updates}/{total_updates} successful")
    context.log.info(f"Additions: {successful_additions}/{total_additions} successful")
    context.log.info(f"Subtasks: {total_subtasks_added} rows added across {successful_subtasks} claims")
    context.log.info(f"Overall success rate: {df_summary['overall_success_rate'].iloc[0]:.1f}%")
    
    return MaterializeResult(
        metadata={
            "total_claims_processed": total_updates + total_additions,
            "successful_updates": successful_updates,
            "successful_additions": successful_additions,
            "total_subtasks_added": int(total_subtasks_added),
            "overall_success_rate": f"{df_summary['overall_success_rate'].iloc[0]:.1f}%",
            "processing_timestamp": datetime.now().isoformat(),
        }
    )