# src/qb_ss_insurance_tracker/utils.py
import datetime
import time
import json
from typing import Any, Dict, List, Optional, Callable
from collections import OrderedDict

def retry_operation(operation: Callable, retries: int = 3, delay: int = 2) -> Any:
    """Retry an operation with exponential backoff"""
    for attempt in range(retries):
        try:
            result = operation()
            return result
        except Exception as e:
            print(f"Attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                time.sleep(wait_time)
    return None

def format_qb_data(field_mapping: List[Dict], outage_tracker_resp: Dict) -> Dict[str, Any]:
    """Format QuickBase data for Smartsheet import"""
    qb_data = {}
    qb_field_list = [field["qb_field"] for field in field_mapping]
    
    for field in qb_field_list:
        qb_value = outage_tracker_resp.get(field)
        
        if qb_value is None and not field.endswith("Date"):
            if field == "Unit Number":
                qb_data[field] = "Multiple Units"
                continue
            qb_value = "NO_DATA"
            qb_data[field] = qb_value
            continue
            
        # Handle date fields
        if field.endswith("Date"):
            if qb_value is None:
                qb_value = "2000-01-01T00:00:00Z"
                qb_data[field] = qb_value
            else:
                try:
                    dt_object = datetime.datetime.fromtimestamp(float(qb_value) / 1000)
                    qb_value = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
                    qb_data[field] = qb_value
                except (ValueError, TypeError):
                    qb_data[field] = qb_value
            continue
            
        # Handle coordinates
        if field in ["Unit # - Latitude", "Unit # - Longitude"]:
            try:
                qb_data[field] = float(qb_value)
            except (ValueError, TypeError):
                qb_data[field] = 0
            continue
            
        # Handle cost formatting
        if field == "Estimated cost to repair / return to service":
            try:
                qb_data[field] = "${:,.2f}".format(float(qb_value))
            except (ValueError, TypeError):
                qb_data[field] = "$0.00"
            continue
            
        # Handle downtime conversion (milliseconds to hours)
        if field == "Total Downtime":
            try:
                downtime_adjusted = float(qb_value) / 3600000
                qb_data[field] = downtime_adjusted
            except (ValueError, TypeError):
                qb_data[field] = 0
            continue
            
        # Handle complex field structures
        if isinstance(qb_value, (dict, OrderedDict)):
            qb_data[field] = qb_value.get("#text", "")
            continue
        else:
            qb_data[field] = qb_value
    
    return qb_data

def get_sheet_column_mapping(sheet_data: Dict, field_mapping: List[Dict]) -> Dict[str, int]:
    """Extract column ID mapping from sheet data"""
    sheet_col_names = [field["ss_field"] for field in field_mapping]
    sheet_col_names.append("Claim Name / Task Name")
    
    col_ids = {}
    relevant_cols = [col for col in sheet_data['columns'] if col['title'] in sheet_col_names]
    
    for col in relevant_cols:
        col_ids[col['title']] = col['id']
    
    return col_ids

def extract_claim_row_mapping(sheet_rows: List[Dict], record_id_col_id: int) -> Dict[str, int]:
    """Extract mapping of claim IDs to sheet row IDs"""
    claim_lookup = {}
    
    for row in sheet_rows:
        row_id = row['id']
        for cell in row['cells']:
            if cell['columnId'] == record_id_col_id and len(cell) > 1:
                claim_id = cell['displayValue']
                claim_lookup[claim_id] = row_id
                
    return claim_lookup