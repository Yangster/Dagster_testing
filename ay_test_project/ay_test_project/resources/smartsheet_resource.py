# ay_test_project/ay_test_project/resources/smartsheet_resource.py
import os
import requests
import smartsheet
import time
import math
from collections import defaultdict
from dagster import ConfigurableResource
from typing import Dict, List, Optional


class SmartsheetBatchProcessor:
    """
    Handles batch operations for Smartsheet API calls
    Optimized for bulk updates and additions
    """
    
    def __init__(self, ss_client, sheet_id, max_batch_size=100, max_retries=3):
        self.ss_client = ss_client
        self.sheet_id = sheet_id
        self.max_batch_size = max_batch_size
        self.max_retries = max_retries
        
    def create_bulk_update_rows(self, update_records, column_mapping):
        """Create bulk update rows for batch operation"""
        update_rows = []
        
        for record in update_records:
            if "Sheet Row ID" not in record:
                print(f"Warning: Record missing 'Sheet Row ID': {record}")
                continue
                
            row = smartsheet.models.Row()
            row.id = record["Sheet Row ID"]
            
            # Process all fields except Sheet Row ID
            for field_name, field_value in record.items():
                if field_name == "Sheet Row ID":
                    continue
                if field_name in column_mapping:
                    cell = smartsheet.models.Cell()
                    cell.column_id = column_mapping[field_name]
                    cell.value = field_value
                    row.cells.append(cell)
            
            update_rows.append(row)
        
        return update_rows

    def create_bulk_add_rows(self, add_records, column_mapping):
        """Create bulk add rows for batch operation"""
        add_rows = []
        
        for record in add_records:
            row = smartsheet.models.Row()
            
            for field_name, field_value in record.items():
                if field_name == "Claim Name / Task Name":
                    # Add formula for claim name
                    if field_name in column_mapping:
                        cell = smartsheet.models.Cell()
                        cell.column_id = column_mapping[field_name]
                        cell.formula = "=IF([Record # in Outage Management (QB)*]@row <> 0, [Site Name (QB)*]@row + \" -  \" + [Accident Description]@row, \"\")"
                        row.cells.append(cell)
                elif field_name in column_mapping:
                    cell = smartsheet.models.Cell()
                    cell.column_id = column_mapping[field_name]
                    cell.value = field_value
                    row.cells.append(cell)
            
            add_rows.append(row)
        
        return add_rows

    def batch_operation_with_retry(self, rows_batch, operation_type="update"):
        """Execute batch operation with retry logic"""
        for attempt in range(self.max_retries):
            try:
                if operation_type == "update":
                    response = self.ss_client.Sheets.update_rows(self.sheet_id, rows_batch)
                elif operation_type == "add":
                    response = self.ss_client.Sheets.add_rows(self.sheet_id, rows_batch)
                else:
                    return False, None, f"Invalid operation_type: {operation_type}"
                
                return True, response, None
                
            except Exception as e:
                error_msg = str(e)
                print(f"{operation_type.title()} batch attempt {attempt + 1}/{self.max_retries} failed: {error_msg}")
                
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** (attempt + 1)
                    print(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    return False, None, error_msg

    def process_update_batches(self, update_records, column_mapping):
        """Process update records in batches"""
        if not update_records:
            return {"total_records": 0, "successful": 0, "failed": 0, "batches": []}
        
        print(f"Processing {len(update_records)} updates in batches of {self.max_batch_size}...")
        
        results = {
            "total_records": len(update_records),
            "successful": 0,
            "failed": 0,
            "batches": []
        }
        
        total_batches = math.ceil(len(update_records) / self.max_batch_size)
        
        for i in range(0, len(update_records), self.max_batch_size):
            batch_num = (i // self.max_batch_size) + 1
            batch_records = update_records[i:i + self.max_batch_size]
            
            print(f"Processing update batch {batch_num}/{total_batches} ({len(batch_records)} records)")
            
            # Create batch rows
            batch_rows = self.create_bulk_update_rows(batch_records, column_mapping)
            
            if not batch_rows:
                print(f"No valid rows in batch {batch_num}, skipping...")
                continue
            
            # Execute batch
            success, response, error_msg = self.batch_operation_with_retry(batch_rows, "update")
            
            batch_result = {
                "batch_num": batch_num,
                "records_in_batch": len(batch_rows),
                "success": success,
                "error_msg": error_msg
            }
            results["batches"].append(batch_result)
            
            if success:
                results["successful"] += len(batch_rows)
                print(f"✓ Update batch {batch_num} completed successfully")
            else:
                results["failed"] += len(batch_rows)
                print(f"✗ Update batch {batch_num} failed: {error_msg}")
            
            # Rate limiting pause between batches
            if batch_num < total_batches:
                time.sleep(1)
        
        print(f"Update results: {results['successful']}/{results['total_records']} successful")
        return results

    def process_add_batches(self, add_records, column_mapping):
        """Process add records in batches"""
        if not add_records:
            return {"total_records": 0, "successful": 0, "failed": 0, "batches": [], "new_row_ids": []}
        
        print(f"Processing {len(add_records)} additions in batches of {self.max_batch_size}...")
        
        results = {
            "total_records": len(add_records),
            "successful": 0,
            "failed": 0,
            "batches": [],
            "new_row_ids": []
        }
        
        total_batches = math.ceil(len(add_records) / self.max_batch_size)
        
        for i in range(0, len(add_records), self.max_batch_size):
            batch_num = (i // self.max_batch_size) + 1
            batch_records = add_records[i:i + self.max_batch_size]
            
            print(f"Processing add batch {batch_num}/{total_batches} ({len(batch_records)} records)")
            
            # Create batch rows
            batch_rows = self.create_bulk_add_rows(batch_records, column_mapping)
            
            # Execute batch
            success, response, error_msg = self.batch_operation_with_retry(batch_rows, "add")
            
            batch_result = {
                "batch_num": batch_num,
                "records_in_batch": len(batch_rows),
                "success": success,
                "error_msg": error_msg
            }
            results["batches"].append(batch_result)
            
            if success:
                results["successful"] += len(batch_rows)
                print(f"✓ Add batch {batch_num} completed successfully")
                
                # Extract new row IDs from response
                if response and hasattr(response, 'result'):
                    for row_result in response.result:
                        if hasattr(row_result, 'id'):
                            results["new_row_ids"].append(row_result.id)
            else:
                results["failed"] += len(batch_rows)
                print(f"✗ Add batch {batch_num} failed: {error_msg}")
            
            # Rate limiting pause between batches
            if batch_num < total_batches:
                time.sleep(1)
        
        print(f"Add results: {results['successful']}/{results['total_records']} successful")
        return results


class SmartsheetResource(ConfigurableResource):
    """Enhanced Smartsheet API client resource with batch processing capabilities"""
    bearer_token_env_var: str = "SMARTSHEETS_TOKEN"
    
    def get_client(self):
        """Create and return authenticated Smartsheet client"""
        bearer = os.environ.get(self.bearer_token_env_var)
        if not bearer:
            raise ValueError(f"Environment variable {self.bearer_token_env_var} not found")
        
        return smartsheet.Smartsheet(f"Bearer {bearer}")
    
    def get_sheet_url(self, ss_client, sheet_name: str) -> str:
        """Get sheet URL from sheet name"""
        base_url = "https://api.smartsheet.com/2.0/sheets"
        response = ss_client.Sheets.list_sheets(include_all=True)
        all_sheets = response.data
        
        for sheet in all_sheets:
            if sheet.name == sheet_name:
                return f"{base_url}/{sheet.id}"
        
        raise ValueError(f"Sheet '{sheet_name}' not found")
    
    def get_sheet_data(self, url: str) -> Dict:
        """Get sheet data from URL"""
        bearer = os.environ.get(self.bearer_token_env_var)
        headers = {
            "Authorization": f"Bearer {bearer}",
            "ContentType": "application/json"
        }
        
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"Failed to get sheet data: {response.status_code} - {response.text}")
    
    def get_col_ids(self, field_mapping: List[Dict], sheet_data: Dict) -> Dict[str, int]:
        """Get column ID mapping"""
        sheet_col_names = [field["ss_field"] for field in field_mapping]
        sheet_col_names.append("Claim Name / Task Name")
        
        col_ids = {}
        relevant_cols = [col for col in sheet_data['columns'] if col['title'] in sheet_col_names]
        
        for col in relevant_cols:
            col_ids[col['title']] = col['id']
        
        return col_ids
    
    def get_relevant_row_ids(self, sheet_rows: List[Dict], claim_id_col_id: int) -> Dict[str, int]:
        """Get mapping of claim IDs to sheet row IDs"""
        claim_lookup = {}
        
        for row in sheet_rows:
            row_id = row['id']
            for cell in row['cells']:
                if cell['columnId'] == claim_id_col_id and len(cell) > 1:
                    claim_id = cell['displayValue']
                    claim_lookup[claim_id] = row_id
        
        return claim_lookup
    
    def create_batch_processor(self, ss_client, sheet_id: str, max_batch_size: int = 100, max_retries: int = 3) -> SmartsheetBatchProcessor:
        """Create batch processor instance"""
        return SmartsheetBatchProcessor(ss_client, sheet_id, max_batch_size, max_retries)
    
    def column_formula_holding(self, ss_client, sheet_data: Dict, field_mapping: List[Dict]) -> Dict:
        """Disable column formulas temporarily"""
        sheet_id = sheet_data['id']
        formula_jail = {}
        
        sheet_col_names = [f['ss_field'] for f in field_mapping]
        relevant_col_data = [col for col in sheet_data['columns'] if col['title'] in sheet_col_names]
        
        for col in relevant_col_data:
            try:
                formula = col['formula']
            except KeyError:
                continue
                
            print(f"Column formula found in {col['title']}. Temporarily disabling...")
            update_col = col.copy()
            col_id = update_col['id']
            formula_jail[col_id] = formula
            update_col['formula'] = ""
            update_col.pop("id")
            update_col_obj = smartsheet.models.Column(update_col)
            ss_client.Sheets.update_column(sheet_id, col_id, update_col_obj)
        
        if len(formula_jail) == 0:
            print("No column formulas found, continuing\n")
        else:
            print(f"{len(formula_jail)} column formula(s) have been temporarily disabled.\n")
        
        return formula_jail
    
    def column_formula_release(self, ss_client, sheet_data: Dict, formula_jail: Dict) -> None:
        """Re-enable column formulas"""
        sheet_id = sheet_data['id']
        relevant_col_data = [col for col in sheet_data['columns'] if col['id'] in formula_jail]
        
        print(f"{len(relevant_col_data)} column formula(s) detected")
        
        for col in relevant_col_data:
            update_col = col.copy()
            col_title = update_col['title']
            col_id = update_col['id']
            update_col['formula'] = formula_jail[col_id]
            update_col.pop("id")
            update_col_obj = smartsheet.models.Column(update_col)
            ss_client.Sheets.update_column(sheet_id, col_id, update_col_obj)
            print(f"{col_title}'s column formula has been re-enabled")
        
        print("All column formulas have been re-enabled.\n")
    
    def add_subtasks_with_grouping(self, ss_client, sheet_url: str, parent_rows_info: List[Dict], 
                                 template_sheet_data: Dict, main_sheet_data: Dict) -> int:
        """Add subtasks with optimized grouping by parent"""
        print("Adding subtasks with optimized grouping...")
        
        template_rows = self.get_nested_rows(main_sheet_data, template_sheet_data)
        template_indent_mapping = {
            1: "1", 2: "2", 3: "1", 4: "2", 5: "2", 6: "2", 7: "2", 8: "2", 
            9: "2", 10: "2", 11: "2", 12: "2", 13: "2", 14: "1", 15: "2", 
            16: "2", 17: "2", 18: "2", 19: "2", 20: "2", 21: "2", 22: "2"
        }
        
        successful_additions = 0
        
        for parent_info in parent_rows_info:
            parent_row_id = parent_info["parent_row_id"]
            claim_id = parent_info.get("claim_id", "unknown")
            
            print(f"Adding subtasks for claim {claim_id}...")
            
            # Add level 1 rows (phases) - must be individual due to parent relationship
            phase_mapping = {}
            
            for idx, template_row in enumerate(template_rows, 1):
                if template_indent_mapping.get(idx) == "1":
                    row_data = template_row.copy()
                    row_data["parentId"] = parent_row_id
                    row_data["toBottom"] = True
                    
                    # Retry logic for individual subtask addition
                    for attempt in range(3):
                        try:
                            response = self.add_row_from_json(sheet_url, row_data)
                            if "result" in response and "id" in response["result"]:
                                phase_mapping[idx] = response["result"]["id"]
                                successful_additions += 1
                                break
                        except Exception as e:
                            print(f"Phase row attempt {attempt + 1}/3 failed: {e}")
                            if attempt < 2:
                                time.sleep(2)
            
            # Add level 2 rows (tasks) - group by parent phase where possible
            level_2_groups = defaultdict(list)
            
            for idx, template_row in enumerate(template_rows, 1):
                if template_indent_mapping.get(idx) == "2":
                    # Find parent phase
                    parent_phase_idx = None
                    for phase_idx in range(idx - 1, 0, -1):
                        if template_indent_mapping.get(phase_idx) == "1":
                            parent_phase_idx = phase_idx
                            break
                    
                    if parent_phase_idx and parent_phase_idx in phase_mapping:
                        parent_phase_id = phase_mapping[parent_phase_idx]
                        row_data = template_row.copy()
                        row_data["parentId"] = parent_phase_id
                        row_data["toBottom"] = True
                        level_2_groups[parent_phase_id].append(row_data)
            
            # Add level 2 rows (still individual calls due to API limitations)
            for parent_phase_id, phase_rows in level_2_groups.items():
                for row_data in phase_rows:
                    for attempt in range(3):
                        try:
                            response = self.add_row_from_json(sheet_url, row_data)
                            if "result" in response and "id" in response["result"]:
                                successful_additions += 1
                                break
                        except Exception as e:
                            print(f"Task row attempt {attempt + 1}/3 failed: {e}")
                            if attempt < 2:
                                time.sleep(2)
        
        print(f"Subtask addition completed. {successful_additions} rows added successfully.")
        return successful_additions
    
    def get_nested_rows(self, main_sheet_data: Dict, template_sheet_data: Dict) -> List[Dict]:
        """Get nested rows from template formatted for main sheet"""
        # used only for matching the title of the column to the destination table
        template_col_names = [col['title'] for col in template_sheet_data['columns']]
        main_sheet_colid_lookup = {}
        
        # {template col id : destination sheet col id}
        for template_name in template_col_names:
            sheet_col_id = [col['id'] for col in main_sheet_data['columns'] if col['title'] == template_name][0]
            template_col_id = [col['id'] for col in template_sheet_data['columns'] if col['title'] == template_name][0]
            main_sheet_colid_lookup[template_col_id] = sheet_col_id

        new_template_rows = []
        for row in template_sheet_data['rows']:
            new_row_from_template = {}
            current_row_cells = []
            
            for cell in row['cells']:
                template_col_id = cell['columnId']
                dest_col_id = main_sheet_colid_lookup[template_col_id]
                
                try:
                    value_from_template = cell['value']
                except KeyError:
                    value_from_template = ""
                
                try:
                    url_from_template = cell['hyperlink']
                    current_row_cells.append({
                        'columnId': dest_col_id,
                        'value': value_from_template,
                        'hyperlink': url_from_template
                    })
                except KeyError:
                    current_row_cells.append({
                        'columnId': dest_col_id,
                        'value': value_from_template
                    })
            
            new_row_from_template["cells"] = current_row_cells
            new_template_rows.append(new_row_from_template)
        
        return new_template_rows
    
    def add_row_from_json(self, url: str, row_as_json: Dict) -> Dict:
        """Add row using JSON data"""
        bearer = os.environ.get(self.bearer_token_env_var)
        headers = {
            "Authorization": f"Bearer {bearer}",
            "ContentType": "application/json"
        }
        
        row_url = url + "/rows"
        response = requests.post(row_url, headers=headers, json=row_as_json)
        return response.json()