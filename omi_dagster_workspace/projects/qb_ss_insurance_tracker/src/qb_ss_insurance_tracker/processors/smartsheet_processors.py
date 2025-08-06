# src/qb_ss_insurance_tracker/processors/smartsheet_processors.py
"""
Smartsheet processing utilities for batch operations and template management.
Separates complex business logic from asset definitions.
"""

import smartsheet
import time
import math
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
from dagster import AssetExecutionContext


class SmartsheetBatchProcessor:
    """
    Handles batch operations for Smartsheet API calls.
    Optimized for bulk updates and additions.
    """
    
    def __init__(self, ss_client: smartsheet.Smartsheet, sheet_id: int, 
                 max_batch_size: int = 100, max_retries: int = 3):
        """
        Initialize batch processor.
        
        Args:
            ss_client: Authenticated Smartsheet client
            sheet_id: Target sheet ID
            max_batch_size: Maximum rows per batch (default 100)
            max_retries: Number of retry attempts (default 3)
        """
        self.ss_client = ss_client
        self.sheet_id = sheet_id
        self.max_batch_size = max_batch_size
        self.max_retries = max_retries
    
    def process_update_batches(self, update_records: List[Dict], 
                               column_mapping: Dict[str, int]) -> Dict:
        """
        Process update records in batches.
        
        Args:
            update_records: List of dicts with data and 'sheet_row_id'
            column_mapping: Dict mapping field names to column IDs
            
        Returns:
            Dict with results and statistics
        """
        if not update_records:
            return {"total_records": 0, "successful": 0, "failed": 0}
        
        results = {
            "total_records": len(update_records),
            "successful": 0,
            "failed": 0,
            "batches": []
        }
        
        total_batches = math.ceil(len(update_records) / self.max_batch_size)
        
        for i in range(0, len(update_records), self.max_batch_size):
            batch_records = update_records[i:i + self.max_batch_size]
            batch_num = (i // self.max_batch_size) + 1
            
            # Create batch rows
            batch_rows = self._create_bulk_update_rows(batch_records, column_mapping)
            
            if not batch_rows:
                continue
            
            # Execute batch with retry
            success, response, error_msg = self._batch_operation_with_retry(
                batch_rows, "update"
            )
            
            batch_result = {
                "batch_num": batch_num,
                "records_in_batch": len(batch_rows),
                "success": success,
                "error_msg": error_msg
            }
            results["batches"].append(batch_result)
            
            if success:
                results["successful"] += len(batch_rows)
            else:
                results["failed"] += len(batch_rows)
            
            # Rate limiting pause
            if batch_num < total_batches:
                time.sleep(1)
        
        return results
    
    def process_add_batches(self, add_records: List[Dict], 
                           column_mapping: Dict[str, int]) -> Dict:
        """
        Process add records in batches.
        
        Args:
            add_records: List of dicts with data for new rows
            column_mapping: Dict mapping field names to column IDs
            
        Returns:
            Dict with results, statistics, and new row IDs
        """
        if not add_records:
            return {"total_records": 0, "successful": 0, "failed": 0, "new_row_ids": []}
        
        results = {
            "total_records": len(add_records),
            "successful": 0,
            "failed": 0,
            "new_row_ids": [],
            "batches": []
        }
        
        total_batches = math.ceil(len(add_records) / self.max_batch_size)
        
        for i in range(0, len(add_records), self.max_batch_size):
            batch_records = add_records[i:i + self.max_batch_size]
            batch_num = (i // self.max_batch_size) + 1
            
            # Create batch rows
            batch_rows = self._create_bulk_add_rows(batch_records, column_mapping)
            
            # Execute batch with retry
            success, response, error_msg = self._batch_operation_with_retry(
                batch_rows, "add"
            )
            
            batch_result = {
                "batch_num": batch_num,
                "records_in_batch": len(batch_rows),
                "success": success,
                "error_msg": error_msg
            }
            results["batches"].append(batch_result)
            
            if success:
                results["successful"] += len(batch_rows)
                # Extract new row IDs
                if response and hasattr(response, 'result'):
                    for row_result in response.result:
                        if hasattr(row_result, 'id'):
                            results["new_row_ids"].append(row_result.id)
            else:
                results["failed"] += len(batch_rows)
            
            # Rate limiting pause
            if batch_num < total_batches:
                time.sleep(1)
        
        return results
    
    def _create_bulk_update_rows(self, update_records: List[Dict], 
                                 column_mapping: Dict[str, int]) -> List[smartsheet.models.Row]:
        """Create bulk update rows for batch operation."""
        update_rows = []
        
        for record in update_records:
            if "sheet_row_id" not in record:
                continue
            
            row = smartsheet.models.Row()
            row.id = record["sheet_row_id"]
            
            for field_name, field_value in record.items():
                if field_name in ["sheet_row_id", "operation", "claim_id", 
                                  "extracted_at", "source"]:
                    continue
                if field_name in column_mapping:
                    cell = smartsheet.models.Cell()
                    cell.column_id = column_mapping[field_name]
                    cell.value = field_value
                    row.cells.append(cell)
            
            update_rows.append(row)
        
        return update_rows
    
    def _create_bulk_add_rows(self, add_records: List[Dict], 
                             column_mapping: Dict[str, int]) -> List[smartsheet.models.Row]:
        """Create bulk add rows for batch operation."""
        add_rows = []
        
        for record in add_records:
            row = smartsheet.models.Row()
            
            for field_name, field_value in record.items():
                if field_name in ["operation", "claim_id", "extracted_at", "source"]:
                    continue
                
                if field_name == "Claim Name / Task Name" and field_name in column_mapping:
                    # Add formula for claim name
                    cell = smartsheet.models.Cell()
                    cell.column_id = column_mapping[field_name]
                    cell.formula = ('=IF([Record # in Outage Management (QB)*]@row <> 0, '
                                  '[Site Name (QB)*]@row + " -  " + [Accident Description]@row, "")')
                    row.cells.append(cell)
                elif field_name in column_mapping:
                    cell = smartsheet.models.Cell()
                    cell.column_id = column_mapping[field_name]
                    cell.value = field_value
                    row.cells.append(cell)
            
            add_rows.append(row)
        
        return add_rows
    
    def _batch_operation_with_retry(self, rows_batch: List[smartsheet.models.Row], 
                                    operation_type: str) -> Tuple[bool, Any, Optional[str]]:
        """
        Execute batch operation with retry logic.
        
        Returns:
            Tuple (success: bool, response: Any, error_msg: Optional[str])
        """
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
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** (attempt + 1)  # Exponential backoff
                    time.sleep(wait_time)
                else:
                    return False, None, error_msg
        
        return False, None, "Max retries exceeded"


class TemplateProcessor:
    """
    Handles template-to-sheet mapping and subtask creation.
    Encapsulates all template processing logic.
    """
    
    # Template row indent mapping (defines hierarchy)
    INDENT_MAP = {
        1: 1,   # Phase
        2: 2,   # Task
        3: 1,   # Phase
        4: 2, 5: 2, 6: 2, 7: 2, 8: 2, 9: 2, 10: 2, 11: 2, 12: 2, 13: 2,  # Tasks
        14: 1,  # Phase
        15: 2, 16: 2, 17: 2, 18: 2, 19: 2, 20: 2, 21: 2, 22: 2  # Tasks
    }
    
    def __init__(self, ss_client: smartsheet.Smartsheet, 
                 template_sheet_id: int, main_sheet_id: int):
        """
        Initialize template processor.
        
        Args:
            ss_client: Authenticated Smartsheet client
            template_sheet_id: Template sheet ID
            main_sheet_id: Main sheet ID where rows will be added
        """
        self.client = ss_client
        self.template_sheet_id = template_sheet_id
        self.main_sheet_id = main_sheet_id
        
        # Load sheets
        self.template_sheet = self.client.Sheets.get_sheet(template_sheet_id)
        self.main_sheet = self.client.Sheets.get_sheet(main_sheet_id, 
                                                       include_all=False, 
                                                       columns='all')
        
        # Build column mapping
        self.column_map = self._build_column_mapping()
        
        # Prepare template rows
        self.template_rows = self._prepare_template_rows()
    
    def _build_column_mapping(self) -> Dict[int, int]:
        """Map template column IDs to main sheet column IDs by title."""
        mapping = {}
        
        # Create lookup of main sheet columns by title
        main_cols = {col.title: col.id for col in self.main_sheet.columns}
        
        # Map template columns to main sheet columns
        for template_col in self.template_sheet.columns:
            if template_col.title in main_cols:
                mapping[template_col.id] = main_cols[template_col.title]
        
        return mapping
    
    def _prepare_template_rows(self) -> List[Tuple[int, smartsheet.models.Row]]:
        """
        Prepare template rows with proper column mapping.
        
        Returns:
            List of (template_index, prepared_row)
        """
        prepared = []
        
        for idx, template_row in enumerate(self.template_sheet.rows, 1):
            # Create new row with mapped columns
            new_row = smartsheet.models.Row()
            new_row.to_bottom = True
            
            # Map cells to main sheet columns
            for cell in template_row.cells:
                if cell.column_id in self.column_map:
                    new_cell = smartsheet.models.Cell()
                    new_cell.column_id = self.column_map[cell.column_id]
                    
                    # Copy cell content
                    if hasattr(cell, 'value') and cell.value is not None:
                        new_cell.value = cell.value
                    if hasattr(cell, 'hyperlink') and cell.hyperlink:
                        new_cell.hyperlink = smartsheet.models.Hyperlink({
                            'url': cell.hyperlink.url
                        })
                    
                    new_row.cells.append(new_cell)
            
            prepared.append((idx, new_row))
        
        return prepared
    
    def apply_to_parent(self, parent_row_id: int, 
                       context: Optional[AssetExecutionContext] = None) -> int:
        """
        Apply template structure under a parent row.
        
        Args:
            parent_row_id: ID of the parent row
            context: Optional Dagster context for logging
            
        Returns:
            Number of rows successfully added
        """
        rows_added = 0
        phase_mapping = {}  # Maps template index to created row ID
        
        # Step 1: Add all phase rows (indent level 1)
        phase_rows = self._get_rows_by_indent(1)
        
        if phase_rows:
            # Batch add all phases under the parent
            batch = []
            for template_idx, row in phase_rows:
                row_copy = self._copy_row(row)
                row_copy.parent_id = parent_row_id
                batch.append((template_idx, row_copy))
            
            # Add batch and map results
            added_phases = self._add_row_batch(batch, "phase", context)
            phase_mapping.update(added_phases)
            rows_added += len(added_phases)
        
        # Step 2: Group task rows by their parent phase
        tasks_by_phase = self._group_tasks_by_phase()
        
        # Step 3: Add tasks under each phase
        for phase_idx, task_indices in tasks_by_phase.items():
            if phase_idx not in phase_mapping:
                continue
            
            parent_phase_id = phase_mapping[phase_idx]
            task_batch = []
            
            for task_idx in task_indices:
                _, task_row = self.template_rows[task_idx - 1]
                row_copy = self._copy_row(task_row)
                row_copy.parent_id = parent_phase_id
                task_batch.append((task_idx, row_copy))
            
            # Add task batch
            if task_batch:
                added_tasks = self._add_row_batch(task_batch, "task", context)
                rows_added += len(added_tasks)
        
        return rows_added
    
    def _get_rows_by_indent(self, indent_level: int) -> List[Tuple[int, smartsheet.models.Row]]:
        """Get all template rows with specified indent level."""
        return [
            (idx, row) for idx, row in self.template_rows
            if self.INDENT_MAP.get(idx) == indent_level
        ]
    
    def _group_tasks_by_phase(self) -> Dict[int, List[int]]:
        """Group task indices by their parent phase index."""
        groups = defaultdict(list)
        
        for idx in range(1, len(self.template_rows) + 1):
            if self.INDENT_MAP.get(idx) == 2:  # Task level
                # Find parent phase (walk backwards to find indent level 1)
                for phase_idx in range(idx - 1, 0, -1):
                    if self.INDENT_MAP.get(phase_idx) == 1:
                        groups[phase_idx].append(idx)
                        break
        
        return groups
    
    def _copy_row(self, row: smartsheet.models.Row) -> smartsheet.models.Row:
        """Create a deep copy of a row."""
        new_row = smartsheet.models.Row()
        new_row.to_bottom = True
        
        for cell in row.cells:
            new_cell = smartsheet.models.Cell()
            new_cell.column_id = cell.column_id
            
            if hasattr(cell, 'value') and cell.value is not None:
                new_cell.value = cell.value
            if hasattr(cell, 'hyperlink') and cell.hyperlink:
                new_cell.hyperlink = smartsheet.models.Hyperlink({
                    'url': cell.hyperlink.url
                })
            
            new_row.cells.append(new_cell)
        
        return new_row
    
    def _add_row_batch(self, rows: List[Tuple[int, smartsheet.models.Row]], 
                      row_type: str, 
                      context: Optional[AssetExecutionContext] = None) -> Dict[int, int]:
        """
        Add a batch of rows and return mapping of template_idx to row_id.
        
        Args:
            rows: List of (template_index, row) tuples
            row_type: Type of rows being added (for logging)
            context: Optional Dagster context for logging
            
        Returns:
            Dict mapping template indices to created row IDs
        """
        mapping = {}
        
        if not rows:
            return mapping
        
        # Try batch add first
        try:
            row_objects = [row for _, row in rows]
            response = self.client.Sheets.add_rows(self.main_sheet_id, row_objects)
            
            # Map results
            if hasattr(response, 'result'):
                for i, row_result in enumerate(response.result):
                    if i < len(rows):
                        template_idx = rows[i][0]
                        mapping[template_idx] = row_result.id
            
            if context:
                context.log.debug(f"Batch added {len(mapping)} {row_type} rows")
            
        except Exception as e:
            if context:
                context.log.warning(f"Batch add failed: {e}. Falling back to individual adds")
            
            # Fall back to individual adds
            for template_idx, row in rows:
                try:
                    response = self.client.Sheets.add_rows(self.main_sheet_id, [row])
                    if hasattr(response, 'result') and response.result:
                        mapping[template_idx] = response.result[0].id
                except Exception as add_error:
                    if context:
                        context.log.error(f"Failed to add {row_type} row {template_idx}: {add_error}")
        
        return mapping


class ColumnFormulaManager:
    """
    Manages column formulas during batch operations.
    Temporarily disables and re-enables formulas to allow updates.
    """
    
    def __init__(self, ss_client: smartsheet.Smartsheet, sheet_id: int):
        """
        Initialize formula manager.
        
        Args:
            ss_client: Authenticated Smartsheet client
            sheet_id: Target sheet ID
        """
        self.client = ss_client
        self.sheet_id = sheet_id
        self.formula_storage = {}
    
    def disable_formulas(self, field_names: List[str]) -> Dict[int, str]:
        """
        Disable column formulas for specified fields.
        
        Args:
            field_names: List of field names to check for formulas
            
        Returns:
            Dict mapping column IDs to their formulas
        """
        try:
            sheet = self.client.Sheets.get_sheet(self.sheet_id)
            
            for col in sheet.columns:
                if col.title in field_names and hasattr(col, 'formula') and col.formula:
                    self.formula_storage[col.id] = col.formula
                    
                    # Create updated column without formula
                    update_col = smartsheet.models.Column({
                        'title': col.title,
                        'type': col.type,
                        'formula': ""
                    })
                    
                    self.client.Sheets.update_column(self.sheet_id, col.id, update_col)
            
        except Exception as e:
            print(f"Failed to disable column formulas: {e}")
        
        return self.formula_storage
    
    def enable_formulas(self) -> None:
        """Re-enable all previously disabled formulas."""
        if not self.formula_storage:
            return
        
        try:
            sheet = self.client.Sheets.get_sheet(self.sheet_id)
            
            for col in sheet.columns:
                if col.id in self.formula_storage:
                    # Create updated column with formula
                    update_col = smartsheet.models.Column({
                        'title': col.title,
                        'type': col.type,
                        'formula': self.formula_storage[col.id]
                    })
                    
                    self.client.Sheets.update_column(self.sheet_id, col.id, update_col)
            
            # Clear storage after re-enabling
            self.formula_storage.clear()
            
        except Exception as e:
            print(f"Failed to re-enable column formulas: {e}")