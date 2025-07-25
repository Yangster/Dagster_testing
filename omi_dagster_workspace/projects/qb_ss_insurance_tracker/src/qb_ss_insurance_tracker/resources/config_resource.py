# src/qb_ss_insurance_tracker/resources/config_resource.py
import json
import os
from pathlib import Path
from dagster import ConfigurableResource
from typing import List, Dict, Any

class FieldMappingResource(ConfigurableResource):
    """Resource for managing field mapping configuration"""
    config_file: str = "config.json"
    
    def load_field_mapping(self) -> List[Dict[str, str]]:
        """Load field mapping from config file"""
        # Look for config file relative to this file's location
        config_path = Path(__file__).parent.parent / "config" / self.config_file
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def get_qb_fields(self) -> List[str]:
        """Get list of QuickBase field names"""
        mapping = self.load_field_mapping()
        return [field["qb_field"] for field in mapping]
    
    def get_ss_fields(self) -> List[str]:
        """Get list of Smartsheet field names"""
        mapping = self.load_field_mapping()
        return [field["ss_field"] for field in mapping]
    
    def map_qb_to_ss(self, qb_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map QuickBase data to Smartsheet field names"""
        mapping = self.load_field_mapping()
        mapped_data = {}
        
        for qb_field, qb_value in qb_data.items():
            for field_map in mapping:
                if qb_field == field_map["qb_field"]:
                    mapped_data[field_map["ss_field"]] = qb_value
                    break
        
        # Add placeholder for formula field
        mapped_data["Claim Name / Task Name"] = None
        return mapped_data