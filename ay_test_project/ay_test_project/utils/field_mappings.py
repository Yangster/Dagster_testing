# ay_test_project/ay_test_project/utils/field_mappings.py
import json
import os
from typing import List, Dict

def load_field_mapping() -> List[Dict]:
    """Load field mapping from config.json file"""
    config_file_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
        "config", 
        "config.json"
    )
    
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Config file not found: {config_file_path}")
    
    with open(config_file_path, "r") as json_file:
        field_mapping = json.load(json_file)
    
    return field_mapping

# Remove the hardcoded dictionary entirely