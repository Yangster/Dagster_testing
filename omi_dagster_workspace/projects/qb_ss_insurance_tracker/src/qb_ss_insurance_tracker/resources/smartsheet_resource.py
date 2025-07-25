# src/qb_ss_insurance_tracker/resources/smartsheet_resource.py
import os
import smartsheet
import requests
from dagster import ConfigurableResource
from typing import Dict, Any

class SmartsheetResource(ConfigurableResource):
    """Smartsheet API client resource"""
    bearer_token_env_var: str = "SMARTSHEETS_TOKEN"
    
    def get_client(self):
        """Create and return authenticated Smartsheet client"""
        bearer = os.environ.get(self.bearer_token_env_var)
        if not bearer:
            raise ValueError(f"Environment variable {self.bearer_token_env_var} not found")
        
        return smartsheet.Smartsheet(f"Bearer {bearer}")
    
    def get_headers(self):
        """Get headers for direct API requests"""
        bearer = os.environ.get(self.bearer_token_env_var)
        if not bearer:
            raise ValueError(f"Environment variable {self.bearer_token_env_var} not found")
            
        return {
            "Authorization": f"Bearer {bearer}",
            "ContentType": "application/json"
        }
    
    def make_request(self, method, endpoint, **kwargs):
        """Make authenticated request to Smartsheet API"""
        url = f"https://api.smartsheet.com/2.0{endpoint}"
        return requests.request(
            method,
            url,
            headers=self.get_headers(),
            **kwargs
        )