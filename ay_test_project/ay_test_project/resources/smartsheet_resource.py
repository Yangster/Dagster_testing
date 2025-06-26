import os
import smartsheet
from dagster import ConfigurableResource

class SmartsheetResource(ConfigurableResource):
    """Smartsheet API client resource"""
    bearer_token_env_var: str = "SMARTSHEETS_TOKEN"
    
    def get_client(self):
        """Create and return authenticated Smartsheet client"""
        bearer = os.environ.get(self.bearer_token_env_var)
        if not bearer:
            raise ValueError(f"Environment variable {self.bearer_token_env_var} not found")
        
        return smartsheet.Smartsheet(f"Bearer {bearer}")