# src/qb_ss_insurance_tracker/resources/quickbase_resource.py
import os
import pyqb
import requests
from dagster import ConfigurableResource

class QuickBaseResource(ConfigurableResource):
    """QuickBase API client resource"""
    realm: str = 'https://enelgreenpowernorthamerica.quickbase.com'
    token_env_var: str = "QB_INS_TRACKER_TOKEN"
    
    def get_client(self):
        """Create and return authenticated QuickBase client"""
        token = os.environ.get(self.token_env_var)
        if not token:
            raise ValueError(f"Environment variable {self.token_env_var} not found")
        
        return pyqb.Client(url=self.realm, user_token=token)
    
    def get_headers(self):
        """Get headers for direct API requests"""
        token = os.environ.get(self.token_env_var)
        if not token:
            raise ValueError(f"Environment variable {self.token_env_var} not found")
        
        return {
            'QB-Realm-Hostname': self.realm,
            'Authorization': "QB-USER-TOKEN " + token
        }
    
    def make_request(self, method, endpoint, **kwargs):
        """Make authenticated request to QuickBase API"""
        url = f"https://api.quickbase.com/v1{endpoint}"
        return requests.request(
            method,
            url,
            headers=self.get_headers(),
            **kwargs
        )