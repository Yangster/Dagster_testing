# src/economic_curtailment_thresholds/resources/pi_webapi_resource.py
import requests
from requests_kerberos import HTTPKerberosAuth, DISABLED
from dagster import ConfigurableResource

class PIWebAPIResource(ConfigurableResource):
    """PI Web API client resource"""
    base_url: str = "https://egpna-pi-web.enelint.global/piwebapi"
    database_webid: str = "F1RDfBX_hedyUEKNiDBi2ij-swoeupCWwgO0Sod5PaUcAa2wRUdQTkEtQkEtQUZcRUdQTkE"
    
    def get_auth(self):
        """Get Kerberos authentication"""
        return HTTPKerberosAuth(mutual_authentication=DISABLED)
    
    def get_headers(self):
        """Get standard headers for PI Web API requests"""
        return {
            "Host": "egpna-pi-web",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/json",
        }
    
    def make_request(self, method, endpoint, **kwargs):
        """Make authenticated request to PI Web API"""
        url = f"{self.base_url}{endpoint}"
        return requests.request(
            method,
            url,
            auth=self.get_auth(),
            headers=self.get_headers(),
            verify=True,
            **kwargs
        )