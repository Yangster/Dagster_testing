# ay_test_project/ay_test_project/resources/quickbase_resource.py
import os
import requests
import time
import datetime
from collections import OrderedDict
from dagster import ConfigurableResource
from typing import Dict, List, Optional


class QuickBaseResource(ConfigurableResource):
    """QuickBase API client resource for insurance claim data"""
    realm: str = "https://enelgreenpowernorthamerica.quickbase.com"
    token_env_var: str = "QB_INS_TRACKER_TOKEN"
    outage_table_id: str = "bjfqxt7qp"
    
    # Query string from your original code
    query_string: str = (
        "({'516'.EX.'yes'}AND{'440'.OAF.'11-01-2024'})AND("
        "{'Total Downtime'.GT.'720'}OR("
        "{'Technology'.EX.'Wind'}AND{'Estimated cost to repair / return to service'.GT.'216000'}"
        ")OR(("
        "{'Technology'.EX.'Solar'}OR{'Technology'.EX.'BESS'}"
        ")AND{'Estimated cost to repair / return to service'.GT.'160000'}"
        "))"
    )
    
    def get_token(self) -> str:
        """Get QB token from environment"""
        token = os.environ.get(self.token_env_var)
        if not token:
            raise ValueError(f"Environment variable {self.token_env_var} not found")
        return token
    
    def get_headers(self) -> Dict[str, str]:
        """Get standard headers for QB API requests"""
        return {
            'QB-Realm-Hostname': self.realm,
            'Authorization': f"QB-USER-TOKEN {self.get_token()}"
        }
    
    def get_client(self):
        """Create pyqb client (if needed for compatibility)"""
        try:
            import pyqb
            return pyqb.Client(url=self.realm, user_token=self.get_token())
        except ImportError:
            raise ImportError("pyqb package required for legacy client support")
    
    def get_open_claims(self, qb_client=None) -> List[str]:
        """
        Find all open claims that meet insurance criteria.
        Returns list of claim Record IDs.
        """
        query_body = {
            "from": self.outage_table_id,
            "where": self.query_string,
            "select": [3]  # Record ID field
        }
        
        response = requests.post(
            'https://api.quickbase.com/v1/records/query',
            headers=self.get_headers(),
            json=query_body,
            timeout=30
        )
        
        if response.status_code != 200:
            raise ValueError(f"QB query failed with status {response.status_code}: {response.text}")
        
        qb_response = response.json()['data']
        open_claim_fids = []
        
        for record in qb_response:
            claim_fid = str(record['3']['value'])  # Record ID field
            if claim_fid not in open_claim_fids:
                open_claim_fids.append(claim_fid)
            else:
                print(f"#{claim_fid} is a duplicate claim!")
        
        return open_claim_fids
    
    def outage_tracker_query(self, record_id: str) -> Optional[Dict]:
        """
        Query QuickBase for a specific outage record.
        Returns dictionary of field data or None if not found.
        """
        query_string = f"{{'Record ID#'.EX.'{record_id}'}}"
        query_body = {
            "from": self.outage_table_id,
            "where": query_string
        }
        
        # Get QB field mapping
        from ay_test_project.utils.field_mappings import load_field_mapping
        qb_field_mapping = [field['qb_field'] for field in load_field_mapping()]
        
        # Get field schema
        field_request = requests.get(
            f'https://api.quickbase.com/v1/fields?tableId={self.outage_table_id}',
            headers=self.get_headers(),
            timeout=30
        )
        
        if field_request.status_code != 200:
            raise ValueError(f"Failed to get QB schema: {field_request.status_code}")
        
        outage_tracker_schema = field_request.json()
        outage_tracker_fields = [
            field for field in outage_tracker_schema 
            if field['label'] in qb_field_mapping
        ]
        
        # Build field ID lookup
        qb_field_id_lookup = {'515': "Unit # - Longitude"}  # Special case from your code
        for field in outage_tracker_fields:
            fid = str(field['id'])
            label = field['label']
            qb_field_id_lookup[fid] = label
        
        query_body["select"] = list(qb_field_id_lookup.keys())
        
        # Execute query with retries
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.post(
                    'https://api.quickbase.com/v1/records/query',
                    headers=self.get_headers(),
                    json=query_body,
                    timeout=30
                )
                
                if response.status_code != 200:
                    raise ValueError(f"Query failed: {response.status_code}")
                
                outage_tracker_raw_data = response.json()['data']
                break
                
            except Exception as e:
                print(f"Query attempt {attempt + 1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(5)
                else:
                    print("Query failed, returning None")
                    return None
        
        if not outage_tracker_raw_data:
            return None
        
        # Process response data
        outage_tracker_data = {}
        
        # Handle multiple results (from your original logic)
        if isinstance(outage_tracker_raw_data, list):
            print(f"{len(outage_tracker_raw_data)} query results found...")
            for row in outage_tracker_raw_data:
                result_record_id = row['3']['value']
                if str(result_record_id) == record_id:
                    for key, value in row.items():
                        if key in qb_field_id_lookup:
                            field_label = qb_field_id_lookup[key]
                            field_value = value['value']
                            outage_tracker_data[field_label] = field_value
                    break
        else:
            for key, value in outage_tracker_raw_data.items():
                if key in qb_field_id_lookup:
                    field_label = qb_field_id_lookup[key]
                    field_value = value['value']
                    outage_tracker_data[field_label] = field_value
        
        return outage_tracker_data
    
    def format_qb_data(self, field_mapping: List[Dict], outage_tracker_resp: Dict) -> Dict:
        """
        Format QB data for Smartsheet import.
        Handles data type conversions and field mapping.
        """
        qb_data = {}
        qb_field_list = [field["qb_field"] for field in field_mapping]
        
        for fld in qb_field_list:
            qb_value = outage_tracker_resp.get(fld)
            
            # Handle null values
            if qb_value is None and fld[-4:] != "Date":
                if fld == "Unit Number":
                    qb_data[fld] = "Multiple Units"
                    continue
                qb_value = "NO_DATA"
                qb_data[fld] = qb_value
                continue
            
            # Handle date fields
            if fld[-4:] == "Date":
                if qb_value is None:
                    qb_value = "2000-01-01T00:00:00Z"
                else:
                    try:
                        dt_object = datetime.datetime.fromtimestamp(float(qb_value) / 1000)
                        qb_value = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except (ValueError, TypeError):
                        pass  # Keep original value
                qb_data[fld] = qb_value
                continue
            
            # Handle latitude/longitude
            if fld in ["Unit # - Latitude", "Unit # - Longitude"]:
                try:
                    qb_data[fld] = float(qb_value)
                except (ValueError, TypeError):
                    qb_data[fld] = 0
                continue
            
            # Handle cost fields
            if fld == "Estimated cost to repair / return to service":
                try:
                    qb_data[fld] = "${:,.2f}".format(float(qb_value))
                except (ValueError, TypeError):
                    qb_data[fld] = "$0.00"
                continue
            
            # Handle downtime (milliseconds to hours)
            if fld == "Total Downtime":
                try:
                    downtime_adjusted = float(qb_value) / 3600000
                    qb_data[fld] = downtime_adjusted
                except (ValueError, TypeError):
                    qb_data[fld] = 0
                continue
            
            # Handle complex objects (from your original code)
            if isinstance(qb_value, (dict, OrderedDict)):
                qb_data[fld] = qb_value.get("#text", str(qb_value))
                continue
            
            # Default case
            qb_data[fld] = qb_value
        
        # Re-map fields to Smartsheet field names
        mapped_qb_data = {}
        for key, value in qb_data.items():
            for fm in field_mapping:
                if key == fm["qb_field"]:
                    mapped_qb_data[fm["ss_field"]] = value
        
        # Add formula placeholder
        mapped_qb_data["Claim Name / Task Name"] = None
        
        return mapped_qb_data