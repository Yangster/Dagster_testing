# tests/test_resources.py
import pytest
import os
from unittest.mock import patch, Mock
from qb_ss_insurance_tracker.resources.quickbase_resource import QuickBaseResource
from qb_ss_insurance_tracker.resources.smartsheet_resource import SmartsheetResource
from qb_ss_insurance_tracker.resources.config_resource import FieldMappingResource

class TestQuickBaseResource:
    """Test QuickBase resource functionality"""
    
    @patch.dict(os.environ, {'QB_INS_TRACKER_TOKEN': 'test_token'})
    def test_quickbase_resource_initialization(self):
        """Test that QuickBase resource initializes correctly"""
        resource = QuickBaseResource()
        
        # Test headers generation
        headers = resource.get_headers()
        assert 'QB-Realm-Hostname' in headers
        assert 'Authorization' in headers
        assert headers['Authorization'] == 'QB-USER-TOKEN test_token'
    
    def test_quickbase_resource_missing_token(self):
        """Test that missing token raises appropriate error"""
        with patch.dict(os.environ, {}, clear=True):
            resource = QuickBaseResource()
            
            with pytest.raises(ValueError, match="Environment variable QB_INS_TRACKER_TOKEN not found"):
                resource.get_headers()
    
    @patch('qb_ss_insurance_tracker.resources.quickbase_resource.pyqb.Client')
    @patch.dict(os.environ, {'QB_INS_TRACKER_TOKEN': 'test_token'})
    def test_quickbase_client_creation(self, mock_pyqb):
        """Test QB client creation"""
        resource = QuickBaseResource()
        client = resource.get_client()
        
        mock_pyqb.assert_called_once_with(
            url='https://enelgreenpowernorthamerica.quickbase.com',
            user_token='test_token'
        )

class TestSmartsheetResource:
    """Test Smartsheet resource functionality"""
    
    @patch.dict(os.environ, {'SMARTSHEETS_TOKEN': 'test_ss_token'})
    def test_smartsheet_resource_initialization(self):
        """Test that Smartsheet resource initializes correctly"""
        resource = SmartsheetResource()
        
        headers = resource.get_headers()
        assert 'Authorization' in headers
        assert headers['Authorization'] == 'Bearer test_ss_token'
    
    @patch('qb_ss_insurance_tracker.resources.smartsheet_resource.smartsheet.Smartsheet')
    @patch.dict(os.environ, {'SMARTSHEETS_TOKEN': 'test_ss_token'})
    def test_smartsheet_client_creation(self, mock_smartsheet):
        """Test Smartsheet client creation"""
        resource = SmartsheetResource()
        client = resource.get_client()
        
        mock_smartsheet.assert_called_once_with('Bearer test_ss_token')

class TestFieldMappingResource:
    """Test field mapping configuration resource"""
    
    @patch('builtins.open')
    @patch('qb_ss_insurance_tracker.resources.config_resource.Path.exists')
    def test_load_field_mapping(self, mock_exists, mock_open):
        """Test loading field mapping from config file"""
        mock_exists.return_value = True
        
        # Mock config file content
        test_config = [
            {"qb_field": "Record ID#", "ss_field": "Record # in Outage Management (QB)*"},
            {"qb_field": "Plant Name", "ss_field": "Site Name (QB)*"}
        ]
        
        mock_file = Mock()
        mock_file.read.return_value = str(test_config).replace("'", '"')
        mock_open.return_value.__enter__.return_value = mock_file
        
        resource = FieldMappingResource()
        
        with patch('json.load', return_value=test_config):
            mapping = resource.load_field_mapping()
        
        assert len(mapping) == 2
        assert mapping[0]['qb_field'] == 'Record ID#'
    
    def test_missing_config_file(self):
        """Test behavior when config file is missing"""
        with patch('qb_ss_insurance_tracker.resources.config_resource.Path.exists', return_value=False):
            resource = FieldMappingResource()
            
            with pytest.raises(FileNotFoundError):
                resource.load_field_mapping()
    
    def test_get_qb_fields(self):
        """Test extraction of QB field names"""
        resource = FieldMappingResource()
        
        test_mapping = [
            {"qb_field": "Field1", "ss_field": "SS_Field1"},
            {"qb_field": "Field2", "ss_field": "SS_Field2"}
        ]
        
        with patch.object(resource, 'load_field_mapping', return_value=test_mapping):
            qb_fields = resource.get_qb_fields()
        
        assert qb_fields == ["Field1", "Field2"]
    
    def test_map_qb_to_ss(self):
        """Test mapping QB data to SS field names"""
        resource = FieldMappingResource()
        
        test_mapping = [
            {"qb_field": "Record ID#", "ss_field": "Record # in Outage Management (QB)*"},
            {"qb_field": "Plant Name", "ss_field": "Site Name (QB)*"}
        ]
        
        qb_data = {
            "Record ID#": "12345",
            "Plant Name": "Test Plant",
            "Other Field": "Should be ignored"
        }
        
        with patch.object(resource, 'load_field_mapping', return_value=test_mapping):
            mapped_data = resource.map_qb_to_ss(qb_data)
        
        expected = {
            "Record # in Outage Management (QB)*": "12345",
            "Site Name (QB)*": "Test Plant",
            "Claim Name / Task Name": None  # Added by the method
        }
        
        assert mapped_data == expected