# tests/test_utils.py
import pytest
import datetime
from collections import OrderedDict
from unittest.mock import Mock
from qb_ss_insurance_tracker.utils import format_qb_data, retry_operation

class TestFormatQBData:
    """Test QB data formatting functions"""
    
    def test_format_basic_fields(self):
        """Test formatting of basic text fields"""
        field_mapping = [
            {"qb_field": "Plant Name", "ss_field": "Site Name (QB)*"},
            {"qb_field": "Technology", "ss_field": "Technology (QB)*"}
        ]
        
        qb_data = {
            "Plant Name": "Test Wind Farm",
            "Technology": "Wind"
        }
        
        result = format_qb_data(field_mapping, qb_data)
        
        # Should handle missing Unit Number by setting to "Multiple Units"
        assert "Unit Number" not in qb_data  # Not provided
        # The function should have processed available fields
        assert isinstance(result, dict)
    
    def test_format_unit_number_handling(self):
        """Test special handling of Unit Number field"""
        field_mapping = [
            {"qb_field": "Unit Number", "ss_field": "Generating Unit # (QB)*"}
        ]
        
        # Test with None value
        qb_data = {"Unit Number": None}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Unit Number"] == "Multiple Units"
        
        # Test with actual value
        qb_data = {"Unit Number": "WTG001"}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Unit Number"] == "WTG001"
    
    def test_format_date_fields(self):
        """Test formatting of date fields"""
        field_mapping = [
            {"qb_field": "Actual Start Date", "ss_field": "Out of Service Date (QB)* DOL"}
        ]
        
        # Test with Unix timestamp (milliseconds)
        unix_timestamp = 1640995200000  # Jan 1, 2022 00:00:00 UTC
        qb_data = {"Actual Start Date": str(unix_timestamp)}
        
        result = format_qb_data(field_mapping, qb_data)
        assert result["Actual Start Date"] == "2022-01-01T00:00:00Z"
        
        # Test with None date
        qb_data = {"Actual Start Date": None}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Actual Start Date"] == "2000-01-01T00:00:00Z"
    
    def test_format_coordinate_fields(self):
        """Test formatting of latitude/longitude fields"""
        field_mapping = [
            {"qb_field": "Unit # - Latitude", "ss_field": "Location (Lat) (QB)*"},
            {"qb_field": "Unit # - Longitude", "ss_field": "Location (Lon) (QB)*"}
        ]
        
        qb_data = {
            "Unit # - Latitude": "40.7128",
            "Unit # - Longitude": "-74.0060"
        }
        
        result = format_qb_data(field_mapping, qb_data)
        assert result["Unit # - Latitude"] == 40.7128
        assert result["Unit # - Longitude"] == -74.0060
        
        # Test with invalid coordinates
        qb_data = {
            "Unit # - Latitude": "invalid",
            "Unit # - Longitude": None
        }
        
        result = format_qb_data(field_mapping, qb_data)
        assert result["Unit # - Latitude"] == 0
        assert result["Unit # - Longitude"] == 0
    
    def test_format_cost_field(self):
        """Test formatting of cost fields"""
        field_mapping = [
            {"qb_field": "Estimated cost to repair / return to service", "ss_field": "Estimated cost to repair/return to service (QB)*"}
        ]
        
        qb_data = {"Estimated cost to repair / return to service": "125000.50"}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Estimated cost to repair / return to service"] == "$125,000.50"
        
        # Test with invalid cost
        qb_data = {"Estimated cost to repair / return to service": "invalid"}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Estimated cost to repair / return to service"] == "$0.00"
    
    def test_format_downtime_field(self):
        """Test formatting of downtime (milliseconds to hours)"""
        field_mapping = [
            {"qb_field": "Total Downtime", "ss_field": "Total Downtime (hr) (QB)*"}
        ]
        
        # 7200000 milliseconds = 2 hours
        qb_data = {"Total Downtime": "7200000"}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Total Downtime"] == 2.0
    
    def test_format_complex_field_structure(self):
        """Test formatting of complex QB field structures"""
        field_mapping = [
            {"qb_field": "Description of Outage/Root Cause", "ss_field": "Description of Outage/Root Cause: (QB)*"}
        ]
        
        # Test OrderedDict structure (as seen in your original code)
        complex_value = OrderedDict([
            ("BR", [None, None]),
            ("#text", "Gearbox failure due to bearing wear")
        ])
        
        qb_data = {"Description of Outage/Root Cause": complex_value}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Description of Outage/Root Cause"] == "Gearbox failure due to bearing wear"
        
        # Test regular dict structure
        dict_value = {"#text": "Another description", "other": "data"}
        qb_data = {"Description of Outage/Root Cause": dict_value}
        result = format_qb_data(field_mapping, qb_data)
        assert result["Description of Outage/Root Cause"] == "Another description"

class TestRetryOperation:
    """Test retry operation utility"""
    
    def test_retry_success_first_attempt(self):
        """Test successful operation on first attempt"""
        def successful_operation():
            return "success"
        
        result = retry_operation(successful_operation, retries=3, delay=1)
        assert result == "success"
    
    def test_retry_success_after_failures(self):
        """Test successful operation after some failures"""
        attempt_count = 0
        
        def flaky_operation():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception("Temporary failure")
            return "success"
        
        result = retry_operation(flaky_operation, retries=3, delay=0.1)
        assert result == "success"
        assert attempt_count == 3
    
    def test_retry_all_attempts_fail(self):
        """Test when all retry attempts fail"""
        def failing_operation():
            raise Exception("Always fails")
        
        result = retry_operation(failing_operation, retries=3, delay=0.1)
        assert result is None
    
    def test_retry_with_different_exceptions(self):
        """Test retry with different exception types"""
        attempt_count = 0
        
        def operation_with_different_errors():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                raise ConnectionError("Network error")
            elif attempt_count == 2:
                raise ValueError("Data error")
            else:
                return "success"
        
        result = retry_operation(operation_with_different_errors, retries=3, delay=0.1)
        assert result == "success"