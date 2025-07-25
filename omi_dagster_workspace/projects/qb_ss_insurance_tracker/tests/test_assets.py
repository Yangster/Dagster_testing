# tests/test_assets.py
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from dagster import materialize, AssetKey
from dagster_duckdb import DuckDBResource
from qb_ss_insurance_tracker.assets.quickbase_assets import qb_field_schema, qb_open_claims
from qb_ss_insurance_tracker.resources.quickbase_resource import QuickBaseResource
from qb_ss_insurance_tracker.resources.config_resource import FieldMappingResource

class MockQuickBaseResource(QuickBaseResource):
    """Mock QuickBase resource for testing"""
    
    def get_client(self):
        mock_client = Mock()
        # Mock the doquery response structure
        mock_client.doquery.return_value = {
            'record': [
                {'f': {'#text': '12345'}},
                {'f': {'#text': '12346'}},
                {'f': {'#text': '12347'}}
            ]
        }
        return mock_client
    
    def make_request(self, method, endpoint, **kwargs):
        mock_response = Mock()
        mock_response.status_code = 200
        
        # Mock field schema response
        if 'fields' in endpoint:
            mock_response.json.return_value = [
                {'id': 3, 'label': 'Record ID#', 'type': 'TEXT'},
                {'id': 7, 'label': 'Plant Name', 'type': 'TEXT'},
                {'id': 16, 'label': 'Technology', 'type': 'TEXT'}
            ]
        
        return mock_response

class MockFieldMappingResource(FieldMappingResource):
    """Mock field mapping resource for testing"""
    
    def load_field_mapping(self):
        return [
            {"qb_field": "Record ID#", "ss_field": "Record # in Outage Management (QB)*"},
            {"qb_field": "Plant Name", "ss_field": "Site Name (QB)*"},
            {"qb_field": "Technology", "ss_field": "Technology (QB)*"}
        ]

class TestQuickBaseAssets:
    """Test QuickBase assets"""
    
    @pytest.fixture
    def mock_duckdb(self):
        """Create a mock DuckDB resource for testing"""
        mock_db = Mock(spec=DuckDBResource)
        mock_conn = Mock()
        mock_db.get_connection.return_value.__enter__.return_value = mock_conn
        mock_db.get_connection.return_value.__exit__.return_value = None
        return mock_db
    
    def test_qb_field_schema_asset(self, mock_duckdb):
        """Test QB field schema extraction"""
        result = materialize(
            [qb_field_schema],
            resources={
                "quickbase_client": MockQuickBaseResource(),
                "field_mapping": MockFieldMappingResource(),
                "database": mock_duckdb
            }
        )
        
        assert result.success
        
        # Check that the asset materialized
        materialization = result.get_asset_materialization(AssetKey(["qb_field_schema"]))
        assert materialization is not None
        
        # Check metadata
        metadata = materialization.metadata
        assert "num_fields" in metadata
        assert metadata["num_fields"].value > 0
        assert "table_name" in metadata
        assert metadata["table_name"].value == "quickbase.field_schema"
    
    def test_qb_open_claims_asset(self, mock_duckdb):
        """Test QB open claims extraction"""
        from qb_ss_insurance_tracker.assets.quickbase_assets import QuickBaseConfig
        
        config = QuickBaseConfig(
            include_manual_claims=True,
            manual_claim_ids=['99999', '99998']
        )
        
        result = materialize(
            [qb_open_claims],
            resources={
                "quickbase_client": MockQuickBaseResource(),
                "database": mock_duckdb
            },
            run_config={
                "resources": {
                    "quickbase_client": {"config": {}},
                    "database": {"config": {}}
                },
                "ops": {
                    "qb_open_claims": {"config": config.dict()}
                }
            }
        )
        
        assert result.success
        
        materialization = result.get_asset_materialization(AssetKey(["qb_open_claims"]))
        metadata = materialization.metadata
        
        # Should have query claims + manual claims
        assert metadata["total_claims"].value >= 5  # 3 from mock + 2 manual
        assert metadata["manual_claims"].value == 2

class TestAssetIntegration:
    """Test asset interactions and dependencies"""
    
    @pytest.fixture
    def test_database(self, tmp_path):
        """Create a real temporary DuckDB for integration tests"""
        db_path = tmp_path / "test.duckdb"
        return DuckDBResource(database=str(db_path))
    
    def test_qb_extraction_pipeline(self, test_database):
        """Test the full QB extraction pipeline"""
        
        # Mock external dependencies but use real DuckDB
        result = materialize(
            [qb_field_schema, qb_open_claims],
            resources={
                "quickbase_client": MockQuickBaseResource(),
                "field_mapping": MockFieldMappingResource(),
                "database": test_database
            }
        )
        
        assert result.success
        
        # Verify data was stored in DuckDB
        with test_database.get_connection() as conn:
            # Check field schema table
            schema_result = conn.execute("SELECT COUNT(*) as count FROM quickbase.field_schema").fetch_df()
            assert schema_result.iloc[0]['count'] > 0
            
            # Check open claims table
            claims_result = conn.execute("SELECT COUNT(*) as count FROM quickbase.open_claims").fetch_df()
            assert claims_result.iloc[0]['count'] > 0
    
    def test_asset_dependency_order(self):
        """Test that assets execute in correct dependency order"""
        
        # This would test that qb_claim_details waits for qb_open_claims and qb_field_schema
        # Note: This is more of an integration test that would require more setup
        pass

class TestErrorHandling:
    """Test error handling in assets"""
    
    def test_qb_connection_failure(self, mock_duckdb):
        """Test handling of QB connection failures"""
        
        class FailingQuickBaseResource(QuickBaseResource):
            def make_request(self, method, endpoint, **kwargs):
                mock_response = Mock()
                mock_response.status_code = 500
                return mock_response
        
        # This should raise an exception
        with pytest.raises(ValueError, match="Failed to get QB schema"):
            materialize(
                [qb_field_schema],
                resources={
                    "quickbase_client": FailingQuickBaseResource(),
                    "field_mapping": MockFieldMappingResource(),
                    "database": mock_duckdb
                }
            )
    
    def test_missing_field_mapping(self, mock_duckdb):
        """Test handling of missing field mapping"""
        
        class EmptyFieldMappingResource(FieldMappingResource):
            def load_field_mapping(self):
                raise FileNotFoundError("Config file not found")
        
        with pytest.raises(FileNotFoundError):
            materialize(
                [qb_field_schema],
                resources={
                    "quickbase_client": MockQuickBaseResource(),
                    "field_mapping": EmptyFieldMappingResource(),
                    "database": mock_duckdb
                }
            )