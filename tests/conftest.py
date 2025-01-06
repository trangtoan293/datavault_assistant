import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import Mock, patch
from datavault_assistant.core.nodes.data_vault_parser import DataProcessor
from datavault_assistant.configs.settings import ParserConfig
import json
# Fixtures cơ bản
@pytest.fixture
def sample_config():
    return ParserConfig()

@pytest.fixture
def data_processor(sample_config):
    return DataProcessor(config=sample_config)

@pytest.fixture
def sample_input_data():
    """
    Đọc sample data từ JSON file
    Returns:
        Dict chứa test data
    """
    # Lấy đường dẫn tới thư mục hiện tại của file test
    current_dir = Path(__file__).parent
    
    # Tạo đường dẫn tới file sample data
    sample_file = current_dir / "test_data" / "sample_data.json"
    
    # Đọc và return data
    with open(sample_file, 'r', encoding='utf-8') as f:
        return json.load(f)

@pytest.fixture
def sample_mapping_data():
    """DataFrame mapping mẫu cho test"""
    current_dir = Path(__file__).parent
    
    # Tạo đường dẫn tới file sample data
    sample_file = current_dir / "test_data" / "metadata_src.csv"
    return pd.read_csv(sample_file)


# tests/conftest.py
import pytest
import yaml
from pathlib import Path

@pytest.fixture
def expected_hub_metadata():
    """Fixture chứa expected metadata của hub"""
    return {
        "source_schema": "FLEXLIVE",
        "source_table": "sttm_customer",
        "target_schema": "integration",
        "target_table": "hub_customer",
        "target_entity_type": "hub",
        "collision_code": "CUSTOMER",
        "description": "Customer information, including personal details and identification numbers.",
        "metadata": {
            "version": "1.0.0",
            "validation_status": "valid",
            "validation_warnings": None
        },
        "columns": [
            {
                "target": "dv_hkey_hub_customer",
                "dtype": "raw",
                "key_type": "hash_key_hub",
                "source": [
                    "CUSTOMER_NO",
                    "UNIQUE_ID_VALUE"
                ]
            },
            {
                "target": "CUS_CUSTOMER_NO",
                "dtype": "VARCHAR2(255)",
                "key_type": "biz_key",
                "source": {
                    "name": "CUSTOMER_NO",
                    "dtype": "VARCHAR2(255)"
                }
            },
            {
                "target": "CUS_UNIQUE_ID_VALUE",
                "dtype": "VARCHAR2(20)",
                "key_type": "biz_key",
                "source": {
                    "name": "UNIQUE_ID_VALUE",
                    "dtype": "VARCHAR2(20)"
                }
            }
        ]
    }