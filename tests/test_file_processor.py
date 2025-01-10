# tests/test_data_processor.py
import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import Mock, patch
    
def test_process_file_success(data_processor, tmp_path):
    """Test xử lý file thành công"""
    # Tạo test files
    input_file = tmp_path / "input.json"
    mapping_file = tmp_path / "mapping.csv"
    output_dir = tmp_path / "output"
    
    # Lưu test data
    input_file.write_text('{"hubs": [{"name": "TestHub"}]}')
    pd.DataFrame({'source': ['col1']}).to_csv(mapping_file, index=False)
    
    # Mock các dependencies
    with patch.object(data_processor.hub_parser, 'parse') as mock_hub_parse:
        mock_hub_parse.return_value = {
            "metadata": {
                "validation_status": "success"
            }
        }
        
        # Thực thi function test
        result = data_processor.process_file(
            input_file=input_file,
            mapping_file=mapping_file,
            output_dir=output_dir
        )
        
        # Kiểm tra kết quả
        assert "hubs" in result
        assert len(result["hubs"]) == 1
        assert result["hubs"][0]["status"] == "success"