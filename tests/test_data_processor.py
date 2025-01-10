import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import Mock, patch

def test_process_data_error_handling(data_processor, sample_input_data, 
                                   sample_mapping_data, tmp_path):
    """Test xử lý lỗi trong quá trình process"""
    
    # Mock để tạo lỗi
    with patch.object(data_processor.hub_parser, 'parse') as mock_hub_parse:
        mock_hub_parse.side_effect = ValueError("Test error")
        
        # Thực thi test
        result = data_processor.process_data(
            input_data=sample_input_data,
            mapping_data=sample_mapping_data,
            output_dir=tmp_path
        )
        
        # Kiểm tra kết quả
        assert "hubs" in result
        assert len(result["hubs"]) == 4
        assert result["hubs"][0]["status"] == "error"



def test_process_link_with_hub_metadata(data_processor,sample_input_data, sample_mapping_data, tmp_path):
    """Test xử lý link với hub metadata"""
    
    # Mock các dependencies
    with patch.object(data_processor.hub_parser, 'parse') as mock_hub_parse, \
         patch.object(data_processor.link_parser, 'parse') as mock_link_parse:
            
        mock_hub_parse.return_value = {
            "metadata": {"validation_status": "success"}
        }
        mock_link_parse.return_value = {
            "metadata": {"validation_status": "success"}
        }
        
        # Thực thi test
        result = data_processor.process_data(
            input_data=sample_input_data,
            mapping_data=sample_mapping_data,
            output_dir=tmp_path
        )
        
        # Kiểm tra kết quả
        assert len(result["hubs"]) == 4
        assert len(result["links"]) == 3