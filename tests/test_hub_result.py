# tests/test_data_processor.py
from deepdiff import DeepDiff
import yaml
import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import Mock, patch

def test_process_hub_metadata_matches(data_processor, sample_input_data, 
                                    sample_mapping_data, expected_hub_metadata, 
                                    tmp_path):
    """Test kết quả xử lý hub metadata khớp với expected YAML"""
    
    # Mock hub parser để return kết quả mong đợi
    with patch.object(data_processor.hub_parser, 'parse') as mock_hub_parse:
        mock_hub_parse.return_value = expected_hub_metadata
        
        # Thực thi process_data
        result = data_processor.process_data(
            input_data=sample_input_data,
            mapping_data=sample_mapping_data,
            output_dir=tmp_path
        )
        
        # Đọc file YAML được tạo ra
        output_file = tmp_path / "hub_customer_metadata.yaml"
        with open(output_file, 'r', encoding='utf-8') as f:
            actual_yaml = yaml.safe_load(f)
            
        # Bỏ qua so sánh created_at vì nó sẽ khác nhau
        if 'created_at' in actual_yaml.get('metadata', {}):
            del actual_yaml['metadata']['created_at']
        if 'created_at' in expected_hub_metadata.get('metadata', {}):
            del expected_hub_metadata['metadata']['created_at']
            
        # So sánh chi tiết structure
        diff = DeepDiff(actual_yaml, expected_hub_metadata, ignore_order=True)
        
        assert not diff, f"Differences found: {diff}"
        
        # Kiểm tra các giá trị quan trọng
        assert actual_yaml['target_table'] == expected_hub_metadata['target_table']
        assert actual_yaml['target_entity_type'] == expected_hub_metadata['target_entity_type']
        assert actual_yaml['collision_code'] == expected_hub_metadata['collision_code']
        
        # Kiểm tra columns
        assert len(actual_yaml['columns']) == len(expected_hub_metadata['columns'])
        
        # Kiểm tra hash key
        hash_key_col = next(col for col in actual_yaml['columns'] 
                           if col['key_type'] == 'hash_key_hub')
        expected_hash_key = next(col for col in expected_hub_metadata['columns'] 
                               if col['key_type'] == 'hash_key_hub')
        assert hash_key_col['target'] == expected_hash_key['target']
        assert hash_key_col['source'] == expected_hash_key['source']
        
        # Kiểm tra business keys
        biz_key_cols = [col for col in actual_yaml['columns'] 
                       if col['key_type'] == 'biz_key']
        expected_biz_keys = [col for col in expected_hub_metadata['columns'] 
                            if col['key_type'] == 'biz_key']
        
        assert len(biz_key_cols) == len(expected_biz_keys)
        for actual, expected in zip(biz_key_cols, expected_biz_keys):
            assert actual['target'] == expected['target']
            assert actual['dtype'] == expected['dtype']
            assert actual['source']['name'] == expected['source']['name']

