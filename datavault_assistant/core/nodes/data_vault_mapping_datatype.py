import pandas as pd
import json
from typing import Dict, List, Optional

def lookup_column_datatypes(hub_config: Dict, mapping_df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Tra cứu thông tin data type của các business keys từ mapping data
    
    Args:
        hub_config (Dict): Configuration của hub dạng JSON
        mapping_df (pd.DataFrame): DataFrame chứa thông tin mapping
        
    Returns:
        Dict[str, Dict]: Dictionary chứa thông tin data type của mỗi business key
    """
    # Extract các thông tin cần thiết từ hub config
    business_keys = hub_config.get("business_keys", [])
    source_tables = hub_config.get("source_tables", [])
    target_tables = hub_config.get("name")
    # Tạo dictionary để lưu kết quả
    result = {}
    
    # Filter mapping_df theo source tables
    filtered_df = mapping_df[mapping_df['TABLE_NAME'].isin(source_tables)]
    
    # Lookup từng business key
    for key in business_keys:
        # Tìm thông tin column trong mapping data
        column_info = filtered_df[filtered_df['COLUMN_NAME'] == key].iloc[0] if not filtered_df[filtered_df['COLUMN_NAME'] == key].empty else None
        
        if column_info is not None:
            result[key] = {
                'data_type': column_info['DATA_TYPE'],
                'length': column_info['LENGTH'],
                'nullable': column_info['NULLABLE'],
                'description': column_info['DESCRIPTION'],
                'source_table': column_info['TABLE_NAME'],
                'target_tabel' : target_tables
            }
        else:
            result[key] = {
                'error': f'Column {key} not found in mapping data'
            }
            
    return result

# Example usage
if __name__ == "__main__":
    # Read mapping CSV
    mapping_df = pd.read_csv(r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv')
    
    # Hub config
    hub_config = {
        "name": "HUB_ADDRESS",
        "business_keys": ["ADDRESS_LINE1", "ADDRESS_LINE2", "ADDRESS_LINE3", "COUNTRY"],
        "source_tables": ["STTM_CUSTOMER"],
        "description": "Address information associated with a customer."
    }
    
    # Get column data types
    result = lookup_column_datatypes(hub_config, mapping_df)
    print(json.dumps(result, indent=2))