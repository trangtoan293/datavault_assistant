import json
import yaml
from typing import Dict, Any

def read_json_file(file_path: str) -> Dict[str, Any]:
    """
    Đọc và parse JSON file
    
    Args:
        file_path (str): Đường dẫn tới file JSON
        
    Returns:
        Dict[str, Any]: Data được parse từ JSON file
        
    Raises:
        FileNotFoundError: Nếu file không tồn tại
        json.JSONDecodeError: Nếu file không đúng format JSON
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Lỗi parse JSON file: {str(e)}", e.doc, e.pos)

def transform_hub_metadata(input_json: Dict) -> str:
    """
    Transform hub metadata from input JSON format to output YAML format
    
    Args:
        input_json (str): Input metadata in JSON format
        
    Returns:
        str: Output metadata in YAML format
    """
    # Parse input JSON
    input_data = input_json
    
    # Khởi tạo output dictionary với structure cần thiết
    output_dict = {
        "source_schema": "source",
        "source_table": input_data["source_tables"][0].lower(),
        "target_schema": "integration_demo",
        "target_table": input_data["name"].lower(),
        "target_entity_type": "hub",
        "collision_code": input_data["name"].split("_")[1],  # Lấy phần sau HUB_
        "description": input_data["description"],
        "columns": []
    }
    
    # Thêm hash key column
    hash_key_column = {
        "target": f"dv_hkey_{output_dict['target_table']}",
        "dtype": "string",
        "key_type": "hash_key_hub",
        "source": input_data["business_keys"]
    }
    output_dict["columns"].append(hash_key_column)
    
    # Thêm business key column
    for biz_key in input_data["business_keys"]:
        biz_key_column = {
            "target": f"CUS_{biz_key}",
            "dtype": "int",
            "key_type": "biz_key",
            "source": {
                "name": biz_key,
                "dtype": "int"
            }
        }
        output_dict["columns"].append(biz_key_column)
    
    # Convert to YAML
    # return output_dict
    return yaml.dump(output_dict, sort_keys=False, allow_unicode=True)

# Test the transformation
if __name__ == "__main__":
    input_data = read_json_file(r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\hub_sample.json')
    # result = input_data['hubs']
    for hub in input_data['hubs']:
        print(transform_hub_metadata(hub))
    