import json
import yaml
from typing import Dict, Any, List
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataVaultParserException(Exception):
    """Custom exception for Data Vault Parser errors"""
    pass

class HubDataVaultParser:
    """Parser class để transform Data Vault metadata từ JSON sang YAML format"""
    
    def __init__(self, 
                 source_schema: str = "source",
                 target_schema: str = "integration_demo"):
        self.source_schema = source_schema
        self.target_schema = target_schema
        
    def read_json_file(self, file_path: str) -> Dict[str, Any]:
        """
        Đọc và parse JSON file
        
        Args:
            file_path (str): Đường dẫn tới file JSON
            
        Returns:
            Dict[str, Any]: Parsed JSON data
            
        Raises:
            DataVaultParserException: Khi có lỗi đọc hoặc parse file
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise DataVaultParserException(f"File không tồn tại: {file_path}")
                
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise DataVaultParserException(f"Lỗi parse JSON: {str(e)}")
        except Exception as e:
            raise DataVaultParserException(f"Unexpected error: {str(e)}")

    def _validate_hub_metadata(self, hub_data: Dict[str, Any]) -> None:
        """
        Validate hub metadata
        
        Args:
            hub_data (Dict[str, Any]): Hub metadata cần validate
            
        Raises:
            DataVaultParserException: Khi data không hợp lệ
        """
        required_fields = ['name', 'business_keys', 'source_tables', 'description']
        for field in required_fields:
            if field not in hub_data:
                raise DataVaultParserException(f"Missing required field: {field}")
                
        if not hub_data['business_keys']:
            raise DataVaultParserException("Business keys cannot be empty")
            
        if not hub_data['source_tables']:
            raise DataVaultParserException("Source tables cannot be empty")

    def transform_hub_metadata(self, hub_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform hub metadata sang format mới
        
        Args:
            hub_data (Dict[str, Any]): Input hub metadata
            
        Returns:
            Dict[str, Any]: Transformed metadata
        """
        self._validate_hub_metadata(hub_data)
        
        output_dict = {
            "source_schema": self.source_schema,
            "source_table": hub_data["source_tables"][0].lower(),
            "target_schema": self.target_schema,
            "target_table": hub_data["name"].lower(),
            "target_entity_type": "hub",
            "collision_code": hub_data["name"].split("_")[1],
            "description": hub_data["description"],
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "version": "1.0"
            },
            "columns": []
        }
        
        # Add hash key column
        hash_key_column = {
            "target": f"dv_hkey_{output_dict['target_table']}",
            "dtype": "string",
            "key_type": "hash_key_hub",
            "source": hub_data["business_keys"]
        }
        output_dict["columns"].append(hash_key_column)
        
        # Add business key columns
        for biz_key in hub_data["business_keys"]:
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
            
        return output_dict

    def save_yaml(self, data: Dict[str, Any], output_path: str) -> None:
        """
        Save data to YAML file
        
        Args:
            data (Dict[str, Any]): Data to save
            output_path (str): Output file path
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, sort_keys=False, allow_unicode=True)
                
            logger.info(f"Successfully saved YAML to: {output_path}")
        except Exception as e:
            raise DataVaultParserException(f"Error saving YAML: {str(e)}")

    def process_hub_file(self, input_path: str, output_dir: str) -> None:
        """
        Process một file hub metadata
        
        Args:
            input_path (str): Input JSON file path
            output_dir (str): Output directory for YAML files
        """
        try:
            logger.info(f"Processing file: {input_path}")
            
            # Read and parse input file
            input_data = self.read_json_file(input_path)
            
            # Process each hub
            for idx, hub in enumerate(input_data.get('hubs', [])):
                try:
                    # Transform hub metadata
                    transformed_data = self.transform_hub_metadata(hub)
                    
                    # Generate output filename
                    output_file = Path(output_dir) / f"{hub['name'].lower()}_metadata.yaml"
                    
                    # Save to YAML
                    self.save_yaml(transformed_data, output_file)
                    
                except Exception as e:
                    logger.error(f"Error processing hub {idx + 1}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Failed to process file: {str(e)}")
            raise

def main():
    # Example usage
    parser = HubDataVaultParser()
    input_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\hub_link.json'
    output_dir = "output"
    
    try:
        parser.process_hub_file(input_file, output_dir)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()