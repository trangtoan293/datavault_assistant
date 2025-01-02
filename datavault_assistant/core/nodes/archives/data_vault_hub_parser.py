import json
import yaml
from typing import Dict, Any, List
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd

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
                 target_schema: str = "integration"):
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
        
    def process_hub_file_v2(self, input_path: Dict[str, Any], output_dir: str,mapping_file: Dict[str, Any]) -> None:
        """
        Process hub metadata from a JSON object
        
        Args:
            input_data (Dict[str, Any]): Input JSON data
            output_dir (str): Output directory for YAML files
        """
        try:
            logger.info("Processing input data")
            mapping_df = pd.read_csv(mapping_file)
            input_data = self.read_json_file(input_path)
            # Process each hub
            for idx, hub in enumerate(input_data.get('hubs', [])):
                try:
                    # Transform hub metadata
                    transformed_data = self.transform_hub_metadata_with_datatypes(hub,mapping_df)
                    
                    # Generate output filename
                    output_file = Path(output_dir) / f"{hub['name'].lower()}_metadata.yaml"
                    
                    # Save to YAML
                    self.save_yaml(transformed_data, output_file)
                    
                except Exception as e:
                    logger.error(f"Error processing hub {idx + 1}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Failed to process input data: {str(e)}")
            raise
        
    def _lookup_datatypes(self, business_keys: list, filtered_df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Tra cứu thông tin data type của các business keys
        """
        result = {}
        for key in business_keys:
            column_info = filtered_df[filtered_df['COLUMN_NAME'] == key].iloc[0] if not filtered_df[filtered_df['COLUMN_NAME'] == key].empty else None
            
            if column_info is not None:
                # Xử lý datatype VARCHAR2 với length
                data_type = column_info['DATA_TYPE']
                if data_type == 'VARCHAR2' and column_info['LENGTH'] not in ['-', '',' ']:
                    data_type = f"VARCHAR2({column_info['LENGTH']})"
                elif data_type == 'VARCHAR2':
                    data_type = 'VARCHAR2(255)'  # default fallback for VARCHAR2
                result[key] = {
                    'data_type': data_type,
                    'original_type': column_info['DATA_TYPE'],
                    'length': column_info['LENGTH'],
                    'nullable': column_info['NULLABLE'],
                    'description': column_info['DESCRIPTION']
                }
            else:
                result[key] = {
                    'error': f'Column {key} not found in mapping data',
                    'data_type': 'VARCHAR2(255)'  # default fallback for VARCHAR2
                }
                
        return result
    
    def transform_hub_metadata_with_datatypes(self, hub_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Transform hub metadata với mapping datatype
        
        Args:
            hub_data (Dict[str, Any]): Input hub metadata
            mapping_df (pd.DataFrame): DataFrame chứa mapping data
            
        Returns:
            Dict[str, Any]: Transformed metadata với mapped datatypes
        """
        # Validate input
        self._validate_hub_metadata(hub_data)
        
        # Get basic info
        business_keys = hub_data.get("business_keys", [])
        source_tables = hub_data.get("source_tables", [])
        
        # Filter mapping data
        filtered_df = mapping_df[mapping_df['TABLE_NAME'].isin(source_tables)]
        
        # Lookup datatypes
        datatype_info = self._lookup_datatypes(business_keys, filtered_df)
        
        # Construct output
        # Lấy source schema từ mapping_df
        source_schema = filtered_df['SCHEMA_NAME'].iloc[0] if not filtered_df.empty else None
        if not source_schema:
            raise ValueError("Could not determine source schema from mapping data")
            
        output_dict = {
            "source_schema": source_schema,
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
        
        # Add hash key column với composite datatype từ business keys
        hash_key_column = {
            "target": f"dv_hkey_{output_dict['target_table']}",
            "dtype": "raw",  # Hash key luôn là string
            "key_type": "hash_key_hub",
            "source": [
                {
                    "name": key,
                    "dtype": datatype_info[key]['data_type']
                } for key in business_keys
            ]
        }
        output_dict["columns"].append(hash_key_column)
        
        # Add business key columns với mapped datatypes
        for biz_key in business_keys:
            biz_key_column = {
                "target": f"CUS_{biz_key}",
                "dtype": datatype_info[biz_key]['data_type'],
                "key_type": "biz_key",
                "source": {
                    "name": biz_key,
                    "dtype": datatype_info[biz_key]['data_type']
                }
            }
            output_dict["columns"].append(biz_key_column)
            
        return output_dict

def main():
    parser = HubDataVaultParser()
    input_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\hub_link.json'
    output_dir = "output"
    mapping_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv'
    try:
        parser.process_hub_file_v2(input_file, output_dir,mapping_file)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()