import json
import yaml
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd
import time

# Setup logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('datavault_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataVaultParserException(Exception):
    """Custom exception for Data Vault Parser errors"""
    pass

class DataVaultValidationError(Exception):
    """Custom exception for Data Vault validation errors"""
    pass

class SatDataVaultParser:
    """Parser class để transform Data Vault metadata từ JSON sang YAML format với data type mapping"""
    
    def __init__(self, 
                target_schema: str = "integration",
                config: Optional[Dict] = None):
        """
        Initialize parser với configurable options
        
        Args:
            target_schema (str): Target schema name
            config (Dict, optional): Configuration dictionary
        """
        self.target_schema = target_schema
        self.hubs_metadata = {}
        self.config = config or {
            "version": "1.0.0",
            "default_varchar_length": 255,
            "enable_detailed_logging": True,
            "validation_level": "strict"  # strict/relaxed
        }
        self.validation_warnings = []
        self.processing_stats = {
            "start_time": None,
            "end_time": None,
            "processed_satellites": 0,
            "warnings_count": 0,
            "errors_count": 0
        }

    def _lookup_datatypes(self, columns: List[str], filtered_df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Enhanced datatype lookup với better error handling
        """
        result = {}
        for col in columns:
            try:
                column_info = filtered_df[filtered_df['COLUMN_NAME'] == col].iloc[0] if not filtered_df[filtered_df['COLUMN_NAME'] == col].empty else None
                
                if column_info is not None:
                    data_type = column_info['DATA_TYPE']
                    length = str(column_info['LENGTH']).strip()
                    
                    # Enhanced VARCHAR2 handling
                    if data_type == 'VARCHAR2':
                        if length and length != '-':
                            data_type = f"VARCHAR2({length})"
                        else:
                            data_type = f"VARCHAR2({self.config['default_varchar_length']})"
                            logger.warning(f"Using default length for {col}: {data_type}")
                    
                    result[col] = {
                        'data_type': data_type,
                        'original_type': column_info['DATA_TYPE'],
                        'length': length,
                        'nullable': column_info['NULLABLE'],
                        'description': column_info['DESCRIPTION']
                    }
                else:
                    msg = f"Column {col} not found in mapping data"
                    result[col] = {
                        'error': msg,
                        'data_type': f"VARCHAR2({self.config['default_varchar_length']})"
                    }
                    self.validation_warnings.append(msg)
                    logger.warning(msg)
                    
            except Exception as e:
                logger.error(f"Error processing column {col}: {str(e)}")
                raise DataVaultParserException(f"Error in datatype lookup for column {col}")
                
        return result
    
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
        
    # def _validate_satellite_metadata(self, sat_data: Dict[str, Any]) -> None:
    #     """
    #     Validate satellite metadata
        
    #     Args:
    #         sat_data (Dict[str, Any]): Satellite metadata cần validate
            
    #     Raises:
    #         DataVaultValidationError: Khi data không hợp lệ
    #     """
    #     required_fields = ['name', 'hub', 'business_keys', 'source_table', 'descriptive_attrs']
    #     for field in required_fields:
    #         if field not in sat_data:
    #             raise DataVaultValidationError(f"Missing required field: {field}")
            
    #     # if not sat_data['descriptive_attrs']:
    #     #     raise DataVaultValidationError("descriptive_attrs cannot be empty")
            
    #     if not sat_data['business_keys']:
    #         raise DataVaultValidationError("business_keys cannot be empty")
        
    def _validate_satellite_business_keys(self, sat_data: Dict[str, Any]) -> List[str]:
        """
        Validate satellite business keys với parent hub
        
        Args:
            sat_data (Dict[str, Any]): Satellite metadata
            
        Returns:
            List[str]: List of validation warnings
        """
        warnings = []
        hub_name = sat_data["hub"]
        
        # Check if parent hub exists
        if hub_name not in self.hubs_metadata:
            raise DataVaultValidationError(f"Parent hub does not exist: {hub_name}")
            
        # Check business keys consistency with parent hub
        hub_keys = self.hubs_metadata[hub_name]["business_keys"]
        sat_keys = set(sat_data["business_keys"])
        
        # Check missing keys
        missing_keys = hub_keys - sat_keys
        if missing_keys:
            warning_msg = f"Satellite {sat_data['name']} is missing business keys from parent hub {hub_name}: {sorted(list(missing_keys))}"
            warnings.append(warning_msg)
            
        # Check extra keys
        extra_keys = sat_keys - hub_keys
        if extra_keys:
            warning_msg = f"Satellite {sat_data['name']} contains extra business keys not in parent hub {hub_name}: {sorted(list(extra_keys))}"
            warnings.append(warning_msg)
            
        return warnings               
    def transform_satellite_metadata_with_datatypes(self, 
                                                  sat_data: Dict[str, Any], 
                                                  mapping_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Enhanced satellite metadata transformation với datatype mapping
        """
        start_time = time.time()
        logger.info(f"Starting transformation for satellite: {sat_data['name']}")
        
        try:
            # Filter mapping data và get source schema
            logger.debug(f"Filtering mapping data for table: {sat_data['source_table']}")
            filtered_df = mapping_df[mapping_df['TABLE_NAME'] == sat_data["source_table"]]
            logger.debug(f"Found {len(filtered_df)} rows in mapping data")
            
            source_schema = filtered_df['SCHEMA_NAME'].iloc[0] if not filtered_df.empty else None
            logger.info(f"Determined source schema: {source_schema}")
            
            if not source_schema:
                raise DataVaultValidationError(f"Could not determine source schema for table: {sat_data['source_table']}")
            
            # Setup hub metadata for validation
            logger.debug(f"Setting up hub metadata for validation: {sat_data['hub']}")
            self.hubs_metadata=sat_data['hub']
            
            # Lookup datatypes for all columns
            logger.info("Starting datatype lookup for columns")
            all_columns = sat_data["business_keys"] + sat_data["descriptive_attrs"]
            logger.debug(f"Processing {len(all_columns)} total columns")
            
            datatype_info = self._lookup_datatypes(all_columns, filtered_df)
            logger.info(f"Completed datatype lookup. Found types for {len(datatype_info)} columns")
            
            # Generate warnings from business keys validation
            logger.info("Validating business keys")
            warnings = None # self._validate_satellite_business_keys(sat_data)
            if warnings:
                logger.warning(f"Found {len(warnings)} validation warnings")
            
            # Calculate processing info
            end_time = time.time()
            processing_info = {
                "start_time": datetime.fromtimestamp(start_time).isoformat(),
                "end_time": datetime.fromtimestamp(end_time).isoformat(),
                "duration_seconds": round(end_time - start_time, 3)
            }
            logger.debug(f"Processing duration: {processing_info['duration_seconds']} seconds")
            
            logger.info("Building output dictionary structure")
            output_dict = {
                "source_schema": source_schema,
                "source_table": sat_data["source_table"],
                "target_schema": self.target_schema,
                "target_table": sat_data["name"],
                "target_entity_type": "sat",
                "parent_table": sat_data["hub"],
                "collision_code": sat_data["name"].split("_")[1],
                "description": sat_data.get("description", ""),
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "version": self.config["version"],
                    "validation_status": "valid" if not warnings else "warnings",
                    "processing_info": processing_info,
                    "validation_warnings": warnings if warnings else None
                },
                "columns": []
            }
            
            # Add column definitions
            logger.info("Adding column definitions")
            
            logger.debug("Adding satellite hash key")
            sat_hash_key = {
                "target": f"dv_hkey_{sat_data['name'].lower()}",
                "dtype": "string",
                "key_type": "hash_key_sat",
                "source": None
            }
            output_dict["columns"].append(sat_hash_key)
            
            logger.debug("Adding hub hash key")
            hub_hash_key = {
                "target": f"dv_hkey_{sat_data['hub'].lower()}",
                "dtype": "string",
                "key_type": "hash_key_hub",
                "source": [key for key in sat_data["business_keys"]]
            }
            output_dict["columns"].append(hub_hash_key)
            
            logger.debug("Adding hash diff column")
            hash_diff = {
                "target": "dv_hsh_diff",
                "dtype": "string",
                "key_type": "hash_diff",
                "source": None
            }
            output_dict["columns"].append(hash_diff)
            
            logger.info(f"Adding {len(sat_data['descriptive_attrs'])} descriptive attributes")
            for attr in sat_data["descriptive_attrs"]:
                logger.debug(f"Processing descriptive attribute: {attr}")
                attr_column = {
                    "target": attr,
                    "dtype": datatype_info[attr]['data_type'],
                    "key_type": "descriptive",
                    "source": {
                        "name": attr,
                        "dtype": datatype_info[attr]['data_type']
                    }
                }
                output_dict["columns"].append(attr_column)
            
            logger.info(f"Successfully transformed satellite: {sat_data['name']}")
            return output_dict
            
        except Exception as e:
            logger.error(f"Error transforming satellite {sat_data['name']}: {str(e)}")
            raise DataVaultParserException(f"Error in satellite transformation: {str(e)}")

    def process_satellite_file(self, 
                             input_file: str, 
                             mapping_file: str,
                             output_dir: str) -> None:
        """
        Process complete satellite definition file với enhanced error handling và logging
        """
        self.processing_stats["start_time"] = time.time()
        
        try:
            logger.info(f"Starting satellite processing from file: {input_file}")
            logger.debug(f"Input parameters - input_file: {input_file}, mapping_file: {mapping_file}, output_dir: {output_dir}")
            
            # Read input files
            logger.debug("Reading input JSON file...")
            input_data = self.read_json_file(input_file)
            logger.debug(f"Found {len(input_data.get('satellites', []))} satellites in input file")
            
            logger.debug("Reading mapping CSV file...")
            mapping_df = pd.read_csv(mapping_file)
            logger.debug(f"Loaded mapping data with {len(mapping_df)} rows")
            
            # Setup output directory
            output_dir = Path(output_dir)
            logger.debug(f"Creating output directory: {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Process satellites
            total_satellites = len(input_data.get("satellites", []))
            logger.info(f"Beginning to process {total_satellites} satellites")
            
            for idx, sat in enumerate(input_data.get("satellites", []), 1):
                try:
                    logger.info(f"Processing satellite {idx}/{total_satellites}: {sat.get('name', 'unknown')}")
                    self.processing_stats["processed_satellites"] += 1
                    
                    logger.debug(f"Transforming metadata for satellite: {sat.get('name')}")
                    transformed_data = self.transform_satellite_metadata_with_datatypes(sat, mapping_df)
                    
                    # Save individual satellite file
                    output_file = output_dir / f"{sat['name'].lower()}_metadata.yaml"
                    logger.debug(f"Saving transformed data to: {output_file}")
                    self.save_yaml(transformed_data, output_file)
                    
                    if transformed_data["metadata"]["validation_warnings"]:
                        warning_count = len(transformed_data["metadata"]["validation_warnings"])
                        self.processing_stats["warnings_count"] += warning_count
                        logger.warning(f"Found {warning_count} warnings for satellite {sat['name']}")
                        
                except Exception as e:
                    self.processing_stats["errors_count"] += 1
                    logger.error(f"Error processing satellite {sat.get('name', 'unknown')}: {str(e)}", exc_info=True)
            
            # Generate and save processing summary
            self.processing_stats["end_time"] = time.time()
            logger.debug("Generating processing summary...")
            self._save_processing_summary(output_dir)
            
            # Log final statistics
            duration = round(self.processing_stats["end_time"] - self.processing_stats["start_time"], 2)
            logger.info(f"Satellite processing completed in {duration} seconds")
            logger.info(f"Processed {self.processing_stats['processed_satellites']} satellites")
            logger.info(f"Found {self.processing_stats['warnings_count']} warnings")
            logger.info(f"Encountered {self.processing_stats['errors_count']} errors")
            
        except Exception as e:
            logger.error(f"Error in satellite file processing: {str(e)}", exc_info=True)
            raise DataVaultParserException(f"Error processing satellite file: {str(e)}")

    def _save_processing_summary(self, output_dir: Path) -> None:
        """
        Save processing summary to file
        """
        duration = round(self.processing_stats["end_time"] - self.processing_stats["start_time"], 3)
        
        summary = {
            "processing_summary": {
                "timestamp": datetime.now().isoformat(),
                "total_duration_seconds": duration,
                "processed_satellites": self.processing_stats["processed_satellites"],
                "warnings_count": self.processing_stats["warnings_count"],
                "errors_count": self.processing_stats["errors_count"],
                "config_used": self.config
            }
        }
        
        summary_file = output_dir / "processing_summary.yaml"
        self.save_yaml(summary, summary_file)
        logger.info(f"Saved processing summary to: {summary_file}")

def main():
    # Configure parser với custom settings
    config = {
        "version": "1.0.0",
        "default_varchar_length": 255,
        "enable_detailed_logging": True,
        "validation_level": "strict"
    }
    
    parser = SatDataVaultParser(
        target_schema="integration",
        config=config
    )
    
    try:
        parser.process_satellite_file(
                input_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\sat_lsat.json',
                output_dir = "output",
                mapping_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv'
        )
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()

# def main():
#     parser = SatDataVaultParser()
#     input_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\hub_link.json'
#     output_dir = "output"
#     mapping_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv'
#     try:
#         parser.process_hub_file_v2(input_file, output_dir,mapping_file)
#     except Exception as e:
#         logger.error(f"Error in main: {str(e)}")

# if __name__ == "__main__":
#     main()