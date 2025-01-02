from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Set, Optional
from pathlib import Path
import logging
import json
import yaml
import pandas as pd
from datetime import datetime

# Configuration using dataclass
@dataclass
class ParserConfig:
    version: str = "1.0.0"
    source_schema: str = "source"
    target_schema: str = "integration_demo"
    default_varchar_length: int = 255
    enable_detailed_logging: bool = True
    validation_level: str = "strict"

class DataVaultValidationError(Exception):
    """Custom exception for Data Vault validation errors"""
    pass

# Enhanced Logging Mixin
class LoggingMixin:
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('datavault_parser.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(self.__class__.__name__)

# Abstract Parser Interface
class DataVaultParser(ABC):
    @abstractmethod
    def parse(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def validate(self) -> List[str]:
        pass

# Data Type Service
class DataTypeService:
    def __init__(self, config: ParserConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def lookup_datatypes(self, business_keys: List[str], mapping_df: pd.DataFrame) -> Dict[str, Dict]:
        result = {}
        for key in business_keys:
            try:
                filtered_df = mapping_df[mapping_df['COLUMN_NAME'] == key]
                if filtered_df.empty:
                    result[key] = self._get_default_type(key)
                    self.logger.warning(f"Column {key} not found in mapping data, using default type")
                else:
                    column_info = filtered_df.iloc[0]
                    result[key] = self._process_column_type(key, column_info)
            except Exception as e:
                self.logger.error(f"Error processing column {key}: {str(e)}")
                raise
        return result

    def _get_default_type(self, col: str) -> Dict[str, Any]:
        return {
            'error': f'Column {col} not found in mapping data',
            'data_type': f'VARCHAR2({self.config.default_varchar_length})'
        }
        
    def _process_column_type(self, col: str, column_info: pd.Series) -> Dict[str, Any]:
        try:
            data_type = column_info['DATA_TYPE']
            length = str(column_info.get('LENGTH', '')).strip()
            
            if data_type == 'VARCHAR2':
                data_type = self._process_varchar(length)
                
            return {
                'data_type': data_type,
                'original_type': column_info['DATA_TYPE'],
                'length': length,
                'nullable': column_info.get('NULLABLE', True),
                'description': column_info.get('DESCRIPTION', '')
            }
        except KeyError as e:
            self.logger.error(f"Missing required column in mapping data: {e}")
            return self._get_default_type(col)
            
    def _process_varchar(self, length: str) -> str:
        if length and length not in ['-', '', ' ']:
            return f"VARCHAR2({length})"
        return f"VARCHAR2({self.config.default_varchar_length})"

# Hub Metadata Service
class HubMetadataService:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.hubs_metadata = {}
        
    def cache_hubs_metadata(self, data: Dict[str, Any]) -> None:
        self.logger.info("Caching hubs metadata")
        self.hubs_metadata = {
            hub["name"]: {
                "business_keys": set(hub["business_keys"]),
                "source_tables": set(hub["source_tables"])
            }
            for hub in data.get("hubs", [])
        }
        
    def get_hub_business_keys(self, hub_name: str, link_keys: List[str]) -> Set[str]:
        if hub_name not in self.hubs_metadata:
            raise DataVaultValidationError(f"Unknown hub: {hub_name}")
            
        hub_keys = self.hubs_metadata[hub_name]["business_keys"]
        return set(link_keys).intersection(hub_keys)

# Link Parser Implementation
class LinkParser(DataVaultParser, LoggingMixin):
    def __init__(self, config: ParserConfig):
        self.config = config
        self.logger = self.setup_logging()
        self.datatype_service = DataTypeService(config)
        self.hub_service = HubMetadataService()
        
    def parse(self, link_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Any]:
        """Main parsing method for link metadata"""
        try:
            self.logger.info(f"Parsing link: {link_data['name']}")
            
            # Get source schema and validate
            filtered_df = mapping_df[mapping_df['TABLE_NAME'].isin(link_data["source_tables"])]
            source_schema = self._get_source_schema(filtered_df)
            
            # Validate and get data types
            validation_warnings = self.validate(link_data)
            datatype_info = self.datatype_service.lookup_datatypes(link_data["business_keys"], filtered_df)
            
            return self._build_output_dict(link_data, source_schema, datatype_info, validation_warnings)
            
        except Exception as e:
            self.logger.error(f"Error in link transformation: {str(e)}")
            raise
    
    def validate(self, link_data: Dict[str, Any]) -> List[str]:
        """Validate link metadata"""
        warnings = []
        link_keys = set(link_data["business_keys"])
        link_name = link_data["name"]
        
        # Collect all hub business keys
        all_hub_keys = set()
        for hub_name in link_data["related_hubs"]:
            if hub_name not in self.hub_service.hubs_metadata:
                raise DataVaultValidationError(f"Link {link_name} references non-existent hub: {hub_name}")
                
            hub_keys = self.hub_service.hubs_metadata[hub_name]["business_keys"]
            all_hub_keys.update(hub_keys)
            
            warnings.extend(self._validate_hub_keys(link_name, hub_name, link_keys, hub_keys))
            
        # Check extra keys
        extra_keys = link_keys - all_hub_keys
        if extra_keys:
            raise DataVaultValidationError(
                f"Link {link_name} contains business keys that don't belong to any related hub: {sorted(list(extra_keys))}"
            )
            
        return warnings
    
    def _validate_hub_keys(self, link_name: str, hub_name: str, 
                          link_keys: Set[str], hub_keys: Set[str]) -> List[str]:
        warnings = []
        
        # Check missing keys
        missing_keys = hub_keys - link_keys
        if missing_keys:
            warnings.append(
                f"Link {link_name} is missing business keys from hub {hub_name}: {sorted(list(missing_keys))}"
            )
        
        # Check if link has any keys from this hub
        hub_related_keys = link_keys.intersection(hub_keys)
        if not hub_related_keys:
            warnings.append(
                f"Link {link_name} has no business keys from hub {hub_name}"
            )
            
        return warnings
    
    def _get_source_schema(self, filtered_df: pd.DataFrame) -> str:
        if filtered_df.empty:
            raise ValueError("Could not determine source schema from mapping data")
        return filtered_df['SCHEMA_NAME'].iloc[0]
    
    def _build_metadata(self, warnings: List[str]) -> Dict[str, Any]:
        """Build metadata section"""
        return {
            "created_at": datetime.now().isoformat(),
            "version": self.config.version,
            "validation_status": "valid" if not warnings else "warnings",
            "validation_warnings": warnings if warnings else None
        }
        
    def _build_columns(self, link_data: Dict[str, Any], datatype_info: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Build columns section"""
        columns = []
        
        # Add link hash key
        columns.append({
            "target": f"dv_hkey_{link_data['name'].lower()}",
            "dtype": "raw",
            "key_type": "hash_key_lnk",
            "source": [
                key for key in link_data["business_keys"]
            ]
        })
        
        # Add hub hash keys
        for hub_name in link_data["related_hubs"]:
            hub_keys = self.hub_service.get_hub_business_keys(hub_name, link_data["business_keys"])
            columns.append({
                "target": f"dv_hkey_{hub_name.lower()}",
                "dtype": "raw",
                "key_type": "hash_key_hub",
                "parent": hub_name.lower(),
                "source": [
                    {
                        "name": key,
                        "dtype": datatype_info[key]['data_type']
                    } for key in hub_keys
                ]
            })
            
        return columns
        
    def _build_output_dict(self, link_data: Dict[str, Any], source_schema: str,
                          datatype_info: Dict[str, Dict], warnings: List[str]) -> Dict[str, Any]:
        """Build the output dictionary"""
        return {
            "source_schema": source_schema,
            "source_table": link_data["source_tables"][0],
            "target_schema": self.config.target_schema,
            "target_table": link_data["name"],
            "target_entity_type": "lnk",
            "collision_code": "mdm",
            "description": link_data["description"],
            "metadata": self._build_metadata(warnings),
            "columns": self._build_columns(link_data, datatype_info)
        }

# File Processor
class FileProcessor:
    def __init__(self, parser: DataVaultParser, config: ParserConfig):
        self.parser = parser
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def process_file(self, input_file: Path, mapping_file: Path, output_dir: Path) -> None:
        """Process input files and generate output"""
        try:
            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Read input files
            input_data = self._read_json(input_file)
            mapping_df = pd.read_csv(mapping_file)
            
            # Cache hub metadata
            if isinstance(self.parser, LinkParser):
                self.parser.hub_service.cache_hubs_metadata(input_data)
            
            # Process links
            results = []
            for link in input_data.get("links", []):
                try:
                    self.logger.info(f"Processing link: {link['name']}")
                    result = self.parser.parse(link, mapping_df)
                    
                    # Save individual link file
                    output_file = output_dir / f"{link['name'].lower()}_metadata.yaml"
                    self._save_yaml(result, output_file)
                    
                    results.append({
                        "link": link["name"],
                        "status": result["metadata"]["validation_status"],
                        "warnings": result["metadata"].get("validation_warnings", [])
                    })
                    
                except Exception as e:
                    self.logger.error(f"Error processing link {link.get('name')}: {str(e)}")
                    results.append({
                        "link": link.get("name"),
                        "status": "error",
                        "error": str(e)
                    })
            
            # Save processing summary
            self._save_processing_summary(results, output_dir)
            
        except Exception as e:
            self.logger.error(f"Error processing file: {str(e)}")
            raise
            
    def _read_json(self, file_path: Path) -> Dict[str, Any]:
        """Read and parse JSON file"""
        try:
            if not file_path.exists():
                raise FileNotFoundError(f"File không tồn tại: {file_path}")
                
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Lỗi parse JSON: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error reading JSON: {str(e)}")
            
    def _save_yaml(self, data: Dict[str, Any], output_path: Path) -> None:
        """Save data to YAML file"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, sort_keys=False, allow_unicode=True)
            self.logger.info(f"Saved YAML to: {output_path}")
        except Exception as e:
            raise Exception(f"Error saving YAML: {str(e)}")
            
    def _save_processing_summary(self, results: List[Dict[str, Any]], output_dir: Path) -> None:
        """Save processing summary"""
        summary = {
            "processing_summary": {
                "processed_at": datetime.now().isoformat(),
                "total_links": len(results),
                "successful": sum(1 for r in results if r["status"] != "error"),
                "warnings": sum(1 for r in results if r["status"] == "warnings"),
                "errors": sum(1 for r in results if r["status"] == "error"),
                "details": results
            }
        }
        
        summary_file = output_dir / "processing_link_summary.yaml"
        self._save_yaml(summary, summary_file)

def main():
    config = ParserConfig()
    parser = LinkParser(config)
    processor = FileProcessor(parser, config)
    
    
    try:
        processor.process_file(
            input_file=Path(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\sat_lsat.json"),
            output_dir=Path("output"),
            mapping_file=Path(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv")
        )
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()