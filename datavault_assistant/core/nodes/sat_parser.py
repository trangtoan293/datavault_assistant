from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
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
    default_varchar_length: int = 255
    enable_detailed_logging: bool = True
    validation_level: str = "strict"
    target_schema: str = "integration"

# Base Parser Interface
class DataVaultParser(ABC):
    @abstractmethod
    def parse(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def validate(self) -> List[str]:
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

# Data Type Service
class DataTypeService:
    def __init__(self, config: ParserConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def lookup_datatypes(self, columns: List[str], mapping_df: pd.DataFrame) -> Dict[str, Dict]:
        result = {}
        for col in columns:
            try:
                filtered_df = mapping_df[mapping_df['COLUMN_NAME'] == col]
                if filtered_df.empty:
                    result[col] = self._get_default_type(col)
                    self.logger.warning(f"Column {col} not found in mapping data, using default type")
                else:
                    column_info = filtered_df.iloc[0]
                    result[col] = self._process_column_type(col, column_info)
            except Exception as e:
                self.logger.error(f"Error processing column {col}: {str(e)}")
                raise
        return result
    
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
    
    def _get_default_type(self, col: str) -> Dict[str, Any]:
        return {
            'error': f"Column {col} not found in mapping data",
            'data_type': f"VARCHAR2({self.config.default_varchar_length})"
        }
        
    def _process_varchar(self, length: str) -> str:
        return f"VARCHAR2({length})" if length and length != '-' else f"VARCHAR2({self.config.default_varchar_length})"

# Satellite Parser Implementation
class SatelliteParser(DataVaultParser, LoggingMixin):
    def __init__(self, config: ParserConfig):
        self.config = config
        self.logger = self.setup_logging()
        self.datatype_service = DataTypeService(config)
        
    def parse(self, sat_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Any]:
        """Main parsing method for satellite metadata"""
        try:
            self.logger.info(f"Starting transformation for satellite: {sat_data['name']}")
            
            source_schema = self._get_source_schema(sat_data, mapping_df)
            datatype_info = self._get_datatype_info(sat_data, mapping_df)
            validation_warnings = self.validate(sat_data)
            
            return self._build_output_dict(sat_data, source_schema, datatype_info, validation_warnings)
            
        except Exception as e:
            self.logger.error(f"Error in satellite transformation: {str(e)}")
            raise
    
    def validate(self, sat_data: Dict[str, Any]) -> List[str]:
        """Implement validation logic"""
        warnings = []
        # Add validation logic here
        return warnings
    
    def _get_source_schema(self, sat_data: Dict[str, Any], mapping_df: pd.DataFrame) -> str:
        filtered_df = mapping_df[mapping_df['TABLE_NAME'] == sat_data["source_table"]]
        return filtered_df['SCHEMA_NAME'].iloc[0] if not filtered_df.empty else None
    
    def _get_datatype_info(self, sat_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Dict]:
        all_columns = sat_data["business_keys"] + sat_data["descriptive_attrs"]
        return self.datatype_service.lookup_datatypes(all_columns, mapping_df)
    
    def _build_metadata(self, warnings: List[str]) -> Dict[str, Any]:
        """Build metadata section for output"""
        return {
            "created_at": datetime.now().isoformat(),
            "version": self.config.version,
            "validation_status": "valid" if not warnings else "warnings",
            "validation_warnings": warnings if warnings else None
        }

    def _build_columns(self, sat_data: Dict[str, Any], datatype_info: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Build columns section for output"""
        columns = []
        
        # Add satellite hash key
        columns.append({
            "target": f"dv_hkey_{sat_data['name'].lower()}",
            "dtype": "raw",
            "key_type": "hash_key_sat",
            "source": None
        })
        
        # Add hub hash key
        columns.append({
            "target": f"dv_hkey_{sat_data['hub'].lower()}",
            "dtype": "raw", 
            "key_type": "hash_key_hub",
            "source": [key for key in sat_data["business_keys"]]
        })
        
        # Add hash diff
        columns.append({
            "target": "dv_hsh_diff",
            "dtype": "raw",
            "key_type": "hash_diff",
            "source": None
        })
        
        # Add descriptive attributes
        for attr in sat_data["descriptive_attrs"]:
            columns.append({
                "target": attr,
                "dtype": datatype_info[attr]['data_type'],
                "source": {
                    "name": attr,
                    "dtype": datatype_info[attr]['data_type']
                }
            })
            
        return columns

    def _build_output_dict(self, sat_data: Dict[str, Any], source_schema: str, 
                          datatype_info: Dict[str, Dict], warnings: List[str]) -> Dict[str, Any]:
        """Build the output dictionary with all necessary metadata"""
        output_dict = {
            "source_schema": source_schema,
            "source_table": sat_data["source_table"],
            "target_schema": self.config.target_schema,
            "target_table": sat_data["name"],
            "target_entity_type": "sat",
            "parent_table": sat_data["hub"],
            "metadata": self._build_metadata(warnings),
            "columns": self._build_columns(sat_data, datatype_info)
        }
        return output_dict

# File Processor
class FileProcessor:
    def __init__(self, parser: DataVaultParser, config: ParserConfig):
        self.parser = parser
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def process_file(self, input_file: Path, mapping_file: Path, output_dir: Path):
        """Process input files and generate output YAML files"""
        try:
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Read input files
            self.logger.info(f"Reading input file: {input_file}")
            input_data = self._read_json(input_file)
            
            self.logger.info(f"Reading mapping file: {mapping_file}")
            mapping_df = pd.read_csv(mapping_file)
            
            # Process each satellite
            results=[]
            for sat in input_data.get("satellites", []):
                try: 
                    self.logger.info(f"Processing satellite: {sat.get('name')}")
                    result = self.parser.parse(sat, mapping_df)
                    output_file = output_dir / f"{sat['name'].lower()}_metadata.yaml"
                    self._save_yaml(result, output_file)
                    results.append({
                            "lsat": sat["name"],
                            "status": result["metadata"]["validation_status"],
                            "warnings": result["metadata"].get("validation_warnings", [])
                        })
                        
                except Exception as e:
                    self.logger.error(f"Error processing lsat {sat.get('name')}: {str(e)}")
                    results.append({
                        "lsat": sat.get("name"),
                        "status": "error",
                        "error": str(e)
                    })                
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
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, sort_keys=False, allow_unicode=True)
                
            self.logger.info(f"Successfully saved YAML to: {output_path}")
        except Exception as e:
            raise Exception(f"Error saving YAML: {str(e)}")
        
    def _save_processing_summary(self, results: List[Dict[str, Any]], output_dir: Path) -> None:
        """Save processing summary"""
        summary = {
            "processing_summary": {
                "processed_at": datetime.now().isoformat(),
                "total_hubs": len(results),
                "successful": sum(1 for r in results if r["status"] != "error"),
                "warnings": sum(1 for r in results if r["status"] == "warnings"),
                "errors": sum(1 for r in results if r["status"] == "error"),
                "details": results
            }
        }
        
        summary_file = output_dir / "processing_sat_summary.yaml"
        self._save_yaml(summary, summary_file)

def main():
    config = ParserConfig()
    parser = SatelliteParser(config)
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