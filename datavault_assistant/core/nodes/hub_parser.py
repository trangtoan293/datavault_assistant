from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging
import json
import yaml
import pandas as pd
from datetime import datetime
import numpy as np
from datavault_assistant.configs.settings import ParserConfig


class DataVaultParserException(Exception):
    """Custom exception for Data Vault Parser errors"""
    pass

# Enhanced Logging Mixin
class LoggingMixin:
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data_vault_builder.log'),
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
            
            if data_type.upper() == 'VARCHAR2':
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
        import numpy as np
        if length and length.lower() not in ['-', ' ','nan']:
            return f"VARCHAR2({length})"
        return f"VARCHAR2({self.config.default_varchar_length})"

# Hub Parser Implementation
class HubParser(DataVaultParser, LoggingMixin):
    def __init__(self, config: ParserConfig):
        self.config = config
        self.logger = self.setup_logging()
        self.datatype_service = DataTypeService(config)
        
    def parse(self, hub_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Any]:
        """Main parsing method for hub metadata"""
        try:
            self.logger.info(f"Parsing hub: {hub_data['name']}")
            
            # Validate and get warnings
            self.logger.info(f"validate hub: {hub_data['name']}")
            validation_warnings = self.validate(hub_data)
            
            
            # Get source schema and validate
            self.logger.info(f"Get source schema and validate hub: {hub_data['name']}")
            filtered_df = mapping_df[mapping_df['TABLE_NAME'].isin(hub_data["source_tables"])]
            source_schema = self._get_source_schema(filtered_df)
            
            
            # Get datatypes for business keys
            self.logger.info(f"Get datatypes for business keys: {hub_data['name']}")
            datatype_info = self.datatype_service.lookup_datatypes(hub_data["business_keys"], filtered_df)
            
            self.logger.info(f"Get output yml format file: {hub_data['name']}")
            return self._build_output_dict(hub_data, source_schema, datatype_info, validation_warnings)
            
        except Exception as e:
            self.logger.error(f"Error in hub transformation: {str(e)}")
            raise
    
    def validate(self, hub_data: Dict[str, Any]) -> List[str]:
        """Validate hub metadata"""
        warnings = []
        required_fields = ['name', 'business_keys', 'source_tables', 'description']
        
        # Check required fields
        for field in required_fields:
            if field not in hub_data:
                raise DataVaultParserException(f"Missing required field: {field}")
        
        # Validate business keys
        if not hub_data['business_keys']:
            raise DataVaultParserException("Business keys cannot be empty")
            
        # Validate source tables
        if not hub_data['source_tables']:
            raise DataVaultParserException("Source tables cannot be empty")
            
        return warnings
        
    def _get_source_schema(self, filtered_df: pd.DataFrame) -> str:
        """Get source schema from mapping DataFrame"""
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
        
    def _build_columns(self, hub_data: Dict[str, Any], datatype_info: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Build columns section"""
        columns = []
        
        # Add hash key column
        columns.append({
            "target": f"DV_HKEY_{hub_data['name'].upper()}",
            "dtype": "raw",
            "key_type": "hash_key_hub",
            "source": [
                key for key in hub_data["business_keys"]
            ]
        })
        
        # Add business key columns
        for biz_key in hub_data["business_keys"]:
            columns.append({
                "target": f"{biz_key}",
                "dtype": datatype_info[biz_key]['data_type'],
                "key_type": "biz_key",
                "source": {
                    "name": biz_key,
                    "dtype": datatype_info[biz_key]['data_type']
                }
            })
            
        return columns
        
    def _build_output_dict(self, hub_data: Dict[str, Any], source_schema: str,
                          datatype_info: Dict[str, Dict], warnings: List[str]) -> Dict[str, Any]:
        """Build the output dictionary"""
        return {
            "source_schema": source_schema.upper(),
            "source_table": hub_data["source_tables"][0].upper(),
            "target_schema": self.config.target_schema.upper(),
            "target_table": hub_data["name"].upper(),
            "target_entity_type": "hub",
            "collision_code": self.config.collision_code.upper(),
            "description": hub_data["description"],
            "metadata": self._build_metadata(warnings),
            "columns": self._build_columns(hub_data, datatype_info)
        }
