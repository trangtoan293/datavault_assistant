from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging
import json
import yaml
import pandas as pd
from datetime import datetime
from datavault_assistant.configs.settings import ParserConfig


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
    
    def _get_default_type(self, col: str) -> Dict[str, Any]:
        return {
            'error': f"Column {col} not found in mapping data",
            'data_type': f"VARCHAR2({self.config.default_varchar_length})"
        }
        
    def _process_varchar(self, length: str) -> str:
        import numpy as np
        if length and length.lower() not in ['-', ' ','nan']:
            return f"VARCHAR2({length})"
        return f"VARCHAR2({self.config.default_varchar_length})"

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
        required_fields = ["name", "hub", "source_table", "business_keys", "descriptive_attrs"]
        for field in required_fields:
            if field not in sat_data:
                warnings.append(f"Missing required field: {field}")
        
        if warnings:  # Nếu thiếu fields bắt buộc thì return luôn
            return warnings
        
        if not sat_data["hub"].startswith("HUB_"):
            warnings.append(f"Hub name should start with 'HUB_': {sat_data['hub']}")
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
            "target": f"DV_HKEY_{sat_data['name'].upper()}",
            "dtype": "raw",
            "key_type": "hash_key_sat",
            "source": None
        })
        
        # Add hub hash key
        columns.append({
            "target": f"DV_HKEY_{sat_data['hub'].upper()}",
            "dtype": "raw", 
            "key_type": "hash_key_hub",
            "source": [key for key in sat_data["business_keys"]]
        })
        
        # Add hash diff
        columns.append({
            "target": "DV_HSH_DIFF",
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
            "source_schema": source_schema.upper(),
            "source_table": sat_data["source_table"].upper(),
            "target_schema": self.config.target_schema.upper(),
            "target_table": sat_data["name"].upper(),
            "target_entity_type": "sat",
            "collision_code": self.config.collision_code.upper(),
            "parent_table": sat_data["hub"],
            "metadata": self._build_metadata(warnings),
            "columns": self._build_columns(sat_data, datatype_info)
        }
        return output_dict