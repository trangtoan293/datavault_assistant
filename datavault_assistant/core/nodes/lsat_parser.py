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
                logging.FileHandler('data_vault_builder.log'),
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
# Link Satellite Parser Implementation
class LinkSatelliteParser(DataVaultParser, LoggingMixin):
    def __init__(self, config: ParserConfig):
        self.config = config
        self.logger = self.setup_logging()
        self.datatype_service = DataTypeService(config)
        self.links_metadata = {}  # Cache for link metadata
        
    def parse(self, lsat_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Any]:
        """Main parsing method for link satellite metadata"""
        try:
            self.logger.info(f"Starting transformation for link satellite: {lsat_data['name']}")
            
            # Get source schema and validate
            source_schema = self._get_source_schema(lsat_data, mapping_df)
            datatype_info = self._get_datatype_info(lsat_data, mapping_df)
           
            validation_warnings = self.validate(lsat_data)
            
            return self._build_output_dict(lsat_data, source_schema, datatype_info, validation_warnings)
            
        except Exception as e:
            self.logger.error(f"Error in link satellite transformation: {str(e)}")
            raise
    
    def _cache_links_metadata(self, data: Dict[str, Any]) -> None:
        """Cache link metadata for validation"""
        self.logger.info("Caching links metadata")
        self.links_metadata = {
            link["name"]: {
                "business_keys": set(link["business_keys"]),
                "related_hubs": set(link["related_hubs"]),
                "source_tables": set(link["source_tables"])
            }
            for link in data.get("links", [])
        }
        self.logger.info(f"Caching links metadata {self.links_metadata}")
    
    def validate(self, lsat_data: Dict[str, Any]) -> List[str]:
        """Validate link satellite metadata"""
        warnings = []
        link_name = lsat_data["link"]
        
        # Validate link existence
        if link_name not in self.links_metadata:
            raise ValueError(f"Link satellite references non-existent link: {link_name}")
        
        # Validate business keys consistency
        link_keys = self.links_metadata[link_name]["business_keys"]
        lsat_keys = set(lsat_data["business_keys"])
        
        # Check missing keys
        missing_keys = link_keys - lsat_keys
        if missing_keys:
            warnings.append(
                f"Link satellite {lsat_data['name']} is missing business keys from parent link {link_name}: {sorted(list(missing_keys))}"
            )
        
        # Check extra keys
        extra_keys = lsat_keys - link_keys
        if extra_keys:
            warnings.append(
                f"Link satellite {lsat_data['name']} contains extra business keys not in parent link {link_name}: {sorted(list(extra_keys))}"
            )
        return warnings
    
    def _get_source_schema(self, lsat_data: Dict[str, Any], mapping_df: pd.DataFrame) -> str:
        """Get source schema from mapping DataFrame"""
        filtered_df = mapping_df[mapping_df['TABLE_NAME'] == lsat_data["source_table"][0]]
        if filtered_df.empty:
            raise ValueError(f"Could not find source table {lsat_data['source_table']} in mapping data")
        return filtered_df['SCHEMA_NAME'].iloc[0]
    
    def _get_datatype_info(self, lsat_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Dict]:
        """Get datatype information for all columns"""
        all_columns = lsat_data["business_keys"] + lsat_data["descriptive_attrs"]
        return self.datatype_service.lookup_datatypes(all_columns, mapping_df)
    
    def _build_metadata(self, warnings: List[str]) -> Dict[str, Any]:
        """Build metadata section for output"""
        return {
            "created_at": datetime.now().isoformat(),
            "version": self.config.version,
            "validation_status": "valid" if not warnings else "warnings",
            "validation_warnings": warnings if warnings else None
        }

    def _build_columns(self, lsat_data: Dict[str, Any], datatype_info: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Build columns section for output"""
        columns = []
        
        # Add link satellite hash key
        columns.append({
            "target": f"DV_HKEY_{lsat_data['name'].upper()}",
            "dtype": "raw",
            "key_type": "hash_key_lsat"
        })
        
        # Add link hash key
        columns.append({
            "target": f"DV_HKEY_{lsat_data['link'].upper()}",
            "dtype": "raw",
            "key_type": "hash_key_lnk",
            "source": [key for key in lsat_data["business_keys"]]
        })
        
        # Add hash diff
        columns.append({
            "target": "DV_HSH_DIFF",
            "dtype": "raw",
            "key_type": "hash_diff"
        })
        
        # Add descriptive attributes
        for attr in lsat_data["descriptive_attrs"]:
            columns.append({
                "target": attr,
                "dtype": datatype_info[attr]['data_type'],
                "source": {
                    "name": attr,
                    "dtype": datatype_info[attr]['data_type']
                }
            })
            
        return columns

    def _build_output_dict(self, lsat_data: Dict[str, Any], source_schema: str, 
                          datatype_info: Dict[str, Dict], warnings: List[str]) -> Dict[str, Any]:
        """Build the output dictionary with all necessary metadata"""
        output_dict = {
            "source_schema": source_schema.upper(),
            "source_table": lsat_data["source_table"][0].upper(),
            "target_schema": self.config.target_schema.upper(),
            "target_table": lsat_data["name"].upper(),
            "target_entity_type": "lsat",
            "collision_code": self.config.collision_code.upper(),
            "parent_table": lsat_data["link"],
            "metadata": self._build_metadata(warnings),
            "columns": self._build_columns(lsat_data, datatype_info)
        }
        return output_dict

