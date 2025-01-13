from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Set, Optional
from pathlib import Path
import logging
import json
import yaml
import pandas as pd
from datetime import datetime
from datavault_assistant.configs.settings import ParserConfig

# Configuration using dataclass
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
            "target": f"DV_HKEY_{link_data['name'].upper()}",
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
                "target": f"DV_HKEY_{hub_name.upper()}",
                "dtype": "raw",
                "key_type": "hash_key_hub",
                "parent": hub_name.upper(),
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
            "source_schema": source_schema.upper(),
            "source_table": link_data["source_tables"][0].upper(),
            "target_schema": self.config.target_schema.upper(),
            "target_table": link_data["name"].upper(),
            "target_entity_type": "lnk",
            "collision_code": self.config.collision_code.upper(),
            "description": link_data["description"],
            "metadata": self._build_metadata(warnings),
            "columns": self._build_columns(link_data, datatype_info)
        }
