import json
import yaml
from typing import Dict, Any, List, Set
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataVaultValidationError(Exception):
    """Custom exception for Data Vault validation errors"""
    pass

class LinkDataVaultParser:
    def __init__(self, 
                 source_schema: str = "source",
                 target_schema: str = "integration_demo"):
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.hubs_metadata = {}  # Cache for hub metadata
        self.validation_warnings = []  # Store warnings during validation


    def _cache_hubs_metadata(self, data: Dict[str, Any]) -> None:
        """
        Cache hub metadata for cross-validation
        
        Args:
            data (Dict[str, Any]): Full data vault definition
        """
        logger.info("Caching hubs metadata")
        self.hubs_metadata = {
            hub["name"]: {
                "business_keys": set(hub["business_keys"]),
                "source_tables": set(hub["source_tables"])
            }
            for hub in data.get("hubs", [])
        }


    def validate_business_keys_consistency(self, link_data: Dict[str, Any]) -> List[str]:
        """
        Validate business keys consistency và return list of warnings
        
        Args:
            link_data (Dict[str, Any]): Link metadata
            
        Returns:
            List[str]: List of validation warnings
            
        Raises:
            DataVaultValidationError: When validation fails
        """
        logger.info(f"Validating business keys consistency for link: {link_data['name']}")
        warnings = []
        link_keys = set(link_data["business_keys"])
        link_name = link_data["name"]
        
        # Collect all hub business keys
        all_hub_keys = set()
        
        for hub_name in link_data["related_hubs"]:
            if hub_name not in self.hubs_metadata:
                raise DataVaultValidationError(
                    f"Link {link_name} references non-existent hub: {hub_name}"
                )
            
            hub_keys = self.hubs_metadata[hub_name]["business_keys"]
            all_hub_keys.update(hub_keys)
            
            # Check missing keys
            missing_keys = hub_keys - link_keys
            if missing_keys:
                warning_msg = f"Link {link_name} is missing business keys from hub {hub_name}: {sorted(list(missing_keys))}"
                warnings.append(warning_msg)
            
            # Check if link has any keys from this hub
            hub_related_keys = link_keys.intersection(hub_keys)
            if not hub_related_keys:
                warning_msg = f"Link {link_name} has no business keys from hub {hub_name}"
                warnings.append(warning_msg)

        # Check extra keys
        extra_keys = link_keys - all_hub_keys
        if extra_keys:
            raise DataVaultValidationError(
                f"Link {link_name} contains business keys that don't belong to any related hub: {sorted(list(extra_keys))}"
            )
            
        return warnings

    def generate_metadata(self, warnings: List[str]) -> Dict[str, Any]:
        """
        Generate metadata section including validation status and warnings
        """
        logger.info("Generating metadata")
        status = "valid" if not warnings else "warnings"
        
        metadata = {
            "created_at": datetime.now().isoformat(),
            "validation_status": status
        }
        
        if warnings:
            metadata["validation_warnings"] = warnings
            
        return metadata

    def _get_hub_business_keys(self, hub_name: str, link_keys: List[str]) -> Set[str]:
        """
        Get business keys for a hub that are present in the link
        """
        if hub_name not in self.hubs_metadata:
            raise DataVaultValidationError(f"Unknown hub: {hub_name}")
            
        hub_keys = self.hubs_metadata[hub_name]["business_keys"]
        return set(link_keys).intersection(hub_keys)
    
    def _lookup_datatypes(self, business_keys: list, filtered_df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Tra cứu thông tin data type của các business keys
        """
        logger.info("Looking up datatypes for business keys")
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

    def transform_link_metadata_with_datatypes(self, link_data: Dict[str, Any], mapping_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Transform link metadata với mapping datatype
        
        Args:
            link_data (Dict[str, Any]): Input link metadata
            mapping_df (pd.DataFrame): DataFrame chứa mapping data
            
        Returns:
            Dict[str, Any]: Transformed metadata với mapped datatypes
        """
        logger.info(f"Transforming link metadata for link: {link_data['name']}")
        # Validate and collect warnings
        warnings = self.validate_business_keys_consistency(link_data)
        
        # Filter mapping data theo source tables
        filtered_df = mapping_df[mapping_df['TABLE_NAME'].isin(link_data["source_tables"])]
        
        # Lấy source schema từ mapping_df
        source_schema = filtered_df['SCHEMA_NAME'].iloc[0] if not filtered_df.empty else None
        if not source_schema:
            raise ValueError("Could not determine source schema from mapping data")
            
        # Lookup datatypes cho business keys
        datatype_info = self._lookup_datatypes(link_data["business_keys"], filtered_df)
        
        # Generate base output structure
        output_dict = {
            "source_schema": source_schema,
            "source_table": link_data["source_tables"][0],
            "target_schema": self.target_schema,
            "target_table": link_data["name"],
            "target_entity_type": "lnk",
            "collision_code": "mdm",
            "description": link_data["description"],
            "metadata": self.generate_metadata(warnings),
            "columns": []
        }
        
        # Add link hash key với source datatypes
        link_hash_key = {
            "target": f"dv_hkey_{link_data['name'].lower()}",
            "dtype": "raw",
            "key_type": "hash_key_lnk",
            "source": [
                {
                    "name": key,
                    "dtype": datatype_info[key]['data_type']
                } for key in link_data["business_keys"]
            ]
        }
        output_dict["columns"].append(link_hash_key)
        
        # Add hub hash keys với source datatypes
        for hub_name in link_data["related_hubs"]:
            hub_keys = self._get_hub_business_keys(hub_name, link_data["business_keys"])
            hub_hash_key = {
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
            }
            output_dict["columns"].append(hub_hash_key)
        
        return output_dict

    def process_link_file(self, input_file: str, output_dir: str, mapping_file: str) -> None:
        """
        Process complete link metadata with enhanced metadata
        """
        try:
            logger.info(f"Processing link file: {input_file}")
            # Read input file
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Cache hubs metadata
            self._cache_hubs_metadata(data)
            mapping_df = pd.read_csv(mapping_file)
            # Process links
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            results_summary = {
                "processed_at": datetime.now().isoformat(),
                "total_links": len(data.get("links", [])),
                "successful": 0,
                "warnings": 0,
                "errors": 0,
                "details": []
            }
            
            for link in data.get("links", []):
                try:
                    transformed_data = self.transform_link_metadata_with_datatypes(link, mapping_df)
                    output_file = output_dir / f"{link['name'].lower()}_metadata.yaml"
                    
                    with open(output_file, 'w', encoding='utf-8') as f:
                        yaml.dump(transformed_data, f, sort_keys=False, allow_unicode=True)
                    
                    # Update summary
                    status = transformed_data["metadata"]["validation_status"]
                    results_summary["successful"] += 1
                    if status == "warnings":
                        results_summary["warnings"] += 1
                    
                    results_summary["details"].append({
                        "link": link["name"],
                        "status": status,
                        "warnings": transformed_data["metadata"].get("validation_warnings", [])
                    })
                    
                except Exception as e:
                    results_summary["errors"] += 1
                    results_summary["details"].append({
                        "link": link["name"],
                        "status": "error",
                        "error": str(e)
                    })
            
            # Save summary
            summary_file = output_dir / "processing_link_summary.yaml"
            with open(summary_file, 'w', encoding='utf-8') as f:
                yaml.dump(results_summary, f, sort_keys=False, allow_unicode=True)
                
        except Exception as e:
            logger.error(f"Error processing data vault definition: {str(e)}")
            raise


def main():
    parser = LinkDataVaultParser()
    input_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\hub_link.json'
    mapping_file = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv'

    try:
        parser.process_link_file(
            input_file,
            "output",
            mapping_file
        )
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()
