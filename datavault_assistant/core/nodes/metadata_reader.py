import pandas as pd
from typing import Dict, Union
from pathlib import Path
import tempfile
import os
import yaml
from fastapi import UploadFile
from typing import Dict
from pathlib import Path

class MetadataSourceParser:
    def __init__(self):
        self.required_columns = ['SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE', 'LENGTH', 'NULLABLE', 'DESCRIPTION']
    
    def validate_columns(self, df: pd.DataFrame) -> bool:
        return all(col in df.columns for col in self.required_columns)
    
    def read_metadata_source(self, file_path: Union[str, Path]) -> Dict:
        try:
            df = pd.read_excel(file_path) if str(file_path).endswith(('.xlsx', '.xls')) else pd.read_csv(file_path)
            if not self.validate_columns(df):
                raise ValueError(f"CSV file must have columns: {self.required_columns}")
            df = df.fillna('')
            return self._process_metadata(df)
        except Exception as e:
            raise Exception(f"Error parsing metadata source: {str(e)}")
    
    def _process_metadata(self, df: pd.DataFrame) -> str:
        metadata = df.to_string( index=False)
        return metadata


class MetadataService:
    def __init__(self):
        self.parser = MetadataSourceParser()

    async def process_file(self, file: UploadFile) -> Dict:
        """
        Process uploaded file và return metadata
        """
        temp_path = None
        try:
            # Save upload to temp file
            temp_path = await self._save_upload_file(file)
            
            # Process based on file type
            if file.filename.endswith(('.csv', '.xlsx', '.xls')):
                result = self.parser.read_metadata_source(temp_path)
            elif file.filename.endswith(('.yaml', '.yml')):
                result = await self._process_yaml_file(temp_path)
            else:
                raise ValueError(f"Unsupported file format: {file.filename}")
            
            return result
            
        finally:
            # Cleanup temp file
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)

    async def _save_upload_file(self, file: UploadFile) -> str:
        """Save uploaded file to temporary location"""
        suffix = Path(file.filename).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            content = await file.read()
            temp_file.write(content)
            return temp_file.name

    async def _process_yaml_file(self, file_path: str) -> Dict:
        """Process YAML file"""
        with open(file_path, 'r') as yaml_file:
            yaml_content = yaml.safe_load(yaml_file)
            return self._convert_yaml_to_metadata_format(yaml_content)

    def _convert_yaml_to_metadata_format(self, yaml_content: Dict) -> Dict:
        """Convert YAML content to metadata format"""
        try:
            metadata = {'tables': {}}
            
            for table_name, table_info in yaml_content.get('tables', {}).items():
                if 'columns' not in table_info:
                    continue
                    
                metadata['tables'][table_name] = {
                    'columns': [
                        {
                            'name': col.get('name', ''),
                            'data_type': col.get('data_type', ''),
                            'length': str(col.get('length', '')),
                            'nullable': col.get('nullable', True),
                            'description': col.get('description', '')
                        }
                        for col in table_info['columns']
                    ]
                }
                
            return metadata
            
        except Exception as e:
            raise ValueError(f"Invalid YAML format: {str(e)}")