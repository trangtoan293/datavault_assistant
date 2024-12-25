import pandas as pd
from typing import Dict, List, Union
from pathlib import Path
import json

class MetadataSourceParser:
    """
    Class để parse metadata từ CSV theo format:
    - COLUMN_NAME: String
    - DATA_TYPE: String
    - LENGTH: String
    - NULLABLE: String
    - DESCRIPTION: String
    """
    def __init__(self):
        self.required_columns = [
            'COLUMN_NAME', 'DATA_TYPE', 'LENGTH',
            'NULLABLE', 'DESCRIPTION','TABLE_NAME'
        ]
    
    def validate_columns(self, df: pd.DataFrame) -> bool:
        """Validate xem DataFrame có đúng format không"""
        return all(col in df.columns for col in self.required_columns)
    
    def read_metadata_source(self, file_path: Union[str, Path]) -> Dict:
        """
        Đọc và parse metadata source CSV file
        
        Args:
            file_path: Đường dẫn đến file metadata source
            
        Returns:
            Dict chứa metadata đã được organize theo table
        """
        try:
            # Đọc CSV file
            df = pd.read_csv(file_path)
            
            # Validate columns
            if not self.validate_columns(df):
                raise ValueError(
                    f"CSV file phải có các columns: {self.required_columns}"
                )
            
            # Clean data
            df = df.fillna('')  # Replace NaN với empty string
            
            # Parse và organize metadata
            return self._process_metadata(df)
            
        except Exception as e:
            raise Exception(f"Error parsing metadata source: {str(e)}")
    
    def _process_metadata(self, df: pd.DataFrame) -> Dict:
        """
        Process metadata từ DataFrame và organize theo format
        
        Returns:
            Dict với format:
            {
                'tables': {
                    'table_name': {
                        'columns': [
                            {
                                'name': str,
                                'data_type': str,
                                'length': str,
                                'nullable': bool,
                                'description': str
                            }
                        ]
                    }
                }
            }
        """
        metadata = {'tables': {}}
        
        # Convert DataFrame thành list of dictionaries
        records = df.to_dict('records')
        
        # Process từng record
        for record in records:
            column_info = {
                'name': record['COLUMN_NAME'],
                'data_type': record['DATA_TYPE'],
                'length': record['LENGTH'],
                'nullable': record['NULLABLE'].upper() == 'Y',
                'description': record['DESCRIPTION']
            }
            
            # Extract table name từ column name (giả sử format: TABLE_NAME.COLUMN_NAME)

            if record['TABLE_NAME'] != '':
                table_name = record['TABLE_NAME']
            elif '.' in record['COLUMN_NAME']:
                table_name, column_name = record['COLUMN_NAME'].split('.')
                column_info['name'] = column_name  # Update column name không có prefix
            else:
                table_name = 'UNKNOWN'
                
            # Add vào metadata dict
            if table_name not in metadata['tables']:
                metadata['tables'][table_name] = {
                    'columns': []
                }
            
            metadata['tables'][table_name]['columns'].append(column_info)
        
        return metadata
