from pathlib import Path
from typing import Union, Dict
import pandas as pd
import yaml
# from core.metadata.exceptions import InvalidFileFormat
# from core.metadata.validators import MetadataValidator
# from datavault_assistant.models.metadata import MetadataSource, Table, Column
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel

# Input Models
class MetadataSource(BaseModel):
    id: str
    name: str
    file_type: str  # csv/xlsx
    upload_date: datetime
    content: str  # Parsed metadata content
    status: str
    
# Processing Models
class Column(BaseModel):
    name: str
    data_type: str
    description: Optional[str]

class Table(BaseModel):
    name: str
    schema: str
    columns: List[Column]
    source_system: str

class MetadataParser:
    @classmethod
    def parse_file(cls, file_path: Union[str, Path]) -> MetadataSource:
        """Parse metadata from file"""
        file_path = Path(file_path)
        
        if file_path.suffix.lower() in ('.xlsx', '.xls'):
            return cls._parse_excel(file_path)
        elif file_path.suffix.lower() == '.csv':
            return cls._parse_csv(file_path)
        elif file_path.suffix.lower() in ('.yaml', '.yml'):
            return cls._parse_yaml(file_path)
        else:
            return None
            # raise InvalidFileFormat(f"Unsupported file format: {file_path.suffix}")

    @classmethod
    def _parse_excel(cls, file_path: Path) -> MetadataSource:
        df = pd.read_excel(file_path)
        return cls._process_dataframe(df, 'excel')

    @classmethod
    def _parse_csv(cls, file_path: Path) -> MetadataSource:
        df = pd.read_csv(file_path)
        return cls._process_dataframe(df, 'csv')

    @classmethod
    def _process_dataframe(cls, df: pd.DataFrame, source_type: str) -> MetadataSource:
        # Validate DataFrame
        # MetadataValidator.validate_dataframe(df)
        df = df.fillna('')
        
        # Group by schema and table
        tables = []
        for (schema, table), group in df.groupby(['SCHEMA_NAME', 'TABLE_NAME']):
            columns = [
                Column(
                    name=row['COLUMN_NAME'],
                    data_type=row['DATA_TYPE'],
                    length=str(row['LENGTH']),
                    nullable=bool(row['NULLABLE']),
                    description=row['DESCRIPTION']
                )
                for _, row in group.iterrows()
            ]
            # print(columns)
            print(table)
            tables.append(Table(
                schema=schema,
                name=table,
                columns=columns,
                source_system=source_type
            ))
        return tables
        # return MetadataSource(name='toantt',tables=tables, file_type=source_type,)

    @classmethod
    def _parse_yaml(cls, file_path: Path) -> MetadataSource:
        with open(file_path) as f:
            content = yaml.safe_load(f)
            
        # MetadataValidator.validate_yaml_structure(content)
        
        tables = []
        for table_info in content['tables']:
            columns = [
                Column(**col_data)
                for col_data in table_info.get('columns', [])
            ]
            tables.append(Table(
                schema_name=table_info['schema_name'],
                table_name=table_info['table_name'],
                columns=columns
            ))
            
        return MetadataSource(tables=tables, source_type='yaml')


if __name__ == "__main__":
    parser = MetadataParser()
    path = r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv'
    metadata = parser.parse_file(path)
    print(type(metadata))
    print(metadata)