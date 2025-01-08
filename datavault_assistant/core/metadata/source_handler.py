# core/processors/source_processor.py
from typing import Dict, Any, List
import pandas as pd
from datavault_assistant.core.utils.db_handler import DatabaseHandler

class SourceMetadataProcessor:
    def __init__(self, db_handler: DatabaseHandler, system_name: str, user_id: str):
        self.db = db_handler
        self.system_name = system_name
        self.user_id = user_id

    def process_source_metadata(self, metadata_df: pd.DataFrame) -> None:
        """Process source metadata from DataFrame"""
        # Get or create system
        system_id = self._get_or_create_system()

        # Process tables
        grouped = metadata_df.groupby(['SCHEMA_NAME', 'TABLE_NAME'])
        for (schema_name, table_name), table_df in grouped:
            table_id = self._get_or_create_table(system_id, schema_name, table_name)
            self._process_columns(table_id, table_df)

    def _get_or_create_system(self) -> int:
        query = """
            INSERT INTO metadata.source_systems (system_name, created_by)
            VALUES (%s, %s)
            ON CONFLICT (system_name) DO UPDATE 
                SET system_name = EXCLUDED.system_name
            RETURNING id
        """
        result = self.db.execute_query(query, (self.system_name, self.user_id))
        return result[0][0]

    def _get_or_create_table(self, system_id: int, schema_name: str, table_name: str) -> int:
        query = """
            INSERT INTO metadata.source_tables (
                system_id, schema_name, table_name, created_by
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (system_id, schema_name, table_name) DO UPDATE 
                SET schema_name = EXCLUDED.schema_name
            RETURNING id
        """
        result = self.db.execute_query(
            query, 
            (system_id, schema_name, table_name, self.user_id)
        )
        return result[0][0]

    def _process_columns(self, table_id: int, df: pd.DataFrame) -> None:
        columns_data = []
        for _, row in df.iterrows():
            length = None if row['LENGTH'] == '-' else row['LENGTH']
            nullable = True if row['NULLABLE'] == 'Y' else False
            description = None if row['DESCRIPTION'] == '-' else row['DESCRIPTION']
            
            columns_data.append((
                table_id,
                row['COLUMN_NAME'],
                row['DATA_TYPE'],
                length,
                nullable,
                description,
                self.user_id
            ))

        query = """
            INSERT INTO metadata.source_columns (
                table_id, column_name, data_type, length, 
                nullable, description, created_by
            ) VALUES %s
            ON CONFLICT (table_id, column_name) DO UPDATE SET 
                data_type = EXCLUDED.data_type,
                length = EXCLUDED.length,
                nullable = EXCLUDED.nullable,
                description = EXCLUDED.description
        """
        self.db.execute_many(query, columns_data)

    def get_source_metadata(self) -> pd.DataFrame:
        """Retrieve all source metadata"""
        return self.db.query_to_df("SELECT * FROM metadata.v_source_metadata")
    
if __name__ == "__main__":
    from datavault_assistant.configs.settings import get_settings
    settings = get_settings()
    db_config = {
        'dbname':settings.DB_NAME,
        'user':settings.DB_USER,
        'password':settings.DB_PASSWORD,
        'host':settings.DB_HOST,
        'port':settings.DB_PORT
    }

    db = DatabaseHandler(db_config)
    source_metadata = pd.read_csv(r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv')
    source_processor = SourceMetadataProcessor(
        db_handler=db,
        system_name='FLEXLIVE',
        user_id='admin'
    )
    source_processor.process_source_metadata(source_metadata)