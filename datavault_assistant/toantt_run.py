# from datavault_assistant.core.nodes.data_vault_parser import DataProcessor
# from datavault_assistant.core.nodes.data_vault_builder import DataVaultAnalyzer
# from datavault_assistant.configs.settings import ParserConfig
# from datavault_assistant.core.utils.llm import init_llm
# from pathlib import Path
# import pandas as pd

# config = ParserConfig()
# processor = DataProcessor(config)
# metadata=pd.read_excel(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\test_dv_autovault.xlsx")
# analyzer = DataVaultAnalyzer(init_llm(provider="groq"))
# result=analyzer.analyze(metadata.to_string(index=False))
# processor.process_data(
#     input_data=result,
#     mapping_data=metadata,
#     output_dir=Path("output")
# )


import yaml
import psycopg2
from typing import Dict, Any, List, Optional
from datetime import datetime
from datavault_assistant.configs.settings import get_settings
import psycopg2
from psycopg2 import sql

from datavault_assistant.configs.log_handler import create_logger

logger = create_logger(__name__,'yml_parser.log')

def connect_to_postgres():
    settings = get_settings()
    conn = psycopg2.connect(
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        host=settings.DB_HOST,
        port=settings.DB_PORT
    )
    return conn

conn = connect_to_postgres()
def query_data(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return result



class MetadataProcessor:
    def __init__(self, db_config: Dict[str, Any]):
        self.conn = psycopg2.connect(**db_config)

    def store_metadata(self, yaml_content: str, user_id: str) -> int:
        data = yaml.safe_load(yaml_content)
        logger.info('INSERT INTO metadata_definitions')
        with self.conn.cursor() as cur:
            # Insert main metadata
            cur.execute("""
                INSERT INTO metadata.metadata_definitions (
                    source_schema, source_table, target_schema, target_table,
                    target_entity_type, collision_code, description, parent_table,
                    version, validation_status, validation_warnings
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data['source_schema'], data['source_table'],
                data['target_schema'], data['target_table'],
                
                data['target_entity_type'], data['collision_code'],
                data.get('description'), data.get('parent_table'),
                data['metadata']['version'],
                data['metadata']['validation_status'],
                data['metadata'].get('validation_warnings')
            ))
            metadata_id = cur.fetchone()[0]

            # Insert column mappings
            for column in data['columns']:
                is_composite = False 
                source_columns = None
                source_column = None
                source_data_type = None
                
                source_info = column.get('source')
                if source_info:
                    if isinstance(source_info, list):
                        is_composite = True
                        source_columns = source_info
                    elif isinstance(source_info, dict):
                        source_column = source_info.get('name')
                        source_data_type = source_info.get('dtype')

                cur.execute("""
                    INSERT INTO metadata.column_mappings (
                        metadata_id, target_column, data_type,
                        key_type, parent_hub, is_composite,
                        source_columns, source_column, source_data_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    metadata_id, column['target'], column['dtype'],
                    column.get('key_type'), column.get('parent'),
                    is_composite, source_columns, source_column, source_data_type
                ))

            # Insert Data Vault relationships
            if data['target_entity_type'] in ['sat', 'lsat']:
                cur.execute("""
                    INSERT INTO metadata.dv_relationships (
                        metadata_id, entity_type,
                        parent_entity_id, relationship_type
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    metadata_id, data['target_entity_type'],
                    self._get_parent_id(data['parent_table']),
                    'parent-child'
                ))

            self.conn.commit()
            return metadata_id

    def _get_parent_id(self, parent_table: str) -> Optional[int]:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM metadata.metadata_definitions
                WHERE target_table = %s
            """, (parent_table,))
            result = cur.fetchone()
            return result[0] if result else None

    def get_data_lineage(self) -> List[Dict]:
        with self.conn.cursor() as cur:
            cur.execute("""
                WITH RECURSIVE lineage AS (
                    -- Base case: Get all tables
                    SELECT 
                        id, target_table, parent_table,
                        target_entity_type, 1 as level,
                        ARRAY[target_table]::VARCHAR[] as path
                    FROM metadata.metadata_definitions
                    WHERE parent_table IS NULL
                    
                    UNION ALL
                    
                    -- Recursive case: Get children
                    SELECT 
                        m.id, m.target_table, m.parent_table,
                        m.target_entity_type, l.level + 1,
                        l.path || m.target_table
                    FROM metadata.metadata_definitions m
                    JOIN lineage l ON m.parent_table = l.target_table
                    WHERE NOT m.target_table = ANY(l.path)  -- Prevent cycles
                )
                SELECT * FROM lineage
                ORDER BY level, target_table;
            """)
            return cur.fetchall()

    def get_hub_references(self, hub_table: str) -> List[Dict]:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                    md.target_table,
                    md.target_entity_type,
                    cm.target_column
                FROM metadata.metadata_definitions md
                JOIN metadata.column_mappings cm ON md.id = cm.metadata_id
                WHERE cm.parent_hub = %s
                ORDER BY md.target_entity_type, md.target_table;
            """, (hub_table,))
            return cur.fetchall()
        
if __name__ == "__main__":
    settings = get_settings()
    db_config = {
        'dbname':settings.DB_NAME,
        'user':settings.DB_USER,
        'password':settings.DB_PASSWORD,
        'host':settings.DB_HOST,
        'port':settings.DB_PORT
    }

    yaml_content = """
source_schema: corebank
source_table: corebank_corp
target_schema: integration
target_table: hub_corporation
target_entity_type: hub
collision_code: CORPORATION
description: A unique corporation in the banking system.
metadata:
  created_at: '2025-01-06T22:01:19.882126'
  version: 1.0.0
  validation_status: valid
  validation_warnings: null
columns:
- target: dv_hkey_hub_corporation
  dtype: raw
  key_type: hash_key_hub
  source:
  - CUS_CORP_CODE
  - CUS_CORP_ID
- target: CUS_CUS_CORP_CODE
  dtype: number
  key_type: biz_key
  source:
    name: CUS_CORP_CODE
    dtype: number
- target: CUS_CUS_CORP_ID
  dtype: VARCHAR2(255)
  key_type: biz_key
  source:
    name: CUS_CORP_ID
    dtype: VARCHAR2(255)

    """
    
    processor = MetadataProcessor(db_config)
    user_id = "sample_user_id"
    logger.info('process metadata ...')
    metadata_id = processor.store_metadata(yaml_content, user_id)
    print(f"Stored metadata with ID: {metadata_id}")

    lineage = processor.get_data_lineage()
    print("Data Lineage:")
    for record in lineage:
        print(record)

    hub_references = processor.get_hub_references('parent_hub')
    print("Hub References:")
    for record in hub_references:
        print(record)