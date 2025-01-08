import yaml
import psycopg2
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from datavault_assistant.configs.log_handler import create_logger

logger = create_logger(__name__,'yml_parser.log')

class MetadataProcessor:
    def __init__(self, db_config: Dict[str, Any]):
        self.conn = psycopg2.connect(**db_config)

    def store_metadata(self, yaml_content: str, user_id: str) -> int:
        data = yaml.safe_load(yaml_content)
        
        with self.conn.cursor() as cur:
            # Insert main metadata
            cur.execute("""
                INSERT INTO metadata_definitions (
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
                    INSERT INTO column_mappings (
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
                    INSERT INTO dv_relationships (
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
                SELECT id FROM metadata_definitions
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
                    FROM metadata_definitions
                    WHERE parent_table IS NULL
                    
                    UNION ALL
                    
                    -- Recursive case: Get children
                    SELECT 
                        m.id, m.target_table, m.parent_table,
                        m.target_entity_type, l.level + 1,
                        l.path || m.target_table
                    FROM metadata_definitions m
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
                FROM metadata_definitions md
                JOIN column_mappings cm ON md.id = cm.metadata_id
                WHERE cm.parent_hub = %s
                ORDER BY md.target_entity_type, md.target_table;
            """, (hub_table,))
            return cur.fetchall()