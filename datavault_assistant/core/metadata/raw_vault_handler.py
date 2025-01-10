
# core/processors/datavault_processor.py
from typing import Dict, Any, List, Union, Optional
import yaml
from datavault_assistant.core.utils.db_handler import DatabaseHandler
from datavault_assistant.core.utils.log_handler import create_logger
import logging 
logger = create_logger(__name__, 'dv_processor.log',level= logging.DEBUG)

class DataVaultMetadataProcessor:
    def __init__(self, db_handler: DatabaseHandler, user_id: str):
        self.db = db_handler
        self.user_id = user_id
        
    def process_yaml_files(self,yaml_path: str):
        from pathlib import Path
        """Process YAML files with proper path handling"""
        try:
            path = Path(yaml_path)
            if path.is_file():
                with open(path, 'r') as f:
                    yaml_content = f.read()
                    try:
                        entity_id = dv_processor.process_metadata(yaml_content)
                        print(f"Processed {path.name} - ID: {entity_id}")
                    except Exception as e:
                        print(f"Error processing {path.name}: {str(e)}")
            elif path.is_dir():
                for yaml_file in path.glob('*.yaml'):
                    with open(yaml_file, 'r') as f:
                        yaml_content = f.read()
                        try:
                            entity_id = dv_processor.process_metadata(yaml_content)
                            print(f"Processed {yaml_file.name} - ID: {entity_id}")
                        except Exception as e:
                            print(f"Error processing {yaml_file.name}: {str(e)}")
            else:
                print(f"Path not found: {yaml_path}")
        except Exception as e:
            print(f"Error processing path {yaml_path}: {str(e)}")
            
    def process_metadata(self, yaml_content: str) -> int:
        """Process Data Vault metadata from YAML content"""
        logger.debug("Processing metadata from YAML content")
        data = yaml.safe_load(yaml_content)
        entity_type = data['target_entity_type'].lower()
        logger.debug(f"Entity type: {entity_type}")
        
        processors = {
            'hub': self._process_hub,
            'lnk': self._process_link,
            'sat': self._process_satellite,
            'lsat': self._process_link_satellite
        }
        
        if entity_type not in processors:
            logger.error(f"Unknown entity type: {entity_type}")
            raise ValueError(f"Unknown entity type: {entity_type}")
            
        return processors[entity_type](data)

    def _process_hub(self, data: Dict) -> int:
        """Process HUB metadata with UPSERT"""
        logger.debug(f"Processing HUB metadata: {data['target_table']}")
        
        # Get business key from columns
        biz_key_cols = [col['target'] for col in data['columns'] 
                        if col.get('key_type') == 'biz_key']
        business_key = ','.join(biz_key_cols)
        source_table = f"{data.get('source_schema', '')}.{data.get('source_table', '')}"
        print(type(source_table))
        logger.debug(f"Business key columns: {biz_key_cols}")
        logger.debug(f"Combined business key: {business_key}")
        
        # UPSERT hub record
        query = """
            INSERT INTO metadata.dv_hub_tables (
                schema_name, table_name, business_key,
                description,source_table_name, created_by
            ) VALUES (%s, %s, %s, %s, %s,%s)
            ON CONFLICT (schema_name, table_name) 
            DO UPDATE SET
                business_key = EXCLUDED.business_key,
                description = EXCLUDED.description,
                source_table_name = EXCLUDED.source_table_name,
                created_by = EXCLUDED.created_by
            RETURNING id;
        """
        params = (
            data['target_schema'],
            data['target_table'],
            business_key,
            data.get('description', ''),
            source_table,
            self.user_id
        )
        logger.debug(f"Executing hub insert with params: {params}")
        
        try:
            result = self.db.execute_query(query, params)
            if not result:
                logger.error("No ID returned from hub insert")
                raise ValueError("Failed to get hub ID after insert")
            hub_id = result[0][0]
            logger.debug(f"Successfully inserted/updated hub with ID: {hub_id}")
            self._process_column_mappings(data, hub_id)
            return hub_id
        except Exception as e:
            logger.error(f"Error in _process_hub: {str(e)}")
            raise

    def _process_link(self, data: Dict) -> int:
        """Process LINK metadata with UPSERT"""
        logger.debug(f"Processing LINK metadata: {data['target_table']}")
        
        # Get source table information
        source_table = f"{data.get('source_schema', '')}.{data.get('source_table', '')}"
        
        # UPSERT link record
        query = """
            INSERT INTO metadata.dv_link_tables (
                schema_name, 
                table_name, 
                description, 
                source_table_name,
                created_by
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (schema_name, table_name) 
            DO UPDATE SET
                description = EXCLUDED.description,
                source_table_name = EXCLUDED.source_table_name,
                created_by = EXCLUDED.created_by
            RETURNING id;
        """
        
        params = (
            data['target_schema'],
            data['target_table'],
            data.get('description', ''),
            source_table,
            self.user_id
        )
        
        try:
            result = self.db.execute_query(query, params)
            if not result:
                raise ValueError("No ID returned from link insert")
            link_id = result[0][0]
            
            # First, remove existing hub references for clean slate
            self._remove_existing_link_hub_relations(link_id)
            # Process hub references
            hub_refs = [col for col in data['columns'] 
                    if col.get('key_type') == 'hash_key_hub' and col.get('parent')]
            
            for ref in hub_refs:
                hub_id = self._get_hub_id(ref['parent'])
                if hub_id:
                    self._create_link_hub_relation(link_id, hub_id)
            
            # Process column mappings
            self._process_column_mappings(data, link_id)
            
            return link_id
            
        except Exception as e:
            logger.error(f"Error in _process_link: {str(e)}")
            raise

    def _process_satellite(self, data: Dict) -> int:
        """Process SATELLITE metadata with UPSERT"""
        logger.debug(f"Processing SATELLITE metadata: {data['target_table']}")
        
        # Get parent hub id
        parent_hub_id = self._get_hub_id(data['parent_table'])
        if not parent_hub_id:
            raise ValueError(f"Parent hub not found: {data['parent_table']}")
            
        # Get source table information
        source_table = f"{data.get('source_schema', '')}.{data.get('source_table', '')}"
        
        query = """
            INSERT INTO metadata.dv_satellite_tables (
                hub_id, 
                schema_name, 
                table_name,
                description, 
                source_table_name,
                created_by
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (schema_name, table_name) 
            DO UPDATE SET
                hub_id = EXCLUDED.hub_id,
                description = EXCLUDED.description,
                source_table_name = EXCLUDED.source_table_name,
                created_by = EXCLUDED.created_by
            RETURNING id;
        """
        params = (
            parent_hub_id,
            data['target_schema'],
            data['target_table'],
            data.get('description', ''),
            source_table,
            self.user_id
        )
        try:
            result = self.db.execute_query(query, params)
            if not result:
                raise ValueError("No ID returned from satellite insert")
            sat_id = result[0][0]
            
            # Process column mappings
            self._process_column_mappings(data, sat_id)
            
            return sat_id
            
        except Exception as e:
            logger.error(f"Error in _process_satellite: {str(e)}")
            raise
    
    def _process_link_satellite(self, data: Dict) -> int:
        """Process LINK SATELLITE metadata with UPSERT
        
        Args:
            data (Dict): Dictionary containing link satellite metadata from YAML
            
        Returns:
            int: ID of inserted/updated link satellite record
        """
        logger.debug(f"Processing LINK SATELLITE metadata: {data['target_table']}")
        
        try:
            # Get parent link id
            parent_link_id = self._get_link_id(data['parent_table'])
            if not parent_link_id:
                raise ValueError(f"Parent link not found: {data['parent_table']}")
                
            # Get source table information
            source_table = self._get_source_table_name(data)
            
            # UPSERT link satellite record
            query = """
                INSERT INTO metadata.dv_link_satellite_tables (
                    link_id, 
                    schema_name, 
                    table_name,
                    description, 
                    source_table_name,
                    created_by
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (schema_name, table_name) 
                DO UPDATE SET
                    link_id = EXCLUDED.link_id,
                    description = EXCLUDED.description,
                    source_table_name = EXCLUDED.source_table_name,
                    created_by = EXCLUDED.created_by
                RETURNING id;
            """
            
            params = (
                parent_link_id,
                data['target_schema'],
                data['target_table'],
                data.get('description', ''),
                source_table,
                self.user_id
            )
            
            logger.debug(f"Executing link satellite insert with params: {params}")
            result = self.db.execute_query(query, params)
            
            if not result:
                raise ValueError("No ID returned from link satellite insert")
            
            lsat_id = result[0][0]
            logger.debug(f"Successfully inserted/updated link satellite with ID: {lsat_id}")
            
            # Process column mappings
            self._process_column_mappings(data, lsat_id)
            
            return lsat_id
            
        except Exception as e:
            logger.error(f"Error in _process_link_satellite: {str(e)}")
            raise
        
    def _remove_existing_column_mappings(self, parent_id: int) -> None:
        """Remove existing column mappings for an entity"""
        query = """
            DELETE FROM metadata.dv_column_mappings
            WHERE parent_id = %s;
        """
        self.db.execute_query(query, (parent_id,))
        
    def _remove_existing_link_hub_relations(self, link_id: int) -> None:
        """Remove existing hub relations for a link"""
        query = """
            DELETE FROM metadata.dv_link_hubs
            WHERE link_id = %s;
        """
        self.db.execute_query(query, (link_id,))

    def _add_source_column_to_mapping(
            self, 
            mapping_id: int, 
            source_column_id: int
        ) -> None:
        """Add source column to mapping with specified order"""
        query = """
            INSERT INTO metadata.dv_column_mapping_sources (
                mapping_id,
                source_column_id,
                created_by,
                updated_by
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (mapping_id, source_column_id) 
            DO UPDATE SET
                updated_by = EXCLUDED.updated_by,
                updated_at = CURRENT_TIMESTAMP;
        """
        self.db.execute_query(
            query, 
            (mapping_id, source_column_id, self.user_id, self.user_id)
        )

    def _create_column_mapping(
            self, 
            table_id: int,
            target_schema: str,
            target_table: str,
            target_column: str,
            target_dtype: str,
            source_columns: List[Dict[str, Any]],
            is_business_key: bool = False,
            is_hash_key: bool = False,
            transformation_rule: Optional[str] = None
        ) -> int:
        """Create a column mapping record with multiple source columns"""
        query = """
            INSERT INTO metadata.dv_column_mappings (
                table_id,
                target_schema,
                target_table,
                target_column,
                target_dtype,
                is_business_key,
                is_hash_key,
                transformation_rule,
                created_by,
                updated_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (target_schema, target_table, target_column) 
            DO UPDATE SET
                table_id = EXCLUDED.table_id,
                target_dtype = EXCLUDED.target_dtype,
                is_business_key = EXCLUDED.is_business_key,
                is_hash_key = EXCLUDED.is_hash_key,
                transformation_rule = EXCLUDED.transformation_rule,
                updated_by = EXCLUDED.updated_by,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """
        result = self.db.execute_query(
            query,
            (
                table_id,
                target_schema,
                target_table,
                target_column,
                target_dtype,
                is_business_key,
                is_hash_key,
                transformation_rule,
                self.user_id,
                self.user_id
            )
        )
        mapping_id = result[0][0]

        # Delete existing source mappings for this target
        self._delete_existing_source_mappings(mapping_id)
        
        # Then insert all source column relationships
        for idx, source_col in enumerate(source_columns):
            source_column_name = source_col['name'] if isinstance(source_col, dict) else source_col
            source_column_id = self._get_source_column_id(table_id, source_column_name)
            if source_column_id:
                self._add_source_column_to_mapping(
                    mapping_id=mapping_id,
                    source_column_id=source_column_id
                )
                
        return mapping_id

    def _process_column_mappings(self, data: Dict, parent_id: int) -> None:
        """Process column mappings including multiple source columns"""
        logger.debug(f"Processing column mappings for parent ID: {parent_id}")
        
        source_info = self._get_source_info(data)
        if not source_info:
            logger.error("No source information found")
            return
            
        source_table_id = self._get_source_table_id(
            schema_name=source_info['schema_name'],
            table_name=source_info['table_name']
        )
        if not source_table_id:
            logger.error(f"Source table not found: {source_info['schema_name']}.{source_info['table_name']}")
            return

        # Process each column mapping
        for column in data['columns']:
            source_info = column.get('source')
            source_columns = []
            transformation_rule = None
            target_dtype = column.get('dtype')
            
            # Process source column information
            if source_info:
                if isinstance(source_info, dict):
                    # Single source column with data type
                    source_columns = [{
                        'name': source_info['name'],
                        'dtype': source_info.get('dtype')
                    }]
                elif isinstance(source_info, list):
                    # Multiple source columns
                    if isinstance(source_info[0], dict):
                        source_columns = source_info
                    else:
                        # Get data types from source system if available
                        source_columns = []
                        for col_name in source_info:
                            source_dtype = self._get_source_column_dtype(source_table_id, col_name)
                            source_columns.append({
                                'name': col_name,
                                'dtype': source_dtype
                            })
                            
                    # Create transformation rule if multiple columns
                    if len(source_columns) > 1 and column['target'].startswith('DV_HKEY'):
                        transformation_rule = self._create_transformation_rule_hkey(source_columns)
                    elif len(source_columns) > 1:
                        transformation_rule = self._create_transformation_rule_multiple_col(source_columns)
            
            # Create the mapping
            self._create_column_mapping(
                table_id=source_table_id,
                target_schema=data['target_schema'],
                target_table=data['target_table'],
                target_column=column['target'],
                target_dtype=target_dtype,
                source_columns=source_columns,
                is_business_key=column.get('key_type') == 'biz_key',
                is_hash_key=column.get('key_type', '').startswith('hash_key'),
                transformation_rule=transformation_rule
            )
            
    def _get_source_info(self, data: Dict) -> Optional[Dict[str, str]]:
        """Get source schema and table information"""
        source_schema = data.get('source_schema')
        source_table = data.get('source_table')
        if source_schema and source_table:
            return {
                'schema_name': source_schema,
                'table_name': source_table
            }
        return None
    
    def _get_source_table_id(self, schema_name: str, table_name: str) -> Optional[int]:
        """Get source table ID from metadata.source_tables"""
        query = """
            SELECT id 
            FROM metadata.source_tables 
            WHERE schema_name = %s AND table_name = %s;
        """
        result = self.db.execute_query(query, (schema_name, table_name))
        return result[0][0] if result else None

    def _get_source_column_id(self, table_id: int, column_name: str) -> Optional[int]:
        """Get source column ID from metadata.source_columns"""
        query = """
            SELECT id 
            FROM metadata.source_columns
            WHERE table_id = %s AND column_name = %s;
        """
        result = self.db.execute_query(query, (table_id, column_name))
        return result[0][0] if result else None
    
    def _get_source_column_dtype(self, table_id: int, column_name: str) -> Optional[str]:
        """Get formatted data type of a source column"""
        query = """
            select 
            case 
                when UPPER(data_type) ='VARCHAR2' and (length is null) then 'VARCHAR2(255)'
                when UPPER(data_type) ='VARCHAR2' then UPPER(data_type)||'('|| length || ')' 
                when UPPER(data_type) ='CHAR' and (length is null) then 'VARCHAR2(255)'
                when UPPER(data_type) ='CHAR' then UPPER(data_type)||'('|| length || ')' 
                else upper(data_type) 
            end data_type
            from metadata.source_columns 
            WHERE table_id = %s AND column_name = %s;
        """
        result = self.db.execute_query(query, (table_id, column_name))
        return result[0][0] if result else None

    def _create_transformation_rule_hkey(self, source_columns: List[Dict[str, Any]]) -> str:
        """Create transformation rule for multiple source columns"""
        column_names = [col['name'] for col in source_columns]
        return f"HASH({','.join(column_names)})"
    
    def _create_transformation_rule_multiple_col(self, source_columns: List[Dict[str, Any]]) -> str:
        """Create transformation rule for multiple source columns"""
        column_names = [col['name'] for col in source_columns]
        return f"CONCAT({','.join(column_names)})"
    
    def _get_source_table_name(self, data: Dict) -> str:
        """Helper function to get source table name from data
        
        Args:
            data (Dict): Dictionary containing metadata
            
        Returns:
            str: Source table name in format schema.table
        """
        source_schema = data.get('source_schema', '')
        source_table = data.get('source_table', '')
        return f"{source_schema}.{source_table}" if source_schema and source_table else ''   
    
    def _get_link_satellite_id(self, table_name: str) -> Optional[int]:
        """Get link satellite ID by table name
        
        Args:
            table_name (str): Name of the link satellite table
            
        Returns:
            Optional[int]: ID of the link satellite if found, None otherwise
        """
        logger.debug(f"Getting LINK SATELLITE ID for table: {table_name}")
        query = """
            SELECT id FROM metadata.dv_link_satellite_tables
            WHERE table_name = %s
        """
        result = self.db.execute_query(query, (table_name,))
        return result[0][0] if result else None
    
    def _get_hub_id(self, table_name: str) -> Optional[int]:
        """Get hub ID by table name"""
        logger.debug(f"Getting HUB ID for table: {table_name}")
        query = """
            SELECT id FROM metadata.dv_hub_tables
            WHERE table_name = %s
        """
        result = self.db.execute_query(query, (table_name,))
        return result[0][0] if result else None

    def _get_link_id(self, table_name: str) -> Optional[int]:
        """Get link ID by table name"""
        logger.debug(f"Getting LINK ID for table: {table_name}")
        query = """
            SELECT id FROM metadata.dv_link_tables
            WHERE table_name = %s
        """
        result = self.db.execute_query(query, (table_name,))
        return result[0][0] if result else None
    
    def _delete_existing_source_mappings(self, mapping_id: int) -> None:
        """Delete existing source mappings for a given mapping ID"""
        query = """
            DELETE FROM metadata.dv_column_mapping_sources
            WHERE mapping_id = %s;
        """
        self.db.execute_query(query, (mapping_id,))
        
    def _create_link_hub_relation(self, link_id: int, hub_id: int) -> None:
        """Create relationship between link and hub"""
        logger.debug(f"Creating LINK-HUB relation: LINK ID {link_id}, HUB ID {hub_id}")
        query = """
            INSERT INTO metadata.dv_link_hubs (
                link_id, hub_id, created_by
            ) VALUES (%s, %s, %s)
            ON CONFLICT (link_id, hub_id) DO NOTHING
        """
        self.db.execute_query(query, (link_id, hub_id, self.user_id))

    def get_column_mapping_detail(self, mapping_id: int) -> Dict[str, Any]:
        """Get detailed information about a column mapping including all source columns"""
        query = """
            SELECT 
                cm.id as mapping_id,
                cm.target_schema,
                cm.target_table,
                cm.target_column,
                cm.target_dtype,
                cm.is_business_key,
                cm.is_hash_key,
                cm.transformation_rule,
                array_agg(sc.column_name ORDER BY cms.source_order) as source_columns,
                array_agg(cms.source_dtype ORDER BY cms.source_order) as source_dtypes,
                array_agg(CASE WHEN cms.is_primary THEN sc.column_name ELSE NULL END) as primary_source_column,
                string_agg(
                    st.schema_name || '.' || st.table_name || '.' || sc.column_name,
                    ', ' ORDER BY cms.source_order
                ) as source_path
            FROM metadata.dv_column_mappings cm
            LEFT JOIN metadata.dv_column_mapping_sources cms ON cm.id = cms.mapping_id
            LEFT JOIN metadata.source_columns sc ON cms.source_column_id = sc.id
            LEFT JOIN metadata.source_tables st ON sc.table_id = st.id
            WHERE cm.id = %s
            GROUP BY 
                cm.id, 
                cm.target_schema,
                cm.target_table,
                cm.target_column,
                cm.target_dtype,
                cm.is_business_key,
                cm.is_hash_key,
                cm.transformation_rule;
        """
        result = self.db.execute_query(query, (mapping_id,))
        if result:
            return dict(zip(
                ['mapping_info', 'source_columns', 'source_dtypes', 
                'primary_source_column', 'source_path'], 
                result[0]
            ))
        return None

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
    print('Connecting to database')
    db = DatabaseHandler(db_config)
    dv_processor = DataVaultMetadataProcessor(db_handler=db, user_id='admin')
    def process_yaml_files(yaml_path: str):
        from pathlib import Path
        """Process YAML files with proper path handling"""
        try:
            path = Path(yaml_path)
            if path.is_file():
                with open(path, 'r') as f:
                    yaml_content = f.read()
                    try:
                        entity_id = dv_processor.process_metadata(yaml_content)
                        print(f"Processed {path.name} - ID: {entity_id}")
                    except Exception as e:
                        print(f"Error processing {path.name}: {str(e)}")
            elif path.is_dir():
                for yaml_file in path.glob('*.yaml'):
                    with open(yaml_file, 'r') as f:
                        yaml_content = f.read()
                        try:
                            entity_id = dv_processor.process_metadata(yaml_content)
                            print(f"Processed {yaml_file.name} - ID: {entity_id}")
                        except Exception as e:
                            print(f"Error processing {yaml_file.name}: {str(e)}")
            else:
                print(f"Path not found: {yaml_path}")
        except Exception as e:
            print(f"Error processing path {yaml_path}: {str(e)}")
    
    process_yaml_files(r'D:\01_work\08_dev\ai_datavault\datavault_assistant\output')
 
