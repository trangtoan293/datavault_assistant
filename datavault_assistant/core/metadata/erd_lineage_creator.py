

from typing import Dict, List
from datavault_assistant.core.utils.db_handler import DatabaseHandler

class MetadataProcessor:
    def __init__(self, db_handler: DatabaseHandler, user_id: str):
        """
        Khởi tạo connection tới PostgreSQL database
        
        Args:
            conn_params (Dict): Dictionary chứa thông tin connection
        """
        self.db = db_handler
        self.user_id = user_id

    def generate_table_lineage(self) -> str:
        """
        Generate data lineage ở table level sử dụng Mermaid
        Bao gồm cả source system và target tables trong Data Vault
        """

        
        # Query để lấy relationship giữa source và target tables
        query = """
SELECT * 
        FROM (SELECT DISTINCT 
    ss.system_name,
    st.schema_name || '.' || st.table_name as source_table,
h.schema_name || '.' || h.table_name as target_table,
'hub' as target_type
FROM metadata.source_systems ss
JOIN metadata.source_tables st ON ss.id = st.system_id
left JOIN metadata.dv_hub_tables h ON h.source_table_name = st.schema_name || '.' || st.table_name
UNION ALL
SELECT DISTINCT 
    ss.system_name,
    st.schema_name || '.' || st.table_name as source_table,
sat.schema_name || '.' || sat.table_name as target_table,
'sat' as target_type
FROM metadata.source_systems ss
JOIN metadata.source_tables st ON ss.id = st.system_id
left JOIN metadata.dv_satellite_tables sat ON sat.source_table_name = st.schema_name || '.' || st.table_name
UNION ALL
SELECT DISTINCT 
    ss.system_name,
    st.schema_name || '.' || st.table_name as source_table,
l.schema_name || '.' || l.table_name as target_table,
'link' as target_type
FROM metadata.source_systems ss
JOIN metadata.source_tables st ON ss.id = st.system_id
left JOIN metadata.dv_link_tables l ON l.source_table_name = st.schema_name || '.' || st.table_name) a
        where target_table is not null
        ORDER BY system_name, source_table, target_table
        """
        
        results = self.db.execute_query(query)

        # Generate Mermaid flowchart
        mermaid = ["graph LR"]
        
        # Dictionary để track unique nodes
        nodes = {}
        
        for row in results:
            system, source, target, target_type = row
            
            # Create unique IDs for nodes
            if source not in nodes:
                source_clean = source.replace('.', '_').replace('-', '_')
                nodes[source] = f"{source_clean}[{source}]"
                
            if target not in nodes:
                target_clean = target.replace('.', '_').replace('-', '_')
                # Add target type information
                target_label = f"{target}"
                nodes[target] = f"{target_clean}[{target_label}]"
            
            # Add relationship
            mermaid.append(f"    {nodes[source]} --> {nodes[target]}")
            
        return "\n".join(mermaid)
    
    def generate_column_lineage(self) -> str:
        """
        Generate data lineage ở column level sử dụng Mermaid
        """
        
        # Query để lấy column mapping
        query = """
        SELECT 
            ss.system_name,
            st.schema_name || '.' || st.table_name as source_table,
            sc.column_name as source_column,
            dcm.target_schema || '.' || dcm.target_table as target_table,
            dcm.target_column,
            dcm.transformation_rule
        FROM metadata.source_systems ss
        JOIN metadata.source_tables st ON ss.id = st.system_id
        JOIN metadata.source_columns sc ON st.id = sc.table_id
        JOIN metadata.dv_column_mappings dcm ON st.id = dcm.table_id
        JOIN metadata.dv_column_mapping_sources dcms ON dcm.id = dcms.mapping_id 
            AND sc.id = dcms.source_column_id
        """
        
        results = self.db.execute_query(query)
        
        # Generate Mermaid flowchart
        mermaid = ["graph LR"]
        for row in results:
            system, s_table, s_col, t_table, t_col, trans = row
            source_id = f"{s_table}.{s_col}"
            target_id = f"{t_table}.{t_col}"
            
            # Add transformation rule if exists
            if trans:
                mermaid.append(f"    {source_id}-->|{trans}|{target_id}")
            else:
                mermaid.append(f"    {source_id}-->{target_id}")
                
        return "\n".join(mermaid)
    
    def generate_data_dictionary(self) -> Dict:
        """
        Generate data dictionary cho end-users
        """
        
        # Query để lấy table descriptions
        tables_query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            d.description
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE n.nspname = 'metadata'
        AND c.relkind = 'r'
        ORDER BY n.nspname, c.relname;
        """
        
        # Query để lấy column descriptions
        columns_query = """
        SELECT 
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            pgd.description
        FROM information_schema.columns c
        LEFT JOIN pg_description pgd ON 
            pgd.objoid = (
                SELECT oid FROM pg_class 
                WHERE relname = c.table_name
            )::regclass
            AND pgd.objsubid = c.ordinal_position
        WHERE c.table_schema = 'metadata'
        """
        tables = self.db.execute_query(tables_query)
        columns = self.db.execute_query(columns_query)
        # Format kết quả thành dictionary
        dictionary = {}
        
        for schema, table, desc in tables:
            if schema not in dictionary:
                dictionary[schema] = {}
            dictionary[schema][table] = {
                'description': desc,
                'columns': {}
            }
            
        for schema, table, column, dtype, desc in columns:
            if schema in dictionary and table in dictionary[schema]:
                dictionary[schema][table]['columns'][column] = {
                    'data_type': dtype,
                    'description': desc
                }
                
        return dictionary



    def generate_advanced_lineage(self) -> str:
        """
        Generate comprehensive data lineage hiển thị cả table và column level
        """
        # Query to get table relationships with column information
        query = """

SELECT system_name,source_table,target_table,target_type,source_column,target_column,transformation_rule
FROM (SELECT DISTINCT 
    ss.system_name,
    st.schema_name || '.' || st.table_name as source_table,
    h.schema_name || '.' || h.table_name as target_table,
    'hub' as target_type,
    sc.column_name as source_column,
    dcm.target_column,
    dcm.transformation_rule
FROM metadata.source_systems ss
JOIN metadata.source_tables st ON ss.id = st.system_id
JOIN metadata.source_columns sc ON st.id = sc.table_id
 JOIN metadata.dv_column_mappings dcm ON st.id = dcm.table_id
 JOIN metadata.dv_column_mapping_sources dcms ON dcm.id = dcms.mapping_id 
    AND sc.id = dcms.source_column_id
JOIN metadata.dv_hub_tables h ON h.source_table_name = st.schema_name || '.' || st.table_name
UNION ALL
SELECT DISTINCT 
    ss.system_name,
    st.schema_name || '.' || st.table_name as source_table,
    sat.schema_name || '.' || sat.table_name as target_table,
    'sat' as target_type,
    sc.column_name as source_column,
    dcm.target_column,
    dcm.transformation_rule
FROM metadata.source_systems ss
JOIN metadata.source_tables st ON ss.id = st.system_id
JOIN metadata.source_columns sc ON st.id = sc.table_id
 JOIN metadata.dv_column_mappings dcm ON st.id = dcm.table_id
 JOIN metadata.dv_column_mapping_sources dcms ON dcm.id = dcms.mapping_id 
    AND sc.id = dcms.source_column_id
JOIN metadata.dv_satellite_tables sat ON sat.source_table_name = st.schema_name || '.' || st.table_name
UNION ALL
SELECT DISTINCT 
    ss.system_name,
    st.schema_name || '.' || st.table_name as source_table,
    l.schema_name || '.' || l.table_name as target_table,
    'link' as target_type,
    sc.column_name as source_column,
    dcm.target_column,
    dcm.transformation_rule
FROM metadata.source_systems ss
JOIN metadata.source_tables st ON ss.id = st.system_id
JOIN metadata.source_columns sc ON st.id = sc.table_id
 JOIN metadata.dv_column_mappings dcm ON st.id = dcm.table_id
 JOIN metadata.dv_column_mapping_sources dcms ON dcm.id = dcms.mapping_id 
    AND sc.id = dcms.source_column_id
JOIN metadata.dv_link_tables l ON l.source_table_name = st.schema_name || '.' || st.table_name) table_mappings
WHERE target_table is not null
ORDER BY system_name, source_table, target_table, source_column
        """
        results = self.db.execute_query(query)
        
        # Generate Mermaid flowchart
        mermaid = ["graph LR"]
        
        # Dictionary to track unique nodes and subgraphs
        nodes = {}
        subgraphs = {}
        
        # Process each row to create subgraphs for tables and their columns
        for row in results:
            system, source_table, target_table, target_type, source_col, target_col, trans = row
            
            # Create source table subgraph if not exists
            if source_table not in subgraphs:
                source_clean = source_table.replace('.', '_').replace('-', '_')
                subgraphs[source_table] = f"""
    subgraph {source_clean}[{source_table}]
        direction TB
        style {source_clean} fill:#f9f,stroke:#333,stroke-width:2px"""
                
            # Create target table subgraph if not exists
            if target_table not in subgraphs:
                target_clean = target_table.replace('.', '_').replace('-', '_')
                subgraphs[target_table] = f"""
    subgraph {target_clean}[{target_table} ({target_type})]
        direction TB
        style {target_clean} fill:#bbf,stroke:#333,stroke-width:2px"""
            
            # Add columns to respective subgraphs
            if source_col:
                source_col_id = f"{source_table}_{source_col}".replace('.', '_').replace('-', '_')
                subgraphs[source_table] += f"\n        {source_col_id}[{source_col}]"
                
            if target_col:
                target_col_id = f"{target_table}_{target_col}".replace('.', '_').replace('-', '_')
                subgraphs[target_table] += f"\n        {target_col_id}[{target_col}]"
                
                # Add column mapping with transformation if exists
                if trans:
                    mermaid.append(f"    {source_col_id}-->|{trans}|{target_col_id}")
                else:
                    mermaid.append(f"    {source_col_id}-->{target_col_id}")
        
        # Close all subgraphs
        for table in subgraphs:
            subgraphs[table] += "\n    end"
            
        # Add all subgraphs to the diagram
        mermaid.extend(subgraphs.values())
        
        return "\n".join(mermaid)


    def generate_dv_model_erd(self) -> str:
        """
        Generate ERD diagram cho Data Vault model dựa trên metadata
        """
        # Query để lấy thông tin về Hubs, Satellites, Links và relationships
        query = """
WITH hub_data AS (
    SELECT id, schema_name || '.' || table_name as table_name, business_key, description FROM metadata.dv_hub_tables
),
sat_data AS (
    SELECT id, hub_id, schema_name || '.' || table_name as table_name, description FROM metadata.dv_satellite_tables
),
link_data AS (
    SELECT id, schema_name || '.' || table_name as table_name, description FROM metadata.dv_link_tables
),
link_sat_data AS (
    SELECT id, link_id, schema_name || '.' || table_name as table_name, description FROM metadata.dv_link_satellite_tables
),
link_hub_relations AS (
    SELECT link_id, hub_id FROM metadata.dv_link_hubs
)
SELECT json_build_object('hubs', (SELECT json_agg(hub_data) FROM hub_data),
'satellites', (SELECT json_agg(sat_data) FROM sat_data),
'links', (SELECT json_agg(link_data) FROM link_data),
'link_satellites', (SELECT json_agg(link_sat_data) FROM link_sat_data),
'link_hub_relations', (SELECT json_agg(link_hub_relations) FROM link_hub_relations)) as metadata;
        """
        
        # Execute query
        print(query)
        results = self.db.execute_query(query)
        print(results)
        metadata = results[0][0]  # Get JSON result
        
        # Generate Mermaid ERD
        mermaid_lines = ["erDiagram"]
        
        # Add Hubs
        hubs = metadata.get('hubs', [])
        for hub in hubs:
            table_name = hub['table_name'].replace('.', '_')
            mermaid_lines.append(f"{table_name} {{")
            mermaid_lines.append(f"        varchar {hub['business_key']} PK")
            mermaid_lines.append("        timestamp load_date")
            mermaid_lines.append("        varchar record_source")
            mermaid_lines.append("}")
        
        # Add Satellites
        satellites = metadata.get('satellites', [])
        for sat in satellites:
            table_name = sat['table_name'].replace('.', '_')
            # Find parent hub
            for hub in hubs:
                if hub['id'] == sat['hub_id']:
                    hub_name = hub['table_name'].replace('.', '_')
                    mermaid_lines.append(f"    {hub_name} ||--o{{ {table_name} : has")
            mermaid_lines.append(f"{table_name} {{")
            mermaid_lines.append("        varchar hash_key FK")
            mermaid_lines.append("        timestamp load_date")
            mermaid_lines.append("        varchar record_source")
            mermaid_lines.append("}")
        
        # Add Links
        links = metadata.get('links', [])
        link_relations = metadata.get('link_hub_relations', [])
        
        for link in links:
            table_name = link['table_name'].replace('.', '_')
            mermaid_lines.append(f"{table_name} {{")
            mermaid_lines.append("        varchar hash_key PK")
            mermaid_lines.append("        timestamp load_date")
            mermaid_lines.append("        varchar record_source")
            mermaid_lines.append("}")
            
            # Add Link-Hub relationships
            for rel in link_relations:
                if rel['link_id'] == link['id']:
                    for hub in hubs:
                        if hub['id'] == rel['hub_id']:
                            hub_name = hub['table_name'].replace('.', '_')
                            mermaid_lines.append(f"{table_name} }}o--|| {hub_name} : connects")
        
        # Add Link Satellites
        link_sats = metadata.get('link_satellites', [])
        for link_sat in link_sats:
            table_name = link_sat['table_name'].replace('.', '_')
            # Find parent link
            for link in links:
                if link['id'] == link_sat['link_id']:
                    link_name = link['table_name'].replace('.', '_')
                    mermaid_lines.append(f"{link_name} ||--o{{ {table_name} : has")
            mermaid_lines.append(f"{table_name} {{")
            mermaid_lines.append("        varchar hash_key FK")
            mermaid_lines.append("        timestamp load_date")
            mermaid_lines.append("        varchar record_source")
            mermaid_lines.append("}")
        
        return "\n".join(mermaid_lines)

if __name__ == '__main__':
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
    processor = MetadataProcessor(db_handler=db, user_id='admin')
    erd= processor.generate_dv_model_erd()
    print(erd)
    # table_lineage = processor.generate_table_lineage()
    # print(table_lineage)

    # # Generate column lineage
    # column_lineage = processor.generate_column_lineage()
    # print(column_lineage)

    # # Generate data dictionary
    # data_dict = processor.generate_data_dictionary()
    # print(data_dict)