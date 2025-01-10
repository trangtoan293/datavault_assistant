-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS metadata;

-- Source metadata tables
CREATE TABLE metadata.source_systems (
    id SERIAL PRIMARY KEY,
    system_name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(system_name)
);

CREATE TABLE metadata.source_tables (
    id SERIAL PRIMARY KEY,
    system_id INTEGER REFERENCES metadata.source_systems(id),
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(system_id, schema_name, table_name)
);

CREATE TABLE metadata.source_columns (
    id SERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES metadata.source_tables(id),
    column_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    length INTEGER,
    nullable BOOLEAN DEFAULT true,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(table_id, column_name)
);

-- Data Vault metadata tables
CREATE TABLE metadata.dv_hub_tables (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    business_key VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table_name VARCHAR(100) NOT NULL,
    created_by VARCHAR(100),
    UNIQUE(schema_name, table_name)
);

CREATE TABLE metadata.dv_satellite_tables (
    id SERIAL PRIMARY KEY,
    hub_id INTEGER REFERENCES metadata.dv_hub_tables(id),
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table_name VARCHAR(100) NOT NULL,
    created_by VARCHAR(100),
    UNIQUE(schema_name, table_name)
);

CREATE TABLE metadata.dv_link_tables (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table_name VARCHAR(100) NOT NULL,
    created_by VARCHAR(100),
    UNIQUE(schema_name, table_name)
);
-- Create new table for link satellite metadata
CREATE TABLE metadata.dv_link_satellite_tables (
    id SERIAL PRIMARY KEY,
    link_id INTEGER REFERENCES metadata.dv_link_tables(id),
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    description TEXT,
    source_table_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(schema_name, table_name)
);

CREATE TABLE metadata.dv_link_hubs (
    id SERIAL PRIMARY KEY,
    link_id INTEGER REFERENCES metadata.dv_link_tables(id),
    hub_id INTEGER REFERENCES metadata.dv_hub_tables(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(link_id, hub_id)
);

-- Column mappings
CREATE TABLE metadata.dv_column_mappings (
    id SERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES metadata.source_tables(id),
    target_schema VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100) NOT NULL,
    target_dtype VARCHAR(100) NOT NULL,
    is_business_key BOOLEAN DEFAULT false,
    is_hash_key BOOLEAN DEFAULT false,
    transformation_rule TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(100),
    UNIQUE(target_schema, target_table, target_column)
);

CREATE TABLE metadata.dv_column_mapping_sources (
    id SERIAL PRIMARY KEY,
    mapping_id INTEGER REFERENCES metadata.dv_column_mappings(id),
    source_column_id INTEGER REFERENCES metadata.source_columns(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(100),
    UNIQUE(mapping_id, source_column_id)
);

-- Source System Metadata Section
COMMENT ON TABLE metadata.source_systems IS 
'Manages information about source systems within the organization. Each source system represents an independent data source such as ERP, CRM, or internal systems. This table serves as the starting point for tracking data flow from sources and enables comprehensive data lineage.';

COMMENT ON TABLE metadata.source_tables IS 
'Stores information about tables from source systems. Each record represents a physical table in the source system, including schema and table name. Maintains a parent-child relationship with source_systems through system_id. Essential for source mapping and automated ETL processes.';

COMMENT ON TABLE metadata.source_columns IS 
'Contains detailed structure of source tables, storing information about columns including name, data type, length, and other attributes. Used for mapping and data lineage tracking. Critical for understanding source data structure and enabling automated transformations.';

-- Data Vault Core Component Section
COMMENT ON TABLE metadata.dv_hub_tables IS 
'Manages metadata for Hubs in the Data Vault model. Hubs are core components containing business keys used to identify business objects. Each hub table is linked to a source table, forming the foundation of the Data Vault architecture. Essential for maintaining business entity definitions.';

COMMENT ON TABLE metadata.dv_satellite_tables IS 
'Manages metadata for Satellites in Data Vault. Satellites contain descriptive attributes and historical changes of business objects in hubs. Each satellite is linked to a hub through hub_id. Critical for tracking temporal changes and maintaining historical context of business entities.';

COMMENT ON TABLE metadata.dv_link_tables IS 
'Manages metadata for Links in Data Vault. Link tables represent business relationships between hubs, enabling modeling of m-n relationships. Source table name indicates the origin of the relationship. Key component for maintaining business relationship integrity.';

COMMENT ON TABLE metadata.dv_link_satellite_tables IS 
'Manages metadata for Link Satellites. Link Satellites store context and additional attributes of a relationship (link), similar to regular satellites but specifically for link tables. Important for maintaining historical context of business relationships.';

-- Relationship and Mapping Section
COMMENT ON TABLE metadata.dv_link_hubs IS 
'Manages connections between Links and Hubs, representing the relationship structure in Data Vault. Each record indicates a hub participating in a specific link. Essential for maintaining the structural integrity of complex business relationships.';

COMMENT ON TABLE metadata.dv_column_mappings IS 
'Central table for mapping between source and target in Data Vault. Stores transformation rules, target data types, and flags special columns like business keys or hash keys. Critical for automated ETL code generation and maintaining transformation logic.';

COMMENT ON TABLE metadata.dv_column_mapping_sources IS 
'Details the origin of each column mapping. A target column may be derived from multiple source columns, and this table stores these relationships. Supports impact analysis and detailed data lineage tracking. Essential for change management and data governance.';

-- Source Systems Column Comments
COMMENT ON COLUMN metadata.source_systems.system_name IS 
'Unique identifier name for the source system. Examples: SAP_ERP, SALESFORCE_CRM. Used in automated processes and data lineage tracking.';

COMMENT ON COLUMN metadata.source_systems.description IS 
'Detailed description of the source system including its business purpose and technical context. Important for documentation and knowledge sharing.';

-- Source Tables Column Comments
COMMENT ON COLUMN metadata.source_tables.schema_name IS 
'Name of the database schema in the source system. Part of the unique identifier for table location. Critical for automated data extraction.';

COMMENT ON COLUMN metadata.source_tables.table_name IS 
'Physical table name in the source system. Combined with schema_name creates unique identifier. Used in ETL automation and lineage tracking.';

COMMENT ON COLUMN metadata.source_tables.system_id IS 
'Foreign key to source_systems. Establishes ownership and enables cross-system analysis and impact assessment.';

-- Source Columns Column Comments
COMMENT ON COLUMN metadata.source_columns.column_name IS 
'Physical column name in the source table. Used for mapping and transformation logic.';

COMMENT ON COLUMN metadata.source_columns.data_type IS 
'Native data type of the column in source system. Critical for automated type conversion and validation rules.';

COMMENT ON COLUMN metadata.source_columns.length IS 
'Maximum length for string/numeric fields. Essential for data quality validation and target schema generation.';

-- Data Vault Hub Tables Column Comments
COMMENT ON COLUMN metadata.dv_hub_tables.business_key IS 
'Identifies the business key column(s) that uniquely identify the business entity. Critical for hub key generation and data integration.';

COMMENT ON COLUMN metadata.dv_hub_tables.source_table_name IS 
'Primary source table for the hub. Important for tracking data origin and automated loading process.';

-- Data Vault Satellite Tables Column Comments
COMMENT ON COLUMN metadata.dv_satellite_tables.hub_id IS 
'Reference to parent hub. Establishes the core relationship in Data Vault model. Essential for maintaining structural integrity.';

COMMENT ON COLUMN metadata.dv_satellite_tables.source_table_name IS 
'Source table containing the descriptive attributes. May differ from hub source for multi-source satellites.';

-- Data Vault Link Tables Column Comments
COMMENT ON COLUMN metadata.dv_link_tables.source_table_name IS 
'Source table defining the relationship. Critical for understanding relationship origin and automation of link loading.';

-- Column Mappings Column Comments
COMMENT ON COLUMN metadata.dv_column_mappings.transformation_rule IS 
'SQL expression or transformation logic applied to source columns. Used by ETL automation to generate transformation code. ';

COMMENT ON COLUMN metadata.dv_column_mappings.is_business_key IS 
'Flags columns that form part of a business key. Critical for hub and link key generation and relationship management.';

COMMENT ON COLUMN metadata.dv_column_mappings.is_hash_key IS 
'Indicates if column is used in hash key generation. Important for Data Vault key management and change detection.';

COMMENT ON COLUMN metadata.dv_column_mappings.target_dtype IS 
'Target data type in Data Vault. May differ from source for standardization. Used in automated DDL generation and validation.';

-- Column Mapping Sources Column Comments
COMMENT ON COLUMN metadata.dv_column_mapping_sources.mapping_id IS 
'Reference to the target column mapping. Enables complex transformations involving multiple source columns.';

COMMENT ON COLUMN metadata.dv_column_mapping_sources.source_column_id IS 
'Reference to source column. Part of the detailed lineage tracking system.';

-- Common Audit Columns Comments
COMMENT ON COLUMN metadata.source_systems.created_at IS 
'Timestamp of record creation. Part of standard audit trail for change tracking and compliance.';

COMMENT ON COLUMN metadata.source_systems.created_by IS 
'User or process that created the record. Important for governance and audit requirements.';

COMMENT ON COLUMN metadata.dv_column_mappings.updated_at IS 
'Last modification timestamp. Critical for tracking mapping evolution and change management.';

COMMENT ON COLUMN metadata.dv_column_mappings.updated_by IS 
'User or process that last modified the mapping. Important for governance and troubleshooting.';

-- Views for easy querying
CREATE OR REPLACE VIEW metadata.v_source_metadata AS
SELECT 
    ss.system_name,
    st.schema_name as source_schema,
    st.table_name as source_table,
    sc.column_name as source_column,
    sc.data_type,
    sc.length,
    sc.nullable,
    sc.description
FROM metadata.source_systems ss
JOIN metadata.source_tables st ON ss.id = st.system_id
JOIN metadata.source_columns sc ON st.id = sc.table_id;
;
CREATE OR REPLACE VIEW metadata.v_hub_metadata AS
SELECT 
    h.schema_name,
    h.table_name,
    h.business_key,
    h.description,
    string_agg(DISTINCT s.table_name, ', ') as satellites
FROM metadata.dv_hub_tables h
LEFT JOIN metadata.dv_satellite_tables s ON h.id = s.hub_id
GROUP BY h.id, h.schema_name, h.table_name, h.business_key, h.description;
;
CREATE OR REPLACE VIEW metadata.v_link_metadata AS
SELECT 
    l.schema_name,
    l.table_name,
    l.description,
    string_agg(DISTINCT h.table_name, ', ') as connected_hubs,
    string_agg(DISTINCT s.table_name, ', ') as satellites
FROM metadata.dv_link_tables l
LEFT JOIN metadata.dv_link_hubs lh ON l.id = lh.link_id
LEFT JOIN metadata.dv_hub_tables h ON lh.hub_id = h.id
LEFT JOIN metadata.dv_link_satellites ls ON l.id = ls.link_id
LEFT JOIN metadata.dv_satellite_tables s ON ls.sat_id = s.id
GROUP BY l.id, l.schema_name, l.table_name, l.description;
;
CREATE OR REPLACE VIEW metadata.v_column_mappings AS
SELECT 
    sm.system_name,
    sm.source_schema,
    sm.source_table,
    sm.source_column,
    cm.target_schema,
    cm.target_table,
    cm.target_column,
    cm.is_business_key,
    cm.is_hash_key,
    cm.transformation_rule
FROM metadata.v_source_metadata sm
JOIN metadata.dv_column_mappings cm ON sm.source_column = cm.source_column_id;