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
COMMENT ON TABLE metadata.dv_link_satellite_tables 
IS 'Metadata for link satellite tables in data vault model';

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

{# 
-- Business rules and validation
CREATE TABLE metadata.business_rules (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    rule_description TEXT,
    rule_sql TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(rule_name)
);

CREATE TABLE metadata.column_rules (
    id SERIAL PRIMARY KEY,
    column_id INTEGER REFERENCES metadata.source_columns(id),
    rule_id INTEGER REFERENCES metadata.business_rules(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(column_id, rule_id)
); #}

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