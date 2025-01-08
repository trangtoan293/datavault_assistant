-- Create metadata schema
CREATE SCHEMA IF NOT EXISTS metadata;

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Core metadata definitions table
CREATE TABLE metadata.metadata_definitions (
    id SERIAL PRIMARY KEY,
    source_schema VARCHAR(100) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_schema VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_entity_type VARCHAR(50) NOT NULL,  -- hub, lnk, sat, lsat
    collision_code VARCHAR(100),
    description TEXT,
    parent_table VARCHAR(100),                -- For sat, lsat references
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    version VARCHAR(20),
    validation_status VARCHAR(50),
    validation_warnings TEXT
);

-- Column mapping details
CREATE TABLE metadata.column_mappings (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata.metadata_definitions(id),
    target_column VARCHAR(100) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    key_type VARCHAR(50),                     -- hash_key_hub, hash_key_lnk, hash_key_sat, business_key, etc.
    parent_hub VARCHAR(100),                  -- Reference to parent hub for links
    is_composite BOOLEAN DEFAULT false,
    source_columns TEXT[],                    -- For composite keys
    source_column VARCHAR(100),               -- Single source column
    source_data_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);

-- Data Vault relationships
CREATE TABLE metadata.dv_relationships (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata.metadata_definitions(id),
    entity_type VARCHAR(50) NOT NULL,         -- hub, link, satellite
    parent_entity_id INTEGER REFERENCES metadata.metadata_definitions(id),
    relationship_type VARCHAR(50) NOT NULL,   -- parent-child, reference
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Version control for metadata
CREATE TABLE metadata.metadata_versions (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata.metadata_definitions(id),
    version_number FLOAT NOT NULL,
    change_type VARCHAR(20) NOT NULL,         -- INSERT, UPDATE, DELETE
    changes JSONB,                            -- Store what changed
    changed_by VARCHAR(100) NOT NULL,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    previous_version_id INTEGER REFERENCES metadata.metadata_versions(id),
    UNIQUE(metadata_id, version_number)
);

-- Audit logging
CREATE TABLE metadata.audit_log (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata.metadata_definitions(id),
    action VARCHAR(50) NOT NULL,
    action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(100) NOT NULL,
    details TEXT,
    ip_address VARCHAR(45)
);

-- Create indexes
CREATE INDEX idx_metadata_target ON metadata.metadata_definitions(target_schema, target_table);
CREATE INDEX idx_metadata_source ON metadata.metadata_definitions(source_schema, source_table);
CREATE INDEX idx_column_mappings_metadata ON metadata.column_mappings(metadata_id);
CREATE INDEX idx_dv_relationships_parent ON metadata.dv_relationships(parent_entity_id);
CREATE INDEX idx_metadata_versions_metadata ON metadata.metadata_versions(metadata_id);
CREATE INDEX idx_audit_log_metadata ON metadata.audit_log(metadata_id);

-- Create views for easy querying
CREATE OR REPLACE VIEW metadata.v_column_lineage AS
SELECT 
    md.source_schema,
    md.source_table,
    md.target_schema,
    md.target_table,
    md.target_entity_type,
    cm.source_column,
    cm.target_column,
    cm.key_type,
    cm.data_type
FROM metadata.metadata_definitions md
JOIN metadata.column_mappings cm ON md.id = cm.metadata_id;

CREATE OR REPLACE VIEW metadata.v_table_relationships AS
WITH RECURSIVE table_hierarchy AS (
    -- Base case
    SELECT 
        id,
        target_schema,
        target_table,
        parent_table,
        target_entity_type,
        1 as level,
        ARRAY[target_table]::VARCHAR[] as path
    FROM metadata.metadata_definitions
    WHERE parent_table IS NULL
    
    UNION ALL
    
    -- Recursive case
    SELECT 
        m.id,
        m.target_schema,
        m.target_table,
        m.parent_table,
        m.target_entity_type,
        h.level + 1,
        h.path || m.target_table
    FROM metadata.metadata_definitions m
    JOIN table_hierarchy h ON m.parent_table = h.target_table
    WHERE NOT m.target_table = ANY(h.path)
)
SELECT * FROM table_hierarchy;

-- Add comments
COMMENT ON TABLE metadata.metadata_definitions IS 'Core table storing Data Vault metadata definitions';
COMMENT ON TABLE metadata.column_mappings IS 'Stores column-level mapping information between source and target';
COMMENT ON TABLE metadata.dv_relationships IS 'Tracks relationships between Data Vault entities';
COMMENT ON TABLE metadata.metadata_versions IS 'Version control for metadata changes';
COMMENT ON TABLE metadata.audit_log IS 'Audit trail for metadata operations';