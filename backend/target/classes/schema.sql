-- FlowForge Database Schema
-- Pipeline definitions
CREATE TABLE IF NOT EXISTS pipelines (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    definition JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_pipeline_definition ON pipelines USING GIN (definition);

-- Node type registry
CREATE TABLE IF NOT EXISTS node_types (
    id VARCHAR(50) PRIMARY KEY,
    category VARCHAR(20) NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    config_schema JSONB NOT NULL,
    icon VARCHAR(50)
);

-- Seed node types
INSERT INTO node_types (id, category, display_name, description, config_schema, icon) VALUES
('S3_SOURCE', 'SOURCE', 'S3 File Source', 'Read CSV/JSON files from S3 buckets',
 '{"type":"object","properties":{"bucket":{"type":"string","title":"Bucket Name"},"key":{"type":"string","title":"File Key"},"format":{"type":"string","enum":["csv","json"],"title":"Format"},"hasHeader":{"type":"boolean","title":"Has Header Row"}}}',
 'cloud-download'),

('KAFKA_SOURCE', 'SOURCE', 'Kafka Topic', 'Consume records from a Kafka topic',
 '{"type":"object","properties":{"topic":{"type":"string","title":"Topic Name"},"consumerGroup":{"type":"string","title":"Consumer Group"}}}',
 'stream'),

('FILTER', 'TRANSFORM', 'Filter', 'Filter records by field condition',
 '{"type":"object","properties":{"field":{"type":"string","title":"Field Name"},"operator":{"type":"string","enum":["==","!=",">","<","contains"],"title":"Operator"},"value":{"type":"string","title":"Value"}}}',
 'filter'),

('FIELD_MAPPER', 'TRANSFORM', 'Field Mapper', 'Rename or select fields',
 '{"type":"object","properties":{"mappings":{"type":"array","title":"Field Mappings","items":{"type":"object","properties":{"from":{"type":"string"},"to":{"type":"string"}}}}}}',
 'shuffle'),

('AGGREGATOR', 'TRANSFORM', 'Aggregator', 'Group and aggregate records',
 '{"type":"object","properties":{"groupBy":{"type":"string","title":"Group By Field"},"function":{"type":"string","enum":["count","sum","avg"],"title":"Function"},"field":{"type":"string","title":"Aggregate Field"}}}',
 'calculator'),

('POSTGRES_SINK', 'SINK', 'PostgreSQL', 'Write records to a Postgres table',
 '{"type":"object","properties":{"table":{"type":"string","title":"Table Name"},"writeMode":{"type":"string","enum":["append","upsert"],"title":"Write Mode"}}}',
 'database'),

('S3_SINK', 'SINK', 'S3 File Output', 'Write records to S3 as CSV/JSON',
 '{"type":"object","properties":{"bucket":{"type":"string","title":"Bucket Name"},"keyPrefix":{"type":"string","title":"Key Prefix"},"format":{"type":"string","enum":["csv","json"],"title":"Format"}}}',
 'cloud-upload'),

('KAFKA_SINK', 'SINK', 'Kafka Topic Output', 'Publish records to a Kafka topic',
 '{"type":"object","properties":{"topic":{"type":"string","title":"Topic Name"}}}',
 'send')

ON CONFLICT (id) DO NOTHING;
