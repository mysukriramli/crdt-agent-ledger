CREATE SEQUENCE IF NOT EXISTS seq_commit_id START 1;

CREATE TABLE IF NOT EXISTS crdt_state (
    entity_id VARCHAR,
    property_key VARCHAR,
    property_value VARCHAR,
    lamport_timestamp BIGINT,
    wall_time_utc VARCHAR,
    agent_id VARCHAR,
    is_deleted BOOLEAN,
    PRIMARY KEY (entity_id, property_key)
);

CREATE TABLE IF NOT EXISTS vector_clocks (
    agent_id VARCHAR PRIMARY KEY,
    last_lamport_timestamp BIGINT,
    last_sync_utc VARCHAR
);

CREATE TABLE IF NOT EXISTS crdt_ledger (
    commit_id BIGINT PRIMARY KEY,
    timestamp_utc VARCHAR,
    delta_id VARCHAR,
    agent_id VARCHAR,
    lamport_timestamp BIGINT,
    operations_count INTEGER
);
