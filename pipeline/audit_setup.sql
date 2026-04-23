CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.audit_jobs (
    job_id SERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds FLOAT,
    rows_processed INTEGER,
    error_message TEXT,
    context JSONB
);

CREATE TABLE IF NOT EXISTS metadata.processed_files (
    file_id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_hash TEXT UNIQUE NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_count INTEGER
);

CREATE TABLE IF NOT EXISTS metadata.event_log (
    event_id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    idempotency_key TEXT UNIQUE, -- Prevents duplicate event processing
    partition_key TEXT,         -- Simulates event partitioning (e.g., by table)
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata.event_processing (
    event_id INTEGER REFERENCES metadata.event_log(event_id),
    consumer_group TEXT NOT NULL,
    status TEXT NOT NULL, -- 'RECEIVED', 'PROCESSED', 'FAILED'
    processed_at TIMESTAMP,
    PRIMARY KEY (event_id, consumer_group)
);

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id UUID PRIMARY KEY,
    status TEXT NOT NULL, -- 'PENDING', 'RUNNING', 'SUCCESS', 'FAILED'
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    metadata JSONB
);
