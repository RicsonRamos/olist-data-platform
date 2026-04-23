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
    context JSONB -- Store additional info like table name, env, etc.
);
