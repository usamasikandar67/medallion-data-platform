-- Create Monitoring Schema Table
-- Target Database: data/warehouse.db

CREATE TABLE IF NOT EXISTS monitor_pipeline_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    duration_seconds REAL,
    status TEXT NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    records_processed INTEGER DEFAULT 0,
    error_message TEXT
);
