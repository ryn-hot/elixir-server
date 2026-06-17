CREATE TABLE IF NOT EXISTS extension_source_certification_jobs (
    job_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    registry_id TEXT REFERENCES extension_source_registries(registry_id) ON DELETE CASCADE,
    source_module_id TEXT REFERENCES extension_source_modules(source_module_id) ON DELETE CASCADE,
    requested_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'degraded', 'blocked', 'failed', 'cancelled', 'skipped')),
    priority INTEGER NOT NULL DEFAULT 100,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 2,
    language_eligibility TEXT,
    marketplace_state TEXT,
    summary TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_extension_source_certification_jobs_instance_status
    ON extension_source_certification_jobs(instance_id, status, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_extension_source_certification_jobs_registry_status
    ON extension_source_certification_jobs(registry_id, status, updated_at);

CREATE INDEX IF NOT EXISTS idx_extension_source_certification_jobs_module
    ON extension_source_certification_jobs(source_module_id, updated_at);
