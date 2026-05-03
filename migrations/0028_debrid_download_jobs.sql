CREATE TABLE IF NOT EXISTS debrid_download_jobs (
    job_id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL DEFAULT 'default',
    source TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    category TEXT,
    display_name TEXT,
    remote_torrent_id TEXT,
    remote_download_id TEXT,
    status TEXT NOT NULL DEFAULT 'submitted',
    local_path TEXT,
    links_json TEXT NOT NULL DEFAULT '[]',
    progress REAL,
    downloaded_bytes INTEGER,
    total_bytes INTEGER,
    download_rate_bps INTEGER,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_debrid_download_jobs_provider_status
    ON debrid_download_jobs(provider_id, status, updated_at);

CREATE INDEX IF NOT EXISTS idx_debrid_download_jobs_instance_status
    ON debrid_download_jobs(instance_id, status, updated_at);

CREATE INDEX IF NOT EXISTS idx_debrid_download_jobs_owner
    ON debrid_download_jobs(owner_id, created_at);
