CREATE TABLE IF NOT EXISTS torrentio_acquisition_jobs (
    job_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    source_provider_id TEXT,
    media_type TEXT NOT NULL,
    target_key TEXT NOT NULL,
    title TEXT NOT NULL,
    year INTEGER,
    external_ids_json TEXT,
    season_number INTEGER,
    episode_number INTEGER,
    absolute_episode_number INTEGER,
    route_policy TEXT NOT NULL DEFAULT 'debrid_first',
    status TEXT NOT NULL DEFAULT 'pending',
    route_logical_id TEXT,
    candidate_id TEXT,
    candidate_title TEXT,
    candidate_source TEXT,
    candidate_source_kind TEXT,
    candidate_rank INTEGER,
    download_id TEXT,
    last_error TEXT,
    last_search_at TEXT,
    last_submitted_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(intent_id, target_key)
);

CREATE INDEX IF NOT EXISTS idx_torrentio_acquisition_jobs_intent
    ON torrentio_acquisition_jobs(intent_id, status);

CREATE INDEX IF NOT EXISTS idx_torrentio_acquisition_jobs_status
    ON torrentio_acquisition_jobs(status, updated_at);
