CREATE TABLE IF NOT EXISTS torrentio_acquisition_subscriptions (
    intent_id TEXT PRIMARY KEY,
    media_type TEXT NOT NULL,
    last_metadata_refresh_at TEXT,
    next_metadata_refresh_at TEXT,
    last_expanded_target_count INTEGER NOT NULL DEFAULT 0,
    next_air_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_torrentio_acquisition_subscriptions_next_refresh
    ON torrentio_acquisition_subscriptions(next_metadata_refresh_at);

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN search_media_type TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN aired_at TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN next_search_at TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN search_attempts INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_torrentio_acquisition_jobs_next_search
    ON torrentio_acquisition_jobs(status, next_search_at);
