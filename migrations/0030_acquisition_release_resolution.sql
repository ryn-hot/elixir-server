CREATE TABLE IF NOT EXISTS acquisition_releases (
    release_id TEXT PRIMARY KEY,
    subscription_id TEXT REFERENCES acquisition_subscriptions(subscription_id) ON DELETE SET NULL,
    source_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    source_extension_id TEXT NOT NULL,
    owner_id TEXT NOT NULL DEFAULT 'default',
    media_type TEXT NOT NULL,
    title TEXT NOT NULL,
    release_title TEXT NOT NULL,
    source TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    info_hash TEXT,
    fingerprint TEXT NOT NULL,
    release_kind TEXT NOT NULL,
    resolver_kind TEXT NOT NULL,
    resolver_version TEXT NOT NULL,
    confidence TEXT NOT NULL,
    score REAL,
    selected_route_logical_id TEXT,
    selected_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    download_id TEXT,
    remote_release_id TEXT,
    state TEXT NOT NULL DEFAULT 'candidate',
    state_reason TEXT,
    selected_candidate_json TEXT,
    coverage_plan_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_releases_owner_source_fingerprint
    ON acquisition_releases(owner_id, source_extension_id, fingerprint);

CREATE INDEX IF NOT EXISTS idx_acquisition_releases_subscription_state
    ON acquisition_releases(subscription_id, state, updated_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_releases_download_id
    ON acquisition_releases(download_id);

CREATE INDEX IF NOT EXISTS idx_acquisition_releases_remote_release_id
    ON acquisition_releases(remote_release_id);

CREATE TABLE IF NOT EXISTS acquisition_release_files (
    release_file_id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES acquisition_releases(release_id) ON DELETE CASCADE,
    file_index INTEGER,
    file_id TEXT,
    path TEXT NOT NULL,
    basename TEXT NOT NULL,
    size_bytes INTEGER,
    selectable INTEGER NOT NULL DEFAULT 1,
    parsed_title TEXT,
    parsed_season_number INTEGER,
    parsed_episode_number INTEGER,
    parsed_episode_end_number INTEGER,
    parsed_absolute_episode_number INTEGER,
    parsed_absolute_episode_end_number INTEGER,
    parsed_air_date TEXT,
    parsed_quality TEXT,
    parsed_language TEXT,
    parsed_release_group TEXT,
    parser_confidence TEXT NOT NULL,
    parser_reason TEXT,
    raw_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_release_files_release
    ON acquisition_release_files(release_id, file_index, path);

CREATE INDEX IF NOT EXISTS idx_acquisition_release_files_file_id
    ON acquisition_release_files(release_id, file_id);

CREATE TABLE IF NOT EXISTS acquisition_release_coverage (
    coverage_id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES acquisition_releases(release_id) ON DELETE CASCADE,
    release_file_id TEXT REFERENCES acquisition_release_files(release_file_id) ON DELETE CASCADE,
    target_id TEXT NOT NULL REFERENCES acquisition_targets(target_id) ON DELETE CASCADE,
    coverage_kind TEXT NOT NULL,
    confidence TEXT NOT NULL,
    score REAL,
    reason TEXT,
    state TEXT NOT NULL,
    verified_by TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(release_id, target_id, release_file_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_release_coverage_target_without_file
    ON acquisition_release_coverage(release_id, target_id)
    WHERE release_file_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_acquisition_release_coverage_target
    ON acquisition_release_coverage(target_id, state);

CREATE TABLE IF NOT EXISTS acquisition_release_jobs (
    release_job_id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES acquisition_releases(release_id) ON DELETE CASCADE,
    route_logical_id TEXT NOT NULL,
    provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    download_id TEXT,
    remote_release_id TEXT,
    state TEXT NOT NULL,
    state_reason TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_release_jobs_release_active
    ON acquisition_release_jobs(release_id, active, state);

CREATE INDEX IF NOT EXISTS idx_acquisition_release_jobs_route_active
    ON acquisition_release_jobs(route_logical_id, active, state, updated_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_release_jobs_download_id
    ON acquisition_release_jobs(download_id);
