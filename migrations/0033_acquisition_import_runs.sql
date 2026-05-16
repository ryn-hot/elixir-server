CREATE TABLE IF NOT EXISTS acquisition_import_runs (
    import_run_id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES acquisition_releases(release_id) ON DELETE CASCADE,
    release_job_id TEXT NOT NULL REFERENCES acquisition_release_jobs(release_job_id) ON DELETE CASCADE,
    route_logical_id TEXT NOT NULL,
    provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    download_id TEXT,
    remote_release_id TEXT,
    state TEXT NOT NULL,
    state_reason TEXT,
    mismatch_class TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    provenance_json TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(release_id, release_job_id)
);

CREATE INDEX IF NOT EXISTS idx_acquisition_import_runs_state
    ON acquisition_import_runs(state, updated_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_import_runs_release
    ON acquisition_import_runs(release_id, state);

CREATE TABLE IF NOT EXISTS acquisition_import_file_links (
    import_link_id TEXT PRIMARY KEY,
    import_run_id TEXT NOT NULL REFERENCES acquisition_import_runs(import_run_id) ON DELETE CASCADE,
    release_id TEXT NOT NULL REFERENCES acquisition_releases(release_id) ON DELETE CASCADE,
    release_file_id TEXT REFERENCES acquisition_release_files(release_file_id) ON DELETE SET NULL,
    target_id TEXT REFERENCES acquisition_targets(target_id) ON DELETE CASCADE,
    local_path TEXT,
    media_file_id TEXT REFERENCES media_files(id) ON DELETE SET NULL,
    movie_id TEXT REFERENCES movies(id) ON DELETE SET NULL,
    episode_id TEXT REFERENCES episodes(id) ON DELETE SET NULL,
    state TEXT NOT NULL,
    state_reason TEXT,
    verification_state TEXT,
    mismatch_class TEXT,
    evidence_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_import_links_file_target
    ON acquisition_import_file_links(import_run_id, release_file_id, target_id)
    WHERE release_file_id IS NOT NULL
      AND target_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_import_links_target_without_file
    ON acquisition_import_file_links(import_run_id, target_id)
    WHERE release_file_id IS NULL
      AND target_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_acquisition_import_links_release
    ON acquisition_import_file_links(release_id, state);
