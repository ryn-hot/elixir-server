ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_info_hash TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_file_index INTEGER;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_quality TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_size_bytes INTEGER;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_seeders INTEGER;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_language TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_cached_debrid INTEGER;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_score INTEGER;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN candidate_score_badges_json TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN import_event_id TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN imported_at TEXT;

ALTER TABLE torrentio_acquisition_jobs
    ADD COLUMN import_error TEXT;

CREATE INDEX IF NOT EXISTS idx_torrentio_acquisition_jobs_download
    ON torrentio_acquisition_jobs(download_id);

CREATE INDEX IF NOT EXISTS idx_torrentio_acquisition_jobs_import
    ON torrentio_acquisition_jobs(imported_at, import_event_id);
