ALTER TABLE debrid_download_jobs ADD COLUMN provider_implementation TEXT;
ALTER TABLE debrid_download_jobs ADD COLUMN remote_release_id TEXT;
ALTER TABLE debrid_download_jobs ADD COLUMN remote_release_status TEXT;
ALTER TABLE debrid_download_jobs ADD COLUMN provider_capabilities_json TEXT;
ALTER TABLE debrid_download_jobs ADD COLUMN selection_mode TEXT;
ALTER TABLE debrid_download_jobs ADD COLUMN selected_file_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE debrid_download_jobs ADD COLUMN skipped_file_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE debrid_download_jobs ADD COLUMN selection_error TEXT;
ALTER TABLE debrid_download_jobs ADD COLUMN release_id TEXT REFERENCES acquisition_releases(release_id) ON DELETE SET NULL;

UPDATE debrid_download_jobs
SET provider_implementation = 'real_debrid'
WHERE provider_implementation IS NULL;

UPDATE debrid_download_jobs
SET remote_release_id = COALESCE(remote_torrent_id, remote_download_id)
WHERE remote_release_id IS NULL;

UPDATE debrid_download_jobs
SET remote_release_status = status
WHERE remote_release_status IS NULL;

CREATE INDEX IF NOT EXISTS idx_debrid_download_jobs_remote_release
    ON debrid_download_jobs(provider_id, remote_release_id);

CREATE INDEX IF NOT EXISTS idx_debrid_download_jobs_release
    ON debrid_download_jobs(release_id);

ALTER TABLE acquisition_release_files ADD COLUMN provider_file_id TEXT;
ALTER TABLE acquisition_release_files ADD COLUMN selected INTEGER;
ALTER TABLE acquisition_release_files ADD COLUMN provider_metadata_json TEXT;

UPDATE acquisition_release_files
SET provider_file_id = file_id
WHERE provider_file_id IS NULL
  AND file_id IS NOT NULL;

UPDATE acquisition_release_files
SET provider_metadata_json = raw_json
WHERE provider_metadata_json IS NULL
  AND raw_json IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_acquisition_release_files_provider_file_id
    ON acquisition_release_files(release_id, provider_file_id);
