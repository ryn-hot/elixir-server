-- Phase 2 playback parity: durable normalized probe data and playback plans.

CREATE TABLE IF NOT EXISTS media_file_probes (
    media_file_id TEXT PRIMARY KEY REFERENCES media_files(id) ON DELETE CASCADE,
    probe_version INTEGER NOT NULL,
    ffprobe_version TEXT,
    probe_status TEXT NOT NULL,
    probed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_mtime_ms BIGINT,
    source_size_bytes BIGINT,
    normalized_json TEXT,
    raw_json TEXT,
    error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_media_file_probes_status
    ON media_file_probes(probe_status);

ALTER TABLE playback_sessions
    ADD COLUMN playback_plan_json TEXT;

ALTER TABLE playback_sessions
    ADD COLUMN job_state_json TEXT;
