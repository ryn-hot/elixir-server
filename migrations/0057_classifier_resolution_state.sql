-- Automatic classifier state is internal retry/diagnostic data. It is
-- intentionally separate from the user-facing manual review queue.
CREATE TABLE IF NOT EXISTS classifier_resolution_state (
    media_file_id TEXT PRIMARY KEY REFERENCES media_files(id) ON DELETE CASCADE,
    disposition TEXT NOT NULL,
    confidence REAL,
    hint_json TEXT,
    candidates_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK(disposition IN ('unresolved', 'applied'))
);

CREATE INDEX IF NOT EXISTS idx_classifier_resolution_disposition
    ON classifier_resolution_state(disposition);
