ALTER TABLE seasons ADD COLUMN external_anilist TEXT;

CREATE TABLE IF NOT EXISTS season_external_ids (
    id TEXT PRIMARY KEY,
    season_id TEXT NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    source TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season_id, provider, external_id)
);

CREATE INDEX IF NOT EXISTS idx_season_external_ids_season_id
    ON season_external_ids(season_id);
