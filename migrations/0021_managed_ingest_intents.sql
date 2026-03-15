CREATE TABLE IF NOT EXISTS managed_ingest_intents (
    intent_id TEXT PRIMARY KEY,
    media_type TEXT NOT NULL,
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    year INTEGER,
    external_ids_json TEXT,
    manager_provider_id TEXT NOT NULL,
    manager_item_id TEXT,
    manager_label TEXT,
    source TEXT NOT NULL DEFAULT 'find_media_add',
    active INTEGER NOT NULL DEFAULT 1,
    last_matched_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_managed_ingest_intents_provider_item
    ON managed_ingest_intents(manager_provider_id, manager_item_id);

CREATE INDEX IF NOT EXISTS idx_managed_ingest_intents_active_type
    ON managed_ingest_intents(active, media_type);

CREATE INDEX IF NOT EXISTS idx_managed_ingest_intents_normalized_title_year
    ON managed_ingest_intents(normalized_title, year);
