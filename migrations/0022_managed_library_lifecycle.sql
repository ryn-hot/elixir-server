CREATE TABLE IF NOT EXISTS managed_library_provenance (
    media_item_id TEXT PRIMARY KEY REFERENCES media_items(id) ON DELETE CASCADE,
    media_type TEXT NOT NULL,
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    year INTEGER,
    external_ids_json TEXT,
    manager_provider_id TEXT NOT NULL,
    manager_item_id TEXT,
    manager_label TEXT,
    manager_implementation TEXT,
    intent_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_managed_library_provenance_provider_item
    ON managed_library_provenance(manager_provider_id, manager_item_id);

CREATE INDEX IF NOT EXISTS idx_managed_library_provenance_title_year
    ON managed_library_provenance(media_type, normalized_title, year);

CREATE TABLE IF NOT EXISTS managed_media_tombstones (
    tombstone_id TEXT PRIMARY KEY,
    media_type TEXT NOT NULL,
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    year INTEGER,
    external_ids_json TEXT,
    manager_provider_id TEXT,
    manager_item_id TEXT,
    manager_label TEXT,
    manager_implementation TEXT,
    action TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    cleared_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_managed_media_tombstones_active_type
    ON managed_media_tombstones(active, media_type);

CREATE INDEX IF NOT EXISTS idx_managed_media_tombstones_provider_item
    ON managed_media_tombstones(manager_provider_id, manager_item_id);

CREATE INDEX IF NOT EXISTS idx_managed_media_tombstones_title_year
    ON managed_media_tombstones(active, media_type, normalized_title, year);
