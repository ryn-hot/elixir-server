CREATE TABLE IF NOT EXISTS managed_episode_tombstones (
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
    season_number INTEGER NOT NULL,
    episode_number INTEGER NOT NULL,
    absolute_episode_number INTEGER,
    action TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    cleared_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_managed_episode_tombstones_active_lookup
    ON managed_episode_tombstones(active, media_type, normalized_title, year, season_number, episode_number);

CREATE INDEX IF NOT EXISTS idx_managed_episode_tombstones_provider_item
    ON managed_episode_tombstones(active, manager_provider_id, manager_item_id, season_number, episode_number);
