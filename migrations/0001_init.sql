-- Core schema for Elixir server (users, server instances, extensions, media library, files).

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS server_instances (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_name TEXT NOT NULL,
    lan_addresses TEXT NOT NULL,
    wan_direct_endpoint TEXT,
    overlay_endpoint TEXT,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_configs (
    id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL REFERENCES server_instances(id) ON DELETE CASCADE,
    extension_id TEXT NOT NULL,
    config_json TEXT,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_items (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    external_ids TEXT,
    title TEXT NOT NULL,
    year INTEGER,
    season INTEGER,
    episode INTEGER,
    runtime_seconds INTEGER,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_files (
    id TEXT PRIMARY KEY,
    media_item_id TEXT NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
    source_config_id TEXT REFERENCES source_configs(id) ON DELETE SET NULL,
    path TEXT NOT NULL UNIQUE,
    size_bytes BIGINT,
    container TEXT,
    video_codec TEXT,
    audio_codec TEXT,
    width INTEGER,
    height INTEGER,
    bitrate_bps BIGINT,
    hash TEXT,
    scan_state TEXT NOT NULL DEFAULT 'ok',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hash)
);

CREATE INDEX IF NOT EXISTS idx_server_instances_user_id ON server_instances(user_id);
CREATE INDEX IF NOT EXISTS idx_media_items_type_year_title ON media_items(type, year, title);
CREATE INDEX IF NOT EXISTS idx_media_files_media_item_id ON media_files(media_item_id);
CREATE INDEX IF NOT EXISTS idx_media_files_scan_state ON media_files(scan_state);

