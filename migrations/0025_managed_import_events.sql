CREATE TABLE IF NOT EXISTS managed_import_events (
    event_id TEXT PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE,
    intent_id TEXT NOT NULL,
    media_type TEXT NOT NULL,
    external_ids_json TEXT,
    manager_provider_id TEXT NOT NULL,
    manager_item_id TEXT,
    manager_label TEXT,
    manager_implementation TEXT,
    imported_files_json TEXT NOT NULL,
    raw_manager_payload_json TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    linked_media_item_id TEXT,
    last_error TEXT,
    imported_at TIMESTAMP,
    processed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_managed_import_events_intent
    ON managed_import_events(intent_id, status);

CREATE INDEX IF NOT EXISTS idx_managed_import_events_provider_item
    ON managed_import_events(manager_provider_id, manager_item_id, status);

CREATE INDEX IF NOT EXISTS idx_managed_import_events_status
    ON managed_import_events(status, updated_at);
