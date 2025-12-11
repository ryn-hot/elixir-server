-- Persisted control-plane registry entries keyed by user and server instance.

CREATE TABLE IF NOT EXISTS server_registry (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    server_id TEXT NOT NULL,
    device_name TEXT NOT NULL,
    lan_addresses TEXT NOT NULL,
    wan_direct_endpoint TEXT,
    overlay_endpoint TEXT,
    status TEXT NOT NULL DEFAULT 'online',
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, server_id)
);
