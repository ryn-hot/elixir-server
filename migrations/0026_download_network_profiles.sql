CREATE TABLE IF NOT EXISTS download_network_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('external_only', 'direct', 'cloudflare_warp', 'wireguard_config', 'openvpn_config', 'provider_preset', 'debrid_only')),
    enabled BOOLEAN NOT NULL DEFAULT 1,
    strict BOOLEAN NOT NULL DEFAULT 1,
    scope TEXT NOT NULL DEFAULT 'managed_downloaders',
    provider TEXT,
    gateway_runtime TEXT,
    config_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'unknown',
    active BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_applied_at TIMESTAMP,
    last_verified_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS download_network_profile_secrets (
    profile_id TEXT NOT NULL REFERENCES download_network_profiles(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    secret_ref TEXT NOT NULL,
    PRIMARY KEY (profile_id, key)
);

CREATE TABLE IF NOT EXISTS download_warp_enrollments (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES download_network_profiles(id) ON DELETE CASCADE,
    enrollment_id TEXT NOT NULL UNIQUE,
    identity_secret_ref TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending_runtime',
    disclosure_version TEXT NOT NULL,
    disclosure_accepted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_checked_at TIMESTAMP,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS download_provider_bindings (
    id TEXT PRIMARY KEY,
    logical_role TEXT NOT NULL,
    binding_kind TEXT NOT NULL,
    provider_id TEXT,
    external_instance_id TEXT,
    managed_instance_id TEXT,
    profile_id TEXT REFERENCES download_network_profiles(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS download_network_events (
    id TEXT PRIMARY KEY,
    profile_id TEXT REFERENCES download_network_profiles(id) ON DELETE SET NULL,
    operation TEXT NOT NULL,
    status TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_download_network_profiles_active
    ON download_network_profiles(active, status);

CREATE INDEX IF NOT EXISTS idx_download_warp_enrollments_profile
    ON download_warp_enrollments(profile_id, status);

CREATE INDEX IF NOT EXISTS idx_download_provider_bindings_role
    ON download_provider_bindings(logical_role, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_download_provider_bindings_logical_role
    ON download_provider_bindings(logical_role);

CREATE INDEX IF NOT EXISTS idx_download_network_events_profile
    ON download_network_events(profile_id, started_at);
