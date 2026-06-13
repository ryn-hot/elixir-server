CREATE TABLE IF NOT EXISTS extension_source_registries (
    registry_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    registry_key TEXT NOT NULL,
    registry_type TEXT NOT NULL CHECK (registry_type IN ('elixir_curated_cloudstream_pack', 'cloudstream_repo_json', 'cloudstream_plugins_json')),
    trust_class TEXT NOT NULL CHECK (trust_class IN ('curated', 'maintainer_known', 'custom')),
    display_name TEXT NOT NULL,
    url TEXT,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    auto_refresh BOOLEAN NOT NULL DEFAULT 1,
    trusted_for_executable_updates BOOLEAN NOT NULL DEFAULT 0,
    etag TEXT,
    last_modified TEXT,
    last_fetch_status TEXT NOT NULL DEFAULT 'unknown' CHECK (last_fetch_status IN ('unknown', 'success', 'failed', 'skipped')),
    last_fetch_error TEXT,
    last_fetched_at TIMESTAMP,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(instance_id, registry_key)
);

CREATE TABLE IF NOT EXISTS extension_source_modules (
    source_module_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    registry_id TEXT NOT NULL REFERENCES extension_source_registries(registry_id) ON DELETE CASCADE,
    module_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    ecosystem TEXT NOT NULL CHECK (ecosystem IN ('cloudstream', 'aniyomi')),
    plugin_package TEXT,
    active_version TEXT,
    rollback_version TEXT,
    media_types_json TEXT,
    language_tags_json TEXT,
    region_tags_json TEXT,
    source_domains_json TEXT,
    account_required BOOLEAN NOT NULL DEFAULT 0,
    unsupported BOOLEAN NOT NULL DEFAULT 0,
    unsupported_reason TEXT,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    installed BOOLEAN NOT NULL DEFAULT 0,
    pinned_version TEXT,
    health_state TEXT NOT NULL DEFAULT 'unknown' CHECK (health_state IN ('unknown', 'available', 'healthy', 'degraded', 'broken', 'unsupported', 'account_required', 'disabled')),
    replacement_recommendation_key TEXT,
    last_success_at TIMESTAMP,
    last_failure_at TIMESTAMP,
    last_error TEXT,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(instance_id, module_key)
);

CREATE TABLE IF NOT EXISTS extension_source_module_versions (
    version_id TEXT PRIMARY KEY,
    source_module_id TEXT NOT NULL REFERENCES extension_source_modules(source_module_id) ON DELETE CASCADE,
    version TEXT NOT NULL,
    artifact_url TEXT,
    artifact_sha256 TEXT,
    signature TEXT,
    install_state TEXT NOT NULL DEFAULT 'available' CHECK (install_state IN ('available', 'staged', 'installed', 'active', 'failed', 'rolled_back')),
    smoke_status TEXT NOT NULL DEFAULT 'unknown' CHECK (smoke_status IN ('unknown', 'passed', 'failed', 'skipped')),
    smoke_error TEXT,
    rollback_of_version_id TEXT REFERENCES extension_source_module_versions(version_id) ON DELETE SET NULL,
    installed_at TIMESTAMP,
    activated_at TIMESTAMP,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_module_id, version)
);

CREATE TABLE IF NOT EXISTS extension_source_health_events (
    health_event_id TEXT PRIMARY KEY,
    source_module_id TEXT NOT NULL REFERENCES extension_source_modules(source_module_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('unknown', 'available', 'healthy', 'degraded', 'broken', 'unsupported', 'account_required', 'disabled')),
    severity TEXT NOT NULL DEFAULT 'info' CHECK (severity IN ('info', 'warning', 'error')),
    reason TEXT,
    evidence_json TEXT,
    observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS extension_source_replacement_recommendations (
    recommendation_id TEXT PRIMARY KEY,
    source_module_id TEXT NOT NULL REFERENCES extension_source_modules(source_module_id) ON DELETE CASCADE,
    replacement_source_module_id TEXT REFERENCES extension_source_modules(source_module_id) ON DELETE SET NULL,
    replacement_registry_id TEXT REFERENCES extension_source_registries(registry_id) ON DELETE SET NULL,
    recommendation_key TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('replace', 'disable', 'pin', 'none')),
    recommended_version TEXT,
    reason TEXT,
    metadata_json TEXT,
    active BOOLEAN NOT NULL DEFAULT 1,
    applied_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_module_id, recommendation_key)
);

CREATE INDEX IF NOT EXISTS idx_extension_source_registries_instance
    ON extension_source_registries(instance_id, enabled);

CREATE INDEX IF NOT EXISTS idx_extension_source_registries_type
    ON extension_source_registries(registry_type, trust_class);

CREATE INDEX IF NOT EXISTS idx_extension_source_modules_instance
    ON extension_source_modules(instance_id, enabled, health_state);

CREATE INDEX IF NOT EXISTS idx_extension_source_modules_registry
    ON extension_source_modules(registry_id, enabled, health_state);

CREATE INDEX IF NOT EXISTS idx_extension_source_modules_ecosystem
    ON extension_source_modules(ecosystem, health_state);

CREATE INDEX IF NOT EXISTS idx_extension_source_module_versions_module
    ON extension_source_module_versions(source_module_id, install_state);

CREATE INDEX IF NOT EXISTS idx_extension_source_health_events_module
    ON extension_source_health_events(source_module_id, observed_at);

CREATE INDEX IF NOT EXISTS idx_extension_source_health_events_state
    ON extension_source_health_events(state, severity, observed_at);

CREATE INDEX IF NOT EXISTS idx_extension_source_recommendations_module
    ON extension_source_replacement_recommendations(source_module_id, active);
