CREATE TABLE IF NOT EXISTS extension_source_registries_v0042 (
    registry_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    registry_key TEXT NOT NULL,
    registry_type TEXT NOT NULL CHECK (registry_type IN ('elixir_curated_cloudstream_pack', 'cloudstream_repo_json', 'cloudstream_plugins_json', 'elixir_curated_nuvio_pack', 'nuvio_manifest_json', 'stremio_manifest_json')),
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

INSERT INTO extension_source_registries_v0042 (
    registry_id,
    instance_id,
    registry_key,
    registry_type,
    trust_class,
    display_name,
    url,
    enabled,
    auto_refresh,
    trusted_for_executable_updates,
    etag,
    last_modified,
    last_fetch_status,
    last_fetch_error,
    last_fetched_at,
    metadata_json,
    created_at,
    updated_at
)
SELECT
    registry_id,
    instance_id,
    registry_key,
    registry_type,
    trust_class,
    display_name,
    url,
    enabled,
    auto_refresh,
    trusted_for_executable_updates,
    etag,
    last_modified,
    last_fetch_status,
    last_fetch_error,
    last_fetched_at,
    metadata_json,
    created_at,
    updated_at
FROM extension_source_registries;

DROP TABLE extension_source_registries;
ALTER TABLE extension_source_registries_v0042 RENAME TO extension_source_registries;

CREATE TABLE IF NOT EXISTS extension_source_modules_v0042 (
    source_module_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    registry_id TEXT NOT NULL REFERENCES extension_source_registries(registry_id) ON DELETE CASCADE,
    module_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    ecosystem TEXT NOT NULL CHECK (ecosystem IN ('cloudstream', 'aniyomi', 'nuvio', 'stremio')),
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

INSERT INTO extension_source_modules_v0042 (
    source_module_id,
    instance_id,
    registry_id,
    module_key,
    display_name,
    ecosystem,
    plugin_package,
    active_version,
    rollback_version,
    media_types_json,
    language_tags_json,
    region_tags_json,
    source_domains_json,
    account_required,
    unsupported,
    unsupported_reason,
    enabled,
    installed,
    pinned_version,
    health_state,
    replacement_recommendation_key,
    last_success_at,
    last_failure_at,
    last_error,
    metadata_json,
    created_at,
    updated_at
)
SELECT
    source_module_id,
    instance_id,
    registry_id,
    module_key,
    display_name,
    ecosystem,
    plugin_package,
    active_version,
    rollback_version,
    media_types_json,
    language_tags_json,
    region_tags_json,
    source_domains_json,
    account_required,
    unsupported,
    unsupported_reason,
    enabled,
    installed,
    pinned_version,
    health_state,
    replacement_recommendation_key,
    last_success_at,
    last_failure_at,
    last_error,
    metadata_json,
    created_at,
    updated_at
FROM extension_source_modules;

DROP TABLE extension_source_modules;
ALTER TABLE extension_source_modules_v0042 RENAME TO extension_source_modules;

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
