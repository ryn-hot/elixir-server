-- Extension system schema (extensions, instances, providers, bindings, desired state, secrets, orchestrator runs).

CREATE TABLE IF NOT EXISTS extensions (
    extension_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('module', 'connector', 'blueprint')),
    publisher_name TEXT,
    signing_key_id TEXT,
    trust_level TEXT NOT NULL DEFAULT 'community' CHECK (trust_level IN ('verified', 'community', 'untrusted')),
    manifest_json TEXT NOT NULL,
    package_hash TEXT,
    installed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    enabled BOOLEAN NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS extension_instances (
    instance_id TEXT PRIMARY KEY,
    extension_id TEXT NOT NULL REFERENCES extensions(extension_id) ON DELETE CASCADE,
    instance_name TEXT NOT NULL,
    config_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    UNIQUE(extension_id, instance_name)
);

CREATE TABLE IF NOT EXISTS providers (
    provider_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    capability TEXT NOT NULL,
    slot_id TEXT NOT NULL DEFAULT 'default',
    cardinality TEXT NOT NULL CHECK (cardinality IN ('one', 'many', 'zero_or_one')),
    endpoint_json TEXT,
    health_state TEXT NOT NULL DEFAULT 'unknown' CHECK (health_state IN ('unknown', 'healthy', 'degraded', 'unhealthy')),
    last_healthcheck_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(instance_id, capability, slot_id)
);

CREATE TABLE IF NOT EXISTS bindings (
    binding_id TEXT PRIMARY KEY,
    consumer_provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    requires_capability TEXT NOT NULL,
    requires_slot_id TEXT NOT NULL DEFAULT 'default',
    target_provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    binding_params_json TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'applied', 'failed')),
    last_error TEXT,
    last_applied_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(consumer_provider_id, requires_capability, requires_slot_id, target_provider_id)
);

CREATE TABLE IF NOT EXISTS desired_blueprints (
    desired_id TEXT PRIMARY KEY,
    blueprint_extension_id TEXT NOT NULL,
    blueprint_version TEXT NOT NULL,
    params_json TEXT,
    applied BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS secrets (
    secret_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL CHECK (scope IN ('instance', 'provider', 'global')),
    scope_id TEXT,
    key TEXT NOT NULL,
    value_encrypted TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rotatable BOOLEAN NOT NULL DEFAULT 0,
    UNIQUE(scope, scope_id, key)
);

CREATE TABLE IF NOT EXISTS orchestrator_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'failed', 'completed', 'canceled')),
    phase TEXT,
    plan_json TEXT,
    error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS operation_steps (
    step_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES orchestrator_runs(run_id) ON DELETE CASCADE,
    step_index INTEGER NOT NULL,
    action_type TEXT NOT NULL,
    action_json TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'failed', 'completed', 'skipped')),
    error TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, step_index)
);

CREATE TABLE IF NOT EXISTS runtime_logs (
    log_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    log_uri TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_extensions_enabled ON extensions(enabled);
CREATE INDEX IF NOT EXISTS idx_extensions_kind ON extensions(kind);

CREATE INDEX IF NOT EXISTS idx_extension_instances_extension_id ON extension_instances(extension_id);
CREATE INDEX IF NOT EXISTS idx_extension_instances_enabled ON extension_instances(enabled);

CREATE INDEX IF NOT EXISTS idx_providers_capability_slot ON providers(capability, slot_id);
CREATE INDEX IF NOT EXISTS idx_providers_health_state ON providers(health_state);
CREATE INDEX IF NOT EXISTS idx_providers_instance_id ON providers(instance_id);

CREATE INDEX IF NOT EXISTS idx_bindings_consumer ON bindings(consumer_provider_id);
CREATE INDEX IF NOT EXISTS idx_bindings_target ON bindings(target_provider_id);
CREATE INDEX IF NOT EXISTS idx_bindings_status ON bindings(status);

CREATE INDEX IF NOT EXISTS idx_desired_blueprints_applied ON desired_blueprints(applied);

CREATE INDEX IF NOT EXISTS idx_secrets_scope ON secrets(scope, scope_id);

CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_status ON orchestrator_runs(status);

CREATE INDEX IF NOT EXISTS idx_operation_steps_run_id ON operation_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_operation_steps_status ON operation_steps(status);

CREATE INDEX IF NOT EXISTS idx_runtime_logs_instance_id ON runtime_logs(instance_id);
