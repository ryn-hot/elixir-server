CREATE TABLE IF NOT EXISTS extension_source_module_certifications (
    certification_id TEXT PRIMARY KEY,
    source_module_id TEXT NOT NULL REFERENCES extension_source_modules(source_module_id) ON DELETE CASCADE,
    source_module_version_id TEXT REFERENCES extension_source_module_versions(version_id) ON DELETE SET NULL,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    adapter TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('certified', 'degraded', 'unsupported', 'broken', 'account_required', 'network_blocked', 'unknown', 'probation')),
    failure_class TEXT,
    summary TEXT,
    media_type_results_json TEXT NOT NULL DEFAULT '{}',
    materialization_results_json TEXT NOT NULL DEFAULT '{}',
    probe_targets_json TEXT NOT NULL DEFAULT '[]',
    candidate_evidence_json TEXT NOT NULL DEFAULT '[]',
    runtime_version TEXT,
    policy_version TEXT NOT NULL,
    certified_at TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_module_id, source_module_version_id, instance_id, adapter)
);

CREATE INDEX IF NOT EXISTS idx_extension_source_module_certifications_module
    ON extension_source_module_certifications(source_module_id, source_module_version_id, instance_id);

CREATE INDEX IF NOT EXISTS idx_extension_source_module_certifications_status
    ON extension_source_module_certifications(status, expires_at);

CREATE INDEX IF NOT EXISTS idx_extension_source_module_certifications_instance
    ON extension_source_module_certifications(instance_id, updated_at);

CREATE TABLE IF NOT EXISTS extension_source_module_quarantines (
    quarantine_id TEXT PRIMARY KEY,
    source_module_id TEXT NOT NULL REFERENCES extension_source_modules(source_module_id) ON DELETE CASCADE,
    source_module_version_id TEXT REFERENCES extension_source_module_versions(version_id) ON DELETE SET NULL,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    failure_class TEXT NOT NULL,
    hoster_domain TEXT,
    candidate_fingerprint TEXT,
    media_type TEXT,
    failure_count INTEGER NOT NULL DEFAULT 1,
    reason TEXT,
    evidence_json TEXT,
    first_observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    cleared_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_module_id, source_module_version_id, failure_class, hoster_domain, candidate_fingerprint, media_type)
);

CREATE INDEX IF NOT EXISTS idx_extension_source_module_quarantines_active
    ON extension_source_module_quarantines(instance_id, cleared_at, expires_at, failure_count);

CREATE INDEX IF NOT EXISTS idx_extension_source_module_quarantines_module
    ON extension_source_module_quarantines(source_module_id, failure_class, last_observed_at);
