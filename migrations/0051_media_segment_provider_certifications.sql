-- MIDM marketplace segment provider certification evidence.
--
-- Segment provider certification is intentionally separate from acquisition
-- source-module certification because media.segment_provider is a provider
-- capability, not a source registry module.

CREATE TABLE IF NOT EXISTS media_segment_provider_certifications (
    certification_id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    provider_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    failure_class TEXT,
    summary TEXT,
    media_type_results_json TEXT NOT NULL,
    segment_type_results_json TEXT NOT NULL,
    probe_targets_json TEXT NOT NULL,
    response_evidence_json TEXT NOT NULL,
    runtime_version TEXT,
    policy_version TEXT NOT NULL,
    certified_at TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(provider_id, policy_version)
);

CREATE INDEX IF NOT EXISTS idx_media_segment_provider_certifications_provider
    ON media_segment_provider_certifications(provider_id, updated_at);

CREATE INDEX IF NOT EXISTS idx_media_segment_provider_certifications_status
    ON media_segment_provider_certifications(status, expires_at);

CREATE INDEX IF NOT EXISTS idx_media_segment_provider_certifications_instance
    ON media_segment_provider_certifications(instance_id, updated_at);
