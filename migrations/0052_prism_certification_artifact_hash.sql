ALTER TABLE extension_source_module_certifications
    ADD COLUMN artifact_sha256 TEXT;

CREATE INDEX IF NOT EXISTS idx_extension_source_module_certifications_artifact
    ON extension_source_module_certifications(source_module_id, artifact_sha256, instance_id);
