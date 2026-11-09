-- Restriction marker, NOT enrollment/native/source or cleanup authority.
-- Fail closed for all existing roots: none may be silently adopted by the
-- separate owner-directed foreground controller after a journal is lost.
-- Only an explicitly fresh foreground namespace is tagged foreground.
ALTER TABLE marketplace_instance_sdk_state
    ADD COLUMN purpose TEXT NOT NULL DEFAULT 'installed'
    CHECK (purpose IN ('installed', 'foreground'));
CREATE INDEX IF NOT EXISTS marketplace_sdk_namespace_extension_purpose
    ON marketplace_instance_sdk_state(extension_id, purpose);
