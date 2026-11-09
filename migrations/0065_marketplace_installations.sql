-- One bounded local journal per owner-installed identity; no lifetime history.
-- Staging never enables an extension/instance. Source archive bytes are separate
-- owner-allocated storage; these counters cover encoded main-database records.
-- Revocation must not depend on a healthy installation journal. Ordinary new
-- approvals leave this boundary alone; revoke/reapprove cannot revive old code.
ALTER TABLE marketplace_extension_grants
ADD COLUMN revocation_revision BIGINT NOT NULL DEFAULT 0 CHECK (revocation_revision>=0);
UPDATE marketplace_extension_grants SET revocation_revision=revision WHERE enabled=FALSE;

CREATE TABLE IF NOT EXISTS marketplace_installation_budget (
    singleton BIGINT PRIMARY KEY CHECK (singleton=1),
    revision BIGINT NOT NULL CHECK (revision BETWEEN 0 AND 9007199254740991),
    entries BIGINT NOT NULL CHECK (entries BETWEEN 0 AND 4096),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes BETWEEN 0 AND 1073741824)
);
INSERT INTO marketplace_installation_budget VALUES (1,0,0,0)
ON CONFLICT(singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS marketplace_installations (
    extension_id TEXT PRIMARY KEY REFERENCES extensions(extension_id) ON DELETE RESTRICT,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE RESTRICT,
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    record_json TEXT NOT NULL CHECK (LENGTH(record_json) BETWEEN 1 AND 409600),
    record_bytes BIGINT NOT NULL CHECK (record_bytes BETWEEN 1 AND 409600),
    record_sha256 TEXT NOT NULL CHECK (LENGTH(record_sha256)=71),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes=record_bytes+131072),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS marketplace_installations_accounting
ON marketplace_installations(encoded_bytes);
