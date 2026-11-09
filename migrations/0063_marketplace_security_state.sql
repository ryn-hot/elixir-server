-- Private trusted high-water state, never writable by the peer worker. The
-- singleton is initialized only by migration; missing authority is not reset.
CREATE TABLE IF NOT EXISTS marketplace_security_budget (
    singleton BIGINT PRIMARY KEY CHECK (singleton=1),
    revision BIGINT NOT NULL CHECK (revision BETWEEN 0 AND 9007199254740991),
    publishers BIGINT NOT NULL CHECK (publishers BETWEEN 0 AND 10000),
    extensions BIGINT NOT NULL CHECK (extensions BETWEEN 0 AND 10000),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes BETWEEN 0 AND 8388608),
    blocked BIGINT NOT NULL CHECK (blocked IN (0,1))
);
INSERT INTO marketplace_security_budget VALUES (1,0,0,0,0,0)
ON CONFLICT(singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS marketplace_publisher_security (
    root_key TEXT PRIMARY KEY CHECK (LENGTH(root_key)=43),
    state_json TEXT NOT NULL CHECK (LENGTH(state_json) BETWEEN 1 AND 1024),
    state_bytes BIGINT NOT NULL CHECK (state_bytes=LENGTH(state_json)),
    state_sha256 TEXT NOT NULL CHECK (LENGTH(state_sha256)=71)
);
CREATE TABLE IF NOT EXISTS marketplace_extension_security (
    extension_id TEXT PRIMARY KEY CHECK (LENGTH(extension_id) BETWEEN 1 AND 256),
    root_key TEXT NOT NULL REFERENCES marketplace_publisher_security(root_key) ON DELETE RESTRICT,
    state_json TEXT NOT NULL CHECK (LENGTH(state_json) BETWEEN 1 AND 16384),
    state_bytes BIGINT NOT NULL CHECK (state_bytes=LENGTH(state_json)),
    state_sha256 TEXT NOT NULL CHECK (LENGTH(state_sha256)=71)
);
-- Keep quota reconciliation off the larger JSON payload pages.
CREATE INDEX IF NOT EXISTS marketplace_publisher_security_accounting
ON marketplace_publisher_security(state_bytes);
CREATE INDEX IF NOT EXISTS marketplace_extension_security_accounting
ON marketplace_extension_security(state_bytes);
