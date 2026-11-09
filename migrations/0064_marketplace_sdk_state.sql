-- Initialized with a fresh, disabled public instance, never by an SDK request.
-- Only current entries, no event history. Accounted bytes include encrypted
-- payloads plus controller-reserved row/index headroom. Primary-DB WAL/physical
-- maintenance is separate; this is not a filesystem-quota claim.
CREATE TABLE IF NOT EXISTS marketplace_instance_sdk_state (
    instance_id TEXT PRIMARY KEY REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    extension_id TEXT NOT NULL CHECK (LENGTH(extension_id) BETWEEN 1 AND 128),
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    key_count BIGINT NOT NULL CHECK (key_count BETWEEN 0 AND 128),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes BETWEEN 0 AND 12582912),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS marketplace_instance_sdk_values (
    instance_id TEXT NOT NULL REFERENCES marketplace_instance_sdk_state(instance_id) ON DELETE CASCADE,
    key TEXT NOT NULL CHECK (LENGTH(key) BETWEEN 1 AND 64),
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    value_encrypted TEXT NOT NULL CHECK (LENGTH(value_encrypted) BETWEEN 1 AND 98304),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes = LENGTH(value_encrypted)),
    PRIMARY KEY (instance_id, key)
);
