-- SDK namespace IDs are private state generations, not operational instance
-- IDs. Preserve existing IDs/ciphertexts as identity-mapped namespaces. This
-- migration grants no runtime, adoption, cleanup or activation authority.
CREATE TABLE marketplace_sdk_namespaces (
    namespace_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    extension_id TEXT NOT NULL REFERENCES extensions(extension_id) ON DELETE CASCADE,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    purpose TEXT NOT NULL CHECK (purpose IN ('installed', 'foreground'))
);
CREATE INDEX marketplace_sdk_namespaces_instance
    ON marketplace_sdk_namespaces(instance_id, namespace_id);
CREATE INDEX marketplace_sdk_namespaces_extension_purpose
    ON marketplace_sdk_namespaces(extension_id, purpose);
INSERT INTO marketplace_sdk_namespaces(namespace_id,instance_id,extension_id,home_id,purpose)
    SELECT instance_id,instance_id,extension_id,home_id,purpose
    FROM marketplace_instance_sdk_state;

-- Rebuild only these two private tables transactionally so their FK follows
-- the namespace, not a replaceable operational instance. PostgreSQL's explicit
-- adapter retargets the FK without copying state. No operational row is rewritten.
CREATE TABLE marketplace_instance_sdk_state_v2 (
    instance_id TEXT PRIMARY KEY REFERENCES marketplace_sdk_namespaces(namespace_id) ON DELETE CASCADE,
    extension_id TEXT NOT NULL CHECK (LENGTH(extension_id) BETWEEN 1 AND 128),
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    key_count BIGINT NOT NULL CHECK (key_count BETWEEN 0 AND 128),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes BETWEEN 0 AND 12582912),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    purpose TEXT NOT NULL DEFAULT 'installed' CHECK (purpose IN ('installed', 'foreground'))
);
INSERT INTO marketplace_instance_sdk_state_v2
    SELECT instance_id,extension_id,home_id,revision,key_count,encoded_bytes,updated_at,purpose
    FROM marketplace_instance_sdk_state;
CREATE TABLE marketplace_instance_sdk_values_v2 (
    instance_id TEXT NOT NULL REFERENCES marketplace_instance_sdk_state_v2(instance_id) ON DELETE CASCADE,
    key TEXT NOT NULL CHECK (LENGTH(key) BETWEEN 1 AND 64),
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    value_encrypted TEXT NOT NULL CHECK (LENGTH(value_encrypted) BETWEEN 1 AND 98304),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes = LENGTH(value_encrypted)),
    PRIMARY KEY (instance_id, key)
);
INSERT INTO marketplace_instance_sdk_values_v2
    SELECT instance_id,key,revision,value_encrypted,encoded_bytes
    FROM marketplace_instance_sdk_values;
DROP TABLE marketplace_instance_sdk_values;
DROP TABLE marketplace_instance_sdk_state;
ALTER TABLE marketplace_instance_sdk_state_v2 RENAME TO marketplace_instance_sdk_state;
ALTER TABLE marketplace_instance_sdk_values_v2 RENAME TO marketplace_instance_sdk_values;
CREATE INDEX marketplace_sdk_namespace_extension_purpose
    ON marketplace_instance_sdk_state(extension_id, purpose);
