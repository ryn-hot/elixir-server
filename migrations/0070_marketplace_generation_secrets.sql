-- New private generations do not share the legacy instance/global/provider
-- secret address space. Namespace deletion is NOT native-retirement proof.
-- There is no backfill/adoption or rewrite of existing secrets/ciphertexts.
CREATE TABLE marketplace_generation_secrets (
    namespace_id TEXT NOT NULL REFERENCES marketplace_sdk_namespaces(namespace_id) ON DELETE CASCADE,
    key TEXT NOT NULL CHECK (LENGTH(key) BETWEEN 1 AND 64),
    secret_id TEXT NOT NULL UNIQUE CHECK (LENGTH(secret_id)=36),
    value_encrypted TEXT NOT NULL CHECK (LENGTH(value_encrypted) BETWEEN 1 AND 98304),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes=LENGTH(value_encrypted)),
    rotatable BOOLEAN NOT NULL,
    PRIMARY KEY(namespace_id,key)
);
