-- Preserve legacy records byte-for-byte with NULL configured ownership. They
-- are NOT adopted into the new runtime model by this structural migration.
-- SQLite cannot remove the old extension-only primary key in place.
CREATE TABLE marketplace_installations_per_instance (
    extension_id TEXT NOT NULL REFERENCES extensions(extension_id) ON DELETE RESTRICT,
    instance_id TEXT REFERENCES extension_instances(instance_id) ON DELETE RESTRICT,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE RESTRICT,
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    record_json TEXT NOT NULL CHECK (LENGTH(record_json) BETWEEN 1 AND 409600),
    record_bytes BIGINT NOT NULL CHECK (record_bytes BETWEEN 1 AND 409600),
    record_sha256 TEXT NOT NULL CHECK (LENGTH(record_sha256)=71),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes=record_bytes+131072),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO marketplace_installations_per_instance
    (extension_id,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes,updated_at)
SELECT extension_id,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes,updated_at
FROM marketplace_installations;
DROP TABLE marketplace_installations;
ALTER TABLE marketplace_installations_per_instance RENAME TO marketplace_installations;
CREATE UNIQUE INDEX marketplace_installations_instance ON marketplace_installations(instance_id) WHERE instance_id IS NOT NULL;
CREATE UNIQUE INDEX marketplace_installations_legacy ON marketplace_installations(extension_id) WHERE instance_id IS NULL;
CREATE INDEX marketplace_installations_extension ON marketplace_installations(extension_id);
CREATE INDEX marketplace_installations_accounting ON marketplace_installations(encoded_bytes);

-- Desired UI/product metadata is not evidence of an instance's actual source.
-- This bounded marker is allocated atomically with an instance journal, within
-- its existing row reservation; no source, runtime or grant authority lives here.
CREATE TABLE marketplace_installation_metadata (
    extension_id TEXT PRIMARY KEY REFERENCES extensions(extension_id) ON DELETE RESTRICT,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE RESTRICT,
    metadata_sha256 TEXT NOT NULL CHECK (LENGTH(metadata_sha256)=71)
);
