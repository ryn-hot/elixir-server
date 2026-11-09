-- PostgreSQL can preserve all existing record bytes/timestamps in place.
ALTER TABLE marketplace_installations ADD COLUMN instance_id TEXT REFERENCES extension_instances(instance_id) ON DELETE RESTRICT;
ALTER TABLE marketplace_installations DROP CONSTRAINT marketplace_installations_pkey;
CREATE UNIQUE INDEX marketplace_installations_instance ON marketplace_installations(instance_id) WHERE instance_id IS NOT NULL;
CREATE UNIQUE INDEX marketplace_installations_legacy ON marketplace_installations(extension_id) WHERE instance_id IS NULL;
CREATE INDEX marketplace_installations_extension ON marketplace_installations(extension_id);
CREATE TABLE marketplace_installation_metadata (
    extension_id TEXT PRIMARY KEY REFERENCES extensions(extension_id) ON DELETE RESTRICT,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE RESTRICT,
    metadata_sha256 TEXT NOT NULL CHECK (LENGTH(metadata_sha256)=71)
);
