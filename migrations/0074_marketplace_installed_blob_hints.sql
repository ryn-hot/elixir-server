-- Derived lookup hints, NEVER source, grant or byte-serving authority. Each
-- configured installation contributes at most three fixed archive slots. Their
-- rows/index overhead fit inside its existing 128-KiB journal reservation.
CREATE TABLE marketplace_installed_blob_hints (
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    extension_id TEXT NOT NULL,
    home_id TEXT NOT NULL,
    sha256 TEXT NOT NULL CHECK (LENGTH(sha256)=71),
    size BIGINT NOT NULL CHECK (size>0 AND size<=536870912),
    PRIMARY KEY(instance_id,sha256)
);
CREATE INDEX marketplace_installed_blob_digest
    ON marketplace_installed_blob_hints(sha256,instance_id);
