-- Explicit owner consent, not catalog admission. One latest choice per
-- configured installation; the <=64-KiB publication and fixed row/index data
-- use that journal's existing 128-KiB ancillary reservation. No peer allocation.
CREATE TABLE marketplace_publisher_seeding (
    instance_id TEXT PRIMARY KEY REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    home_id TEXT NOT NULL,
    extension_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision>0 AND revision<=9007199254740991),
    mutation_id TEXT NOT NULL,
    request_id TEXT NOT NULL CHECK (LENGTH(request_id)=71),
    publication_json TEXT CHECK (publication_json IS NULL OR LENGTH(publication_json)<=65536),
    capsule_id TEXT CHECK (capsule_id IS NULL OR LENGTH(capsule_id)=71),
    approved_by_user_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    authorization_revision BIGINT NOT NULL CHECK (authorization_revision>=0),
    CHECK ((publication_json IS NULL)=(capsule_id IS NULL))
);
