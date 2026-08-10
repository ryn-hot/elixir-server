-- Full ani.zip responses are cached by AniList identity so anime episode
-- numbering can be resolved before library links are created. This cache is
-- global provider data and intentionally has no library-item foreign key.
CREATE TABLE IF NOT EXISTS anizip_mapping_cache (
    anilist_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    mapping_json TEXT NOT NULL,
    fetched_at_epoch_seconds BIGINT NOT NULL,
    updated_at_epoch_seconds BIGINT NOT NULL,
    CHECK(schema_version > 0),
    CHECK(fetched_at_epoch_seconds >= 0),
    CHECK(updated_at_epoch_seconds >= 0),
    CHECK(LENGTH(TRIM(anilist_id)) > 0),
    CHECK(LENGTH(mapping_json) > 0)
);

CREATE INDEX IF NOT EXISTS idx_anizip_mapping_cache_fetched_at
    ON anizip_mapping_cache(fetched_at_epoch_seconds);
