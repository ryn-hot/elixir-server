-- PostgreSQL can widen these checks in place. SQLite migration 0042 rebuilds
-- both tables because SQLite cannot alter an existing CHECK constraint.

ALTER TABLE extension_source_registries
    DROP CONSTRAINT extension_source_registries_registry_type_check;
ALTER TABLE extension_source_registries
    ADD CONSTRAINT extension_source_registries_registry_type_check
    CHECK (registry_type IN (
        'elixir_curated_cloudstream_pack',
        'cloudstream_repo_json',
        'cloudstream_plugins_json',
        'elixir_curated_nuvio_pack',
        'nuvio_manifest_json',
        'stremio_manifest_json'
    ));

ALTER TABLE extension_source_modules
    DROP CONSTRAINT extension_source_modules_ecosystem_check;
ALTER TABLE extension_source_modules
    ADD CONSTRAINT extension_source_modules_ecosystem_check
    CHECK (ecosystem IN ('cloudstream', 'aniyomi', 'nuvio', 'stremio'));
