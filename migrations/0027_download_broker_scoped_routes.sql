ALTER TABLE download_provider_bindings ADD COLUMN owner_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE download_provider_bindings ADD COLUMN category TEXT;
ALTER TABLE download_provider_bindings ADD COLUMN download_path TEXT;
ALTER TABLE download_provider_bindings ADD COLUMN allow_shared_path INTEGER NOT NULL DEFAULT 0;

DROP INDEX IF EXISTS idx_download_provider_bindings_logical_role;

CREATE UNIQUE INDEX IF NOT EXISTS idx_download_provider_bindings_logical_role_owner
    ON download_provider_bindings(logical_role, owner_id);
