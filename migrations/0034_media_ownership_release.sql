CREATE TABLE IF NOT EXISTS media_ownerships (
    ownership_id TEXT PRIMARY KEY,
    media_item_id TEXT NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
    owner_type TEXT NOT NULL CHECK (owner_type IN ('external', 'extension', 'acquisition', 'system')),
    owner_role TEXT NOT NULL DEFAULT 'primary',
    owner_label TEXT,
    owner_implementation TEXT,
    owner_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    owner_instance_id TEXT REFERENCES extension_instances(instance_id) ON DELETE SET NULL,
    owner_extension_id TEXT,
    owner_external_id TEXT,
    acquisition_subscription_id TEXT REFERENCES acquisition_subscriptions(subscription_id) ON DELETE SET NULL,
    acquisition_target_scope_json TEXT,
    release_capability TEXT NOT NULL DEFAULT 'none',
    release_policy TEXT NOT NULL DEFAULT 'unsupported',
    metadata_json TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_media_ownerships_item_active
    ON media_ownerships(media_item_id, active);

CREATE INDEX IF NOT EXISTS idx_media_ownerships_owner_active
    ON media_ownerships(owner_type, active);

CREATE INDEX IF NOT EXISTS idx_media_ownerships_provider_external
    ON media_ownerships(owner_provider_id, owner_external_id, active);

CREATE INDEX IF NOT EXISTS idx_media_ownerships_acquisition_subscription
    ON media_ownerships(acquisition_subscription_id, active);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_ownerships_primary_active
    ON media_ownerships(media_item_id, owner_role)
    WHERE active = 1 AND owner_role = 'primary';

CREATE TABLE IF NOT EXISTS media_owner_release_events (
    release_event_id TEXT PRIMARY KEY,
    media_item_id TEXT REFERENCES media_items(id) ON DELETE SET NULL,
    ownership_id TEXT REFERENCES media_ownerships(ownership_id) ON DELETE SET NULL,
    requested_action TEXT NOT NULL,
    owner_type TEXT NOT NULL,
    owner_label TEXT,
    owner_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    acquisition_subscription_id TEXT REFERENCES acquisition_subscriptions(subscription_id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'unsupported', 'skipped', 'failed', 'rolled_back_local_delete')),
    status_reason TEXT,
    request_json TEXT,
    response_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_media_owner_release_events_media
    ON media_owner_release_events(media_item_id, created_at);

CREATE INDEX IF NOT EXISTS idx_media_owner_release_events_ownership
    ON media_owner_release_events(ownership_id, created_at);

INSERT INTO media_ownerships (
    ownership_id,
    media_item_id,
    owner_type,
    owner_role,
    owner_label,
    owner_implementation,
    owner_provider_id,
    owner_instance_id,
    owner_extension_id,
    owner_external_id,
    acquisition_subscription_id,
    acquisition_target_scope_json,
    release_capability,
    release_policy,
    metadata_json,
    active
)
SELECT
    mlp.media_item_id,
    mlp.media_item_id,
    'extension',
    'primary',
    COALESCE(mlp.manager_label, mlp.manager_implementation, 'Managed extension'),
    mlp.manager_implementation,
    mlp.manager_provider_id,
    p.instance_id,
    ei.extension_id,
    mlp.manager_item_id,
    NULL,
    NULL,
    CASE
        WHEN lower(COALESCE(mlp.manager_implementation, p.implementation, '')) IN ('sonarr', 'radarr')
        THEN 'manager.remove_item'
        ELSE 'none'
    END,
    CASE
        WHEN lower(COALESCE(mlp.manager_implementation, p.implementation, '')) IN ('sonarr', 'radarr')
        THEN 'supported'
        ELSE 'unsupported'
    END,
    NULL,
    1
FROM managed_library_provenance mlp
LEFT JOIN providers p ON p.provider_id = mlp.manager_provider_id
LEFT JOIN extension_instances ei ON ei.instance_id = p.instance_id
WHERE 1 = 1
ON CONFLICT(ownership_id) DO UPDATE SET
    owner_type = excluded.owner_type,
    owner_role = excluded.owner_role,
    owner_label = excluded.owner_label,
    owner_implementation = excluded.owner_implementation,
    owner_provider_id = excluded.owner_provider_id,
    owner_instance_id = excluded.owner_instance_id,
    owner_extension_id = excluded.owner_extension_id,
    owner_external_id = excluded.owner_external_id,
    acquisition_subscription_id = excluded.acquisition_subscription_id,
    acquisition_target_scope_json = excluded.acquisition_target_scope_json,
    release_capability = excluded.release_capability,
    release_policy = excluded.release_policy,
    metadata_json = excluded.metadata_json,
    active = 1,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO media_ownerships (
    ownership_id,
    media_item_id,
    owner_type,
    owner_role,
    owner_label,
    owner_implementation,
    owner_provider_id,
    owner_instance_id,
    owner_extension_id,
    owner_external_id,
    acquisition_subscription_id,
    acquisition_target_scope_json,
    release_capability,
    release_policy,
    metadata_json,
    active
)
SELECT
    acquired.media_item_id,
    acquired.media_item_id,
    'acquisition',
    'primary',
    'Elixir acquisition',
    acquired.source_extension_id,
    acquired.source_provider_id,
    acquired.instance_id,
    acquired.extension_id,
    acquired.subscription_id,
    acquired.subscription_id,
    NULL,
    'acquisition.stop_monitoring',
    'supported',
    NULL,
    1
FROM (
    SELECT
        COALESCE(mf.media_item_id, ail.movie_id, e.series_id) AS media_item_id,
        r.subscription_id,
        MIN(r.source_provider_id) AS source_provider_id,
        MIN(p.instance_id) AS instance_id,
        MIN(ei.extension_id) AS extension_id,
        MIN(r.source_extension_id) AS source_extension_id
    FROM acquisition_import_file_links ail
    JOIN acquisition_releases r ON r.release_id = ail.release_id
    LEFT JOIN media_files mf ON mf.id = ail.media_file_id
    LEFT JOIN episodes e ON e.id = ail.episode_id
    LEFT JOIN providers p ON p.provider_id = r.source_provider_id
    LEFT JOIN extension_instances ei ON ei.instance_id = p.instance_id
    WHERE ail.state = 'imported'
      AND r.subscription_id IS NOT NULL
      AND COALESCE(mf.media_item_id, ail.movie_id, e.series_id) IS NOT NULL
    GROUP BY COALESCE(mf.media_item_id, ail.movie_id, e.series_id), r.subscription_id
) acquired
WHERE NOT EXISTS (
    SELECT 1
    FROM media_ownerships existing
    WHERE existing.media_item_id = acquired.media_item_id
      AND existing.owner_role = 'primary'
      AND existing.active = 1
)
ON CONFLICT(ownership_id) DO NOTHING;

INSERT INTO media_ownerships (
    ownership_id,
    media_item_id,
    owner_type,
    owner_role,
    owner_label,
    release_capability,
    release_policy,
    active
)
SELECT
    mi.id,
    mi.id,
    'external',
    'primary',
    'External import',
    'none',
    'unsupported',
    1
FROM media_items mi
WHERE NOT EXISTS (
    SELECT 1
    FROM media_ownerships existing
    WHERE existing.media_item_id = mi.id
      AND existing.owner_role = 'primary'
      AND existing.active = 1
)
ON CONFLICT(ownership_id) DO NOTHING;
