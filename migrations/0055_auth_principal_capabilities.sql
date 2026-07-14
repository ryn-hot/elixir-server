-- Add household authorization state and a durable generic revocation outbox.

CREATE TABLE IF NOT EXISTS library_sections (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    key TEXT NOT NULL CHECK (LENGTH(TRIM(key)) > 0),
    display_name TEXT NOT NULL CHECK (LENGTH(TRIM(display_name)) > 0),
    media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'series', 'anime')),
    source_config_id TEXT REFERENCES source_configs(id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(home_id, key)
);

CREATE TABLE IF NOT EXISTS library_grants (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    library_section_id TEXT NOT NULL REFERENCES library_sections(id) ON DELETE CASCADE,
    can_view BOOLEAN NOT NULL DEFAULT TRUE CHECK (can_view IN (FALSE, TRUE)),
    can_play BOOLEAN NOT NULL DEFAULT TRUE CHECK (can_play IN (FALSE, TRUE)),
    can_download BOOLEAN NOT NULL DEFAULT FALSE CHECK (can_download IN (FALSE, TRUE)),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(profile_id, library_section_id)
);

CREATE TABLE IF NOT EXISTS restriction_policies (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (LENGTH(TRIM(name)) > 0),
    preset TEXT NOT NULL CHECK (preset IN ('none', 'teen', 'kids', 'custom')),
    max_rating TEXT,
    blocked_labels_json TEXT,
    allowed_labels_json TEXT,
    allow_unrated BOOLEAN NOT NULL DEFAULT TRUE CHECK (allow_unrated IN (FALSE, TRUE)),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS home_invites (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    invited_email TEXT NOT NULL CHECK (LENGTH(TRIM(invited_email)) > 0),
    role TEXT NOT NULL CHECK (role IN ('admin', 'manager', 'viewer')),
    token_hash TEXT NOT NULL UNIQUE CHECK (LENGTH(TRIM(token_hash)) > 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'revoked', 'expired')),
    expires_at TIMESTAMP NOT NULL,
    accepted_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    accepted_at TIMESTAMP,
    created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS invite_library_grants (
    id TEXT PRIMARY KEY,
    invite_id TEXT NOT NULL REFERENCES home_invites(id) ON DELETE CASCADE,
    library_section_id TEXT NOT NULL REFERENCES library_sections(id) ON DELETE CASCADE,
    can_view BOOLEAN NOT NULL DEFAULT TRUE CHECK (can_view IN (FALSE, TRUE)),
    can_play BOOLEAN NOT NULL DEFAULT TRUE CHECK (can_play IN (FALSE, TRUE)),
    can_download BOOLEAN NOT NULL DEFAULT FALSE CHECK (can_download IN (FALSE, TRUE)),
    UNIQUE(invite_id, library_section_id)
);

CREATE TABLE IF NOT EXISTS profile_capability_overrides (
    profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    capability TEXT NOT NULL CHECK (capability IN (
        'server_admin',
        'users_manage',
        'sharing_manage',
        'devices_manage_own',
        'devices_manage_all',
        'library_read',
        'media_play',
        'media_delete',
        'library_scan',
        'review_queue_manage',
        'acquisition_request',
        'acquisition_manage',
        'extensions_view',
        'extensions_manage',
        'secrets_manage',
        'settings_view',
        'settings_manage',
        'live_browse',
        'live_play',
        'live_manage'
    )),
    allowed BOOLEAN NOT NULL CHECK (allowed IN (FALSE, TRUE)),
    created_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_by_actor_snapshot TEXT NOT NULL CHECK (LENGTH(TRIM(created_by_actor_snapshot)) > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(profile_id, capability)
);

CREATE TABLE IF NOT EXISTS profile_authorization_revisions (
    profile_id TEXT PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS authorization_revocation_outbox (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL CHECK (LENGTH(TRIM(home_id)) > 0),
    event_type TEXT NOT NULL CHECK (LENGTH(TRIM(event_type)) > 0),
    subject_type TEXT NOT NULL CHECK (LENGTH(TRIM(subject_type)) > 0),
    subject_id TEXT NOT NULL CHECK (LENGTH(TRIM(subject_id)) > 0),
    actor_user_id TEXT,
    account_session_id TEXT,
    profile_id TEXT,
    provider_id TEXT,
    grant_id TEXT,
    reason_code TEXT NOT NULL CHECK (LENGTH(TRIM(reason_code)) > 0),
    payload_json TEXT NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retain_until TIMESTAMP NOT NULL,
    published_at TIMESTAMP,
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    last_error_redacted TEXT
);

CREATE TABLE IF NOT EXISTS authorization_revocation_consumers (
    consumer_name TEXT PRIMARY KEY CHECK (LENGTH(TRIM(consumer_name)) > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS authorization_revocation_registry (
    singleton_key TEXT PRIMARY KEY CHECK (singleton_key = 'authorization-revocation-v1'),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS authorization_revocation_receipts (
    event_id TEXT NOT NULL REFERENCES authorization_revocation_outbox(id) ON DELETE CASCADE,
    consumer_name TEXT NOT NULL REFERENCES authorization_revocation_consumers(consumer_name) ON DELETE CASCADE,
    lease_owner TEXT,
    lease_expires_at TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    acknowledged_at TIMESTAMP,
    last_error_redacted TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    PRIMARY KEY(event_id, consumer_name)
);

CREATE INDEX IF NOT EXISTS idx_library_sections_home
    ON library_sections(home_id, key);
CREATE INDEX IF NOT EXISTS idx_library_grants_profile
    ON library_grants(profile_id, library_section_id);
CREATE INDEX IF NOT EXISTS idx_profile_capability_overrides_profile
    ON profile_capability_overrides(profile_id);
CREATE INDEX IF NOT EXISTS idx_profile_authorization_revisions_home
    ON profile_authorization_revisions(home_id);
CREATE INDEX IF NOT EXISTS idx_auth_revocation_outbox_unpublished
    ON authorization_revocation_outbox(published_at, occurred_at, id);
CREATE INDEX IF NOT EXISTS idx_auth_revocation_outbox_subject
    ON authorization_revocation_outbox(subject_type, subject_id, occurred_at, id);
CREATE INDEX IF NOT EXISTS idx_auth_revocation_outbox_retention
    ON authorization_revocation_outbox(retain_until, id);
CREATE INDEX IF NOT EXISTS idx_auth_revocation_receipts_pending
    ON authorization_revocation_receipts(consumer_name, acknowledged_at, lease_expires_at);

INSERT INTO library_sections (id, home_id, key, display_name, media_type)
SELECT id || ':movies', id, 'movies', 'Movies', 'movie' FROM homes
UNION ALL
SELECT id || ':series', id, 'series', 'Series', 'series' FROM homes
UNION ALL
SELECT id || ':anime', id, 'anime', 'Anime', 'anime' FROM homes
WHERE TRUE
ON CONFLICT(home_id, key) DO NOTHING;

INSERT INTO library_grants (
    id,
    profile_id,
    library_section_id,
    can_view,
    can_play,
    can_download
)
SELECT
    profiles.id || ':' || library_sections.key,
    profiles.id,
    library_sections.id,
    TRUE,
    TRUE,
    TRUE
FROM profiles
JOIN home_members
  ON home_members.home_id = profiles.home_id
 AND home_members.user_id = profiles.user_id
 AND home_members.role = 'owner'
 AND home_members.status = 'active'
JOIN library_sections ON library_sections.home_id = profiles.home_id
WHERE profiles.profile_type = 'account'
ON CONFLICT(profile_id, library_section_id) DO NOTHING;

INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
SELECT id, home_id, 1 FROM profiles
WHERE TRUE
ON CONFLICT(profile_id) DO NOTHING;

INSERT INTO authorization_revocation_registry (singleton_key, revision)
VALUES ('authorization-revocation-v1', 1)
ON CONFLICT(singleton_key) DO NOTHING;
