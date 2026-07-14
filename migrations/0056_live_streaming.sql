-- Standalone Live streaming persistence, authorization, audit, and fenced ownership.

CREATE TABLE IF NOT EXISTS live_provider_cache (
    cache_key TEXT PRIMARY KEY CHECK (LENGTH(cache_key) BETWEEN 16 AND 128),
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    operation TEXT NOT NULL CHECK (operation IN ('catalogs', 'catalog', 'meta')),
    payload_json TEXT NOT NULL CHECK (LENGTH(payload_json) <= 2097152),
    etag TEXT CHECK (etag IS NULL OR LENGTH(etag) <= 512),
    fresh_until TIMESTAMP NOT NULL,
    stale_until TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (fresh_until <= stale_until)
);

CREATE TABLE IF NOT EXISTS live_provider_grants (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    can_browse BOOLEAN NOT NULL DEFAULT FALSE CHECK (can_browse IN (FALSE, TRUE)),
    can_play BOOLEAN NOT NULL DEFAULT FALSE CHECK (can_play IN (FALSE, TRUE)),
    created_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_by_actor_snapshot TEXT NOT NULL
        CHECK (LENGTH(TRIM(created_by_actor_snapshot)) BETWEEN 1 AND 4096),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(profile_id, provider_id),
    CHECK (can_play = FALSE OR can_browse = TRUE)
);

CREATE TABLE IF NOT EXISTS live_provider_admin_state (
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    provider_revision BIGINT NOT NULL DEFAULT 1 CHECK (provider_revision > 0),
    grant_revision BIGINT NOT NULL DEFAULT 1 CHECK (grant_revision > 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(home_id, provider_id)
);

CREATE TABLE IF NOT EXISTS live_provider_destination_rules (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    scheme TEXT NOT NULL CHECK (scheme IN ('http', 'https', 'rtmp', 'srt')),
    normalized_host TEXT NOT NULL
        CHECK (
            LENGTH(normalized_host) BETWEEN 1 AND 253
            AND normalized_host = LOWER(TRIM(normalized_host))
            AND normalized_host NOT GLOB '*[^a-z0-9.:-]*'
            AND normalized_host NOT LIKE '%*%'
            AND normalized_host NOT LIKE '%?%'
            AND normalized_host NOT LIKE '%/%'
            AND normalized_host NOT LIKE '%#%'
            AND normalized_host NOT LIKE '%@%'
        ),
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    exact_path TEXT NOT NULL
        CHECK (
            LENGTH(exact_path) BETWEEN 1 AND 2048
            AND SUBSTR(exact_path, 1, 1) = '/'
            AND exact_path NOT LIKE '%*%'
            AND exact_path NOT LIKE '%?%'
            AND exact_path NOT LIKE '%#%'
            AND exact_path NOT LIKE '% %'
        ),
    network_scope TEXT NOT NULL CHECK (network_scope IN ('public', 'private_lan')),
    allow_fetch BOOLEAN NOT NULL DEFAULT FALSE CHECK (allow_fetch IN (FALSE, TRUE)),
    allow_credentials BOOLEAN NOT NULL DEFAULT FALSE CHECK (allow_credentials IN (FALSE, TRUE)),
    allow_client_disclosure BOOLEAN NOT NULL DEFAULT FALSE
        CHECK (allow_client_disclosure IN (FALSE, TRUE)),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_by_actor_snapshot TEXT NOT NULL
        CHECK (LENGTH(TRIM(created_by_actor_snapshot)) BETWEEN 1 AND 4096),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(home_id, provider_id, scheme, normalized_host, port, exact_path, network_scope),
    CHECK (
        scheme NOT IN ('rtmp', 'srt')
        OR (allow_fetch = TRUE AND allow_credentials = FALSE AND allow_client_disclosure = FALSE)
    ),
    CHECK (
        allow_client_disclosure = FALSE
        OR (
            scheme = 'https'
            AND network_scope = 'public'
            AND allow_fetch = TRUE
            AND allow_credentials = FALSE
        )
    ),
    CHECK (network_scope != 'private_lan' OR allow_client_disclosure = FALSE)
);

CREATE TABLE IF NOT EXISTS live_admin_audit_events (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL CHECK (LENGTH(TRIM(home_id)) > 0),
    action TEXT NOT NULL CHECK (LENGTH(TRIM(action)) BETWEEN 1 AND 128),
    target_type TEXT NOT NULL CHECK (LENGTH(TRIM(target_type)) BETWEEN 1 AND 64),
    target_id TEXT NOT NULL CHECK (LENGTH(TRIM(target_id)) BETWEEN 1 AND 512),
    actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    actor_snapshot_json TEXT NOT NULL
        CHECK (LENGTH(TRIM(actor_snapshot_json)) BETWEEN 2 AND 4096),
    before_json TEXT CHECK (before_json IS NULL OR LENGTH(before_json) <= 65536),
    after_json TEXT CHECK (after_json IS NULL OR LENGTH(after_json) <= 65536),
    tombstone_json TEXT CHECK (tombstone_json IS NULL OR LENGTH(tombstone_json) <= 65536),
    audit_key_id TEXT NOT NULL
        CHECK (
            LENGTH(audit_key_id) BETWEEN 1 AND 32
            AND audit_key_id NOT GLOB '*[^A-Za-z0-9_-]*'
        ),
    previous_hash TEXT CHECK (
        previous_hash IS NULL
        OR (
            LENGTH(previous_hash) = 64
            AND previous_hash NOT GLOB '*[^0-9a-f]*'
        )
    ),
    record_hash TEXT NOT NULL CHECK (
        LENGTH(record_hash) = 64
        AND record_hash NOT GLOB '*[^0-9a-f]*'
    ),
    occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retain_until TIMESTAMP NOT NULL,
    UNIQUE(home_id, record_hash),
    CHECK (retain_until > occurred_at),
    CHECK (before_json IS NOT NULL OR after_json IS NOT NULL OR tombstone_json IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS live_admin_audit_chain_heads (
    home_id TEXT PRIMARY KEY,
    last_record_hash TEXT CHECK (
        last_record_hash IS NULL
        OR (
            LENGTH(last_record_hash) = 64
            AND last_record_hash NOT GLOB '*[^0-9a-f]*'
        )
    ),
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS live_key_rotation_state (
    state_id TEXT PRIMARY KEY CHECK (state_id = 'live-crypto-v1'),
    envelope_primary_key_id TEXT NOT NULL
        CHECK (
            LENGTH(envelope_primary_key_id) BETWEEN 1 AND 32
            AND envelope_primary_key_id NOT GLOB '*[^A-Za-z0-9_-]*'
        ),
    token_hash_primary_key_id TEXT NOT NULL
        CHECK (
            LENGTH(token_hash_primary_key_id) BETWEEN 1 AND 32
            AND token_hash_primary_key_id NOT GLOB '*[^A-Za-z0-9_-]*'
        ),
    audit_primary_key_id TEXT NOT NULL
        CHECK (
            LENGTH(audit_primary_key_id) BETWEEN 1 AND 32
            AND audit_primary_key_id NOT GLOB '*[^A-Za-z0-9_-]*'
        ),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS live_playback_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    account_session_id TEXT NOT NULL REFERENCES account_sessions(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    item_key_hash TEXT NOT NULL CHECK (LENGTH(item_key_hash) BETWEEN 32 AND 128),
    stream_option_key_hash TEXT NOT NULL CHECK (LENGTH(stream_option_key_hash) BETWEEN 32 AND 128),
    encrypted_item_snapshot TEXT NOT NULL CHECK (encrypted_item_snapshot LIKE 'elx-live:v1:%'),
    delivery_mode TEXT NOT NULL
        CHECK (delivery_mode IN ('client_direct', 'server_relay', 'server_remux')),
    protocol TEXT NOT NULL
        CHECK (protocol IN ('hls', 'dash', 'http_progressive', 'mpeg_ts', 'rtmp', 'srt')),
    state TEXT NOT NULL CHECK (state IN (
        'resolving', 'planning', 'provisioning_egress', 'starting_remux',
        'ready', 'playing', 'reconnecting', 'refreshing', 'failing_over',
        'ended', 'expired', 'failed'
    )),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    token_revision BIGINT NOT NULL DEFAULT 1 CHECK (token_revision > 0),
    control_fencing_token BIGINT NOT NULL CHECK (control_fencing_token > 0),
    token_hash TEXT NOT NULL CHECK (token_hash LIKE 'elx-live-token-hash:v1:%'),
    encrypted_descriptor TEXT NOT NULL CHECK (encrypted_descriptor LIKE 'elx-live:v1:%'),
    source_index INTEGER NOT NULL DEFAULT 0 CHECK (source_index >= 0),
    failover_count INTEGER NOT NULL DEFAULT 0 CHECK (failover_count >= 0),
    refresh_count INTEGER NOT NULL DEFAULT 0 CHECK (refresh_count >= 0),
    egress_binding_id TEXT,
    remux_job_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    hard_expires_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    error_code TEXT CHECK (error_code IS NULL OR LENGTH(error_code) <= 128),
    error_detail_redacted TEXT CHECK (error_detail_redacted IS NULL OR LENGTH(error_detail_redacted) <= 4096),
    CHECK (expires_at <= hard_expires_at),
    CHECK (
        (state IN ('ended', 'expired', 'failed') AND ended_at IS NOT NULL)
        OR (state NOT IN ('ended', 'expired', 'failed') AND ended_at IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS live_track_preferences (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    audio_track_id TEXT CHECK (audio_track_id IS NULL OR LENGTH(audio_track_id) BETWEEN 1 AND 256),
    audio_language TEXT CHECK (audio_language IS NULL OR LENGTH(audio_language) BETWEEN 1 AND 64),
    audio_title TEXT CHECK (audio_title IS NULL OR LENGTH(audio_title) BETWEEN 1 AND 256),
    subtitle_track_id TEXT CHECK (subtitle_track_id IS NULL OR LENGTH(subtitle_track_id) BETWEEN 1 AND 256),
    subtitle_language TEXT CHECK (subtitle_language IS NULL OR LENGTH(subtitle_language) BETWEEN 1 AND 64),
    subtitle_title TEXT CHECK (subtitle_title IS NULL OR LENGTH(subtitle_title) BETWEEN 1 AND 256),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(user_id, provider_id),
    CHECK (audio_track_id IS NOT NULL OR subtitle_track_id IS NOT NULL),
    CHECK (audio_track_id IS NOT NULL OR (audio_language IS NULL AND audio_title IS NULL)),
    CHECK (subtitle_track_id IS NOT NULL OR (subtitle_language IS NULL AND subtitle_title IS NULL))
);

CREATE TABLE IF NOT EXISTS live_session_idempotency (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    idempotency_key_hash TEXT NOT NULL CHECK (LENGTH(idempotency_key_hash) BETWEEN 32 AND 128),
    request_hash TEXT NOT NULL CHECK (LENGTH(request_hash) BETWEEN 32 AND 128),
    session_id TEXT NOT NULL REFERENCES live_playback_sessions(id) ON DELETE CASCADE,
    encrypted_response TEXT NOT NULL CHECK (encrypted_response LIKE 'elx-live:v1:%'),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    PRIMARY KEY(user_id, profile_id, idempotency_key_hash),
    CHECK (expires_at > created_at)
);

CREATE TABLE IF NOT EXISTS live_control_server_leases (
    lease_name TEXT PRIMARY KEY CHECK (lease_name = 'live-control-v1'),
    owner_instance_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    acquired_at TIMESTAMP,
    heartbeat_at TIMESTAMP,
    expires_at TIMESTAMP,
    CHECK (
        (owner_instance_id IS NULL AND acquired_at IS NULL AND heartbeat_at IS NULL AND expires_at IS NULL)
        OR (owner_instance_id IS NOT NULL AND acquired_at IS NOT NULL AND heartbeat_at IS NOT NULL AND expires_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS live_egress_policy_assignments (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('server_default', 'profile', 'provider')),
    scope_key TEXT NOT NULL CHECK (LENGTH(TRIM(scope_key)) BETWEEN 1 AND 128),
    profile_id TEXT REFERENCES profiles(id) ON DELETE CASCADE,
    provider_id TEXT REFERENCES providers(provider_id) ON DELETE CASCADE,
    mode TEXT NOT NULL CHECK (mode IN ('off', 'prefer_protected', 'require_protected')),
    policy_id TEXT CHECK (policy_id IS NULL OR LENGTH(TRIM(policy_id)) BETWEEN 1 AND 128),
    allow_fallback BOOLEAN NOT NULL DEFAULT FALSE CHECK (allow_fallback IN (FALSE, TRUE)),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(home_id, scope_type, scope_key),
    CHECK (
        (scope_type = 'server_default' AND scope_key = 'server' AND profile_id IS NULL AND provider_id IS NULL)
        OR (scope_type = 'profile' AND scope_key = profile_id AND profile_id IS NOT NULL AND provider_id IS NULL)
        OR (scope_type = 'provider' AND scope_key = provider_id AND profile_id IS NULL AND provider_id IS NOT NULL)
    ),
    CHECK (
        (mode = 'off' AND policy_id IS NULL AND allow_fallback = FALSE)
        OR (mode = 'prefer_protected' AND policy_id IS NOT NULL)
        OR (mode = 'require_protected' AND policy_id IS NOT NULL AND allow_fallback = FALSE)
    )
);

CREATE TABLE IF NOT EXISTS live_egress_bindings (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE REFERENCES live_playback_sessions(id) ON DELETE CASCADE,
    policy_id TEXT NOT NULL CHECK (LENGTH(TRIM(policy_id)) BETWEEN 1 AND 128),
    mode TEXT NOT NULL CHECK (mode IN ('warp', 'wireguard', 'openvpn')),
    gateway_instance_id TEXT,
    worker_instance_id TEXT,
    gateway_container_name TEXT NOT NULL CHECK (LENGTH(TRIM(gateway_container_name)) BETWEEN 1 AND 128),
    worker_container_name TEXT NOT NULL CHECK (LENGTH(TRIM(worker_container_name)) BETWEEN 1 AND 128),
    state TEXT NOT NULL CHECK (state IN ('provisioning', 'ready', 'releasing', 'released', 'failed')),
    control_fencing_token BIGINT NOT NULL CHECK (control_fencing_token > 0),
    policy_revision BIGINT NOT NULL CHECK (policy_revision > 0),
    failure_reason_redacted TEXT
        CHECK (failure_reason_redacted IS NULL OR LENGTH(failure_reason_redacted) <= 4096),
    readiness_json TEXT CHECK (readiness_json IS NULL OR LENGTH(readiness_json) <= 16384),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ready_at TIMESTAMP,
    last_health_at TIMESTAMP,
    released_at TIMESTAMP,
    CHECK (state != 'ready' OR ready_at IS NOT NULL),
    CHECK (state NOT IN ('released', 'failed') OR released_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_live_provider_cache_expiry
    ON live_provider_cache(provider_id, operation, fresh_until, stale_until);
CREATE UNIQUE INDEX IF NOT EXISTS idx_live_crypto_global_secret_key
    ON secrets(key)
    WHERE scope = 'global' AND scope_id IS NULL AND key LIKE 'live.crypto.%';
CREATE INDEX IF NOT EXISTS idx_live_provider_grants_profile
    ON live_provider_grants(profile_id, provider_id);
CREATE INDEX IF NOT EXISTS idx_live_provider_grants_provider
    ON live_provider_grants(provider_id, profile_id);
CREATE INDEX IF NOT EXISTS idx_live_provider_admin_state_provider
    ON live_provider_admin_state(provider_id, home_id);
CREATE INDEX IF NOT EXISTS idx_live_destination_rules_lookup
    ON live_provider_destination_rules(home_id, provider_id, scheme, normalized_host, port);
CREATE INDEX IF NOT EXISTS idx_live_admin_audit_home_chain
    ON live_admin_audit_events(home_id, occurred_at, id);
CREATE INDEX IF NOT EXISTS idx_live_admin_audit_retention
    ON live_admin_audit_events(retain_until, id);
CREATE INDEX IF NOT EXISTS idx_live_sessions_owner
    ON live_playback_sessions(user_id, profile_id, state, expires_at);
CREATE INDEX IF NOT EXISTS idx_live_sessions_account_session
    ON live_playback_sessions(account_session_id, state);
CREATE INDEX IF NOT EXISTS idx_live_sessions_provider
    ON live_playback_sessions(provider_id, state);
CREATE INDEX IF NOT EXISTS idx_live_sessions_fence
    ON live_playback_sessions(control_fencing_token, state);
CREATE INDEX IF NOT EXISTS idx_live_idempotency_expiry
    ON live_session_idempotency(expires_at, session_id);
CREATE INDEX IF NOT EXISTS idx_live_egress_state
    ON live_egress_bindings(state, control_fencing_token);
CREATE INDEX IF NOT EXISTS idx_live_egress_assignments_lookup
    ON live_egress_policy_assignments(home_id, scope_type, scope_key);

CREATE TRIGGER IF NOT EXISTS trg_live_destination_rule_guard
BEFORE UPDATE ON live_provider_destination_rules
WHEN NEW.id IS NOT OLD.id
  OR NEW.home_id IS NOT OLD.home_id
  OR NEW.provider_id IS NOT OLD.provider_id
  OR NEW.created_by_user_id IS NOT OLD.created_by_user_id
  OR NEW.created_by_actor_snapshot IS NOT OLD.created_by_actor_snapshot
  OR NEW.created_at IS NOT OLD.created_at
  OR NEW.revision != OLD.revision + 1
BEGIN
    SELECT RAISE(ABORT, 'live destination-rule owner fields are immutable and revision must increment once');
END;

CREATE TRIGGER IF NOT EXISTS trg_live_admin_audit_no_update
BEFORE UPDATE ON live_admin_audit_events
BEGIN
    SELECT RAISE(ABORT, 'live admin audit events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_live_admin_audit_retention
BEFORE DELETE ON live_admin_audit_events
WHEN OLD.retain_until > CURRENT_TIMESTAMP
BEGIN
    SELECT RAISE(ABORT, 'live admin audit event is still retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_live_session_ownership_immutable
BEFORE UPDATE ON live_playback_sessions
WHEN NEW.id IS NOT OLD.id
  OR NEW.user_id IS NOT OLD.user_id
  OR NEW.home_id IS NOT OLD.home_id
  OR NEW.profile_id IS NOT OLD.profile_id
  OR NEW.account_session_id IS NOT OLD.account_session_id
  OR NEW.provider_id IS NOT OLD.provider_id
  OR NEW.item_key_hash IS NOT OLD.item_key_hash
  OR NEW.stream_option_key_hash IS NOT OLD.stream_option_key_hash
  OR NEW.created_at IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'live session ownership is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_live_control_lease_guard
BEFORE UPDATE ON live_control_server_leases
WHEN NEW.lease_name IS NOT OLD.lease_name
  OR NEW.fencing_token < OLD.fencing_token
  OR NEW.fencing_token > OLD.fencing_token + 1
  OR (
      NEW.owner_instance_id IS NOT OLD.owner_instance_id
      AND NEW.owner_instance_id IS NOT NULL
      AND NEW.fencing_token != OLD.fencing_token + 1
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid live control lease mutation');
END;

CREATE TRIGGER IF NOT EXISTS trg_live_control_lease_no_delete
BEFORE DELETE ON live_control_server_leases
BEGIN
    SELECT RAISE(ABORT, 'live control lease row is permanent');
END;

INSERT INTO live_control_server_leases (lease_name, fencing_token)
VALUES ('live-control-v1', 0)
ON CONFLICT(lease_name) DO NOTHING;
