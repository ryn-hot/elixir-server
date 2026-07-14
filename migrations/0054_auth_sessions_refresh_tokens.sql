-- Persist remembered device sessions and one-time rotating refresh-token families.

CREATE TABLE IF NOT EXISTS account_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    home_id TEXT REFERENCES homes(id) ON DELETE SET NULL,
    active_profile_id TEXT REFERENCES profiles(id) ON DELETE SET NULL,
    device_name TEXT,
    device_type TEXT,
    client_name TEXT,
    client_version TEXT,
    user_agent TEXT,
    ip_hash TEXT,
    remember_device BOOLEAN NOT NULL DEFAULT TRUE
        CHECK (remember_device IN (FALSE, TRUE)),
    csrf_revision INTEGER NOT NULL DEFAULT 1 CHECK (csrf_revision > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    recent_auth_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP,
    revoked_reason TEXT,
    CHECK (
        (revoked_at IS NULL AND revoked_reason IS NULL)
        OR (revoked_at IS NOT NULL AND revoked_reason IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES account_sessions(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE CHECK (LENGTH(token_hash) = 43),
    token_family TEXT NOT NULL,
    previous_token_id TEXT REFERENCES refresh_tokens(id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    used_at TIMESTAMP,
    revoked_at TIMESTAMP,
    replaced_by_token_id TEXT REFERENCES refresh_tokens(id) ON DELETE SET NULL,
    CHECK (replaced_by_token_id IS NULL OR used_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_account_sessions_user
    ON account_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_account_sessions_profile
    ON account_sessions(active_profile_id);
CREATE INDEX IF NOT EXISTS idx_account_sessions_active
    ON account_sessions(user_id, revoked_at, expires_at);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_session
    ON refresh_tokens(session_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family
    ON refresh_tokens(token_family);
