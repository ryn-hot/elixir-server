CREATE TABLE IF NOT EXISTS acquisition_subscriptions (
    subscription_id TEXT PRIMARY KEY,
    media_type TEXT NOT NULL,
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    year INTEGER,
    external_ids_json TEXT,
    monitor_policy TEXT NOT NULL DEFAULT 'all_missing',
    route_policy TEXT NOT NULL DEFAULT 'debrid_first',
    source_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    release_delay_seconds INTEGER NOT NULL DEFAULT 0,
    quality_profile_json TEXT,
    metadata_refresh_after TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    candidate_search_after TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_metadata_refresh_at TIMESTAMP,
    last_candidate_search_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'active',
    active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_subscriptions_active_due_metadata
    ON acquisition_subscriptions(active, status, metadata_refresh_after);

CREATE INDEX IF NOT EXISTS idx_acquisition_subscriptions_active_due_candidates
    ON acquisition_subscriptions(active, status, candidate_search_after);

CREATE INDEX IF NOT EXISTS idx_acquisition_subscriptions_identity
    ON acquisition_subscriptions(media_type, normalized_title, year);

CREATE TABLE IF NOT EXISTS acquisition_targets (
    target_id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL REFERENCES acquisition_subscriptions(subscription_id) ON DELETE CASCADE,
    target_key TEXT NOT NULL,
    media_type TEXT NOT NULL,
    title TEXT NOT NULL,
    season_number INTEGER,
    episode_number INTEGER,
    absolute_episode_number INTEGER,
    air_date TEXT,
    air_time TIMESTAMP,
    metadata_json TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    state_reason TEXT,
    selected_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    selected_route_logical_id TEXT,
    selected_candidate_json TEXT,
    download_id TEXT,
    import_event_id TEXT,
    search_attempts INTEGER NOT NULL DEFAULT 0,
    last_search_at TIMESTAMP,
    next_search_after TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_targets_subscription_key
    ON acquisition_targets(subscription_id, target_key);

CREATE INDEX IF NOT EXISTS idx_acquisition_targets_subscription_state
    ON acquisition_targets(subscription_id, state);

CREATE INDEX IF NOT EXISTS idx_acquisition_targets_search_due
    ON acquisition_targets(state, next_search_after, air_time);
