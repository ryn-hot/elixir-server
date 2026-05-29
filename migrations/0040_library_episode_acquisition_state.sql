CREATE TABLE IF NOT EXISTS library_episode_acquisition_state (
    episode_id TEXT PRIMARY KEY REFERENCES episodes(id) ON DELETE CASCADE,
    media_item_id TEXT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    season_id TEXT NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    target_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'queued',
            'searching',
            'downloading',
            'post_processing',
            'review_needed',
            'no_results',
            'failed',
            'imported'
        )
    ),
    reason_code TEXT,
    reason_message TEXT,
    source_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    source_provider_label TEXT,
    route_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    route_provider_label TEXT,
    subscription_id TEXT REFERENCES acquisition_subscriptions(subscription_id) ON DELETE SET NULL,
    target_id TEXT REFERENCES acquisition_targets(target_id) ON DELETE SET NULL,
    release_id TEXT,
    job_id TEXT,
    candidate_count INTEGER,
    selected_release_title TEXT,
    last_attempt_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_library_episode_acquisition_state_media
    ON library_episode_acquisition_state(media_item_id, season_id, state);

CREATE INDEX IF NOT EXISTS idx_library_episode_acquisition_state_subscription
    ON library_episode_acquisition_state(subscription_id, target_id);

CREATE INDEX IF NOT EXISTS idx_library_episode_acquisition_state_state
    ON library_episode_acquisition_state(state, updated_at);
