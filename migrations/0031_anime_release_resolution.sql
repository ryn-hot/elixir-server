CREATE TABLE IF NOT EXISTS acquisition_anime_graph_snapshots (
    graph_snapshot_id TEXT PRIMARY KEY,
    subscription_id TEXT REFERENCES acquisition_subscriptions(subscription_id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL DEFAULT 'default',
    media_type TEXT NOT NULL DEFAULT 'anime',
    anilist_root_id INTEGER,
    anilist_season_id INTEGER,
    anilist_status TEXT,
    anilist_next_airing_at TIMESTAMP,
    tvdb_series_id INTEGER,
    anidb_anime_id INTEGER,
    fingerprint TEXT NOT NULL,
    graph_json TEXT NOT NULL,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_anime_graph_subscription_fingerprint
    ON acquisition_anime_graph_snapshots(subscription_id, fingerprint);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_anime_graph_owner_fingerprint
    ON acquisition_anime_graph_snapshots(owner_id, fingerprint)
    WHERE subscription_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_acquisition_anime_graph_anilist
    ON acquisition_anime_graph_snapshots(anilist_root_id, anilist_season_id);

CREATE TABLE IF NOT EXISTS acquisition_anime_candidate_parses (
    candidate_parse_id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES acquisition_releases(release_id) ON DELETE CASCADE,
    source_provider_id TEXT REFERENCES providers(provider_id) ON DELETE SET NULL,
    source_candidate_id TEXT,
    release_title TEXT NOT NULL,
    normalized_title TEXT,
    parsed_json TEXT NOT NULL,
    confidence TEXT NOT NULL,
    review_reasons_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_anime_candidate_release_source
    ON acquisition_anime_candidate_parses(release_id, source_candidate_id)
    WHERE source_candidate_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_anime_candidate_release_title
    ON acquisition_anime_candidate_parses(release_id, release_title)
    WHERE source_candidate_id IS NULL;

CREATE TABLE IF NOT EXISTS acquisition_file_hashes (
    file_hash_id TEXT PRIMARY KEY,
    release_file_id TEXT REFERENCES acquisition_release_files(release_file_id) ON DELETE SET NULL,
    local_file_id TEXT,
    file_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    mtime_fingerprint TEXT,
    ed2k TEXT,
    crc32 TEXT,
    hash_status TEXT NOT NULL,
    hash_computed_at TIMESTAMP,
    hash_invalidated_at TIMESTAMP,
    filename_history_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_file_hashes_path
    ON acquisition_file_hashes(file_path);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_file_hashes_local_file
    ON acquisition_file_hashes(local_file_id)
    WHERE local_file_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_acquisition_file_hashes_work
    ON acquisition_file_hashes(hash_status, updated_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_file_hashes_ed2k_size
    ON acquisition_file_hashes(ed2k, size_bytes);

CREATE TABLE IF NOT EXISTS acquisition_anidb_file_cache (
    lookup_key TEXT PRIMARY KEY,
    ed2k TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    lookup_status TEXT NOT NULL,
    anidb_file_id INTEGER,
    anidb_anime_id INTEGER,
    anidb_episode_ids_json TEXT NOT NULL DEFAULT '[]',
    anidb_group_id INTEGER,
    anidb_group_name TEXT,
    anidb_group_short_name TEXT,
    anidb_version INTEGER,
    anidb_source TEXT,
    anidb_quality TEXT,
    anidb_audio_languages_json TEXT NOT NULL DEFAULT '[]',
    anidb_subtitle_languages_json TEXT NOT NULL DEFAULT '[]',
    anidb_state_flags_json TEXT NOT NULL DEFAULT '[]',
    anidb_original_filename TEXT,
    released_at TIMESTAMP,
    raw_response TEXT,
    positive_cached_at TIMESTAMP,
    negative_cached_until TIMESTAMP,
    last_lookup_attempt_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_anidb_file_cache_ed2k_size
    ON acquisition_anidb_file_cache(ed2k, size_bytes);

CREATE INDEX IF NOT EXISTS idx_acquisition_anidb_file_cache_status
    ON acquisition_anidb_file_cache(lookup_status, updated_at);

CREATE TABLE IF NOT EXISTS acquisition_anidb_channel_state (
    channel TEXT PRIMARY KEY,
    banned_until TIMESTAMP,
    ban_reason TEXT,
    backoff_until TIMESTAMP,
    last_failure_reason TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    active_since TIMESTAMP,
    last_request_at TIMESTAMP,
    request_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_anidb_channel_state_banned
    ON acquisition_anidb_channel_state(channel, banned_until);

CREATE TABLE IF NOT EXISTS acquisition_anidb_file_xrefs (
    xref_id TEXT PRIMARY KEY,
    lookup_key TEXT NOT NULL REFERENCES acquisition_anidb_file_cache(lookup_key) ON DELETE CASCADE,
    release_file_id TEXT REFERENCES acquisition_release_files(release_file_id) ON DELETE SET NULL,
    anidb_file_id INTEGER,
    anidb_anime_id INTEGER NOT NULL,
    anidb_episode_id INTEGER NOT NULL,
    episode_type TEXT NOT NULL,
    percentage_start INTEGER NOT NULL DEFAULT 0,
    percentage_end INTEGER NOT NULL DEFAULT 100,
    episode_order INTEGER NOT NULL DEFAULT 0,
    provider TEXT NOT NULL,
    confidence TEXT NOT NULL,
    is_manual_override INTEGER NOT NULL DEFAULT 0,
    created_from_release_id TEXT REFERENCES acquisition_releases(release_id) ON DELETE SET NULL,
    created_from_target_id TEXT REFERENCES acquisition_targets(target_id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_anidb_file_xrefs_identity
    ON acquisition_anidb_file_xrefs(
        lookup_key,
        anidb_episode_id,
        percentage_start,
        percentage_end,
        episode_order
    );

CREATE INDEX IF NOT EXISTS idx_acquisition_anidb_file_xrefs_episode
    ON acquisition_anidb_file_xrefs(anidb_anime_id, anidb_episode_id);

CREATE TABLE IF NOT EXISTS acquisition_anime_match_attempts (
    match_attempt_id TEXT PRIMARY KEY,
    release_id TEXT REFERENCES acquisition_releases(release_id) ON DELETE SET NULL,
    release_file_id TEXT REFERENCES acquisition_release_files(release_file_id) ON DELETE SET NULL,
    attempted_providers_json TEXT NOT NULL DEFAULT '[]',
    selected_provider TEXT,
    ed2k TEXT,
    size_bytes INTEGER,
    candidate_fingerprint TEXT,
    planned_targets_json TEXT NOT NULL DEFAULT '[]',
    verified_targets_json TEXT NOT NULL DEFAULT '[]',
    outcome TEXT NOT NULL,
    rejection_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_anime_match_attempts_release
    ON acquisition_anime_match_attempts(release_id, created_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_anime_match_attempts_file
    ON acquisition_anime_match_attempts(release_file_id, created_at);

CREATE TABLE IF NOT EXISTS acquisition_anime_identity_mismatches (
    mismatch_id TEXT PRIMARY KEY,
    release_id TEXT REFERENCES acquisition_releases(release_id) ON DELETE SET NULL,
    release_file_id TEXT REFERENCES acquisition_release_files(release_file_id) ON DELETE SET NULL,
    target_id TEXT REFERENCES acquisition_targets(target_id) ON DELETE SET NULL,
    planned_target_json TEXT NOT NULL,
    verified_identity_json TEXT NOT NULL,
    provider TEXT NOT NULL,
    confidence TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_anime_identity_mismatches_release
    ON acquisition_anime_identity_mismatches(release_id, state, created_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_anime_identity_mismatches_target
    ON acquisition_anime_identity_mismatches(target_id, state);
