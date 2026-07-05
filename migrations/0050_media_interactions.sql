-- Phase 23: server-owned media interactions.
--
-- This migration is intentionally additive around playback. Stream serving,
-- HLS generation, transcode planning, and ffmpeg job behavior are unchanged.

ALTER TABLE playback_sessions ADD COLUMN selected_item_type TEXT;
ALTER TABLE playback_sessions ADD COLUMN selected_item_id TEXT;
ALTER TABLE playback_sessions ADD COLUMN selected_series_id TEXT;
ALTER TABLE playback_sessions ADD COLUMN selected_season_id TEXT;
ALTER TABLE playback_sessions ADD COLUMN selected_episode_id TEXT;
ALTER TABLE playback_sessions ADD COLUMN playback_context_json TEXT;
ALTER TABLE playback_sessions ADD COLUMN last_progress_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_playback_sessions_selected_item
    ON playback_sessions(user_id, selected_item_type, selected_item_id);

CREATE TABLE IF NOT EXISTS user_media_state (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    item_id TEXT NOT NULL,
    media_file_id TEXT REFERENCES media_files(id) ON DELETE SET NULL,
    series_id TEXT REFERENCES series(id) ON DELETE SET NULL,
    season_id TEXT REFERENCES seasons(id) ON DELETE SET NULL,
    resume_seconds REAL NOT NULL DEFAULT 0,
    duration_seconds INTEGER,
    watched BOOLEAN NOT NULL DEFAULT 0,
    watched_at TIMESTAMP,
    play_count INTEGER NOT NULL DEFAULT 0,
    last_played_at TIMESTAMP,
    last_session_id TEXT REFERENCES playback_sessions(id) ON DELETE SET NULL,
    state_source TEXT NOT NULL DEFAULT 'playback',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, item_type, item_id)
);

CREATE INDEX IF NOT EXISTS idx_user_media_state_continue
    ON user_media_state(user_id, watched, last_played_at);

CREATE INDEX IF NOT EXISTS idx_user_media_state_series
    ON user_media_state(user_id, series_id, season_id, item_type, item_id);

CREATE TABLE IF NOT EXISTS playback_progress_events (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES playback_sessions(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    item_id TEXT NOT NULL,
    media_file_id TEXT REFERENCES media_files(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    position_seconds REAL NOT NULL,
    duration_seconds INTEGER,
    paused BOOLEAN NOT NULL DEFAULT 0,
    client_reported_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_playback_progress_events_session
    ON playback_progress_events(session_id, created_at);

CREATE TABLE IF NOT EXISTS media_file_fingerprints (
    media_file_id TEXT PRIMARY KEY REFERENCES media_files(id) ON DELETE CASCADE,
    duration_seconds INTEGER,
    file_size_bytes INTEGER,
    container TEXT,
    video_codec TEXT,
    audio_codec TEXT,
    video_frame_hash_json TEXT,
    audio_fingerprint_json TEXT,
    fingerprint_version TEXT NOT NULL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_segment_candidates (
    id TEXT PRIMARY KEY,
    media_file_id TEXT REFERENCES media_files(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    item_id TEXT NOT NULL,
    segment_type TEXT NOT NULL,
    start_seconds REAL NOT NULL,
    end_seconds REAL NOT NULL,
    provider_kind TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    provider_version TEXT,
    confidence REAL NOT NULL DEFAULT 0,
    validation_state TEXT NOT NULL DEFAULT 'pending',
    validation_reason TEXT,
    identity_strength TEXT NOT NULL DEFAULT 'unknown',
    source_payload_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_media_segment_candidates_item
    ON media_segment_candidates(item_type, item_id, segment_type);

CREATE INDEX IF NOT EXISTS idx_media_segment_candidates_file
    ON media_segment_candidates(media_file_id, segment_type);

CREATE TABLE IF NOT EXISTS media_segments (
    id TEXT PRIMARY KEY,
    media_file_id TEXT REFERENCES media_files(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    item_id TEXT NOT NULL,
    segment_type TEXT NOT NULL,
    start_seconds REAL NOT NULL,
    end_seconds REAL NOT NULL,
    canonical_candidate_id TEXT REFERENCES media_segment_candidates(id) ON DELETE SET NULL,
    source_label TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    locked BOOLEAN NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_media_segments_playback
    ON media_segments(media_file_id, status, start_seconds);

CREATE INDEX IF NOT EXISTS idx_media_segments_item
    ON media_segments(item_type, item_id, status, segment_type);

CREATE TABLE IF NOT EXISTS media_segment_jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    scope_type TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    provider_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 2,
    next_attempt_at TIMESTAMP,
    locked_by TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_media_segment_jobs_work
    ON media_segment_jobs(status, next_attempt_at, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_media_segment_jobs_scope
    ON media_segment_jobs(scope_type, scope_id, provider_kind);

CREATE TABLE IF NOT EXISTS media_segment_provider_rate_limits (
    provider_kind TEXT PRIMARY KEY,
    window_started_at TIMESTAMP NOT NULL,
    requests_in_window INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_segment_provider_cache (
    id TEXT PRIMARY KEY,
    media_file_id TEXT REFERENCES media_files(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    item_id TEXT NOT NULL,
    provider_kind TEXT NOT NULL,
    provider_cache_key TEXT NOT NULL,
    status TEXT NOT NULL,
    response_json TEXT,
    error_json TEXT,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(provider_kind, provider_cache_key)
);

CREATE INDEX IF NOT EXISTS idx_media_segment_provider_cache_file
    ON media_segment_provider_cache(media_file_id, provider_kind, status);

CREATE INDEX IF NOT EXISTS idx_media_segment_provider_cache_expiry
    ON media_segment_provider_cache(provider_kind, expires_at);

CREATE TABLE IF NOT EXISTS media_interaction_library_provider_settings (
    source_config_id TEXT NOT NULL REFERENCES source_configs(id) ON DELETE CASCADE,
    provider_kind TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    settings_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_config_id, provider_kind)
);

CREATE INDEX IF NOT EXISTS idx_media_interaction_library_provider_settings_provider
    ON media_interaction_library_provider_settings(provider_kind, enabled);

CREATE TABLE IF NOT EXISTS user_playback_preferences (
    user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    skip_intro_behavior TEXT NOT NULL DEFAULT 'prompt',
    skip_recap_behavior TEXT NOT NULL DEFAULT 'prompt',
    skip_preview_behavior TEXT NOT NULL DEFAULT 'prompt',
    skip_credits_behavior TEXT NOT NULL DEFAULT 'prompt',
    skip_outro_behavior TEXT NOT NULL DEFAULT 'prompt',
    autoplay_enabled BOOLEAN NOT NULL DEFAULT 1,
    autoplay_countdown_seconds INTEGER NOT NULL DEFAULT 10,
    autoplay_max_consecutive INTEGER NOT NULL DEFAULT 3,
    autoplay_max_elapsed_minutes INTEGER NOT NULL DEFAULT 180,
    segment_provider_settings_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_autoplay_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    series_id TEXT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_episode_id TEXT REFERENCES episodes(id) ON DELETE SET NULL,
    consecutive_count INTEGER NOT NULL DEFAULT 0,
    elapsed_autoplay_seconds INTEGER NOT NULL DEFAULT 0,
    last_progress_session_id TEXT REFERENCES playback_sessions(id) ON DELETE SET NULL,
    last_progress_position_seconds REAL,
    canceled BOOLEAN NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_autoplay_sessions_user_series
    ON user_autoplay_sessions(user_id, series_id, canceled, updated_at);

UPDATE playback_sessions
SET selected_item_type = 'movie',
    selected_item_id = (
        SELECT movie_id
        FROM movie_files
        WHERE movie_files.media_file_id = playback_sessions.media_file_id
        LIMIT 1
    )
WHERE selected_item_id IS NULL
  AND (
      SELECT COUNT(*)
      FROM movie_files
      WHERE movie_files.media_file_id = playback_sessions.media_file_id
  ) = 1
  AND NOT EXISTS (
      SELECT 1
      FROM episode_files
      WHERE episode_files.media_file_id = playback_sessions.media_file_id
  );

UPDATE playback_sessions
SET selected_item_type = 'episode',
    selected_item_id = (
        SELECT episode_id
        FROM episode_files
        WHERE episode_files.media_file_id = playback_sessions.media_file_id
        LIMIT 1
    ),
    selected_episode_id = (
        SELECT episode_id
        FROM episode_files
        WHERE episode_files.media_file_id = playback_sessions.media_file_id
        LIMIT 1
    ),
    selected_series_id = (
        SELECT episodes.series_id
        FROM episode_files
        JOIN episodes ON episodes.id = episode_files.episode_id
        WHERE episode_files.media_file_id = playback_sessions.media_file_id
        LIMIT 1
    ),
    selected_season_id = (
        SELECT episodes.season_id
        FROM episode_files
        JOIN episodes ON episodes.id = episode_files.episode_id
        WHERE episode_files.media_file_id = playback_sessions.media_file_id
        LIMIT 1
    )
WHERE selected_item_id IS NULL
  AND (
      SELECT COUNT(*)
      FROM episode_files
      WHERE episode_files.media_file_id = playback_sessions.media_file_id
  ) = 1
  AND NOT EXISTS (
      SELECT 1
      FROM movie_files
      WHERE movie_files.media_file_id = playback_sessions.media_file_id
  );

WITH session_candidates AS (
    SELECT
        ps.id AS session_id,
        ps.user_id AS user_id,
        ps.media_file_id AS media_file_id,
        'movie' AS item_type,
        mf.movie_id AS item_id,
        NULL AS series_id,
        NULL AS season_id,
        CASE
            WHEN ps.logical_position_seconds > 0 THEN ps.logical_position_seconds
            ELSE 0
        END AS position_seconds,
        COALESCE(ps.duration_seconds, m.runtime_seconds, 0) AS duration_seconds,
        ps.created_at AS created_at,
        ps.updated_at AS updated_at,
        600 AS remaining_threshold_seconds
    FROM playback_sessions ps
    JOIN movie_files mf ON mf.media_file_id = ps.media_file_id
    JOIN movies m ON m.id = mf.movie_id
    WHERE (
        SELECT COUNT(*)
        FROM movie_files
        WHERE movie_files.media_file_id = ps.media_file_id
    ) = 1
      AND NOT EXISTS (
          SELECT 1
          FROM episode_files
          WHERE episode_files.media_file_id = ps.media_file_id
      )
    UNION ALL
    SELECT
        ps.id AS session_id,
        ps.user_id AS user_id,
        ps.media_file_id AS media_file_id,
        'episode' AS item_type,
        ef.episode_id AS item_id,
        e.series_id AS series_id,
        e.season_id AS season_id,
        CASE
            WHEN ps.logical_position_seconds > 0 THEN ps.logical_position_seconds
            ELSE 0
        END AS position_seconds,
        COALESCE(ps.duration_seconds, e.runtime_seconds, 0) AS duration_seconds,
        ps.created_at AS created_at,
        ps.updated_at AS updated_at,
        180 AS remaining_threshold_seconds
    FROM playback_sessions ps
    JOIN episode_files ef ON ef.media_file_id = ps.media_file_id
    JOIN episodes e ON e.id = ef.episode_id
    WHERE (
        SELECT COUNT(*)
        FROM episode_files
        WHERE episode_files.media_file_id = ps.media_file_id
    ) = 1
      AND NOT EXISTS (
          SELECT 1
          FROM movie_files
          WHERE movie_files.media_file_id = ps.media_file_id
      )
),
classified_sessions AS (
    SELECT
        *,
        CASE
            WHEN duration_seconds > 0
                 AND (
                     position_seconds >= duration_seconds * 0.9
                     OR duration_seconds - position_seconds <= remaining_threshold_seconds
                 )
            THEN 1
            ELSE 0
        END AS completed
    FROM session_candidates
),
latest_sessions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, item_type, item_id
            ORDER BY updated_at DESC, created_at DESC, session_id DESC
        ) AS row_number
    FROM classified_sessions
),
session_rollups AS (
    SELECT
        user_id,
        item_type,
        item_id,
        SUM(completed) AS completed_count,
        MAX(CASE WHEN completed = 1 THEN updated_at ELSE NULL END) AS last_watched_at
    FROM classified_sessions
    GROUP BY user_id, item_type, item_id
)
INSERT INTO user_media_state (
    user_id,
    item_type,
    item_id,
    media_file_id,
    series_id,
    season_id,
    resume_seconds,
    duration_seconds,
    watched,
    watched_at,
    play_count,
    last_played_at,
    last_session_id,
    state_source
)
SELECT
    latest.user_id,
    latest.item_type,
    latest.item_id,
    latest.media_file_id,
    latest.series_id,
    latest.season_id,
    CASE
        WHEN rollup.completed_count > 0 THEN 0
        WHEN latest.position_seconds > 0 THEN latest.position_seconds
        ELSE 0
    END,
    CASE
        WHEN latest.duration_seconds > 0 THEN latest.duration_seconds
        ELSE NULL
    END,
    CASE
        WHEN rollup.completed_count > 0 THEN TRUE
        ELSE FALSE
    END,
    rollup.last_watched_at,
    rollup.completed_count,
    latest.updated_at,
    latest.session_id,
    'migration'
FROM latest_sessions latest
JOIN session_rollups rollup
  ON rollup.user_id = latest.user_id
 AND rollup.item_type = latest.item_type
 AND rollup.item_id = latest.item_id
WHERE latest.row_number = 1
ON CONFLICT(user_id, item_type, item_id) DO UPDATE SET
    media_file_id = excluded.media_file_id,
    series_id = excluded.series_id,
    season_id = excluded.season_id,
    resume_seconds = excluded.resume_seconds,
    duration_seconds = excluded.duration_seconds,
    watched = excluded.watched,
    watched_at = excluded.watched_at,
    play_count = excluded.play_count,
    last_played_at = excluded.last_played_at,
    last_session_id = excluded.last_session_id,
    state_source = excluded.state_source,
    updated_at = CURRENT_TIMESTAMP
WHERE user_media_state.state_source = 'migration';
