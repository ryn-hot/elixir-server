-- Library v2 schema (movies/series/seasons/episodes, linking tables, metadata scaffolding).

CREATE TABLE IF NOT EXISTS movies (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    year INTEGER,
    external_imdb TEXT,
    external_tmdb TEXT,
    metadata_json TEXT,
    runtime_seconds INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS series (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    year INTEGER,
    library_type TEXT NOT NULL,
    external_imdb TEXT,
    external_tvdb_series TEXT,
    external_anilist TEXT,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS seasons (
    id TEXT PRIMARY KEY,
    series_id TEXT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    season_number INTEGER NOT NULL,
    title TEXT,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series_id, season_number)
);

CREATE TABLE IF NOT EXISTS episodes (
    id TEXT PRIMARY KEY,
    series_id TEXT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    season_id TEXT NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    season_number INTEGER NOT NULL,
    episode_number INTEGER NOT NULL,
    absolute_episode_number INTEGER,
    title TEXT,
    runtime_seconds INTEGER,
    metadata_json TEXT,
    has_file BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series_id, season_number, episode_number)
);

CREATE TABLE IF NOT EXISTS movie_files (
    movie_id TEXT NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    media_file_id TEXT NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (movie_id, media_file_id)
);

CREATE TABLE IF NOT EXISTS episode_files (
    episode_id TEXT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    media_file_id TEXT NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (episode_id, media_file_id)
);

CREATE TABLE IF NOT EXISTS movie_external_ids (
    id TEXT PRIMARY KEY,
    movie_id TEXT NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    source TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(movie_id, provider, external_id)
);

CREATE TABLE IF NOT EXISTS series_external_ids (
    id TEXT PRIMARY KEY,
    series_id TEXT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    source TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series_id, provider, external_id)
);

CREATE TABLE IF NOT EXISTS episode_external_ids (
    id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    source TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(episode_id, provider, external_id)
);

CREATE TABLE IF NOT EXISTS anime_episode_meta (
    id TEXT PRIMARY KEY,
    season_id TEXT NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    episode_number INTEGER NOT NULL,
    title TEXT,
    snapshot_url TEXT,
    duration_seconds INTEGER,
    raw_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season_id, episode_number)
);

CREATE TABLE IF NOT EXISTS episode_provider_keys (
    id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(episode_id, provider)
);

CREATE TABLE IF NOT EXISTS artwork_refs (
    id TEXT PRIMARY KEY,
    owner_type TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    url TEXT NOT NULL,
    language TEXT,
    width INTEGER,
    height INTEGER,
    provider TEXT,
    score REAL,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS artwork_cache (
    id TEXT PRIMARY KEY,
    artwork_id TEXT NOT NULL REFERENCES artwork_refs(id) ON DELETE CASCADE,
    local_path TEXT NOT NULL,
    cached_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_tracks (
    id TEXT PRIMARY KEY,
    media_file_id TEXT NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    track_type TEXT NOT NULL,
    language TEXT,
    title TEXT,
    codec TEXT,
    channels INTEGER,
    is_default BOOLEAN NOT NULL DEFAULT 0,
    is_forced BOOLEAN NOT NULL DEFAULT 0,
    stream_index INTEGER,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS external_subtitles (
    id TEXT PRIMARY KEY,
    media_file_id TEXT NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    language TEXT,
    title TEXT,
    format TEXT,
    is_default BOOLEAN NOT NULL DEFAULT 0,
    is_forced BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(media_file_id, path)
);

CREATE TABLE IF NOT EXISTS review_queue (
    id TEXT PRIMARY KEY,
    media_file_id TEXT NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    confidence REAL,
    hint_json TEXT,
    candidates_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS classifier_overrides (
    id TEXT PRIMARY KEY,
    library_type TEXT NOT NULL,
    normalized_key TEXT NOT NULL,
    imdb_id TEXT,
    anilist_id TEXT,
    tvdb_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(library_type, normalized_key)
);

CREATE INDEX IF NOT EXISTS idx_movies_updated_at ON movies(updated_at);
CREATE INDEX IF NOT EXISTS idx_series_updated_at ON series(updated_at);
CREATE INDEX IF NOT EXISTS idx_seasons_series_id ON seasons(series_id);
CREATE INDEX IF NOT EXISTS idx_episodes_series_id ON episodes(series_id);
CREATE INDEX IF NOT EXISTS idx_episodes_season_id ON episodes(season_id);
CREATE INDEX IF NOT EXISTS idx_movie_files_media_file_id ON movie_files(media_file_id);
CREATE INDEX IF NOT EXISTS idx_episode_files_media_file_id ON episode_files(media_file_id);
CREATE INDEX IF NOT EXISTS idx_movie_external_ids_lookup ON movie_external_ids(provider, external_id);
CREATE INDEX IF NOT EXISTS idx_series_external_ids_lookup ON series_external_ids(provider, external_id);
CREATE INDEX IF NOT EXISTS idx_episode_external_ids_lookup ON episode_external_ids(provider, external_id);
CREATE INDEX IF NOT EXISTS idx_artwork_refs_owner ON artwork_refs(owner_type, owner_id);
CREATE INDEX IF NOT EXISTS idx_media_tracks_media_file_id ON media_tracks(media_file_id);
CREATE INDEX IF NOT EXISTS idx_external_subtitles_media_file_id ON external_subtitles(media_file_id);
CREATE INDEX IF NOT EXISTS idx_review_queue_status ON review_queue(status);
