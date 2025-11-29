-- Playback sessions table to track direct and transcode sessions.

CREATE TABLE IF NOT EXISTS playback_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    media_file_id TEXT NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    mode TEXT NOT NULL,
    network_type TEXT,
    client_capabilities TEXT,
    transcode_state TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_playback_sessions_media_file_id ON playback_sessions(media_file_id);
