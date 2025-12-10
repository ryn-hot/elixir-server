-- Align playback_sessions with v1 spec: add server linkage, state, positions, duration.

ALTER TABLE playback_sessions
    ADD COLUMN server_id TEXT REFERENCES server_instances(id) ON DELETE SET NULL;

ALTER TABLE playback_sessions
    ADD COLUMN state TEXT NOT NULL DEFAULT 'active';

ALTER TABLE playback_sessions
    ADD COLUMN logical_position_seconds REAL NOT NULL DEFAULT 0;

ALTER TABLE playback_sessions
    ADD COLUMN duration_seconds INTEGER;
