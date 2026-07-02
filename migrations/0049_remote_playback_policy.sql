-- Phase 22: remote playback token lifetime and policy snapshots.

ALTER TABLE playback_sessions ADD COLUMN token_expires_at TIMESTAMP;
ALTER TABLE playback_sessions ADD COLUMN share_id TEXT;
ALTER TABLE playback_sessions ADD COLUMN remote_policy_json TEXT;

CREATE INDEX IF NOT EXISTS idx_playback_sessions_share_state
    ON playback_sessions(share_id, state);
