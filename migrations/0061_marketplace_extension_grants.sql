-- Owner approvals are main-server state, never a disposable peer cache.
-- Revocation keeps its revision/tombstone; reapproval cannot revive old tokens.
CREATE TABLE IF NOT EXISTS marketplace_extension_grants (
    extension_id TEXT PRIMARY KEY CHECK (LENGTH(extension_id) BETWEEN 1 AND 128),
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL CHECK (revision > 0),
    enabled BOOLEAN NOT NULL CHECK (enabled IN (FALSE, TRUE)),
    automatic_updates BOOLEAN NOT NULL DEFAULT FALSE CHECK (automatic_updates IN (FALSE, TRUE)),
    review_json TEXT NOT NULL CHECK (LENGTH(review_json) BETWEEN 1 AND 16384),
    review_id TEXT NOT NULL CHECK (LENGTH(review_id) = 71),
    mutation_id TEXT NOT NULL,
    approved_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
