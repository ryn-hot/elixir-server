-- Server-owned preferences, separate from disposable catalog state and public
-- paid endorsements. Missing/corrupt established state must never imply consent.
CREATE TABLE IF NOT EXISTS marketplace_preferences (
    singleton BIGINT PRIMARY KEY CHECK (singleton = 1),
    revision BIGINT NOT NULL CHECK (revision BETWEEN 0 AND 9007199254740991),
    home_id TEXT REFERENCES homes(id) ON DELETE RESTRICT,
    preferences_json TEXT NOT NULL CHECK (LENGTH(preferences_json) <= 4096),
    mutation_id TEXT,
    approved_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((revision = 0 AND home_id IS NULL AND preferences_json = '' AND mutation_id IS NULL AND approved_by_user_id IS NULL)
        OR (revision > 0 AND home_id IS NOT NULL AND preferences_json <> '' AND mutation_id IS NOT NULL))
);
INSERT INTO marketplace_preferences (singleton,revision,preferences_json)
VALUES (1,0,'') ON CONFLICT(singleton) DO NOTHING;
