-- One bounded owner decision, not credentials, networking or registration.
-- Missing established state must not be recreated by an application request.
CREATE TABLE marketplace_provider_setup (
    singleton BIGINT PRIMARY KEY CHECK (singleton = 1),
    revision BIGINT NOT NULL CHECK (revision BETWEEN 0 AND 9007199254740991),
    home_id TEXT REFERENCES homes(id) ON DELETE RESTRICT,
    setup_json TEXT NOT NULL CHECK (LENGTH(setup_json) <= 2048),
    mutation_id TEXT,
    approved_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((revision = 0 AND home_id IS NULL AND setup_json = '' AND mutation_id IS NULL AND approved_by_user_id IS NULL)
        OR (revision > 0 AND home_id IS NOT NULL AND setup_json <> '' AND mutation_id IS NOT NULL))
);
INSERT INTO marketplace_provider_setup (singleton,revision,setup_json) VALUES (1,0,'');
