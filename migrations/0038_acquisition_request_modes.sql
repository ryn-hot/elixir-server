ALTER TABLE acquisition_subscriptions
    ADD COLUMN request_mode TEXT NOT NULL DEFAULT 'monitored';

ALTER TABLE acquisition_subscriptions
    ADD COLUMN request_scope TEXT NOT NULL DEFAULT 'subscription';

ALTER TABLE acquisition_subscriptions
    ADD COLUMN scope_json TEXT;

ALTER TABLE acquisition_subscriptions
    ADD COLUMN metadata_policy TEXT NOT NULL DEFAULT 'recurring';

ALTER TABLE acquisition_subscriptions
    ADD COLUMN completion_policy TEXT NOT NULL DEFAULT 'manual';

CREATE INDEX IF NOT EXISTS idx_acquisition_subscriptions_request_metadata_due
    ON acquisition_subscriptions(active, status, request_mode, metadata_policy, metadata_refresh_after);

CREATE INDEX IF NOT EXISTS idx_acquisition_subscriptions_request_identity
    ON acquisition_subscriptions(media_type, normalized_title, year, request_mode, request_scope);
