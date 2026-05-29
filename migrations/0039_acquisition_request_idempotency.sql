ALTER TABLE acquisition_subscriptions
    ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_subscriptions_active_idempotency
    ON acquisition_subscriptions(idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND active = 1;

CREATE INDEX IF NOT EXISTS idx_acquisition_subscriptions_request_route_identity
    ON acquisition_subscriptions(
        media_type,
        normalized_title,
        year,
        request_mode,
        request_scope,
        route_policy,
        source_provider_id
    );
