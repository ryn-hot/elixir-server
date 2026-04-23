CREATE TABLE provider_readiness (
    provider_id TEXT PRIMARY KEY REFERENCES providers(provider_id) ON DELETE CASCADE,
    readiness_phase TEXT NOT NULL DEFAULT 'unknown' CHECK (readiness_phase IN ('unknown', 'transport_ready', 'bootstrap_ready', 'driver_ready')),
    readiness_detail TEXT,
    last_checked_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO provider_readiness (provider_id, readiness_phase, readiness_detail, last_checked_at, created_at, updated_at)
SELECT
    provider_id,
    CASE
        WHEN health_state IN ('healthy', 'degraded') THEN 'driver_ready'
        ELSE 'unknown'
    END,
    NULL,
    last_healthcheck_at,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM providers;

CREATE INDEX idx_provider_readiness_phase ON provider_readiness(readiness_phase);
