CREATE TABLE IF NOT EXISTS acquisition_audit_events (
    audit_event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    release_id TEXT REFERENCES acquisition_releases(release_id) ON DELETE SET NULL,
    subscription_id TEXT REFERENCES acquisition_subscriptions(subscription_id) ON DELETE SET NULL,
    target_id TEXT REFERENCES acquisition_targets(target_id) ON DELETE SET NULL,
    release_job_id TEXT REFERENCES acquisition_release_jobs(release_job_id) ON DELETE SET NULL,
    import_run_id TEXT REFERENCES acquisition_import_runs(import_run_id) ON DELETE SET NULL,
    import_link_id TEXT REFERENCES acquisition_import_file_links(import_link_id) ON DELETE SET NULL,
    actor_user_id TEXT,
    state TEXT,
    reason TEXT,
    evidence_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_acquisition_audit_events_release
    ON acquisition_audit_events(release_id, created_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_audit_events_subscription
    ON acquisition_audit_events(subscription_id, created_at);

CREATE INDEX IF NOT EXISTS idx_acquisition_audit_events_type
    ON acquisition_audit_events(event_type, created_at);
