CREATE TABLE IF NOT EXISTS playback_performance_envelopes (
    id TEXT PRIMARY KEY,
    host_fingerprint TEXT NOT NULL,
    os_family TEXT NOT NULL,
    os_version TEXT,
    gpu_vendor TEXT,
    gpu_model TEXT,
    gpu_driver_version TEXT,
    hardware_api TEXT,
    ffmpeg_path TEXT,
    ffmpeg_version TEXT,
    ffmpeg_sha256 TEXT,
    elixir_version TEXT,
    workload_class_id TEXT NOT NULL,
    pipeline_signature TEXT NOT NULL,
    support_decision TEXT NOT NULL,
    performance_decision TEXT NOT NULL,
    confidence TEXT NOT NULL,
    p50_realtime_factor_millis INTEGER,
    p95_realtime_factor_millis INTEGER,
    startup_latency_ms INTEGER,
    first_segment_latency_ms INTEGER,
    failure_count INTEGER NOT NULL DEFAULT 0,
    sample_count INTEGER NOT NULL DEFAULT 0,
    reasons_json TEXT NOT NULL DEFAULT '[]',
    warnings_json TEXT NOT NULL DEFAULT '[]',
    remediation_json TEXT NOT NULL DEFAULT '[]',
    invalidation_fingerprint TEXT NOT NULL,
    last_observed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(host_fingerprint, workload_class_id, pipeline_signature, invalidation_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_playback_performance_envelopes_lookup
    ON playback_performance_envelopes(host_fingerprint, workload_class_id, pipeline_signature);

CREATE INDEX IF NOT EXISTS idx_playback_performance_envelopes_status
    ON playback_performance_envelopes(hardware_api, support_decision, performance_decision, confidence);
