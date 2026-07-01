CREATE TABLE IF NOT EXISTS playback_hardware_readiness (
    id TEXT PRIMARY KEY,
    host_fingerprint TEXT NOT NULL,
    accelerator_id TEXT NOT NULL,
    api TEXT NOT NULL,
    os_family TEXT NOT NULL,
    os_version TEXT,
    arch TEXT NOT NULL,
    gpu_vendor TEXT,
    gpu_model TEXT,
    gpu_device_id TEXT,
    gpu_driver_version TEXT,
    ffmpeg_path TEXT,
    ffmpeg_version TEXT,
    ffmpeg_sha256 TEXT,
    elixir_accel_schema_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    status_reason TEXT NOT NULL,
    user_message_code TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    inventory_json TEXT NOT NULL,
    probe_report_json TEXT NOT NULL,
    raw_error_excerpt TEXT,
    stale INTEGER NOT NULL DEFAULT 0,
    last_checked_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(host_fingerprint, accelerator_id)
);

CREATE INDEX IF NOT EXISTS idx_playback_hardware_readiness_fingerprint
    ON playback_hardware_readiness(host_fingerprint, stale);

CREATE INDEX IF NOT EXISTS idx_playback_hardware_readiness_status
    ON playback_hardware_readiness(api, status, stale);

CREATE TABLE IF NOT EXISTS playback_hardware_readiness_events (
    id TEXT PRIMARY KEY,
    readiness_id TEXT,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    message_code TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_playback_hardware_readiness_events_readiness
    ON playback_hardware_readiness_events(readiness_id, created_at);
