-- Extension settings + auto-wire support.

CREATE TABLE IF NOT EXISTS extension_settings (
    setting_key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE orchestrator_runs ADD COLUMN source TEXT NOT NULL DEFAULT 'manual';

CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_source ON orchestrator_runs(source);
CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_source_status ON orchestrator_runs(source, status);
