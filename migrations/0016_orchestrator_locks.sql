CREATE TABLE IF NOT EXISTS orchestrator_locks (
    lock_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    locked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orchestrator_locks_locked_at ON orchestrator_locks(locked_at);
