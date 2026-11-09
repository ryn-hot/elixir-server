-- One restart-lineage binding, never an execution/native-stop/readiness flag.
-- Installed controllers cannot adopt a fresh/lost filesystem root when their
-- prior native ownership history is unavailable. No event history or secrets.
CREATE TABLE IF NOT EXISTS marketplace_installed_runtime_owner (
    singleton BIGINT PRIMARY KEY CHECK (singleton = 1),
    nonce TEXT NOT NULL CHECK (LENGTH(nonce) = 36),
    binding_sha256 TEXT NOT NULL CHECK (LENGTH(binding_sha256) = 71)
);
