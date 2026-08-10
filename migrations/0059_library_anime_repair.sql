-- ALM-8 keeps the evidence caused by an applied classifier result on the
-- media-file row that caused it. Repair code can therefore distinguish that
-- evidence from managed-import, identity-lock, and explicit-override data.
ALTER TABLE classifier_resolution_state
    ADD COLUMN applied_identity_version INTEGER NOT NULL DEFAULT 0
        CHECK(applied_identity_version >= 0);

ALTER TABLE classifier_resolution_state
    ADD COLUMN applied_identity_evidence_json TEXT
        CHECK(
            (applied_identity_version = 0 AND applied_identity_evidence_json IS NULL)
            OR (
                applied_identity_version > 0
                AND applied_identity_evidence_json IS NOT NULL
                AND LENGTH(TRIM(applied_identity_evidence_json)) > 0
            )
        );

ALTER TABLE classifier_resolution_state
    ADD COLUMN anime_match_assist_json TEXT
        CHECK(
            anime_match_assist_json IS NULL
            OR LENGTH(TRIM(anime_match_assist_json)) > 0
        );

-- One durable row represents one repair version's work for one physical media
-- file. The composite key makes enqueueing and replay idempotent.
CREATE TABLE IF NOT EXISTS library_anime_repairs (
    media_file_id TEXT NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    repair_version INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    claim_token TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    repaired_link_count INTEGER NOT NULL DEFAULT 0,
    repaired_identity_count INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL,
    evidence_snapshot_json TEXT NOT NULL,
    last_error TEXT,
    last_assist_json TEXT,
    claimed_at TIMESTAMP,
    -- Unix seconds keep lease comparisons identical through sqlx::Any on
    -- SQLite and PostgreSQL; native timestamp values cannot be bound through
    -- the Any driver without backend-specific query branches.
    claim_expires_at BIGINT,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(media_file_id, repair_version),
    CHECK(repair_version > 0),
    CHECK(status IN ('pending', 'running', 'retryable', 'completed', 'protected')),
    CHECK(attempt_count >= 0),
    CHECK(repaired_link_count >= 0),
    CHECK(repaired_identity_count >= 0),
    CHECK(LENGTH(TRIM(reason)) > 0),
    CHECK(LENGTH(TRIM(evidence_snapshot_json)) > 0),
    CHECK(last_error IS NULL OR LENGTH(TRIM(last_error)) > 0),
    CHECK(last_assist_json IS NULL OR LENGTH(TRIM(last_assist_json)) > 0),
    CHECK(claim_token IS NULL OR LENGTH(TRIM(claim_token)) > 0),
    CHECK(claim_expires_at IS NULL OR claim_expires_at > 0),
    CHECK(
        (
            status = 'running'
            AND claim_token IS NOT NULL
            AND claimed_at IS NOT NULL
            AND claim_expires_at IS NOT NULL
            AND completed_at IS NULL
            AND attempt_count > 0
        )
        OR (
            status IN ('pending', 'retryable')
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
            AND completed_at IS NULL
        )
        OR (
            status IN ('completed', 'protected')
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
            AND completed_at IS NOT NULL
        )
    ),
    CHECK(
        status = 'completed'
        OR (repaired_link_count = 0 AND repaired_identity_count = 0)
    )
);

CREATE INDEX IF NOT EXISTS idx_library_anime_repairs_work
    ON library_anime_repairs(status, repair_version, updated_at);

CREATE INDEX IF NOT EXISTS idx_library_anime_repairs_version_file
    ON library_anime_repairs(repair_version, media_file_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_library_anime_repairs_claim_token
    ON library_anime_repairs(claim_token)
    WHERE claim_token IS NOT NULL;

-- One support-facing aggregate row records progress for each repair version.
-- The lease and counters survive worker task interruptions; the per-file
-- ledger remains the source of truth for idempotent claims and final outcomes.
CREATE TABLE IF NOT EXISTS library_anime_repair_runs (
    repair_version INTEGER PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    claim_token TEXT,
    claim_expires_at BIGINT,
    scanned_count INTEGER NOT NULL DEFAULT 0,
    claimed_count INTEGER NOT NULL DEFAULT 0,
    retryable_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    protected_count INTEGER NOT NULL DEFAULT 0,
    repaired_link_count INTEGER NOT NULL DEFAULT 0,
    repaired_identity_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK(repair_version > 0),
    CHECK(status IN ('pending', 'running', 'completed', 'failed')),
    CHECK(claim_token IS NULL OR LENGTH(TRIM(claim_token)) > 0),
    CHECK(claim_expires_at IS NULL OR claim_expires_at > 0),
    CHECK(scanned_count >= 0),
    CHECK(claimed_count >= 0),
    CHECK(retryable_count >= 0),
    CHECK(completed_count >= 0),
    CHECK(protected_count >= 0),
    CHECK(repaired_link_count >= 0),
    CHECK(repaired_identity_count >= 0),
    CHECK(failure_count >= 0),
    CHECK(last_error IS NULL OR LENGTH(TRIM(last_error)) > 0),
    CHECK(
        (
            status = 'pending'
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
            AND finished_at IS NULL
        )
        OR (
            status = 'running'
            AND claim_token IS NOT NULL
            AND claim_expires_at IS NOT NULL
            AND started_at IS NOT NULL
            AND finished_at IS NULL
        )
        OR (
            status IN ('completed', 'failed')
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
            AND started_at IS NOT NULL
            AND finished_at IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_library_anime_repair_runs_status
    ON library_anime_repair_runs(status, updated_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_library_anime_repair_runs_claim_token
    ON library_anime_repair_runs(claim_token)
    WHERE claim_token IS NOT NULL;
