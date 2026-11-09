-- Controller-owned singleton, never a peer event log. Only this migration may
-- create the empty first-boot state. A missing established row is an error,
-- not permission to recreate an unused monthly allowance.
CREATE TABLE marketplace_transfer_accounting (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK(singleton = 1),
    revision BIGINT NOT NULL CHECK(revision >= 0),
    state_json TEXT NOT NULL CHECK(LENGTH(state_json) <= 1024),
    CHECK(
        (revision = 0 AND state_json = '')
        OR (revision > 0 AND LENGTH(state_json) > 0)
    )
);

INSERT INTO marketplace_transfer_accounting(singleton, revision, state_json)
    VALUES (1, 0, '');
