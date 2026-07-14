-- Establish the account household boundary before sessions and authorization grants.

CREATE TABLE IF NOT EXISTS homes (
    id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (LENGTH(TRIM(name)) > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS home_members (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'manager', 'viewer')),
    status TEXT NOT NULL CHECK (status IN ('active', 'invited', 'suspended')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (role <> 'owner' OR status = 'active'),
    UNIQUE(home_id, user_id)
);

CREATE TABLE IF NOT EXISTS profiles (
    id TEXT PRIMARY KEY,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    user_id TEXT REFERENCES users(id) ON DELETE CASCADE,
    profile_type TEXT NOT NULL CHECK (profile_type IN ('account', 'managed')),
    display_name TEXT NOT NULL CHECK (LENGTH(TRIM(display_name)) > 0),
    avatar_color TEXT,
    pin_hash TEXT,
    restriction_policy_id TEXT,
    is_default BOOLEAN NOT NULL DEFAULT FALSE CHECK (is_default IN (FALSE, TRUE)),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (profile_type = 'account' AND user_id IS NOT NULL)
        OR (profile_type = 'managed' AND user_id IS NULL)
    ),
    UNIQUE(home_id, display_name)
);

CREATE INDEX IF NOT EXISTS idx_homes_owner ON homes(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_home_members_user ON home_members(user_id);
CREATE INDEX IF NOT EXISTS idx_profiles_home ON profiles(home_id);
CREATE INDEX IF NOT EXISTS idx_profiles_user ON profiles(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_home_members_active_owner
    ON home_members(home_id)
    WHERE role = 'owner' AND status = 'active';
CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_account_user
    ON profiles(home_id, user_id)
    WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_default
    ON profiles(home_id)
    WHERE is_default = TRUE;

ALTER TABLE server_instances
    ADD COLUMN home_id TEXT REFERENCES homes(id) ON DELETE SET NULL;

-- IDs are deterministic and table-local. Reusing the account identifier across
-- these three new table namespaces makes the portable SQL backfill repeatable
-- without a database-specific UUID extension.
INSERT INTO homes (id, owner_user_id, name)
SELECT
    id,
    id,
    CASE
        WHEN LENGTH(TRIM(email)) > 0 THEN TRIM(email) || '''s Home'
        ELSE 'Elixir Home'
    END
FROM users;

INSERT INTO home_members (id, home_id, user_id, role, status)
SELECT id, id, id, 'owner', 'active'
FROM users;

WITH RECURSIVE email_local_parts (user_id, remaining, local_part) AS (
    SELECT id, SUBSTR(TRIM(email), 1, 320), ''
    FROM users

    UNION ALL

    SELECT
        user_id,
        SUBSTR(remaining, 2),
        local_part || SUBSTR(remaining, 1, 1)
    FROM email_local_parts
    WHERE LENGTH(remaining) > 0
      AND SUBSTR(remaining, 1, 1) <> '@'
), account_profile_names (user_id, display_name) AS (
    SELECT
        user_id,
        CASE
            WHEN SUBSTR(remaining, 1, 1) = '@'
             AND LENGTH(TRIM(local_part)) > 0
                THEN TRIM(local_part)
            ELSE 'Owner'
        END
    FROM email_local_parts
    WHERE LENGTH(remaining) = 0
       OR SUBSTR(remaining, 1, 1) = '@'
)
INSERT INTO profiles (
    id,
    home_id,
    user_id,
    profile_type,
    display_name,
    is_default
)
SELECT users.id, users.id, users.id, 'account', names.display_name, TRUE
FROM users
JOIN account_profile_names AS names ON names.user_id = users.id;

UPDATE server_instances
SET home_id = user_id
WHERE home_id IS NULL
  AND EXISTS (
      SELECT 1
      FROM homes
      WHERE homes.id = server_instances.user_id
        AND homes.owner_user_id = server_instances.user_id
  );
