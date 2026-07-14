use std::fmt;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Any, AnyPool, Transaction};
use uuid::Uuid;

const ID_DOMAIN: &str = "elixir.auth.home-profiles.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HomeRole {
    Owner,
    Admin,
    Manager,
    Viewer,
}

impl HomeRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Admin => "admin",
            Self::Manager => "manager",
            Self::Viewer => "viewer",
        }
    }
}

impl TryFrom<&str> for HomeRole {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "owner" => Ok(Self::Owner),
            "admin" => Ok(Self::Admin),
            "manager" => Ok(Self::Manager),
            "viewer" => Ok(Self::Viewer),
            _ => bail!("invalid home role"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HomeMemberStatus {
    Active,
    Invited,
    Suspended,
}

impl HomeMemberStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Invited => "invited",
            Self::Suspended => "suspended",
        }
    }
}

impl TryFrom<&str> for HomeMemberStatus {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "invited" => Ok(Self::Invited),
            "suspended" => Ok(Self::Suspended),
            _ => bail!("invalid home member status"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProfileType {
    Account,
    Managed,
}

impl ProfileType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Managed => "managed",
        }
    }
}

impl TryFrom<&str> for ProfileType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "account" => Ok(Self::Account),
            "managed" => Ok(Self::Managed),
            _ => bail!("invalid profile type"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Home {
    pub id: Uuid,
    pub owner_user_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeMember {
    pub id: Uuid,
    pub home_id: Uuid,
    pub user_id: Uuid,
    pub role: HomeRole,
    pub status: HomeMemberStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    pub id: Uuid,
    pub home_id: Uuid,
    pub user_id: Option<Uuid>,
    pub profile_type: ProfileType,
    pub display_name: String,
    pub avatar_color: Option<String>,
    #[serde(skip)]
    pub pin_hash: Option<String>,
    pub restriction_policy_id: Option<String>,
    pub is_default: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl fmt::Debug for Profile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Profile")
            .field("id", &self.id)
            .field("home_id", &self.home_id)
            .field("user_id", &self.user_id)
            .field("profile_type", &self.profile_type)
            .field("display_name", &self.display_name)
            .field("avatar_color", &self.avatar_color)
            .field("pin_configured", &self.pin_hash.is_some())
            .field("restriction_policy_id", &self.restriction_policy_id)
            .field("is_default", &self.is_default)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerHomeBootstrap {
    pub home: Home,
    pub membership: HomeMember,
    pub profile: Profile,
}

#[derive(Debug, sqlx::FromRow)]
struct HomeRow {
    id: String,
    owner_user_id: String,
    name: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, sqlx::FromRow)]
struct HomeMemberRow {
    id: String,
    home_id: String,
    user_id: String,
    role: String,
    status: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, sqlx::FromRow)]
struct ProfileRow {
    id: String,
    home_id: String,
    user_id: String,
    profile_type: String,
    display_name: String,
    avatar_color: String,
    pin_hash: String,
    restriction_policy_id: String,
    is_default: i64,
    created_at: String,
    updated_at: String,
}

impl TryFrom<HomeRow> for Home {
    type Error = anyhow::Error;

    fn try_from(row: HomeRow) -> Result<Self> {
        Ok(Self {
            id: parse_uuid(&row.id, "home id")?,
            owner_user_id: parse_uuid(&row.owner_user_id, "home owner user id")?,
            name: row.name,
            created_at: parse_timestamp(&row.created_at)?,
            updated_at: parse_timestamp(&row.updated_at)?,
        })
    }
}

impl TryFrom<HomeMemberRow> for HomeMember {
    type Error = anyhow::Error;

    fn try_from(row: HomeMemberRow) -> Result<Self> {
        Ok(Self {
            id: parse_uuid(&row.id, "home membership id")?,
            home_id: parse_uuid(&row.home_id, "membership home id")?,
            user_id: parse_uuid(&row.user_id, "membership user id")?,
            role: HomeRole::try_from(row.role.as_str())?,
            status: HomeMemberStatus::try_from(row.status.as_str())?,
            created_at: parse_timestamp(&row.created_at)?,
            updated_at: parse_timestamp(&row.updated_at)?,
        })
    }
}

impl TryFrom<ProfileRow> for Profile {
    type Error = anyhow::Error;

    fn try_from(row: ProfileRow) -> Result<Self> {
        Ok(Self {
            id: parse_uuid(&row.id, "profile id")?,
            home_id: parse_uuid(&row.home_id, "profile home id")?,
            user_id: (!row.user_id.is_empty())
                .then(|| parse_uuid(&row.user_id, "profile user id"))
                .transpose()?,
            profile_type: ProfileType::try_from(row.profile_type.as_str())?,
            display_name: row.display_name,
            avatar_color: nonempty_text(row.avatar_color),
            pin_hash: nonempty_text(row.pin_hash),
            restriction_policy_id: nonempty_text(row.restriction_policy_id),
            is_default: row.is_default != 0,
            created_at: parse_timestamp(&row.created_at)?,
            updated_at: parse_timestamp(&row.updated_at)?,
        })
    }
}

#[derive(Clone, Copy)]
pub struct HomeProfileRepository<'a> {
    pool: &'a AnyPool,
}

impl<'a> HomeProfileRepository<'a> {
    pub const fn new(pool: &'a AnyPool) -> Self {
        Self { pool }
    }

    pub async fn ensure_owner_home(&self, user_id: Uuid) -> Result<OwnerHomeBootstrap> {
        let mut transaction = self.pool.begin().await?;
        let bootstrap = ensure_owner_home_in_transaction(&mut transaction, user_id).await?;
        transaction.commit().await?;
        Ok(bootstrap)
    }

    pub async fn home_for_owner(&self, user_id: Uuid) -> Result<Option<Home>> {
        let row: Option<HomeRow> = sqlx::query_as(
            "SELECT id,
                    owner_user_id,
                    name,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
             FROM homes
             WHERE owner_user_id = $1
             ORDER BY created_at, id
             LIMIT 1",
        )
        .bind(user_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(Home::try_from).transpose()
    }

    pub async fn membership(&self, home_id: Uuid, user_id: Uuid) -> Result<Option<HomeMember>> {
        let row: Option<HomeMemberRow> = sqlx::query_as(
            "SELECT id,
                    home_id,
                    user_id,
                    role,
                    status,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
             FROM home_members
             WHERE home_id = $1 AND user_id = $2",
        )
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(HomeMember::try_from).transpose()
    }

    pub async fn profile(&self, profile_id: Uuid) -> Result<Option<Profile>> {
        let row: Option<ProfileRow> = sqlx::query_as(
            "SELECT id,
                    home_id,
                    COALESCE(CAST(user_id AS TEXT), '') AS user_id,
                    profile_type,
                    display_name,
                    COALESCE(CAST(avatar_color AS TEXT), '') AS avatar_color,
                    COALESCE(CAST(pin_hash AS TEXT), '') AS pin_hash,
                    COALESCE(CAST(restriction_policy_id AS TEXT), '') AS restriction_policy_id,
                    CAST(CASE WHEN is_default THEN 1 ELSE 0 END AS BIGINT) AS is_default,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
             FROM profiles
             WHERE id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(Profile::try_from).transpose()
    }

    pub async fn list_profiles(&self, home_id: Uuid) -> Result<Vec<Profile>> {
        let rows: Vec<ProfileRow> = sqlx::query_as(
            "SELECT id,
                    home_id,
                    COALESCE(CAST(user_id AS TEXT), '') AS user_id,
                    profile_type,
                    display_name,
                    COALESCE(CAST(avatar_color AS TEXT), '') AS avatar_color,
                    COALESCE(CAST(pin_hash AS TEXT), '') AS pin_hash,
                    COALESCE(CAST(restriction_policy_id AS TEXT), '') AS restriction_policy_id,
                    CAST(CASE WHEN is_default THEN 1 ELSE 0 END AS BIGINT) AS is_default,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
             FROM profiles
             WHERE home_id = $1
             ORDER BY CASE WHEN is_default THEN 0 ELSE 1 END,
                      LOWER(display_name),
                      id",
        )
        .bind(home_id.to_string())
        .fetch_all(self.pool)
        .await?;
        rows.into_iter().map(Profile::try_from).collect()
    }
}

pub(crate) async fn ensure_owner_home_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    user_id: Uuid,
) -> Result<OwnerHomeBootstrap> {
    let email: Option<String> = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?;
    let email = email.context("account does not exist")?;
    let candidate_home_id = stable_id("home", &[user_id]);
    let home_name = owner_home_name(&email);

    sqlx::query(
        "INSERT INTO homes (id, owner_user_id, name)
         SELECT $1, $2, $3
         WHERE NOT EXISTS (
             SELECT 1
             FROM homes
             WHERE owner_user_id = $4
         )
         ON CONFLICT DO NOTHING",
    )
    .bind(candidate_home_id.to_string())
    .bind(user_id.to_string())
    .bind(home_name)
    .bind(user_id.to_string())
    .execute(&mut **transaction)
    .await?;

    let home_row: HomeRow = sqlx::query_as(
        "SELECT id,
                owner_user_id,
                name,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM homes
         WHERE owner_user_id = $1
         ORDER BY CASE WHEN id = $2 THEN 0 ELSE 1 END, created_at, id
         LIMIT 1",
    )
    .bind(user_id.to_string())
    .bind(candidate_home_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let home = Home::try_from(home_row)?;

    let membership_id = stable_id("owner-membership", &[home.id, user_id]);
    sqlx::query(
        "INSERT INTO home_members (id, home_id, user_id, role, status)
         VALUES ($1, $2, $3, 'owner', 'active')
         ON CONFLICT DO NOTHING",
    )
    .bind(membership_id.to_string())
    .bind(home.id.to_string())
    .bind(user_id.to_string())
    .execute(&mut **transaction)
    .await?;

    let profile_id = stable_id("account-profile", &[home.id, user_id]);
    sqlx::query(
        "INSERT INTO profiles
            (id, home_id, user_id, profile_type, display_name, is_default)
         VALUES ($1, $2, $3, 'account', $4, TRUE)
         ON CONFLICT DO NOTHING",
    )
    .bind(profile_id.to_string())
    .bind(home.id.to_string())
    .bind(user_id.to_string())
    .bind(owner_display_name(&email))
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
         VALUES ($1, $2, 1)
         ON CONFLICT(profile_id) DO NOTHING",
    )
    .bind(profile_id.to_string())
    .bind(home.id.to_string())
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        "UPDATE server_instances
         SET home_id = $1
         WHERE user_id = $2 AND home_id IS NULL",
    )
    .bind(home.id.to_string())
    .bind(user_id.to_string())
    .execute(&mut **transaction)
    .await?;

    let membership_row: HomeMemberRow = sqlx::query_as(
        "SELECT id,
                home_id,
                user_id,
                role,
                status,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM home_members
         WHERE home_id = $1 AND user_id = $2",
    )
    .bind(home.id.to_string())
    .bind(user_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let membership = HomeMember::try_from(membership_row)?;

    let profile_row: ProfileRow = sqlx::query_as(
        "SELECT id,
                home_id,
                COALESCE(CAST(user_id AS TEXT), '') AS user_id,
                profile_type,
                display_name,
                COALESCE(CAST(avatar_color AS TEXT), '') AS avatar_color,
                COALESCE(CAST(pin_hash AS TEXT), '') AS pin_hash,
                COALESCE(CAST(restriction_policy_id AS TEXT), '') AS restriction_policy_id,
                CAST(CASE WHEN is_default THEN 1 ELSE 0 END AS BIGINT) AS is_default,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM profiles
         WHERE home_id = $1 AND user_id = $2",
    )
    .bind(home.id.to_string())
    .bind(user_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let profile = Profile::try_from(profile_row)?;

    require_owner_bootstrap(&home, &membership, &profile, user_id)?;
    Ok(OwnerHomeBootstrap {
        home,
        membership,
        profile,
    })
}

fn require_owner_bootstrap(
    home: &Home,
    membership: &HomeMember,
    profile: &Profile,
    user_id: Uuid,
) -> Result<()> {
    if home.owner_user_id != user_id
        || membership.home_id != home.id
        || membership.user_id != user_id
        || membership.role != HomeRole::Owner
        || membership.status != HomeMemberStatus::Active
        || profile.home_id != home.id
        || profile.user_id != Some(user_id)
        || profile.profile_type != ProfileType::Account
        || !profile.is_default
    {
        bail!("owner home bootstrap invariant failed");
    }
    Ok(())
}

fn stable_id(kind: &str, values: &[Uuid]) -> Uuid {
    let mut name = format!("{ID_DOMAIN}:{kind}");
    for value in values {
        name.push(':');
        name.push_str(&value.to_string());
    }
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

fn owner_home_name(email: &str) -> String {
    let email = email.trim();
    if email.is_empty() {
        "Elixir Home".to_string()
    } else {
        format!("{email}'s Home")
    }
}

fn owner_display_name(email: &str) -> String {
    email
        .split_once('@')
        .map(|(local, _)| local.trim())
        .filter(|local| !local.is_empty())
        .unwrap_or("Owner")
        .to_string()
}

fn parse_uuid(value: &str, label: &str) -> Result<Uuid> {
    Uuid::parse_str(value).with_context(|| format!("invalid {label}"))
}

fn nonempty_text(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(timestamp.and_utc());
        }
    }
    bail!("invalid database timestamp")
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::{config::DatabaseConfig, db::Database};

    async fn migrated_database() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    #[tokio::test]
    async fn owner_bootstrap_is_idempotent_and_assigns_existing_servers() -> Result<()> {
        let database = migrated_database().await?;
        let user_id = Uuid::new_v4();
        let server_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(user_id.to_string())
            .bind("alice@example.com")
            .bind("hashed")
            .execute(&database.pool)
            .await?;
        sqlx::query(
            "INSERT INTO server_instances (id, user_id, device_name, lan_addresses)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(server_id.to_string())
        .bind(user_id.to_string())
        .bind("Alice Server")
        .bind("[]")
        .execute(&database.pool)
        .await?;

        let repository = HomeProfileRepository::new(&database.pool);
        let first = repository.ensure_owner_home(user_id).await?;
        let second = repository.ensure_owner_home(user_id).await?;
        assert_eq!(first, second);
        assert_eq!(first.home.name, "alice@example.com's Home");
        assert_eq!(first.membership.role, HomeRole::Owner);
        assert_eq!(first.membership.status, HomeMemberStatus::Active);
        assert_eq!(first.profile.display_name, "alice");
        assert_eq!(first.profile.profile_type, ProfileType::Account);
        assert!(first.profile.is_default);

        let server_home_id: String =
            sqlx::query_scalar("SELECT home_id FROM server_instances WHERE id = $1")
                .bind(server_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(server_home_id, first.home.id.to_string());

        for table in ["homes", "home_members", "profiles"] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&database.pool)
                .await?;
            assert_eq!(count, 1, "unexpected {table} row count");
        }
        let expected_home = first.home.clone();
        let expected_membership = first.membership.clone();
        let expected_profile = first.profile.clone();
        assert_eq!(
            repository.home_for_owner(user_id).await?,
            Some(expected_home)
        );
        assert_eq!(
            repository
                .membership(first.membership.home_id, user_id)
                .await?,
            Some(expected_membership)
        );
        assert_eq!(
            repository.profile(first.profile.id).await?,
            Some(expected_profile.clone())
        );
        assert_eq!(
            repository.list_profiles(first.profile.home_id).await?,
            vec![expected_profile]
        );
        let mut redacted_profile = first.profile.clone();
        redacted_profile.pin_hash = Some("sensitive-test-pin-hash".to_string());
        let serialized = serde_json::to_value(&redacted_profile)?;
        assert!(serialized.get("pin_hash").is_none());
        assert!(!format!("{redacted_profile:?}").contains("sensitive-test-pin-hash"));
        Ok(())
    }

    #[tokio::test]
    async fn owner_bootstrap_reuses_the_deterministic_legacy_backfill() -> Result<()> {
        let database = migrated_database().await?;
        let user_id = Uuid::new_v4();
        let server_id = Uuid::new_v4();
        let id = user_id.to_string();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(&id)
            .bind("legacy@example.com")
            .bind("hashed")
            .execute(&database.pool)
            .await?;
        sqlx::query("INSERT INTO homes (id, owner_user_id, name) VALUES ($1, $2, $3)")
            .bind(&id)
            .bind(&id)
            .bind("legacy@example.com's Home")
            .execute(&database.pool)
            .await?;
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, 'owner', 'active')",
        )
        .bind(&id)
        .bind(&id)
        .bind(&id)
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO profiles
                (id, home_id, user_id, profile_type, display_name, is_default)
             VALUES ($1, $2, $3, 'account', 'legacy', TRUE)",
        )
        .bind(&id)
        .bind(&id)
        .bind(&id)
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO server_instances
                (id, user_id, home_id, device_name, lan_addresses)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(server_id.to_string())
        .bind(&id)
        .bind(&id)
        .bind("Legacy Server")
        .bind("[]")
        .execute(&database.pool)
        .await?;

        let repository = HomeProfileRepository::new(&database.pool);
        let bootstrap = repository.ensure_owner_home(user_id).await?;
        assert_eq!(bootstrap.home.id, user_id);
        assert_eq!(bootstrap.membership.id, user_id);
        assert_eq!(bootstrap.profile.id, user_id);
        for table in ["homes", "home_members", "profiles"] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&database.pool)
                .await?;
            assert_eq!(count, 1, "bootstrap duplicated the legacy {table} row");
        }
        Ok(())
    }

    #[tokio::test]
    async fn owner_bootstrap_rejects_unknown_account_without_partial_rows() -> Result<()> {
        let database = migrated_database().await?;
        let repository = HomeProfileRepository::new(&database.pool);
        assert!(repository.ensure_owner_home(Uuid::new_v4()).await.is_err());
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM homes")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    fn frozen_enum_spellings_round_trip() -> Result<()> {
        for role in [
            HomeRole::Owner,
            HomeRole::Admin,
            HomeRole::Manager,
            HomeRole::Viewer,
        ] {
            assert_eq!(HomeRole::try_from(role.as_str())?, role);
        }
        for status in [
            HomeMemberStatus::Active,
            HomeMemberStatus::Invited,
            HomeMemberStatus::Suspended,
        ] {
            assert_eq!(HomeMemberStatus::try_from(status.as_str())?, status);
        }
        for profile_type in [ProfileType::Account, ProfileType::Managed] {
            assert_eq!(ProfileType::try_from(profile_type.as_str())?, profile_type);
        }
        assert_eq!(owner_display_name("local-only"), "Owner");
        Ok(())
    }
}
