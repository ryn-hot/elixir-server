use std::fmt;

use serde::{Deserialize, Serialize};
use sqlx::{Any, AnyPool, Row, Transaction, any::AnyRow};
use thiserror::Error;
use uuid::Uuid;

use crate::auth::{
    home_profiles::{HomeRole, ProfileType},
    revocation::{
        AuthorizationRevocationEventType, AuthorizationRevocationNotifier,
        AuthorizationSubjectType, NewAuthorizationRevocation, RevocationError,
        append_authorization_revocation_in_transaction,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Capability {
    ServerAdmin,
    UsersManage,
    SharingManage,
    DevicesManageOwn,
    DevicesManageAll,
    LibraryRead,
    MediaPlay,
    MediaDelete,
    LibraryScan,
    ReviewQueueManage,
    AcquisitionRequest,
    AcquisitionManage,
    ExtensionsView,
    ExtensionsManage,
    SecretsManage,
    SettingsView,
    SettingsManage,
    LiveBrowse,
    LivePlay,
    LiveManage,
}

impl Capability {
    pub const ALL: [Self; 20] = [
        Self::ServerAdmin,
        Self::UsersManage,
        Self::SharingManage,
        Self::DevicesManageOwn,
        Self::DevicesManageAll,
        Self::LibraryRead,
        Self::MediaPlay,
        Self::MediaDelete,
        Self::LibraryScan,
        Self::ReviewQueueManage,
        Self::AcquisitionRequest,
        Self::AcquisitionManage,
        Self::ExtensionsView,
        Self::ExtensionsManage,
        Self::SecretsManage,
        Self::SettingsView,
        Self::SettingsManage,
        Self::LiveBrowse,
        Self::LivePlay,
        Self::LiveManage,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ServerAdmin => "server_admin",
            Self::UsersManage => "users_manage",
            Self::SharingManage => "sharing_manage",
            Self::DevicesManageOwn => "devices_manage_own",
            Self::DevicesManageAll => "devices_manage_all",
            Self::LibraryRead => "library_read",
            Self::MediaPlay => "media_play",
            Self::MediaDelete => "media_delete",
            Self::LibraryScan => "library_scan",
            Self::ReviewQueueManage => "review_queue_manage",
            Self::AcquisitionRequest => "acquisition_request",
            Self::AcquisitionManage => "acquisition_manage",
            Self::ExtensionsView => "extensions_view",
            Self::ExtensionsManage => "extensions_manage",
            Self::SecretsManage => "secrets_manage",
            Self::SettingsView => "settings_view",
            Self::SettingsManage => "settings_manage",
            Self::LiveBrowse => "live_browse",
            Self::LivePlay => "live_play",
            Self::LiveManage => "live_manage",
        }
    }

    const fn bit(self) -> u64 {
        1_u64 << (self as u8)
    }
}

impl TryFrom<&str> for Capability {
    type Error = AuthorizationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::ALL
            .into_iter()
            .find(|capability| capability.as_str() == value)
            .ok_or(AuthorizationError::InvalidState("capability name"))
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct CapabilitySet(u64);

impl CapabilitySet {
    pub const fn empty() -> Self {
        Self(0)
    }

    pub fn all() -> Self {
        Self::from_iter(Capability::ALL)
    }

    pub fn from_iter(values: impl IntoIterator<Item = Capability>) -> Self {
        let mut set = Self::empty();
        for value in values {
            set.insert(value);
        }
        set
    }

    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability.bit() != 0
    }

    pub fn insert(&mut self, capability: Capability) {
        self.0 |= capability.bit();
    }

    pub fn remove(&mut self, capability: Capability) {
        self.0 &= !capability.bit();
    }

    pub fn iter(self) -> impl Iterator<Item = Capability> {
        Capability::ALL
            .into_iter()
            .filter(move |capability| self.contains(*capability))
    }
}

impl fmt::Debug for CapabilitySet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_set()
            .entries(self.iter().map(Capability::as_str))
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveAuthorization {
    pub revision: i64,
    pub capabilities: CapabilitySet,
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("principal lacks required capability {0:?}")]
    Forbidden(Capability),
    #[error("capability override exceeds the target profile role ceiling")]
    OverrideExceedsRoleCeiling,
    #[error("owner recovery capabilities cannot be overridden")]
    OwnerOverrideForbidden,
    #[error("only an active home owner can change capability overrides")]
    OverrideActorNotAuthorized,
    #[error("authorization revision changed while loading principal")]
    RevisionChanged,
    #[error("authorization revision overflow")]
    RevisionOverflow,
    #[error("authorization profile was not found or is inactive")]
    ProfileUnavailable,
    #[error("invalid authorization input: {0}")]
    InvalidInput(&'static str),
    #[error("invalid persisted authorization state: {0}")]
    InvalidState(&'static str),
    #[error("authorization database operation failed")]
    Storage(#[from] sqlx::Error),
    #[error("authorization revocation operation failed")]
    Revocation(#[from] RevocationError),
}

#[derive(Clone, Copy)]
pub struct AuthorizationRepository<'a> {
    pool: &'a AnyPool,
}

impl<'a> AuthorizationRepository<'a> {
    pub const fn new(pool: &'a AnyPool) -> Self {
        Self { pool }
    }

    pub async fn load_effective(
        &self,
        profile_id: Uuid,
        role: HomeRole,
        profile_type: ProfileType,
    ) -> Result<EffectiveAuthorization, AuthorizationError> {
        let rows = sqlx::query(
            "SELECT revision.revision,
                    CAST(overrides.capability AS TEXT) AS capability,
                    CAST(CASE WHEN overrides.allowed THEN 1 ELSE 0 END AS BIGINT) AS allowed
             FROM profile_authorization_revisions AS revision
             LEFT JOIN profile_capability_overrides AS overrides
               ON overrides.profile_id = revision.profile_id
             WHERE revision.profile_id = $1
             ORDER BY overrides.capability",
        )
        .bind(profile_id.to_string())
        .fetch_all(self.pool)
        .await?;
        decode_effective_authorization(&rows, role, profile_type)
    }

    pub async fn set_profile_override(
        &self,
        actor_user_id: Uuid,
        actor_snapshot: &str,
        profile_id: Uuid,
        capability: Capability,
        allowed: bool,
        notifier: Option<&AuthorizationRevocationNotifier>,
    ) -> Result<i64, AuthorizationError> {
        validate_actor_snapshot(actor_snapshot)?;
        let mut transaction = self.pool.begin().await?;
        let target = load_override_target(&mut transaction, profile_id).await?;
        require_override_owner(&mut transaction, target.home_id, actor_user_id).await?;
        validate_override(&target, capability, allowed)?;
        let existing: Option<i64> = sqlx::query_scalar(
            "SELECT CAST(CASE WHEN allowed THEN 1 ELSE 0 END AS BIGINT)
             FROM profile_capability_overrides
             WHERE profile_id = $1 AND capability = $2",
        )
        .bind(profile_id.to_string())
        .bind(capability.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if existing.map(|value| value != 0) == Some(allowed) {
            transaction.commit().await?;
            return Ok(target.revision);
        }
        sqlx::query(
            "INSERT INTO profile_capability_overrides (
                profile_id, capability, allowed, created_by_user_id,
                created_by_actor_snapshot
             ) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(profile_id, capability) DO UPDATE
             SET allowed = excluded.allowed,
                 created_by_user_id = excluded.created_by_user_id,
                 created_by_actor_snapshot = excluded.created_by_actor_snapshot,
                 updated_at = CURRENT_TIMESTAMP",
        )
        .bind(profile_id.to_string())
        .bind(capability.as_str())
        .bind(allowed)
        .bind(actor_user_id.to_string())
        .bind(actor_snapshot)
        .execute(&mut *transaction)
        .await?;
        let revision =
            bump_profile_authorization_revision_in_transaction(&mut transaction, profile_id)
                .await?;
        let event = append_authorization_revocation_in_transaction(
            &mut transaction,
            &NewAuthorizationRevocation {
                home_id: target.home_id,
                event_type: AuthorizationRevocationEventType::AuthorizationContextChanged,
                subject_type: AuthorizationSubjectType::Profile,
                subject_id: profile_id.to_string(),
                actor_user_id: Some(actor_user_id),
                account_session_id: None,
                profile_id: Some(profile_id),
                provider_id: None,
                grant_id: None,
                reason_code: "profile_capability_override_changed".to_string(),
                payload: serde_json::json!({
                    "capability": capability.as_str(),
                    "allowed": allowed,
                    "revision": revision,
                }),
            },
        )
        .await?;
        transaction.commit().await?;
        if let Some(notifier) = notifier {
            notifier.publish(event.id);
        }
        Ok(revision)
    }

    pub async fn remove_profile_override(
        &self,
        actor_user_id: Uuid,
        profile_id: Uuid,
        capability: Capability,
        notifier: Option<&AuthorizationRevocationNotifier>,
    ) -> Result<Option<i64>, AuthorizationError> {
        let mut transaction = self.pool.begin().await?;
        let target = load_override_target(&mut transaction, profile_id).await?;
        require_override_owner(&mut transaction, target.home_id, actor_user_id).await?;
        let deleted = sqlx::query(
            "DELETE FROM profile_capability_overrides
             WHERE profile_id = $1 AND capability = $2",
        )
        .bind(profile_id.to_string())
        .bind(capability.as_str())
        .execute(&mut *transaction)
        .await?;
        if deleted.rows_affected() == 0 {
            transaction.commit().await?;
            return Ok(None);
        }
        let revision =
            bump_profile_authorization_revision_in_transaction(&mut transaction, profile_id)
                .await?;
        let event = append_authorization_revocation_in_transaction(
            &mut transaction,
            &NewAuthorizationRevocation {
                home_id: target.home_id,
                event_type: AuthorizationRevocationEventType::AuthorizationContextChanged,
                subject_type: AuthorizationSubjectType::Profile,
                subject_id: profile_id.to_string(),
                actor_user_id: Some(actor_user_id),
                account_session_id: None,
                profile_id: Some(profile_id),
                provider_id: None,
                grant_id: None,
                reason_code: "profile_capability_override_removed".to_string(),
                payload: serde_json::json!({
                    "capability": capability.as_str(),
                    "revision": revision,
                }),
            },
        )
        .await?;
        transaction.commit().await?;
        if let Some(notifier) = notifier {
            notifier.publish(event.id);
        }
        Ok(Some(revision))
    }
}

async fn require_override_owner(
    transaction: &mut Transaction<'_, Any>,
    home_id: Uuid,
    actor_user_id: Uuid,
) -> Result<(), AuthorizationError> {
    let authorized: Option<i64> = sqlx::query_scalar(
        "SELECT 1
         FROM home_members
         WHERE home_id = $1
           AND user_id = $2
           AND role = 'owner'
           AND status = 'active'",
    )
    .bind(home_id.to_string())
    .bind(actor_user_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if authorized.is_none() {
        return Err(AuthorizationError::OverrideActorNotAuthorized);
    }
    Ok(())
}

pub async fn bump_profile_authorization_revision_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    profile_id: Uuid,
) -> Result<i64, AuthorizationError> {
    let revision: Option<i64> = sqlx::query_scalar(
        "UPDATE profile_authorization_revisions
         SET revision = revision + 1,
             updated_at = CURRENT_TIMESTAMP
         WHERE profile_id = $1 AND revision < $2
         RETURNING revision",
    )
    .bind(profile_id.to_string())
    .bind(i64::MAX)
    .fetch_optional(&mut **transaction)
    .await?;
    revision.ok_or(AuthorizationError::RevisionOverflow)
}

pub async fn bump_home_authorization_revisions_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    home_id: Uuid,
) -> Result<Vec<(Uuid, i64)>, AuthorizationError> {
    let rows = sqlx::query(
        "UPDATE profile_authorization_revisions
         SET revision = revision + 1,
             updated_at = CURRENT_TIMESTAMP
         WHERE home_id = $1 AND revision < $2
         RETURNING profile_id, revision",
    )
    .bind(home_id.to_string())
    .bind(i64::MAX)
    .fetch_all(&mut **transaction)
    .await?;
    let expected: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM profile_authorization_revisions WHERE home_id = $1",
    )
    .bind(home_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if i64::try_from(rows.len()).ok() != Some(expected) {
        return Err(AuthorizationError::RevisionOverflow);
    }
    rows.into_iter()
        .map(|row| {
            let profile_id: String = row.try_get("profile_id")?;
            let revision: i64 = row.try_get("revision")?;
            Ok((
                Uuid::parse_str(&profile_id)
                    .map_err(|_| AuthorizationError::InvalidState("profile UUID"))?,
                revision,
            ))
        })
        .collect()
}

pub fn role_default_capabilities(role: HomeRole, profile_type: ProfileType) -> CapabilitySet {
    if profile_type == ProfileType::Managed {
        return CapabilitySet::from_iter([
            Capability::DevicesManageOwn,
            Capability::LibraryRead,
            Capability::MediaPlay,
            Capability::LiveBrowse,
            Capability::LivePlay,
        ]);
    }
    match role {
        HomeRole::Owner => CapabilitySet::all(),
        HomeRole::Admin => CapabilitySet::from_iter([
            Capability::UsersManage,
            Capability::SharingManage,
            Capability::DevicesManageOwn,
            Capability::DevicesManageAll,
            Capability::LibraryRead,
            Capability::MediaPlay,
            Capability::MediaDelete,
            Capability::LibraryScan,
            Capability::ReviewQueueManage,
            Capability::AcquisitionRequest,
            Capability::AcquisitionManage,
            Capability::ExtensionsView,
            Capability::ExtensionsManage,
            Capability::SettingsView,
            Capability::SettingsManage,
            Capability::LiveBrowse,
            Capability::LivePlay,
        ]),
        HomeRole::Manager => CapabilitySet::from_iter([
            Capability::DevicesManageOwn,
            Capability::LibraryRead,
            Capability::MediaPlay,
            Capability::MediaDelete,
            Capability::LibraryScan,
            Capability::ReviewQueueManage,
            Capability::AcquisitionRequest,
            Capability::AcquisitionManage,
            Capability::ExtensionsView,
            Capability::LiveBrowse,
            Capability::LivePlay,
        ]),
        HomeRole::Viewer => CapabilitySet::from_iter([
            Capability::DevicesManageOwn,
            Capability::LibraryRead,
            Capability::MediaPlay,
            Capability::LiveBrowse,
            Capability::LivePlay,
        ]),
    }
}

fn override_ceiling(role: HomeRole, profile_type: ProfileType) -> CapabilitySet {
    if profile_type == ProfileType::Managed {
        return role_default_capabilities(role, profile_type);
    }
    match role {
        HomeRole::Owner => CapabilitySet::all(),
        HomeRole::Admin => {
            let mut ceiling = role_default_capabilities(role, profile_type);
            ceiling.insert(Capability::LiveManage);
            ceiling.insert(Capability::SecretsManage);
            ceiling
        }
        HomeRole::Manager | HomeRole::Viewer => role_default_capabilities(role, profile_type),
    }
}

fn decode_effective_authorization(
    rows: &[AnyRow],
    role: HomeRole,
    profile_type: ProfileType,
) -> Result<EffectiveAuthorization, AuthorizationError> {
    let first = rows.first().ok_or(AuthorizationError::ProfileUnavailable)?;
    let revision: i64 = first.try_get("revision")?;
    if revision <= 0 {
        return Err(AuthorizationError::InvalidState(
            "profile authorization revision",
        ));
    }
    let mut capabilities = role_default_capabilities(role, profile_type);
    if role != HomeRole::Owner || profile_type == ProfileType::Managed {
        let ceiling = override_ceiling(role, profile_type);
        for row in rows {
            let capability = optional_string(row, "capability")?;
            let Some(capability) = capability else {
                continue;
            };
            let capability = Capability::try_from(capability.as_str())?;
            let allowed: i64 = row.try_get("allowed")?;
            if allowed != 0 {
                if !ceiling.contains(capability) {
                    return Err(AuthorizationError::InvalidState(
                        "capability override exceeds role ceiling",
                    ));
                }
                capabilities.insert(capability);
            } else {
                capabilities.remove(capability);
            }
        }
    }
    Ok(EffectiveAuthorization {
        revision,
        capabilities,
    })
}

#[derive(Debug)]
struct OverrideTarget {
    home_id: Uuid,
    role: HomeRole,
    profile_type: ProfileType,
    revision: i64,
}

async fn load_override_target(
    transaction: &mut Transaction<'_, Any>,
    profile_id: Uuid,
) -> Result<OverrideTarget, AuthorizationError> {
    let row = sqlx::query(
        "SELECT profile.home_id,
                profile.profile_type,
                CAST(member.role AS TEXT) AS role,
                revision.revision
         FROM profiles AS profile
         JOIN profile_authorization_revisions AS revision
           ON revision.profile_id = profile.id
          AND revision.home_id = profile.home_id
         LEFT JOIN home_members AS member
           ON member.home_id = profile.home_id
          AND member.user_id = profile.user_id
          AND member.status = 'active'
         WHERE profile.id = $1",
    )
    .bind(profile_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(AuthorizationError::ProfileUnavailable)?;
    let home_id: String = row.try_get("home_id")?;
    let profile_type_raw: String = row.try_get("profile_type")?;
    let profile_type = ProfileType::try_from(profile_type_raw.as_str())
        .map_err(|_| AuthorizationError::InvalidState("profile type"))?;
    let role = if profile_type == ProfileType::Managed {
        HomeRole::Viewer
    } else {
        let role: Option<String> = optional_string(&row, "role")?;
        HomeRole::try_from(
            role.as_deref()
                .ok_or(AuthorizationError::ProfileUnavailable)?,
        )
        .map_err(|_| AuthorizationError::InvalidState("home role"))?
    };
    Ok(OverrideTarget {
        home_id: Uuid::parse_str(&home_id)
            .map_err(|_| AuthorizationError::InvalidState("home UUID"))?,
        role,
        profile_type,
        revision: row.try_get("revision")?,
    })
}

fn validate_override(
    target: &OverrideTarget,
    capability: Capability,
    allowed: bool,
) -> Result<(), AuthorizationError> {
    if target.role == HomeRole::Owner && target.profile_type == ProfileType::Account {
        return Err(AuthorizationError::OwnerOverrideForbidden);
    }
    if allowed && !override_ceiling(target.role, target.profile_type).contains(capability) {
        return Err(AuthorizationError::OverrideExceedsRoleCeiling);
    }
    Ok(())
}

fn validate_actor_snapshot(value: &str) -> Result<(), AuthorizationError> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > 512
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(AuthorizationError::InvalidInput("actor snapshot"));
    }
    Ok(())
}

fn optional_string(row: &AnyRow, field: &str) -> Result<Option<String>, sqlx::Error> {
    use sqlx::ValueRef;

    let raw = row.try_get_raw(field)?;
    if raw.is_null() {
        Ok(None)
    } else {
        row.try_get(field).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tokio::sync::broadcast::error::TryRecvError;

    use super::*;
    use crate::{auth::home_profiles::HomeProfileRepository, config::DatabaseConfig, db::Database};

    async fn test_database() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn create_user(database: &Database, label: &str) -> Result<Uuid> {
        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, 'hashed')")
            .bind(user_id.to_string())
            .bind(format!("{label}-{user_id}@example.test"))
            .execute(&database.pool)
            .await?;
        Ok(user_id)
    }

    async fn add_account_profile(
        database: &Database,
        home_id: Uuid,
        role: HomeRole,
        label: &str,
    ) -> Result<(Uuid, Uuid)> {
        let user_id = create_user(database, label).await?;
        let profile_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, $4, 'active')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .bind(role.as_str())
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO profiles (
                id, home_id, user_id, profile_type, display_name, is_default
             ) VALUES ($1, $2, $3, 'account', $4, FALSE)",
        )
        .bind(profile_id.to_string())
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .bind(format!("{label}-{profile_id}"))
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
             VALUES ($1, $2, 1)",
        )
        .bind(profile_id.to_string())
        .bind(home_id.to_string())
        .execute(&database.pool)
        .await?;
        Ok((user_id, profile_id))
    }

    #[test]
    fn a12_role_defaults_keep_live_management_and_secrets_separate() {
        let owner = role_default_capabilities(HomeRole::Owner, ProfileType::Account);
        assert_eq!(owner.iter().count(), Capability::ALL.len());

        let admin = role_default_capabilities(HomeRole::Admin, ProfileType::Account);
        assert!(admin.contains(Capability::LiveBrowse));
        assert!(admin.contains(Capability::LivePlay));
        assert!(!admin.contains(Capability::LiveManage));
        assert!(!admin.contains(Capability::SecretsManage));

        let manager = role_default_capabilities(HomeRole::Manager, ProfileType::Account);
        assert!(manager.contains(Capability::LiveBrowse));
        assert!(manager.contains(Capability::LivePlay));
        assert!(manager.contains(Capability::ExtensionsView));
        assert!(!manager.contains(Capability::LiveManage));
        assert!(!manager.contains(Capability::ExtensionsManage));
        assert!(!manager.contains(Capability::SecretsManage));
        assert!(!manager.contains(Capability::SharingManage));

        let viewer = role_default_capabilities(HomeRole::Viewer, ProfileType::Account);
        assert!(viewer.contains(Capability::LiveBrowse));
        assert!(viewer.contains(Capability::LivePlay));
        assert!(!viewer.contains(Capability::MediaDelete));
        assert!(!viewer.contains(Capability::ExtensionsView));

        let managed = role_default_capabilities(HomeRole::Owner, ProfileType::Managed);
        assert!(managed.contains(Capability::LiveBrowse));
        assert!(managed.contains(Capability::LivePlay));
        assert!(!managed.contains(Capability::LiveManage));
        assert!(!managed.contains(Capability::ServerAdmin));
    }

    #[tokio::test]
    async fn a12_owner_override_is_atomic_revisioned_and_emits_profile_revocation() -> Result<()> {
        let database = test_database().await?;
        let owner_user_id = create_user(&database, "owner").await?;
        let owner = HomeProfileRepository::new(&database.pool)
            .ensure_owner_home(owner_user_id)
            .await?;
        let (admin_user_id, admin_profile_id) =
            add_account_profile(&database, owner.home.id, HomeRole::Admin, "admin").await?;
        let (_, manager_profile_id) =
            add_account_profile(&database, owner.home.id, HomeRole::Manager, "manager").await?;
        let repository = AuthorizationRepository::new(&database.pool);
        let notifier = AuthorizationRevocationNotifier::new();
        let mut notifications = notifier.subscribe();

        let initial = repository
            .load_effective(admin_profile_id, HomeRole::Admin, ProfileType::Account)
            .await?;
        assert_eq!(initial.revision, 1);
        assert!(!initial.capabilities.contains(Capability::LiveManage));
        assert!(!initial.capabilities.contains(Capability::SecretsManage));

        let revision = repository
            .set_profile_override(
                owner_user_id,
                "owner@example.test",
                admin_profile_id,
                Capability::LiveManage,
                true,
                Some(&notifier),
            )
            .await?;
        assert_eq!(revision, 2);
        let event_id = notifications.try_recv()?;
        let event: (String, String, String, String) = sqlx::query_as(
            "SELECT event_type, subject_type, subject_id, payload_json
             FROM authorization_revocation_outbox WHERE id = $1",
        )
        .bind(event_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(event.0, "authorization_context_changed");
        assert_eq!(event.1, "profile");
        assert_eq!(event.2, admin_profile_id.to_string());
        let payload: serde_json::Value = serde_json::from_str(&event.3)?;
        assert_eq!(payload["capability"], "live_manage");
        assert_eq!(payload["revision"], 2);

        let effective = repository
            .load_effective(admin_profile_id, HomeRole::Admin, ProfileType::Account)
            .await?;
        assert_eq!(effective.revision, 2);
        assert!(effective.capabilities.contains(Capability::LiveManage));
        assert!(
            effective
                .capabilities
                .contains(Capability::ExtensionsManage)
        );
        assert!(effective.capabilities.contains(Capability::SharingManage));
        assert!(!effective.capabilities.contains(Capability::SecretsManage));

        assert_eq!(
            repository
                .set_profile_override(
                    owner_user_id,
                    "owner@example.test",
                    admin_profile_id,
                    Capability::LiveManage,
                    true,
                    Some(&notifier),
                )
                .await?,
            2,
            "an idempotent write must not consume a revision"
        );
        assert!(matches!(notifications.try_recv(), Err(TryRecvError::Empty)));

        assert!(matches!(
            repository
                .set_profile_override(
                    admin_user_id,
                    "admin@example.test",
                    admin_profile_id,
                    Capability::SecretsManage,
                    true,
                    None,
                )
                .await,
            Err(AuthorizationError::OverrideActorNotAuthorized)
        ));
        assert!(matches!(
            repository
                .set_profile_override(
                    owner_user_id,
                    "owner@example.test",
                    manager_profile_id,
                    Capability::LiveManage,
                    true,
                    None,
                )
                .await,
            Err(AuthorizationError::OverrideExceedsRoleCeiling)
        ));
        assert!(matches!(
            repository
                .set_profile_override(
                    owner_user_id,
                    "owner@example.test",
                    owner.profile.id,
                    Capability::LivePlay,
                    false,
                    None,
                )
                .await,
            Err(AuthorizationError::OwnerOverrideForbidden)
        ));

        assert_eq!(
            repository
                .remove_profile_override(
                    owner_user_id,
                    admin_profile_id,
                    Capability::LiveManage,
                    Some(&notifier),
                )
                .await?,
            Some(3)
        );
        let _ = notifications.try_recv()?;

        sqlx::query(
            "UPDATE profile_authorization_revisions SET revision = $1 WHERE profile_id = $2",
        )
        .bind(i64::MAX)
        .bind(admin_profile_id.to_string())
        .execute(&database.pool)
        .await?;
        assert!(matches!(
            repository
                .set_profile_override(
                    owner_user_id,
                    "owner@example.test",
                    admin_profile_id,
                    Capability::LiveManage,
                    true,
                    None,
                )
                .await,
            Err(AuthorizationError::RevisionOverflow)
        ));
        let persisted_override: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM profile_capability_overrides
             WHERE profile_id = $1 AND capability = 'live_manage'",
        )
        .bind(admin_profile_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            persisted_override, 0,
            "overflow must roll back the mutation"
        );
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM authorization_revocation_outbox
             WHERE profile_id = $1",
        )
        .bind(admin_profile_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(event_count, 2, "overflow must not append an event");
        Ok(())
    }
}
