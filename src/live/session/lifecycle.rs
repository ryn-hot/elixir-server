use std::{fmt, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::{AnyPool, Row};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    auth::{
        home_profiles::{HomeRole, ProfileType},
        revocation::{
            AuthorizationRevocationEvent, AuthorizationRevocationEventType,
            AuthorizationRevocationNotifier, AuthorizationRevocationStore, RevocationError,
        },
    },
    authz::{AuthorizationError, AuthorizationRepository, Capability},
    live::{
        catalog::{LiveProviderAccess, LiveProviderGrantError, LiveProviderGrantRepository},
        provider::{LiveProviderClient, ProviderDirectoryError, ProviderDirectoryErrorCode},
    },
};

use super::{
    LiveSessionRepository, SessionRecord, SessionRepositoryError, SessionState, TerminalReason,
};

const CONSUMER_NAME: &str = "live-session-revoker-v1";
const CLAIM_SECONDS: u64 = 300;
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_ACTIVE_SESSIONS: u32 = 10_000;
const MAX_CAS_RETRIES: usize = 8;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionReconciliationReport {
    pub inspected: u64,
    pub adopted: u64,
    pub terminated: u64,
    pub revocations_consumed: u64,
}

#[derive(Debug, Error)]
pub enum SessionLifecycleError {
    #[error("Live session repository reconciliation failed")]
    Session(#[from] SessionRepositoryError),
    #[error("Live authorization reconciliation failed")]
    Authorization(#[from] AuthorizationError),
    #[error("Live provider-grant reconciliation failed")]
    Grant(#[from] LiveProviderGrantError),
    #[error("Live provider reconciliation failed")]
    Provider(#[from] ProviderDirectoryError),
    #[error("Live revocation consumption failed")]
    Revocation(#[from] RevocationError),
    #[error("Live session reconciliation database operation failed")]
    Storage(#[from] sqlx::Error),
    #[error("invalid persisted Live session lifecycle state")]
    InvalidState,
}

#[derive(Clone)]
pub struct LiveSessionLifecycle {
    pool: AnyPool,
    repository: Arc<LiveSessionRepository>,
    provider_client: Arc<LiveProviderClient>,
    lease_owner: String,
}

impl LiveSessionLifecycle {
    pub fn new(
        pool: AnyPool,
        repository: Arc<LiveSessionRepository>,
        provider_client: Arc<LiveProviderClient>,
        owner_instance_id: Uuid,
    ) -> Self {
        Self {
            pool,
            repository,
            provider_client,
            lease_owner: format!("live-session-revoker-{owner_instance_id}"),
        }
    }

    pub async fn reconcile_startup(
        &self,
        control_fencing_token: i64,
    ) -> Result<SessionReconciliationReport, SessionLifecycleError> {
        let store = AuthorizationRevocationStore::new(&self.pool);
        store.register_consumer(CONSUMER_NAME).await?;

        let now = Utc::now();
        self.repository
            .assert_current_fence(control_fencing_token, now)
            .await?;
        let sessions = self
            .repository
            .list_nonterminal(MAX_ACTIVE_SESSIONS)
            .await?;
        let mut report = SessionReconciliationReport::default();
        for session in sessions {
            report.inspected += 1;
            let outcome = self
                .reconcile_session(session, control_fencing_token, now)
                .await?;
            report.adopted += u64::from(outcome.adopted);
            report.terminated += u64::from(outcome.terminated);
        }
        report.revocations_consumed = self.drain_revocations(control_fencing_token, None).await?;
        self.repository
            .cleanup(now, control_fencing_token, MAX_ACTIVE_SESSIONS)
            .await?;
        Ok(report)
    }

    pub async fn run(
        &self,
        control_fencing_token: i64,
        notifier: AuthorizationRevocationNotifier,
        shutdown: CancellationToken,
    ) -> Result<(), SessionLifecycleError> {
        let mut notifications = notifier.subscribe();
        loop {
            self.repository
                .assert_current_fence(control_fencing_token, Utc::now())
                .await?;
            self.drain_revocations(control_fencing_token, Some(&shutdown))
                .await?;
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
                notification = notifications.recv() => {
                    match notification {
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err(SessionLifecycleError::InvalidState);
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn drain_revocations(
        &self,
        control_fencing_token: i64,
        shutdown: Option<&CancellationToken>,
    ) -> Result<u64, SessionLifecycleError> {
        let store = AuthorizationRevocationStore::new(&self.pool);
        let mut consumed = 0;
        loop {
            if shutdown.is_some_and(CancellationToken::is_cancelled) {
                return Ok(consumed);
            }
            let Some(claim) = store
                .claim_next(
                    CONSUMER_NAME,
                    &self.lease_owner,
                    Duration::from_secs(CLAIM_SECONDS),
                )
                .await?
            else {
                return Ok(consumed);
            };
            let event_id = claim.event.id;
            match self
                .apply_revocation(&claim.event, control_fencing_token)
                .await
            {
                Ok(()) => {
                    store
                        .acknowledge(event_id, CONSUMER_NAME, &self.lease_owner)
                        .await?;
                    consumed += 1;
                }
                Err(error) => {
                    let _ = store
                        .fail_claim(
                            event_id,
                            CONSUMER_NAME,
                            &self.lease_owner,
                            "live_session_revocation_failed",
                        )
                        .await;
                    return Err(error);
                }
            }
        }
    }

    pub(crate) async fn apply_revocation(
        &self,
        event: &AuthorizationRevocationEvent,
        control_fencing_token: i64,
    ) -> Result<(), SessionLifecycleError> {
        let sessions = self
            .repository
            .list_nonterminal(MAX_ACTIVE_SESSIONS)
            .await?;
        for session in sessions {
            if event_matches_session(event, &session)? {
                let forced_error_code = (event.event_type
                    == AuthorizationRevocationEventType::ProviderPolicyChanged)
                    .then_some("LIVE_DESTINATION_POLICY_CHANGED");
                self.reconcile_session_with_reason(
                    session,
                    control_fencing_token,
                    Utc::now(),
                    forced_error_code,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn reconcile_session(
        &self,
        session: SessionRecord,
        control_fencing_token: i64,
        now: DateTime<Utc>,
    ) -> Result<ReconcileOutcome, SessionLifecycleError> {
        self.reconcile_session_with_reason(session, control_fencing_token, now, None)
            .await
    }

    async fn reconcile_session_with_reason(
        &self,
        mut session: SessionRecord,
        control_fencing_token: i64,
        now: DateTime<Utc>,
        forced_error_code: Option<&'static str>,
    ) -> Result<ReconcileOutcome, SessionLifecycleError> {
        let mut outcome = ReconcileOutcome::default();
        for _ in 0..MAX_CAS_RETRIES {
            if session.state.is_terminal() {
                return Ok(outcome);
            }
            if session.control_fencing_token > control_fencing_token {
                return Err(SessionRepositoryError::FenceLost.into());
            }
            if session.control_fencing_token < control_fencing_token {
                match self
                    .repository
                    .adopt_control_fence(
                        session.owner,
                        session.id,
                        session.revision,
                        session.control_fencing_token,
                        control_fencing_token,
                        now,
                    )
                    .await
                {
                    Ok(adopted) => {
                        session = adopted.session;
                        outcome.adopted = true;
                    }
                    Err(SessionRepositoryError::RevisionChanged) => {
                        session = self.reload(&session).await?;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            }

            let invalid_code = match forced_error_code {
                Some(error_code) => Some(error_code),
                None => self.current_invalid_reason(&session, now).await?,
            };
            let Some(error_code) = invalid_code else {
                return Ok(outcome);
            };
            let state = if error_code == "LIVE_SESSION_EXPIRED" {
                SessionState::Expired
            } else {
                SessionState::Failed
            };
            match self
                .repository
                .terminate(
                    session.owner,
                    session.id,
                    session.revision,
                    control_fencing_token,
                    TerminalReason {
                        state,
                        error_code: Some(error_code.to_string()),
                        error_detail_redacted: None,
                    },
                    now,
                )
                .await
            {
                Ok(_) => {
                    outcome.terminated = true;
                    return Ok(outcome);
                }
                Err(SessionRepositoryError::RevisionChanged) => {
                    session = self.reload(&session).await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(SessionRepositoryError::RevisionChanged.into())
    }

    async fn reload(
        &self,
        session: &SessionRecord,
    ) -> Result<SessionRecord, SessionLifecycleError> {
        self.repository
            .get_owned(session.owner, session.id)
            .await?
            .ok_or(SessionLifecycleError::InvalidState)
    }

    async fn current_invalid_reason(
        &self,
        session: &SessionRecord,
        now: DateTime<Utc>,
    ) -> Result<Option<&'static str>, SessionLifecycleError> {
        if session.expires_at <= now || session.hard_expires_at <= now {
            return Ok(Some("LIVE_SESSION_EXPIRED"));
        }
        let principal = sqlx::query(
            "SELECT membership.role, profiles.profile_type
             FROM account_sessions AS sessions
             JOIN home_members AS membership
               ON membership.home_id = sessions.home_id
              AND membership.user_id = sessions.user_id
              AND membership.status = 'active'
             JOIN profiles
               ON profiles.id = sessions.active_profile_id
              AND profiles.home_id = sessions.home_id
             WHERE sessions.id = $1 AND sessions.user_id = $2 AND sessions.home_id = $3
               AND sessions.active_profile_id = $4 AND sessions.revoked_at IS NULL
               AND sessions.expires_at > $5
               AND ((profiles.profile_type = 'account' AND profiles.user_id = sessions.user_id)
                    OR (profiles.profile_type = 'managed' AND profiles.user_id IS NULL))
             LIMIT 1",
        )
        .bind(session.owner.account_session_id.to_string())
        .bind(session.owner.user_id.to_string())
        .bind(session.owner.home_id.to_string())
        .bind(session.owner.profile_id.to_string())
        .bind(now.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        let Some(principal) = principal else {
            return Ok(Some("LIVE_AUTHORIZATION_REVOKED"));
        };
        let role = HomeRole::try_from(principal.try_get::<String, _>("role")?.as_str())
            .map_err(|_| SessionLifecycleError::InvalidState)?;
        let profile_type =
            ProfileType::try_from(principal.try_get::<String, _>("profile_type")?.as_str())
                .map_err(|_| SessionLifecycleError::InvalidState)?;
        let authorization = AuthorizationRepository::new(&self.pool)
            .load_effective(session.owner.profile_id, role, profile_type)
            .await?;
        if !authorization.capabilities.contains(Capability::LivePlay) {
            return Ok(Some("LIVE_AUTHORIZATION_REVOKED"));
        }
        let visibility = LiveProviderGrantRepository::new(self.pool.clone())
            .visibility(
                session.owner.home_id,
                session.owner.profile_id,
                role,
                profile_type,
                session.owner.provider_id,
                LiveProviderAccess::Play,
            )
            .await?;
        if !visibility.allowed {
            return Ok(Some("LIVE_PROVIDER_GRANT_REVOKED"));
        }

        let provider = match self
            .provider_client
            .directory()
            .get(session.owner.provider_id)
            .await
        {
            Ok(provider) => provider,
            Err(error)
                if matches!(
                    error.code(),
                    ProviderDirectoryErrorCode::NotReady
                        | ProviderDirectoryErrorCode::InvalidSnapshot
                        | ProviderDirectoryErrorCode::RevisionChanged
                ) =>
            {
                return Ok(Some("LIVE_PROVIDER_UNAVAILABLE"));
            }
            Err(error) => return Err(error.into()),
        };
        let secrets = self
            .repository
            .decrypt_secrets(session.owner, session.id)
            .await?;
        let binding: ProviderBinding = serde_json::from_slice(secrets.descriptor.expose_secret())
            .map_err(|_| SessionLifecycleError::InvalidState)?;
        if binding.provider_revision != format!("{:?}", provider.revision) {
            return Ok(Some("LIVE_PROVIDER_REVISION_CHANGED"));
        }
        Ok(None)
    }
}

#[derive(Debug, Default)]
struct ReconcileOutcome {
    adopted: bool,
    terminated: bool,
}

#[derive(Deserialize)]
struct ProviderBinding {
    provider_revision: String,
}

fn event_matches_session(
    event: &AuthorizationRevocationEvent,
    session: &SessionRecord,
) -> Result<bool, SessionLifecycleError> {
    validate_event_subject(event)?;
    if session.owner.home_id != event.home_id {
        return Ok(false);
    }
    let matches = match event.event_type {
        AuthorizationRevocationEventType::AccountSessionRevoked => {
            session.owner.account_session_id == required(event.account_session_id)?
        }
        AuthorizationRevocationEventType::AccountRevoked => {
            session.owner.user_id == parse_subject_uuid(event)?
        }
        AuthorizationRevocationEventType::ProfileSwitched
        | AuthorizationRevocationEventType::ProfileDisabled
        | AuthorizationRevocationEventType::AuthorizationContextChanged => {
            session.owner.profile_id == required(event.profile_id)?
        }
        AuthorizationRevocationEventType::ProviderDisabled => {
            session.owner.provider_id == required(event.provider_id)?
        }
        AuthorizationRevocationEventType::ProviderPolicyChanged => {
            session.owner.provider_id == required(event.provider_id)?
        }
        AuthorizationRevocationEventType::ProviderGrantRevoked => {
            session.owner.profile_id == required(event.profile_id)?
                && session.owner.provider_id == required(event.provider_id)?
        }
    };
    Ok(matches)
}

fn validate_event_subject(
    event: &AuthorizationRevocationEvent,
) -> Result<(), SessionLifecycleError> {
    use crate::auth::revocation::AuthorizationSubjectType;

    let valid = match event.event_type {
        AuthorizationRevocationEventType::AccountSessionRevoked => {
            event.subject_type == AuthorizationSubjectType::AccountSession
                && event.account_session_id == Some(parse_subject_uuid(event)?)
        }
        AuthorizationRevocationEventType::AccountRevoked => {
            event.subject_type == AuthorizationSubjectType::Account
                && event.account_session_id.is_none()
                && event.profile_id.is_none()
                && event.provider_id.is_none()
                && event.grant_id.is_none()
                && parse_subject_uuid(event).is_ok()
        }
        AuthorizationRevocationEventType::ProfileSwitched => {
            event.subject_type == AuthorizationSubjectType::AccountSession
                && event.account_session_id == Some(parse_subject_uuid(event)?)
                && event.profile_id.is_some()
        }
        AuthorizationRevocationEventType::ProfileDisabled
        | AuthorizationRevocationEventType::AuthorizationContextChanged => {
            event.subject_type == AuthorizationSubjectType::Profile
                && event.profile_id == Some(parse_subject_uuid(event)?)
        }
        AuthorizationRevocationEventType::ProviderDisabled
        | AuthorizationRevocationEventType::ProviderPolicyChanged => {
            event.subject_type == AuthorizationSubjectType::Provider
                && event.provider_id == Some(parse_subject_uuid(event)?)
        }
        AuthorizationRevocationEventType::ProviderGrantRevoked => {
            event.subject_type == AuthorizationSubjectType::ProviderGrant
                && event.grant_id == Some(parse_subject_uuid(event)?)
                && event.profile_id.is_some()
                && event.provider_id.is_some()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(SessionLifecycleError::InvalidState)
    }
}

fn required(value: Option<Uuid>) -> Result<Uuid, SessionLifecycleError> {
    value.ok_or(SessionLifecycleError::InvalidState)
}

fn parse_subject_uuid(event: &AuthorizationRevocationEvent) -> Result<Uuid, SessionLifecycleError> {
    Uuid::parse_str(&event.subject_id).map_err(|_| SessionLifecycleError::InvalidState)
}

impl fmt::Debug for LiveSessionLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveSessionLifecycle")
            .field("lease_owner", &self.lease_owner)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use std::sync::Arc;

    use crate::{
        auth::revocation::{AuthorizationSubjectType, NewAuthorizationRevocation},
        config::DatabaseConfig,
        db::Database,
        live::{
            catalog::LiveProviderGrantRepository,
            config::{LiveProviderLimits, LiveSessionLimits},
            crypto::{LiveCrypto, LiveMasterKey, SecretBytes},
            diagnostics::LiveRedactor,
            provider::{LiveProviderClient, tests::seed_provider},
            session::{DeliveryMode, NewSession, SessionOwner, SessionProtocol},
        },
    };

    use super::*;

    fn session(created_at: DateTime<Utc>) -> SessionRecord {
        SessionRecord {
            id: Uuid::new_v4(),
            owner: SessionOwner {
                user_id: Uuid::new_v4(),
                home_id: Uuid::new_v4(),
                profile_id: Uuid::new_v4(),
                account_session_id: Uuid::new_v4(),
                provider_id: Uuid::new_v4(),
            },
            delivery_mode: DeliveryMode::ClientDirect,
            protocol: SessionProtocol::Hls,
            state: SessionState::Playing,
            revision: 4,
            token_revision: 1,
            control_fencing_token: 2,
            source_index: 0,
            failover_count: 0,
            refresh_count: 0,
            egress_binding_id: None,
            remux_job_id: None,
            created_at,
            last_heartbeat_at: created_at,
            expires_at: created_at + chrono::Duration::minutes(2),
            hard_expires_at: created_at + chrono::Duration::hours(1),
            ended_at: None,
            error_code: None,
            error_detail_redacted: None,
        }
    }

    fn event(
        session: &SessionRecord,
        event_type: AuthorizationRevocationEventType,
        occurred_at: DateTime<Utc>,
    ) -> AuthorizationRevocationEvent {
        let generated_grant_id = Uuid::new_v4();
        let (subject_type, subject_id, account_session_id, profile_id, provider_id, grant_id) =
            match event_type {
                AuthorizationRevocationEventType::AccountSessionRevoked
                | AuthorizationRevocationEventType::ProfileSwitched => (
                    AuthorizationSubjectType::AccountSession,
                    session.owner.account_session_id.to_string(),
                    Some(session.owner.account_session_id),
                    Some(session.owner.profile_id),
                    None,
                    None,
                ),
                AuthorizationRevocationEventType::AccountRevoked => (
                    AuthorizationSubjectType::Account,
                    session.owner.user_id.to_string(),
                    None,
                    None,
                    None,
                    None,
                ),
                AuthorizationRevocationEventType::ProfileDisabled
                | AuthorizationRevocationEventType::AuthorizationContextChanged => (
                    AuthorizationSubjectType::Profile,
                    session.owner.profile_id.to_string(),
                    None,
                    Some(session.owner.profile_id),
                    None,
                    None,
                ),
                AuthorizationRevocationEventType::ProviderDisabled
                | AuthorizationRevocationEventType::ProviderPolicyChanged => (
                    AuthorizationSubjectType::Provider,
                    session.owner.provider_id.to_string(),
                    None,
                    None,
                    Some(session.owner.provider_id),
                    None,
                ),
                AuthorizationRevocationEventType::ProviderGrantRevoked => (
                    AuthorizationSubjectType::ProviderGrant,
                    generated_grant_id.to_string(),
                    None,
                    Some(session.owner.profile_id),
                    Some(session.owner.provider_id),
                    Some(generated_grant_id),
                ),
            };
        AuthorizationRevocationEvent {
            id: Uuid::new_v4(),
            home_id: session.owner.home_id,
            event_type,
            subject_type,
            subject_id,
            actor_user_id: None,
            account_session_id,
            profile_id,
            provider_id,
            grant_id,
            reason_code: "test".to_string(),
            payload: json!({}),
            occurred_at,
            retain_until: occurred_at + chrono::Duration::days(30),
            published_at: None,
            publish_attempts: 0,
            last_error_redacted: None,
        }
    }

    #[test]
    fn p12_revocation_matching_is_scoped_for_every_subject_type() -> anyhow::Result<()> {
        let created_at = Utc.with_ymd_and_hms(2026, 7, 12, 20, 0, 0).unwrap();
        let session = session(created_at);
        for event_type in [
            AuthorizationRevocationEventType::AccountSessionRevoked,
            AuthorizationRevocationEventType::ProfileSwitched,
            AuthorizationRevocationEventType::ProfileDisabled,
            AuthorizationRevocationEventType::AuthorizationContextChanged,
            AuthorizationRevocationEventType::ProviderDisabled,
            AuthorizationRevocationEventType::ProviderPolicyChanged,
            AuthorizationRevocationEventType::ProviderGrantRevoked,
        ] {
            assert!(event_matches_session(
                &event(
                    &session,
                    event_type,
                    created_at + chrono::Duration::seconds(1)
                ),
                &session
            )?);
        }
        let account = event(
            &session,
            AuthorizationRevocationEventType::AccountRevoked,
            created_at + chrono::Duration::seconds(1),
        );
        assert!(event_matches_session(&account, &session)?);

        let mut other_home = event(
            &session,
            AuthorizationRevocationEventType::ProviderDisabled,
            created_at + chrono::Duration::seconds(1),
        );
        other_home.home_id = Uuid::new_v4();
        assert!(!event_matches_session(&other_home, &session)?);
        Ok(())
    }

    #[test]
    fn p12_revocation_matching_rejects_malformed_required_subjects() {
        let created_at = Utc.with_ymd_and_hms(2026, 7, 12, 20, 0, 0).unwrap();
        let session = session(created_at);
        let mut malformed = event(
            &session,
            AuthorizationRevocationEventType::ProviderGrantRevoked,
            created_at + chrono::Duration::seconds(1),
        );
        malformed.provider_id = None;
        assert!(matches!(
            event_matches_session(&malformed, &session),
            Err(SessionLifecycleError::InvalidState)
        ));

        let _constructor_contract = NewAuthorizationRevocation::account_session(
            session.owner.home_id,
            session.owner.account_session_id,
            Some(session.owner.profile_id),
            "test",
        );
    }

    #[tokio::test]
    async fn p12_takeover_reconciles_fences_and_provider_and_grant_revocations()
    -> anyhow::Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite:file:/tmp/p12-live-lifecycle-{}?mode=memory&cache=shared",
                Uuid::new_v4()
            ),
            max_connections: 8,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, 45_678, json!({})).await?;
        let provider_client = Arc::new(LiveProviderClient::new_for_test(
            pool.clone(),
            LiveProviderLimits::default(),
            Arc::new(LiveRedactor::default()),
        )?);
        let provider_revision = format!(
            "{:?}",
            provider_client.directory().get(provider_id).await?.revision
        );
        let crypto = Arc::new(LiveCrypto::new(
            "p12",
            [LiveMasterKey::new("p12", [12; 32])?],
        )?);
        let limits = LiveSessionLimits {
            per_user: 4,
            server_total: 16,
            lease_seconds: 90,
            max_lifetime_seconds: 3_600,
            startup_queue_seconds: 15,
        };
        let repository = Arc::new(LiveSessionRepository::new(pool.clone(), crypto, limits));

        let now = Utc::now();
        let user_id = Uuid::new_v4();
        let home_id = Uuid::new_v4();
        let account_profile_id = Uuid::new_v4();
        let managed_profile_id = Uuid::new_v4();
        let account_session_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, 'test')")
            .bind(user_id.to_string())
            .bind(format!("{user_id}@example.invalid"))
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO homes (id, owner_user_id, name) VALUES ($1, $2, 'P12')")
            .bind(home_id.to_string())
            .bind(user_id.to_string())
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, 'owner', 'active')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO profiles (id, home_id, user_id, profile_type, display_name, is_default)
             VALUES ($1, $2, $3, 'account', 'Owner', TRUE),
                    ($4, $2, NULL, 'managed', 'Managed', FALSE)",
        )
        .bind(account_profile_id.to_string())
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .bind(managed_profile_id.to_string())
        .execute(&pool)
        .await?;
        for profile_id in [account_profile_id, managed_profile_id] {
            sqlx::query(
                "INSERT INTO profile_authorization_revisions (profile_id, home_id)
                 VALUES ($1, $2)",
            )
            .bind(profile_id.to_string())
            .bind(home_id.to_string())
            .execute(&pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO account_sessions (id, user_id, home_id, active_profile_id, expires_at)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_session_id.to_string())
        .bind(user_id.to_string())
        .bind(home_id.to_string())
        .bind(account_profile_id.to_string())
        .bind((now + chrono::Duration::days(1)).to_rfc3339())
        .execute(&pool)
        .await?;
        sqlx::query(
            "UPDATE live_control_server_leases
             SET owner_instance_id = $1, fencing_token = 1, acquired_at = $2,
                 heartbeat_at = $2, expires_at = $3
             WHERE lease_name = 'live-control-v1'",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(now.to_rfc3339())
        .bind((now + chrono::Duration::hours(1)).to_rfc3339())
        .execute(&pool)
        .await?;

        let account_owner = SessionOwner {
            user_id,
            home_id,
            profile_id: account_profile_id,
            account_session_id,
            provider_id,
        };
        let descriptor =
            || SecretBytes::from_utf8(json!({"provider_revision": provider_revision}).to_string());
        let account_session = repository
            .create(
                NewSession {
                    owner: account_owner,
                    item_key: SecretBytes::from_utf8("account-item".to_string()),
                    stream_option_key: SecretBytes::from_utf8("account-stream".to_string()),
                    item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                    descriptor: descriptor(),
                    delivery_mode: DeliveryMode::ClientDirect,
                    protocol: SessionProtocol::Hls,
                    source_index: 0,
                    control_fencing_token: 1,
                    now,
                },
                None,
            )
            .await?;
        let hard_expired = repository
            .create(
                NewSession {
                    owner: account_owner,
                    item_key: SecretBytes::from_utf8("expired-item".to_string()),
                    stream_option_key: SecretBytes::from_utf8("expired-stream".to_string()),
                    item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                    descriptor: descriptor(),
                    delivery_mode: DeliveryMode::ClientDirect,
                    protocol: SessionProtocol::Hls,
                    source_index: 0,
                    control_fencing_token: 1,
                    now,
                },
                None,
            )
            .await?;
        sqlx::query(
            "UPDATE live_playback_sessions
             SET expires_at = $1, hard_expires_at = $1
             WHERE id = $2",
        )
        .bind((now - chrono::Duration::seconds(1)).to_rfc3339())
        .bind(hard_expired.session.id.to_string())
        .execute(&pool)
        .await?;

        sqlx::query(
            "UPDATE live_control_server_leases
             SET owner_instance_id = $1, fencing_token = 2, acquired_at = $2,
                 heartbeat_at = $2, expires_at = $3
             WHERE lease_name = 'live-control-v1' AND fencing_token = 1",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(Utc::now().to_rfc3339())
        .bind((Utc::now() + chrono::Duration::hours(1)).to_rfc3339())
        .execute(&pool)
        .await?;
        let lifecycle = LiveSessionLifecycle::new(
            pool.clone(),
            Arc::clone(&repository),
            Arc::clone(&provider_client),
            Uuid::new_v4(),
        );
        let takeover = lifecycle.reconcile_startup(2).await?;
        assert_eq!(takeover.adopted, 2);
        assert_eq!(takeover.terminated, 1);
        let adopted = repository
            .get_owned(account_owner, account_session.session.id)
            .await?
            .expect("adopted session");
        assert_eq!(adopted.control_fencing_token, 2);
        let expired_terminal = repository
            .get_owned(account_owner, hard_expired.session.id)
            .await?
            .expect("hard-expired terminal diagnostic row");
        assert_eq!(expired_terminal.state, SessionState::Expired);
        assert!(matches!(
            repository
                .heartbeat(account_owner, adopted.id, adopted.revision, 1, Utc::now())
                .await,
            Err(SessionRepositoryError::FenceLost)
        ));

        let unchanged_event = NewAuthorizationRevocation {
            home_id,
            event_type: AuthorizationRevocationEventType::AuthorizationContextChanged,
            subject_type: AuthorizationSubjectType::Profile,
            subject_id: account_profile_id.to_string(),
            actor_user_id: Some(user_id),
            account_session_id: None,
            profile_id: Some(account_profile_id),
            provider_id: None,
            grant_id: None,
            reason_code: "p12_non_contraction".to_string(),
            payload: json!({}),
        };
        AuthorizationRevocationStore::new(&pool)
            .append(&unchanged_event, None)
            .await?;
        let unchanged = lifecycle.reconcile_startup(2).await?;
        assert_eq!(unchanged.revocations_consumed, 1);
        assert_eq!(unchanged.terminated, 0);

        sqlx::query(
            "UPDATE extensions SET enabled = FALSE
             WHERE extension_id = (
                 SELECT instances.extension_id FROM extension_instances AS instances
                 JOIN providers ON providers.instance_id = instances.instance_id
                 WHERE providers.provider_id = $1
             )",
        )
        .bind(provider_id.to_string())
        .execute(&pool)
        .await?;
        let provider_event = NewAuthorizationRevocation {
            home_id,
            event_type: AuthorizationRevocationEventType::ProviderDisabled,
            subject_type: AuthorizationSubjectType::Provider,
            subject_id: provider_id.to_string(),
            actor_user_id: Some(user_id),
            account_session_id: None,
            profile_id: None,
            provider_id: Some(provider_id),
            grant_id: None,
            reason_code: "p12_provider_disabled".to_string(),
            payload: json!({}),
        };
        AuthorizationRevocationStore::new(&pool)
            .append(&provider_event, None)
            .await?;
        let disabled = lifecycle.reconcile_startup(2).await?;
        assert_eq!(disabled.terminated, 1);
        let terminated = repository
            .get_owned(account_owner, account_session.session.id)
            .await?
            .expect("terminal diagnostic row");
        assert_eq!(terminated.state, SessionState::Failed);
        assert_eq!(
            terminated.error_code.as_deref(),
            Some("LIVE_PROVIDER_UNAVAILABLE")
        );

        sqlx::query(
            "UPDATE extensions SET enabled = TRUE
             WHERE extension_id = (
                 SELECT instances.extension_id FROM extension_instances AS instances
                 JOIN providers ON providers.instance_id = instances.instance_id
                 WHERE providers.provider_id = $1
             )",
        )
        .bind(provider_id.to_string())
        .execute(&pool)
        .await?;
        sqlx::query("UPDATE account_sessions SET active_profile_id = $1 WHERE id = $2")
            .bind(managed_profile_id.to_string())
            .bind(account_session_id.to_string())
            .execute(&pool)
            .await?;
        let managed_provider_revision = format!(
            "{:?}",
            provider_client.directory().get(provider_id).await?.revision
        );
        let grants = LiveProviderGrantRepository::new(pool.clone());
        grants
            .set_grant(
                user_id,
                &json!({"userId": user_id, "role": "owner"}).to_string(),
                managed_profile_id,
                provider_id,
                true,
                true,
                None,
                None,
            )
            .await?;
        let managed_owner = SessionOwner {
            profile_id: managed_profile_id,
            ..account_owner
        };
        let managed_session = repository
            .create(
                NewSession {
                    owner: managed_owner,
                    item_key: SecretBytes::from_utf8("managed-item".to_string()),
                    stream_option_key: SecretBytes::from_utf8("managed-stream".to_string()),
                    item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                    descriptor: SecretBytes::from_utf8(
                        json!({"provider_revision": managed_provider_revision}).to_string(),
                    ),
                    delivery_mode: DeliveryMode::ClientDirect,
                    protocol: SessionProtocol::Hls,
                    source_index: 0,
                    control_fencing_token: 2,
                    now: Utc::now(),
                },
                None,
            )
            .await?;
        let revoked = grants
            .set_grant(
                user_id,
                &json!({"userId": user_id, "role": "owner"}).to_string(),
                managed_profile_id,
                provider_id,
                false,
                false,
                None,
                None,
            )
            .await?;
        assert!(revoked.revocation_event_id.is_some());
        lifecycle.reconcile_startup(2).await?;
        let managed_terminal = repository
            .get_owned(managed_owner, managed_session.session.id)
            .await?
            .expect("managed terminal diagnostic row");
        assert_eq!(managed_terminal.state, SessionState::Failed);
        assert_eq!(
            managed_terminal.error_code.as_deref(),
            Some("LIVE_PROVIDER_GRANT_REVOKED")
        );

        sqlx::query("UPDATE account_sessions SET active_profile_id = $1 WHERE id = $2")
            .bind(account_profile_id.to_string())
            .bind(account_session_id.to_string())
            .execute(&pool)
            .await?;
        let revoked_account_session = repository
            .create(
                NewSession {
                    owner: account_owner,
                    item_key: SecretBytes::from_utf8("revoked-account-item".to_string()),
                    stream_option_key: SecretBytes::from_utf8("revoked-account-stream".to_string()),
                    item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                    descriptor: SecretBytes::from_utf8(
                        json!({"provider_revision": managed_provider_revision}).to_string(),
                    ),
                    delivery_mode: DeliveryMode::ClientDirect,
                    protocol: SessionProtocol::Hls,
                    source_index: 0,
                    control_fencing_token: 2,
                    now: Utc::now(),
                },
                None,
            )
            .await?;
        sqlx::query(
            "UPDATE account_sessions
             SET revoked_at = CURRENT_TIMESTAMP, revoked_reason = 'p12_explicit_revoke'
             WHERE id = $1",
        )
        .bind(account_session_id.to_string())
        .execute(&pool)
        .await?;
        AuthorizationRevocationStore::new(&pool)
            .append(
                &NewAuthorizationRevocation::account_session(
                    home_id,
                    account_session_id,
                    Some(account_profile_id),
                    "p12_explicit_revoke",
                ),
                None,
            )
            .await?;
        lifecycle.reconcile_startup(2).await?;
        let account_terminal = repository
            .get_owned(account_owner, revoked_account_session.session.id)
            .await?
            .expect("revoked account-session diagnostic row");
        assert_eq!(account_terminal.state, SessionState::Failed);
        assert_eq!(
            account_terminal.error_code.as_deref(),
            Some("LIVE_AUTHORIZATION_REVOKED")
        );

        let local_playback_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM playback_sessions")
                .fetch_one(&pool)
                .await?;
        let media_file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&pool)
            .await?;
        assert_eq!(local_playback_count, 0);
        assert_eq!(media_file_count, 0);
        Ok(())
    }
}
