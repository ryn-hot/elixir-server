use std::{collections::HashMap, sync::Arc};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration, Utc};
use rand::{RngCore, rngs::OsRng};
use tokio::sync::Mutex;
use uuid::Uuid;

const SETUP_TTL_MINUTES: i64 = 10;
const MAX_SESSIONS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountSetupState {
    Pending,
    Completing,
    Completed,
}

#[derive(Debug, Clone)]
pub struct AccountSetupSession {
    pub setup_id: Uuid,
    pub owner_user_id: Uuid,
    pub extension_id: String,
    pub extension_version: String,
    pub instance_id: Uuid,
    pub result_setting_ids: Vec<String>,
    pub expires_at: DateTime<Utc>,
    pub state: AccountSetupState,
    token_hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct StartedAccountSetup {
    pub token: String,
    pub session: AccountSetupSession,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountSetupSessionError {
    NotFound,
    Expired,
    Forbidden,
    AlreadyUsed,
    Capacity,
}

#[derive(Clone, Default)]
pub struct AccountSetupSessions {
    inner: Arc<Mutex<AccountSetupSessionsInner>>,
}

#[derive(Default)]
struct AccountSetupSessionsInner {
    sessions: HashMap<Uuid, AccountSetupSession>,
    token_index: HashMap<[u8; 32], Uuid>,
}

impl AccountSetupSessions {
    pub async fn start(
        &self,
        owner_user_id: Uuid,
        extension_id: String,
        extension_version: String,
        instance_id: Uuid,
        result_setting_ids: Vec<String>,
        now: DateTime<Utc>,
    ) -> Result<StartedAccountSetup, AccountSetupSessionError> {
        let mut inner = self.inner.lock().await;
        purge_expired(&mut inner, now);

        let replaced = inner
            .sessions
            .values()
            .filter(|session| {
                session.owner_user_id == owner_user_id
                    && session.extension_id == extension_id
                    && session.instance_id == instance_id
                    && session.state != AccountSetupState::Completed
            })
            .map(|session| session.setup_id)
            .collect::<Vec<_>>();
        for setup_id in replaced {
            remove_session(&mut inner, setup_id);
        }
        if inner.sessions.len() >= MAX_SESSIONS {
            return Err(AccountSetupSessionError::Capacity);
        }

        let mut token_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut token_bytes);
        let token = URL_SAFE_NO_PAD.encode(token_bytes);
        let token_hash = token_hash(&token);
        let session = AccountSetupSession {
            setup_id: Uuid::new_v4(),
            owner_user_id,
            extension_id,
            extension_version,
            instance_id,
            result_setting_ids,
            expires_at: now + Duration::minutes(SETUP_TTL_MINUTES),
            state: AccountSetupState::Pending,
            token_hash,
        };
        inner.token_index.insert(token_hash, session.setup_id);
        inner.sessions.insert(session.setup_id, session.clone());
        Ok(StartedAccountSetup { token, session })
    }

    pub async fn claim(
        &self,
        token: &str,
        now: DateTime<Utc>,
    ) -> Result<AccountSetupSession, AccountSetupSessionError> {
        let mut inner = self.inner.lock().await;
        let hash = token_hash(token);
        let setup_id = inner
            .token_index
            .get(&hash)
            .copied()
            .ok_or(AccountSetupSessionError::NotFound)?;
        if session_is_expired(&inner, setup_id, now) {
            remove_session(&mut inner, setup_id);
            return Err(AccountSetupSessionError::Expired);
        }
        purge_expired(&mut inner, now);
        let session = inner
            .sessions
            .get_mut(&setup_id)
            .ok_or(AccountSetupSessionError::NotFound)?;
        match session.state {
            AccountSetupState::Pending => {
                session.state = AccountSetupState::Completing;
                Ok(session.clone())
            }
            AccountSetupState::Completing | AccountSetupState::Completed => {
                Err(AccountSetupSessionError::AlreadyUsed)
            }
        }
    }

    pub async fn release(&self, setup_id: Uuid) {
        let mut inner = self.inner.lock().await;
        if let Some(session) = inner.sessions.get_mut(&setup_id)
            && session.state == AccountSetupState::Completing
        {
            session.state = AccountSetupState::Pending;
        }
    }

    pub async fn finish(
        &self,
        setup_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<(), AccountSetupSessionError> {
        let mut inner = self.inner.lock().await;
        if session_is_expired(&inner, setup_id, now) {
            remove_session(&mut inner, setup_id);
            return Err(AccountSetupSessionError::Expired);
        }
        purge_expired(&mut inner, now);
        let token_hash = {
            let session = inner
                .sessions
                .get_mut(&setup_id)
                .ok_or(AccountSetupSessionError::NotFound)?;
            if session.state != AccountSetupState::Completing {
                return Err(AccountSetupSessionError::AlreadyUsed);
            }
            session.state = AccountSetupState::Completed;
            session.token_hash
        };
        inner.token_index.remove(&token_hash);
        Ok(())
    }

    pub async fn status(
        &self,
        setup_id: Uuid,
        owner_user_id: Uuid,
        extension_id: &str,
        instance_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<AccountSetupState, AccountSetupSessionError> {
        let mut inner = self.inner.lock().await;
        {
            let session = inner
                .sessions
                .get(&setup_id)
                .ok_or(AccountSetupSessionError::NotFound)?;
            if session.owner_user_id != owner_user_id
                || session.extension_id != extension_id
                || session.instance_id != instance_id
            {
                return Err(AccountSetupSessionError::Forbidden);
            }
        }
        if session_is_expired(&inner, setup_id, now) {
            remove_session(&mut inner, setup_id);
            return Err(AccountSetupSessionError::Expired);
        }
        purge_expired(&mut inner, now);
        let session = inner
            .sessions
            .get(&setup_id)
            .ok_or(AccountSetupSessionError::NotFound)?;
        Ok(session.state)
    }
}

fn token_hash(token: &str) -> [u8; 32] {
    *blake3::hash(token.as_bytes()).as_bytes()
}

fn purge_expired(inner: &mut AccountSetupSessionsInner, now: DateTime<Utc>) {
    let expired = inner
        .sessions
        .values()
        .filter(|session| session.expires_at <= now)
        .map(|session| session.setup_id)
        .collect::<Vec<_>>();
    for setup_id in expired {
        remove_session(inner, setup_id);
    }
}

fn session_is_expired(
    inner: &AccountSetupSessionsInner,
    setup_id: Uuid,
    now: DateTime<Utc>,
) -> bool {
    inner
        .sessions
        .get(&setup_id)
        .is_some_and(|session| session.expires_at <= now)
}

fn remove_session(inner: &mut AccountSetupSessionsInner, setup_id: Uuid) {
    if let Some(session) = inner.sessions.remove(&setup_id) {
        inner.token_index.remove(&session.token_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture(store: &AccountSetupSessions, now: DateTime<Utc>) -> StartedAccountSetup {
        store
            .start(
                Uuid::new_v4(),
                "example.live".to_string(),
                "1.0.0".to_string(),
                Uuid::new_v4(),
                vec!["manifestUrl".to_string()],
                now,
            )
            .await
            .expect("start setup")
    }

    #[tokio::test]
    async fn setup_session_is_single_use_and_reports_completion() {
        let store = AccountSetupSessions::default();
        let now = Utc::now();
        let started = fixture(&store, now).await;
        let claimed = store.claim(&started.token, now).await.expect("claim");
        store.finish(claimed.setup_id, now).await.expect("finish");
        assert_eq!(
            store
                .status(
                    claimed.setup_id,
                    claimed.owner_user_id,
                    &claimed.extension_id,
                    claimed.instance_id,
                    now,
                )
                .await,
            Ok(AccountSetupState::Completed)
        );
        assert!(matches!(
            store.claim(&started.token, now).await,
            Err(AccountSetupSessionError::NotFound)
        ));
    }

    #[tokio::test]
    async fn setup_session_expires_and_is_bound_to_owner_and_instance() {
        let store = AccountSetupSessions::default();
        let now = Utc::now();
        let started = fixture(&store, now).await;
        let session = &started.session;
        assert_eq!(
            store
                .status(
                    session.setup_id,
                    Uuid::new_v4(),
                    &session.extension_id,
                    session.instance_id,
                    now,
                )
                .await,
            Err(AccountSetupSessionError::Forbidden)
        );
        assert_eq!(
            store
                .status(
                    session.setup_id,
                    session.owner_user_id,
                    &session.extension_id,
                    Uuid::new_v4(),
                    now,
                )
                .await,
            Err(AccountSetupSessionError::Forbidden)
        );
        assert!(matches!(
            store
                .claim(&started.token, now + Duration::minutes(11))
                .await,
            Err(AccountSetupSessionError::Expired)
        ));
    }

    #[tokio::test]
    async fn failed_completion_can_be_retried_without_replaying_success() {
        let store = AccountSetupSessions::default();
        let now = Utc::now();
        let started = fixture(&store, now).await;
        let session = store.claim(&started.token, now).await.expect("claim");
        store.release(session.setup_id).await;
        assert!(store.claim(&started.token, now).await.is_ok());
    }
}
