use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::live::{
    contract::SensitiveString,
    crypto::{EnvelopeContext, EnvelopePurpose, LiveCrypto, LiveCryptoError, SecretBytes},
};

const MAX_OPAQUE_KEY_BYTES: usize = 2_048;
const MAX_LOCAL_ID_BYTES: usize = 2_048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveArtworkKind {
    Poster,
    Background,
    Logo,
}

impl LiveArtworkKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Poster => "poster",
            Self::Background => "background",
            Self::Logo => "logo",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivePublicKeyScope {
    pub home_id: Uuid,
    pub profile_id: Uuid,
    pub authorization_revision: i64,
}

pub struct OpenedArtworkKey {
    pub provider_id: Uuid,
    pub item_id: String,
    pub kind: LiveArtworkKind,
    pub source: SensitiveString,
}

impl std::fmt::Debug for OpenedArtworkKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenedArtworkKey")
            .field("provider_id", &self.provider_id)
            .field("item_id", &self.item_id)
            .field("kind", &self.kind)
            .field("source", &"<sensitive>")
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum LivePublicKeyError {
    #[error("invalid Live public key input")]
    InvalidInput,
    #[error("Live public key is invalid or expired")]
    InvalidKey,
    #[error("Live public key cryptography failed")]
    Crypto(#[from] LiveCryptoError),
}

#[derive(Clone)]
pub struct LivePublicKeyCodec {
    crypto: Arc<LiveCrypto>,
}

impl LivePublicKeyCodec {
    pub fn new(crypto: Arc<LiveCrypto>) -> Self {
        Self { crypto }
    }

    pub fn seal_item(
        &self,
        provider_id: Uuid,
        item_id: &str,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        self.seal(
            EnvelopePurpose::CatalogItemKey,
            provider_id,
            "item",
            PublicKeyPayload::Item {
                provider_id,
                scope: scope.into(),
                item_id: bounded_id(item_id)?,
                expires_at: expires(now, Duration::hours(6))?,
            },
        )
    }

    pub fn open_item(
        &self,
        key: &str,
        provider_id: Uuid,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        match self.open(
            EnvelopePurpose::CatalogItemKey,
            provider_id,
            "item",
            key,
            now,
        )? {
            PublicKeyPayload::Item {
                provider_id: actual_provider,
                scope: actual_scope,
                item_id,
                ..
            } if actual_provider == provider_id && actual_scope == scope.into() => Ok(item_id),
            _ => Err(LivePublicKeyError::InvalidKey),
        }
    }

    pub fn seal_cursor(
        &self,
        provider_id: Uuid,
        catalog_id: &str,
        cursor: &str,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        self.seal(
            EnvelopePurpose::CatalogCursor,
            provider_id,
            "cursor",
            PublicKeyPayload::Cursor {
                provider_id,
                scope: scope.into(),
                catalog_id: bounded_id(catalog_id)?,
                cursor: bounded_id(cursor)?,
                expires_at: expires(now, Duration::minutes(15))?,
            },
        )
    }

    pub fn open_cursor(
        &self,
        key: &str,
        provider_id: Uuid,
        catalog_id: &str,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        match self.open(
            EnvelopePurpose::CatalogCursor,
            provider_id,
            "cursor",
            key,
            now,
        )? {
            PublicKeyPayload::Cursor {
                provider_id: actual_provider,
                scope: actual_scope,
                catalog_id: actual_catalog,
                cursor,
                ..
            } if actual_provider == provider_id
                && actual_scope == scope.into()
                && actual_catalog == catalog_id =>
            {
                Ok(cursor)
            }
            _ => Err(LivePublicKeyError::InvalidKey),
        }
    }

    pub fn seal_stream(
        &self,
        provider_id: Uuid,
        item_id: &str,
        stream_id: &str,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        self.seal(
            EnvelopePurpose::StreamOptionKey,
            provider_id,
            "stream",
            PublicKeyPayload::Stream {
                provider_id,
                scope: scope.into(),
                item_id: bounded_id(item_id)?,
                stream_id: bounded_id(stream_id)?,
                expires_at: expires(now, Duration::minutes(15))?,
            },
        )
    }

    pub fn open_stream(
        &self,
        key: &str,
        provider_id: Uuid,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<(String, String), LivePublicKeyError> {
        match self.open(
            EnvelopePurpose::StreamOptionKey,
            provider_id,
            "stream",
            key,
            now,
        )? {
            PublicKeyPayload::Stream {
                provider_id: actual_provider,
                scope: actual_scope,
                item_id,
                stream_id,
                ..
            } if actual_provider == provider_id && actual_scope == scope.into() => {
                Ok((item_id, stream_id))
            }
            _ => Err(LivePublicKeyError::InvalidKey),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn seal_source(
        &self,
        session_id: Uuid,
        provider_id: Uuid,
        provider_revision: &str,
        source_id: &str,
        session_revision: i64,
        expires_at: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        if session_revision < 1
            || provider_revision.len() != 64
            || !provider_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(LivePublicKeyError::InvalidInput);
        }
        self.seal_with_record(
            EnvelopePurpose::SourceKey,
            &session_id.to_string(),
            "source",
            PublicKeyPayload::Source {
                session_id,
                provider_id,
                provider_revision: provider_revision.to_ascii_lowercase(),
                source_id: bounded_id(source_id)?,
                session_revision,
                expires_at,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_source(
        &self,
        key: &str,
        session_id: Uuid,
        provider_id: Uuid,
        provider_revision: &str,
        session_revision: i64,
        now: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        match self.open_with_record(
            EnvelopePurpose::SourceKey,
            &session_id.to_string(),
            "source",
            key,
            now,
        )? {
            PublicKeyPayload::Source {
                session_id: actual_session,
                provider_id: actual_provider,
                provider_revision: actual_provider_revision,
                source_id,
                session_revision: actual_session_revision,
                ..
            } if actual_session == session_id
                && actual_provider == provider_id
                && actual_provider_revision == provider_revision
                && actual_session_revision == session_revision =>
            {
                Ok(source_id)
            }
            _ => Err(LivePublicKeyError::InvalidKey),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn seal_artwork(
        &self,
        provider_id: Uuid,
        item_id: &str,
        kind: LiveArtworkKind,
        source: &str,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<String, LivePublicKeyError> {
        if source.is_empty() || source.len() > 8_192 {
            return Err(LivePublicKeyError::InvalidInput);
        }
        self.seal_with_record(
            EnvelopePurpose::ArtworkKey,
            &artwork_record_id(scope),
            "artwork",
            PublicKeyPayload::Artwork {
                provider_id,
                scope: scope.into(),
                item_id: bounded_id(item_id)?,
                kind,
                source: source.to_string(),
                expires_at: expires(now, Duration::hours(24))?,
            },
        )
    }

    pub fn open_artwork(
        &self,
        key: &str,
        scope: LivePublicKeyScope,
        now: DateTime<Utc>,
    ) -> Result<OpenedArtworkKey, LivePublicKeyError> {
        match self.open_with_record(
            EnvelopePurpose::ArtworkKey,
            &artwork_record_id(scope),
            "artwork",
            key,
            now,
        )? {
            PublicKeyPayload::Artwork {
                provider_id: actual_provider,
                scope: actual_scope,
                item_id,
                kind,
                source,
                ..
            } if actual_scope == scope.into() => Ok(OpenedArtworkKey {
                provider_id: actual_provider,
                item_id,
                kind,
                source: SensitiveString::new(source),
            }),
            _ => Err(LivePublicKeyError::InvalidKey),
        }
    }

    fn seal(
        &self,
        purpose: EnvelopePurpose,
        provider_id: Uuid,
        column: &'static str,
        payload: PublicKeyPayload,
    ) -> Result<String, LivePublicKeyError> {
        let record_id = provider_id.to_string();
        self.seal_with_record(purpose, &record_id, column, payload)
    }

    fn seal_with_record(
        &self,
        purpose: EnvelopePurpose,
        record_id: &str,
        column: &'static str,
        payload: PublicKeyPayload,
    ) -> Result<String, LivePublicKeyError> {
        let encoded = serde_json::to_vec(&payload).map_err(|_| LivePublicKeyError::InvalidInput)?;
        let context = EnvelopeContext::new(purpose, "live_public_keys", &record_id, column)?;
        let envelope = self.crypto.encrypt(context, &SecretBytes::new(encoded))?;
        let key = general_purpose::URL_SAFE_NO_PAD.encode(envelope.as_bytes());
        if key.len() > MAX_OPAQUE_KEY_BYTES {
            return Err(LivePublicKeyError::InvalidInput);
        }
        Ok(key)
    }

    fn open(
        &self,
        purpose: EnvelopePurpose,
        provider_id: Uuid,
        column: &'static str,
        key: &str,
        now: DateTime<Utc>,
    ) -> Result<PublicKeyPayload, LivePublicKeyError> {
        let record_id = provider_id.to_string();
        self.open_with_record(purpose, &record_id, column, key, now)
    }

    fn open_with_record(
        &self,
        purpose: EnvelopePurpose,
        record_id: &str,
        column: &'static str,
        key: &str,
        now: DateTime<Utc>,
    ) -> Result<PublicKeyPayload, LivePublicKeyError> {
        if !(16..=MAX_OPAQUE_KEY_BYTES).contains(&key.len())
            || !key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
            })
        {
            return Err(LivePublicKeyError::InvalidKey);
        }
        let envelope = general_purpose::URL_SAFE_NO_PAD
            .decode(key)
            .map_err(|_| LivePublicKeyError::InvalidKey)?;
        let envelope =
            std::str::from_utf8(&envelope).map_err(|_| LivePublicKeyError::InvalidKey)?;
        let context = EnvelopeContext::new(purpose, "live_public_keys", &record_id, column)
            .map_err(|_| LivePublicKeyError::InvalidKey)?;
        let plaintext = self
            .crypto
            .decrypt(context, envelope)
            .map_err(|_| LivePublicKeyError::InvalidKey)?;
        let payload: PublicKeyPayload = serde_json::from_slice(plaintext.expose_secret())
            .map_err(|_| LivePublicKeyError::InvalidKey)?;
        if payload.expires_at() < now {
            return Err(LivePublicKeyError::InvalidKey);
        }
        Ok(payload)
    }
}

fn artwork_record_id(scope: LivePublicKeyScope) -> String {
    format!(
        "{}:{}:{}",
        scope.home_id, scope.profile_id, scope.authorization_revision
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredScope {
    home_id: Uuid,
    profile_id: Uuid,
    authorization_revision: i64,
}

impl From<LivePublicKeyScope> for StoredScope {
    fn from(value: LivePublicKeyScope) -> Self {
        Self {
            home_id: value.home_id,
            profile_id: value.profile_id,
            authorization_revision: value.authorization_revision,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "key_type", rename_all = "snake_case")]
enum PublicKeyPayload {
    Item {
        provider_id: Uuid,
        scope: StoredScope,
        item_id: String,
        expires_at: DateTime<Utc>,
    },
    Cursor {
        provider_id: Uuid,
        scope: StoredScope,
        catalog_id: String,
        cursor: String,
        expires_at: DateTime<Utc>,
    },
    Stream {
        provider_id: Uuid,
        scope: StoredScope,
        item_id: String,
        stream_id: String,
        expires_at: DateTime<Utc>,
    },
    Source {
        session_id: Uuid,
        provider_id: Uuid,
        provider_revision: String,
        source_id: String,
        session_revision: i64,
        expires_at: DateTime<Utc>,
    },
    Artwork {
        provider_id: Uuid,
        scope: StoredScope,
        item_id: String,
        kind: LiveArtworkKind,
        source: String,
        expires_at: DateTime<Utc>,
    },
}

impl PublicKeyPayload {
    const fn expires_at(&self) -> DateTime<Utc> {
        match self {
            Self::Item { expires_at, .. }
            | Self::Cursor { expires_at, .. }
            | Self::Stream { expires_at, .. }
            | Self::Source { expires_at, .. }
            | Self::Artwork { expires_at, .. } => *expires_at,
        }
    }
}

fn bounded_id(value: &str) -> Result<String, LivePublicKeyError> {
    if value.is_empty() || value.len() > MAX_LOCAL_ID_BYTES || value.chars().any(char::is_control) {
        return Err(LivePublicKeyError::InvalidInput);
    }
    Ok(value.to_string())
}

fn expires(now: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>, LivePublicKeyError> {
    now.checked_add_signed(duration)
        .ok_or(LivePublicKeyError::InvalidInput)
}
