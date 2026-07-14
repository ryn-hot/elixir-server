use std::{fmt, sync::Arc};

use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::live::{
    contract::{
        ArtworkSource, CacheHint, CatalogDefinition, CatalogPage, CatalogPageRequest, CatalogSet,
        Fact, ItemDiagnostic, ItemMetadata, LiveItem, LiveItemStatus, MetaRequest, StreamChoice,
    },
    crypto::{EnvelopeContext, EnvelopePurpose, LiveCrypto, LiveCryptoError, SecretBytes},
    provider::LiveProviderSnapshot,
};

use super::grants::LiveProviderAccess;

const CACHE_PREFIX: &str = "live-cache-v1:";
const MAX_CACHE_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheOperation {
    Catalogs,
    Catalog,
    Meta,
}

impl CacheOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalogs => "catalogs",
            Self::Catalog => "catalog",
            Self::Meta => "meta",
        }
    }
}

impl TryFrom<&str> for CacheOperation {
    type Error = CatalogCacheError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "catalogs" => Ok(Self::Catalogs),
            "catalog" => Ok(Self::Catalog),
            "meta" => Ok(Self::Meta),
            _ => Err(CatalogCacheError::InvalidState),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityPartition {
    pub home_id: Uuid,
    pub profile_id: Uuid,
    pub authorization_revision: i64,
    pub access: LiveProviderAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum CacheRequest {
    Catalogs,
    Catalog { request: CatalogPageRequest },
    Meta { request: MetaRequest },
}

impl CacheRequest {
    pub const fn operation(&self) -> CacheOperation {
        match self {
            Self::Catalogs => CacheOperation::Catalogs,
            Self::Catalog { .. } => CacheOperation::Catalog,
            Self::Meta { .. } => CacheOperation::Meta,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CacheKey(String);

impl CacheKey {
    pub fn derive(
        provider: &LiveProviderSnapshot,
        request: &CacheRequest,
        locale: &str,
        timezone: &str,
        visibility: &VisibilityPartition,
    ) -> Result<Self, CatalogCacheError> {
        if locale.is_empty()
            || locale.len() > 64
            || timezone.is_empty()
            || timezone.len() > 128
            || visibility.authorization_revision < 1
        {
            return Err(CatalogCacheError::InvalidInput);
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Material<'a> {
            version: u8,
            provider_id: Uuid,
            provider_revision: String,
            request: &'a CacheRequest,
            locale: &'a str,
            timezone: &'a str,
            visibility: &'a VisibilityPartition,
        }
        let material = Material {
            version: 1,
            provider_id: provider.provider_id,
            provider_revision: blake3::Hash::from_bytes(*provider.revision.as_bytes())
                .to_hex()
                .to_string(),
            request,
            locale,
            timezone,
            visibility,
        };
        let encoded = serde_json::to_vec(&material).map_err(|_| CatalogCacheError::InvalidInput)?;
        let digest = blake3::hash(&encoded);
        Ok(Self(format!(
            "{CACHE_PREFIX}{}",
            general_purpose::URL_SAFE_NO_PAD.encode(digest.as_bytes())
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn for_test(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Debug for CacheKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("CacheKey").field(&self.0).finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogCacheValue {
    Catalogs(CatalogSet),
    Catalog(CatalogPage),
    Meta(ItemMetadata),
}

impl CatalogCacheValue {
    pub const fn operation(&self) -> CacheOperation {
        match self {
            Self::Catalogs(_) => CacheOperation::Catalogs,
            Self::Catalog(_) => CacheOperation::Catalog,
            Self::Meta(_) => CacheOperation::Meta,
        }
    }

    pub fn cache_hint(&self) -> &CacheHint {
        match self {
            Self::Catalogs(value) => &value.cache,
            Self::Catalog(value) => &value.cache,
            Self::Meta(value) => &value.cache,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheFreshness {
    Fresh,
    Stale,
}

#[derive(Debug, Clone)]
pub struct CatalogCacheEntry {
    pub key: CacheKey,
    pub provider_id: Uuid,
    pub operation: CacheOperation,
    pub value: Arc<CatalogCacheValue>,
    pub etag: Option<String>,
    pub fresh_until: DateTime<Utc>,
    pub stale_until: DateTime<Utc>,
    pub freshness: CacheFreshness,
}

#[derive(Debug, Error)]
pub enum CatalogCacheError {
    #[error("invalid Live catalog cache input")]
    InvalidInput,
    #[error("invalid persisted Live catalog cache state")]
    InvalidState,
    #[error("Live catalog cache payload exceeds its storage bound")]
    PayloadTooLarge,
    #[error("Live catalog cache key collision")]
    KeyCollision,
    #[error("Live catalog cache cryptography failed")]
    Crypto(#[from] LiveCryptoError),
    #[error("Live catalog cache database operation failed")]
    Storage(#[from] sqlx::Error),
}

#[derive(Clone)]
pub struct CatalogCacheRepository {
    pool: AnyPool,
    crypto: Arc<LiveCrypto>,
}

impl CatalogCacheRepository {
    pub fn new(pool: AnyPool, crypto: Arc<LiveCrypto>) -> Self {
        Self { pool, crypto }
    }

    pub async fn get(
        &self,
        key: &CacheKey,
        now: DateTime<Utc>,
    ) -> Result<Option<CatalogCacheEntry>, CatalogCacheError> {
        let row = sqlx::query(
            "SELECT cache_key, provider_id, operation, payload_json,
                    CAST(etag AS TEXT) AS etag,
                    CAST(fresh_until AS TEXT) AS fresh_until,
                    CAST(stale_until AS TEXT) AS stale_until
             FROM live_provider_cache
             WHERE cache_key = $1
             LIMIT 1",
        )
        .bind(key.as_str())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let stale_until = parse_timestamp(&row.try_get::<String, _>("stale_until")?)?;
        if stale_until < now {
            self.delete(key).await?;
            return Ok(None);
        }
        let persisted_key: String = row.try_get("cache_key")?;
        if persisted_key != key.as_str() {
            return Err(CatalogCacheError::KeyCollision);
        }
        let provider_id = parse_uuid(&row.try_get::<String, _>("provider_id")?)?;
        let operation_text: String = row.try_get("operation")?;
        let operation = CacheOperation::try_from(operation_text.as_str())?;
        let fresh_until = parse_timestamp(&row.try_get::<String, _>("fresh_until")?)?;
        let payload_json: String = row.try_get("payload_json")?;
        let value = decode_payload(&self.crypto, key, operation, payload_json.as_bytes())?;
        Ok(Some(CatalogCacheEntry {
            key: key.clone(),
            provider_id,
            operation,
            value: Arc::new(value),
            etag: row.try_get("etag")?,
            fresh_until,
            stale_until,
            freshness: if now <= fresh_until {
                CacheFreshness::Fresh
            } else {
                CacheFreshness::Stale
            },
        }))
    }

    pub async fn put(
        &self,
        key: &CacheKey,
        provider_id: Uuid,
        value: &CatalogCacheValue,
        now: DateTime<Utc>,
    ) -> Result<CatalogCacheEntry, CatalogCacheError> {
        let hint = value.cache_hint();
        let fresh_until = now
            .checked_add_signed(chrono::Duration::seconds(hint.max_age_seconds.into()))
            .ok_or(CatalogCacheError::InvalidInput)?;
        let stale_until = fresh_until
            .checked_add_signed(chrono::Duration::seconds(
                hint.stale_while_revalidate_seconds.into(),
            ))
            .ok_or(CatalogCacheError::InvalidInput)?;
        let payload = encode_payload(&self.crypto, key, value)?;
        if payload.len() > MAX_CACHE_PAYLOAD_BYTES {
            return Err(CatalogCacheError::PayloadTooLarge);
        }
        let operation = value.operation();
        let etag = hint.etag.clone();
        let result = sqlx::query(
            "INSERT INTO live_provider_cache (
                cache_key, provider_id, operation, payload_json, etag,
                fresh_until, stale_until
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT(cache_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                etag = excluded.etag,
                fresh_until = excluded.fresh_until,
                stale_until = excluded.stale_until,
                updated_at = CURRENT_TIMESTAMP
             WHERE live_provider_cache.provider_id = excluded.provider_id
               AND live_provider_cache.operation = excluded.operation",
        )
        .bind(key.as_str())
        .bind(provider_id.to_string())
        .bind(operation.as_str())
        .bind(&payload)
        .bind(etag.as_deref())
        .bind(fresh_until.to_rfc3339())
        .bind(stale_until.to_rfc3339())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(CatalogCacheError::KeyCollision);
        }
        Ok(CatalogCacheEntry {
            key: key.clone(),
            provider_id,
            operation,
            value: Arc::new(value.clone()),
            etag,
            fresh_until,
            stale_until,
            freshness: CacheFreshness::Fresh,
        })
    }

    pub async fn delete(&self, key: &CacheKey) -> Result<bool, CatalogCacheError> {
        Ok(
            sqlx::query("DELETE FROM live_provider_cache WHERE cache_key = $1")
                .bind(key.as_str())
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
    }

    pub async fn delete_provider(&self, provider_id: Uuid) -> Result<u64, CatalogCacheError> {
        Ok(
            sqlx::query("DELETE FROM live_provider_cache WHERE provider_id = $1")
                .bind(provider_id.to_string())
                .execute(&self.pool)
                .await?
                .rows_affected(),
        )
    }

    pub async fn cleanup_expired(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, CatalogCacheError> {
        if !(1..=1_000).contains(&limit) {
            return Err(CatalogCacheError::InvalidInput);
        }
        Ok(sqlx::query(
            "DELETE FROM live_provider_cache
             WHERE cache_key IN (
                 SELECT cache_key
                 FROM live_provider_cache
                 WHERE stale_until < $1
                 ORDER BY stale_until, cache_key
                 LIMIT $2
             )",
        )
        .bind(now.to_rfc3339())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?
        .rows_affected())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredPayload {
    Catalogs {
        schema_version: u8,
        catalogs: Vec<CatalogDefinition>,
        cache: CacheHint,
    },
    Catalog {
        schema_version: u8,
        items: Vec<StoredLiveItem>,
        next_cursor: Option<String>,
        cache: CacheHint,
        diagnostics: Vec<ItemDiagnostic>,
    },
    Meta {
        schema_version: u8,
        item: StoredLiveItem,
        streams: Vec<StreamChoice>,
        cache: CacheHint,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredLiveItem {
    id: String,
    item_type: crate::live::contract::LiveItemType,
    title: String,
    subtitle: Option<String>,
    description: Option<String>,
    status: LiveItemStatus,
    starts_at: Option<DateTime<Utc>>,
    ends_at: Option<DateTime<Utc>>,
    poster_envelope: Option<String>,
    background_envelope: Option<String>,
    logo_envelope: Option<String>,
    categories: Vec<String>,
    badges: Vec<String>,
    facts: Vec<Fact>,
}

fn encode_payload(
    crypto: &LiveCrypto,
    key: &CacheKey,
    value: &CatalogCacheValue,
) -> Result<String, CatalogCacheError> {
    let payload = match value {
        CatalogCacheValue::Catalogs(value) => StoredPayload::Catalogs {
            schema_version: 1,
            catalogs: value.catalogs.clone(),
            cache: value.cache.clone(),
        },
        CatalogCacheValue::Catalog(value) => StoredPayload::Catalog {
            schema_version: 1,
            items: value
                .items
                .iter()
                .enumerate()
                .map(|(index, item)| seal_item(crypto, key, index, item))
                .collect::<Result<_, _>>()?,
            next_cursor: value.next_cursor.clone(),
            cache: value.cache.clone(),
            diagnostics: value.diagnostics.clone(),
        },
        CatalogCacheValue::Meta(value) => StoredPayload::Meta {
            schema_version: 1,
            item: seal_item(crypto, key, 0, &value.item)?,
            streams: value.streams.clone(),
            cache: value.cache.clone(),
        },
    };
    serde_json::to_string(&payload).map_err(|_| CatalogCacheError::InvalidState)
}

fn decode_payload(
    crypto: &LiveCrypto,
    key: &CacheKey,
    operation: CacheOperation,
    payload: &[u8],
) -> Result<CatalogCacheValue, CatalogCacheError> {
    if payload.len() > MAX_CACHE_PAYLOAD_BYTES {
        return Err(CatalogCacheError::PayloadTooLarge);
    }
    let payload: StoredPayload =
        serde_json::from_slice(payload).map_err(|_| CatalogCacheError::InvalidState)?;
    match (operation, payload) {
        (
            CacheOperation::Catalogs,
            StoredPayload::Catalogs {
                schema_version: 1,
                catalogs,
                cache,
            },
        ) => Ok(CatalogCacheValue::Catalogs(CatalogSet { catalogs, cache })),
        (
            CacheOperation::Catalog,
            StoredPayload::Catalog {
                schema_version: 1,
                items,
                next_cursor,
                cache,
                diagnostics,
            },
        ) => Ok(CatalogCacheValue::Catalog(CatalogPage {
            items: items
                .into_iter()
                .enumerate()
                .map(|(index, item)| open_item(crypto, key, index, item))
                .collect::<Result<_, _>>()?,
            next_cursor,
            cache,
            diagnostics,
        })),
        (
            CacheOperation::Meta,
            StoredPayload::Meta {
                schema_version: 1,
                item,
                streams,
                cache,
            },
        ) => Ok(CatalogCacheValue::Meta(ItemMetadata {
            item: open_item(crypto, key, 0, item)?,
            streams,
            cache,
        })),
        _ => Err(CatalogCacheError::InvalidState),
    }
}

fn seal_item(
    crypto: &LiveCrypto,
    key: &CacheKey,
    index: usize,
    item: &LiveItem,
) -> Result<StoredLiveItem, CatalogCacheError> {
    Ok(StoredLiveItem {
        id: item.id.clone(),
        item_type: item.item_type,
        title: item.title.clone(),
        subtitle: item.subtitle.clone(),
        description: item.description.clone(),
        status: item.status,
        starts_at: item.starts_at,
        ends_at: item.ends_at,
        poster_envelope: seal_artwork(crypto, key, index, "poster", item.poster.as_ref())?,
        background_envelope: seal_artwork(
            crypto,
            key,
            index,
            "background",
            item.background.as_ref(),
        )?,
        logo_envelope: seal_artwork(crypto, key, index, "logo", item.logo.as_ref())?,
        categories: item.categories.clone(),
        badges: item.badges.clone(),
        facts: item.facts.clone(),
    })
}

fn open_item(
    crypto: &LiveCrypto,
    key: &CacheKey,
    index: usize,
    item: StoredLiveItem,
) -> Result<LiveItem, CatalogCacheError> {
    Ok(LiveItem {
        id: item.id,
        item_type: item.item_type,
        title: item.title,
        subtitle: item.subtitle,
        description: item.description,
        status: item.status,
        starts_at: item.starts_at,
        ends_at: item.ends_at,
        poster: open_artwork(crypto, key, index, "poster", item.poster_envelope)?,
        background: open_artwork(crypto, key, index, "background", item.background_envelope)?,
        logo: open_artwork(crypto, key, index, "logo", item.logo_envelope)?,
        categories: item.categories,
        badges: item.badges,
        facts: item.facts,
    })
}

fn seal_artwork(
    crypto: &LiveCrypto,
    key: &CacheKey,
    index: usize,
    kind: &str,
    artwork: Option<&ArtworkSource>,
) -> Result<Option<String>, CatalogCacheError> {
    let Some(artwork) = artwork else {
        return Ok(None);
    };
    let column = artwork_column(index, kind);
    let context = EnvelopeContext::new(
        EnvelopePurpose::ArtworkKey,
        "live_provider_cache",
        key.as_str(),
        &column,
    )?;
    Ok(Some(crypto.encrypt(
        context,
        &SecretBytes::from_utf8(artwork.expose().to_string()),
    )?))
}

fn open_artwork(
    crypto: &LiveCrypto,
    key: &CacheKey,
    index: usize,
    kind: &str,
    envelope: Option<String>,
) -> Result<Option<ArtworkSource>, CatalogCacheError> {
    let Some(envelope) = envelope else {
        return Ok(None);
    };
    let column = artwork_column(index, kind);
    let context = EnvelopeContext::new(
        EnvelopePurpose::ArtworkKey,
        "live_provider_cache",
        key.as_str(),
        &column,
    )?;
    let plaintext = crypto.decrypt(context, &envelope)?;
    let value = String::from_utf8(plaintext.expose_secret().to_vec())
        .map_err(|_| CatalogCacheError::InvalidState)?;
    Ok(Some(ArtworkSource::new(value)))
}

fn artwork_column(index: usize, kind: &str) -> String {
    format!("item-{index}-{kind}")
}

fn parse_uuid(value: &str) -> Result<Uuid, CatalogCacheError> {
    Uuid::parse_str(value).map_err(|_| CatalogCacheError::InvalidState)
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, CatalogCacheError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc())
        })
        .map_err(|_| CatalogCacheError::InvalidState)
}
