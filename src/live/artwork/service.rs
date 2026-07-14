use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    hash::{Hash, Hasher},
    io::Cursor,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::body::Bytes;
use image::{ImageFormat, ImageReader, Limits};
use reqwest::header::CONTENT_TYPE;
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::live::{
    catalog::{LiveArtworkKind, LivePublicKeyScope, OpenedArtworkKey},
    contract::SensitiveString,
    upstream::{
        DestinationPolicy, DestinationRule, DnsResolver, LocalDestinationDenylist, NetworkScope,
        SafeRequestHeaders, SystemDnsResolver, UpstreamErrorCode, UpstreamFetcher, UpstreamLimits,
        UpstreamMethod,
    },
};

const MAX_POLICY_ROWS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveArtworkErrorCode {
    InvalidRequest,
    PolicyDenied,
    UpstreamUnavailable,
    UpstreamStatus,
    MediaTypeRejected,
    ImageRejected,
    ImageTooLarge,
    DecodeTimeout,
    Cancelled,
    Internal,
}

impl LiveArtworkErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "LIVE_ARTWORK_INVALID_REQUEST",
            Self::PolicyDenied => "LIVE_ARTWORK_POLICY_DENIED",
            Self::UpstreamUnavailable => "LIVE_ARTWORK_UPSTREAM_UNAVAILABLE",
            Self::UpstreamStatus => "LIVE_ARTWORK_UPSTREAM_STATUS",
            Self::MediaTypeRejected => "LIVE_ARTWORK_MEDIA_TYPE_REJECTED",
            Self::ImageRejected => "LIVE_ARTWORK_IMAGE_REJECTED",
            Self::ImageTooLarge => "LIVE_ARTWORK_IMAGE_TOO_LARGE",
            Self::DecodeTimeout => "LIVE_ARTWORK_DECODE_TIMEOUT",
            Self::Cancelled => "LIVE_ARTWORK_CANCELLED",
            Self::Internal => "LIVE_ARTWORK_INTERNAL",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LiveArtworkError {
    code: LiveArtworkErrorCode,
}

impl LiveArtworkError {
    pub const fn code(self) -> LiveArtworkErrorCode {
        self.code
    }

    const fn new(code: LiveArtworkErrorCode) -> Self {
        Self { code }
    }

    #[cfg(test)]
    pub(crate) const fn new_for_service_initialization() -> Self {
        Self::new(LiveArtworkErrorCode::Internal)
    }
}

impl fmt::Debug for LiveArtworkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveArtworkError")
            .field("code", &self.code.as_str())
            .finish()
    }
}

impl fmt::Display for LiveArtworkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for LiveArtworkError {}

type Result<T> = std::result::Result<T, LiveArtworkError>;

#[derive(Debug, Clone)]
pub struct LiveArtworkLimits {
    pub max_encoded_bytes: u64,
    pub max_width: u32,
    pub max_height: u32,
    pub max_pixels: u64,
    pub max_decode_alloc_bytes: u64,
    pub decode_timeout: Duration,
    pub decode_concurrency: usize,
    pub cache_ttl: Duration,
    pub cache_max_entries: usize,
    pub cache_max_bytes: usize,
}

impl Default for LiveArtworkLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 8 * 1024 * 1024,
            max_width: 8_192,
            max_height: 8_192,
            max_pixels: 40_000_000,
            max_decode_alloc_bytes: 192 * 1024 * 1024,
            decode_timeout: Duration::from_secs(3),
            decode_concurrency: 4,
            cache_ttl: Duration::from_secs(60 * 60),
            cache_max_entries: 2_048,
            cache_max_bytes: 128 * 1024 * 1024,
        }
    }
}

impl LiveArtworkLimits {
    fn validate(&self) -> Result<()> {
        if !(1_024..=32 * 1024 * 1024).contains(&self.max_encoded_bytes)
            || !(1..=16_384).contains(&self.max_width)
            || !(1..=16_384).contains(&self.max_height)
            || !(1..=100_000_000).contains(&self.max_pixels)
            || !(4 * 1024 * 1024..=512 * 1024 * 1024).contains(&self.max_decode_alloc_bytes)
            || self.decode_timeout.is_zero()
            || self.decode_timeout > Duration::from_secs(30)
            || !(1..=32).contains(&self.decode_concurrency)
            || self.cache_ttl.is_zero()
            || self.cache_ttl > Duration::from_secs(24 * 60 * 60)
            || !(1..=100_000).contains(&self.cache_max_entries)
            || self.cache_max_bytes < 1024 * 1024
            || u64::try_from(self.cache_max_bytes).unwrap_or(u64::MAX) > 4 * 1024 * 1024 * 1024
        {
            return Err(LiveArtworkError::new(LiveArtworkErrorCode::InvalidRequest));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ArtworkFetchRequest {
    pub provider_id: Uuid,
    pub item_id: String,
    pub kind: LiveArtworkKind,
    pub source: SensitiveString,
    pub scope: LivePublicKeyScope,
}

impl ArtworkFetchRequest {
    pub fn from_opened(value: OpenedArtworkKey, scope: LivePublicKeyScope) -> Self {
        Self {
            provider_id: value.provider_id,
            item_id: value.item_id,
            kind: value.kind,
            source: value.source,
            scope,
        }
    }
}

impl fmt::Debug for ArtworkFetchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtworkFetchRequest")
            .field("provider_id", &self.provider_id)
            .field("item", &"<redacted>")
            .field("kind", &self.kind)
            .field("source", &"<sensitive>")
            .field("scope", &self.scope)
            .finish()
    }
}

#[derive(Clone)]
pub struct LiveArtwork {
    pub bytes: Bytes,
    pub content_type: &'static str,
    pub etag: String,
    pub width: u32,
    pub height: u32,
    pub cache_hit: bool,
}

impl fmt::Debug for LiveArtwork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveArtwork")
            .field("bytes", &self.bytes.len())
            .field("content_type", &self.content_type)
            .field("width", &self.width)
            .field("height", &self.height)
            .field("cache_hit", &self.cache_hit)
            .finish()
    }
}

#[derive(Clone)]
pub struct LiveArtworkService {
    inner: Arc<ArtworkInner>,
}

impl fmt::Debug for LiveArtworkService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveArtworkService")
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}

struct ArtworkInner {
    pool: sqlx::AnyPool,
    fetcher: Arc<UpstreamFetcher>,
    limits: LiveArtworkLimits,
    cache: Mutex<ArtworkCache>,
    inflight: AsyncMutex<HashMap<ArtworkCacheKey, Arc<Inflight>>>,
    decode_admission: Arc<Semaphore>,
    allow_fixture_loopback: bool,
}

impl LiveArtworkService {
    pub fn new(pool: sqlx::AnyPool) -> Result<Self> {
        let resolver = Arc::new(
            SystemDnsResolver::new(Duration::from_secs(5))
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?,
        );
        Self::with_resolver(pool, resolver, LiveArtworkLimits::default(), false)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        pool: sqlx::AnyPool,
        resolver: Arc<dyn DnsResolver>,
        limits: LiveArtworkLimits,
    ) -> Result<Self> {
        Self::with_resolver(pool, resolver, limits, true)
    }

    fn with_resolver(
        pool: sqlx::AnyPool,
        resolver: Arc<dyn DnsResolver>,
        limits: LiveArtworkLimits,
        allow_fixture_loopback: bool,
    ) -> Result<Self> {
        limits.validate()?;
        let upstream_limits = UpstreamLimits {
            connect_timeout: Duration::from_secs(5),
            header_timeout: Duration::from_secs(8),
            idle_timeout: Duration::from_secs(8),
            total_timeout: Duration::from_secs(20),
            max_response_bytes: limits.max_encoded_bytes,
            max_response_headers: 64,
            max_response_header_bytes: 32 * 1024,
            max_redirects: 5,
        };
        let fetcher = UpstreamFetcher::new(resolver, upstream_limits)
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
        let cache_max_bytes = limits.cache_max_bytes;
        let cache_max_entries = limits.cache_max_entries;
        let decode_concurrency = limits.decode_concurrency;
        Ok(Self {
            inner: Arc::new(ArtworkInner {
                pool,
                fetcher: Arc::new(fetcher),
                limits,
                cache: Mutex::new(ArtworkCache::new(cache_max_entries, cache_max_bytes)),
                inflight: AsyncMutex::new(HashMap::new()),
                decode_admission: Arc::new(Semaphore::new(decode_concurrency)),
                allow_fixture_loopback,
            }),
        })
    }

    pub async fn fetch(
        &self,
        request: ArtworkFetchRequest,
        cancellation: &CancellationToken,
    ) -> Result<LiveArtwork> {
        validate_request(&request)?;
        let policy = self.inner.load_policy(&request).await?;
        let key = ArtworkCacheKey::new(&request, policy.digest);
        if let Some(cached) = self.inner.cache_get(&key)? {
            return Ok(cached.as_response(true));
        }

        let (flight, created) = {
            let mut flights = self.inner.inflight.lock().await;
            if let Some(existing) = flights.get(&key) {
                (existing.clone(), false)
            } else {
                let flight = Arc::new(Inflight::new());
                flights.insert(key.clone(), flight.clone());
                (flight, true)
            }
        };
        let waiter = FlightWaiter::new(flight.clone());
        if created {
            let inner = self.inner.clone();
            let task_key = key.clone();
            let task_flight = flight.clone();
            tokio::spawn(async move {
                let authority_request = request.clone();
                let result = inner
                    .fetch_uncached(request, policy.policy, task_flight.cancellation.clone())
                    .await;
                let result = match result {
                    Ok(artwork) => match inner.load_policy(&authority_request).await {
                        Ok(current) if current.digest == task_key.policy_hash => {
                            inner.cache_insert(task_key.clone(), artwork)
                        }
                        Ok(_) => Err(LiveArtworkError::new(LiveArtworkErrorCode::PolicyDenied)),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                };
                task_flight.complete(result);
                let mut flights = inner.inflight.lock().await;
                if flights
                    .get(&task_key)
                    .is_some_and(|current| Arc::ptr_eq(current, &task_flight))
                {
                    flights.remove(&task_key);
                }
            });
        }
        waiter
            .wait(cancellation)
            .await
            .map(|value| value.as_response(false))
    }

    pub async fn expire_batch(&self, limit: usize) -> Result<usize> {
        if !(1..=10_000).contains(&limit) {
            return Err(LiveArtworkError::new(LiveArtworkErrorCode::InvalidRequest));
        }
        self.inner
            .cache
            .lock()
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))
            .map(|mut cache| cache.expire_batch(limit))
    }

    pub async fn evict_provider(&self, home_id: Uuid, provider_id: Uuid) -> Result<usize> {
        self.inner
            .cache
            .lock()
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))
            .map(|mut cache| cache.evict_provider(home_id, provider_id))
    }

    pub async fn reconcile_removed_providers_batch(&self, limit: usize) -> Result<usize> {
        if !(1..=1_000).contains(&limit) {
            return Err(LiveArtworkError::new(LiveArtworkErrorCode::InvalidRequest));
        }
        let pairs = self
            .inner
            .cache
            .lock()
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?
            .provider_pairs(limit);
        let mut removed = 0usize;
        for (home_id, provider_id) in pairs {
            let exists: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM providers WHERE provider_id = $1")
                    .bind(provider_id.to_string())
                    .fetch_one(&self.inner.pool)
                    .await
                    .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            if exists == 0 {
                removed = removed.saturating_add(self.evict_provider(home_id, provider_id).await?);
            }
        }
        Ok(removed)
    }
}

impl ArtworkInner {
    async fn load_policy(&self, request: &ArtworkFetchRequest) -> Result<PolicySnapshot> {
        let rows = sqlx::query(
            "SELECT scheme, normalized_host, port, exact_path, network_scope, \
                    CAST(CASE WHEN allow_fetch THEN 1 ELSE 0 END AS BIGINT) AS allow_fetch, \
                    revision \
             FROM live_provider_destination_rules \
             WHERE home_id = $1 AND provider_id = $2 \
             ORDER BY scheme, normalized_host, port, exact_path, network_scope",
        )
        .bind(request.scope.home_id.to_string())
        .bind(request.provider_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
        if rows.len() > MAX_POLICY_ROWS {
            return Err(LiveArtworkError::new(LiveArtworkErrorCode::PolicyDenied));
        }
        let mut rules = Vec::new();
        let mut digest = Sha256::new();
        let mut allow_http = false;
        for row in rows {
            let scheme: String = row
                .try_get("scheme")
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            let host: String = row
                .try_get("normalized_host")
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            let port: i64 = row
                .try_get("port")
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            let path: String = row
                .try_get("exact_path")
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            let scope: String = row
                .try_get("network_scope")
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            let allow_fetch: i64 = row
                .try_get("allow_fetch")
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            let revision: i64 = row
                .try_get("revision")
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
            if allow_fetch != 1 || scope != "public" {
                continue;
            }
            let port = u16::try_from(port)
                .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::PolicyDenied))?;
            let rule =
                DestinationRule::new(&scheme, &host, port, &path, NetworkScope::Public, true)
                    .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::PolicyDenied))?;
            allow_http |= scheme == "http";
            for value in [&scheme, &host, &path] {
                digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
                digest.update(value.as_bytes());
            }
            digest.update(port.to_be_bytes());
            digest.update(revision.to_be_bytes());
            rules.push(rule);
        }
        if rules.is_empty() {
            return Err(LiveArtworkError::new(LiveArtworkErrorCode::PolicyDenied));
        }
        let policy = DestinationPolicy::new(
            rules,
            Default::default(),
            allow_http,
            LocalDestinationDenylist::empty(),
        )
        .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::PolicyDenied))?;
        #[cfg(test)]
        let policy = if self.allow_fixture_loopback {
            policy.allow_fixture_loopback()
        } else {
            policy
        };
        #[cfg(not(test))]
        debug_assert!(!self.allow_fixture_loopback);
        Ok(PolicySnapshot {
            policy,
            digest: digest.finalize().into(),
        })
    }

    async fn fetch_uncached(
        &self,
        request: ArtworkFetchRequest,
        policy: DestinationPolicy,
        cancellation: CancellationToken,
    ) -> Result<CachedArtwork> {
        let mut headers = SafeRequestHeaders::new();
        headers
            .insert("accept", "image/webp,image/png,image/jpeg;q=0.9")
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?;
        let upstream = self
            .fetcher
            .fetch(
                crate::live::upstream::FetchRequest::new(
                    request.source,
                    UpstreamMethod::Get,
                    policy,
                    cancellation.clone(),
                )
                .with_safe_headers(headers),
            )
            .await
            .map_err(map_upstream_error)?;
        if upstream.status() != reqwest::StatusCode::OK {
            return Err(LiveArtworkError::new(LiveArtworkErrorCode::UpstreamStatus));
        }
        let content_types = upstream.headers().get_all(CONTENT_TYPE);
        if content_types.iter().count() != 1 {
            return Err(LiveArtworkError::new(
                LiveArtworkErrorCode::MediaTypeRejected,
            ));
        }
        let content_type = content_types
            .iter()
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(accepted_content_type)
            .ok_or_else(|| LiveArtworkError::new(LiveArtworkErrorCode::MediaTypeRejected))?;
        let body = upstream.collect().await.map_err(map_upstream_error)?;
        let bytes = Bytes::from(body.into_bytes());
        let (width, height) = self
            .decode_validate(bytes.clone(), content_type, &cancellation)
            .await?;
        let content_hash = Sha256::digest(&bytes);
        Ok(CachedArtwork {
            bytes,
            content_type,
            etag: format!("\"{}\"", hex(&content_hash)),
            width,
            height,
            expires_at: Instant::now() + self.limits.cache_ttl,
        })
    }

    async fn decode_validate(
        &self,
        bytes: Bytes,
        content_type: &'static str,
        cancellation: &CancellationToken,
    ) -> Result<(u32, u32)> {
        let decode_deadline = Instant::now() + self.limits.decode_timeout;
        let permit = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(LiveArtworkError::new(LiveArtworkErrorCode::Cancelled));
            }
            permit = tokio::time::timeout(
                self.limits.decode_timeout,
                self.decode_admission.clone().acquire_owned(),
            ) => {
                permit
                    .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::DecodeTimeout))?
                    .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))?
            }
        };
        let decode_remaining = decode_deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| LiveArtworkError::new(LiveArtworkErrorCode::DecodeTimeout))?;
        let limits = self.limits.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            validate_decoded_image(&bytes, content_type, &limits)
        });
        tokio::pin!(task);
        tokio::select! {
            _ = cancellation.cancelled() => {
                task.abort();
                Err(LiveArtworkError::new(LiveArtworkErrorCode::Cancelled))
            }
            result = tokio::time::timeout(decode_remaining, &mut task) => match result {
                Ok(Ok(value)) => value,
                Ok(Err(_)) => Err(LiveArtworkError::new(LiveArtworkErrorCode::Internal)),
                Err(_) => {
                    task.abort();
                    Err(LiveArtworkError::new(LiveArtworkErrorCode::DecodeTimeout))
                }
            }
        }
    }

    fn cache_get(&self, key: &ArtworkCacheKey) -> Result<Option<Arc<CachedArtwork>>> {
        self.cache
            .lock()
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))
            .map(|mut cache| cache.get(key))
    }

    fn cache_insert(
        &self,
        key: ArtworkCacheKey,
        artwork: CachedArtwork,
    ) -> Result<Arc<CachedArtwork>> {
        self.cache
            .lock()
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))
            .map(|mut cache| cache.insert(key, artwork))
    }
}

struct PolicySnapshot {
    policy: DestinationPolicy,
    digest: [u8; 32],
}

#[derive(Clone, Eq)]
struct ArtworkCacheKey {
    home_id: Uuid,
    profile_id: Uuid,
    authorization_revision: i64,
    provider_id: Uuid,
    kind: LiveArtworkKind,
    identity_hash: [u8; 32],
    policy_hash: [u8; 32],
}

impl ArtworkCacheKey {
    fn new(request: &ArtworkFetchRequest, policy_hash: [u8; 32]) -> Self {
        let mut identity = Sha256::new();
        identity.update(request.item_id.as_bytes());
        identity.update([0]);
        identity.update(request.source.expose().as_bytes());
        Self {
            home_id: request.scope.home_id,
            profile_id: request.scope.profile_id,
            authorization_revision: request.scope.authorization_revision,
            provider_id: request.provider_id,
            kind: request.kind,
            identity_hash: identity.finalize().into(),
            policy_hash,
        }
    }
}

impl PartialEq for ArtworkCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.home_id == other.home_id
            && self.profile_id == other.profile_id
            && self.authorization_revision == other.authorization_revision
            && self.provider_id == other.provider_id
            && self.kind == other.kind
            && self.identity_hash == other.identity_hash
            && self.policy_hash == other.policy_hash
    }
}

impl Hash for ArtworkCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.home_id.hash(state);
        self.profile_id.hash(state);
        self.authorization_revision.hash(state);
        self.provider_id.hash(state);
        self.kind.hash(state);
        self.identity_hash.hash(state);
        self.policy_hash.hash(state);
    }
}

struct CacheValue {
    artwork: Arc<CachedArtwork>,
    accessed: u64,
}

struct ArtworkCache {
    values: HashMap<ArtworkCacheKey, CacheValue>,
    bytes: usize,
    clock: u64,
    max_entries: usize,
    max_bytes: usize,
}

impl ArtworkCache {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            values: HashMap::new(),
            bytes: 0,
            clock: 0,
            max_entries,
            max_bytes,
        }
    }

    fn get(&mut self, key: &ArtworkCacheKey) -> Option<Arc<CachedArtwork>> {
        self.clock = self.clock.saturating_add(1);
        let now = Instant::now();
        let expired = self
            .values
            .get(key)
            .is_some_and(|value| value.artwork.expires_at <= now);
        if expired {
            self.remove(key);
            return None;
        }
        let value = self.values.get_mut(key)?;
        value.accessed = self.clock;
        Some(value.artwork.clone())
    }

    fn insert(&mut self, key: ArtworkCacheKey, artwork: CachedArtwork) -> Arc<CachedArtwork> {
        self.remove(&key);
        let artwork = Arc::new(artwork);
        self.bytes = self.bytes.saturating_add(artwork.bytes.len());
        self.clock = self.clock.saturating_add(1);
        self.values.insert(
            key,
            CacheValue {
                artwork: artwork.clone(),
                accessed: self.clock,
            },
        );
        while self.values.len() > self.max_entries || self.bytes > self.max_bytes {
            let Some(oldest) = self
                .values
                .iter()
                .min_by_key(|(_, value)| value.accessed)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
        artwork
    }

    fn expire_batch(&mut self, limit: usize) -> usize {
        let now = Instant::now();
        let keys = self
            .values
            .iter()
            .filter(|(_, value)| value.artwork.expires_at <= now)
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let count = keys.len();
        for key in keys {
            self.remove(&key);
        }
        count
    }

    fn evict_provider(&mut self, home_id: Uuid, provider_id: Uuid) -> usize {
        let keys = self
            .values
            .keys()
            .filter(|key| key.home_id == home_id && key.provider_id == provider_id)
            .cloned()
            .collect::<Vec<_>>();
        let count = keys.len();
        for key in keys {
            self.remove(&key);
        }
        count
    }

    fn provider_pairs(&self, limit: usize) -> Vec<(Uuid, Uuid)> {
        self.values
            .keys()
            .map(|key| (key.home_id, key.provider_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .take(limit)
            .collect()
    }

    fn remove(&mut self, key: &ArtworkCacheKey) {
        if let Some(value) = self.values.remove(key) {
            self.bytes = self.bytes.saturating_sub(value.artwork.bytes.len());
        }
    }
}

struct CachedArtwork {
    bytes: Bytes,
    content_type: &'static str,
    etag: String,
    width: u32,
    height: u32,
    expires_at: Instant,
}

impl CachedArtwork {
    fn as_response(&self, cache_hit: bool) -> LiveArtwork {
        LiveArtwork {
            bytes: self.bytes.clone(),
            content_type: self.content_type,
            etag: self.etag.clone(),
            width: self.width,
            height: self.height,
            cache_hit,
        }
    }
}

struct Inflight {
    waiters: AtomicUsize,
    cancellation: CancellationToken,
    result: Mutex<Option<Result<Arc<CachedArtwork>>>>,
    notify: Notify,
}

impl Inflight {
    fn new() -> Self {
        Self {
            waiters: AtomicUsize::new(0),
            cancellation: CancellationToken::new(),
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    fn complete(&self, result: Result<Arc<CachedArtwork>>) {
        if let Ok(mut slot) = self.result.lock() {
            *slot = Some(result);
        }
        self.notify.notify_waiters();
    }

    fn result(&self) -> Result<Option<Result<Arc<CachedArtwork>>>> {
        self.result
            .lock()
            .map(|slot| slot.clone())
            .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::Internal))
    }
}

struct FlightWaiter {
    flight: Arc<Inflight>,
}

impl FlightWaiter {
    fn new(flight: Arc<Inflight>) -> Self {
        flight.waiters.fetch_add(1, Ordering::AcqRel);
        Self { flight }
    }

    async fn wait(self, cancellation: &CancellationToken) -> Result<Arc<CachedArtwork>> {
        loop {
            let notified = self.flight.notify.notified();
            if let Some(result) = self.flight.result()? {
                return result;
            }
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(LiveArtworkError::new(LiveArtworkErrorCode::Cancelled));
                }
                _ = notified => {}
            }
        }
    }
}

impl Drop for FlightWaiter {
    fn drop(&mut self) {
        if self.flight.waiters.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.flight.cancellation.cancel();
        }
    }
}

fn validate_request(request: &ArtworkFetchRequest) -> Result<()> {
    if request.item_id.is_empty()
        || request.item_id.len() > 2_048
        || request.item_id.chars().any(char::is_control)
        || request.source.expose().is_empty()
        || request.source.expose().len() > 8_192
        || request.scope.authorization_revision < 1
    {
        return Err(LiveArtworkError::new(LiveArtworkErrorCode::InvalidRequest));
    }
    Ok(())
}

fn accepted_content_type(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "image/jpeg" | "image/jpg" => Some("image/jpeg"),
        "image/png" => Some("image/png"),
        "image/webp" => Some("image/webp"),
        _ => None,
    }
}

fn validate_decoded_image(
    bytes: &[u8],
    content_type: &'static str,
    limits: &LiveArtworkLimits,
) -> Result<(u32, u32)> {
    let expected = match content_type {
        "image/jpeg" => ImageFormat::Jpeg,
        "image/png" => ImageFormat::Png,
        "image/webp" => ImageFormat::WebP,
        _ => {
            return Err(LiveArtworkError::new(
                LiveArtworkErrorCode::MediaTypeRejected,
            ));
        }
    };
    let actual = image::guess_format(bytes)
        .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::ImageRejected))?;
    if actual != expected {
        return Err(LiveArtworkError::new(
            LiveArtworkErrorCode::MediaTypeRejected,
        ));
    }
    let mut decode_limits = Limits::default();
    decode_limits.max_image_width = Some(limits.max_width);
    decode_limits.max_image_height = Some(limits.max_height);
    decode_limits.max_alloc = Some(limits.max_decode_alloc_bytes);
    let mut dimensions_reader = ImageReader::with_format(Cursor::new(bytes), expected);
    dimensions_reader.limits(decode_limits.clone());
    let (width, height) = dimensions_reader
        .into_dimensions()
        .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::ImageRejected))?;
    let pixels = u64::from(width).saturating_mul(u64::from(height));
    if width == 0 || height == 0 || pixels > limits.max_pixels {
        return Err(LiveArtworkError::new(LiveArtworkErrorCode::ImageTooLarge));
    }
    let mut decode_reader = ImageReader::with_format(Cursor::new(bytes), expected);
    decode_reader.limits(decode_limits);
    let decoded = decode_reader
        .decode()
        .map_err(|_| LiveArtworkError::new(LiveArtworkErrorCode::ImageRejected))?;
    if decoded.width() != width || decoded.height() != height {
        return Err(LiveArtworkError::new(LiveArtworkErrorCode::ImageRejected));
    }
    Ok((width, height))
}

fn map_upstream_error(error: crate::live::upstream::UpstreamError) -> LiveArtworkError {
    let code = match error.code() {
        UpstreamErrorCode::Cancelled => LiveArtworkErrorCode::Cancelled,
        UpstreamErrorCode::BodyTooLarge => LiveArtworkErrorCode::ImageTooLarge,
        UpstreamErrorCode::DestinationForbidden
        | UpstreamErrorCode::SchemeForbidden
        | UpstreamErrorCode::PortForbidden
        | UpstreamErrorCode::HostForbidden
        | UpstreamErrorCode::AddressForbidden
        | UpstreamErrorCode::DnsMixedScope
        | UpstreamErrorCode::PrivateLanUnauthorized
        | UpstreamErrorCode::NetworkScopeMismatch
        | UpstreamErrorCode::RedirectDowngrade => LiveArtworkErrorCode::PolicyDenied,
        _ => LiveArtworkErrorCode::UpstreamUnavailable,
    };
    LiveArtworkError::new(code)
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}
