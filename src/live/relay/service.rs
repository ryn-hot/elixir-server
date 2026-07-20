use std::{fmt, net::IpAddr, sync::Arc, time::Duration};

use axum::http::{
    HeaderMap, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED},
};
use chrono::Utc;
use dashmap::DashMap;
use reqwest::Url;
use sqlx::Row;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::live::{
    config::LiveRelayLimits,
    contract::{CredentialAuthority, SensitiveString, SourceDescriptor, StreamProtocol},
    egress::{LiveEgressError, LiveEgressService},
    provider::LiveProviderClient,
    session::{
        DeliveryMode, LiveSessionRepository, SessionOwner, SessionProtocol, SessionRecord,
        StoredSessionDescriptor,
    },
    upstream::{
        CredentialSet, DestinationPolicy, DestinationRule, FetchRequest, LocalDestinationDenylist,
        NetworkScope, PrivateLanGate, SafeRequestHeaders, SystemDnsResolver, UpstreamError,
        UpstreamFetcher, UpstreamLimits, UpstreamMethod, UpstreamResponse,
    },
};

use super::coalesce::{CoalescedManifest, ManifestFlightKey, ManifestRequestCoalescer};
use super::hls::{
    HlsManifestScope, HlsResourceDescriptor, HlsResourceId, HlsResourceKind, HlsResourceLimits,
    HlsResourceMap, HlsRewriteConfig, HlsRewriteError, HlsRewriter,
};

const MAX_POLICY_ROWS: usize = 256;
const MAX_MANIFEST_BYTES: u64 = 1_048_576;
const MAX_KEY_BYTES: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRelayBuildError {
    Resolver,
    Fetcher,
    Rewriter,
    LocalDenylist,
    InvalidCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRelayError {
    Unavailable,
    CapacityExhausted,
    SessionExpired,
    SessionMismatch,
    StaleControlFence,
    DescriptorInvalid,
    ProtocolUnsupported,
    ResourceExpired,
    ResourceKindMismatch,
    PolicyRejected,
    CredentialsRejected,
    ManifestRejected,
    ContentTypeRejected,
    RangeRejected,
    UpstreamStatus,
    Upstream(&'static str),
}

impl From<UpstreamError> for LiveRelayError {
    fn from(error: UpstreamError) -> Self {
        Self::Upstream(error.code().as_str())
    }
}

impl From<HlsRewriteError> for LiveRelayError {
    fn from(error: HlsRewriteError) -> Self {
        match error {
            HlsRewriteError::StaleControlFence => Self::StaleControlFence,
            HlsRewriteError::UnknownResource => Self::ResourceExpired,
            _ => Self::ManifestRejected,
        }
    }
}

pub enum LiveRelayPayloadBody {
    Bytes(Vec<u8>),
    Stream(UpstreamResponse),
}

impl fmt::Debug for LiveRelayPayloadBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(bytes) => formatter
                .debug_tuple("Bytes")
                .field(&format_args!("[{} BYTES]", bytes.len()))
                .finish(),
            Self::Stream(response) => formatter.debug_tuple("Stream").field(response).finish(),
        }
    }
}

#[derive(Debug)]
pub struct LiveRelayPayload {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: LiveRelayPayloadBody,
    pub metric_kind: &'static str,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Authority {
    scheme: Scheme,
    host_hash: [u8; 32],
    port: u16,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Scheme {
    Http,
    Https,
}

impl Authority {
    fn from_url(url: &Url) -> Result<Self, LiveRelayError> {
        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            _ => return Err(LiveRelayError::PolicyRejected),
        };
        let host = url
            .host_str()
            .ok_or(LiveRelayError::PolicyRejected)?
            .trim_end_matches('.')
            .to_ascii_lowercase();
        let port = url
            .port_or_known_default()
            .ok_or(LiveRelayError::PolicyRejected)?;
        Ok(Self {
            scheme,
            host_hash: *blake3::hash(host.as_bytes()).as_bytes(),
            port,
        })
    }

    fn from_credential(value: &CredentialAuthority) -> Result<Self, LiveRelayError> {
        Self::from_parts(&value.scheme, &value.host, value.port)
    }

    fn from_parts(scheme: &str, host: &str, port: u16) -> Result<Self, LiveRelayError> {
        let scheme = match scheme {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            _ => return Err(LiveRelayError::PolicyRejected),
        };
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        if host.is_empty() || port == 0 {
            return Err(LiveRelayError::PolicyRejected);
        }
        Ok(Self {
            scheme,
            host_hash: *blake3::hash(host.as_bytes()).as_bytes(),
            port,
        })
    }
}

struct RelaySession {
    session_id: Uuid,
    owner: SessionOwner,
    control_fencing_token: i64,
    token_revision: i64,
    hard_expires_at: chrono::DateTime<Utc>,
    protocol: SessionProtocol,
    source: Arc<SourceDescriptor>,
    root_url: Url,
    allowed_authorities: Vec<Authority>,
    credentials: Arc<CredentialSet>,
    fetcher: Arc<UpstreamFetcher>,
    resources: Mutex<HlsResourceMap>,
    cancellation: CancellationToken,
    _permit: OwnedSemaphorePermit,
}

impl fmt::Debug for RelaySession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelaySession")
            .field("session_id", &self.session_id)
            .field("owner", &self.owner)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("token_revision", &self.token_revision)
            .field("hard_expires_at", &self.hard_expires_at)
            .field("protocol", &self.protocol)
            .field("source", &"[REDACTED]")
            .field("allowed_authority_count", &self.allowed_authorities.len())
            .finish()
    }
}

impl RelaySession {
    fn matches(&self, session: &SessionRecord) -> bool {
        self.session_id == session.id
            && self.owner == session.owner
            && self.control_fencing_token == session.control_fencing_token
            && self.token_revision == session.token_revision
            && self.protocol == session.protocol
            && session.delivery_mode == DeliveryMode::ServerRelay
    }

    fn allows_resource(&self, url: &Url, policy_authorities: &[Authority]) -> bool {
        if !self.source.private_network {
            return matches!(url.scheme(), "http" | "https");
        }
        Authority::from_url(url).is_ok_and(|authority| {
            self.allowed_authorities.contains(&authority) || policy_authorities.contains(&authority)
        })
    }
}

struct LoadedPolicy {
    policy: DestinationPolicy,
    authorities: Vec<Authority>,
}

pub(crate) struct LiveRemuxSource {
    session_id: Uuid,
    owner: SessionOwner,
    control_fencing_token: i64,
    token_revision: i64,
    hard_expires_at: chrono::DateTime<Utc>,
    protocol: SessionProtocol,
    source: Arc<SourceDescriptor>,
    root_url: Url,
    allowed_authorities: Vec<Authority>,
    credentials: Arc<CredentialSet>,
    fetcher: Arc<UpstreamFetcher>,
}

impl fmt::Debug for LiveRemuxSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRemuxSource")
            .field("session_id", &self.session_id)
            .field("owner", &self.owner)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("token_revision", &self.token_revision)
            .field("protocol", &self.protocol)
            .field("source", &"[REDACTED]")
            .finish()
    }
}

impl LiveRemuxSource {
    pub(crate) fn protocol(&self) -> SessionProtocol {
        self.protocol
    }

    pub(crate) fn root_url(&self) -> &Url {
        &self.root_url
    }

    fn matches(&self, session: &SessionRecord) -> bool {
        self.session_id == session.id
            && self.owner == session.owner
            && self.control_fencing_token == session.control_fencing_token
            && self.token_revision == session.token_revision
            && self.protocol == session.protocol
            && session.delivery_mode == DeliveryMode::ServerRemux
            && !session.state.is_terminal()
    }
}

pub struct LiveRelayService {
    pool: sqlx::AnyPool,
    repository: Arc<LiveSessionRepository>,
    provider_client: Arc<LiveProviderClient>,
    fetcher: Arc<UpstreamFetcher>,
    egress: Option<Arc<LiveEgressService>>,
    rewriter: HlsRewriter,
    local_denylist: LocalDestinationDenylist,
    allow_private_lan_sources: bool,
    capacity: Arc<Semaphore>,
    sessions: DashMap<Uuid, Arc<RelaySession>>,
    admission_lock: Mutex<()>,
    manifest_coalescer: ManifestRequestCoalescer,
    #[cfg(test)]
    allow_fixture_loopback: bool,
}

impl fmt::Debug for LiveRelayService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRelayService")
            .field("active_sessions", &self.sessions.len())
            .field("available_capacity", &self.capacity.available_permits())
            .finish_non_exhaustive()
    }
}

impl LiveRelayService {
    pub fn new(
        pool: sqlx::AnyPool,
        repository: Arc<LiveSessionRepository>,
        provider_client: Arc<LiveProviderClient>,
        limits: LiveRelayLimits,
        allow_private_lan_sources: bool,
        egress: Option<Arc<LiveEgressService>>,
    ) -> Result<Self, LiveRelayBuildError> {
        let resolver = SystemDnsResolver::new(Duration::from_secs(5))
            .map_err(|_| LiveRelayBuildError::Resolver)?;
        let fetcher = UpstreamFetcher::new(
            Arc::new(resolver),
            UpstreamLimits {
                connect_timeout: Duration::from_secs(5),
                header_timeout: Duration::from_secs(10),
                idle_timeout: Duration::from_secs(15),
                total_timeout: Duration::from_secs(120),
                max_response_bytes: 4_u64 * 1024 * 1024 * 1024,
                max_response_headers: 64,
                max_response_header_bytes: 32 * 1024,
                max_redirects: 5,
            },
        )
        .map_err(|_| LiveRelayBuildError::Fetcher)?;
        let addresses = local_ip_address::list_afinet_netifas()
            .map_err(|_| LiveRelayBuildError::LocalDenylist)?
            .into_iter()
            .map(|(_, address)| address)
            .collect::<Vec<IpAddr>>();
        let local_denylist = LocalDestinationDenylist::new(addresses, Vec::new())
            .map_err(|_| LiveRelayBuildError::LocalDenylist)?;
        Self::with_fetcher(
            pool,
            repository,
            provider_client,
            limits,
            allow_private_lan_sources,
            Arc::new(fetcher),
            egress,
            local_denylist,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_fetcher(
        pool: sqlx::AnyPool,
        repository: Arc<LiveSessionRepository>,
        provider_client: Arc<LiveProviderClient>,
        limits: LiveRelayLimits,
        allow_private_lan_sources: bool,
        fetcher: Arc<UpstreamFetcher>,
        egress: Option<Arc<LiveEgressService>>,
        local_denylist: LocalDestinationDenylist,
        allow_fixture_loopback: bool,
    ) -> Result<Self, LiveRelayBuildError> {
        #[cfg(not(test))]
        let _ = allow_fixture_loopback;
        let capacity = usize::try_from(limits.max_concurrent)
            .ok()
            .filter(|capacity| *capacity > 0)
            .ok_or(LiveRelayBuildError::InvalidCapacity)?;
        let rewriter = HlsRewriter::new(HlsRewriteConfig::default())
            .map_err(|_| LiveRelayBuildError::Rewriter)?;
        Ok(Self {
            pool,
            repository,
            provider_client,
            fetcher,
            egress,
            rewriter,
            local_denylist,
            allow_private_lan_sources,
            capacity: Arc::new(Semaphore::new(capacity)),
            sessions: DashMap::new(),
            admission_lock: Mutex::new(()),
            manifest_coalescer: ManifestRequestCoalescer::default(),
            #[cfg(test)]
            allow_fixture_loopback,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        pool: sqlx::AnyPool,
        repository: Arc<LiveSessionRepository>,
        provider_client: Arc<LiveProviderClient>,
        limits: LiveRelayLimits,
    ) -> Result<Self, LiveRelayBuildError> {
        let resolver = SystemDnsResolver::new(Duration::from_secs(5))
            .map_err(|_| LiveRelayBuildError::Resolver)?;
        let fetcher = UpstreamFetcher::new(
            Arc::new(resolver),
            UpstreamLimits {
                connect_timeout: Duration::from_secs(5),
                header_timeout: Duration::from_secs(10),
                idle_timeout: Duration::from_secs(15),
                total_timeout: Duration::from_secs(120),
                max_response_bytes: 4_u64 * 1024 * 1024 * 1024,
                max_response_headers: 64,
                max_response_header_bytes: 32 * 1024,
                max_redirects: 5,
            },
        )
        .map_err(|_| LiveRelayBuildError::Fetcher)?;
        Self::new_with_fetcher_for_test(
            pool,
            repository,
            provider_client,
            limits,
            Arc::new(fetcher),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_fetcher_for_test(
        pool: sqlx::AnyPool,
        repository: Arc<LiveSessionRepository>,
        provider_client: Arc<LiveProviderClient>,
        limits: LiveRelayLimits,
        fetcher: Arc<UpstreamFetcher>,
    ) -> Result<Self, LiveRelayBuildError> {
        Self::with_fetcher(
            pool,
            repository,
            provider_client,
            limits,
            false,
            fetcher,
            None,
            LocalDestinationDenylist::empty(),
            true,
        )
    }

    pub fn available_capacity(&self) -> usize {
        self.capacity.available_permits()
    }

    fn fetcher_for(
        &self,
        session: &SessionRecord,
        stored: &StoredSessionDescriptor,
    ) -> Result<Arc<UpstreamFetcher>, LiveRelayError> {
        let policy = stored
            .egress
            .to_effective()
            .map_err(|_| LiveRelayError::DescriptorInvalid)?;
        if !policy.protected() {
            return Ok(self.fetcher.clone());
        }
        let egress = self.egress.as_ref().ok_or(LiveRelayError::Unavailable)?;
        match egress.fetcher_for(session, &policy) {
            Ok(Some(fetcher)) => Ok(fetcher),
            Ok(None) => Ok(self.fetcher.clone()),
            Err(LiveEgressError::StaleFence) => Err(LiveRelayError::StaleControlFence),
            Err(_) => Err(LiveRelayError::Unavailable),
        }
    }

    async fn ensure_session(
        &self,
        session: &SessionRecord,
    ) -> Result<Arc<RelaySession>, LiveRelayError> {
        self.validate_session_authority(session).await?;
        if session.delivery_mode != DeliveryMode::ServerRelay || session.state.is_terminal() {
            return Err(LiveRelayError::SessionMismatch);
        }
        if session.hard_expires_at <= Utc::now() {
            return Err(LiveRelayError::SessionExpired);
        }
        if let Some(existing) = self.sessions.get(&session.id) {
            if existing.matches(session) {
                return Ok(existing.clone());
            }
        }
        let _guard = self.admission_lock.lock().await;
        if let Some(existing) = self.sessions.get(&session.id) {
            if existing.matches(session) {
                return Ok(existing.clone());
            }
            if existing.control_fencing_token > session.control_fencing_token
                || (existing.control_fencing_token == session.control_fencing_token
                    && existing.token_revision > session.token_revision)
                || existing.owner != session.owner
            {
                return Err(LiveRelayError::StaleControlFence);
            }
        }
        if let Some((_, stale)) = self.sessions.remove(&session.id) {
            stale.cancellation.cancel();
        }
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| LiveRelayError::CapacityExhausted)?;
        let secrets = self
            .repository
            .decrypt_secrets(session.owner, session.id)
            .await
            .map_err(|_| LiveRelayError::Unavailable)?;
        let stored: StoredSessionDescriptor =
            serde_json::from_slice(secrets.descriptor.expose_secret())
                .map_err(|_| LiveRelayError::DescriptorInvalid)?;
        let source = stored
            .selected()
            .ok_or(LiveRelayError::DescriptorInvalid)?
            .to_source_descriptor()
            .map_err(|_| LiveRelayError::DescriptorInvalid)?;
        validate_protocol(session.protocol, source.protocol)?;
        if source
            .expires_at
            .is_some_and(|expires| expires <= Utc::now())
        {
            return Err(LiveRelayError::SessionExpired);
        }
        let root_url =
            Url::parse(source.url.expose()).map_err(|_| LiveRelayError::DescriptorInvalid)?;
        let mut allowed_authorities = vec![Authority::from_url(&root_url)?];
        for authority in &source.credential_authorities {
            let authority = Authority::from_credential(authority)?;
            if !allowed_authorities.contains(&authority) {
                allowed_authorities.push(authority);
            }
        }
        let credentials = CredentialSet::from_descriptor(&source)
            .map_err(|_| LiveRelayError::CredentialsRejected)?;
        let fetcher = self.fetcher_for(session, &stored)?;
        let resources = HlsResourceMap::new(
            session.id,
            session.control_fencing_token,
            HlsResourceLimits::default(),
        )?;
        let relay = Arc::new(RelaySession {
            session_id: session.id,
            owner: session.owner,
            control_fencing_token: session.control_fencing_token,
            token_revision: session.token_revision,
            hard_expires_at: session.hard_expires_at,
            protocol: session.protocol,
            source: Arc::new(source),
            root_url,
            allowed_authorities,
            credentials: Arc::new(credentials),
            fetcher,
            resources: Mutex::new(resources),
            cancellation: CancellationToken::new(),
            _permit: permit,
        });
        self.sessions.insert(session.id, relay.clone());
        Ok(relay)
    }

    async fn validate_session_authority(
        &self,
        session: &SessionRecord,
    ) -> Result<(), LiveRelayError> {
        let authoritative = self
            .repository
            .get_owned(session.owner, session.id)
            .await
            .map_err(|_| LiveRelayError::Unavailable)?
            .ok_or(LiveRelayError::SessionMismatch)?;
        if authoritative.owner != session.owner
            || authoritative.delivery_mode != session.delivery_mode
            || authoritative.protocol != session.protocol
            || authoritative.control_fencing_token != session.control_fencing_token
            || authoritative.token_revision != session.token_revision
            || authoritative.source_index != session.source_index
            || authoritative.state.is_terminal()
        {
            return Err(LiveRelayError::SessionMismatch);
        }
        let current_fence = sqlx::query_scalar::<_, i64>(
            "SELECT fencing_token FROM live_control_server_leases
             WHERE lease_name = 'live-control-v1' AND owner_instance_id IS NOT NULL
               AND expires_at > $1",
        )
        .bind(Utc::now().to_rfc3339())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| LiveRelayError::Unavailable)?
        .ok_or(LiveRelayError::StaleControlFence)?;
        if current_fence != session.control_fencing_token {
            return Err(LiveRelayError::StaleControlFence);
        }
        Ok(())
    }

    pub async fn admit_session(&self, session: &SessionRecord) -> Result<(), LiveRelayError> {
        let result = self.ensure_session(session).await.map(|_| ());
        if let Err(error) = &result
            && let Some(reason) = relay_admission_rejection(error)
        {
            crate::live::metrics::ADMISSION_REJECTIONS
                .with_label_values(&["relay", reason])
                .inc();
        }
        result
    }

    pub(crate) async fn prepare_remux_source(
        &self,
        session: &SessionRecord,
    ) -> Result<Arc<LiveRemuxSource>, LiveRelayError> {
        self.validate_session_authority(session).await?;
        if session.delivery_mode != DeliveryMode::ServerRemux || session.state.is_terminal() {
            return Err(LiveRelayError::SessionMismatch);
        }
        if session.hard_expires_at <= Utc::now() {
            return Err(LiveRelayError::SessionExpired);
        }
        if !matches!(
            session.protocol,
            SessionProtocol::Dash | SessionProtocol::MpegTs
        ) {
            return Err(LiveRelayError::ProtocolUnsupported);
        }
        let secrets = self
            .repository
            .decrypt_secrets(session.owner, session.id)
            .await
            .map_err(|_| LiveRelayError::Unavailable)?;
        let stored: StoredSessionDescriptor =
            serde_json::from_slice(secrets.descriptor.expose_secret())
                .map_err(|_| LiveRelayError::DescriptorInvalid)?;
        let source = stored
            .selected()
            .ok_or(LiveRelayError::DescriptorInvalid)?
            .to_source_descriptor()
            .map_err(|_| LiveRelayError::DescriptorInvalid)?;
        validate_protocol(session.protocol, source.protocol)?;
        if source
            .expires_at
            .is_some_and(|expires| expires <= Utc::now())
        {
            return Err(LiveRelayError::SessionExpired);
        }
        let root_url =
            Url::parse(source.url.expose()).map_err(|_| LiveRelayError::DescriptorInvalid)?;
        if !matches!(root_url.scheme(), "http" | "https") {
            return Err(LiveRelayError::ProtocolUnsupported);
        }
        let mut allowed_authorities = vec![Authority::from_url(&root_url)?];
        for authority in &source.credential_authorities {
            let authority = Authority::from_credential(authority)?;
            if !allowed_authorities.contains(&authority) {
                allowed_authorities.push(authority);
            }
        }
        let credentials = CredentialSet::from_descriptor(&source)
            .map_err(|_| LiveRelayError::CredentialsRejected)?;
        let fetcher = self.fetcher_for(session, &stored)?;
        Ok(Arc::new(LiveRemuxSource {
            session_id: session.id,
            owner: session.owner,
            control_fencing_token: session.control_fencing_token,
            token_revision: session.token_revision,
            hard_expires_at: session.hard_expires_at,
            protocol: session.protocol,
            source: Arc::new(source),
            root_url,
            allowed_authorities,
            credentials: Arc::new(credentials),
            fetcher,
        }))
    }

    pub(crate) async fn fetch_remux_source(
        &self,
        session: &SessionRecord,
        source: &LiveRemuxSource,
        target: &Url,
        range: Option<&str>,
        cancellation: CancellationToken,
    ) -> Result<UpstreamResponse, LiveRelayError> {
        if !source.matches(session) || source.hard_expires_at <= Utc::now() {
            return Err(LiveRelayError::SessionMismatch);
        }
        self.validate_session_authority(session).await?;
        if source
            .source
            .expires_at
            .is_some_and(|expires| expires <= Utc::now())
        {
            return Err(LiveRelayError::SessionExpired);
        }
        let discovered = target != &source.root_url;
        let policy = self
            .load_policy_for(
                source.owner,
                &source.source,
                &source.root_url,
                &source.allowed_authorities,
                target,
                discovered,
            )
            .await?;
        let mut safe_headers = SafeRequestHeaders::new();
        if let Some(range) = range {
            safe_headers
                .insert("range", range)
                .map_err(|_| LiveRelayError::RangeRejected)?;
        }
        source
            .fetcher
            .fetch(
                FetchRequest::new(
                    SensitiveString::new(target.to_string()),
                    UpstreamMethod::Get,
                    policy.policy,
                    cancellation,
                )
                .with_safe_headers(safe_headers)
                .with_credentials(source.credentials.clone()),
            )
            .await
            .map_err(Into::into)
    }

    pub async fn hls_manifest(
        self: &Arc<Self>,
        session: &SessionRecord,
        resource_id: Option<&HlsResourceId>,
    ) -> Result<LiveRelayPayload, LiveRelayError> {
        let relay = self.ensure_session(session).await?;
        if relay.protocol != SessionProtocol::Hls {
            return Err(LiveRelayError::ProtocolUnsupported);
        }
        let key = ManifestFlightKey::new(
            session.id,
            session.control_fencing_token,
            resource_id.cloned(),
        );
        let service = self.clone();
        let owned_session = session.clone();
        let owned_resource_id = resource_id.cloned();
        let session_cancellation = relay.cancellation.clone();
        let manifest = self
            .manifest_coalescer
            .run(
                key,
                &session_cancellation,
                move |upstream_cancellation| async move {
                    service
                        .load_hls_manifest(
                            relay,
                            owned_session,
                            owned_resource_id,
                            upstream_cancellation,
                        )
                        .await
                        .map(Arc::new)
                },
            )
            .await?;
        Ok(LiveRelayPayload {
            status: StatusCode::OK,
            headers: manifest.headers.clone(),
            body: LiveRelayPayloadBody::Bytes(manifest.body.clone()),
            metric_kind: if resource_id.is_some() {
                "playlist"
            } else {
                "manifest"
            },
        })
    }

    async fn load_hls_manifest(
        &self,
        relay: Arc<RelaySession>,
        session: SessionRecord,
        resource_id: Option<HlsResourceId>,
        cancellation: CancellationToken,
    ) -> Result<CoalescedManifest, LiveRelayError> {
        let discovered_resource = resource_id.is_some();
        let (url, scope) = if let Some(resource_id) = resource_id {
            let descriptor = relay
                .resources
                .lock()
                .await
                .resolve(&resource_id, session.control_fencing_token)?;
            if descriptor.kind() != HlsResourceKind::Playlist {
                return Err(LiveRelayError::ResourceKindMismatch);
            }
            (
                descriptor.url().clone(),
                HlsManifestScope::from_stable_key(resource_id.as_str().as_bytes())?,
            )
        } else {
            (
                relay.root_url.clone(),
                HlsManifestScope::from_stable_key(b"root")?,
            )
        };
        let policy = self.load_policy(&relay, &url, discovered_resource).await?;
        let mut safe_headers = SafeRequestHeaders::new();
        safe_headers
            .insert(
                "accept",
                "application/vnd.apple.mpegurl, application/x-mpegURL, audio/mpegurl",
            )
            .map_err(|_| LiveRelayError::PolicyRejected)?;
        let response = relay
            .fetcher
            .fetch(
                FetchRequest::new(
                    SensitiveString::new(url.to_string()),
                    UpstreamMethod::Get,
                    policy.policy,
                    cancellation,
                )
                .with_safe_headers(safe_headers)
                .with_credentials(relay.credentials.clone()),
            )
            .await?;
        if response.status() != StatusCode::OK {
            return Err(LiveRelayError::UpstreamStatus);
        }
        validate_manifest_content_type(response.headers(), response.final_url())?;
        let final_url = response.final_url().clone();
        let body = response.collect_bounded(MAX_MANIFEST_BYTES).await?;
        crate::live::metrics::RELAY_UPSTREAM_BYTES
            .with_label_values(&[if discovered_resource {
                "playlist"
            } else {
                "manifest"
            }])
            .inc_by(body.as_bytes().len() as u64);
        let route_base = hls_route_base(session.id);
        let mut resources = relay.resources.lock().await;
        let rewritten = self.rewriter.rewrite_scoped_with_validator(
            &mut resources,
            session.control_fencing_token,
            scope,
            &final_url,
            &route_base,
            body.as_bytes(),
            |descriptor| {
                if relay.allows_resource(descriptor.url(), &policy.authorities) {
                    Ok(())
                } else {
                    Err(HlsRewriteError::InvalidResourceUri)
                }
            },
        )?;
        let bytes = rewritten.body().to_vec();
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            "application/vnd.apple.mpegurl"
                .parse()
                .expect("static HLS content type"),
        );
        headers.insert(
            CONTENT_LENGTH,
            bytes
                .len()
                .to_string()
                .parse()
                .map_err(|_| LiveRelayError::Unavailable)?,
        );
        headers.insert(
            ETAG,
            format!("\"{}\"", blake3::hash(&bytes).to_hex())
                .parse()
                .map_err(|_| LiveRelayError::Unavailable)?,
        );
        Ok(CoalescedManifest {
            headers,
            body: bytes,
        })
    }

    pub async fn hls_resource(
        self: &Arc<Self>,
        session: &SessionRecord,
        resource_id: &HlsResourceId,
        client_range: Option<&str>,
    ) -> Result<LiveRelayPayload, LiveRelayError> {
        let relay = self.ensure_session(session).await?;
        if relay.protocol != SessionProtocol::Hls {
            return Err(LiveRelayError::ProtocolUnsupported);
        }
        let descriptor = relay
            .resources
            .lock()
            .await
            .resolve(resource_id, session.control_fencing_token)?;
        if descriptor.kind() == HlsResourceKind::Playlist {
            return self.hls_manifest(session, Some(resource_id)).await;
        }
        self.fetch_resource(&relay, descriptor, client_range).await
    }

    pub async fn progressive_stream(
        &self,
        session: &SessionRecord,
        client_range: Option<&str>,
    ) -> Result<LiveRelayPayload, LiveRelayError> {
        let relay = self.ensure_session(session).await?;
        if !matches!(
            relay.protocol,
            SessionProtocol::HttpProgressive | SessionProtocol::MpegTs
        ) {
            return Err(LiveRelayError::ProtocolUnsupported);
        }
        let descriptor = HlsResourceDescriptor::new_for_relay_root(
            relay.root_url.clone(),
            HlsResourceKind::MediaSegment,
        );
        let mut payload = self
            .fetch_resource(&relay, descriptor, client_range)
            .await?;
        payload.metric_kind = "progressive";
        Ok(payload)
    }

    async fn fetch_resource(
        &self,
        relay: &Arc<RelaySession>,
        descriptor: HlsResourceDescriptor,
        client_range: Option<&str>,
    ) -> Result<LiveRelayPayload, LiveRelayError> {
        let metric_kind = resource_metric_kind(descriptor.kind());
        if descriptor.kind() == HlsResourceKind::EncryptionKey && client_range.is_some() {
            return Err(LiveRelayError::RangeRejected);
        }
        let (upstream_range, client_requested_range) = authorized_range(&descriptor, client_range)?;
        let policy = self.load_policy(relay, descriptor.url(), true).await?;
        let mut safe_headers = SafeRequestHeaders::new();
        if let Some(range) = &upstream_range {
            safe_headers
                .insert("range", range)
                .map_err(|_| LiveRelayError::RangeRejected)?;
        }
        let cancellation = relay.cancellation.child_token();
        let response = relay
            .fetcher
            .fetch(
                FetchRequest::new(
                    SensitiveString::new(descriptor.url().to_string()),
                    UpstreamMethod::Get,
                    policy.policy,
                    cancellation,
                )
                .with_safe_headers(safe_headers)
                .with_credentials(relay.credentials.clone()),
            )
            .await?;
        validate_resource_status(&response, upstream_range.as_deref())?;
        validate_resource_content_type(descriptor.kind(), response.headers())?;
        if let Some(expected) = upstream_range.as_deref() {
            validate_content_range(response.headers(), expected)?;
        }
        let mut headers = relay_response_headers(response.headers());
        let mut status = response.status();
        if descriptor.byte_range().is_some() && !client_requested_range {
            status = StatusCode::OK;
            headers.remove(CONTENT_RANGE);
        }
        if descriptor.kind() == HlsResourceKind::EncryptionKey {
            let body = response.collect_bounded(MAX_KEY_BYTES).await?;
            if body.as_bytes().len() != 16 {
                return Err(LiveRelayError::ManifestRejected);
            }
            crate::live::metrics::RELAY_UPSTREAM_BYTES
                .with_label_values(&[metric_kind])
                .inc_by(body.as_bytes().len() as u64);
            headers.insert(
                CONTENT_LENGTH,
                "16".parse().expect("static AES-128 key length"),
            );
            return Ok(LiveRelayPayload {
                status,
                headers,
                body: LiveRelayPayloadBody::Bytes(body.into_bytes()),
                metric_kind,
            });
        }
        Ok(LiveRelayPayload {
            status,
            headers,
            body: LiveRelayPayloadBody::Stream(response),
            metric_kind,
        })
    }

    async fn load_policy(
        &self,
        relay: &RelaySession,
        target: &Url,
        allow_discovered_target: bool,
    ) -> Result<LoadedPolicy, LiveRelayError> {
        self.load_policy_for(
            relay.owner,
            &relay.source,
            &relay.root_url,
            &relay.allowed_authorities,
            target,
            allow_discovered_target,
        )
        .await
    }

    async fn load_policy_for(
        &self,
        owner: SessionOwner,
        source: &SourceDescriptor,
        root_url: &Url,
        allowed_authorities: &[Authority],
        target: &Url,
        allow_discovered_target: bool,
    ) -> Result<LoadedPolicy, LiveRelayError> {
        let rows = sqlx::query(
            "SELECT scheme, normalized_host, port, exact_path, network_scope,
                    CAST(CASE WHEN allow_fetch THEN 1 ELSE 0 END AS BIGINT) AS allow_fetch
             FROM live_provider_destination_rules
             WHERE home_id = $1 AND provider_id = $2
             ORDER BY scheme, normalized_host, port, exact_path, network_scope",
        )
        .bind(owner.home_id.to_string())
        .bind(owner.provider_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| LiveRelayError::Unavailable)?;
        if rows.len() > MAX_POLICY_ROWS || (rows.is_empty() && source.private_network) {
            return Err(LiveRelayError::PolicyRejected);
        }
        let mut rules = Vec::with_capacity(rows.len() + usize::from(allow_discovered_target));
        let mut authorities = Vec::new();
        let mut owner_private_rule = false;
        for row in rows {
            if row
                .try_get::<i64, _>("allow_fetch")
                .map_err(|_| LiveRelayError::Unavailable)?
                != 1
            {
                continue;
            }
            let scheme: String = row
                .try_get("scheme")
                .map_err(|_| LiveRelayError::Unavailable)?;
            let host: String = row
                .try_get("normalized_host")
                .map_err(|_| LiveRelayError::Unavailable)?;
            let port = u16::try_from(
                row.try_get::<i64, _>("port")
                    .map_err(|_| LiveRelayError::Unavailable)?,
            )
            .map_err(|_| LiveRelayError::Unavailable)?;
            let path: String = row
                .try_get("exact_path")
                .map_err(|_| LiveRelayError::Unavailable)?;
            let network_scope: String = row
                .try_get("network_scope")
                .map_err(|_| LiveRelayError::Unavailable)?;
            let scope = match network_scope.as_str() {
                "public" => NetworkScope::Public,
                "private_lan" => {
                    owner_private_rule = true;
                    NetworkScope::PrivateLan
                }
                _ => return Err(LiveRelayError::PolicyRejected),
            };
            if source.private_network != (scope == NetworkScope::PrivateLan) {
                continue;
            }
            let rule = DestinationRule::new(&scheme, &host, port, &path, scope, true)
                .map_err(|_| LiveRelayError::PolicyRejected)?;
            let authority = Authority::from_parts(&scheme, &host, port)?;
            if !authorities.contains(&authority) {
                authorities.push(authority);
            }
            rules.push(rule);
        }
        if allow_discovered_target && source.private_network {
            let target_authority = Authority::from_url(target)?;
            if !allowed_authorities.contains(&target_authority)
                && !authorities.contains(&target_authority)
            {
                return Err(LiveRelayError::PolicyRejected);
            }
            let host = target.host_str().ok_or(LiveRelayError::PolicyRejected)?;
            let port = target
                .port_or_known_default()
                .ok_or(LiveRelayError::PolicyRejected)?;
            let scope = if source.private_network {
                NetworkScope::PrivateLan
            } else {
                NetworkScope::Public
            };
            rules.push(
                DestinationRule::new(target.scheme(), host, port, target.path(), scope, true)
                    .map_err(|_| LiveRelayError::PolicyRejected)?,
            );
        }
        let provider_private_permission = if source.private_network {
            self.provider_client
                .directory()
                .get(owner.provider_id)
                .await
                .map_err(|_| LiveRelayError::PolicyRejected)?
                .permits_private_network()
        } else {
            false
        };
        let private_lan = PrivateLanGate {
            server_enabled: self.allow_private_lan_sources,
            provider_permission: provider_private_permission,
            descriptor_requested: source.private_network,
            owner_rule: owner_private_rule,
        };
        let allow_http = target.scheme() == "http" || root_url.scheme() == "http";
        let policy = if source.private_network {
            DestinationPolicy::new(rules, private_lan, allow_http, self.local_denylist.clone())
        } else {
            DestinationPolicy::for_public_session(rules, allow_http, self.local_denylist.clone())
        }
        .map_err(|_| LiveRelayError::PolicyRejected)?;
        #[cfg(test)]
        let policy = if self.allow_fixture_loopback {
            policy.allow_fixture_loopback()
        } else {
            policy
        };
        Ok(LoadedPolicy {
            policy,
            authorities,
        })
    }

    pub fn end_session(&self, session_id: Uuid) {
        if let Some((_, relay)) = self.sessions.remove(&session_id) {
            relay.cancellation.cancel();
        }
    }

    pub fn cancel_all(&self) {
        let session_ids = self
            .sessions
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for session_id in session_ids {
            self.end_session(session_id);
        }
    }

    pub async fn reap_stale(&self) {
        let sessions = self
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        let now = Utc::now();
        for relay in sessions {
            let stale = if relay.hard_expires_at <= now {
                true
            } else {
                match self
                    .repository
                    .get_owned(relay.owner, relay.session_id)
                    .await
                {
                    Ok(Some(session)) => {
                        session.state.is_terminal()
                            || session.control_fencing_token != relay.control_fencing_token
                            || self.validate_session_authority(&session).await.is_err()
                    }
                    Ok(None) | Err(_) => true,
                }
            };
            if stale {
                self.end_session(relay.session_id);
            }
        }
    }
}

const fn resource_metric_kind(kind: HlsResourceKind) -> &'static str {
    match kind {
        HlsResourceKind::Playlist => "playlist",
        HlsResourceKind::MediaSegment => "media_segment",
        HlsResourceKind::InitializationSegment => "initialization_segment",
        HlsResourceKind::EncryptionKey => "encryption_key",
        HlsResourceKind::PartialSegment => "partial_segment",
    }
}

fn validate_protocol(
    session: SessionProtocol,
    descriptor: StreamProtocol,
) -> Result<(), LiveRelayError> {
    let matches = matches!(
        (session, descriptor),
        (SessionProtocol::Hls, StreamProtocol::Hls)
            | (SessionProtocol::Dash, StreamProtocol::Dash)
            | (
                SessionProtocol::HttpProgressive,
                StreamProtocol::HttpProgressive
            )
            | (SessionProtocol::MpegTs, StreamProtocol::MpegTs)
            | (SessionProtocol::Rtmp, StreamProtocol::Rtmp)
            | (SessionProtocol::Srt, StreamProtocol::Srt)
    );
    if matches {
        Ok(())
    } else {
        Err(LiveRelayError::DescriptorInvalid)
    }
}

fn hls_route_base(session_id: Uuid) -> String {
    format!("/api/v1/live/sessions/{session_id}/delivery/hls")
}

fn validate_manifest_content_type(headers: &HeaderMap, url: &Url) -> Result<(), LiveRelayError> {
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase);
    let standard = media_type.as_deref().is_some_and(|value| {
        matches!(
            value,
            "application/vnd.apple.mpegurl"
                | "application/x-mpegurl"
                | "audio/mpegurl"
                | "audio/x-mpegurl"
        )
    });
    let extension_fallback = url.path().to_ascii_lowercase().ends_with(".m3u8")
        && media_type
            .as_deref()
            .is_none_or(|value| value == "application/octet-stream");
    if standard || extension_fallback {
        Ok(())
    } else {
        Err(LiveRelayError::ContentTypeRejected)
    }
}

fn validate_resource_content_type(
    kind: HlsResourceKind,
    headers: &HeaderMap,
) -> Result<(), LiveRelayError> {
    let Some(value) = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase)
    else {
        return Ok(());
    };
    if matches!(
        value.as_str(),
        "text/html" | "application/json" | "text/xml"
    ) {
        return Err(LiveRelayError::ContentTypeRejected);
    }
    if kind == HlsResourceKind::EncryptionKey
        && !matches!(
            value.as_str(),
            "application/octet-stream" | "binary/octet-stream"
        )
    {
        return Err(LiveRelayError::ContentTypeRejected);
    }
    Ok(())
}

fn authorized_range(
    descriptor: &HlsResourceDescriptor,
    client_range: Option<&str>,
) -> Result<(Option<String>, bool), LiveRelayError> {
    let client_range = client_range.map(validate_range).transpose()?;
    if let Some(range) = descriptor.byte_range() {
        let start = range.offset.ok_or(LiveRelayError::RangeRejected)?;
        let end = start
            .checked_add(range.length)
            .and_then(|value| value.checked_sub(1))
            .ok_or(LiveRelayError::RangeRejected)?;
        let expected = format!("bytes={start}-{end}");
        if client_range
            .as_deref()
            .is_some_and(|value| value != expected)
        {
            return Err(LiveRelayError::RangeRejected);
        }
        return Ok((Some(expected), client_range.is_some()));
    }
    let client_requested_range = client_range.is_some();
    Ok((client_range, client_requested_range))
}

fn validate_range(value: &str) -> Result<String, LiveRelayError> {
    let range = value
        .strip_prefix("bytes=")
        .ok_or(LiveRelayError::RangeRejected)?;
    if range.contains(',') {
        return Err(LiveRelayError::RangeRejected);
    }
    let (start, end) = range.split_once('-').ok_or(LiveRelayError::RangeRejected)?;
    if start.is_empty()
        || start.len() > 20
        || end.len() > 20
        || !start.bytes().all(|byte| byte.is_ascii_digit())
        || !end.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(LiveRelayError::RangeRejected);
    }
    let start = start
        .parse::<u64>()
        .map_err(|_| LiveRelayError::RangeRejected)?;
    if !end.is_empty() {
        let end = end
            .parse::<u64>()
            .map_err(|_| LiveRelayError::RangeRejected)?;
        if end < start {
            return Err(LiveRelayError::RangeRejected);
        }
        Ok(format!("bytes={start}-{end}"))
    } else {
        Ok(format!("bytes={start}-"))
    }
}

fn validate_resource_status(
    response: &UpstreamResponse,
    range: Option<&str>,
) -> Result<(), LiveRelayError> {
    match (range.is_some(), response.status()) {
        (true, StatusCode::PARTIAL_CONTENT) | (false, StatusCode::OK) => Ok(()),
        _ => Err(LiveRelayError::UpstreamStatus),
    }
}

fn validate_content_range(headers: &HeaderMap, expected_range: &str) -> Result<(), LiveRelayError> {
    let expected = expected_range
        .strip_prefix("bytes=")
        .ok_or(LiveRelayError::RangeRejected)?;
    let (expected_start, expected_end) = parse_range_bounds(expected, true)?;
    let values = headers.get_all(CONTENT_RANGE).iter().collect::<Vec<_>>();
    if values.len() != 1 {
        return Err(LiveRelayError::RangeRejected);
    }
    let (actual, total) = values[0]
        .to_str()
        .ok()
        .and_then(|value| value.strip_prefix("bytes "))
        .and_then(|value| value.split_once('/'))
        .ok_or(LiveRelayError::RangeRejected)?;
    let (actual_start, actual_end) = parse_range_bounds(actual, false)?;
    let actual_end = actual_end.ok_or(LiveRelayError::RangeRejected)?;
    if actual_start != expected_start
        || expected_end.is_some_and(|expected_end| expected_end != actual_end)
        || actual_end < actual_start
    {
        return Err(LiveRelayError::RangeRejected);
    }
    if total != "*" {
        let total = parse_range_number(total)?;
        if total == 0 || actual_end >= total {
            return Err(LiveRelayError::RangeRejected);
        }
    }
    let expected_length = actual_end
        .checked_sub(actual_start)
        .and_then(|value| value.checked_add(1))
        .ok_or(LiveRelayError::RangeRejected)?;
    if let Some(length) = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
    {
        if parse_range_number(length)? != expected_length {
            return Err(LiveRelayError::RangeRejected);
        }
    }
    Ok(())
}

fn parse_range_bounds(
    value: &str,
    allow_open_end: bool,
) -> Result<(u64, Option<u64>), LiveRelayError> {
    let (start, end) = value.split_once('-').ok_or(LiveRelayError::RangeRejected)?;
    let start = parse_range_number(start)?;
    let end = if end.is_empty() && allow_open_end {
        None
    } else {
        Some(parse_range_number(end)?)
    };
    if end.is_some_and(|end| end < start) {
        return Err(LiveRelayError::RangeRejected);
    }
    Ok((start, end))
}

fn parse_range_number(value: &str) -> Result<u64, LiveRelayError> {
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(LiveRelayError::RangeRejected);
    }
    value.parse().map_err(|_| LiveRelayError::RangeRejected)
}

fn relay_response_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut output = HeaderMap::new();
    for name in [
        CONTENT_TYPE,
        CONTENT_LENGTH,
        CONTENT_RANGE,
        ACCEPT_RANGES,
        ETAG,
        LAST_MODIFIED,
    ] {
        if let Some(value) = upstream.get(&name) {
            output.insert(name, value.clone());
        }
    }
    output
}

fn relay_admission_rejection(error: &LiveRelayError) -> Option<&'static str> {
    match error {
        LiveRelayError::CapacityExhausted => Some("capacity_exhausted"),
        LiveRelayError::StaleControlFence => Some("stale_fence"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r12_content_range_validation_supports_open_ended_clients_and_stays_exact() {
        let mut open = HeaderMap::new();
        open.insert(CONTENT_RANGE, "bytes 0-751/752".parse().unwrap());
        open.insert(CONTENT_LENGTH, "752".parse().unwrap());
        assert_eq!(validate_content_range(&open, "bytes=0-"), Ok(()));

        let mut exact = HeaderMap::new();
        exact.insert(CONTENT_RANGE, "bytes 100-199/752".parse().unwrap());
        exact.insert(CONTENT_LENGTH, "100".parse().unwrap());
        assert_eq!(validate_content_range(&exact, "bytes=100-199"), Ok(()));
        assert_eq!(
            validate_content_range(&exact, "bytes=100-200"),
            Err(LiveRelayError::RangeRejected)
        );
        assert_eq!(
            validate_content_range(&exact, "bytes=99-"),
            Err(LiveRelayError::RangeRejected)
        );

        let mut malformed = HeaderMap::new();
        malformed.insert(CONTENT_RANGE, "bytes 0-752/752".parse().unwrap());
        malformed.insert(CONTENT_LENGTH, "753".parse().unwrap());
        assert_eq!(
            validate_content_range(&malformed, "bytes=0-"),
            Err(LiveRelayError::RangeRejected)
        );
    }

    #[test]
    fn o10_relay_admission_rejections_have_bounded_alert_labels() {
        assert_eq!(
            relay_admission_rejection(&LiveRelayError::CapacityExhausted),
            Some("capacity_exhausted")
        );
        assert_eq!(
            relay_admission_rejection(&LiveRelayError::StaleControlFence),
            Some("stale_fence")
        );
        assert_eq!(
            relay_admission_rejection(&LiveRelayError::Unavailable),
            None
        );
    }
}
