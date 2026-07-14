use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT, ACCEPT_ENCODING, CONTENT_TYPE},
    redirect::Policy,
};
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::live::{
    config::LiveProviderLimits,
    contract::{
        CatalogPage, CatalogPageRequest, CatalogSet, CatalogsRequest, ContractError,
        ContractErrorCode, ItemMetadata, LIVE_PROVIDER_CONTRACT_VERSION,
        MAX_PROVIDER_RESPONSE_BYTES, MetaRequest, ProviderFailure, ProviderHealth,
        ProviderOperation, ProviderRequest, ProviderRequestContext, RefreshRequest, ResolveRequest,
        ResolvedSources, parse_catalog_page_response, parse_catalogs_response,
        parse_health_response, parse_meta_response, parse_provider_failure, parse_refresh_response,
        parse_resolve_response,
    },
    diagnostics::LiveRedactor,
};

use super::directory::{
    LiveProviderDirectory, LiveProviderSnapshot, ProviderDirectoryError, ProviderDirectoryErrorCode,
};

const MAX_PROVIDER_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderClientBuildError {
    InvalidLimits,
    HttpClient,
}

impl fmt::Display for ProviderClientBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Live provider client initialization failed ({self:?})"
        )
    }
}

impl std::error::Error for ProviderClientBuildError {}

#[derive(Clone, PartialEq, Eq)]
pub enum ProviderInvocationError {
    NotReady,
    RevisionChanged,
    UnsupportedOperation,
    InvalidRequest(ContractErrorCode),
    Cancelled,
    RequestTimeout,
    HardTimeout,
    Transport,
    RedirectRejected,
    InvalidContentType,
    ResponseTooLarge,
    Contract(ContractErrorCode),
    Provider(ProviderFailure),
}

impl fmt::Debug for ProviderInvocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(failure) => formatter
                .debug_tuple("Provider")
                .field(&failure.code)
                .finish(),
            other => formatter.write_str(other.code()),
        }
    }
}

impl ProviderInvocationError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NotReady => "provider_not_ready",
            Self::RevisionChanged => "provider_revision_changed",
            Self::UnsupportedOperation => "provider_operation_unsupported",
            Self::InvalidRequest(_) => "provider_request_invalid",
            Self::Cancelled => "provider_call_cancelled",
            Self::RequestTimeout => "provider_request_timeout",
            Self::HardTimeout => "provider_hard_timeout",
            Self::Transport => "provider_transport_failure",
            Self::RedirectRejected => "provider_redirect_rejected",
            Self::InvalidContentType => "provider_content_type_invalid",
            Self::ResponseTooLarge => "provider_response_too_large",
            Self::Contract(_) => "provider_contract_failure",
            Self::Provider(_) => "provider_reported_failure",
        }
    }
}

impl fmt::Display for ProviderInvocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ProviderInvocationError {}

pub struct LiveProviderClient {
    directory: LiveProviderDirectory,
    http: Client,
    limits: LiveProviderLimits,
    response_limit: usize,
    provider_permits: SemaphoreRegistry<Uuid>,
    user_permits: SemaphoreRegistry<Uuid>,
    redactor: Arc<LiveRedactor>,
}

impl LiveProviderClient {
    pub fn new(
        pool: sqlx::AnyPool,
        limits: LiveProviderLimits,
        redactor: Arc<LiveRedactor>,
    ) -> Result<Self, ProviderClientBuildError> {
        Self::new_with_directory(LiveProviderDirectory::new(pool), limits, redactor)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        pool: sqlx::AnyPool,
        limits: LiveProviderLimits,
        redactor: Arc<LiveRedactor>,
    ) -> Result<Self, ProviderClientBuildError> {
        Self::new_with_directory(LiveProviderDirectory::new_for_test(pool), limits, redactor)
    }

    fn new_with_directory(
        directory: LiveProviderDirectory,
        limits: LiveProviderLimits,
        redactor: Arc<LiveRedactor>,
    ) -> Result<Self, ProviderClientBuildError> {
        if !(1..=60).contains(&limits.request_timeout_seconds)
            || !(limits.request_timeout_seconds..=120).contains(&limits.hard_timeout_seconds)
            || !(1_024..=16 * 1024 * 1024).contains(&limits.response_bytes)
            || !(1..=64).contains(&limits.concurrency_per_provider)
            || !(1..=256).contains(&limits.concurrency_per_user)
        {
            return Err(ProviderClientBuildError::InvalidLimits);
        }
        let request_timeout = Duration::from_secs(limits.request_timeout_seconds);
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(request_timeout)
            .pool_max_idle_per_host(limits.concurrency_per_provider as usize)
            .tcp_nodelay(true)
            .build()
            .map_err(|_| ProviderClientBuildError::HttpClient)?;
        Ok(Self {
            directory,
            http,
            response_limit: (limits.response_bytes as usize).min(MAX_PROVIDER_RESPONSE_BYTES),
            provider_permits: SemaphoreRegistry::new(limits.concurrency_per_provider as usize),
            user_permits: SemaphoreRegistry::new(limits.concurrency_per_user as usize),
            limits,
            redactor,
        })
    }

    pub fn directory(&self) -> &LiveProviderDirectory {
        &self.directory
    }

    pub async fn health(
        &self,
        provider: &LiveProviderSnapshot,
        cancellation: &CancellationToken,
    ) -> Result<ProviderHealth, ProviderInvocationError> {
        let started = Instant::now();
        let result = async {
            let body = self
                .bounded_call(
                    provider,
                    None,
                    ProviderOperation::Health,
                    None,
                    cancellation,
                )
                .await?;
            parse_health_response(&body).map_err(map_contract_response)
        }
        .await;
        record_provider_call(ProviderOperation::Health, started, &result);
        result
    }

    pub async fn catalogs(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Uuid,
        context: &ProviderRequestContext,
        cancellation: &CancellationToken,
    ) -> Result<CatalogSet, ProviderInvocationError> {
        let started = Instant::now();
        let result = async {
            let body = self
                .invoke(
                    provider,
                    user_id,
                    context,
                    &CatalogsRequest::default(),
                    cancellation,
                )
                .await?;
            parse_catalogs_response(&body, &provider.contract).map_err(map_contract_response)
        }
        .await;
        record_provider_call(ProviderOperation::Catalogs, started, &result);
        result
    }

    pub async fn catalog(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Uuid,
        context: &ProviderRequestContext,
        request: &CatalogPageRequest,
        cancellation: &CancellationToken,
    ) -> Result<CatalogPage, ProviderInvocationError> {
        let started = Instant::now();
        let result = async {
            let body = self
                .invoke(provider, user_id, context, request, cancellation)
                .await?;
            parse_catalog_page_response(&body, &provider.contract).map_err(map_contract_response)
        }
        .await;
        record_provider_call(ProviderOperation::Catalog, started, &result);
        result
    }

    pub async fn meta(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Uuid,
        context: &ProviderRequestContext,
        request: &MetaRequest,
        cancellation: &CancellationToken,
    ) -> Result<ItemMetadata, ProviderInvocationError> {
        let started = Instant::now();
        let result = async {
            let body = self
                .invoke(provider, user_id, context, request, cancellation)
                .await?;
            parse_meta_response(&body, &provider.contract, &request.item_id)
                .map_err(map_contract_response)
        }
        .await;
        record_provider_call(ProviderOperation::Meta, started, &result);
        result
    }

    pub async fn resolve(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Uuid,
        context: &ProviderRequestContext,
        request: &ResolveRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResolvedSources, ProviderInvocationError> {
        let started = Instant::now();
        let result = async {
            let body = self
                .invoke(provider, user_id, context, request, cancellation)
                .await?;
            parse_resolve_response(&body, &provider.contract, &request.stream_id, context.now)
                .map_err(map_contract_response)
        }
        .await;
        record_provider_call(ProviderOperation::Resolve, started, &result);
        result
    }

    pub async fn refresh(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Uuid,
        context: &ProviderRequestContext,
        request: &RefreshRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResolvedSources, ProviderInvocationError> {
        let started = Instant::now();
        let result = async {
            let body = self
                .invoke(provider, user_id, context, request, cancellation)
                .await?;
            parse_refresh_response(&body, &provider.contract, context.now)
                .map_err(map_contract_response)
        }
        .await;
        record_provider_call(ProviderOperation::Refresh, started, &result);
        result
    }

    async fn invoke<R: ProviderRequest>(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Uuid,
        context: &ProviderRequestContext,
        request: &R,
        cancellation: &CancellationToken,
    ) -> Result<Zeroizing<Vec<u8>>, ProviderInvocationError> {
        context
            .validate()
            .and_then(|()| request.validate())
            .map_err(map_contract_request)?;
        if !provider.permits(R::OPERATION) {
            return Err(ProviderInvocationError::UnsupportedOperation);
        }
        let request_id = Uuid::new_v4();
        let envelope = ProviderRequestEnvelope {
            schema_version: LIVE_PROVIDER_CONTRACT_VERSION,
            request_id,
            request,
            context,
            provider: ProviderIdentity {
                provider_id: provider.provider_id,
                extension_id: &provider.extension_id,
                instance_id: provider.instance_id,
                implementation: &provider.implementation,
                config: provider.config(),
            },
        };
        let body = serde_json::to_vec(&envelope).map_err(|_| {
            ProviderInvocationError::InvalidRequest(ContractErrorCode::InvalidShape)
        })?;
        if body.len() > MAX_PROVIDER_REQUEST_BYTES {
            return Err(ProviderInvocationError::InvalidRequest(
                ContractErrorCode::LimitExceeded,
            ));
        }
        self.bounded_call(
            provider,
            Some(user_id),
            R::OPERATION,
            Some(body),
            cancellation,
        )
        .await
    }

    async fn bounded_call(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Option<Uuid>,
        operation: ProviderOperation,
        body: Option<Vec<u8>>,
        cancellation: &CancellationToken,
    ) -> Result<Zeroizing<Vec<u8>>, ProviderInvocationError> {
        let call = self.call_inner(provider, user_id, operation, body);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ProviderInvocationError::Cancelled),
            result = tokio::time::timeout(
                Duration::from_secs(self.limits.hard_timeout_seconds),
                call,
            ) => result.unwrap_or(Err(ProviderInvocationError::HardTimeout)),
        }
    }

    async fn call_inner(
        &self,
        provider: &LiveProviderSnapshot,
        user_id: Option<Uuid>,
        operation: ProviderOperation,
        body: Option<Vec<u8>>,
    ) -> Result<Zeroizing<Vec<u8>>, ProviderInvocationError> {
        self.directory
            .verify(provider)
            .await
            .map_err(map_directory_error)?;
        let provider_gate = self.provider_permits.get(provider.provider_id);
        let provider_permit = provider_gate
            .acquire_owned()
            .await
            .map_err(|_| ProviderInvocationError::Transport)?;
        let user_permit = if let Some(user_id) = user_id {
            Some(
                self.user_permits
                    .get(user_id)
                    .acquire_owned()
                    .await
                    .map_err(|_| ProviderInvocationError::Transport)?,
            )
        } else {
            None
        };
        let _permits = InvocationPermits {
            _provider: provider_permit,
            _user: user_permit,
        };

        let url = provider
            .operation_url(operation)
            .map_err(map_directory_error)?;
        let request = if let Some(body) = body {
            self.http
                .post(url)
                .header(CONTENT_TYPE, "application/json")
                .body(body)
        } else {
            self.http.get(url)
        }
        .header(ACCEPT, "application/json")
        .header(ACCEPT_ENCODING, "identity")
        .timeout(Duration::from_secs(self.limits.request_timeout_seconds));
        let response = request.send().await.map_err(map_transport_error)?;
        let status = response.status();
        if status.is_redirection() {
            return Err(ProviderInvocationError::RedirectRejected);
        }
        validate_content_type(&response)?;
        let body = read_bounded_body(response, self.response_limit).await?;

        self.directory
            .verify(provider)
            .await
            .map_err(map_directory_error)?;
        if status == StatusCode::OK {
            Ok(body)
        } else {
            let failure =
                parse_provider_failure(&body, &self.redactor).map_err(map_contract_response)?;
            Err(ProviderInvocationError::Provider(failure))
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderRequestEnvelope<'a, R> {
    schema_version: u32,
    request_id: Uuid,
    request: &'a R,
    context: &'a ProviderRequestContext,
    provider: ProviderIdentity<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderIdentity<'a> {
    provider_id: Uuid,
    extension_id: &'a str,
    instance_id: Uuid,
    implementation: &'a str,
    config: &'a serde_json::Value,
}

struct InvocationPermits {
    _provider: OwnedSemaphorePermit,
    _user: Option<OwnedSemaphorePermit>,
}

struct SemaphoreRegistry<K> {
    permits: usize,
    entries: Mutex<HashMap<K, Weak<Semaphore>>>,
}

impl<K> SemaphoreRegistry<K>
where
    K: Eq + Hash + Copy,
{
    fn new(permits: usize) -> Self {
        Self {
            permits,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self, key: K) -> Arc<Semaphore> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|_, semaphore| semaphore.strong_count() > 0);
        if let Some(semaphore) = entries.get(&key).and_then(Weak::upgrade) {
            return semaphore;
        }
        let semaphore = Arc::new(Semaphore::new(self.permits));
        entries.insert(key, Arc::downgrade(&semaphore));
        semaphore
    }
}

fn validate_content_type(response: &Response) -> Result<(), ProviderInvocationError> {
    let valid = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    if valid {
        Ok(())
    } else {
        Err(ProviderInvocationError::InvalidContentType)
    }
}

async fn read_bounded_body(
    mut response: Response,
    limit: usize,
) -> Result<Zeroizing<Vec<u8>>, ProviderInvocationError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(ProviderInvocationError::ResponseTooLarge);
    }
    let mut body = Zeroizing::new(Vec::with_capacity(
        response
            .content_length()
            .map(|length| length.min(limit as u64) as usize)
            .unwrap_or(0),
    ));
    while let Some(chunk) = response.chunk().await.map_err(map_transport_error)? {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(ProviderInvocationError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn map_transport_error(error: reqwest::Error) -> ProviderInvocationError {
    if error.is_timeout() {
        ProviderInvocationError::RequestTimeout
    } else if error.is_redirect() {
        ProviderInvocationError::RedirectRejected
    } else {
        ProviderInvocationError::Transport
    }
}

fn map_directory_error(error: ProviderDirectoryError) -> ProviderInvocationError {
    match error.code() {
        ProviderDirectoryErrorCode::RevisionChanged => ProviderInvocationError::RevisionChanged,
        ProviderDirectoryErrorCode::NotReady
        | ProviderDirectoryErrorCode::StoreUnavailable
        | ProviderDirectoryErrorCode::InvalidSnapshot => ProviderInvocationError::NotReady,
    }
}

fn map_contract_request(error: ContractError) -> ProviderInvocationError {
    ProviderInvocationError::InvalidRequest(error.code())
}

fn map_contract_response(error: ContractError) -> ProviderInvocationError {
    ProviderInvocationError::Contract(error.code())
}

fn record_provider_call<T>(
    operation: ProviderOperation,
    started: Instant,
    result: &Result<T, ProviderInvocationError>,
) {
    let outcome = match result {
        Ok(_) => "success",
        Err(error) => error.code(),
    };
    crate::live::metrics::PROVIDER_REQUESTS
        .with_label_values(&[provider_operation_label(operation), outcome])
        .inc();
    crate::live::metrics::PROVIDER_REQUEST_DURATION
        .with_label_values(&[provider_operation_label(operation), outcome])
        .observe(started.elapsed().as_secs_f64());
    if let Err(ProviderInvocationError::Contract(code)) = result {
        crate::live::metrics::PROVIDER_CONTRACT_FAILURES
            .with_label_values(&[contract_error_label(*code)])
            .inc();
    }
}

const fn provider_operation_label(operation: ProviderOperation) -> &'static str {
    match operation {
        ProviderOperation::Health => "health",
        ProviderOperation::Catalogs => "catalogs",
        ProviderOperation::Catalog => "catalog",
        ProviderOperation::Meta => "meta",
        ProviderOperation::Resolve => "resolve",
        ProviderOperation::Refresh => "refresh",
    }
}

const fn contract_error_label(code: ContractErrorCode) -> &'static str {
    match code {
        ContractErrorCode::MalformedJson => "malformed_json",
        ContractErrorCode::DuplicateJsonKey => "duplicate_json_key",
        ContractErrorCode::InvalidShape => "invalid_shape",
        ContractErrorCode::LimitExceeded => "limit_exceeded",
        ContractErrorCode::InvalidContext => "invalid_context",
        ContractErrorCode::InvalidRequest => "invalid_request",
        ContractErrorCode::InvalidId => "invalid_id",
        ContractErrorCode::InvalidText => "invalid_text",
        ContractErrorCode::InvalidDate => "invalid_date",
        ContractErrorCode::InvalidUrl => "invalid_url",
        ContractErrorCode::InvalidFilter => "invalid_filter",
        ContractErrorCode::DuplicateId => "duplicate_id",
        ContractErrorCode::TooManyInvalidItems => "too_many_invalid_items",
        ContractErrorCode::UndeclaredItemType => "undeclared_item_type",
        ContractErrorCode::UndeclaredProtocol => "undeclared_protocol",
        ContractErrorCode::ForbiddenField => "forbidden_field",
        ContractErrorCode::InvalidCredentials => "invalid_credentials",
        ContractErrorCode::DrmUnsupported => "drm_unsupported",
        ContractErrorCode::DescriptorExpired => "descriptor_expired",
        ContractErrorCode::UnsafeProviderConfig => "unsafe_provider_config",
    }
}
