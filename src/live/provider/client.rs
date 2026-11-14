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
        MAX_PROVIDER_RESPONSE_BYTES, MetaRequest, ProviderFailure, ProviderFailureCode,
        ProviderHealth, ProviderOperation, ProviderRequest, ProviderRequestContext, RefreshRequest,
        ResolveRequest, ResolvedSources, parse_catalog_page_response, parse_catalogs_response,
        parse_health_response, parse_meta_response, parse_provider_failure, parse_refresh_response,
        parse_resolve_response,
    },
    diagnostics::LiveRedactor,
};

use super::directory::{
    LiveProviderDirectory, LiveProviderSnapshot, ProviderDirectoryError, ProviderDirectoryErrorCode,
};

const MAX_PROVIDER_REQUEST_BYTES: usize = 1024 * 1024;

// The trusted transport snapshot chooses the budget. No peer/body/URL can
// select a source allowance for ordinary HTTP or raise native resource limits.
fn transport_timeouts(limits: &LiveProviderLimits, source: bool) -> (Duration, Duration) {
    let (request, hard) = if source {
        (
            limits.source_request_timeout_seconds,
            limits.source_hard_timeout_seconds,
        )
    } else {
        (limits.request_timeout_seconds, limits.hard_timeout_seconds)
    };
    (Duration::from_secs(request), Duration::from_secs(hard))
}

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
            Self::Provider(failure) => match failure.code {
                ProviderFailureCode::AccountRequired => "provider_account_required",
                _ => "provider_reported_failure",
            },
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
    // Negative-only ordering witness; never permission or native evidence.
    #[cfg(all(test, unix))]
    source_dispatch_attempts: std::sync::atomic::AtomicUsize,
}

impl LiveProviderClient {
    #[cfg(unix)]
    pub(crate) fn new_with_marketplace(
        database: crate::db::Database,
        marketplace: crate::marketplace::service::MarketplaceService,
        limits: LiveProviderLimits,
        redactor: Arc<LiveRedactor>,
    ) -> Result<Self, ProviderClientBuildError> {
        Self::new_with_directory(
            LiveProviderDirectory::with_marketplace(database, marketplace),
            limits,
            redactor,
        )
    }
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
            || !(1..=120).contains(&limits.source_request_timeout_seconds)
            || !(limits.source_request_timeout_seconds..=120)
                .contains(&limits.source_hard_timeout_seconds)
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
            #[cfg(all(test, unix))]
            source_dispatch_attempts: std::sync::atomic::AtomicUsize::new(0),
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
        #[cfg(unix)]
        let source = provider.source().is_some();
        #[cfg(not(unix))]
        let source = false;
        let (_, hard) = transport_timeouts(&self.limits, source);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ProviderInvocationError::Cancelled),
            result = tokio::time::timeout(
                hard,
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

        #[cfg(unix)]
        if let Some(source) = provider.source() {
            let result = self.call_source(source, user_id, operation, body).await;
            // The native binding also pins/rechecks this revision before code,
            // every SDK effect and result delivery. This check classifies stale
            // responses; it is not the authority for executing a new release.
            self.directory
                .verify(provider)
                .await
                .map_err(map_directory_error)?;
            return result;
        }

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

    #[cfg(unix)]
    async fn call_source(
        &self,
        source: &crate::marketplace::service::SourceProviderDescriptor,
        user: Option<Uuid>,
        operation: ProviderOperation,
        body: Option<Vec<u8>>,
    ) -> Result<Zeroizing<Vec<u8>>, ProviderInvocationError> {
        let (database, marketplace) = self
            .directory
            .source_runtime()
            .ok_or(ProviderInvocationError::NotReady)?;
        source_member(database, user, source.home).await?;
        let value = tokio::time::timeout(transport_timeouts(&self.limits, true).0, async {
            let turn = marketplace
                .source_provider_turn()
                .await
                .map_err(|_| ProviderInvocationError::NotReady)?;
            // Admission is not membership or execution authority. A queued
            // request must not use a membership check from before its wait.
            source_member(database, user, source.home).await?;
            let request = body
                .map(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes))
                .transpose()
                .map_err(|_| {
                    ProviderInvocationError::InvalidRequest(ContractErrorCode::InvalidShape)
                })?;
            let arguments =
                elixir_marketplace::source_execution::RuntimeJson::from_value(serde_json::json!({
                    "schema_version": 1,
                    "capability": "live.catalog_provider",
                    "operation": operation,
                    "request": request
                }))
                .map_err(|_| {
                    ProviderInvocationError::InvalidRequest(ContractErrorCode::LimitExceeded)
                })?;
            #[cfg(all(test, unix))]
            self.source_dispatch_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut call = marketplace
                .begin_source_provider_call(source, arguments, turn)
                .map_err(|_| ProviderInvocationError::NotReady)?;
            call.finish()
                .await
                .map_err(|_| ProviderInvocationError::Transport)
        })
        .await
        .map_err(|_| ProviderInvocationError::RequestTimeout)??;
        source_member(database, user, source.home).await?;
        self.parse_source_response(&value)
    }

    #[cfg(unix)]
    fn parse_source_response(
        &self,
        value: &elixir_marketplace::source_execution::RuntimeJson,
    ) -> Result<Zeroizing<Vec<u8>>, ProviderInvocationError> {
        // RuntimeJson has already rejected duplicate keys/depth/node/number
        // overflows. Keep the existing per-operation Live response parsers.
        if value.accounted_bytes() > self.response_limit + 4096 {
            return Err(ProviderInvocationError::ResponseTooLarge);
        }
        let response: SourceResponse = serde_json::from_str(value.as_str())
            .map_err(|_| ProviderInvocationError::Contract(ContractErrorCode::InvalidShape))?;
        if response.schema_version != 1
            || (response.status != 200 && !(400..600).contains(&response.status))
        {
            return Err(ProviderInvocationError::Contract(
                ContractErrorCode::InvalidShape,
            ));
        }
        let bytes = Zeroizing::new(
            serde_json::to_vec(&response.body)
                .map_err(|_| ProviderInvocationError::Contract(ContractErrorCode::InvalidShape))?,
        );
        if bytes.len() > self.response_limit {
            return Err(ProviderInvocationError::ResponseTooLarge);
        }
        if response.status == 200 {
            Ok(bytes)
        } else {
            Err(ProviderInvocationError::Provider(
                parse_provider_failure(&bytes, &self.redactor).map_err(map_contract_response)?,
            ))
        }
    }
}

#[cfg(unix)]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceResponse {
    schema_version: u32,
    status: u16,
    body: serde_json::Value,
}

#[cfg(unix)]
async fn source_member(
    database: &crate::db::Database,
    user: Option<Uuid>,
    home: Uuid,
) -> Result<(), ProviderInvocationError> {
    if let Some(user) = user {
        let allowed:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM home_members WHERE home_id=$1 AND user_id=$2 AND status='active') THEN 1 ELSE 0 END AS BIGINT)")
            .bind(home.to_string()).bind(user.to_string()).fetch_one(&database.pool).await.map_err(|_|ProviderInvocationError::NotReady)?;
        if user.is_nil() || allowed != 1 {
            return Err(ProviderInvocationError::NotReady);
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod source_response_tests {
    use super::*;
    use elixir_marketplace::source_execution::RuntimeJson;

    #[test]
    fn source_timeouts_do_not_widen_http_or_override_explicit_source_limits() {
        let mut limits = LiveProviderLimits::default();
        assert_eq!(
            transport_timeouts(&limits, false),
            (Duration::from_secs(5), Duration::from_secs(15))
        );
        assert_eq!(
            transport_timeouts(&limits, true),
            (Duration::from_secs(45), Duration::from_secs(60))
        );
        limits.source_request_timeout_seconds = 2;
        limits.source_hard_timeout_seconds = 3;
        assert_eq!(
            transport_timeouts(&limits, true),
            (Duration::from_secs(2), Duration::from_secs(3))
        );
        assert_eq!(
            (
                limits.concurrency_per_provider,
                limits.concurrency_per_user,
                limits.response_bytes
            ),
            (4, 8, 2_097_152)
        );
    }

    #[tokio::test]
    async fn queued_member_revocation_is_checked_before_dispatch_not_only_after_result()
    -> anyhow::Result<()> {
        use crate::{config::DatabaseConfig, db::Database};
        use std::sync::atomic::Ordering;
        // Real migrated membership SQL + actual coordination; synthetic
        // descriptor and NO controller/native executor. A dispatch counter
        // distinguishes this denial from unconfigured-executor NotReady.
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let owner = Uuid::new_v4();
        let viewer = Uuid::new_v4();
        let home = Uuid::new_v4();
        for user in [owner, viewer] {
            sqlx::query("INSERT INTO users(id,email,password_hash) VALUES($1,$2,$3)")
                .bind(user.to_string())
                .bind(format!("{user}@example.invalid"))
                .bind("negative-only-fixture")
                .execute(&database.pool)
                .await?;
        }
        sqlx::query("INSERT INTO homes(id,owner_user_id,name) VALUES($1,$2,$3)")
            .bind(home.to_string())
            .bind(owner.to_string())
            .bind("Queue membership fixture")
            .execute(&database.pool)
            .await?;
        sqlx::query("INSERT INTO home_members(id,home_id,user_id,role,status) VALUES($1,$2,$3,'viewer','active')")
            .bind(Uuid::new_v4().to_string()).bind(home.to_string()).bind(viewer.to_string())
            .execute(&database.pool).await?;
        let marketplace = crate::marketplace::service::MarketplaceService::default();
        let mut limits = crate::live::config::LiveConfig::default().providers;
        limits.request_timeout_seconds = 10;
        limits.hard_timeout_seconds = 20;
        let client = LiveProviderClient::new_with_marketplace(
            database.clone(),
            marketplace.clone(),
            limits,
            Arc::new(LiveRedactor::default()),
        )?;
        let vector: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        let descriptor = crate::marketplace::service::SourceProviderDescriptor {
            provider: Uuid::new_v4(),
            capability: crate::extensions::manifest::LIVE_CATALOG_PROVIDER_CAPABILITY,
            home,
            instance: Uuid::new_v4(),
            extension: serde_json::from_value(
                vector["release"]["payload"]["extension_id"].clone(),
            )?,
            version: "1.0.0".into(),
            implementation: "source-js-v1".into(),
            scope: crate::extensions::manifest::ManifestProviderScope::default(),
            permissions: vec![],
            config: serde_json::json!({}),
            revision: elixir_marketplace::identity::Digest::of(b"negative-only source descriptor"),
        };
        let held = marketplace.source_provider_turn().await?;
        let mut queued = Box::pin(client.call_source(
            &descriptor,
            Some(viewer),
            ProviderOperation::Catalogs,
            None,
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if marketplace.fixture_source_call_slots_used() == 2 { break; }
                tokio::select! {
                    result = queued.as_mut() => panic!("call completed before queue witness: {result:?}"),
                    _ = tokio::task::yield_now() => {},
                }
            }
        }).await?;
        assert_eq!(client.source_dispatch_attempts.load(Ordering::SeqCst), 0);
        sqlx::query("UPDATE home_members SET status='suspended' WHERE home_id=$1 AND user_id=$2")
            .bind(home.to_string())
            .bind(viewer.to_string())
            .execute(&database.pool)
            .await?;
        // Do not mistake a queue/request timeout for an authorization check.
        assert!(futures_util::poll!(queued.as_mut()).is_pending());
        drop(held);
        let error = tokio::time::timeout(Duration::from_secs(2), queued)
            .await?
            .unwrap_err();
        assert_eq!(error, ProviderInvocationError::NotReady);
        assert_eq!(
            client.source_dispatch_attempts.load(Ordering::SeqCst),
            0,
            "removed member reached the installed dispatcher"
        );
        assert_eq!(marketplace.fixture_source_call_slots_used(), 0);
        // A still-authorized request DOES reach dispatch before failing closed
        // on the deliberately missing controller. This is not native proof.
        sqlx::query("UPDATE home_members SET status='active' WHERE home_id=$1 AND user_id=$2")
            .bind(home.to_string())
            .bind(viewer.to_string())
            .execute(&database.pool)
            .await?;
        assert_eq!(
            client
                .call_source(&descriptor, Some(viewer), ProviderOperation::Catalogs, None)
                .await
                .unwrap_err(),
            ProviderInvocationError::NotReady
        );
        assert_eq!(client.source_dispatch_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(marketplace.fixture_source_call_slots_used(), 0);
        database.pool.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn source_response_is_closed_bounded_and_uses_existing_failure_parser() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::any::AnyPoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let mut limits = crate::live::config::LiveConfig::default().providers;
        limits.response_bytes = 1024;
        let client =
            LiveProviderClient::new(pool, limits, Arc::new(LiveRedactor::default())).unwrap();
        for value in [
            serde_json::json!({"schema_version":2,"status":200,"body":{}}),
            serde_json::json!({"schema_version":1,"status":201,"body":{}}),
            serde_json::json!({"schema_version":1,"status":302,"body":{}}),
            serde_json::json!({"schema_version":1,"status":600,"body":{}}),
            serde_json::json!({"schema_version":1,"status":200,"body":{},"extra":true}),
            serde_json::json!({"schema_version":1,"status":200}),
            serde_json::json!({"schema_version":1,"status":400,"body":{"arbitrary":"publisher text"}}),
        ] {
            assert!(
                client
                    .parse_source_response(&RuntimeJson::from_value(value).unwrap())
                    .is_err()
            );
        }
        let value = RuntimeJson::from_value(
            serde_json::json!({"schema_version":1,"status":200,"body":{"ok":true}}),
        )
        .unwrap();
        assert_eq!(
            &*client.parse_source_response(&value).unwrap(),
            b"{\"ok\":true}"
        );
        let value = RuntimeJson::from_value(
            serde_json::json!({"schema_version":1,"status":200,"body":{"large":"x".repeat(1024)}}),
        )
        .unwrap();
        assert_eq!(
            client.parse_source_response(&value).unwrap_err(),
            ProviderInvocationError::ResponseTooLarge
        );
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
