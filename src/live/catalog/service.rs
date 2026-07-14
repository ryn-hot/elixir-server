use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::Row;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    auth::home_profiles::{HomeRole, ProfileType},
    live::{
        contract::{
            CatalogDefinition, CatalogPage, CatalogPageRequest, CatalogSet, ItemMetadata,
            LiveItemType, MetaRequest, ProviderRequestContext, StreamProtocol,
        },
        crypto::LiveCrypto,
        provider::{LiveProviderClient, LiveProviderSnapshot, ProviderInvocationError},
    },
};

use super::{
    cache::{
        CacheFreshness, CacheKey, CacheRequest, CatalogCacheError, CatalogCacheRepository,
        CatalogCacheValue, VisibilityPartition,
    },
    circuit::{CircuitAdmission, ProviderCircuitBreakers},
    coalesce::{CatalogRequestCoalescer, CoalescedLoadError},
    grants::{LiveProviderAccess, LiveProviderGrantError, LiveProviderGrantRepository},
};

#[derive(Debug, Clone)]
pub struct LiveCatalogAccessContext {
    pub user_id: Uuid,
    pub home_id: Uuid,
    pub profile_id: Uuid,
    pub role: HomeRole,
    pub profile_type: ProfileType,
    pub authorization_revision: i64,
    pub can_browse_live: bool,
    pub locale: String,
    pub timezone: String,
    pub now: DateTime<Utc>,
}

impl LiveCatalogAccessContext {
    fn provider_context(&self) -> ProviderRequestContext {
        ProviderRequestContext {
            locale: self.locale.clone(),
            timezone: self.timezone.clone(),
            now: self.now,
        }
    }

    fn visibility(&self) -> VisibilityPartition {
        VisibilityPartition {
            home_id: self.home_id,
            profile_id: self.profile_id,
            authorization_revision: self.authorization_revision,
            access: LiveProviderAccess::Browse,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProviderCatalog {
    pub provider_id: Uuid,
    pub extension_id: String,
    pub catalogs: CatalogSet,
    pub freshness: CacheFreshness,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderScopedError {
    pub provider_id: Uuid,
    pub code: &'static str,
}

#[derive(Debug, Clone)]
pub struct AggregatedCatalogs {
    pub providers: Vec<ProviderCatalog>,
    pub errors: Vec<ProviderScopedError>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ProviderCatalogPage {
    pub provider_id: Uuid,
    pub page: CatalogPage,
    pub freshness: CacheFreshness,
}

#[derive(Debug, Clone)]
pub struct ProviderItemMetadata {
    pub provider_id: Uuid,
    pub metadata: ItemMetadata,
    pub freshness: CacheFreshness,
}

#[derive(Debug, Clone)]
pub struct VisibleLiveProvider {
    pub provider_id: Uuid,
    pub extension_id: String,
    pub name: String,
    pub readiness: VisibleProviderReadiness,
    pub disabled_reason: Option<&'static str>,
    pub item_types: Vec<LiveItemType>,
    pub protocols: Vec<StreamProtocol>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibleProviderReadiness {
    Ready,
    Degraded,
    Unavailable,
    Disabled,
}

#[derive(Debug, Error)]
pub enum CatalogServiceError {
    #[error("Live catalog access is forbidden")]
    Forbidden,
    #[error("Live catalog authorization revision changed")]
    AuthorizationChanged,
    #[error("Live provider is unavailable")]
    ProviderUnavailable,
    #[error("Live catalog was not found")]
    CatalogNotFound,
    #[error("Live provider operation failed: {0}")]
    Provider(&'static str),
    #[error("Live provider circuit is open")]
    CircuitOpen,
    #[error("Live catalog request was cancelled")]
    Cancelled,
    #[error("Live catalog cache failed")]
    Cache(#[from] CatalogCacheError),
    #[error("Live provider grant lookup failed")]
    Grant(#[from] LiveProviderGrantError),
}

impl CatalogServiceError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Forbidden => "live_browse_forbidden",
            Self::AuthorizationChanged => "authorization_revision_changed",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::CatalogNotFound => "catalog_not_found",
            Self::Provider(code) => code,
            Self::CircuitOpen => "provider_circuit_open",
            Self::Cancelled => "request_cancelled",
            Self::Cache(_) => "catalog_cache_failure",
            Self::Grant(_) => "provider_visibility_failure",
        }
    }
}

#[derive(Clone)]
pub struct LiveCatalogService {
    pool: sqlx::AnyPool,
    client: Arc<LiveProviderClient>,
    cache: CatalogCacheRepository,
    grants: LiveProviderGrantRepository,
    coalescer: CatalogRequestCoalescer,
    circuits: ProviderCircuitBreakers,
}

impl LiveCatalogService {
    pub fn new(
        pool: sqlx::AnyPool,
        crypto: Arc<LiveCrypto>,
        client: Arc<LiveProviderClient>,
    ) -> Self {
        Self {
            pool: pool.clone(),
            client,
            cache: CatalogCacheRepository::new(pool.clone(), crypto),
            grants: LiveProviderGrantRepository::new(pool),
            coalescer: CatalogRequestCoalescer::default(),
            circuits: ProviderCircuitBreakers::default(),
        }
    }

    pub fn grants(&self) -> &LiveProviderGrantRepository {
        &self.grants
    }

    pub async fn providers(
        &self,
        context: &LiveCatalogAccessContext,
        cancellation: &CancellationToken,
    ) -> Result<Vec<VisibleLiveProvider>, CatalogServiceError> {
        self.validate_context(context)?;
        let providers = sqlx::query(
            "SELECT p.provider_id, e.extension_id, e.name,
                    CAST(p.scope_json AS TEXT) AS scope_json,
                    p.health_state,
                    CAST(CASE WHEN e.enabled THEN 1 ELSE 0 END AS BIGINT) AS extension_enabled,
                    CAST(CASE WHEN i.enabled THEN 1 ELSE 0 END AS BIGINT) AS instance_enabled,
                    e.trust_level,
                    CAST(r.readiness_phase AS TEXT) AS readiness_phase
             FROM providers AS p
             JOIN extension_instances AS i ON i.instance_id = p.instance_id
             JOIN extensions AS e ON e.extension_id = i.extension_id
             LEFT JOIN provider_readiness AS r ON r.provider_id = p.provider_id
             WHERE p.capability = $1
             ORDER BY e.extension_id, p.slot_id, p.provider_id",
        )
        .bind(crate::extensions::manifest::LIVE_CATALOG_PROVIDER_CAPABILITY)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| CatalogServiceError::ProviderUnavailable)?;
        let mut visible = Vec::new();
        for row in providers {
            if cancellation.is_cancelled() {
                return Err(CatalogServiceError::Cancelled);
            }
            let provider_id = row
                .try_get::<String, _>("provider_id")
                .ok()
                .and_then(|value| Uuid::parse_str(&value).ok());
            let Some(provider_id) = provider_id else {
                tracing::warn!("excluding Live provider with invalid persisted identifier");
                continue;
            };
            match self.require_visibility(context, provider_id).await {
                Ok(()) => {}
                Err(CatalogServiceError::Forbidden) => continue,
                Err(error) => return Err(error),
            }
            let scope = row
                .try_get::<Option<String>, _>("scope_json")
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_str(&value).ok());
            let Some(scope): Option<crate::extensions::manifest::ManifestProviderScope> = scope
            else {
                tracing::warn!(provider_id = %provider_id, "excluding Live provider with invalid persisted scope");
                continue;
            };
            let item_types = scope
                .live_item_types
                .iter()
                .filter_map(|value| match value.as_str() {
                    "event" => Some(LiveItemType::Event),
                    "channel" => Some(LiveItemType::Channel),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if item_types.is_empty() {
                tracing::warn!(provider_id = %provider_id, "excluding Live provider without declared item types");
                continue;
            }
            let protocols = scope
                .stream_protocols
                .iter()
                .filter_map(|value| match value.as_str() {
                    "hls" => Some(StreamProtocol::Hls),
                    "dash" => Some(StreamProtocol::Dash),
                    "http_progressive" => Some(StreamProtocol::HttpProgressive),
                    "mpeg_ts" => Some(StreamProtocol::MpegTs),
                    "rtmp" => Some(StreamProtocol::Rtmp),
                    "srt" => Some(StreamProtocol::Srt),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let extension_enabled = row
                .try_get::<i64, _>("extension_enabled")
                .unwrap_or_default()
                != 0;
            let instance_enabled = row
                .try_get::<i64, _>("instance_enabled")
                .unwrap_or_default()
                != 0;
            let trust_level = row.try_get::<String, _>("trust_level").unwrap_or_default();
            let health = row.try_get::<String, _>("health_state").unwrap_or_default();
            let readiness_phase = row
                .try_get::<Option<String>, _>("readiness_phase")
                .unwrap_or_default();
            let (readiness, disabled_reason) = if !extension_enabled {
                (
                    VisibleProviderReadiness::Disabled,
                    Some("extension_disabled"),
                )
            } else if !instance_enabled {
                (
                    VisibleProviderReadiness::Disabled,
                    Some("instance_disabled"),
                )
            } else if trust_level == "untrusted" {
                (
                    VisibleProviderReadiness::Disabled,
                    Some("provider_untrusted"),
                )
            } else if health == "degraded" {
                (
                    VisibleProviderReadiness::Degraded,
                    Some("provider_degraded"),
                )
            } else if health != "healthy" {
                (
                    VisibleProviderReadiness::Unavailable,
                    Some("provider_unhealthy"),
                )
            } else if readiness_phase.as_deref() != Some("driver_ready") {
                (
                    VisibleProviderReadiness::Unavailable,
                    Some("runtime_not_ready"),
                )
            } else {
                (VisibleProviderReadiness::Ready, None)
            };
            visible.push(VisibleLiveProvider {
                provider_id,
                extension_id: row
                    .try_get("extension_id")
                    .map_err(|_| CatalogServiceError::ProviderUnavailable)?,
                name: row
                    .try_get("name")
                    .map_err(|_| CatalogServiceError::ProviderUnavailable)?,
                readiness,
                disabled_reason,
                item_types,
                protocols,
            });
        }
        Ok(visible)
    }

    pub async fn catalogs(
        &self,
        context: &LiveCatalogAccessContext,
        cancellation: &CancellationToken,
    ) -> Result<AggregatedCatalogs, CatalogServiceError> {
        self.validate_context(context)?;
        let providers = self
            .client
            .directory()
            .discover()
            .await
            .map_err(|_| CatalogServiceError::ProviderUnavailable)?;
        let mut output = Vec::new();
        let mut errors = Vec::new();
        for provider in providers {
            if cancellation.is_cancelled() {
                return Err(CatalogServiceError::Cancelled);
            }
            match self.require_visibility(context, provider.provider_id).await {
                Ok(()) => {}
                Err(CatalogServiceError::Forbidden) => continue,
                Err(error) => return Err(error),
            }
            let extension_id = provider.extension_id.clone();
            match self
                .cached_call(
                    context,
                    provider.clone(),
                    CacheRequest::Catalogs,
                    cancellation,
                )
                .await
            {
                Ok((value, freshness)) => match value.as_ref() {
                    CatalogCacheValue::Catalogs(catalogs) => output.push(ProviderCatalog {
                        provider_id: provider.provider_id,
                        extension_id,
                        catalogs: catalogs.clone(),
                        freshness,
                    }),
                    _ => errors.push(ProviderScopedError {
                        provider_id: provider.provider_id,
                        code: "catalog_cache_type_mismatch",
                    }),
                },
                Err(CatalogServiceError::Cancelled) => {
                    return Err(CatalogServiceError::Cancelled);
                }
                Err(error) => errors.push(ProviderScopedError {
                    provider_id: provider.provider_id,
                    code: error.code(),
                }),
            }
        }
        Ok(AggregatedCatalogs {
            providers: output,
            errors,
            generated_at: context.now,
        })
    }

    pub async fn catalog(
        &self,
        context: &LiveCatalogAccessContext,
        provider_id: Uuid,
        request: CatalogPageRequest,
        cancellation: &CancellationToken,
    ) -> Result<ProviderCatalogPage, CatalogServiceError> {
        self.validate_context(context)?;
        let provider = self.provider_for(context, provider_id).await?;
        let (value, freshness) = self
            .cached_call(
                context,
                provider,
                CacheRequest::Catalog { request },
                cancellation,
            )
            .await?;
        let CatalogCacheValue::Catalog(page) = value.as_ref() else {
            return Err(CatalogServiceError::Provider("catalog_cache_type_mismatch"));
        };
        Ok(ProviderCatalogPage {
            provider_id,
            page: page.clone(),
            freshness,
        })
    }

    pub async fn catalog_definition(
        &self,
        context: &LiveCatalogAccessContext,
        provider_id: Uuid,
        catalog_id: &str,
        cancellation: &CancellationToken,
    ) -> Result<(CatalogDefinition, CacheFreshness), CatalogServiceError> {
        self.validate_context(context)?;
        let provider = self.provider_for(context, provider_id).await?;
        let (value, freshness) = self
            .cached_call(context, provider, CacheRequest::Catalogs, cancellation)
            .await?;
        let CatalogCacheValue::Catalogs(catalogs) = value.as_ref() else {
            return Err(CatalogServiceError::Provider("catalog_cache_type_mismatch"));
        };
        let catalog = catalogs
            .catalogs
            .iter()
            .find(|catalog| catalog.id == catalog_id)
            .cloned()
            .ok_or(CatalogServiceError::CatalogNotFound)?;
        Ok((catalog, freshness))
    }

    pub async fn meta(
        &self,
        context: &LiveCatalogAccessContext,
        provider_id: Uuid,
        request: MetaRequest,
        cancellation: &CancellationToken,
    ) -> Result<ProviderItemMetadata, CatalogServiceError> {
        self.validate_context(context)?;
        let provider = self.provider_for(context, provider_id).await?;
        let (value, freshness) = self
            .cached_call(
                context,
                provider,
                CacheRequest::Meta { request },
                cancellation,
            )
            .await?;
        let CatalogCacheValue::Meta(metadata) = value.as_ref() else {
            return Err(CatalogServiceError::Provider("catalog_cache_type_mismatch"));
        };
        Ok(ProviderItemMetadata {
            provider_id,
            metadata: metadata.clone(),
            freshness,
        })
    }

    async fn provider_for(
        &self,
        context: &LiveCatalogAccessContext,
        provider_id: Uuid,
    ) -> Result<LiveProviderSnapshot, CatalogServiceError> {
        self.require_visibility(context, provider_id).await?;
        self.client
            .directory()
            .get(provider_id)
            .await
            .map_err(|_| CatalogServiceError::ProviderUnavailable)
    }

    fn validate_context(
        &self,
        context: &LiveCatalogAccessContext,
    ) -> Result<(), CatalogServiceError> {
        if !context.can_browse_live {
            return Err(CatalogServiceError::Forbidden);
        }
        if context.authorization_revision < 1
            || context.home_id.is_nil()
            || context.profile_id.is_nil()
            || context.user_id.is_nil()
            || context.provider_context().validate().is_err()
        {
            return Err(CatalogServiceError::AuthorizationChanged);
        }
        Ok(())
    }

    async fn require_visibility(
        &self,
        context: &LiveCatalogAccessContext,
        provider_id: Uuid,
    ) -> Result<(), CatalogServiceError> {
        let decision = self
            .grants
            .visibility(
                context.home_id,
                context.profile_id,
                context.role,
                context.profile_type,
                provider_id,
                LiveProviderAccess::Browse,
            )
            .await?;
        if decision.authorization_revision != context.authorization_revision {
            return Err(CatalogServiceError::AuthorizationChanged);
        }
        if !decision.allowed {
            return Err(CatalogServiceError::Forbidden);
        }
        Ok(())
    }

    async fn cached_call(
        &self,
        context: &LiveCatalogAccessContext,
        provider: LiveProviderSnapshot,
        request: CacheRequest,
        cancellation: &CancellationToken,
    ) -> Result<(Arc<CatalogCacheValue>, CacheFreshness), CatalogServiceError> {
        let key = CacheKey::derive(
            &provider,
            &request,
            &context.locale,
            &context.timezone,
            &context.visibility(),
        )?;
        if let Some(entry) = self.cache.get(&key, context.now).await? {
            if entry.freshness == CacheFreshness::Fresh {
                crate::live::metrics::CATALOG_CACHE
                    .with_label_values(&["fresh"])
                    .inc();
                return Ok((entry.value, CacheFreshness::Fresh));
            }
            crate::live::metrics::CATALOG_CACHE
                .with_label_values(&["stale"])
                .inc();
            let service = self.clone();
            let refresh_context = context.clone();
            let refresh_provider = provider.clone();
            let refresh_request = request.clone();
            let refresh_key = key.clone();
            tokio::spawn(async move {
                let cancellation = CancellationToken::new();
                let _ = service
                    .load_coalesced(
                        refresh_context,
                        refresh_provider,
                        refresh_request,
                        refresh_key,
                        &cancellation,
                    )
                    .await;
            });
            return Ok((entry.value, CacheFreshness::Stale));
        }
        crate::live::metrics::CATALOG_CACHE
            .with_label_values(&["miss"])
            .inc();
        self.load_coalesced(context.clone(), provider, request, key, cancellation)
            .await
            .map(|value| (value, CacheFreshness::Fresh))
    }

    async fn load_coalesced(
        &self,
        context: LiveCatalogAccessContext,
        provider: LiveProviderSnapshot,
        request: CacheRequest,
        key: CacheKey,
        cancellation: &CancellationToken,
    ) -> Result<Arc<CatalogCacheValue>, CatalogServiceError> {
        let service = self.clone();
        let result = self
            .coalescer
            .run(key.clone(), cancellation, move |upstream| async move {
                service
                    .load_provider(context, provider, request, key, upstream)
                    .await
                    .map_err(|error| CoalescedLoadError::Failed(error.code()))
            })
            .await;
        match result {
            Ok(value) => Ok(value),
            Err(CoalescedLoadError::Cancelled) => Err(CatalogServiceError::Cancelled),
            Err(CoalescedLoadError::Failed("provider_circuit_open")) => {
                Err(CatalogServiceError::CircuitOpen)
            }
            Err(CoalescedLoadError::Failed(code)) => Err(CatalogServiceError::Provider(code)),
        }
    }

    async fn load_provider(
        &self,
        context: LiveCatalogAccessContext,
        provider: LiveProviderSnapshot,
        request: CacheRequest,
        key: CacheKey,
        cancellation: CancellationToken,
    ) -> Result<Arc<CatalogCacheValue>, CatalogServiceError> {
        let operation = request.operation();
        if self.circuits.admit(provider.provider_id, operation).await == CircuitAdmission::Open {
            return Err(CatalogServiceError::CircuitOpen);
        }
        let provider_context = context.provider_context();
        let result = match &request {
            CacheRequest::Catalogs => self
                .client
                .catalogs(&provider, context.user_id, &provider_context, &cancellation)
                .await
                .map(CatalogCacheValue::Catalogs),
            CacheRequest::Catalog { request } => self
                .client
                .catalog(
                    &provider,
                    context.user_id,
                    &provider_context,
                    request,
                    &cancellation,
                )
                .await
                .map(CatalogCacheValue::Catalog),
            CacheRequest::Meta { request } => self
                .client
                .meta(
                    &provider,
                    context.user_id,
                    &provider_context,
                    request,
                    &cancellation,
                )
                .await
                .map(CatalogCacheValue::Meta),
        };
        match result {
            Ok(value) => {
                self.circuits
                    .record_success(provider.provider_id, operation)
                    .await;
                let entry = self
                    .cache
                    .put(&key, provider.provider_id, &value, context.now)
                    .await?;
                Ok(entry.value)
            }
            Err(error) => {
                if circuit_failure(&error) {
                    self.circuits
                        .record_failure(provider.provider_id, operation)
                        .await;
                }
                if error == ProviderInvocationError::Cancelled {
                    Err(CatalogServiceError::Cancelled)
                } else {
                    Err(CatalogServiceError::Provider(error.code()))
                }
            }
        }
    }
}

fn circuit_failure(error: &ProviderInvocationError) -> bool {
    matches!(
        error,
        ProviderInvocationError::RequestTimeout
            | ProviderInvocationError::HardTimeout
            | ProviderInvocationError::Transport
            | ProviderInvocationError::RedirectRejected
            | ProviderInvocationError::InvalidContentType
            | ProviderInvocationError::ResponseTooLarge
            | ProviderInvocationError::Contract(_)
            | ProviderInvocationError::Provider(_)
    )
}
