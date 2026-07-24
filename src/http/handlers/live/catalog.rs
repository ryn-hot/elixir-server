use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use axum::{
    async_trait,
    body::Body,
    extract::{FromRequestParts, Path, RawQuery, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ACCEPT_LANGUAGE, CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH, VARY},
        request::Parts,
    },
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    authz::Capability,
    http::{
        auth::{AccountAuthTransport, CurrentPrincipal},
        error::ApiError,
    },
    live::{
        catalog::{
            CacheFreshness, CatalogServiceError, LiveArtworkKind, LiveCatalogAccessContext,
            LivePublicKeyCodec, LivePublicKeyError, LivePublicKeyScope, ProviderCatalog,
            ProviderScopedError, VisibleLiveProvider, VisibleProviderAccountState,
            VisibleProviderReadiness,
        },
        contract::{
            ArtworkSource, CatalogDefinition, CatalogPageRequest, Fact, FilterDefinition,
            FilterKind, FilterOption, FilterValue, LiveItem, LiveItemStatus, LiveItemType,
            MetaRequest, StreamChoice, StreamProtocol,
        },
    },
    state::AppState,
};

const DEFAULT_PAGE_LIMIT: u16 = 40;
const MAX_QUERY_BYTES: usize = 8_192;
const MAX_QUERY_PAIRS: usize = 32;
const MAX_ADMISSION_USERS: usize = 10_000;
const MAX_REQUESTS_PER_MINUTE: u32 = 120;
const MAX_CONCURRENT_REQUESTS: u32 = 16;
const MAX_ARTWORK_REQUESTS_PER_MINUTE: u32 = 600;
const MAX_CONCURRENT_ARTWORK_REQUESTS: u32 = 32;
const ADMISSION_WINDOW: Duration = Duration::from_secs(60);
const ADMISSION_IDLE_TTL: Duration = Duration::from_secs(5 * 60);

static ADMISSION: OnceLock<Mutex<HashMap<Uuid, AdmissionState>>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionClass {
    Interactive,
    Artwork,
}

#[derive(Debug)]
struct AdmissionState {
    last_seen: Instant,
    interactive: AdmissionWindow,
    artwork: AdmissionWindow,
}

impl AdmissionState {
    fn new(now: Instant) -> Self {
        Self {
            last_seen: now,
            interactive: AdmissionWindow::new(now),
            artwork: AdmissionWindow::new(now),
        }
    }

    fn window_mut(&mut self, class: AdmissionClass) -> &mut AdmissionWindow {
        match class {
            AdmissionClass::Interactive => &mut self.interactive,
            AdmissionClass::Artwork => &mut self.artwork,
        }
    }
}

#[derive(Debug)]
struct AdmissionWindow {
    window_started: Instant,
    requests: u32,
    concurrent: u32,
}

impl AdmissionWindow {
    fn new(now: Instant) -> Self {
        Self {
            window_started: now,
            requests: 0,
            concurrent: 0,
        }
    }
}

#[derive(Debug)]
pub(super) struct AdmissionGuard {
    user_id: Uuid,
    class: AdmissionClass,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if let Ok(mut states) = admission().lock() {
            if let Some(state) = states.get_mut(&self.user_id) {
                let window = state.window_mut(self.class);
                window.concurrent = window.concurrent.saturating_sub(1);
                state.last_seen = Instant::now();
            }
        }
    }
}

fn admission() -> &'static Mutex<HashMap<Uuid, AdmissionState>> {
    ADMISSION.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn admit(user_id: Uuid) -> Result<AdmissionGuard, LiveHttpRejection> {
    admit_class(user_id, AdmissionClass::Interactive)
}

pub(super) fn admit_artwork(user_id: Uuid) -> Result<AdmissionGuard, LiveHttpRejection> {
    admit_class(user_id, AdmissionClass::Artwork)
}

fn admit_class(user_id: Uuid, class: AdmissionClass) -> Result<AdmissionGuard, LiveHttpRejection> {
    let now = Instant::now();
    let mut states = admission()
        .lock()
        .map_err(|_| LiveHttpRejection::unavailable())?;
    if states.len() >= MAX_ADMISSION_USERS && !states.contains_key(&user_id) {
        states.retain(|_, state| now.duration_since(state.last_seen) < ADMISSION_IDLE_TTL);
        if states.len() >= MAX_ADMISSION_USERS {
            return Err(LiveHttpRejection::rate_limited(1));
        }
    }
    let state = states
        .entry(user_id)
        .or_insert_with(|| AdmissionState::new(now));
    let (max_requests, max_concurrent) = match class {
        AdmissionClass::Interactive => (MAX_REQUESTS_PER_MINUTE, MAX_CONCURRENT_REQUESTS),
        AdmissionClass::Artwork => (
            MAX_ARTWORK_REQUESTS_PER_MINUTE,
            MAX_CONCURRENT_ARTWORK_REQUESTS,
        ),
    };
    let window = state.window_mut(class);
    if now.duration_since(window.window_started) >= ADMISSION_WINDOW {
        window.window_started = now;
        window.requests = 0;
    }
    if window.requests >= max_requests {
        let remaining = ADMISSION_WINDOW.saturating_sub(now.duration_since(window.window_started));
        let retry_after_seconds = remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() > 0))
            .clamp(1, u64::from(u32::MAX)) as u32;
        return Err(LiveHttpRejection::rate_limited(retry_after_seconds));
    }
    if window.concurrent >= max_concurrent {
        return Err(LiveHttpRejection::rate_limited(1));
    }
    window.requests += 1;
    window.concurrent += 1;
    state.last_seen = now;
    Ok(AdmissionGuard { user_id, class })
}

pub(super) struct CancelOnDrop(CancellationToken);

impl CancelOnDrop {
    pub(super) fn new() -> Self {
        Self(CancellationToken::new())
    }

    pub(super) fn token(&self) -> &CancellationToken {
        &self.0
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub struct LiveBrowsePrincipal(pub(super) CurrentPrincipal);

#[async_trait]
impl FromRequestParts<AppState> for LiveBrowsePrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled || !state.live.config().catalog_enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if !principal.has_capability(Capability::LiveBrowse) {
            return Err(LiveHttpRejection::capability_required());
        }
        let snapshot = state.live.snapshot().await;
        let catalog_ready = snapshot
            .features
            .iter()
            .any(|feature| feature.flag == "catalog_enabled" && feature.effective_enabled);
        if !catalog_ready || state.live.catalog_service().is_none() {
            return Err(LiveHttpRejection::unavailable());
        }
        Ok(Self(principal))
    }
}

#[derive(Debug)]
pub struct LiveHttpRejection {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    retryable: bool,
    retry_after_seconds: Option<u32>,
    provider_id: Option<Uuid>,
}

impl LiveHttpRejection {
    pub(super) fn invalid_request() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "LIVE_INVALID_REQUEST",
            "The Live request is invalid.",
            false,
        )
    }

    pub(super) fn auth_required() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "LIVE_AUTH_REQUIRED",
            "Account authentication is required.",
            false,
        )
    }

    pub(super) fn capability_required() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "LIVE_CAPABILITY_REQUIRED",
            "The active profile cannot browse Live providers.",
            false,
        )
    }

    pub(super) fn provider_forbidden() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "LIVE_PROVIDER_FORBIDDEN",
            "The provider is not shared with the active profile.",
            false,
        )
    }

    pub(super) fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "LIVE_PROVIDER_NOT_FOUND",
            "The Live resource was not found.",
            false,
        )
    }

    pub(super) fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIVE_PROVIDER_UNAVAILABLE",
            "The Live provider service is unavailable.",
            true,
        )
    }

    fn rate_limited(retry_after_seconds: u32) -> Self {
        let mut error = Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "LIVE_RATE_LIMITED",
            "The Live request rate limit was reached.",
            true,
        );
        error.retry_after_seconds = Some(retry_after_seconds.max(1));
        error
    }

    pub(super) const fn new(
        status: StatusCode,
        code: &'static str,
        message: &'static str,
        retryable: bool,
    ) -> Self {
        Self {
            status,
            code,
            message,
            retryable,
            retry_after_seconds: None,
            provider_id: None,
        }
    }

    pub(super) fn with_provider(mut self, provider_id: Uuid) -> Self {
        self.provider_id = Some(provider_id);
        self
    }

    pub(super) fn from_auth_error(error: ApiError) -> Self {
        match error.into_response().status() {
            StatusCode::FORBIDDEN => Self::capability_required(),
            _ => Self::auth_required(),
        }
    }
}

impl IntoResponse for LiveHttpRejection {
    fn into_response(self) -> Response {
        error_response(self, None)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiMeta {
    request_id: String,
    generated_at: String,
    cache_state: &'static str,
    partial: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveApiError {
    code: &'static str,
    message: &'static str,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<Uuid>,
}

#[derive(Serialize)]
struct LiveEnvelope<T: Serialize> {
    data: T,
    meta: ApiMeta,
    errors: Vec<LiveApiError>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderSummaryDto {
    provider_id: Uuid,
    instance_id: Uuid,
    extension_id: String,
    name: String,
    readiness: &'static str,
    disabled_reason: Option<String>,
    account_state: &'static str,
    contract_version: u32,
    item_types: Vec<LiveItemType>,
    protocols: Vec<StreamProtocol>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogDto {
    provider_id: Uuid,
    catalog_id: String,
    name: String,
    description: Option<String>,
    item_types: Vec<LiveItemType>,
    presentation: crate::live::contract::CatalogPresentation,
    order: i32,
    filters: Vec<FilterDefinitionDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FilterDefinitionDto {
    id: String,
    label: String,
    #[serde(rename = "type")]
    kind: FilterKind,
    required: bool,
    default: Option<FilterValue>,
    options: Vec<FilterOption>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtworkDto {
    artwork_id: String,
    url: String,
    kind: LiveArtworkKind,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ItemDto {
    provider_id: Uuid,
    item_key: String,
    item_type: LiveItemType,
    title: String,
    subtitle: Option<String>,
    description: Option<String>,
    status: LiveItemStatus,
    starts_at: Option<DateTime<Utc>>,
    ends_at: Option<DateTime<Utc>>,
    poster: Option<ArtworkDto>,
    background: Option<ArtworkDto>,
    logo: Option<ArtworkDto>,
    categories: Vec<String>,
    badges: Vec<String>,
    facts: Vec<Fact>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamChoiceDto {
    stream_option_key: String,
    label: String,
    quality: Option<String>,
    language: Option<String>,
    protocol_hint: Option<StreamProtocol>,
    priority: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogPageDto {
    provider_id: Uuid,
    catalog_id: String,
    items: Vec<ItemDto>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct ItemMetadataDto {
    item: ItemDto,
    streams: Vec<StreamChoiceDto>,
}

pub async fn providers(
    State(state): State<AppState>,
    LiveBrowsePrincipal(principal): LiveBrowsePrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let context = access_context(&principal, &headers)?;
        let cancellation = CancelOnDrop::new();
        let service = state
            .live
            .catalog_service()
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let providers = service
            .providers(&context, cancellation.token())
            .await
            .map_err(map_service_error)?;
        let data = providers.into_iter().map(provider_dto).collect::<Vec<_>>();
        Ok(success_response(
            &headers,
            data,
            request_id,
            context.now,
            "none",
            Vec::new(),
            None,
        ))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn catalogs(
    State(state): State<AppState>,
    LiveBrowsePrincipal(principal): LiveBrowsePrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let context = access_context(&principal, &headers)?;
        let cancellation = CancelOnDrop::new();
        let service = state
            .live
            .catalog_service()
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let aggregated = service
            .catalogs(&context, cancellation.token())
            .await
            .map_err(map_service_error)?;
        let cache_state = aggregate_freshness(&aggregated.providers);
        let mut data = Vec::new();
        for provider in aggregated.providers {
            let mut catalogs = provider.catalogs.catalogs;
            catalogs.sort_by(|left, right| {
                left.order
                    .cmp(&right.order)
                    .then_with(|| left.name.cmp(&right.name))
                    .then_with(|| left.id.cmp(&right.id))
            });
            data.extend(
                catalogs
                    .into_iter()
                    .map(|catalog| catalog_dto(provider.provider_id, catalog)),
            );
        }
        let errors = aggregated
            .errors
            .into_iter()
            .map(provider_error_dto)
            .collect::<Vec<_>>();
        Ok(success_response(
            &headers,
            data,
            request_id,
            aggregated.generated_at,
            cache_state,
            errors,
            None,
        ))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn catalog_items(
    State(state): State<AppState>,
    LiveBrowsePrincipal(principal): LiveBrowsePrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path((provider_id, catalog_id)): Path<(String, String)>,
) -> Response {
    let request_id = request_id();
    let result = async {
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let context = access_context(&principal, &headers)?;
        let scope = key_scope(&context);
        let crypto = state
            .live
            .crypto()
            .await
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let keys = LivePublicKeyCodec::new(crypto);
        let cancellation = CancelOnDrop::new();
        let service = state
            .live
            .catalog_service()
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let (definition, definition_freshness) = service
            .catalog_definition(&context, provider_id, &catalog_id, cancellation.token())
            .await
            .map_err(|error| map_service_error(error).with_provider(provider_id))?;
        let query = parse_catalog_query(raw_query.as_deref(), &definition)?;
        let cursor = query
            .cursor
            .as_deref()
            .map(|cursor| {
                keys.open_cursor(cursor, provider_id, &catalog_id, scope, context.now)
                    .map_err(map_key_error)
            })
            .transpose()?;
        let page = service
            .catalog(
                &context,
                provider_id,
                CatalogPageRequest {
                    catalog_id: catalog_id.clone(),
                    cursor,
                    limit: query.limit,
                    filters: query.filters,
                },
                cancellation.token(),
            )
            .await
            .map_err(|error| map_service_error(error).with_provider(provider_id))?;
        let stable_etag = catalog_page_etag(provider_id, &catalog_id, &page.page);
        let next_cursor = page
            .page
            .next_cursor
            .as_deref()
            .map(|cursor| {
                keys.seal_cursor(provider_id, &catalog_id, cursor, scope, context.now)
                    .map_err(map_key_error)
            })
            .transpose()?;
        let items = page
            .page
            .items
            .into_iter()
            .map(|item| item_dto(&keys, provider_id, item, scope, context.now))
            .collect::<Result<Vec<_>, _>>()?;
        let freshness = if page.freshness == CacheFreshness::Stale
            || definition_freshness == CacheFreshness::Stale
        {
            "stale"
        } else {
            "fresh"
        };
        Ok(success_response(
            &headers,
            CatalogPageDto {
                provider_id,
                catalog_id,
                items,
                next_cursor,
            },
            request_id,
            context.now,
            freshness,
            Vec::new(),
            Some(stable_etag),
        ))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn item(
    State(state): State<AppState>,
    LiveBrowsePrincipal(principal): LiveBrowsePrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path((provider_id, item_key)): Path<(String, String)>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let context = access_context(&principal, &headers)?;
        let scope = key_scope(&context);
        let crypto = state
            .live
            .crypto()
            .await
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let keys = LivePublicKeyCodec::new(crypto);
        let item_id = keys
            .open_item(&item_key, provider_id, scope, context.now)
            .map_err(map_key_error)?;
        let cancellation = CancelOnDrop::new();
        let service = state
            .live
            .catalog_service()
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let metadata = service
            .meta(
                &context,
                provider_id,
                MetaRequest {
                    item_id: item_id.clone(),
                },
                cancellation.token(),
            )
            .await
            .map_err(|error| map_service_error(error).with_provider(provider_id))?;
        let stable_etag = item_metadata_etag(provider_id, &metadata.metadata);
        let item = item_dto(
            &keys,
            provider_id,
            metadata.metadata.item,
            scope,
            context.now,
        )?;
        let streams = metadata
            .metadata
            .streams
            .into_iter()
            .map(|stream| stream_dto(&keys, provider_id, &item_id, stream, scope, context.now))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(success_response(
            &headers,
            ItemMetadataDto { item, streams },
            request_id,
            context.now,
            freshness_name(metadata.freshness),
            Vec::new(),
            Some(stable_etag),
        ))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub(super) fn access_context(
    principal: &CurrentPrincipal,
    headers: &HeaderMap,
) -> Result<LiveCatalogAccessContext, LiveHttpRejection> {
    let locale = header_value(headers, ACCEPT_LANGUAGE.as_str())?
        .map(|value| value.split(',').next().unwrap_or("").trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "en-US".to_string());
    let timezone = header_value(headers, "x-elixir-timezone")?.unwrap_or_else(|| "UTC".to_string());
    if !(2..=64).contains(&locale.chars().count())
        || locale.chars().any(char::is_control)
        || !(1..=128).contains(&timezone.chars().count())
        || timezone.chars().any(char::is_control)
    {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(LiveCatalogAccessContext {
        user_id: principal.user_id,
        home_id: principal.home_id,
        profile_id: principal.profile_id,
        role: principal.role,
        profile_type: principal.profile_type,
        authorization_revision: principal.capability_revision,
        can_browse_live: principal.has_capability(Capability::LiveBrowse),
        locale,
        timezone,
        now: Utc::now(),
    })
}

fn header_value(headers: &HeaderMap, name: &str) -> Result<Option<String>, LiveHttpRejection> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_string)
                .map_err(|_| LiveHttpRejection::invalid_request())
        })
        .transpose()
}

pub(super) fn key_scope(context: &LiveCatalogAccessContext) -> LivePublicKeyScope {
    LivePublicKeyScope {
        home_id: context.home_id,
        profile_id: context.profile_id,
        authorization_revision: context.authorization_revision,
    }
}

struct CatalogQuery {
    cursor: Option<String>,
    limit: u16,
    filters: BTreeMap<String, FilterValue>,
}

fn parse_catalog_query(
    raw: Option<&str>,
    definition: &CatalogDefinition,
) -> Result<CatalogQuery, LiveHttpRejection> {
    let raw = raw.unwrap_or_default();
    if raw.len() > MAX_QUERY_BYTES {
        return Err(LiveHttpRejection::invalid_request());
    }
    let pairs: Vec<(String, String)> =
        serde_urlencoded::from_str(raw).map_err(|_| LiveHttpRejection::invalid_request())?;
    if pairs.len() > MAX_QUERY_PAIRS {
        return Err(LiveHttpRejection::invalid_request());
    }
    let mut cursor = None;
    let mut limit = None;
    let mut raw_filters: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (key, value) in pairs {
        match key.as_str() {
            "cursor" if cursor.is_none() => cursor = Some(value),
            "limit" if limit.is_none() => {
                limit = Some(
                    value
                        .parse::<u16>()
                        .map_err(|_| LiveHttpRejection::invalid_request())?,
                )
            }
            _ if key.starts_with("filters[") && key.ends_with(']') => {
                let id = &key[8..key.len() - 1];
                if id.is_empty() || id.len() > 128 {
                    return Err(LiveHttpRejection::invalid_request());
                }
                raw_filters.entry(id.to_string()).or_default().push(value);
            }
            _ => return Err(LiveHttpRejection::invalid_request()),
        }
    }
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if !(1..=100).contains(&limit) {
        return Err(LiveHttpRejection::invalid_request());
    }
    let mut filters = BTreeMap::new();
    for filter in &definition.filters {
        if let Some(values) = raw_filters.remove(&filter.id) {
            filters.insert(filter.id.clone(), parse_filter_value(filter, values)?);
        } else if let Some(default) = filter.default.clone() {
            filters.insert(filter.id.clone(), default);
        } else if filter.required {
            return Err(LiveHttpRejection::invalid_request());
        }
    }
    if !raw_filters.is_empty() || definition.validate_filter_submission(&filters).is_err() {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(CatalogQuery {
        cursor,
        limit,
        filters,
    })
}

fn parse_filter_value(
    definition: &FilterDefinition,
    values: Vec<String>,
) -> Result<FilterValue, LiveHttpRejection> {
    match definition.kind {
        FilterKind::Toggle if values.len() == 1 => match values[0].as_str() {
            "true" => Ok(FilterValue::Toggle(true)),
            "false" => Ok(FilterValue::Toggle(false)),
            _ => Err(LiveHttpRejection::invalid_request()),
        },
        FilterKind::SingleSelect | FilterKind::Search | FilterKind::Date if values.len() == 1 => {
            Ok(FilterValue::Text(
                values.into_iter().next().unwrap_or_default(),
            ))
        }
        FilterKind::MultiSelect => {
            let values = values
                .into_iter()
                .flat_map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            if values.is_empty() || values.iter().any(String::is_empty) {
                Err(LiveHttpRejection::invalid_request())
            } else {
                Ok(FilterValue::Multiple(values))
            }
        }
        _ => Err(LiveHttpRejection::invalid_request()),
    }
}

fn provider_dto(provider: VisibleLiveProvider) -> ProviderSummaryDto {
    ProviderSummaryDto {
        provider_id: provider.provider_id,
        instance_id: provider.instance_id,
        extension_id: provider.extension_id,
        name: provider.name,
        readiness: match provider.readiness {
            VisibleProviderReadiness::Ready => "ready",
            VisibleProviderReadiness::Degraded => "degraded",
            VisibleProviderReadiness::NeedsAccount => "needs_account",
            VisibleProviderReadiness::Unavailable => "unavailable",
            VisibleProviderReadiness::Disabled => "disabled",
        },
        disabled_reason: provider.disabled_reason.map(str::to_string),
        account_state: match provider.account_state {
            VisibleProviderAccountState::NotRequired => "not_required",
            VisibleProviderAccountState::NeedsAccount => "needs_account",
            VisibleProviderAccountState::Connected => "connected",
        },
        contract_version: 1,
        item_types: provider.item_types,
        protocols: provider.protocols,
    }
}

fn catalog_dto(provider_id: Uuid, catalog: CatalogDefinition) -> CatalogDto {
    CatalogDto {
        provider_id,
        catalog_id: catalog.id,
        name: catalog.name,
        description: catalog.description,
        item_types: catalog.item_types.into_iter().collect(),
        presentation: catalog.presentation,
        order: catalog.order,
        filters: catalog
            .filters
            .into_iter()
            .map(|filter| FilterDefinitionDto {
                id: filter.id,
                label: filter.label,
                kind: filter.kind,
                required: filter.required,
                default: filter.default,
                options: filter.options,
            })
            .collect(),
    }
}

fn item_dto(
    keys: &LivePublicKeyCodec,
    provider_id: Uuid,
    item: LiveItem,
    scope: LivePublicKeyScope,
    now: DateTime<Utc>,
) -> Result<ItemDto, LiveHttpRejection> {
    let item_key = keys
        .seal_item(provider_id, &item.id, scope, now)
        .map_err(map_key_error)?;
    Ok(ItemDto {
        provider_id,
        item_key,
        item_type: item.item_type,
        title: item.title,
        subtitle: item.subtitle,
        description: item.description,
        status: item.status,
        starts_at: item.starts_at,
        ends_at: item.ends_at,
        poster: artwork_dto(
            keys,
            provider_id,
            &item.id,
            LiveArtworkKind::Poster,
            item.poster,
            scope,
            now,
        )?,
        background: artwork_dto(
            keys,
            provider_id,
            &item.id,
            LiveArtworkKind::Background,
            item.background,
            scope,
            now,
        )?,
        logo: artwork_dto(
            keys,
            provider_id,
            &item.id,
            LiveArtworkKind::Logo,
            item.logo,
            scope,
            now,
        )?,
        categories: item.categories,
        badges: item.badges,
        facts: item.facts,
    })
}

fn artwork_dto(
    keys: &LivePublicKeyCodec,
    provider_id: Uuid,
    item_id: &str,
    kind: LiveArtworkKind,
    artwork: Option<ArtworkSource>,
    scope: LivePublicKeyScope,
    now: DateTime<Utc>,
) -> Result<Option<ArtworkDto>, LiveHttpRejection> {
    let Some(artwork) = artwork else {
        return Ok(None);
    };
    let artwork_id =
        match keys.seal_artwork(provider_id, item_id, kind, artwork.expose(), scope, now) {
            Ok(key) => key,
            Err(LivePublicKeyError::InvalidInput) => return Ok(None),
            Err(error) => return Err(map_key_error(error)),
        };
    Ok(Some(ArtworkDto {
        url: format!("/api/v1/live/artwork/{artwork_id}"),
        artwork_id,
        kind,
    }))
}

fn stream_dto(
    keys: &LivePublicKeyCodec,
    provider_id: Uuid,
    item_id: &str,
    stream: StreamChoice,
    scope: LivePublicKeyScope,
    now: DateTime<Utc>,
) -> Result<StreamChoiceDto, LiveHttpRejection> {
    Ok(StreamChoiceDto {
        stream_option_key: keys
            .seal_stream(provider_id, item_id, &stream.id, scope, now)
            .map_err(map_key_error)?,
        label: stream.label,
        quality: stream.quality,
        language: stream.language,
        protocol_hint: stream.protocol_hint,
        priority: stream.priority,
    })
}

fn aggregate_freshness(providers: &[ProviderCatalog]) -> &'static str {
    if providers.is_empty() {
        "none"
    } else if providers
        .iter()
        .any(|provider| provider.freshness == CacheFreshness::Stale)
    {
        "stale"
    } else {
        "fresh"
    }
}

const fn freshness_name(freshness: CacheFreshness) -> &'static str {
    match freshness {
        CacheFreshness::Fresh => "fresh",
        CacheFreshness::Stale => "stale",
    }
}

fn provider_error_dto(error: ProviderScopedError) -> LiveApiError {
    let (code, message, retryable) = provider_error_fields(error.code);
    LiveApiError {
        code,
        message,
        retryable,
        retry_after_seconds: None,
        provider_id: Some(error.provider_id),
    }
}

fn provider_error_fields(code: &str) -> (&'static str, &'static str, bool) {
    match code {
        "provider_account_required" => (
            "LIVE_ACCOUNT_REQUIRED",
            "Connect or reconnect this Live provider account.",
            false,
        ),
        "provider_request_timeout" | "provider_hard_timeout" => (
            "LIVE_PROVIDER_TIMEOUT",
            "The Live provider did not respond in time.",
            true,
        ),
        "provider_request_invalid" | "provider_contract_failure" => (
            "LIVE_CONTRACT_INVALID",
            "The Live provider returned an invalid response.",
            false,
        ),
        _ => (
            "LIVE_PROVIDER_UNAVAILABLE",
            "The Live provider is temporarily unavailable.",
            true,
        ),
    }
}

fn map_service_error(error: CatalogServiceError) -> LiveHttpRejection {
    match error {
        CatalogServiceError::Forbidden => LiveHttpRejection::provider_forbidden(),
        CatalogServiceError::CatalogNotFound => LiveHttpRejection::not_found(),
        CatalogServiceError::Cancelled => LiveHttpRejection::new(
            StatusCode::REQUEST_TIMEOUT,
            "LIVE_PROVIDER_TIMEOUT",
            "The Live request was cancelled.",
            true,
        ),
        CatalogServiceError::Provider(code) if code == "provider_account_required" => {
            LiveHttpRejection::new(
                StatusCode::CONFLICT,
                "LIVE_ACCOUNT_REQUIRED",
                "Connect or reconnect this Live provider account.",
                false,
            )
        }
        CatalogServiceError::Provider(code)
            if matches!(code, "provider_request_timeout" | "provider_hard_timeout") =>
        {
            LiveHttpRejection::new(
                StatusCode::GATEWAY_TIMEOUT,
                "LIVE_PROVIDER_TIMEOUT",
                "The Live provider did not respond in time.",
                true,
            )
        }
        CatalogServiceError::Provider(code)
            if matches!(
                code,
                "provider_request_invalid" | "provider_contract_failure"
            ) =>
        {
            LiveHttpRejection::new(
                StatusCode::BAD_GATEWAY,
                "LIVE_CONTRACT_INVALID",
                "The Live provider returned an invalid response.",
                false,
            )
        }
        CatalogServiceError::AuthorizationChanged => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_REVISION_CONFLICT",
            "The active profile authorization changed.",
            false,
        ),
        CatalogServiceError::ProviderUnavailable
        | CatalogServiceError::CircuitOpen
        | CatalogServiceError::Cache(_)
        | CatalogServiceError::Grant(_) => LiveHttpRejection::unavailable(),
        CatalogServiceError::Provider(_) => LiveHttpRejection::new(
            StatusCode::BAD_GATEWAY,
            "LIVE_PROVIDER_UNAVAILABLE",
            "The Live provider is temporarily unavailable.",
            true,
        ),
    }
}

fn map_key_error(_: LivePublicKeyError) -> LiveHttpRejection {
    LiveHttpRejection::invalid_request()
}

fn parse_uuid(value: &str) -> Result<Uuid, LiveHttpRejection> {
    Uuid::parse_str(value).map_err(|_| LiveHttpRejection::invalid_request())
}

pub(super) fn reject_query(raw: Option<&str>) -> Result<(), LiveHttpRejection> {
    if raw.is_some_and(|query| !query.is_empty()) {
        Err(LiveHttpRejection::invalid_request())
    } else {
        Ok(())
    }
}

pub(super) fn request_id() -> Uuid {
    Uuid::new_v4()
}

fn success_response<T: Serialize>(
    request_headers: &HeaderMap,
    data: T,
    request_id: Uuid,
    generated_at: DateTime<Utc>,
    cache_state: &'static str,
    errors: Vec<LiveApiError>,
    stable_etag: Option<String>,
) -> Response {
    let partial = !errors.is_empty();
    let etag_material = serde_json::to_vec(&json!({
        "data": &data,
        "cacheState": cache_state,
        "errors": &errors,
    }))
    .unwrap_or_default();
    let etag = stable_etag.unwrap_or_else(|| {
        format!(
            "\"{}\"",
            general_purpose::URL_SAFE_NO_PAD.encode(blake3::hash(&etag_material).as_bytes())
        )
    });
    if request_headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|value| value.trim() == etag))
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        apply_success_headers(response.headers_mut(), &etag);
        return response;
    }
    let envelope = LiveEnvelope {
        data,
        meta: ApiMeta {
            request_id: request_id.to_string(),
            generated_at: generated_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            cache_state,
            partial,
        },
        errors,
    };
    let body = match serde_json::to_vec(&envelope) {
        Ok(body) => body,
        Err(_) => {
            return error_response(LiveHttpRejection::unavailable(), Some(request_id));
        }
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    apply_success_headers(response.headers_mut(), &etag);
    response
}

fn catalog_page_etag(
    provider_id: Uuid,
    catalog_id: &str,
    page: &crate::live::contract::CatalogPage,
) -> String {
    let material = json!({
        "providerId": provider_id,
        "catalogId": catalog_id,
        "items": page.items.iter().map(stable_item_value).collect::<Vec<_>>(),
        "nextCursor": page.next_cursor,
        "diagnostics": page.diagnostics,
        "providerEtag": page.cache.etag,
    });
    quoted_etag(&material)
}

fn item_metadata_etag(provider_id: Uuid, metadata: &crate::live::contract::ItemMetadata) -> String {
    let material = json!({
        "providerId": provider_id,
        "item": stable_item_value(&metadata.item),
        "streams": metadata.streams,
        "providerEtag": metadata.cache.etag,
    });
    quoted_etag(&material)
}

fn stable_item_value(item: &LiveItem) -> Value {
    json!({
        "id": item.id,
        "itemType": item.item_type,
        "title": item.title,
        "subtitle": item.subtitle,
        "description": item.description,
        "status": item.status,
        "startsAt": item.starts_at,
        "endsAt": item.ends_at,
        "poster": item.poster.as_ref().map(ArtworkSource::expose),
        "background": item.background.as_ref().map(ArtworkSource::expose),
        "logo": item.logo.as_ref().map(ArtworkSource::expose),
        "categories": item.categories,
        "badges": item.badges,
        "facts": item.facts,
    })
}

fn quoted_etag(material: &Value) -> String {
    let encoded = serde_json::to_vec(material).unwrap_or_default();
    format!(
        "\"{}\"",
        general_purpose::URL_SAFE_NO_PAD.encode(blake3::hash(&encoded).as_bytes())
    )
}

fn apply_success_headers(headers: &mut HeaderMap, etag: &str) {
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=0, must-revalidate"),
    );
    headers.insert(VARY, HeaderValue::from_static("Authorization, Cookie"));
    if let Ok(etag) = HeaderValue::from_str(etag) {
        headers.insert(ETAG, etag);
    }
}

pub(super) fn error_response(error: LiveHttpRejection, request_id: Option<Uuid>) -> Response {
    let status = error.status;
    let retry_after_seconds = error.retry_after_seconds;
    let provider_id = error.provider_id;
    let envelope = LiveEnvelope {
        data: Value::Null,
        meta: ApiMeta {
            request_id: request_id.unwrap_or_else(Uuid::new_v4).to_string(),
            generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            cache_state: "none",
            partial: false,
        },
        errors: vec![LiveApiError {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            retry_after_seconds,
            provider_id,
        }],
    };
    let body = serde_json::to_vec(&envelope).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(seconds) = retry_after_seconds {
        if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
            response.headers_mut().insert("retry-after", value);
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        net::Ipv4Addr,
        process::Command,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::*;
    use anyhow::Result;
    use axum::{
        body::{self, Body},
        http::Request,
        routing::get,
    };
    use base64::engine::general_purpose;
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use image::{DynamicImage, ImageFormat, Rgb, RgbImage};

    use crate::{
        artwork::ArtworkService,
        auth::AuthService,
        config::{
            AuthConfig, ClassifierConfig, DatabaseConfig, LibraryConfig, MediaInteractionsConfig,
            RunEnvironment, SecretsConfig, ServerConfig, Settings, TelemetryConfig,
        },
        db::Database,
        extensions::ExtensionManager,
        http::router,
        library::LinkerService,
        live::{
            catalog::{LivePublicKeyCodec, LivePublicKeyScope},
            config::LiveConfig,
            crypto::{LiveCrypto, LiveMasterKey},
            provider::tests::{NativeFixture, seed_provider},
            service::LiveService,
        },
        metadata::MetadataService,
        secrets::SecretsManager,
        state::AppState,
    };

    #[test]
    fn lpi2_account_required_is_distinct_and_provider_scoped() {
        let provider_id = Uuid::new_v4();
        let rejection =
            map_service_error(CatalogServiceError::Provider("provider_account_required"))
                .with_provider(provider_id);
        assert_eq!(rejection.status, StatusCode::CONFLICT);
        assert_eq!(rejection.code, "LIVE_ACCOUNT_REQUIRED");
        assert!(!rejection.retryable);
        assert_eq!(rejection.provider_id, Some(provider_id));

        let aggregated = provider_error_dto(ProviderScopedError {
            provider_id,
            code: "provider_account_required",
        });
        assert_eq!(aggregated.code, "LIVE_ACCOUNT_REQUIRED");
        assert!(!aggregated.retryable);
        assert_eq!(aggregated.provider_id, Some(provider_id));

        let unavailable = map_service_error(CatalogServiceError::Provider(
            "provider_upstream_unavailable",
        ));
        assert_eq!(unavailable.code, "LIVE_PROVIDER_UNAVAILABLE");
        assert!(unavailable.retryable);
    }

    fn settings() -> Settings {
        Settings {
            environment: RunEnvironment::Development,
            server: ServerConfig::default(),
            database: DatabaseConfig {
                url: format!(
                    "sqlite:file:s13-live-http-{}?mode=memory&cache=shared",
                    Uuid::new_v4()
                ),
                max_connections: 8,
                connect_timeout_seconds: 5,
            },
            library: LibraryConfig::default(),
            extensions: crate::config::ExtensionsConfig::default(),
            auth: AuthConfig::default(),
            secrets: SecretsConfig {
                master_key: Some(general_purpose::STANDARD.encode([13u8; 32])),
            },
            telemetry: TelemetryConfig::default(),
            metadata: crate::config::MetadataConfig::default(),
            classifier: ClassifierConfig::default(),
            playback: crate::config::PlaybackConfig::default(),
            media_interactions: MediaInteractionsConfig::default(),
            live: LiveConfig {
                enabled: true,
                catalog_enabled: true,
                ..LiveConfig::default()
            },
            network: crate::config::NetworkConfig::default(),
        }
    }

    async fn json_response(response: Response) -> Result<(StatusCode, HeaderMap, Value)> {
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = body::to_bytes(response.into_body(), 2 * 1024 * 1024).await?;
        let json = serde_json::from_slice(&bytes)?;
        Ok((status, headers, json))
    }

    async fn request_json(
        app: &axum::Router,
        method: &str,
        uri: impl AsRef<str>,
        token: Option<&str>,
        body_value: Option<Value>,
    ) -> Result<(StatusCode, HeaderMap, Value)> {
        let mut builder = Request::builder().method(method).uri(uri.as_ref());
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let body = if let Some(value) = body_value {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        } else {
            Body::empty()
        };
        json_response(app.clone().oneshot(builder.body(body)?).await?).await
    }

    #[test]
    fn s13_public_keys_are_scoped_tamper_evident_and_expiring() -> Result<()> {
        let crypto = Arc::new(LiveCrypto::new(
            "s13-key",
            [LiveMasterKey::new("s13-key", [31u8; 32])?],
        )?);
        let keys = LivePublicKeyCodec::new(crypto);
        let provider = Uuid::new_v4();
        let scope = LivePublicKeyScope {
            home_id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            authorization_revision: 7,
        };
        let now = Utc::now();
        let item = keys.seal_item(provider, "event-live", scope, now)?;
        assert_eq!(keys.open_item(&item, provider, scope, now)?, "event-live");
        assert!(
            keys.open_item(
                &item,
                provider,
                LivePublicKeyScope {
                    authorization_revision: 8,
                    ..scope
                },
                now,
            )
            .is_err()
        );
        let mut tampered = item.clone();
        tampered.replace_range(20..21, if &tampered[20..21] == "A" { "B" } else { "A" });
        assert!(keys.open_item(&tampered, provider, scope, now).is_err());
        assert!(
            keys.open_item(&item, provider, scope, now + chrono::Duration::hours(7))
                .is_err()
        );
        let cursor = keys.seal_cursor(provider, "events", "provider-cursor", scope, now)?;
        assert_eq!(
            keys.open_cursor(&cursor, provider, "events", scope, now)?,
            "provider-cursor"
        );
        assert!(
            keys.open_cursor(&cursor, provider, "channels", scope, now)
                .is_err()
        );
        let stream = keys.seal_stream(provider, "event-live", "primary", scope, now)?;
        assert_eq!(
            keys.open_stream(&stream, provider, scope, now)?,
            ("event-live".to_string(), "primary".to_string())
        );
        let artwork = keys.seal_artwork(
            provider,
            "event-live",
            LiveArtworkKind::Poster,
            "https://artwork.example.invalid/private.jpg",
            scope,
            now,
        )?;
        let artwork = keys.open_artwork(&artwork, scope, now)?;
        assert_eq!(artwork.provider_id, provider);
        assert_eq!(artwork.item_id, "event-live");
        assert_eq!(artwork.kind, LiveArtworkKind::Poster);
        assert!(!format!("{artwork:?}").contains("artwork.example.invalid"));
        let long_source = ArtworkSource::new(format!(
            "https://artwork.example.invalid/{}",
            "x".repeat(3_000)
        ));
        assert!(
            artwork_dto(
                &keys,
                provider,
                "event-live",
                LiveArtworkKind::Poster,
                Some(long_source),
                scope,
                now,
            )
            .expect("oversized artwork key is omitted")
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn s13_http_admission_bounds_rate_and_concurrency_per_user() {
        let rate_user = Uuid::new_v4();
        for _ in 0..MAX_REQUESTS_PER_MINUTE {
            drop(admit(rate_user).expect("request admitted"));
        }
        let rate_error = admit(rate_user).expect_err("rate limit");
        assert_eq!(rate_error.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(
            rate_error
                .retry_after_seconds
                .is_some_and(|seconds| seconds > 1)
        );

        let concurrent_user = Uuid::new_v4();
        let guards = (0..MAX_CONCURRENT_REQUESTS)
            .map(|_| admit(concurrent_user).expect("concurrent request admitted"))
            .collect::<Vec<_>>();
        assert_eq!(
            admit(concurrent_user)
                .expect_err("concurrency limit")
                .status,
            StatusCode::TOO_MANY_REQUESTS
        );
        drop(guards);
        assert!(admit(concurrent_user).is_ok());
    }

    #[test]
    fn s13_artwork_admission_cannot_starve_interactive_requests() {
        let artwork_user = Uuid::new_v4();
        for _ in 0..MAX_ARTWORK_REQUESTS_PER_MINUTE {
            drop(admit_artwork(artwork_user).expect("artwork request admitted"));
        }
        assert_eq!(
            admit_artwork(artwork_user)
                .expect_err("artwork rate limit")
                .status,
            StatusCode::TOO_MANY_REQUESTS
        );
        assert!(
            admit(artwork_user).is_ok(),
            "bulk artwork traffic must not block playback or browsing"
        );

        let interactive_user = Uuid::new_v4();
        for _ in 0..MAX_REQUESTS_PER_MINUTE {
            drop(admit(interactive_user).expect("interactive request admitted"));
        }
        assert_eq!(
            admit(interactive_user)
                .expect_err("interactive rate limit")
                .status,
            StatusCode::TOO_MANY_REQUESTS
        );
        assert!(
            admit_artwork(interactive_user).is_ok(),
            "interactive traffic must not prevent artwork from loading"
        );
    }

    #[tokio::test]
    async fn s13_real_http_browse_enforces_auth_visibility_keys_filters_cursors_and_etags()
    -> Result<()> {
        let fixture = NativeFixture::start().await?;
        let settings = settings();
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, healthy_provider) =
            seed_provider(&database, fixture.port(), serde_json::json!({})).await?;
        let (_, failing_provider) = seed_provider(
            &database,
            fixture.port(),
            serde_json::json!({"fixtureFault": "provider_error"}),
        )
        .await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let app = router(state.clone());

        let (status, _, unauthenticated) =
            request_json(&app, "GET", "/api/v1/live/providers", None, None).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(unauthenticated["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        let (status, _, signup) = request_json(
            &app,
            "POST",
            "/api/v1/auth/signup",
            None,
            Some(serde_json::json!({
                "email": format!("s13-{}@example.invalid", Uuid::new_v4()),
                "password": "correct horse battery staple"
            })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let access = signup["access_token"].as_str().expect("access token");
        let home_id = Uuid::parse_str(signup["home_id"].as_str().expect("signup home identifier"))?;
        let owner_user_id: String =
            sqlx::query_scalar("SELECT owner_user_id FROM homes WHERE id = $1")
                .bind(home_id.to_string())
                .fetch_one(&pool)
                .await?;
        let owner_user_id = Uuid::parse_str(&owner_user_id)?;

        let (status, _, query_auth) = request_json(
            &app,
            "GET",
            format!("/api/v1/live/providers?access_token={access}"),
            None,
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(query_auth["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        let (status, _, providers) =
            request_json(&app, "GET", "/api/v1/live/providers", Some(access), None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(providers["data"].as_array().map(Vec::len), Some(2));
        sqlx::query("UPDATE providers SET health_state = 'degraded' WHERE provider_id = $1")
            .bind(failing_provider.to_string())
            .execute(&pool)
            .await?;
        let (status, _, readiness) =
            request_json(&app, "GET", "/api/v1/live/providers", Some(access), None).await?;
        assert_eq!(status, StatusCode::OK);
        let degraded = readiness["data"]
            .as_array()
            .and_then(|providers| {
                providers
                    .iter()
                    .find(|provider| provider["providerId"] == failing_provider.to_string())
            })
            .expect("degraded provider summary");
        assert_eq!(degraded["readiness"], "degraded");
        assert_eq!(degraded["disabledReason"], "provider_degraded");
        sqlx::query("UPDATE providers SET health_state = 'healthy' WHERE provider_id = $1")
            .bind(failing_provider.to_string())
            .execute(&pool)
            .await?;

        let (status, catalog_headers, catalogs) =
            request_json(&app, "GET", "/api/v1/live/catalogs", Some(access), None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(catalogs["meta"]["partial"], true);
        assert!(
            catalogs["data"]
                .as_array()
                .is_some_and(|data| !data.is_empty())
        );
        assert_eq!(
            catalogs["errors"][0]["providerId"],
            failing_provider.to_string()
        );
        assert_eq!(
            catalog_headers
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("private, max-age=0, must-revalidate")
        );
        assert_eq!(
            catalog_headers
                .get(VARY)
                .and_then(|value| value.to_str().ok()),
            Some("Authorization, Cookie")
        );
        let etag = catalog_headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .expect("catalog ETag");
        let etag_response = app
            .clone()
            .oneshot(
                Request::get("/api/v1/live/catalogs")
                    .header("authorization", format!("Bearer {access}"))
                    .header(IF_NONE_MATCH, etag)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(etag_response.status(), StatusCode::NOT_MODIFIED);

        let forbidden_counts_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        let page_uri = format!(
            "/api/v1/live/catalogs/{healthy_provider}/events/items?limit=1&filters%5Bcategory%5D=sports"
        );
        let (status, page_headers, page) =
            request_json(&app, "GET", &page_uri, Some(access), None).await?;
        assert_eq!(status, StatusCode::OK, "page body: {page}");
        assert_eq!(page["data"]["items"].as_array().map(Vec::len), Some(1));
        let item_key = page["data"]["items"][0]["itemKey"]
            .as_str()
            .expect("sealed item key");
        let next_cursor = page["data"]["nextCursor"]
            .as_str()
            .expect("sealed next cursor");
        let page_text = page.to_string();
        assert!(!page_text.contains("artwork.example.invalid"));
        assert!(!page_text.contains("\"id\":\"event-live\""));
        let page_etag = page_headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .expect("page ETag");
        let page_not_modified = app
            .clone()
            .oneshot(
                Request::get(&page_uri)
                    .header("authorization", format!("Bearer {access}"))
                    .header(IF_NONE_MATCH, page_etag)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(page_not_modified.status(), StatusCode::NOT_MODIFIED);

        let second_page_uri = format!(
            "/api/v1/live/catalogs/{healthy_provider}/events/items?limit=1&cursor={next_cursor}&filters%5Bcategory%5D=sports"
        );
        let (status, _, second_page) =
            request_json(&app, "GET", second_page_uri, Some(access), None).await?;
        assert_eq!(status, StatusCode::OK, "second page body: {second_page}");
        let (status, _, tampered_cursor) = request_json(
            &app,
            "GET",
            format!(
                "/api/v1/live/catalogs/{healthy_provider}/events/items?limit=1&cursor={next_cursor}A&filters%5Bcategory%5D=sports"
            ),
            Some(access),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(tampered_cursor["errors"][0]["code"], "LIVE_INVALID_REQUEST");
        let (status, _, invalid_filter) = request_json(
            &app,
            "GET",
            format!(
                "/api/v1/live/catalogs/{healthy_provider}/events/items?filters%5Bunknown%5D=value"
            ),
            Some(access),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid_filter["errors"][0]["code"], "LIVE_INVALID_REQUEST");

        let stale_now = Utc::now();
        sqlx::query(
            "UPDATE live_provider_cache
             SET fresh_until = $1, stale_until = $2
             WHERE provider_id = $3 AND operation = 'catalog'",
        )
        .bind((stale_now - chrono::Duration::seconds(1)).to_rfc3339())
        .bind((stale_now + chrono::Duration::minutes(5)).to_rfc3339())
        .bind(healthy_provider.to_string())
        .execute(&pool)
        .await?;
        let (status, _, stale_page) =
            request_json(&app, "GET", &page_uri, Some(access), None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stale_page["meta"]["cacheState"], "stale");

        let (status, _, direct_failure) = request_json(
            &app,
            "GET",
            format!(
                "/api/v1/live/catalogs/{failing_provider}/events/items?filters%5Bcategory%5D=sports"
            ),
            Some(access),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            direct_failure["errors"][0]["code"],
            "LIVE_PROVIDER_UNAVAILABLE"
        );

        let (status, metadata_headers, metadata) = request_json(
            &app,
            "GET",
            format!("/api/v1/live/items/{healthy_provider}/{item_key}"),
            Some(access),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "metadata body: {metadata}");
        assert!(metadata["data"]["streams"][0]["streamOptionKey"].is_string());
        assert!(metadata["data"]["streams"][0].get("id").is_none());
        assert!(metadata["data"]["streams"][0].get("url").is_none());
        let metadata_etag = metadata_headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .expect("metadata ETag");
        let metadata_not_modified = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/live/items/{healthy_provider}/{item_key}"))
                    .header("authorization", format!("Bearer {access}"))
                    .header(IF_NONE_MATCH, metadata_etag)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(metadata_not_modified.status(), StatusCode::NOT_MODIFIED);
        let forbidden_counts_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_counts_after, forbidden_counts_before);

        let managed_profile_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO profiles (
                id, home_id, user_id, profile_type, display_name, is_default
             ) VALUES ($1, $2, NULL, 'managed', 'Managed S13', FALSE)",
        )
        .bind(managed_profile_id.to_string())
        .bind(home_id.to_string())
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
             VALUES ($1, $2, 1)",
        )
        .bind(managed_profile_id.to_string())
        .bind(home_id.to_string())
        .execute(&pool)
        .await?;
        let (status, _, _) = request_json(
            &app,
            "POST",
            format!("/api/v1/profiles/{managed_profile_id}/select"),
            Some(access),
            Some(serde_json::json!({})),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let (status, _, managed_providers) =
            request_json(&app, "GET", "/api/v1/live/providers", Some(access), None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(managed_providers["data"].as_array().map(Vec::len), Some(0));
        let (status, _, forbidden_provider) =
            request_json(&app, "GET", &page_uri, Some(access), None).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            forbidden_provider["errors"][0]["code"],
            "LIVE_PROVIDER_FORBIDDEN"
        );

        state
            .live
            .catalog_service()
            .expect("catalog service")
            .grants()
            .set_grant(
                owner_user_id,
                r#"{"role":"owner","source":"s13-test"}"#,
                managed_profile_id,
                healthy_provider,
                true,
                true,
                Some(1),
                None,
            )
            .await?;
        let (status, _, shared_providers) =
            request_json(&app, "GET", "/api/v1/live/providers", Some(access), None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(shared_providers["data"].as_array().map(Vec::len), Some(1));

        fixture.stop().await?;
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        state.live.clone().run_lease_heartbeat(shutdown).await;
        Ok(())
    }

    #[tokio::test]
    async fn g20_real_qt_client_browses_tcp_fixture() -> Result<()> {
        let client_binary = std::env::var("ELIXIR_G20_CLIENT_TEST_BINARY")
            .map_err(|_| anyhow::anyhow!("ELIXIR_G20_CLIENT_TEST_BINARY is required"))?;
        let fixture = NativeFixture::start().await?;
        let settings = settings();
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        seed_provider(&database, fixture.port(), serde_json::json!({})).await?;
        seed_provider(
            &database,
            fixture.port(),
            serde_json::json!({"fixtureFault": "provider_error"}),
        )
        .await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool,
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let app = router(state.clone());

        let email = format!("g20-{}@example.invalid", Uuid::new_v4());
        let password = "correct horse battery staple";
        let (status, _, signup) = request_json(
            &app,
            "POST",
            "/api/v1/auth/signup",
            None,
            Some(serde_json::json!({"email": email, "password": password})),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "signup body: {signup}");

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let server_url = format!("http://{address}");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
                .await
        });

        let output = tokio::task::spawn_blocking(move || {
            Command::new(client_binary)
                .env("ELIXIR_G20_SERVER_URL", server_url)
                .env("ELIXIR_G20_EMAIL", email)
                .env("ELIXIR_G20_PASSWORD", password)
                .output()
        })
        .await?;

        shutdown.cancel();
        let server_result = server.await?;
        fixture.stop().await?;
        let lease_shutdown = CancellationToken::new();
        lease_shutdown.cancel();
        state.live.clone().run_lease_heartbeat(lease_shutdown).await;
        server_result?;
        let output = output?;
        if !output.status.success() {
            anyhow::bail!(
                "real Qt client failed with {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("Totals: 3 passed, 0 failed"),
            "unexpected Qt output: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        Ok(())
    }

    #[tokio::test]
    async fn s14_real_http_artwork_is_authorized_validated_cached_and_pipeline_isolated()
    -> Result<()> {
        let native = NativeFixture::start().await?;
        let png = {
            let image = RgbImage::from_pixel(6, 4, Rgb([31, 117, 211]));
            let mut output = Cursor::new(Vec::new());
            DynamicImage::ImageRgb8(image).write_to(&mut output, ImageFormat::Png)?;
            Arc::new(output.into_inner())
        };
        let origin_requests = Arc::new(AtomicUsize::new(0));
        let image_app = axum::Router::new().route(
            "/poster.png",
            get({
                let png = png.clone();
                let origin_requests = origin_requests.clone();
                move || {
                    let png = png.clone();
                    let origin_requests = origin_requests.clone();
                    async move {
                        origin_requests.fetch_add(1, Ordering::AcqRel);
                        ([(CONTENT_TYPE, "image/png")], png.as_ref().clone())
                    }
                }
            }),
        );
        let image_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let image_port = image_listener.local_addr()?.port();
        let image_task = tokio::spawn(async move {
            let _ = axum::serve(image_listener, image_app).await;
        });

        let settings = settings();
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) =
            seed_provider(&database, native.port(), serde_json::json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let app = router(state.clone());

        let (status, _, signup) = request_json(
            &app,
            "POST",
            "/api/v1/auth/signup",
            None,
            Some(serde_json::json!({
                "email": format!("s14-{}@example.invalid", Uuid::new_v4()),
                "password": "correct horse battery staple"
            })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let access = signup["access_token"].as_str().expect("access token");
        let home_id = Uuid::parse_str(signup["home_id"].as_str().expect("home id"))?;
        let profile_id: String = sqlx::query_scalar(
            "SELECT id FROM profiles WHERE home_id = $1 AND is_default = TRUE LIMIT 1",
        )
        .bind(home_id.to_string())
        .fetch_one(&pool)
        .await?;
        let profile_id = Uuid::parse_str(&profile_id)?;
        let revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO live_provider_destination_rules (
                 id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                 network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                 revision, created_by_actor_snapshot
             ) VALUES ($1, $2, $3, 'http', '127.0.0.1', $4, '/poster.png',
                       'public', TRUE, FALSE, FALSE, 1, 's14-http-test')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(i64::from(image_port))
        .execute(&pool)
        .await?;
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision: revision,
        };
        let crypto = state.live.crypto().await.expect("Live crypto");
        let artwork_id = LivePublicKeyCodec::new(crypto).seal_artwork(
            provider_id,
            "event-s14",
            LiveArtworkKind::Poster,
            &format!("http://127.0.0.1:{image_port}/poster.png?sig=ELIXIR_LIVE_CANARY_ART"),
            scope,
            Utc::now(),
        )?;
        let uri = format!("/api/v1/live/artwork/{artwork_id}");
        let forbidden_counts_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;

        let unauthenticated = app
            .clone()
            .oneshot(Request::builder().uri(&uri).body(Body::empty())?)
            .await?;
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(origin_requests.load(Ordering::Acquire), 0);

        let query_auth = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{uri}?access_token={access}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(query_auth.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(origin_requests.load(Ordering::Acquire), 0);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .header("authorization", format!("Bearer {access}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "image/png");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(
            response.headers()["cross-origin-resource-policy"],
            "same-origin"
        );
        assert!(!response.headers().contains_key("location"));
        assert!(!response.headers().contains_key("set-cookie"));
        let etag = response.headers()[ETAG].to_str()?.to_string();
        let bytes = body::to_bytes(response.into_body(), 1024 * 1024).await?;
        assert_eq!(bytes.as_ref(), png.as_slice());
        assert_eq!(origin_requests.load(Ordering::Acquire), 1);

        let not_modified = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .header("authorization", format!("Bearer {access}"))
                    .header(IF_NONE_MATCH, &etag)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(origin_requests.load(Ordering::Acquire), 1);

        sqlx::query(
            "UPDATE live_provider_destination_rules
             SET allow_fetch = FALSE, revision = revision + 1, updated_at = CURRENT_TIMESTAMP
             WHERE home_id = $1 AND provider_id = $2",
        )
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .execute(&pool)
        .await?;
        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .header("authorization", format!("Bearer {access}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        assert_eq!(origin_requests.load(Ordering::Acquire), 1);

        let mut tampered = artwork_id;
        tampered.replace_range(20..21, if &tampered[20..21] == "A" { "B" } else { "A" });
        let tampered = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/live/artwork/{tampered}"))
                    .header("authorization", format!("Bearer {access}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(tampered.status(), StatusCode::NOT_FOUND);
        assert_eq!(origin_requests.load(Ordering::Acquire), 1);
        let forbidden_counts_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_counts_after, forbidden_counts_before);

        image_task.abort();
        let _ = image_task.await;
        native.stop().await?;
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        state.live.clone().run_lease_heartbeat(shutdown).await;
        Ok(())
    }
}
