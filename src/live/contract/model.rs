use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};
use zeroize::Zeroizing;

#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveString(Zeroizing<String>);

impl SensitiveString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
    }

    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for SensitiveString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<sensitive>")
    }
}

impl Serialize for SensitiveString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.expose())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ArtworkSource(SensitiveString);

impl ArtworkSource {
    pub(crate) fn new(value: String) -> Self {
        Self(SensitiveString::new(value))
    }

    pub fn expose(&self) -> &str {
        self.0.expose()
    }
}

impl fmt::Debug for ArtworkSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<provider-artwork-source>")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperation {
    Health,
    Catalogs,
    Catalog,
    Meta,
    Resolve,
    Refresh,
}

impl ProviderOperation {
    pub const fn path(self) -> &'static str {
        match self {
            Self::Health => "/health",
            Self::Catalogs => "/v1/live/catalogs",
            Self::Catalog => "/v1/live/catalog",
            Self::Meta => "/v1/live/meta",
            Self::Resolve => "/v1/live/resolve",
            Self::Refresh => "/v1/live/refresh",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveItemType {
    Event,
    Channel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveItemStatus {
    Scheduled,
    Live,
    Ended,
    Unavailable,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogPresentation {
    Landscape,
    Poster,
    CompactList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterKind {
    Toggle,
    SingleSelect,
    MultiSelect,
    Search,
    Date,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FilterValue {
    Toggle(bool),
    Text(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamProtocol {
    Hls,
    Dash,
    HttpProgressive,
    MpegTs,
    Rtmp,
    Srt,
}

impl StreamProtocol {
    pub const fn expected_scheme(self) -> &'static [&'static str] {
        match self {
            Self::Hls | Self::Dash | Self::HttpProgressive | Self::MpegTs => &["http", "https"],
            Self::Rtmp => &["rtmp"],
            Self::Srt => &["srt"],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientDisclosure {
    ServerOnly,
    Public,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerEgress {
    NotRequired,
    Preferred,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrmKind {
    None,
    Widevine,
    Fairplay,
    Playready,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshFailureCategory {
    ExpiryThreshold,
    UpstreamUnauthorized,
    UpstreamForbidden,
    UpstreamGone,
    Transport,
    Stalled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContract {
    pub item_types: BTreeSet<LiveItemType>,
    pub protocols: BTreeSet<StreamProtocol>,
}

impl ProviderContract {
    pub fn new(
        item_types: impl IntoIterator<Item = LiveItemType>,
        protocols: impl IntoIterator<Item = StreamProtocol>,
    ) -> Result<Self, ContractError> {
        let item_types = item_types.into_iter().collect::<BTreeSet<_>>();
        let protocols = protocols.into_iter().collect::<BTreeSet<_>>();
        if item_types.is_empty() || item_types.len() > 2 || protocols.is_empty() {
            return Err(ContractError::new(ContractErrorCode::InvalidShape));
        }
        Ok(Self {
            item_types,
            protocols,
        })
    }

    pub fn item_types(&self) -> impl Iterator<Item = LiveItemType> + '_ {
        self.item_types.iter().copied()
    }

    pub fn protocols(&self) -> impl Iterator<Item = StreamProtocol> + '_ {
        self.protocols.iter().copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRequestContext {
    pub locale: String,
    pub timezone: String,
    pub now: DateTime<Utc>,
}

impl ProviderRequestContext {
    pub fn validate(&self) -> Result<(), ContractError> {
        if !(2..=64).contains(&self.locale.chars().count())
            || self.locale.chars().any(char::is_control)
            || !(1..=128).contains(&self.timezone.chars().count())
            || self.timezone.chars().any(char::is_control)
        {
            return Err(ContractError::new(ContractErrorCode::InvalidContext));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CatalogsRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPageRequest {
    pub catalog_id: String,
    pub cursor: Option<String>,
    pub limit: u16,
    pub filters: BTreeMap<String, FilterValue>,
}

impl CatalogPageRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_catalog_id(&self.catalog_id)?;
        if !(1..=100).contains(&self.limit)
            || self
                .cursor
                .as_ref()
                .is_some_and(|value| value.len() > 2_048)
            || self.filters.len() > 12
        {
            return Err(ContractError::new(ContractErrorCode::LimitExceeded));
        }
        for (id, value) in &self.filters {
            validate_catalog_id(id)?;
            validate_filter_value(value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaRequest {
    pub item_id: String,
}

impl MetaRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_provider_id(&self.item_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveRequest {
    pub item_id: String,
    pub stream_id: String,
}

impl ResolveRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_provider_id(&self.item_id)?;
        validate_provider_id(&self.stream_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshFailure {
    pub category: RefreshFailureCategory,
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshSessionContext {
    pub started_at: DateTime<Utc>,
    pub source_attempt: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshRequest {
    pub item_id: String,
    pub stream_id: String,
    pub refresh_handle: SensitiveString,
    pub failure: RefreshFailure,
    pub session: RefreshSessionContext,
}

impl RefreshRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_provider_id(&self.item_id)?;
        validate_provider_id(&self.stream_id)?;
        if self.refresh_handle.expose().is_empty()
            || self.refresh_handle.expose().len() > 2_048
            || !(1..=10).contains(&self.session.source_attempt)
            || self
                .failure
                .http_status
                .is_some_and(|status| !(100..=599).contains(&status))
        {
            return Err(ContractError::new(ContractErrorCode::InvalidRequest));
        }
        Ok(())
    }
}

pub trait ProviderRequest: Serialize {
    const OPERATION: ProviderOperation;

    fn validate(&self) -> Result<(), ContractError>;
}

impl ProviderRequest for CatalogsRequest {
    const OPERATION: ProviderOperation = ProviderOperation::Catalogs;

    fn validate(&self) -> Result<(), ContractError> {
        Ok(())
    }
}

impl ProviderRequest for CatalogPageRequest {
    const OPERATION: ProviderOperation = ProviderOperation::Catalog;

    fn validate(&self) -> Result<(), ContractError> {
        self.validate()
    }
}

impl ProviderRequest for MetaRequest {
    const OPERATION: ProviderOperation = ProviderOperation::Meta;

    fn validate(&self) -> Result<(), ContractError> {
        self.validate()
    }
}

impl ProviderRequest for ResolveRequest {
    const OPERATION: ProviderOperation = ProviderOperation::Resolve;

    fn validate(&self) -> Result<(), ContractError> {
        self.validate()
    }
}

impl ProviderRequest for RefreshRequest {
    const OPERATION: ProviderOperation = ProviderOperation::Refresh;

    fn validate(&self) -> Result<(), ContractError> {
        self.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderHealth {
    pub status: ProviderHealthStatus,
    pub contract_versions: BTreeSet<u32>,
    pub details: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheHint {
    pub max_age_seconds: u32,
    pub stale_while_revalidate_seconds: u32,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FilterOption {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterDefinition {
    pub id: String,
    pub label: String,
    pub kind: FilterKind,
    pub required: bool,
    pub default: Option<FilterValue>,
    pub options: Vec<FilterOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogDefinition {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub item_types: BTreeSet<LiveItemType>,
    pub presentation: CatalogPresentation,
    pub order: i32,
    pub filters: Vec<FilterDefinition>,
}

impl CatalogDefinition {
    pub fn validate_filter_submission(
        &self,
        values: &BTreeMap<String, FilterValue>,
    ) -> Result<(), ContractError> {
        if values.len() > self.filters.len() {
            return Err(ContractError::new(ContractErrorCode::InvalidFilter));
        }
        for definition in &self.filters {
            let value = values.get(&definition.id);
            if definition.required && value.is_none() {
                return Err(ContractError::new(ContractErrorCode::InvalidFilter));
            }
            if let Some(value) = value {
                definition.validate_value(value)?;
            }
        }
        if values
            .keys()
            .any(|id| !self.filters.iter().any(|definition| definition.id == *id))
        {
            return Err(ContractError::new(ContractErrorCode::InvalidFilter));
        }
        Ok(())
    }
}

impl FilterDefinition {
    fn validate_value(&self, value: &FilterValue) -> Result<(), ContractError> {
        validate_filter_value(value)?;
        let known = |candidate: &str| self.options.iter().any(|item| item.value == candidate);
        let valid = match (self.kind, value) {
            (FilterKind::Toggle, FilterValue::Toggle(_)) => true,
            (FilterKind::SingleSelect, FilterValue::Text(value)) => known(value),
            (FilterKind::MultiSelect, FilterValue::Multiple(values)) => {
                values.iter().all(|value| known(value))
            }
            (FilterKind::Search | FilterKind::Date, FilterValue::Text(_)) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(ContractError::new(ContractErrorCode::InvalidFilter))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSet {
    pub catalogs: Vec<CatalogDefinition>,
    pub cache: CacheHint,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Fact {
    pub label: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveItem {
    pub id: String,
    pub item_type: LiveItemType,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    pub status: LiveItemStatus,
    pub starts_at: Option<DateTime<Utc>>,
    pub ends_at: Option<DateTime<Utc>>,
    pub poster: Option<ArtworkSource>,
    pub background: Option<ArtworkSource>,
    pub logo: Option<ArtworkSource>,
    pub categories: Vec<String>,
    pub badges: Vec<String>,
    pub facts: Vec<Fact>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemDiagnosticReason {
    InvalidShape,
    InvalidId,
    InvalidText,
    InvalidDate,
    InvalidArtwork,
    UndeclaredItemType,
    ForbiddenField,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ItemDiagnostic {
    pub index: usize,
    pub reason: ItemDiagnosticReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogPage {
    pub items: Vec<LiveItem>,
    pub next_cursor: Option<String>,
    pub cache: CacheHint,
    pub diagnostics: Vec<ItemDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamChoice {
    pub id: String,
    pub label: String,
    pub quality: Option<String>,
    pub language: Option<String>,
    pub protocol_hint: Option<StreamProtocol>,
    pub priority: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemMetadata {
    pub item: LiveItem,
    pub streams: Vec<StreamChoice>,
    pub cache: CacheHint,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CredentialAuthority {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub send_request_headers: bool,
    pub send_cookies: bool,
    pub send_origin: bool,
    pub send_referer: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderCookie {
    pub name: String,
    pub value: SensitiveString,
    pub domain: Option<String>,
    pub path: Option<String>,
    pub secure: bool,
    pub http_only: bool,
    pub expires_at: Option<DateTime<Utc>>,
}

impl fmt::Debug for ProviderCookie {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderCookie")
            .field("name", &self.name)
            .field("value", &"<sensitive>")
            .field("domain", &self.domain)
            .field("path", &self.path)
            .field("secure", &self.secure)
            .field("http_only", &self.http_only)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeShift {
    pub available: bool,
    pub window_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaHints {
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub stream_id: String,
    pub label: String,
    pub quality: Option<String>,
    pub language: Option<String>,
    pub priority: i32,
    pub protocol: StreamProtocol,
    pub url: SensitiveString,
    pub request_headers: BTreeMap<String, SensitiveString>,
    pub cookies: Vec<ProviderCookie>,
    pub origin: Option<SensitiveString>,
    pub referer: Option<SensitiveString>,
    pub credential_authorities: Vec<CredentialAuthority>,
    pub client_disclosure: ClientDisclosure,
    pub expires_at: Option<DateTime<Utc>>,
    pub refresh_handle: Option<SensitiveString>,
    pub server_egress: ServerEgress,
    pub private_network: bool,
    pub time_shift: TimeShift,
    pub media: Option<MediaHints>,
}

impl fmt::Debug for SourceDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceDescriptor")
            .field("stream_id", &self.stream_id)
            .field("label", &self.label)
            .field("quality", &self.quality)
            .field("language", &self.language)
            .field("priority", &self.priority)
            .field("protocol", &self.protocol)
            .field("url", &"<sensitive>")
            .field("request_header_count", &self.request_headers.len())
            .field("cookie_count", &self.cookies.len())
            .field("has_origin", &self.origin.is_some())
            .field("has_referer", &self.referer.is_some())
            .field("credential_authorities", &self.credential_authorities)
            .field("client_disclosure", &self.client_disclosure)
            .field("expires_at", &self.expires_at)
            .field("has_refresh_handle", &self.refresh_handle.is_some())
            .field("server_egress", &self.server_egress)
            .field("private_network", &self.private_network)
            .field("time_shift", &self.time_shift)
            .field("media", &self.media)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSources {
    pub descriptor: SourceDescriptor,
    pub alternatives: Vec<SourceDescriptor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderFailureCode {
    InvalidRequest,
    ItemNotFound,
    StreamNotFound,
    StreamExpired,
    AccountRequired,
    UpstreamUnavailable,
    UpstreamRateLimited,
    UnsupportedInput,
    ContractVersionUnsupported,
    InternalError,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderFailure {
    pub code: ProviderFailureCode,
    pub message: String,
    pub retryable: bool,
    pub retry_after_seconds: Option<u32>,
}

impl fmt::Debug for ProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderFailure")
            .field("code", &self.code)
            .field("message", &"<provider-message>")
            .field("retryable", &self.retryable)
            .field("retry_after_seconds", &self.retry_after_seconds)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractErrorCode {
    MalformedJson,
    DuplicateJsonKey,
    InvalidShape,
    LimitExceeded,
    InvalidContext,
    InvalidRequest,
    InvalidId,
    InvalidText,
    InvalidDate,
    InvalidUrl,
    InvalidFilter,
    DuplicateId,
    TooManyInvalidItems,
    UndeclaredItemType,
    UndeclaredProtocol,
    ForbiddenField,
    InvalidCredentials,
    DrmUnsupported,
    DescriptorExpired,
    UnsafeProviderConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractError {
    code: ContractErrorCode,
}

impl ContractError {
    pub const fn new(code: ContractErrorCode) -> Self {
        Self { code }
    }

    pub const fn code(self) -> ContractErrorCode {
        self.code
    }
}

impl fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Live provider contract validation failed ({:?})",
            self.code
        )
    }
}

impl std::error::Error for ContractError {}

pub(crate) fn validate_provider_id(value: &str) -> Result<(), ContractError> {
    if value.is_empty()
        || value.len() > 512
        || value.chars().any(|character| {
            character == '\u{7f}'
                || (character.is_control() && !matches!(character, '\t' | '\n' | '\r'))
        })
    {
        return Err(ContractError::new(ContractErrorCode::InvalidId));
    }
    Ok(())
}

pub(crate) fn validate_catalog_id(value: &str) -> Result<(), ContractError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'~'))
    {
        return Err(ContractError::new(ContractErrorCode::InvalidId));
    }
    Ok(())
}

pub(crate) fn validate_short_text(value: &str) -> Result<(), ContractError> {
    if value.chars().count() > 256
        || value.chars().any(|character| {
            character == '\u{7f}'
                || (character.is_control() && !matches!(character, '\t' | '\n' | '\r'))
        })
    {
        return Err(ContractError::new(ContractErrorCode::InvalidText));
    }
    Ok(())
}

pub(crate) fn validate_description(value: &str) -> Result<(), ContractError> {
    if value.chars().count() > 4_000 {
        return Err(ContractError::new(ContractErrorCode::InvalidText));
    }
    validate_plain_text(value)
}

pub(crate) fn validate_plain_text(value: &str) -> Result<(), ContractError> {
    if value.chars().any(|character| {
        character == '\u{7f}'
            || (character.is_control() && !matches!(character, '\t' | '\n' | '\r'))
    }) {
        return Err(ContractError::new(ContractErrorCode::InvalidText));
    }
    Ok(())
}

pub(crate) fn validate_filter_value(value: &FilterValue) -> Result<(), ContractError> {
    let validate_value = |value: &str| {
        if value.len() > 512 {
            return Err(ContractError::new(ContractErrorCode::LimitExceeded));
        }
        validate_plain_text(value)
    };
    match value {
        FilterValue::Toggle(_) => Ok(()),
        FilterValue::Text(value) => validate_value(value),
        FilterValue::Multiple(values) => {
            if values.len() > 200 {
                return Err(ContractError::new(ContractErrorCode::LimitExceeded));
            }
            let mut unique = BTreeSet::new();
            for value in values {
                validate_value(value)?;
                if !unique.insert(value) {
                    return Err(ContractError::new(ContractErrorCode::DuplicateId));
                }
            }
            Ok(())
        }
    }
}
