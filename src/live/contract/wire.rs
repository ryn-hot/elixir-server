use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::{
    CatalogPresentation, ClientDisclosure, DrmKind, FilterKind, FilterValue, LiveItemStatus,
    LiveItemType, ServerEgress, StreamProtocol,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireHealthResponse {
    pub status: String,
    pub contract_versions: Vec<u32>,
    #[serde(default)]
    pub details: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireCacheHint {
    pub max_age_seconds: i64,
    pub stale_while_revalidate_seconds: i64,
    pub etag: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct WireCatalogsResponse {
    pub catalogs: Vec<WireCatalogDefinition>,
    pub cache: WireCacheHint,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireCatalogDefinition {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub item_types: Vec<LiveItemType>,
    pub presentation: CatalogPresentation,
    pub order: i64,
    pub filters: Vec<WireFilterDefinition>,
}

#[derive(Deserialize)]
pub(super) struct WireFilterDefinition {
    pub id: String,
    pub label: String,
    #[serde(rename = "type")]
    pub kind: FilterKind,
    pub required: bool,
    pub default: Option<FilterValue>,
    #[serde(default)]
    pub options: Vec<WireFilterOption>,
}

#[derive(Deserialize)]
pub(super) struct WireFilterOption {
    pub value: String,
    pub label: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireCatalogPage {
    pub items: Vec<Value>,
    pub next_cursor: Option<String>,
    pub cache: WireCacheHint,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireItem {
    pub id: String,
    pub item_type: LiveItemType,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    pub status: LiveItemStatus,
    pub starts_at: Option<String>,
    pub ends_at: Option<String>,
    pub poster_url: Option<String>,
    pub background_url: Option<String>,
    pub logo_url: Option<String>,
    pub categories: Vec<String>,
    pub badges: Vec<String>,
    pub facts: Vec<WireFact>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
pub(super) struct WireFact {
    pub label: String,
    pub value: String,
}

#[derive(Deserialize)]
pub(super) struct WireMetaResponse {
    pub item: Value,
    pub streams: Vec<Value>,
    pub cache: WireCacheHint,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireStreamChoice {
    pub id: String,
    pub label: String,
    pub quality: Option<String>,
    pub language: Option<String>,
    pub protocol_hint: Option<StreamProtocol>,
    pub priority: i64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
pub(super) struct WireResolveResponse {
    pub descriptor: Value,
    pub alternatives: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireSourceDescriptor {
    pub stream_id: String,
    pub label: String,
    pub quality: Option<String>,
    pub language: Option<String>,
    pub priority: i64,
    pub protocol: StreamProtocol,
    pub url: String,
    pub request_headers: BTreeMap<String, String>,
    pub cookies: Vec<Value>,
    pub origin: Option<String>,
    pub referer: Option<String>,
    pub credential_authorities: Vec<WireCredentialAuthority>,
    pub client_disclosure: ClientDisclosure,
    pub expires_at: Option<String>,
    pub refresh_handle: Option<String>,
    pub server_egress: ServerEgress,
    pub private_network: bool,
    pub drm: WireDrm,
    pub time_shift: WireTimeShift,
    pub media: Option<WireMediaHints>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireCredentialAuthority {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub send_request_headers: bool,
    pub send_cookies: bool,
    pub send_origin: bool,
    pub send_referer: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireCookie {
    pub name: String,
    pub value: String,
    pub domain: Option<String>,
    pub path: Option<String>,
    pub secure: bool,
    pub http_only: bool,
    pub expires_at: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct WireDrm {
    pub kind: DrmKind,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireTimeShift {
    pub available: bool,
    pub window_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireMediaHints {
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct WireProviderErrorEnvelope {
    pub error: WireProviderError,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireProviderError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after_seconds: Option<i64>,
}
