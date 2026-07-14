use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use chrono::{DateTime, Utc};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};

use crate::live::diagnostics::LiveRedactor;

use super::{
    ArtworkSource, CacheHint, CatalogDefinition, CatalogPage, CatalogSet, ClientDisclosure,
    ContractError, ContractErrorCode, CredentialAuthority, DrmKind, Fact, FilterDefinition,
    FilterKind, FilterOption, FilterValue, ItemDiagnostic, ItemDiagnosticReason, ItemMetadata,
    LiveItem, MediaHints, ProviderContract, ProviderCookie, ProviderFailure, ProviderFailureCode,
    ProviderHealth, ProviderHealthStatus, ResolvedSources, SensitiveString, ServerEgress,
    SourceDescriptor, StreamChoice, StreamProtocol, TimeShift, validate_catalog_id,
    validate_description, validate_filter_value, validate_plain_text, validate_provider_id,
    validate_short_text,
    wire::{
        WireCacheHint, WireCatalogDefinition, WireCatalogPage, WireCatalogsResponse, WireCookie,
        WireFilterDefinition, WireHealthResponse, WireItem, WireMetaResponse,
        WireProviderErrorEnvelope, WireResolveResponse, WireSourceDescriptor, WireStreamChoice,
    },
};

const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_NODES: usize = 20_000;
const MAX_UNKNOWN_ARRAY_ITEMS: usize = 256;
const MAX_PROVIDER_CONFIG_BYTES: usize = 64 * 1024;
const MAX_PROVIDER_CONFIG_DEPTH: usize = 8;
const MAX_PROVIDER_CONFIG_NODES: usize = 2_048;
const DUPLICATE_KEY_MARKER: &str = "live_duplicate_json_key";
const LIMIT_MARKER: &str = "live_json_limit";

pub fn parse_health_response(body: &[u8]) -> Result<ProviderHealth, ContractError> {
    let value = parse_checked_json(body)?;
    require_fields(&value, &["status", "contractVersions"])?;
    let wire: WireHealthResponse = decode(value)?;
    if wire.contract_versions.is_empty() {
        return Err(contract(ContractErrorCode::InvalidShape));
    }
    let contract_versions = wire
        .contract_versions
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if contract_versions.len() != wire.contract_versions.len()
        || contract_versions.iter().any(|version| *version != 1)
        || wire.details.len() > 20
    {
        return Err(contract(ContractErrorCode::InvalidShape));
    }
    for detail in &wire.details {
        validate_short_text(detail)?;
    }
    let status = match wire.status.as_str() {
        "healthy" => ProviderHealthStatus::Healthy,
        "degraded" => ProviderHealthStatus::Degraded,
        "unhealthy" => ProviderHealthStatus::Unhealthy,
        _ => return Err(contract(ContractErrorCode::InvalidShape)),
    };
    Ok(ProviderHealth {
        status,
        contract_versions,
        details: wire.details,
    })
}

pub fn parse_catalogs_response(
    body: &[u8],
    contract_scope: &ProviderContract,
) -> Result<CatalogSet, ContractError> {
    let value = parse_checked_json(body)?;
    require_fields(&value, &["catalogs", "cache"])?;
    let wire: WireCatalogsResponse = decode(value)?;
    if wire.catalogs.len() > 50 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let mut ids = BTreeSet::new();
    let mut catalogs = Vec::with_capacity(wire.catalogs.len());
    for catalog in wire.catalogs {
        let catalog = validate_catalog_definition(catalog, contract_scope)?;
        if !ids.insert(catalog.id.clone()) {
            return Err(contract(ContractErrorCode::DuplicateId));
        }
        catalogs.push(catalog);
    }
    Ok(CatalogSet {
        catalogs,
        cache: validate_cache(wire.cache)?,
    })
}

pub fn parse_catalog_page_response(
    body: &[u8],
    contract_scope: &ProviderContract,
) -> Result<CatalogPage, ContractError> {
    let value = parse_checked_json(body)?;
    require_fields(&value, &["items", "nextCursor", "cache"])?;
    let wire: WireCatalogPage = decode(value)?;
    if wire.items.len() > 100
        || wire
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > 2_048)
    {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    if let Some(cursor) = wire.next_cursor.as_deref() {
        validate_plain_text(cursor)?;
    }

    let mut raw_ids = BTreeSet::new();
    for item in &wire.items {
        if let Some(id) = item.get("id").and_then(Value::as_str)
            && !raw_ids.insert(id)
        {
            return Err(contract(ContractErrorCode::DuplicateId));
        }
    }

    let total = wire.items.len();
    let allowed_invalid = (total / 10).min(10);
    let mut items = Vec::with_capacity(total);
    let mut diagnostics = Vec::new();
    for (index, item) in wire.items.into_iter().enumerate() {
        match validate_item_value(item, contract_scope) {
            Ok(item) => items.push(item),
            Err(reason) => diagnostics.push(ItemDiagnostic { index, reason }),
        }
    }
    if diagnostics.len() > allowed_invalid {
        return Err(contract(ContractErrorCode::TooManyInvalidItems));
    }
    Ok(CatalogPage {
        items,
        next_cursor: wire.next_cursor,
        cache: validate_cache(wire.cache)?,
        diagnostics,
    })
}

pub fn parse_meta_response(
    body: &[u8],
    contract_scope: &ProviderContract,
    expected_item_id: &str,
) -> Result<ItemMetadata, ContractError> {
    validate_provider_id(expected_item_id)?;
    let value = parse_checked_json(body)?;
    require_fields(&value, &["item", "streams", "cache"])?;
    let wire: WireMetaResponse = decode(value)?;
    if wire.streams.len() > 20 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let item = validate_item_value(wire.item, contract_scope)
        .map_err(|reason| contract(item_reason_code(reason)))?;
    if item.id != expected_item_id {
        return Err(contract(ContractErrorCode::InvalidId));
    }

    let mut ids = BTreeSet::new();
    let mut streams = Vec::with_capacity(wire.streams.len());
    for value in wire.streams {
        require_fields(&value, &["id", "label", "priority"])?;
        let choice: WireStreamChoice = decode(value)?;
        reject_choice_sensitive_fields(choice.extra.keys())?;
        validate_provider_id(&choice.id)?;
        validate_short_text(&choice.label)?;
        if let Some(quality) = choice.quality.as_deref() {
            validate_short_text(quality)?;
        }
        if let Some(language) = choice.language.as_deref() {
            if language.chars().count() > 64 {
                return Err(contract(ContractErrorCode::LimitExceeded));
            }
            validate_plain_text(language)?;
        }
        if !(-100_000..=100_000).contains(&choice.priority) {
            return Err(contract(ContractErrorCode::LimitExceeded));
        }
        if let Some(protocol) = choice.protocol_hint
            && !contract_scope.protocols.contains(&protocol)
        {
            return Err(contract(ContractErrorCode::UndeclaredProtocol));
        }
        if !ids.insert(choice.id.clone()) {
            return Err(contract(ContractErrorCode::DuplicateId));
        }
        streams.push(StreamChoice {
            id: choice.id,
            label: choice.label,
            quality: choice.quality,
            language: choice.language,
            protocol_hint: choice.protocol_hint,
            priority: choice.priority as i32,
        });
    }
    Ok(ItemMetadata {
        item,
        streams,
        cache: validate_cache(wire.cache)?,
    })
}

pub fn parse_resolve_response(
    body: &[u8],
    contract_scope: &ProviderContract,
    expected_stream_id: &str,
    now: DateTime<Utc>,
) -> Result<ResolvedSources, ContractError> {
    validate_provider_id(expected_stream_id)?;
    parse_resolved_sources(body, contract_scope, Some(expected_stream_id), now)
}

pub fn parse_refresh_response(
    body: &[u8],
    contract_scope: &ProviderContract,
    now: DateTime<Utc>,
) -> Result<ResolvedSources, ContractError> {
    parse_resolved_sources(body, contract_scope, None, now)
}

fn parse_resolved_sources(
    body: &[u8],
    contract_scope: &ProviderContract,
    expected_stream_id: Option<&str>,
    now: DateTime<Utc>,
) -> Result<ResolvedSources, ContractError> {
    let value = parse_checked_json(body)?;
    require_fields(&value, &["descriptor", "alternatives"])?;
    let wire: WireResolveResponse = decode(value)?;
    if wire.alternatives.len() > 10 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let descriptor = validate_descriptor_value(wire.descriptor, contract_scope, now)?;
    if expected_stream_id.is_some_and(|expected| descriptor.stream_id != expected) {
        return Err(contract(ContractErrorCode::InvalidId));
    }
    let mut ids = BTreeSet::from([descriptor.stream_id.clone()]);
    let mut alternatives = Vec::with_capacity(wire.alternatives.len());
    for value in wire.alternatives {
        let alternative = validate_descriptor_value(value, contract_scope, now)?;
        if !ids.insert(alternative.stream_id.clone()) {
            return Err(contract(ContractErrorCode::DuplicateId));
        }
        alternatives.push(alternative);
    }
    Ok(ResolvedSources {
        descriptor,
        alternatives,
    })
}

pub(crate) fn parse_provider_failure(
    body: &[u8],
    redactor: &LiveRedactor,
) -> Result<ProviderFailure, ContractError> {
    let value = parse_checked_json(body)?;
    require_fields(&value, &["error"])?;
    let error_value = value
        .get("error")
        .ok_or_else(|| contract(ContractErrorCode::InvalidShape))?;
    require_fields(error_value, &["code", "message", "retryable"])?;
    let wire: WireProviderErrorEnvelope = decode(value)?;
    validate_short_text(&wire.error.message)?;
    if !redactor.scan(&wire.error.message).is_clean()
        || wire
            .error
            .retry_after_seconds
            .is_some_and(|seconds| !(1..=3_600).contains(&seconds))
    {
        return Err(contract(ContractErrorCode::ForbiddenField));
    }
    let code = match wire.error.code.as_str() {
        "invalid_request" => ProviderFailureCode::InvalidRequest,
        "item_not_found" => ProviderFailureCode::ItemNotFound,
        "stream_not_found" => ProviderFailureCode::StreamNotFound,
        "stream_expired" => ProviderFailureCode::StreamExpired,
        "account_required" => ProviderFailureCode::AccountRequired,
        "upstream_unavailable" => ProviderFailureCode::UpstreamUnavailable,
        "upstream_rate_limited" => ProviderFailureCode::UpstreamRateLimited,
        "unsupported_input" => ProviderFailureCode::UnsupportedInput,
        "provider_contract_version_unsupported" => ProviderFailureCode::ContractVersionUnsupported,
        "internal_error" => ProviderFailureCode::InternalError,
        _ => return Err(contract(ContractErrorCode::InvalidShape)),
    };
    Ok(ProviderFailure {
        code,
        message: wire.error.message,
        retryable: wire.error.retryable,
        retry_after_seconds: wire.error.retry_after_seconds.map(|value| value as u32),
    })
}

pub fn validate_provider_config(value: &Value) -> Result<(), ContractError> {
    if !value.is_object()
        || serde_json::to_vec(value)
            .map_err(|_| contract(ContractErrorCode::UnsafeProviderConfig))?
            .len()
            > MAX_PROVIDER_CONFIG_BYTES
    {
        return Err(contract(ContractErrorCode::UnsafeProviderConfig));
    }
    let mut nodes = 0usize;
    validate_config_value(value, 0, &mut nodes)
}

fn validate_config_value(
    value: &Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), ContractError> {
    *nodes = nodes.saturating_add(1);
    if depth > MAX_PROVIDER_CONFIG_DEPTH || *nodes > MAX_PROVIDER_CONFIG_NODES {
        return Err(contract(ContractErrorCode::UnsafeProviderConfig));
    }
    match value {
        Value::Object(object) => {
            if object.len() > 128 {
                return Err(contract(ContractErrorCode::UnsafeProviderConfig));
            }
            for (key, child) in object {
                if key.is_empty()
                    || key.len() > 128
                    || key.chars().any(char::is_control)
                    || sensitive_config_key(key)
                {
                    return Err(contract(ContractErrorCode::UnsafeProviderConfig));
                }
                validate_config_value(child, depth + 1, nodes)?;
            }
        }
        Value::Array(values) => {
            if values.len() > MAX_UNKNOWN_ARRAY_ITEMS {
                return Err(contract(ContractErrorCode::UnsafeProviderConfig));
            }
            for child in values {
                validate_config_value(child, depth + 1, nodes)?;
            }
        }
        Value::String(value) => {
            if value.len() > 4_096
                || value.chars().any(|character| character == '\0')
                || contains_secret_material(value)
            {
                return Err(contract(ContractErrorCode::UnsafeProviderConfig));
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn sensitive_config_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    [
        "password",
        "passphrase",
        "secret",
        "token",
        "cookie",
        "authorization",
        "credential",
        "apikey",
        "privatekey",
        "accesskey",
        "encrypted",
        "refreshhandle",
        "hostpath",
        "hostdirectory",
        "hostdir",
        "volume",
        "mount",
        "librarypath",
        "mediapath",
        "downloadpath",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn contains_secret_material(value: &str) -> bool {
    let lowered = value.to_ascii_lowercase();
    if lowered.starts_with("bearer ")
        || lowered.contains("-----begin private key-----")
        || lowered.contains("-----begin rsa private key-----")
        || lowered.contains("elx_live_v1_")
    {
        return true;
    }
    Url::parse(value).is_ok_and(|url| {
        !url.username().is_empty()
            || url.password().is_some()
            || url.query_pairs().any(|(key, _)| sensitive_config_key(&key))
    })
}

fn validate_catalog_definition(
    wire: WireCatalogDefinition,
    contract_scope: &ProviderContract,
) -> Result<CatalogDefinition, ContractError> {
    validate_catalog_id(&wire.id)?;
    validate_short_text(&wire.name)?;
    if let Some(description) = wire.description.as_deref() {
        validate_description(description)?;
    }
    let item_types = wire.item_types.iter().copied().collect::<BTreeSet<_>>();
    if item_types.is_empty()
        || item_types.len() != wire.item_types.len()
        || item_types.len() > 2
        || !item_types.is_subset(&contract_scope.item_types)
    {
        return Err(contract(ContractErrorCode::UndeclaredItemType));
    }
    if !(-100_000..=100_000).contains(&wire.order) || wire.filters.len() > 12 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let mut filter_ids = BTreeSet::new();
    let mut filters = Vec::with_capacity(wire.filters.len());
    for filter in wire.filters {
        let filter = validate_filter_definition(filter)?;
        if !filter_ids.insert(filter.id.clone()) {
            return Err(contract(ContractErrorCode::DuplicateId));
        }
        filters.push(filter);
    }
    Ok(CatalogDefinition {
        id: wire.id,
        name: wire.name,
        description: wire.description,
        item_types,
        presentation: wire.presentation,
        order: wire.order as i32,
        filters,
    })
}

fn validate_filter_definition(
    wire: WireFilterDefinition,
) -> Result<FilterDefinition, ContractError> {
    validate_catalog_id(&wire.id)?;
    validate_short_text(&wire.label)?;
    if wire.options.len() > 200 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let mut option_values = BTreeSet::new();
    let mut options = Vec::with_capacity(wire.options.len());
    for option in wire.options {
        if option.value.is_empty() || option.value.len() > 512 {
            return Err(contract(ContractErrorCode::InvalidFilter));
        }
        validate_plain_text(&option.value)?;
        validate_short_text(&option.label)?;
        if !option_values.insert(option.value.clone()) {
            return Err(contract(ContractErrorCode::DuplicateId));
        }
        options.push(FilterOption {
            value: option.value,
            label: option.label,
        });
    }
    if let Some(default) = wire.default.as_ref() {
        validate_filter_value(default)?;
    }
    let default_valid = match (&wire.kind, wire.default.as_ref()) {
        (_, None) => true,
        (FilterKind::Toggle, Some(FilterValue::Toggle(_))) => true,
        (FilterKind::SingleSelect, Some(FilterValue::Text(value))) => option_values.contains(value),
        (FilterKind::MultiSelect, Some(FilterValue::Multiple(values))) => {
            values.iter().all(|value| option_values.contains(value))
        }
        (FilterKind::Search | FilterKind::Date, Some(FilterValue::Text(_))) => true,
        _ => false,
    };
    let option_shape_valid = match wire.kind {
        FilterKind::Toggle | FilterKind::Search | FilterKind::Date => options.is_empty(),
        FilterKind::SingleSelect | FilterKind::MultiSelect => true,
    };
    if !default_valid || !option_shape_valid {
        return Err(contract(ContractErrorCode::InvalidFilter));
    }
    Ok(FilterDefinition {
        id: wire.id,
        label: wire.label,
        kind: wire.kind,
        required: wire.required,
        default: wire.default,
        options,
    })
}

fn validate_item_value(
    value: Value,
    contract_scope: &ProviderContract,
) -> Result<LiveItem, ItemDiagnosticReason> {
    require_fields(
        &value,
        &[
            "id",
            "itemType",
            "title",
            "status",
            "categories",
            "badges",
            "facts",
        ],
    )
    .map_err(|_| ItemDiagnosticReason::InvalidShape)?;
    let wire: WireItem = decode(value).map_err(|_| ItemDiagnosticReason::InvalidShape)?;
    reject_item_sensitive_fields(wire.extra.keys())?;
    validate_provider_id(&wire.id).map_err(|_| ItemDiagnosticReason::InvalidId)?;
    if !contract_scope.item_types.contains(&wire.item_type) {
        return Err(ItemDiagnosticReason::UndeclaredItemType);
    }
    validate_short_text(&wire.title).map_err(|_| ItemDiagnosticReason::InvalidText)?;
    if let Some(subtitle) = wire.subtitle.as_deref() {
        validate_short_text(subtitle).map_err(|_| ItemDiagnosticReason::InvalidText)?;
    }
    if let Some(description) = wire.description.as_deref() {
        validate_description(description).map_err(|_| ItemDiagnosticReason::InvalidText)?;
    }
    if wire.categories.len() > 20 || wire.badges.len() > 20 || wire.facts.len() > 20 {
        return Err(ItemDiagnosticReason::InvalidShape);
    }
    for text in wire.categories.iter().chain(&wire.badges) {
        validate_short_text(text).map_err(|_| ItemDiagnosticReason::InvalidText)?;
    }
    let mut facts = Vec::with_capacity(wire.facts.len());
    for fact in wire.facts {
        validate_short_text(&fact.label).map_err(|_| ItemDiagnosticReason::InvalidText)?;
        validate_short_text(&fact.value).map_err(|_| ItemDiagnosticReason::InvalidText)?;
        facts.push(Fact {
            label: fact.label,
            value: fact.value,
        });
    }
    let starts_at = wire
        .starts_at
        .as_deref()
        .map(parse_utc)
        .transpose()
        .map_err(|_| ItemDiagnosticReason::InvalidDate)?;
    let ends_at = wire
        .ends_at
        .as_deref()
        .map(parse_utc)
        .transpose()
        .map_err(|_| ItemDiagnosticReason::InvalidDate)?;
    if starts_at
        .zip(ends_at)
        .is_some_and(|(start, end)| end < start)
    {
        return Err(ItemDiagnosticReason::InvalidDate);
    }
    let poster = wire
        .poster_url
        .map(validate_artwork_url)
        .transpose()
        .map_err(|_| ItemDiagnosticReason::InvalidArtwork)?;
    let background = wire
        .background_url
        .map(validate_artwork_url)
        .transpose()
        .map_err(|_| ItemDiagnosticReason::InvalidArtwork)?;
    let logo = wire
        .logo_url
        .map(validate_artwork_url)
        .transpose()
        .map_err(|_| ItemDiagnosticReason::InvalidArtwork)?;
    Ok(LiveItem {
        id: wire.id,
        item_type: wire.item_type,
        title: wire.title,
        subtitle: wire.subtitle,
        description: wire.description,
        status: wire.status,
        starts_at,
        ends_at,
        poster,
        background,
        logo,
        categories: wire.categories,
        badges: wire.badges,
        facts,
    })
}

fn validate_descriptor_value(
    value: Value,
    contract_scope: &ProviderContract,
    now: DateTime<Utc>,
) -> Result<SourceDescriptor, ContractError> {
    require_fields(
        &value,
        &[
            "streamId",
            "label",
            "priority",
            "protocol",
            "url",
            "requestHeaders",
            "cookies",
            "origin",
            "referer",
            "credentialAuthorities",
            "clientDisclosure",
            "expiresAt",
            "refreshHandle",
            "serverEgress",
            "privateNetwork",
            "drm",
            "timeShift",
        ],
    )?;
    if let Some(cookies) = value.get("cookies").and_then(Value::as_array) {
        for cookie in cookies {
            require_fields(
                cookie,
                &[
                    "name",
                    "value",
                    "domain",
                    "path",
                    "secure",
                    "httpOnly",
                    "expiresAt",
                ],
            )?;
        }
    }
    if let Some(time_shift) = value.get("timeShift") {
        require_fields(time_shift, &["available", "windowSeconds"])?;
    }
    let wire: WireSourceDescriptor = decode(value)?;
    reject_descriptor_fields(wire.extra.keys())?;
    validate_provider_id(&wire.stream_id)?;
    validate_short_text(&wire.label)?;
    if let Some(quality) = wire.quality.as_deref() {
        validate_short_text(quality)?;
    }
    if let Some(language) = wire.language.as_deref() {
        if language.chars().count() > 64 {
            return Err(contract(ContractErrorCode::LimitExceeded));
        }
        validate_plain_text(language)?;
    }
    if !(-100_000..=100_000).contains(&wire.priority) {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    if !contract_scope.protocols.contains(&wire.protocol) {
        return Err(contract(ContractErrorCode::UndeclaredProtocol));
    }
    if wire.drm.kind != DrmKind::None {
        return Err(contract(ContractErrorCode::DrmUnsupported));
    }

    let source_url = validate_source_url(&wire.url, wire.protocol)?;
    let headers = validate_headers(wire.request_headers)?;
    let cookies = validate_cookies(wire.cookies)?;
    let authorities = validate_authorities(wire.credential_authorities)?;
    let origin = wire
        .origin
        .map(|value| validate_origin(&value).map(|()| SensitiveString::new(value)))
        .transpose()?;
    let referer = wire
        .referer
        .map(|value| validate_referer(&value).map(|()| SensitiveString::new(value)))
        .transpose()?;
    require_initial_credential_authority(
        &source_url,
        &authorities,
        !headers.is_empty(),
        !cookies.is_empty(),
        origin.is_some(),
        referer.is_some(),
    )?;

    let expires_at = wire.expires_at.as_deref().map(parse_utc).transpose()?;
    if expires_at.is_some_and(|expires_at| expires_at <= now) {
        return Err(contract(ContractErrorCode::DescriptorExpired));
    }
    let refresh_handle = wire
        .refresh_handle
        .map(|value| {
            if value.is_empty() || value.len() > 2_048 {
                Err(contract(ContractErrorCode::LimitExceeded))
            } else {
                Ok(SensitiveString::new(value))
            }
        })
        .transpose()?;
    let time_shift =
        validate_time_shift(wire.time_shift.available, wire.time_shift.window_seconds)?;
    let media = wire.media.map(validate_media_hints).transpose()?;

    if matches!(wire.protocol, StreamProtocol::Rtmp | StreamProtocol::Srt)
        && (!headers.is_empty()
            || !cookies.is_empty()
            || origin.is_some()
            || referer.is_some()
            || !authorities.is_empty()
            || wire.client_disclosure != ClientDisclosure::ServerOnly)
    {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    if wire.private_network
        && (wire.client_disclosure != ClientDisclosure::ServerOnly
            || wire.server_egress != ServerEgress::Required)
    {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    if wire.client_disclosure == ClientDisclosure::Public
        && (source_url.scheme() != "https"
            || source_url.query().is_some()
            || !headers.is_empty()
            || !cookies.is_empty()
            || origin.is_some()
            || referer.is_some()
            || !authorities.is_empty()
            || expires_at.is_some()
            || refresh_handle.is_some()
            || wire.server_egress != ServerEgress::NotRequired
            || wire.private_network)
    {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }

    Ok(SourceDescriptor {
        stream_id: wire.stream_id,
        label: wire.label,
        quality: wire.quality,
        language: wire.language,
        priority: wire.priority as i32,
        protocol: wire.protocol,
        url: SensitiveString::new(wire.url),
        request_headers: headers,
        cookies,
        origin,
        referer,
        credential_authorities: authorities,
        client_disclosure: wire.client_disclosure,
        expires_at,
        refresh_handle,
        server_egress: wire.server_egress,
        private_network: wire.private_network,
        time_shift,
        media,
    })
}

fn validate_cache(wire: WireCacheHint) -> Result<CacheHint, ContractError> {
    if !(0..=86_400).contains(&wire.max_age_seconds)
        || !(0..=86_400).contains(&wire.stale_while_revalidate_seconds)
        || wire.etag.as_ref().is_some_and(|etag| etag.len() > 512)
    {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    if let Some(etag) = wire.etag.as_deref() {
        validate_plain_text(etag)?;
    }
    Ok(CacheHint {
        max_age_seconds: wire.max_age_seconds as u32,
        stale_while_revalidate_seconds: wire.stale_while_revalidate_seconds as u32,
        etag: wire.etag,
    })
}

fn validate_artwork_url(value: String) -> Result<ArtworkSource, ContractError> {
    if value.len() > 8_192 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let parsed = Url::parse(&value).map_err(|_| contract(ContractErrorCode::InvalidUrl))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.host_str().is_none()
        || parsed.fragment().is_some()
    {
        return Err(contract(ContractErrorCode::InvalidUrl));
    }
    Ok(ArtworkSource::new(value))
}

fn validate_source_url(value: &str, protocol: StreamProtocol) -> Result<Url, ContractError> {
    if value.len() > 8_192 || contains_forbidden_stream_material(value) {
        return Err(contract(ContractErrorCode::InvalidUrl));
    }
    let parsed = Url::parse(value).map_err(|_| contract(ContractErrorCode::InvalidUrl))?;
    if !protocol.expected_scheme().contains(&parsed.scheme())
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.host_str().is_none()
        || parsed.fragment().is_some()
    {
        return Err(contract(ContractErrorCode::InvalidUrl));
    }
    Ok(parsed)
}

fn contains_forbidden_stream_material(value: &str) -> bool {
    let lowered = value.to_ascii_lowercase();
    [
        "magnet:",
        "urn:btih",
        "webtorrent",
        "webrtc",
        "info_hash=",
        "infohash=",
        "javascript:",
        "data:",
        "file:",
    ]
    .iter()
    .any(|needle| lowered.contains(needle))
}

fn validate_headers(
    values: BTreeMap<String, String>,
) -> Result<BTreeMap<String, SensitiveString>, ContractError> {
    if values.len() > 32 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let mut output = BTreeMap::new();
    for (name, value) in values {
        if name.is_empty()
            || name.len() > 128
            || value.len() > 4_096
            || HeaderName::from_bytes(name.as_bytes()).is_err()
            || HeaderValue::from_str(&value).is_err()
        {
            return Err(contract(ContractErrorCode::InvalidCredentials));
        }
        let normalized = name.to_ascii_lowercase();
        if forbidden_header(&normalized)
            || output
                .insert(normalized, SensitiveString::new(value))
                .is_some()
        {
            return Err(contract(ContractErrorCode::InvalidCredentials));
        }
    }
    Ok(output)
}

fn forbidden_header(name: &str) -> bool {
    name.starts_with("proxy-")
        || matches!(
            name,
            "host"
                | "connection"
                | "keep-alive"
                | "transfer-encoding"
                | "te"
                | "trailer"
                | "upgrade"
                | "content-length"
                | "cookie"
                | "set-cookie"
                | "origin"
                | "referer"
                | "via"
                | "forwarded"
                | "x-forwarded-for"
                | "x-forwarded-host"
                | "x-forwarded-proto"
        )
}

fn validate_cookies(values: Vec<Value>) -> Result<Vec<ProviderCookie>, ContractError> {
    if values.len() > 32 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let mut aggregate = 0usize;
    let mut unique = BTreeSet::new();
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        let wire: WireCookie = decode(value)?;
        if !valid_cookie_name(&wire.name)
            || wire.value.len() > 4_096
            || wire
                .value
                .bytes()
                .any(|byte| byte < 0x20 || byte == 0x7f || byte == b';')
        {
            return Err(contract(ContractErrorCode::InvalidCredentials));
        }
        let domain = wire.domain.map(normalize_cookie_domain).transpose()?;
        if let Some(path) = wire.path.as_deref()
            && (path.len() > 1_024 || !path.starts_with('/') || path.chars().any(char::is_control))
        {
            return Err(contract(ContractErrorCode::InvalidCredentials));
        }
        let expires_at = wire.expires_at.as_deref().map(parse_utc).transpose()?;
        aggregate = aggregate
            .saturating_add(wire.name.len())
            .saturating_add(wire.value.len())
            .saturating_add(domain.as_ref().map_or(0, String::len))
            .saturating_add(wire.path.as_ref().map_or(0, String::len));
        if aggregate > 16 * 1024
            || !unique.insert((wire.name.clone(), domain.clone(), wire.path.clone()))
        {
            return Err(contract(ContractErrorCode::InvalidCredentials));
        }
        output.push(ProviderCookie {
            name: wire.name,
            value: SensitiveString::new(wire.value),
            domain,
            path: wire.path,
            secure: wire.secure,
            http_only: wire.http_only,
            expires_at,
        });
    }
    Ok(output)
}

fn valid_cookie_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte > 0x20
                && byte < 0x7f
                && !matches!(
                    byte,
                    b'(' | b')'
                        | b'<'
                        | b'>'
                        | b'@'
                        | b','
                        | b';'
                        | b':'
                        | b'\\'
                        | b'"'
                        | b'/'
                        | b'['
                        | b']'
                        | b'?'
                        | b'='
                        | b'{'
                        | b'}'
                )
        })
}

fn normalize_cookie_domain(value: String) -> Result<String, ContractError> {
    let value = value.trim_start_matches('.').to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 253
        || !value.is_ascii()
        || value.contains('*')
        || value.ends_with('.')
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == b'/')
    {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    validate_authority_host("https", &value, 443).map(|(_, host, _)| host)
}

fn validate_authorities(
    values: Vec<super::wire::WireCredentialAuthority>,
) -> Result<Vec<CredentialAuthority>, ContractError> {
    if values.len() > 8 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let mut unique = BTreeSet::new();
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        if !matches!(value.scheme.as_str(), "http" | "https") {
            return Err(contract(ContractErrorCode::InvalidCredentials));
        }
        let (scheme, host, port) = validate_authority_host(&value.scheme, &value.host, value.port)?;
        if (value.send_request_headers
            || value.send_cookies
            || value.send_origin
            || value.send_referer)
            && scheme != "https"
        {
            return Err(contract(ContractErrorCode::InvalidCredentials));
        }
        if !unique.insert((scheme.clone(), host.clone(), port)) {
            return Err(contract(ContractErrorCode::DuplicateId));
        }
        output.push(CredentialAuthority {
            scheme,
            host,
            port,
            send_request_headers: value.send_request_headers,
            send_cookies: value.send_cookies,
            send_origin: value.send_origin,
            send_referer: value.send_referer,
        });
    }
    Ok(output)
}

fn validate_authority_host(
    scheme: &str,
    host: &str,
    port: u16,
) -> Result<(String, String, u16), ContractError> {
    if !host.is_ascii()
        || host.is_empty()
        || host.len() > 253
        || host.contains('*')
        || host.ends_with('.')
    {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    let formatted_host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let parsed = Url::parse(&format!("{scheme}://{formatted_host}:{port}/"))
        .map_err(|_| contract(ContractErrorCode::InvalidCredentials))?;
    let normalized_host = parsed
        .host_str()
        .ok_or_else(|| contract(ContractErrorCode::InvalidCredentials))?
        .to_ascii_lowercase();
    Ok((scheme.to_string(), normalized_host, port))
}

fn require_initial_credential_authority(
    source: &Url,
    authorities: &[CredentialAuthority],
    headers: bool,
    cookies: bool,
    origin: bool,
    referer: bool,
) -> Result<(), ContractError> {
    if !(headers || cookies || origin || referer) {
        return Ok(());
    }
    if !matches!(source.scheme(), "http" | "https") {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    let source_host = source
        .host_str()
        .ok_or_else(|| contract(ContractErrorCode::InvalidCredentials))?
        .to_ascii_lowercase();
    let source_port = source
        .port_or_known_default()
        .ok_or_else(|| contract(ContractErrorCode::InvalidCredentials))?;
    let authority = authorities.iter().find(|authority| {
        authority.scheme == source.scheme()
            && authority.host == source_host
            && authority.port == source_port
    });
    if authority.is_some_and(|authority| {
        (!headers || authority.send_request_headers)
            && (!cookies || authority.send_cookies)
            && (!origin || authority.send_origin)
            && (!referer || authority.send_referer)
    }) {
        Ok(())
    } else {
        Err(contract(ContractErrorCode::InvalidCredentials))
    }
}

fn validate_origin(value: &str) -> Result<(), ContractError> {
    if value.len() > 2_048 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let parsed = validate_http_reference(value)?;
    if parsed.path() != "/" || parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    Ok(())
}

fn validate_referer(value: &str) -> Result<(), ContractError> {
    if value.len() > 8_192 {
        return Err(contract(ContractErrorCode::LimitExceeded));
    }
    let parsed = validate_http_reference(value)?;
    if parsed.fragment().is_some() {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    Ok(())
}

fn validate_http_reference(value: &str) -> Result<Url, ContractError> {
    let parsed = Url::parse(value).map_err(|_| contract(ContractErrorCode::InvalidUrl))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.host_str().is_none()
    {
        return Err(contract(ContractErrorCode::InvalidCredentials));
    }
    Ok(parsed)
}

fn validate_time_shift(available: bool, window: Option<i64>) -> Result<TimeShift, ContractError> {
    match (available, window) {
        (false, None) => Ok(TimeShift {
            available,
            window_seconds: None,
        }),
        (true, Some(window)) if (1..=86_400).contains(&window) => Ok(TimeShift {
            available,
            window_seconds: Some(window as u32),
        }),
        _ => Err(contract(ContractErrorCode::InvalidShape)),
    }
}

fn validate_media_hints(wire: super::wire::WireMediaHints) -> Result<MediaHints, ContractError> {
    for value in [
        wire.container.as_deref(),
        wire.video_codec.as_deref(),
        wire.audio_codec.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if value.chars().count() > 64 {
            return Err(contract(ContractErrorCode::LimitExceeded));
        }
        validate_plain_text(value)?;
    }
    Ok(MediaHints {
        container: wire.container,
        video_codec: wire.video_codec,
        audio_codec: wire.audio_codec,
    })
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>, ContractError> {
    if !value.ends_with('Z') {
        return Err(contract(ContractErrorCode::InvalidDate));
    }
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| contract(ContractErrorCode::InvalidDate))
}

fn reject_item_sensitive_fields<'a>(
    keys: impl Iterator<Item = &'a String>,
) -> Result<(), ItemDiagnosticReason> {
    const FORBIDDEN: &[&str] = &[
        "url",
        "stream",
        "streams",
        "source",
        "sources",
        "descriptor",
        "descriptors",
        "requestheaders",
        "headers",
        "cookies",
        "origin",
        "referer",
        "refreshhandle",
        "token",
        "credentialauthorities",
        "drm",
        "magnet",
        "infohash",
        "player",
        "iframe",
        "html",
    ];
    if keys
        .map(|key| normalize_key(key))
        .any(|key| FORBIDDEN.contains(&key.as_str()))
    {
        Err(ItemDiagnosticReason::ForbiddenField)
    } else {
        Ok(())
    }
}

fn reject_choice_sensitive_fields<'a>(
    keys: impl Iterator<Item = &'a String>,
) -> Result<(), ContractError> {
    let forbidden = [
        "url",
        "headers",
        "requestheaders",
        "cookies",
        "origin",
        "referer",
        "refreshhandle",
        "token",
        "credential",
        "credentialauthorities",
        "account",
        "descriptor",
        "source",
    ];
    if keys
        .map(|key| normalize_key(key))
        .any(|key| forbidden.iter().any(|value| key.contains(value)))
    {
        Err(contract(ContractErrorCode::ForbiddenField))
    } else {
        Ok(())
    }
}

fn reject_descriptor_fields<'a>(
    keys: impl Iterator<Item = &'a String>,
) -> Result<(), ContractError> {
    let forbidden = [
        "license",
        "licenseurl",
        "magnet",
        "torrent",
        "infohash",
        "webtorrent",
        "webrtc",
        "iframe",
        "player",
        "html",
        "javascript",
        "challenge",
        "executable",
        "browser",
    ];
    if keys
        .map(|key| normalize_key(key))
        .any(|key| forbidden.iter().any(|value| key.contains(value)))
    {
        Err(contract(ContractErrorCode::ForbiddenField))
    } else {
        Ok(())
    }
}

fn normalize_key(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn item_reason_code(reason: ItemDiagnosticReason) -> ContractErrorCode {
    match reason {
        ItemDiagnosticReason::InvalidShape => ContractErrorCode::InvalidShape,
        ItemDiagnosticReason::InvalidId => ContractErrorCode::InvalidId,
        ItemDiagnosticReason::InvalidText => ContractErrorCode::InvalidText,
        ItemDiagnosticReason::InvalidDate => ContractErrorCode::InvalidDate,
        ItemDiagnosticReason::InvalidArtwork => ContractErrorCode::InvalidUrl,
        ItemDiagnosticReason::UndeclaredItemType => ContractErrorCode::UndeclaredItemType,
        ItemDiagnosticReason::ForbiddenField => ContractErrorCode::ForbiddenField,
    }
}

fn contract(code: ContractErrorCode) -> ContractError {
    ContractError::new(code)
}

fn require_fields(value: &Value, fields: &[&str]) -> Result<(), ContractError> {
    let object = value
        .as_object()
        .ok_or_else(|| contract(ContractErrorCode::InvalidShape))?;
    if fields.iter().all(|field| object.contains_key(*field)) {
        Ok(())
    } else {
        Err(contract(ContractErrorCode::InvalidShape))
    }
}

fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, ContractError> {
    serde_json::from_value(value).map_err(|_| contract(ContractErrorCode::InvalidShape))
}

fn parse_checked_json(body: &[u8]) -> Result<Value, ContractError> {
    if body.is_empty() || body.len() > super::MAX_PROVIDER_RESPONSE_BYTES {
        return Err(contract(if body.is_empty() {
            ContractErrorCode::MalformedJson
        } else {
            ContractErrorCode::LimitExceeded
        }));
    }
    let nodes = Cell::new(0usize);
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let value = CheckedValueSeed {
        depth: 0,
        nodes: &nodes,
        sequence_limit: MAX_UNKNOWN_ARRAY_ITEMS,
    }
    .deserialize(&mut deserializer)
    .map_err(|error| {
        let message = error.to_string();
        if message.contains(DUPLICATE_KEY_MARKER) {
            contract(ContractErrorCode::DuplicateJsonKey)
        } else if message.contains(LIMIT_MARKER) {
            contract(ContractErrorCode::LimitExceeded)
        } else {
            contract(ContractErrorCode::MalformedJson)
        }
    })?;
    deserializer
        .end()
        .map_err(|_| contract(ContractErrorCode::MalformedJson))?;
    Ok(value)
}

struct CheckedValueSeed<'a> {
    depth: usize,
    nodes: &'a Cell<usize>,
    sequence_limit: usize,
}

impl<'de> DeserializeSeed<'de> for CheckedValueSeed<'_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let nodes = self.nodes.get().saturating_add(1);
        if self.depth > MAX_JSON_DEPTH || nodes > MAX_JSON_NODES {
            return Err(de::Error::custom(LIMIT_MARKER));
        }
        self.nodes.set(nodes);
        deserializer.deserialize_any(CheckedValueVisitor(self))
    }
}

struct CheckedValueVisitor<'a>(CheckedValueSeed<'a>);

impl<'de> Visitor<'de> for CheckedValueVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("invalid JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(CheckedValueSeed {
            depth: self.0.depth + 1,
            nodes: self.0.nodes,
            sequence_limit: MAX_UNKNOWN_ARRAY_ITEMS,
        })? {
            if values.len() >= self.0.sequence_limit {
                return Err(de::Error::custom(LIMIT_MARKER));
            }
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom(DUPLICATE_KEY_MARKER));
            }
            if values.len() >= 512 {
                return Err(de::Error::custom(LIMIT_MARKER));
            }
            let value = map.next_value_seed(CheckedValueSeed {
                depth: self.0.depth + 1,
                nodes: self.0.nodes,
                sequence_limit: sequence_limit_for_key(&key),
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

fn sequence_limit_for_key(key: &str) -> usize {
    match key {
        "catalogs" => 50,
        "items" => 100,
        "streams" | "categories" | "badges" | "facts" | "details" => 20,
        "alternatives" => 10,
        "filters" => 12,
        "options" => 200,
        "cookies" => 32,
        "credentialAuthorities" => 8,
        "itemTypes" => 2,
        _ => MAX_UNKNOWN_ARRAY_ITEMS,
    }
}
