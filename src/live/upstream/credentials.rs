use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::IpAddr,
    sync::{Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use reqwest::{
    Url,
    header::{
        ACCEPT, ACCEPT_ENCODING, COOKIE, HeaderMap, HeaderName, HeaderValue, ORIGIN, REFERER,
        SET_COOKIE, USER_AGENT,
    },
};

use crate::live::contract::{
    CredentialAuthority, ProviderCookie, SensitiveString, SourceDescriptor,
};

use super::{
    error::{Result, UpstreamErrorCode},
    policy::ValidatedUrl,
};

const USER_AGENT_VALUE: &str = "Elixir-Live/1";
const MAX_SAFE_HEADER_VALUE: usize = 1_024;
const MAX_PROVIDER_HEADERS: usize = 32;
const MAX_PROVIDER_HEADER_BYTES: usize = 16 * 1024;
const MAX_COOKIES: usize = 32;
const MAX_COOKIE_BYTES: usize = 16 * 1024;

#[derive(Clone, Default)]
pub struct SafeRequestHeaders {
    values: BTreeMap<String, String>,
}

impl fmt::Debug for SafeRequestHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeRequestHeaders")
            .field("header_count", &self.values.len())
            .finish()
    }
}

impl SafeRequestHeaders {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: &str, value: &str) -> Result<()> {
        let normalized = name.to_ascii_lowercase();
        if !matches!(normalized.as_str(), "accept" | "accept-language" | "range")
            || value.is_empty()
            || value.len() > MAX_SAFE_HEADER_VALUE
            || HeaderValue::from_str(value).is_err()
        {
            return Err(UpstreamErrorCode::HeaderRejected.into());
        }
        if normalized == "range" && !valid_range(value) {
            return Err(UpstreamErrorCode::HeaderRejected.into());
        }
        self.values.insert(normalized, value.to_string());
        Ok(())
    }

    pub(crate) fn to_header_map(&self) -> Result<HeaderMap> {
        let mut output = HeaderMap::new();
        output.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));
        output.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        if !self.values.contains_key("accept") {
            output.insert(ACCEPT, HeaderValue::from_static("*/*"));
        }
        for (name, value) in &self.values {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| UpstreamErrorCode::HeaderRejected)?;
            let value =
                HeaderValue::from_str(value).map_err(|_| UpstreamErrorCode::HeaderRejected)?;
            output.insert(name, value);
        }
        Ok(output)
    }
}

pub struct CredentialSet {
    request_headers: Vec<(HeaderName, SensitiveString)>,
    cookies: Mutex<CookieJar>,
    origin: Option<SensitiveString>,
    referer: Option<SensitiveString>,
    authorities: Vec<NormalizedAuthority>,
    sensitive_values: Vec<SensitiveString>,
}

impl fmt::Debug for CredentialSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialSet")
            .field("request_header_count", &self.request_headers.len())
            .field("authority_count", &self.authorities.len())
            .field("has_origin", &self.origin.is_some())
            .field("has_referer", &self.referer.is_some())
            .finish()
    }
}

impl CredentialSet {
    pub fn from_descriptor(descriptor: &SourceDescriptor) -> Result<Self> {
        let initial =
            Url::parse(descriptor.url.expose()).map_err(|_| UpstreamErrorCode::InvalidUrl)?;
        let initial_host = initial
            .host_str()
            .ok_or(UpstreamErrorCode::HostForbidden)?
            .trim_end_matches('.')
            .to_ascii_lowercase();
        let initial_path = initial.path();
        let mut request_headers = Vec::with_capacity(descriptor.request_headers.len());
        let mut aggregate = 0usize;
        if descriptor.request_headers.len() > MAX_PROVIDER_HEADERS {
            return Err(UpstreamErrorCode::HeaderRejected.into());
        }
        for (name, value) in &descriptor.request_headers {
            let normalized = name.to_ascii_lowercase();
            if forbidden_provider_header(&normalized)
                || value.expose().len() > 4_096
                || HeaderValue::from_str(value.expose()).is_err()
            {
                return Err(UpstreamErrorCode::HeaderRejected.into());
            }
            aggregate = aggregate
                .saturating_add(normalized.len())
                .saturating_add(value.expose().len());
            if aggregate > MAX_PROVIDER_HEADER_BYTES {
                return Err(UpstreamErrorCode::HeaderRejected.into());
            }
            request_headers.push((
                HeaderName::from_bytes(normalized.as_bytes())
                    .map_err(|_| UpstreamErrorCode::HeaderRejected)?,
                value.clone(),
            ));
        }
        let authorities = descriptor
            .credential_authorities
            .iter()
            .map(NormalizedAuthority::try_from)
            .collect::<Result<Vec<_>>>()?;
        if authorities.len() > 32 {
            return Err(UpstreamErrorCode::HeaderRejected.into());
        }
        let mut unique_authorities = BTreeSet::new();
        if authorities.iter().any(|authority| {
            !unique_authorities.insert((
                authority.scheme.clone(),
                authority.host.clone(),
                authority.port,
            ))
        }) {
            return Err(UpstreamErrorCode::HeaderRejected.into());
        }
        let cookies = CookieJar::from_provider(&descriptor.cookies, &initial_host, initial_path)?;
        let mut sensitive_values = descriptor
            .request_headers
            .values()
            .cloned()
            .chain(descriptor.cookies.iter().map(|cookie| cookie.value.clone()))
            .collect::<Vec<_>>();
        if let Some(value) = &descriptor.origin {
            validate_sensitive_header(value)?;
            sensitive_values.push(value.clone());
        }
        if let Some(value) = &descriptor.referer {
            validate_sensitive_header(value)?;
            sensitive_values.push(value.clone());
        }
        if let Some(value) = &descriptor.refresh_handle {
            sensitive_values.push(value.clone());
        }
        sensitive_values.push(descriptor.url.clone());
        Ok(Self {
            request_headers,
            cookies: Mutex::new(cookies),
            origin: descriptor.origin.clone(),
            referer: descriptor.referer.clone(),
            authorities,
            sensitive_values,
        })
    }

    pub(crate) fn apply(&self, target: &ValidatedUrl, output: &mut HeaderMap) -> Result<()> {
        let Some(authority) = self.authorities.iter().find(|value| value.matches(target)) else {
            return Ok(());
        };
        if authority.send_request_headers {
            for (name, value) in &self.request_headers {
                output.insert(
                    name.clone(),
                    HeaderValue::from_str(value.expose())
                        .map_err(|_| UpstreamErrorCode::HeaderRejected)?,
                );
            }
        }
        if authority.send_cookies {
            let mut jar = self.cookie_jar()?;
            if let Some(value) = jar.header_value(target) {
                output.insert(
                    COOKIE,
                    HeaderValue::from_str(&value).map_err(|_| UpstreamErrorCode::CookieRejected)?,
                );
            }
        }
        if authority.send_origin
            && let Some(value) = &self.origin
        {
            output.insert(
                ORIGIN,
                HeaderValue::from_str(value.expose())
                    .map_err(|_| UpstreamErrorCode::HeaderRejected)?,
            );
        }
        if authority.send_referer
            && let Some(value) = &self.referer
        {
            output.insert(
                REFERER,
                HeaderValue::from_str(value.expose())
                    .map_err(|_| UpstreamErrorCode::HeaderRejected)?,
            );
        }
        Ok(())
    }

    pub(crate) fn ingest_response(&self, target: &ValidatedUrl, headers: &HeaderMap) -> Result<()> {
        let permitted = self
            .authorities
            .iter()
            .any(|authority| authority.matches(target) && authority.send_cookies);
        if !permitted {
            return Ok(());
        }
        let values = headers.get_all(SET_COOKIE);
        let mut jar = self.cookie_jar()?;
        for value in values {
            let value = value
                .to_str()
                .map_err(|_| UpstreamErrorCode::CookieRejected)?;
            jar.ingest(target, value)?;
        }
        Ok(())
    }

    pub(crate) fn value_is_sensitive(&self, value: &str) -> bool {
        value.contains("ELIXIR_LIVE_CANARY_")
            || self
                .sensitive_values
                .iter()
                .any(|secret| secret.expose().len() >= 8 && value.contains(secret.expose()))
            || self
                .cookies
                .lock()
                .map(|jar| jar.contains_secret(value))
                .unwrap_or(true)
    }

    fn cookie_jar(&self) -> Result<MutexGuard<'_, CookieJar>> {
        self.cookies
            .lock()
            .map_err(|_| UpstreamErrorCode::CookieRejected.into())
    }
}

#[derive(Clone)]
struct NormalizedAuthority {
    scheme: String,
    host: String,
    port: u16,
    send_request_headers: bool,
    send_cookies: bool,
    send_origin: bool,
    send_referer: bool,
}

impl NormalizedAuthority {
    fn matches(&self, target: &ValidatedUrl) -> bool {
        target.authority_matches(&self.scheme, &self.host, self.port)
    }
}

impl TryFrom<&CredentialAuthority> for NormalizedAuthority {
    type Error = super::error::UpstreamError;

    fn try_from(value: &CredentialAuthority) -> Result<Self> {
        if !matches!(value.scheme.as_str(), "http" | "https") || value.port == 0 {
            return Err(UpstreamErrorCode::HeaderRejected.into());
        }
        let host = value.host.trim_end_matches('.').to_ascii_lowercase();
        if host.is_empty() || host.len() > 253 || host.chars().any(char::is_control) {
            return Err(UpstreamErrorCode::HeaderRejected.into());
        }
        Ok(Self {
            scheme: value.scheme.clone(),
            host,
            port: value.port,
            send_request_headers: value.send_request_headers,
            send_cookies: value.send_cookies,
            send_origin: value.send_origin,
            send_referer: value.send_referer,
        })
    }
}

#[derive(Clone)]
struct StoredCookie {
    name: String,
    value: SensitiveString,
    domain: String,
    path: String,
    host_only: bool,
    secure: bool,
    expires_at: Option<SystemTime>,
    sequence: u64,
}

#[derive(Clone, Default)]
struct CookieJar {
    values: Vec<StoredCookie>,
    next_sequence: u64,
}

impl CookieJar {
    fn from_provider(values: &[ProviderCookie], host: &str, request_path: &str) -> Result<Self> {
        if values.len() > MAX_COOKIES {
            return Err(UpstreamErrorCode::CookieRejected.into());
        }
        let mut jar = Self::default();
        for value in values {
            let domain = value
                .domain
                .as_deref()
                .map(|domain| domain.trim_start_matches('.').to_ascii_lowercase())
                .unwrap_or_else(|| host.to_string());
            let host_only = value.domain.is_none();
            if !valid_cookie_name(&value.name)
                || !valid_cookie_value(value.value.expose())
                || !domain_matches(host, &domain)
            {
                return Err(UpstreamErrorCode::CookieRejected.into());
            }
            let path = value
                .path
                .clone()
                .unwrap_or_else(|| default_cookie_path(request_path));
            if !valid_cookie_path(&path) {
                return Err(UpstreamErrorCode::CookieRejected.into());
            }
            let expires_at = value.expires_at.map(|expires| {
                u64::try_from(expires.timestamp()).map_or(SystemTime::UNIX_EPOCH, |seconds| {
                    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
                })
            });
            let sequence = jar.next_sequence();
            jar.insert(StoredCookie {
                name: value.name.clone(),
                value: value.value.clone(),
                domain,
                path,
                host_only,
                secure: value.secure,
                expires_at,
                sequence,
            })?;
        }
        Ok(jar)
    }

    fn header_value(&mut self, target: &ValidatedUrl) -> Option<String> {
        let now = SystemTime::now();
        self.values
            .retain(|cookie| cookie.expires_at.is_none_or(|expires| expires > now));
        let mut selected = self
            .values
            .iter()
            .filter(|cookie| {
                (!cookie.secure || target.scheme == "https")
                    && if cookie.host_only {
                        target.host == cookie.domain
                    } else {
                        domain_matches(&target.host, &cookie.domain)
                    }
                    && path_matches(target.url.path(), &cookie.path)
            })
            .collect::<Vec<_>>();
        selected.sort_by_key(|cookie| (std::cmp::Reverse(cookie.path.len()), cookie.sequence));
        if selected.is_empty() {
            return None;
        }
        Some(
            selected
                .into_iter()
                .map(|cookie| format!("{}={}", cookie.name, cookie.value.expose()))
                .collect::<Vec<_>>()
                .join("; "),
        )
    }

    fn ingest(&mut self, target: &ValidatedUrl, raw: &str) -> Result<()> {
        if raw.is_empty()
            || raw.len() > 4_096
            || raw.chars().any(|value| value == '\r' || value == '\n')
        {
            return Err(UpstreamErrorCode::CookieRejected.into());
        }
        let mut parts = raw.split(';');
        let first = parts.next().ok_or(UpstreamErrorCode::CookieRejected)?;
        let (name, value) = first
            .split_once('=')
            .ok_or(UpstreamErrorCode::CookieRejected)?;
        let name = name.trim();
        let value = value.trim();
        if !valid_cookie_name(name) || !valid_cookie_value(value) {
            return Err(UpstreamErrorCode::CookieRejected.into());
        }
        let mut domain = target.host.clone();
        let mut host_only = true;
        let mut path = default_cookie_path(target.url.path());
        let mut secure = false;
        let mut expires_at = None;
        let mut remove = false;
        for attribute in parts {
            let attribute = attribute.trim();
            let (name, value) = attribute
                .split_once('=')
                .map_or((attribute, None), |(name, value)| {
                    (name, Some(value.trim()))
                });
            match name.trim().to_ascii_lowercase().as_str() {
                "domain" => {
                    let candidate = value
                        .ok_or(UpstreamErrorCode::CookieRejected)?
                        .trim_start_matches('.')
                        .to_ascii_lowercase();
                    if candidate.parse::<IpAddr>().is_ok()
                        || !domain_matches(&target.host, &candidate)
                    {
                        return Err(UpstreamErrorCode::CookieRejected.into());
                    }
                    domain = candidate;
                    host_only = false;
                }
                "path" => {
                    let candidate = value.ok_or(UpstreamErrorCode::CookieRejected)?;
                    if !valid_cookie_path(candidate) {
                        return Err(UpstreamErrorCode::CookieRejected.into());
                    }
                    path = candidate.to_string();
                }
                "secure" => secure = true,
                "max-age" => {
                    let seconds = value
                        .ok_or(UpstreamErrorCode::CookieRejected)?
                        .parse::<i64>()
                        .map_err(|_| UpstreamErrorCode::CookieRejected)?;
                    if seconds <= 0 {
                        remove = true;
                    } else {
                        expires_at = SystemTime::now().checked_add(Duration::from_secs(
                            u64::try_from(seconds).unwrap_or(u64::MAX),
                        ));
                    }
                }
                "expires" => {
                    if let Some(value) = value {
                        expires_at = httpdate::parse_http_date(value).ok();
                    }
                }
                "httponly" | "samesite" | "priority" | "partitioned" => {}
                _ => {}
            }
        }
        if secure && target.scheme != "https" {
            return Err(UpstreamErrorCode::CookieRejected.into());
        }
        if name.starts_with("__Secure-") && !secure
            || name.starts_with("__Host-") && (!secure || !host_only || path != "/")
        {
            return Err(UpstreamErrorCode::CookieRejected.into());
        }
        self.values.retain(|cookie| {
            !(cookie.name == name && cookie.domain == domain && cookie.path == path)
        });
        if remove || expires_at.is_some_and(|expires| expires <= SystemTime::now()) {
            return Ok(());
        }
        let sequence = self.next_sequence();
        self.insert(StoredCookie {
            name: name.to_string(),
            value: SensitiveString::new(value),
            domain,
            path,
            host_only,
            secure,
            expires_at,
            sequence,
        })
    }

    fn insert(&mut self, cookie: StoredCookie) -> Result<()> {
        let mut candidate = self.clone();
        candidate.values.retain(|existing| {
            !(existing.name == cookie.name
                && existing.domain == cookie.domain
                && existing.path == cookie.path)
        });
        candidate.values.push(cookie);
        let bytes = candidate.values.iter().fold(0usize, |total, value| {
            total
                .saturating_add(value.name.len())
                .saturating_add(value.value.expose().len())
                .saturating_add(value.domain.len())
                .saturating_add(value.path.len())
        });
        if candidate.values.len() > MAX_COOKIES || bytes > MAX_COOKIE_BYTES {
            return Err(UpstreamErrorCode::CookieRejected.into());
        }
        *self = candidate;
        Ok(())
    }

    fn next_sequence(&mut self) -> u64 {
        let current = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        current
    }

    fn contains_secret(&self, value: &str) -> bool {
        self.values
            .iter()
            .any(|cookie| cookie.value.expose().len() >= 8 && value.contains(cookie.value.expose()))
    }
}

fn forbidden_provider_header(name: &str) -> bool {
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
                | "user-agent"
                | "accept-encoding"
                | "accept"
                | "accept-language"
                | "range"
        )
}

fn validate_sensitive_header(value: &SensitiveString) -> Result<()> {
    if value.expose().is_empty()
        || value.expose().len() > 2_048
        || HeaderValue::from_str(value.expose()).is_err()
    {
        return Err(UpstreamErrorCode::HeaderRejected.into());
    }
    let parsed = Url::parse(value.expose()).map_err(|_| UpstreamErrorCode::HeaderRejected)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(UpstreamErrorCode::HeaderRejected.into());
    }
    Ok(())
}

fn valid_range(value: &str) -> bool {
    let Some(range) = value.strip_prefix("bytes=") else {
        return false;
    };
    if range.contains(',') {
        return false;
    }
    let Some((start, end)) = range.split_once('-') else {
        return false;
    };
    (!start.is_empty() || !end.is_empty())
        && start.bytes().all(|value| value.is_ascii_digit())
        && end.bytes().all(|value| value.is_ascii_digit())
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

fn valid_cookie_value(value: &str) -> bool {
    value.len() <= 4_096
        && value
            .bytes()
            .all(|byte| byte >= 0x20 && byte < 0x7f && byte != b';')
}

fn valid_cookie_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && value.starts_with('/')
        && !value.chars().any(char::is_control)
}

fn domain_matches(host: &str, domain: &str) -> bool {
    host == domain
        || (host.len() > domain.len()
            && host.ends_with(domain)
            && host.as_bytes()[host.len() - domain.len() - 1] == b'.')
}

fn default_cookie_path(request_path: &str) -> String {
    if !request_path.starts_with('/') || request_path == "/" {
        return "/".to_string();
    }
    request_path
        .rfind('/')
        .filter(|index| *index > 0)
        .map_or_else(
            || "/".to_string(),
            |index| request_path[..index].to_string(),
        )
}

fn path_matches(request_path: &str, cookie_path: &str) -> bool {
    request_path == cookie_path
        || (request_path.starts_with(cookie_path)
            && (cookie_path.ends_with('/')
                || request_path.as_bytes().get(cookie_path.len()) == Some(&b'/')))
}
