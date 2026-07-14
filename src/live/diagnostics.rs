//! Central redaction and sensitive-data scanning for Live diagnostics.

use std::{collections::BTreeSet, error::Error, fmt, sync::OnceLock};

use regex::Regex;
use serde_json::Value;
use zeroize::Zeroizing;

const REDACTED_CANARY: &str = "[REDACTED_CANARY]";
const REDACTED_TOKEN: &str = "[REDACTED_LIVE_TOKEN]";
const REDACTED_URL: &str = "[REDACTED_URL]";
const REDACTED_AUTHORIZATION: &str = "[REDACTED_AUTHORIZATION]";
const REDACTED_CREDENTIAL: &str = "[REDACTED_CREDENTIAL]";
const REDACTED_INPUT: &str = "[REDACTED_INPUT]";
const REDACTED_QUERY: &str = "[REDACTED_QUERY_VALUE]";
const TRUNCATED: &str = "[TRUNCATED]";
const MIN_CANARY_BYTES: usize = 8;
const MAX_CANARY_BYTES: usize = 4_096;
const MAX_CANARIES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SensitiveCategory {
    ExactCanary,
    LiveToken,
    Authorization,
    Credential,
    Url,
    QueryMaterial,
    FfmpegInput,
}

impl SensitiveCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactCanary => "exact_canary",
            Self::LiveToken => "live_token",
            Self::Authorization => "authorization",
            Self::Credential => "credential",
            Self::Url => "url",
            Self::QueryMaterial => "query_material",
            Self::FfmpegInput => "ffmpeg_input",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveDataReport {
    categories: BTreeSet<SensitiveCategory>,
}

impl SensitiveDataReport {
    pub fn is_clean(&self) -> bool {
        self.categories.is_empty()
    }

    pub fn categories(&self) -> impl Iterator<Item = SensitiveCategory> + '_ {
        self.categories.iter().copied()
    }
}

impl fmt::Debug for SensitiveDataReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveDataReport")
            .field(
                "categories",
                &self
                    .categories
                    .iter()
                    .map(|category| category.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveDataDetected {
    report: SensitiveDataReport,
}

impl SensitiveDataDetected {
    pub fn report(&self) -> &SensitiveDataReport {
        &self.report
    }
}

impl fmt::Debug for SensitiveDataDetected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveDataDetected")
            .field("report", &self.report)
            .finish()
    }
}

impl fmt::Display for SensitiveDataDetected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let categories = self
            .report
            .categories
            .iter()
            .map(|category| category.as_str())
            .collect::<Vec<_>>()
            .join(",");
        write!(
            formatter,
            "Live diagnostic contains sensitive data categories: {categories}"
        )
    }
}

impl Error for SensitiveDataDetected {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedactionConfigError {
    InvalidCanary,
    TooManyCanaries,
}

impl fmt::Display for RedactionConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCanary => formatter.write_str("invalid sensitive-data canary"),
            Self::TooManyCanaries => formatter.write_str("too many sensitive-data canaries"),
        }
    }
}

impl Error for RedactionConfigError {}

#[derive(Clone, PartialEq, Eq)]
pub struct RedactedText(String);

impl RedactedText {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Debug for RedactedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RedactedText")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for RedactedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub struct LiveRedactor {
    exact_canaries: Vec<Zeroizing<String>>,
}

impl fmt::Debug for LiveRedactor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRedactor")
            .field("exact_canary_count", &self.exact_canaries.len())
            .finish()
    }
}

impl Default for LiveRedactor {
    fn default() -> Self {
        Self {
            exact_canaries: Vec::new(),
        }
    }
}

impl LiveRedactor {
    pub fn with_canaries(
        canaries: impl IntoIterator<Item = String>,
    ) -> Result<Self, RedactionConfigError> {
        let mut exact_canaries = Vec::new();
        for canary in canaries {
            if !(MIN_CANARY_BYTES..=MAX_CANARY_BYTES).contains(&canary.len())
                || canary.chars().any(char::is_control)
                || canary.contains("[REDACTED_")
            {
                return Err(RedactionConfigError::InvalidCanary);
            }
            if exact_canaries.len() >= MAX_CANARIES {
                return Err(RedactionConfigError::TooManyCanaries);
            }
            if exact_canaries
                .iter()
                .any(|existing: &Zeroizing<String>| existing.as_str() == canary)
            {
                continue;
            }
            exact_canaries.push(Zeroizing::new(canary));
        }
        exact_canaries.sort_by_key(|value| std::cmp::Reverse(value.len()));
        Ok(Self { exact_canaries })
    }

    pub fn redact(&self, input: &str) -> RedactedText {
        let mut output = self.redact_exact_canaries(input.to_string());
        output = live_token_pattern()
            .replace_all(&output, REDACTED_TOKEN)
            .into_owned();
        output = authorization_pattern()
            .replace_all(&output, REDACTED_AUTHORIZATION)
            .into_owned();
        output = credential_pattern()
            .replace_all(&output, format!("${{1}}={REDACTED_CREDENTIAL}"))
            .into_owned();
        output = ffmpeg_input_pattern()
            .replace_all(&output, format!("${{1}}-i {REDACTED_INPUT}"))
            .into_owned();
        output = url_pattern()
            .replace_all(&output, REDACTED_URL)
            .into_owned();
        output = query_material_pattern()
            .replace_all(&output, format!("${{1}}{REDACTED_QUERY}"))
            .into_owned();
        RedactedText(output)
    }

    pub fn redact_bounded(&self, input: &str, max_bytes: usize) -> RedactedText {
        let redacted = self.redact(input).into_string();
        if redacted.len() <= max_bytes {
            return RedactedText(redacted);
        }
        if max_bytes == 0 {
            return RedactedText(String::new());
        }
        if max_bytes <= TRUNCATED.len() {
            return RedactedText(TRUNCATED[..max_bytes].to_string());
        }
        let mut prefix_bytes = max_bytes - TRUNCATED.len();
        while prefix_bytes > 0 && !redacted.is_char_boundary(prefix_bytes) {
            prefix_bytes -= 1;
        }
        let mut output = String::with_capacity(max_bytes);
        output.push_str(&redacted[..prefix_bytes]);
        output.push_str(TRUNCATED);
        RedactedText(output)
    }

    pub fn redact_json(&self, value: &Value) -> Value {
        let mut redacted = value.clone();
        self.redact_json_in_place(&mut redacted);
        redacted
    }

    pub fn redact_json_in_place(&self, value: &mut Value) {
        match value {
            Value::Object(object) => {
                for (key, child) in object {
                    if sensitive_json_key(key) {
                        *child = Value::String(REDACTED_CREDENTIAL.to_string());
                    } else {
                        self.redact_json_in_place(child);
                    }
                }
            }
            Value::Array(values) => {
                for child in values {
                    self.redact_json_in_place(child);
                }
            }
            Value::String(text) => {
                *text = self.redact(text).into_string();
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    pub fn scan(&self, input: &str) -> SensitiveDataReport {
        let mut categories = BTreeSet::new();
        if self
            .exact_canaries
            .iter()
            .any(|canary| input.contains(canary.as_str()))
        {
            categories.insert(SensitiveCategory::ExactCanary);
        }
        record_if_replacement_changes(
            &mut categories,
            SensitiveCategory::LiveToken,
            input,
            live_token_pattern(),
            REDACTED_TOKEN,
        );
        record_if_replacement_changes(
            &mut categories,
            SensitiveCategory::Authorization,
            input,
            authorization_pattern(),
            REDACTED_AUTHORIZATION,
        );
        record_if_replacement_changes(
            &mut categories,
            SensitiveCategory::Credential,
            input,
            credential_pattern(),
            &format!("${{1}}={REDACTED_CREDENTIAL}"),
        );
        record_if_replacement_changes(
            &mut categories,
            SensitiveCategory::FfmpegInput,
            input,
            ffmpeg_input_pattern(),
            &format!("${{1}}-i {REDACTED_INPUT}"),
        );
        record_if_replacement_changes(
            &mut categories,
            SensitiveCategory::Url,
            input,
            url_pattern(),
            REDACTED_URL,
        );
        record_if_replacement_changes(
            &mut categories,
            SensitiveCategory::QueryMaterial,
            input,
            query_material_pattern(),
            &format!("${{1}}{REDACTED_QUERY}"),
        );
        SensitiveDataReport { categories }
    }

    pub fn scan_json(&self, value: &Value) -> SensitiveDataReport {
        let mut categories = BTreeSet::new();
        self.scan_json_into(value, &mut categories);
        SensitiveDataReport { categories }
    }

    pub fn assert_clean(&self, input: &str) -> Result<(), SensitiveDataDetected> {
        let report = self.scan(input);
        if report.is_clean() {
            Ok(())
        } else {
            Err(SensitiveDataDetected { report })
        }
    }

    fn redact_exact_canaries(&self, mut value: String) -> String {
        for canary in &self.exact_canaries {
            value = value.replace(canary.as_str(), REDACTED_CANARY);
        }
        value
    }

    fn scan_json_into(&self, value: &Value, categories: &mut BTreeSet<SensitiveCategory>) {
        match value {
            Value::Object(object) => {
                for (key, child) in object {
                    if sensitive_json_key(key) {
                        if child.as_str() != Some(REDACTED_CREDENTIAL) {
                            categories.insert(SensitiveCategory::Credential);
                        }
                    } else {
                        self.scan_json_into(child, categories);
                    }
                }
            }
            Value::Array(values) => {
                for child in values {
                    self.scan_json_into(child, categories);
                }
            }
            Value::String(text) => categories.extend(self.scan(text).categories()),
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

fn record_if_replacement_changes(
    categories: &mut BTreeSet<SensitiveCategory>,
    category: SensitiveCategory,
    input: &str,
    pattern: &Regex,
    replacement: &str,
) {
    if pattern.is_match(input) && pattern.replace_all(input, replacement).as_ref() != input {
        categories.insert(category);
    }
}

fn sensitive_json_key(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    matches!(
        normalized.as_str(),
        "authorization"
            | "proxyauthorization"
            | "cookie"
            | "cookies"
            | "setcookie"
            | "apikey"
            | "xapikey"
            | "accesstoken"
            | "refreshtoken"
            | "refreshhandle"
            | "token"
            | "password"
            | "secret"
            | "clientsecret"
            | "input"
            | "ffmpeginput"
            | "query"
            | "querystring"
            | "fragment"
    ) || normalized.ends_with("url")
        || normalized.ends_with("uri")
        || normalized.ends_with("headers")
}

fn live_token_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"elx_(?:live|refresh)_v1_[A-Za-z0-9_-]{43}")
            .expect("static Live token redaction regex")
    })
}

fn authorization_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)\b(?:authorization\s*[:=]\s*)?bearer\s+[A-Za-z0-9._~+/=-]{8,}")
            .expect("static authorization redaction regex")
    })
}

fn credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)\b(authorization|proxy-authorization|cookie|set-cookie|headers|request[_-]?headers|response[_-]?headers|x-api-key|api[_-]?key|access[_-]?token|refresh[_-]?(?:token|handle)|token|password|client[_-]?secret|secret)\b\s*[:=]\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\r\n,]+)"#,
        )
        .expect("static credential redaction regex")
    })
}

fn ffmpeg_input_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?i)(^|\s)-i\s+(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s]+)"#)
            .expect("static ffmpeg input redaction regex")
    })
}

fn url_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?i)\b[a-z][a-z0-9+.-]{1,31}://[^\s"'<>]+"#)
            .expect("static URL redaction regex")
    })
}

fn query_material_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)([?&#](?:access_token|token|authorization|auth|api[_-]?key|signature|sig|key)=)[^&#\s]+",
        )
        .expect("static query-material redaction regex")
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::live::crypto::LiveDeliveryToken;

    use super::*;

    #[test]
    fn k10_redactor_removes_tokens_urls_headers_handles_inputs_and_exact_canaries() {
        let canary = "provider-canary-8f2760b4".to_string();
        let redactor = LiveRedactor::with_canaries([canary.clone()]).unwrap();
        let token = LiveDeliveryToken::generate().unwrap();
        let input = format!(
            "Authorization: Bearer bearer-secret-value\nCookie: sid=cookie-secret; theme=dark\nrefresh_handle=opaque-refresh-value\nurl=https://origin.invalid/live.m3u8?token=query-secret\nrejected=gopher://metadata.invalid/private\nffmpeg -i 'srt://origin.invalid:9000?passphrase=secret' -c copy\ntoken={}\ncanary={canary}",
            token.expose_secret()
        );
        let report = redactor.scan(&input);
        for category in [
            SensitiveCategory::ExactCanary,
            SensitiveCategory::LiveToken,
            SensitiveCategory::Authorization,
            SensitiveCategory::Credential,
            SensitiveCategory::Url,
            SensitiveCategory::QueryMaterial,
            SensitiveCategory::FfmpegInput,
        ] {
            assert!(report.categories().any(|found| found == category));
        }
        let redacted = redactor.redact(&input);
        for secret in [
            canary.as_str(),
            token.expose_secret(),
            "bearer-secret-value",
            "cookie-secret",
            "opaque-refresh-value",
            "query-secret",
            "passphrase=secret",
        ] {
            assert!(!redacted.as_str().contains(secret));
        }
        assert!(!redacted.as_str().contains("https://"));
        assert!(!redacted.as_str().contains("gopher://"));
        assert!(!redacted.as_str().contains("srt://"));
        redactor.assert_clean(redacted.as_str()).unwrap();
    }

    #[test]
    fn k10_redactor_recursively_sanitizes_json_without_mutating_safe_fields() {
        let canary = "json-canary-cf58c928".to_string();
        let redactor = LiveRedactor::with_canaries([canary.clone()]).unwrap();
        let input = json!({
            "provider_id": "provider-1",
            "streamUrl": "https://origin.invalid/live?key=secret",
            "headers": {
                "Authorization": "Bearer nested-secret",
                "User-Agent": "Elixir"
            },
            "refreshHandle": "opaque-handle",
            "events": ["safe", format!("value={canary}")],
            "token_revision": 7
        });
        assert!(!redactor.scan_json(&input).is_clean());
        let output = redactor.redact_json(&input);
        assert_eq!(output["provider_id"], "provider-1");
        assert_eq!(output["token_revision"], 7);
        assert_eq!(output["streamUrl"], REDACTED_CREDENTIAL);
        assert_eq!(output["headers"], REDACTED_CREDENTIAL);
        assert_eq!(output["refreshHandle"], REDACTED_CREDENTIAL);
        assert!(!output.to_string().contains(&canary));
        assert!(redactor.scan_json(&output).is_clean());
    }

    #[test]
    fn k10_redactor_bounds_utf8_output_and_is_idempotent() {
        let redactor = LiveRedactor::default();
        let input = "status=failed source=https://origin.invalid/path?token=secret detail=\
                     repeated-repeated-repeated-repeated-repeated \u{1f680}";
        let once = redactor.redact(input);
        let twice = redactor.redact(once.as_str());
        assert_eq!(once, twice);
        let bounded = redactor.redact_bounded(input, 24);
        assert!(bounded.as_str().is_char_boundary(bounded.as_str().len()));
        assert!(bounded.as_str().len() <= 48);
        assert!(bounded.as_str().ends_with(TRUNCATED));
        redactor.assert_clean(bounded.as_str()).unwrap();
    }

    #[test]
    fn k10_scanner_errors_report_categories_without_echoing_secrets() {
        let canary = "error-canary-97d64fe8".to_string();
        let redactor = LiveRedactor::with_canaries([canary.clone()]).unwrap();
        let error = redactor
            .assert_clean(&format!("diagnostic contains {canary}"))
            .unwrap_err();
        assert!(
            error
                .report()
                .categories()
                .any(|category| category == SensitiveCategory::ExactCanary)
        );
        assert!(!error.to_string().contains(&canary));
        assert!(!format!("{error:?}").contains(&canary));
    }

    #[test]
    fn k10_redactor_rejects_unsafe_or_unbounded_canary_configuration() {
        assert_eq!(
            LiveRedactor::with_canaries(["short".to_string()]).unwrap_err(),
            RedactionConfigError::InvalidCanary
        );
        let too_many = (0..=MAX_CANARIES)
            .map(|index| format!("canary-value-{index:04}"))
            .collect::<Vec<_>>();
        assert_eq!(
            LiveRedactor::with_canaries(too_many).unwrap_err(),
            RedactionConfigError::TooManyCanaries
        );
    }
}
