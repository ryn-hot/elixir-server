use reqwest::Url;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{db::models::MediaType, http::handlers::acquisition_sources::AcquisitionCandidate};

const SIZE_BUCKET_BYTES: u64 = 100 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ReleaseFingerprintInput<'a> {
    pub source_kind: &'a str,
    pub source: &'a str,
    pub info_hash: Option<&'a str>,
    pub release_title: &'a str,
    pub size_bytes: Option<u64>,
    pub source_provider_id: Option<Uuid>,
}

pub fn candidate_release_fingerprint(
    candidate: &AcquisitionCandidate,
    source_provider_id: Option<Uuid>,
) -> String {
    build_release_fingerprint(&ReleaseFingerprintInput {
        source_kind: &candidate.source_kind,
        source: &candidate.source,
        info_hash: candidate.info_hash.as_deref(),
        release_title: &candidate.title,
        size_bytes: candidate.size_bytes,
        source_provider_id,
    })
}

#[derive(Debug, Clone)]
pub struct ReviewCandidateFingerprintInput<'a> {
    pub candidate: &'a AcquisitionCandidate,
    pub source_provider_id: Option<Uuid>,
    pub subscription_id: Option<Uuid>,
    pub media_type: MediaType,
}

pub fn review_candidate_release_fingerprint(input: &ReviewCandidateFingerprintInput<'_>) -> String {
    let candidate_fingerprint =
        candidate_release_fingerprint(input.candidate, input.source_provider_id);
    let provider = input
        .source_provider_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown-provider".to_string());
    let subscription = input
        .subscription_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown-subscription".to_string());
    let scope = format!(
        "provider:{provider}:subscription:{subscription}:media:{}",
        input.media_type.as_str()
    );
    format!(
        "review:v1:{}:{}:{}",
        input.media_type.as_str(),
        short_hash(&scope),
        candidate_fingerprint
    )
}

pub fn build_release_fingerprint(input: &ReleaseFingerprintInput<'_>) -> String {
    let source_kind = normalize_source_kind(input.source_kind);
    let title = normalize_release_title(input.release_title);
    let title_hash = short_hash(&title);
    let size_bucket = size_bucket(input.size_bytes);
    let identity = release_identity(input);
    let identity_hash = short_hash(&identity);
    format!("v1:{source_kind}:{identity_hash}:{title_hash}:{size_bucket}")
}

fn release_identity(input: &ReleaseFingerprintInput<'_>) -> String {
    if let Some(info_hash) = input.info_hash.and_then(normalize_info_hash) {
        return format!("btih:{info_hash}");
    }
    if let Some(info_hash) = extract_magnet_info_hash(input.source) {
        return format!("btih:{info_hash}");
    }
    if let Some(url) = normalize_source_url(input.source) {
        return format!("url:{url}");
    }
    let provider = input
        .source_provider_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown-provider".to_string());
    format!(
        "provider:{provider}:title:{}:size:{}",
        normalize_release_title(input.release_title),
        size_bucket(input.size_bytes)
    )
}

pub fn normalize_info_hash(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let stripped = trimmed
        .strip_prefix("urn:btih:")
        .or_else(|| trimmed.strip_prefix("URN:BTIH:"))
        .unwrap_or(trimmed)
        .trim();
    if stripped.is_empty() {
        return None;
    }
    let normalized = stripped
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    if normalized.len() == 40 || normalized.len() == 32 {
        Some(normalized)
    } else {
        None
    }
}

pub fn extract_magnet_info_hash(source: &str) -> Option<String> {
    let trimmed = source.trim();
    if !trimmed
        .get(..7)
        .map(|prefix| prefix.eq_ignore_ascii_case("magnet:"))
        .unwrap_or(false)
    {
        return None;
    }
    let query = trimmed.split_once('?')?.1;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key.eq_ignore_ascii_case("xt") {
            let decoded = urlencoding::decode(value).ok()?;
            if let Some(info_hash) = normalize_info_hash(&decoded) {
                return Some(info_hash);
            }
        }
    }
    None
}

pub fn normalize_source_url(source: &str) -> Option<String> {
    let mut url = Url::parse(source.trim()).ok()?;
    match url.scheme() {
        "http" | "https" => {}
        _ => return None,
    }
    url.set_fragment(None);
    url.set_username("").ok()?;
    url.set_password(None).ok()?;

    let mut pairs = url
        .query_pairs()
        .filter(|(key, _)| !is_sensitive_query_key(key))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<Vec<_>>();
    pairs.sort();
    url.set_query(None);
    if !pairs.is_empty() {
        let query = pairs
            .into_iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&query));
    }

    Some(url.to_string())
}

pub fn normalize_release_title(value: &str) -> String {
    let mut normalized = String::new();
    let mut last_was_space = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_was_space = false;
        } else if !last_was_space {
            normalized.push(' ');
            last_was_space = true;
        }
    }
    normalized.trim().to_string()
}

fn normalize_source_kind(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

fn size_bucket(size_bytes: Option<u64>) -> u64 {
    size_bytes
        .map(|bytes| bytes / SIZE_BUCKET_BYTES)
        .unwrap_or_default()
}

fn short_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded[..24].to_string()
}

fn is_sensitive_query_key(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "access_token"
            | "apikey"
            | "api_key"
            | "auth"
            | "auth_token"
            | "expires"
            | "exp"
            | "key"
            | "passkey"
            | "session"
            | "sid"
            | "sig"
            | "signature"
            | "token"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magnet_tracker_order_does_not_change_fingerprint() {
        let first = build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: "magnet",
            source: "magnet:?xt=urn:btih:0123456789ABCDEF0123456789ABCDEF01234567&tr=udp://one&tr=udp://two",
            info_hash: None,
            release_title: "Show S01 Complete",
            size_bytes: Some(10 * 1024 * 1024 * 1024),
            source_provider_id: None,
        });
        let second = build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: "magnet",
            source: "magnet:?tr=udp://two&tr=udp://one&xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            info_hash: None,
            release_title: "show.s01.complete",
            size_bytes: Some(10 * 1024 * 1024 * 1024),
            source_provider_id: None,
        });

        assert_eq!(first, second);
    }

    #[test]
    fn source_url_fingerprint_redacts_sensitive_query_params() {
        let first = build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: "hoster",
            source: "https://files.example/video.mkv?token=secret&quality=1080&x=1",
            info_hash: None,
            release_title: "Example",
            size_bytes: Some(1024),
            source_provider_id: None,
        });
        let second = build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: "hoster",
            source: "https://files.example/video.mkv?x=1&quality=1080&token=different",
            info_hash: None,
            release_title: "Example",
            size_bytes: Some(1024),
            source_provider_id: None,
        });

        assert_eq!(first, second);
        assert_eq!(
            normalize_source_url("https://files.example/video.mkv?token=secret&quality=1080")
                .as_deref(),
            Some("https://files.example/video.mkv?quality=1080")
        );
    }

    #[test]
    fn provider_title_fallback_is_stable() {
        let provider_id = Uuid::new_v4();
        let first = build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: "unknown",
            source: "not a url",
            info_hash: None,
            release_title: "Release Title",
            size_bytes: Some(123),
            source_provider_id: Some(provider_id),
        });
        let second = build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: "unknown",
            source: "not a url",
            info_hash: None,
            release_title: "release.title",
            size_bytes: Some(123),
            source_provider_id: Some(provider_id),
        });

        assert_eq!(first, second);
    }

    #[test]
    fn candidate_fingerprint_uses_candidate_info_hash() {
        let candidate = AcquisitionCandidate {
            id: None,
            title: "Show.S01.COMPLETE.1080p".to_string(),
            source: "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("0123456789ABCDEF0123456789ABCDEF01234567".to_string()),
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(1024),
            seeders: None,
            language: None,
            cached_debrid: Some(true),
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: Vec::new(),
            default_route: None,
            raw: None,
        };
        let fingerprint = candidate_release_fingerprint(&candidate, None);

        assert_eq!(
            fingerprint,
            build_release_fingerprint(&ReleaseFingerprintInput {
                source_kind: "magnet",
                source: "ignored because info_hash wins",
                info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
                release_title: "show s01 complete 1080p",
                size_bytes: Some(1024),
                source_provider_id: None,
            })
        );
    }

    #[test]
    fn review_candidate_fingerprint_is_scoped_to_subscription_and_media_type() {
        let provider_id = Uuid::new_v4();
        let subscription_id = Uuid::new_v4();
        let candidate = AcquisitionCandidate {
            id: Some("candidate-1".to_string()),
            title: "Example.Show.Episode.1".to_string(),
            source: "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: None,
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(1024),
            seeders: Some(3),
            language: Some("en".to_string()),
            cached_debrid: None,
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: Vec::new(),
            default_route: None,
            raw: None,
        };

        let first = review_candidate_release_fingerprint(&ReviewCandidateFingerprintInput {
            candidate: &candidate,
            source_provider_id: Some(provider_id),
            subscription_id: Some(subscription_id),
            media_type: MediaType::Series,
        });
        let second = review_candidate_release_fingerprint(&ReviewCandidateFingerprintInput {
            candidate: &candidate,
            source_provider_id: Some(provider_id),
            subscription_id: Some(subscription_id),
            media_type: MediaType::Series,
        });
        let other_media_type =
            review_candidate_release_fingerprint(&ReviewCandidateFingerprintInput {
                candidate: &candidate,
                source_provider_id: Some(provider_id),
                subscription_id: Some(subscription_id),
                media_type: MediaType::Anime,
            });
        let other_subscription =
            review_candidate_release_fingerprint(&ReviewCandidateFingerprintInput {
                candidate: &candidate,
                source_provider_id: Some(provider_id),
                subscription_id: Some(Uuid::new_v4()),
                media_type: MediaType::Series,
            });

        assert_eq!(first, second);
        assert_ne!(first, other_media_type);
        assert_ne!(first, other_subscription);
        assert!(first.starts_with("review:v1:series:"));
    }
}
