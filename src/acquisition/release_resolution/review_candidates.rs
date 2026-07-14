use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sha2::{Digest, Sha256};
use sqlx::AnyPool;
use uuid::Uuid;

use crate::{
    acquisition::{
        audit::{
            EVENT_REVIEW_CANDIDATE_CREATED, NewAcquisitionAuditEvent,
            record_acquisition_audit_event,
        },
        release_resolution::{
            fingerprint::{
                ReviewCandidateFingerprintInput, normalize_source_url,
                review_candidate_release_fingerprint,
            },
            models::{
                AcquisitionRelease, AcquisitionReleaseFile, AcquisitionReleaseState,
                NewAcquisitionRelease, NewAcquisitionReleaseCoverage, NewAcquisitionReleaseFile,
                ReleaseConfidence, ReleaseCoverageKind, ReleaseCoverageState, ReleaseKind,
                ReleaseResolverKind,
            },
            store::{
                get_release_by_fingerprint, list_release_coverage, list_release_files,
                upsert_release, upsert_release_coverage, upsert_release_file,
            },
        },
    },
    db::models::MediaType,
    http::handlers::acquisition_sources::{AcquisitionCandidate, CandidateScoreBadge},
};

pub const MANUAL_REVIEW_EVIDENCE_SCHEMA_VERSION: u32 = 1;
pub const MANUAL_REVIEW_REASON_RESOLVER_REJECTED: &str = "resolver_rejected_source_candidate";
pub const MANUAL_REVIEW_VERIFIER: &str = "manual_review";
pub const SYNTHETIC_SOURCE_CANDIDATE_FILE_ID: &str = "source-candidate";

#[derive(Debug, Clone)]
pub struct NewManualReviewCandidateRelease {
    pub subscription_id: Option<Uuid>,
    pub source_provider_id: Option<Uuid>,
    pub source_extension_id: String,
    pub owner_id: String,
    pub media_type: MediaType,
    pub title: String,
    pub candidate: AcquisitionCandidate,
    pub target_scope: ManualReviewTargetScope,
    pub resolver_evidence: ManualReviewResolverEvidence,
    pub route_policy: ManualReviewRoutePolicyEvidence,
    pub release_kind: ReleaseKind,
    pub score: Option<f64>,
    pub state_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualReviewEvidenceEnvelope {
    pub schema_version: u32,
    pub review_reason: String,
    pub target_scope: ManualReviewTargetScope,
    pub source_candidate: ManualReviewSourceCandidateEvidence,
    pub resolver_evidence: ManualReviewResolverEvidence,
    pub route_policy: ManualReviewRoutePolicyEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ManualReviewTargetScope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<Uuid>,
    pub media_type: MediaType,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub episode_numbers: Vec<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absolute_episode_numbers: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualReviewSourceCandidateEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<Uuid>,
    pub extension_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_id: Option<String>,
    pub release_title: String,
    pub source_kind: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seeders: Option<u32>,
    pub tracker_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_debrid: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub score_badges: Vec<CandidateScoreBadge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_routes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualReviewResolverEvidence {
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parsed_release: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejection_codes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualReviewRoutePolicyEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_routes: Vec<String>,
}

impl Default for ManualReviewTargetScope {
    fn default() -> Self {
        Self {
            subscription_id: None,
            media_type: MediaType::Series,
            targets: Vec::new(),
            target_keys: Vec::new(),
            season_number: None,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
        }
    }
}

impl ManualReviewEvidenceEnvelope {
    pub fn new(input: &NewManualReviewCandidateRelease) -> Self {
        let mut target_scope = input.target_scope.clone();
        target_scope.subscription_id = target_scope.subscription_id.or(input.subscription_id);
        target_scope.media_type = input.media_type;
        target_scope.targets = stable_uuid_list(target_scope.targets);
        target_scope.target_keys = stable_string_list(target_scope.target_keys);
        target_scope.episode_numbers = stable_i32_list(target_scope.episode_numbers);
        target_scope.absolute_episode_numbers =
            stable_i32_list(target_scope.absolute_episode_numbers);

        Self {
            schema_version: MANUAL_REVIEW_EVIDENCE_SCHEMA_VERSION,
            review_reason: MANUAL_REVIEW_REASON_RESOLVER_REJECTED.to_string(),
            target_scope,
            source_candidate: ManualReviewSourceCandidateEvidence::from_input(input),
            resolver_evidence: input.resolver_evidence.sanitized(),
            route_policy: input.route_policy.normalized(),
        }
    }

    pub fn to_json_value(&self) -> Result<JsonValue> {
        serde_json::to_value(self).context("serializing manual review evidence")
    }
}

impl ManualReviewSourceCandidateEvidence {
    fn from_input(input: &NewManualReviewCandidateRelease) -> Self {
        let candidate = &input.candidate;
        Self {
            provider_id: input.source_provider_id,
            extension_id: input.source_extension_id.trim().to_string(),
            candidate_id: candidate.id.clone(),
            release_title: candidate.title.trim().to_string(),
            source_kind: candidate.source_kind.trim().to_ascii_lowercase(),
            source: redact_candidate_source(&candidate.source),
            info_hash: candidate.info_hash.clone(),
            size_bytes: candidate.size_bytes,
            quality: candidate.quality.clone(),
            language: candidate.language.clone(),
            seeders: candidate.seeders,
            tracker_count: tracker_count(&candidate.source),
            cached_debrid: candidate.cached_debrid,
            rank: candidate.rank,
            score: candidate.score,
            score_badges: candidate.score_badges.clone(),
            supported_routes: stable_string_list(candidate.supported_routes.clone()),
            default_route: candidate.default_route.clone(),
            raw: candidate.raw.as_ref().map(sanitize_review_json),
        }
    }
}

impl ManualReviewResolverEvidence {
    fn sanitized(&self) -> Self {
        Self {
            resolver_kind: self.resolver_kind,
            resolver_version: self.resolver_version.trim().to_string(),
            parsed_release: self.parsed_release.as_ref().map(sanitize_review_json),
            rejection_codes: stable_string_list(self.rejection_codes.clone()),
            candidate_score: self.candidate_score,
            reason: self.reason.clone(),
        }
    }
}

impl ManualReviewRoutePolicyEvidence {
    fn normalized(&self) -> Self {
        Self {
            preferred: self.preferred.clone(),
            allowed_routes: stable_string_list(self.allowed_routes.clone()),
        }
    }
}

pub async fn upsert_manual_review_candidate_release(
    pool: &AnyPool,
    input: NewManualReviewCandidateRelease,
) -> Result<AcquisitionRelease> {
    validate_manual_review_input(&input)?;
    let fingerprint = review_candidate_release_fingerprint(&ReviewCandidateFingerprintInput {
        candidate: &input.candidate,
        source_provider_id: input.source_provider_id,
        subscription_id: input.subscription_id,
        media_type: input.media_type,
    });
    let existing = get_release_by_fingerprint(
        pool,
        input.owner_id.trim(),
        input.source_extension_id.trim(),
        &fingerprint,
    )
    .await?;
    let evidence = ManualReviewEvidenceEnvelope::new(&input);
    let evidence_json = evidence.to_json_value()?;
    let selected_candidate =
        serde_json::to_value(&input.candidate).context("serializing source candidate")?;
    let reason = input.state_reason.clone().or_else(|| {
        input
            .resolver_evidence
            .reason
            .clone()
            .or_else(|| Some("Release requires manual review before download.".to_string()))
    });

    let release = upsert_release(
        pool,
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: input.subscription_id,
            source_provider_id: input.source_provider_id,
            source_extension_id: input.source_extension_id.trim().to_string(),
            owner_id: input.owner_id.trim().to_string(),
            media_type: input.media_type,
            title: input.title.trim().to_string(),
            release_title: input.candidate.title.trim().to_string(),
            source: input.candidate.source.trim().to_string(),
            source_kind: input.candidate.source_kind.trim().to_ascii_lowercase(),
            info_hash: input.candidate.info_hash.clone(),
            fingerprint: fingerprint.clone(),
            release_kind: input.release_kind,
            resolver_kind: input.resolver_evidence.resolver_kind,
            resolver_version: input.resolver_evidence.resolver_version.trim().to_string(),
            confidence: ReleaseConfidence::ReviewRequired,
            score: input.score.or(input.candidate.score),
            selected_route_logical_id: None,
            selected_provider_id: None,
            download_id: None,
            remote_release_id: None,
            state: AcquisitionReleaseState::ReviewRequired,
            state_reason: reason,
            selected_candidate: Some(selected_candidate),
            coverage_plan: Some(evidence_json),
        },
    )
    .await?;

    for target_id in evidence.target_scope.targets.iter().copied() {
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id,
                coverage_kind: coverage_kind_for_review(input.media_type, input.release_kind),
                confidence: ReleaseConfidence::ReviewRequired,
                score: input.score.or(input.candidate.score),
                reason: input
                    .resolver_evidence
                    .reason
                    .clone()
                    .or_else(|| Some("Manual review required before coverage is trusted.".into())),
                state: ReleaseCoverageState::ReviewRequired,
                verified_by: Some(MANUAL_REVIEW_VERIFIER.to_string()),
            },
        )
        .await?;
    }

    ensure_manual_review_release_files(pool, &release).await?;

    if existing.is_none() {
        record_acquisition_audit_event(
            pool,
            NewAcquisitionAuditEvent {
                event_type: EVENT_REVIEW_CANDIDATE_CREATED.to_string(),
                release_id: Some(release.release_id),
                subscription_id: release.subscription_id,
                actor_user_id: None,
                state: Some(release.state.as_str().to_string()),
                reason: release.state_reason.clone(),
                evidence: Some(json!({
                    "releaseFingerprint": fingerprint,
                    "sourceProviderId": input.source_provider_id,
                    "sourceExtensionId": input.source_extension_id,
                    "targetScope": evidence.target_scope,
                    "resolverEvidence": evidence.resolver_evidence,
                    "routePolicy": evidence.route_policy,
                })),
                ..NewAcquisitionAuditEvent::default()
            },
        )
        .await?;
    }

    Ok(release)
}

pub async fn ensure_manual_review_release_files(
    pool: &AnyPool,
    release: &AcquisitionRelease,
) -> Result<Vec<AcquisitionReleaseFile>> {
    let existing = list_release_files(pool, release.release_id).await?;
    if !existing.is_empty() || release.confidence != ReleaseConfidence::ReviewRequired {
        return Ok(existing);
    }

    let Some(candidate) = release
        .selected_candidate
        .clone()
        .and_then(|value| serde_json::from_value::<AcquisitionCandidate>(value).ok())
    else {
        return Ok(existing);
    };

    let mut files = candidate
        .files
        .iter()
        .enumerate()
        .filter_map(|(index, file)| {
            let path = file.path.trim();
            if path.is_empty() {
                return None;
            }
            Some(ReviewCandidateFileInput {
                file_id: file
                    .file_id
                    .clone()
                    .filter(|value| !value.trim().is_empty()),
                file_index: file.file_index.or_else(|| i64::try_from(index).ok()),
                path: path.to_string(),
                size_bytes: file.size_bytes.and_then(|value| i64::try_from(value).ok()),
                selectable: file.selectable.unwrap_or(true),
                synthetic: false,
            })
        })
        .collect::<Vec<_>>();

    if files.is_empty()
        && let Some(path) = synthetic_candidate_path(&release.release_title, &candidate)
    {
        files.push(ReviewCandidateFileInput {
            file_id: Some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID.to_string()),
            file_index: candidate.file_index,
            path,
            size_bytes: candidate
                .size_bytes
                .and_then(|value| i64::try_from(value).ok()),
            selectable: true,
            synthetic: true,
        });
    }

    for file in files {
        upsert_release_file(
            pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: file.file_index,
                file_id: file.file_id.clone(),
                provider_file_id: file
                    .synthetic
                    .then_some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID.to_string()),
                path: file.path.clone(),
                basename: None,
                size_bytes: file.size_bytes,
                selectable: file.selectable,
                selected: None,
                parsed_title: None,
                parsed_season_number: None,
                parsed_episode_number: None,
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: candidate.quality.clone(),
                parsed_language: candidate.language.clone(),
                parsed_release_group: None,
                parser_confidence: ReleaseConfidence::ReviewRequired,
                parser_reason: Some(
                    "Source candidate file row created for manual review mapping.".to_string(),
                ),
                raw: Some(json!({
                    "source": "manual_review_source_candidate",
                    "synthetic": file.synthetic,
                    "candidateId": candidate.id.clone(),
                    "candidateTitle": candidate.title.clone(),
                    "infoHash": candidate.info_hash.clone(),
                })),
                provider_metadata: Some(json!({
                    "sourceCandidateFile": true,
                    "synthetic": file.synthetic,
                    "selectable": file.selectable,
                })),
            },
        )
        .await?;
    }

    list_release_files(pool, release.release_id).await
}

pub async fn count_manual_review_coverage(pool: &AnyPool, release_id: Uuid) -> Result<usize> {
    Ok(list_release_coverage(pool, release_id)
        .await?
        .into_iter()
        .filter(|coverage| coverage.state == ReleaseCoverageState::ReviewRequired)
        .count())
}

fn validate_manual_review_input(input: &NewManualReviewCandidateRelease) -> Result<()> {
    if input.source_extension_id.trim().is_empty() {
        bail!("source_extension_id is required");
    }
    if input.owner_id.trim().is_empty() {
        bail!("owner_id is required");
    }
    if input.title.trim().is_empty() {
        bail!("title is required");
    }
    if input.candidate.title.trim().is_empty() {
        bail!("candidate title is required");
    }
    if input.candidate.source.trim().is_empty() {
        bail!("candidate source is required");
    }
    if input.candidate.source_kind.trim().is_empty() {
        bail!("candidate source_kind is required");
    }
    if input.resolver_evidence.resolver_version.trim().is_empty() {
        bail!("resolver_version is required");
    }
    Ok(())
}

#[derive(Debug)]
struct ReviewCandidateFileInput {
    file_id: Option<String>,
    file_index: Option<i64>,
    path: String,
    size_bytes: Option<i64>,
    selectable: bool,
    synthetic: bool,
}

fn synthetic_candidate_path(
    release_title: &str,
    candidate: &AcquisitionCandidate,
) -> Option<String> {
    for value in [&candidate.title, release_title] {
        let path = value.trim();
        if looks_like_media_file(path) {
            return Some(path.to_string());
        }
    }
    None
}

fn looks_like_media_file(path: &str) -> bool {
    let lower = path.trim().to_ascii_lowercase();
    matches!(
        lower.rsplit('.').next(),
        Some("mkv" | "mp4" | "m4v" | "avi" | "mov" | "wmv" | "ts" | "m2ts" | "webm")
    )
}

fn coverage_kind_for_release_kind(release_kind: ReleaseKind) -> ReleaseCoverageKind {
    match release_kind {
        ReleaseKind::Single => ReleaseCoverageKind::SingleEpisode,
        ReleaseKind::MultiEpisode => ReleaseCoverageKind::MultiEpisodeRange,
        ReleaseKind::SeasonPack => ReleaseCoverageKind::SeasonPack,
        ReleaseKind::MultiSeasonPack => ReleaseCoverageKind::MultiSeasonPack,
        ReleaseKind::SeriesPack => ReleaseCoverageKind::SeriesPack,
        ReleaseKind::Unknown => ReleaseCoverageKind::ManualOverride,
    }
}

fn coverage_kind_for_review(
    media_type: MediaType,
    release_kind: ReleaseKind,
) -> ReleaseCoverageKind {
    if media_type == MediaType::Movie {
        return ReleaseCoverageKind::Movie;
    }
    coverage_kind_for_release_kind(release_kind)
}

pub fn sanitize_review_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut sanitized = JsonMap::new();
            for (key, value) in map {
                if is_sensitive_key(key) {
                    sanitized.insert(key.clone(), JsonValue::String("[redacted]".to_string()));
                } else if key.eq_ignore_ascii_case("source")
                    || key.eq_ignore_ascii_case("url")
                    || key.eq_ignore_ascii_case("link")
                    || key.eq_ignore_ascii_case("magnet")
                {
                    sanitized.insert(
                        key.clone(),
                        value
                            .as_str()
                            .map(redact_candidate_source)
                            .map(JsonValue::String)
                            .unwrap_or_else(|| sanitize_review_json(value)),
                    );
                } else {
                    sanitized.insert(key.clone(), sanitize_review_json(value));
                }
            }
            JsonValue::Object(sanitized)
        }
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(sanitize_review_json).collect())
        }
        _ => value.clone(),
    }
}

fn redact_candidate_source(source: &str) -> String {
    if let Some(url) = normalize_source_url(source) {
        return url;
    }

    let trimmed = source.trim();
    if !trimmed
        .get(..7)
        .map(|prefix| prefix.eq_ignore_ascii_case("magnet:"))
        .unwrap_or(false)
    {
        return trimmed.to_string();
    }
    let Some((prefix, query)) = trimmed.split_once('?') else {
        return trimmed.to_string();
    };
    let pairs = query
        .split('&')
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if is_sensitive_key(key) {
                format!("{key}=[redacted]")
            } else if value.is_empty() {
                key.to_string()
            } else {
                format!("{key}={value}")
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{prefix}?{pairs}")
}

fn tracker_count(source: &str) -> usize {
    let Some(query) = source.trim().split_once('?').map(|(_, query)| query) else {
        return 0;
    };
    query
        .split('&')
        .filter(|pair| {
            pair.split_once('=')
                .map(|(key, _)| key.eq_ignore_ascii_case("tr"))
                .unwrap_or_else(|| pair.eq_ignore_ascii_case("tr"))
        })
        .count()
}

fn stable_uuid_list(values: Vec<Uuid>) -> Vec<Uuid> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn stable_i32_list(values: Vec<i32>) -> Vec<i32> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn stable_string_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "access_token"
            | "apikey"
            | "api_key"
            | "auth"
            | "auth_token"
            | "bearer"
            | "client_secret"
            | "cookie"
            | "expires"
            | "exp"
            | "key"
            | "pass"
            | "passkey"
            | "password"
            | "refresh_token"
            | "secret"
            | "session"
            | "sid"
            | "sig"
            | "signature"
            | "token"
    )
}

fn short_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded[..24].to_string()
}

pub fn manual_review_scope_fingerprint(scope: &ManualReviewTargetScope) -> String {
    let canonical = json!({
        "subscriptionId": scope.subscription_id.map(|value| value.to_string()),
        "mediaType": scope.media_type.as_str(),
        "targets": stable_uuid_list(scope.targets.clone())
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>(),
        "targetKeys": stable_string_list(scope.target_keys.clone()),
        "seasonNumber": scope.season_number,
        "episodeNumbers": stable_i32_list(scope.episode_numbers.clone()),
        "absoluteEpisodeNumbers": stable_i32_list(scope.absolute_episode_numbers.clone()),
    });
    short_hash(&canonical.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::{
            audit::count_acquisition_audit_events,
            release_resolution::store::list_release_coverage,
            subscriptions::{
                AcquisitionMonitorPolicy, AcquisitionRoutePolicy, NewAcquisitionSubscription,
                NewAcquisitionTarget, create_subscription, upsert_subscription_targets,
            },
        },
        config::DatabaseConfig,
        db::Database,
        download_broker::{DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID},
    };

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    fn candidate_with_secret_raw() -> AcquisitionCandidate {
        AcquisitionCandidate {
            id: Some("src-1".to_string()),
            title: "Example Show Episode 1".to_string(),
            source: "https://sources.example/release.mkv?quality=1080p&token=super-secret&x=1"
                .to_string(),
            source_kind: "url".to_string(),
            info_hash: None,
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(1_000_000),
            seeders: Some(12),
            language: Some("en".to_string()),
            cached_debrid: Some(false),
            rank: Some(1),
            score: Some(10.0),
            score_badges: vec![CandidateScoreBadge {
                label: "Exact title".to_string(),
                detail: None,
                score: Some(3.0),
            }],
            files: Vec::new(),
            supported_routes: vec![
                TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                DEBRID_DEFAULT_LOGICAL_ID.to_string(),
            ],
            default_route: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            raw: Some(json!({
                "url": "https://sources.example/release.mkv?token=another-secret&x=1",
                "token": "raw-secret",
                "nested": {
                    "apiKey": "nested-secret",
                    "safe": "value"
                }
            })),
        }
    }

    async fn create_source_provider(pool: &AnyPool) -> Result<Uuid> {
        let instance_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let extension_id = format!("test.torrentio.{instance_id}");
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extensions (
                extension_id, name, version, kind, trust_level, manifest_json, enabled
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&extension_id)
        .bind("Test Torrentio")
        .bind("0.1.0")
        .bind("module")
        .bind("community")
        .bind("{}")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_instances (
                instance_id, extension_id, instance_name, config_json, enabled
             ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(instance_id.to_string())
        .bind(&extension_id)
        .bind("default")
        .bind("{}")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO providers (
                provider_id, instance_id, capability, slot_id, cardinality, implementation, health_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(provider_id.to_string())
        .bind(instance_id.to_string())
        .bind("acquisition.candidate_provider")
        .bind("default")
        .bind("many")
        .bind("torrentio_stremio")
        .bind("healthy")
        .execute(pool)
        .await?;
        Ok(provider_id)
    }

    async fn review_input(database: &Database) -> Result<(NewManualReviewCandidateRelease, Uuid)> {
        let source_provider_id = create_source_provider(&database.pool).await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                year: Some(2026),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: Some(source_provider_id),
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Episode 1".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: Some(json!({ "title": "Episode 1" })),
                state: None,
                next_search_after: None,
            }],
        )
        .await?;

        Ok((
            NewManualReviewCandidateRelease {
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: Some(source_provider_id),
                source_extension_id: "elixir.sources.torrentio_stremio".to_string(),
                owner_id: "default".to_string(),
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                candidate: candidate_with_secret_raw(),
                target_scope: ManualReviewTargetScope {
                    subscription_id: Some(subscription.subscription_id),
                    media_type: MediaType::Series,
                    targets: vec![targets[0].target_id],
                    target_keys: vec!["S01E01".to_string()],
                    season_number: Some(1),
                    episode_numbers: vec![1],
                    absolute_episode_numbers: Vec::new(),
                },
                resolver_evidence: ManualReviewResolverEvidence {
                    resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                    resolver_version: "amr0-test".to_string(),
                    parsed_release: Some(json!({
                        "token": "parsed-secret",
                        "seasonNumber": 1,
                        "episodeNumber": null
                    })),
                    rejection_codes: vec![
                        "pack_shape_not_proven".to_string(),
                        "ambiguous_episode_numbering".to_string(),
                    ],
                    candidate_score: Some(0.2),
                    reason: Some("Candidate numbering is ambiguous.".to_string()),
                },
                route_policy: ManualReviewRoutePolicyEvidence {
                    preferred: Some("debrid_first".to_string()),
                    allowed_routes: vec![
                        DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                        TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                    ],
                },
                release_kind: ReleaseKind::Unknown,
                score: Some(0.2),
                state_reason: None,
            },
            source_provider_id,
        ))
    }

    #[tokio::test]
    async fn manual_review_candidate_upsert_is_idempotent() -> Result<()> {
        let database = setup_db().await?;
        let (mut input, _) = review_input(&database).await?;
        let created = upsert_manual_review_candidate_release(&database.pool, input.clone()).await?;

        input.resolver_evidence.reason = Some("Updated resolver evidence.".to_string());
        let updated = upsert_manual_review_candidate_release(&database.pool, input).await?;

        assert_eq!(created.release_id, updated.release_id);
        assert_eq!(updated.state, AcquisitionReleaseState::ReviewRequired);
        assert_eq!(updated.confidence, ReleaseConfidence::ReviewRequired);
        assert_eq!(
            updated.state_reason.as_deref(),
            Some("Updated resolver evidence.")
        );
        assert_eq!(
            count_manual_review_coverage(&database.pool, updated.release_id).await?,
            1
        );
        assert_eq!(
            list_release_coverage(&database.pool, updated.release_id)
                .await?
                .len(),
            1
        );
        assert_eq!(
            count_acquisition_audit_events(
                &database.pool,
                updated.release_id,
                EVENT_REVIEW_CANDIDATE_CREATED,
            )
            .await?,
            1,
            "idempotent candidate upserts must not duplicate review-created audit events"
        );
        Ok(())
    }

    #[tokio::test]
    async fn manual_review_candidate_keeps_source_and_route_provider_distinct() -> Result<()> {
        let database = setup_db().await?;
        let (input, source_provider_id) = review_input(&database).await?;
        let release = upsert_manual_review_candidate_release(&database.pool, input).await?;

        assert_eq!(release.source_provider_id, Some(source_provider_id));
        assert_eq!(
            release.source_extension_id,
            "elixir.sources.torrentio_stremio"
        );
        assert_eq!(release.selected_provider_id, None);
        assert_eq!(release.selected_route_logical_id, None);
        assert_eq!(release.download_id, None);
        Ok(())
    }

    #[tokio::test]
    async fn manual_review_evidence_redacts_sensitive_values() -> Result<()> {
        let database = setup_db().await?;
        let (input, _) = review_input(&database).await?;
        let release = upsert_manual_review_candidate_release(&database.pool, input).await?;
        let evidence = release.coverage_plan.expect("review evidence");
        let rendered = evidence.to_string();

        assert!(!rendered.contains("super-secret"));
        assert!(!rendered.contains("another-secret"));
        assert!(!rendered.contains("raw-secret"));
        assert!(!rendered.contains("nested-secret"));
        assert!(!rendered.contains("parsed-secret"));
        assert!(rendered.contains("[redacted]"));
        assert!(rendered.contains("quality=1080p"));
        Ok(())
    }

    #[tokio::test]
    async fn manual_review_candidate_creates_review_required_target_coverage() -> Result<()> {
        let database = setup_db().await?;
        let (input, _) = review_input(&database).await?;
        let release = upsert_manual_review_candidate_release(&database.pool, input).await?;
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;

        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, ReleaseCoverageState::ReviewRequired);
        assert_eq!(coverage[0].confidence, ReleaseConfidence::ReviewRequired);
        assert_eq!(
            coverage[0].coverage_kind,
            ReleaseCoverageKind::ManualOverride
        );
        assert_eq!(
            coverage[0].verified_by.as_deref(),
            Some(MANUAL_REVIEW_VERIFIER)
        );
        Ok(())
    }

    #[test]
    fn manual_review_scope_fingerprint_is_order_independent() {
        let first_target = Uuid::new_v4();
        let second_target = Uuid::new_v4();
        let first = ManualReviewTargetScope {
            subscription_id: Some(Uuid::new_v4()),
            media_type: MediaType::Series,
            targets: vec![first_target, second_target],
            target_keys: vec!["S01E02".to_string(), "S01E01".to_string()],
            season_number: Some(1),
            episode_numbers: vec![2, 1, 1],
            absolute_episode_numbers: Vec::new(),
        };
        let second = ManualReviewTargetScope {
            targets: vec![second_target, first_target, first_target],
            target_keys: vec!["S01E01".to_string(), "S01E02".to_string()],
            episode_numbers: vec![1, 2],
            ..first.clone()
        };

        assert_eq!(
            manual_review_scope_fingerprint(&first),
            manual_review_scope_fingerprint(&second)
        );
    }
}
