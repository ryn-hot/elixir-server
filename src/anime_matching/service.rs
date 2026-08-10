use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use thiserror::Error;

use crate::metrics::{ANIME_MATCH_ASSIST_EVENTS, ANIME_MATCH_ASSIST_LATENCY};

use super::types::{
    ANIME_MATCH_MAX_CANDIDATES, ANIME_MATCH_MAX_REQUEST_BYTES, ANIME_MATCH_SCHEMA_VERSION,
    AnimeCandidateMatch, AnimeMatchBatchInput, AnimeMatchCandidate, AnimeMatchFile,
    AnimeMatchRequest, AnimeMatchResponse, AnimeMatchSourceMap, PreparedAnimeMatchRequest,
};

#[async_trait]
pub trait AnimeMatchEngine: Send + Sync {
    async fn match_candidates(&self, request: AnimeMatchRequest) -> Result<AnimeMatchResponse>;

    /// Returns the response together with the immutable runtime identity that
    /// produced it. Engines which do not manage a replaceable runtime retain
    /// the narrow V1 contract through the default implementation.
    async fn match_candidates_with_provenance(
        &self,
        request: AnimeMatchRequest,
    ) -> Result<AnimeMatchEngineOutput> {
        Ok(AnimeMatchEngineOutput {
            response: self.match_candidates(request).await?,
            runtime: None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchRuntimeProvenance {
    pub bundle_version: String,
    pub model_id: String,
    pub model_revision: String,
    pub worker_revision: String,
    pub backend: String,
    pub profile_fingerprint: String,
    pub prompt_revision: String,
    pub protocol_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnimeMatchEngineOutput {
    pub response: AnimeMatchResponse,
    pub runtime: Option<AnimeMatchRuntimeProvenance>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeterministicMatchState {
    Definitive,
    Difficult,
}

#[derive(Debug)]
pub struct AnimeDeterministicResult<B> {
    pub value: B,
    pub state: DeterministicMatchState,
}

impl<B> AnimeDeterministicResult<B> {
    pub fn definitive(value: B) -> Self {
        Self {
            value,
            state: DeterministicMatchState::Definitive,
        }
    }

    pub fn difficult(value: B) -> Self {
        Self {
            value,
            state: DeterministicMatchState::Difficult,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeMatchAssistSource {
    DeterministicFastPath,
    LocalModel,
    DeterministicFallback,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeMatchAssistResult {
    Definitive,
    Matched,
    Fallback,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeMatchFallbackReason {
    EngineUnavailable,
    EngineError,
    InvalidRequest,
    EmptyModelMatches,
    InvalidModelResponse,
    CoverageValidationFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchAssistProvenance {
    pub source: AnimeMatchAssistSource,
    pub result: AnimeMatchAssistResult,
    pub matcher_schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<AnimeMatchFallbackReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<AnimeMatchRuntimeProvenance>,
    pub latency_ms: u64,
}

impl AnimeMatchAssistProvenance {
    pub fn as_json(&self) -> JsonValue {
        json!({ "animeMatchAssist": self })
    }
}

#[derive(Debug)]
pub struct AnimeMatchingOutcome<B> {
    pub value: B,
    pub matches: Vec<AnimeCandidateMatch>,
    pub provenance: AnimeMatchAssistProvenance,
}

impl<B> AnimeMatchingOutcome<B> {
    pub fn used_model(&self) -> bool {
        self.provenance.result == AnimeMatchAssistResult::Matched
    }

    pub fn into_value(self) -> B {
        self.value
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AnimeMatchValidationError {
    #[error("unsupported request schema version {0}")]
    UnsupportedRequestSchemaVersion(u32),
    #[error("unsupported response schema version {0}")]
    UnsupportedResponseSchemaVersion(u32),
    #[error("requestId must not be empty")]
    EmptyRequestId,
    #[error("canonicalTitle must not be empty")]
    EmptyCanonicalTitle,
    #[error("graphFingerprint must not be empty")]
    EmptyGraphFingerprint,
    #[error("wantedTargetKeys must not be empty")]
    EmptyWantedTargets,
    #[error("request contains no candidates")]
    EmptyCandidates,
    #[error("request exceeds the candidate limit ({actual} > {maximum})")]
    TooManyCandidates { actual: usize, maximum: usize },
    #[error("request could not be encoded for the matching engine")]
    RequestEncodingFailed,
    #[error("encoded request exceeds the V1 byte limit ({actual} > {maximum})")]
    RequestTooLarge { actual: usize, maximum: usize },
    #[error("duplicate wanted target key '{0}'")]
    DuplicateWantedTarget(String),
    #[error("duplicate context target key '{0}'")]
    DuplicateContextTarget(String),
    #[error("{location} contains an empty target key")]
    EmptyTargetKey { location: &'static str },
    #[error("wanted target key '{0}' is absent from context")]
    UnknownWantedTarget(String),
    #[error("duplicate request candidate key '{0}'")]
    DuplicateRequestCandidate(String),
    #[error("duplicate request file key '{0}'")]
    DuplicateRequestFile(String),
    #[error("request candidate '{0}' has an empty title")]
    EmptyCandidateTitle(String),
    #[error("request file '{0}' has an empty path")]
    EmptyFilePath(String),
    #[error("private source map does not match request key '{0}'")]
    SourceMapMismatch(String),
    #[error("response references unknown candidate '{0}'")]
    UnknownCandidate(String),
    #[error("response repeats candidate '{0}'")]
    DuplicateCandidate(String),
    #[error("candidate '{0}' returned no target references")]
    EmptyMatchTargets(String),
    #[error("candidate '{candidate_key}' references unknown target '{target_key}'")]
    UnknownTarget {
        candidate_key: String,
        target_key: String,
    },
    #[error("candidate '{candidate_key}' references context-only target '{target_key}'")]
    TargetOutsideWantedScope {
        candidate_key: String,
        target_key: String,
    },
    #[error("candidate '{candidate_key}' repeats target '{target_key}'")]
    DuplicateTarget {
        candidate_key: String,
        target_key: String,
    },
    #[error("candidate '{candidate_key}' references unknown file '{file_key}'")]
    UnknownFile {
        candidate_key: String,
        file_key: String,
    },
    #[error(
        "candidate '{candidate_key}' references file '{file_key}' owned by '{owner_candidate_key}'"
    )]
    FileOwnedByDifferentCandidate {
        candidate_key: String,
        file_key: String,
        owner_candidate_key: String,
    },
    #[error("candidate '{candidate_key}' repeats file '{file_key}'")]
    DuplicateFile {
        candidate_key: String,
        file_key: String,
    },
    #[error("candidate '{0}' returned an empty selectedFileKeys list")]
    EmptySelectedFiles(String),
}

#[derive(Clone, Default)]
pub struct AnimeMatchingService {
    engine: Option<Arc<dyn AnimeMatchEngine>>,
}

impl AnimeMatchingService {
    pub fn disabled() -> Self {
        Self { engine: None }
    }

    pub fn with_engine(engine: Arc<dyn AnimeMatchEngine>) -> Self {
        Self {
            engine: Some(engine),
        }
    }

    pub fn prepare_request<C, F>(
        input: AnimeMatchBatchInput<C, F>,
    ) -> std::result::Result<PreparedAnimeMatchRequest<C, F>, AnimeMatchValidationError> {
        if input.candidates.len() > ANIME_MATCH_MAX_CANDIDATES {
            return Err(AnimeMatchValidationError::TooManyCandidates {
                actual: input.candidates.len(),
                maximum: ANIME_MATCH_MAX_CANDIDATES,
            });
        }

        let mut candidate_sources = BTreeMap::new();
        let mut file_sources = BTreeMap::new();
        let candidates = input
            .candidates
            .into_iter()
            .enumerate()
            .map(|(candidate_index, candidate)| {
                let candidate_key = format!("candidate-{candidate_index}");
                candidate_sources.insert(candidate_key.clone(), candidate.source);
                let files = candidate
                    .files
                    .into_iter()
                    .enumerate()
                    .map(|(file_index, file)| {
                        let file_key = format!("{candidate_key}-file-{file_index}");
                        file_sources.insert(file_key.clone(), (candidate_key.clone(), file.source));
                        AnimeMatchFile {
                            file_key,
                            path: file.path,
                        }
                    })
                    .collect();
                AnimeMatchCandidate {
                    candidate_key,
                    title: candidate.title,
                    files,
                    parse_facts: candidate.parse_facts,
                }
            })
            .collect();

        let prepared = PreparedAnimeMatchRequest {
            request: AnimeMatchRequest {
                schema_version: ANIME_MATCH_SCHEMA_VERSION,
                request_id: input.request_id,
                target: input.target,
                context: input.context,
                candidates,
            },
            source_map: AnimeMatchSourceMap::new(candidate_sources, file_sources),
        };
        validate_anime_match_request(&prepared)?;
        Ok(prepared)
    }

    /// Applies an override only after the complete response passes reference
    /// validation. `apply_override` must construct and validate an in-memory
    /// result without external side effects; returning an error restores the
    /// exact deterministic value supplied by the caller.
    pub async fn match_or_fallback<B, C, F, A>(
        &self,
        deterministic: AnimeDeterministicResult<B>,
        input: AnimeMatchBatchInput<C, F>,
        apply_override: A,
    ) -> AnimeMatchingOutcome<B>
    where
        B: Send,
        C: Send,
        F: Send,
        A: FnOnce(
                &B,
                &AnimeMatchRequest,
                &[AnimeCandidateMatch],
                &AnimeMatchSourceMap<C, F>,
            ) -> Result<B>
            + Send,
    {
        if deterministic.state == DeterministicMatchState::Definitive {
            record_assist_event("deterministic_fast_path");
            return AnimeMatchingOutcome {
                value: deterministic.value,
                matches: Vec::new(),
                provenance: AnimeMatchAssistProvenance {
                    source: AnimeMatchAssistSource::DeterministicFastPath,
                    result: AnimeMatchAssistResult::Definitive,
                    matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
                    request_fingerprint: None,
                    reason: None,
                    detail: None,
                    runtime: None,
                    latency_ms: 0,
                },
            };
        }

        let prepared = match Self::prepare_request(input) {
            Ok(prepared) => prepared,
            Err(error) => {
                record_assist_event("invalid_request");
                return fallback_outcome(
                    deterministic.value,
                    None,
                    AnimeMatchFallbackReason::InvalidRequest,
                    Some(error.to_string()),
                    None,
                    0,
                );
            }
        };
        self.match_prepared_or_fallback(deterministic, prepared, apply_override)
            .await
    }

    /// Internal execution boundary for an already-keyed, request-local wire
    /// request. Production adapters normally call [`Self::match_or_fallback`];
    /// release qualification uses this boundary to permute candidates without
    /// changing their stable keys, while retaining the exact response
    /// validation, override, and fallback behavior used in production.
    pub(crate) async fn match_prepared_or_fallback<B, C, F, A>(
        &self,
        deterministic: AnimeDeterministicResult<B>,
        prepared: PreparedAnimeMatchRequest<C, F>,
        apply_override: A,
    ) -> AnimeMatchingOutcome<B>
    where
        B: Send,
        C: Send,
        F: Send,
        A: FnOnce(
                &B,
                &AnimeMatchRequest,
                &[AnimeCandidateMatch],
                &AnimeMatchSourceMap<C, F>,
            ) -> Result<B>
            + Send,
    {
        if deterministic.state == DeterministicMatchState::Definitive {
            record_assist_event("deterministic_fast_path");
            return AnimeMatchingOutcome {
                value: deterministic.value,
                matches: Vec::new(),
                provenance: AnimeMatchAssistProvenance {
                    source: AnimeMatchAssistSource::DeterministicFastPath,
                    result: AnimeMatchAssistResult::Definitive,
                    matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
                    request_fingerprint: None,
                    reason: None,
                    detail: None,
                    runtime: None,
                    latency_ms: 0,
                },
            };
        }

        let request_fingerprint = Some(anime_match_request_fingerprint(&prepared.request));
        let Some(engine) = self.engine.as_ref() else {
            record_assist_event("engine_unavailable");
            return fallback_outcome(
                deterministic.value,
                request_fingerprint,
                AnimeMatchFallbackReason::EngineUnavailable,
                None,
                None,
                0,
            );
        };

        let started = Instant::now();
        record_assist_event("model_attempt");
        let engine_output = match engine
            .match_candidates_with_provenance(prepared.request.clone())
            .await
        {
            Ok(output) => output,
            Err(error) => {
                let latency_ms = elapsed_millis(started);
                record_assist_event("engine_error");
                record_assist_latency("engine_error", latency_ms);
                return fallback_outcome(
                    deterministic.value,
                    request_fingerprint,
                    AnimeMatchFallbackReason::EngineError,
                    Some(error.to_string()),
                    None,
                    latency_ms,
                );
            }
        };
        let response = engine_output.response;
        let runtime = engine_output.runtime;
        let latency_ms = elapsed_millis(started);

        if let Err(error) = validate_anime_match_response(&prepared, &response) {
            record_assist_event("invalid_model_response");
            record_assist_latency("invalid_response", latency_ms);
            return fallback_outcome(
                deterministic.value,
                request_fingerprint,
                AnimeMatchFallbackReason::InvalidModelResponse,
                Some(error.to_string()),
                runtime,
                latency_ms,
            );
        }
        if response.matches.is_empty() {
            record_assist_event("empty_model_matches");
            record_assist_latency("empty", latency_ms);
            return fallback_outcome(
                deterministic.value,
                request_fingerprint,
                AnimeMatchFallbackReason::EmptyModelMatches,
                None,
                runtime,
                latency_ms,
            );
        }

        match apply_override(
            &deterministic.value,
            &prepared.request,
            &response.matches,
            &prepared.source_map,
        ) {
            Ok(value) => {
                record_assist_event("valid_model_match");
                record_assist_latency("matched", latency_ms);
                AnimeMatchingOutcome {
                    value,
                    matches: response.matches,
                    provenance: AnimeMatchAssistProvenance {
                        source: AnimeMatchAssistSource::LocalModel,
                        result: AnimeMatchAssistResult::Matched,
                        matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
                        request_fingerprint,
                        reason: None,
                        detail: None,
                        runtime,
                        latency_ms,
                    },
                }
            }
            Err(error) => {
                record_assist_event("coverage_validation_failed");
                record_assist_latency("coverage_rejected", latency_ms);
                fallback_outcome(
                    deterministic.value,
                    request_fingerprint,
                    AnimeMatchFallbackReason::CoverageValidationFailed,
                    Some(error.to_string()),
                    runtime,
                    latency_ms,
                )
            }
        }
    }
}

pub fn validate_anime_match_request<C, F>(
    prepared: &PreparedAnimeMatchRequest<C, F>,
) -> std::result::Result<(), AnimeMatchValidationError> {
    let request = &prepared.request;
    if request.schema_version != ANIME_MATCH_SCHEMA_VERSION {
        return Err(AnimeMatchValidationError::UnsupportedRequestSchemaVersion(
            request.schema_version,
        ));
    }
    if request.request_id.trim().is_empty() {
        return Err(AnimeMatchValidationError::EmptyRequestId);
    }
    if request.target.canonical_title.trim().is_empty() {
        return Err(AnimeMatchValidationError::EmptyCanonicalTitle);
    }
    if request.context.graph_fingerprint.trim().is_empty() {
        return Err(AnimeMatchValidationError::EmptyGraphFingerprint);
    }
    if request.target.wanted_target_keys.is_empty() {
        return Err(AnimeMatchValidationError::EmptyWantedTargets);
    }
    if request.candidates.is_empty() {
        return Err(AnimeMatchValidationError::EmptyCandidates);
    }
    if request.candidates.len() > ANIME_MATCH_MAX_CANDIDATES {
        return Err(AnimeMatchValidationError::TooManyCandidates {
            actual: request.candidates.len(),
            maximum: ANIME_MATCH_MAX_CANDIDATES,
        });
    }
    let encoded_size = serde_json::to_vec(request)
        .map_err(|_| AnimeMatchValidationError::RequestEncodingFailed)?
        .len();
    if encoded_size > ANIME_MATCH_MAX_REQUEST_BYTES {
        return Err(AnimeMatchValidationError::RequestTooLarge {
            actual: encoded_size,
            maximum: ANIME_MATCH_MAX_REQUEST_BYTES,
        });
    }

    let mut wanted_targets = BTreeSet::new();
    for target_key in &request.target.wanted_target_keys {
        if target_key.trim().is_empty() {
            return Err(AnimeMatchValidationError::EmptyTargetKey {
                location: "wantedTargetKeys",
            });
        }
        if !wanted_targets.insert(target_key.as_str()) {
            return Err(AnimeMatchValidationError::DuplicateWantedTarget(
                target_key.clone(),
            ));
        }
    }
    let mut context_targets = BTreeSet::new();
    for season in &request.context.seasons {
        for target in &season.targets {
            if target.target_key.trim().is_empty() {
                return Err(AnimeMatchValidationError::EmptyTargetKey {
                    location: "context",
                });
            }
            if !context_targets.insert(target.target_key.as_str()) {
                return Err(AnimeMatchValidationError::DuplicateContextTarget(
                    target.target_key.clone(),
                ));
            }
        }
    }
    for target_key in wanted_targets {
        if !context_targets.contains(target_key) {
            return Err(AnimeMatchValidationError::UnknownWantedTarget(
                target_key.to_string(),
            ));
        }
    }

    let mut candidate_keys = BTreeSet::new();
    let mut file_keys = BTreeSet::new();
    for candidate in &request.candidates {
        if !candidate_keys.insert(candidate.candidate_key.as_str()) {
            return Err(AnimeMatchValidationError::DuplicateRequestCandidate(
                candidate.candidate_key.clone(),
            ));
        }
        if candidate.title.trim().is_empty() {
            return Err(AnimeMatchValidationError::EmptyCandidateTitle(
                candidate.candidate_key.clone(),
            ));
        }
        if prepared
            .source_map
            .candidate_source(&candidate.candidate_key)
            .is_none()
        {
            return Err(AnimeMatchValidationError::SourceMapMismatch(
                candidate.candidate_key.clone(),
            ));
        }
        for file in &candidate.files {
            if !file_keys.insert(file.file_key.as_str()) {
                return Err(AnimeMatchValidationError::DuplicateRequestFile(
                    file.file_key.clone(),
                ));
            }
            if file.path.trim().is_empty() {
                return Err(AnimeMatchValidationError::EmptyFilePath(
                    file.file_key.clone(),
                ));
            }
            if prepared
                .source_map
                .file_source(&candidate.candidate_key, &file.file_key)
                .is_none()
            {
                return Err(AnimeMatchValidationError::SourceMapMismatch(
                    file.file_key.clone(),
                ));
            }
        }
    }
    if prepared.source_map.candidate_count() != request.candidates.len()
        || prepared.source_map.file_count() != file_keys.len()
    {
        return Err(AnimeMatchValidationError::SourceMapMismatch(
            "source-map-cardinality".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_anime_match_response<C, F>(
    prepared: &PreparedAnimeMatchRequest<C, F>,
    response: &AnimeMatchResponse,
) -> std::result::Result<(), AnimeMatchValidationError> {
    if response.schema_version != ANIME_MATCH_SCHEMA_VERSION {
        return Err(AnimeMatchValidationError::UnsupportedResponseSchemaVersion(
            response.schema_version,
        ));
    }

    let request = &prepared.request;
    let candidate_keys = request
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_key.as_str())
        .collect::<BTreeSet<_>>();
    let context_target_keys = request
        .context
        .seasons
        .iter()
        .flat_map(|season| season.targets.iter())
        .map(|target| target.target_key.as_str())
        .collect::<BTreeSet<_>>();
    let wanted_target_keys = request
        .target
        .wanted_target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut seen_candidates = BTreeSet::new();

    for matched in &response.matches {
        if !candidate_keys.contains(matched.candidate_key.as_str()) {
            return Err(AnimeMatchValidationError::UnknownCandidate(
                matched.candidate_key.clone(),
            ));
        }
        if !seen_candidates.insert(matched.candidate_key.as_str()) {
            return Err(AnimeMatchValidationError::DuplicateCandidate(
                matched.candidate_key.clone(),
            ));
        }
        if matched.matched_target_keys.is_empty() {
            return Err(AnimeMatchValidationError::EmptyMatchTargets(
                matched.candidate_key.clone(),
            ));
        }

        // Targets may recur under different ordered candidate alternatives,
        // but never within one candidate mapping.
        let mut seen_targets = BTreeSet::new();
        for target_key in &matched.matched_target_keys {
            if !seen_targets.insert(target_key.as_str()) {
                return Err(AnimeMatchValidationError::DuplicateTarget {
                    candidate_key: matched.candidate_key.clone(),
                    target_key: target_key.clone(),
                });
            }
            if !context_target_keys.contains(target_key.as_str()) {
                return Err(AnimeMatchValidationError::UnknownTarget {
                    candidate_key: matched.candidate_key.clone(),
                    target_key: target_key.clone(),
                });
            }
            if !wanted_target_keys.contains(target_key.as_str()) {
                return Err(AnimeMatchValidationError::TargetOutsideWantedScope {
                    candidate_key: matched.candidate_key.clone(),
                    target_key: target_key.clone(),
                });
            }
        }

        if let Some(selected_file_keys) = matched.selected_file_keys.as_ref() {
            if selected_file_keys.is_empty() {
                return Err(AnimeMatchValidationError::EmptySelectedFiles(
                    matched.candidate_key.clone(),
                ));
            }
            let mut seen_files = BTreeSet::new();
            for file_key in selected_file_keys {
                if !seen_files.insert(file_key.as_str()) {
                    return Err(AnimeMatchValidationError::DuplicateFile {
                        candidate_key: matched.candidate_key.clone(),
                        file_key: file_key.clone(),
                    });
                }
                match prepared.source_map.file_candidate_key(file_key) {
                    Some(owner) if owner == matched.candidate_key => {}
                    Some(owner) => {
                        return Err(AnimeMatchValidationError::FileOwnedByDifferentCandidate {
                            candidate_key: matched.candidate_key.clone(),
                            file_key: file_key.clone(),
                            owner_candidate_key: owner.to_string(),
                        });
                    }
                    None => {
                        return Err(AnimeMatchValidationError::UnknownFile {
                            candidate_key: matched.candidate_key.clone(),
                            file_key: file_key.clone(),
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn anime_match_request_fingerprint(request: &AnimeMatchRequest) -> String {
    // These DTOs contain no fallible serializer primitives. Retain a stable
    // fallback byte sequence if a future schema revision changes that.
    let bytes =
        serde_json::to_vec(request).unwrap_or_else(|_| request.request_id.as_bytes().to_vec());
    blake3::hash(&bytes).to_hex().to_string()
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn record_assist_event(event: &'static str) {
    ANIME_MATCH_ASSIST_EVENTS.with_label_values(&[event]).inc();
}

fn record_assist_latency(result: &'static str, latency_ms: u64) {
    ANIME_MATCH_ASSIST_LATENCY
        .with_label_values(&[result])
        .observe(latency_ms as f64 / 1_000.0);
}

fn fallback_outcome<B>(
    value: B,
    request_fingerprint: Option<String>,
    reason: AnimeMatchFallbackReason,
    detail: Option<String>,
    runtime: Option<AnimeMatchRuntimeProvenance>,
    latency_ms: u64,
) -> AnimeMatchingOutcome<B> {
    record_assist_event("deterministic_fallback");
    AnimeMatchingOutcome {
        value,
        matches: Vec::new(),
        provenance: AnimeMatchAssistProvenance {
            source: AnimeMatchAssistSource::DeterministicFallback,
            result: AnimeMatchAssistResult::Fallback,
            matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
            request_fingerprint,
            reason: Some(reason),
            detail,
            runtime,
            latency_ms,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{bail, ensure};

    use super::*;
    use crate::anime_matching::{
        AnimeMatchAlias, AnimeMatchAliasKind, AnimeMatchAudioPreference, AnimeMatchAudioProfile,
        AnimeMatchCandidateInput, AnimeMatchContext, AnimeMatchContextTarget, AnimeMatchFileInput,
        AnimeMatchMediaType, AnimeMatchParseFacts, AnimeMatchScope, AnimeMatchSeasonContext,
        AnimeMatchTarget,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestBaseline {
        canonical_selection: String,
        coverage: Vec<String>,
        selected_files: Vec<String>,
        route_eligible: bool,
        persistence_decision: String,
    }

    impl TestBaseline {
        fn deterministic() -> Self {
            Self {
                canonical_selection: "S01E01".to_string(),
                coverage: vec!["S01E01".to_string()],
                selected_files: vec!["deterministic-file".to_string()],
                route_eligible: false,
                persistence_decision: "retry".to_string(),
            }
        }
    }

    struct StaticEngine {
        response: AnimeMatchResponse,
        fail: bool,
        calls: AtomicUsize,
    }

    impl StaticEngine {
        fn responding(response: AnimeMatchResponse) -> Arc<Self> {
            Arc::new(Self {
                response,
                fail: false,
                calls: AtomicUsize::new(0),
            })
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self {
                response: AnimeMatchResponse {
                    schema_version: ANIME_MATCH_SCHEMA_VERSION,
                    matches: Vec::new(),
                },
                fail: true,
                calls: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl AnimeMatchEngine for StaticEngine {
        async fn match_candidates(
            &self,
            _request: AnimeMatchRequest,
        ) -> Result<AnimeMatchResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                bail!("scripted engine failure");
            }
            Ok(self.response.clone())
        }
    }

    struct ProvenanceEngine {
        response: AnimeMatchResponse,
        runtime: AnimeMatchRuntimeProvenance,
    }

    #[async_trait]
    impl AnimeMatchEngine for ProvenanceEngine {
        async fn match_candidates(
            &self,
            _request: AnimeMatchRequest,
        ) -> Result<AnimeMatchResponse> {
            Ok(self.response.clone())
        }

        async fn match_candidates_with_provenance(
            &self,
            _request: AnimeMatchRequest,
        ) -> Result<AnimeMatchEngineOutput> {
            Ok(AnimeMatchEngineOutput {
                response: self.response.clone(),
                runtime: Some(self.runtime.clone()),
            })
        }
    }

    fn tokyo_ghoul_batch() -> AnimeMatchBatchInput<String, String> {
        AnimeMatchBatchInput {
            request_id: "tokyo-ghoul-s2".to_string(),
            target: AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Tokyo Ghoul".to_string(),
                scope: AnimeMatchScope::Episode,
                wanted_target_keys: vec!["S02E01".to_string()],
                season_number: Some(2),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![13],
                audio_preference: AnimeMatchAudioPreference::default(),
            },
            context: AnimeMatchContext {
                graph_fingerprint: "rr3-scoped-tokyo-ghoul".to_string(),
                seasons: vec![AnimeMatchSeasonContext {
                    season_number: 2,
                    anilist_id: "1002".to_string(),
                    aliases: vec![AnimeMatchAlias {
                        value: "Tokyo Ghoul Root A".to_string(),
                        kind: AnimeMatchAliasKind::English,
                        source: Some("anizip_title".to_string()),
                        language: Some("en".to_string()),
                    }],
                    targets: vec![
                        AnimeMatchContextTarget {
                            target_key: "S02E01".to_string(),
                            title: "New Surge".to_string(),
                            season_number: Some(2),
                            episode_number: Some(1),
                            absolute_episode_number: Some(13),
                            tvdb_episode_id: Some("2013".to_string()),
                            anidb_episode_id: None,
                        },
                        AnimeMatchContextTarget {
                            target_key: "S02E02".to_string(),
                            title: "Dancing Flowers".to_string(),
                            season_number: Some(2),
                            episode_number: Some(2),
                            absolute_episode_number: Some(14),
                            tvdb_episode_id: Some("2014".to_string()),
                            anidb_episode_id: None,
                        },
                    ],
                }],
            },
            candidates: vec![
                AnimeMatchCandidateInput {
                    source: "provider-candidate-a".to_string(),
                    title: "[Group] Tokyo Ghoul Root A - 01".to_string(),
                    files: vec![AnimeMatchFileInput {
                        source: "provider-file-a".to_string(),
                        path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
                    }],
                    parse_facts: AnimeMatchParseFacts::default(),
                },
                AnimeMatchCandidateInput {
                    source: "provider-candidate-b".to_string(),
                    title: "Tokyo Ghoul S02E01".to_string(),
                    files: vec![AnimeMatchFileInput {
                        source: "provider-file-b".to_string(),
                        path: "Tokyo Ghoul S02E01.mkv".to_string(),
                    }],
                    parse_facts: AnimeMatchParseFacts::default(),
                },
            ],
        }
    }

    fn candidate_match(candidate_key: &str, file_key: Option<&str>) -> AnimeCandidateMatch {
        AnimeCandidateMatch {
            candidate_key: candidate_key.to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: file_key.map(|key| vec![key.to_string()]),
        }
    }

    #[test]
    fn alm5_prepared_request_round_trip_uses_opaque_keys_and_private_source_maps() {
        let prepared = AnimeMatchingService::prepare_request(tokyo_ghoul_batch())
            .expect("valid prepared request");

        assert_eq!(prepared.request.candidates[0].candidate_key, "candidate-0");
        assert_eq!(
            prepared.request.candidates[0].files[0].file_key,
            "candidate-0-file-0"
        );
        assert_eq!(
            prepared
                .source_map
                .candidate_source("candidate-0")
                .map(String::as_str),
            Some("provider-candidate-a")
        );
        assert_eq!(
            prepared
                .source_map
                .file_source("candidate-0", "candidate-0-file-0")
                .map(String::as_str),
            Some("provider-file-a")
        );

        let wire = serde_json::to_string(&prepared.request).expect("serialize request");
        assert!(!wire.contains("provider-candidate-a"));
        assert!(!wire.contains("provider-file-a"));
        let decoded: AnimeMatchRequest = serde_json::from_str(&wire).expect("deserialize request");
        assert_eq!(decoded, prepared.request);

        let mut unknown_field = serde_json::to_value(&prepared.request).expect("request value");
        unknown_field
            .as_object_mut()
            .expect("request object")
            .insert("modelId".to_string(), serde_json::json!("not-wire-data"));
        assert!(serde_json::from_value::<AnimeMatchRequest>(unknown_field).is_err());

        let mut missing_required = serde_json::to_value(&prepared.request).expect("request value");
        missing_required
            .as_object_mut()
            .expect("request object")
            .remove("target");
        assert!(serde_json::from_value::<AnimeMatchRequest>(missing_required).is_err());

        let response = AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![candidate_match("candidate-0", Some("candidate-0-file-0"))],
        };
        let response_wire = serde_json::to_string(&response).expect("serialize response");
        let decoded_response: AnimeMatchResponse =
            serde_json::from_str(&response_wire).expect("deserialize response");
        assert_eq!(decoded_response, response);

        let mut response_unknown = serde_json::to_value(&response).expect("response value");
        response_unknown
            .as_object_mut()
            .expect("response object")
            .insert("confidence".to_string(), serde_json::json!(0.99));
        assert!(serde_json::from_value::<AnimeMatchResponse>(response_unknown).is_err());

        let mut missing_audio_profile = serde_json::to_value(&response).expect("response value");
        missing_audio_profile["matches"][0]
            .as_object_mut()
            .expect("match object")
            .remove("audioProfile");
        assert!(serde_json::from_value::<AnimeMatchResponse>(missing_audio_profile).is_err());
    }

    #[test]
    fn alm5_response_reference_validation_rejects_unknown_duplicate_and_wrong_owner_keys() {
        let prepared = AnimeMatchingService::prepare_request(tokyo_ghoul_batch())
            .expect("valid prepared request");

        let response = |matches| AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches,
        };

        let unknown_candidate = response(vec![candidate_match("candidate-9", None)]);
        assert!(matches!(
            validate_anime_match_response(&prepared, &unknown_candidate),
            Err(AnimeMatchValidationError::UnknownCandidate(_))
        ));

        let duplicate_candidate = response(vec![
            candidate_match("candidate-0", None),
            candidate_match("candidate-0", None),
        ]);
        assert!(matches!(
            validate_anime_match_response(&prepared, &duplicate_candidate),
            Err(AnimeMatchValidationError::DuplicateCandidate(_))
        ));

        let mut duplicate_target_match = candidate_match("candidate-0", None);
        duplicate_target_match
            .matched_target_keys
            .push("S02E01".to_string());
        assert!(matches!(
            validate_anime_match_response(&prepared, &response(vec![duplicate_target_match])),
            Err(AnimeMatchValidationError::DuplicateTarget { .. })
        ));

        let mut unknown_target = candidate_match("candidate-0", None);
        unknown_target.matched_target_keys = vec!["S99E99".to_string()];
        assert!(matches!(
            validate_anime_match_response(&prepared, &response(vec![unknown_target])),
            Err(AnimeMatchValidationError::UnknownTarget { .. })
        ));

        let mut context_only_target = candidate_match("candidate-0", None);
        context_only_target.matched_target_keys = vec!["S02E02".to_string()];
        assert!(matches!(
            validate_anime_match_response(&prepared, &response(vec![context_only_target])),
            Err(AnimeMatchValidationError::TargetOutsideWantedScope { .. })
        ));

        let wrong_owner = response(vec![candidate_match(
            "candidate-1",
            Some("candidate-0-file-0"),
        )]);
        assert!(matches!(
            validate_anime_match_response(&prepared, &wrong_owner),
            Err(AnimeMatchValidationError::FileOwnedByDifferentCandidate { .. })
        ));

        let unknown_file = response(vec![candidate_match(
            "candidate-0",
            Some("candidate-0-file-99"),
        )]);
        assert!(matches!(
            validate_anime_match_response(&prepared, &unknown_file),
            Err(AnimeMatchValidationError::UnknownFile { .. })
        ));

        let mut duplicate_file = candidate_match("candidate-0", Some("candidate-0-file-0"));
        duplicate_file
            .selected_file_keys
            .as_mut()
            .expect("selected files")
            .push("candidate-0-file-0".to_string());
        assert!(matches!(
            validate_anime_match_response(&prepared, &response(vec![duplicate_file])),
            Err(AnimeMatchValidationError::DuplicateFile { .. })
        ));

        let mut empty_files = candidate_match("candidate-0", None);
        empty_files.selected_file_keys = Some(Vec::new());
        assert!(matches!(
            validate_anime_match_response(&prepared, &response(vec![empty_files])),
            Err(AnimeMatchValidationError::EmptySelectedFiles(_))
        ));
    }

    #[test]
    fn alm5_schema_versions_and_candidate_bound_are_strict() {
        let mut prepared = AnimeMatchingService::prepare_request(tokyo_ghoul_batch())
            .expect("valid prepared request");
        prepared.request.schema_version = ANIME_MATCH_SCHEMA_VERSION + 1;
        assert!(matches!(
            validate_anime_match_request(&prepared),
            Err(AnimeMatchValidationError::UnsupportedRequestSchemaVersion(
                _
            ))
        ));

        prepared.request.schema_version = ANIME_MATCH_SCHEMA_VERSION;
        let unsupported_response = AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION + 1,
            matches: Vec::new(),
        };
        assert!(matches!(
            validate_anime_match_response(&prepared, &unsupported_response),
            Err(AnimeMatchValidationError::UnsupportedResponseSchemaVersion(
                _
            ))
        ));

        let mut oversized = tokyo_ghoul_batch();
        let template = oversized.candidates[0].clone();
        oversized.candidates = (0..=ANIME_MATCH_MAX_CANDIDATES)
            .map(|_| template.clone())
            .collect();
        assert!(matches!(
            AnimeMatchingService::prepare_request(oversized),
            Err(AnimeMatchValidationError::TooManyCandidates {
                actual,
                maximum: ANIME_MATCH_MAX_CANDIDATES
            }) if actual == ANIME_MATCH_MAX_CANDIDATES + 1
        ));

        let mut at_limit = tokyo_ghoul_batch();
        let template = at_limit.candidates[0].clone();
        at_limit.candidates = (0..ANIME_MATCH_MAX_CANDIDATES)
            .map(|_| template.clone())
            .collect();
        assert!(AnimeMatchingService::prepare_request(at_limit).is_ok());

        let mut oversized_wire = tokyo_ghoul_batch();
        oversized_wire.candidates[0].title = "x".repeat(ANIME_MATCH_MAX_REQUEST_BYTES + 1);
        assert!(matches!(
            AnimeMatchingService::prepare_request(oversized_wire),
            Err(AnimeMatchValidationError::RequestTooLarge {
                maximum: ANIME_MATCH_MAX_REQUEST_BYTES,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn alm5_invalid_request_returns_exact_baseline_without_engine_or_callback() {
        let engine = StaticEngine::responding(AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![candidate_match("candidate-0", None)],
        });
        let service = AnimeMatchingService::with_engine(engine.clone());
        let expected = TestBaseline::deterministic();
        let apply_calls = AtomicUsize::new(0);
        let mut invalid = tokyo_ghoul_batch();
        invalid.candidates[0].title = "   ".to_string();

        let outcome = service
            .match_or_fallback(
                AnimeDeterministicResult::difficult(expected.clone()),
                invalid,
                |_, _, _, _| {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    bail!("invalid request must not apply")
                },
            )
            .await;

        assert_eq!(outcome.value, expected);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            outcome.provenance.reason,
            Some(AnimeMatchFallbackReason::InvalidRequest)
        );
    }

    #[tokio::test]
    async fn alm5_definitive_and_engine_error_paths_return_the_exact_baseline() {
        let engine = StaticEngine::failing();
        let service = AnimeMatchingService::with_engine(engine.clone());
        let expected = TestBaseline::deterministic();

        let definitive = service
            .match_or_fallback(
                AnimeDeterministicResult::definitive(expected.clone()),
                tokyo_ghoul_batch(),
                |_, _, _, _| bail!("definitive path must not apply an override"),
            )
            .await;
        assert_eq!(definitive.value, expected);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 0);

        let fallback = service
            .match_or_fallback(
                AnimeDeterministicResult::difficult(expected.clone()),
                tokyo_ghoul_batch(),
                |_, _, _, _| bail!("failed engine must not apply an override"),
            )
            .await;
        assert_eq!(fallback.value, expected);
        assert_eq!(
            fallback.provenance.reason,
            Some(AnimeMatchFallbackReason::EngineError)
        );
        assert_eq!(engine.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn alm5_disabled_engine_returns_the_exact_baseline_without_applying() {
        let expected = TestBaseline::deterministic();
        let apply_calls = AtomicUsize::new(0);
        let outcome = AnimeMatchingService::disabled()
            .match_or_fallback(
                AnimeDeterministicResult::difficult(expected.clone()),
                tokyo_ghoul_batch(),
                |_, _, _, _| {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    bail!("disabled engine must not apply an override")
                },
            )
            .await;

        assert_eq!(outcome.value, expected);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            outcome.provenance.reason,
            Some(AnimeMatchFallbackReason::EngineUnavailable)
        );
    }

    #[tokio::test]
    async fn alm5_empty_response_returns_the_exact_baseline_without_applying() {
        let engine = StaticEngine::responding(AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: Vec::new(),
        });
        let expected = TestBaseline::deterministic();
        let apply_calls = AtomicUsize::new(0);
        let outcome = AnimeMatchingService::with_engine(engine)
            .match_or_fallback(
                AnimeDeterministicResult::difficult(expected.clone()),
                tokyo_ghoul_batch(),
                |_, _, _, _| {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    bail!("empty response must not apply an override")
                },
            )
            .await;

        assert_eq!(outcome.value, expected);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            outcome.provenance.reason,
            Some(AnimeMatchFallbackReason::EmptyModelMatches)
        );
    }

    #[tokio::test]
    async fn alm5_callback_failure_returns_the_exact_baseline() {
        let engine = StaticEngine::responding(AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![candidate_match("candidate-0", Some("candidate-0-file-0"))],
        });
        let expected = TestBaseline::deterministic();
        let outcome = AnimeMatchingService::with_engine(engine)
            .match_or_fallback(
                AnimeDeterministicResult::difficult(expected.clone()),
                tokyo_ghoul_batch(),
                |_, _, _, _| bail!("coverage planner rejected the model mapping"),
            )
            .await;

        assert_eq!(outcome.value, expected);
        assert_eq!(
            outcome.provenance.reason,
            Some(AnimeMatchFallbackReason::CoverageValidationFailed)
        );
    }

    #[tokio::test]
    async fn alm5_mixed_valid_and_invalid_response_falls_back_without_partial_application() {
        let response = AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![
                candidate_match("candidate-0", Some("candidate-0-file-0")),
                candidate_match("candidate-1", Some("candidate-0-file-0")),
            ],
        };
        let engine = StaticEngine::responding(response);
        let service = AnimeMatchingService::with_engine(engine);
        let expected = TestBaseline::deterministic();
        let apply_calls = AtomicUsize::new(0);

        let outcome = service
            .match_or_fallback(
                AnimeDeterministicResult::difficult(expected.clone()),
                tokyo_ghoul_batch(),
                |_, _, _, _| {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    bail!("mixed response must not reach coverage application")
                },
            )
            .await;

        assert_eq!(outcome.value, expected);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            outcome.provenance.reason,
            Some(AnimeMatchFallbackReason::InvalidModelResponse)
        );
    }

    #[tokio::test]
    async fn alm5_valid_override_preserves_order_none_and_repeated_alternative_targets() {
        let response = AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![
                candidate_match("candidate-1", None),
                candidate_match("candidate-0", Some("candidate-0-file-0")),
            ],
        };
        let engine = StaticEngine::responding(response);
        let service = AnimeMatchingService::with_engine(engine);

        let outcome = service
            .match_or_fallback(
                AnimeDeterministicResult::difficult(TestBaseline::deterministic()),
                tokyo_ghoul_batch(),
                |baseline, request, matches, source_map| {
                    ensure!(baseline.canonical_selection == "S01E01");
                    ensure!(request.target.wanted_target_keys == vec!["S02E01".to_string()]);
                    ensure!(matches[0].candidate_key == "candidate-1");
                    ensure!(matches[1].candidate_key == "candidate-0");
                    ensure!(matches[0].selected_file_keys.is_none());
                    ensure!(
                        source_map
                            .candidate_source("candidate-1")
                            .map(String::as_str)
                            == Some("provider-candidate-b")
                    );
                    Ok(TestBaseline {
                        canonical_selection: "S02E01".to_string(),
                        coverage: vec!["S02E01".to_string()],
                        selected_files: vec!["provider-file-a".to_string()],
                        route_eligible: true,
                        persistence_decision: "apply".to_string(),
                    })
                },
            )
            .await;

        assert!(outcome.used_model());
        assert_eq!(outcome.value.canonical_selection, "S02E01");
        assert_eq!(outcome.matches[0].candidate_key, "candidate-1");
        assert!(outcome.matches[0].selected_file_keys.is_none());
    }

    #[tokio::test]
    async fn alm6_success_preserves_the_invocation_runtime_provenance() {
        let expected_runtime = AnimeMatchRuntimeProvenance {
            bundle_version: "2026.08.1".to_string(),
            model_id: "qwen3-4b-instruct-2507".to_string(),
            model_revision: "elixir-q4km-r1".to_string(),
            worker_revision: "llama-b9637".to_string(),
            backend: "metal_cpu".to_string(),
            profile_fingerprint: "sha256:profile".to_string(),
            prompt_revision: "anime-match-v1".to_string(),
            protocol_version: 1,
        };
        let engine = Arc::new(ProvenanceEngine {
            response: AnimeMatchResponse {
                schema_version: ANIME_MATCH_SCHEMA_VERSION,
                matches: vec![candidate_match("candidate-0", Some("candidate-0-file-0"))],
            },
            runtime: expected_runtime.clone(),
        });

        let outcome = AnimeMatchingService::with_engine(engine)
            .match_or_fallback(
                AnimeDeterministicResult::difficult(TestBaseline::deterministic()),
                tokyo_ghoul_batch(),
                |_, _, _, _| Ok(TestBaseline::deterministic()),
            )
            .await;

        assert_eq!(outcome.provenance.runtime, Some(expected_runtime));
    }
}
