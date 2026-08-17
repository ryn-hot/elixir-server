//! Native ALM-9 release-qualification harness.
//!
//! This is deliberately a release-engineering API, not a product setting. It
//! runs a frozen corpus through the installed `llama-server` worker and the
//! same matcher, coverage, language, and fallback boundaries used by anime
//! acquisition, then emits evidence for the independent Python scorer.

pub mod corpus_compiler;
pub mod training_dataset;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeOwned, Error as DeError, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::{
    acquisition::{
        anime_matching::{
            AcquisitionAnimeCandidateSource, AcquisitionAnimeFileSource,
            acquisition_anime_deterministic_state, acquisition_candidate_language_evidence,
            acquisition_candidate_parse_facts, acquisition_match_context,
            acquisition_model_audio_profile_evidence, bind_exact_single_anime_provider_file,
            bind_exact_single_anime_provider_file_with_semantic_evidence,
            model_derived_anime_coverage_plans_with_selection_resolver,
            selectable_anime_media_file,
        },
        automation::{
            anime_semantic_candidate_evidence, anime_semantic_media_kinds,
            synthetic_stream_candidate_requires_manual_review,
        },
        language_policy::{
            AcquisitionLanguagePreference, LanguagePreferenceAssessment,
            LanguagePreferenceAssessmentState, LanguagePreferenceMediaRule, LanguagePreferenceMode,
            UnknownLanguagePolicy, assess_language_preference,
        },
        release_resolution::{
            anime::{
                ANIME_SHOKO_STYLE_RESOLVER_VERSION, AnimeCandidateInput,
                AnimeCandidateScoringContext, AnimeCoverageOptions, AnimeFileCoveragePlan,
                AnimeReleaseFileInput, plan_anime_file_coverage_with_options,
                plan_anime_file_coverage_with_semantic_evidence,
            },
            models::ReleaseConfidence,
        },
    },
    anime_matching::{
        ANIME_MATCH_MAX_CANDIDATES, ANIME_MATCH_PROMPT_REVISION,
        ANIME_MATCH_RESPONSE_SCHEMA_REVISION, ANIME_MATCH_SAMPLING_REVISION,
        ANIME_MATCH_SCHEMA_VERSION, AnimeCandidateMatch, AnimeDeterministicResult,
        AnimeExecutionBackend, AnimeInferenceBundleManifest, AnimeKvCacheType,
        AnimeMatchAssistResult, AnimeMatchAudioPreference, AnimeMatchAudioPreferenceMode,
        AnimeMatchAudioProfile, AnimeMatchBatchInput, AnimeMatchCandidateInput, AnimeMatchEngine,
        AnimeMatchFallbackReason, AnimeMatchFileInput, AnimeMatchRequest, AnimeMatchResponse,
        AnimeMatchSourceMap, AnimeMatchingService, AnimeRuntimeArtifactManifest,
        AnimeRuntimeBackend, AnimeRuntimeProbeResult, AnimeRuntimeProfile,
        AnimeSemanticEvidenceEngine, AnimeSemanticEvidenceResponse, DeterministicMatchState,
        InferenceProbeLimits, LocalModelEngine, LocalModelRuntimeProfile,
        LocalModelSamplingProfile, PreparedAnimeMatchRequest, build_semantic_evidence_request,
        collect_inference_hardware_inventory, extract_anime_runtime_for_qualification,
        inference_hardware_fingerprint, validate_anime_match_request,
        validate_anime_match_response, validate_semantic_evidence_response,
    },
    db::models::MediaType,
    http::handlers::acquisition_sources::AcquisitionCandidate,
    playback::hardware::collect_host_hardware_inventory,
};

#[cfg(test)]
use crate::acquisition::language_policy::CandidateLanguageEvidence;

const MAX_QUALIFICATION_JSON_BYTES: u64 = 64 * 1024 * 1024;
const EXPECTED_CASE_COUNT: usize = 520;
const CLEAN_VALIDATION_DIAGNOSTIC_CASE_COUNT: usize = 640;
const QUALIFICATION_CORPUS_SCHEMA_VERSION: u32 = 2;
const QUALIFICATION_OUTPUT_SCHEMA_VERSION: u32 = 2;
const QUALIFICATION_REPORT_SCHEMA_VERSION: u32 = 3;
const QUALIFICATION_SCORER_REVISION: &str = "alm9-qualification-v5-frozen-label-errata";
const SHA256_PREFIX: &str = "sha256:";
const FILE_HASH_BUFFER_BYTES: usize = 1024 * 1024;
const LOCAL_MODEL_CONTRACT_SOURCE: &str = include_str!("../anime_matching/local_model.rs");
const FAILURE_MODES: [&str; 4] = ["unavailable", "timeout", "invalid", "empty"];
const PLAN_FIELDS: [(&str, PlanField); 5] = [
    ("disposition", PlanField::Disposition),
    ("seasonNumber", PlanField::SeasonNumber),
    ("episodeNumbers", PlanField::EpisodeNumbers),
    ("absoluteEpisodeNumbers", PlanField::AbsoluteEpisodeNumbers),
    ("candidatePlans", PlanField::CandidatePlans),
];

/// Exact local inputs needed to produce one immutable qualification run.
/// Paths are supplied by release automation; none are product configuration.
#[derive(Debug, Clone)]
pub struct AnimeQualificationRunConfig {
    pub corpus_path: PathBuf,
    pub identity_path: PathBuf,
    pub manifest_path: PathBuf,
    pub runtime_profile_path: PathBuf,
    pub model_path: PathBuf,
    pub runtime_artifact_path: PathBuf,
    pub runtime_source_lock_path: PathBuf,
    pub scorer_path: PathBuf,
    pub gpu_preflight_evidence_path: Option<PathBuf>,
    pub output_path: PathBuf,
}

/// Physical combined-path corpus run. Unlike the retired qualification
/// adapter, this asks the model only to select one server-authored semantic
/// hypothesis and lets the deterministic resolver author the final plan.
#[derive(Debug, Clone)]
pub struct AnimeSemanticCorpusRunConfig {
    pub corpus_path: PathBuf,
    pub manifest_path: PathBuf,
    pub runtime_profile_path: PathBuf,
    pub model_path: PathBuf,
    pub runtime_artifact_path: PathBuf,
    pub output_path: PathBuf,
    /// Server-owned prompt projection paired with the frozen selector under
    /// test. Product activation continues to use the current server revision.
    pub semantic_prompt_revision: String,
    /// Diagnostic iteration mode: execute inference only for cases the current
    /// deterministic production path does not already solve exactly.
    pub baseline_failures_only: bool,
}

/// Model-free replay of one retained semantic corpus report through the current
/// anime resolver. The recorded index-or-null decisions are immutable inputs;
/// only deterministic acceptance and coverage code can affect the result.
#[derive(Debug, Clone)]
pub struct AnimeSemanticCorpusReplayConfig {
    pub corpus_path: PathBuf,
    pub recorded_report_path: PathBuf,
    pub output_path: PathBuf,
    pub baseline_failures_only: bool,
}

/// Model-free corpus replay used while changing deterministic parsing and
/// coverage. It intentionally shares the exact corpus decoder, production
/// adapters, planner, and final-plan scorer with the physical semantic run.
#[derive(Debug, Clone)]
pub struct AnimeSemanticBaselineRunConfig {
    pub corpus_path: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeSemanticCorpusRunSummary {
    pub status: String,
    pub corpus_id: String,
    pub baseline_failures_only: bool,
    pub case_count: usize,
    pub passed: usize,
    pub failed: usize,
    pub baseline_passed: usize,
    pub recovered: usize,
    pub regressed: usize,
    pub selector_invocations: usize,
    pub accepted_evidence: usize,
    pub expectation_audit_cases: usize,
    pub rejection_reason_counts: BTreeMap<String, usize>,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeQualificationRunSummary {
    pub status: String,
    pub corpus_id: String,
    pub case_count: usize,
    pub output_path: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationCorpus {
    schema_version: u32,
    status: String,
    corpus_id: String,
    #[serde(default)]
    profile: QualificationCorpusProfile,
    sets: QualificationSets,
    cases: Vec<QualificationCase>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum QualificationCorpusProfile {
    #[default]
    Certification520,
    CleanValidationDiagnosticV1,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QualificationSets {
    smoke: Vec<String>,
    development: Vec<String>,
    frozen: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationCase {
    case_id: String,
    set: String,
    slice: Option<String>,
    origin: String,
    realistic_noise: bool,
    counterfactual_pair_id: Option<String>,
    counterfactual_mutation: Option<JsonValue>,
    stability_subset: bool,
    deterministic_easy: bool,
    input_fingerprint: String,
    input: JsonValue,
    allowed_references: AllowedReferences,
    expected_final_plan: QualificationFinalPlan,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationCaseInput {
    request: AnimeMatchRequest,
    scoring_context: AnimeCandidateScoringContext,
    acquisition_candidates: Vec<AcquisitionCandidate>,
    route_context: QualificationRouteContext,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationRouteContext {
    file_selection_supported_by_candidate_key: BTreeMap<String, bool>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AllowedReferences {
    candidate_keys: Vec<String>,
    target_keys: Vec<String>,
    file_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationIdentity {
    manifest_fingerprint: String,
    runtime_profile_fingerprint: String,
    runtime_profile_sha256: String,
    runtime_profile_size_bytes: u64,
    model_sha256: String,
    model_size_bytes: u64,
    model_revision: String,
    qualification_runtime_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    certification_target_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    case_selection: Option<QualificationCaseSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gpu_preflight_evidence: Option<QualificationGpuPreflightEvidence>,
    runtime_artifact_sha256: String,
    runtime_source_lock_sha256: String,
    worker_revision: String,
    prompt_revision: String,
    prompt_fingerprint: String,
    response_schema_revision: String,
    response_schema_fingerprint: String,
    protocol_version: u32,
    matcher_schema_version: u32,
    sampling_profile_revision: String,
    sampling_profile_fingerprint: String,
    context_tokens: u32,
    max_output_tokens: u32,
    candidate_cap: usize,
    candidate_order_seeds: Vec<u64>,
    qualification_corpus_schema_version: u32,
    qualification_output_schema_version: u32,
    qualification_report_schema_version: u32,
    corpus_sha256: String,
    corpus_size_bytes: u64,
    scorer_revision: String,
    scorer_sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationCaseSelection {
    selection_id: String,
    case_ids: Vec<String>,
    case_ids_fingerprint: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationGpuPreflightEvidence {
    target_id: String,
    evidence_sha256: String,
    evidence_size_bytes: u64,
    evidence_fingerprint: String,
    gpu_uuids: Vec<String>,
    host_container_parity: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum QualificationDisposition {
    Matched,
    NoMatch,
    Unresolved,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum QualificationAudioEligibility {
    Eligible,
    Ineligible,
    Unknown,
    NotApplicable,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum QualificationCoverageStatus {
    Covered,
    Missing,
    Ineligible,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationCoverageEntry {
    target_key: String,
    file_key: Option<String>,
    status: QualificationCoverageStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationFinalPlan {
    disposition: QualificationDisposition,
    season_number: Option<i32>,
    episode_numbers: Vec<i32>,
    absolute_episode_numbers: Vec<i32>,
    candidate_plans: Vec<QualificationCandidatePlan>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QualificationCandidatePlan {
    candidate_key: String,
    target_keys: Vec<String>,
    file_keys: Vec<String>,
    audio_eligibility: QualificationAudioEligibility,
    coverage: Vec<QualificationCoverageEntry>,
}

#[derive(Debug, Clone, Copy)]
enum PlanField {
    Disposition,
    SeasonNumber,
    EpisodeNumbers,
    AbsoluteEpisodeNumbers,
    CandidatePlans,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QualificationPlanDiff {
    matches: bool,
    mismatched_fields: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QualificationObservation {
    order_seed: u64,
    candidate_order: Vec<String>,
    request_fingerprint: String,
    model_decision: String,
    fallback_reason: Option<String>,
    model_output: JsonValue,
    model_output_sha256: String,
    reference_validation_passed: bool,
    final_plan: QualificationFinalPlan,
}

#[derive(Debug, Clone)]
struct QualificationResolutionState {
    candidate_plans: Vec<Option<QualificationCandidatePlan>>,
    saw_partial_or_ambiguous: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QualificationCaseOutput {
    case_id: String,
    input_fingerprint: String,
    model_decision: String,
    fallback_reason: Option<String>,
    model_output: JsonValue,
    model_output_sha256: String,
    reference_validation_passed: bool,
    baseline_final_plan: QualificationFinalPlan,
    final_plan: QualificationFinalPlan,
    final_plan_diff: QualificationPlanDiff,
    stability_runs: Vec<QualificationObservation>,
    failure_fallback_plans: BTreeMap<String, QualificationFinalPlan>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QualificationOutput {
    schema_version: u32,
    status: String,
    identity: QualificationIdentity,
    cases: Vec<QualificationCaseOutput>,
    skipped_checks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QualificationCandidateSource {
    candidate_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QualificationFileSource {
    candidate_index: usize,
    file_index: usize,
}

type QualificationPreparedRequest =
    PreparedAnimeMatchRequest<QualificationCandidateSource, QualificationFileSource>;

#[derive(Clone)]
enum InjectedEngine {
    Error(&'static str),
    Response(AnimeMatchResponse),
}

#[async_trait]
impl AnimeMatchEngine for InjectedEngine {
    async fn match_candidates(&self, _request: AnimeMatchRequest) -> Result<AnimeMatchResponse> {
        match self {
            Self::Error(detail) => Err(anyhow!(*detail)),
            Self::Response(response) => Ok(response.clone()),
        }
    }
}

struct PreparedQualificationRuntime {
    profile: LocalModelRuntimeProfile,
    extraction: tempfile::TempDir,
}

/// Run the complete frozen corpus, or the lock-bound cross-runtime selection,
/// and write a new schema-v2 evidence artifact.
/// The destination is never overwritten.
pub async fn run_anime_inference_qualification(
    mut config: AnimeQualificationRunConfig,
) -> Result<AnimeQualificationRunSummary> {
    config.model_path = canonical_regular_file(&config.model_path, "model")?;
    let corpus_bytes = read_limited(&config.corpus_path, "qualification corpus")?;
    let corpus: QualificationCorpus =
        serde_json::from_value(parse_strict_json(&corpus_bytes, "qualification corpus")?)
            .context("decoding qualification corpus")?;
    validate_corpus_shape(&corpus, &corpus_bytes)?;

    let identity: QualificationIdentity = read_json(&config.identity_path, "identity")?;
    let corpus_case_ids = corpus
        .cases
        .iter()
        .map(|case| case.case_id.clone())
        .collect::<Vec<_>>();
    let selected_case_ids = validate_case_selection(
        &corpus_case_ids,
        identity.certification_target_id.as_deref(),
        identity.case_selection.as_ref(),
    )?;
    let manifest_bytes = read_limited(&config.manifest_path, "bundle manifest")?;
    let manifest_value = parse_strict_json(&manifest_bytes, "bundle manifest")?;
    let manifest: AnimeInferenceBundleManifest =
        serde_json::from_value(manifest_value.clone()).context("validating bundle manifest")?;
    let runtime_profile_bytes = read_limited(&config.runtime_profile_path, "runtime profile")?;
    let probe_profile: AnimeRuntimeProfile = serde_json::from_value(parse_strict_json(
        &runtime_profile_bytes,
        "runtime profile",
    )?)
    .context("decoding runtime profile")?;
    let current_host = collect_host_hardware_inventory().await;
    let current_hardware = collect_inference_hardware_inventory(current_host).await;
    let current_hardware_fingerprint = inference_hardware_fingerprint(&current_hardware);
    let PreparedQualificationRuntime {
        profile,
        extraction: runtime_extraction,
    } = validate_run_identity(
        &identity,
        &corpus_bytes,
        &manifest,
        manifest_value,
        &probe_profile,
        &runtime_profile_bytes,
        &current_hardware_fingerprint,
        &config,
    )
    .await?;

    let engine = LocalModelEngine::allow_all_for_probe()?;
    engine.activate_profile_for_probe(profile).await?;
    engine
        .prime()
        .await
        .context("priming exact qualification worker")?;
    let result = run_cases(&corpus, &identity, selected_case_ids.as_ref(), &engine).await;
    engine.shutdown().await;
    drop(runtime_extraction);
    let cases = result?;
    let output = QualificationOutput {
        schema_version: QUALIFICATION_OUTPUT_SCHEMA_VERSION,
        status: "complete".to_string(),
        identity,
        cases,
        skipped_checks: Vec::new(),
    };
    let case_count = output.cases.len();
    write_new_canonical_json(&config.output_path, &output)?;
    Ok(AnimeQualificationRunSummary {
        status: "complete".to_string(),
        corpus_id: corpus.corpus_id,
        case_count,
        output_path: config.output_path,
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AnimeSemanticCorpusReport {
    schema_version: u32,
    status: String,
    corpus_id: String,
    model_id: String,
    model_revision: String,
    prompt_revision: String,
    backend: String,
    profile_fingerprint: String,
    summary: AnimeSemanticCorpusRunSummary,
    cases: Vec<AnimeSemanticCorpusCaseObservation>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AnimeSemanticCorpusCaseObservation {
    case_id: String,
    set: String,
    slice: Option<String>,
    origin: String,
    deterministic_definitive: bool,
    baseline_matches_expected: bool,
    final_matches_expected: bool,
    changed: bool,
    recovered: bool,
    regressed: bool,
    selector_invocations: usize,
    selector_selections: usize,
    selector_abstentions: usize,
    selector_errors: usize,
    accepted_evidence: usize,
    rejected_evidence: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expectation_audit: Option<String>,
    selected_hypotheses: Vec<AnimeSemanticCorpusSelection>,
    baseline_final_plan: QualificationFinalPlan,
    final_plan: QualificationFinalPlan,
    expected_final_plan: QualificationFinalPlan,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AnimeSemanticCorpusSelection {
    candidate_key: String,
    hypothesis_index: usize,
    accepted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rejection_reasons: Vec<String>,
}

#[derive(Default)]
struct SemanticCaseCounters {
    selector_invocations: usize,
    selector_selections: usize,
    selector_abstentions: usize,
    selector_errors: usize,
    accepted_evidence: usize,
    rejected_evidence: usize,
    rejection_reason_counts: BTreeMap<String, usize>,
    selected_hypotheses: Vec<AnimeSemanticCorpusSelection>,
}

/// Run the frozen corpus through the current combined production architecture:
/// deterministic fast path, bounded index-or-null semantic evidence, the same
/// deterministic coverage planner, and unchanged fallback on every failure.
pub async fn run_anime_semantic_corpus(
    mut config: AnimeSemanticCorpusRunConfig,
) -> Result<AnimeSemanticCorpusRunSummary> {
    config.model_path = canonical_regular_file(&config.model_path, "model")?;
    let corpus_bytes = read_limited(&config.corpus_path, "qualification corpus")?;
    let corpus: QualificationCorpus =
        serde_json::from_value(parse_strict_json(&corpus_bytes, "qualification corpus")?)
            .context("decoding qualification corpus")?;
    validate_corpus_shape(&corpus, &corpus_bytes)?;

    let manifest: AnimeInferenceBundleManifest = read_json(&config.manifest_path, "manifest")?;
    let runtime_profile: AnimeRuntimeProfile =
        read_json(&config.runtime_profile_path, "runtime profile")?;
    runtime_profile.validate()?;
    ensure!(
        runtime_profile.bundle_version == manifest.bundle_version
            && runtime_profile.model_id == manifest.model.id
            && runtime_profile.model_revision == manifest.model.revision
            && runtime_profile.worker_revision == manifest.worker_revision,
        "runtime profile does not identify the supplied manifest"
    );
    let runtime = manifest
        .runtimes
        .iter()
        .find(|runtime| runtime.artifact_key() == runtime_profile.runtime_artifact_key)
        .context("runtime profile artifact is absent from the manifest")?;
    let extraction = tempfile::Builder::new()
        .prefix("elixir-anime-semantic-corpus-")
        .tempdir()?;
    let worker_path = extract_anime_runtime_for_qualification(
        &config.runtime_artifact_path,
        &extraction.path().join("runtime"),
        runtime,
    )
    .await?;
    let local_profile = LocalModelRuntimeProfile {
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        backend: runtime_profile.execution_backend.as_str().to_string(),
        profile_fingerprint: runtime_profile.profile_fingerprint.clone(),
        protocol_version: manifest.protocol_version,
        matcher_schema_version: manifest.matcher_schema_version,
        prompt_revision: config.semantic_prompt_revision.clone(),
        worker_path,
        model_path: config.model_path.clone(),
        context_tokens: manifest.model.context_tokens,
        max_output_tokens: manifest.model.max_output_tokens,
        threads: u32::from(runtime_profile.cpu_thread_count),
        batch_threads: u32::from(runtime_profile.batch_thread_count),
        gpu_layers: runtime_profile.gpu_layer_count,
        kv_cache_type: match runtime_profile.kv_cache_type {
            AnimeKvCacheType::F16 => "f16",
            AnimeKvCacheType::Q8_0 => "q8_0",
        }
        .to_string(),
        peak_rss_bytes: runtime_profile.peak_rss_bytes,
        idle_unload_seconds: manifest.runtime_policy.idle_unload_seconds,
        sampling: LocalModelSamplingProfile::default(),
    };
    local_profile.validate_contract()?;

    let engine = LocalModelEngine::allow_all_for_probe()?;
    engine.activate_profile_for_probe(local_profile).await?;
    let result = run_semantic_corpus_cases(&corpus, &engine, config.baseline_failures_only).await;
    engine.shutdown().await;
    drop(extraction);
    let cases = result?;
    finish_semantic_corpus_report(
        corpus.corpus_id,
        cases,
        config.baseline_failures_only,
        SemanticCorpusReportIdentity {
            model_id: manifest.model.id,
            model_revision: manifest.model.revision,
            prompt_revision: config.semantic_prompt_revision,
            backend: runtime_profile.execution_backend.as_str().to_string(),
            profile_fingerprint: runtime_profile.profile_fingerprint,
        },
        config.output_path,
    )
}

#[derive(Debug)]
struct SemanticCorpusReportIdentity {
    model_id: String,
    model_revision: String,
    prompt_revision: String,
    backend: String,
    profile_fingerprint: String,
}

fn finish_semantic_corpus_report(
    corpus_id: String,
    cases: Vec<AnimeSemanticCorpusCaseObservation>,
    baseline_failures_only: bool,
    identity: SemanticCorpusReportIdentity,
    output_path: PathBuf,
) -> Result<AnimeSemanticCorpusRunSummary> {
    let passed = cases
        .iter()
        .filter(|case| case.final_matches_expected)
        .count();
    let baseline_passed = cases
        .iter()
        .filter(|case| case.baseline_matches_expected)
        .count();
    let recovered = cases.iter().filter(|case| case.recovered).count();
    let regressed = cases.iter().filter(|case| case.regressed).count();
    let selector_invocations = cases.iter().map(|case| case.selector_invocations).sum();
    let accepted_evidence = cases.iter().map(|case| case.accepted_evidence).sum();
    let expectation_audit_cases = cases
        .iter()
        .filter(|case| case.expectation_audit.is_some())
        .count();
    let mut rejection_reason_counts = BTreeMap::new();
    for selection in cases
        .iter()
        .flat_map(|case| case.selected_hypotheses.iter())
    {
        for reason in &selection.rejection_reasons {
            *rejection_reason_counts.entry(reason.clone()).or_insert(0) += 1;
        }
    }
    let summary = AnimeSemanticCorpusRunSummary {
        status: if passed == cases.len() {
            "passed".to_string()
        } else {
            "failed".to_string()
        },
        corpus_id: corpus_id.clone(),
        baseline_failures_only,
        case_count: cases.len(),
        passed,
        failed: cases.len() - passed,
        baseline_passed,
        recovered,
        regressed,
        selector_invocations,
        accepted_evidence,
        expectation_audit_cases,
        rejection_reason_counts,
        output_path: output_path.clone(),
    };
    let report = AnimeSemanticCorpusReport {
        schema_version: 1,
        status: summary.status.clone(),
        corpus_id,
        model_id: identity.model_id,
        model_revision: identity.model_revision,
        prompt_revision: identity.prompt_revision,
        backend: identity.backend,
        profile_fingerprint: identity.profile_fingerprint,
        summary: summary.clone(),
        cases,
    };
    write_new_canonical_json(&output_path, &report)?;
    Ok(summary)
}

struct RecordedSemanticEvidenceEngine {
    request_ids: BTreeSet<String>,
    selected_hypotheses: BTreeMap<(String, String), usize>,
}

#[async_trait]
impl AnimeSemanticEvidenceEngine for RecordedSemanticEvidenceEngine {
    async fn select_hypothesis(
        &self,
        request: crate::anime_matching::AnimeSemanticEvidenceRequest,
    ) -> Result<AnimeSemanticEvidenceResponse> {
        ensure!(
            self.request_ids.contains(&request.request_id),
            "recorded semantic replay has no request {}",
            request.request_id
        );
        Ok(AnimeSemanticEvidenceResponse {
            schema_version: request.schema_version,
            hypothesis_index: self
                .selected_hypotheses
                .get(&(request.request_id, request.candidate_key))
                .copied(),
        })
    }
}

/// Replay one retained model run through the current deterministic anime path.
/// This never starts a worker and never changes the model-visible contract.
pub async fn run_anime_semantic_corpus_replay(
    config: AnimeSemanticCorpusReplayConfig,
) -> Result<AnimeSemanticCorpusRunSummary> {
    let corpus_bytes = read_limited(&config.corpus_path, "qualification corpus")?;
    let corpus: QualificationCorpus =
        serde_json::from_value(parse_strict_json(&corpus_bytes, "qualification corpus")?)
            .context("decoding qualification corpus")?;
    validate_corpus_shape(&corpus, &corpus_bytes)?;

    let recorded: AnimeSemanticCorpusReport = read_json(
        &config.recorded_report_path,
        "recorded semantic corpus report",
    )?;
    ensure!(
        recorded.schema_version == 1,
        "unsupported recorded report schema"
    );
    ensure!(
        recorded.corpus_id == corpus.corpus_id,
        "recorded report identifies a different corpus"
    );
    ensure!(
        recorded.cases.len() == corpus.cases.len(),
        "recorded report case count differs from the corpus"
    );
    ensure!(
        recorded.cases.iter().all(|case| case.selector_errors == 0),
        "recorded report contains selector errors that cannot be replayed exactly"
    );

    let recorded_cases = recorded
        .cases
        .iter()
        .map(|case| (case.case_id.as_str(), case))
        .collect::<BTreeMap<_, _>>();
    let mut request_ids = BTreeSet::new();
    let mut selected_hypotheses = BTreeMap::new();
    for case in &corpus.cases {
        let input: QualificationCaseInput = serde_json::from_value(case.input.clone())
            .with_context(|| format!("decoding input for replay case {}", case.case_id))?;
        let recorded_case = recorded_cases
            .get(case.case_id.as_str())
            .with_context(|| format!("recorded report lacks case {}", case.case_id))?;
        ensure!(
            request_ids.insert(input.request.request_id.clone()),
            "qualification corpus repeats request ID {}",
            input.request.request_id
        );
        for selection in &recorded_case.selected_hypotheses {
            ensure!(
                input
                    .request
                    .candidates
                    .iter()
                    .any(|candidate| candidate.candidate_key == selection.candidate_key),
                "recorded case {} selects unknown candidate {}",
                case.case_id,
                selection.candidate_key
            );
            ensure!(
                selected_hypotheses
                    .insert(
                        (
                            input.request.request_id.clone(),
                            selection.candidate_key.clone(),
                        ),
                        selection.hypothesis_index,
                    )
                    .is_none(),
                "recorded case {} selects candidate {} more than once",
                case.case_id,
                selection.candidate_key
            );
        }
    }
    let engine = RecordedSemanticEvidenceEngine {
        request_ids,
        selected_hypotheses,
    };
    let cases = run_semantic_corpus_cases(&corpus, &engine, config.baseline_failures_only).await?;
    finish_semantic_corpus_report(
        corpus.corpus_id,
        cases,
        config.baseline_failures_only,
        SemanticCorpusReportIdentity {
            model_id: recorded.model_id,
            model_revision: recorded.model_revision,
            prompt_revision: recorded.prompt_revision,
            backend: "recorded-replay".to_string(),
            profile_fingerprint: recorded.profile_fingerprint,
        },
        config.output_path,
    )
}

/// Evaluate the deterministic production path across the frozen corpus without
/// loading a worker or model. This makes parser and file-coverage iteration
/// cheap and ensures GPU runs are reserved for genuine semantic evaluation.
pub fn run_anime_semantic_baseline(
    config: AnimeSemanticBaselineRunConfig,
) -> Result<AnimeSemanticCorpusRunSummary> {
    let corpus_bytes = read_limited(&config.corpus_path, "qualification corpus")?;
    let corpus: QualificationCorpus =
        serde_json::from_value(parse_strict_json(&corpus_bytes, "qualification corpus")?)
            .context("decoding qualification corpus")?;
    validate_corpus_shape(&corpus, &corpus_bytes)?;

    let mut cases = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let input: QualificationCaseInput = serde_json::from_value(case.input.clone())
            .with_context(|| format!("decoding input for semantic case {}", case.case_id))?;
        let input = current_semantic_case_input(case, input)?;
        let resolution = deterministic_baseline(&input)?;
        let final_plan = final_plan_for_resolution(&input.request, &resolution)?;
        let matches_expected = semantic_final_plans_match(&final_plan, &case.expected_final_plan);
        let deterministic_definitive = deterministic_union_state(&input.request, &resolution)
            == DeterministicMatchState::Definitive;
        let expectation_audit =
            semantic_expectation_audit(case, deterministic_definitive, &final_plan);
        cases.push(AnimeSemanticCorpusCaseObservation {
            case_id: case.case_id.clone(),
            set: case.set.clone(),
            slice: case.slice.clone(),
            origin: case.origin.clone(),
            deterministic_definitive,
            baseline_matches_expected: matches_expected,
            final_matches_expected: matches_expected,
            changed: false,
            recovered: false,
            regressed: false,
            selector_invocations: 0,
            selector_selections: 0,
            selector_abstentions: 0,
            selector_errors: 0,
            accepted_evidence: 0,
            rejected_evidence: 0,
            expectation_audit,
            selected_hypotheses: Vec::new(),
            baseline_final_plan: final_plan.clone(),
            final_plan,
            expected_final_plan: case.expected_final_plan.clone(),
        });
    }

    let passed = cases
        .iter()
        .filter(|case| case.final_matches_expected)
        .count();
    let summary = AnimeSemanticCorpusRunSummary {
        status: if passed == cases.len() {
            "passed".to_string()
        } else {
            "failed".to_string()
        },
        corpus_id: corpus.corpus_id.clone(),
        baseline_failures_only: false,
        case_count: cases.len(),
        passed,
        failed: cases.len() - passed,
        baseline_passed: passed,
        recovered: 0,
        regressed: 0,
        selector_invocations: 0,
        accepted_evidence: 0,
        expectation_audit_cases: cases
            .iter()
            .filter(|case| case.expectation_audit.is_some())
            .count(),
        rejection_reason_counts: BTreeMap::new(),
        output_path: config.output_path.clone(),
    };
    let report = AnimeSemanticCorpusReport {
        schema_version: 1,
        status: summary.status.clone(),
        corpus_id: corpus.corpus_id,
        model_id: "deterministic".to_string(),
        model_revision: ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
        backend: "none".to_string(),
        profile_fingerprint: "deterministic".to_string(),
        summary: summary.clone(),
        cases,
    };
    write_new_canonical_json(&config.output_path, &report)?;
    Ok(summary)
}

async fn run_semantic_corpus_cases(
    corpus: &QualificationCorpus,
    engine: &dyn AnimeSemanticEvidenceEngine,
    baseline_failures_only: bool,
) -> Result<Vec<AnimeSemanticCorpusCaseObservation>> {
    let mut observations = Vec::with_capacity(corpus.cases.len());
    for (index, case) in corpus.cases.iter().enumerate() {
        let started = Instant::now();
        let input: QualificationCaseInput = serde_json::from_value(case.input.clone())
            .with_context(|| format!("decoding input for semantic case {}", case.case_id))?;
        let input = current_semantic_case_input(case, input)?;
        let baseline_resolution = deterministic_baseline(&input)?;
        let baseline = final_plan_for_resolution(&input.request, &baseline_resolution)?;
        let baseline_matches_expected =
            semantic_final_plans_match(&baseline, &case.expected_final_plan);
        if baseline_failures_only && baseline_matches_expected {
            continue;
        }
        let deterministic_definitive =
            deterministic_union_state(&input.request, &baseline_resolution)
                == DeterministicMatchState::Definitive;
        let mut combined = baseline_resolution;
        let mut counters = SemanticCaseCounters::default();

        if !deterministic_definitive {
            for (candidate_index, candidate) in input.acquisition_candidates.iter().enumerate() {
                if combined.candidate_plans[candidate_index].is_some() {
                    continue;
                }
                let request_candidate = input
                    .request
                    .candidates
                    .get(candidate_index)
                    .ok_or_else(|| anyhow!("semantic corpus candidate cardinality mismatch"))?;
                let candidate_input = anime_candidate_input(candidate);
                let files = qualification_release_files(request_candidate, candidate)?;
                let deterministic_plan = bind_exact_single_anime_provider_file(
                    plan_anime_file_coverage_with_options(
                        &input.scoring_context,
                        &candidate_input,
                        &files,
                        AnimeCoverageOptions {
                            file_selection_supported: *input
                                .route_context
                                .file_selection_supported_by_candidate_key
                                .get(&request_candidate.candidate_key)
                                .context("route context lacks candidate file-selection key")?,
                        },
                    ),
                    &input.scoring_context,
                    &candidate_input,
                    &files,
                );
                if acquisition_anime_deterministic_state(&deterministic_plan)
                    == DeterministicMatchState::Definitive
                    && plan_definitively_excludes_wanted_targets(
                        &input.request,
                        &deterministic_plan,
                    )
                {
                    continue;
                }

                let facts = &request_candidate.parse_facts;
                let semantic_request = build_semantic_evidence_request(
                    &input.request,
                    request_candidate.candidate_key.clone(),
                    request_candidate.title.clone(),
                    None,
                    facts.title_candidates.iter().cloned(),
                    facts.season_numbers.iter().copied(),
                    facts.episode_numbers.iter().copied(),
                    facts.absolute_episode_numbers.iter().copied(),
                    anime_semantic_media_kinds(&request_candidate.title, facts),
                )?;
                let Some(semantic_request) = semantic_request else {
                    continue;
                };
                counters.selector_invocations += 1;
                let output = match engine
                    .select_hypothesis_with_provenance(semantic_request.clone())
                    .await
                {
                    Ok(output) => output,
                    Err(_) => {
                        counters.selector_errors += 1;
                        continue;
                    }
                };
                let hypothesis = match validate_semantic_evidence_response(
                    &semantic_request,
                    &output.response,
                ) {
                    Ok(Some(hypothesis)) => hypothesis,
                    Ok(None) => {
                        counters.selector_abstentions += 1;
                        continue;
                    }
                    Err(_) => {
                        counters.selector_errors += 1;
                        continue;
                    }
                };
                counters.selector_selections += 1;
                let Some(evidence) =
                    anime_semantic_candidate_evidence(&semantic_request, hypothesis)
                else {
                    counters.rejected_evidence += 1;
                    let reason = "canonical_entity_unavailable".to_string();
                    *counters
                        .rejection_reason_counts
                        .entry(reason.clone())
                        .or_insert(0) += 1;
                    counters
                        .selected_hypotheses
                        .push(AnimeSemanticCorpusSelection {
                            candidate_key: request_candidate.candidate_key.clone(),
                            hypothesis_index: hypothesis.index,
                            accepted: false,
                            rejection_reasons: vec![reason],
                        });
                    continue;
                };
                let semantic_plan = plan_anime_file_coverage_with_semantic_evidence(
                    &input.scoring_context,
                    &candidate_input,
                    &files,
                    AnimeCoverageOptions {
                        file_selection_supported: *input
                            .route_context
                            .file_selection_supported_by_candidate_key
                            .get(&request_candidate.candidate_key)
                            .context("route context lacks candidate file-selection key")?,
                    },
                    &evidence,
                )
                .map(|plan| {
                    bind_exact_single_anime_provider_file_with_semantic_evidence(
                        plan,
                        &input.scoring_context,
                        &candidate_input,
                        &files,
                        &evidence,
                    )
                });
                let preference = language_preference(&input.request.target.audio_preference);
                let assessment = assess_language_preference(
                    &preference,
                    MediaType::Anime,
                    &acquisition_candidate_language_evidence(candidate),
                );
                let rejection_reasons = semantic_evidence_rejection_reasons(
                    &input.request,
                    semantic_plan.as_ref(),
                    &preference,
                    &assessment,
                    candidate,
                );
                let accepted = rejection_reasons.is_empty();
                for reason in &rejection_reasons {
                    *counters
                        .rejection_reason_counts
                        .entry(reason.clone())
                        .or_insert(0) += 1;
                }
                counters
                    .selected_hypotheses
                    .push(AnimeSemanticCorpusSelection {
                        candidate_key: request_candidate.candidate_key.clone(),
                        hypothesis_index: hypothesis.index,
                        accepted,
                        rejection_reasons,
                    });
                if accepted {
                    let plan = semantic_plan.expect("accepted semantic plan");
                    combined.candidate_plans[candidate_index] = Some(candidate_plan_for_coverage(
                        &request_candidate.candidate_key,
                        &plan,
                        &assessment,
                    ));
                    counters.accepted_evidence += 1;
                } else {
                    counters.rejected_evidence += 1;
                }
            }
        }

        let final_plan = final_plan_for_resolution(&input.request, &combined)?;
        let final_matches_expected =
            semantic_final_plans_match(&final_plan, &case.expected_final_plan);
        let changed = !semantic_final_plans_match(&final_plan, &baseline);
        let recovered = !baseline_matches_expected && final_matches_expected;
        let regressed = baseline_matches_expected && !final_matches_expected;
        observations.push(AnimeSemanticCorpusCaseObservation {
            case_id: case.case_id.clone(),
            set: case.set.clone(),
            slice: case.slice.clone(),
            origin: case.origin.clone(),
            deterministic_definitive,
            baseline_matches_expected,
            final_matches_expected,
            changed,
            recovered,
            regressed,
            selector_invocations: counters.selector_invocations,
            selector_selections: counters.selector_selections,
            selector_abstentions: counters.selector_abstentions,
            selector_errors: counters.selector_errors,
            accepted_evidence: counters.accepted_evidence,
            rejected_evidence: counters.rejected_evidence,
            expectation_audit: semantic_expectation_audit(
                case,
                deterministic_definitive,
                &baseline,
            ),
            selected_hypotheses: counters.selected_hypotheses,
            baseline_final_plan: baseline,
            final_plan,
            expected_final_plan: case.expected_final_plan.clone(),
        });
        eprintln!(
            "ALM9_SEMANTIC_CORPUS_CASE index={} total={} id={} final={} elapsedMs={}",
            index + 1,
            corpus.cases.len(),
            case.case_id,
            if final_matches_expected {
                "pass"
            } else {
                "fail"
            },
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
        );
    }
    Ok(observations)
}

async fn run_cases(
    corpus: &QualificationCorpus,
    identity: &QualificationIdentity,
    selected_case_ids: Option<&BTreeSet<String>>,
    engine: &dyn AnimeMatchEngine,
) -> Result<Vec<QualificationCaseOutput>> {
    let base_seed = identity.candidate_order_seeds[0];
    let permutation_seed = identity.candidate_order_seeds[1];
    let selected_count = selected_case_ids.map_or(corpus.cases.len(), BTreeSet::len);
    let mut outputs = Vec::with_capacity(selected_count);
    let mut selected_index = 0usize;
    for case in &corpus.cases {
        if selected_case_ids.is_some_and(|case_ids| !case_ids.contains(&case.case_id)) {
            continue;
        }
        selected_index += 1;
        let case_started = Instant::now();
        eprintln!(
            "ALM9_QUALIFICATION_CASE_START index={selected_index} total={selected_count} id={}",
            case.case_id
        );
        let input: QualificationCaseInput = serde_json::from_value(case.input.clone())
            .with_context(|| format!("decoding input for qualification case {}", case.case_id))?;
        validate_case_input(case, &input)?;
        let baseline_resolution = deterministic_baseline(&input)?;
        let baseline = final_plan_for_resolution(&input.request, &baseline_resolution)?;

        let main = run_model_observation(&input, &baseline, base_seed, base_seed, engine)
            .await
            .with_context(|| format!("running qualification case {}", case.case_id))?;
        let stability_runs = if case.stability_subset {
            let mut values = Vec::with_capacity(4);
            for _ in 0..3 {
                values.push(
                    run_model_observation(&input, &baseline, base_seed, base_seed, engine).await?,
                );
            }
            values.push(
                run_model_observation(&input, &baseline, permutation_seed, base_seed, engine)
                    .await?,
            );
            values
        } else {
            Vec::new()
        };
        let failure_fallback_plans = run_failure_injections(&input, &baseline).await?;
        for mode in FAILURE_MODES {
            ensure!(
                failure_fallback_plans.get(mode) == Some(&baseline),
                "{} failure injection changed deterministic plan in case {}",
                mode,
                case.case_id
            );
        }
        outputs.push(QualificationCaseOutput {
            case_id: case.case_id.clone(),
            input_fingerprint: case.input_fingerprint.clone(),
            model_decision: main.model_decision,
            fallback_reason: main.fallback_reason,
            model_output: main.model_output,
            model_output_sha256: main.model_output_sha256,
            reference_validation_passed: main.reference_validation_passed,
            baseline_final_plan: baseline,
            final_plan_diff: final_plan_diff(&case.expected_final_plan, &main.final_plan),
            final_plan: main.final_plan,
            stability_runs,
            failure_fallback_plans,
        });
        eprintln!(
            "ALM9_QUALIFICATION_CASE_COMPLETE index={selected_index} total={selected_count} id={} elapsedMs={}",
            case.case_id,
            u64::try_from(case_started.elapsed().as_millis()).unwrap_or(u64::MAX)
        );
    }
    Ok(outputs)
}

async fn run_model_observation(
    input: &QualificationCaseInput,
    baseline: &QualificationFinalPlan,
    order_seed: u64,
    base_seed: u64,
    engine: &dyn AnimeMatchEngine,
) -> Result<QualificationObservation> {
    let ordered_request = ordered_request(&input.request, order_seed, base_seed);
    let candidate_order = ordered_request
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_key.clone())
        .collect::<Vec<_>>();
    let request_fingerprint = canonical_json_fingerprint(&serde_json::to_value(&ordered_request)?)?;
    let prepared = prepare_stable_request(
        &input.request,
        ordered_request,
        &input.acquisition_candidates,
    )?;
    let preference = language_preference(&input.request.target.audio_preference);
    let engine_output = match engine
        .match_candidates_with_provenance(prepared.request().clone())
        .await
    {
        Ok(output) => output,
        Err(_) => {
            let model_output = serde_json::json!({"qualificationInferenceError": true});
            return Ok(QualificationObservation {
                order_seed,
                candidate_order,
                request_fingerprint,
                model_decision: "fallback".to_string(),
                fallback_reason: Some("engine_error".to_string()),
                model_output_sha256: canonical_json_fingerprint(&model_output)?,
                model_output,
                reference_validation_passed: false,
                final_plan: baseline.clone(),
            });
        }
    };
    let response = engine_output.response;
    let model_output = serde_json::to_value(&response).expect("anime response is serializable");
    let model_output_sha256 = canonical_json_fingerprint(&model_output)?;
    if validate_anime_match_response(&prepared, &response).is_err() {
        return Ok(QualificationObservation {
            order_seed,
            candidate_order,
            request_fingerprint,
            model_decision: "fallback".to_string(),
            fallback_reason: Some("invalid_model_response".to_string()),
            model_output,
            model_output_sha256,
            reference_validation_passed: false,
            final_plan: baseline.clone(),
        });
    }
    let acquisition_sources = acquisition_source_map(
        prepared.request(),
        &input.acquisition_candidates,
        prepared.source_map(),
    )?;
    let selection_support = file_selection_support_by_candidate_index(
        prepared.request(),
        input.acquisition_candidates.len(),
        &input.route_context,
        prepared.source_map(),
    )?;
    let coverage = match model_derived_anime_coverage_plans_with_selection_resolver(
        prepared.request(),
        &input.scoring_context,
        &input.acquisition_candidates,
        &response.matches,
        &acquisition_sources,
        |candidate_index, _| selection_support[candidate_index],
    ) {
        Ok(coverage) => coverage,
        Err(_) => {
            return Ok(QualificationObservation {
                order_seed,
                candidate_order,
                request_fingerprint,
                model_decision: "fallback".to_string(),
                fallback_reason: Some("coverage_validation_failed".to_string()),
                model_output,
                model_output_sha256,
                reference_validation_passed: true,
                final_plan: baseline.clone(),
            });
        }
    };
    let mut resolution = QualificationResolutionState {
        candidate_plans: vec![None; input.acquisition_candidates.len()],
        saw_partial_or_ambiguous: false,
    };
    ensure!(
        coverage.len() == response.matches.len(),
        "production coverage result count differs from validated model matches"
    );
    for mapped in &coverage {
        let assessment = assess_language_preference(
            &preference,
            MediaType::Anime,
            &acquisition_model_audio_profile_evidence(mapped.audio_profile),
        );
        if required_language_is_hard_mismatch(&preference, &assessment) {
            return Ok(QualificationObservation {
                order_seed,
                candidate_order,
                request_fingerprint,
                model_decision: "fallback".to_string(),
                fallback_reason: Some("audio_policy_failed".to_string()),
                model_output,
                model_output_sha256,
                reference_validation_passed: true,
                final_plan: baseline.clone(),
            });
        }
    }
    for (mapped, matched) in coverage.into_iter().zip(&response.matches) {
        let assessment = assess_language_preference(
            &preference,
            MediaType::Anime,
            &acquisition_model_audio_profile_evidence(mapped.audio_profile),
        );
        resolution.saw_partial_or_ambiguous = true;
        if required_language_satisfied(&preference, &assessment) {
            resolution.candidate_plans[mapped.candidate_index] = Some(
                candidate_plan_for_model_coverage(matched, &mapped.plan, &assessment)?,
            );
        }
    }
    let final_plan = final_plan_for_resolution(&input.request, &resolution)?;
    Ok(QualificationObservation {
        order_seed,
        candidate_order,
        request_fingerprint,
        model_decision: "accepted".to_string(),
        fallback_reason: None,
        model_output_sha256,
        model_output,
        reference_validation_passed: true,
        final_plan,
    })
}

async fn run_failure_injections(
    input: &QualificationCaseInput,
    baseline: &QualificationFinalPlan,
) -> Result<BTreeMap<String, QualificationFinalPlan>> {
    let target_key = input
        .request
        .target
        .wanted_target_keys
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("qualification request has no wanted target"))?;
    let invalid_response = AnimeMatchResponse {
        schema_version: ANIME_MATCH_SCHEMA_VERSION,
        matches: vec![AnimeCandidateMatch {
            candidate_key: "__qualification_unknown_candidate__".to_string(),
            matched_target_keys: vec![target_key],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: None,
        }],
    };
    let modes: [(
        &str,
        Option<Arc<dyn AnimeMatchEngine>>,
        AnimeMatchFallbackReason,
    ); 4] = [
        (
            "unavailable",
            None,
            AnimeMatchFallbackReason::EngineUnavailable,
        ),
        (
            "timeout",
            Some(Arc::new(InjectedEngine::Error(
                "qualification injected worker timeout",
            ))),
            AnimeMatchFallbackReason::EngineError,
        ),
        (
            "invalid",
            Some(Arc::new(InjectedEngine::Response(invalid_response))),
            AnimeMatchFallbackReason::InvalidModelResponse,
        ),
        (
            "empty",
            Some(Arc::new(InjectedEngine::Response(AnimeMatchResponse {
                schema_version: ANIME_MATCH_SCHEMA_VERSION,
                matches: Vec::new(),
            }))),
            AnimeMatchFallbackReason::EmptyModelMatches,
        ),
    ];
    let mut plans = BTreeMap::new();
    for (mode, engine, expected_reason) in modes {
        let service = engine
            .map(AnimeMatchingService::with_engine)
            .unwrap_or_else(AnimeMatchingService::disabled);
        let prepared = prepare_stable_request(
            &input.request,
            input.request.clone(),
            &input.acquisition_candidates,
        )?;
        let outcome = service
            .match_prepared_or_fallback(
                AnimeDeterministicResult {
                    value: baseline.clone(),
                    // Every named injection must reach the service boundary,
                    // including cases whose normal run uses the deterministic
                    // union fast path.
                    state: DeterministicMatchState::Difficult,
                },
                prepared,
                |_, _, _, _| -> Result<QualificationFinalPlan> {
                    bail!("failure injection must not produce an accepted override")
                },
            )
            .await;
        ensure!(
            outcome.provenance.result != AnimeMatchAssistResult::Matched,
            "{mode} failure injection unexpectedly accepted a model override"
        );
        ensure!(
            outcome.provenance.reason == Some(expected_reason),
            "{mode} failure injection did not reach its expected service fallback branch"
        );
        plans.insert(mode.to_string(), outcome.value);
    }
    Ok(plans)
}

#[cfg(test)]
fn model_only_resolution(
    candidate_count: usize,
    preference: &AcquisitionLanguagePreference,
    matches: &[AnimeCandidateMatch],
    source_map: &AnimeMatchSourceMap<QualificationCandidateSource, QualificationFileSource>,
) -> Result<QualificationResolutionState> {
    let mut resolution = QualificationResolutionState {
        candidate_plans: vec![None; candidate_count],
        saw_partial_or_ambiguous: false,
    };
    for matched in matches {
        let source = source_map
            .candidate_source(&matched.candidate_key)
            .ok_or_else(|| anyhow!("model match lacks qualification candidate source"))?;
        ensure!(
            source.candidate_index < candidate_count,
            "model match candidate source is outside the qualification input"
        );
        let assessment = assess_language_preference(
            preference,
            MediaType::Anime,
            &acquisition_model_audio_profile_evidence(matched.audio_profile),
        );
        if required_language_is_hard_mismatch(preference, &assessment) {
            continue;
        }
        resolution.saw_partial_or_ambiguous = true;
        if required_language_satisfied(preference, &assessment) {
            resolution.candidate_plans[source.candidate_index] =
                Some(candidate_plan_for_model_match(matched, &assessment));
        }
    }
    Ok(resolution)
}

fn deterministic_baseline(input: &QualificationCaseInput) -> Result<QualificationResolutionState> {
    let preference = language_preference(&input.request.target.audio_preference);
    let mut saw_partial_or_ambiguous = false;
    let mut candidate_plans = vec![None; input.acquisition_candidates.len()];
    for (index, candidate) in input.acquisition_candidates.iter().enumerate() {
        let request_candidate = input
            .request
            .candidates
            .get(index)
            .ok_or_else(|| anyhow!("qualification candidate cardinality mismatch"))?;
        let candidate_input = anime_candidate_input(candidate);
        let files = qualification_release_files(request_candidate, candidate)?;
        let plan = bind_exact_single_anime_provider_file(
            plan_anime_file_coverage_with_options(
                &input.scoring_context,
                &candidate_input,
                &files,
                AnimeCoverageOptions {
                    file_selection_supported: *input
                        .route_context
                        .file_selection_supported_by_candidate_key
                        .get(&request_candidate.candidate_key)
                        .ok_or_else(|| {
                            anyhow!("route context lacks candidate file-selection key")
                        })?,
                },
            ),
            &input.scoring_context,
            &candidate_input,
            &files,
        );
        let assessment = assess_language_preference(
            &preference,
            MediaType::Anime,
            &acquisition_candidate_language_evidence(candidate),
        );
        let deterministic_state = acquisition_anime_deterministic_state(&plan);
        let definitively_matches_another_target = deterministic_state
            == DeterministicMatchState::Definitive
            && plan_definitively_excludes_wanted_targets(&input.request, &plan);
        if deterministic_state == DeterministicMatchState::Definitive
            && plan_covers_only_wanted_targets(&input.request, &plan)
            && required_language_satisfied(&preference, &assessment)
            && !synthetic_stream_candidate_requires_manual_review(candidate)
        {
            candidate_plans[index] = Some(candidate_plan_for_coverage(
                &request_candidate.candidate_key,
                &plan,
                &assessment,
            ));
        }
        if !required_language_is_hard_mismatch(&preference, &assessment)
            && !definitively_matches_another_target
        {
            saw_partial_or_ambiguous |= !plan.entries.is_empty()
                || plan.confidence == ReleaseConfidence::ReviewRequired
                || plan.rejection_reasons.is_empty();
        }
    }
    Ok(QualificationResolutionState {
        candidate_plans,
        saw_partial_or_ambiguous,
    })
}

fn plan_definitively_excludes_wanted_targets(
    request: &AnimeMatchRequest,
    plan: &AnimeFileCoveragePlan,
) -> bool {
    let wanted = request
        .target
        .wanted_target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    !wanted.is_empty()
        && !plan.entries.is_empty()
        && plan
            .entries
            .iter()
            .all(|entry| !wanted.contains(entry.target_key.as_str()))
}

fn plan_covers_only_wanted_targets(
    request: &AnimeMatchRequest,
    plan: &AnimeFileCoveragePlan,
) -> bool {
    let wanted = request
        .target
        .wanted_target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let planned = plan
        .entries
        .iter()
        .map(|entry| entry.target_key.as_str())
        .collect::<BTreeSet<_>>();
    !wanted.is_empty()
        && wanted.len() == request.target.wanted_target_keys.len()
        && !planned.is_empty()
        && planned.len() == plan.entries.len()
        && planned.is_subset(&wanted)
}

fn semantic_evidence_rejection_reasons(
    request: &AnimeMatchRequest,
    plan: Option<&AnimeFileCoveragePlan>,
    preference: &AcquisitionLanguagePreference,
    assessment: &LanguagePreferenceAssessment,
    candidate: &AcquisitionCandidate,
) -> Vec<String> {
    let Some(plan) = plan else {
        return vec!["semantic_planner_no_plan".to_string()];
    };
    let mut reasons = BTreeSet::new();
    if acquisition_anime_deterministic_state(plan) != DeterministicMatchState::Definitive {
        reasons.insert("plan_not_definitive".to_string());
    }
    if plan.entries.is_empty() {
        reasons.insert("empty_target_coverage".to_string());
    }
    if !plan_covers_only_wanted_targets(request, plan) {
        reasons.insert("wanted_target_coverage_failed".to_string());
    }
    if !required_language_satisfied(preference, assessment) {
        reasons.insert("required_audio_failed".to_string());
    }
    if synthetic_stream_candidate_requires_manual_review(candidate) {
        reasons.insert("candidate_policy_blocked".to_string());
    }
    reasons.extend(
        plan.rejection_reasons
            .iter()
            .map(|reason| format!("planner_rejection:{reason}")),
    );
    reasons.extend(
        plan.review_reasons
            .iter()
            .map(|reason| format!("planner_review:{reason}")),
    );
    reasons.into_iter().collect()
}

/// Preserve raw corpus scoring while identifying frozen synthetic labels that
/// contradict a definitive source-shaped match. These remain visible failures;
/// the implementation must not learn product behavior from them until their
/// expected plan is independently corrected.
fn semantic_expectation_audit(
    case: &QualificationCase,
    deterministic_definitive: bool,
    baseline: &QualificationFinalPlan,
) -> Option<String> {
    (case.origin == "synthetic"
        && case.counterfactual_pair_id.is_some()
        && case.expected_final_plan.disposition == QualificationDisposition::NoMatch
        && deterministic_definitive
        && baseline.disposition == QualificationDisposition::Matched)
        .then(|| "synthetic_no_match_conflicts_with_definitive_match".to_string())
}

fn candidate_plan_for_coverage(
    candidate_key: &str,
    plan: &AnimeFileCoveragePlan,
    assessment: &LanguagePreferenceAssessment,
) -> QualificationCandidatePlan {
    let target_keys = unique_strings(plan.entries.iter().map(|entry| entry.target_key.clone()));
    let file_keys = unique_strings(plan.selected_file_keys.iter().cloned());
    let coverage = plan
        .entries
        .iter()
        .map(|entry| QualificationCoverageEntry {
            target_key: entry.target_key.clone(),
            file_key: entry.release_file_key.clone(),
            status: QualificationCoverageStatus::Covered,
        })
        .collect::<Vec<_>>();
    QualificationCandidatePlan {
        candidate_key: candidate_key.to_string(),
        target_keys,
        file_keys,
        audio_eligibility: audio_eligibility(assessment),
        coverage,
    }
}

fn candidate_plan_for_model_coverage(
    matched: &AnimeCandidateMatch,
    plan: &AnimeFileCoveragePlan,
    assessment: &LanguagePreferenceAssessment,
) -> Result<QualificationCandidatePlan> {
    let file_keys = matched.selected_file_keys.clone().unwrap_or_default();
    ensure!(
        file_keys.is_empty() || file_keys.len() == 1 || file_keys.len() == plan.entries.len(),
        "production coverage cannot be represented with request-local file keys"
    );
    let coverage = plan
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| QualificationCoverageEntry {
            target_key: entry.target_key.clone(),
            file_key: if file_keys.len() == 1 {
                file_keys.first().cloned()
            } else {
                file_keys.get(index).cloned()
            },
            status: QualificationCoverageStatus::Covered,
        })
        .collect::<Vec<_>>();
    Ok(QualificationCandidatePlan {
        candidate_key: matched.candidate_key.clone(),
        target_keys: unique_strings(plan.entries.iter().map(|entry| entry.target_key.clone())),
        file_keys,
        audio_eligibility: audio_eligibility(assessment),
        coverage,
    })
}

#[cfg(test)]
fn candidate_plan_for_model_match(
    matched: &AnimeCandidateMatch,
    assessment: &LanguagePreferenceAssessment,
) -> QualificationCandidatePlan {
    let file_keys = matched.selected_file_keys.clone().unwrap_or_default();
    let coverage = matched
        .matched_target_keys
        .iter()
        .enumerate()
        .map(|(index, target_key)| QualificationCoverageEntry {
            target_key: target_key.clone(),
            file_key: if file_keys.len() == 1 {
                file_keys.first().cloned()
            } else {
                file_keys.get(index).cloned()
            },
            status: QualificationCoverageStatus::Covered,
        })
        .collect();
    QualificationCandidatePlan {
        candidate_key: matched.candidate_key.clone(),
        target_keys: matched.matched_target_keys.clone(),
        file_keys,
        audio_eligibility: audio_eligibility(assessment),
        coverage,
    }
}

fn final_plan_for_resolution(
    request: &AnimeMatchRequest,
    resolution: &QualificationResolutionState,
) -> Result<QualificationFinalPlan> {
    let candidate_plans = resolution
        .candidate_plans
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let covered = candidate_plans
        .iter()
        .flat_map(|plan| plan.target_keys.iter().cloned())
        .collect::<BTreeSet<_>>();
    let wanted = request
        .target
        .wanted_target_keys
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let disposition = if !wanted.is_empty() && wanted.iter().all(|key| covered.contains(key)) {
        QualificationDisposition::Matched
    } else if !candidate_plans.is_empty() || resolution.saw_partial_or_ambiguous {
        QualificationDisposition::Unresolved
    } else {
        QualificationDisposition::NoMatch
    };
    let targets_by_key = request
        .context
        .seasons
        .iter()
        .flat_map(|season| {
            season
                .targets
                .iter()
                .map(move |target| (target.target_key.as_str(), (season, target)))
        })
        .collect::<BTreeMap<_, _>>();
    let mut seasons = BTreeSet::new();
    let mut episodes = Vec::new();
    let mut absolutes = Vec::new();
    for key in request
        .target
        .wanted_target_keys
        .iter()
        .filter(|key| covered.contains(*key))
    {
        let (season, target) = targets_by_key
            .get(key.as_str())
            .ok_or_else(|| anyhow!("final plan references unknown target '{key}'"))?;
        seasons.insert(target.season_number.unwrap_or(season.season_number));
        if let Some(number) = target.episode_number {
            if !episodes.contains(&number) {
                episodes.push(number);
            }
        }
        if let Some(number) = target.absolute_episode_number {
            if !absolutes.contains(&number) {
                absolutes.push(number);
            }
        }
    }
    Ok(QualificationFinalPlan {
        disposition,
        season_number: (seasons.len() == 1).then(|| *seasons.first().expect("one season")),
        episode_numbers: episodes,
        absolute_episode_numbers: absolutes,
        candidate_plans,
    })
}

/// Candidate, target, and file arrays are protocol sets. Coverage remains an
/// exact target-to-file relation, but provider enumeration order is not part of
/// anime identity and must not turn a correct plan into a corpus failure.
fn semantic_final_plans_match(
    left: &QualificationFinalPlan,
    right: &QualificationFinalPlan,
) -> bool {
    canonical_semantic_final_plan(left) == canonical_semantic_final_plan(right)
}

fn canonical_semantic_final_plan(plan: &QualificationFinalPlan) -> QualificationFinalPlan {
    let mut plan = plan.clone();
    plan.episode_numbers.sort_unstable();
    plan.absolute_episode_numbers.sort_unstable();
    for candidate in &mut plan.candidate_plans {
        candidate.target_keys.sort();
        candidate.file_keys.sort();
        candidate.coverage.sort_by(|left, right| {
            (
                &left.target_key,
                &left.file_key,
                coverage_status_order(&left.status),
            )
                .cmp(&(
                    &right.target_key,
                    &right.file_key,
                    coverage_status_order(&right.status),
                ))
        });
    }
    plan.candidate_plans.sort_by(|left, right| {
        (&left.candidate_key, &left.target_keys, &left.file_keys).cmp(&(
            &right.candidate_key,
            &right.target_keys,
            &right.file_keys,
        ))
    });
    plan
}

fn coverage_status_order(status: &QualificationCoverageStatus) -> u8 {
    match status {
        QualificationCoverageStatus::Covered => 0,
        QualificationCoverageStatus::Missing => 1,
        QualificationCoverageStatus::Ineligible => 2,
    }
}

fn deterministic_union_state(
    request: &AnimeMatchRequest,
    resolution: &QualificationResolutionState,
) -> DeterministicMatchState {
    let wanted = request
        .target
        .wanted_target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let covered = resolution
        .candidate_plans
        .iter()
        .flatten()
        .flat_map(|plan| plan.target_keys.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    if !wanted.is_empty()
        && wanted.len() == request.target.wanted_target_keys.len()
        && wanted == covered
    {
        DeterministicMatchState::Definitive
    } else {
        DeterministicMatchState::Difficult
    }
}

#[cfg(test)]
fn empty_final_plan(disposition: QualificationDisposition) -> QualificationFinalPlan {
    QualificationFinalPlan {
        disposition,
        season_number: None,
        episode_numbers: Vec::new(),
        absolute_episode_numbers: Vec::new(),
        candidate_plans: Vec::new(),
    }
}

fn language_preference(preference: &AnimeMatchAudioPreference) -> AcquisitionLanguagePreference {
    let mode = match preference.mode {
        AnimeMatchAudioPreferenceMode::Any => LanguagePreferenceMode::Off,
        AnimeMatchAudioPreferenceMode::Prefer | AnimeMatchAudioPreferenceMode::PreferDub => {
            LanguagePreferenceMode::Prefer
        }
        AnimeMatchAudioPreferenceMode::Require | AnimeMatchAudioPreferenceMode::RequireDub => {
            LanguagePreferenceMode::RequireReview
        }
    };
    let mut profiles = preference.accepted_profiles.clone();
    if matches!(
        preference.mode,
        AnimeMatchAudioPreferenceMode::PreferDub | AnimeMatchAudioPreferenceMode::RequireDub
    ) {
        profiles.extend(
            ["en_audio", "dual_audio", "dubbed"]
                .into_iter()
                .map(str::to_string),
        );
    }
    AcquisitionLanguagePreference {
        mode,
        anime: LanguagePreferenceMediaRule {
            audio: preference.languages.clone(),
            subtitles: preference.subtitle_languages.clone(),
            profiles: unique_strings(profiles),
        },
        unknown_language: if mode == LanguagePreferenceMode::RequireReview {
            UnknownLanguagePolicy::RequireReview
        } else {
            UnknownLanguagePolicy::AllowLowerPriority
        },
        ..AcquisitionLanguagePreference::default()
    }
    .normalized()
}

fn required_language_satisfied(
    preference: &AcquisitionLanguagePreference,
    assessment: &LanguagePreferenceAssessment,
) -> bool {
    preference.mode != LanguagePreferenceMode::RequireReview
        || assessment.state == LanguagePreferenceAssessmentState::Match
}

fn required_language_is_hard_mismatch(
    preference: &AcquisitionLanguagePreference,
    assessment: &LanguagePreferenceAssessment,
) -> bool {
    preference.mode == LanguagePreferenceMode::RequireReview
        && assessment.state == LanguagePreferenceAssessmentState::Mismatch
}

fn audio_eligibility(assessment: &LanguagePreferenceAssessment) -> QualificationAudioEligibility {
    match assessment.state {
        LanguagePreferenceAssessmentState::Off => QualificationAudioEligibility::NotApplicable,
        LanguagePreferenceAssessmentState::Match => QualificationAudioEligibility::Eligible,
        LanguagePreferenceAssessmentState::Mismatch => QualificationAudioEligibility::Ineligible,
        LanguagePreferenceAssessmentState::Unknown => QualificationAudioEligibility::Unknown,
    }
}

fn anime_candidate_input(candidate: &AcquisitionCandidate) -> AnimeCandidateInput {
    AnimeCandidateInput {
        title: candidate.title.clone(),
        source_kind: candidate.source_kind.clone(),
        quality: candidate.quality.clone(),
        size_bytes: candidate.size_bytes,
        seeders: candidate.seeders,
        cached_debrid: candidate.cached_debrid,
        rank: candidate.rank,
        source_score: candidate.score,
        supported_routes: candidate.supported_routes.clone(),
        default_route: candidate.default_route.clone(),
    }
}

fn qualification_release_files(
    request_candidate: &crate::anime_matching::AnimeMatchCandidate,
    candidate: &AcquisitionCandidate,
) -> Result<Vec<AnimeReleaseFileInput>> {
    let request_files_by_index = selectable_request_file_bindings(request_candidate, candidate)?
        .into_iter()
        .map(|(file, index)| (index, file))
        .collect::<BTreeMap<_, _>>();
    candidate
        .files
        .iter()
        .enumerate()
        .map(|(file_index, file)| {
            let file_key = request_files_by_index
                .get(&file_index)
                .map(|request_file| request_file.file_key.clone())
                .unwrap_or_else(|| format!("__qualification_unselectable_file_{file_index}"));
            Ok(AnimeReleaseFileInput {
                file_key,
                file_id: file.file_id.clone(),
                file_index: file.file_index,
                path: file.path.clone(),
                size_bytes: file.size_bytes.and_then(|value| i64::try_from(value).ok()),
                selectable: file.selectable.unwrap_or(true),
            })
        })
        .collect()
}

fn selectable_request_file_bindings<'a>(
    request_candidate: &'a crate::anime_matching::AnimeMatchCandidate,
    candidate: &'a AcquisitionCandidate,
) -> Result<Vec<(&'a crate::anime_matching::AnimeMatchFile, usize)>> {
    let selectable_indexes = candidate
        .files
        .iter()
        .enumerate()
        .filter_map(|(index, file)| selectable_anime_media_file(file).then_some(index))
        .collect::<Vec<_>>();
    ensure!(
        request_candidate.files.len() == selectable_indexes.len(),
        "qualification request differs from production selectable-file filtering"
    );
    request_candidate
        .files
        .iter()
        .zip(selectable_indexes)
        .map(|(request_file, file_index)| {
            ensure!(
                request_file.path == candidate.files[file_index].path,
                "qualification request and acquisition file paths differ"
            );
            Ok((request_file, file_index))
        })
        .collect()
}

fn validate_case_input(case: &QualificationCase, input: &QualificationCaseInput) -> Result<()> {
    validate_frozen_case_input(case, input)?;
    let prepared = prepare_production_request(input)?;
    ensure!(
        prepared.request() == &input.request,
        "case {} request differs from production adapter output",
        case.case_id
    );
    Ok(())
}

/// Semantic integration evaluates today's parser against frozen raw releases,
/// metadata context, and expected plans. Parser-derived request facts are not
/// labels and must be rebuilt after deterministic parser improvements.
fn current_semantic_case_input(
    case: &QualificationCase,
    mut input: QualificationCaseInput,
) -> Result<QualificationCaseInput> {
    validate_frozen_case_input(case, &input)?;
    let prepared = prepare_production_request(&input)?;
    input.request = prepared.request().clone();
    validate_case_references(case, &input)?;
    Ok(input)
}

fn validate_frozen_case_input(
    case: &QualificationCase,
    input: &QualificationCaseInput,
) -> Result<()> {
    let actual_fingerprint = canonical_json_fingerprint(&case.input)?;
    ensure!(
        normalize_sha256(&case.input_fingerprint)? == actual_fingerprint,
        "case {} input fingerprint mismatch",
        case.case_id
    );
    ensure!(
        input.request.candidates.len() == input.acquisition_candidates.len(),
        "case {} candidate cardinality mismatch",
        case.case_id
    );
    ensure!(
        serde_json::to_value(&input.request)?
            == case
                .input
                .get("request")
                .cloned()
                .unwrap_or(JsonValue::Null),
        "case {} worker request is not the exact production wire shape",
        case.case_id
    );
    validate_scoring_context_binding(&case.case_id, input)?;
    validate_case_references(case, input)
}

fn validate_case_references(
    case: &QualificationCase,
    input: &QualificationCaseInput,
) -> Result<()> {
    let candidate_keys = input
        .request
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_key.clone())
        .collect::<Vec<_>>();
    ensure!(
        input
            .route_context
            .file_selection_supported_by_candidate_key
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            == candidate_keys.iter().cloned().collect::<BTreeSet<_>>(),
        "case {} route context differs from request candidate keys",
        case.case_id
    );
    let target_keys = input.request.target.wanted_target_keys.clone();
    let file_keys = input
        .request
        .candidates
        .iter()
        .flat_map(|candidate| candidate.files.iter().map(|file| file.file_key.clone()))
        .collect::<Vec<_>>();
    ensure!(
        case.allowed_references.candidate_keys == candidate_keys
            && case.allowed_references.target_keys == target_keys
            && case.allowed_references.file_keys == file_keys,
        "case {} allowed references differ from the worker request",
        case.case_id
    );
    Ok(())
}

fn validate_scoring_context_binding(case_id: &str, input: &QualificationCaseInput) -> Result<()> {
    let derived_context = acquisition_match_context(
        &input.request.target.canonical_title,
        &input.scoring_context,
        &input.request.target,
    )?;
    ensure!(
        derived_context == input.request.context,
        "case {case_id} worker context differs from the production scoring graph"
    );
    Ok(())
}

fn prepare_production_request(
    input: &QualificationCaseInput,
) -> Result<QualificationPreparedRequest> {
    let candidates = input
        .acquisition_candidates
        .iter()
        .enumerate()
        .map(|(candidate_index, candidate)| AnimeMatchCandidateInput {
            source: QualificationCandidateSource { candidate_index },
            title: candidate.title.clone(),
            files: candidate
                .files
                .iter()
                .enumerate()
                .filter(|(_, file)| selectable_anime_media_file(file))
                .map(|(file_index, file)| AnimeMatchFileInput {
                    source: QualificationFileSource {
                        candidate_index,
                        file_index,
                    },
                    path: file.path.clone(),
                })
                .collect(),
            parse_facts: acquisition_candidate_parse_facts(candidate),
        })
        .collect();
    AnimeMatchingService::prepare_request(AnimeMatchBatchInput {
        request_id: input.request.request_id.clone(),
        target: input.request.target.clone(),
        context: input.request.context.clone(),
        candidates,
    })
    .map_err(Into::into)
}

fn prepare_stable_request(
    base_request: &AnimeMatchRequest,
    ordered_request: AnimeMatchRequest,
    candidates: &[AcquisitionCandidate],
) -> Result<QualificationPreparedRequest> {
    let candidate_indexes = base_request
        .candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| (candidate.candidate_key.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        candidate_indexes.len() == candidates.len(),
        "qualification request candidate keys are not unique"
    );
    let mut candidate_sources = BTreeMap::new();
    let mut file_sources = BTreeMap::new();
    for candidate in &ordered_request.candidates {
        let candidate_index = *candidate_indexes
            .get(candidate.candidate_key.as_str())
            .ok_or_else(|| anyhow!("ordered request introduced an unknown candidate"))?;
        candidate_sources.insert(
            candidate.candidate_key.clone(),
            QualificationCandidateSource { candidate_index },
        );
        let acquisition = candidates
            .get(candidate_index)
            .ok_or_else(|| anyhow!("qualification candidate source is out of bounds"))?;
        for (file, file_index) in selectable_request_file_bindings(candidate, acquisition)? {
            file_sources.insert(
                file.file_key.clone(),
                (
                    candidate.candidate_key.clone(),
                    QualificationFileSource {
                        candidate_index,
                        file_index,
                    },
                ),
            );
        }
    }
    let prepared = PreparedAnimeMatchRequest {
        request: ordered_request,
        source_map: AnimeMatchSourceMap::new(candidate_sources, file_sources),
    };
    validate_anime_match_request(&prepared)?;
    Ok(prepared)
}

fn acquisition_source_map(
    request: &AnimeMatchRequest,
    candidates: &[AcquisitionCandidate],
    qualification_source_map: &AnimeMatchSourceMap<
        QualificationCandidateSource,
        QualificationFileSource,
    >,
) -> Result<AnimeMatchSourceMap<AcquisitionAnimeCandidateSource, AcquisitionAnimeFileSource>> {
    ensure!(
        request.candidates.len() == candidates.len(),
        "acquisition source-map candidate count differs"
    );
    let mut candidate_sources = BTreeMap::new();
    let mut file_sources = BTreeMap::new();
    for request_candidate in &request.candidates {
        let candidate_index = qualification_source_map
            .candidate_source(&request_candidate.candidate_key)
            .ok_or_else(|| anyhow!("qualification source map lacks candidate"))?
            .candidate_index;
        let candidate = candidates
            .get(candidate_index)
            .ok_or_else(|| anyhow!("qualification candidate source is out of bounds"))?;
        candidate_sources.insert(
            request_candidate.candidate_key.clone(),
            AcquisitionAnimeCandidateSource { candidate_index },
        );
        for (file, file_index) in selectable_request_file_bindings(request_candidate, candidate)? {
            let qualification_file = qualification_source_map
                .file_source(&request_candidate.candidate_key, &file.file_key)
                .ok_or_else(|| anyhow!("qualification source map lacks candidate file"))?;
            ensure!(
                qualification_file.candidate_index == candidate_index
                    && qualification_file.file_index == file_index,
                "qualification file source changed across adapters"
            );
            file_sources.insert(
                file.file_key.clone(),
                (
                    request_candidate.candidate_key.clone(),
                    AcquisitionAnimeFileSource {
                        candidate_index,
                        file_index,
                    },
                ),
            );
        }
    }
    Ok(AnimeMatchSourceMap::new(candidate_sources, file_sources))
}

fn file_selection_support_by_candidate_index(
    request: &AnimeMatchRequest,
    candidate_count: usize,
    route_context: &QualificationRouteContext,
    source_map: &AnimeMatchSourceMap<QualificationCandidateSource, QualificationFileSource>,
) -> Result<Vec<bool>> {
    let request_keys = request
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_key.as_str())
        .collect::<BTreeSet<_>>();
    let route_keys = route_context
        .file_selection_supported_by_candidate_key
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        request_keys == route_keys,
        "route context must contain exactly every request candidate key"
    );
    let mut support = vec![None; candidate_count];
    for candidate in &request.candidates {
        let source = source_map
            .candidate_source(&candidate.candidate_key)
            .ok_or_else(|| anyhow!("route context candidate lacks a source mapping"))?;
        let selected = *route_context
            .file_selection_supported_by_candidate_key
            .get(&candidate.candidate_key)
            .ok_or_else(|| anyhow!("route context lacks candidate key"))?;
        let slot = support
            .get_mut(source.candidate_index)
            .ok_or_else(|| anyhow!("route context candidate source is out of bounds"))?;
        ensure!(
            slot.replace(selected).is_none(),
            "route context repeats a source"
        );
    }
    support
        .into_iter()
        .map(|value| value.ok_or_else(|| anyhow!("route context source map is incomplete")))
        .collect()
}

fn ordered_request(
    request: &AnimeMatchRequest,
    order_seed: u64,
    base_seed: u64,
) -> AnimeMatchRequest {
    let mut request = request.clone();
    if order_seed == base_seed {
        return request;
    }
    let original = request
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_key.clone())
        .collect::<Vec<_>>();
    request.candidates.sort_by(|left, right| {
        candidate_order_digest(order_seed, &left.candidate_key)
            .cmp(&candidate_order_digest(order_seed, &right.candidate_key))
    });
    let sorted = request
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_key.clone())
        .collect::<Vec<_>>();
    if sorted == original {
        request.candidates.rotate_left(1);
    }
    request
}

fn candidate_order_digest(seed: u64, key: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(seed.to_string().as_bytes());
    digest.update([0]);
    digest.update(key.as_bytes());
    digest.finalize().into()
}

fn final_plan_diff(
    expected: &QualificationFinalPlan,
    actual: &QualificationFinalPlan,
) -> QualificationPlanDiff {
    let mismatched_fields = PLAN_FIELDS
        .iter()
        .filter_map(|(name, field)| {
            (!plan_field_matches(expected, actual, *field)).then(|| (*name).to_string())
        })
        .collect::<Vec<_>>();
    QualificationPlanDiff {
        matches: mismatched_fields.is_empty(),
        mismatched_fields,
    }
}

fn plan_field_matches(
    left: &QualificationFinalPlan,
    right: &QualificationFinalPlan,
    field: PlanField,
) -> bool {
    match field {
        PlanField::Disposition => left.disposition == right.disposition,
        PlanField::SeasonNumber => left.season_number == right.season_number,
        PlanField::EpisodeNumbers => left.episode_numbers == right.episode_numbers,
        PlanField::AbsoluteEpisodeNumbers => {
            left.absolute_episode_numbers == right.absolute_episode_numbers
        }
        PlanField::CandidatePlans => left.candidate_plans == right.candidate_plans,
    }
}

fn validate_case_selection(
    corpus_case_ids: &[String],
    certification_target_id: Option<&str>,
    case_selection: Option<&QualificationCaseSelection>,
) -> Result<Option<BTreeSet<String>>> {
    if let Some(target_id) = certification_target_id {
        ensure!(
            !target_id.is_empty()
                && target_id.len() <= 128
                && target_id.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                }),
            "qualification certification target ID is invalid"
        );
    } else {
        ensure!(
            case_selection.is_none(),
            "qualification case selection requires a certification target identity"
        );
    }

    let Some(selection) = case_selection else {
        return Ok(None);
    };
    ensure!(
        !selection.selection_id.is_empty()
            && selection.selection_id.len() <= 128
            && selection
                .selection_id
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') }),
        "qualification case-selection ID is invalid"
    );
    ensure!(
        !selection.case_ids.is_empty() && selection.case_ids.len() < corpus_case_ids.len(),
        "qualification cross-runtime case selection must be a non-empty proper subset"
    );
    let selected = selection.case_ids.iter().cloned().collect::<BTreeSet<_>>();
    ensure!(
        selected.len() == selection.case_ids.len(),
        "qualification case selection contains duplicate case IDs"
    );
    ensure!(
        selection
            .case_ids
            .iter()
            .all(|case_id| corpus_case_ids.contains(case_id)),
        "qualification case selection contains a case outside the frozen corpus"
    );
    ensure!(
        selection.case_ids
            == corpus_case_ids
                .iter()
                .filter(|case_id| selected.contains(*case_id))
                .cloned()
                .collect::<Vec<_>>(),
        "qualification case selection is not in frozen corpus order"
    );
    ensure!(
        normalize_sha256(&selection.case_ids_fingerprint)?
            == canonical_json_fingerprint(&serde_json::to_value(&selection.case_ids)?)?,
        "qualification case-selection fingerprint does not bind its ordered case IDs"
    );
    Ok(Some(selected))
}

fn validate_corpus_shape(corpus: &QualificationCorpus, raw: &[u8]) -> Result<()> {
    ensure!(
        corpus.schema_version == QUALIFICATION_CORPUS_SCHEMA_VERSION && corpus.status == "frozen",
        "qualification corpus must be frozen schema v{QUALIFICATION_CORPUS_SCHEMA_VERSION}"
    );
    match corpus.profile {
        QualificationCorpusProfile::Certification520 => {
            ensure!(
                corpus.cases.len() == EXPECTED_CASE_COUNT,
                "certification qualification corpus must contain exactly {EXPECTED_CASE_COUNT} cases"
            );
            ensure!(
                corpus.sets.smoke.len() == 40,
                "qualification smoke set must contain 40 cases"
            );
            ensure!(
                corpus.sets.development.len() == 160,
                "qualification development set must contain 160 cases"
            );
            ensure!(
                corpus.sets.frozen.len() == 320,
                "qualification frozen set must contain 320 cases"
            );
        }
        QualificationCorpusProfile::CleanValidationDiagnosticV1 => {
            ensure!(
                corpus.cases.len() == CLEAN_VALIDATION_DIAGNOSTIC_CASE_COUNT,
                "clean validation diagnostic corpus must contain exactly {CLEAN_VALIDATION_DIAGNOSTIC_CASE_COUNT} cases"
            );
            ensure!(
                corpus.sets.smoke.is_empty()
                    && corpus.sets.frozen.is_empty()
                    && corpus.sets.development.len() == CLEAN_VALIDATION_DIAGNOSTIC_CASE_COUNT,
                "clean validation diagnostic cases must all belong to development"
            );
        }
    }
    let membership = corpus
        .sets
        .smoke
        .iter()
        .chain(&corpus.sets.development)
        .chain(&corpus.sets.frozen)
        .collect::<BTreeSet<_>>();
    ensure!(
        membership.len() == corpus.cases.len(),
        "qualification set membership contains duplicates"
    );
    let case_ids = corpus
        .cases
        .iter()
        .map(|case| &case.case_id)
        .collect::<BTreeSet<_>>();
    ensure!(
        case_ids == membership,
        "qualification case IDs and set membership differ"
    );
    ensure!(!raw.is_empty(), "qualification corpus is empty");
    for case in &corpus.cases {
        ensure!(
            case.set == "smoke" || case.set == "development" || case.set == "frozen",
            "qualification case {} has invalid set",
            case.case_id
        );
        ensure!(
            case.origin == "real" || case.origin == "synthetic",
            "qualification case {} has invalid origin",
            case.case_id
        );
        if corpus.profile == QualificationCorpusProfile::CleanValidationDiagnosticV1 {
            ensure!(
                case.set == "development"
                    && case.origin == "real"
                    && case.counterfactual_pair_id.is_none()
                    && case.counterfactual_mutation.is_none()
                    && !case.stability_subset,
                "clean validation diagnostic case {} has certification-only state",
                case.case_id
            );
        }
        let expected_membership = match case.set.as_str() {
            "smoke" => &corpus.sets.smoke,
            "development" => &corpus.sets.development,
            _ => &corpus.sets.frozen,
        };
        ensure!(
            expected_membership.contains(&case.case_id),
            "qualification case {} set differs from membership",
            case.case_id
        );
        if case.counterfactual_pair_id.is_some() {
            ensure!(
                case.counterfactual_mutation.is_some(),
                "qualification case {} lacks counterfactual mutation",
                case.case_id
            );
        }
        if case.set == "frozen" {
            ensure!(
                case.slice.is_some(),
                "frozen qualification case {} lacks a slice",
                case.case_id
            );
        }
        let _ = (case.realistic_noise, case.deterministic_easy);
    }
    Ok(())
}

async fn validate_run_identity(
    identity: &QualificationIdentity,
    corpus_bytes: &[u8],
    manifest: &AnimeInferenceBundleManifest,
    mut manifest_value: JsonValue,
    probe_profile: &AnimeRuntimeProfile,
    runtime_profile_bytes: &[u8],
    current_hardware_fingerprint: &str,
    config: &AnimeQualificationRunConfig,
) -> Result<PreparedQualificationRuntime> {
    probe_profile
        .validate()
        .context("validating sealed hardware-envelope runtime profile")?;
    ensure!(
        probe_profile.probe_result != AnimeRuntimeProbeResult::DeterministicOnly,
        "qualification requires a successful model-capable hardware profile"
    );
    ensure!(
        normalize_sha256(&probe_profile.host_fingerprint)?
            == normalize_sha256(current_hardware_fingerprint)?,
        "hardware-envelope profile belongs to a different host or hardware/driver state"
    );

    let (expected_os, expected_arch, expected_backend) =
        runtime_identity(&identity.qualification_runtime_id)?;
    ensure!(
        std::env::consts::OS == expected_os && std::env::consts::ARCH == expected_arch,
        "qualification runner host does not match qualification runtime ID"
    );
    let matching_runtimes = manifest
        .runtimes
        .iter()
        .filter(|runtime| {
            runtime.os.as_str() == expected_os
                && runtime.arch.as_str() == expected_arch
                && runtime.backend.as_str() == expected_backend
        })
        .collect::<Vec<_>>();
    ensure!(
        matching_runtimes.len() == 1,
        "qualification runtime ID does not resolve uniquely in manifest"
    );
    let runtime = matching_runtimes[0];
    validate_probe_profile(manifest, runtime, probe_profile)?;
    ensure!(
        normalize_sha256(&identity.runtime_profile_fingerprint)?
            == normalize_sha256(&probe_profile.profile_fingerprint)?,
        "qualification runtime profile fingerprint differs from identity"
    );
    ensure!(
        normalize_sha256(&identity.runtime_profile_sha256)? == sha256_bytes(runtime_profile_bytes)
            && identity.runtime_profile_size_bytes == runtime_profile_bytes.len() as u64,
        "qualification runtime profile bytes differ from identity"
    );

    ensure!(
        identity.candidate_order_seeds.len() == 2,
        "qualification identity must contain two candidate-order seeds"
    );
    ensure!(
        identity.candidate_order_seeds[0] != identity.candidate_order_seeds[1],
        "qualification candidate-order seeds must differ"
    );
    ensure!(
        identity.qualification_corpus_schema_version == QUALIFICATION_CORPUS_SCHEMA_VERSION
            && identity.qualification_output_schema_version == QUALIFICATION_OUTPUT_SCHEMA_VERSION
            && identity.qualification_report_schema_version == QUALIFICATION_REPORT_SCHEMA_VERSION,
        "qualification schema-version identity differs from the compiled runner contract"
    );
    ensure!(
        identity.scorer_revision == QUALIFICATION_SCORER_REVISION,
        "qualification scorer revision differs from the compiled runner contract"
    );
    ensure!(
        identity.candidate_cap == ANIME_MATCH_MAX_CANDIDATES,
        "qualification candidate cap differs from production"
    );
    ensure!(
        identity.prompt_revision == ANIME_MATCH_PROMPT_REVISION,
        "qualification prompt revision differs from production"
    );
    ensure!(
        identity.response_schema_revision == ANIME_MATCH_RESPONSE_SCHEMA_REVISION,
        "qualification response schema revision differs from production"
    );
    let (prompt_fingerprint, response_schema_fingerprint, sampling_fingerprint) =
        compiled_matcher_contract_fingerprints()?;
    ensure!(
        normalize_sha256(&identity.prompt_fingerprint)? == prompt_fingerprint,
        "qualification prompt fingerprint differs from compiled production prompt"
    );
    ensure!(
        normalize_sha256(&identity.response_schema_fingerprint)? == response_schema_fingerprint,
        "qualification response schema fingerprint differs from compiled production schema"
    );
    ensure!(
        normalize_sha256(&identity.sampling_profile_fingerprint)? == sampling_fingerprint,
        "qualification sampling fingerprint differs from compiled production profile"
    );
    ensure!(
        identity.protocol_version == manifest.protocol_version,
        "qualification protocol version differs from manifest"
    );
    ensure!(
        identity.matcher_schema_version == ANIME_MATCH_SCHEMA_VERSION
            && identity.matcher_schema_version == manifest.matcher_schema_version,
        "qualification matcher schema differs from production"
    );
    ensure!(
        identity.sampling_profile_revision == ANIME_MATCH_SAMPLING_REVISION
            && identity.sampling_profile_revision
                == manifest.runtime_policy.sampling_profile_revision,
        "qualification sampling revision differs from production"
    );
    ensure!(
        identity.context_tokens == manifest.model.context_tokens
            && identity.max_output_tokens == manifest.model.max_output_tokens,
        "qualification context/output limits differ from manifest"
    );
    ensure!(
        identity.worker_revision == manifest.worker_revision,
        "qualification worker revision differs from manifest"
    );
    ensure!(
        identity.model_revision == manifest.model.revision,
        "qualification model revision differs from manifest"
    );
    let model_hash = sha256_file(&config.model_path, "model")?;
    ensure!(
        model_hash.1 == identity.model_size_bytes && model_hash.1 == manifest.model.size_bytes,
        "qualification model size differs from identity or manifest"
    );
    ensure!(
        normalize_sha256(&identity.model_sha256)? == model_hash.0,
        "qualification model hash differs from installed model"
    );
    ensure!(
        normalize_sha256(&manifest.model.sha256)? == model_hash.0,
        "qualification manifest model hash differs from installed model"
    );
    let runtime_hash = sha256_file(&config.runtime_artifact_path, "runtime artifact")?;
    ensure!(
        normalize_sha256(&identity.runtime_artifact_sha256)? == runtime_hash.0
            && normalize_sha256(&runtime.sha256)? == runtime_hash.0
            && runtime.size_bytes == runtime_hash.1,
        "qualification runtime artifact differs from identity or manifest"
    );
    let source_hash = sha256_file(&config.runtime_source_lock_path, "runtime source lock")?;
    ensure!(
        normalize_sha256(&identity.runtime_source_lock_sha256)? == source_hash.0,
        "qualification runtime source lock differs from identity"
    );
    let scorer_hash = sha256_file(&config.scorer_path, "qualification scorer")?;
    ensure!(
        normalize_sha256(&identity.scorer_sha256)? == scorer_hash.0,
        "qualification scorer bytes differ from identity"
    );
    let corpus_hash = sha256_bytes(corpus_bytes);
    ensure!(
        normalize_sha256(&identity.corpus_sha256)? == corpus_hash,
        "qualification corpus bytes differ from identity"
    );
    ensure!(
        identity.corpus_size_bytes == corpus_bytes.len() as u64,
        "qualification corpus size differs from identity"
    );
    validate_gpu_preflight_binding(identity, config)?;

    let report_fingerprint = manifest_value
        .pointer_mut("/model/qualificationReportFingerprint")
        .ok_or_else(|| anyhow!("manifest lacks qualification report fingerprint"))?;
    *report_fingerprint = JsonValue::String(format!("{SHA256_PREFIX}{}", "0".repeat(64)));
    ensure!(
        normalize_sha256(&identity.manifest_fingerprint)?
            == canonical_json_fingerprint(&manifest_value)?,
        "qualification manifest fingerprint differs from identity"
    );

    let extraction = tempfile::Builder::new()
        .prefix("elixir-anime-qualification-runtime-")
        .tempdir()
        .context("creating qualification runtime extraction directory")?;
    let runtime_root = extraction.path().join("runtime");
    let worker_path = extract_anime_runtime_for_qualification(
        &config.runtime_artifact_path,
        &runtime_root,
        runtime,
    )
    .await
    .context("extracting verified qualification runtime")?;
    let profile = local_profile_from_qualification(
        &config.model_path,
        &worker_path,
        manifest,
        probe_profile,
    )?;
    profile.validate_contract()?;
    Ok(PreparedQualificationRuntime {
        profile,
        extraction,
    })
}

fn validate_gpu_preflight_binding(
    identity: &QualificationIdentity,
    config: &AnimeQualificationRunConfig,
) -> Result<()> {
    let (binding, evidence_path) = match (
        identity.gpu_preflight_evidence.as_ref(),
        config.gpu_preflight_evidence_path.as_ref(),
    ) {
        (None, None) => return Ok(()),
        (Some(_), None) => bail!(
            "qualification identity binds GPU preflight evidence but no evidence path was supplied"
        ),
        (None, Some(_)) => bail!(
            "GPU preflight evidence was supplied but the qualification identity does not bind it"
        ),
        (Some(binding), Some(path)) => (binding, path),
    };
    ensure!(
        identity.certification_target_id.as_deref() == Some(binding.target_id.as_str()),
        "GPU preflight target differs from qualification certification target"
    );
    ensure!(
        !binding.gpu_uuids.is_empty()
            && binding.gpu_uuids.len() <= 8
            && binding
                .gpu_uuids
                .iter()
                .all(|uuid| valid_nvidia_gpu_uuid(uuid)),
        "qualification GPU preflight binding contains an invalid NVIDIA GPU UUID"
    );
    let unique_uuids = binding.gpu_uuids.iter().collect::<BTreeSet<_>>();
    ensure!(
        unique_uuids.len() == binding.gpu_uuids.len(),
        "qualification GPU preflight binding contains duplicate GPU UUIDs"
    );

    let evidence_bytes = read_limited(evidence_path, "qualification GPU preflight evidence")?;
    ensure!(
        normalize_sha256(&binding.evidence_sha256)? == sha256_bytes(&evidence_bytes)
            && binding.evidence_size_bytes == evidence_bytes.len() as u64,
        "qualification GPU preflight evidence bytes differ from identity"
    );
    let evidence = parse_strict_json(&evidence_bytes, "qualification GPU preflight evidence")?;
    ensure!(
        normalize_sha256(&binding.evidence_fingerprint)? == canonical_json_fingerprint(&evidence)?,
        "qualification GPU preflight evidence fingerprint differs from identity"
    );
    ensure!(
        evidence.get("schemaVersion").and_then(JsonValue::as_u64) == Some(1)
            && evidence.get("status").and_then(JsonValue::as_str) == Some("passed"),
        "qualification GPU preflight evidence is not passed schema v1 evidence"
    );
    let host_uuids = gpu_uuids_from_preflight(&evidence, "/host/gpus", "host")?;
    ensure!(
        host_uuids == binding.gpu_uuids,
        "qualification GPU preflight host UUIDs differ from identity"
    );
    if binding.host_container_parity {
        let container_uuids = gpu_uuids_from_preflight(&evidence, "/container/gpus", "container")?;
        ensure!(
            container_uuids == host_uuids
                && evidence
                    .pointer("/checks/hostContainerGpuIdentityParity")
                    .and_then(JsonValue::as_bool)
                    == Some(true),
            "qualification GPU preflight does not prove exact host/container UUID parity"
        );
    } else {
        ensure!(
            evidence.get("container").is_none(),
            "host-only qualification identity cannot bind container GPU evidence"
        );
    }
    Ok(())
}

fn gpu_uuids_from_preflight(
    evidence: &JsonValue,
    pointer: &str,
    label: &str,
) -> Result<Vec<String>> {
    let gpus = evidence
        .pointer(pointer)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("qualification GPU preflight {label} GPU list is missing"))?;
    ensure!(
        !gpus.is_empty() && gpus.len() <= 8,
        "qualification GPU preflight {label} GPU list has an invalid size"
    );
    let mut uuids = Vec::with_capacity(gpus.len());
    for gpu in gpus {
        let uuid = gpu
            .get("uuid")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("qualification GPU preflight {label} GPU UUID is missing"))?;
        ensure!(
            valid_nvidia_gpu_uuid(uuid),
            "qualification GPU preflight {label} GPU UUID is invalid"
        );
        uuids.push(uuid.to_string());
    }
    ensure!(
        uuids.iter().collect::<BTreeSet<_>>().len() == uuids.len(),
        "qualification GPU preflight {label} GPU UUIDs are not unique"
    );
    Ok(uuids)
}

fn valid_nvidia_gpu_uuid(value: &str) -> bool {
    value.strip_prefix("GPU-").is_some_and(|suffix| {
        (8..=124).contains(&suffix.len())
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

fn validate_probe_profile(
    manifest: &AnimeInferenceBundleManifest,
    runtime: &AnimeRuntimeArtifactManifest,
    profile: &AnimeRuntimeProfile,
) -> Result<()> {
    ensure!(
        profile.bundle_version == manifest.bundle_version
            && profile.model_id == manifest.model.id
            && profile.model_revision == manifest.model.revision
            && profile.worker_revision == manifest.worker_revision,
        "hardware-envelope profile identity differs from candidate manifest"
    );
    ensure!(
        profile.runtime_artifact_key == runtime.artifact_key(),
        "hardware-envelope profile runtime artifact differs from candidate manifest"
    );
    ensure!(
        profile.kv_cache_type == manifest.runtime_policy.kv_cache_type,
        "hardware-envelope profile KV cache differs from candidate policy"
    );
    ensure!(
        runtime_backend_supports_profile(runtime.backend, profile.execution_backend),
        "hardware-envelope profile backend is incompatible with qualification runtime"
    );

    let limits = InferenceProbeLimits::default();
    ensure!(
        profile.load_time_ms > 0
            && Duration::from_millis(profile.load_time_ms) <= limits.maximum_load_time,
        "hardware-envelope profile load measurement is absent or exceeds the production gate"
    );
    ensure!(
        profile.warm_latency_ms > 0
            && Duration::from_millis(profile.warm_latency_ms) <= limits.maximum_warm_latency,
        "hardware-envelope profile warm-latency measurement is absent or exceeds the production gate"
    );
    ensure!(
        profile.peak_rss_bytes > 0 && profile.peak_rss_bytes <= limits.maximum_worker_rss_bytes,
        "hardware-envelope profile RSS measurement is absent or exceeds the production gate"
    );
    Ok(())
}

fn runtime_backend_supports_profile(
    runtime: AnimeRuntimeBackend,
    execution: AnimeExecutionBackend,
) -> bool {
    match execution {
        AnimeExecutionBackend::Cpu => matches!(
            runtime,
            AnimeRuntimeBackend::MetalCpu
                | AnimeRuntimeBackend::CudaCpu
                | AnimeRuntimeBackend::HipCpu
                | AnimeRuntimeBackend::VulkanCpu
                | AnimeRuntimeBackend::Cpu
        ),
        AnimeExecutionBackend::Metal => runtime == AnimeRuntimeBackend::MetalCpu,
        AnimeExecutionBackend::Cuda => runtime == AnimeRuntimeBackend::CudaCpu,
        AnimeExecutionBackend::Hip => runtime == AnimeRuntimeBackend::HipCpu,
        AnimeExecutionBackend::Vulkan => runtime == AnimeRuntimeBackend::VulkanCpu,
    }
}

fn local_profile_from_qualification(
    model_path: &Path,
    worker_path: &Path,
    manifest: &AnimeInferenceBundleManifest,
    profile: &AnimeRuntimeProfile,
) -> Result<LocalModelRuntimeProfile> {
    let sampling = LocalModelSamplingProfile::default();
    ensure!(
        sampling.revision == manifest.runtime_policy.sampling_profile_revision,
        "candidate manifest sampling profile is unsupported by this server"
    );
    let kv_cache_type = match profile.kv_cache_type {
        AnimeKvCacheType::F16 => "f16",
        AnimeKvCacheType::Q8_0 => "q8_0",
    };
    Ok(LocalModelRuntimeProfile {
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        backend: profile.execution_backend.as_str().to_string(),
        profile_fingerprint: profile.profile_fingerprint.clone(),
        protocol_version: manifest.protocol_version,
        matcher_schema_version: manifest.matcher_schema_version,
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
        worker_path: worker_path.to_path_buf(),
        model_path: model_path.to_path_buf(),
        context_tokens: manifest.model.context_tokens,
        max_output_tokens: manifest.model.max_output_tokens,
        threads: u32::from(profile.cpu_thread_count),
        batch_threads: u32::from(profile.batch_thread_count),
        gpu_layers: profile.gpu_layer_count,
        kv_cache_type: kv_cache_type.to_string(),
        peak_rss_bytes: profile.peak_rss_bytes,
        idle_unload_seconds: manifest.runtime_policy.idle_unload_seconds,
        sampling,
    })
}

fn compiled_matcher_contract_fingerprints() -> Result<(String, String, String)> {
    matcher_contract_fingerprints(LOCAL_MODEL_CONTRACT_SOURCE)
}

fn matcher_contract_fingerprints(source: &str) -> Result<(String, String, String)> {
    let normalized_source = source.replace("\r\n", "\n");
    let source = normalized_source.as_str();
    let prompt = source_between(
        source,
        "const SEMANTIC_EVIDENCE_PROMPT: &str = r#\"",
        "\n#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]",
        "semantic evidence prompt",
    )?;
    let prompt_builder = source_between(
        source,
        "fn build_semantic_chat_request(",
        "\nfn semantic_response_grammar(",
        "semantic evidence prompt builder",
    )?;
    let response_schema = source_between(
        source,
        "fn semantic_response_grammar(",
        "\nfn compact_direct_request(",
        "semantic evidence response grammar",
    )?;
    let response_execution = source_between(
        source,
        "async fn resolve_semantic_request(",
        "\nasync fn request_direct_completion(",
        "semantic evidence response decoding and validation",
    )?;
    let sampling = source_between(
        source,
        "impl Default for LocalModelSamplingProfile {",
        "\nimpl LocalModelSamplingProfile {",
        "sampling profile",
    )?;
    let prompt_contract = format!("{prompt}\n--semantic-request-builder--\n{prompt_builder}");
    let response_contract =
        format!("{response_schema}\n--semantic-response-execution--\n{response_execution}");
    Ok((
        sha256_bytes(prompt_contract.as_bytes()),
        sha256_bytes(response_contract.as_bytes()),
        sha256_bytes(sampling.as_bytes()),
    ))
}

fn source_between<'a>(source: &'a str, start: &str, end: &str, label: &str) -> Result<&'a str> {
    let start_index = source
        .find(start)
        .ok_or_else(|| anyhow!("compiled local-model source lacks {label} start"))?;
    let content_index = start_index + start.len();
    let end_offset = source[content_index..]
        .find(end)
        .ok_or_else(|| anyhow!("compiled local-model source lacks {label} end"))?;
    // Include start markers in every fingerprint. They carry the contract's
    // field/function identity and make renamed semantics an explicit revision.
    Ok(&source[start_index..content_index + end_offset])
}

fn runtime_identity(runtime_id: &str) -> Result<(&'static str, &'static str, &'static str)> {
    match runtime_id {
        "macos-aarch64-metal-cpu" => Ok(("macos", "aarch64", "metal_cpu")),
        "macos-x86_64-metal-cpu" => Ok(("macos", "x86_64", "metal_cpu")),
        "windows-x86_64-cpu" => Ok(("windows", "x86_64", "cpu")),
        "windows-x86_64-cuda" => Ok(("windows", "x86_64", "cuda_cpu")),
        "windows-x86_64-vulkan" => Ok(("windows", "x86_64", "vulkan_cpu")),
        "linux-x86_64-cpu" => Ok(("linux", "x86_64", "cpu")),
        "linux-x86_64-cuda" => Ok(("linux", "x86_64", "cuda_cpu")),
        "linux-x86_64-hip" => Ok(("linux", "x86_64", "hip_cpu")),
        "linux-x86_64-vulkan" => Ok(("linux", "x86_64", "vulkan_cpu")),
        "linux-aarch64-cpu" => Ok(("linux", "aarch64", "cpu")),
        other => bail!("unknown qualification runtime ID '{other}'"),
    }
}

fn normalize_sha256(value: &str) -> Result<String> {
    let digest = value
        .trim()
        .strip_prefix(SHA256_PREFIX)
        .unwrap_or(value.trim());
    ensure!(
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid SHA-256 value"
    );
    Ok(format!("{SHA256_PREFIX}{}", digest.to_ascii_lowercase()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{SHA256_PREFIX}{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path, label: &str) -> Result<(String, u64)> {
    let mut file =
        File::open(path).with_context(|| format!("opening {label} at {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("reading {label} metadata"))?;
    ensure!(metadata.is_file(), "{label} is not a regular file");
    let mut digest = Sha256::new();
    let mut bytes = file_hash_buffer();
    let mut size = 0_u64;
    loop {
        let read = file
            .read(&mut bytes)
            .with_context(|| format!("hashing {label}"))?;
        if read == 0 {
            break;
        }
        digest.update(&bytes[..read]);
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("{label} size overflow"))?;
    }
    Ok((format!("{SHA256_PREFIX}{:x}", digest.finalize()), size))
}

fn file_hash_buffer() -> Vec<u8> {
    vec![0_u8; FILE_HASH_BUFFER_BYTES]
}

fn canonical_regular_file(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("resolving {label} at {}", path.display()))?;
    ensure!(
        canonical
            .metadata()
            .with_context(|| format!("reading {label} metadata"))?
            .is_file(),
        "{label} is not a regular file"
    );
    Ok(canonical)
}

fn canonical_json_fingerprint(value: &JsonValue) -> Result<String> {
    Ok(sha256_bytes(&canonical_json_bytes(value)?))
}

fn canonical_json_bytes(value: &JsonValue) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonicalize_json(value)).context("encoding canonical qualification JSON")
}

fn canonicalize_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(canonicalize_json).collect())
        }
        JsonValue::Object(values) => {
            let mut ordered = values.iter().collect::<Vec<_>>();
            ordered.sort_by(|(left, _), (right, _)| left.cmp(right));
            JsonValue::Object(
                ordered
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonicalize_json(value)))
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
}

fn read_limited(path: &Path, label: &str) -> Result<Vec<u8>> {
    let mut file =
        File::open(path).with_context(|| format!("opening {label} at {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("reading {label} metadata"))?;
    ensure!(
        metadata.is_file() && metadata.len() > 0 && metadata.len() <= MAX_QUALIFICATION_JSON_BYTES,
        "{label} size is invalid"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)
        .with_context(|| format!("reading {label}"))?;
    Ok(bytes)
}

struct StrictJsonValue(JsonValue);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonVisitor)
    }
}

struct StrictJsonVisitor;

impl<'de> Visitor<'de> for StrictJsonVisitor {
    type Value = StrictJsonValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: DeError,
    {
        serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .map(StrictJsonValue)
            .ok_or_else(|| E::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: DeError,
    {
        self.visit_string(value.to_string())
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::String(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        StrictJsonValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut values: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut result = Vec::with_capacity(values.size_hint().unwrap_or(0));
        while let Some(StrictJsonValue(value)) = values.next_element()? {
            result.push(value);
        }
        Ok(StrictJsonValue(JsonValue::Array(result)))
    }

    fn visit_map<A>(self, mut values: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut result = serde_json::Map::with_capacity(values.size_hint().unwrap_or(0));
        while let Some((key, StrictJsonValue(value))) = values.next_entry::<String, _>()? {
            if result.insert(key.clone(), value).is_some() {
                return Err(A::Error::custom(format!(
                    "JSON contains duplicate key {key:?}"
                )));
            }
        }
        Ok(StrictJsonValue(JsonValue::Object(result)))
    }
}

fn parse_strict_json(bytes: &[u8], label: &str) -> Result<JsonValue> {
    serde_json::from_slice::<StrictJsonValue>(bytes)
        .map(|value| value.0)
        .with_context(|| format!("decoding strict {label} JSON"))
}

fn read_json<T: DeserializeOwned>(path: &Path, label: &str) -> Result<T> {
    let bytes = read_limited(path, label)?;
    serde_json::from_value(parse_strict_json(&bytes, label)?)
        .with_context(|| format!("decoding {label}"))
}

fn write_new_canonical_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).context("creating qualification output directory")?;
    ensure!(
        !path.exists(),
        "qualification output already exists at {}",
        path.display()
    );
    let value = serde_json::to_value(value).context("encoding qualification output")?;
    let mut bytes = canonical_json_bytes(&value)?;
    bytes.push(b'\n');
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .context("creating qualification output temporary file")?;
    temporary
        .write_all(&bytes)
        .context("writing qualification output")?;
    temporary
        .as_file_mut()
        .sync_all()
        .context("syncing qualification output")?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| error.error)
        .with_context(|| format!("publishing qualification output at {}", path.display()))?;
    Ok(())
}

fn unique_strings(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acquisition::release_resolution::anime::{AnimeCandidateTarget, AnimeScopedAlias};
    use crate::anime_matching::{
        ANIME_RUNTIME_PROFILE_SCHEMA_VERSION, AnimeDeviceClass, AnimeHostArch, AnimeHostOs,
        AnimeMatchAlias, AnimeMatchAliasKind, AnimeMatchCandidate, AnimeMatchContext,
        AnimeMatchContextTarget, AnimeMatchFile, AnimeMatchMediaType, AnimeMatchParseFacts,
        AnimeMatchScope, AnimeMatchSeasonContext, AnimeMatchTarget, AnimeModelArtifactManifest,
        AnimeModelFormat, AnimeRuntimeArchiveFormat, AnimeRuntimePolicyManifest, AnimeThinkingMode,
    };
    use crate::http::handlers::acquisition_sources::AcquisitionCandidateFile;

    #[test]
    fn alm9_frozen_ova_episode_two_cannot_validate_for_episode_one() -> Result<()> {
        let corpus: QualificationCorpus = serde_json::from_str(include_str!(
            "../../../artifacts/anime-inference/frozen-corpus/qualification-corpus.json"
        ))?;
        let case = corpus
            .cases
            .iter()
            .find(|case| case.case_id == "frozen-special_boundaries-real-005")
            .context("frozen OVA boundary case")?;
        let input: QualificationCaseInput = serde_json::from_value(case.input.clone())?;
        let candidate = input
            .request
            .candidates
            .iter()
            .find(|candidate| candidate.candidate_key == "candidate-5")
            .context("episode-two candidate")?;
        let request = build_semantic_evidence_request(
            &input.request,
            candidate.candidate_key.clone(),
            candidate.title.clone(),
            None,
            candidate.parse_facts.title_candidates.iter().cloned(),
            candidate.parse_facts.season_numbers.iter().copied(),
            candidate.parse_facts.episode_numbers.iter().copied(),
            candidate
                .parse_facts
                .absolute_episode_numbers
                .iter()
                .copied(),
            anime_semantic_media_kinds(&candidate.title, &candidate.parse_facts),
        )?
        .context("semantic request")?;

        assert_eq!(request.observed_release_episode_numbers, vec![2, 2014]);
        let hypothesis = request
            .hypotheses
            .get(2)
            .context("reported hypothesis index")?;
        assert_eq!(
            hypothesis.numbering,
            crate::anime_matching::AnimeSemanticNumbering::EntityOnly
        );
        assert_eq!(
            hypothesis.media_kind,
            crate::anime_matching::AnimeSemanticMediaKind::Ova
        );
        assert!(hypothesis.episode_numbers.is_empty());
        assert!(hypothesis.absolute_episode_numbers.is_empty());
        assert!(
            validate_semantic_evidence_response(
                &request,
                &crate::anime_matching::AnimeSemanticEvidenceResponse {
                    schema_version: 1,
                    hypothesis_index: Some(2),
                },
            )?
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn qualification_hash_buffer_is_heap_backed() {
        let buffer = file_hash_buffer();
        assert_eq!(buffer.len(), FILE_HASH_BUFFER_BYTES);
        assert_eq!(
            std::mem::size_of_val(&buffer),
            std::mem::size_of::<Vec<u8>>()
        );
    }

    fn qualification_manifest() -> AnimeInferenceBundleManifest {
        AnimeInferenceBundleManifest {
            schema_version: 1,
            bundle_version: "1.0.0".to_string(),
            protocol_version: 1,
            matcher_schema_version: 1,
            minimum_server_version: "0.1.0".to_string(),
            worker_revision: "worker-1".to_string(),
            model: AnimeModelArtifactManifest {
                id: "qwen".to_string(),
                revision: "model-1".to_string(),
                upstream_model_id: "Qwen/Qwen".to_string(),
                upstream_revision: "upstream-1".to_string(),
                license: "apache-2.0".to_string(),
                format: AnimeModelFormat::Gguf,
                quantization: "Q4_K_M".to_string(),
                transformer_layers: 36,
                context_tokens: 4_096,
                max_output_tokens: 512,
                thinking_mode: AnimeThinkingMode::NonThinkingOnly,
                chat_template_revision: "template-1".to_string(),
                conversion_tool_revision: "converter-1".to_string(),
                qualification_report_fingerprint: format!("sha256:{}", "0".repeat(64)),
                url: "https://example.invalid/model.gguf".to_string(),
                sha256: format!("sha256:{}", "1".repeat(64)),
                size_bytes: 1_024,
            },
            runtime_policy: AnimeRuntimePolicyManifest {
                sampling_profile_revision: ANIME_MATCH_SAMPLING_REVISION.to_string(),
                parallel: 1,
                kv_cache_type: AnimeKvCacheType::Q8_0,
                idle_unload_seconds: 300,
            },
            runtimes: vec![AnimeRuntimeArtifactManifest {
                os: AnimeHostOs::Macos,
                arch: AnimeHostArch::X86_64,
                device_class: Some(AnimeDeviceClass::Cpu),
                backend: AnimeRuntimeBackend::MetalCpu,
                priority: 1,
                revision: "runtime-1".to_string(),
                minimum_os_version: "13.0".to_string(),
                required_cpu_features: Vec::new(),
                minimum_driver_version: None,
                minimum_device_memory_bytes: 0,
                archive_format: AnimeRuntimeArchiveFormat::TarGz,
                entrypoint: "bin/llama-server".to_string(),
                packaged_dependencies: Vec::new(),
                url: "https://example.invalid/runtime.tar.gz".to_string(),
                sha256: format!("sha256:{}", "2".repeat(64)),
                size_bytes: 2_048,
                installed_size_bytes: 4_096,
            }],
        }
    }

    fn sealed_cpu_profile(manifest: &AnimeInferenceBundleManifest) -> Result<AnimeRuntimeProfile> {
        AnimeRuntimeProfile {
            schema_version: ANIME_RUNTIME_PROFILE_SCHEMA_VERSION,
            bundle_version: manifest.bundle_version.clone(),
            model_id: manifest.model.id.clone(),
            model_revision: manifest.model.revision.clone(),
            worker_revision: manifest.worker_revision.clone(),
            runtime_artifact_key: manifest.runtimes[0].artifact_key(),
            host_fingerprint: format!("sha256:{}", "3".repeat(64)),
            execution_backend: AnimeExecutionBackend::Cpu,
            device_id: None,
            gpu_layer_count: 0,
            cpu_thread_count: 2,
            batch_thread_count: 4,
            kv_cache_type: manifest.runtime_policy.kv_cache_type,
            load_time_ms: 1_000,
            warm_latency_ms: 100,
            peak_rss_bytes: 512 * 1024 * 1024,
            peak_device_memory_bytes: None,
            probe_result: AnimeRuntimeProbeResult::CpuBalanced,
            probed_at: "2026-08-08T12:00:00Z".to_string(),
            profile_fingerprint: String::new(),
        }
        .seal()
    }

    fn request() -> AnimeMatchRequest {
        AnimeMatchRequest {
            schema_version: 1,
            request_id: "qualification-test".to_string(),
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
                graph_fingerprint: "graph".to_string(),
                seasons: vec![AnimeMatchSeasonContext {
                    season_number: 2,
                    anilist_id: "27899".to_string(),
                    episode_number_offset: 0,
                    aliases: vec![AnimeMatchAlias {
                        value: "Tokyo Ghoul Root A".to_string(),
                        kind: AnimeMatchAliasKind::English,
                        source: Some("fixture".to_string()),
                        language: Some("en".to_string()),
                    }],
                    targets: vec![AnimeMatchContextTarget {
                        target_key: "S02E01".to_string(),
                        title: "New Surge".to_string(),
                        season_number: Some(2),
                        episode_number: Some(1),
                        absolute_episode_number: Some(13),
                        tvdb_episode_id: None,
                        anidb_episode_id: None,
                    }],
                }],
            },
            candidates: (0..4)
                .map(|index| AnimeMatchCandidate {
                    candidate_key: format!("candidate-{index}"),
                    title: format!("Tokyo Ghoul Root A - {:02}", index + 1),
                    files: Vec::new(),
                    parse_facts: AnimeMatchParseFacts::default(),
                })
                .collect(),
        }
    }

    fn scoring_context() -> AnimeCandidateScoringContext {
        AnimeCandidateScoringContext {
            graph_fingerprint: Some("graph".to_string()),
            aliases: vec!["Tokyo Ghoul".to_string()],
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Tokyo Ghoul Root A".to_string(),
                source: "fixture".to_string(),
                language: Some("en".to_string()),
                season_number: Some(2),
                anilist_season_id: Some("27899".to_string()),
            }],
            targets: vec![AnimeCandidateTarget {
                target_key: "S02E01".to_string(),
                canonical_key: None,
                title: "New Surge".to_string(),
                season_number: Some(2),
                anilist_season_id: Some("27899".to_string()),
                episode_number: Some(1),
                absolute_episode_number: Some(13),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        }
    }

    #[test]
    fn alm9_qualification_binds_worker_context_to_exact_production_scoring_graph() -> Result<()> {
        let base_scoring_context = scoring_context();
        let mut request = request();
        request.context = acquisition_match_context(
            &request.target.canonical_title,
            &base_scoring_context,
            &request.target,
        )?;
        let mut input = QualificationCaseInput {
            request,
            scoring_context: base_scoring_context,
            acquisition_candidates: Vec::new(),
            route_context: QualificationRouteContext {
                file_selection_supported_by_candidate_key: BTreeMap::new(),
            },
        };
        validate_scoring_context_binding("bound", &input)?;

        input.scoring_context.graph_fingerprint = Some("different-graph".to_string());
        assert!(
            validate_scoring_context_binding("graph", &input)
                .expect_err("divergent graph must fail")
                .to_string()
                .contains("production scoring graph")
        );
        input.scoring_context = scoring_context();
        input.scoring_context.targets[0].title = "Different Episode".to_string();
        assert!(
            validate_scoring_context_binding("target", &input)
                .expect_err("divergent target must fail")
                .to_string()
                .contains("production scoring graph")
        );
        input.scoring_context = scoring_context();
        input.scoring_context.scoped_aliases[0].display = "Invented Alias".to_string();
        assert!(
            validate_scoring_context_binding("alias", &input)
                .expect_err("divergent alias must fail")
                .to_string()
                .contains("production scoring graph")
        );
        Ok(())
    }

    #[test]
    fn alm9_candidate_order_is_stable_and_preserves_keys() {
        let request = request();
        assert_eq!(ordered_request(&request, 0, 0), request);
        let permuted = ordered_request(&request, 1729, 0);
        let original = request
            .candidates
            .iter()
            .map(|item| &item.candidate_key)
            .collect::<BTreeSet<_>>();
        let changed = permuted
            .candidates
            .iter()
            .map(|item| &item.candidate_key)
            .collect::<BTreeSet<_>>();
        assert_eq!(changed, original);
        assert_ne!(permuted.candidates, request.candidates);
    }

    #[test]
    fn alm9_cross_runtime_case_selection_is_exact_ordered_and_content_bound() -> Result<()> {
        let corpus_case_ids = vec![
            "smoke-001".to_string(),
            "development-001".to_string(),
            "frozen-001".to_string(),
        ];
        let selected_ids = vec!["smoke-001".to_string(), "frozen-001".to_string()];
        let selection = QualificationCaseSelection {
            selection_id: "cross-runtime-v1".to_string(),
            case_ids_fingerprint: canonical_json_fingerprint(&serde_json::to_value(
                &selected_ids,
            )?)?,
            case_ids: selected_ids,
        };
        let selected =
            validate_case_selection(&corpus_case_ids, Some("linux-amd-cloud"), Some(&selection))?
                .expect("selection");
        assert_eq!(selected.len(), 2);

        let mut reversed = selection.clone();
        reversed.case_ids.reverse();
        reversed.case_ids_fingerprint =
            canonical_json_fingerprint(&serde_json::to_value(&reversed.case_ids)?)?;
        assert!(
            validate_case_selection(&corpus_case_ids, Some("linux-amd-cloud"), Some(&reversed),)
                .expect_err("corpus-order drift must fail")
                .to_string()
                .contains("frozen corpus order")
        );
        assert!(
            validate_case_selection(&corpus_case_ids, None, Some(&selection))
                .expect_err("unscoped selection must fail")
                .to_string()
                .contains("certification target")
        );
        Ok(())
    }

    #[test]
    fn alm9_permutation_preserves_original_acquisition_source_indexes() -> Result<()> {
        let base = request();
        let candidates = base
            .candidates
            .iter()
            .map(|candidate| AcquisitionCandidate {
                id: None,
                title: candidate.title.clone(),
                source: "fixture".to_string(),
                source_kind: "torrent".to_string(),
                info_hash: None,
                file_index: None,
                quality: None,
                size_bytes: None,
                seeders: None,
                language: None,
                cached_debrid: None,
                rank: None,
                score: None,
                score_badges: Vec::new(),
                files: Vec::new(),
                supported_routes: Vec::new(),
                default_route: None,
                raw: None,
            })
            .collect::<Vec<_>>();
        let prepared = prepare_stable_request(&base, ordered_request(&base, 1729, 0), &candidates)?;
        let acquisition =
            acquisition_source_map(prepared.request(), &candidates, prepared.source_map())?;
        for (index, candidate) in base.candidates.iter().enumerate() {
            assert_eq!(
                acquisition
                    .candidate_source(&candidate.candidate_key)
                    .expect("candidate source")
                    .candidate_index,
                index
            );
        }
        Ok(())
    }

    #[test]
    fn alm9_plan_diff_uses_exact_scorer_field_order() {
        let expected = empty_final_plan(QualificationDisposition::NoMatch);
        let mut actual = expected.clone();
        actual.disposition = QualificationDisposition::Unresolved;
        actual.candidate_plans.push(QualificationCandidatePlan {
            candidate_key: "candidate-0".to_string(),
            target_keys: vec!["S02E01".to_string()],
            file_keys: Vec::new(),
            audio_eligibility: QualificationAudioEligibility::Unknown,
            coverage: Vec::new(),
        });
        let diff = final_plan_diff(&expected, &actual);
        assert!(!diff.matches);
        assert_eq!(
            diff.mismatched_fields,
            vec!["disposition", "candidatePlans"]
        );
    }

    #[test]
    fn alm9_model_only_resolution_rejects_required_audio_mismatches() -> Result<()> {
        let mut request = request();
        request.target.audio_preference.mode = AnimeMatchAudioPreferenceMode::Require;
        request.target.audio_preference.accepted_profiles = vec!["dubbed".to_string()];
        let preference = language_preference(&request.target.audio_preference);
        let source_map = AnimeMatchSourceMap::new(
            BTreeMap::from([(
                "candidate-0".to_string(),
                QualificationCandidateSource { candidate_index: 0 },
            )]),
            BTreeMap::new(),
        );
        let resolution = model_only_resolution(
            1,
            &preference,
            &[AnimeCandidateMatch {
                candidate_key: "candidate-0".to_string(),
                matched_target_keys: vec!["S02E01".to_string()],
                audio_profile: AnimeMatchAudioProfile::DualAudio,
                selected_file_keys: None,
            }],
            &source_map,
        )?;
        assert!(resolution.candidate_plans.iter().all(Option::is_none));
        assert!(!resolution.saw_partial_or_ambiguous);
        assert_eq!(
            final_plan_for_resolution(&request, &resolution)?.disposition,
            QualificationDisposition::NoMatch
        );
        Ok(())
    }

    #[test]
    fn alm9_vector_union_retains_partial_plans_and_matches_only_complete_coverage() -> Result<()> {
        let mut request = request();
        request.target.wanted_target_keys.push("S02E02".to_string());
        request.target.episode_numbers.push(2);
        request.target.absolute_episode_numbers.push(14);
        request.context.seasons[0]
            .targets
            .push(AnimeMatchContextTarget {
                target_key: "S02E02".to_string(),
                title: "Dancing Flowers".to_string(),
                season_number: Some(2),
                episode_number: Some(2),
                absolute_episode_number: Some(14),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            });

        let candidate_plan = |candidate_key: &str, target_key: &str| QualificationCandidatePlan {
            candidate_key: candidate_key.to_string(),
            target_keys: vec![target_key.to_string()],
            file_keys: Vec::new(),
            audio_eligibility: QualificationAudioEligibility::NotApplicable,
            coverage: vec![QualificationCoverageEntry {
                target_key: target_key.to_string(),
                file_key: None,
                status: QualificationCoverageStatus::Covered,
            }],
        };
        let partial = QualificationResolutionState {
            candidate_plans: vec![
                None,
                None,
                Some(candidate_plan("candidate-2", "S02E01")),
                None,
            ],
            saw_partial_or_ambiguous: true,
        };
        let partial_final = final_plan_for_resolution(&request, &partial)?;
        assert_eq!(
            deterministic_union_state(&request, &partial),
            DeterministicMatchState::Difficult
        );
        assert_eq!(
            partial_final.disposition,
            QualificationDisposition::Unresolved
        );
        assert_eq!(partial_final.candidate_plans.len(), 1);

        let mut complete = partial;
        complete.candidate_plans[0] = Some(candidate_plan("candidate-0", "S02E02"));
        let complete_final = final_plan_for_resolution(&request, &complete)?;
        assert_eq!(
            deterministic_union_state(&request, &complete),
            DeterministicMatchState::Definitive
        );
        assert_eq!(
            complete_final.disposition,
            QualificationDisposition::Matched
        );
        assert_eq!(complete_final.season_number, Some(2));
        assert_eq!(complete_final.episode_numbers, vec![1, 2]);
        assert_eq!(complete_final.absolute_episode_numbers, vec![13, 14]);
        assert_eq!(
            complete_final
                .candidate_plans
                .iter()
                .map(|plan| plan.candidate_key.as_str())
                .collect::<Vec<_>>(),
            vec!["candidate-0", "candidate-2"]
        );

        let mut overcomplete = complete;
        overcomplete.candidate_plans[1] = Some(candidate_plan("candidate-1", "S02E03"));
        assert_eq!(
            deterministic_union_state(&request, &overcomplete),
            DeterministicMatchState::Difficult
        );
        Ok(())
    }

    #[test]
    fn alm9_definitive_other_target_is_no_match_not_unresolved() -> Result<()> {
        let candidate_title = "[SubsPlease] Tokyo Ghoul:re S03E01 [1080p]";
        let mut scoring_context = scoring_context();
        scoring_context.scoped_aliases.push(AnimeScopedAlias {
            display: "Tokyo Ghoul:re".to_string(),
            source: "fixture".to_string(),
            language: Some("en".to_string()),
            season_number: Some(3),
            anilist_season_id: Some("100240".to_string()),
        });
        scoring_context.targets.push(AnimeCandidateTarget {
            target_key: "S03E01".to_string(),
            canonical_key: None,
            title: "Place".to_string(),
            season_number: Some(3),
            anilist_season_id: Some("100240".to_string()),
            episode_number: Some(1),
            absolute_episode_number: Some(25),
            tvdb_episode_id: None,
            anidb_episode_id: None,
        });

        let mut request = request();
        request.candidates.truncate(1);
        request.candidates[0].title = candidate_title.to_string();
        request.context = acquisition_match_context(
            &request.target.canonical_title,
            &scoring_context,
            &request.target,
        )?;
        let candidate_key = request.candidates[0].candidate_key.clone();
        let input = QualificationCaseInput {
            request,
            scoring_context,
            acquisition_candidates: vec![AcquisitionCandidate {
                id: None,
                title: candidate_title.to_string(),
                source: "fixture".to_string(),
                source_kind: "torrent".to_string(),
                info_hash: None,
                file_index: None,
                quality: None,
                size_bytes: None,
                seeders: None,
                language: None,
                cached_debrid: None,
                rank: None,
                score: None,
                score_badges: Vec::new(),
                files: Vec::new(),
                supported_routes: Vec::new(),
                default_route: None,
                raw: None,
            }],
            route_context: QualificationRouteContext {
                file_selection_supported_by_candidate_key: [(candidate_key, true)]
                    .into_iter()
                    .collect(),
            },
        };

        let baseline = deterministic_baseline(&input)?;
        assert!(baseline.candidate_plans.iter().all(Option::is_none));
        assert!(!baseline.saw_partial_or_ambiguous);
        assert_eq!(
            final_plan_for_resolution(&input.request, &baseline)?.disposition,
            QualificationDisposition::NoMatch
        );
        Ok(())
    }

    #[test]
    fn alm9_route_capabilities_follow_original_sources_through_permutation() -> Result<()> {
        let base = request();
        let candidates = base
            .candidates
            .iter()
            .map(|candidate| AcquisitionCandidate {
                id: None,
                title: candidate.title.clone(),
                source: "fixture".to_string(),
                source_kind: "torrent".to_string(),
                info_hash: None,
                file_index: None,
                quality: None,
                size_bytes: None,
                seeders: None,
                language: None,
                cached_debrid: None,
                rank: None,
                score: None,
                score_badges: Vec::new(),
                files: Vec::new(),
                supported_routes: Vec::new(),
                default_route: None,
                raw: None,
            })
            .collect::<Vec<_>>();
        let ordered = ordered_request(&base, 1729, 0);
        let prepared = prepare_stable_request(&base, ordered, &candidates)?;
        let route_context = QualificationRouteContext {
            file_selection_supported_by_candidate_key: base
                .candidates
                .iter()
                .enumerate()
                .map(|(index, candidate)| (candidate.candidate_key.clone(), index % 2 == 0))
                .collect(),
        };
        assert_eq!(
            file_selection_support_by_candidate_index(
                prepared.request(),
                candidates.len(),
                &route_context,
                prepared.source_map(),
            )?,
            vec![true, false, true, false]
        );
        Ok(())
    }

    #[test]
    fn alm9_retired_direct_plan_lock_cannot_certify_the_semantic_contract() -> Result<()> {
        let lock = parse_strict_json(
            include_bytes!("../../../docs/contracts/anime-inference-qualification.lock.json"),
            "qualification lock fixture",
        )?;
        let contract = lock
            .get("contract")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| anyhow!("qualification lock fixture lacks contract"))?;
        let (prompt, schema, sampling) = compiled_matcher_contract_fingerprints()?;
        assert_ne!(contract["promptRevision"], ANIME_MATCH_PROMPT_REVISION);
        assert_ne!(
            contract["responseSchemaRevision"],
            ANIME_MATCH_RESPONSE_SCHEMA_REVISION
        );
        assert_ne!(contract["promptFingerprint"], prompt);
        assert_ne!(contract["responseSchemaFingerprint"], schema);
        assert_eq!(contract["samplingProfileFingerprint"], sampling);
        assert_eq!(
            contract["qualificationCorpusSchemaVersion"],
            QUALIFICATION_CORPUS_SCHEMA_VERSION
        );
        assert_eq!(
            contract["qualificationOutputSchemaVersion"],
            QUALIFICATION_OUTPUT_SCHEMA_VERSION
        );
        assert_eq!(
            contract["qualificationReportSchemaVersion"],
            QUALIFICATION_REPORT_SCHEMA_VERSION
        );
        assert_eq!(lock["scorer"]["revision"], QUALIFICATION_SCORER_REVISION);
        Ok(())
    }

    #[test]
    fn alm9_contract_fingerprints_are_independent_of_checkout_line_endings() -> Result<()> {
        let expected = matcher_contract_fingerprints(LOCAL_MODEL_CONTRACT_SOURCE)?;
        let crlf_source = LOCAL_MODEL_CONTRACT_SOURCE.replace('\n', "\r\n");
        assert_eq!(matcher_contract_fingerprints(&crlf_source)?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn alm9_qualification_worker_is_extracted_from_the_bound_runtime_archive() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let archive_path = directory.path().join("llama-server.raw");
        let runtime_bytes = b"qualification worker fixture";
        std::fs::write(&archive_path, runtime_bytes)?;
        let mut runtime = qualification_manifest().runtimes.remove(0);
        runtime.archive_format = AnimeRuntimeArchiveFormat::Raw;
        runtime.entrypoint = "bin/llama-server".to_string();
        runtime.packaged_dependencies.clear();
        runtime.size_bytes = runtime_bytes.len() as u64;
        runtime.installed_size_bytes = runtime_bytes.len() as u64;
        let destination = directory.path().join("runtime");

        let worker =
            extract_anime_runtime_for_qualification(&archive_path, &destination, &runtime).await?;

        assert_eq!(worker, destination.join("bin/llama-server"));
        assert_eq!(std::fs::read(worker)?, runtime_bytes);
        Ok(())
    }

    #[test]
    fn alm9_canonical_json_sorts_every_object_level() -> Result<()> {
        let value = serde_json::from_str::<JsonValue>(r#"{"z":{"b":2,"a":1},"a":0}"#)?;
        assert_eq!(
            String::from_utf8(canonical_json_bytes(&value)?)?,
            r#"{"a":0,"z":{"a":1,"b":2}}"#
        );
        Ok(())
    }

    #[test]
    fn alm9_canonical_float_spelling_matches_python_scorer() -> Result<()> {
        let value = serde_json::json!({
            "positive": 1e20_f64,
            "small": 1e-7_f64,
            "ordinary": 0.5_f64,
            "negativeZero": -0.0_f64,
        });
        assert_eq!(
            String::from_utf8(canonical_json_bytes(&value)?)?,
            r#"{"negativeZero":-0.0,"ordinary":0.5,"positive":1e20,"small":1e-7}"#
        );
        Ok(())
    }

    #[test]
    fn alm9_strict_json_rejects_duplicate_keys_at_every_level() {
        assert!(parse_strict_json(br#"{"case":{"id":1,"id":2}}"#, "fixture").is_err());
    }

    #[test]
    fn alm9_gpu_preflight_uuid_extraction_is_exact_and_rejects_duplicates() -> Result<()> {
        let evidence = serde_json::json!({
            "host": {
                "gpus": [{"uuid": "GPU-12345678-1234-1234-1234-123456789abc"}]
            },
            "container": {
                "gpus": [{"uuid": "GPU-12345678-1234-1234-1234-123456789abc"}]
            }
        });
        let host = gpu_uuids_from_preflight(&evidence, "/host/gpus", "host")?;
        let container = gpu_uuids_from_preflight(&evidence, "/container/gpus", "container")?;
        assert_eq!(host, container);
        assert!(valid_nvidia_gpu_uuid(&host[0]));
        assert!(!valid_nvidia_gpu_uuid("GPU-short"));

        let duplicate = serde_json::json!({
            "host": {
                "gpus": [
                    {"uuid": "GPU-12345678-1234-1234-1234-123456789abc"},
                    {"uuid": "GPU-12345678-1234-1234-1234-123456789abc"}
                ]
            }
        });
        assert!(gpu_uuids_from_preflight(&duplicate, "/host/gpus", "host").is_err());
        Ok(())
    }

    #[test]
    fn alm9_sealed_probe_profile_binds_runtime_policy_and_measurements() -> Result<()> {
        let manifest = qualification_manifest();
        let runtime = &manifest.runtimes[0];
        let profile = sealed_cpu_profile(&manifest)?;
        validate_probe_profile(&manifest, runtime, &profile)?;

        let mut tampered = profile.clone();
        tampered.warm_latency_ms += 1;
        assert!(tampered.validate().is_err());

        let mut missing_measurement = profile;
        missing_measurement.warm_latency_ms = 0;
        let missing_measurement = missing_measurement.seal()?;
        assert!(validate_probe_profile(&manifest, runtime, &missing_measurement).is_err());
        Ok(())
    }

    #[test]
    fn alm9_worker_profile_is_derived_from_sealed_probe_and_resolved_paths() -> Result<()> {
        let manifest = qualification_manifest();
        let profile = sealed_cpu_profile(&manifest)?;
        let qualification_root = std::env::current_dir()?.join("qualification-fixture");
        let worker_path = qualification_root.join("runtime/bin/llama-server");
        let model_path = qualification_root.join("model/model.gguf");
        let local =
            local_profile_from_qualification(&model_path, &worker_path, &manifest, &profile)?;
        assert_eq!(local.profile_fingerprint, profile.profile_fingerprint);
        assert_eq!(local.backend, "cpu");
        assert_eq!(local.threads, 2);
        assert_eq!(local.worker_path, worker_path);
        assert_eq!(local.model_path, model_path);
        local.validate_contract()
    }

    #[test]
    fn alm9_file_sources_preserve_production_selectable_file_indexes() -> Result<()> {
        let request_candidate = crate::anime_matching::AnimeMatchCandidate {
            candidate_key: "candidate-0".to_string(),
            title: "Pack".to_string(),
            files: vec![AnimeMatchFile {
                file_key: "candidate-0-file-0".to_string(),
                path: "Pack/Episode 01.mkv".to_string(),
            }],
            parse_facts: AnimeMatchParseFacts::default(),
        };
        let candidate = AcquisitionCandidate {
            id: None,
            title: "Pack".to_string(),
            source: "fixture".to_string(),
            source_kind: "torrent".to_string(),
            info_hash: None,
            file_index: None,
            quality: None,
            size_bytes: None,
            seeders: None,
            language: None,
            cached_debrid: None,
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: vec![
                AcquisitionCandidateFile {
                    file_id: Some("sample".to_string()),
                    file_index: Some(0),
                    path: "Pack/sample.mkv".to_string(),
                    size_bytes: Some(1),
                    selectable: Some(false),
                },
                AcquisitionCandidateFile {
                    file_id: Some("episode-1".to_string()),
                    file_index: Some(1),
                    path: "Pack/Episode 01.mkv".to_string(),
                    size_bytes: Some(1_000),
                    selectable: Some(true),
                },
            ],
            supported_routes: Vec::new(),
            default_route: None,
            raw: None,
        };
        let bindings = selectable_request_file_bindings(&request_candidate, &candidate)?;
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].1, 1);
        let release_files = qualification_release_files(&request_candidate, &candidate)?;
        assert_eq!(
            release_files[0].file_key,
            "__qualification_unselectable_file_0"
        );
        assert_eq!(release_files[1].file_key, "candidate-0-file-0");
        Ok(())
    }

    #[test]
    fn alm9_language_policy_maps_required_dub_to_a_real_automatic_gate() {
        let preference = language_preference(&AnimeMatchAudioPreference {
            mode: AnimeMatchAudioPreferenceMode::RequireDub,
            languages: vec!["en".to_string()],
            subtitle_languages: Vec::new(),
            accepted_profiles: Vec::new(),
        });
        let dubbed = assess_language_preference(
            &preference,
            MediaType::Anime,
            &acquisition_model_audio_profile_evidence(AnimeMatchAudioProfile::Dubbed),
        );
        let subbed = assess_language_preference(
            &preference,
            MediaType::Anime,
            &acquisition_model_audio_profile_evidence(AnimeMatchAudioProfile::Subbed),
        );
        assert!(required_language_satisfied(&preference, &dubbed));
        assert!(!required_language_satisfied(&preference, &subbed));
        assert!(!required_language_is_hard_mismatch(&preference, &dubbed));
        assert!(required_language_is_hard_mismatch(&preference, &subbed));

        let unknown = assess_language_preference(
            &preference,
            MediaType::Anime,
            &CandidateLanguageEvidence::default(),
        );
        assert!(!required_language_satisfied(&preference, &unknown));
        assert!(!required_language_is_hard_mismatch(&preference, &unknown));
    }

    #[test]
    fn alm9_stable_prepared_request_rejects_unknown_candidate_keys() {
        let base = request();
        let mut changed = base.clone();
        changed.candidates[0].candidate_key = "invented".to_string();
        let candidates = (0..4)
            .map(|index| AcquisitionCandidate {
                id: None,
                title: format!("Tokyo Ghoul Root A - {:02}", index + 1),
                source: "fixture".to_string(),
                source_kind: "torrent".to_string(),
                info_hash: None,
                file_index: None,
                quality: None,
                size_bytes: None,
                seeders: None,
                language: None,
                cached_debrid: None,
                rank: None,
                score: None,
                score_badges: Vec::new(),
                files: Vec::new(),
                supported_routes: Vec::new(),
                default_route: None,
                raw: None,
            })
            .collect::<Vec<_>>();
        assert!(prepare_stable_request(&base, changed, &candidates).is_err());
    }

    #[tokio::test]
    async fn alm9_every_injected_model_failure_preserves_the_exact_baseline() -> Result<()> {
        let request = request();
        let candidates = request
            .candidates
            .iter()
            .map(|candidate| AcquisitionCandidate {
                id: None,
                title: candidate.title.clone(),
                source: "fixture".to_string(),
                source_kind: "torrent".to_string(),
                info_hash: None,
                file_index: None,
                quality: None,
                size_bytes: None,
                seeders: None,
                language: None,
                cached_debrid: None,
                rank: None,
                score: None,
                score_badges: Vec::new(),
                files: Vec::new(),
                supported_routes: Vec::new(),
                default_route: None,
                raw: None,
            })
            .collect();
        let input = QualificationCaseInput {
            route_context: QualificationRouteContext {
                file_selection_supported_by_candidate_key: request
                    .candidates
                    .iter()
                    .map(|candidate| (candidate.candidate_key.clone(), true))
                    .collect(),
            },
            request,
            scoring_context: AnimeCandidateScoringContext::default(),
            acquisition_candidates: candidates,
        };
        let baseline = empty_final_plan(QualificationDisposition::Unresolved);
        let plans = run_failure_injections(&input, &baseline).await?;
        assert_eq!(plans.len(), FAILURE_MODES.len());
        assert!(plans.values().all(|plan| plan == &baseline));
        Ok(())
    }
}
