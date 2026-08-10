//! Native compiler for declarative ALM-9 corpus blueprints.
//!
//! The collector owns source discovery and curation. This module owns the
//! production boundary: raw release names and torrent file lists become the
//! exact acquisition inputs, parser facts, request-local keys, and expected
//! reference bindings consumed by the qualification harness.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use chrono::{DateTime, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::{
    AllowedReferences, QualificationAudioEligibility, QualificationCandidatePlan,
    QualificationCase, QualificationCaseInput, QualificationCoverageEntry,
    QualificationCoverageStatus, QualificationDisposition, QualificationFinalPlan,
    QualificationResolutionState, QualificationRouteContext, canonical_json_fingerprint,
    deterministic_baseline, deterministic_union_state, final_plan_for_resolution, read_json,
    selectable_anime_media_file, unique_strings, validate_case_input, write_new_canonical_json,
};
use crate::{
    acquisition::{
        anime_matching::{acquisition_candidate_parse_facts, acquisition_match_context},
        release_resolution::anime::AnimeCandidateScoringContext,
    },
    anime_matching::{
        AnimeMatchBatchInput, AnimeMatchCandidateInput, AnimeMatchFileInput, AnimeMatchTarget,
        AnimeMatchingService, DeterministicMatchState,
    },
    http::handlers::acquisition_sources::AcquisitionCandidate,
};

const BLUEPRINT_SCHEMA_VERSION: u32 = 1;
const SOURCE_MANIFEST_SCHEMA_VERSION: u32 = 1;
const EXPECTED_TOTAL_CASES: usize = 520;
const EXPECTED_SMOKE_CASES: usize = 40;
const EXPECTED_DEVELOPMENT_CASES: usize = 160;
const EXPECTED_FROZEN_CASES: usize = 320;
const EXPECTED_REAL_CASES: usize = 260;
const EXPECTED_SYNTHETIC_CASES: usize = 260;
const EXPECTED_FROZEN_ORIGIN_CASES: usize = 160;
const EXPECTED_FROZEN_SLICE_CASES: usize = 40;
const EXPECTED_STABILITY_PER_SLICE: usize = 6;
const EXPECTED_COUNTERFACTUAL_PAIRS: usize = 80;
const MIN_CANDIDATES: usize = 4;
const MAX_CANDIDATES: usize = 12;
const SHA256_PREFIX: &str = "sha256:";
const FROZEN_SLICES: [&str; 8] = [
    "season_aliases",
    "numbering",
    "cours_parts_arcs",
    "cross_script",
    "special_boundaries",
    "packs",
    "audio",
    "no_match_near_miss",
];
const NOISE_AXES: [&str; 11] = [
    "release_group",
    "resolution",
    "codec",
    "source",
    "audio",
    "language",
    "checksum",
    "batch",
    "edition",
    "punctuation",
    "romanization",
];

/// A create-once compilation. `output_root` must not already exist.
#[derive(Debug, Clone)]
pub struct AnimeCorpusCompileConfig {
    pub blueprint_path: PathBuf,
    pub output_root: PathBuf,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCorpusCompileSummary {
    pub status: String,
    pub corpus_id: String,
    pub case_count: usize,
    pub case_root: PathBuf,
    pub source_manifest_draft_path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CorpusBlueprint {
    schema_version: u32,
    assembly: BlueprintIdentity,
    corpus: BlueprintIdentity,
    curator: BlueprintIdentity,
    timestamps: BlueprintTimestamps,
    frozen_set_withheld_until_rules_frozen: bool,
    representative_subset: BlueprintRepresentativeSubset,
    cases: Vec<BlueprintCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlueprintIdentity {
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlueprintTimestamps {
    created_at: String,
    rules_frozen_at: String,
    frozen_labels_first_exposed_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlueprintRepresentativeSubset {
    id: String,
    case_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlueprintCase {
    case_id: String,
    set: String,
    slice: Option<String>,
    origin: String,
    realistic_noise: bool,
    counterfactual_pair_id: Option<String>,
    counterfactual_mutation: Option<BlueprintCounterfactualMutation>,
    stability_subset: bool,
    request_id: String,
    target: AnimeMatchTarget,
    scoring_context: AnimeCandidateScoringContext,
    acquisition_candidates: Vec<AcquisitionCandidate>,
    route_file_selection_supported_by_candidate_index: Vec<bool>,
    expected_final_plan: BlueprintExpectedFinalPlan,
    provenance: BlueprintProvenance,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlueprintCounterfactualMutation {
    field: CounterfactualField,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterfactualField {
    Season,
    AbsoluteNumber,
    AliasBinding,
    AudioRequirement,
}

impl CounterfactualField {
    fn as_str(self) -> &'static str {
        match self {
            Self::Season => "season",
            Self::AbsoluteNumber => "absolute_number",
            Self::AliasBinding => "alias_binding",
            Self::AudioRequirement => "audio_requirement",
        }
    }

    fn allows_pointer(self, pointer: &str) -> bool {
        let Ok(tokens) = json_pointer_tokens(pointer) else {
            return false;
        };
        let indexed = |value: &str| value.parse::<usize>().is_ok();
        match self {
            Self::Season | Self::AbsoluteNumber => {
                (tokens.len() == 3
                    && tokens[0] == "request"
                    && tokens[1] == "target"
                    && tokens[2] == "seasonNumber")
                    || (tokens.len() == 4
                        && tokens[0] == "request"
                        && tokens[1] == "target"
                        && matches!(
                            tokens[2].as_str(),
                            "wantedTargetKeys" | "episodeNumbers" | "absoluteEpisodeNumbers"
                        )
                        && indexed(&tokens[3]))
            }
            Self::AliasBinding => {
                (tokens.len() == 3
                    && tokens[0] == "scoringContext"
                    && tokens[1] == "aliases"
                    && indexed(&tokens[2]))
                    || (tokens.len() == 4
                        && tokens[0] == "scoringContext"
                        && tokens[1] == "scopedAliases"
                        && indexed(&tokens[2])
                        && tokens[3] == "display")
                    || (tokens.len() == 7
                        && tokens[0] == "request"
                        && tokens[1] == "context"
                        && tokens[2] == "seasons"
                        && indexed(&tokens[3])
                        && tokens[4] == "aliases"
                        && indexed(&tokens[5])
                        && tokens[6] == "value")
            }
            Self::AudioRequirement => {
                tokens.len() >= 4
                    && tokens[0] == "request"
                    && tokens[1] == "target"
                    && tokens[2] == "audioPreference"
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlueprintExpectedFinalPlan {
    disposition: QualificationDisposition,
    candidate_plans: Vec<BlueprintExpectedCandidatePlan>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlueprintExpectedCandidatePlan {
    candidate_index: usize,
    audio_eligibility: QualificationAudioEligibility,
    coverage: Vec<BlueprintCoverageEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlueprintCoverageEntry {
    target_index: usize,
    file_index: Option<usize>,
    status: QualificationCoverageStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlueprintProvenance {
    origin: String,
    source_kind: String,
    source_record_id: String,
    record_fingerprint: String,
    noise_profile: Vec<String>,
    captured_at: Option<String>,
    derived_from_case_id: Option<String>,
    transformation_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceManifestDraft {
    schema_version: u32,
    status: String,
    assembly_id: String,
    corpus_id: String,
    curator_id: String,
    created_at: String,
    rules_frozen_at: String,
    frozen_labels_first_exposed_at: String,
    frozen_set_withheld_until_rules_frozen: bool,
    representative_subset: SourceManifestRepresentativeSubset,
    cases: Vec<SourceManifestCase>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceManifestRepresentativeSubset {
    id: String,
    case_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceManifestCase {
    case_id: String,
    path: String,
    provenance: BlueprintProvenance,
}

struct CompiledCase {
    case: QualificationCase,
    provenance: BlueprintProvenance,
    mutation_field: Option<CounterfactualField>,
}

/// Compile a complete curator blueprint through the production acquisition
/// adapter and publish individual create-once case files plus the freeze-tool
/// source-manifest draft.
pub fn compile_anime_corpus_blueprint(
    config: AnimeCorpusCompileConfig,
) -> Result<AnimeCorpusCompileSummary> {
    let blueprint: CorpusBlueprint = read_json(&config.blueprint_path, "anime corpus blueprint")?;
    let compiled = compile_blueprint(blueprint)?;
    publish_compilation(&config.output_root, compiled)
}

fn compile_blueprint(blueprint: CorpusBlueprint) -> Result<CompiledCorpus> {
    validate_blueprint_header(&blueprint)?;
    ensure!(
        blueprint.cases.len() == EXPECTED_TOTAL_CASES,
        "anime corpus blueprint must contain exactly {EXPECTED_TOTAL_CASES} cases"
    );

    let mut case_ids = BTreeSet::new();
    let mut source_record_ids = BTreeSet::new();
    let mut compiled = Vec::with_capacity(blueprint.cases.len());
    for source in blueprint.cases {
        ensure!(
            case_ids.insert(source.case_id.clone()),
            "anime corpus blueprint repeats case ID {}",
            source.case_id
        );
        ensure!(
            source_record_ids.insert(source.provenance.source_record_id.clone()),
            "anime corpus blueprint repeats source record ID {}",
            source.provenance.source_record_id
        );
        compiled.push(compile_case(source)?);
    }
    compiled.sort_by_key(|item| set_order(&item.case.set));
    validate_composition(&compiled)?;
    apply_counterfactual_mutations(&mut compiled)?;
    validate_provenance_graph(&compiled)?;
    validate_representative_subset(&blueprint.representative_subset, &compiled)?;

    Ok(CompiledCorpus {
        assembly_id: blueprint.assembly.id,
        corpus_id: blueprint.corpus.id,
        curator_id: blueprint.curator.id,
        timestamps: blueprint.timestamps,
        frozen_set_withheld_until_rules_frozen: blueprint.frozen_set_withheld_until_rules_frozen,
        representative_subset: blueprint.representative_subset,
        cases: compiled,
    })
}

struct CompiledCorpus {
    assembly_id: String,
    corpus_id: String,
    curator_id: String,
    timestamps: BlueprintTimestamps,
    frozen_set_withheld_until_rules_frozen: bool,
    representative_subset: BlueprintRepresentativeSubset,
    cases: Vec<CompiledCase>,
}

fn validate_blueprint_header(blueprint: &CorpusBlueprint) -> Result<()> {
    ensure!(
        blueprint.schema_version == BLUEPRINT_SCHEMA_VERSION,
        "anime corpus blueprint schemaVersion must be {BLUEPRINT_SCHEMA_VERSION}"
    );
    validate_component(&blueprint.assembly.id, "assembly.id")?;
    validate_component(&blueprint.corpus.id, "corpus.id")?;
    validate_identity(&blueprint.curator.id, "curator.id")?;
    validate_component(
        &blueprint.representative_subset.id,
        "representativeSubset.id",
    )?;
    let created = validate_timestamp(&blueprint.timestamps.created_at, "timestamps.createdAt")?;
    let rules = validate_timestamp(
        &blueprint.timestamps.rules_frozen_at,
        "timestamps.rulesFrozenAt",
    )?;
    let exposed = validate_timestamp(
        &blueprint.timestamps.frozen_labels_first_exposed_at,
        "timestamps.frozenLabelsFirstExposedAt",
    )?;
    ensure!(
        rules <= exposed && exposed <= created,
        "rules must freeze before labels are exposed and before assembly creation"
    );
    ensure!(
        blueprint.frozen_set_withheld_until_rules_frozen,
        "frozenSetWithheldUntilRulesFrozen must be true"
    );
    for case in &blueprint.cases {
        if let Some(captured_at) = case.provenance.captured_at.as_deref() {
            ensure!(
                validate_timestamp(captured_at, "provenance.capturedAt")? <= created,
                "case {} provenance capture is later than assembly creation",
                case.case_id
            );
        }
    }
    Ok(())
}

fn compile_case(source: BlueprintCase) -> Result<CompiledCase> {
    validate_component(&source.case_id, "caseId")?;
    ensure!(
        matches!(source.set.as_str(), "smoke" | "development" | "frozen"),
        "case {} has invalid set",
        source.case_id
    );
    if source.set == "frozen" {
        ensure!(
            source
                .slice
                .as_deref()
                .is_some_and(|slice| FROZEN_SLICES.contains(&slice)),
            "frozen case {} has an invalid slice",
            source.case_id
        );
    } else {
        ensure!(
            source.slice.is_none(),
            "non-frozen case {} cannot declare a slice",
            source.case_id
        );
        ensure!(
            !source.stability_subset,
            "non-frozen case {} cannot be in the stability subset",
            source.case_id
        );
    }
    ensure!(
        matches!(source.origin.as_str(), "real" | "synthetic"),
        "case {} has invalid origin",
        source.case_id
    );
    ensure!(
        (MIN_CANDIDATES..=MAX_CANDIDATES).contains(&source.acquisition_candidates.len()),
        "case {} must contain {MIN_CANDIDATES}-{MAX_CANDIDATES} acquisition candidates",
        source.case_id
    );
    ensure!(
        source
            .route_file_selection_supported_by_candidate_index
            .len()
            == source.acquisition_candidates.len(),
        "case {} route selection array differs from candidate count",
        source.case_id
    );
    ensure!(
        source.counterfactual_pair_id.is_some() == source.counterfactual_mutation.is_some(),
        "case {} counterfactual pair and mutation field must be declared together",
        source.case_id
    );
    if source.counterfactual_pair_id.is_some() {
        ensure!(
            source.set == "frozen",
            "counterfactual case {} must be frozen",
            source.case_id
        );
    }
    validate_provenance(&source)?;

    let context = acquisition_match_context(
        &source.target.canonical_title,
        &source.scoring_context,
        &source.target,
    )
    .with_context(|| format!("building production graph context for {}", source.case_id))?;
    let prepared = AnimeMatchingService::prepare_request(AnimeMatchBatchInput {
        request_id: source.request_id.clone(),
        target: source.target.clone(),
        context,
        candidates: source
            .acquisition_candidates
            .iter()
            .enumerate()
            .map(|(candidate_index, candidate)| AnimeMatchCandidateInput {
                source: candidate_index,
                title: candidate.title.clone(),
                files: candidate
                    .files
                    .iter()
                    .enumerate()
                    .filter(|(_, file)| selectable_anime_media_file(file))
                    .map(|(file_index, file)| AnimeMatchFileInput {
                        source: (candidate_index, file_index),
                        path: file.path.clone(),
                    })
                    .collect(),
                parse_facts: acquisition_candidate_parse_facts(candidate),
            })
            .collect(),
    })
    .with_context(|| {
        format!(
            "preparing production matcher request for {}",
            source.case_id
        )
    })?;
    let request = prepared.request().clone();
    let route_context = QualificationRouteContext {
        file_selection_supported_by_candidate_key: request
            .candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| {
                (
                    candidate.candidate_key.clone(),
                    source.route_file_selection_supported_by_candidate_index[index],
                )
            })
            .collect(),
    };
    let expected_final_plan = bind_expected_plan(
        &source.case_id,
        &source.expected_final_plan,
        &request,
        prepared.source_map(),
        source.acquisition_candidates.len(),
    )?;
    let input = QualificationCaseInput {
        request,
        scoring_context: source.scoring_context,
        acquisition_candidates: source.acquisition_candidates,
        route_context,
    };
    let input_value = serde_json::to_value(&input).context("encoding compiled case input")?;
    let input_fingerprint = canonical_json_fingerprint(&input_value)?;
    let allowed_references = AllowedReferences {
        candidate_keys: input
            .request
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_key.clone())
            .collect(),
        target_keys: input.request.target.wanted_target_keys.clone(),
        file_keys: input
            .request
            .candidates
            .iter()
            .flat_map(|candidate| candidate.files.iter().map(|file| file.file_key.clone()))
            .collect(),
    };
    let deterministic = deterministic_baseline(&input)
        .with_context(|| format!("running deterministic baseline for {}", source.case_id))?;
    let deterministic_final_plan = final_plan_for_resolution(&input.request, &deterministic)?;
    let deterministic_easy = deterministic_union_state(&input.request, &deterministic)
        == DeterministicMatchState::Definitive
        && deterministic_final_plan == expected_final_plan;
    let case = QualificationCase {
        case_id: source.case_id,
        set: source.set,
        slice: source.slice,
        origin: source.origin,
        realistic_noise: source.realistic_noise,
        counterfactual_pair_id: source.counterfactual_pair_id,
        counterfactual_mutation: None,
        stability_subset: source.stability_subset,
        deterministic_easy,
        input_fingerprint,
        input: input_value,
        allowed_references,
        expected_final_plan,
    };
    validate_case_input(&case, &input)?;
    Ok(CompiledCase {
        case,
        provenance: source.provenance,
        mutation_field: source
            .counterfactual_mutation
            .map(|mutation| mutation.field),
    })
}

fn bind_expected_plan(
    case_id: &str,
    blueprint: &BlueprintExpectedFinalPlan,
    request: &crate::anime_matching::AnimeMatchRequest,
    source_map: &crate::anime_matching::AnimeMatchSourceMap<usize, (usize, usize)>,
    candidate_count: usize,
) -> Result<QualificationFinalPlan> {
    let mut candidate_plans = vec![None; candidate_count];
    for plan in &blueprint.candidate_plans {
        let request_candidate = request
            .candidates
            .get(plan.candidate_index)
            .ok_or_else(|| {
                anyhow!(
                    "case {case_id} expected plan candidateIndex {} is out of bounds",
                    plan.candidate_index
                )
            })?;
        ensure!(
            source_map.candidate_source(&request_candidate.candidate_key)
                == Some(&plan.candidate_index),
            "case {case_id} candidate source binding changed during request preparation"
        );
        ensure!(
            candidate_plans[plan.candidate_index].is_none(),
            "case {case_id} expected plan repeats candidateIndex {}",
            plan.candidate_index
        );
        ensure!(
            !plan.coverage.is_empty(),
            "case {case_id} expected candidate plan has empty coverage"
        );
        let mut coverage = Vec::with_capacity(plan.coverage.len());
        for entry in &plan.coverage {
            let target_key = request
                .target
                .wanted_target_keys
                .get(entry.target_index)
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "case {case_id} expected plan targetIndex {} is out of bounds",
                        entry.target_index
                    )
                })?;
            let file_key = match entry.file_index {
                Some(source_file_index) => Some(
                    request_candidate
                        .files
                        .iter()
                        .find(|file| {
                            source_map.file_source(&request_candidate.candidate_key, &file.file_key)
                                == Some(&(plan.candidate_index, source_file_index))
                        })
                        .map(|file| file.file_key.clone())
                        .ok_or_else(|| {
                            anyhow!(
                                "case {case_id} expected plan fileIndex {source_file_index} is not a selectable media file for candidateIndex {}",
                                plan.candidate_index
                            )
                        })?,
                ),
                None => None,
            };
            ensure!(
                entry.status == QualificationCoverageStatus::Covered || file_key.is_none(),
                "case {case_id} non-covered coverage cannot select a file"
            );
            coverage.push(QualificationCoverageEntry {
                target_key,
                file_key,
                status: entry.status.clone(),
            });
        }
        let target_keys = unique_strings(
            coverage
                .iter()
                .filter(|entry| entry.status == QualificationCoverageStatus::Covered)
                .map(|entry| entry.target_key.clone()),
        );
        let file_keys = unique_strings(
            coverage
                .iter()
                .filter(|entry| entry.status == QualificationCoverageStatus::Covered)
                .filter_map(|entry| entry.file_key.clone()),
        );
        ensure!(
            !target_keys.is_empty(),
            "case {case_id} expected candidate plan covers no targets"
        );
        candidate_plans[plan.candidate_index] = Some(QualificationCandidatePlan {
            candidate_key: request_candidate.candidate_key.clone(),
            target_keys,
            file_keys,
            audio_eligibility: plan.audio_eligibility.clone(),
            coverage,
        });
    }
    let resolution = QualificationResolutionState {
        candidate_plans,
        saw_partial_or_ambiguous: blueprint.disposition == QualificationDisposition::Unresolved,
    };
    let bound = final_plan_for_resolution(request, &resolution)?;
    ensure!(
        bound.disposition == blueprint.disposition,
        "case {case_id} expected disposition is inconsistent with its indexed coverage"
    );
    Ok(bound)
}

fn validate_provenance(source: &BlueprintCase) -> Result<()> {
    let provenance = &source.provenance;
    ensure!(
        provenance.origin == source.origin,
        "case {} provenance origin differs from case origin",
        source.case_id
    );
    validate_identity(&provenance.source_record_id, "provenance.sourceRecordId")?;
    validate_sha256(
        &provenance.record_fingerprint,
        "provenance.recordFingerprint",
    )?;
    let noise = provenance.noise_profile.iter().collect::<BTreeSet<_>>();
    ensure!(
        noise.len() == provenance.noise_profile.len()
            && provenance
                .noise_profile
                .iter()
                .all(|axis| NOISE_AXES.contains(&axis.as_str())),
        "case {} provenance noiseProfile is invalid",
        source.case_id
    );
    ensure!(
        provenance.noise_profile.is_empty() != source.realistic_noise,
        "case {} provenance noiseProfile differs from realisticNoise",
        source.case_id
    );
    match source.origin.as_str() {
        "real" => {
            ensure!(
                matches!(
                    provenance.source_kind.as_str(),
                    "historical_failure" | "public_release_name"
                ),
                "case {} has invalid real provenance sourceKind",
                source.case_id
            );
            let captured = provenance.captured_at.as_deref().ok_or_else(|| {
                anyhow!("real case {} provenance lacks capturedAt", source.case_id)
            })?;
            validate_timestamp(captured, "provenance.capturedAt")?;
            ensure!(
                provenance.derived_from_case_id.is_none() && provenance.transformation_id.is_none(),
                "real case {} cannot declare synthetic provenance",
                source.case_id
            );
        }
        "synthetic" => {
            ensure!(
                matches!(
                    provenance.source_kind.as_str(),
                    "canonical_graph_derivation"
                        | "title_substitution"
                        | "counterfactual_derivation"
                ),
                "case {} has invalid synthetic provenance sourceKind",
                source.case_id
            );
            ensure!(
                provenance.captured_at.is_none()
                    && provenance.derived_from_case_id.is_some()
                    && provenance.transformation_id.is_some(),
                "synthetic case {} provenance is incomplete",
                source.case_id
            );
            validate_component(
                provenance
                    .derived_from_case_id
                    .as_deref()
                    .unwrap_or_default(),
                "provenance.derivedFromCaseId",
            )?;
            validate_component(
                provenance.transformation_id.as_deref().unwrap_or_default(),
                "provenance.transformationId",
            )?;
        }
        _ => unreachable!("case origin validated before provenance"),
    }
    Ok(())
}

fn validate_composition(cases: &[CompiledCase]) -> Result<()> {
    let set_counts = count_values(cases.iter().map(|item| item.case.set.as_str()));
    ensure!(
        set_counts.get("smoke") == Some(&EXPECTED_SMOKE_CASES)
            && set_counts.get("development") == Some(&EXPECTED_DEVELOPMENT_CASES)
            && set_counts.get("frozen") == Some(&EXPECTED_FROZEN_CASES),
        "corpus set composition must be 40 smoke, 160 development, and 320 frozen"
    );
    let origin_counts = count_values(cases.iter().map(|item| item.case.origin.as_str()));
    ensure!(
        origin_counts.get("real") == Some(&EXPECTED_REAL_CASES)
            && origin_counts.get("synthetic") == Some(&EXPECTED_SYNTHETIC_CASES),
        "complete corpus must contain exactly 260 real and 260 synthetic cases"
    );
    let frozen = cases
        .iter()
        .filter(|item| item.case.set == "frozen")
        .collect::<Vec<_>>();
    let frozen_origins = count_values(frozen.iter().map(|item| item.case.origin.as_str()));
    ensure!(
        frozen_origins.get("real") == Some(&EXPECTED_FROZEN_ORIGIN_CASES)
            && frozen_origins.get("synthetic") == Some(&EXPECTED_FROZEN_ORIGIN_CASES),
        "frozen corpus must contain exactly 160 real and 160 synthetic cases"
    );
    let slices = count_values(
        frozen
            .iter()
            .map(|item| item.case.slice.as_deref().unwrap_or_default()),
    );
    ensure!(
        FROZEN_SLICES
            .iter()
            .all(|slice| slices.get(*slice) == Some(&EXPECTED_FROZEN_SLICE_CASES)),
        "frozen corpus must contain exactly 40 cases in every required slice"
    );
    let stability = count_values(frozen.iter().filter_map(|item| {
        item.case
            .stability_subset
            .then_some(item.case.slice.as_deref().unwrap_or_default())
    }));
    ensure!(
        FROZEN_SLICES
            .iter()
            .all(|slice| stability.get(*slice) == Some(&EXPECTED_STABILITY_PER_SLICE)),
        "frozen corpus must contain exactly six stability cases in every slice"
    );
    ensure!(
        cases
            .iter()
            .filter(|item| item.case.realistic_noise)
            .count()
            >= EXPECTED_TOTAL_CASES / 2,
        "at least half of all corpus cases must contain realistic release noise"
    );
    ensure!(
        frozen
            .iter()
            .filter(|item| item.case.realistic_noise)
            .count()
            >= EXPECTED_FROZEN_CASES / 2,
        "at least half of frozen cases must contain realistic release noise"
    );
    Ok(())
}

fn apply_counterfactual_mutations(cases: &mut [CompiledCase]) -> Result<()> {
    let mut pair_members: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, item) in cases.iter().enumerate() {
        if let Some(pair_id) = &item.case.counterfactual_pair_id {
            validate_component(pair_id, "counterfactualPairId")?;
            pair_members.entry(pair_id.clone()).or_default().push(index);
        }
    }
    ensure!(
        pair_members.len() == EXPECTED_COUNTERFACTUAL_PAIRS,
        "frozen corpus must contain exactly {EXPECTED_COUNTERFACTUAL_PAIRS} counterfactual pairs"
    );
    for (pair_id, indexes) in pair_members {
        ensure!(
            indexes.len() == 2,
            "counterfactual pair {pair_id} must contain exactly two cases"
        );
        let left_index = indexes[0];
        let right_index = indexes[1];
        let field = cases[left_index]
            .mutation_field
            .ok_or_else(|| anyhow!("counterfactual pair {pair_id} lacks a mutation field"))?;
        ensure!(
            cases[right_index].mutation_field == Some(field),
            "counterfactual pair {pair_id} mutation fields differ"
        );
        ensure!(
            cases[left_index].case.expected_final_plan
                != cases[right_index].case.expected_final_plan,
            "counterfactual pair {pair_id} does not change the expected plan"
        );
        let left_input = normalized_counterfactual_input(&cases[left_index].case.input)?;
        let right_input = normalized_counterfactual_input(&cases[right_index].case.input)?;
        let pointers = differing_leaf_pointers(&left_input, &right_input)?;
        ensure!(
            (1..=8).contains(&pointers.len()),
            "counterfactual pair {pair_id} must differ at 1-8 non-overlapping JSON leaves"
        );
        ensure!(
            pointers.iter().all(|pointer| field.allows_pointer(pointer)),
            "counterfactual pair {pair_id} changes data outside its declared {} field",
            field.as_str()
        );
        if matches!(
            field,
            CounterfactualField::Season | CounterfactualField::AbsoluteNumber
        ) {
            validate_selector_counterfactual(field, &left_input, &right_input).with_context(
                || format!("counterfactual pair {pair_id} is not an atomic selector swap"),
            )?;
        }
        let (left_value, left_invariant) = counterfactual_fingerprints(&left_input, &pointers)?;
        let (right_value, right_invariant) = counterfactual_fingerprints(&right_input, &pointers)?;
        ensure!(
            left_invariant == right_invariant && left_value != right_value,
            "counterfactual pair {pair_id} does not preserve its declared invariant"
        );
        cases[left_index].case.counterfactual_mutation = Some(serde_json::json!({
            "field": field.as_str(),
            "jsonPointers": pointers,
            "valueFingerprint": left_value,
            "invariantFingerprint": left_invariant,
        }));
        cases[right_index].case.counterfactual_mutation = Some(serde_json::json!({
            "field": field.as_str(),
            "jsonPointers": cases[left_index].case.counterfactual_mutation.as_ref().unwrap()["jsonPointers"].clone(),
            "valueFingerprint": right_value,
            "invariantFingerprint": right_invariant,
        }));
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct CounterfactualSelectorIdentity {
    wanted_target_key: String,
    season_number: i64,
    anilist_id: String,
    episode_number: i64,
    absolute_episode_number: i64,
}

fn singleton_positive_i64(value: &JsonValue, field: &str) -> Result<i64> {
    let values = value
        .as_array()
        .ok_or_else(|| anyhow!("selector {field} must be an array"))?;
    ensure!(
        values.len() == 1,
        "selector {field} must contain exactly one value"
    );
    let number = values[0]
        .as_i64()
        .ok_or_else(|| anyhow!("selector {field} must contain an integer"))?;
    ensure!(number > 0, "selector {field} must be positive");
    Ok(number)
}

fn counterfactual_selector_identity(input: &JsonValue) -> Result<CounterfactualSelectorIdentity> {
    let request = input
        .get("request")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("counterfactual input lacks request"))?;
    let selector = request
        .get("target")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("counterfactual input lacks request target"))?;
    let wanted = selector
        .get("wantedTargetKeys")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("selector wantedTargetKeys must be an array"))?;
    ensure!(
        wanted.len() == 1,
        "selector wantedTargetKeys must contain exactly one key"
    );
    let wanted_target_key = wanted[0]
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("selector wantedTargetKeys must contain a string"))?;
    let selector_season = selector
        .get("seasonNumber")
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| anyhow!("selector seasonNumber must be an integer"))?;
    let selector_episode = singleton_positive_i64(
        selector
            .get("episodeNumbers")
            .ok_or_else(|| anyhow!("selector lacks episodeNumbers"))?,
        "episodeNumbers",
    )?;
    let selector_absolute = singleton_positive_i64(
        selector
            .get("absoluteEpisodeNumbers")
            .ok_or_else(|| anyhow!("selector lacks absoluteEpisodeNumbers"))?,
        "absoluteEpisodeNumbers",
    )?;

    let seasons = request
        .get("context")
        .and_then(|value| value.get("seasons"))
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("counterfactual input lacks request context seasons"))?;
    let mut resolved = Vec::new();
    for season in seasons {
        let season_object = season
            .as_object()
            .ok_or_else(|| anyhow!("request context season must be an object"))?;
        let season_number = season_object
            .get("seasonNumber")
            .and_then(JsonValue::as_i64)
            .ok_or_else(|| anyhow!("request context season lacks seasonNumber"))?;
        let anilist_id = season_object
            .get("anilistId")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("request context season lacks anilistId"))?;
        let targets = season_object
            .get("targets")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| anyhow!("request context season lacks targets"))?;
        for target in targets {
            if target.get("targetKey").and_then(JsonValue::as_str)
                != Some(wanted_target_key.as_str())
            {
                continue;
            }
            let target_season = target
                .get("seasonNumber")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| anyhow!("wanted context target lacks seasonNumber"))?;
            let episode_number = target
                .get("episodeNumber")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| anyhow!("wanted context target lacks episodeNumber"))?;
            let absolute_episode_number = target
                .get("absoluteEpisodeNumber")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| anyhow!("wanted context target lacks absoluteEpisodeNumber"))?;
            ensure!(
                selector_season == target_season
                    && selector_episode == episode_number
                    && selector_absolute == absolute_episode_number,
                "request selector does not exactly identify its wanted context target"
            );
            resolved.push(CounterfactualSelectorIdentity {
                wanted_target_key: wanted_target_key.clone(),
                season_number,
                anilist_id: anilist_id.to_string(),
                episode_number,
                absolute_episode_number,
            });
        }
    }
    ensure!(
        resolved.len() == 1,
        "wanted selector key must resolve to exactly one context target"
    );
    Ok(resolved.remove(0))
}

fn validate_selector_counterfactual(
    field: CounterfactualField,
    left: &JsonValue,
    right: &JsonValue,
) -> Result<()> {
    let left = counterfactual_selector_identity(left)?;
    let right = counterfactual_selector_identity(right)?;
    ensure!(
        left.wanted_target_key != right.wanted_target_key,
        "selector counterfactual must change wantedTargetKeys"
    );
    match field {
        CounterfactualField::Season => ensure!(
            left.anilist_id != right.anilist_id && left.season_number != right.season_number,
            "season counterfactual must select different graph seasons"
        ),
        CounterfactualField::AbsoluteNumber => ensure!(
            left.anilist_id == right.anilist_id
                && left.season_number == right.season_number
                && left.absolute_episode_number != right.absolute_episode_number,
            "absolute-number counterfactual must select different absolute episodes in one graph season"
        ),
        _ => bail!("non-selector field passed to selector validation"),
    }
    Ok(())
}

fn normalized_counterfactual_input(input: &JsonValue) -> Result<JsonValue> {
    let mut normalized = input.clone();
    let request = normalized
        .get_mut("request")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| anyhow!("counterfactual input lacks request object"))?;
    ensure!(
        request.contains_key("requestId"),
        "counterfactual input lacks requestId"
    );
    request.insert(
        "requestId".to_string(),
        JsonValue::String("__counterfactual_request_id__".to_string()),
    );
    Ok(normalized)
}

fn differing_leaf_pointers(left: &JsonValue, right: &JsonValue) -> Result<Vec<String>> {
    let mut pointers = Vec::new();
    collect_differing_leaves(left, right, "", &mut pointers)?;
    Ok(pointers)
}

fn collect_differing_leaves(
    left: &JsonValue,
    right: &JsonValue,
    pointer: &str,
    output: &mut Vec<String>,
) -> Result<()> {
    if left == right {
        return Ok(());
    }
    match (left, right) {
        (JsonValue::Object(left), JsonValue::Object(right)) => {
            ensure!(
                left.keys().collect::<BTreeSet<_>>() == right.keys().collect::<BTreeSet<_>>(),
                "counterfactual pair changes JSON object shape at {pointer}"
            );
            let mut keys = left.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                collect_differing_leaves(
                    &left[key],
                    &right[key],
                    &format!("{pointer}/{}", escape_json_pointer_token(key)),
                    output,
                )?;
            }
        }
        (JsonValue::Array(left), JsonValue::Array(right)) => {
            ensure!(
                left.len() == right.len(),
                "counterfactual pair changes JSON array shape at {pointer}"
            );
            for index in 0..left.len() {
                collect_differing_leaves(
                    &left[index],
                    &right[index],
                    &format!("{pointer}/{index}"),
                    output,
                )?;
            }
        }
        _ => {
            ensure!(
                !pointer.is_empty(),
                "counterfactual pair changes the input root"
            );
            output.push(pointer.to_string());
        }
    }
    Ok(())
}

fn counterfactual_fingerprints(
    normalized_input: &JsonValue,
    pointers: &[String],
) -> Result<(String, String)> {
    let values = pointers
        .iter()
        .map(|pointer| json_pointer_value(normalized_input, pointer).cloned())
        .collect::<Result<Vec<_>>>()?;
    let mut invariant = normalized_input.clone();
    for pointer in pointers {
        let slot = json_pointer_value_mut(&mut invariant, pointer)?;
        *slot = serde_json::json!({"__elixirCounterfactualValue__": pointer});
    }
    Ok((
        canonical_json_fingerprint(&JsonValue::Array(values))?,
        canonical_json_fingerprint(&invariant)?,
    ))
}

fn json_pointer_value<'a>(root: &'a JsonValue, pointer: &str) -> Result<&'a JsonValue> {
    let mut current = root;
    for token in json_pointer_tokens(pointer)? {
        current = match current {
            JsonValue::Object(values) => values
                .get(&token)
                .ok_or_else(|| anyhow!("JSON pointer {pointer} does not resolve"))?,
            JsonValue::Array(values) => values
                .get(
                    token
                        .parse::<usize>()
                        .context("invalid JSON array pointer")?,
                )
                .ok_or_else(|| anyhow!("JSON pointer {pointer} does not resolve"))?,
            _ => bail!("JSON pointer {pointer} traverses a scalar"),
        };
    }
    Ok(current)
}

fn json_pointer_value_mut<'a>(root: &'a mut JsonValue, pointer: &str) -> Result<&'a mut JsonValue> {
    let tokens = json_pointer_tokens(pointer)?;
    let mut current = root;
    for token in tokens {
        current = match current {
            JsonValue::Object(values) => values
                .get_mut(&token)
                .ok_or_else(|| anyhow!("JSON pointer {pointer} does not resolve"))?,
            JsonValue::Array(values) => values
                .get_mut(
                    token
                        .parse::<usize>()
                        .context("invalid JSON array pointer")?,
                )
                .ok_or_else(|| anyhow!("JSON pointer {pointer} does not resolve"))?,
            _ => bail!("JSON pointer {pointer} traverses a scalar"),
        };
    }
    Ok(current)
}

fn json_pointer_tokens(pointer: &str) -> Result<Vec<String>> {
    ensure!(
        pointer.starts_with('/') && pointer != "/",
        "invalid non-root JSON pointer {pointer:?}"
    );
    pointer[1..]
        .split('/')
        .map(|raw| {
            let mut decoded = String::new();
            let mut chars = raw.chars();
            while let Some(character) = chars.next() {
                if character == '~' {
                    match chars.next() {
                        Some('0') => decoded.push('~'),
                        Some('1') => decoded.push('/'),
                        _ => bail!("invalid JSON pointer escape in {pointer:?}"),
                    }
                } else {
                    decoded.push(character);
                }
            }
            ensure!(!decoded.is_empty(), "JSON pointer contains an empty token");
            Ok(decoded)
        })
        .collect()
}

fn escape_json_pointer_token(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn validate_provenance_graph(cases: &[CompiledCase]) -> Result<()> {
    let indexes = cases
        .iter()
        .enumerate()
        .map(|(index, item)| (item.case.case_id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    for item in cases {
        if let Some(parent) = item.provenance.derived_from_case_id.as_deref() {
            ensure!(
                indexes.contains_key(parent) && parent != item.case.case_id,
                "case {} derives from an absent or identical source case",
                item.case.case_id
            );
            if item.provenance.source_kind == "counterfactual_derivation" {
                let parent_case = &cases[indexes[parent]].case;
                ensure!(
                    item.case.counterfactual_pair_id.is_some()
                        && item.case.counterfactual_pair_id == parent_case.counterfactual_pair_id,
                    "case {} counterfactual provenance must derive from its pair member",
                    item.case.case_id
                );
            }
        }
    }
    fn visit(
        index: usize,
        cases: &[CompiledCase],
        indexes: &BTreeMap<&str, usize>,
        visiting: &mut BTreeSet<usize>,
        visited: &mut BTreeSet<usize>,
    ) -> Result<()> {
        ensure!(
            visiting.insert(index),
            "synthetic provenance contains a cycle at {}",
            cases[index].case.case_id
        );
        if let Some(parent) = cases[index].provenance.derived_from_case_id.as_deref() {
            let parent_index = indexes[parent];
            if !visited.contains(&parent_index) {
                visit(parent_index, cases, indexes, visiting, visited)?;
            }
        }
        visiting.remove(&index);
        visited.insert(index);
        Ok(())
    }
    let mut visited = BTreeSet::new();
    for index in 0..cases.len() {
        if !visited.contains(&index) {
            visit(index, cases, &indexes, &mut BTreeSet::new(), &mut visited)?;
        }
    }
    Ok(())
}

fn validate_representative_subset(
    subset: &BlueprintRepresentativeSubset,
    cases: &[CompiledCase],
) -> Result<()> {
    ensure!(
        !subset.case_ids.is_empty() && subset.case_ids.len() < cases.len(),
        "representative subset must be a non-empty proper subset"
    );
    let selected = subset.case_ids.iter().collect::<BTreeSet<_>>();
    ensure!(
        selected.len() == subset.case_ids.len(),
        "representative subset contains duplicate case IDs"
    );
    let ordered = cases
        .iter()
        .filter(|item| selected.contains(&item.case.case_id))
        .map(|item| item.case.case_id.clone())
        .collect::<Vec<_>>();
    ensure!(
        ordered == subset.case_ids,
        "representative subset contains an unknown case or does not preserve corpus order"
    );
    let selected_cases = cases
        .iter()
        .filter(|item| selected.contains(&item.case.case_id))
        .collect::<Vec<_>>();
    ensure!(
        count_values(selected_cases.iter().map(|item| item.case.set.as_str())).len() == 3,
        "representative subset must include every corpus set"
    );
    let slices = selected_cases
        .iter()
        .filter_map(|item| item.case.slice.as_deref())
        .collect::<BTreeSet<_>>();
    ensure!(
        FROZEN_SLICES.iter().all(|slice| slices.contains(slice)),
        "representative subset must cover every frozen slice"
    );
    ensure!(
        selected_cases.iter().any(|item| item.case.stability_subset),
        "representative subset must include stability evidence"
    );
    let pair_counts = count_values(
        selected_cases
            .iter()
            .filter_map(|item| item.case.counterfactual_pair_id.as_deref()),
    );
    ensure!(
        pair_counts.values().any(|count| *count == 2),
        "representative subset must contain both members of a counterfactual pair"
    );
    Ok(())
}

fn publish_compilation(
    output_root: &Path,
    compiled: CompiledCorpus,
) -> Result<AnimeCorpusCompileSummary> {
    ensure!(
        !output_root.exists(),
        "anime corpus compilation output already exists at {}",
        output_root.display()
    );
    let parent = output_root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).context("creating corpus compiler parent directory")?;
    fs::create_dir(output_root).with_context(|| {
        format!(
            "creating create-once corpus output {}",
            output_root.display()
        )
    })?;
    let case_root = output_root.join("cases");
    fs::create_dir(&case_root).context("creating corpus case root")?;
    for set in ["smoke", "development", "frozen"] {
        fs::create_dir(case_root.join(set))
            .with_context(|| format!("creating {set} case directory"))?;
    }

    let mut manifest_cases = Vec::with_capacity(compiled.cases.len());
    for item in compiled.cases {
        let relative = format!("{}/{}.json", item.case.set, item.case.case_id);
        write_new_canonical_json(&case_root.join(&relative), &item.case)
            .with_context(|| format!("publishing corpus case {}", item.case.case_id))?;
        manifest_cases.push(SourceManifestCase {
            case_id: item.case.case_id,
            path: relative,
            provenance: item.provenance,
        });
    }
    let manifest = SourceManifestDraft {
        schema_version: SOURCE_MANIFEST_SCHEMA_VERSION,
        status: "curation-draft".to_string(),
        assembly_id: compiled.assembly_id,
        corpus_id: compiled.corpus_id.clone(),
        curator_id: compiled.curator_id,
        created_at: compiled.timestamps.created_at,
        rules_frozen_at: compiled.timestamps.rules_frozen_at,
        frozen_labels_first_exposed_at: compiled.timestamps.frozen_labels_first_exposed_at,
        frozen_set_withheld_until_rules_frozen: compiled.frozen_set_withheld_until_rules_frozen,
        representative_subset: SourceManifestRepresentativeSubset {
            id: compiled.representative_subset.id,
            case_ids: compiled.representative_subset.case_ids,
        },
        cases: manifest_cases,
    };
    let source_manifest_draft_path = output_root.join("source-manifest-draft.json");
    write_new_canonical_json(&source_manifest_draft_path, &manifest)
        .context("publishing source-manifest draft")?;
    Ok(AnimeCorpusCompileSummary {
        status: "compiled".to_string(),
        corpus_id: compiled.corpus_id,
        case_count: EXPECTED_TOTAL_CASES,
        case_root,
        source_manifest_draft_path,
    })
}

fn set_order(set: &str) -> u8 {
    match set {
        "smoke" => 0,
        "development" => 1,
        "frozen" => 2,
        _ => u8::MAX,
    }
}

fn count_values<'a>(values: impl IntoIterator<Item = &'a str>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value.to_string()).or_default() += 1;
    }
    counts
}

fn validate_component(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value.as_bytes()[0].is_ascii_alphanumeric()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
        "{label} is not a portable component"
    );
    Ok(())
}

fn validate_identity(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value.as_bytes()[0].is_ascii_alphanumeric()
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b':' | b'@' | b'+' | b'-')),
        "{label} is not a safe identity"
    );
    Ok(())
}

fn validate_timestamp(value: &str, label: &str) -> Result<DateTime<chrono::FixedOffset>> {
    ensure!(
        value.ends_with('Z'),
        "{label} must be an RFC3339 UTC timestamp ending in Z"
    );
    let parsed = DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("decoding {label} as RFC3339"))?;
    ensure!(
        parsed.nanosecond() == 0,
        "{label} must have whole-second precision"
    );
    Ok(parsed)
}

fn validate_sha256(value: &str, label: &str) -> Result<()> {
    let digest = value
        .strip_prefix(SHA256_PREFIX)
        .ok_or_else(|| anyhow!("{label} must start with {SHA256_PREFIX}"))?;
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "{label} must be a complete lowercase SHA-256"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::release_resolution::anime::{AnimeCandidateTarget, AnimeScopedAlias},
        anime_matching::{
            AnimeMatchAudioPreference, AnimeMatchAudioPreferenceMode, AnimeMatchMediaType,
            AnimeMatchScope,
        },
        http::handlers::acquisition_sources::AcquisitionCandidateFile,
    };

    fn candidate(title: &str, files: &[&str]) -> AcquisitionCandidate {
        AcquisitionCandidate {
            id: None,
            title: title.to_string(),
            source: "animetosho-nyaa".to_string(),
            source_kind: "torrent".to_string(),
            info_hash: None,
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: None,
            seeders: None,
            language: None,
            cached_debrid: None,
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: files
                .iter()
                .enumerate()
                .map(|(index, path)| AcquisitionCandidateFile {
                    file_id: Some(format!("file-{index}")),
                    file_index: Some(index as i64),
                    path: (*path).to_string(),
                    size_bytes: None,
                    selectable: Some(true),
                })
                .collect(),
            supported_routes: vec!["torrent".to_string()],
            default_route: Some("torrent".to_string()),
            raw: None,
        }
    }

    fn test_case() -> BlueprintCase {
        let targets = vec![AnimeCandidateTarget {
            target_key: "tg-s2-e1".to_string(),
            canonical_key: None,
            title: "Tokyo Ghoul √A".to_string(),
            season_number: Some(2),
            anilist_season_id: Some("20850".to_string()),
            episode_number: Some(1),
            absolute_episode_number: Some(13),
            tvdb_episode_id: None,
            anidb_episode_id: Some("166344".to_string()),
        }];
        BlueprintCase {
            case_id: "frozen-season-001".to_string(),
            set: "frozen".to_string(),
            slice: Some("season_aliases".to_string()),
            origin: "real".to_string(),
            realistic_noise: true,
            counterfactual_pair_id: None,
            counterfactual_mutation: None,
            stability_subset: false,
            request_id: "corpus-frozen-season-001".to_string(),
            target: AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Tokyo Ghoul".to_string(),
                scope: AnimeMatchScope::Episode,
                wanted_target_keys: vec!["tg-s2-e1".to_string()],
                season_number: Some(2),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![13],
                audio_preference: AnimeMatchAudioPreference {
                    mode: AnimeMatchAudioPreferenceMode::Any,
                    ..AnimeMatchAudioPreference::default()
                },
            },
            scoring_context: AnimeCandidateScoringContext {
                graph_fingerprint: Some("tokyo-ghoul-graph-v1".to_string()),
                aliases: vec!["Tokyo Ghoul".to_string(), "Tokyo Ghoul Root A".to_string()],
                scoped_aliases: vec![AnimeScopedAlias {
                    display: "Tokyo Ghoul √A".to_string(),
                    source: "anilist.romaji".to_string(),
                    language: Some("romaji".to_string()),
                    season_number: Some(2),
                    anilist_season_id: Some("20850".to_string()),
                }],
                targets,
            },
            acquisition_candidates: vec![
                candidate(
                    "[SubsPlease] Tokyo Ghoul Root A - 01 (1080p) [A1B2C3D4].mkv",
                    &[],
                ),
                candidate("[Group] Tokyo Ghoul - 13 [1080p].mkv", &[]),
                candidate("[Group] Tokyo Ghoul re - 01 [1080p].mkv", &[]),
                candidate(
                    "[Group] Tokyo Ghoul Root A Batch",
                    &["Tokyo Ghoul Root A - 01.mkv", "Tokyo Ghoul Root A - 02.mkv"],
                ),
            ],
            route_file_selection_supported_by_candidate_index: vec![false, false, false, true],
            expected_final_plan: BlueprintExpectedFinalPlan {
                disposition: QualificationDisposition::Matched,
                candidate_plans: vec![BlueprintExpectedCandidatePlan {
                    candidate_index: 0,
                    audio_eligibility: QualificationAudioEligibility::NotApplicable,
                    coverage: vec![BlueprintCoverageEntry {
                        target_index: 0,
                        file_index: None,
                        status: QualificationCoverageStatus::Covered,
                    }],
                }],
            },
            provenance: BlueprintProvenance {
                origin: "real".to_string(),
                source_kind: "public_release_name".to_string(),
                source_record_id: "animetosho:123".to_string(),
                record_fingerprint:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                noise_profile: vec!["release_group".to_string(), "resolution".to_string()],
                captured_at: Some("2026-08-09T12:00:00Z".to_string()),
                derived_from_case_id: None,
                transformation_id: None,
            },
        }
    }

    #[test]
    fn compiler_uses_production_parser_and_opaque_keys() -> Result<()> {
        let compiled = compile_case(test_case())?;
        let request = &compiled.case.input["request"];
        assert_eq!(request["candidates"][0]["candidateKey"], "candidate-0");
        assert_eq!(
            request["candidates"][3]["files"][0]["fileKey"],
            "candidate-3-file-0"
        );
        assert_eq!(
            request["candidates"][0]["parseFacts"]["episodeNumbers"][0],
            1
        );
        assert_eq!(
            compiled.case.allowed_references.file_keys,
            vec!["candidate-3-file-0", "candidate-3-file-1"]
        );
        Ok(())
    }

    #[test]
    fn expected_plan_source_indexes_bind_to_request_keys() -> Result<()> {
        let mut source = test_case();
        source.expected_final_plan.candidate_plans[0].candidate_index = 3;
        source.expected_final_plan.candidate_plans[0].coverage[0].file_index = Some(0);
        let compiled = compile_case(source)?;
        let plan = &compiled.case.expected_final_plan.candidate_plans[0];
        assert_eq!(plan.candidate_key, "candidate-3");
        assert_eq!(plan.target_keys, vec!["tg-s2-e1"]);
        assert_eq!(plan.file_keys, vec!["candidate-3-file-0"]);
        Ok(())
    }

    #[test]
    fn counterfactual_diff_ignores_request_id_and_binds_leaf_values() -> Result<()> {
        let left = serde_json::json!({
            "request": {"requestId": "left", "target": {"seasonNumber": 1}},
            "other": true
        });
        let right = serde_json::json!({
            "request": {"requestId": "right", "target": {"seasonNumber": 2}},
            "other": true
        });
        let left = normalized_counterfactual_input(&left)?;
        let right = normalized_counterfactual_input(&right)?;
        let pointers = differing_leaf_pointers(&left, &right)?;
        assert_eq!(pointers, vec!["/request/target/seasonNumber"]);
        let (left_value, left_invariant) = counterfactual_fingerprints(&left, &pointers)?;
        let (right_value, right_invariant) = counterfactual_fingerprints(&right, &pointers)?;
        assert_ne!(left_value, right_value);
        assert_eq!(left_invariant, right_invariant);
        Ok(())
    }

    #[test]
    fn selector_counterfactual_allowlist_rejects_non_selector_state() {
        for field in [
            CounterfactualField::Season,
            CounterfactualField::AbsoluteNumber,
        ] {
            assert!(field.allows_pointer("/request/target/wantedTargetKeys/0"));
            assert!(field.allows_pointer("/request/target/seasonNumber"));
            assert!(field.allows_pointer("/request/target/episodeNumbers/0"));
            assert!(field.allows_pointer("/request/target/absoluteEpisodeNumbers/0"));
            for rejected in [
                "/request/candidates/0/parseFacts/seasonNumbers/0",
                "/request/candidates/0/parseFacts/absoluteEpisodeNumbers/0",
                "/request/context/seasons/0/seasonNumber",
                "/request/context/seasons/0/targets/0/absoluteEpisodeNumber",
                "/scoringContext/targets/0/seasonNumber",
                "/scoringContext/targets/0/absoluteEpisodeNumber",
                "/request/context/graphFingerprint",
                "/scoringContext/graphFingerprint",
                "/request/target/audioPreference/mode",
            ] {
                assert!(!field.allows_pointer(rejected), "accepted {rejected}");
            }
        }
    }

    fn selector_input(
        wanted: &str,
        season_number: i64,
        episode_number: i64,
        absolute_episode_number: i64,
    ) -> JsonValue {
        serde_json::json!({
            "request": {
                "requestId": wanted,
                "target": {
                    "wantedTargetKeys": [wanted],
                    "seasonNumber": season_number,
                    "episodeNumbers": [episode_number],
                    "absoluteEpisodeNumbers": [absolute_episode_number],
                    "audioPreference": {"mode": "any"}
                },
                "context": {
                    "graphFingerprint": "fixed-graph",
                    "seasons": [
                        {
                            "seasonNumber": 1,
                            "anilistId": "season-one",
                            "aliases": [],
                            "targets": [{
                                "targetKey": "s1-e1",
                                "title": "Season One Episode One",
                                "seasonNumber": 1,
                                "episodeNumber": 1,
                                "absoluteEpisodeNumber": 1
                            }, {
                                "targetKey": "s1-e2",
                                "title": "Season One Episode Two",
                                "seasonNumber": 1,
                                "episodeNumber": 2,
                                "absoluteEpisodeNumber": 2
                            }]
                        },
                        {
                            "seasonNumber": 2,
                            "anilistId": "season-two",
                            "aliases": [],
                            "targets": [{
                                "targetKey": "s2-e1",
                                "title": "Season Two Episode One",
                                "seasonNumber": 2,
                                "episodeNumber": 1,
                                "absoluteEpisodeNumber": 13
                            }]
                        }
                    ]
                }
            }
        })
    }

    #[test]
    fn selector_counterfactual_semantics_bind_graph_targets() -> Result<()> {
        let season_one = selector_input("s1-e1", 1, 1, 1);
        let season_two = selector_input("s2-e1", 2, 1, 13);
        validate_selector_counterfactual(CounterfactualField::Season, &season_one, &season_two)?;

        let episode_two = selector_input("s1-e2", 1, 2, 2);
        validate_selector_counterfactual(
            CounterfactualField::AbsoluteNumber,
            &season_one,
            &episode_two,
        )?;
        assert!(
            validate_selector_counterfactual(
                CounterfactualField::AbsoluteNumber,
                &season_one,
                &season_two,
            )
            .is_err()
        );
        assert!(
            validate_selector_counterfactual(
                CounterfactualField::Season,
                &season_one,
                &episode_two,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn output_root_is_create_once() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let output = temporary.path().join("already-created");
        fs::create_dir(&output)?;
        let error = publish_compilation(
            &output,
            CompiledCorpus {
                assembly_id: "assembly-v1".to_string(),
                corpus_id: "corpus-v1".to_string(),
                curator_id: "owner".to_string(),
                timestamps: BlueprintTimestamps {
                    created_at: "2026-08-09T12:00:00Z".to_string(),
                    rules_frozen_at: "2026-08-09T12:00:00Z".to_string(),
                    frozen_labels_first_exposed_at: "2026-08-09T12:00:00Z".to_string(),
                },
                frozen_set_withheld_until_rules_frozen: true,
                representative_subset: BlueprintRepresentativeSubset {
                    id: "subset-v1".to_string(),
                    case_ids: vec!["case-1".to_string()],
                },
                cases: Vec::new(),
            },
        )
        .expect_err("existing output root must fail");
        assert!(error.to_string().contains("already exists"));
        Ok(())
    }
}
