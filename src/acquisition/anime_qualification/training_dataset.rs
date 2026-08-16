//! Production-shaped semantic-selector training dataset compiler.
//!
//! Public metadata collection and label curation happen outside the native
//! matcher. This module owns the final boundary: every training request is
//! rebuilt with the production parser, request validator, hypothesis builder,
//! prompt, and model-visible JSON projection.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

use super::{
    AllowedReferences, QualificationAudioEligibility, QualificationCandidatePlan,
    QualificationCase, QualificationCaseInput, QualificationCorpus, QualificationCorpusProfile,
    QualificationCoverageEntry, QualificationCoverageStatus, QualificationDisposition,
    QualificationFinalPlan, QualificationResolutionState, QualificationRouteContext,
    QualificationSets, canonical_json_fingerprint, deterministic_baseline,
    deterministic_union_state, final_plan_for_resolution, semantic_final_plans_match,
    validate_case_input, validate_corpus_shape,
};
use crate::{
    acquisition::{
        anime_matching::{
            acquisition_candidate_parse_facts, acquisition_match_context,
            selectable_anime_media_file,
        },
        automation::anime_semantic_media_kinds,
        release_resolution::anime::{
            AnimeCandidateScoringContext, AnimeCandidateTarget, AnimeScopedAlias,
        },
    },
    anime_matching::{
        ANIME_MATCH_PROMPT_REVISION, ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION, AnimeMatchAliasKind,
        AnimeMatchBatchInput, AnimeMatchCandidateInput, AnimeMatchContext, AnimeMatchFileInput,
        AnimeMatchTarget, AnimeMatchingService, AnimeSemanticEvidenceResponse,
        AnimeSemanticMediaKind, AnimeSemanticNumbering, build_semantic_evidence_request,
        semantic_evidence_training_messages,
    },
    http::handlers::acquisition_sources::AcquisitionCandidate,
};

const SOURCE_SCHEMA_VERSION: u32 = 1;
const DATASET_SCHEMA_VERSION: u32 = 1;
const SPLITS: [&str; 3] = ["train", "validation", "holdout"];
const INTEGRATED_DIAGNOSTIC_CASE_COUNT: usize = 640;
const INTEGRATED_MATCH_CASE_COUNT: usize = 512;
const INTEGRATED_NEGATIVE_CASE_COUNT: usize = 128;
const INTEGRATED_MIN_RECOVERABLE_CASES: usize = 160;
const INTEGRATED_TARGET_RECOVERABLE_CASES: usize = 256;
const INTEGRATED_MIN_RELATION_COMPONENTS: usize = 160;
const INTEGRATED_MAX_CASES_PER_COMPONENT: usize = 8;
const INTEGRATED_CANDIDATE_COUNT: usize = 6;

#[derive(Debug, Clone)]
pub struct AnimeTrainingCompileConfig {
    pub source_path: PathBuf,
    pub output_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct AnimeIntegratedDiagnosticCompileConfig {
    pub source_path: PathBuf,
    pub output_root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeIntegratedDiagnosticCompileSummary {
    pub status: String,
    pub dataset_id: String,
    pub corpus_id: String,
    pub case_count: usize,
    pub matched_case_count: usize,
    pub negative_case_count: usize,
    pub baseline_passed: usize,
    pub baseline_failed: usize,
    pub recoverable_case_count: usize,
    pub relation_component_count: usize,
    pub corpus_sha256: String,
    pub output_root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeTrainingCompileSummary {
    pub status: String,
    pub dataset_id: String,
    pub base_release_count: usize,
    pub example_count: usize,
    pub review_count: usize,
    pub output_root: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TrainingSource {
    schema_version: u32,
    dataset_id: String,
    created_at: String,
    source_fingerprint: String,
    base_release_count: usize,
    excluded_evaluation_source_ids: Vec<String>,
    examples: Vec<TrainingSourceExample>,
    review_queue: Vec<TrainingReviewEntry>,
    provenance: JsonValue,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TrainingSourceExample {
    example_id: String,
    base_release_id: String,
    source_record_id: String,
    split: String,
    example_kind: String,
    label_confidence: String,
    source_record_fingerprint: String,
    candidate: AcquisitionCandidate,
    target: AnimeMatchTarget,
    context: AnimeMatchContext,
    expected_anilist_id: Option<String>,
    expected_media_kind: Option<AnimeSemanticMediaKind>,
    provenance: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TrainingReviewEntry {
    review_id: String,
    source_record_id: String,
    release_title: String,
    #[serde(default)]
    file_names: Vec<String>,
    reason_codes: Vec<String>,
    proposed_target: JsonValue,
    evidence: JsonValue,
    #[serde(default)]
    owner_decision: Option<String>,
    #[serde(default)]
    owner_corrected_target: Option<JsonValue>,
    #[serde(default)]
    owner_notes: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompiledTrainingExample {
    schema_version: u32,
    example_id: String,
    base_release_id: String,
    source_record_id: String,
    split: String,
    example_kind: String,
    label_confidence: String,
    request_fingerprint: String,
    messages: Vec<JsonValue>,
    expected_response: AnimeSemanticEvidenceResponse,
    provenance: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetFileIdentity {
    path: String,
    sha256: String,
    size_bytes: u64,
    records: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TrainingDatasetManifest {
    schema_version: u32,
    status: String,
    dataset_id: String,
    created_at: String,
    prompt_revision: String,
    source_fingerprint: String,
    base_release_count: usize,
    example_count: usize,
    review_count: usize,
    split_base_release_counts: BTreeMap<String, usize>,
    split_example_counts: BTreeMap<String, usize>,
    excluded_evaluation_source_count: usize,
    source_provenance: JsonValue,
    files: Vec<DatasetFileIdentity>,
}

pub fn compile_anime_training_dataset(
    config: AnimeTrainingCompileConfig,
) -> Result<AnimeTrainingCompileSummary> {
    ensure!(
        !config.output_root.exists(),
        "training output already exists: {}",
        config.output_root.display()
    );
    let source_bytes = fs::read(&config.source_path)
        .with_context(|| format!("reading training source {}", config.source_path.display()))?;
    let source: TrainingSource = serde_json::from_slice(&source_bytes)
        .with_context(|| format!("decoding training source {}", config.source_path.display()))?;
    validate_source(&source)?;

    fs::create_dir(&config.output_root).with_context(|| {
        format!(
            "creating training output directory {}",
            config.output_root.display()
        )
    })?;

    let mut compiled_by_split = SPLITS
        .into_iter()
        .map(|split| (split.to_string(), Vec::new()))
        .collect::<BTreeMap<_, Vec<CompiledTrainingExample>>>();
    for example in source.examples {
        let compiled = compile_example(example)?;
        compiled_by_split
            .get_mut(&compiled.split)
            .expect("validated training split")
            .push(compiled);
    }

    let mut identities = Vec::new();
    let mut split_example_counts = BTreeMap::new();
    let mut split_base_release_counts = BTreeMap::new();
    for split in SPLITS {
        let examples = compiled_by_split
            .get(split)
            .expect("training split initialized");
        let path = config.output_root.join(format!("{split}.jsonl"));
        write_json_lines(&path, examples)?;
        identities.push(file_identity(&config.output_root, &path, examples.len())?);
        split_example_counts.insert(split.to_string(), examples.len());
        split_base_release_counts.insert(
            split.to_string(),
            examples
                .iter()
                .map(|example| example.base_release_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
        );
    }

    let review_jsonl = config.output_root.join("owner-review.jsonl");
    write_json_lines(&review_jsonl, &source.review_queue)?;
    identities.push(file_identity(
        &config.output_root,
        &review_jsonl,
        source.review_queue.len(),
    )?);
    let review_csv = config.output_root.join("owner-review.csv");
    write_review_csv(&review_csv, &source.review_queue)?;
    identities.push(file_identity(
        &config.output_root,
        &review_csv,
        source.review_queue.len(),
    )?);

    let example_count = split_example_counts.values().sum();
    let manifest = TrainingDatasetManifest {
        schema_version: DATASET_SCHEMA_VERSION,
        status: "pilot-ready-for-owner-review".to_string(),
        dataset_id: source.dataset_id.clone(),
        created_at: source.created_at,
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
        source_fingerprint: source.source_fingerprint,
        base_release_count: source.base_release_count,
        example_count,
        review_count: source.review_queue.len(),
        split_base_release_counts,
        split_example_counts,
        excluded_evaluation_source_count: source.excluded_evaluation_source_ids.len(),
        source_provenance: source.provenance,
        files: identities,
    };
    let manifest_path = config.output_root.join("manifest.json");
    write_canonical_json(&manifest_path, &manifest)?;
    validate_anime_training_dataset(&config.output_root)?;

    Ok(AnimeTrainingCompileSummary {
        status: manifest.status,
        dataset_id: manifest.dataset_id,
        base_release_count: manifest.base_release_count,
        example_count: manifest.example_count,
        review_count: manifest.review_count,
        output_root: config.output_root,
    })
}

#[derive(Debug, Clone)]
struct DiagnosticRelease {
    base_release_id: String,
    source_record_id: String,
    source_record_fingerprint: String,
    candidate: AcquisitionCandidate,
    target: AnimeMatchTarget,
    context: AnimeMatchContext,
    anilist_id: String,
    relation_component_id: i64,
    slice_tags: Vec<String>,
}

struct DiagnosticTrial {
    case: QualificationCase,
    target_source_record_id: String,
    candidate_source_record_ids: Vec<String>,
    relation_component_id: i64,
    baseline_matches_expected: bool,
    baseline_has_candidate_plan: bool,
    recoverable: bool,
    negative: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IntegratedDiagnosticAudit {
    schema_version: u32,
    status: String,
    dataset_id: String,
    source_fingerprint: String,
    corpus_id: String,
    corpus_sha256: String,
    case_count: usize,
    matched_case_count: usize,
    negative_case_count: usize,
    baseline_passed: usize,
    baseline_failed: usize,
    recoverable_case_count: usize,
    relation_component_count: usize,
    maximum_cases_per_relation_component: usize,
    candidate_count_per_case: usize,
    selected_target_source_count: usize,
    selected_candidate_source_count: usize,
    selected_target_sources_fingerprint: String,
    selected_candidate_sources_fingerprint: String,
    slice_counts: BTreeMap<String, usize>,
    projected_splits: Vec<String>,
    train_source_overlap: usize,
    holdout_source_overlap: usize,
    forced_training_component_overlap: usize,
    holdout_projected_or_scored: bool,
}

/// Build a clean production-path diagnostic corpus solely from the already
/// opened validation partition. Training and holdout examples are used only
/// as identity sets for the contamination assertion; their labels are never
/// projected into a case or scored.
pub fn compile_anime_integrated_diagnostic_corpus(
    config: AnimeIntegratedDiagnosticCompileConfig,
) -> Result<AnimeIntegratedDiagnosticCompileSummary> {
    ensure!(
        !config.output_root.exists(),
        "integrated diagnostic output already exists: {}",
        config.output_root.display()
    );
    let source_bytes = fs::read(&config.source_path)
        .with_context(|| format!("reading training source {}", config.source_path.display()))?;
    let source: TrainingSource = serde_json::from_slice(&source_bytes)
        .with_context(|| format!("decoding training source {}", config.source_path.display()))?;
    validate_source(&source)?;

    let dataset_id = source.dataset_id.clone();
    let source_fingerprint = source.source_fingerprint.clone();
    let mut split_source_ids = SPLITS
        .into_iter()
        .map(|split| (split.to_string(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut component_splits = BTreeMap::<i64, String>::new();
    for example in source
        .examples
        .iter()
        .filter(|example| example.example_kind == "positive")
    {
        split_source_ids
            .get_mut(&example.split)
            .expect("validated split")
            .insert(example.source_record_id.clone());
        let component = provenance_integer(
            &example.provenance,
            "relationComponentId",
            &example.example_id,
        )?;
        match component_splits.insert(component, example.split.clone()) {
            Some(existing) => ensure!(
                existing == example.split,
                "relation component {component} crosses {existing} and {}",
                example.split
            ),
            None => {}
        }
    }
    let forced_training_components = source
        .provenance
        .pointer("/partitionHistory/forcedTrainingComponentIds")
        .and_then(JsonValue::as_array)
        .context("training source lacks forced-training component identities")?
        .iter()
        .map(|value| {
            value
                .as_i64()
                .context("forced-training component identity is not an integer")
        })
        .collect::<Result<BTreeSet<_>>>()?;

    let mut releases = source
        .examples
        .into_iter()
        .filter(|example| example.split == "validation" && example.example_kind == "positive")
        .map(diagnostic_release)
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        releases.len() >= INTEGRATED_DIAGNOSTIC_CASE_COUNT,
        "validation partition has only {} positive releases; need at least {INTEGRATED_DIAGNOSTIC_CASE_COUNT}",
        releases.len()
    );
    releases.sort_by(|left, right| {
        left.source_record_fingerprint
            .cmp(&right.source_record_fingerprint)
            .then_with(|| left.source_record_id.cmp(&right.source_record_id))
    });
    let target_order = interleaved_component_order(&releases);

    let mut recoverable_trials = Vec::new();
    let mut easy_trials = Vec::new();
    for &target_index in &target_order {
        let trial = build_diagnostic_trial(&releases, target_index, false)?;
        if trial.baseline_matches_expected {
            if easy_trials.len() < INTEGRATED_MATCH_CASE_COUNT {
                easy_trials.push(trial);
            }
        } else if !trial.baseline_has_candidate_plan {
            if recoverable_trials.len() < INTEGRATED_MATCH_CASE_COUNT {
                recoverable_trials.push(trial);
            }
        }
    }
    ensure!(
        recoverable_trials.len() >= INTEGRATED_MIN_RECOVERABLE_CASES,
        "clean validation contains only {} safely recoverable integrated cases; need at least {INTEGRATED_MIN_RECOVERABLE_CASES}",
        recoverable_trials.len()
    );

    let desired_recoverable = INTEGRATED_TARGET_RECOVERABLE_CASES.min(recoverable_trials.len());
    let mut selected = Vec::<DiagnosticTrial>::with_capacity(INTEGRATED_DIAGNOSTIC_CASE_COUNT);
    let mut component_case_counts = BTreeMap::<i64, usize>::new();
    let mut selected_target_ids = BTreeSet::<String>::new();
    take_diagnostic_trials(
        recoverable_trials,
        desired_recoverable,
        &mut selected,
        &mut component_case_counts,
        &mut selected_target_ids,
    );
    let remaining_matches = INTEGRATED_MATCH_CASE_COUNT - selected.len();
    take_diagnostic_trials(
        easy_trials,
        remaining_matches,
        &mut selected,
        &mut component_case_counts,
        &mut selected_target_ids,
    );
    ensure!(
        selected.len() == INTEGRATED_MATCH_CASE_COUNT,
        "could select only {} clean matched cases; need {INTEGRATED_MATCH_CASE_COUNT}",
        selected.len()
    );

    for &target_index in &target_order {
        if selected.len() == INTEGRATED_DIAGNOSTIC_CASE_COUNT {
            break;
        }
        let target = &releases[target_index];
        if selected_target_ids.contains(&target.source_record_id)
            || component_case_counts
                .get(&target.relation_component_id)
                .copied()
                .unwrap_or_default()
                >= INTEGRATED_MAX_CASES_PER_COMPONENT
        {
            continue;
        }
        let trial = build_diagnostic_trial(&releases, target_index, true)?;
        if trial.baseline_has_candidate_plan {
            continue;
        }
        selected_target_ids.insert(trial.target_source_record_id.clone());
        *component_case_counts
            .entry(trial.relation_component_id)
            .or_insert(0) += 1;
        selected.push(trial);
    }
    ensure!(
        selected.len() == INTEGRATED_DIAGNOSTIC_CASE_COUNT,
        "could select only {} clean integrated cases; need {INTEGRATED_DIAGNOSTIC_CASE_COUNT}",
        selected.len()
    );
    let relation_component_count = component_case_counts.len();
    ensure!(
        relation_component_count >= INTEGRATED_MIN_RELATION_COMPONENTS,
        "clean integrated projection covers only {relation_component_count} relation components; need at least {INTEGRATED_MIN_RELATION_COMPONENTS}"
    );
    ensure!(
        component_case_counts
            .values()
            .all(|count| *count <= INTEGRATED_MAX_CASES_PER_COMPONENT),
        "clean integrated projection exceeds its relation-component cap"
    );

    let selected_candidate_ids = selected
        .iter()
        .flat_map(|trial| trial.candidate_source_record_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let source_components = releases
        .iter()
        .map(|release| {
            (
                release.source_record_id.as_str(),
                release.relation_component_id,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut selected_component_ids = selected
        .iter()
        .map(|trial| trial.relation_component_id)
        .collect::<BTreeSet<_>>();
    selected_component_ids.extend(
        selected_candidate_ids
            .iter()
            .map(|source_id| source_components[source_id.as_str()]),
    );
    let train_overlap = selected_target_ids
        .union(&selected_candidate_ids)
        .filter(|source_id| split_source_ids["train"].contains(*source_id))
        .count();
    let holdout_overlap = selected_target_ids
        .union(&selected_candidate_ids)
        .filter(|source_id| split_source_ids["holdout"].contains(*source_id))
        .count();
    let forced_component_overlap = selected_component_ids
        .intersection(&forced_training_components)
        .count();
    ensure!(
        train_overlap == 0 && holdout_overlap == 0 && forced_component_overlap == 0,
        "clean integrated projection is contaminated: train={train_overlap}, holdout={holdout_overlap}, historyComponents={forced_component_overlap}"
    );
    ensure!(
        selected_target_ids
            .union(&selected_candidate_ids)
            .all(|source_id| split_source_ids["validation"].contains(source_id)),
        "clean integrated projection contains a non-validation source"
    );

    let baseline_passed = selected
        .iter()
        .filter(|trial| trial.baseline_matches_expected)
        .count();
    let baseline_failed = selected.len() - baseline_passed;
    let recoverable_case_count = selected.iter().filter(|trial| trial.recoverable).count();
    let negative_case_count = selected.iter().filter(|trial| trial.negative).count();
    ensure!(
        negative_case_count == INTEGRATED_NEGATIVE_CASE_COUNT,
        "clean integrated projection has {negative_case_count} negatives"
    );
    ensure!(
        recoverable_case_count >= INTEGRATED_MIN_RECOVERABLE_CASES,
        "clean integrated projection retained only {recoverable_case_count} recoverable cases"
    );
    let mut slice_counts = BTreeMap::<String, usize>::new();
    for trial in &selected {
        *slice_counts
            .entry(
                trial
                    .case
                    .slice
                    .clone()
                    .unwrap_or_else(|| "unspecified".to_string()),
            )
            .or_insert(0) += 1;
    }

    let corpus_id = format!("{dataset_id}-clean-integrated-validation-v1");
    let cases = selected
        .into_iter()
        .map(|trial| trial.case)
        .collect::<Vec<_>>();
    let development = cases
        .iter()
        .map(|case| case.case_id.clone())
        .collect::<Vec<_>>();
    let corpus = QualificationCorpus {
        schema_version: 2,
        status: "frozen".to_string(),
        corpus_id: corpus_id.clone(),
        profile: QualificationCorpusProfile::CleanValidationDiagnosticV1,
        sets: QualificationSets {
            smoke: Vec::new(),
            development,
            frozen: Vec::new(),
        },
        cases,
    };
    let corpus_value = serde_json::to_value(&corpus)?;
    let corpus_bytes = serde_json::to_vec(&corpus_value)?;
    validate_corpus_shape(&corpus, &corpus_bytes)?;

    fs::create_dir(&config.output_root).with_context(|| {
        format!(
            "creating integrated diagnostic output {}",
            config.output_root.display()
        )
    })?;
    let corpus_path = config.output_root.join("qualification-corpus.json");
    write_canonical_json(&corpus_path, &corpus)?;
    let corpus_file_bytes = fs::read(&corpus_path)?;
    let corpus_sha256 = format!("sha256:{:x}", Sha256::digest(&corpus_file_bytes));
    let audit = IntegratedDiagnosticAudit {
        schema_version: 1,
        status: "pure".to_string(),
        dataset_id: dataset_id.clone(),
        source_fingerprint,
        corpus_id: corpus_id.clone(),
        corpus_sha256: corpus_sha256.clone(),
        case_count: INTEGRATED_DIAGNOSTIC_CASE_COUNT,
        matched_case_count: INTEGRATED_MATCH_CASE_COUNT,
        negative_case_count,
        baseline_passed,
        baseline_failed,
        recoverable_case_count,
        relation_component_count,
        maximum_cases_per_relation_component: component_case_counts
            .values()
            .copied()
            .max()
            .unwrap_or_default(),
        candidate_count_per_case: INTEGRATED_CANDIDATE_COUNT,
        selected_target_source_count: selected_target_ids.len(),
        selected_candidate_source_count: selected_candidate_ids.len(),
        selected_target_sources_fingerprint: json_fingerprint(&serde_json::to_value(
            &selected_target_ids,
        )?)?,
        selected_candidate_sources_fingerprint: json_fingerprint(&serde_json::to_value(
            &selected_candidate_ids,
        )?)?,
        slice_counts,
        projected_splits: vec!["validation".to_string()],
        train_source_overlap: train_overlap,
        holdout_source_overlap: holdout_overlap,
        forced_training_component_overlap: forced_component_overlap,
        holdout_projected_or_scored: false,
    };
    write_canonical_json(&config.output_root.join("projection-audit.json"), &audit)?;

    Ok(AnimeIntegratedDiagnosticCompileSummary {
        status: "ready".to_string(),
        dataset_id,
        corpus_id,
        case_count: INTEGRATED_DIAGNOSTIC_CASE_COUNT,
        matched_case_count: INTEGRATED_MATCH_CASE_COUNT,
        negative_case_count,
        baseline_passed,
        baseline_failed,
        recoverable_case_count,
        relation_component_count,
        corpus_sha256,
        output_root: config.output_root,
    })
}

fn diagnostic_release(example: TrainingSourceExample) -> Result<DiagnosticRelease> {
    let expected_anilist_id = example
        .expected_anilist_id
        .as_deref()
        .context("validation positive lacks expected AniList identity")?;
    ensure!(
        example.context.seasons.iter().any(|season| {
            season.anilist_id == expected_anilist_id
                && season.targets.iter().any(|candidate| {
                    example
                        .target
                        .wanted_target_keys
                        .contains(&candidate.target_key)
                })
        }),
        "validation positive {} does not bind its wanted target to expected AniList entity",
        example.example_id
    );
    ensure!(
        example
            .candidate
            .files
            .iter()
            .filter(|file| selectable_anime_media_file(file))
            .count()
            == 1,
        "validation positive {} is not a single selectable media release",
        example.example_id
    );
    let relation_component_id = provenance_integer(
        &example.provenance,
        "relationComponentId",
        &example.example_id,
    )?;
    let anilist_id =
        provenance_integer(&example.provenance, "anilistId", &example.example_id)?.to_string();
    ensure!(
        anilist_id == expected_anilist_id,
        "validation positive {} provenance and label AniList identities differ",
        example.example_id
    );
    let mut slice_tags = example
        .provenance
        .get("sliceTags")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if example
        .provenance
        .get("titleDivergent")
        .and_then(JsonValue::as_bool)
        == Some(true)
    {
        slice_tags.push("title_divergent".to_string());
    }
    slice_tags.sort();
    slice_tags.dedup();
    Ok(DiagnosticRelease {
        base_release_id: example.base_release_id,
        source_record_id: example.source_record_id,
        source_record_fingerprint: example.source_record_fingerprint,
        candidate: example.candidate,
        target: example.target,
        context: example.context,
        anilist_id,
        relation_component_id,
        slice_tags,
    })
}

fn provenance_integer(provenance: &JsonValue, key: &str, example_id: &str) -> Result<i64> {
    provenance
        .get(key)
        .and_then(JsonValue::as_i64)
        .filter(|value| *value > 0)
        .with_context(|| format!("example {example_id} lacks positive integer provenance {key}"))
}

fn interleaved_component_order(releases: &[DiagnosticRelease]) -> Vec<usize> {
    let mut by_component = BTreeMap::<i64, Vec<usize>>::new();
    for (index, release) in releases.iter().enumerate() {
        by_component
            .entry(release.relation_component_id)
            .or_default()
            .push(index);
    }
    let maximum = by_component
        .values()
        .map(Vec::len)
        .max()
        .unwrap_or_default();
    (0..maximum)
        .flat_map(|offset| {
            by_component
                .values()
                .filter_map(move |values| values.get(offset).copied())
        })
        .collect()
}

fn build_diagnostic_trial(
    releases: &[DiagnosticRelease],
    target_index: usize,
    negative: bool,
) -> Result<DiagnosticTrial> {
    let target_release = releases
        .get(target_index)
        .context("diagnostic target index is out of bounds")?;
    let (candidate_indexes, correct_candidate_index) =
        diagnostic_candidate_indexes(releases, target_index, !negative)?;
    let acquisition_candidates = candidate_indexes
        .iter()
        .map(|index| releases[*index].candidate.clone())
        .collect::<Vec<_>>();
    let scoring_context = diagnostic_scoring_context(target_release)?;
    let context = acquisition_match_context(
        &target_release.target.canonical_title,
        &scoring_context,
        &target_release.target,
    )?;
    let case_kind = if negative { "negative" } else { "match" };
    let case_digest = Sha256::digest(format!(
        "clean-integrated-v1:{case_kind}:{}",
        target_release.base_release_id
    ));
    let case_digest = format!("{case_digest:x}");
    let case_id = format!("clean-{case_kind}-{}", &case_digest[..16]);
    let request_id = format!("diagnostic-{case_id}");
    let prepared = AnimeMatchingService::prepare_request(AnimeMatchBatchInput {
        request_id,
        target: target_release.target.clone(),
        context,
        candidates: acquisition_candidates
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
    .with_context(|| format!("preparing clean integrated case {case_id}"))?;
    let request = prepared.request().clone();
    ensure!(
        request.candidates.len() == INTEGRATED_CANDIDATE_COUNT,
        "clean integrated case {case_id} did not retain six candidates"
    );
    let route_context = QualificationRouteContext {
        file_selection_supported_by_candidate_key: request
            .candidates
            .iter()
            .map(|candidate| (candidate.candidate_key.clone(), true))
            .collect(),
    };
    let expected_final_plan = if let Some(candidate_index) = correct_candidate_index {
        let request_candidate = request
            .candidates
            .get(candidate_index)
            .context("correct diagnostic candidate is out of bounds")?;
        ensure!(
            request_candidate.files.len() == 1,
            "correct diagnostic candidate does not have exactly one media file"
        );
        let target_key = request
            .target
            .wanted_target_keys
            .first()
            .cloned()
            .context("clean integrated target has no wanted key")?;
        ensure!(
            request.target.wanted_target_keys.len() == 1,
            "clean integrated target must request exactly one episode"
        );
        let file_key = request_candidate.files[0].file_key.clone();
        let mut candidate_plans = vec![None; INTEGRATED_CANDIDATE_COUNT];
        candidate_plans[candidate_index] = Some(QualificationCandidatePlan {
            candidate_key: request_candidate.candidate_key.clone(),
            target_keys: vec![target_key.clone()],
            file_keys: vec![file_key.clone()],
            audio_eligibility: QualificationAudioEligibility::NotApplicable,
            coverage: vec![QualificationCoverageEntry {
                target_key,
                file_key: Some(file_key),
                status: QualificationCoverageStatus::Covered,
            }],
        });
        final_plan_for_resolution(
            &request,
            &QualificationResolutionState {
                candidate_plans,
                saw_partial_or_ambiguous: false,
            },
        )?
    } else {
        QualificationFinalPlan {
            disposition: QualificationDisposition::NoMatch,
            season_number: None,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            candidate_plans: Vec::new(),
        }
    };
    let input = QualificationCaseInput {
        request,
        scoring_context,
        acquisition_candidates,
        route_context,
    };
    let input_value = serde_json::to_value(&input)?;
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
    let mut case = QualificationCase {
        case_id,
        set: "development".to_string(),
        slice: Some(if negative {
            "hard_negative".to_string()
        } else {
            diagnostic_slice(&target_release.slice_tags)
        }),
        origin: "real".to_string(),
        realistic_noise: true,
        counterfactual_pair_id: None,
        counterfactual_mutation: None,
        stability_subset: false,
        deterministic_easy: false,
        input_fingerprint,
        input: input_value,
        allowed_references,
        expected_final_plan,
    };
    let baseline_resolution = deterministic_baseline(&input)?;
    let baseline = final_plan_for_resolution(&input.request, &baseline_resolution)?;
    if negative
        && baseline.candidate_plans.is_empty()
        && baseline.disposition != QualificationDisposition::Matched
    {
        case.expected_final_plan = baseline.clone();
    }
    let baseline_matches_expected =
        semantic_final_plans_match(&baseline, &case.expected_final_plan);
    let baseline_has_candidate_plan = !baseline.candidate_plans.is_empty();
    case.deterministic_easy = deterministic_union_state(&input.request, &baseline_resolution)
        == crate::anime_matching::DeterministicMatchState::Definitive
        && baseline_matches_expected;
    validate_case_input(&case, &input)?;
    Ok(DiagnosticTrial {
        case,
        target_source_record_id: target_release.source_record_id.clone(),
        candidate_source_record_ids: candidate_indexes
            .into_iter()
            .map(|index| releases[index].source_record_id.clone())
            .collect(),
        relation_component_id: target_release.relation_component_id,
        baseline_matches_expected,
        baseline_has_candidate_plan,
        recoverable: !negative && !baseline_matches_expected && !baseline_has_candidate_plan,
        negative,
    })
}

fn diagnostic_candidate_indexes(
    releases: &[DiagnosticRelease],
    target_index: usize,
    include_correct: bool,
) -> Result<(Vec<usize>, Option<usize>)> {
    let target = &releases[target_index];
    let needed = if include_correct {
        INTEGRATED_CANDIDATE_COUNT - 1
    } else {
        INTEGRATED_CANDIDATE_COUNT
    };
    let start = stable_index(&target.source_record_id, releases.len());
    let circular = (0..releases.len()).map(|offset| (start + offset) % releases.len());
    let mut distractors = Vec::with_capacity(needed);
    for same_component_only in [true, false] {
        for index in circular.clone() {
            let candidate = &releases[index];
            if index == target_index
                || candidate.anilist_id == target.anilist_id
                || distractors.contains(&index)
                || (same_component_only
                    && candidate.relation_component_id != target.relation_component_id)
            {
                continue;
            }
            distractors.push(index);
            if distractors.len() == needed {
                break;
            }
        }
        if distractors.len() == needed {
            break;
        }
    }
    ensure!(
        distractors.len() == needed,
        "validation release {} has too few distinct distractors",
        target.source_record_id
    );
    if !include_correct {
        return Ok((distractors, None));
    }
    let correct_index = stable_index(
        &format!("candidate-position:{}", target.source_record_id),
        INTEGRATED_CANDIDATE_COUNT,
    );
    distractors.insert(correct_index, target_index);
    Ok((distractors, Some(correct_index)))
}

fn stable_index(value: &str, modulus: usize) -> usize {
    let digest = Sha256::digest(value.as_bytes());
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(prefix) % modulus as u64) as usize
}

fn diagnostic_scoring_context(release: &DiagnosticRelease) -> Result<AnimeCandidateScoringContext> {
    let mut scoped_aliases = Vec::new();
    let mut alias_identities = BTreeSet::new();
    let mut targets = Vec::new();
    for season in &release.context.seasons {
        for alias in &season.aliases {
            if matches!(
                alias.kind,
                AnimeMatchAliasKind::Canonical | AnimeMatchAliasKind::Generated
            ) {
                continue;
            }
            let source = alias
                .source
                .clone()
                .unwrap_or_else(|| diagnostic_alias_source(alias.kind).to_string());
            let identity = (
                alias.value.clone(),
                source.clone(),
                alias.language.clone(),
                season.season_number,
                season.anilist_id.clone(),
            );
            if alias_identities.insert(identity) {
                scoped_aliases.push(AnimeScopedAlias {
                    display: alias.value.clone(),
                    source,
                    language: alias.language.clone(),
                    season_number: Some(season.season_number),
                    anilist_season_id: Some(season.anilist_id.clone()),
                });
            }
        }
        targets.extend(season.targets.iter().map(|target| AnimeCandidateTarget {
            target_key: target.target_key.clone(),
            canonical_key: None,
            title: target.title.clone(),
            season_number: target.season_number.or(Some(season.season_number)),
            anilist_season_id: Some(season.anilist_id.clone()),
            episode_number: target.episode_number,
            absolute_episode_number: target.absolute_episode_number,
            tvdb_episode_id: target.tvdb_episode_id.clone(),
            anidb_episode_id: target.anidb_episode_id.clone(),
        }));
    }
    ensure!(
        !targets.is_empty(),
        "validation release {} has incomplete graph context",
        release.source_record_id
    );
    Ok(AnimeCandidateScoringContext {
        graph_fingerprint: Some(release.context.graph_fingerprint.clone()),
        aliases: Vec::new(),
        scoped_aliases,
        targets,
    })
}

fn diagnostic_alias_source(kind: AnimeMatchAliasKind) -> &'static str {
    match kind {
        AnimeMatchAliasKind::Canonical => "canonical_title",
        AnimeMatchAliasKind::English => "anilist.english",
        AnimeMatchAliasKind::Romaji => "anilist.romaji",
        AnimeMatchAliasKind::Native => "anilist.native",
        AnimeMatchAliasKind::Synonym => "anilist.synonym",
        AnimeMatchAliasKind::Generated => "generated_season",
    }
}

fn diagnostic_slice(tags: &[String]) -> String {
    [
        "title_divergent",
        "season_aliases",
        "numbering",
        "cours_parts_arcs",
        "special_boundaries",
        "cross_script",
        "audio",
        "no_match_near_miss",
    ]
    .into_iter()
    .find(|candidate| tags.iter().any(|tag| tag == candidate))
    .unwrap_or("real_release_name")
    .to_string()
}

fn take_diagnostic_trials(
    trials: Vec<DiagnosticTrial>,
    requested: usize,
    selected: &mut Vec<DiagnosticTrial>,
    component_case_counts: &mut BTreeMap<i64, usize>,
    selected_target_ids: &mut BTreeSet<String>,
) {
    let starting_len = selected.len();
    for trial in trials {
        if selected.len() - starting_len == requested {
            break;
        }
        let count = component_case_counts
            .get(&trial.relation_component_id)
            .copied()
            .unwrap_or_default();
        if count >= INTEGRATED_MAX_CASES_PER_COMPONENT
            || selected_target_ids.contains(&trial.target_source_record_id)
        {
            continue;
        }
        selected_target_ids.insert(trial.target_source_record_id.clone());
        component_case_counts.insert(trial.relation_component_id, count + 1);
        selected.push(trial);
    }
}

pub fn validate_anime_training_dataset(output_root: &Path) -> Result<AnimeTrainingCompileSummary> {
    let manifest_path = output_root.join("manifest.json");
    let manifest: TrainingDatasetManifest = serde_json::from_slice(
        &fs::read(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?,
    )
    .with_context(|| format!("decoding {}", manifest_path.display()))?;
    ensure!(
        manifest.schema_version == DATASET_SCHEMA_VERSION,
        "unsupported training manifest schema {}",
        manifest.schema_version
    );
    ensure!(
        manifest.status == "pilot-ready-for-owner-review",
        "training manifest status is not reviewable"
    );

    let expected_paths = [
        "train.jsonl",
        "validation.jsonl",
        "holdout.jsonl",
        "owner-review.jsonl",
        "owner-review.csv",
    ];
    ensure!(
        manifest
            .files
            .iter()
            .map(|file| file.path.as_str())
            .eq(expected_paths),
        "training manifest file order or inventory differs"
    );
    for identity in &manifest.files {
        let path = output_root.join(&identity.path);
        let measured = file_identity(output_root, &path, identity.records)?;
        ensure!(
            measured.sha256 == identity.sha256 && measured.size_bytes == identity.size_bytes,
            "training artifact identity differs: {}",
            identity.path
        );
    }

    let mut source_split = BTreeMap::<String, String>::new();
    let mut example_ids = BTreeSet::new();
    let mut example_count = 0usize;
    let mut base_release_ids = BTreeSet::new();
    for split in SPLITS {
        let path = output_root.join(format!("{split}.jsonl"));
        let examples: Vec<CompiledTrainingExample> = read_json_lines(&path)?;
        ensure!(
            examples.len() == manifest.split_example_counts[split],
            "{split} example count differs from manifest"
        );
        let split_bases = examples
            .iter()
            .map(|example| example.base_release_id.as_str())
            .collect::<BTreeSet<_>>();
        ensure!(
            split_bases.len() == manifest.split_base_release_counts[split],
            "{split} base-release count differs from manifest"
        );
        for example in examples {
            ensure!(example.split == split, "example stored in the wrong split");
            ensure!(
                example_ids.insert(example.example_id.clone()),
                "duplicate training example id {}",
                example.example_id
            );
            base_release_ids.insert(example.base_release_id.clone());
            match source_split.insert(example.source_record_id.clone(), split.to_string()) {
                Some(existing) => ensure!(
                    existing == split,
                    "source record {} leaks across {existing} and {split}",
                    example.source_record_id
                ),
                None => {}
            }
            validate_compiled_example(&example)?;
            example_count += 1;
        }
    }
    ensure!(
        example_count == manifest.example_count,
        "training example count differs from manifest"
    );
    ensure!(
        base_release_ids.len() == manifest.base_release_count,
        "training base-release count differs from manifest"
    );

    let reviews: Vec<TrainingReviewEntry> =
        read_json_lines(&output_root.join("owner-review.jsonl"))?;
    ensure!(
        reviews.len() == manifest.review_count,
        "owner-review count differs from manifest"
    );
    ensure!(
        reviews
            .iter()
            .map(|entry| entry.review_id.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            == reviews.len(),
        "owner-review queue contains duplicate ids"
    );

    Ok(AnimeTrainingCompileSummary {
        status: "valid".to_string(),
        dataset_id: manifest.dataset_id,
        base_release_count: manifest.base_release_count,
        example_count: manifest.example_count,
        review_count: manifest.review_count,
        output_root: output_root.to_path_buf(),
    })
}

fn validate_source(source: &TrainingSource) -> Result<()> {
    ensure!(
        source.schema_version == SOURCE_SCHEMA_VERSION,
        "unsupported training source schema {}",
        source.schema_version
    );
    ensure!(
        !source.dataset_id.trim().is_empty(),
        "training dataset id is empty"
    );
    ensure!(
        source.source_fingerprint.starts_with("sha256:"),
        "training source fingerprint is not SHA-256"
    );
    ensure!(source.base_release_count > 0, "training source is empty");
    let excluded = source
        .excluded_evaluation_source_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        excluded.len() == source.excluded_evaluation_source_ids.len(),
        "evaluation exclusion list contains duplicates"
    );
    let mut example_ids = BTreeSet::new();
    let mut base_ids = BTreeSet::new();
    let mut source_splits = BTreeMap::<&str, &str>::new();
    for example in &source.examples {
        ensure!(
            SPLITS.contains(&example.split.as_str()),
            "example {} has invalid split {}",
            example.example_id,
            example.split
        );
        ensure!(
            matches!(
                example.example_kind.as_str(),
                "positive" | "wrong_coordinate" | "wrong_entity"
            ),
            "example {} has invalid kind {}",
            example.example_id,
            example.example_kind
        );
        ensure!(
            example.label_confidence == "source_grounded_gold",
            "example {} is not gold-tier",
            example.example_id
        );
        ensure!(
            example.source_record_fingerprint.starts_with("sha256:"),
            "example {} lacks a source fingerprint",
            example.example_id
        );
        ensure!(
            !excluded.contains(example.source_record_id.as_str()),
            "frozen evaluation source {} leaked into training",
            example.source_record_id
        );
        ensure!(
            example_ids.insert(example.example_id.as_str()),
            "duplicate source example id {}",
            example.example_id
        );
        base_ids.insert(example.base_release_id.as_str());
        match source_splits.insert(&example.source_record_id, &example.split) {
            Some(existing) => ensure!(
                existing == example.split,
                "source record {} leaks across source splits",
                example.source_record_id
            ),
            None => {}
        }
        if matches!(
            example.example_kind.as_str(),
            "positive" | "wrong_coordinate"
        ) {
            ensure!(
                example.expected_anilist_id.is_some() && example.expected_media_kind.is_some(),
                "semantic-positive example {} lacks its gold hypothesis identity",
                example.example_id
            );
        } else {
            ensure!(
                example.expected_anilist_id.is_none() && example.expected_media_kind.is_none(),
                "negative example {} declares a positive hypothesis",
                example.example_id
            );
        }
    }
    ensure!(
        base_ids.len() == source.base_release_count,
        "declared base-release count differs from source examples"
    );
    ensure!(
        SPLITS.iter().all(|split| source
            .examples
            .iter()
            .any(|example| example.split == *split)),
        "training source must populate every split"
    );
    Ok(())
}

fn compile_example(source: TrainingSourceExample) -> Result<CompiledTrainingExample> {
    let candidate = source.candidate;
    let parse_facts = acquisition_candidate_parse_facts(&candidate);
    let prepared = AnimeMatchingService::prepare_request(AnimeMatchBatchInput {
        request_id: source.example_id.clone(),
        target: source.target,
        context: source.context,
        candidates: vec![AnimeMatchCandidateInput {
            source: (),
            title: candidate.title.clone(),
            files: candidate
                .files
                .iter()
                .filter(|file| file.selectable.unwrap_or(true) && is_video_path(&file.path))
                .map(|file| AnimeMatchFileInput {
                    source: (),
                    path: file.path.clone(),
                })
                .collect(),
            parse_facts,
        }],
    })
    .with_context(|| format!("preparing training example {}", source.example_id))?;
    let request = prepared.request();
    let candidate = request
        .candidates
        .first()
        .context("prepared training request has no candidate")?;
    let facts = &candidate.parse_facts;
    let semantic = build_semantic_evidence_request(
        request,
        candidate.candidate_key.clone(),
        candidate.title.clone(),
        None,
        facts.title_candidates.iter().cloned(),
        facts.season_numbers.iter().copied(),
        facts.episode_numbers.iter().copied(),
        facts.absolute_episode_numbers.iter().copied(),
        anime_semantic_media_kinds(&candidate.title, facts),
    )?
    .with_context(|| {
        format!(
            "training example {} has no semantic request",
            source.example_id
        )
    })?;

    let hypothesis_index = if source.expected_anilist_id.is_some() {
        Some(select_gold_hypothesis(
            &source.example_id,
            &semantic,
            source
                .expected_anilist_id
                .as_deref()
                .expect("semantic-positive source validated"),
            source
                .expected_media_kind
                .expect("semantic-positive source validated"),
        )?)
    } else {
        None
    };
    let response = AnimeSemanticEvidenceResponse {
        schema_version: ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
        hypothesis_index,
    };
    let mut messages = semantic_evidence_training_messages(&semantic)?;
    messages.push(json!({
        "role": "assistant",
        "content": serde_json::to_string(&response)
            .context("encoding semantic training response")?
    }));
    let request_fingerprint = json_fingerprint(&json!({
        "messages": &messages[..messages.len() - 1],
        "response": response,
    }))?;

    Ok(CompiledTrainingExample {
        schema_version: DATASET_SCHEMA_VERSION,
        example_id: source.example_id,
        base_release_id: source.base_release_id,
        source_record_id: source.source_record_id,
        split: source.split,
        example_kind: source.example_kind,
        label_confidence: source.label_confidence,
        request_fingerprint,
        messages,
        expected_response: response,
        provenance: source.provenance,
    })
}

fn select_gold_hypothesis(
    example_id: &str,
    semantic: &crate::anime_matching::AnimeSemanticEvidenceRequest,
    expected_anilist_id: &str,
    expected_media_kind: AnimeSemanticMediaKind,
) -> Result<usize> {
    let entity_index = semantic
        .entities
        .iter()
        .find(|entity| entity.anilist_id == expected_anilist_id)
        .map(|entity| entity.index)
        .with_context(|| {
            format!(
                "positive training example {example_id} lacks expected entity {expected_anilist_id}"
            )
        })?;
    let matches = semantic
        .hypotheses
        .iter()
        .filter(|hypothesis| {
            hypothesis.entity_index == entity_index
                && hypothesis.media_kind == expected_media_kind
                && hypothesis.numbering == AnimeSemanticNumbering::EntityOnly
        })
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        return Ok(matches[0].index);
    }
    bail!(
        "positive training example {example_id} has no unique {:?} hypothesis for entity {}",
        expected_media_kind,
        expected_anilist_id
    )
}

fn validate_compiled_example(example: &CompiledTrainingExample) -> Result<()> {
    ensure!(
        example.schema_version == DATASET_SCHEMA_VERSION,
        "compiled example {} has an unsupported schema",
        example.example_id
    );
    ensure!(
        example.request_fingerprint.starts_with("sha256:"),
        "compiled example {} lacks request fingerprint",
        example.example_id
    );
    ensure!(
        example.messages.len() == 3
            && example.messages[0].get("role").and_then(JsonValue::as_str) == Some("system")
            && example.messages[1].get("role").and_then(JsonValue::as_str) == Some("user")
            && example.messages[2].get("role").and_then(JsonValue::as_str) == Some("assistant"),
        "compiled example {} messages are not system/user/assistant",
        example.example_id
    );
    let response_text = example.messages[2]
        .get("content")
        .and_then(JsonValue::as_str)
        .context("compiled assistant message has no text")?;
    let response: AnimeSemanticEvidenceResponse =
        serde_json::from_str(response_text).context("compiled assistant response is invalid")?;
    ensure!(
        response == example.expected_response,
        "compiled assistant response differs from expected response"
    );
    Ok(())
}

fn is_video_path(path: &str) -> bool {
    let lowered = path.to_ascii_lowercase();
    [
        ".mkv", ".mp4", ".m4v", ".avi", ".mov", ".wmv", ".ts", ".m2ts", ".webm",
    ]
    .iter()
    .any(|extension| lowered.ends_with(extension))
}

fn write_json_lines<T: Serialize>(path: &Path, values: &[T]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    for value in values {
        serde_json::to_writer(&mut writer, value)
            .with_context(|| format!("encoding {}", path.display()))?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn read_json_lines<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .enumerate()
        .map(|(index, line)| {
            let line = line.with_context(|| format!("reading {}", path.display()))?;
            serde_json::from_str(&line)
                .with_context(|| format!("decoding {} line {}", path.display(), index + 1))
        })
        .collect()
}

fn write_review_csv(path: &Path, reviews: &[TrainingReviewEntry]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(
        b"review_id,source_record_id,release_title,file_names,reason_codes,evidence,proposed_target,owner_decision,owner_corrected_target,owner_notes\n",
    )?;
    for review in reviews {
        let row = [
            review.review_id.clone(),
            review.source_record_id.clone(),
            review.release_title.clone(),
            review.file_names.join(" | "),
            review.reason_codes.join(" | "),
            serde_json::to_string(&review.evidence)?,
            serde_json::to_string(&review.proposed_target)?,
            review.owner_decision.clone().unwrap_or_default(),
            review
                .owner_corrected_target
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?
                .unwrap_or_default(),
            review.owner_notes.clone().unwrap_or_default(),
        ]
        .into_iter()
        .map(|value| csv_cell(&value))
        .collect::<Vec<_>>()
        .join(",");
        writer.write_all(row.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn csv_cell(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn write_canonical_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))
}

fn file_identity(root: &Path, path: &Path, records: usize) -> Result<DatasetFileIdentity> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let relative = path
        .strip_prefix(root)
        .context("training artifact is outside output root")?
        .to_string_lossy()
        .replace('\\', "/");
    Ok(DatasetFileIdentity {
        path: relative,
        sha256: format!("sha256:{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
        records,
    })
}

fn json_fingerprint(value: &JsonValue) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_cells_preserve_quotes_and_commas() {
        assert_eq!(csv_cell("a,\"b\""), "\"a,\"\"b\"\"\"");
    }

    #[test]
    fn video_paths_are_bounded_to_supported_media() {
        assert!(is_video_path("Show/Episode 01.MKV"));
        assert!(!is_video_path("Show/subtitles.ass"));
    }
}
