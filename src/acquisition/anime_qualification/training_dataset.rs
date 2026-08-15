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

use crate::{
    acquisition::{
        anime_matching::acquisition_candidate_parse_facts, automation::anime_semantic_media_kinds,
    },
    anime_matching::{
        ANIME_MATCH_PROMPT_REVISION, ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION, AnimeMatchBatchInput,
        AnimeMatchCandidateInput, AnimeMatchContext, AnimeMatchFileInput, AnimeMatchTarget,
        AnimeMatchingService, AnimeSemanticEvidenceResponse, AnimeSemanticMediaKind,
        AnimeSemanticNumbering, build_semantic_evidence_request,
        semantic_evidence_training_messages,
    },
    http::handlers::acquisition_sources::AcquisitionCandidate,
};

const SOURCE_SCHEMA_VERSION: u32 = 1;
const DATASET_SCHEMA_VERSION: u32 = 1;
const SPLITS: [&str; 3] = ["train", "validation", "holdout"];

#[derive(Debug, Clone)]
pub struct AnimeTrainingCompileConfig {
    pub source_path: PathBuf,
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

#[derive(Debug, Deserialize)]
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
