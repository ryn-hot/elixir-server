use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use super::{
    ANIME_MATCH_PROMPT_REVISION, AnimeInferenceBundleManifest, AnimeKvCacheType,
    AnimeRuntimeProfile, AnimeSemanticEvidenceEngine, AnimeSemanticEvidenceRequest,
    LocalModelEngine, LocalModelRuntimeProfile, LocalModelSamplingProfile,
    extract_anime_runtime_for_qualification, validate_semantic_evidence_response,
};

const CORPUS_SCHEMA_VERSION: u32 = 1;
const REPORT_SCHEMA_VERSION: u32 = 1;
const EXPECTED_CASE_COUNT: usize = 18;
const MAX_JSON_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct AnimeSemanticSmokeConfig {
    pub corpus_path: PathBuf,
    pub manifest_path: PathBuf,
    pub runtime_profile_path: PathBuf,
    pub model_path: PathBuf,
    pub runtime_artifact_path: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnimeSemanticSmokeSummary {
    pub status: String,
    pub case_count: usize,
    pub passed: usize,
    pub failed: usize,
    pub output_path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SemanticSmokeCorpus {
    schema_version: u32,
    status: String,
    cases: Vec<SemanticSmokeCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SemanticSmokeCase {
    case_id: String,
    request: AnimeSemanticEvidenceRequest,
    expected_hypothesis_index: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SemanticSmokeReport {
    schema_version: u32,
    status: &'static str,
    model_id: String,
    model_revision: String,
    backend: String,
    profile_fingerprint: String,
    cases: Vec<SemanticSmokeObservation>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SemanticSmokeObservation {
    case_id: String,
    expected_hypothesis_index: Option<usize>,
    actual_hypothesis_index: Option<usize>,
    passed: bool,
    latency_ms: u64,
    error: Option<String>,
}

pub async fn run_anime_semantic_smoke(
    config: AnimeSemanticSmokeConfig,
) -> Result<AnimeSemanticSmokeSummary> {
    let corpus: SemanticSmokeCorpus = read_json(&config.corpus_path, "semantic smoke corpus")?;
    validate_corpus(&corpus)?;
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
        .prefix("elixir-anime-semantic-smoke-")
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
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
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
    engine
        .prime()
        .await
        .context("priming semantic smoke worker")?;

    let mut observations = Vec::with_capacity(corpus.cases.len());
    for case in corpus.cases {
        let started = Instant::now();
        let result = engine
            .select_hypothesis_with_provenance(case.request.clone())
            .await;
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let observation = match result {
            Ok(output) => {
                validate_semantic_evidence_response(&case.request, &output.response)
                    .with_context(|| format!("validating semantic output for {}", case.case_id))?;
                let actual = output.response.hypothesis_index;
                SemanticSmokeObservation {
                    case_id: case.case_id,
                    expected_hypothesis_index: case.expected_hypothesis_index,
                    actual_hypothesis_index: actual,
                    passed: actual == case.expected_hypothesis_index,
                    latency_ms,
                    error: None,
                }
            }
            Err(error) => SemanticSmokeObservation {
                case_id: case.case_id,
                expected_hypothesis_index: case.expected_hypothesis_index,
                actual_hypothesis_index: None,
                passed: false,
                latency_ms,
                error: Some(error.to_string().chars().take(1_024).collect()),
            },
        };
        observations.push(observation);
    }
    engine.shutdown().await;
    drop(extraction);

    let passed = observations.iter().filter(|case| case.passed).count();
    let failed = observations.len() - passed;
    let report = SemanticSmokeReport {
        schema_version: REPORT_SCHEMA_VERSION,
        status: if failed == 0 { "passed" } else { "failed" },
        model_id: manifest.model.id,
        model_revision: manifest.model.revision,
        backend: runtime_profile.execution_backend.as_str().to_string(),
        profile_fingerprint: runtime_profile.profile_fingerprint,
        cases: observations,
    };
    write_new_json(&config.output_path, &report)?;
    let summary = AnimeSemanticSmokeSummary {
        status: report.status.to_string(),
        case_count: report.cases.len(),
        passed,
        failed,
        output_path: config.output_path,
    };
    if failed != 0 {
        bail!(
            "semantic smoke failed {failed}/{} cases",
            summary.case_count
        );
    }
    Ok(summary)
}

fn validate_corpus(corpus: &SemanticSmokeCorpus) -> Result<()> {
    ensure!(
        corpus.schema_version == CORPUS_SCHEMA_VERSION,
        "unsupported semantic smoke corpus schema"
    );
    ensure!(
        corpus.status == "frozen-smoke",
        "semantic smoke corpus is not frozen"
    );
    ensure!(
        corpus.cases.len() == EXPECTED_CASE_COUNT,
        "semantic smoke corpus must contain exactly {EXPECTED_CASE_COUNT} cases"
    );
    let mut ids = BTreeSet::new();
    for case in &corpus.cases {
        ensure!(
            !case.case_id.trim().is_empty(),
            "semantic smoke case ID is empty"
        );
        ensure!(
            ids.insert(case.case_id.as_str()),
            "duplicate semantic smoke case ID"
        );
        ensure!(
            case.request.request_id == case.case_id,
            "semantic smoke request ID differs from its case ID"
        );
        ensure!(
            !case.request.entities.is_empty() && !case.request.hypotheses.is_empty(),
            "semantic smoke case has no entities or hypotheses"
        );
        ensure!(
            case.request
                .entities
                .iter()
                .enumerate()
                .all(|(index, entity)| entity.index == index),
            "semantic smoke entity indexes are not contiguous"
        );
        ensure!(
            case.request
                .hypotheses
                .iter()
                .enumerate()
                .all(|(index, hypothesis)| hypothesis.index == index
                    && hypothesis.entity_index < case.request.entities.len()),
            "semantic smoke hypothesis indexes are invalid"
        );
        if let Some(index) = case.expected_hypothesis_index {
            ensure!(
                index < case.request.hypotheses.len(),
                "expected hypothesis is unknown"
            );
        }
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path, label: &str) -> Result<T> {
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("reading {label} metadata at {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{label} is not a regular file"
    );
    ensure!(
        metadata.len() <= MAX_JSON_BYTES,
        "{label} exceeds the size limit"
    );
    serde_json::from_slice(&std::fs::read(path)?).with_context(|| format!("decoding {label}"))
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alm9_semantic_smoke_fixture_is_frozen_and_reference_closed() {
        let corpus: SemanticSmokeCorpus =
            serde_json::from_slice(include_bytes!("fixtures/semantic-selector-smoke-v1.json"))
                .expect("semantic smoke fixture");
        validate_corpus(&corpus).expect("valid semantic smoke fixture");
    }
}
