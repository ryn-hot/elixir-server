//! Native, release-only model contract smoke producer for ALM-9.
//!
//! The producer runs the exact packaged worker through [`LocalModelEngine`],
//! compares its tokenizer and embedded chat template with frozen outputs from
//! the immutable Qwen source revision, and sends frozen production-shaped
//! requests through the same constrained completion path used by the server.
//! It creates a report only after every check succeeds.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use chrono::{SecondsFormat, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::playback::hardware::collect_host_hardware_inventory;

use super::certification::{read_strict_json, runtime_id, verify_artifact};
use super::{
    ANIME_MATCH_PROMPT_REVISION, AnimeArtifactUrlPolicy, AnimeBundleCompatibilityPolicy,
    AnimeBundleQualificationGate, AnimeExecutionBackend, AnimeInferenceBundleManifest,
    AnimeKvCacheType, AnimeMatchRequest, AnimeMatchSourceMap, AnimeRuntimeArtifactManifest,
    AnimeRuntimeBackend, AnimeRuntimeProbeResult, AnimeRuntimeProfile, LocalModelEngine,
    LocalModelRuntimeProfile, LocalModelSamplingProfile, PreparedAnimeMatchRequest,
    collect_inference_hardware_inventory, extract_anime_runtime_for_qualification,
    inference_hardware_fingerprint, validate_anime_bundle, validate_anime_match_request,
    validate_anime_match_response,
};

const SMOKE_SCHEMA_VERSION: u32 = 1;
const CONTRACT_BYTES: &str = include_str!("fixtures/model-smoke-contract-v1.json");
const REQUEST_CORPUS_BYTES: &[u8] = include_bytes!("fixtures/hardware-certification-requests.json");
const FROZEN_QUALIFICATION_STATUS: &str = "frozen-qualification-inputs";
const EXPECTED_QUANTIZATION: &str = "Q4_K_M";
const EXPECTED_GGUF_ARCHITECTURE: &str = "qwen3";
const EXPECTED_GGUF_FILE_TYPE: u64 = 15;
const EXPECTED_GGUF_BLOCK_COUNT: u64 = 36;
const MAX_JSON_BYTES: u64 = 64 * 1024 * 1024;
const MAX_GGUF_METADATA_ENTRIES: u64 = 1_000_000;
const MAX_GGUF_ARRAY_ITEMS: u64 = 5_000_000;
const MAX_GGUF_STRING_BYTES: u64 = 64 * 1024 * 1024;
const FILE_HASH_BUFFER_BYTES: usize = 1024 * 1024;

const REQUIRED_SMOKES: [&str; 7] = [
    "gguf_metadata",
    "model_load",
    "tokenizer_equivalence",
    "chat_template_equivalence",
    "worker_protocol",
    "strict_json_schema",
    "request_reference_integrity",
];

#[derive(Debug, Clone)]
pub struct AnimeInferenceModelSmokeConfig {
    pub runtime_id: String,
    pub manifest_path: PathBuf,
    pub runtime_profile_path: PathBuf,
    pub model_path: PathBuf,
    pub runtime_artifact_path: PathBuf,
    pub model_build_report_path: PathBuf,
    pub model_source_lock_path: PathBuf,
    pub qualification_lock_path: PathBuf,
    pub request_corpus_path: PathBuf,
    pub producer_commit: String,
    pub producer_run_id: String,
    pub output_path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FrozenSmokeContract {
    schema_version: u32,
    status: String,
    model_id: String,
    model_revision: String,
    source_tokenizer_sha256: String,
    source_template_sha256: String,
    tokenizer_cases: Vec<FrozenTokenizerCase>,
    chat_template_cases: Vec<FrozenTemplateCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FrozenTokenizerCase {
    text: String,
    expected_token_ids: Vec<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FrozenTemplateCase {
    messages: JsonValue,
    expected_prompt: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SmokeRequestCorpus {
    schema_version: u32,
    status: String,
    requests: Vec<AnimeMatchRequest>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelSmokeReport {
    schema_version: u32,
    status: &'static str,
    model: ArtifactEvidence,
    model_build_report: BuildReportEvidence,
    model_source_lock_fingerprint: String,
    runtime: RuntimeEvidence,
    checks: BTreeMap<String, SmokeCheck>,
    producer: ProducerEvidence,
    completed_at: String,
    skipped_checks: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactEvidence {
    sha256: String,
    size_bytes: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildReportEvidence {
    sha256: String,
    size_bytes: u64,
    report_fingerprint: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeEvidence {
    id: String,
    artifact_sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SmokeCheck {
    status: &'static str,
    evidence_sha256: String,
    detail: JsonValue,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProducerEvidence {
    run_id: String,
    commit: String,
}

struct PreparedSmokeRuntime {
    engine: LocalModelEngine,
    profile: AnimeRuntimeProfile,
    runtime: AnimeRuntimeArtifactManifest,
    _extraction: TempDir,
}

struct ModelBuildIdentity {
    artifact_sha256: String,
    artifact_size: u64,
    report_fingerprint: String,
}

#[derive(Debug, PartialEq, Eq)]
struct SourceLockIdentity {
    fingerprint: String,
    architecture: String,
    tokenizer_sha256: String,
    template_sha256: String,
}

#[derive(Debug, Clone)]
struct GgufIdentity {
    architecture: String,
    file_type: u64,
    block_count: u64,
    nextn_predict_layers_present: bool,
}

/// Run every mandatory smoke and create a gate-compatible report. Any error
/// leaves `output_path` absent.
pub async fn run_anime_inference_model_smoke(config: AnimeInferenceModelSmokeConfig) -> Result<()> {
    validate_config(&config)?;
    ensure_output_absent(&config.output_path)?;

    let manifest_document: AnimeInferenceBundleManifest =
        read_strict_json(&config.manifest_path, "candidate manifest")?;
    let manifest_input = hash_regular_file(&config.manifest_path, "candidate manifest")?;
    let profile_input = hash_regular_file(&config.runtime_profile_path, "runtime profile")?;
    let source_lock_input = hash_regular_file(&config.model_source_lock_path, "model source lock")?;
    let qualification_lock_input =
        hash_regular_file(&config.qualification_lock_path, "qualification input lock")?;
    let corpus_input = hash_regular_file(&config.request_corpus_path, "smoke request corpus")?;
    let policy = AnimeBundleCompatibilityPolicy {
        server_version: Version::parse(env!("CARGO_PKG_VERSION"))?,
        qualification_gate: AnimeBundleQualificationGate::DevelopmentAllowUnqualified,
        artifact_url_policy: AnimeArtifactUrlPolicy::HttpsOnly,
        require_complete_platform_matrix: true,
    };
    let validated = validate_anime_bundle(manifest_document.clone(), &policy)
        .context("validating strict candidate manifest")?;
    let manifest = validated.manifest();

    let qualification_lock: JsonValue =
        read_strict_json(&config.qualification_lock_path, "qualification input lock")?;
    validate_qualification_runtime(&qualification_lock, &config.runtime_id)?;

    let source_lock: JsonValue =
        read_strict_json(&config.model_source_lock_path, "model source lock")?;
    let contract: FrozenSmokeContract =
        serde_json::from_str(CONTRACT_BYTES).context("decoding compiled model smoke contract")?;
    let source_identity = validate_source_lock(&source_lock, &contract)?;

    let corpus_bytes = read_regular_bytes(&config.request_corpus_path, "smoke request corpus")?;
    ensure!(
        corpus_bytes == REQUEST_CORPUS_BYTES,
        "smoke request corpus differs byte-for-byte from the compiled frozen corpus"
    );
    let corpus: SmokeRequestCorpus =
        read_strict_json(&config.request_corpus_path, "smoke request corpus")?;
    validate_request_corpus(&corpus)?;

    let model_hash = hash_regular_file(&config.model_path, "candidate model")?;
    ensure!(
        sha256_equal(&model_hash.0, &manifest.model.sha256)
            && model_hash.1 == manifest.model.size_bytes,
        "candidate model differs from manifest"
    );
    let build_identity = validate_model_build_report(
        &config.model_build_report_path,
        &source_identity,
        manifest,
        &model_hash,
    )?;
    let gguf = read_gguf_identity(&config.model_path)?;
    validate_release_gguf_identity(&gguf)?;
    ensure!(
        manifest.model.quantization == EXPECTED_QUANTIZATION,
        "manifest quantization differs from the model smoke contract"
    );

    let prepared = prepare_runtime(&config, manifest).await?;
    let tokenizer_inputs = contract
        .tokenizer_cases
        .iter()
        .map(|case| case.text.clone())
        .collect::<Vec<_>>();
    let template_messages = contract
        .chat_template_cases
        .iter()
        .map(|case| case.messages.clone())
        .collect::<Vec<_>>();

    let smoke_result = async {
        prepared
            .engine
            .prime()
            .await
            .context("loading and priming exact candidate model")?;
        let contract_measurement = prepared
            .engine
            .contract_smoke(&tokenizer_inputs, &template_messages)
            .await
            .context("running tokenizer and chat-template smokes")?;
        ensure!(
            contract_measurement.tokenizations.len() == contract.tokenizer_cases.len()
                && contract_measurement
                    .tokenizations
                    .iter()
                    .zip(&contract.tokenizer_cases)
                    .all(|(actual, expected)| actual == &expected.expected_token_ids),
            "GGUF tokenizer differs from the frozen official-source cases"
        );
        ensure!(
            contract_measurement.rendered_templates.len() == contract.chat_template_cases.len()
                && contract_measurement
                    .rendered_templates
                    .iter()
                    .zip(&contract.chat_template_cases)
                    .all(|(actual, expected)| actual == &expected.expected_prompt),
            "GGUF chat template differs from the frozen official-source cases"
        );

        for request in &corpus.requests {
            let prepared_request = prepared_request(request.clone())?;
            let measurement = prepared
                .engine
                .benchmark_match(request.clone())
                .await
                .with_context(|| format!("running request smoke {}", request.request_id))?;
            validate_anime_match_response(&prepared_request, &measurement.output.response)
                .with_context(|| {
                    format!("validating model references for {}", request.request_id)
                })?;
            let provenance = measurement
                .output
                .runtime
                .as_ref()
                .ok_or_else(|| anyhow!("model smoke response omitted runtime provenance"))?;
            ensure!(
                provenance.bundle_version == manifest.bundle_version
                    && provenance.model_id == manifest.model.id
                    && provenance.model_revision == manifest.model.revision
                    && provenance.worker_revision == manifest.worker_revision
                    && provenance.profile_fingerprint == prepared.profile.profile_fingerprint,
                "model smoke response provenance differs from the exact candidate"
            );
        }
        Result::<_>::Ok(contract_measurement)
    }
    .await;
    prepared.engine.shutdown().await;
    let contract_measurement = smoke_result?;

    verify_artifact(
        &config.model_path,
        &manifest.model.sha256,
        manifest.model.size_bytes,
        "candidate model after smoke",
    )?;
    verify_artifact(
        &config.runtime_artifact_path,
        &prepared.runtime.sha256,
        prepared.runtime.size_bytes,
        "candidate runtime after smoke",
    )?;
    let final_inventory =
        collect_inference_hardware_inventory(collect_host_hardware_inventory().await).await;
    ensure!(
        prepared
            .profile
            .host_fingerprint
            .eq_ignore_ascii_case(&inference_hardware_fingerprint(&final_inventory)),
        "hardware or driver identity changed during model smoke"
    );
    verify_unchanged_input(&config.manifest_path, "candidate manifest", &manifest_input)?;
    verify_unchanged_input(
        &config.runtime_profile_path,
        "runtime profile",
        &profile_input,
    )?;
    verify_unchanged_input(
        &config.model_source_lock_path,
        "model source lock",
        &source_lock_input,
    )?;
    verify_unchanged_input(
        &config.qualification_lock_path,
        "qualification input lock",
        &qualification_lock_input,
    )?;
    verify_unchanged_input(
        &config.request_corpus_path,
        "smoke request corpus",
        &corpus_input,
    )?;

    let final_manifest: AnimeInferenceBundleManifest =
        read_strict_json(&config.manifest_path, "candidate manifest after smoke")?;
    ensure!(
        final_manifest == manifest_document,
        "candidate manifest changed during model smoke"
    );
    let final_profile: AnimeRuntimeProfile =
        read_strict_json(&config.runtime_profile_path, "runtime profile after smoke")?;
    final_profile.validate()?;
    ensure!(
        final_profile == prepared.profile,
        "runtime profile changed during model smoke"
    );
    let final_source_lock: JsonValue = read_strict_json(
        &config.model_source_lock_path,
        "model source lock after smoke",
    )?;
    ensure!(
        final_source_lock == source_lock
            && validate_source_lock(&final_source_lock, &contract)? == source_identity,
        "model source lock changed during model smoke"
    );
    let final_qualification_lock: JsonValue = read_strict_json(
        &config.qualification_lock_path,
        "qualification input lock after smoke",
    )?;
    ensure!(
        final_qualification_lock == qualification_lock,
        "qualification input lock changed during model smoke"
    );
    validate_qualification_runtime(&final_qualification_lock, &config.runtime_id)?;
    let final_corpus_bytes = read_regular_bytes(
        &config.request_corpus_path,
        "smoke request corpus after smoke",
    )?;
    ensure!(
        final_corpus_bytes == corpus_bytes && final_corpus_bytes == REQUEST_CORPUS_BYTES,
        "smoke request corpus changed during model smoke"
    );
    let final_corpus: SmokeRequestCorpus = read_strict_json(
        &config.request_corpus_path,
        "smoke request corpus after smoke",
    )?;
    validate_request_corpus(&final_corpus)?;
    ensure!(
        final_corpus == corpus,
        "smoke request corpus semantics changed during model smoke"
    );

    let tokenizer_fingerprint =
        fingerprint_json(&serde_json::to_value(&contract_measurement.tokenizations)?)?;
    let template_fingerprint = fingerprint_json(&serde_json::to_value(
        &contract_measurement.rendered_templates,
    )?)?;
    let runtime_hash = normalize_sha256(&prepared.runtime.sha256, "runtime SHA-256")?;
    let request_count = u64::try_from(corpus.requests.len())?;
    let mut checks = BTreeMap::new();
    add_check(
        &mut checks,
        "gguf_metadata",
        json!({
            "format": "gguf",
            "quantization": manifest.model.quantization,
            "architecture": gguf.architecture,
            "sourceArchitecture": source_identity.architecture,
            "blockCount": gguf.block_count,
            "nextnPredictLayersPresent": gguf.nextn_predict_layers_present,
        }),
    )?;
    add_check(
        &mut checks,
        "model_load",
        json!({
            "loaded": true,
            "modelSha256": model_hash.0,
            "runtimeId": config.runtime_id,
            "runtimeArtifactSha256": runtime_hash,
        }),
    )?;
    add_check(
        &mut checks,
        "tokenizer_equivalence",
        json!({
            "sourceTokenizerSha256": source_identity.tokenizer_sha256,
            "ggufTokenizerFingerprint": tokenizer_fingerprint,
            "caseCount": contract.tokenizer_cases.len(),
            "mismatches": 0,
        }),
    )?;
    add_check(
        &mut checks,
        "chat_template_equivalence",
        json!({
            "sourceTemplateSha256": source_identity.template_sha256,
            "ggufTemplateFingerprint": template_fingerprint,
            "caseCount": contract.chat_template_cases.len(),
            "mismatches": 0,
        }),
    )?;
    add_check(
        &mut checks,
        "worker_protocol",
        json!({
            "protocolVersion": manifest.protocol_version,
            "caseCount": request_count,
            "failures": 0,
        }),
    )?;
    add_check(
        &mut checks,
        "strict_json_schema",
        json!({
            "schemaVersion": manifest.matcher_schema_version,
            "caseCount": request_count,
            "failures": 0,
        }),
    )?;
    add_check(
        &mut checks,
        "request_reference_integrity",
        json!({
            "matcherSchemaVersion": manifest.matcher_schema_version,
            "caseCount": request_count,
            "failures": 0,
        }),
    )?;
    ensure!(
        checks.keys().map(String::as_str).collect::<BTreeSet<_>>()
            == REQUIRED_SMOKES.into_iter().collect(),
        "model smoke check closure is incomplete"
    );

    let report_hash = hash_regular_file(
        &config.model_build_report_path,
        "model build report after smoke",
    )?;
    ensure!(
        report_hash.0 == build_identity.artifact_sha256
            && report_hash.1 == build_identity.artifact_size,
        "model build report changed during smoke"
    );
    let report = ModelSmokeReport {
        schema_version: SMOKE_SCHEMA_VERSION,
        status: "passed",
        model: ArtifactEvidence {
            sha256: model_hash.0,
            size_bytes: model_hash.1,
        },
        model_build_report: BuildReportEvidence {
            sha256: report_hash.0,
            size_bytes: report_hash.1,
            report_fingerprint: build_identity.report_fingerprint,
        },
        model_source_lock_fingerprint: source_identity.fingerprint,
        runtime: RuntimeEvidence {
            id: config.runtime_id,
            artifact_sha256: runtime_hash,
        },
        checks,
        producer: ProducerEvidence {
            run_id: config.producer_run_id,
            commit: config.producer_commit,
        },
        completed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        skipped_checks: Vec::new(),
    };
    write_new_json(&config.output_path, &report)
}

fn validate_config(config: &AnimeInferenceModelSmokeConfig) -> Result<()> {
    ensure!(
        !config.runtime_id.is_empty()
            && config.runtime_id.len() <= 128
            && config.runtime_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            }),
        "runtime ID is invalid"
    );
    ensure!(
        config.producer_commit.len() == 40
            && config
                .producer_commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "producer commit must be 40 lowercase hexadecimal characters"
    );
    ensure!(
        !config.producer_run_id.is_empty()
            && config.producer_run_id != "0"
            && config
                .producer_run_id
                .bytes()
                .all(|byte| byte.is_ascii_digit()),
        "producer run ID must be a positive decimal value"
    );
    for input in [
        &config.manifest_path,
        &config.runtime_profile_path,
        &config.model_path,
        &config.runtime_artifact_path,
        &config.model_build_report_path,
        &config.model_source_lock_path,
        &config.qualification_lock_path,
        &config.request_corpus_path,
    ] {
        ensure!(
            input != &config.output_path,
            "output path aliases an input path"
        );
    }
    Ok(())
}

fn validate_qualification_runtime(lock: &JsonValue, runtime_id_value: &str) -> Result<()> {
    let root = exact_object(
        lock,
        "qualification lock",
        &[
            "schemaVersion",
            "status",
            "contract",
            "corpus",
            "scorer",
            "crossRuntimeCorrectness",
        ],
    )?;
    ensure!(
        root.get("schemaVersion") == Some(&json!(3))
            && text(root.get("status"), "qualification lock.status")?
                == FROZEN_QUALIFICATION_STATUS,
        "qualification input lock is not frozen schema v3"
    );
    ensure!(
        root.get("corpus").is_some_and(JsonValue::is_object),
        "qualification input lock has no frozen corpus identity"
    );
    let contract = root
        .get("contract")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("qualification lock.contract must be an object"))?;
    ensure!(
        text(
            contract.get("qualificationRuntimeId"),
            "qualification lock runtime ID"
        )? == runtime_id_value,
        "model smoke runtime differs from the frozen qualification runtime"
    );
    Ok(())
}

fn validate_source_lock(
    lock: &JsonValue,
    contract: &FrozenSmokeContract,
) -> Result<SourceLockIdentity> {
    let root = exact_object(
        lock,
        "model source lock",
        &[
            "schemaVersion",
            "status",
            "primaryModel",
            "controlModel",
            "conversion",
        ],
    )?;
    ensure!(
        root.get("schemaVersion") == Some(&json!(1))
            && text(root.get("status"), "model source lock.status")?
                == "immutable-release-input-lock",
        "model source lock is not immutable schema v1"
    );
    ensure!(
        contract.schema_version == 1
            && contract.status == "frozen"
            && (1..=64).contains(&contract.tokenizer_cases.len())
            && (1..=32).contains(&contract.chat_template_cases.len()),
        "compiled model smoke contract is invalid"
    );
    let primary = root
        .get("primaryModel")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("model source lock.primaryModel must be an object"))?;
    let model_id = text(primary.get("id"), "model source lock primary ID")?;
    let model_revision = text(
        primary.get("revision"),
        "model source lock primary revision",
    )?;
    ensure!(
        model_id == contract.model_id && model_revision == contract.model_revision,
        "compiled model smoke contract differs from the immutable model revision"
    );
    let architecture = text(
        primary.get("architecture"),
        "model source lock architecture",
    )?
    .to_string();
    let files = primary
        .get("files")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("model source lock primary files must be an array"))?;
    let tokenizer_sha256 = source_file_sha(files, "tokenizer.json")?;
    // Qwen3-8B stores its immutable chat template inside the
    // official tokenizer_config.json rather than as a separate Jinja file.
    let template_sha256 = source_file_sha(files, "tokenizer_config.json")?;
    ensure!(
        tokenizer_sha256 == contract.source_tokenizer_sha256
            && template_sha256 == contract.source_template_sha256,
        "compiled model smoke cases are not bound to the locked tokenizer sources"
    );
    let conversion = root
        .get("conversion")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("model source lock.conversion must be an object"))?;
    let required = conversion
        .get("requiredSmokes")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("model source lock required smokes must be an array"))?;
    ensure!(
        required
            .iter()
            .map(|value| value.as_str())
            .collect::<Option<Vec<_>>>()
            .is_some_and(|values| values == REQUIRED_SMOKES),
        "model source lock required-smoke closure changed"
    );
    Ok(SourceLockIdentity {
        fingerprint: fingerprint_json_plain(lock)?,
        architecture,
        tokenizer_sha256,
        template_sha256,
    })
}

fn source_file_sha(files: &[JsonValue], expected_path: &str) -> Result<String> {
    let matches = files
        .iter()
        .filter_map(JsonValue::as_object)
        .filter(|file| file.get("path").and_then(JsonValue::as_str) == Some(expected_path))
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "model source lock must contain exactly one {expected_path}"
    );
    let value = text(matches[0].get("sha256"), "model source file SHA-256")?;
    Ok(normalize_sha256(value, "model source file SHA-256")?
        .trim_start_matches("sha256:")
        .to_string())
}

fn validate_model_build_report(
    path: &Path,
    source: &SourceLockIdentity,
    manifest: &AnimeInferenceBundleManifest,
    model_hash: &(String, u64),
) -> Result<ModelBuildIdentity> {
    let document: JsonValue = read_strict_json(path, "model build report")?;
    let root = exact_object(
        &document,
        "model build report",
        &[
            "schemaVersion",
            "status",
            "model",
            "sourceLockFingerprint",
            "sourceFiles",
            "conversion",
            "toolchain",
            "license",
            "smokeStatus",
            "builtAt",
            "reportFingerprint",
        ],
    )?;
    ensure!(
        root.get("schemaVersion") == Some(&json!(1))
            && text(root.get("status"), "model build report.status")?
                == "built-awaiting-qualification"
            && text(root.get("smokeStatus"), "model build report.smokeStatus")?
                == "required-before-qualification",
        "model build report is not an original pre-qualification report"
    );
    ensure!(
        normalize_sha256(
            text(
                root.get("sourceLockFingerprint"),
                "model build source-lock fingerprint"
            )?,
            "model build source-lock fingerprint"
        )?
        .trim_start_matches("sha256:")
            == source.fingerprint,
        "model build report differs from the model source lock"
    );
    let report_fingerprint = normalize_sha256(
        text(
            root.get("reportFingerprint"),
            "model build report fingerprint",
        )?,
        "model build report fingerprint",
    )?
    .trim_start_matches("sha256:")
    .to_string();
    let mut unsigned = document.clone();
    unsigned
        .as_object_mut()
        .expect("validated report is an object")
        .remove("reportFingerprint");
    ensure!(
        fingerprint_json_plain(&unsigned)? == report_fingerprint,
        "model build report self-fingerprint is invalid"
    );
    let built = root
        .get("model")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("model build report.model must be an object"))?;
    ensure!(
        text(built.get("id"), "built model ID")? == manifest.model.upstream_model_id
            && text(built.get("revision"), "built model revision")?
                == manifest.model.upstream_revision
            && text(built.get("quantization"), "built model quantization")?
                == manifest.model.quantization
            && integer(built.get("sizeBytes"), "built model size")? == model_hash.1
            && sha256_equal(
                text(built.get("sha256"), "built model SHA-256")?,
                &model_hash.0
            ),
        "model build report artifact identity differs from candidate model"
    );
    let artifact = hash_regular_file(path, "model build report")?;
    Ok(ModelBuildIdentity {
        artifact_sha256: artifact.0,
        artifact_size: artifact.1,
        report_fingerprint,
    })
}

fn validate_request_corpus(corpus: &SmokeRequestCorpus) -> Result<()> {
    ensure!(
        corpus.schema_version == 1
            && corpus.status == "frozen"
            && (1..=32).contains(&corpus.requests.len()),
        "smoke request corpus is not frozen schema v1"
    );
    for request in &corpus.requests {
        prepared_request(request.clone())?;
    }
    Ok(())
}

fn prepared_request(request: AnimeMatchRequest) -> Result<PreparedAnimeMatchRequest<usize, usize>> {
    let mut candidates = BTreeMap::new();
    let mut files = BTreeMap::new();
    for (candidate_index, candidate) in request.candidates.iter().enumerate() {
        candidates.insert(candidate.candidate_key.clone(), candidate_index);
        for (file_index, file) in candidate.files.iter().enumerate() {
            files.insert(
                file.file_key.clone(),
                (candidate.candidate_key.clone(), file_index),
            );
        }
    }
    let prepared = PreparedAnimeMatchRequest {
        request,
        source_map: AnimeMatchSourceMap::new(candidates, files),
    };
    validate_anime_match_request(&prepared).context("validating smoke request")?;
    Ok(prepared)
}

async fn prepare_runtime(
    config: &AnimeInferenceModelSmokeConfig,
    manifest: &AnimeInferenceBundleManifest,
) -> Result<PreparedSmokeRuntime> {
    let profile: AnimeRuntimeProfile =
        read_strict_json(&config.runtime_profile_path, "runtime profile")?;
    profile
        .validate()
        .context("validating sealed runtime profile")?;
    ensure!(
        profile.probe_result != AnimeRuntimeProbeResult::DeterministicOnly,
        "model smoke requires a model-capable runtime profile"
    );
    ensure!(
        profile.bundle_version == manifest.bundle_version
            && profile.model_id == manifest.model.id
            && profile.model_revision == manifest.model.revision
            && profile.worker_revision == manifest.worker_revision
            && profile.kv_cache_type == manifest.runtime_policy.kv_cache_type,
        "runtime profile and candidate manifest identities differ"
    );
    let runtimes = manifest
        .runtimes
        .iter()
        .filter(|runtime| runtime.artifact_key() == profile.runtime_artifact_key)
        .collect::<Vec<_>>();
    ensure!(
        runtimes.len() == 1 && runtime_id(runtimes[0]) == config.runtime_id,
        "runtime profile does not resolve the frozen qualification runtime"
    );
    let runtime = runtimes[0].clone();
    ensure!(
        runtime_execution_is_qualifying(runtime.backend, profile.execution_backend),
        "model smoke must execute the named runtime's qualifying backend"
    );
    verify_artifact(
        &config.model_path,
        &manifest.model.sha256,
        manifest.model.size_bytes,
        "candidate model",
    )?;
    verify_artifact(
        &config.runtime_artifact_path,
        &runtime.sha256,
        runtime.size_bytes,
        "candidate runtime artifact",
    )?;
    let model_path = absolute_regular_path(&config.model_path, "candidate model")?;
    let runtime_artifact_path =
        absolute_regular_path(&config.runtime_artifact_path, "candidate runtime artifact")?;
    let current_inventory =
        collect_inference_hardware_inventory(collect_host_hardware_inventory().await).await;
    ensure!(
        profile
            .host_fingerprint
            .eq_ignore_ascii_case(&inference_hardware_fingerprint(&current_inventory)),
        "sealed runtime profile belongs to a different host or driver state"
    );
    let extraction = tempfile::Builder::new()
        .prefix("elixir-alm9-model-smoke-")
        .tempdir()?;
    let worker_path = extract_anime_runtime_for_qualification(
        &runtime_artifact_path,
        &extraction.path().join("runtime"),
        &runtime,
    )
    .await?;
    let sampling = LocalModelSamplingProfile::default();
    ensure!(
        sampling.revision == manifest.runtime_policy.sampling_profile_revision,
        "candidate sampling profile is unsupported"
    );
    let local = LocalModelRuntimeProfile {
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        backend: profile.execution_backend.as_str().to_string(),
        profile_fingerprint: profile.profile_fingerprint.clone(),
        protocol_version: manifest.protocol_version,
        matcher_schema_version: manifest.matcher_schema_version,
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
        worker_path,
        model_path,
        context_tokens: manifest.model.context_tokens,
        max_output_tokens: manifest.model.max_output_tokens,
        threads: u32::from(profile.cpu_thread_count),
        batch_threads: u32::from(profile.batch_thread_count),
        gpu_layers: profile.gpu_layer_count,
        kv_cache_type: match profile.kv_cache_type {
            AnimeKvCacheType::F16 => "f16",
            AnimeKvCacheType::Q8_0 => "q8_0",
        }
        .to_string(),
        peak_rss_bytes: profile.peak_rss_bytes,
        idle_unload_seconds: manifest.runtime_policy.idle_unload_seconds,
        sampling,
    };
    local.validate_contract()?;
    let engine = LocalModelEngine::new_for_probe(Arc::new(super::AllowLocalModelAdmission))?;
    engine.activate_profile_for_probe(local).await?;
    Ok(PreparedSmokeRuntime {
        engine,
        profile,
        runtime,
        _extraction: extraction,
    })
}

fn primary_runtime_execution(runtime: AnimeRuntimeBackend) -> AnimeExecutionBackend {
    match runtime {
        AnimeRuntimeBackend::MetalCpu => AnimeExecutionBackend::Metal,
        AnimeRuntimeBackend::CudaCpu => AnimeExecutionBackend::Cuda,
        AnimeRuntimeBackend::HipCpu => AnimeExecutionBackend::Hip,
        AnimeRuntimeBackend::VulkanCpu => AnimeExecutionBackend::Vulkan,
        AnimeRuntimeBackend::Cpu => AnimeExecutionBackend::Cpu,
    }
}

fn runtime_execution_is_qualifying(
    runtime: AnimeRuntimeBackend,
    execution: AnimeExecutionBackend,
) -> bool {
    match runtime {
        // macOS ships one combined runtime slot. Its automatic hardware probe
        // may honestly select Metal or CPU; unlike Windows/Linux there is no
        // separate CPU artifact to certify instead.
        AnimeRuntimeBackend::MetalCpu => {
            matches!(
                execution,
                AnimeExecutionBackend::Metal | AnimeExecutionBackend::Cpu
            )
        }
        _ => primary_runtime_execution(runtime) == execution,
    }
}

fn add_check(
    checks: &mut BTreeMap<String, SmokeCheck>,
    name: &str,
    detail: JsonValue,
) -> Result<()> {
    ensure!(detail.is_object(), "model smoke detail must be an object");
    let check = SmokeCheck {
        status: "passed",
        evidence_sha256: fingerprint_json(&detail)?,
        detail,
    };
    ensure!(
        checks.insert(name.to_string(), check).is_none(),
        "duplicate model smoke check {name}"
    );
    Ok(())
}

fn read_gguf_identity(path: &Path) -> Result<GgufIdentity> {
    let metadata = path.symlink_metadata()?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "candidate model must be a regular non-symlink GGUF"
    );
    let file_length = metadata.len();
    let mut file = File::open(path)?;
    let mut magic = [0_u8; 4];
    file.read_exact(&mut magic)?;
    ensure!(&magic == b"GGUF", "candidate model has invalid GGUF magic");
    let version = read_u32(&mut file)?;
    ensure!(
        matches!(version, 2 | 3),
        "unsupported GGUF version {version}"
    );
    let tensor_count = read_u64(&mut file)?;
    let metadata_count = read_u64(&mut file)?;
    ensure!(
        tensor_count > 0 && metadata_count <= MAX_GGUF_METADATA_ENTRIES,
        "GGUF header counts are invalid"
    );
    let mut architecture = None;
    let mut file_type = None;
    let mut block_count = None;
    let mut nextn_predict_layers_present = false;
    for _ in 0..metadata_count {
        let key = read_gguf_string(&mut file, 4_096)?;
        let value_type = read_u32(&mut file)?;
        match key.as_str() {
            "general.architecture" => {
                ensure!(
                    architecture.is_none() && value_type == 8,
                    "invalid GGUF architecture metadata"
                );
                architecture = Some(read_gguf_string(&mut file, 256)?);
            }
            "general.file_type" => {
                ensure!(file_type.is_none(), "duplicate GGUF file-type metadata");
                file_type = Some(read_gguf_unsigned(&mut file, value_type)?);
            }
            "qwen3.block_count" => {
                ensure!(block_count.is_none(), "duplicate GGUF block-count metadata");
                block_count = Some(read_gguf_unsigned(&mut file, value_type)?);
            }
            "qwen3.nextn_predict_layers" => {
                ensure!(
                    !nextn_predict_layers_present,
                    "duplicate GGUF nextn metadata"
                );
                nextn_predict_layers_present = true;
                skip_gguf_value(&mut file, value_type, file_length, 0)?;
            }
            _ => skip_gguf_value(&mut file, value_type, file_length, 0)?,
        }
    }
    Ok(GgufIdentity {
        architecture: architecture
            .ok_or_else(|| anyhow!("GGUF architecture metadata is missing"))?,
        file_type: file_type.ok_or_else(|| anyhow!("GGUF file-type metadata is missing"))?,
        block_count: block_count
            .ok_or_else(|| anyhow!("GGUF Qwen3.5 block-count metadata is missing"))?,
        nextn_predict_layers_present,
    })
}

fn read_gguf_unsigned(file: &mut File, value_type: u32) -> Result<u64> {
    match value_type {
        0 => Ok(u64::from(read_u8(file)?)),
        2 => Ok(u64::from(read_u16(file)?)),
        4 => Ok(u64::from(read_u32(file)?)),
        10 => read_u64(file),
        _ => bail!("GGUF unsigned metadata has unexpected type {value_type}"),
    }
}

fn validate_release_gguf_identity(identity: &GgufIdentity) -> Result<()> {
    ensure!(
        identity.architecture == EXPECTED_GGUF_ARCHITECTURE
            && identity.file_type == EXPECTED_GGUF_FILE_TYPE
            && identity.block_count == EXPECTED_GGUF_BLOCK_COUNT
            && !identity.nextn_predict_layers_present,
        "GGUF metadata is not the pinned text-only Qwen3-8B Q4_K_M contract"
    );
    Ok(())
}

fn skip_gguf_value(file: &mut File, value_type: u32, file_length: u64, depth: u8) -> Result<()> {
    ensure!(depth <= 2, "GGUF metadata nesting is too deep");
    match value_type {
        0 | 1 | 7 => skip_bytes(file, 1, file_length),
        2 | 3 => skip_bytes(file, 2, file_length),
        4 | 5 | 6 => skip_bytes(file, 4, file_length),
        10 | 11 | 12 => skip_bytes(file, 8, file_length),
        8 => {
            let length = read_u64(file)?;
            ensure!(length <= MAX_GGUF_STRING_BYTES, "GGUF string is too large");
            skip_bytes(file, length, file_length)
        }
        9 => {
            let element_type = read_u32(file)?;
            ensure!(
                element_type != 9,
                "nested GGUF metadata arrays are unsupported"
            );
            let count = read_u64(file)?;
            ensure!(count <= MAX_GGUF_ARRAY_ITEMS, "GGUF array is too large");
            if let Some(width) = gguf_fixed_width(element_type) {
                let bytes = count
                    .checked_mul(width)
                    .ok_or_else(|| anyhow!("GGUF array byte count overflow"))?;
                skip_bytes(file, bytes, file_length)
            } else {
                for _ in 0..count {
                    skip_gguf_value(file, element_type, file_length, depth + 1)?;
                }
                Ok(())
            }
        }
        _ => bail!("unsupported GGUF metadata type {value_type}"),
    }
}

fn gguf_fixed_width(value_type: u32) -> Option<u64> {
    match value_type {
        0 | 1 | 7 => Some(1),
        2 | 3 => Some(2),
        4 | 5 | 6 => Some(4),
        10 | 11 | 12 => Some(8),
        _ => None,
    }
}

fn skip_bytes(file: &mut File, count: u64, file_length: u64) -> Result<()> {
    let current = file.stream_position()?;
    let destination = current
        .checked_add(count)
        .ok_or_else(|| anyhow!("GGUF offset overflow"))?;
    ensure!(destination <= file_length, "GGUF metadata extends past EOF");
    file.seek(SeekFrom::Start(destination))?;
    Ok(())
}

fn read_gguf_string(file: &mut File, maximum: u64) -> Result<String> {
    let length = read_u64(file)?;
    ensure!(length <= maximum, "GGUF metadata string is too large");
    let mut bytes = vec![0; usize::try_from(length)?];
    file.read_exact(&mut bytes)?;
    String::from_utf8(bytes).context("GGUF metadata string is not UTF-8")
}

fn read_u8(file: &mut File) -> Result<u8> {
    let mut bytes = [0; 1];
    file.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn read_u16(file: &mut File) -> Result<u16> {
    let mut bytes = [0; 2];
    file.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(file: &mut File) -> Result<u32> {
    let mut bytes = [0; 4];
    file.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(file: &mut File) -> Result<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn exact_object<'a>(
    value: &'a JsonValue,
    label: &str,
    expected: &[&str],
) -> Result<&'a JsonMap<String, JsonValue>> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("{label} must be an object"))?;
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    ensure!(actual == expected, "{label} has missing or unknown fields");
    Ok(object)
}

fn text<'a>(value: Option<&'a JsonValue>, label: &str) -> Result<&'a str> {
    let value = value
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("{label} must be a string"))?;
    ensure!(
        !value.is_empty() && value == value.trim() && value.len() <= 4_096,
        "{label} is invalid"
    );
    Ok(value)
}

fn integer(value: Option<&JsonValue>, label: &str) -> Result<u64> {
    value
        .and_then(JsonValue::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{label} must be a positive integer"))
}

fn normalize_sha256(value: &str, label: &str) -> Result<String> {
    let plain = value.strip_prefix("sha256:").unwrap_or(value);
    ensure!(
        plain.len() == 64
            && plain
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "{label} must be a complete lowercase SHA-256"
    );
    Ok(format!("sha256:{plain}"))
}

fn sha256_equal(left: &str, right: &str) -> bool {
    match (
        normalize_sha256(left, "SHA-256"),
        normalize_sha256(right, "SHA-256"),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn fingerprint_json(value: &JsonValue) -> Result<String> {
    Ok(format!("sha256:{}", fingerprint_json_plain(value)?))
}

fn fingerprint_json_plain(value: &JsonValue) -> Result<String> {
    // serde_json's map implementation is ordered without `preserve_order`,
    // matching Python's recursively sorted builder fingerprint contract.
    let bytes = serde_json::to_vec(value).context("encoding canonical JSON fingerprint")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn hash_regular_file(path: &Path, label: &str) -> Result<(String, u64)> {
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("reading {label}"))?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && (1..=u64::MAX).contains(&metadata.len()),
        "{label} must be a non-empty regular non-symlink file"
    );
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = file_hash_buffer();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((format!("sha256:{:x}", hasher.finalize()), metadata.len()))
}

fn file_hash_buffer() -> Vec<u8> {
    vec![0_u8; FILE_HASH_BUFFER_BYTES]
}

fn read_regular_bytes(path: &Path, label: &str) -> Result<Vec<u8>> {
    let metadata = path.symlink_metadata()?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && (1..=MAX_JSON_BYTES).contains(&metadata.len()),
        "{label} file shape or size is invalid"
    );
    std::fs::read(path).with_context(|| format!("reading {label}"))
}

fn verify_unchanged_input(path: &Path, label: &str, expected: &(String, u64)) -> Result<()> {
    ensure!(
        &hash_regular_file(path, label)? == expected,
        "{label} bytes changed during model smoke"
    );
    Ok(())
}

fn absolute_regular_path(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = path.symlink_metadata()?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{label} must be a regular non-symlink file"
    );
    path.canonicalize()
        .with_context(|| format!("resolving absolute {label} path"))
}

fn ensure_output_absent(path: &Path) -> Result<()> {
    match path.symlink_metadata() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => bail!("model smoke output already exists"),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspecting model smoke output {}", path.display()));
        }
    }
    ensure!(path.parent().is_some(), "model smoke output has no parent");
    Ok(())
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("model smoke output has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec(value)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    let write_result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(error) = write_result {
        drop(file);
        return match std::fs::remove_file(path) {
            Ok(()) => Err(error).context("writing model smoke report; partial output removed"),
            Err(remove_error) => Err(error).context(format!(
                "writing model smoke report; also failed to remove partial output: {remove_error}"
            )),
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_hash_buffer_is_heap_backed() {
        let buffer = file_hash_buffer();
        assert_eq!(buffer.len(), FILE_HASH_BUFFER_BYTES);
        assert_eq!(
            std::mem::size_of_val(&buffer),
            std::mem::size_of::<Vec<u8>>()
        );
    }

    #[test]
    fn compiled_contract_is_bound_to_the_source_lock() {
        let contract: FrozenSmokeContract = serde_json::from_str(CONTRACT_BYTES).unwrap();
        let lock: JsonValue = serde_json::from_str(include_str!(
            "../../../docs/contracts/anime-inference-model-sources.lock.json"
        ))
        .unwrap();
        let identity = validate_source_lock(&lock, &contract).unwrap();
        assert_eq!(
            identity.tokenizer_sha256,
            "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4"
        );
        assert_eq!(
            identity.template_sha256,
            "d5d09f07b48c3086c508b30d1c9114bd1189145b74e982a265350c923acd8101"
        );
        assert_eq!(contract.tokenizer_cases.len(), 16);
        assert_eq!(contract.chat_template_cases.len(), 4);
    }

    #[test]
    fn evidence_fingerprint_matches_serialized_detail_bytes() {
        let detail = json!({"loaded": true, "runtimeId": "runtime"});
        let mut checks = BTreeMap::new();
        add_check(&mut checks, "model_load", detail.clone()).unwrap();
        assert_eq!(
            checks["model_load"].evidence_sha256,
            format!(
                "sha256:{:x}",
                Sha256::digest(serde_json::to_vec(&detail).unwrap())
            )
        );
    }

    #[test]
    fn combined_macos_runtime_accepts_metal_or_cpu_only() {
        assert!(runtime_execution_is_qualifying(
            AnimeRuntimeBackend::MetalCpu,
            AnimeExecutionBackend::Metal
        ));
        assert!(runtime_execution_is_qualifying(
            AnimeRuntimeBackend::MetalCpu,
            AnimeExecutionBackend::Cpu
        ));
        assert!(!runtime_execution_is_qualifying(
            AnimeRuntimeBackend::MetalCpu,
            AnimeExecutionBackend::Vulkan
        ));
    }

    #[test]
    fn explicit_accelerator_runtime_rejects_cpu_fallback() {
        assert!(runtime_execution_is_qualifying(
            AnimeRuntimeBackend::CudaCpu,
            AnimeExecutionBackend::Cuda
        ));
        assert!(!runtime_execution_is_qualifying(
            AnimeRuntimeBackend::CudaCpu,
            AnimeExecutionBackend::Cpu
        ));
    }

    #[test]
    fn gguf_metadata_reader_rejects_missing_contract_fields() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fixture.gguf");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"GGUF");
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();
        assert!(read_gguf_identity(&path).is_err());
    }

    #[test]
    fn qwen3_release_metadata_requires_36_blocks_and_no_nextn_layers() {
        let accepted = GgufIdentity {
            architecture: "qwen3".into(),
            file_type: 15,
            block_count: 36,
            nextn_predict_layers_present: false,
        };
        validate_release_gguf_identity(&accepted).unwrap();

        for rejected in [
            GgufIdentity {
                architecture: "qwen35".into(),
                ..accepted.clone()
            },
            GgufIdentity {
                file_type: 14,
                ..accepted.clone()
            },
            GgufIdentity {
                block_count: 37,
                ..accepted.clone()
            },
            GgufIdentity {
                nextn_predict_layers_present: true,
                ..accepted.clone()
            },
        ] {
            assert!(validate_release_gguf_identity(&rejected).is_err());
        }
    }

    #[test]
    fn gguf_reader_detects_qwen3_nextn_metadata() {
        fn push_key(bytes: &mut Vec<u8>, key: &str, value_type: u32) {
            bytes.extend_from_slice(&(key.len() as u64).to_le_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(&value_type.to_le_bytes());
        }

        fn fixture(include_nextn: bool) -> Vec<u8> {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"GGUF");
            bytes.extend_from_slice(&3_u32.to_le_bytes());
            bytes.extend_from_slice(&1_u64.to_le_bytes());
            bytes.extend_from_slice(&(if include_nextn { 4_u64 } else { 3_u64 }).to_le_bytes());
            push_key(&mut bytes, "general.architecture", 8);
            bytes.extend_from_slice(&5_u64.to_le_bytes());
            bytes.extend_from_slice(b"qwen3");
            push_key(&mut bytes, "general.file_type", 4);
            bytes.extend_from_slice(&15_u32.to_le_bytes());
            push_key(&mut bytes, "qwen3.block_count", 4);
            bytes.extend_from_slice(&36_u32.to_le_bytes());
            if include_nextn {
                push_key(&mut bytes, "qwen3.nextn_predict_layers", 4);
                bytes.extend_from_slice(&1_u32.to_le_bytes());
            }
            bytes
        }

        let temp = tempfile::tempdir().unwrap();
        let text_only_path = temp.path().join("text-only.gguf");
        std::fs::write(&text_only_path, fixture(false)).unwrap();
        let text_only = read_gguf_identity(&text_only_path).unwrap();
        validate_release_gguf_identity(&text_only).unwrap();

        let nextn_path = temp.path().join("nextn.gguf");
        std::fs::write(&nextn_path, fixture(true)).unwrap();
        let nextn = read_gguf_identity(&nextn_path).unwrap();
        assert!(nextn.nextn_predict_layers_present);
        assert!(validate_release_gguf_identity(&nextn).is_err());
    }

    #[test]
    fn rejects_invalid_producer_identity() {
        let config = AnimeInferenceModelSmokeConfig {
            runtime_id: "macos-x86_64-metal-cpu".into(),
            manifest_path: "manifest".into(),
            runtime_profile_path: "profile".into(),
            model_path: "model".into(),
            runtime_artifact_path: "runtime".into(),
            model_build_report_path: "build".into(),
            model_source_lock_path: "source".into(),
            qualification_lock_path: "qualification".into(),
            request_corpus_path: "requests".into(),
            producer_commit: "not-a-commit".into(),
            producer_run_id: "0".into(),
            output_path: "output".into(),
        };
        assert!(validate_config(&config).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn output_preflight_rejects_broken_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("report.json");
        symlink(temp.path().join("missing-target"), &output).unwrap();
        assert!(ensure_output_absent(&output).is_err());
    }
}
