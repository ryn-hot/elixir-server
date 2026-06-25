#![allow(dead_code)]

use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{process::Command, time::timeout};
use uuid::Uuid;

use crate::{
    media::ffprobe,
    playback::{
        decision::{PlaybackSelection, plan_playback},
        plan::{Delivery, PlaybackMode, StreamAction},
        probe::{MediaCapabilities, normalize_ffprobe_metadata},
        profile::{ClientPlaybackProfile, EffectivePlaybackPolicy},
    },
};

use super::{
    DEFAULT_FPS, HlsOutputLayout, TranscodeParams, build_direct_stream_ffmpeg_args,
    build_transcode_ffmpeg_args, detect_text_subtitles, probe_video_fps,
};

const PLAYBACK_CORPUS_MANIFEST: &str = include_str!("../../../docs/contracts/playback-corpus.yml");
const PUBLIC_CORPUS_LOCK: &str =
    include_str!("../../../docs/contracts/playback-public-corpus.lock.yml");

#[derive(Debug, Deserialize)]
struct PlaybackCorpusManifest {
    schema_version: u32,
    title: String,
    diagnostics: DiagnosticsContract,
    coverage_requirements: CoverageRequirements,
    #[serde(default)]
    generated_cases: Vec<GeneratedCase>,
    #[serde(default)]
    public_corpora: Vec<PublicCorpus>,
    #[serde(default)]
    local_real_media: Vec<LocalRealMediaCase>,
}

#[derive(Debug, Deserialize)]
struct DiagnosticsContract {
    artifact_root_prefix: String,
    retain_temp_dir_env: String,
    real_media_enable_env: String,
    #[serde(default)]
    captures: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CoverageRequirements {
    #[serde(default)]
    containers: Vec<String>,
    #[serde(default)]
    video_codecs: Vec<String>,
    #[serde(default)]
    audio_codecs: Vec<String>,
    #[serde(default)]
    subtitle_codecs: Vec<String>,
    #[serde(default)]
    playback_modes: Vec<String>,
    #[serde(default)]
    negative_cases: Vec<String>,
    #[serde(default)]
    real_media_required: Vec<String>,
    #[serde(default)]
    public_corpora_required: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GeneratedCase {
    id: String,
    title: String,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    optional: bool,
    source: GeneratedSource,
    expected: ExpectedProfiles,
}

#[derive(Debug, Deserialize)]
struct GeneratedSource {
    kind: String,
    container: String,
    video_codec: String,
    audio_codec: String,
    #[serde(default)]
    subtitle_codecs: Vec<String>,
    duration_seconds: f64,
    #[serde(default)]
    required_ffmpeg_encoders: Vec<String>,
    #[serde(default)]
    required_ffmpeg_encoder_alternatives: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ExpectedProfiles {
    #[serde(default)]
    profiles: Vec<ExpectedProfile>,
}

#[derive(Debug, Deserialize, Clone)]
struct ExpectedProfile {
    profile: String,
    #[serde(default = "default_true")]
    playable: bool,
    mode: String,
    delivery: Option<String>,
    selected_subtitle_stream: Option<i32>,
    video_action: Option<String>,
    audio_action: Option<String>,
    subtitle_action: Option<String>,
    #[serde(default)]
    run_hls_output: bool,
    output_video_codec: Option<String>,
    output_audio_codec: Option<String>,
    output_subtitle_codec: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublicCorpus {
    id: String,
    title: String,
    cadence: String,
    license_policy: String,
    #[serde(default)]
    references: Vec<String>,
    #[serde(default)]
    use_: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublicCorpusLock {
    schema_version: u32,
    title: String,
    cache_root: PathBuf,
    enable_env: String,
    suite_filter_env: String,
    download_script: PathBuf,
    #[serde(default)]
    required_sources: Vec<String>,
    #[serde(default)]
    required_type_labels: Vec<String>,
    #[serde(default)]
    samples: Vec<PublicCorpusSample>,
}

#[derive(Debug, Deserialize)]
struct PublicCorpusSample {
    id: String,
    title: String,
    source: String,
    #[serde(rename = "type")]
    sample_type: String,
    #[serde(default)]
    labels: Vec<String>,
    suite: String,
    license_policy: String,
    url: String,
    local_path: PathBuf,
    size_bytes: u64,
    sha256: String,
    expected_probe: PublicExpectedProbe,
    playback: PublicPlaybackExpectation,
}

#[derive(Debug, Deserialize)]
struct PublicExpectedProbe {
    container: String,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    audio_channels: Option<i32>,
    audio_profile: Option<String>,
    #[serde(default)]
    subtitle_codecs: Vec<String>,
    width: Option<u32>,
    height: Option<u32>,
    hdr: String,
    dovi_profile: Option<u32>,
    dovi_compatibility_id: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
struct PublicPlaybackExpectation {
    profile: String,
    #[serde(default = "default_true")]
    playable: bool,
    mode: String,
    delivery: Option<String>,
    video_action: Option<String>,
    audio_action: Option<String>,
    subtitle_action: Option<String>,
    #[serde(default)]
    run_hls_output: bool,
    output_video_codec: Option<String>,
    output_audio_codec: Option<String>,
    output_subtitle_codec: Option<String>,
    output_seconds: Option<f64>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LocalRealMediaCase {
    id: String,
    title: String,
    #[serde(default = "default_true")]
    enabled: bool,
    path: PathBuf,
    smoke: LocalSmokeExpectation,
}

#[derive(Debug, Deserialize)]
struct LocalSmokeExpectation {
    profile: String,
    expected_mode: String,
    seek_seconds: f32,
    output_seconds: f64,
}

#[test]
fn playback_corpus_manifest_is_valid_and_covers_required_axes() -> Result<()> {
    let manifest = load_manifest()?;
    ensure!(
        manifest.schema_version == 1,
        "unexpected playback corpus schema version {}",
        manifest.schema_version
    );
    ensure!(
        manifest.diagnostics.artifact_root_prefix == "elixir-playback-corpus",
        "artifact root prefix must stay stable for cleanup and CI collection"
    );
    ensure!(
        manifest.diagnostics.retain_temp_dir_env == "ELIXIR_KEEP_PLAYBACK_CORPUS_ARTIFACTS",
        "retain artifact env var drifted"
    );
    ensure!(
        manifest.diagnostics.real_media_enable_env == "ELIXIR_PLAYBACK_CORPUS_REAL_MEDIA",
        "real media env var drifted"
    );

    assert_unique(
        "generated case",
        manifest.generated_cases.iter().map(|case| case.id.as_str()),
    )?;
    assert_unique(
        "public corpus",
        manifest.public_corpora.iter().map(|case| case.id.as_str()),
    )?;
    assert_unique(
        "local real media",
        manifest
            .local_real_media
            .iter()
            .map(|case| case.id.as_str()),
    )?;

    assert_required_ids(
        "generated case",
        manifest.generated_cases.iter().map(|case| case.id.as_str()),
        &[
            "generated_h264_aac_mp4",
            "generated_h264_aac_mkv",
            "generated_h264_eac3_mkv",
            "generated_hevc10_aac_mkv",
            "generated_av1_opus_mkv",
            "generated_h264_aac_ass_mkv",
            "generated_corrupt_truncated_mkv",
        ],
    )?;
    assert_required_ids(
        "local real media",
        manifest
            .local_real_media
            .iter()
            .map(|case| case.id.as_str()),
        &manifest.coverage_requirements.real_media_required,
    )?;
    assert_required_ids(
        "public corpus",
        manifest.public_corpora.iter().map(|case| case.id.as_str()),
        &manifest.coverage_requirements.public_corpora_required,
    )?;

    ensure!(
        manifest
            .local_real_media
            .iter()
            .any(|case| case.id == "solo_leveling_s02e02"),
        "Solo Leveling must remain in the local real-media smoke corpus"
    );

    let generated_containers = manifest
        .generated_cases
        .iter()
        .map(|case| case.source.container.as_str())
        .collect::<BTreeSet<_>>();
    assert_required_ids(
        "generated container",
        generated_containers.iter().copied(),
        &manifest.coverage_requirements.containers,
    )?;

    let generated_video_codecs = manifest
        .generated_cases
        .iter()
        .map(|case| case.source.video_codec.as_str())
        .collect::<BTreeSet<_>>();
    assert_required_ids(
        "generated video codec",
        generated_video_codecs.iter().copied(),
        &manifest.coverage_requirements.video_codecs,
    )?;

    let generated_audio_codecs = manifest
        .generated_cases
        .iter()
        .map(|case| case.source.audio_codec.as_str())
        .collect::<BTreeSet<_>>();
    assert_required_ids(
        "generated audio codec",
        generated_audio_codecs.iter().copied(),
        &manifest.coverage_requirements.audio_codecs,
    )?;

    let generated_modes = manifest
        .generated_cases
        .iter()
        .flat_map(|case| case.expected.profiles.iter())
        .filter(|profile| profile.mode != "not_playable")
        .map(|profile| profile.mode.as_str())
        .collect::<BTreeSet<_>>();
    assert_required_ids(
        "generated playback mode",
        generated_modes.iter().copied(),
        &manifest.coverage_requirements.playback_modes,
    )?;

    let generated_subtitles = manifest
        .generated_cases
        .iter()
        .flat_map(|case| case.source.subtitle_codecs.iter().map(String::as_str))
        .chain(
            manifest
                .generated_cases
                .iter()
                .flat_map(|case| case.expected.profiles.iter())
                .filter_map(|profile| profile.output_subtitle_codec.as_deref()),
        )
        .collect::<BTreeSet<_>>();
    assert_required_ids(
        "generated text subtitle codec",
        generated_subtitles.iter().copied(),
        &["ass".to_string(), "webvtt".to_string()],
    )?;
    assert_required_ids(
        "declared subtitle codec",
        manifest
            .coverage_requirements
            .subtitle_codecs
            .iter()
            .map(String::as_str),
        &["pgs".to_string(), "dvd_subtitle".to_string()],
    )?;

    ensure!(
        manifest
            .generated_cases
            .iter()
            .any(|case| case.source.kind == "corrupt"
                && manifest
                    .coverage_requirements
                    .negative_cases
                    .iter()
                    .any(|case| case == "corrupt_or_unreadable")),
        "corrupt/unreadable negative coverage is required"
    );
    ensure!(
        manifest
            .diagnostics
            .captures
            .iter()
            .any(|capture| capture == "ffmpeg_command")
            && manifest
                .diagnostics
                .captures
                .iter()
                .any(|capture| capture == "playback_plan_json"),
        "diagnostic contract must capture ffmpeg commands and playback plans"
    );

    Ok(())
}

#[test]
fn playback_public_corpus_lock_is_valid_and_labeled_by_type() -> Result<()> {
    let manifest = load_manifest()?;
    let lock = load_public_corpus_lock()?;
    ensure!(
        lock.schema_version == 1,
        "unexpected public playback corpus lock schema version {}",
        lock.schema_version
    );
    ensure!(
        lock.cache_root == PathBuf::from("data/playback-corpus/public"),
        "public playback corpus cache root drifted: {}",
        lock.cache_root.display()
    );
    ensure!(
        lock.enable_env == "ELIXIR_PLAYBACK_PUBLIC_CORPUS",
        "public playback corpus enable env var drifted"
    );
    ensure!(
        lock.suite_filter_env == "ELIXIR_PLAYBACK_PUBLIC_CORPUS_SUITE",
        "public playback corpus suite filter env var drifted"
    );
    ensure!(
        lock.download_script == PathBuf::from("scripts/download_playback_public_corpus.py"),
        "public playback corpus downloader path drifted"
    );
    ensure!(
        !lock.samples.is_empty(),
        "public playback corpus lock must include pinned samples"
    );

    assert_unique(
        "public sample",
        lock.samples.iter().map(|sample| sample.id.as_str()),
    )?;
    assert_required_ids(
        "public sample source",
        lock.samples.iter().map(|sample| sample.source.as_str()),
        &lock.required_sources,
    )?;
    assert_required_ids(
        "public sample type label",
        lock.samples
            .iter()
            .flat_map(|sample| sample.labels.iter().map(String::as_str)),
        &lock.required_type_labels,
    )?;
    assert_required_ids(
        "manifest public corpus",
        manifest
            .public_corpora
            .iter()
            .map(|corpus| corpus.id.as_str()),
        &[
            "jellyfin_test_videos",
            "dolby_ddp_online_delivery_kit",
            "blender_open_movies",
            "xiph_derf_y4m",
        ],
    )?;

    let xiph_samples = lock
        .samples
        .iter()
        .filter(|sample| sample.source == "xiph")
        .count();
    ensure!(
        xiph_samples >= 8,
        "Xiph corpus should include raw-video robustness coverage beyond a token sample, got {xiph_samples}"
    );
    ensure!(
        lock.samples
            .iter()
            .any(|sample| sample.id == "blender_tearsofsteel_4k_mov" && sample.suite == "torture"),
        "Tears of Steel 4K must be present and labeled as a torture public sample"
    );
    ensure!(
        lock.samples.iter().any(|sample| sample.source == "matroska"
            && sample.labels.iter().any(|label| label == "case:audio-gap")),
        "Matroska conformance corpus must include the audio-gap test case"
    );
    ensure!(
        lock.samples
            .iter()
            .any(|sample| sample.labels.iter().any(|label| label == "type:chroma-444")),
        "public corpus must include 4:4:4 raw-video coverage"
    );
    ensure!(
        lock.samples
            .iter()
            .any(|sample| sample.labels.iter().any(|label| label == "type:interlaced")),
        "public corpus must include interlaced raw-video coverage"
    );

    for sample in &lock.samples {
        ensure!(
            sample.local_path.starts_with(&lock.cache_root),
            "{} local path must stay under {}",
            sample.id,
            lock.cache_root.display()
        );
        ensure!(
            sample.url.starts_with("https://"),
            "{} must use an HTTPS source URL",
            sample.id
        );
        ensure!(
            sample.size_bytes > 0,
            "{} must pin a positive size",
            sample.id
        );
        ensure!(
            is_sha256_hex(&sample.sha256),
            "{} must pin a lowercase SHA-256 hash",
            sample.id
        );
        ensure!(
            sample
                .labels
                .iter()
                .any(|label| label == &format!("source:{}", sample.source)),
            "{} must include its source label",
            sample.id
        );
        ensure!(
            sample.labels.iter().any(|label| label.starts_with("type:")),
            "{} must include a type label",
            sample.id
        );
        ensure!(
            sample
                .labels
                .iter()
                .any(|label| label == &format!("suite:{}", sample.suite)),
            "{} must include its suite label",
            sample.id
        );
        ensure!(
            ["smoke", "heavy", "torture"].contains(&sample.suite.as_str()),
            "{} has unsupported suite {}",
            sample.id,
            sample.suite
        );
        ensure!(
            !sample.title.trim().is_empty()
                && !sample.sample_type.trim().is_empty()
                && !sample.license_policy.trim().is_empty(),
            "{} must include title, type, and license policy",
            sample.id
        );
        let expected = public_expected_profile(sample);
        parse_playback_mode(&expected.mode)
            .with_context(|| format!("{} has invalid playback mode", sample.id))?;
        if let Some(delivery) = expected.delivery.as_deref() {
            parse_delivery(delivery)
                .with_context(|| format!("{} has invalid delivery", sample.id))?;
        }
        for action in [
            expected.video_action.as_deref(),
            expected.audio_action.as_deref(),
            expected.subtitle_action.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            parse_stream_action(action)
                .with_context(|| format!("{} has invalid stream action", sample.id))?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn playback_corpus_generated_media_streaming_outputs_are_stable() -> Result<()> {
    if !tool_available("ffmpeg").await || !tool_available("ffprobe").await {
        eprintln!("skipping playback corpus: ffmpeg or ffprobe is unavailable");
        return Ok(());
    }

    let manifest = load_manifest()?;
    let artifact_root = create_artifact_root("generated")?;
    let result = run_generated_corpus(&manifest, &artifact_root).await;
    if result.is_ok() && !keep_artifacts() {
        let _ = fs::remove_dir_all(&artifact_root);
    } else {
        eprintln!(
            "playback corpus artifacts retained at {}",
            artifact_root.display()
        );
    }
    result
}

#[tokio::test]
#[ignore = "requires private media paths from docs/contracts/playback-corpus.yml"]
async fn playback_corpus_real_user_media_smoke_when_present() -> Result<()> {
    let manifest = load_manifest()?;
    if std::env::var(&manifest.diagnostics.real_media_enable_env).is_err() {
        eprintln!(
            "skipping real media playback corpus: set {}=1 to enable",
            manifest.diagnostics.real_media_enable_env
        );
        return Ok(());
    }
    if !tool_available("ffmpeg").await || !tool_available("ffprobe").await {
        eprintln!("skipping real media playback corpus: ffmpeg or ffprobe is unavailable");
        return Ok(());
    }

    let artifact_root = create_artifact_root("real")?;
    let result = run_real_media_corpus(&manifest, &artifact_root).await;
    if result.is_ok() && !keep_artifacts() {
        let _ = fs::remove_dir_all(&artifact_root);
    } else {
        eprintln!(
            "real media playback corpus artifacts retained at {}",
            artifact_root.display()
        );
    }
    result
}

#[tokio::test]
#[ignore = "requires downloaded public media from docs/contracts/playback-public-corpus.lock.yml"]
async fn playback_public_corpus_cached_samples_probe_and_plan_when_present() -> Result<()> {
    let lock = load_public_corpus_lock()?;
    if std::env::var(&lock.enable_env).is_err() {
        eprintln!(
            "skipping public playback corpus: set {}=1 to enable",
            lock.enable_env
        );
        return Ok(());
    }
    if !tool_available("ffmpeg").await || !tool_available("ffprobe").await {
        eprintln!("skipping public playback corpus: ffmpeg or ffprobe is unavailable");
        return Ok(());
    }

    let artifact_root = create_artifact_root("public")?;
    let result = run_public_media_corpus(&lock, &artifact_root).await;
    if result.is_ok() && !keep_artifacts() {
        let _ = fs::remove_dir_all(&artifact_root);
    } else {
        eprintln!(
            "public playback corpus artifacts retained at {}",
            artifact_root.display()
        );
    }
    result
}

async fn run_generated_corpus(manifest: &PlaybackCorpusManifest, root: &Path) -> Result<()> {
    let encoders = ffmpeg_encoder_inventory().await?;
    let mut executed_cases = 0usize;
    let mut skipped_cases = Vec::new();

    for case in manifest.generated_cases.iter().filter(|case| case.enabled) {
        if missing_required_encoders(case, &encoders).is_some() {
            let missing = missing_required_encoders(case, &encoders).unwrap();
            if case.optional {
                skipped_cases.push(json!({
                    "case": case.id,
                    "reason": "required_encoder_unavailable",
                    "missing": missing,
                }));
                eprintln!(
                    "skipping optional playback corpus case {}: missing encoder {}",
                    case.id, missing
                );
                continue;
            }
            bail!(
                "playback corpus case {} requires unavailable encoder {}",
                case.id,
                missing
            );
        }

        if case.source.kind == "corrupt" {
            run_corrupt_case(case, root).await?;
            executed_cases += 1;
            continue;
        }

        let source_path = generate_media_case(case, root, &encoders).await?;
        let metadata = ffprobe::probe(path_to_str(&source_path)?)
            .await
            .with_context(|| format!("ffprobe source for {}", case.id))?;
        let capabilities =
            normalize_ffprobe_metadata(&metadata, None, Some(source_path.display().to_string()));
        write_json(
            &root.join(&case.id).join("source_probe.json"),
            &metadata.raw_json,
        )?;
        write_json(
            &root.join(&case.id).join("normalized_capabilities.json"),
            &serde_json::to_value(&capabilities)?,
        )?;

        for expected in &case.expected.profiles {
            let plan = validate_expected_profile(&case.id, &capabilities, expected)
                .with_context(|| diagnostic_context(case, expected, root, None, None))?;
            write_json(
                &root
                    .join(&case.id)
                    .join(format!("plan_{}.json", expected.profile)),
                &serde_json::to_value(&plan)?,
            )?;

            if expected.run_hls_output {
                run_hls_output(
                    &case.id,
                    &expected.profile,
                    &source_path,
                    &plan,
                    expected,
                    root,
                    0.0,
                    None,
                )
                .await?;
            }
        }
        executed_cases += 1;
    }

    write_json(
        &root.join("generated_summary.json"),
        &json!({
            "executed_cases": executed_cases,
            "skipped_cases": skipped_cases,
        }),
    )?;
    ensure!(
        executed_cases >= 6,
        "expected at least six generated playback corpus cases to run, ran {executed_cases}"
    );
    Ok(())
}

async fn run_public_media_corpus(lock: &PublicCorpusLock, root: &Path) -> Result<()> {
    let suite_filter = std::env::var(&lock.suite_filter_env)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "all");
    let selected = lock
        .samples
        .iter()
        .filter(|sample| match suite_filter.as_deref() {
            Some(suite) => sample.suite == suite,
            None => true,
        })
        .collect::<Vec<_>>();
    ensure!(
        !selected.is_empty(),
        "public playback corpus suite {:?} selected no samples",
        suite_filter
    );

    let missing = selected
        .iter()
        .filter(|sample| !public_sample_path(sample).exists())
        .map(|sample| format!("{} ({})", sample.id, public_sample_path(sample).display()))
        .collect::<Vec<_>>();
    ensure!(
        missing.is_empty(),
        "public playback corpus files are missing: {missing:?}; run ./{} --suite {}",
        lock.download_script.display(),
        suite_filter.as_deref().unwrap_or("all")
    );

    let mut executed_cases = 0usize;
    let mut hls_cases = 0usize;
    for sample in selected {
        let sample_path = public_sample_path(sample);
        verify_public_sample_cache(sample, &sample_path)?;

        let metadata = ffprobe::probe(path_to_str(&sample_path)?)
            .await
            .with_context(|| format!("ffprobe public media {}", sample.id))?;
        validate_public_probe(sample, &metadata.raw_json)?;

        let capabilities =
            normalize_ffprobe_metadata(&metadata, None, Some(sample_path.display().to_string()));
        let case_dir = root.join(&sample.id);
        fs::create_dir_all(&case_dir)?;
        write_json(&case_dir.join("source_probe.json"), &metadata.raw_json)?;
        write_json(
            &case_dir.join("normalized_capabilities.json"),
            &serde_json::to_value(&capabilities)?,
        )?;

        let expected = public_expected_profile(sample);
        let plan = validate_expected_profile(&sample.id, &capabilities, &expected)
            .with_context(|| diagnostic_context_public(sample, root, None, None))?;
        write_json(
            &case_dir.join(format!("plan_{}.json", expected.profile)),
            &serde_json::to_value(&plan)?,
        )?;

        if expected.run_hls_output {
            ensure!(
                plan.mode.is_hls_producing(),
                "{} requested HLS output but planned {}",
                sample.id,
                plan.mode.as_str()
            );
            run_hls_output(
                &sample.id,
                &expected.profile,
                &sample_path,
                &plan,
                &expected,
                root,
                0.0,
                sample.playback.output_seconds,
            )
            .await?;
            hls_cases += 1;
        }
        executed_cases += 1;
    }

    write_json(
        &root.join("public_summary.json"),
        &json!({
            "executed_cases": executed_cases,
            "hls_cases": hls_cases,
            "suite_filter": suite_filter,
        }),
    )?;
    ensure!(
        executed_cases > 0,
        "public playback corpus did not execute any cached samples"
    );
    ensure!(
        hls_cases > 0,
        "public playback corpus must include at least one HLS output smoke"
    );
    Ok(())
}

fn verify_public_sample_cache(sample: &PublicCorpusSample, path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("read public corpus cache file {}", path.display()))?;
    ensure!(
        metadata.len() == sample.size_bytes,
        "{} size mismatch: expected {}, got {} at {}",
        sample.id,
        sample.size_bytes,
        metadata.len(),
        path.display()
    );
    let actual_sha256 = sha256_hex(path)?;
    ensure!(
        actual_sha256 == sample.sha256,
        "{} SHA-256 mismatch: expected {}, got {} at {}",
        sample.id,
        sample.sha256,
        actual_sha256,
        path.display()
    );
    Ok(())
}

fn validate_public_probe(sample: &PublicCorpusSample, probe: &Value) -> Result<()> {
    let format_name = probe
        .get("format")
        .and_then(|format| format.get("format_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    ensure!(
        public_container_matches(
            &sample.expected_probe.container,
            &sample.local_path,
            format_name
        ),
        "{} container mismatch: expected {}, got {format_name}",
        sample.id,
        sample.expected_probe.container
    );

    if let Some(expected_video_codec) = sample.expected_probe.video_codec.as_deref() {
        let video =
            find_stream_by_codec(probe, "video", expected_video_codec).with_context(|| {
                format!(
                    "{} expected video codec {}",
                    sample.id, expected_video_codec
                )
            })?;
        if let Some(width) = sample.expected_probe.width {
            ensure!(
                stream_u32(video, "width") == Some(width),
                "{} width mismatch: expected {}, got {:?}",
                sample.id,
                width,
                stream_u32(video, "width")
            );
        }
        if let Some(height) = sample.expected_probe.height {
            ensure!(
                stream_u32(video, "height") == Some(height),
                "{} height mismatch: expected {}, got {:?}",
                sample.id,
                height,
                stream_u32(video, "height")
            );
        }
        validate_public_hdr(sample, video)?;
    }

    match sample.expected_probe.audio_codec.as_deref() {
        Some(expected_audio_codec) => {
            let audio =
                find_stream_by_codec(probe, "audio", expected_audio_codec).with_context(|| {
                    format!(
                        "{} expected audio codec {}",
                        sample.id, expected_audio_codec
                    )
                })?;
            if let Some(channels) = sample.expected_probe.audio_channels {
                ensure!(
                    stream_i32(audio, "channels") == Some(channels),
                    "{} audio channel mismatch: expected {}, got {:?}",
                    sample.id,
                    channels,
                    stream_i32(audio, "channels")
                );
            }
            if let Some(profile) = sample.expected_probe.audio_profile.as_deref() {
                ensure!(
                    stream_string(audio, "profile") == Some(profile),
                    "{} audio profile mismatch: expected {}, got {:?}",
                    sample.id,
                    profile,
                    stream_string(audio, "profile")
                );
            }
        }
        None => ensure!(
            stream_codecs(probe, "audio").is_empty(),
            "{} expected no audio streams, got {:?}",
            sample.id,
            stream_codecs(probe, "audio")
        ),
    }

    let subtitle_codecs = stream_codecs(probe, "subtitle");
    for expected_subtitle_codec in &sample.expected_probe.subtitle_codecs {
        ensure!(
            subtitle_codecs
                .iter()
                .any(|codec| codec == expected_subtitle_codec),
            "{} missing expected subtitle codec {}; got {:?}",
            sample.id,
            expected_subtitle_codec,
            subtitle_codecs
        );
    }
    Ok(())
}

fn validate_public_hdr(sample: &PublicCorpusSample, video: &Value) -> Result<()> {
    match sample.expected_probe.hdr.as_str() {
        "none" => Ok(()),
        "hdr10" => {
            ensure!(
                stream_string(video, "color_transfer") == Some("smpte2084")
                    && stream_string(video, "color_primaries") == Some("bt2020"),
                "{} expected HDR10 signaling, got transfer={:?} primaries={:?}",
                sample.id,
                stream_string(video, "color_transfer"),
                stream_string(video, "color_primaries")
            );
            Ok(())
        }
        "dolby_vision" => {
            let dovi = video
                .get("side_data_list")
                .and_then(Value::as_array)
                .and_then(|items| {
                    items.iter().find(|item| {
                        item.get("side_data_type")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == "DOVI configuration record")
                    })
                })
                .with_context(|| format!("{} missing DOVI configuration record", sample.id))?;
            if let Some(expected_profile) = sample.expected_probe.dovi_profile {
                ensure!(
                    value_u32(dovi, "dv_profile") == Some(expected_profile),
                    "{} DOVI profile mismatch: expected {}, got {:?}",
                    sample.id,
                    expected_profile,
                    value_u32(dovi, "dv_profile")
                );
            }
            if let Some(expected_compatibility) = sample.expected_probe.dovi_compatibility_id {
                ensure!(
                    value_u32(dovi, "dv_bl_signal_compatibility_id")
                        == Some(expected_compatibility),
                    "{} DOVI compatibility id mismatch: expected {}, got {:?}",
                    sample.id,
                    expected_compatibility,
                    value_u32(dovi, "dv_bl_signal_compatibility_id")
                );
            }
            Ok(())
        }
        other => bail!("{} has unknown expected HDR marker {other}", sample.id),
    }
}

async fn run_real_media_corpus(manifest: &PlaybackCorpusManifest, root: &Path) -> Result<()> {
    let mut existing_cases = 0usize;
    let mut skipped_cases = Vec::new();

    for case in manifest.local_real_media.iter().filter(|case| case.enabled) {
        if !case.path.exists() {
            skipped_cases.push(json!({
                "case": case.id,
                "path": case.path.display().to_string(),
                "reason": "path_not_present",
            }));
            eprintln!(
                "skipping real media playback corpus case {}: {} is not present",
                case.id,
                case.path.display()
            );
            continue;
        }
        existing_cases += 1;

        let metadata = ffprobe::probe(path_to_str(&case.path)?)
            .await
            .with_context(|| format!("ffprobe real media {}", case.id))?;
        let capabilities =
            normalize_ffprobe_metadata(&metadata, None, Some(case.path.display().to_string()));
        let case_dir = root.join(&case.id);
        fs::create_dir_all(&case_dir)?;
        write_json(&case_dir.join("source_probe.json"), &metadata.raw_json)?;
        write_json(
            &case_dir.join("normalized_capabilities.json"),
            &serde_json::to_value(&capabilities)?,
        )?;

        let expected = ExpectedProfile {
            profile: case.smoke.profile.clone(),
            playable: true,
            mode: case.smoke.expected_mode.clone(),
            delivery: None,
            selected_subtitle_stream: None,
            video_action: None,
            audio_action: None,
            subtitle_action: None,
            run_hls_output: true,
            output_video_codec: None,
            output_audio_codec: None,
            output_subtitle_codec: None,
            reason: None,
        };
        let plan = validate_expected_profile(&case.id, &capabilities, &expected)
            .with_context(|| diagnostic_context_real(case, root, None, None))?;
        write_json(
            &case_dir.join(format!("plan_{}.json", expected.profile)),
            &serde_json::to_value(&plan)?,
        )?;

        if plan.mode.is_hls_producing() {
            run_hls_output(
                &case.id,
                &expected.profile,
                &case.path,
                &plan,
                &expected,
                root,
                case.smoke.seek_seconds,
                Some(case.smoke.output_seconds),
            )
            .await?;
        }
    }

    write_json(
        &root.join("real_media_summary.json"),
        &json!({
            "existing_cases": existing_cases,
            "skipped_cases": skipped_cases,
        }),
    )?;
    ensure!(
        existing_cases > 0,
        "real media corpus was enabled but none of the manifest paths exist"
    );
    Ok(())
}

async fn run_corrupt_case(case: &GeneratedCase, root: &Path) -> Result<()> {
    let case_dir = root.join(&case.id);
    fs::create_dir_all(&case_dir)?;
    let corrupt_path = case_dir.join("corrupt.mkv");
    fs::write(
        &corrupt_path,
        b"not a matroska file\ntruncated playback corpus fixture",
    )?;
    ensure!(
        ffprobe::probe(path_to_str(&corrupt_path)?).await.is_err(),
        "ffprobe unexpectedly accepted corrupt playback corpus fixture"
    );

    let capabilities = MediaCapabilities::probe_failed(
        corrupt_path.display().to_string(),
        "invalid data found when processing input",
    );
    for expected in &case.expected.profiles {
        let plan = validate_expected_profile(&case.id, &capabilities, expected)
            .with_context(|| diagnostic_context(case, expected, root, None, None))?;
        write_json(
            &case_dir.join(format!("plan_{}.json", expected.profile)),
            &serde_json::to_value(&plan)?,
        )?;
    }
    Ok(())
}

async fn run_hls_output(
    case_id: &str,
    profile: &str,
    source_path: &Path,
    plan: &crate::playback::plan::PlaybackPlan,
    expected: &ExpectedProfile,
    root: &Path,
    seek_seconds: f32,
    output_seconds: Option<f64>,
) -> Result<()> {
    let case_dir = root.join(case_id);
    let output_dir = case_dir.join(format!("hls_{profile}"));
    fs::create_dir_all(&output_dir)?;

    let layout = HlsOutputLayout::for_job(&output_dir, plan.mode, plan.delivery);
    let params = TranscodeParams {
        seek_seconds,
        mode: plan.mode,
        delivery: plan.delivery,
    };
    let input = path_to_str(source_path)?;
    let subtitles = if plan.subtitle_action == StreamAction::ConvertTextToWebvtt {
        detect_text_subtitles(input, plan.selected_subtitle_track).await
    } else {
        Vec::new()
    };
    let fps = probe_video_fps(input).await.unwrap_or(DEFAULT_FPS);
    let mut args = if plan.mode == PlaybackMode::DirectStream {
        build_direct_stream_ffmpeg_args(input, &params, Some(plan), &layout)
    } else {
        build_transcode_ffmpeg_args(
            input,
            &params,
            Some(plan),
            &layout,
            &output_dir,
            &subtitles,
            fps,
        )
    };
    if let Some(seconds) = output_seconds {
        insert_output_duration_limit(&mut args, seconds);
    }

    let command_json = json!({
        "tool": "ffmpeg",
        "args": &args,
        "case": case_id,
        "profile": profile,
        "source": source_path.display().to_string(),
        "output_dir": output_dir.display().to_string(),
    });
    write_json(
        &case_dir.join(format!("ffmpeg_command_{profile}.json")),
        &command_json,
    )?;

    let output = run_command_capture("ffmpeg", &args, 120)
        .await
        .with_context(|| diagnostic_context_raw(case_id, profile, root, Some(&args), None))?;
    if !output.status.success() {
        let stderr_tail = tail_lossy(&output.stderr);
        write_json(
            &case_dir.join(format!("ffmpeg_failure_{profile}.json")),
            &json!({
                "case": case_id,
                "profile": profile,
                "status": output.status.code(),
                "stderr_tail": stderr_tail,
                "args": &args,
                "artifacts": list_artifacts(&output_dir),
                "plan": plan,
            }),
        )?;
        bail!(
            "{} {} ffmpeg HLS output failed: {}",
            case_id,
            profile,
            tail_lossy(&output.stderr)
        );
    }

    ensure!(
        layout.master_playlist_path.exists(),
        "{} {} missing master playlist at {}",
        case_id,
        profile,
        layout.master_playlist_path.display()
    );
    let media_playlist = if plan.mode == PlaybackMode::DirectStream {
        output_dir.join("media.m3u8")
    } else {
        output_dir.join("stream_0.m3u8")
    };
    ensure!(
        media_playlist.exists(),
        "{} {} missing media playlist at {}",
        case_id,
        profile,
        media_playlist.display()
    );

    let output_probe = probe_media(&media_playlist)
        .await
        .with_context(|| diagnostic_context_raw(case_id, profile, root, Some(&args), None))?;
    if let Some(expected_codec) = expected.output_video_codec.as_deref() {
        let video = first_stream(&output_probe, "video")?;
        ensure!(
            stream_string(video, "codec_name") == Some(expected_codec),
            "{} {} output video codec mismatch: expected {}, got {:?}",
            case_id,
            profile,
            expected_codec,
            stream_string(video, "codec_name")
        );
        if plan.video_action == StreamAction::Transcode {
            ensure!(
                stream_string(video, "pix_fmt") == Some("yuv420p"),
                "{} {} transcoded video must be browser-safe yuv420p, got {:?}",
                case_id,
                profile,
                stream_string(video, "pix_fmt")
            );
        }
    }
    if let Some(expected_codec) = expected.output_audio_codec.as_deref() {
        let audio = first_stream(&output_probe, "audio")?;
        ensure!(
            stream_string(audio, "codec_name") == Some(expected_codec),
            "{} {} output audio codec mismatch: expected {}, got {:?}",
            case_id,
            profile,
            expected_codec,
            stream_string(audio, "codec_name")
        );
    }
    if expected.output_subtitle_codec.as_deref() == Some("webvtt") {
        let subtitle_playlist = output_dir.join("sub_0.m3u8");
        ensure!(
            subtitle_playlist.exists(),
            "{} {} missing subtitle playlist at {}",
            case_id,
            profile,
            subtitle_playlist.display()
        );
        let vtt_segments = list_artifacts(&output_dir)
            .into_iter()
            .filter(|path| path.starts_with("sub_0_") && path.ends_with(".vtt"))
            .collect::<Vec<_>>();
        ensure!(
            !vtt_segments.is_empty(),
            "{} {} did not produce WebVTT subtitle segments",
            case_id,
            profile
        );
    }

    write_json(
        &case_dir.join(format!("hls_probe_{profile}.json")),
        &json!({
            "case": case_id,
            "profile": profile,
            "output_probe": output_probe,
            "artifacts": list_artifacts(&output_dir),
        }),
    )?;
    Ok(())
}

fn validate_expected_profile(
    case_id: &str,
    capabilities: &MediaCapabilities,
    expected: &ExpectedProfile,
) -> Result<crate::playback::plan::PlaybackPlan> {
    let client = client_profile(&expected.profile)?;
    let policy = corpus_policy();
    let plan = plan_playback(
        format!("playback-corpus-{case_id}-{}", expected.profile),
        capabilities,
        PlaybackSelection {
            audio_stream_index: None,
            subtitle_stream_index: expected.selected_subtitle_stream,
            start_position_seconds: None,
        },
        &client,
        &policy,
    );

    ensure!(
        plan.playable == expected.playable,
        "{} {} playable mismatch: expected {}, got {}; reasons={:?}",
        case_id,
        expected.profile,
        expected.playable,
        plan.playable,
        plan.reasons
    );
    if !expected.playable {
        if let Some(reason) = expected.reason.as_deref() {
            ensure!(
                plan.reasons.iter().any(|value| value == reason),
                "{} {} expected reason {}, got {:?}",
                case_id,
                expected.profile,
                reason,
                plan.reasons
            );
        }
        return Ok(plan);
    }

    let expected_mode = parse_playback_mode(&expected.mode)?;
    ensure!(
        plan.mode == expected_mode,
        "{} {} mode mismatch: expected {}, got {}; reasons={:?}",
        case_id,
        expected.profile,
        expected.mode,
        plan.mode.as_str(),
        plan.reasons
    );
    if let Some(expected_delivery) = expected.delivery.as_deref() {
        let expected_delivery = parse_delivery(expected_delivery)?;
        ensure!(
            plan.delivery == expected_delivery,
            "{} {} delivery mismatch: expected {}, got {}",
            case_id,
            expected.profile,
            expected.delivery.as_deref().unwrap_or_default(),
            plan.delivery.as_str()
        );
    }
    if let Some(expected_action) = expected.video_action.as_deref() {
        ensure!(
            plan.video_action == parse_stream_action(expected_action)?,
            "{} {} video action mismatch: expected {}, got {}",
            case_id,
            expected.profile,
            expected_action,
            stream_action_as_str(plan.video_action)
        );
    }
    if let Some(expected_action) = expected.audio_action.as_deref() {
        ensure!(
            plan.audio_action == parse_stream_action(expected_action)?,
            "{} {} audio action mismatch: expected {}, got {}",
            case_id,
            expected.profile,
            expected_action,
            stream_action_as_str(plan.audio_action)
        );
    }
    if let Some(expected_action) = expected.subtitle_action.as_deref() {
        ensure!(
            plan.subtitle_action == parse_stream_action(expected_action)?,
            "{} {} subtitle action mismatch: expected {}, got {}",
            case_id,
            expected.profile,
            expected_action,
            stream_action_as_str(plan.subtitle_action)
        );
    }
    Ok(plan)
}

async fn generate_media_case(
    case: &GeneratedCase,
    root: &Path,
    encoders: &BTreeSet<String>,
) -> Result<PathBuf> {
    let case_dir = root.join(&case.id);
    fs::create_dir_all(&case_dir)?;
    let output = case_dir.join(format!("source.{}", case.source.container));
    let args = match case.id.as_str() {
        "generated_h264_aac_mp4" => h264_aac_args(&output, "mp4", case.source.duration_seconds),
        "generated_h264_aac_mkv" => {
            h264_aac_args(&output, "matroska", case.source.duration_seconds)
        }
        "generated_h264_eac3_mkv" => h264_eac3_args(&output, case.source.duration_seconds),
        "generated_hevc10_aac_mkv" => hevc10_aac_args(&output, case.source.duration_seconds),
        "generated_av1_opus_mkv" => {
            let encoder = if encoders.contains("libsvtav1") {
                "libsvtav1"
            } else {
                "libaom-av1"
            };
            av1_opus_args(&output, case.source.duration_seconds, encoder)
        }
        "generated_h264_aac_ass_mkv" => {
            let ass_path = case_dir.join("subtitle.ass");
            fs::write(&ass_path, ass_fixture())?;
            h264_aac_ass_args(&output, &ass_path, case.source.duration_seconds)
        }
        other => bail!("no generator implemented for playback corpus case {other}"),
    };

    let generated = run_command_capture("ffmpeg", &args, 120).await?;
    if !generated.status.success() {
        write_json(
            &case_dir.join("generation_failure.json"),
            &json!({
                "case": case.id,
                "args": args,
                "stderr_tail": tail_lossy(&generated.stderr),
            }),
        )?;
        bail!(
            "{} fixture generation failed: {}",
            case.id,
            tail_lossy(&generated.stderr)
        );
    }
    ensure!(
        output.exists(),
        "{} fixture generation did not create {}",
        case.id,
        output.display()
    );
    Ok(output)
}

fn h264_aac_args(output: &Path, format: &str, duration: f64) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size=160x90:rate=24:duration={duration}"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("sine=frequency=1000:sample_rate=48000:duration={duration}"),
        "-c:v".to_string(),
        "libx264".to_string(),
        "-preset".to_string(),
        "ultrafast".to_string(),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
        "-profile:v".to_string(),
        "high".to_string(),
        "-level:v".to_string(),
        "4.1".to_string(),
        "-c:a".to_string(),
        "aac".to_string(),
        "-b:a".to_string(),
        "96k".to_string(),
        "-shortest".to_string(),
        "-f".to_string(),
        format.to_string(),
        output.to_string_lossy().to_string(),
    ]
}

fn h264_eac3_args(output: &Path, duration: f64) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size=160x90:rate=24:duration={duration}"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("anullsrc=channel_layout=5.1:sample_rate=48000:d={duration}"),
        "-c:v".to_string(),
        "libx264".to_string(),
        "-preset".to_string(),
        "ultrafast".to_string(),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
        "-profile:v".to_string(),
        "high".to_string(),
        "-level:v".to_string(),
        "4.1".to_string(),
        "-c:a".to_string(),
        "eac3".to_string(),
        "-b:a".to_string(),
        "256k".to_string(),
        "-shortest".to_string(),
        output.to_string_lossy().to_string(),
    ]
}

fn hevc10_aac_args(output: &Path, duration: f64) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size=160x90:rate=24:duration={duration}"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("sine=frequency=660:sample_rate=48000:duration={duration}"),
        "-c:v".to_string(),
        "libx265".to_string(),
        "-preset".to_string(),
        "ultrafast".to_string(),
        "-x265-params".to_string(),
        "log-level=error:keyint=24:min-keyint=24:scenecut=0".to_string(),
        "-pix_fmt".to_string(),
        "yuv420p10le".to_string(),
        "-c:a".to_string(),
        "aac".to_string(),
        "-b:a".to_string(),
        "96k".to_string(),
        "-shortest".to_string(),
        output.to_string_lossy().to_string(),
    ]
}

fn av1_opus_args(output: &Path, duration: f64, encoder: &str) -> Vec<String> {
    let mut args = vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size=96x54:rate=24:duration={duration}"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("sine=frequency=880:sample_rate=48000:duration={duration}"),
        "-c:v".to_string(),
        encoder.to_string(),
    ];
    if encoder == "libsvtav1" {
        args.extend(
            ["-preset", "13", "-crf", "45"]
                .into_iter()
                .map(str::to_string),
        );
    } else {
        args.extend(
            ["-cpu-used", "8", "-row-mt", "1", "-crf", "45", "-b:v", "0"]
                .into_iter()
                .map(str::to_string),
        );
    }
    args.extend(
        [
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "libopus",
            "-b:a",
            "48k",
            "-shortest",
            &output.to_string_lossy(),
        ]
        .into_iter()
        .map(str::to_string),
    );
    args
}

fn h264_aac_ass_args(output: &Path, ass_path: &Path, duration: f64) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size=160x90:rate=24:duration={duration}"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("sine=frequency=330:sample_rate=48000:duration={duration}"),
        "-i".to_string(),
        ass_path.to_string_lossy().to_string(),
        "-map".to_string(),
        "0:v:0".to_string(),
        "-map".to_string(),
        "1:a:0".to_string(),
        "-map".to_string(),
        "2:0".to_string(),
        "-c:v".to_string(),
        "libx264".to_string(),
        "-preset".to_string(),
        "ultrafast".to_string(),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
        "-profile:v".to_string(),
        "high".to_string(),
        "-level:v".to_string(),
        "4.1".to_string(),
        "-c:a".to_string(),
        "aac".to_string(),
        "-b:a".to_string(),
        "96k".to_string(),
        "-c:s".to_string(),
        "ass".to_string(),
        "-metadata:s:s:0".to_string(),
        "language=eng".to_string(),
        "-shortest".to_string(),
        output.to_string_lossy().to_string(),
    ]
}

fn ass_fixture() -> &'static str {
    r#"[Script Info]
ScriptType: v4.00+
PlayResX: 160
PlayResY: 90

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,12,&H00FFFFFF,&H000000FF,&H00000000,&H64000000,0,0,0,0,100,100,0,0,1,1,0,2,10,10,10,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
Dialogue: 0,0:00:00.10,0:00:00.90,Default,,0,0,0,,Playback corpus subtitle
"#
}

fn insert_output_duration_limit(args: &mut Vec<String>, seconds: f64) {
    if !seconds.is_finite() || seconds <= 0.0 {
        return;
    }
    if let Some(pos) = args
        .windows(2)
        .position(|window| window[0] == "-f" && window[1] == "hls")
    {
        args.splice(pos..pos, ["-t".to_string(), format!("{seconds}")]);
    }
}

async fn probe_media(path: &Path) -> Result<Value> {
    let args = vec![
        "-v".to_string(),
        "error".to_string(),
        "-show_streams".to_string(),
        "-print_format".to_string(),
        "json".to_string(),
        path.to_string_lossy().to_string(),
    ];
    let output = run_command_capture("ffprobe", &args, 30).await?;
    ensure!(
        output.status.success(),
        "ffprobe failed for {}: {}",
        path.display(),
        tail_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

async fn run_command_capture(
    tool: &str,
    args: &[String],
    timeout_seconds: u64,
) -> Result<std::process::Output> {
    let mut command = Command::new(tool);
    command.args(args);
    command.kill_on_drop(true);
    match timeout(Duration::from_secs(timeout_seconds), command.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(err)) => Err(err).with_context(|| format!("failed to spawn {tool}")),
        Err(_) => bail!("{tool} timed out after {timeout_seconds}s: {:?}", args),
    }
}

async fn tool_available(tool: &str) -> bool {
    let args = vec!["-version".to_string()];
    run_command_capture(tool, &args, 10)
        .await
        .map(|output| output.status.success())
        .unwrap_or(false)
}

async fn ffmpeg_encoder_inventory() -> Result<BTreeSet<String>> {
    let args = vec!["-hide_banner".to_string(), "-encoders".to_string()];
    let output = run_command_capture("ffmpeg", &args, 30).await?;
    ensure!(
        output.status.success(),
        "ffmpeg -encoders failed: {}",
        tail_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .filter_map(|line| {
            let mut columns = line.split_whitespace();
            let flags = columns.next()?;
            let name = columns.next()?;
            flags
                .starts_with('V')
                .then_some(name.to_string())
                .or_else(|| flags.starts_with('A').then_some(name.to_string()))
                .or_else(|| flags.starts_with('S').then_some(name.to_string()))
        })
        .collect())
}

fn missing_required_encoders(case: &GeneratedCase, encoders: &BTreeSet<String>) -> Option<String> {
    for encoder in &case.source.required_ffmpeg_encoders {
        if !encoders.contains(encoder) {
            return Some(encoder.clone());
        }
    }
    for alternatives in &case.source.required_ffmpeg_encoder_alternatives {
        if !alternatives
            .iter()
            .any(|encoder| encoders.contains(encoder))
        {
            return Some(alternatives.join("|"));
        }
    }
    None
}

fn client_profile(name: &str) -> Result<ClientPlaybackProfile> {
    match name {
        "native_mpv" => Ok(ClientPlaybackProfile::native_mpv()),
        "browser_like" => Ok(ClientPlaybackProfile::browser_like()),
        other => bail!("unknown playback corpus client profile {other}"),
    }
}

fn corpus_policy() -> EffectivePlaybackPolicy {
    EffectivePlaybackPolicy {
        allow_direct_stream: true,
        allow_audio_transcode: true,
        allow_video_transcode: true,
        allow_adaptive_transcode: true,
        max_bitrate_bps: Some(50_000_000),
        max_resolution: Some("2160p".to_string()),
        hardware_acceleration: "off".to_string(),
        allow_hardware_decode: false,
        allow_hardware_encode: false,
        ..EffectivePlaybackPolicy::default()
    }
}

fn parse_playback_mode(value: &str) -> Result<PlaybackMode> {
    match value {
        "direct_play" => Ok(PlaybackMode::DirectPlay),
        "direct_stream" => Ok(PlaybackMode::DirectStream),
        "audio_transcode" => Ok(PlaybackMode::AudioTranscode),
        "subtitle_transcode" => Ok(PlaybackMode::SubtitleTranscode),
        "video_transcode" => Ok(PlaybackMode::VideoTranscode),
        "adaptive_transcode" => Ok(PlaybackMode::AdaptiveTranscode),
        other => bail!("unknown playback mode {other}"),
    }
}

fn parse_delivery(value: &str) -> Result<Delivery> {
    match value {
        "direct_file" => Ok(Delivery::DirectFile),
        "hls_fmp4" => Ok(Delivery::HlsFmp4),
        "hls_mpegts" => Ok(Delivery::HlsMpegts),
        "hls_adaptive_fmp4" => Ok(Delivery::HlsAdaptiveFmp4),
        "hls_adaptive_mpegts" => Ok(Delivery::HlsAdaptiveMpegts),
        other => bail!("unknown playback delivery {other}"),
    }
}

fn parse_stream_action(value: &str) -> Result<StreamAction> {
    match value {
        "copy" => Ok(StreamAction::Copy),
        "transcode" => Ok(StreamAction::Transcode),
        "drop" => Ok(StreamAction::Drop),
        "burn_in" => Ok(StreamAction::BurnIn),
        "passthrough" => Ok(StreamAction::Passthrough),
        "convert_text_to_webvtt" => Ok(StreamAction::ConvertTextToWebvtt),
        "disabled" => Ok(StreamAction::Disabled),
        other => bail!("unknown stream action {other}"),
    }
}

fn stream_action_as_str(value: StreamAction) -> &'static str {
    match value {
        StreamAction::Copy => "copy",
        StreamAction::Transcode => "transcode",
        StreamAction::Drop => "drop",
        StreamAction::BurnIn => "burn_in",
        StreamAction::Passthrough => "passthrough",
        StreamAction::ConvertTextToWebvtt => "convert_text_to_webvtt",
        StreamAction::Disabled => "disabled",
    }
}

fn first_stream<'a>(probe: &'a Value, codec_type: &str) -> Result<&'a Value> {
    probe
        .get("streams")
        .and_then(Value::as_array)
        .and_then(|streams| {
            streams.iter().find(|stream| {
                stream
                    .get("codec_type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == codec_type)
            })
        })
        .with_context(|| format!("{codec_type} stream missing from {probe}"))
}

fn stream_string<'a>(stream: &'a Value, key: &str) -> Option<&'a str> {
    stream.get(key).and_then(Value::as_str)
}

fn stream_u32(stream: &Value, key: &str) -> Option<u32> {
    value_u32(stream, key)
}

fn stream_i32(stream: &Value, key: &str) -> Option<i32> {
    stream
        .get(key)
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .or_else(|| {
            stream
                .get(key)
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<i32>().ok())
        })
}

fn value_u32(value: &Value, key: &str) -> Option<u32> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| {
            value
                .get(key)
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<u32>().ok())
        })
}

fn stream_codecs(probe: &Value, codec_type: &str) -> Vec<String> {
    probe
        .get("streams")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|stream| stream_string(stream, "codec_type") == Some(codec_type))
        .filter_map(|stream| stream_string(stream, "codec_name").map(str::to_string))
        .collect()
}

fn find_stream_by_codec<'a>(
    probe: &'a Value,
    codec_type: &str,
    codec_name: &str,
) -> Result<&'a Value> {
    probe
        .get("streams")
        .and_then(Value::as_array)
        .and_then(|streams| {
            streams.iter().find(|stream| {
                stream_string(stream, "codec_type") == Some(codec_type)
                    && stream_string(stream, "codec_name") == Some(codec_name)
            })
        })
        .with_context(|| {
            format!(
                "{} stream with codec {} missing from {}",
                codec_type, codec_name, probe
            )
        })
}

fn public_container_matches(expected: &str, path: &Path, format_name: &str) -> bool {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    match expected {
        "mp4" => extension == "mp4" && format_name.contains("mp4"),
        "mov" => extension == "mov" && format_name.contains("mov"),
        "mkv" => extension == "mkv" && format_name.contains("matroska"),
        "m4v" => extension == "m4v" && format_name.contains("mp4"),
        "avi" => extension == "avi" && format_name.contains("avi"),
        "divx" => extension == "divx" && format_name.contains("avi"),
        "webm" => extension == "webm" && format_name.contains("matroska"),
        "ogg" => matches!(extension, "ogv" | "ogg") && format_name.contains("ogg"),
        "y4m" => extension == "y4m" && format_name.contains("yuv4mpegpipe"),
        _ => false,
    }
}

fn public_expected_profile(sample: &PublicCorpusSample) -> ExpectedProfile {
    ExpectedProfile {
        profile: sample.playback.profile.clone(),
        playable: sample.playback.playable,
        mode: sample.playback.mode.clone(),
        delivery: sample.playback.delivery.clone(),
        selected_subtitle_stream: None,
        video_action: sample.playback.video_action.clone(),
        audio_action: sample.playback.audio_action.clone(),
        subtitle_action: sample.playback.subtitle_action.clone(),
        run_hls_output: sample.playback.run_hls_output,
        output_video_codec: sample.playback.output_video_codec.clone(),
        output_audio_codec: sample.playback.output_audio_codec.clone(),
        output_subtitle_codec: sample.playback.output_subtitle_codec.clone(),
        reason: sample.playback.reason.clone(),
    }
}

fn public_sample_path(sample: &PublicCorpusSample) -> PathBuf {
    if sample.local_path.is_absolute() {
        return sample.local_path.clone();
    }
    repo_root().join(&sample.local_path)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

fn sha256_hex(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)
        .with_context(|| format!("open file for SHA-256 hashing: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read file for SHA-256 hashing: {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn load_manifest() -> Result<PlaybackCorpusManifest> {
    serde_yaml::from_str(PLAYBACK_CORPUS_MANIFEST).context("parse playback corpus manifest")
}

fn load_public_corpus_lock() -> Result<PublicCorpusLock> {
    serde_yaml::from_str(PUBLIC_CORPUS_LOCK).context("parse public playback corpus lock")
}

fn default_true() -> bool {
    true
}

fn assert_unique<'a>(label: &str, values: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        ensure!(
            seen.insert(value.to_string()),
            "duplicate {label} id {value}"
        );
    }
    Ok(())
}

fn assert_required_ids<'a, R>(
    label: &str,
    values: impl IntoIterator<Item = &'a str>,
    required: R,
) -> Result<()>
where
    R: IntoIterator,
    R::Item: AsRef<str>,
{
    let values = values.into_iter().collect::<BTreeSet<_>>();
    for required_id in required {
        let required_id = required_id.as_ref();
        ensure!(
            values.contains(required_id),
            "missing required {label} {required_id}; present={values:?}"
        );
    }
    Ok(())
}

fn create_artifact_root(suffix: &str) -> Result<PathBuf> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let root = std::env::temp_dir().join(format!(
        "elixir-playback-corpus-{suffix}-{millis}-{}",
        Uuid::new_v4()
    ));
    fs::create_dir_all(&root)?;
    Ok(root)
}

fn keep_artifacts() -> bool {
    matches!(
        std::env::var("ELIXIR_KEEP_PLAYBACK_CORPUS_ARTIFACTS")
            .ok()
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("path is not valid utf-8: {}", path.display()))
}

fn list_artifacts(dir: &Path) -> Vec<String> {
    let mut artifacts = Vec::new();
    collect_artifacts(dir, dir, &mut artifacts);
    artifacts.sort();
    artifacts
}

fn collect_artifacts(root: &Path, dir: &Path, artifacts: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_artifacts(root, &path, artifacts);
        } else if let Ok(relative) = path.strip_prefix(root) {
            artifacts.push(relative.display().to_string());
        }
    }
}

fn tail_lossy(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let lines = text.lines().rev().take(40).collect::<Vec<_>>();
    lines.into_iter().rev().collect::<Vec<_>>().join("\n")
}

fn diagnostic_context(
    case: &GeneratedCase,
    expected: &ExpectedProfile,
    root: &Path,
    args: Option<&[String]>,
    stderr: Option<&[u8]>,
) -> String {
    diagnostic_context_raw(&case.id, &expected.profile, root, args, stderr)
}

fn diagnostic_context_real(
    case: &LocalRealMediaCase,
    root: &Path,
    args: Option<&[String]>,
    stderr: Option<&[u8]>,
) -> String {
    diagnostic_context_raw(&case.id, &case.smoke.profile, root, args, stderr)
}

fn diagnostic_context_public(
    sample: &PublicCorpusSample,
    root: &Path,
    args: Option<&[String]>,
    stderr: Option<&[u8]>,
) -> String {
    diagnostic_context_raw(&sample.id, &sample.playback.profile, root, args, stderr)
}

fn diagnostic_context_raw(
    case_id: &str,
    profile: &str,
    root: &Path,
    args: Option<&[String]>,
    stderr: Option<&[u8]>,
) -> String {
    json!({
        "case": case_id,
        "profile": profile,
        "artifact_root": root.display().to_string(),
        "ffmpeg_args": args,
        "stderr_tail": stderr.map(tail_lossy),
    })
    .to_string()
}
