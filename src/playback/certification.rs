use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{process::Command, time::timeout};

use crate::{
    media::ffprobe,
    playback::{
        decision::{PlaybackSelection, plan_playback},
        hardware::{
            HardwareApi, HardwareCapabilities, HardwareDetectionConfig, HardwarePreference,
            detect_hardware_capabilities, parse_ffmpeg_components, parse_hwaccels,
        },
        plan::{Delivery, PlaybackMode, PlaybackPlan, StreamAction, VideoFrameRateMode},
        probe::{MediaCapabilities, normalize_ffprobe_metadata},
        profile::{ClientPlaybackProfile, EffectivePlaybackPolicy},
    },
};

use super::{
    DEFAULT_FPS, HlsOutputLayout, TranscodeParams, build_direct_stream_ffmpeg_args,
    build_transcode_ffmpeg_args, detect_text_subtitles, probe_video_fps,
};

const PUBLIC_CORPUS_LOCK: &str =
    include_str!("../../../docs/contracts/playback-public-corpus.lock.yml");
const CERTIFICATION_SCHEMA_VERSION: u32 = 1;
const DEFAULT_CASE_TIMEOUT_SECONDS: u64 = 180;
const DEFAULT_OUTPUT_SECONDS: f64 = 6.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationSuite {
    Smoke,
    Robust,
    Torture,
}

impl CertificationSuite {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "smoke" => Ok(Self::Smoke),
            "robust" | "heavy" => Ok(Self::Robust),
            "torture" | "full" => Ok(Self::Torture),
            other => bail!("unsupported certification suite {other:?}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Robust => "robust",
            Self::Torture => "torture",
        }
    }

    fn includes_public(self) -> bool {
        true
    }

    fn includes_robust(self) -> bool {
        matches!(self, Self::Robust | Self::Torture)
    }

    fn includes_full_public_corpus(self) -> bool {
        matches!(self, Self::Torture)
    }
}

#[derive(Debug, Clone)]
pub struct HardwareCertificationConfig {
    pub suite: CertificationSuite,
    pub hardware_preference: HardwarePreference,
    pub hardware_api_label: String,
    pub corpus_root: PathBuf,
    pub artifact_dir: PathBuf,
    pub target_id: String,
    pub require_hardware: bool,
    pub allow_software_fallback_test: bool,
    pub case_timeout_seconds: u64,
    pub output_seconds: f64,
    pub skip_public_if_missing: bool,
}

impl HardwareCertificationConfig {
    pub fn new(
        suite: CertificationSuite,
        hardware_api: impl Into<String>,
        corpus_root: PathBuf,
        artifact_dir: PathBuf,
    ) -> Self {
        let hardware_api_label = hardware_api.into();
        Self {
            suite,
            hardware_preference: HardwarePreference::parse(&hardware_api_label),
            hardware_api_label,
            corpus_root,
            artifact_dir,
            target_id: "local-hardware".to_string(),
            require_hardware: true,
            allow_software_fallback_test: true,
            case_timeout_seconds: DEFAULT_CASE_TIMEOUT_SECONDS,
            output_seconds: DEFAULT_OUTPUT_SECONDS,
            skip_public_if_missing: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationReport {
    pub schema_version: u32,
    pub status: CertificationStatus,
    pub target_id: String,
    pub suite: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub commit_sha: Option<String>,
    pub run_id: Option<String>,
    pub corpus_lock_sha256: String,
    pub os: HostOsReport,
    pub gpu: HostGpuReport,
    pub hardware_api: Option<String>,
    pub requested_hardware_api: String,
    pub require_hardware: bool,
    pub ffmpeg: FfmpegInventoryReport,
    pub hardware_capabilities: HardwareCapabilities,
    pub cases: CaseSummary,
    pub performance: PerformanceSummary,
    pub failure_reasons: Vec<String>,
    pub artifact_digest: Option<String>,
}

impl CertificationReport {
    pub fn passed(&self) -> bool {
        self.status == CertificationStatus::Passed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationStatus {
    Passed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostOsReport {
    pub family: String,
    pub arch: String,
    pub version: Option<String>,
    pub raw: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostGpuReport {
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub device_id: Option<String>,
    pub driver_version: Option<String>,
    pub raw: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FfmpegInventoryReport {
    pub version: Option<String>,
    pub hwaccels: Vec<String>,
    pub encoders: Vec<String>,
    pub decoders: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CaseSummary {
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub hls_cases: usize,
    pub hardware_cases: usize,
    pub case_reports: Vec<CaseReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseReport {
    pub id: String,
    pub title: String,
    pub source_kind: String,
    pub features: Vec<String>,
    pub status: CaseStatus,
    pub hardware_required: bool,
    pub hardware_used: bool,
    pub seek_seconds: f32,
    pub mode: Option<String>,
    pub delivery: Option<String>,
    pub encoder: Option<String>,
    pub decoder: Option<String>,
    pub realtime_factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performance_gate: Option<PerformanceGateReport>,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub artifacts: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceSummary {
    pub min_realtime_factor: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceGateReport {
    pub tier: String,
    pub required_realtime_factor: f64,
    pub actual_realtime_factor: f64,
    pub passed: bool,
}

#[derive(Debug, Deserialize)]
struct PublicCorpusLock {
    cache_root: PathBuf,
    samples: Vec<PublicCorpusSample>,
}

#[derive(Debug, Clone, Deserialize)]
struct PublicCorpusSample {
    id: String,
    title: String,
    source: String,
    #[serde(rename = "type")]
    sample_type: String,
    #[serde(default)]
    labels: Vec<String>,
    suite: String,
    local_path: PathBuf,
    size_bytes: u64,
    sha256: String,
    playback: PublicPlaybackExpectation,
}

#[derive(Debug, Clone, Deserialize)]
struct PublicPlaybackExpectation {
    output_seconds: Option<f64>,
}

struct CertificationRun {
    config: HardwareCertificationConfig,
    report: CertificationReport,
    selected_api: Option<HardwareApi>,
}

struct SourceCase {
    id: String,
    title: String,
    source_kind: String,
    source_path: PathBuf,
    require_hardware: bool,
    selected_subtitle_stream: Option<i32>,
    seek_seconds: f32,
    output_seconds: f64,
    direct_stream_probe: bool,
    features: Vec<String>,
}

#[derive(Debug, Default)]
struct GpuIdentity {
    vendor: Option<String>,
    model: Option<String>,
    device_id: Option<String>,
    driver_version: Option<String>,
}

pub async fn run_hardware_certification(
    config: HardwareCertificationConfig,
) -> Result<CertificationReport> {
    fs::create_dir_all(&config.artifact_dir)
        .with_context(|| format!("create artifact dir {}", config.artifact_dir.display()))?;

    let ffmpeg = collect_ffmpeg_inventory().await;
    let hardware_capabilities = detect_hardware_capabilities(&HardwareDetectionConfig {
        preference: config.hardware_preference,
    })
    .await;
    let selected_api = select_certification_api(&config, &hardware_capabilities);
    let os = collect_os_report().await;
    let gpu = collect_gpu_report().await;

    let mut run = CertificationRun {
        report: CertificationReport {
            schema_version: CERTIFICATION_SCHEMA_VERSION,
            status: CertificationStatus::Failed,
            target_id: config.target_id.clone(),
            suite: config.suite.as_str().to_string(),
            started_at: Utc::now(),
            finished_at: None,
            commit_sha: current_commit_sha(),
            run_id: current_run_id(),
            corpus_lock_sha256: sha256_hex_bytes(PUBLIC_CORPUS_LOCK.as_bytes()),
            os,
            gpu,
            hardware_api: selected_api.map(|api| api.as_str().to_string()),
            requested_hardware_api: config.hardware_api_label.clone(),
            require_hardware: config.require_hardware,
            ffmpeg,
            hardware_capabilities,
            cases: CaseSummary::default(),
            performance: PerformanceSummary::default(),
            failure_reasons: Vec::new(),
            artifact_digest: None,
        },
        config,
        selected_api,
    };
    run.write_host_evidence()
        .context("write host certification evidence")?;

    if run.config.require_hardware && run.selected_api.is_none() {
        run.fail_global("required_hardware_api_unavailable");
    }

    let generated = run.generate_cases().await;
    match generated {
        Ok(cases) => {
            for case in cases {
                let report = run.execute_case(case).await;
                run.record_case(report);
            }
        }
        Err(err) => run.fail_global(format!("generated_case_setup_failed:{err}")),
    }

    if run.config.suite.includes_public() {
        match run.public_cases().await {
            Ok(cases) => {
                for case in cases {
                    let report = run.execute_case(case).await;
                    run.record_case(report);
                }
            }
            Err(err) => run.fail_global(format!("public_case_selection_failed:{err}")),
        }
    }

    if run.config.allow_software_fallback_test {
        let report = run.execute_fallback_case().await;
        run.record_case(report);
    }

    run.finish()?;
    Ok(run.report)
}

impl CertificationRun {
    fn write_host_evidence(&self) -> Result<()> {
        write_host_evidence_files(&self.config.artifact_dir, &self.report)
    }

    fn fail_global(&mut self, reason: impl Into<String>) {
        push_unique(&mut self.report.failure_reasons, reason.into());
    }

    fn record_case(&mut self, case: CaseReport) {
        match case.status {
            CaseStatus::Passed => self.report.cases.passed += 1,
            CaseStatus::Failed => {
                self.report.cases.failed += 1;
                for error in &case.errors {
                    push_unique(
                        &mut self.report.failure_reasons,
                        format!("{}:{error}", case.id),
                    );
                }
            }
            CaseStatus::Skipped => self.report.cases.skipped += 1,
        }
        if case
            .mode
            .as_deref()
            .is_some_and(|mode| mode != "direct_play")
        {
            self.report.cases.hls_cases += 1;
        }
        if case.status == CaseStatus::Passed && case.hardware_used {
            self.report.cases.hardware_cases += 1;
        }
        if let Some(speed) = case.realtime_factor {
            self.report.performance.min_realtime_factor = Some(
                self.report
                    .performance
                    .min_realtime_factor
                    .map(|current| current.min(speed))
                    .unwrap_or(speed),
            );
        }
        self.report.cases.case_reports.push(case);
    }

    fn finish(&mut self) -> Result<()> {
        if self.report.cases.passed == 0 {
            self.fail_global("no_cases_passed");
        }
        if self.config.require_hardware && self.report.cases.hardware_cases == 0 {
            self.fail_global("no_hardware_cases_passed");
        }
        if self.report.cases.failed == 0 && self.report.failure_reasons.is_empty() {
            self.report.status = CertificationStatus::Passed;
        } else {
            self.report.status = CertificationStatus::Failed;
        }
        self.report.finished_at = Some(Utc::now());
        self.report.artifact_digest = Some(artifact_tree_digest(
            &self.config.artifact_dir,
            &self.report,
        )?);
        write_json(
            &self.config.artifact_dir.join("certification.json"),
            &self.report,
        )?;
        Ok(())
    }

    async fn generate_cases(&self) -> Result<Vec<SourceCase>> {
        let generated_dir = self.config.artifact_dir.join("generated");
        fs::create_dir_all(&generated_dir)?;
        let mut cases = Vec::new();

        let h264 = generated_dir.join("generated_h264_aac.mp4");
        generate_h264_aac_fixture(&h264, self.config.output_seconds.max(4.0)).await?;
        cases.push(SourceCase {
            id: "generated_h264_hardware_transcode".to_string(),
            title: "Generated H.264/AAC hardware transcode".to_string(),
            source_kind: "generated".to_string(),
            source_path: h264,
            require_hardware: self.config.require_hardware,
            selected_subtitle_stream: None,
            seek_seconds: 0.0,
            output_seconds: self.config.output_seconds,
            direct_stream_probe: false,
            features: vec!["generated_h264".to_string()],
        });

        if self
            .report
            .ffmpeg
            .encoders
            .iter()
            .any(|encoder| encoder == "libx265")
        {
            let hevc = generated_dir.join("generated_hevc_aac.mp4");
            generate_hevc_aac_fixture(&hevc, self.config.output_seconds.max(4.0)).await?;
            cases.push(SourceCase {
                id: "generated_hevc_hardware_transcode".to_string(),
                title: "Generated HEVC/AAC hardware transcode".to_string(),
                source_kind: "generated".to_string(),
                source_path: hevc,
                require_hardware: self.config.require_hardware,
                selected_subtitle_stream: None,
                seek_seconds: 0.0,
                output_seconds: self.config.output_seconds,
                direct_stream_probe: false,
                features: vec!["generated_hevc".to_string()],
            });
        }

        Ok(cases)
    }

    async fn public_cases(&self) -> Result<Vec<SourceCase>> {
        let lock: PublicCorpusLock = serde_yaml::from_str(PUBLIC_CORPUS_LOCK)
            .context("parse public playback corpus lock")?;
        let samples = selected_public_samples(&lock, self.config.suite);

        let mut missing = Vec::new();
        let mut cases = Vec::new();
        for sample in samples {
            let path = public_sample_path(&self.config.corpus_root, &lock.cache_root, &sample);
            if !path.exists() {
                missing.push(format!("{} ({})", sample.id, path.display()));
                continue;
            }
            verify_public_sample(&sample, &path)?;
            let direct_stream_probe =
                self.config.suite.includes_robust() && sample.id == "jellyfin_sdr_avc_1080p_3m";
            let mut features = sample
                .labels
                .iter()
                .filter_map(|label| {
                    if label.starts_with("type:") || label.starts_with("resolution:") {
                        Some(label.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let source_type_features = features.clone();
            if direct_stream_probe {
                features.push("direct_stream_probe".to_string());
            }
            cases.push(SourceCase {
                id: format!("public_{}", sample.id),
                title: format!("Public {} {}", sample.source, sample.title),
                source_kind: format!("public:{}:{}", sample.source, sample.sample_type),
                source_path: path.clone(),
                require_hardware: self.config.require_hardware,
                selected_subtitle_stream: None,
                seek_seconds: 0.0,
                output_seconds: sample
                    .playback
                    .output_seconds
                    .unwrap_or(self.config.output_seconds),
                direct_stream_probe,
                features,
            });
            if self.config.suite.includes_robust() && sample.id == "jellyfin_sdr_avc_1080p_3m" {
                let duration = probe_duration_seconds(&path).await.with_context(|| {
                    format!("probe robust seek source duration {}", path.display())
                })?;
                for (feature, fraction, seek_seconds) in robust_seek_offsets(
                    duration,
                    sample
                        .playback
                        .output_seconds
                        .unwrap_or(self.config.output_seconds),
                )? {
                    let mut seek_features = source_type_features.clone();
                    seek_features.push(feature.clone());
                    cases.push(SourceCase {
                        id: format!("public_{}_{}", sample.id, feature),
                        title: format!(
                            "Public {} {} seek restart at {:.0}%",
                            sample.source,
                            sample.title,
                            fraction * 100.0
                        ),
                        source_kind: format!(
                            "public:{}:{}:seek",
                            sample.source, sample.sample_type
                        ),
                        source_path: path.clone(),
                        require_hardware: self.config.require_hardware,
                        selected_subtitle_stream: None,
                        seek_seconds,
                        output_seconds: sample
                            .playback
                            .output_seconds
                            .unwrap_or(self.config.output_seconds),
                        direct_stream_probe: false,
                        features: seek_features,
                    });
                }
            }
        }

        if !missing.is_empty() && !self.config.skip_public_if_missing {
            bail!(
                "missing public corpus sample(s): {}; hydrate {} from the playback public corpus lock",
                missing.join(", "),
                self.config.corpus_root.display()
            );
        }
        Ok(cases)
    }

    async fn execute_case(&self, case: SourceCase) -> CaseReport {
        let case_dir = self.config.artifact_dir.join("cases").join(&case.id);
        let mut report = CaseReport {
            id: case.id.clone(),
            title: case.title.clone(),
            source_kind: case.source_kind.clone(),
            features: case.features.clone(),
            status: CaseStatus::Failed,
            hardware_required: case.require_hardware,
            hardware_used: false,
            seek_seconds: case.seek_seconds,
            mode: None,
            delivery: None,
            encoder: None,
            decoder: None,
            realtime_factor: None,
            performance_gate: None,
            errors: Vec::new(),
            warnings: Vec::new(),
            artifacts: Vec::new(),
        };
        if let Err(err) = fs::create_dir_all(&case_dir) {
            report.errors.push(format!("create_case_dir_failed:{err}"));
            return report;
        }

        let result = self.execute_case_inner(&case, &case_dir, &mut report).await;
        match result {
            Ok(()) => {
                report.artifacts = list_artifacts(&case_dir);
                report.status = CaseStatus::Passed;
            }
            Err(err) => {
                let error = format!("{err:#}");
                report.errors.push(error.clone());
                let _ = write_json(
                    &case_dir.join("case_failure.json"),
                    &json!({
                        "case": case.id,
                        "error": error,
                        "report": report,
                    }),
                );
                report.artifacts = list_artifacts(&case_dir);
                report.status = CaseStatus::Failed;
            }
        }
        let _ = write_json(&case_dir.join("case-report.json"), &report);
        report
    }

    async fn execute_case_inner(
        &self,
        case: &SourceCase,
        case_dir: &Path,
        report: &mut CaseReport,
    ) -> Result<()> {
        let source_probe = ffprobe::probe(path_to_str(&case.source_path)?)
            .await
            .with_context(|| format!("ffprobe source {}", case.source_path.display()))?;
        write_json(&case_dir.join("source-probe.json"), &source_probe.raw_json)?;
        let capabilities = normalize_ffprobe_metadata(
            &source_probe,
            None,
            Some(case.source_path.display().to_string()),
        );
        write_json(
            &case_dir.join("normalized-capabilities.json"),
            &capabilities,
        )?;

        let plan = self.plan_for_case(case, &capabilities, false);
        write_json(&case_dir.join("playback-plan.json"), &plan)?;
        ensure!(
            plan.playable,
            "playback plan is not playable: {:?}",
            plan.reasons
        );
        ensure!(
            plan.mode.is_hls_producing(),
            "certification case planned {} instead of HLS-producing mode",
            plan.mode.as_str()
        );

        report.mode = Some(plan.mode.as_str().to_string());
        report.delivery = Some(plan.delivery.as_str().to_string());
        let hardware_required = case.require_hardware && plan_requires_video_hardware(&plan);
        report.hardware_required = hardware_required;
        report.encoder = plan.hardware_acceleration.encoder.clone();
        report.decoder = plan.hardware_acceleration.decoder.clone();
        report.hardware_used = plan.hardware_acceleration.enabled;
        report.warnings = plan.warnings.clone();

        if hardware_required {
            ensure!(
                plan.hardware_acceleration.enabled,
                "required hardware was not selected; reasons={:?} warnings={:?}",
                plan.reasons,
                plan.warnings
            );
            if let Some(selected_api) = self.selected_api {
                ensure!(
                    plan.hardware_acceleration.api.as_deref() == Some(selected_api.as_str()),
                    "hardware API mismatch: expected {}, got {:?}",
                    selected_api.as_str(),
                    plan.hardware_acceleration.api
                );
            }
        }

        let metrics = self
            .run_hls_output(case, case_dir, &plan, "hardware")
            .await
            .context("hardware HLS output failed")?;
        report.realtime_factor = Some(metrics.realtime_factor);
        report.performance_gate = metrics.performance_gate.clone();

        if hardware_required {
            let command = read_json(case_dir.join("ffmpeg-command.json"))?;
            ensure!(
                command_mentions_hardware(&command, &plan),
                "ffmpeg command did not contain selected hardware encoder/decoder"
            );
        }

        if case.direct_stream_probe {
            let direct_plan = self.plan_for_case(case, &capabilities, true);
            write_json(&case_dir.join("direct-stream-plan.json"), &direct_plan)?;
            ensure!(
                direct_plan.mode == PlaybackMode::DirectStream,
                "direct stream probe expected direct_stream, got {} with reasons {:?}",
                direct_plan.mode.as_str(),
                direct_plan.reasons
            );
            ensure!(
                !direct_plan.hardware_acceleration.enabled,
                "direct stream probe must not select hardware acceleration"
            );
            self.run_hls_output(case, case_dir, &direct_plan, "direct_stream")
                .await
                .context("direct stream HLS output failed")?;
        }

        if let Some(gate) = report.performance_gate.as_ref() {
            ensure!(
                gate.passed,
                "hardware transcode missed {} performance gate: {:.2}x realtime < required {:.2}x",
                gate.tier,
                gate.actual_realtime_factor,
                gate.required_realtime_factor
            );
        }

        Ok(())
    }

    fn plan_for_case(
        &self,
        case: &SourceCase,
        capabilities: &MediaCapabilities,
        direct_stream_probe: bool,
    ) -> PlaybackPlan {
        let client = ClientPlaybackProfile::browser_like();
        let policy = certification_effective_policy(
            &client,
            &self.config.hardware_api_label,
            &self.report.hardware_capabilities,
            direct_stream_probe,
        );
        plan_playback(
            format!("hardware-certification-{}", case.id),
            capabilities,
            PlaybackSelection {
                audio_stream_index: None,
                subtitle_stream_index: case.selected_subtitle_stream,
                start_position_seconds: None,
            },
            &client,
            &policy,
        )
    }

    async fn run_hls_output(
        &self,
        case: &SourceCase,
        case_dir: &Path,
        plan: &PlaybackPlan,
        label: &str,
    ) -> Result<CaseMetrics> {
        let output_dir = output_dir_for_label(case_dir, label);
        fs::create_dir_all(&output_dir)?;
        let layout = HlsOutputLayout::for_job(&output_dir, plan.mode, plan.delivery);
        let params = TranscodeParams {
            seek_seconds: case.seek_seconds,
            mode: plan.mode,
            delivery: plan.delivery,
        };
        let input = path_to_str(&case.source_path)?;
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
        insert_output_duration_limit(&mut args, case.output_seconds);
        write_json(
            &artifact_path_for_label(case_dir, label, "ffmpeg-command.json"),
            &json!({
                "tool": "ffmpeg",
                "label": label,
                "args": args,
                "source": case.source_path.display().to_string(),
                "output_dir": output_dir.display().to_string(),
            }),
        )?;

        let before = process_snapshot().await;
        write_json(
            &artifact_path_for_label(case_dir, label, "process-snapshot-before.json"),
            &json!({ "processes": before }),
        )?;
        let started = Instant::now();
        let output = run_command_capture("ffmpeg", &args, self.config.case_timeout_seconds).await?;
        let elapsed = started.elapsed();
        fs::write(
            artifact_path_for_label(case_dir, label, "ffmpeg-stderr.txt"),
            String::from_utf8_lossy(&output.stderr).as_bytes(),
        )?;
        fs::write(
            artifact_path_for_label(case_dir, label, "ffmpeg-stdout.txt"),
            String::from_utf8_lossy(&output.stdout).as_bytes(),
        )?;
        let after = process_snapshot().await;
        write_json(
            &artifact_path_for_label(case_dir, label, "process-snapshot-after.json"),
            &json!({ "processes": after }),
        )?;

        if !output.status.success() {
            bail!(
                "ffmpeg exited with {:?}: {}",
                output.status.code(),
                tail_lossy(&output.stderr)
            );
        }
        ensure!(
            layout.master_playlist_path.exists(),
            "missing master playlist {}",
            layout.master_playlist_path.display()
        );
        let media_playlist = if plan.mode == PlaybackMode::DirectStream {
            output_dir.join("media.m3u8")
        } else {
            output_dir.join("stream_0.m3u8")
        };
        ensure!(
            media_playlist.exists(),
            "missing media playlist {}",
            media_playlist.display()
        );
        ensure_expected_init_segment(plan, &output_dir)?;
        let output_probe = probe_media(&media_playlist).await?;
        write_json(
            &artifact_path_for_label(case_dir, label, "output-probe.json"),
            &output_probe,
        )?;
        validate_output_probe(&output_probe)?;
        if requires_nonblank_frame_validation(case, plan) {
            validate_nonblank_frame(
                &media_playlist,
                &artifact_path_for_label(case_dir, label, "thumbnails"),
            )
            .await?;
        }
        write_json(
            &artifact_path_for_label(case_dir, label, "hls-artifacts.json"),
            &json!({
                "artifacts": list_artifacts(&output_dir),
                "master_playlist": layout.master_playlist_path,
                "media_playlist": media_playlist,
            }),
        )?;
        write_json(
            &artifact_path_for_label(case_dir, label, "temp-usage.json"),
            &json!({
                "bytes": directory_size(&output_dir),
            }),
        )?;

        let elapsed_seconds = elapsed.as_secs_f64().max(0.001);
        let realtime_factor = case.output_seconds / elapsed_seconds;
        let performance_gate =
            performance_gate_for_case(self.config.suite, case, plan, realtime_factor);
        let metrics = CaseMetrics {
            realtime_factor,
            performance_gate,
        };
        write_json(
            &artifact_path_for_label(case_dir, label, "metrics.json"),
            &metrics,
        )?;
        Ok(metrics)
    }

    async fn execute_fallback_case(&self) -> CaseReport {
        let fallback_dir = self
            .config
            .artifact_dir
            .join("cases")
            .join("software_fallback_probe");
        let mut report = CaseReport {
            id: "software_fallback_probe".to_string(),
            title: "Forced hardware failure followed by software fallback".to_string(),
            source_kind: "generated".to_string(),
            status: CaseStatus::Failed,
            features: vec!["software_fallback".to_string()],
            hardware_required: false,
            hardware_used: false,
            seek_seconds: 0.0,
            mode: None,
            delivery: None,
            encoder: Some("libx264".to_string()),
            decoder: None,
            realtime_factor: None,
            performance_gate: None,
            errors: Vec::new(),
            warnings: Vec::new(),
            artifacts: Vec::new(),
        };
        if let Err(err) = self
            .execute_fallback_case_inner(&fallback_dir, &mut report)
            .await
        {
            report.errors.push(format!("{err:#}"));
            report.status = CaseStatus::Failed;
        } else {
            report.status = CaseStatus::Passed;
        }
        report.artifacts = list_artifacts(&fallback_dir);
        let _ = write_json(&fallback_dir.join("case-report.json"), &report);
        report
    }

    async fn execute_fallback_case_inner(
        &self,
        fallback_dir: &Path,
        report: &mut CaseReport,
    ) -> Result<()> {
        fs::create_dir_all(fallback_dir)?;
        let source = fallback_dir.join("fallback_source.mp4");
        generate_h264_aac_fixture(&source, self.config.output_seconds.max(4.0)).await?;
        let failed = run_command_capture(
            "ffmpeg",
            &[
                "-hide_banner".to_string(),
                "-f".to_string(),
                "lavfi".to_string(),
                "-i".to_string(),
                "testsrc=size=64x64:rate=1:duration=0.2".to_string(),
                "-frames:v".to_string(),
                "1".to_string(),
                "-c:v".to_string(),
                "elixir_missing_hardware_encoder".to_string(),
                "-f".to_string(),
                "null".to_string(),
                "-".to_string(),
            ],
            20,
        )
        .await?;
        ensure!(
            !failed.status.success(),
            "forced hardware failure command unexpectedly succeeded"
        );
        fs::write(
            fallback_dir.join("forced-hardware-failure-stderr.txt"),
            String::from_utf8_lossy(&failed.stderr).as_bytes(),
        )?;

        let metadata = ffprobe::probe(path_to_str(&source)?).await?;
        let capabilities =
            normalize_ffprobe_metadata(&metadata, None, Some(source.display().to_string()));
        let case = SourceCase {
            id: "software_fallback_probe".to_string(),
            title: "Software fallback probe".to_string(),
            source_kind: "generated".to_string(),
            source_path: source,
            require_hardware: false,
            selected_subtitle_stream: None,
            seek_seconds: 0.0,
            output_seconds: self.config.output_seconds,
            direct_stream_probe: false,
            features: vec!["software_fallback".to_string()],
        };
        let mut policy = EffectivePlaybackPolicy::default();
        policy.allow_direct_play = false;
        policy.allow_audio_transcode = true;
        policy.allow_video_transcode = true;
        policy.hardware_acceleration = "off".to_string();
        policy.allow_hardware_decode = false;
        policy.allow_hardware_encode = false;
        policy.hardware_capabilities = HardwareCapabilities::software_only();
        let plan = plan_playback(
            "hardware-certification-software-fallback",
            &capabilities,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );
        write_json(&fallback_dir.join("playback-plan.json"), &plan)?;
        ensure!(plan.playable, "fallback software plan is not playable");
        ensure!(
            !plan.hardware_acceleration.enabled,
            "fallback software plan must not use hardware"
        );
        let metrics = self
            .run_hls_output(&case, fallback_dir, &plan, "software_fallback")
            .await?;
        report.mode = Some(plan.mode.as_str().to_string());
        report.delivery = Some(plan.delivery.as_str().to_string());
        report.realtime_factor = Some(metrics.realtime_factor);
        report.performance_gate = metrics.performance_gate;
        Ok(())
    }
}

fn selected_public_samples(
    lock: &PublicCorpusLock,
    suite: CertificationSuite,
) -> Vec<PublicCorpusSample> {
    if suite.includes_full_public_corpus() {
        return lock.samples.clone();
    }

    let selectable = lock
        .samples
        .iter()
        .filter(|sample| sample.suite != "torture")
        .cloned()
        .collect::<Vec<_>>();
    let mut samples = Vec::new();
    push_first_public_match(&mut samples, &selectable, |sample| {
        sample.id == "jellyfin_sdr_avc_1080p_3m"
    });
    push_first_public_match(&mut samples, &selectable, |sample| {
        sample.suite == "smoke" && sample.labels.iter().any(|label| label == "type:hdr10")
    });
    push_first_public_match(&mut samples, &selectable, |sample| {
        sample.suite == "smoke"
            && sample
                .labels
                .iter()
                .any(|label| label == "type:dolby-audio")
    });
    if suite.includes_robust() {
        for label in [
            "type:dolby-vision",
            "type:high-bitrate",
            "type:open-movie",
            "type:matroska-conformance",
            "type:interlaced",
            "type:chroma-422",
            "type:chroma-444",
        ] {
            push_first_public_match(&mut samples, &selectable, |sample| {
                sample.labels.iter().any(|candidate| candidate == label)
            });
        }
        push_first_public_match(&mut samples, &selectable, |sample| {
            sample.id == "jellyfin_sdr_avc_1080p_3m"
        });
    }
    samples
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CaseMetrics {
    realtime_factor: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    performance_gate: Option<PerformanceGateReport>,
}

fn select_certification_api(
    config: &HardwareCertificationConfig,
    capabilities: &HardwareCapabilities,
) -> Option<HardwareApi> {
    match config.hardware_preference {
        HardwarePreference::Api(api) if capabilities.is_api_available(api) => Some(api),
        HardwarePreference::Api(_) | HardwarePreference::Off => None,
        HardwarePreference::Auto => capabilities.preferred_api_for_encode("h264"),
    }
}

async fn collect_ffmpeg_inventory() -> FfmpegInventoryReport {
    let version = run_text("ffmpeg", &["-version"], 5)
        .await
        .ok()
        .and_then(|value| value.lines().next().map(str::to_string));
    let hwaccels = run_text("ffmpeg", &["-hide_banner", "-hwaccels"], 5)
        .await
        .map(|value| parse_hwaccels(&value).into_iter().collect())
        .unwrap_or_default();
    let encoders = run_text("ffmpeg", &["-hide_banner", "-encoders"], 5)
        .await
        .map(|value| parse_ffmpeg_components(&value).into_iter().collect())
        .unwrap_or_default();
    let decoders = run_text("ffmpeg", &["-hide_banner", "-decoders"], 5)
        .await
        .map(|value| parse_ffmpeg_components(&value).into_iter().collect())
        .unwrap_or_default();
    FfmpegInventoryReport {
        version,
        hwaccels,
        encoders,
        decoders,
    }
}

async fn collect_os_report() -> HostOsReport {
    let mut raw = BTreeMap::new();
    let mut version = None;
    if cfg!(target_os = "windows") {
        if let Ok(value) = run_text("cmd", &["/C", "ver"], 5).await {
            let value = value.trim().to_string();
            version = Some(value.clone());
            raw.insert("ver".to_string(), json!(value));
        }
    } else if cfg!(target_os = "macos") {
        if let Ok(value) = run_text("sw_vers", &[], 5).await {
            version = macos_pretty_version(&value).or_else(|| Some(value.trim().to_string()));
            raw.insert("sw_vers".to_string(), json!(value));
        }
    } else if cfg!(target_os = "linux") {
        if let Ok(value) = fs::read_to_string("/etc/os-release") {
            let os_release = parse_os_release(&value);
            if let Some(pretty) = os_release.get("PRETTY_NAME") {
                version = Some(pretty.clone());
            }
            raw.insert("os_release".to_string(), json!(os_release));
        }
        if let Ok(value) = run_text("uname", &["-a"], 5).await {
            raw.insert("uname".to_string(), json!(value.trim()));
        }
    } else if let Ok(value) = run_text("uname", &["-a"], 5).await {
        let value = value.trim().to_string();
        version = Some(value.clone());
        raw.insert("uname".to_string(), json!(value));
    }
    HostOsReport {
        family: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        version,
        raw,
    }
}

fn parse_os_release(raw: &str) -> BTreeMap<String, String> {
    raw.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let (key, value) = trimmed.split_once('=')?;
            Some((key.to_string(), unquote_os_release_value(value)))
        })
        .collect()
}

fn unquote_os_release_value(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1]
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
    } else {
        trimmed.to_string()
    }
}

fn macos_pretty_version(raw: &str) -> Option<String> {
    let mut product_name = None;
    let mut product_version = None;
    let mut build_version = None;
    for line in raw.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "ProductName" => product_name = Some(value.to_string()),
            "ProductVersion" => product_version = Some(value.to_string()),
            "BuildVersion" => build_version = Some(value.to_string()),
            _ => {}
        }
    }
    match (product_name, product_version, build_version) {
        (Some(name), Some(version), Some(build)) => Some(format!("{name} {version} ({build})")),
        (Some(name), Some(version), None) => Some(format!("{name} {version}")),
        _ => None,
    }
}

async fn collect_gpu_report() -> HostGpuReport {
    let mut raw = BTreeMap::new();
    if let Ok(value) = run_text(
        "nvidia-smi",
        &[
            "--query-gpu=name,driver_version,pci.device_id,memory.total",
            "--format=csv,noheader",
        ],
        8,
    )
    .await
    {
        raw.insert("nvidia_smi".to_string(), json!(value.trim()));
        let first = value.lines().next().unwrap_or_default();
        let fields = first.split(',').map(str::trim).collect::<Vec<_>>();
        return HostGpuReport {
            vendor: Some("nvidia".to_string()),
            model: fields.first().and_then(|value| non_empty_string(value)),
            driver_version: fields.get(1).and_then(|value| non_empty_string(value)),
            device_id: fields.get(2).and_then(|value| non_empty_string(value)),
            raw,
        };
    }
    if cfg!(target_os = "macos") {
        if let Ok(value) = run_text("system_profiler", &["SPDisplaysDataType", "-json"], 15).await {
            let parsed = serde_json::from_str(&value).unwrap_or_else(|_| json!(value));
            let identity = gpu_identity_from_macos_system_profiler(&parsed);
            raw.insert("system_profiler_spdisplays".to_string(), parsed);
            return HostGpuReport {
                vendor: identity.vendor.or_else(|| infer_gpu_vendor(&raw)),
                model: identity.model,
                device_id: identity.device_id,
                driver_version: identity.driver_version,
                raw,
            };
        }
    } else if cfg!(target_os = "windows") {
        if let Ok(value) = run_text(
            "powershell",
            &[
                "-NoProfile",
                "-Command",
                "Get-CimInstance Win32_VideoController | Select-Object Name,PNPDeviceID,DriverVersion,AdapterRAM | ConvertTo-Json -Compress",
            ],
            15,
        )
        .await
        {
            let parsed = serde_json::from_str(&value).unwrap_or_else(|_| json!(value));
            let identity = gpu_identity_from_windows_cim(&parsed);
            raw.insert(
                "win32_video_controller".to_string(),
                parsed,
            );
            return HostGpuReport {
                vendor: identity.vendor.or_else(|| infer_gpu_vendor(&raw)),
                model: identity.model,
                device_id: identity.device_id,
                driver_version: identity.driver_version,
                raw,
            };
        }
    } else if let Ok(value) = run_text(
        "sh",
        &[
            "-lc",
            "lspci 2>/dev/null | grep -Ei 'vga|3d|display' || true",
        ],
        8,
    )
    .await
    {
        raw.insert("lspci_display".to_string(), json!(value.trim()));
        let identity = gpu_identity_from_lspci(&value);
        return HostGpuReport {
            vendor: identity.vendor.or_else(|| infer_gpu_vendor(&raw)),
            model: identity.model,
            device_id: identity.device_id,
            driver_version: identity.driver_version,
            raw,
        };
    }
    HostGpuReport {
        vendor: infer_gpu_vendor(&raw),
        model: None,
        device_id: None,
        driver_version: None,
        raw,
    }
}

fn gpu_identity_from_windows_cim(value: &Value) -> GpuIdentity {
    let controllers = value_array_or_single(value);
    let selected = controllers
        .iter()
        .copied()
        .find(|candidate| infer_gpu_vendor_value(candidate).is_some())
        .or_else(|| controllers.first().copied());
    let Some(controller) = selected else {
        return GpuIdentity::default();
    };
    GpuIdentity {
        vendor: infer_gpu_vendor_value(controller),
        model: string_field_any(controller, &["Name", "Caption", "Description"]),
        device_id: string_field_any(controller, &["PNPDeviceID", "DeviceID"]),
        driver_version: string_field_any(controller, &["DriverVersion"]),
    }
}

fn gpu_identity_from_macos_system_profiler(value: &Value) -> GpuIdentity {
    let displays = value
        .get("SPDisplaysDataType")
        .and_then(Value::as_array)
        .map(|values| values.iter().collect::<Vec<_>>())
        .unwrap_or_default();
    let selected = displays
        .iter()
        .copied()
        .find(|candidate| infer_gpu_vendor_value(candidate).is_some())
        .or_else(|| displays.first().copied());
    let Some(display) = selected else {
        return GpuIdentity::default();
    };
    GpuIdentity {
        vendor: infer_gpu_vendor_value(display),
        model: string_field_any(
            display,
            &[
                "sppci_model",
                "spdisplays_chipset-model",
                "spdisplays_vendor",
                "_name",
            ],
        ),
        device_id: string_field_any(
            display,
            &[
                "spdisplays_device-id",
                "spdisplays_vendor-id",
                "spdisplays_revision-id",
            ],
        ),
        driver_version: string_field_any(
            display,
            &[
                "spdisplays_metal",
                "spdisplays_mtlgpufamilysupport",
                "spdisplays_revision-id",
            ],
        ),
    }
}

fn gpu_identity_from_lspci(value: &str) -> GpuIdentity {
    let model = value
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .and_then(non_empty_string);
    GpuIdentity {
        vendor: infer_gpu_vendor_text(value),
        model,
        device_id: None,
        driver_version: None,
    }
}

fn value_array_or_single(value: &Value) -> Vec<&Value> {
    match value {
        Value::Array(values) => values.iter().collect(),
        Value::Object(_) => vec![value],
        _ => Vec::new(),
    }
}

fn string_field_any(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(value_to_non_empty_string)
}

fn value_to_non_empty_string(value: &Value) -> Option<String> {
    match value {
        Value::String(raw) => non_empty_string(raw),
        Value::Number(raw) => Some(raw.to_string()),
        _ => None,
    }
}

fn non_empty_string(value: impl AsRef<str>) -> Option<String> {
    let trimmed = value.as_ref().trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn infer_gpu_vendor(raw: &BTreeMap<String, Value>) -> Option<String> {
    infer_gpu_vendor_text(&serde_json::to_string(raw).ok()?)
}

fn infer_gpu_vendor_value(value: &Value) -> Option<String> {
    infer_gpu_vendor_text(&serde_json::to_string(value).ok()?)
}

fn infer_gpu_vendor_text(value: &str) -> Option<String> {
    let text = value.to_ascii_lowercase();
    if text.contains("nvidia") {
        Some("nvidia".to_string())
    } else if text.contains("amd") || text.contains("radeon") {
        Some("amd".to_string())
    } else if text.contains("intel") {
        Some("intel".to_string())
    } else if text.contains("apple") {
        Some("apple".to_string())
    } else {
        None
    }
}

fn write_host_evidence_files(artifact_dir: &Path, report: &CertificationReport) -> Result<()> {
    let host_dir = artifact_dir.join("host");
    fs::create_dir_all(&host_dir)?;
    write_json(&host_dir.join("os.json"), &report.os)?;
    write_json(&host_dir.join("gpu.json"), &report.gpu)?;
    write_json(&host_dir.join("ffmpeg.json"), &report.ffmpeg)?;
    write_json(
        &host_dir.join("hardware-capabilities.json"),
        &report.hardware_capabilities,
    )?;
    fs::write(
        host_dir.join("ffmpeg-version.txt"),
        report.ffmpeg.version.as_deref().unwrap_or_default(),
    )?;
    write_lines(
        &host_dir.join("ffmpeg-hwaccels.txt"),
        &report.ffmpeg.hwaccels,
    )?;
    write_lines(
        &host_dir.join("ffmpeg-encoders.txt"),
        &report.ffmpeg.encoders,
    )?;
    write_lines(
        &host_dir.join("ffmpeg-decoders.txt"),
        &report.ffmpeg.decoders,
    )?;
    Ok(())
}

async fn generate_h264_aac_fixture(path: &Path, duration: f64) -> Result<()> {
    let args = vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size=1280x720:rate=30:duration={duration}"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("sine=frequency=1000:sample_rate=48000:duration={duration}"),
        "-c:v".to_string(),
        "libx264".to_string(),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
        "-profile:v".to_string(),
        "high".to_string(),
        "-level:v".to_string(),
        "4.1".to_string(),
        "-c:a".to_string(),
        "aac".to_string(),
        "-shortest".to_string(),
        command_path(path),
    ];
    run_ffmpeg_generation(path, &args).await
}

async fn generate_hevc_aac_fixture(path: &Path, duration: f64) -> Result<()> {
    let args = vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size=1280x720:rate=30:duration={duration}"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("sine=frequency=1200:sample_rate=48000:duration={duration}"),
        "-c:v".to_string(),
        "libx265".to_string(),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
        "-tag:v".to_string(),
        "hvc1".to_string(),
        "-c:a".to_string(),
        "aac".to_string(),
        "-shortest".to_string(),
        command_path(path),
    ];
    run_ffmpeg_generation(path, &args).await
}

async fn run_ffmpeg_generation(path: &Path, args: &[String]) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    let output = run_command_capture("ffmpeg", args, 120).await?;
    if !output.status.success() {
        bail!(
            "fixture generation failed for {}: {}",
            path.display(),
            tail_lossy(&output.stderr)
        );
    }
    ensure!(
        path.exists(),
        "fixture was not created at {}",
        path.display()
    );
    Ok(())
}

fn public_sample_path(
    corpus_root: &Path,
    lock_root: &Path,
    sample: &PublicCorpusSample,
) -> PathBuf {
    sample
        .local_path
        .strip_prefix(lock_root)
        .map(|relative| corpus_root.join(relative))
        .unwrap_or_else(|_| sample.local_path.clone())
}

fn robust_seek_offsets(
    duration_seconds: f64,
    output_seconds: f64,
) -> Result<Vec<(String, f64, f32)>> {
    ensure!(
        duration_seconds.is_finite() && duration_seconds >= 12.0,
        "robust seek source is too short: {duration_seconds:.2}s"
    );
    let output_window = output_seconds.max(1.0);
    let latest_safe_offset = (duration_seconds - output_window - 1.0).max(0.0);
    ensure!(
        latest_safe_offset >= 8.0,
        "robust seek source does not leave enough output window: duration={duration_seconds:.2}s output={output_window:.2}s"
    );
    let offsets = [
        ("seek_25_percent", 0.25, duration_seconds * 0.25),
        ("seek_50_percent", 0.50, duration_seconds * 0.50),
        (
            "seek_near_end",
            0.90,
            (duration_seconds * 0.90).min(latest_safe_offset),
        ),
    ];
    Ok(offsets
        .into_iter()
        .map(|(feature, fraction, seconds)| (feature.to_string(), fraction, seconds as f32))
        .collect())
}

fn push_first_public_match<F>(
    samples: &mut Vec<PublicCorpusSample>,
    all: &[PublicCorpusSample],
    predicate: F,
) where
    F: Fn(&PublicCorpusSample) -> bool,
{
    if let Some(sample) = all.iter().find(|sample| {
        predicate(sample) && !samples.iter().any(|selected| selected.id == sample.id)
    }) {
        samples.push(sample.clone());
    }
}

fn verify_public_sample(sample: &PublicCorpusSample, path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("read public corpus file {}", path.display()))?;
    ensure!(
        metadata.len() == sample.size_bytes,
        "{} size mismatch: expected {}, got {}",
        sample.id,
        sample.size_bytes,
        metadata.len()
    );
    let actual = sha256_hex_file(path)?;
    ensure!(
        actual == sample.sha256,
        "{} sha256 mismatch: expected {}, got {}",
        sample.id,
        sample.sha256,
        actual
    );
    Ok(())
}

fn insert_output_duration_limit(args: &mut Vec<String>, seconds: f64) {
    let insertion = args
        .iter()
        .position(|arg| arg == "-f")
        .unwrap_or(args.len());
    args.splice(
        insertion..insertion,
        ["-t".to_string(), seconds.max(1.0).to_string()],
    );
}

fn output_dir_for_label(case_dir: &Path, label: &str) -> PathBuf {
    if label == "direct_stream" {
        case_dir.join("direct-stream-hls")
    } else {
        case_dir.join("hls")
    }
}

fn artifact_path_for_label(case_dir: &Path, label: &str, name: &str) -> PathBuf {
    if label == "direct_stream" {
        case_dir.join(format!("direct-stream-{name}"))
    } else {
        case_dir.join(name)
    }
}

async fn probe_duration_seconds(path: &Path) -> Result<f64> {
    let output = run_command_capture(
        "ffprobe",
        &[
            "-v".to_string(),
            "error".to_string(),
            "-show_entries".to_string(),
            "format=duration".to_string(),
            "-of".to_string(),
            "default=nokey=1:noprint_wrappers=1".to_string(),
            command_path(path),
        ],
        30,
    )
    .await?;
    if !output.status.success() {
        bail!("ffprobe duration failed: {}", tail_lossy(&output.stderr));
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let duration = raw
        .trim()
        .parse::<f64>()
        .with_context(|| format!("parse ffprobe duration {raw:?}"))?;
    ensure!(
        duration.is_finite() && duration > 0.0,
        "invalid ffprobe duration {duration:?}"
    );
    Ok(duration)
}

async fn probe_media(path: &Path) -> Result<Value> {
    let output = run_command_capture(
        "ffprobe",
        &[
            "-v".to_string(),
            "quiet".to_string(),
            "-print_format".to_string(),
            "json".to_string(),
            "-show_format".to_string(),
            "-show_streams".to_string(),
            command_path(path),
        ],
        30,
    )
    .await?;
    if !output.status.success() {
        bail!(
            "ffprobe failed for {} with {:?}: stderr={} stdout={}",
            path.display(),
            output.status.code(),
            non_empty_tail(&output.stderr),
            non_empty_tail(&output.stdout)
        );
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("parse output ffprobe json for {}", path.display()))
}

fn ensure_expected_init_segment(plan: &PlaybackPlan, output_dir: &Path) -> Result<()> {
    if !matches!(plan.delivery, Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4) {
        return Ok(());
    }
    let init_name = if plan.mode == PlaybackMode::DirectStream {
        "init.mp4"
    } else {
        "init_0.mp4"
    };
    let init_segment = output_dir.join(init_name);
    ensure!(
        init_segment.exists(),
        "missing fMP4 init segment {} referenced by HLS playlist",
        init_segment.display()
    );
    Ok(())
}

fn validate_output_probe(probe: &Value) -> Result<()> {
    let streams = probe
        .get("streams")
        .and_then(Value::as_array)
        .context("output probe missing streams")?;
    let video = streams
        .iter()
        .find(|stream| stream.get("codec_type").and_then(Value::as_str) == Some("video"))
        .context("output probe missing video stream")?;
    ensure!(
        video.get("codec_name").and_then(Value::as_str).is_some(),
        "output video stream missing codec_name"
    );
    Ok(())
}

async fn validate_nonblank_frame(media_playlist: &Path, thumbnail_dir: &Path) -> Result<()> {
    fs::create_dir_all(thumbnail_dir)?;
    let frame_pattern = thumbnail_dir.join("frame_%04d.png");
    let output = run_command_capture(
        "ffmpeg",
        &[
            "-hide_banner".to_string(),
            "-loglevel".to_string(),
            "error".to_string(),
            "-y".to_string(),
            "-i".to_string(),
            command_path(media_playlist),
            "-vf".to_string(),
            "fps=1".to_string(),
            "-frames:v".to_string(),
            "6".to_string(),
            command_path(&frame_pattern),
        ],
        30,
    )
    .await?;
    if !output.status.success() {
        bail!(
            "thumbnail extraction failed: {}",
            tail_lossy(&output.stderr)
        );
    }
    let mut frames = fs::read_dir(thumbnail_dir)
        .with_context(|| format!("read thumbnail dir {}", thumbnail_dir.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("png"))
        .collect::<Vec<_>>();
    frames.sort();
    ensure!(
        !frames.is_empty(),
        "thumbnail extraction produced no frames from {}",
        media_playlist.display()
    );

    let mut best_delta = 0u8;
    let mut best_min = u8::MAX;
    let mut best_max = u8::MIN;
    for frame in frames {
        let image = image::open(&frame)
            .with_context(|| format!("open extracted output frame {}", frame.display()))?
            .to_rgb8();
        let mut min_luma = u8::MAX;
        let mut max_luma = u8::MIN;
        for pixel in image.pixels() {
            let luma =
                ((u16::from(pixel[0]) + u16::from(pixel[1]) + u16::from(pixel[2])) / 3) as u8;
            min_luma = min_luma.min(luma);
            max_luma = max_luma.max(luma);
        }
        let delta = max_luma.saturating_sub(min_luma);
        if delta > best_delta {
            best_delta = delta;
            best_min = min_luma;
            best_max = max_luma;
        }
        if delta > 8 {
            return Ok(());
        }
    }
    bail!("output frames appear blank: min_luma={best_min} max_luma={best_max}");
}

fn command_mentions_hardware(command: &Value, plan: &PlaybackPlan) -> bool {
    let encoder_ok = plan
        .hardware_acceleration
        .encoder
        .as_deref()
        .map(|encoder| command_args_contain_token(command, encoder))
        .unwrap_or(false);
    let decoder_ok = plan
        .hardware_acceleration
        .decoder
        .as_deref()
        .map(|decoder| command_args_contain_token(command, decoder))
        .unwrap_or(true);
    encoder_ok && decoder_ok
}

fn command_args_contain_token(command: &Value, token: &str) -> bool {
    let Some(args) = command.get("args").and_then(Value::as_array) else {
        return false;
    };
    args.iter()
        .filter_map(Value::as_str)
        .any(|arg| arg.eq_ignore_ascii_case(token))
}

fn certification_effective_policy(
    client: &ClientPlaybackProfile,
    hardware_api_label: &str,
    hardware_capabilities: &HardwareCapabilities,
    direct_stream_probe: bool,
) -> EffectivePlaybackPolicy {
    let mut policy = EffectivePlaybackPolicy::default();
    policy.allow_direct_play = false;
    policy.allow_direct_stream = direct_stream_probe;
    policy.allow_audio_transcode = true;
    policy.allow_video_transcode = true;
    policy.allow_adaptive_transcode = false;
    policy.max_bitrate_bps = client.max_bitrate_bps;
    policy.max_resolution = client.max_resolution.clone();
    policy.hardware_acceleration = hardware_api_label.to_string();
    policy.allow_hardware_decode = true;
    policy.allow_hardware_encode = true;
    policy.hardware_fallback = "software".to_string();
    policy.force_sdr_output = true;
    policy.hardware_capabilities = hardware_capabilities.clone();
    policy
}

fn performance_gate_for_case(
    suite: CertificationSuite,
    case: &SourceCase,
    plan: &PlaybackPlan,
    realtime_factor: f64,
) -> Option<PerformanceGateReport> {
    if !(case.require_hardware && plan_requires_video_hardware(plan) && case.output_seconds <= 20.0)
    {
        return None;
    }

    let (tier, required_realtime_factor) = hardware_performance_requirement(suite, case, plan);
    Some(PerformanceGateReport {
        tier: tier.to_string(),
        required_realtime_factor,
        actual_realtime_factor: realtime_factor,
        passed: realtime_factor >= required_realtime_factor,
    })
}

fn hardware_performance_requirement(
    suite: CertificationSuite,
    case: &SourceCase,
    plan: &PlaybackPlan,
) -> (&'static str, f64) {
    if suite == CertificationSuite::Smoke {
        return ("smoke_functional_floor", 0.25);
    }
    if selected_4k_hdr_to_1080p_sdr_case(case, plan) {
        return ("selected_4k_hdr_to_1080p_sdr", 1.0);
    }
    if compatible_1080p_sdr_case(case, plan) {
        return ("compatible_1080p_sdr", 2.0);
    }
    ("hardware_functional_floor", 0.25)
}

fn selected_4k_hdr_to_1080p_sdr_case(case: &SourceCase, plan: &PlaybackPlan) -> bool {
    let Some(output) = plan.video_output.as_ref() else {
        return false;
    };
    let scaled_to_1080p = output
        .scale
        .as_ref()
        .is_some_and(|scale| scale.height <= 1080);
    scaled_to_1080p
        && case_has_feature(case, "resolution:4k")
        && output.tone_map.is_some()
        && (case_has_feature(case, "type:high-bitrate")
            || case_has_feature(case, "type:dolby-vision"))
}

fn requires_nonblank_frame_validation(case: &SourceCase, _plan: &PlaybackPlan) -> bool {
    !case_has_feature(case, "type:dolby-audio")
}

fn compatible_1080p_sdr_case(case: &SourceCase, plan: &PlaybackPlan) -> bool {
    let Some(output) = plan.video_output.as_ref() else {
        return false;
    };
    case_has_feature(case, "type:sdr")
        && output.scale.is_none()
        && output.tone_map.is_none()
        && output.burn_in.is_none()
        && output.frame_rate.mode == VideoFrameRateMode::Source
}

fn case_has_feature(case: &SourceCase, feature: &str) -> bool {
    case.features.iter().any(|candidate| candidate == feature)
}

fn plan_requires_video_hardware(plan: &PlaybackPlan) -> bool {
    let video_work = matches!(
        plan.mode,
        PlaybackMode::VideoTranscode | PlaybackMode::AdaptiveTranscode
    ) || matches!(
        plan.video_action,
        StreamAction::Transcode | StreamAction::BurnIn
    ) || plan.video_output.is_some();
    if !video_work {
        return false;
    }
    !plan
        .warnings
        .iter()
        .any(|warning| warning.starts_with("hardware_encoder_min_width_unsupported:"))
}

async fn run_text(program: &str, args: &[&str], timeout_seconds: u64) -> Result<String> {
    let output = run_command_capture(
        program,
        &args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>(),
        timeout_seconds,
    )
    .await?;
    if !output.status.success() {
        bail!("{program} failed: {}", tail_lossy(&output.stderr));
    }
    let mut text = String::from_utf8_lossy(&output.stdout).to_string();
    if text.trim().is_empty() {
        text = String::from_utf8_lossy(&output.stderr).to_string();
    }
    Ok(text)
}

async fn run_command_capture(
    program: &str,
    args: &[String],
    timeout_seconds: u64,
) -> Result<std::process::Output> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = command
        .spawn()
        .with_context(|| format!("spawn {program}"))?;
    timeout(
        Duration::from_secs(timeout_seconds),
        child.wait_with_output(),
    )
    .await
    .with_context(|| format!("{program} timed out after {timeout_seconds}s"))?
    .with_context(|| format!("wait for {program}"))
}

async fn process_snapshot() -> Value {
    if cfg!(target_os = "windows") {
        run_text(
            "powershell",
            &[
                "-NoProfile",
                "-Command",
                "Get-Process ffmpeg -ErrorAction SilentlyContinue | Select-Object Id,ProcessName,StartTime | ConvertTo-Json -Compress",
            ],
            8,
        )
        .await
        .map(|value| serde_json::from_str(&value).unwrap_or_else(|_| json!(value)))
        .unwrap_or_else(|err| json!({ "error": err.to_string() }))
    } else {
        run_text(
            "sh",
            &["-lc", "ps -axo pid,comm | grep '[f]fmpeg' || true"],
            8,
        )
        .await
        .map(|value| json!(value))
        .unwrap_or_else(|err| json!({ "error": err.to_string() }))
    }
}

fn current_commit_sha() -> Option<String> {
    std::env::var("GITHUB_SHA")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        })
}

fn current_run_id() -> Option<String> {
    std::env::var("GITHUB_RUN_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn artifact_tree_digest(artifact_dir: &Path, report: &CertificationReport) -> Result<String> {
    let mut digest = Sha256::new();
    let mut clone = report.clone();
    clone.artifact_digest = None;
    let report_bytes = serde_json::to_vec(&clone)?;
    digest.update(b"certification-report\0");
    digest.update(report_bytes.len().to_string().as_bytes());
    digest.update(b"\0");
    digest.update(&report_bytes);

    for relative in list_artifacts(artifact_dir)
        .into_iter()
        .filter(|relative| relative != "certification.json")
    {
        let path = artifact_dir.join(&relative);
        digest.update(b"artifact-file\0");
        digest.update(relative.as_bytes());
        digest.update(b"\0");
        let metadata = fs::metadata(&path)
            .with_context(|| format!("stat artifact for digest {}", path.display()))?;
        digest.update(metadata.len().to_string().as_bytes());
        digest.update(b"\0");
        let mut file = fs::File::open(&path)
            .with_context(|| format!("open artifact for digest {}", path.display()))?;
        std::io::copy(&mut file, &mut digest)
            .with_context(|| format!("hash artifact {}", path.display()))?;
    }

    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn sha256_hex_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)?;
    Ok(format!("{:x}", digest.finalize()))
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn write_lines(path: &Path, values: &[String]) -> Result<()> {
    let mut text = values.join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    fs::write(path, text)?;
    Ok(())
}

fn read_json(path: impl AsRef<Path>) -> Result<Value> {
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
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
            artifacts.push(normalized_relative_path(relative));
        }
    }
}

fn normalized_relative_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn directory_size(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_size(&path)
            } else {
                entry.metadata().map(|metadata| metadata.len()).unwrap_or(0)
            }
        })
        .sum()
}

fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("path is not valid UTF-8: {}", path.display()))
}

fn command_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn tail_lossy(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let lines = text.lines().rev().take(16).collect::<Vec<_>>();
    lines.into_iter().rev().collect::<Vec<_>>().join("\n")
}

fn non_empty_tail(bytes: &[u8]) -> String {
    let tail = tail_lossy(bytes);
    if tail.trim().is_empty() {
        "<empty>".to_string()
    } else {
        tail
    }
}

fn push_unique(values: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_certification_suite_aliases() {
        assert_eq!(
            CertificationSuite::parse("smoke").unwrap(),
            CertificationSuite::Smoke
        );
        assert_eq!(
            CertificationSuite::parse("heavy").unwrap(),
            CertificationSuite::Robust
        );
        assert_eq!(
            CertificationSuite::parse("full").unwrap(),
            CertificationSuite::Torture
        );
        assert!(CertificationSuite::parse("nightly").is_err());
    }

    #[test]
    fn torture_public_selection_uses_entire_locked_corpus() {
        let lock: PublicCorpusLock = serde_yaml::from_str(PUBLIC_CORPUS_LOCK).unwrap();
        let selected = selected_public_samples(&lock, CertificationSuite::Torture);
        let selected_ids = selected
            .iter()
            .map(|sample| sample.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let locked_ids = lock
            .samples
            .iter()
            .map(|sample| sample.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(selected.len(), lock.samples.len());
        assert_eq!(selected_ids, locked_ids);
    }

    #[test]
    fn robust_public_selection_stays_representative() {
        let lock: PublicCorpusLock = serde_yaml::from_str(PUBLIC_CORPUS_LOCK).unwrap();
        let selected = selected_public_samples(&lock, CertificationSuite::Robust);
        let selected_ids = selected
            .iter()
            .map(|sample| sample.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert!(selected.len() < lock.samples.len());
        assert!(
            selected.iter().all(|sample| sample.suite != "torture"),
            "robust selection must match smoke+heavy workflow hydration"
        );
        assert!(selected_ids.contains("jellyfin_sdr_avc_1080p_3m"));
        assert!(selected.iter().any(|sample| {
            sample
                .labels
                .iter()
                .any(|label| label == "type:high-bitrate")
        }));
        assert!(
            selected
                .iter()
                .any(|sample| sample.labels.iter().any(|label| label == "type:interlaced"))
        );
    }

    #[test]
    fn public_sample_path_uses_configured_cache_root() {
        let sample = PublicCorpusSample {
            id: "sample".to_string(),
            title: "Sample".to_string(),
            source: "test".to_string(),
            sample_type: "sdr".to_string(),
            labels: Vec::new(),
            suite: "smoke".to_string(),
            local_path: PathBuf::from("data/playback-corpus/public/jellyfin/sample.mp4"),
            size_bytes: 1,
            sha256: "0".repeat(64),
            playback: PublicPlaybackExpectation {
                output_seconds: Some(4.0),
            },
        };
        let path = public_sample_path(
            Path::new("/cache/public"),
            Path::new("data/playback-corpus/public"),
            &sample,
        );
        assert_eq!(path, PathBuf::from("/cache/public/jellyfin/sample.mp4"));
    }

    #[test]
    fn parses_linux_os_release_identity() {
        let parsed = parse_os_release(
            r#"
NAME="Ubuntu"
VERSION_ID="22.04"
PRETTY_NAME="Ubuntu 22.04.4 LTS"
ESCAPED="a \"quoted\" value"
"#,
        );

        assert_eq!(parsed.get("NAME").map(String::as_str), Some("Ubuntu"));
        assert_eq!(parsed.get("VERSION_ID").map(String::as_str), Some("22.04"));
        assert_eq!(
            parsed.get("PRETTY_NAME").map(String::as_str),
            Some("Ubuntu 22.04.4 LTS")
        );
        assert_eq!(
            parsed.get("ESCAPED").map(String::as_str),
            Some("a \"quoted\" value")
        );
    }

    #[test]
    fn parses_macos_pretty_version() {
        let parsed = macos_pretty_version(
            "ProductName:\t\tmacOS\nProductVersion:\t\t14.5\nBuildVersion:\t\t23F79\n",
        );

        assert_eq!(parsed.as_deref(), Some("macOS 14.5 (23F79)"));
    }

    #[test]
    fn artifact_tree_digest_tracks_evidence_files_but_not_final_report_file() {
        let temp = tempfile::tempdir().unwrap();
        let case_dir = temp.path().join("cases").join("case");
        fs::create_dir_all(&case_dir).unwrap();
        fs::write(case_dir.join("metrics.json"), br#"{"value":1}"#).unwrap();
        let report = CertificationReport {
            schema_version: 1,
            status: CertificationStatus::Passed,
            target_id: "target".to_string(),
            suite: "robust".to_string(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            commit_sha: Some("a".repeat(40)),
            run_id: Some("123456".to_string()),
            corpus_lock_sha256: "b".repeat(64),
            os: HostOsReport {
                family: "test".to_string(),
                arch: "x86_64".to_string(),
                version: Some("1".to_string()),
                raw: BTreeMap::new(),
            },
            gpu: HostGpuReport {
                vendor: Some("nvidia".to_string()),
                model: Some("test".to_string()),
                device_id: Some("id".to_string()),
                driver_version: Some("driver".to_string()),
                raw: BTreeMap::new(),
            },
            hardware_api: Some("nvenc".to_string()),
            requested_hardware_api: "nvenc".to_string(),
            require_hardware: true,
            ffmpeg: FfmpegInventoryReport {
                version: Some("ffmpeg".to_string()),
                hwaccels: vec!["cuda".to_string()],
                encoders: vec!["h264_nvenc".to_string()],
                decoders: vec!["h264_cuvid".to_string()],
            },
            hardware_capabilities: HardwareCapabilities::default(),
            cases: CaseSummary::default(),
            performance: PerformanceSummary::default(),
            failure_reasons: Vec::new(),
            artifact_digest: None,
        };

        write_host_evidence_files(temp.path(), &report).unwrap();
        assert!(temp.path().join("host/os.json").exists());
        assert!(temp.path().join("host/gpu.json").exists());
        assert!(
            fs::read_to_string(temp.path().join("host/ffmpeg-encoders.txt"))
                .unwrap()
                .contains("h264_nvenc")
        );

        let first = artifact_tree_digest(temp.path(), &report).unwrap();
        fs::write(case_dir.join("metrics.json"), br#"{"value":2}"#).unwrap();
        let second = artifact_tree_digest(temp.path(), &report).unwrap();
        fs::write(
            temp.path().join("certification.json"),
            br#"{"ignored":true}"#,
        )
        .unwrap();
        let third = artifact_tree_digest(temp.path(), &report).unwrap();

        assert_ne!(first, second);
        assert_eq!(second, third);
    }

    #[test]
    fn parses_windows_cim_gpu_identity() {
        let identity = gpu_identity_from_windows_cim(&json!([
            {
                "Name": "Microsoft Basic Render Driver",
                "PNPDeviceID": "ROOT\\BasicRender",
                "DriverVersion": "10.0"
            },
            {
                "Name": "NVIDIA RTX A10",
                "PNPDeviceID": "PCI\\VEN_10DE&DEV_2236&SUBSYS_145F10DE",
                "DriverVersion": "555.99"
            }
        ]));

        assert_eq!(identity.vendor.as_deref(), Some("nvidia"));
        assert_eq!(identity.model.as_deref(), Some("NVIDIA RTX A10"));
        assert_eq!(identity.driver_version.as_deref(), Some("555.99"));
        assert_eq!(
            identity.device_id.as_deref(),
            Some("PCI\\VEN_10DE&DEV_2236&SUBSYS_145F10DE")
        );
    }

    #[test]
    fn parses_macos_system_profiler_gpu_identity() {
        let identity = gpu_identity_from_macos_system_profiler(&json!({
            "SPDisplaysDataType": [
                {
                    "sppci_model": "AMD Radeon Pro 560X",
                    "spdisplays_device-id": "0x67ef",
                    "spdisplays_revision-id": "0x00ef",
                    "spdisplays_vendor": "sppci_vendor_amd"
                }
            ]
        }));

        assert_eq!(identity.vendor.as_deref(), Some("amd"));
        assert_eq!(identity.model.as_deref(), Some("AMD Radeon Pro 560X"));
        assert_eq!(identity.device_id.as_deref(), Some("0x67ef"));
    }

    #[test]
    fn parses_lspci_gpu_identity() {
        let identity = gpu_identity_from_lspci(
            "65:00.0 VGA compatible controller: Advanced Micro Devices, Inc. [AMD/ATI] Navi 14\n",
        );

        assert_eq!(identity.vendor.as_deref(), Some("amd"));
        assert!(
            identity
                .model
                .as_deref()
                .is_some_and(|model| model.contains("Navi 14"))
        );
    }

    #[test]
    fn robust_seek_offsets_cover_quarter_half_and_near_end() {
        let offsets = robust_seek_offsets(30.0, 4.0).unwrap();

        assert_eq!(offsets[0].0, "seek_25_percent");
        assert_eq!(offsets[1].0, "seek_50_percent");
        assert_eq!(offsets[2].0, "seek_near_end");
        assert!((offsets[0].2 - 7.5).abs() < 0.01);
        assert!((offsets[1].2 - 15.0).abs() < 0.01);
        assert!(offsets[2].2 <= 25.0);
        assert!(robust_seek_offsets(8.0, 4.0).is_err());
    }

    fn test_playback_plan(mode: PlaybackMode, video_action: StreamAction) -> PlaybackPlan {
        PlaybackPlan {
            plan_version: 1,
            mode,
            delivery: crate::playback::plan::Delivery::HlsFmp4,
            media_file_id: "m".to_string(),
            selected_video_track: Some(0),
            video_action,
            audio_action: StreamAction::Transcode,
            subtitle_action: StreamAction::Disabled,
            seek_behavior: crate::playback::plan::SeekBehavior::ServerHlsRestart,
            adaptive: false,
            selected_audio_track: Some(1),
            selected_subtitle_track: None,
            hdr_action: crate::playback::plan::HdrAction::None,
            hardware_acceleration: crate::playback::plan::HardwareAccelerationPlan {
                enabled: true,
                api: Some("nvenc".to_string()),
                decoder: Some("cuda".to_string()),
                encoder: Some("h264_nvenc".to_string()),
                fallback: Some("software".to_string()),
            },
            audio_output: None,
            video_output: None,
            adaptive_ladder: None,
            video_transcode_reason: None,
            compatibility_report: crate::playback::plan::CompatibilityReport::empty("m"),
            reasons: Vec::new(),
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: true,
        }
    }

    fn test_source_case(features: &[&str]) -> SourceCase {
        SourceCase {
            id: "case".to_string(),
            title: "case".to_string(),
            source_kind: "test".to_string(),
            source_path: PathBuf::from("case.mp4"),
            require_hardware: true,
            selected_subtitle_stream: None,
            seek_seconds: 0.0,
            output_seconds: 6.0,
            direct_stream_probe: false,
            features: features.iter().map(|feature| feature.to_string()).collect(),
        }
    }

    fn attach_video_output(
        plan: &mut PlaybackPlan,
        scale: Option<crate::playback::plan::VideoScalePlan>,
        tone_map: Option<crate::playback::plan::VideoToneMapPlan>,
    ) {
        plan.video_output = Some(crate::playback::plan::VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "h264_nvenc".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(8_000_000),
            maxrate_bps: Some(8_000_000),
            bufsize_bps: Some(16_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale,
            tone_map,
            frame_rate: crate::playback::plan::VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("24".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: crate::playback::plan::Delivery::HlsFmp4,
            burn_in: None,
            reasons: Vec::new(),
        });
    }

    #[test]
    fn fmp4_output_validation_requires_init_segment() {
        let temp = tempfile::tempdir().unwrap();
        let plan = test_playback_plan(PlaybackMode::VideoTranscode, StreamAction::Transcode);
        let err = ensure_expected_init_segment(&plan, temp.path()).unwrap_err();
        assert!(
            err.to_string().contains("missing fMP4 init segment"),
            "unexpected error: {err:#}"
        );

        fs::write(temp.path().join("init_0.mp4"), b"init").unwrap();
        ensure_expected_init_segment(&plan, temp.path()).unwrap();
    }

    #[test]
    fn command_path_normalizes_windows_separators() {
        assert_eq!(
            command_path(Path::new(r"C:\runner\_work\elixir\hls\stream_0.m3u8")),
            "C:/runner/_work/elixir/hls/stream_0.m3u8"
        );
    }

    #[test]
    fn empty_process_output_tail_is_explicit() {
        assert_eq!(non_empty_tail(b""), "<empty>");
        assert_eq!(non_empty_tail(b"line\n"), "line");
    }

    #[test]
    fn hardware_requirement_classifier_only_requires_video_work() {
        assert!(!plan_requires_video_hardware(&test_playback_plan(
            PlaybackMode::AudioTranscode,
            StreamAction::Copy
        )));
        assert!(!plan_requires_video_hardware(&test_playback_plan(
            PlaybackMode::DirectStream,
            StreamAction::Copy
        )));
        assert!(plan_requires_video_hardware(&test_playback_plan(
            PlaybackMode::VideoTranscode,
            StreamAction::Transcode
        )));
        assert!(plan_requires_video_hardware(&test_playback_plan(
            PlaybackMode::SubtitleTranscode,
            StreamAction::BurnIn
        )));
        assert!(plan_requires_video_hardware(&test_playback_plan(
            PlaybackMode::AdaptiveTranscode,
            StreamAction::Transcode
        )));
        let mut known_fallback =
            test_playback_plan(PlaybackMode::VideoTranscode, StreamAction::Transcode);
        known_fallback
            .warnings
            .push("hardware_encoder_min_width_unsupported:videotoolbox:h264".to_string());
        assert!(!plan_requires_video_hardware(&known_fallback));
    }

    #[test]
    fn performance_gate_classifies_launch_tiers() {
        let mut hdr_4k_plan =
            test_playback_plan(PlaybackMode::VideoTranscode, StreamAction::Transcode);
        attach_video_output(
            &mut hdr_4k_plan,
            Some(crate::playback::plan::VideoScalePlan {
                width: 1920,
                height: 1080,
                reason: "resolution_exceeds_policy".to_string(),
            }),
            Some(crate::playback::plan::VideoToneMapPlan {
                algorithm: "hable".to_string(),
                input_primaries: Some("bt2020".to_string()),
                input_transfer: Some("smpte2084".to_string()),
                input_matrix: Some("bt2020nc".to_string()),
                output_primaries: "bt709".to_string(),
                output_transfer: "bt709".to_string(),
                output_matrix: "bt709".to_string(),
            }),
        );
        let hdr_gate = performance_gate_for_case(
            CertificationSuite::Robust,
            &test_source_case(&["type:dolby-vision", "type:high-bitrate", "resolution:4k"]),
            &hdr_4k_plan,
            0.8,
        )
        .unwrap();
        assert_eq!(hdr_gate.tier, "selected_4k_hdr_to_1080p_sdr");
        assert_eq!(hdr_gate.required_realtime_factor, 1.0);
        assert!(!hdr_gate.passed);

        let hdr_8k_gate = performance_gate_for_case(
            CertificationSuite::Torture,
            &test_source_case(&["type:dolby-vision", "type:high-bitrate", "resolution:8k"]),
            &hdr_4k_plan,
            0.3,
        )
        .unwrap();
        assert_eq!(hdr_8k_gate.tier, "hardware_functional_floor");
        assert_eq!(hdr_8k_gate.required_realtime_factor, 0.25);
        assert!(hdr_8k_gate.passed);

        let mut sdr_plan =
            test_playback_plan(PlaybackMode::VideoTranscode, StreamAction::Transcode);
        attach_video_output(&mut sdr_plan, None, None);
        let sdr_gate = performance_gate_for_case(
            CertificationSuite::Robust,
            &test_source_case(&["type:sdr"]),
            &sdr_plan,
            2.1,
        )
        .unwrap();
        assert_eq!(sdr_gate.tier, "compatible_1080p_sdr");
        assert_eq!(sdr_gate.required_realtime_factor, 2.0);
        assert!(sdr_gate.passed);

        let smoke_gate = performance_gate_for_case(
            CertificationSuite::Smoke,
            &test_source_case(&["type:sdr"]),
            &sdr_plan,
            0.3,
        )
        .unwrap();
        assert_eq!(smoke_gate.tier, "smoke_functional_floor");
        assert_eq!(smoke_gate.required_realtime_factor, 0.25);
        assert!(smoke_gate.passed);
    }

    #[test]
    fn nonblank_frame_validation_skips_dolby_audio_fixtures() {
        let audio_plan = test_playback_plan(PlaybackMode::AudioTranscode, StreamAction::Copy);
        assert!(!requires_nonblank_frame_validation(
            &test_source_case(&["type:dolby-audio"]),
            &audio_plan
        ));

        let video_plan = test_playback_plan(PlaybackMode::VideoTranscode, StreamAction::Transcode);
        assert!(!requires_nonblank_frame_validation(
            &test_source_case(&["type:dolby-audio"]),
            &video_plan
        ));
        assert!(requires_nonblank_frame_validation(
            &test_source_case(&["type:sdr"]),
            &audio_plan
        ));
    }

    #[test]
    fn certification_policy_preserves_browser_caps() {
        let client = ClientPlaybackProfile::browser_like();
        let policy = certification_effective_policy(
            &client,
            "videotoolbox",
            &HardwareCapabilities::default(),
            false,
        );

        assert!(!policy.allow_direct_play);
        assert!(!policy.allow_direct_stream);
        assert_eq!(policy.max_resolution.as_deref(), Some("1080p"));
        assert_eq!(policy.max_bitrate_bps, Some(8_000_000));
        assert_eq!(policy.hardware_acceleration, "videotoolbox");
        assert!(policy.force_sdr_output);

        let direct_stream_policy = certification_effective_policy(
            &client,
            "videotoolbox",
            &HardwareCapabilities::default(),
            true,
        );
        assert!(direct_stream_policy.allow_direct_stream);
    }

    #[test]
    fn command_hardware_detection_requires_selected_encoder() {
        let plan = test_playback_plan(PlaybackMode::VideoTranscode, StreamAction::Transcode);
        assert!(command_mentions_hardware(
            &json!({"args": ["-hwaccel", "cuda", "-c:v", "h264_nvenc"]}),
            &plan
        ));
        assert!(!command_mentions_hardware(
            &json!({"args": ["-c:v", "libx264"]}),
            &plan
        ));
        assert!(!command_mentions_hardware(
            &json!({
                "args": ["-hwaccel", "cuda", "-c:v", "libx264"],
                "source": "/tmp/h264_nvenc/input.mkv"
            }),
            &plan
        ));
        assert!(!command_mentions_hardware(
            &json!({"args": ["-c:v", "h264_nvenc"]}),
            &plan
        ));
    }

    #[test]
    fn case_summary_counts_only_passed_hardware_cases() {
        let temp = tempfile::tempdir().unwrap();
        let config = HardwareCertificationConfig::new(
            CertificationSuite::Smoke,
            "nvenc",
            temp.path().join("corpus"),
            temp.path().join("artifacts"),
        );
        let mut run = CertificationRun {
            config,
            report: CertificationReport {
                schema_version: 1,
                status: CertificationStatus::Failed,
                target_id: "target".to_string(),
                suite: "smoke".to_string(),
                started_at: Utc::now(),
                finished_at: None,
                commit_sha: Some("a".repeat(40)),
                run_id: Some("123456".to_string()),
                corpus_lock_sha256: "b".repeat(64),
                os: HostOsReport {
                    family: "test".to_string(),
                    arch: "x86_64".to_string(),
                    version: Some("1".to_string()),
                    raw: BTreeMap::new(),
                },
                gpu: HostGpuReport {
                    vendor: Some("nvidia".to_string()),
                    model: Some("test".to_string()),
                    device_id: Some("id".to_string()),
                    driver_version: Some("driver".to_string()),
                    raw: BTreeMap::new(),
                },
                hardware_api: Some("nvenc".to_string()),
                requested_hardware_api: "nvenc".to_string(),
                require_hardware: true,
                ffmpeg: FfmpegInventoryReport {
                    version: Some("ffmpeg".to_string()),
                    hwaccels: vec!["cuda".to_string()],
                    encoders: vec!["h264_nvenc".to_string()],
                    decoders: vec!["h264_cuvid".to_string()],
                },
                hardware_capabilities: HardwareCapabilities::default(),
                cases: CaseSummary::default(),
                performance: PerformanceSummary::default(),
                failure_reasons: Vec::new(),
                artifact_digest: None,
            },
            selected_api: Some(HardwareApi::Nvenc),
        };
        let failed_case = CaseReport {
            id: "failed".to_string(),
            title: "failed".to_string(),
            source_kind: "test".to_string(),
            features: Vec::new(),
            status: CaseStatus::Failed,
            hardware_required: true,
            hardware_used: true,
            seek_seconds: 0.0,
            mode: Some("video_transcode".to_string()),
            delivery: Some("hls_fmp4".to_string()),
            encoder: Some("h264_nvenc".to_string()),
            decoder: Some("cuda".to_string()),
            realtime_factor: None,
            performance_gate: None,
            errors: vec!["ffmpeg failed".to_string()],
            warnings: Vec::new(),
            artifacts: Vec::new(),
        };
        let mut passed_case = failed_case.clone();
        passed_case.id = "passed".to_string();
        passed_case.status = CaseStatus::Passed;
        passed_case.errors.clear();

        run.record_case(failed_case);
        assert_eq!(run.report.cases.failed, 1);
        assert_eq!(run.report.cases.hardware_cases, 0);

        run.record_case(passed_case);
        assert_eq!(run.report.cases.passed, 1);
        assert_eq!(run.report.cases.hardware_cases, 1);
    }

    #[tokio::test]
    async fn command_capture_times_out_promptly() {
        let (program, args): (&str, Vec<String>) = if cfg!(target_os = "windows") {
            (
                "powershell",
                vec![
                    "-NoProfile".to_string(),
                    "-Command".to_string(),
                    "Start-Sleep -Seconds 5".to_string(),
                ],
            )
        } else {
            ("sh", vec!["-c".to_string(), "sleep 5".to_string()])
        };
        let started = Instant::now();
        let err = run_command_capture(program, &args, 1).await.unwrap_err();

        assert!(
            started.elapsed() < Duration::from_secs(4),
            "timeout did not return promptly"
        );
        assert!(
            err.to_string().contains("timed out after 1s"),
            "unexpected timeout error: {err:#}"
        );
    }
}
