use super::plan::{AdaptiveLadderPlan, AdaptiveRungPlan};

const SEGMENT_SECONDS: f64 = 4.0;
const START_BUFFER_SECONDS: f64 = 8.0;
const MAX_BUFFER_SECONDS: f64 = 24.0;
const SAFETY_FACTOR: f64 = 0.82;
const UPSHIFT_RECOVERY_WINDOWS: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkEmulationScenario {
    LanNoLoss,
    WanFixedBandwidth,
    BandwidthStepDown,
    BandwidthStepUp,
    Latency,
    Jitter,
    PacketLoss,
    RequestStalls,
    InterruptedResume,
}

#[derive(Debug, Clone)]
pub struct NetworkEmulationStep {
    pub duration_segments: u8,
    pub bandwidth_bps: i64,
    pub latency_ms: u32,
    pub jitter_ms: u32,
    pub packet_loss_per_mille: u32,
    pub request_stall_ms: u32,
    pub interrupted: bool,
}

#[derive(Debug, Clone)]
pub struct NetworkEmulationProfile {
    pub scenario: NetworkEmulationScenario,
    pub steps: Vec<NetworkEmulationStep>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbrSimulationEventKind {
    InitialSelection,
    Downshift,
    Upshift,
    ManualOverride,
    Rebuffer,
    RequestStall,
    Interrupted,
    Resume,
}

#[derive(Debug, Clone)]
pub struct AbrSimulationEvent {
    pub segment_index: usize,
    pub kind: AbrSimulationEventKind,
    pub rung_id: String,
    pub bandwidth_bps: i64,
    pub effective_network_bps: i64,
}

#[derive(Debug, Clone)]
pub struct AbrSimulationResult {
    pub initial_rung_id: String,
    pub final_rung_id: String,
    pub events: Vec<AbrSimulationEvent>,
    pub rebuffer_events: usize,
    pub oscillation_count: usize,
    pub resumed_after_interruption: bool,
    pub manual_override_respected: bool,
}

impl NetworkEmulationScenario {
    pub fn profile(self) -> NetworkEmulationProfile {
        let steps = match self {
            Self::LanNoLoss => vec![step(6, 80_000_000)],
            Self::WanFixedBandwidth => vec![step(6, 6_000_000)],
            Self::BandwidthStepDown => vec![step(2, 18_000_000), step(6, 2_200_000)],
            Self::BandwidthStepUp => vec![step(4, 2_200_000), step(6, 12_000_000)],
            Self::Latency => vec![NetworkEmulationStep {
                latency_ms: 450,
                ..step(6, 8_000_000)
            }],
            Self::Jitter => vec![NetworkEmulationStep {
                latency_ms: 120,
                jitter_ms: 380,
                ..step(6, 8_000_000)
            }],
            Self::PacketLoss => vec![NetworkEmulationStep {
                packet_loss_per_mille: 45,
                ..step(6, 8_000_000)
            }],
            Self::RequestStalls => vec![
                step(2, 10_000_000),
                NetworkEmulationStep {
                    request_stall_ms: 2_200,
                    ..step(2, 5_000_000)
                },
                step(3, 8_000_000),
            ],
            Self::InterruptedResume => vec![
                step(2, 10_000_000),
                NetworkEmulationStep {
                    interrupted: true,
                    ..step(1, 0)
                },
                step(4, 8_000_000),
            ],
        };
        NetworkEmulationProfile {
            scenario: self,
            steps,
        }
    }
}

pub fn simulate_abr_playback(
    ladder: &AdaptiveLadderPlan,
    profile: &NetworkEmulationProfile,
    manual_rung_id: Option<&str>,
) -> AbrSimulationResult {
    let ordered = sorted_rungs(ladder);
    assert!(
        !ordered.is_empty(),
        "adaptive ladder must contain at least one rung"
    );

    let first_effective_bps = profile
        .steps
        .first()
        .map(effective_bandwidth_bps)
        .unwrap_or(0);
    let initial = manual_rung_id
        .and_then(|id| ordered.iter().find(|rung| rung.id == id).copied())
        .unwrap_or_else(|| select_rung(&ordered, first_effective_bps));

    let mut current = initial;
    let mut buffer_seconds = START_BUFFER_SECONDS;
    let mut segment_index = 0_usize;
    let mut rebuffer_events = 0_usize;
    let mut recovery_windows = 0_u8;
    let mut resumed_after_interruption = false;
    let mut manual_override_respected = manual_rung_id.is_none();
    let mut last_direction: Option<i8> = None;
    let mut oscillation_count = 0_usize;
    let mut events = vec![AbrSimulationEvent {
        segment_index,
        kind: if manual_rung_id.is_some() {
            AbrSimulationEventKind::ManualOverride
        } else {
            AbrSimulationEventKind::InitialSelection
        },
        rung_id: current.id.clone(),
        bandwidth_bps: current.bandwidth_bps,
        effective_network_bps: first_effective_bps,
    }];

    for step in &profile.steps {
        for _ in 0..step.duration_segments {
            let effective_bps = effective_bandwidth_bps(step);
            if step.interrupted {
                events.push(event(
                    segment_index,
                    AbrSimulationEventKind::Interrupted,
                    current,
                    effective_bps,
                ));
                rebuffer_events += 1;
                buffer_seconds = START_BUFFER_SECONDS / 2.0;
                resumed_after_interruption = true;
                events.push(event(
                    segment_index,
                    AbrSimulationEventKind::Resume,
                    current,
                    effective_bps,
                ));
                segment_index += 1;
                continue;
            }

            if manual_rung_id.is_none() {
                let target = select_rung(&ordered, effective_bps);
                if target.bandwidth_bps < current.bandwidth_bps {
                    record_shift(
                        &mut events,
                        &mut oscillation_count,
                        &mut last_direction,
                        segment_index,
                        AbrSimulationEventKind::Downshift,
                        target,
                        effective_bps,
                        -1,
                    );
                    current = target;
                    recovery_windows = 0;
                } else if target.bandwidth_bps > current.bandwidth_bps {
                    recovery_windows = recovery_windows.saturating_add(1);
                    if recovery_windows >= UPSHIFT_RECOVERY_WINDOWS {
                        record_shift(
                            &mut events,
                            &mut oscillation_count,
                            &mut last_direction,
                            segment_index,
                            AbrSimulationEventKind::Upshift,
                            target,
                            effective_bps,
                            1,
                        );
                        current = target;
                        recovery_windows = 0;
                    }
                } else {
                    recovery_windows = 0;
                }
            } else if manual_rung_id == Some(current.id.as_str()) {
                manual_override_respected = true;
            }

            if step.request_stall_ms > 0 {
                events.push(event(
                    segment_index,
                    AbrSimulationEventKind::RequestStall,
                    current,
                    effective_bps,
                ));
            }

            let request_seconds = request_seconds_for_segment(current, effective_bps, step);
            let overrun_seconds = (request_seconds - SEGMENT_SECONDS).max(0.0);
            if overrun_seconds > buffer_seconds {
                rebuffer_events += 1;
                buffer_seconds = 0.0;
                events.push(event(
                    segment_index,
                    AbrSimulationEventKind::Rebuffer,
                    current,
                    effective_bps,
                ));
            } else {
                buffer_seconds =
                    (buffer_seconds - overrun_seconds + SEGMENT_SECONDS).min(MAX_BUFFER_SECONDS);
            }
            segment_index += 1;
        }
    }

    AbrSimulationResult {
        initial_rung_id: initial.id.clone(),
        final_rung_id: current.id.clone(),
        events,
        rebuffer_events,
        oscillation_count,
        resumed_after_interruption,
        manual_override_respected,
    }
}

fn step(duration_segments: u8, bandwidth_bps: i64) -> NetworkEmulationStep {
    NetworkEmulationStep {
        duration_segments,
        bandwidth_bps,
        latency_ms: 0,
        jitter_ms: 0,
        packet_loss_per_mille: 0,
        request_stall_ms: 0,
        interrupted: false,
    }
}

fn sorted_rungs(ladder: &AdaptiveLadderPlan) -> Vec<&AdaptiveRungPlan> {
    let mut rungs = ladder.rungs.iter().collect::<Vec<_>>();
    rungs.sort_by_key(|rung| rung.bandwidth_bps);
    rungs
}

fn effective_bandwidth_bps(step: &NetworkEmulationStep) -> i64 {
    if step.interrupted || step.bandwidth_bps <= 0 {
        return 0;
    }
    let latency_penalty =
        1.0 - ((step.latency_ms + step.jitter_ms) as f64 / 2_000.0).clamp(0.0, 0.35);
    let loss_penalty = 1.0 - (step.packet_loss_per_mille as f64 / 1_000.0 * 4.0).clamp(0.0, 0.75);
    ((step.bandwidth_bps as f64) * latency_penalty * loss_penalty).round() as i64
}

fn select_rung<'a>(ordered: &[&'a AdaptiveRungPlan], effective_bps: i64) -> &'a AdaptiveRungPlan {
    let budget = ((effective_bps.max(0) as f64) * SAFETY_FACTOR).round() as i64;
    ordered
        .iter()
        .rev()
        .find(|rung| rung.bandwidth_bps <= budget)
        .copied()
        .unwrap_or(ordered[0])
}

fn request_seconds_for_segment(
    rung: &AdaptiveRungPlan,
    effective_bps: i64,
    step: &NetworkEmulationStep,
) -> f64 {
    if effective_bps <= 0 {
        return SEGMENT_SECONDS + START_BUFFER_SECONDS + 1.0;
    }
    let media_seconds = SEGMENT_SECONDS * rung.bandwidth_bps as f64 / effective_bps as f64;
    media_seconds
        + (step.latency_ms as f64 / 1_000.0)
        + (step.jitter_ms as f64 / 1_000.0)
        + (step.request_stall_ms as f64 / 1_000.0)
}

fn record_shift(
    events: &mut Vec<AbrSimulationEvent>,
    oscillation_count: &mut usize,
    last_direction: &mut Option<i8>,
    segment_index: usize,
    kind: AbrSimulationEventKind,
    rung: &AdaptiveRungPlan,
    effective_bps: i64,
    direction: i8,
) {
    if last_direction.is_some_and(|last| last != direction) {
        *oscillation_count += 1;
    }
    *last_direction = Some(direction);
    events.push(event(segment_index, kind, rung, effective_bps));
}

fn event(
    segment_index: usize,
    kind: AbrSimulationEventKind,
    rung: &AdaptiveRungPlan,
    effective_network_bps: i64,
) -> AbrSimulationEvent {
    AbrSimulationEvent {
        segment_index,
        kind,
        rung_id: rung.id.clone(),
        bandwidth_bps: rung.bandwidth_bps,
        effective_network_bps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::playback::plan::{
        AdaptiveAudioStrategy, Delivery, VideoFrameRateMode, VideoFrameRatePlan, VideoOutputPlan,
    };

    fn ladder() -> AdaptiveLadderPlan {
        AdaptiveLadderPlan {
            rungs: vec![
                rung("1080p", 8_000_000, 1920, 1080),
                rung("720p", 4_000_000, 1280, 720),
                rung("480p", 1_500_000, 854, 480),
                rung("360p", 800_000, 640, 360),
            ],
            starting_rung_id: "480p".to_string(),
            active_rung_id: "480p".to_string(),
            audio_strategy: AdaptiveAudioStrategy::PerRung,
            reasons: vec!["phase21_network_emulator_fixture".to_string()],
        }
    }

    fn rung(id: &str, bandwidth_bps: i64, width: i32, height: i32) -> AdaptiveRungPlan {
        AdaptiveRungPlan {
            id: id.to_string(),
            label: format!("{height}p {}k", bandwidth_bps / 1_000),
            bandwidth_bps,
            average_bandwidth_bps: bandwidth_bps * 90 / 100,
            width,
            height,
            resolution: format!("{width}x{height}"),
            codecs: "avc1.640029,mp4a.40.2".to_string(),
            frame_rate: Some("24".to_string()),
            video: VideoOutputPlan {
                codec: "h264".to_string(),
                encoder: "libx264".to_string(),
                preset: "veryfast".to_string(),
                profile: Some("high".to_string()),
                level: Some("4.1".to_string()),
                crf: None,
                bitrate_bps: Some(bandwidth_bps),
                maxrate_bps: Some(bandwidth_bps),
                bufsize_bps: Some(bandwidth_bps * 2),
                pixel_format: Some("yuv420p".to_string()),
                scale: None,
                tone_map: None,
                frame_rate: VideoFrameRatePlan {
                    mode: VideoFrameRateMode::Source,
                    source_fps: Some("24".to_string()),
                    target_fps: None,
                },
                gop_frames: Some(96),
                segment_seconds: "4".to_string(),
                keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
                hls_delivery: Delivery::HlsAdaptiveFmp4,
                burn_in: None,
                reasons: Vec::new(),
            },
        }
    }

    #[test]
    fn phase21_lan_no_loss_selects_highest_rung_without_buffering() {
        let result = simulate_abr_playback(
            &ladder(),
            &NetworkEmulationScenario::LanNoLoss.profile(),
            None,
        );

        assert_eq!(result.initial_rung_id, "1080p");
        assert_eq!(result.final_rung_id, "1080p");
        assert_eq!(result.rebuffer_events, 0);
    }

    #[test]
    fn phase21_wan_fixed_bandwidth_selects_bounded_initial_rung() {
        let result = simulate_abr_playback(
            &ladder(),
            &NetworkEmulationScenario::WanFixedBandwidth.profile(),
            None,
        );

        assert_eq!(result.initial_rung_id, "720p");
        assert_eq!(result.rebuffer_events, 0);
    }

    #[test]
    fn phase21_step_down_downshifts_before_repeated_rebuffering() {
        let result = simulate_abr_playback(
            &ladder(),
            &NetworkEmulationScenario::BandwidthStepDown.profile(),
            None,
        );

        assert!(
            result
                .events
                .iter()
                .any(|event| event.kind == AbrSimulationEventKind::Downshift)
        );
        assert!(result.final_rung_id == "480p" || result.final_rung_id == "360p");
        assert!(
            result.rebuffer_events <= 1,
            "downshift should prevent repeated buffering after adaptation: {result:?}"
        );
    }

    #[test]
    fn phase21_step_up_improves_quality_without_oscillation() {
        let result = simulate_abr_playback(
            &ladder(),
            &NetworkEmulationScenario::BandwidthStepUp.profile(),
            None,
        );

        assert_eq!(result.initial_rung_id, "480p");
        assert_ne!(result.final_rung_id, result.initial_rung_id);
        assert!(
            result
                .events
                .iter()
                .any(|event| event.kind == AbrSimulationEventKind::Upshift)
        );
        assert!(
            result
                .events
                .iter()
                .filter(|event| event.kind == AbrSimulationEventKind::Upshift)
                .last()
                .is_some_and(|event| event.bandwidth_bps > 1_500_000),
            "{result:?}"
        );
        assert!(
            result.oscillation_count <= 1,
            "upshift must not oscillate: {result:?}"
        );
    }

    #[test]
    fn phase21_manual_quality_override_holds_selected_rung() {
        let result = simulate_abr_playback(
            &ladder(),
            &NetworkEmulationScenario::LanNoLoss.profile(),
            Some("480p"),
        );

        assert_eq!(result.initial_rung_id, "480p");
        assert_eq!(result.final_rung_id, "480p");
        assert!(result.manual_override_respected);
        assert!(
            result
                .events
                .iter()
                .all(|event| event.kind != AbrSimulationEventKind::Upshift)
        );
    }

    #[test]
    fn phase21_latency_jitter_loss_stalls_and_resume_are_deterministic() {
        for scenario in [
            NetworkEmulationScenario::Latency,
            NetworkEmulationScenario::Jitter,
            NetworkEmulationScenario::PacketLoss,
            NetworkEmulationScenario::RequestStalls,
        ] {
            let result = simulate_abr_playback(&ladder(), &scenario.profile(), None);
            assert!(
                result.rebuffer_events <= 1,
                "{scenario:?} should remain bounded after adaptation: {result:?}"
            );
        }

        let interrupted = simulate_abr_playback(
            &ladder(),
            &NetworkEmulationScenario::InterruptedResume.profile(),
            None,
        );
        assert!(interrupted.resumed_after_interruption);
        assert!(
            interrupted
                .events
                .iter()
                .any(|event| event.kind == AbrSimulationEventKind::Resume)
        );
    }
}
