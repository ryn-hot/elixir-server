use crate::playback::{
    hardware::{HardwareApi, HardwareFallbackPolicy, HardwarePreference},
    plan::{
        AdaptiveAudioStrategy, AdaptiveLadderPlan, AdaptiveRungPlan, AudioOutputPlan,
        CompatibilityCheck, CompatibilityReport, Delivery, ExpectedOutput,
        HardwareAccelerationPlan, HdrAction, PLAYBACK_PLAN_VERSION, PlaybackFeasibilityAction,
        PlaybackFeasibilityDecision, PlaybackMode, PlaybackPerformanceConfidence,
        PlaybackPerformanceDecision, PlaybackPerformanceEnvelope, PlaybackPlan,
        PlaybackSupportDecision, PlaybackWorkloadClass, SeekBehavior, StreamAction,
        SubtitleBurnInMode, SubtitleBurnInPlan, VideoFrameRateMode, VideoFrameRatePlan,
        VideoOutputPlan, VideoScalePlan, VideoToneMapPlan,
    },
    probe::{
        AudioStreamCapabilities, MediaCapabilities, ProbeStatus, SubtitleKind,
        SubtitleStreamCapabilities, VideoStreamCapabilities, canonical_video_codec,
    },
    profile::{
        AssComplexitySupport, ClientKind, ClientPlaybackProfile, DefaultSubtitlePolicy,
        EffectivePlaybackPolicy, ForcedSubtitlePolicy, ImageSubtitleSupport, QualityMode,
        SubtitleBurnPolicy, SubtitleRendering, UnknownPerformancePolicy,
    },
};

const VIDEOTOOLBOX_H264_MIN_OUTPUT_WIDTH: i32 = 640;
const ADAPTIVE_TRANSCODE_CAPACITY_WEIGHT: u32 = 2;
const H264_LEVEL_LIMITS: &[H264LevelLimit] = &[
    H264LevelLimit {
        level: 30,
        max_macroblocks_per_second: 40_500,
        max_macroblocks_per_frame: 1_620,
    },
    H264LevelLimit {
        level: 31,
        max_macroblocks_per_second: 108_000,
        max_macroblocks_per_frame: 3_600,
    },
    H264LevelLimit {
        level: 40,
        max_macroblocks_per_second: 245_760,
        max_macroblocks_per_frame: 8_192,
    },
    H264LevelLimit {
        level: 41,
        max_macroblocks_per_second: 245_760,
        max_macroblocks_per_frame: 8_192,
    },
    H264LevelLimit {
        level: 42,
        max_macroblocks_per_second: 522_240,
        max_macroblocks_per_frame: 8_704,
    },
    H264LevelLimit {
        level: 50,
        max_macroblocks_per_second: 589_824,
        max_macroblocks_per_frame: 22_080,
    },
    H264LevelLimit {
        level: 51,
        max_macroblocks_per_second: 983_040,
        max_macroblocks_per_frame: 36_864,
    },
    H264LevelLimit {
        level: 52,
        max_macroblocks_per_second: 2_073_600,
        max_macroblocks_per_frame: 36_864,
    },
    H264LevelLimit {
        level: 60,
        max_macroblocks_per_second: 4_177_920,
        max_macroblocks_per_frame: 139_264,
    },
    H264LevelLimit {
        level: 61,
        max_macroblocks_per_second: 8_355_840,
        max_macroblocks_per_frame: 139_264,
    },
    H264LevelLimit {
        level: 62,
        max_macroblocks_per_second: 16_711_680,
        max_macroblocks_per_frame: 139_264,
    },
];

#[derive(Debug, Clone, Copy)]
struct H264LevelLimit {
    level: u8,
    max_macroblocks_per_second: i64,
    max_macroblocks_per_frame: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackSelection {
    pub audio_stream_index: Option<i32>,
    pub subtitle_stream_index: Option<i32>,
    pub subtitle_mode: SubtitleSelectionMode,
    pub preferred_subtitle_language: Option<String>,
    pub preferred_subtitle_title: Option<String>,
    pub start_position_seconds: Option<i32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SubtitleSelectionMode {
    Default,
    #[default]
    Off,
    Forced,
    Track,
}

pub fn plan_playback(
    media_file_id: impl Into<String>,
    media: &MediaCapabilities,
    selected: PlaybackSelection,
    client: &ClientPlaybackProfile,
    policy: &EffectivePlaybackPolicy,
) -> PlaybackPlan {
    let media_file_id = media_file_id.into();
    let mut report = CompatibilityReport::empty(&media_file_id);

    if media_file_id.trim().is_empty() {
        report.checks.push(CompatibilityCheck::fail(
            "media_file",
            "media_file_id_missing",
        ));
    } else {
        report.checks.push(CompatibilityCheck::pass("media_file"));
    }

    if matches!(selected.start_position_seconds, Some(seconds) if seconds < 0) {
        report.checks.push(CompatibilityCheck::fail(
            "start_position",
            "requested_start_position_invalid",
        ));
    } else if let (Some(start), Some(duration)) =
        (selected.start_position_seconds, media.duration_seconds)
    {
        if start as f64 > duration.ceil() {
            report.checks.push(CompatibilityCheck::fail(
                "start_position",
                "requested_start_position_out_of_range",
            ));
        } else {
            report
                .checks
                .push(CompatibilityCheck::pass("start_position"));
        }
    } else {
        report
            .checks
            .push(CompatibilityCheck::pass("start_position"));
    }

    let validation_reasons = report.failed_reasons();
    if !validation_reasons.is_empty() {
        return not_playable_plan(media_file_id, validation_reasons, report);
    }

    if media.probe_status != ProbeStatus::Ok {
        let reason = match media.probe_status {
            ProbeStatus::ProbeRequired => "probe_required",
            ProbeStatus::ProbeFailed => "probe_failed",
            ProbeStatus::Ok => unreachable!(),
        };
        report
            .checks
            .push(CompatibilityCheck::fail("probe", reason));
        return not_playable_plan(media_file_id, vec![reason.to_string()], report);
    }
    report.checks.push(CompatibilityCheck::pass("probe"));

    let Some(video) = media.primary_video() else {
        report
            .checks
            .push(CompatibilityCheck::fail("video", "probe_no_video_stream"));
        return not_playable_plan(
            media_file_id,
            vec!["probe_no_video_stream".to_string()],
            report,
        );
    };

    let audio = match selected.audio_stream_index {
        Some(index) => match media
            .audio_streams
            .iter()
            .find(|stream| stream.index == Some(index))
        {
            Some(stream) => Some(stream),
            None => {
                report.selected_audio_track = Some(index);
                report.checks.push(CompatibilityCheck::fail(
                    "audio_track",
                    "selected_audio_track_not_found",
                ));
                return not_playable_plan(
                    media_file_id,
                    vec!["selected_audio_track_not_found".to_string()],
                    report,
                );
            }
        },
        None => media.primary_audio(),
    };
    report.selected_audio_track = audio.and_then(|stream| stream.index);
    report.checks.push(CompatibilityCheck::pass("audio_track"));

    let subtitle = match select_subtitle_stream(media, audio, &selected, client) {
        Ok(subtitle) => subtitle,
        Err((index, reason)) => {
            report.selected_subtitle_track = index;
            report
                .checks
                .push(CompatibilityCheck::fail("subtitle_track", &reason));
            return not_playable_plan(media_file_id, vec![reason], report);
        }
    };
    report.selected_subtitle_track = subtitle.and_then(|stream| stream.index);
    report.requested_start_seconds = selected.start_position_seconds;
    report.source_container = media.container.canonical.clone();
    report.source_video_codec = video.codec.clone();
    report.source_audio_codec = audio.and_then(|stream| stream.codec.clone());
    report.source_subtitle_codec = subtitle.and_then(|stream| stream.codec.clone());
    report.source_width = video.width;
    report.source_height = video.height;

    let source_bitrate_bps = media
        .overall_bitrate_bps
        .or_else(|| video.bitrate_bps)
        .unwrap_or(0);
    report.source_bitrate_bps = (source_bitrate_bps > 0).then_some(source_bitrate_bps);
    let source_within_bitrate_policy = bitrate_allowed(source_bitrate_bps, policy.max_bitrate_bps);
    let source_within_resolution_policy =
        resolution_allowed(video.height, policy.max_resolution.as_deref());
    let hdr_action = hdr_action(video, client, policy);
    let direct_play_container_supported = can_direct_play_container(client, media);
    let direct_play_video_codec_supported =
        codec_allowed(video.codec.as_deref(), &client.supported_video_codecs);
    let direct_play_video_profile_supported = video_profile_allowed(video, client);
    let direct_play_video_level_supported = video_level_allowed(video, client);
    let direct_play_video_pixel_format_supported = video_pixel_format_allowed(video, client);
    let direct_play_video_supported = direct_play_video_codec_supported
        && direct_play_video_profile_supported
        && direct_play_video_level_supported
        && direct_play_video_pixel_format_supported;
    let direct_play_audio_supported = audio
        .map(|audio| codec_allowed(audio.codec.as_deref(), &client.supported_audio_codecs))
        .unwrap_or(true);
    let direct_play_audio_channels_supported = audio
        .map(|audio| audio_channels_allowed(audio, client))
        .unwrap_or(true);
    let subtitle_burn_in_requested = subtitle
        .map(|_| matches!(client.subtitle_burn_policy, SubtitleBurnPolicy::Always))
        .unwrap_or(false);
    let direct_play_subtitle_supported = subtitle
        .map(|subtitle| {
            !subtitle_burn_in_requested
                && can_deliver_subtitle(subtitle, Delivery::DirectFile, client)
        })
        .unwrap_or(true);

    push_check(
        &mut report,
        "container",
        direct_play_container_supported,
        "container_not_supported",
    );
    push_check(
        &mut report,
        "video",
        direct_play_video_codec_supported,
        "video_codec_not_supported",
    );
    push_check(
        &mut report,
        "video_profile",
        direct_play_video_profile_supported,
        "video_profile_not_supported",
    );
    push_check(
        &mut report,
        "video_level",
        direct_play_video_level_supported,
        "video_level_not_supported",
    );
    push_check(
        &mut report,
        "pixel_format",
        direct_play_video_pixel_format_supported,
        "video_pixel_format_not_supported",
    );
    push_check(
        &mut report,
        "audio",
        direct_play_audio_supported,
        "audio_codec_not_supported",
    );
    push_check(
        &mut report,
        "audio_channels",
        direct_play_audio_channels_supported,
        "audio_channel_count_not_supported",
    );
    push_check(
        &mut report,
        "subtitles",
        direct_play_subtitle_supported,
        if subtitle_burn_in_requested {
            "subtitle_burn_in_requested"
        } else if subtitle.is_some_and(|subtitle| subtitle.kind == SubtitleKind::Image) {
            "subtitle_requires_burn_in"
        } else {
            "subtitle_not_supported"
        },
    );
    push_check(
        &mut report,
        "hdr",
        !hdr_blocks_direct_play(hdr_action),
        "hdr_tone_mapping_required",
    );
    push_check(
        &mut report,
        "bitrate",
        source_within_bitrate_policy,
        "source_bitrate_exceeds_policy",
    );
    push_check(
        &mut report,
        "resolution",
        source_within_resolution_policy,
        "resolution_exceeds_policy",
    );
    push_check(
        &mut report,
        "delivery_protocol",
        !client.supported_hls_segment_types.is_empty(),
        "hls_delivery_not_supported",
    );
    push_check(
        &mut report,
        "policy",
        policy.allow_direct_play
            || policy.allow_direct_stream
            || policy.allow_audio_transcode
            || policy.allow_video_transcode,
        "transcode_disabled",
    );

    let mut blockers = Vec::new();
    if !policy.allow_direct_play {
        push_unique(&mut blockers, "direct_play_disabled_by_policy");
    }
    if !direct_play_container_supported {
        push_unique(&mut blockers, "container_not_supported");
        push_unique(&mut blockers, "container_not_direct_playable");
    }
    if !direct_play_video_codec_supported {
        push_unique(&mut blockers, "video_codec_not_supported");
        push_unique(&mut blockers, "video_codec_not_direct_playable");
    }
    if !direct_play_video_profile_supported {
        push_unique(&mut blockers, "video_profile_not_supported");
        push_unique(&mut blockers, "video_profile_not_direct_playable");
    }
    if !direct_play_video_level_supported {
        push_unique(&mut blockers, "video_level_not_supported");
        push_unique(&mut blockers, "video_level_not_direct_playable");
    }
    if !direct_play_video_pixel_format_supported {
        push_unique(&mut blockers, "video_pixel_format_not_supported");
        push_unique(&mut blockers, "video_pixel_format_not_direct_playable");
    }
    if !direct_play_audio_supported {
        push_unique(&mut blockers, "audio_codec_not_supported");
        push_unique(&mut blockers, "audio_codec_not_direct_playable");
    }
    if !direct_play_audio_channels_supported {
        push_unique(&mut blockers, "audio_channel_count_not_supported");
        push_unique(&mut blockers, "audio_channel_count_not_direct_playable");
    }
    if !source_within_resolution_policy {
        push_unique(&mut blockers, "resolution_exceeds_policy");
    }
    if !source_within_bitrate_policy {
        push_unique(&mut blockers, "source_bitrate_exceeds_policy");
        push_unique(&mut blockers, "source_bitrate_exceeds_bandwidth_policy");
    }
    if hdr_blocks_direct_play(hdr_action) {
        push_unique(&mut blockers, hdr_blocker_reason(hdr_action));
        push_hdr_detail_reasons(&mut blockers, video, hdr_action);
    }
    if !direct_play_subtitle_supported {
        push_unique(
            &mut blockers,
            if subtitle_burn_in_requested {
                "subtitle_burn_in_requested"
            } else if subtitle.is_some_and(|subtitle| subtitle.kind == SubtitleKind::Image) {
                "subtitle_requires_burn_in"
            } else {
                "subtitle_not_supported"
            },
        );
        push_unique(&mut blockers, "subtitle_not_direct_playable");
    }

    let adaptive_delivery = preferred_hls_delivery(client, true);
    let adaptive_audio_output_candidate =
        audio.and_then(|audio| planned_audio_output(audio, client, adaptive_delivery));
    let adaptive_ladder_candidate = if adaptive_transcode_can_be_considered(client, policy) {
        match planned_adaptive_ladder(
            media,
            video,
            subtitle,
            video_transcode_subtitle_action(subtitle, client),
            hdr_action,
            adaptive_delivery,
            adaptive_audio_output_candidate.as_ref(),
            source_bitrate_bps,
            &blockers,
            primary_video_transcode_reason(&blockers).as_deref(),
            policy,
        ) {
            Ok(ladder) => Some(ladder),
            Err(_) => None,
        }
    } else {
        None
    };
    if adaptive_ladder_candidate.is_some() && !adaptive_transcode_capacity_available(policy) {
        let mut reasons = blockers;
        push_unique(
            &mut reasons,
            "adaptive_transcode_automatic_quality_requested",
        );
        push_unique(&mut reasons, "transcode_capacity_exhausted");
        push_unique(&mut reasons, "adaptive_transcode_capacity_exhausted");
        return not_playable_plan(media_file_id, reasons, report);
    }

    if let Some(mut adaptive_ladder) = adaptive_ladder_candidate {
        let video_delivery = adaptive_delivery;
        let video_audio_output = adaptive_audio_output_candidate;
        let subtitle_action = video_transcode_subtitle_action(subtitle, client);
        let mut video_output = adaptive_ladder.rungs[0].video.clone();
        let (hardware_acceleration, hardware_warnings) =
            match planned_hardware_acceleration(video, &mut video_output, policy) {
                Ok(selection) => selection,
                Err(reason) => {
                    let mut reasons = blockers;
                    push_unique(&mut reasons, &reason);
                    return not_playable_plan(media_file_id, reasons, report);
                }
            };
        if !software_decode_supported_for_plan(video, &hardware_acceleration) {
            let mut reasons = blockers;
            push_unique(&mut reasons, "software_decode_unsupported");
            return not_playable_plan(media_file_id, reasons, report);
        }
        if video_output.encoder != adaptive_ladder.rungs[0].video.encoder {
            for rung in adaptive_ladder.rungs.iter_mut() {
                rung.video.encoder = video_output.encoder.clone();
                push_unique(
                    &mut rung.video.reasons,
                    &format!("hardware_encoder_selected:{}", video_output.encoder),
                );
            }
        }
        adaptive_ladder.active_rung_id = adaptive_ladder.starting_rung_id.clone();
        let mut reasons = blockers;
        push_unique(
            &mut reasons,
            "adaptive_transcode_automatic_quality_requested",
        );
        for reason in &adaptive_ladder.reasons {
            push_unique(&mut reasons, reason);
        }
        let video_transcode_reason = primary_video_transcode_reason(&reasons);
        let mut plan = hls_plan(
            media_file_id,
            PlaybackMode::AdaptiveTranscode,
            video_delivery,
            StreamAction::Transcode,
            if audio.is_some() {
                StreamAction::Transcode
            } else {
                StreamAction::Disabled
            },
            subtitle_action,
            video.index,
            audio.and_then(|stream| stream.index),
            subtitle.and_then(|stream| stream.index),
            hdr_action,
            report,
            reasons,
            video_audio_output,
            Some(video_output),
            Some(adaptive_ladder),
            video_transcode_reason,
        );
        plan.hardware_acceleration = hardware_acceleration;
        plan.warnings = hardware_warnings;
        plan.expected_outputs = hls_expected_outputs_for_plan(&plan);
        return apply_runtime_feasibility(plan, media, video, audio, subtitle, policy);
    }

    if blockers.is_empty() {
        let mut reasons = vec!["direct_play_all_capabilities_satisfied".to_string()];
        push_hdr_action_reason(&mut reasons, hdr_action);
        push_hdr_detail_reasons(&mut reasons, video, hdr_action);
        let plan = PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode: PlaybackMode::DirectPlay,
            delivery: Delivery::DirectFile,
            media_file_id,
            selected_video_track: video.index,
            video_action: StreamAction::Passthrough,
            audio_action: if audio.is_some() {
                StreamAction::Passthrough
            } else {
                StreamAction::Disabled
            },
            subtitle_action: if subtitle.is_some() {
                StreamAction::Passthrough
            } else {
                StreamAction::Disabled
            },
            seek_behavior: SeekBehavior::ClientRange,
            adaptive: false,
            selected_audio_track: audio.and_then(|stream| stream.index),
            selected_subtitle_track: subtitle.and_then(|stream| stream.index),
            hdr_action,
            hardware_acceleration: HardwareAccelerationPlan::default(),
            audio_output: None,
            video_output: None,
            adaptive_ladder: None,
            video_transcode_reason: None,
            workload_class: None,
            feasibility: None,
            compatibility_report: report,
            reasons,
            warnings: Vec::new(),
            expected_outputs: direct_file_outputs(),
            playable: true,
        };
        return apply_runtime_feasibility(plan, media, video, audio, subtitle, policy);
    }

    let delivery = preferred_hls_delivery(client, false);
    let direct_stream_delivery = preferred_copy_hls_delivery(video, audio, client, false);
    let hls_video_copyable =
        direct_play_video_supported && can_copy_video_to_delivery(video, delivery);
    let hls_audio_copyable = audio
        .map(|audio| {
            codec_allowed(audio.codec.as_deref(), &client.supported_audio_codecs)
                && can_copy_audio_to_delivery(audio, delivery)
                && audio_channels_allowed(audio, client)
        })
        .unwrap_or(true);
    let hls_subtitle_copyable = subtitle
        .map(|subtitle| can_copy_subtitle_to_delivery(subtitle, delivery, client))
        .unwrap_or(true);
    let hls_subtitle_convertible = subtitle
        .map(|subtitle| can_convert_text_subtitle_to_webvtt(subtitle, client))
        .unwrap_or(false);
    let hls_subtitle_deliverable_without_video =
        !subtitle_burn_in_requested && (hls_subtitle_copyable || hls_subtitle_convertible);
    let hls_policy_ok = source_within_bitrate_policy
        && source_within_resolution_policy
        && !hdr_blocks_direct_play(hdr_action);

    if let Some(direct_stream_delivery) = direct_stream_delivery.filter(|_| {
        policy.allow_direct_stream
            && source_within_bitrate_policy
            && source_within_resolution_policy
            && direct_play_video_supported
            && hls_audio_copyable
            && subtitle.is_none()
            && hls_policy_ok
    }) {
        let plan = hls_plan(
            media_file_id,
            PlaybackMode::DirectStream,
            direct_stream_delivery,
            StreamAction::Copy,
            if audio.is_some() {
                StreamAction::Copy
            } else {
                StreamAction::Disabled
            },
            StreamAction::Disabled,
            video.index,
            audio.and_then(|stream| stream.index),
            None,
            hdr_action,
            report,
            vec!["direct_stream_codecs_copyable_container_changed".to_string()],
            None,
            None,
            None,
            None,
        );
        return apply_runtime_feasibility(plan, media, video, audio, subtitle, policy);
    }

    let audio_transcode_required = audio
        .map(|audio| {
            !codec_allowed(audio.codec.as_deref(), &client.supported_audio_codecs)
                || !can_copy_audio_to_delivery(audio, delivery)
                || !audio_channels_allowed(audio, client)
        })
        .unwrap_or(false);
    let audio_output = audio.and_then(|audio| planned_audio_output(audio, client, delivery));
    if policy.allow_audio_transcode
        && source_within_bitrate_policy
        && source_within_resolution_policy
        && hls_video_copyable
        && audio.is_some()
        && audio_transcode_required
        && hls_subtitle_deliverable_without_video
        && hls_policy_ok
    {
        let mut reasons =
            vec!["audio_transcode_video_copyable_audio_codec_not_supported".to_string()];
        if let Some(output) = audio_output.as_ref() {
            for reason in &output.reasons {
                push_unique(&mut reasons, reason);
            }
        }
        let plan = hls_plan(
            media_file_id,
            PlaybackMode::AudioTranscode,
            delivery,
            StreamAction::Copy,
            StreamAction::Transcode,
            subtitle_action(subtitle, delivery, client),
            video.index,
            audio.and_then(|stream| stream.index),
            subtitle.and_then(|stream| stream.index),
            hdr_action,
            report,
            reasons,
            audio_output,
            None,
            None,
            None,
        );
        return apply_runtime_feasibility(plan, media, video, audio, subtitle, policy);
    }

    let subtitle_transcode_required = subtitle
        .map(|subtitle| {
            !subtitle_burn_in_requested
                && subtitle.kind == SubtitleKind::Text
                && can_convert_text_subtitle_to_webvtt(subtitle, client)
        })
        .unwrap_or(false);
    if source_within_bitrate_policy
        && source_within_resolution_policy
        && hls_video_copyable
        && hls_audio_copyable
        && subtitle_transcode_required
        && hls_policy_ok
    {
        let plan = hls_plan(
            media_file_id,
            PlaybackMode::SubtitleTranscode,
            delivery,
            StreamAction::Copy,
            if audio.is_some() {
                StreamAction::Copy
            } else {
                StreamAction::Disabled
            },
            StreamAction::ConvertTextToWebvtt,
            video.index,
            audio.and_then(|stream| stream.index),
            subtitle.and_then(|stream| stream.index),
            hdr_action,
            report,
            vec!["subtitle_transcode_text_to_webvtt".to_string()],
            None,
            None,
            None,
            None,
        );
        return apply_runtime_feasibility(plan, media, video, audio, subtitle, policy);
    }

    if !policy.allow_video_transcode {
        let mut reasons = blockers;
        push_unique(&mut reasons, "transcode_disabled");
        push_unique(&mut reasons, "video_transcode_disabled_by_policy");
        return not_playable_plan(media_file_id, reasons, report);
    }

    if !video_transcode_capacity_available(policy) {
        let mut reasons = blockers;
        push_unique(&mut reasons, "transcode_capacity_exhausted");
        return not_playable_plan(media_file_id, reasons, report);
    }

    let mode = PlaybackMode::VideoTranscode;
    let mut reasons = blockers;
    push_unique(&mut reasons, "video_transcode_required");
    let video_transcode_reason = primary_video_transcode_reason(&reasons);

    let video_delivery = preferred_hls_delivery(client, mode == PlaybackMode::AdaptiveTranscode);
    let video_audio_output =
        audio.and_then(|audio| planned_audio_output(audio, client, video_delivery));
    let subtitle_action = video_transcode_subtitle_action(subtitle, client);
    let mut video_output = match planned_video_output(
        media,
        video,
        subtitle,
        subtitle_action,
        hdr_action,
        video_delivery,
        &reasons,
        video_transcode_reason.as_deref(),
        policy,
    ) {
        Ok(output) => output,
        Err(reason) => {
            let mut reasons = reasons;
            push_unique(&mut reasons, &reason);
            return not_playable_plan(media_file_id, reasons, report);
        }
    };
    let (hardware_acceleration, hardware_warnings) =
        match planned_hardware_acceleration(video, &mut video_output, policy) {
            Ok(selection) => selection,
            Err(reason) => {
                let mut reasons = reasons;
                push_unique(&mut reasons, &reason);
                return not_playable_plan(media_file_id, reasons, report);
            }
        };
    if !software_decode_supported_for_plan(video, &hardware_acceleration) {
        let mut reasons = reasons;
        push_unique(&mut reasons, "software_decode_unsupported");
        return not_playable_plan(media_file_id, reasons, report);
    }

    let mut plan = hls_plan(
        media_file_id,
        mode,
        video_delivery,
        StreamAction::Transcode,
        if audio.is_some() {
            StreamAction::Transcode
        } else {
            StreamAction::Disabled
        },
        subtitle_action,
        video.index,
        audio.and_then(|stream| stream.index),
        subtitle.and_then(|stream| stream.index),
        hdr_action,
        report,
        reasons,
        video_audio_output,
        Some(video_output),
        None,
        video_transcode_reason,
    );
    plan.hardware_acceleration = hardware_acceleration;
    plan.warnings = hardware_warnings;
    apply_runtime_feasibility(plan, media, video, audio, subtitle, policy)
}

pub fn can_direct_play_container(
    client: &ClientPlaybackProfile,
    media: &MediaCapabilities,
) -> bool {
    let Some(container) = media.container.canonical.as_deref() else {
        return false;
    };
    client
        .supported_containers
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(container))
}

fn select_subtitle_stream<'a>(
    media: &'a MediaCapabilities,
    audio: Option<&AudioStreamCapabilities>,
    selected: &PlaybackSelection,
    client: &ClientPlaybackProfile,
) -> Result<Option<&'a SubtitleStreamCapabilities>, (Option<i32>, String)> {
    if let Some(index) = selected.subtitle_stream_index {
        return media
            .subtitle_streams
            .iter()
            .find(|stream| stream.index == Some(index))
            .map(Some)
            .ok_or_else(|| (Some(index), "selected_subtitle_track_not_found".to_string()));
    }

    if selected.subtitle_mode == SubtitleSelectionMode::Off {
        return Ok(None);
    }

    if selected.subtitle_mode == SubtitleSelectionMode::Track {
        return Ok(select_preferred_subtitle(media, selected));
    }

    if !matches!(
        client.forced_subtitle_policy,
        ForcedSubtitlePolicy::Disabled
    ) {
        if let Some(forced) = select_forced_subtitle(media, audio, client.forced_subtitle_policy) {
            return Ok(Some(forced));
        }
    }

    if selected.subtitle_mode == SubtitleSelectionMode::Forced {
        return Ok(None);
    }

    if !matches!(
        client.default_subtitle_policy,
        DefaultSubtitlePolicy::Disabled
    ) {
        if let Some(preferred) = select_preferred_subtitle(media, selected) {
            return Ok(Some(preferred));
        }
        if let Some(default) = media
            .subtitle_streams
            .iter()
            .find(|stream| stream.is_default && !stream.is_forced)
        {
            return Ok(Some(default));
        }
    }

    Ok(None)
}

fn select_forced_subtitle<'a>(
    media: &'a MediaCapabilities,
    audio: Option<&AudioStreamCapabilities>,
    policy: ForcedSubtitlePolicy,
) -> Option<&'a SubtitleStreamCapabilities> {
    let audio_language = audio.and_then(|stream| stream.language.as_deref());
    media
        .subtitle_streams
        .iter()
        .filter(|stream| stream.is_forced)
        .find(|stream| match policy {
            ForcedSubtitlePolicy::Disabled => false,
            ForcedSubtitlePolicy::Any => true,
            ForcedSubtitlePolicy::MatchingAudio => {
                language_matches(stream.language.as_deref(), audio_language)
            }
        })
}

fn select_preferred_subtitle<'a>(
    media: &'a MediaCapabilities,
    selected: &PlaybackSelection,
) -> Option<&'a SubtitleStreamCapabilities> {
    let preferred_language = selected
        .preferred_subtitle_language
        .as_deref()
        .map(normalize_language_key)
        .filter(|value| !value.is_empty());
    let preferred_title = selected
        .preferred_subtitle_title
        .as_deref()
        .map(normalize_title_key)
        .filter(|value| !value.is_empty());

    if preferred_language.is_none() && preferred_title.is_none() {
        return None;
    }

    media.subtitle_streams.iter().find(|stream| {
        preferred_language
            .as_deref()
            .map(|language| {
                normalize_language_key(stream.language.as_deref().unwrap_or_default()) == language
            })
            .unwrap_or(true)
            && preferred_title
                .as_deref()
                .map(|title| {
                    normalize_title_key(stream.title.as_deref().unwrap_or_default()) == title
                })
                .unwrap_or(true)
    })
}

fn language_matches(subtitle_language: Option<&str>, audio_language: Option<&str>) -> bool {
    let Some(subtitle_language) = subtitle_language.map(normalize_language_key) else {
        return false;
    };
    let Some(audio_language) = audio_language.map(normalize_language_key) else {
        return false;
    };
    !subtitle_language.is_empty() && subtitle_language == audio_language
}

fn normalize_language_key(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "english" => "eng".to_string(),
        "en" => "eng".to_string(),
        "japanese" => "jpn".to_string(),
        "ja" => "jpn".to_string(),
        "spanish" => "spa".to_string(),
        "es" => "spa".to_string(),
        "french" => "fra".to_string(),
        "fr" => "fra".to_string(),
        _ => normalized,
    }
}

fn normalize_title_key(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn can_copy_video_to_delivery(video: &VideoStreamCapabilities, delivery: Delivery) -> bool {
    let Some(codec) = video.codec.as_deref() else {
        return false;
    };
    match delivery {
        Delivery::DirectFile => true,
        Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts => {
            matches!(codec, "h264" | "mpeg2video")
        }
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => matches!(codec, "h264" | "hevc" | "av1"),
    }
}

fn software_decode_supported_for_plan(
    video: &VideoStreamCapabilities,
    hardware_acceleration: &HardwareAccelerationPlan,
) -> bool {
    hardware_acceleration.decoder.is_some()
        || software_video_decode_supported(video.codec.as_deref())
}

fn software_video_decode_supported(codec: Option<&str>) -> bool {
    let Some(codec) = codec else {
        return false;
    };
    let codec = canonical_video_codec(codec);
    matches!(
        codec.as_str(),
        "h264"
            | "hevc"
            | "av1"
            | "vp9"
            | "vp8"
            | "mpeg2video"
            | "mpeg4"
            | "msmpeg4v3"
            | "vc1"
            | "prores"
            | "theora"
            | "mjpeg"
            | "jpeg2000"
            | "h263"
            | "h263p"
            | "dnxhd"
            | "ffv1"
            | "rawvideo"
    )
}

pub fn can_copy_audio_to_delivery(audio: &AudioStreamCapabilities, delivery: Delivery) -> bool {
    let Some(codec) = audio.codec.as_deref() else {
        return false;
    };
    can_mux_audio_codec_to_delivery(codec, delivery)
}

fn can_mux_audio_codec_to_delivery(codec: &str, delivery: Delivery) -> bool {
    let codec = codec.to_ascii_lowercase();
    match delivery {
        Delivery::DirectFile => true,
        Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts => {
            matches!(codec.as_str(), "aac" | "ac3" | "eac3")
        }
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => {
            matches!(codec.as_str(), "aac" | "ac3" | "eac3" | "opus")
        }
    }
}

fn audio_channels_allowed(audio: &AudioStreamCapabilities, client: &ClientPlaybackProfile) -> bool {
    match (audio.channels, client.max_audio_channels) {
        (Some(channels), Some(max_channels)) if channels > 0 && max_channels > 0 => {
            channels <= max_channels
        }
        _ => true,
    }
}

fn planned_audio_output(
    audio: &AudioStreamCapabilities,
    client: &ClientPlaybackProfile,
    delivery: Delivery,
) -> Option<AudioOutputPlan> {
    let codec = select_audio_output_codec(client, delivery)?;
    let source_channels = audio.channels.filter(|channels| *channels > 0).unwrap_or(2);
    let channels = target_audio_channels(source_channels, client);
    let bitrate_bps = target_audio_bitrate_bps(&codec, channels);
    let mut reasons = Vec::new();

    let source_codec = audio.codec.as_deref().unwrap_or_default();
    if !source_codec.eq_ignore_ascii_case(&codec)
        || !codec_allowed(audio.codec.as_deref(), &client.supported_audio_codecs)
        || !can_copy_audio_to_delivery(audio, delivery)
    {
        push_unique(&mut reasons, "audio_codec_conversion_required");
    }
    if channels < source_channels {
        push_unique(&mut reasons, "audio_channel_downmix_required");
    }
    if audio
        .bitrate_bps
        .zip(bitrate_bps)
        .is_some_and(|(source, target)| source > target)
    {
        push_unique(&mut reasons, "audio_bitrate_cap_applied");
    }

    Some(AudioOutputPlan {
        codec,
        channels: Some(channels),
        bitrate_bps,
        language: audio.language.clone(),
        title: audio.title.clone(),
        reasons,
    })
}

fn select_audio_output_codec(client: &ClientPlaybackProfile, delivery: Delivery) -> Option<String> {
    if codec_allowed(Some("aac"), &client.supported_audio_codecs)
        && can_mux_audio_codec_to_delivery("aac", delivery)
    {
        return Some("aac".to_string());
    }

    for codec in ["eac3", "ac3"] {
        if codec_allowed(Some(codec), &client.supported_audio_codecs)
            && can_mux_audio_codec_to_delivery(codec, delivery)
        {
            return Some(codec.to_string());
        }
    }

    can_mux_audio_codec_to_delivery("aac", delivery).then_some("aac".to_string())
}

fn target_audio_channels(source_channels: i32, client: &ClientPlaybackProfile) -> i32 {
    let capped = match client.max_audio_channels {
        Some(max_channels) if max_channels > 0 => source_channels.min(max_channels),
        _ => source_channels.min(6),
    };
    capped.max(1)
}

fn target_audio_bitrate_bps(codec: &str, channels: i32) -> Option<i64> {
    match codec {
        "aac" if channels > 2 => Some(384_000),
        "aac" => Some(128_000),
        "ac3" if channels > 2 => Some(448_000),
        "ac3" => Some(192_000),
        "eac3" if channels > 2 => Some(640_000),
        "eac3" => Some(192_000),
        _ => None,
    }
}

fn planned_tone_map(
    video: &VideoStreamCapabilities,
    hdr_action: HdrAction,
) -> Option<VideoToneMapPlan> {
    if !matches!(hdr_action, HdrAction::ToneMapToSdr) {
        return None;
    }

    Some(VideoToneMapPlan {
        algorithm: "hable".to_string(),
        input_primaries: tone_map_input_primaries(video),
        input_transfer: tone_map_input_transfer(video),
        input_matrix: tone_map_input_matrix(video),
        output_primaries: "bt709".to_string(),
        output_transfer: "bt709".to_string(),
        output_matrix: "bt709".to_string(),
    })
}

fn tone_map_input_primaries(video: &VideoStreamCapabilities) -> Option<String> {
    normalize_zscale_primaries(video.color_primaries.as_deref()).or_else(|| {
        (video.dolby_vision || video.hdr10 || video.hdr10_plus).then(|| "bt2020".to_string())
    })
}

fn tone_map_input_transfer(video: &VideoStreamCapabilities) -> Option<String> {
    normalize_zscale_transfer(video.color_transfer.as_deref()).or_else(|| {
        (video.dolby_vision || video.hdr10 || video.hdr10_plus).then(|| "smpte2084".to_string())
    })
}

fn tone_map_input_matrix(video: &VideoStreamCapabilities) -> Option<String> {
    if video.dolby_vision && !video.dolby_vision_has_hdr10_fallback {
        return Some("ictcp".to_string());
    }
    normalize_zscale_matrix(video.color_matrix.as_deref()).or_else(|| {
        (video.dolby_vision || video.hdr10 || video.hdr10_plus).then(|| "bt2020nc".to_string())
    })
}

fn normalize_zscale_primaries(raw: Option<&str>) -> Option<String> {
    let value = raw?.trim().to_ascii_lowercase();
    match value.as_str() {
        "" | "unknown" | "unspecified" => None,
        "bt2020" | "2020" => Some("bt2020".to_string()),
        "bt709" | "709" => Some("bt709".to_string()),
        "smpte170m" | "170m" => Some("smpte170m".to_string()),
        "smpte240m" | "240m" => Some("smpte240m".to_string()),
        "bt470m" | "bt470bg" | "film" | "smpte428" | "smpte431" | "smpte432" => Some(value),
        _ => None,
    }
}

fn normalize_zscale_transfer(raw: Option<&str>) -> Option<String> {
    let value = raw?.trim().to_ascii_lowercase();
    match value.as_str() {
        "" | "unknown" | "unspecified" => None,
        "smpte2084" | "pq" => Some("smpte2084".to_string()),
        "arib-std-b67" | "hlg" => Some("arib-std-b67".to_string()),
        "bt709" | "709" => Some("bt709".to_string()),
        "bt2020-10" | "bt2020_10" | "2020_10" => Some("bt2020-10".to_string()),
        "bt2020-12" | "bt2020_12" | "2020_12" => Some("bt2020-12".to_string()),
        "linear" | "smpte170m" | "smpte240m" | "bt470m" | "bt470bg" => Some(value),
        _ => None,
    }
}

fn normalize_zscale_matrix(raw: Option<&str>) -> Option<String> {
    let value = raw?.trim().to_ascii_lowercase();
    match value.as_str() {
        "" | "unknown" | "unspecified" => None,
        "bt2020nc" | "bt2020_ncl" | "2020_ncl" => Some("bt2020nc".to_string()),
        "bt2020c" | "bt2020_cl" | "2020_cl" => Some("bt2020c".to_string()),
        "bt709" | "709" => Some("bt709".to_string()),
        "ictcp" => Some("ictcp".to_string()),
        "gbr" | "fcc" | "bt470bg" | "smpte170m" | "smpte240m" | "ycgco" => Some(value),
        _ => None,
    }
}

fn planned_video_output(
    media: &MediaCapabilities,
    video: &VideoStreamCapabilities,
    subtitle: Option<&SubtitleStreamCapabilities>,
    subtitle_action: StreamAction,
    hdr_action: HdrAction,
    delivery: Delivery,
    reasons: &[String],
    primary_reason: Option<&str>,
    policy: &EffectivePlaybackPolicy,
) -> Result<VideoOutputPlan, String> {
    let scale = target_video_scale(video, policy.max_resolution.as_deref());
    let tone_map = planned_tone_map(video, hdr_action);
    let output_width = scale
        .as_ref()
        .map(|scale| scale.width)
        .or(video.width)
        .filter(|width| *width > 0);
    let output_height = scale
        .as_ref()
        .map(|scale| scale.height)
        .or(video.height)
        .filter(|height| *height > 0);
    let target_bitrate = target_video_bitrate_bps(
        output_height,
        policy.max_bitrate_bps,
        scale.is_some(),
        reasons,
    );
    let maxrate_bps = target_bitrate;
    let bufsize_bps = target_bitrate.map(|bitrate| {
        bitrate.saturating_mul(policy.video_encoder_bufsize_multiplier.max(1) as i64)
    });
    let frame_rate = planned_frame_rate(video);
    let fps_for_gop = frame_rate
        .target_fps
        .as_deref()
        .or(frame_rate.source_fps.as_deref())
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(24.0);
    let segment_seconds = 4;
    let gop_frames = ((fps_for_gop * segment_seconds as f64).round() as i32).clamp(12, 300);
    if subtitle.is_some() && subtitle_action == StreamAction::Disabled {
        return Err("selected_subtitle_unsupported_by_client_profile".to_string());
    }
    let burn_in = planned_burn_in(media, subtitle, subtitle_action)?;
    let mut output_reasons = Vec::new();
    for reason in reasons {
        push_unique(&mut output_reasons, reason);
    }
    if matches!(
        hdr_action,
        HdrAction::Unsupported | HdrAction::UnknownFailClosed
    ) {
        return Err(hdr_blocker_reason(hdr_action).to_string());
    }
    if matches!(hdr_action, HdrAction::ToneMapToSdr) {
        push_unique(&mut output_reasons, "hdr_to_sdr_required");
    }
    if burn_in.is_some() {
        push_unique(&mut output_reasons, "subtitle_burn_in_required");
    }
    if let Some(primary_reason) = primary_reason {
        push_unique(
            &mut output_reasons,
            &format!("video_transcode_reason:{primary_reason}"),
        );
    }
    let level = planned_h264_level(
        &policy.video_encoder_level,
        output_width,
        output_height,
        fps_for_gop,
        &mut output_reasons,
    );

    Ok(VideoOutputPlan {
        codec: "h264".to_string(),
        encoder: "libx264".to_string(),
        preset: clean_or_default(&policy.video_encoder_preset, "veryfast"),
        profile: non_empty_string(&policy.video_encoder_profile),
        level,
        crf: target_bitrate
            .is_none()
            .then_some(policy.video_encoder_crf.clamp(0, 51)),
        bitrate_bps: target_bitrate,
        maxrate_bps,
        bufsize_bps,
        pixel_format: Some("yuv420p".to_string()),
        scale,
        tone_map,
        frame_rate,
        gop_frames: Some(gop_frames),
        segment_seconds: segment_seconds.to_string(),
        keyframe_expression: format!("expr:gte(t,n_forced*{segment_seconds})"),
        hls_delivery: delivery,
        burn_in,
        reasons: output_reasons,
    })
}

fn adaptive_transcode_can_be_considered(
    client: &ClientPlaybackProfile,
    policy: &EffectivePlaybackPolicy,
) -> bool {
    client.quality_mode == QualityMode::Automatic
        && policy.allow_adaptive_transcode
        && client.abr_support_type.supports_adaptive_hls()
        && policy.abr_support_type.supports_adaptive_hls()
        && policy.allow_video_transcode
        && !direct_play_original_quality_required(client, policy)
}

fn direct_play_original_quality_required(
    client: &ClientPlaybackProfile,
    policy: &EffectivePlaybackPolicy,
) -> bool {
    client.quality_mode == QualityMode::Original
        || (client.client_kind == ClientKind::NativeMpv && policy.force_direct_play_for_native_mpv)
}

fn planned_adaptive_ladder(
    media: &MediaCapabilities,
    video: &VideoStreamCapabilities,
    subtitle: Option<&SubtitleStreamCapabilities>,
    subtitle_action: StreamAction,
    hdr_action: HdrAction,
    delivery: Delivery,
    audio_output: Option<&AudioOutputPlan>,
    source_bitrate_bps: i64,
    reasons: &[String],
    primary_reason: Option<&str>,
    policy: &EffectivePlaybackPolicy,
) -> Result<AdaptiveLadderPlan, String> {
    let source_width = video
        .width
        .filter(|width| *width > 0)
        .ok_or_else(|| "adaptive_ladder_source_resolution_unknown".to_string())?;
    let source_height = video
        .height
        .filter(|height| *height > 0)
        .ok_or_else(|| "adaptive_ladder_source_resolution_unknown".to_string())?;
    let source_bitrate_bps = (source_bitrate_bps > 0)
        .then_some(source_bitrate_bps)
        .ok_or_else(|| "adaptive_ladder_source_bitrate_unknown".to_string())?;
    let (total_bitrate_cap_bps, mut ladder_reasons) =
        adaptive_bitrate_cap_bps(source_bitrate_bps, policy);
    let audio_bitrate_bps = audio_output
        .and_then(|audio| audio.bitrate_bps)
        .filter(|bitrate| *bitrate > 0)
        .unwrap_or(0);
    let max_video_bitrate_bps = total_bitrate_cap_bps.saturating_sub(audio_bitrate_bps);
    if max_video_bitrate_bps < 250_000 {
        return Err("adaptive_ladder_bandwidth_cap_too_low".to_string());
    }
    let max_height = adaptive_max_height(source_height, policy);
    let min_height = adaptive_min_height(max_height, policy);
    let min_total_bitrate_bps = policy
        .automatic_min_bitrate_bps
        .filter(|value| *value > 0 && *value <= total_bitrate_cap_bps);
    let frame_rate = planned_frame_rate(video);
    let fps_for_gop = frame_rate
        .target_fps
        .as_deref()
        .or(frame_rate.source_fps.as_deref())
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(24.0);
    let segment_seconds = 4;
    let gop_frames = ((fps_for_gop * segment_seconds as f64).round() as i32).clamp(12, 300);
    let tone_map = planned_tone_map(video, hdr_action);

    let mut rungs = Vec::new();
    for (target_height, default_bitrate_bps) in adaptive_default_ladder() {
        if target_height > max_height {
            continue;
        }
        if min_height.is_some_and(|height| target_height < height) {
            continue;
        }
        let target_bitrate_bps = default_bitrate_bps.min(max_video_bitrate_bps);
        if target_bitrate_bps < 250_000 {
            continue;
        }
        let total_bandwidth_bps = target_bitrate_bps.saturating_add(audio_bitrate_bps);
        if total_bandwidth_bps > total_bitrate_cap_bps {
            continue;
        }
        if min_total_bitrate_bps.is_some_and(|minimum| total_bandwidth_bps < minimum) {
            continue;
        }
        if total_bandwidth_bps >= source_bitrate_bps.saturating_mul(95) / 100 {
            continue;
        }
        if rungs.last().is_some_and(|previous: &AdaptiveRungPlan| {
            rungs_too_close(previous.bandwidth_bps, total_bandwidth_bps)
        }) {
            continue;
        }

        let target_width = even_dimension(
            ((source_width as i64 * target_height as i64 + (source_height / 2) as i64)
                / source_height as i64) as i32,
        );
        let scale = (target_width != source_width || target_height != source_height).then(|| {
            VideoScalePlan {
                width: target_width,
                height: even_dimension(target_height),
                reason: "adaptive_ladder_rung".to_string(),
            }
        });
        if subtitle.is_some() && subtitle_action == StreamAction::Disabled {
            return Err("selected_subtitle_unsupported_by_client_profile".to_string());
        }
        let burn_in = planned_burn_in(media, subtitle, subtitle_action)?;
        let mut output_reasons = Vec::new();
        for reason in reasons {
            push_unique(&mut output_reasons, reason);
        }
        push_unique(&mut output_reasons, "adaptive_ladder_rung");
        if target_bitrate_bps < default_bitrate_bps {
            push_unique(&mut output_reasons, "adaptive_ladder_bitrate_capped");
        }
        if matches!(
            hdr_action,
            HdrAction::Unsupported | HdrAction::UnknownFailClosed
        ) {
            return Err(hdr_blocker_reason(hdr_action).to_string());
        }
        if matches!(hdr_action, HdrAction::ToneMapToSdr) {
            push_unique(&mut output_reasons, "hdr_to_sdr_required");
        }
        if burn_in.is_some() {
            push_unique(&mut output_reasons, "subtitle_burn_in_required");
        }
        if let Some(primary_reason) = primary_reason {
            push_unique(
                &mut output_reasons,
                &format!("video_transcode_reason:{primary_reason}"),
            );
        }

        let rung_id = rungs.len().to_string();
        let label = format!("{}p {}k", target_height, target_bitrate_bps / 1000);
        let output_height = even_dimension(target_height);
        let level = planned_h264_level(
            &policy.video_encoder_level,
            Some(target_width),
            Some(output_height),
            fps_for_gop,
            &mut output_reasons,
        );
        let video = VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "libx264".to_string(),
            preset: clean_or_default(&policy.video_encoder_preset, "veryfast"),
            profile: non_empty_string(&policy.video_encoder_profile),
            level,
            crf: None,
            bitrate_bps: Some(target_bitrate_bps),
            maxrate_bps: Some(target_bitrate_bps),
            bufsize_bps: Some(
                target_bitrate_bps
                    .saturating_mul(policy.video_encoder_bufsize_multiplier.max(1) as i64),
            ),
            pixel_format: Some("yuv420p".to_string()),
            scale,
            tone_map: tone_map.clone(),
            frame_rate: frame_rate.clone(),
            gop_frames: Some(gop_frames),
            segment_seconds: segment_seconds.to_string(),
            keyframe_expression: format!("expr:gte(t,n_forced*{segment_seconds})"),
            hls_delivery: delivery,
            burn_in,
            reasons: output_reasons,
        };
        let codecs = hls_variant_codecs(&video, audio_output);
        let frame_rate_metadata = hls_variant_frame_rate(&video.frame_rate);
        rungs.push(AdaptiveRungPlan {
            id: rung_id,
            label,
            bandwidth_bps: total_bandwidth_bps,
            average_bandwidth_bps: hls_average_bandwidth_bps(total_bandwidth_bps),
            width: target_width,
            height: output_height,
            resolution: format!("{}x{}", target_width, output_height),
            codecs,
            frame_rate: frame_rate_metadata,
            video,
        });
    }

    if rungs.len() < 2 {
        return Err("adaptive_ladder_insufficient_useful_rungs".to_string());
    }

    push_unique(&mut ladder_reasons, "adaptive_ladder_source_aware");
    push_unique(&mut ladder_reasons, "adaptive_audio_strategy_per_rung");
    if policy.automatic_min_bitrate_bps.is_some() || policy.automatic_min_resolution.is_some() {
        push_unique(&mut ladder_reasons, "adaptive_ladder_min_quality_applied");
    }
    if policy.automatic_max_bitrate_bps.is_some() || policy.automatic_max_resolution.is_some() {
        push_unique(&mut ladder_reasons, "adaptive_ladder_max_quality_applied");
    }
    let starting_rung_id = rungs[0].id.clone();
    Ok(AdaptiveLadderPlan {
        rungs,
        starting_rung_id: starting_rung_id.clone(),
        active_rung_id: starting_rung_id,
        audio_strategy: AdaptiveAudioStrategy::PerRung,
        reasons: ladder_reasons,
    })
}

fn adaptive_default_ladder() -> [(i32, i64); 7] {
    [
        (2160, 20_000_000),
        (1440, 12_000_000),
        (1080, 8_000_000),
        (720, 4_000_000),
        (480, 1_500_000),
        (360, 800_000),
        (240, 400_000),
    ]
}

fn adaptive_bitrate_cap_bps(
    source_bitrate_bps: i64,
    policy: &EffectivePlaybackPolicy,
) -> (i64, Vec<String>) {
    let mut caps = vec![source_bitrate_bps];
    let mut reasons = vec!["adaptive_source_bitrate_cap_applied".to_string()];
    if let Some(value) = policy.max_bitrate_bps.filter(|value| *value > 0) {
        caps.push(value);
        reasons.push("adaptive_client_bitrate_cap_applied".to_string());
    }
    if let Some(value) = policy.automatic_max_bitrate_bps.filter(|value| *value > 0) {
        caps.push(value);
        reasons.push("adaptive_max_bitrate_cap_applied".to_string());
    }
    if matches!(
        policy.network_class,
        crate::playback::profile::NetworkClass::Wan
            | crate::playback::profile::NetworkClass::Unknown
    ) {
        if let Some(value) = policy.max_remote_bitrate_bps.filter(|value| *value > 0) {
            caps.push(value);
            reasons.push("adaptive_remote_stream_cap_applied".to_string());
        }
        if let Some(value) = policy.server_upload_cap_bps.filter(|value| *value > 0) {
            caps.push(value);
            reasons.push("adaptive_server_upload_cap_applied".to_string());
        }
    }
    (
        caps.into_iter().min().unwrap_or(source_bitrate_bps),
        reasons,
    )
}

fn adaptive_max_height(source_height: i32, policy: &EffectivePlaybackPolicy) -> i32 {
    [
        Some(source_height),
        policy.max_resolution.as_deref().and_then(resolution_height),
        policy
            .automatic_max_resolution
            .as_deref()
            .and_then(resolution_height),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(source_height)
}

fn adaptive_min_height(max_height: i32, policy: &EffectivePlaybackPolicy) -> Option<i32> {
    policy
        .automatic_min_resolution
        .as_deref()
        .and_then(resolution_height)
        .filter(|height| *height <= max_height)
}

fn hls_average_bandwidth_bps(bandwidth_bps: i64) -> i64 {
    bandwidth_bps
        .saturating_mul(90)
        .checked_div(100)
        .unwrap_or(bandwidth_bps)
}

fn hls_variant_codecs(video: &VideoOutputPlan, audio: Option<&AudioOutputPlan>) -> String {
    let mut codecs = vec![h264_codec_string(video)];
    if let Some(audio) = audio.and_then(|audio| hls_audio_codec_string(&audio.codec)) {
        codecs.push(audio);
    }
    codecs.join(",")
}

fn h264_codec_string(video: &VideoOutputPlan) -> String {
    format!(
        "avc1.{}00{}",
        h264_profile_compatibility_hex(video.profile.as_deref()),
        h264_level_hex(video.level.as_deref())
    )
}

fn h264_profile_compatibility_hex(profile: Option<&str>) -> &'static str {
    match profile
        .unwrap_or("high")
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "baseline" | "constrained_baseline" => "42",
        "main" => "4d",
        _ => "64",
    }
}

fn h264_level_hex(level: Option<&str>) -> String {
    let raw = level.unwrap_or("4.1").trim();
    let normalized = raw
        .split_once('.')
        .and_then(|(major, minor)| {
            Some(format!(
                "{}{}",
                major.trim().parse::<u8>().ok()?,
                minor.trim().parse::<u8>().ok()?
            ))
        })
        .or_else(|| raw.parse::<u8>().ok().map(|value| value.to_string()))
        .and_then(|value| value.parse::<u8>().ok())
        .unwrap_or(41);
    format!("{normalized:02x}")
}

fn hls_audio_codec_string(codec: &str) -> Option<String> {
    match codec.trim().to_ascii_lowercase().as_str() {
        "aac" => Some("mp4a.40.2".to_string()),
        "ac3" => Some("ac-3".to_string()),
        "eac3" => Some("ec-3".to_string()),
        "opus" => Some("opus".to_string()),
        "mp3" => Some("mp4a.40.34".to_string()),
        _ => None,
    }
}

fn hls_variant_frame_rate(frame_rate: &VideoFrameRatePlan) -> Option<String> {
    frame_rate
        .target_fps
        .as_deref()
        .or(frame_rate.source_fps.as_deref())
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|fps| fps.is_finite() && *fps > 0.0)
        .map(format_fps)
}

fn rungs_too_close(previous_bitrate_bps: i64, next_bitrate_bps: i64) -> bool {
    if previous_bitrate_bps <= 0 || next_bitrate_bps <= 0 {
        return false;
    }
    let minimum_delta = (previous_bitrate_bps / 5).max(250_000);
    previous_bitrate_bps.saturating_sub(next_bitrate_bps) < minimum_delta
}

fn planned_hardware_acceleration(
    video: &VideoStreamCapabilities,
    output: &mut VideoOutputPlan,
    policy: &EffectivePlaybackPolicy,
) -> Result<(HardwareAccelerationPlan, Vec<String>), String> {
    let preference = HardwarePreference::parse(&policy.hardware_acceleration);
    let fail_if_unavailable = matches!(preference, HardwarePreference::Api(_));
    let fallback_policy = HardwareFallbackPolicy::parse(&policy.hardware_fallback);
    let fallback = Some(fallback_policy.as_str().to_string());
    let mut software = HardwareAccelerationPlan {
        fallback: fallback.clone(),
        ..HardwareAccelerationPlan::default()
    };

    if preference == HardwarePreference::Off {
        software.fallback = None;
        return Ok((software, Vec::new()));
    }

    let capabilities = &policy.hardware_capabilities;
    let api = match preference {
        HardwarePreference::Auto => capabilities.preferred_api_for_encode(&output.codec),
        HardwarePreference::Api(api) if capabilities.is_api_available(api) => Some(api),
        HardwarePreference::Api(api) => {
            return hardware_unavailable_or_software(
                fail_if_unavailable,
                fallback_policy,
                software,
                format!("hardware_unavailable:{}", api.as_str()),
            );
        }
        HardwarePreference::Off => None,
    };

    let Some(api) = api else {
        return hardware_unavailable_or_software(
            fail_if_unavailable,
            fallback_policy,
            software,
            "hardware_unavailable".to_string(),
        );
    };

    let mut warnings = Vec::new();
    let source_codec = video.codec.as_deref().unwrap_or_default();
    let filter_graph_requires_software = video_filter_graph_requires_software(output);
    if let Some(reason) = hardware_encoder_unsupported_reason(api, video, output) {
        return hardware_unavailable_or_software(
            fail_if_unavailable,
            fallback_policy,
            software,
            reason,
        );
    }

    let source_bit_depth = video.bit_depth.and_then(i32_to_u8);
    let output_bit_depth = output
        .pixel_format
        .as_deref()
        .and_then(bit_depth_from_pixel_format_name)
        .or(Some(8));
    let decoder = if policy.allow_hardware_decode && !filter_graph_requires_software {
        if capabilities.capability_matrices.is_empty() {
            capabilities
                .decode_support(api, source_codec)
                .map(|support| support.ffmpeg_name.clone())
        } else {
            capabilities
                .supported_decode_matrix_entry(
                    api,
                    source_codec,
                    video.profile.as_deref(),
                    source_bit_depth,
                    video.pixel_format.as_deref(),
                )
                .and_then(|entry| entry.ffmpeg_decoder.clone())
        }
    } else {
        if policy.allow_hardware_decode && filter_graph_requires_software {
            push_unique(&mut warnings, "hardware_decode_disabled_filter_graph");
        }
        None
    };

    let encoder = if policy.allow_hardware_encode {
        if capabilities.capability_matrices.is_empty() {
            capabilities
                .encode_support(api, &output.codec)
                .map(|support| support.ffmpeg_name.clone())
        } else {
            capabilities
                .supported_encode_matrix_entry(
                    api,
                    &output.codec,
                    output.profile.as_deref(),
                    output_bit_depth,
                    output.pixel_format.as_deref(),
                )
                .and_then(|entry| entry.ffmpeg_encoder.clone())
        }
    } else {
        None
    };

    let decode_status = if decoder.is_some() {
        "selected"
    } else if !policy.allow_hardware_decode {
        "disabled_by_policy"
    } else if filter_graph_requires_software {
        "disabled_filter_graph"
    } else {
        "unsupported"
    };
    let encode_status = if encoder.is_some() {
        "selected"
    } else if !policy.allow_hardware_encode {
        "disabled_by_policy"
    } else {
        "unsupported"
    };

    if decoder.is_none() && encoder.is_none() {
        let reason = if !policy.allow_hardware_decode && !policy.allow_hardware_encode {
            "hardware_disabled_by_policy"
        } else if filter_graph_requires_software {
            "hardware_not_compatible_with_filter_graph"
        } else {
            "hardware_codec_not_supported"
        };
        return hardware_unavailable_or_software(
            fail_if_unavailable,
            fallback_policy,
            software,
            reason.to_string(),
        );
    }

    if let Some(encoder) = encoder.as_ref() {
        output.encoder = encoder.clone();
        if output.bitrate_bps.is_none() {
            let output_height = output
                .scale
                .as_ref()
                .map(|scale| scale.height)
                .or(video.height)
                .filter(|height| *height > 0);
            let bitrate = output_height
                .and_then(default_video_bitrate_for_height)
                .unwrap_or(8_000_000);
            output.crf = None;
            output.bitrate_bps = Some(bitrate);
            output.maxrate_bps = Some(bitrate);
            output.bufsize_bps =
                Some(bitrate.saturating_mul(policy.video_encoder_bufsize_multiplier.max(1) as i64));
            push_unique(
                &mut output.reasons,
                "hardware_encoder_bitrate_target_applied",
            );
        }
        push_unique(
            &mut output.reasons,
            &format!("hardware_encoder_selected:{encoder}"),
        );
    }

    let plan = HardwareAccelerationPlan {
        enabled: true,
        api: Some(api.as_str().to_string()),
        decoder,
        encoder,
        fallback,
        readiness_id: None,
        decode_status: Some(decode_status.to_string()),
        encode_status: Some(encode_status.to_string()),
        warnings: warnings.clone(),
    };
    Ok((plan, warnings))
}

fn hardware_encoder_unsupported_reason(
    api: HardwareApi,
    video: &VideoStreamCapabilities,
    output: &VideoOutputPlan,
) -> Option<String> {
    let output_width = output
        .scale
        .as_ref()
        .map(|scale| scale.width)
        .or(video.width)
        .filter(|width| *width > 0);
    if api == HardwareApi::VideoToolbox
        && output.codec == "h264"
        && output_width.is_some_and(|width| width < VIDEOTOOLBOX_H264_MIN_OUTPUT_WIDTH)
    {
        return Some("hardware_encoder_min_width_unsupported:videotoolbox:h264".to_string());
    }
    None
}

fn hardware_unavailable_or_software(
    fail_if_unavailable: bool,
    fallback_policy: HardwareFallbackPolicy,
    mut software: HardwareAccelerationPlan,
    reason: String,
) -> Result<(HardwareAccelerationPlan, Vec<String>), String> {
    if fail_if_unavailable && fallback_policy == HardwareFallbackPolicy::Fail {
        Err(reason)
    } else {
        software.warnings.push(reason.clone());
        Ok((software, vec![reason]))
    }
}

fn video_filter_graph_requires_software(output: &VideoOutputPlan) -> bool {
    output.tone_map.is_some()
        || output.scale.is_some()
        || output.burn_in.is_some()
        || output.frame_rate.mode == VideoFrameRateMode::Convert
}

fn i32_to_u8(value: i32) -> Option<u8> {
    u8::try_from(value).ok()
}

fn bit_depth_from_pixel_format_name(value: &str) -> Option<u8> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "yuv420p" | "nv12" | "videotoolbox_vld" | "cuda" | "vaapi" | "qsv" | "d3d11"
        | "dxva2_vld" => Some(8),
        "yuv420p10le" | "p010le" | "p016le" | "yuv422p10le" | "yuv444p10le" => Some(10),
        "yuv420p12le" | "yuv422p12le" | "yuv444p12le" => Some(12),
        _ => None,
    }
}

fn planned_burn_in(
    media: &MediaCapabilities,
    subtitle: Option<&SubtitleStreamCapabilities>,
    subtitle_action: StreamAction,
) -> Result<Option<SubtitleBurnInPlan>, String> {
    if subtitle_action != StreamAction::BurnIn {
        return Ok(None);
    }
    let subtitle = subtitle.ok_or_else(|| "subtitle_burn_in_stream_missing".to_string())?;
    let external_path = subtitle.external_path.clone();
    let stream_index = if external_path.is_some() {
        0
    } else {
        subtitle
            .index
            .ok_or_else(|| "subtitle_burn_in_stream_index_missing".to_string())?
    };
    let codec = subtitle
        .codec
        .as_deref()
        .filter(|codec| !codec.trim().is_empty())
        .ok_or_else(|| "subtitle_burn_in_codec_unknown".to_string())?;
    let codec_lower = codec.to_ascii_lowercase();
    let mode = if subtitle.kind == SubtitleKind::Image && is_image_subtitle_burnable(&codec_lower) {
        SubtitleBurnInMode::Image
    } else if subtitle.kind == SubtitleKind::Text && is_ass_ssa_subtitle(&codec_lower) {
        SubtitleBurnInMode::AssSsaExactStyle
    } else {
        return Err("subtitle_burn_in_filter_unsupported".to_string());
    };

    let filter_stream_index = if mode == SubtitleBurnInMode::AssSsaExactStyle {
        Some(
            subtitle_filter_stream_index(media, stream_index)
                .ok_or_else(|| "subtitle_burn_in_filter_stream_index_missing".to_string())?,
        )
    } else {
        None
    };

    Ok(Some(SubtitleBurnInPlan {
        stream_index,
        filter_stream_index,
        external_path,
        codec: codec_lower,
        mode,
        reason: "selected_subtitle_requires_video_burn_in".to_string(),
    }))
}

fn subtitle_filter_stream_index(media: &MediaCapabilities, stream_index: i32) -> Option<i32> {
    media
        .subtitle_streams
        .iter()
        .filter(|stream| stream.external_path.is_none())
        .position(|stream| stream.index == Some(stream_index))
        .and_then(|position| i32::try_from(position).ok())
}

fn planned_frame_rate(video: &VideoStreamCapabilities) -> VideoFrameRatePlan {
    let source_fps = video
        .frame_rate
        .filter(|fps| fps.is_finite() && *fps > 0.0)
        .map(format_fps);
    let target_fps = video
        .frame_rate
        .filter(|fps| fps.is_finite() && *fps > 60.0)
        .map(|_| "60".to_string());

    VideoFrameRatePlan {
        mode: if target_fps.is_some() {
            VideoFrameRateMode::Convert
        } else {
            VideoFrameRateMode::Source
        },
        source_fps,
        target_fps,
    }
}

fn planned_h264_level(
    policy_level: &str,
    output_width: Option<i32>,
    output_height: Option<i32>,
    output_fps: f64,
    reasons: &mut Vec<String>,
) -> Option<String> {
    let configured = non_empty_string(policy_level)?;
    let Some(required) = required_h264_level(output_width, output_height, output_fps) else {
        return Some(configured);
    };
    let configured_level = parse_h264_level(&configured);
    if configured_level.is_some_and(|level| level >= required) {
        return Some(configured);
    }

    push_unique(reasons, "h264_level_raised_for_output_geometry");
    Some(format_h264_level(required))
}

fn required_h264_level(width: Option<i32>, height: Option<i32>, fps: f64) -> Option<u8> {
    let width = width.filter(|value| *value > 0)?;
    let height = height.filter(|value| *value > 0)?;
    let fps = if fps.is_finite() && fps > 0.0 {
        fps
    } else {
        24.0
    };
    let macroblock_width = ((i64::from(width)) + 15) / 16;
    let macroblock_height = ((i64::from(height)) + 15) / 16;
    let macroblocks_per_frame = macroblock_width.saturating_mul(macroblock_height);
    let macroblocks_per_second = ((macroblocks_per_frame as f64) * fps).ceil() as i64;

    H264_LEVEL_LIMITS
        .iter()
        .find(|limit| {
            macroblocks_per_frame <= limit.max_macroblocks_per_frame
                && macroblocks_per_second <= limit.max_macroblocks_per_second
        })
        .map(|limit| limit.level)
}

fn parse_h264_level(raw: &str) -> Option<u8> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }

    if let Some((major, minor)) = value.split_once('.') {
        let major = major.trim().parse::<u8>().ok()?;
        let minor = minor.trim().chars().find_map(|ch| ch.to_digit(10))? as u8;
        return Some(major.saturating_mul(10).saturating_add(minor));
    }

    let numeric = value.parse::<u8>().ok()?;
    if numeric < 10 {
        Some(numeric.saturating_mul(10))
    } else {
        Some(numeric)
    }
}

fn format_h264_level(level: u8) -> String {
    format!("{}.{}", level / 10, level % 10)
}

fn target_video_scale(
    video: &VideoStreamCapabilities,
    max_resolution: Option<&str>,
) -> Option<VideoScalePlan> {
    let max_height = max_resolution.and_then(resolution_height)?;
    let source_height = video.height.filter(|height| *height > 0)?;
    let source_width = video.width.filter(|width| *width > 0)?;
    if source_height <= max_height {
        return None;
    }
    let target_height = even_dimension(max_height);
    let target_width = even_dimension(
        ((source_width as i64 * target_height as i64 + (source_height / 2) as i64)
            / source_height as i64) as i32,
    );
    Some(VideoScalePlan {
        width: target_width,
        height: target_height,
        reason: "resolution_exceeds_policy".to_string(),
    })
}

fn target_video_bitrate_bps(
    output_height: Option<i32>,
    max_bitrate_bps: Option<i64>,
    scaled: bool,
    reasons: &[String],
) -> Option<i64> {
    let policy_cap = max_bitrate_bps.filter(|bitrate| *bitrate > 0);
    if policy_cap.is_none()
        && !scaled
        && !reasons
            .iter()
            .any(|reason| reason == "source_bitrate_exceeds_policy")
    {
        return None;
    }

    let rung = output_height
        .and_then(default_video_bitrate_for_height)
        .unwrap_or(8_000_000);
    Some(
        policy_cap
            .map(|cap| cap.min(rung))
            .unwrap_or(rung)
            .max(250_000),
    )
}

fn default_video_bitrate_for_height(height: i32) -> Option<i64> {
    match height {
        height if height >= 2160 => Some(20_000_000),
        height if height >= 1440 => Some(12_000_000),
        height if height >= 1080 => Some(8_000_000),
        height if height >= 720 => Some(4_000_000),
        height if height >= 480 => Some(1_500_000),
        height if height > 0 => Some(800_000),
        _ => None,
    }
}

fn primary_video_transcode_reason(reasons: &[String]) -> Option<String> {
    const PRIORITY: &[&str] = &[
        "hdr_tone_mapping_required",
        "resolution_exceeds_policy",
        "source_bitrate_exceeds_policy",
        "video_codec_not_supported",
        "video_profile_not_supported",
        "video_level_not_supported",
        "video_pixel_format_not_supported",
        "subtitle_requires_burn_in",
        "subtitle_burn_in_requested",
        "direct_play_disabled_by_policy",
    ];

    PRIORITY
        .iter()
        .find(|reason| reasons.iter().any(|value| value == **reason))
        .map(|reason| (*reason).to_string())
        .or_else(|| reasons.first().cloned())
}

fn video_profile_allowed(video: &VideoStreamCapabilities, client: &ClientPlaybackProfile) -> bool {
    let Some(codec) = video.codec.as_deref() else {
        return false;
    };
    if !codec.eq_ignore_ascii_case("h264") || client.client_kind == ClientKind::NativeMpv {
        return true;
    }
    let Some(profile) = video.profile.as_deref() else {
        return true;
    };
    matches!(
        profile.to_ascii_lowercase().as_str(),
        "baseline" | "constrained baseline" | "main" | "high"
    )
}

fn video_level_allowed(video: &VideoStreamCapabilities, client: &ClientPlaybackProfile) -> bool {
    let Some(codec) = video.codec.as_deref() else {
        return false;
    };
    if !codec.eq_ignore_ascii_case("h264") || client.client_kind == ClientKind::NativeMpv {
        return true;
    }
    video.level.map(|level| level <= 42).unwrap_or(true)
}

fn video_pixel_format_allowed(
    video: &VideoStreamCapabilities,
    client: &ClientPlaybackProfile,
) -> bool {
    let Some(codec) = video.codec.as_deref() else {
        return false;
    };
    if !codec.eq_ignore_ascii_case("h264") || client.client_kind == ClientKind::NativeMpv {
        return true;
    }
    let Some(pixel_format) = video.pixel_format.as_deref() else {
        return true;
    };
    matches!(
        pixel_format.to_ascii_lowercase().as_str(),
        "yuv420p" | "nv12" | "videotoolbox_vld"
    )
}

pub fn can_deliver_subtitle(
    subtitle: &SubtitleStreamCapabilities,
    delivery: Delivery,
    client: &ClientPlaybackProfile,
) -> bool {
    let Some(codec) = subtitle.codec.as_deref() else {
        return false;
    };
    match delivery {
        Delivery::DirectFile => {
            if subtitle.kind == SubtitleKind::Image
                && !matches!(
                    client.image_subtitle_support,
                    ImageSubtitleSupport::Native | ImageSubtitleSupport::NativeOrBurnIn
                )
            {
                return false;
            }
            if is_ass_ssa_subtitle(codec)
                && !matches!(client.ass_complexity_support, AssComplexitySupport::Native)
            {
                return false;
            }
            matches!(
                client.subtitle_rendering,
                SubtitleRendering::Native | SubtitleRendering::Sidecar
            ) && client
                .supported_subtitle_codecs
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(codec))
        }
        _ if subtitle.kind == SubtitleKind::Text => {
            can_convert_text_subtitle_to_webvtt(subtitle, client)
        }
        _ => false,
    }
}

fn can_copy_subtitle_to_delivery(
    subtitle: &SubtitleStreamCapabilities,
    delivery: Delivery,
    client: &ClientPlaybackProfile,
) -> bool {
    let Some(codec) = subtitle.codec.as_deref() else {
        return false;
    };
    match delivery {
        Delivery::DirectFile => can_deliver_subtitle(subtitle, delivery, client),
        Delivery::HlsMpegts
        | Delivery::HlsFmp4
        | Delivery::HlsAdaptiveMpegts
        | Delivery::HlsAdaptiveFmp4 => {
            subtitle.kind == SubtitleKind::Text
                && codec.eq_ignore_ascii_case("webvtt")
                && client
                    .supported_subtitle_codecs
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case("webvtt"))
        }
    }
}

fn can_convert_text_subtitle_to_webvtt(
    subtitle: &SubtitleStreamCapabilities,
    client: &ClientPlaybackProfile,
) -> bool {
    let Some(codec) = subtitle.codec.as_deref() else {
        return false;
    };
    subtitle.kind == SubtitleKind::Text
        && is_webvtt_convertible_text_subtitle_codec(codec)
        && matches!(
            client.subtitle_rendering,
            SubtitleRendering::HlsWebvtt | SubtitleRendering::Sidecar
        )
        && (!is_ass_ssa_subtitle(codec)
            || matches!(
                client.ass_complexity_support,
                AssComplexitySupport::SimpleWebvtt
            ))
        && client
            .supported_subtitle_codecs
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case("webvtt"))
}

fn is_webvtt_convertible_text_subtitle_codec(codec: &str) -> bool {
    matches!(
        codec.to_ascii_lowercase().as_str(),
        "ass" | "ssa" | "subrip" | "srt" | "webvtt" | "mov_text"
    )
}

fn is_image_subtitle_burnable(codec: &str) -> bool {
    matches!(
        codec.to_ascii_lowercase().as_str(),
        "hdmv_pgs_subtitle" | "pgs" | "dvd_subtitle" | "dvdsub" | "vobsub" | "sub" | "idx" | "xsub"
    )
}

fn is_ass_ssa_subtitle(codec: &str) -> bool {
    matches!(codec.to_ascii_lowercase().as_str(), "ass" | "ssa")
}

fn resolution_height(raw: &str) -> Option<i32> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "480p" => Some(480),
        "720p" => Some(720),
        "1080p" => Some(1080),
        "1440p" => Some(1440),
        "4k" | "2160p" => Some(2160),
        "8k" | "4320p" => Some(4320),
        _ => None,
    }
}

fn even_dimension(value: i32) -> i32 {
    (value.max(2) / 2) * 2
}

fn format_fps(fps: f64) -> String {
    let rounded = (fps * 1000.0).round() / 1000.0;
    if (rounded.fract()).abs() < f64::EPSILON {
        format!("{}", rounded as i64)
    } else {
        format!("{rounded:.3}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

fn clean_or_default(raw: &str, default: &str) -> String {
    let value = raw.trim();
    if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    }
}

fn non_empty_string(raw: &str) -> Option<String> {
    let value = raw.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn push_check(
    report: &mut CompatibilityReport,
    category: impl Into<String>,
    passed: bool,
    reason: impl Into<String>,
) {
    if passed {
        report.checks.push(CompatibilityCheck::pass(category));
    } else {
        report
            .checks
            .push(CompatibilityCheck::fail(category, reason));
    }
}

fn push_unique(reasons: &mut Vec<String>, reason: &str) {
    if !reasons.iter().any(|existing| existing == reason) {
        reasons.push(reason.to_string());
    }
}

fn push_hdr_action_reason(reasons: &mut Vec<String>, action: HdrAction) {
    let reason = match action {
        HdrAction::Direct => Some("hdr_direct"),
        HdrAction::DirectDolbyVision => Some("hdr_direct_dolby_vision"),
        HdrAction::DirectHdr10Fallback => Some("hdr_direct_hdr10_fallback"),
        HdrAction::ToneMapToSdr => Some("hdr_tone_mapping_required"),
        HdrAction::Unsupported => Some("hdr_unsupported"),
        HdrAction::UnknownFailClosed => Some("hdr_unknown_fail_closed"),
        HdrAction::None | HdrAction::Unknown => None,
    };
    if let Some(reason) = reason {
        push_unique(reasons, reason);
    }
}

fn push_hdr_detail_reasons(
    reasons: &mut Vec<String>,
    video: &VideoStreamCapabilities,
    action: HdrAction,
) {
    if video.dolby_vision
        && matches!(action, HdrAction::Unsupported)
        && !video.dolby_vision_has_hdr10_fallback
    {
        push_unique(reasons, "dolby_vision_hdr10_fallback_missing");
    }
    if video.dolby_vision && matches!(action, HdrAction::DirectHdr10Fallback) {
        push_unique(reasons, "dolby_vision_hdr10_fallback_selected");
    }
    if video.dolby_vision
        && video.dolby_vision_has_hdr10_fallback
        && matches!(action, HdrAction::ToneMapToSdr)
    {
        push_unique(reasons, "dolby_vision_hdr10_fallback_tone_map_to_sdr");
    }
    if video.hdr10_plus && matches!(action, HdrAction::DirectHdr10Fallback) {
        push_unique(reasons, "hdr10_plus_hdr10_fallback_selected");
    }
}

fn not_playable_plan(
    media_file_id: String,
    reasons: Vec<String>,
    report: CompatibilityReport,
) -> PlaybackPlan {
    let hdr_action = not_playable_hdr_action(&reasons);
    PlaybackPlan {
        plan_version: PLAYBACK_PLAN_VERSION,
        mode: PlaybackMode::VideoTranscode,
        delivery: Delivery::HlsMpegts,
        media_file_id,
        selected_video_track: None,
        video_action: StreamAction::Disabled,
        audio_action: StreamAction::Disabled,
        subtitle_action: StreamAction::Disabled,
        seek_behavior: SeekBehavior::ServerHlsRestart,
        adaptive: false,
        selected_audio_track: None,
        selected_subtitle_track: None,
        hdr_action,
        hardware_acceleration: HardwareAccelerationPlan::default(),
        audio_output: None,
        video_output: None,
        adaptive_ladder: None,
        video_transcode_reason: None,
        workload_class: None,
        feasibility: Some(PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::Reject,
            reason: reasons
                .first()
                .cloned()
                .unwrap_or_else(|| "playback_not_playable".to_string()),
            support_decision: PlaybackSupportDecision::Unknown,
            performance_decision: PlaybackPerformanceDecision::Unknown,
            confidence: PlaybackPerformanceConfidence::Unknown,
            selected_envelope_id: None,
            selected_hardware_api: None,
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: None,
            selected_envelope_sample_count: None,
            realtime_required_millis: 1000,
            reasons: reasons.clone(),
            warnings: Vec::new(),
            remediation_codes: Vec::new(),
            background_probe_queued: false,
        }),
        compatibility_report: report,
        reasons,
        warnings: Vec::new(),
        expected_outputs: Vec::new(),
        playable: false,
    }
}

fn not_playable_hdr_action(reasons: &[String]) -> HdrAction {
    if reasons
        .iter()
        .any(|reason| reason == "hdr_unknown_fail_closed")
    {
        HdrAction::UnknownFailClosed
    } else if reasons.iter().any(|reason| reason == "hdr_unsupported") {
        HdrAction::Unsupported
    } else if reasons
        .iter()
        .any(|reason| reason == "hdr_tone_mapping_required")
    {
        HdrAction::ToneMapToSdr
    } else {
        HdrAction::None
    }
}

fn hls_plan(
    media_file_id: String,
    mode: PlaybackMode,
    delivery: Delivery,
    video_action: StreamAction,
    audio_action: StreamAction,
    subtitle_action: StreamAction,
    selected_video_track: Option<i32>,
    selected_audio_track: Option<i32>,
    selected_subtitle_track: Option<i32>,
    hdr_action: HdrAction,
    compatibility_report: CompatibilityReport,
    reasons: Vec<String>,
    audio_output: Option<AudioOutputPlan>,
    video_output: Option<VideoOutputPlan>,
    adaptive_ladder: Option<AdaptiveLadderPlan>,
    video_transcode_reason: Option<String>,
) -> PlaybackPlan {
    let mut reasons = reasons;
    push_hdr_action_reason(&mut reasons, hdr_action);
    PlaybackPlan {
        plan_version: PLAYBACK_PLAN_VERSION,
        mode,
        delivery,
        media_file_id,
        selected_video_track,
        video_action,
        audio_action,
        subtitle_action,
        seek_behavior: SeekBehavior::ServerHlsRestart,
        adaptive: matches!(mode, PlaybackMode::AdaptiveTranscode),
        selected_audio_track,
        selected_subtitle_track,
        hdr_action,
        hardware_acceleration: HardwareAccelerationPlan::default(),
        audio_output,
        video_output,
        adaptive_ladder,
        video_transcode_reason,
        workload_class: None,
        feasibility: None,
        compatibility_report,
        reasons,
        warnings: Vec::new(),
        expected_outputs: hls_expected_outputs(mode, delivery, subtitle_action),
        playable: true,
    }
}

fn direct_file_outputs() -> Vec<ExpectedOutput> {
    vec![ExpectedOutput::new("direct_file", "direct_file")]
}

fn hls_expected_outputs(
    mode: PlaybackMode,
    delivery: Delivery,
    subtitle_action: StreamAction,
) -> Vec<ExpectedOutput> {
    let direct_stream = mode == PlaybackMode::DirectStream;
    let media_playlist = if direct_stream {
        "media.m3u8"
    } else {
        "stream_0.m3u8"
    };
    let segment_prefix = if direct_stream {
        "segment_*"
    } else {
        "seg_0_*"
    };
    let init_segment = if direct_stream {
        "init.mp4"
    } else {
        "init_0.mp4"
    };
    let mut outputs = vec![
        ExpectedOutput::new("master.m3u8", "hls_master_playlist"),
        ExpectedOutput::new(media_playlist, "hls_media_playlist"),
    ];

    match delivery {
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => {
            outputs.push(ExpectedOutput::new(init_segment, "hls_init_segment"));
            outputs.push(ExpectedOutput::new(
                format!("{segment_prefix}.m4s"),
                "hls_media_segment_pattern",
            ));
        }
        Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts => {
            outputs.push(ExpectedOutput::new(
                format!("{segment_prefix}.ts"),
                "hls_media_segment_pattern",
            ));
        }
        Delivery::DirectFile => {}
    }

    if subtitle_action == StreamAction::ConvertTextToWebvtt {
        outputs.push(ExpectedOutput::new("sub_0.m3u8", "hls_subtitle_playlist"));
        outputs.push(ExpectedOutput::new(
            "sub_0_*.vtt",
            "hls_subtitle_segment_pattern",
        ));
    }

    outputs
}

fn hls_expected_outputs_for_plan(plan: &PlaybackPlan) -> Vec<ExpectedOutput> {
    let Some(ladder) = plan.adaptive_ladder.as_ref() else {
        return hls_expected_outputs(plan.mode, plan.delivery, plan.subtitle_action);
    };
    let extension = match plan.delivery {
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => "m4s",
        Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts => "ts",
        Delivery::DirectFile => "bin",
    };
    let mut outputs = vec![ExpectedOutput::new("master.m3u8", "hls_master_playlist")];
    for rung in &ladder.rungs {
        outputs.push(ExpectedOutput::new(
            format!("stream_{}.m3u8", rung.id),
            "hls_media_playlist",
        ));
        if matches!(plan.delivery, Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4) {
            outputs.push(ExpectedOutput::new(
                format!("init_{}.mp4", rung.id),
                "hls_init_segment",
            ));
        }
        outputs.push(ExpectedOutput::new(
            format!("seg_{}_*.{extension}", rung.id),
            "hls_media_segment_pattern",
        ));
    }
    if plan.subtitle_action == StreamAction::ConvertTextToWebvtt {
        outputs.push(ExpectedOutput::new("sub_0.m3u8", "hls_subtitle_playlist"));
        outputs.push(ExpectedOutput::new(
            "sub_0_*.vtt",
            "hls_subtitle_segment_pattern",
        ));
    }
    outputs
}

fn video_transcode_capacity_available(policy: &EffectivePlaybackPolicy) -> bool {
    policy
        .max_simultaneous_video_transcodes
        .map(|max| policy.active_video_transcodes < max)
        .unwrap_or(true)
}

fn adaptive_transcode_capacity_available(policy: &EffectivePlaybackPolicy) -> bool {
    policy
        .max_simultaneous_video_transcodes
        .map(|max| {
            policy
                .active_video_transcodes
                .saturating_add(ADAPTIVE_TRANSCODE_CAPACITY_WEIGHT)
                <= max
        })
        .unwrap_or(true)
}

fn subtitle_action(
    subtitle: Option<&SubtitleStreamCapabilities>,
    delivery: Delivery,
    client: &ClientPlaybackProfile,
) -> StreamAction {
    let Some(subtitle) = subtitle else {
        return StreamAction::Disabled;
    };
    if delivery == Delivery::DirectFile {
        return StreamAction::Passthrough;
    }
    match subtitle.kind {
        SubtitleKind::Text if can_deliver_subtitle(subtitle, delivery, client) => {
            StreamAction::ConvertTextToWebvtt
        }
        SubtitleKind::Image => StreamAction::BurnIn,
        _ => StreamAction::Disabled,
    }
}

fn video_transcode_subtitle_action(
    subtitle: Option<&SubtitleStreamCapabilities>,
    client: &ClientPlaybackProfile,
) -> StreamAction {
    let Some(subtitle) = subtitle else {
        return StreamAction::Disabled;
    };
    match (&subtitle.kind, &client.subtitle_burn_policy) {
        (_, SubtitleBurnPolicy::Always) => StreamAction::BurnIn,
        (SubtitleKind::Image, SubtitleBurnPolicy::Automatic | SubtitleBurnPolicy::ImageOnly)
            if matches!(
                client.image_subtitle_support,
                ImageSubtitleSupport::BurnIn | ImageSubtitleSupport::NativeOrBurnIn
            ) =>
        {
            StreamAction::BurnIn
        }
        (SubtitleKind::Text, SubtitleBurnPolicy::Automatic)
            if subtitle.codec.as_deref().is_some_and(is_ass_ssa_subtitle)
                && matches!(client.ass_complexity_support, AssComplexitySupport::BurnIn) =>
        {
            StreamAction::BurnIn
        }
        (SubtitleKind::Text, _) if can_convert_text_subtitle_to_webvtt(subtitle, client) => {
            StreamAction::ConvertTextToWebvtt
        }
        _ => StreamAction::Disabled,
    }
}

fn preferred_hls_delivery(client: &ClientPlaybackProfile, adaptive: bool) -> Delivery {
    if supports_hls_segment_type(client, "fmp4") {
        if adaptive {
            Delivery::HlsAdaptiveFmp4
        } else {
            Delivery::HlsFmp4
        }
    } else if adaptive {
        Delivery::HlsAdaptiveMpegts
    } else {
        Delivery::HlsMpegts
    }
}

fn preferred_copy_hls_delivery(
    video: &VideoStreamCapabilities,
    audio: Option<&AudioStreamCapabilities>,
    client: &ClientPlaybackProfile,
    adaptive: bool,
) -> Option<Delivery> {
    let fmp4 = if adaptive {
        Delivery::HlsAdaptiveFmp4
    } else {
        Delivery::HlsFmp4
    };
    let mpegts = if adaptive {
        Delivery::HlsAdaptiveMpegts
    } else {
        Delivery::HlsMpegts
    };

    [("fmp4", fmp4), ("mpegts", mpegts)]
        .into_iter()
        .filter(|(segment_type, _)| supports_hls_segment_type(client, segment_type))
        .map(|(_, delivery)| delivery)
        .find(|delivery| {
            can_copy_video_to_delivery(video, *delivery)
                && audio
                    .map(|audio| {
                        can_copy_audio_to_delivery(audio, *delivery)
                            && audio_channels_allowed(audio, client)
                    })
                    .unwrap_or(true)
        })
}

fn supports_hls_segment_type(client: &ClientPlaybackProfile, segment_type: &str) -> bool {
    client
        .supported_hls_segment_types
        .iter()
        .any(|value| value.eq_ignore_ascii_case(segment_type))
}

fn codec_allowed(codec: Option<&str>, allowed: &[String]) -> bool {
    let Some(codec) = codec else {
        return false;
    };
    allowed
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(codec))
}

fn resolution_allowed(height: Option<i32>, max_resolution: Option<&str>) -> bool {
    let Some(max_resolution) = max_resolution else {
        return true;
    };
    let Some(height) = height else {
        return false;
    };
    match max_resolution.trim().to_ascii_lowercase().as_str() {
        "" | "any" | "none" | "unlimited" | "original" | "source" => true,
        "480p" => height <= 480,
        "720p" => height <= 720,
        "1080p" => height <= 1080,
        "1440p" => height <= 1440,
        "4k" | "2160p" => height <= 2160,
        "8k" | "4320p" => height <= 4320,
        _ => true,
    }
}

fn bitrate_allowed(actual_bps: i64, max_bps: Option<i64>) -> bool {
    max_bps
        .map(|max| max <= 0 || actual_bps <= max)
        .unwrap_or(true)
}

fn hdr_action(
    video: &VideoStreamCapabilities,
    client: &ClientPlaybackProfile,
    policy: &EffectivePlaybackPolicy,
) -> HdrAction {
    if policy.force_sdr_output && source_has_hdr(video) {
        return HdrAction::ToneMapToSdr;
    }

    if video.dolby_vision {
        if client.supports_dolby_vision && dolby_vision_profile_direct_playable(video) {
            return HdrAction::DirectDolbyVision;
        }
        if video.dolby_vision_has_hdr10_fallback {
            return if client.supports_hdr {
                HdrAction::DirectHdr10Fallback
            } else {
                HdrAction::ToneMapToSdr
            };
        }
        return HdrAction::Unsupported;
    }
    if unknown_hdr_metadata(video) {
        return HdrAction::UnknownFailClosed;
    }
    if video.hdr10_plus {
        if client.supports_hdr10_plus {
            return HdrAction::Direct;
        }
        if client.supports_hdr {
            return HdrAction::DirectHdr10Fallback;
        }
        return HdrAction::ToneMapToSdr;
    }
    if video.hdr10 || video.hdr10_plus {
        if client.supports_hdr {
            HdrAction::Direct
        } else {
            HdrAction::ToneMapToSdr
        }
    } else {
        HdrAction::None
    }
}

fn hdr_blocks_direct_play(action: HdrAction) -> bool {
    matches!(
        action,
        HdrAction::ToneMapToSdr | HdrAction::Unsupported | HdrAction::UnknownFailClosed
    )
}

fn hdr_blocker_reason(action: HdrAction) -> &'static str {
    match action {
        HdrAction::ToneMapToSdr => "hdr_tone_mapping_required",
        HdrAction::Unsupported => "hdr_unsupported",
        HdrAction::UnknownFailClosed => "hdr_unknown_fail_closed",
        _ => "hdr_tone_mapping_required",
    }
}

fn source_has_hdr(video: &VideoStreamCapabilities) -> bool {
    video.hdr10 || video.hdr10_plus || video.dolby_vision || unknown_hdr_metadata(video)
}

fn unknown_hdr_metadata(video: &VideoStreamCapabilities) -> bool {
    let high_bit_depth = video.bit_depth.is_some_and(|depth| depth > 8);
    let transfer = video
        .color_transfer
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let primaries = video
        .color_primaries
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let matrix = video
        .color_matrix
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let hdr_signals_present = transfer.contains("2084")
        || transfer.contains("arib-std-b67")
        || transfer.contains("hlg")
        || primaries.contains("bt2020")
        || matrix.contains("bt2020");
    high_bit_depth
        && hdr_signals_present
        && !video.hdr10
        && !video.hdr10_plus
        && !video.dolby_vision
}

fn dolby_vision_profile_direct_playable(video: &VideoStreamCapabilities) -> bool {
    matches!(video.dolby_vision_profile, Some(5 | 8) | None)
}

fn apply_runtime_feasibility(
    mut plan: PlaybackPlan,
    media: &MediaCapabilities,
    video: &VideoStreamCapabilities,
    audio: Option<&AudioStreamCapabilities>,
    subtitle: Option<&SubtitleStreamCapabilities>,
    policy: &EffectivePlaybackPolicy,
) -> PlaybackPlan {
    let mut workload = derive_playback_workload_class(media, video, audio, subtitle, &plan);
    let mut decision = decide_playback_feasibility(&plan, &workload, policy);

    if decision.action == PlaybackFeasibilityAction::Reject
        && plan.mode == PlaybackMode::AdaptiveTranscode
        && let Some((rung_id, video_output)) = lower_realtime_safe_adaptive_rung(&plan)
    {
        if let Some(ladder) = plan.adaptive_ladder.as_mut() {
            ladder.starting_rung_id = rung_id.clone();
            ladder.active_rung_id = rung_id;
            push_unique(
                &mut ladder.reasons,
                "runtime_feasibility_lower_rung_selected",
            );
        }
        plan.video_output = Some(video_output);
        workload = derive_playback_workload_class(media, video, audio, subtitle, &plan);
        decision = PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::DowngradeQuality,
            reason: "downgrade_quality_runtime_feasibility".to_string(),
            support_decision: PlaybackSupportDecision::Supported,
            performance_decision: PlaybackPerformanceDecision::RealtimeMarginal,
            confidence: PlaybackPerformanceConfidence::StaticInferred,
            selected_envelope_id: None,
            selected_hardware_api: plan.hardware_acceleration.api.clone(),
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: None,
            selected_envelope_sample_count: None,
            realtime_required_millis: 1000,
            reasons: vec![
                "downgrade_quality_runtime_feasibility".to_string(),
                "lower_rung_expected_realtime_safe".to_string(),
            ],
            warnings: vec!["playback_quality_downgraded_for_realtime_feasibility".to_string()],
            remediation_codes: Vec::new(),
            background_probe_queued: false,
        };
    }

    for warning in &decision.warnings {
        push_unique(&mut plan.warnings, warning);
    }
    for reason in &decision.reasons {
        push_unique(&mut plan.reasons, reason);
    }
    if decision.action == PlaybackFeasibilityAction::SoftwareFallback
        && plan.hardware_acceleration.enabled
    {
        plan.hardware_acceleration.enabled = false;
        plan.hardware_acceleration.api = None;
        plan.hardware_acceleration.decoder = None;
        plan.hardware_acceleration.encoder = None;
        plan.hardware_acceleration.decode_status = Some("software_fallback".to_string());
        plan.hardware_acceleration.encode_status = Some("software_fallback".to_string());
        if let Some(output) = plan.video_output.as_mut() {
            output.encoder = "libx264".to_string();
            push_unique(
                &mut output.reasons,
                "runtime_feasibility_hardware_to_software_fallback",
            );
        }
    }
    if decision.action == PlaybackFeasibilityAction::Reject {
        plan.playable = false;
    }
    plan.workload_class = Some(workload);
    plan.feasibility = Some(decision);
    plan
}

fn derive_playback_workload_class(
    media: &MediaCapabilities,
    video: &VideoStreamCapabilities,
    audio: Option<&AudioStreamCapabilities>,
    subtitle: Option<&SubtitleStreamCapabilities>,
    plan: &PlaybackPlan,
) -> PlaybackWorkloadClass {
    let output = plan.video_output.as_ref().or_else(|| {
        plan.adaptive_ladder
            .as_ref()?
            .rungs
            .first()
            .map(|rung| &rung.video)
    });
    let output_width = output
        .and_then(|output| output.scale.as_ref().map(|scale| scale.width))
        .or(video.width);
    let output_height = output
        .and_then(|output| output.scale.as_ref().map(|scale| scale.height))
        .or(video.height);
    let output_codec = output.map(|output| output.codec.clone());
    let output_pixel_format = output.and_then(|output| output.pixel_format.clone());
    let pipeline_stages = pipeline_stages_for_plan(plan);
    let pipeline_signature = pipeline_stages.join("+");
    let mut cost_labels = Vec::new();
    push_unique(&mut cost_labels, resolution_cost_label(video.height));
    if video.hdr10 {
        push_unique(&mut cost_labels, "hdr10");
    }
    if video.hdr10_plus {
        push_unique(&mut cost_labels, "hdr10_plus");
    }
    if video.dolby_vision {
        push_unique(&mut cost_labels, "dolby_vision");
    }
    if matches!(plan.hdr_action, HdrAction::ToneMapToSdr) {
        push_unique(&mut cost_labels, "hdr_tonemap");
    }
    if matches!(plan.subtitle_action, StreamAction::BurnIn) {
        push_unique(&mut cost_labels, subtitle_burn_cost_label(subtitle));
    }
    if plan.video_action == StreamAction::Transcode
        && video.codec.as_deref().is_some_and(|codec| codec == "av1")
    {
        push_unique(&mut cost_labels, "av1_source");
    }
    if plan.audio_action == StreamAction::Transcode {
        push_unique(&mut cost_labels, audio_cost_label(audio));
    }
    if output_height
        .zip(video.height)
        .is_some_and(|(out, src)| out < src)
    {
        push_unique(&mut cost_labels, "downscale");
    }

    let class_id = workload_class_id(
        plan,
        video,
        output_codec.as_deref(),
        output_height,
        &cost_labels,
        &pipeline_signature,
    );

    PlaybackWorkloadClass {
        schema_version: 1,
        class_id,
        source_container: media.container.canonical.clone(),
        source_video_codec: video.codec.clone(),
        source_video_profile: video.profile.clone(),
        source_bit_depth: video.bit_depth.and_then(i32_to_u8),
        source_pixel_format: video.pixel_format.clone(),
        source_width: video.width,
        source_height: video.height,
        source_frame_rate: video
            .frame_rate
            .map(|frame_rate| format!("{frame_rate:.3}")),
        source_bitrate_bps: video.bitrate_bps.or(media.overall_bitrate_bps),
        hdr_action: plan.hdr_action,
        subtitle_action: plan.subtitle_action,
        audio_action: plan.audio_action,
        output_codec,
        output_width,
        output_height,
        output_pixel_format,
        delivery: plan.delivery,
        pipeline_signature,
        pipeline_stages,
        cost_labels,
    }
}

fn decide_playback_feasibility(
    plan: &PlaybackPlan,
    workload: &PlaybackWorkloadClass,
    policy: &EffectivePlaybackPolicy,
) -> PlaybackFeasibilityDecision {
    if matches!(
        plan.mode,
        PlaybackMode::DirectPlay | PlaybackMode::DirectStream
    ) {
        return PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::AllowDirect,
            reason: "server_transcode_not_required".to_string(),
            support_decision: PlaybackSupportDecision::Supported,
            performance_decision: PlaybackPerformanceDecision::RealtimeSafe,
            confidence: PlaybackPerformanceConfidence::StaticInferred,
            selected_envelope_id: None,
            selected_hardware_api: None,
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: None,
            selected_envelope_sample_count: None,
            realtime_required_millis: 1000,
            reasons: vec!["server_transcode_not_required".to_string()],
            warnings: Vec::new(),
            remediation_codes: Vec::new(),
            background_probe_queued: false,
        };
    }

    if matches!(
        plan.mode,
        PlaybackMode::AudioTranscode | PlaybackMode::SubtitleTranscode
    ) {
        return PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::AllowTranscode,
            reason: "partial_transcode_video_copyable".to_string(),
            support_decision: PlaybackSupportDecision::Supported,
            performance_decision: PlaybackPerformanceDecision::RealtimeSafe,
            confidence: PlaybackPerformanceConfidence::StaticInferred,
            selected_envelope_id: None,
            selected_hardware_api: None,
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: None,
            selected_envelope_sample_count: None,
            realtime_required_millis: 1000,
            reasons: vec!["partial_transcode_video_copyable".to_string()],
            warnings: Vec::new(),
            remediation_codes: Vec::new(),
            background_probe_queued: false,
        };
    }

    if let Some(envelope) = select_performance_envelope(plan, workload, policy) {
        return decision_from_envelope(plan, envelope);
    }

    static_feasibility_decision(plan, workload, policy)
}

fn decision_from_envelope(
    plan: &PlaybackPlan,
    envelope: &PlaybackPerformanceEnvelope,
) -> PlaybackFeasibilityDecision {
    let selected_hardware_api = plan.hardware_acceleration.api.clone();
    let mut reasons = envelope.reasons.clone();
    let mut warnings = envelope.warnings.clone();
    let mut remediation_codes = envelope.remediation_codes.clone();
    let action = match (envelope.support_decision, envelope.performance_decision) {
        (PlaybackSupportDecision::Unsupported, _) => {
            if plan
                .hardware_acceleration
                .fallback
                .as_deref()
                .is_some_and(|fallback| fallback.eq_ignore_ascii_case("software"))
            {
                push_unique(&mut reasons, "software_fallback");
                push_unique(&mut warnings, "hardware_path_unsupported_software_fallback");
                PlaybackFeasibilityAction::SoftwareFallback
            } else {
                push_unique(&mut reasons, "hardware_decode_unsupported");
                push_unique(
                    &mut remediation_codes,
                    "update_driver_or_use_original_quality",
                );
                PlaybackFeasibilityAction::Reject
            }
        }
        (_, PlaybackPerformanceDecision::RealtimeSafe) => PlaybackFeasibilityAction::AllowTranscode,
        (_, PlaybackPerformanceDecision::RealtimeMarginal) => {
            push_unique(&mut warnings, "transcode_realtime_marginal");
            PlaybackFeasibilityAction::AllowWithWarning
        }
        (_, PlaybackPerformanceDecision::NotRealtime) => {
            push_unique(&mut reasons, workload_not_realtime_reason(plan));
            push_unique(
                &mut remediation_codes,
                "use_original_quality_or_lower_quality",
            );
            PlaybackFeasibilityAction::Reject
        }
        (_, PlaybackPerformanceDecision::Unknown) => {
            push_unique(&mut reasons, "transcode_performance_unknown_policy_denied");
            push_unique(
                &mut remediation_codes,
                "try_original_quality_or_lower_quality",
            );
            PlaybackFeasibilityAction::Reject
        }
    };
    let reason = reasons
        .first()
        .cloned()
        .unwrap_or_else(|| action.as_str().to_string());
    PlaybackFeasibilityDecision {
        action,
        reason,
        support_decision: envelope.support_decision,
        performance_decision: envelope.performance_decision,
        confidence: envelope.confidence,
        selected_envelope_id: Some(envelope.id.clone()),
        selected_hardware_api,
        selected_envelope_p50_realtime_factor_millis: envelope.p50_realtime_factor_millis,
        selected_envelope_p95_realtime_factor_millis: envelope.p95_realtime_factor_millis,
        selected_envelope_startup_latency_ms: envelope.startup_latency_ms,
        selected_envelope_first_segment_latency_ms: envelope.first_segment_latency_ms,
        selected_envelope_failure_count: Some(envelope.failure_count),
        selected_envelope_sample_count: Some(envelope.sample_count),
        realtime_required_millis: 1000,
        reasons,
        warnings,
        remediation_codes,
        background_probe_queued: false,
    }
}

fn static_feasibility_decision(
    plan: &PlaybackPlan,
    workload: &PlaybackWorkloadClass,
    policy: &EffectivePlaybackPolicy,
) -> PlaybackFeasibilityDecision {
    let selected_hardware_api = plan.hardware_acceleration.api.clone();
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();
    let mut remediation_codes = Vec::new();
    if static_known_not_realtime(workload) {
        push_unique(&mut reasons, workload_not_realtime_reason(plan));
        push_unique(
            &mut remediation_codes,
            "use_original_quality_or_lower_quality",
        );
        return PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::Reject,
            reason: reasons[0].clone(),
            support_decision: support_decision_for_plan(plan),
            performance_decision: PlaybackPerformanceDecision::NotRealtime,
            confidence: PlaybackPerformanceConfidence::StaticInferred,
            selected_envelope_id: None,
            selected_hardware_api,
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: None,
            selected_envelope_sample_count: None,
            realtime_required_millis: 1000,
            reasons,
            warnings,
            remediation_codes,
            background_probe_queued: false,
        };
    }

    match policy.unknown_performance_policy {
        UnknownPerformancePolicy::Deny => {
            push_unique(&mut reasons, "transcode_performance_unknown_policy_denied");
            push_unique(
                &mut remediation_codes,
                "try_original_quality_or_lower_quality",
            );
            PlaybackFeasibilityDecision {
                action: PlaybackFeasibilityAction::Reject,
                reason: "transcode_performance_unknown_policy_denied".to_string(),
                support_decision: support_decision_for_plan(plan),
                performance_decision: PlaybackPerformanceDecision::Unknown,
                confidence: PlaybackPerformanceConfidence::Unknown,
                selected_envelope_id: None,
                selected_hardware_api,
                selected_envelope_p50_realtime_factor_millis: None,
                selected_envelope_p95_realtime_factor_millis: None,
                selected_envelope_startup_latency_ms: None,
                selected_envelope_first_segment_latency_ms: None,
                selected_envelope_failure_count: None,
                selected_envelope_sample_count: None,
                realtime_required_millis: 1000,
                reasons,
                warnings,
                remediation_codes,
                background_probe_queued: false,
            }
        }
        UnknownPerformancePolicy::AllowBestEffort => {
            push_unique(&mut warnings, "transcode_performance_unknown_best_effort");
            PlaybackFeasibilityDecision {
                action: PlaybackFeasibilityAction::AllowWithWarning,
                reason: "transcode_performance_unknown_best_effort".to_string(),
                support_decision: support_decision_for_plan(plan),
                performance_decision: PlaybackPerformanceDecision::Unknown,
                confidence: PlaybackPerformanceConfidence::Unknown,
                selected_envelope_id: None,
                selected_hardware_api,
                selected_envelope_p50_realtime_factor_millis: None,
                selected_envelope_p95_realtime_factor_millis: None,
                selected_envelope_startup_latency_ms: None,
                selected_envelope_first_segment_latency_ms: None,
                selected_envelope_failure_count: None,
                selected_envelope_sample_count: None,
                realtime_required_millis: 1000,
                reasons: vec!["transcode_performance_unknown_best_effort".to_string()],
                warnings,
                remediation_codes,
                background_probe_queued: false,
            }
        }
    }
}

fn select_performance_envelope<'a>(
    plan: &PlaybackPlan,
    workload: &PlaybackWorkloadClass,
    policy: &'a EffectivePlaybackPolicy,
) -> Option<&'a PlaybackPerformanceEnvelope> {
    let selected_api = plan.hardware_acceleration.api.as_deref();
    policy
        .performance_envelopes
        .iter()
        .filter(|envelope| envelope.workload_class_id == workload.class_id)
        .filter(|envelope| envelope.pipeline_signature == workload.pipeline_signature)
        .filter(|envelope| {
            envelope.hardware_api.as_deref().is_none()
                || envelope.hardware_api.as_deref() == selected_api
        })
        .max_by_key(|envelope| confidence_rank(envelope.confidence))
}

fn confidence_rank(confidence: PlaybackPerformanceConfidence) -> i32 {
    match confidence {
        PlaybackPerformanceConfidence::Certified => 4,
        PlaybackPerformanceConfidence::LocalBenchmark => 3,
        PlaybackPerformanceConfidence::LiveObserved => 2,
        PlaybackPerformanceConfidence::StaticInferred => 1,
        PlaybackPerformanceConfidence::Unknown => 0,
    }
}

fn static_known_not_realtime(workload: &PlaybackWorkloadClass) -> bool {
    workload.cost_labels.iter().any(|label| label == "8k")
}

fn support_decision_for_plan(plan: &PlaybackPlan) -> PlaybackSupportDecision {
    if !plan.hardware_acceleration.enabled {
        PlaybackSupportDecision::SoftwareOnly
    } else if plan
        .hardware_acceleration
        .warnings
        .iter()
        .any(|warning| warning.contains("unsupported"))
    {
        PlaybackSupportDecision::MixedFallback
    } else {
        PlaybackSupportDecision::Supported
    }
}

fn workload_not_realtime_reason(plan: &PlaybackPlan) -> &'static str {
    if matches!(plan.hdr_action, HdrAction::ToneMapToSdr) {
        "server_cannot_realtime_tonemap_source"
    } else if plan.subtitle_action == StreamAction::BurnIn {
        "server_cannot_realtime_burn_subtitles"
    } else {
        "server_cannot_realtime_transcode_source"
    }
}

fn pipeline_stages_for_plan(plan: &PlaybackPlan) -> Vec<String> {
    if plan.video_action != StreamAction::Transcode {
        return vec!["video_copy".to_string()];
    }
    let mut stages = Vec::new();
    if plan.hardware_acceleration.decoder.is_some() {
        stages.push("hardware_decode".to_string());
    } else {
        stages.push("software_decode".to_string());
    }
    if plan
        .video_output
        .as_ref()
        .is_some_and(video_filter_graph_requires_software)
    {
        stages.push("software_filter".to_string());
    }
    if plan.hardware_acceleration.encoder.is_some() {
        stages.push("hardware_encode".to_string());
    } else {
        stages.push("software_encode".to_string());
    }
    if plan.audio_action == StreamAction::Transcode {
        stages.push("audio_transcode".to_string());
    }
    if plan.subtitle_action == StreamAction::BurnIn {
        stages.push("subtitle_burn_in".to_string());
    }
    stages
}

fn lower_realtime_safe_adaptive_rung(plan: &PlaybackPlan) -> Option<(String, VideoOutputPlan)> {
    let ladder = plan.adaptive_ladder.as_ref()?;
    ladder
        .rungs
        .iter()
        .find(|rung| rung.height <= 1080)
        .or_else(|| ladder.rungs.last())
        .map(|rung| (rung.id.clone(), rung.video.clone()))
}

fn resolution_cost_label(height: Option<i32>) -> &'static str {
    match height.unwrap_or_default() {
        height if height >= 4320 => "8k",
        height if height >= 2160 => "4k",
        height if height >= 1440 => "1440p",
        height if height >= 1080 => "1080p",
        height if height >= 720 => "720p",
        _ => "sd",
    }
}

fn subtitle_burn_cost_label(subtitle: Option<&SubtitleStreamCapabilities>) -> &'static str {
    match subtitle.map(|subtitle| &subtitle.kind) {
        Some(SubtitleKind::Image) => "image_subtitle_burn_in",
        Some(SubtitleKind::Text) => "text_subtitle_burn_in",
        _ => "subtitle_burn_in",
    }
}

fn audio_cost_label(audio: Option<&AudioStreamCapabilities>) -> &'static str {
    if audio
        .and_then(|audio| audio.channels)
        .is_some_and(|channels| channels > 6)
    {
        "high_channel_audio_transcode"
    } else {
        "audio_transcode"
    }
}

fn workload_class_id(
    plan: &PlaybackPlan,
    video: &VideoStreamCapabilities,
    output_codec: Option<&str>,
    output_height: Option<i32>,
    cost_labels: &[String],
    pipeline_signature: &str,
) -> String {
    let codec = video.codec.as_deref().unwrap_or("unknown");
    let source_resolution = resolution_cost_label(video.height);
    let output_resolution = resolution_cost_label(output_height);
    let hdr = match plan.hdr_action {
        HdrAction::ToneMapToSdr => "hdr_tonemap",
        HdrAction::DirectDolbyVision => "dolby_vision_direct",
        HdrAction::DirectHdr10Fallback => "hdr10_fallback",
        HdrAction::Unsupported => "hdr_unsupported",
        HdrAction::UnknownFailClosed => "hdr_unknown",
        HdrAction::Direct => "hdr_direct",
        HdrAction::None | HdrAction::Unknown => "sdr",
    };
    let subtitle = match plan.subtitle_action {
        StreamAction::BurnIn => "sub_burn",
        StreamAction::ConvertTextToWebvtt => "sub_webvtt",
        StreamAction::Passthrough | StreamAction::Copy => "sub_copy",
        StreamAction::Disabled | StreamAction::Drop => "sub_none",
        StreamAction::Transcode => "sub_transcode",
    };
    let labels = if cost_labels.is_empty() {
        "baseline".to_string()
    } else {
        cost_labels
            .iter()
            .map(|label| sanitize_class_token(label))
            .collect::<Vec<_>>()
            .join("-")
    };
    [
        sanitize_class_token(plan.mode.as_str()),
        sanitize_class_token(codec),
        sanitize_class_token(source_resolution),
        sanitize_class_token(output_codec.unwrap_or("copy")),
        sanitize_class_token(output_resolution),
        sanitize_class_token(hdr),
        sanitize_class_token(subtitle),
        sanitize_class_token(pipeline_signature),
        labels,
    ]
    .join(":")
}

fn sanitize_class_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::{
        media::ffprobe,
        playback::{
            hardware::{
                HARDWARE_READINESS_SCHEMA_VERSION, HardwareCapabilities, HardwareCapabilityMatrix,
                HardwareCodecMatrixEntry, HardwareCodecSupport, HardwareFilterMatrix,
                HardwareReadinessStatus,
            },
            plan::StreamAction,
            probe::{SubtitleKind, normalize_ffprobe_metadata},
            profile::{
                AbrSupportType, ClientPlaybackProfile, NetworkClass, NetworkPlaybackPolicy,
                ServerPlaybackPolicy, derive_effective_playback_policy,
            },
        },
    };

    fn capabilities(raw: &str) -> MediaCapabilities {
        let value: Value = serde_json::from_str(raw).unwrap();
        let parsed: ffprobe::FfprobeStreams = serde_json::from_value(value.clone()).unwrap();
        let metadata = ffprobe::MediaMetadata {
            container: parsed
                .format
                .as_ref()
                .and_then(|format| format.format_name.clone()),
            video_codec: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.codec_name.clone()),
            audio_codec: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("audio"))
                .and_then(|stream| stream.codec_name.clone()),
            width: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.width),
            height: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.height),
            bitrate_bps: parsed
                .format
                .as_ref()
                .and_then(|format| format.bit_rate.as_deref())
                .and_then(|value| value.parse::<i64>().ok()),
            duration_seconds: parsed
                .format
                .as_ref()
                .and_then(|format| format.duration.as_deref())
                .and_then(|value| value.parse::<f64>().ok())
                .map(|seconds| seconds.round() as i32),
            streams: parsed.streams,
            format: parsed.format,
            chapters: parsed.chapters,
            raw_json: value,
        };
        normalize_ffprobe_metadata(&metadata, None, None)
    }

    fn server_policy() -> ServerPlaybackPolicy {
        ServerPlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..ServerPlaybackPolicy::default()
        }
    }

    fn network_policy(
        network_class: NetworkClass,
        max_bitrate_bps: Option<i64>,
    ) -> NetworkPlaybackPolicy {
        NetworkPlaybackPolicy {
            network_class,
            max_bitrate_bps,
            max_remote_bitrate_bps: match network_class {
                NetworkClass::Wan | NetworkClass::Unknown => max_bitrate_bps,
                NetworkClass::Lan => None,
            },
            max_resolution: None,
            server_upload_cap_bps: None,
        }
    }

    fn h264_1080p60_capabilities() -> MediaCapabilities {
        capabilities(
            r#"
            {
              "format": {
                "format_name": "matroska,webm",
                "bit_rate": "12000000",
                "duration": "30.000000"
              },
              "streams": [
                {
                  "index": 0,
                  "codec_type": "video",
                  "codec_name": "h264",
                  "profile": "High",
                  "pix_fmt": "yuv420p",
                  "width": 1920,
                  "height": 1080,
                  "avg_frame_rate": "60/1",
                  "bit_rate": "11800000",
                  "level": 40
                },
                {
                  "index": 1,
                  "codec_type": "audio",
                  "codec_name": "aac",
                  "channels": 2,
                  "channel_layout": "stereo",
                  "sample_rate": "48000",
                  "bit_rate": "192000"
                }
              ]
            }
            "#,
        )
    }

    fn av1_4k_capabilities() -> MediaCapabilities {
        capabilities(
            r#"
            {
              "format": {
                "format_name": "matroska,webm",
                "bit_rate": "32000000",
                "duration": "30.000000"
              },
              "streams": [
                {
                  "index": 0,
                  "codec_type": "video",
                  "codec_name": "av1",
                  "profile": "Main",
                  "pix_fmt": "yuv420p10le",
                  "width": 3840,
                  "height": 2160,
                  "avg_frame_rate": "24000/1001",
                  "bit_rate": "31800000",
                  "level": 12
                },
                {
                  "index": 1,
                  "codec_type": "audio",
                  "codec_name": "opus",
                  "channels": 6,
                  "channel_layout": "5.1",
                  "sample_rate": "48000",
                  "bit_rate": "384000"
                }
              ]
            }
            "#,
        )
    }

    fn ffv1_mkv_capabilities() -> MediaCapabilities {
        capabilities(
            r#"
            {
              "format": {
                "format_name": "matroska,webm",
                "bit_rate": "16000000",
                "duration": "30.000000"
              },
              "streams": [
                {
                  "index": 0,
                  "codec_type": "video",
                  "codec_name": "ffv1",
                  "pix_fmt": "yuv420p",
                  "width": 1920,
                  "height": 1080,
                  "avg_frame_rate": "24000/1001",
                  "bit_rate": "15500000"
                },
                {
                  "index": 1,
                  "codec_type": "audio",
                  "codec_name": "flac",
                  "channels": 2,
                  "channel_layout": "stereo",
                  "sample_rate": "48000",
                  "bit_rate": "500000"
                }
              ]
            }
            "#,
        )
    }

    fn vp9_webm_capabilities() -> MediaCapabilities {
        capabilities(
            r#"
            {
              "format": {
                "format_name": "matroska,webm",
                "bit_rate": "9000000",
                "duration": "30.000000"
              },
              "streams": [
                {
                  "index": 0,
                  "codec_type": "video",
                  "codec_name": "vp9",
                  "profile": "Profile 0",
                  "pix_fmt": "yuv420p",
                  "width": 1920,
                  "height": 1080,
                  "avg_frame_rate": "30000/1001",
                  "bit_rate": "8500000"
                },
                {
                  "index": 1,
                  "codec_type": "audio",
                  "codec_name": "opus",
                  "channels": 2,
                  "channel_layout": "stereo",
                  "sample_rate": "48000",
                  "bit_rate": "192000"
                }
              ]
            }
            "#,
        )
    }

    fn unsupported_video_codec_capabilities() -> MediaCapabilities {
        capabilities(
            r#"
            {
              "format": {
                "format_name": "matroska,webm",
                "bit_rate": "9000000",
                "duration": "30.000000"
              },
              "streams": [
                {
                  "index": 0,
                  "codec_type": "video",
                  "codec_name": "unsupported_vendor_codec",
                  "pix_fmt": "yuv420p",
                  "width": 1920,
                  "height": 1080,
                  "avg_frame_rate": "24000/1001",
                  "bit_rate": "8500000"
                },
                {
                  "index": 1,
                  "codec_type": "audio",
                  "codec_name": "aac",
                  "channels": 2,
                  "channel_layout": "stereo",
                  "sample_rate": "48000",
                  "bit_rate": "192000"
                }
              ]
            }
            "#,
        )
    }

    fn videotoolbox_capabilities() -> HardwareCapabilities {
        HardwareCapabilities {
            platform: "macos-x86_64".to_string(),
            ffmpeg_version: Some("ffmpeg fixture".to_string()),
            available_apis: vec!["videotoolbox".to_string()],
            capability_matrices: vec![HardwareCapabilityMatrix {
                schema_version: HARDWARE_READINESS_SCHEMA_VERSION,
                api: "videotoolbox".to_string(),
                status: HardwareReadinessStatus::Available,
                encode: vec![HardwareCodecMatrixEntry {
                    codec: "h264".to_string(),
                    profile: Some("high".to_string()),
                    bit_depth: Some(8),
                    pixel_formats: vec!["yuv420p".to_string(), "nv12".to_string()],
                    ffmpeg_encoder: Some("h264_videotoolbox".to_string()),
                    ffmpeg_decoder: None,
                    status: "supported".to_string(),
                }],
                decode: vec![
                    HardwareCodecMatrixEntry {
                        codec: "h264".to_string(),
                        profile: None,
                        bit_depth: Some(8),
                        pixel_formats: Vec::new(),
                        ffmpeg_encoder: None,
                        ffmpeg_decoder: Some("videotoolbox".to_string()),
                        status: "supported".to_string(),
                    },
                    HardwareCodecMatrixEntry {
                        codec: "hevc".to_string(),
                        profile: None,
                        bit_depth: Some(8),
                        pixel_formats: Vec::new(),
                        ffmpeg_encoder: None,
                        ffmpeg_decoder: Some("videotoolbox".to_string()),
                        status: "supported".to_string(),
                    },
                ],
                filters: HardwareFilterMatrix::default(),
            }],
            supported_decode_codecs: vec![
                HardwareCodecSupport {
                    api: "videotoolbox".to_string(),
                    codec: "h264".to_string(),
                    ffmpeg_name: "videotoolbox".to_string(),
                },
                HardwareCodecSupport {
                    api: "videotoolbox".to_string(),
                    codec: "hevc".to_string(),
                    ffmpeg_name: "videotoolbox".to_string(),
                },
            ],
            supported_encode_codecs: vec![HardwareCodecSupport {
                api: "videotoolbox".to_string(),
                codec: "h264".to_string(),
                ffmpeg_name: "h264_videotoolbox".to_string(),
            }],
            max_sessions: None,
            hdr_tone_mapping: false,
            subtitle_burn_in_limitations: vec![
                "subtitle_burn_in_requires_software_filter".to_string(),
            ],
            startup_probes: Vec::new(),
            detection_errors: Vec::new(),
        }
    }

    fn nvenc_capabilities() -> HardwareCapabilities {
        HardwareCapabilities {
            platform: "windows-x86_64".to_string(),
            ffmpeg_version: Some("ffmpeg fixture".to_string()),
            available_apis: vec!["nvenc".to_string()],
            capability_matrices: vec![HardwareCapabilityMatrix {
                schema_version: HARDWARE_READINESS_SCHEMA_VERSION,
                api: "nvenc".to_string(),
                status: HardwareReadinessStatus::Available,
                encode: vec![HardwareCodecMatrixEntry {
                    codec: "h264".to_string(),
                    profile: Some("high".to_string()),
                    bit_depth: Some(8),
                    pixel_formats: vec!["yuv420p".to_string(), "nv12".to_string()],
                    ffmpeg_encoder: Some("h264_nvenc".to_string()),
                    ffmpeg_decoder: None,
                    status: "supported".to_string(),
                }],
                decode: vec![HardwareCodecMatrixEntry {
                    codec: "h264".to_string(),
                    profile: None,
                    bit_depth: Some(8),
                    pixel_formats: Vec::new(),
                    ffmpeg_encoder: None,
                    ffmpeg_decoder: Some("cuda".to_string()),
                    status: "supported".to_string(),
                }],
                filters: HardwareFilterMatrix::default(),
            }],
            supported_decode_codecs: vec![HardwareCodecSupport {
                api: "nvenc".to_string(),
                codec: "h264".to_string(),
                ffmpeg_name: "cuda".to_string(),
            }],
            supported_encode_codecs: vec![HardwareCodecSupport {
                api: "nvenc".to_string(),
                codec: "h264".to_string(),
                ffmpeg_name: "h264_nvenc".to_string(),
            }],
            max_sessions: None,
            hdr_tone_mapping: false,
            subtitle_burn_in_limitations: Vec::new(),
            startup_probes: Vec::new(),
            detection_errors: Vec::new(),
        }
    }

    fn video_transcode_policy() -> EffectivePlaybackPolicy {
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(3_000_000);
        policy
    }

    fn envelope_for_plan(
        plan: &PlaybackPlan,
        support_decision: PlaybackSupportDecision,
        performance_decision: PlaybackPerformanceDecision,
        confidence: PlaybackPerformanceConfidence,
    ) -> PlaybackPerformanceEnvelope {
        let workload = plan
            .workload_class
            .as_ref()
            .expect("plan should have a workload class");
        PlaybackPerformanceEnvelope {
            id: "phase20-envelope".to_string(),
            host_fingerprint: "host-fixture".to_string(),
            os_family: "windows".to_string(),
            os_version: Some("11".to_string()),
            gpu_vendor: plan
                .hardware_acceleration
                .enabled
                .then(|| "nvidia".to_string()),
            gpu_model: plan
                .hardware_acceleration
                .enabled
                .then(|| "fixture gpu".to_string()),
            gpu_driver_version: plan
                .hardware_acceleration
                .enabled
                .then(|| "fixture driver".to_string()),
            hardware_api: plan.hardware_acceleration.api.clone(),
            ffmpeg_path: Some("ffmpeg".to_string()),
            ffmpeg_version: Some("ffmpeg fixture".to_string()),
            ffmpeg_sha256: Some("sha256:fixture".to_string()),
            elixir_version: Some("test".to_string()),
            workload_class_id: workload.class_id.clone(),
            pipeline_signature: workload.pipeline_signature.clone(),
            support_decision,
            performance_decision,
            confidence,
            p50_realtime_factor_millis: Some(1_250),
            p95_realtime_factor_millis: Some(match performance_decision {
                PlaybackPerformanceDecision::RealtimeSafe => 1_500,
                PlaybackPerformanceDecision::RealtimeMarginal => 975,
                PlaybackPerformanceDecision::NotRealtime => 650,
                PlaybackPerformanceDecision::Unknown => 0,
            }),
            startup_latency_ms: Some(350),
            first_segment_latency_ms: Some(750),
            failure_count: 0,
            sample_count: 8,
            invalidation_fingerprint: "invalidation-fixture".to_string(),
            last_observed_at: Some("2026-07-01T00:00:00Z".to_string()),
            reasons: Vec::new(),
            warnings: Vec::new(),
            remediation_codes: Vec::new(),
        }
    }

    #[test]
    fn planner_goldens_from_probe_fixtures_without_filesystem() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let plan = plan_playback(
            "file-1",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::native_mpv(),
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(plan.mode, PlaybackMode::DirectPlay);
        assert_eq!(
            plan.reasons,
            vec!["direct_play_all_capabilities_satisfied".to_string()]
        );

        let mut browser_policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            ..EffectivePlaybackPolicy::default()
        };
        browser_policy.max_bitrate_bps = Some(20_000_000);
        let mut browser = ClientPlaybackProfile::browser_like();
        browser.ass_complexity_support = AssComplexitySupport::SimpleWebvtt;
        let browser_plan = plan_playback(
            "file-1",
            &media,
            PlaybackSelection::default(),
            &browser,
            &browser_policy,
        );
        assert_eq!(browser_plan.mode, PlaybackMode::DirectStream);
        assert_eq!(browser_plan.video_action, StreamAction::Copy);
        assert_eq!(browser_plan.audio_action, StreamAction::Copy);
    }

    #[test]
    fn phase20_weird_codec_direct_plays_when_client_supports_it() {
        let media = ffv1_mkv_capabilities();
        let mut client = ClientPlaybackProfile::native_mpv();
        client.supported_video_codecs.push("ffv1".to_string());

        let plan = plan_playback(
            "phase20-ffv1-direct-play",
            &media,
            PlaybackSelection::default(),
            &client,
            &EffectivePlaybackPolicy::default(),
        );

        assert!(plan.playable, "{:?}", plan.reasons);
        assert_eq!(plan.mode, PlaybackMode::DirectPlay);
        assert_eq!(plan.video_action, StreamAction::Passthrough);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(feasibility.action, PlaybackFeasibilityAction::AllowDirect);
    }

    #[test]
    fn phase20_weird_codec_uses_software_decode_when_realtime_envelope_is_safe() {
        let media = vp9_webm_capabilities();
        let mut policy = video_transcode_policy();
        policy.unknown_performance_policy = UnknownPerformancePolicy::AllowBestEffort;

        let dry_run = plan_playback(
            "phase20-vp9-software-decode-dry-run",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );
        assert_eq!(dry_run.mode, PlaybackMode::VideoTranscode);
        assert_eq!(dry_run.hardware_acceleration.decoder, None);
        assert!(
            dry_run
                .workload_class
                .as_ref()
                .expect("workload class")
                .pipeline_stages
                .contains(&"software_decode".to_string())
        );

        policy.unknown_performance_policy = UnknownPerformancePolicy::Deny;
        policy.performance_envelopes = vec![envelope_for_plan(
            &dry_run,
            PlaybackSupportDecision::SoftwareOnly,
            PlaybackPerformanceDecision::RealtimeSafe,
            PlaybackPerformanceConfidence::Certified,
        )];
        let plan = plan_playback(
            "phase20-vp9-software-decode",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert!(plan.playable, "{:?}", plan.reasons);
        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(
            feasibility.action,
            PlaybackFeasibilityAction::AllowTranscode
        );
        assert_eq!(
            feasibility.support_decision,
            PlaybackSupportDecision::SoftwareOnly
        );
    }

    #[test]
    fn phase20_weird_codec_rejects_cleanly_when_software_decode_is_unsupported() {
        let media = unsupported_video_codec_capabilities();
        let mut policy = video_transcode_policy();
        policy.unknown_performance_policy = UnknownPerformancePolicy::AllowBestEffort;

        let plan = plan_playback(
            "phase20-unsupported-codec",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert!(!plan.playable);
        assert_eq!(plan.video_action, StreamAction::Disabled);
        assert!(plan.expected_outputs.is_empty());
        assert!(
            plan.reasons
                .contains(&"software_decode_unsupported".to_string()),
            "{:?}",
            plan.reasons
        );
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(feasibility.action, PlaybackFeasibilityAction::Reject);
        assert!(
            feasibility
                .reasons
                .contains(&"software_decode_unsupported".to_string()),
            "{:?}",
            feasibility.reasons
        );
    }

    #[test]
    fn phase20_direct_play_bypasses_unknown_performance_fail_closed_policy() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let policy = EffectivePlaybackPolicy {
            unknown_performance_policy: UnknownPerformancePolicy::Deny,
            ..EffectivePlaybackPolicy::default()
        };

        let plan = plan_playback(
            "phase20-direct",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::native_mpv(),
            &policy,
        );

        assert!(plan.playable);
        assert_eq!(plan.mode, PlaybackMode::DirectPlay);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(feasibility.action, PlaybackFeasibilityAction::AllowDirect);
        assert_eq!(feasibility.reason, "server_transcode_not_required");
    }

    #[test]
    fn phase20_unknown_video_transcode_performance_can_fail_closed_before_job_start() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let mut policy = video_transcode_policy();
        policy.unknown_performance_policy = UnknownPerformancePolicy::Deny;

        let plan = plan_playback(
            "phase20-unknown-denied",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert!(!plan.playable);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(feasibility.action, PlaybackFeasibilityAction::Reject);
        assert_eq!(
            feasibility.reason,
            "transcode_performance_unknown_policy_denied"
        );
        assert!(
            plan.reasons
                .contains(&"transcode_performance_unknown_policy_denied".to_string()),
            "{:?}",
            plan.reasons
        );
    }

    #[test]
    fn phase20_realtime_safe_envelope_admits_matching_video_transcode() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let policy = video_transcode_policy();
        let dry_run = plan_playback(
            "phase20-envelope-dry-run",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );
        assert_eq!(dry_run.mode, PlaybackMode::VideoTranscode);

        let mut policy = policy;
        policy.unknown_performance_policy = UnknownPerformancePolicy::Deny;
        policy.performance_envelopes = vec![envelope_for_plan(
            &dry_run,
            PlaybackSupportDecision::Supported,
            PlaybackPerformanceDecision::RealtimeSafe,
            PlaybackPerformanceConfidence::Certified,
        )];

        let plan = plan_playback(
            "phase20-envelope-admitted",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert!(plan.playable, "{:?}", plan.reasons);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(
            feasibility.action,
            PlaybackFeasibilityAction::AllowTranscode
        );
        assert_eq!(
            feasibility.selected_envelope_id.as_deref(),
            Some("phase20-envelope")
        );
        assert_eq!(
            feasibility.confidence,
            PlaybackPerformanceConfidence::Certified
        );
    }

    #[test]
    fn phase20_not_realtime_tonemap_envelope_rejects_before_transcode_job() {
        let media = capabilities(include_str!("fixtures/hevc_hdr_pgs.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            force_sdr_output: true,
            max_resolution: Some("1080p".to_string()),
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(50_000_000);
        let selection = PlaybackSelection {
            audio_stream_index: Some(1),
            subtitle_stream_index: Some(2),
            start_position_seconds: None,
            ..PlaybackSelection::default()
        };
        let dry_run = plan_playback(
            "phase20-tonemap-dry-run",
            &media,
            selection.clone(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );
        assert_eq!(dry_run.mode, PlaybackMode::VideoTranscode);
        assert!(matches!(dry_run.hdr_action, HdrAction::ToneMapToSdr));

        policy.performance_envelopes = vec![envelope_for_plan(
            &dry_run,
            PlaybackSupportDecision::Supported,
            PlaybackPerformanceDecision::NotRealtime,
            PlaybackPerformanceConfidence::Certified,
        )];

        let plan = plan_playback(
            "phase20-tonemap-rejected",
            &media,
            selection,
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert!(!plan.playable);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(feasibility.action, PlaybackFeasibilityAction::Reject);
        assert_eq!(feasibility.reason, "server_cannot_realtime_tonemap_source");
        assert!(
            plan.reasons
                .contains(&"server_cannot_realtime_tonemap_source".to_string()),
            "{:?}",
            plan.reasons
        );
    }

    #[test]
    fn phase20_unsupported_hardware_envelope_falls_back_to_software_when_allowed() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            hardware_capabilities: nvenc_capabilities(),
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(3_000_000);
        let dry_run = plan_playback(
            "phase20-hardware-fallback-dry-run",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );
        assert!(dry_run.hardware_acceleration.enabled);
        assert_eq!(dry_run.hardware_acceleration.api.as_deref(), Some("nvenc"));

        policy.performance_envelopes = vec![envelope_for_plan(
            &dry_run,
            PlaybackSupportDecision::Unsupported,
            PlaybackPerformanceDecision::Unknown,
            PlaybackPerformanceConfidence::Certified,
        )];

        let plan = plan_playback(
            "phase20-hardware-fallback",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert!(plan.playable, "{:?}", plan.reasons);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(
            feasibility.action,
            PlaybackFeasibilityAction::SoftwareFallback
        );
        assert!(!plan.hardware_acceleration.enabled);
        assert_eq!(
            plan.video_output
                .as_ref()
                .map(|output| output.encoder.as_str()),
            Some("libx264")
        );
        assert!(
            plan.warnings
                .contains(&"hardware_path_unsupported_software_fallback".to_string()),
            "{:?}",
            plan.warnings
        );
    }

    #[test]
    fn phase20_non_av1_gpu_keeps_hardware_h264_encode_with_software_av1_decode_when_safe() {
        let media = av1_4k_capabilities();
        let mut policy = video_transcode_policy();
        policy.hardware_acceleration = "nvenc".to_string();
        policy.hardware_capabilities = nvenc_capabilities();
        policy.unknown_performance_policy = UnknownPerformancePolicy::AllowBestEffort;

        let dry_run = plan_playback(
            "phase20-av1-software-decode-hardware-encode-dry-run",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );
        assert_eq!(dry_run.mode, PlaybackMode::VideoTranscode);
        assert_eq!(
            dry_run.hardware_acceleration.encoder.as_deref(),
            Some("h264_nvenc")
        );
        assert_eq!(dry_run.hardware_acceleration.decoder, None);
        let workload = dry_run
            .workload_class
            .as_ref()
            .expect("dry run should classify workload");
        assert!(workload.cost_labels.contains(&"av1_source".to_string()));
        assert!(
            workload
                .pipeline_stages
                .contains(&"software_decode".to_string())
        );
        assert!(
            workload
                .pipeline_stages
                .contains(&"hardware_encode".to_string())
        );

        policy.unknown_performance_policy = UnknownPerformancePolicy::Deny;
        policy.performance_envelopes = vec![envelope_for_plan(
            &dry_run,
            PlaybackSupportDecision::MixedFallback,
            PlaybackPerformanceDecision::RealtimeSafe,
            PlaybackPerformanceConfidence::Certified,
        )];
        let plan = plan_playback(
            "phase20-av1-software-decode-hardware-encode",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert!(plan.playable, "{:?}", plan.reasons);
        assert_eq!(
            plan.hardware_acceleration.encoder.as_deref(),
            Some("h264_nvenc")
        );
        assert_eq!(plan.hardware_acceleration.decoder, None);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(
            feasibility.action,
            PlaybackFeasibilityAction::AllowTranscode
        );
        assert_eq!(
            feasibility.support_decision,
            PlaybackSupportDecision::MixedFallback
        );
    }

    #[test]
    fn phase20_static_8k_adaptive_source_downgrades_to_lower_rung_before_job_start() {
        let mut media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let video = media.video_streams.first_mut().unwrap();
        video.width = Some(7680);
        video.height = Some(4320);
        video.bitrate_bps = Some(60_000_000);
        media.overall_bitrate_bps = Some(60_000_000);
        let mut client = ClientPlaybackProfile::browser_like();
        client.quality_mode = QualityMode::Automatic;
        client.max_bitrate_bps = Some(60_000_000);
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            allow_adaptive_transcode: true,
            max_bitrate_bps: Some(60_000_000),
            abr_support_type: AbrSupportType::HlsJs,
            ..EffectivePlaybackPolicy::default()
        };
        policy.unknown_performance_policy = UnknownPerformancePolicy::AllowBestEffort;

        let plan = plan_playback(
            "phase20-adaptive-downgrade",
            &media,
            PlaybackSelection::default(),
            &client,
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::AdaptiveTranscode);
        assert!(plan.playable, "{:?}", plan.reasons);
        let feasibility = plan.feasibility.as_ref().expect("feasibility decision");
        assert_eq!(
            feasibility.action,
            PlaybackFeasibilityAction::DowngradeQuality
        );
        let ladder = plan.adaptive_ladder.as_ref().expect("adaptive ladder");
        let active = ladder
            .rungs
            .iter()
            .find(|rung| rung.id == ladder.active_rung_id)
            .expect("active rung");
        assert!(active.height <= 1080, "{active:?}");
        assert!(
            plan.warnings
                .contains(&"playback_quality_downgraded_for_realtime_feasibility".to_string()),
            "{:?}",
            plan.warnings
        );
    }

    #[test]
    fn phase3_client_profiles_and_effective_policy_acceptance() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let server = server_policy();

        let native = ClientPlaybackProfile::native_mpv();
        let native_lan_policy = derive_effective_playback_policy(
            &native,
            &server,
            &network_policy(NetworkClass::Lan, Some(20_000_000)),
        );
        let native_plan = plan_playback(
            "file-native",
            &media,
            PlaybackSelection::default(),
            &native,
            &native_lan_policy,
        );
        assert_eq!(native_plan.mode, PlaybackMode::DirectPlay);

        let mut browser = ClientPlaybackProfile::browser_like();
        browser.ass_complexity_support = AssComplexitySupport::SimpleWebvtt;
        let browser_lan_policy = derive_effective_playback_policy(
            &browser,
            &server,
            &network_policy(NetworkClass::Lan, Some(20_000_000)),
        );
        let browser_plan = plan_playback(
            "file-browser",
            &media,
            PlaybackSelection::default(),
            &browser,
            &browser_lan_policy,
        );
        assert_eq!(browser_plan.mode, PlaybackMode::DirectStream);
        assert_eq!(browser_plan.delivery, Delivery::HlsFmp4);
        assert_eq!(browser_plan.video_action, StreamAction::Copy);
        assert_eq!(browser_plan.audio_action, StreamAction::Copy);

        let wan_policy = derive_effective_playback_policy(
            &browser,
            &server,
            &network_policy(NetworkClass::Wan, Some(3_000_000)),
        );
        let wan_plan = plan_playback(
            "file-wan",
            &media,
            PlaybackSelection::default(),
            &browser,
            &wan_policy,
        );
        assert_eq!(wan_plan.mode, PlaybackMode::VideoTranscode);
        assert!(
            wan_plan
                .reasons
                .contains(&"source_bitrate_exceeds_bandwidth_policy".to_string()),
            "{:?}",
            wan_plan.reasons
        );

        let no_transcode_server = ServerPlaybackPolicy {
            allow_direct_stream: false,
            allow_audio_transcode: false,
            allow_video_transcode: false,
            ..ServerPlaybackPolicy::default()
        };
        let disabled_policy = derive_effective_playback_policy(
            &browser,
            &no_transcode_server,
            &network_policy(NetworkClass::Lan, Some(20_000_000)),
        );
        let disabled_plan = plan_playback(
            "file-disabled",
            &media,
            PlaybackSelection::default(),
            &browser,
            &disabled_policy,
        );
        assert!(!disabled_plan.playable);
        assert!(
            disabled_plan
                .reasons
                .contains(&"video_transcode_disabled_by_policy".to_string()),
            "{:?}",
            disabled_plan.reasons
        );
        assert!(
            disabled_plan
                .reasons
                .contains(&"container_not_direct_playable".to_string()),
            "{:?}",
            disabled_plan.reasons
        );
    }

    #[test]
    fn planner_distinguishes_audio_and_subtitle_facts() {
        let media = capabilities(include_str!("fixtures/h264_dts_ass.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(20_000_000);
        let mut browser = ClientPlaybackProfile::browser_like();
        browser.ass_complexity_support = AssComplexitySupport::SimpleWebvtt;

        let plan = plan_playback(
            "file-2",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::AudioTranscode);
        assert_eq!(plan.video_action, StreamAction::Copy);
        assert_eq!(plan.audio_action, StreamAction::Transcode);
        assert_eq!(plan.subtitle_action, StreamAction::ConvertTextToWebvtt);
        let audio_output = plan.audio_output.as_ref().unwrap();
        assert_eq!(audio_output.codec, "aac");
        assert_eq!(audio_output.channels, Some(2));
        assert_eq!(audio_output.bitrate_bps, Some(128_000));
        assert_eq!(audio_output.language.as_deref(), Some("eng"));
        assert_eq!(audio_output.title.as_deref(), Some("DTS 5.1"));
        assert!(
            audio_output
                .reasons
                .contains(&"audio_codec_conversion_required".to_string())
        );
        assert!(
            audio_output
                .reasons
                .contains(&"audio_channel_downmix_required".to_string())
        );
        assert!(
            audio_output
                .reasons
                .contains(&"audio_bitrate_cap_applied".to_string())
        );
        assert_eq!(media.subtitle_streams[0].kind, SubtitleKind::Text);
    }

    #[test]
    fn phase8_audio_transcode_targets_least_expensive_compatible_output() {
        let media = capabilities(include_str!("fixtures/h264_dts_ass.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(20_000_000);

        let mut aac_surround_client = ClientPlaybackProfile::browser_like();
        aac_surround_client.max_audio_channels = Some(6);
        let aac_surround_plan = plan_playback(
            "file-aac-surround",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: None,
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &aac_surround_client,
            &policy,
        );
        let output = aac_surround_plan.audio_output.as_ref().unwrap();
        assert_eq!(aac_surround_plan.mode, PlaybackMode::AudioTranscode);
        assert_eq!(output.codec, "aac");
        assert_eq!(output.channels, Some(6));
        assert_eq!(output.bitrate_bps, Some(384_000));
        assert!(
            !output
                .reasons
                .contains(&"audio_channel_downmix_required".to_string()),
            "{:?}",
            output.reasons
        );

        let mut ac3_only_client = ClientPlaybackProfile::browser_like();
        ac3_only_client.supported_audio_codecs = vec!["ac3".to_string()];
        ac3_only_client.max_audio_channels = Some(6);
        let ac3_plan = plan_playback(
            "file-ac3-only",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: None,
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &ac3_only_client,
            &policy,
        );
        let output = ac3_plan.audio_output.as_ref().unwrap();
        assert_eq!(ac3_plan.mode, PlaybackMode::AudioTranscode);
        assert_eq!(output.codec, "ac3");
        assert_eq!(output.channels, Some(6));
        assert_eq!(output.bitrate_bps, Some(448_000));
    }

    #[test]
    fn h264_level_helpers_require_4_2_for_1080p60_and_5_2_for_4k60() {
        assert_eq!(parse_h264_level("4.1"), Some(41));
        assert_eq!(parse_h264_level("41"), Some(41));
        assert_eq!(parse_h264_level("5"), Some(50));
        assert_eq!(required_h264_level(Some(1280), Some(720), 30.0), Some(31));
        assert_eq!(required_h264_level(Some(1920), Some(1080), 60.0), Some(42));
        assert_eq!(required_h264_level(Some(3840), Some(2160), 60.0), Some(52));
    }

    #[test]
    fn h264_output_level_raises_for_1080p60_video_transcode() {
        let media = h264_1080p60_capabilities();
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_play: false,
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            max_bitrate_bps: Some(3_000_000),
            ..EffectivePlaybackPolicy::default()
        };
        policy.video_encoder_level = "4.1".to_string();

        let plan = plan_playback(
            "h264-1080p60-output-level",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        let output = plan.video_output.as_ref().unwrap();
        assert_eq!(output.level.as_deref(), Some("4.2"));
        assert!(
            output
                .reasons
                .contains(&"h264_level_raised_for_output_geometry".to_string()),
            "{:?}",
            output.reasons
        );
    }

    #[test]
    fn adaptive_ladder_raises_1080p60_rung_level() {
        let media = h264_1080p60_capabilities();
        let mut client = ClientPlaybackProfile::browser_like();
        client.quality_mode = QualityMode::Automatic;
        client.max_bitrate_bps = Some(50_000_000);
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_play: false,
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            allow_adaptive_transcode: true,
            max_bitrate_bps: Some(50_000_000),
            abr_support_type: AbrSupportType::HlsJs,
            ..EffectivePlaybackPolicy::default()
        };
        policy.video_encoder_level = "4.1".to_string();

        let plan = plan_playback(
            "h264-1080p60-adaptive-level",
            &media,
            PlaybackSelection::default(),
            &client,
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::AdaptiveTranscode);
        let ladder = plan.adaptive_ladder.as_ref().unwrap();
        let full_hd_rung = ladder
            .rungs
            .iter()
            .find(|rung| rung.height == 1080)
            .expect("1080p rung should be present for 1080p source");
        assert_eq!(full_hd_rung.video.level.as_deref(), Some("4.2"));
        assert!(
            full_hd_rung
                .video
                .reasons
                .contains(&"h264_level_raised_for_output_geometry".to_string()),
            "{:?}",
            full_hd_rung.video.reasons
        );
    }

    #[test]
    fn dolby_vision_profile5_tone_map_declares_ictcp_input() {
        let media = capabilities(
            r#"
            {
              "format": {
                "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
                "bit_rate": "13366070",
                "duration": "29.950000"
              },
              "streams": [
                {
                  "index": 0,
                  "codec_type": "video",
                  "codec_name": "hevc",
                  "codec_tag_string": "dvh1",
                  "profile": "Main 10",
                  "pix_fmt": "yuv420p10le",
                  "width": 1920,
                  "height": 1080,
                  "avg_frame_rate": "60/1",
                  "bit_rate": "13359836",
                  "side_data_list": [
                    {
                      "side_data_type": "DOVI configuration record",
                      "dv_profile": 5,
                      "dv_bl_signal_compatibility_id": 0,
                      "rpu_present_flag": 1,
                      "bl_present_flag": 1,
                      "el_present_flag": 0
                    }
                  ]
                }
              ]
            }
            "#,
        );
        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_video_transcode: true,
            force_sdr_output: true,
            ..EffectivePlaybackPolicy::default()
        };

        let plan = plan_playback(
            "dv-p5-no-hdr10-fallback",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        let tone_map = plan
            .video_output
            .as_ref()
            .unwrap()
            .tone_map
            .as_ref()
            .unwrap();
        assert_eq!(tone_map.input_primaries.as_deref(), Some("bt2020"));
        assert_eq!(tone_map.input_transfer.as_deref(), Some("smpte2084"));
        assert_eq!(tone_map.input_matrix.as_deref(), Some("ictcp"));
        assert_eq!(tone_map.output_matrix, "bt709");
    }

    #[test]
    fn phase8_selected_text_subtitles_use_webvtt_without_video_transcode() {
        let mut media = capabilities(include_str!("fixtures/h264_dts_ass.json"));
        media.audio_streams[0].codec = Some("aac".to_string());
        media.audio_streams[0].channels = Some(2);
        media.audio_streams[0].bitrate_bps = Some(128_000);
        media.subtitle_streams[0].codec = Some("webvtt".to_string());
        media.subtitle_streams[0].kind = SubtitleKind::Text;
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(20_000_000);

        let plan = plan_playback(
            "file-webvtt-subtitle",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::SubtitleTranscode);
        assert_eq!(plan.video_action, StreamAction::Copy);
        assert_eq!(plan.audio_action, StreamAction::Copy);
        assert_eq!(plan.subtitle_action, StreamAction::ConvertTextToWebvtt);
        assert!(plan.audio_output.is_none());
    }

    #[test]
    fn phase8_native_text_subtitles_direct_play_unless_burn_in_explicit() {
        let media = capabilities(include_str!("fixtures/h264_dts_ass.json"));
        let native = ClientPlaybackProfile::native_mpv();
        let direct_plan = plan_playback(
            "file-native-ass",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &native,
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(direct_plan.mode, PlaybackMode::DirectPlay);
        assert_eq!(direct_plan.subtitle_action, StreamAction::Passthrough);

        let mut burn_client = native;
        burn_client.subtitle_burn_policy = SubtitleBurnPolicy::Always;
        let burn_plan = plan_playback(
            "file-native-ass-burn",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &burn_client,
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(burn_plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(burn_plan.subtitle_action, StreamAction::BurnIn);
        assert!(
            burn_plan
                .reasons
                .contains(&"subtitle_burn_in_requested".to_string()),
            "{:?}",
            burn_plan.reasons
        );
    }

    #[test]
    fn phase17_subtitle_selection_respects_off_default_forced_and_ass_capability() {
        let mut media = capabilities(include_str!("fixtures/h264_aac_text_subtitles.json"));
        media.subtitle_streams[0].is_default = true;
        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        let browser = ClientPlaybackProfile::browser_like();

        let off_plan = plan_playback(
            "phase17-subtitle-off",
            &media,
            PlaybackSelection {
                subtitle_mode: SubtitleSelectionMode::Off,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(off_plan.selected_subtitle_track, None);
        assert_eq!(off_plan.subtitle_action, StreamAction::Disabled);

        let default_plan = plan_playback(
            "phase17-subtitle-default",
            &media,
            PlaybackSelection {
                subtitle_mode: SubtitleSelectionMode::Default,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(default_plan.selected_subtitle_track, Some(2));
        assert_eq!(default_plan.mode, PlaybackMode::SubtitleTranscode);
        assert_eq!(
            default_plan.subtitle_action,
            StreamAction::ConvertTextToWebvtt
        );

        let mut forced_media = media.clone();
        forced_media.subtitle_streams[0].is_default = false;
        forced_media.subtitle_streams[1].is_forced = true;
        forced_media.subtitle_streams[1].language = Some("eng".to_string());
        let forced_plan = plan_playback(
            "phase17-subtitle-forced",
            &forced_media,
            PlaybackSelection {
                subtitle_mode: SubtitleSelectionMode::Forced,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(forced_plan.selected_subtitle_track, Some(3));
        assert_eq!(
            forced_plan.subtitle_action,
            StreamAction::ConvertTextToWebvtt
        );

        let ass_plan = plan_playback(
            "phase17-ass-default-burn",
            &media,
            PlaybackSelection {
                subtitle_stream_index: Some(4),
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(ass_plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(ass_plan.subtitle_action, StreamAction::BurnIn);
        let ass_burn_in = ass_plan
            .video_output
            .as_ref()
            .and_then(|output| output.burn_in.as_ref())
            .expect("ASS subtitles should burn in for browser-like clients");
        assert_eq!(ass_burn_in.stream_index, 4);
        assert_eq!(ass_burn_in.filter_stream_index, Some(2));

        let mut simple_ass_client = browser.clone();
        simple_ass_client.ass_complexity_support = AssComplexitySupport::SimpleWebvtt;
        let simple_ass_plan = plan_playback(
            "phase17-simple-ass-webvtt",
            &media,
            PlaybackSelection {
                subtitle_stream_index: Some(4),
                ..PlaybackSelection::default()
            },
            &simple_ass_client,
            &policy,
        );
        assert_eq!(simple_ass_plan.mode, PlaybackMode::SubtitleTranscode);
        assert_eq!(
            simple_ass_plan.subtitle_action,
            StreamAction::ConvertTextToWebvtt
        );
    }

    #[test]
    fn phase17_forced_image_subtitles_burn_only_when_policy_selects_them() {
        let mut media = capabilities(include_str!("fixtures/h264_aac_text_subtitles.json"));
        let subtitle = media.subtitle_streams.get_mut(0).unwrap();
        subtitle.codec = Some("pgs".to_string());
        subtitle.kind = SubtitleKind::Image;
        subtitle.title = Some("Forced PGS".to_string());
        subtitle.is_default = false;
        subtitle.is_forced = true;

        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        let browser = ClientPlaybackProfile::browser_like();

        let off_plan = plan_playback(
            "phase17-forced-image-off",
            &media,
            PlaybackSelection {
                subtitle_mode: SubtitleSelectionMode::Off,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(off_plan.selected_subtitle_track, None);
        assert_eq!(off_plan.subtitle_action, StreamAction::Disabled);
        assert_eq!(off_plan.video_action, StreamAction::Copy);
        assert!(
            !off_plan
                .reasons
                .contains(&"subtitle_requires_burn_in".to_string()),
            "{:?}",
            off_plan.reasons
        );

        let forced_plan = plan_playback(
            "phase17-forced-image-burn",
            &media,
            PlaybackSelection {
                subtitle_mode: SubtitleSelectionMode::Forced,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(forced_plan.selected_subtitle_track, Some(2));
        assert_eq!(forced_plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(forced_plan.subtitle_action, StreamAction::BurnIn);
        assert_eq!(
            forced_plan
                .video_output
                .as_ref()
                .and_then(|output| output.burn_in.as_ref())
                .map(|burn| burn.mode),
            Some(SubtitleBurnInMode::Image)
        );
        assert!(
            forced_plan
                .reasons
                .contains(&"subtitle_requires_burn_in".to_string()),
            "{:?}",
            forced_plan.reasons
        );

        let mut forced_disabled_client = browser;
        forced_disabled_client.forced_subtitle_policy = ForcedSubtitlePolicy::Disabled;
        let disabled_plan = plan_playback(
            "phase17-forced-image-disabled-policy",
            &media,
            PlaybackSelection {
                subtitle_mode: SubtitleSelectionMode::Forced,
                ..PlaybackSelection::default()
            },
            &forced_disabled_client,
            &policy,
        );
        assert_eq!(disabled_plan.selected_subtitle_track, None);
        assert_eq!(disabled_plan.subtitle_action, StreamAction::Disabled);
        assert_eq!(disabled_plan.video_action, StreamAction::Copy);
    }

    #[test]
    fn phase17_image_subtitle_aliases_plan_image_burn_in() {
        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        let browser = ClientPlaybackProfile::browser_like();

        for codec in [
            "pgs",
            "hdmv_pgs_subtitle",
            "dvd_subtitle",
            "dvdsub",
            "vobsub",
            "sub",
            "idx",
            "xsub",
        ] {
            let mut media = capabilities(include_str!("fixtures/h264_aac_text_subtitles.json"));
            let subtitle = media.subtitle_streams.get_mut(0).unwrap();
            subtitle.codec = Some(codec.to_string());
            subtitle.kind = SubtitleKind::Image;
            subtitle.title = Some(format!("{codec} forced"));
            subtitle.is_default = false;
            subtitle.is_forced = true;

            let plan = plan_playback(
                &format!("phase17-image-alias-{codec}"),
                &media,
                PlaybackSelection {
                    subtitle_mode: SubtitleSelectionMode::Forced,
                    ..PlaybackSelection::default()
                },
                &browser,
                &policy,
            );

            assert_eq!(plan.mode, PlaybackMode::VideoTranscode, "{codec}");
            assert_eq!(plan.subtitle_action, StreamAction::BurnIn, "{codec}");
            let burn_in = plan
                .video_output
                .as_ref()
                .and_then(|output| output.burn_in.as_ref());
            assert_eq!(
                burn_in.map(|burn| burn.mode),
                Some(SubtitleBurnInMode::Image),
                "{codec}: {plan:?}"
            );
            assert_eq!(
                burn_in.map(|burn| burn.codec.as_str()),
                Some(codec),
                "{codec}: {plan:?}"
            );
        }
    }

    #[test]
    fn phase17_external_image_subtitle_sidecar_plans_image_burn_in() {
        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        let browser = ClientPlaybackProfile::browser_like();
        let mut media = capabilities(include_str!("fixtures/h264_aac_text_subtitles.json"));
        let subtitle = media.subtitle_streams.get_mut(0).unwrap();
        subtitle.index = Some(-100_000);
        subtitle.external_id = Some("external-vobsub".to_string());
        subtitle.external_path = Some("/media/Phase17.External.VobSub.idx".to_string());
        subtitle.codec = Some("idx".to_string());
        subtitle.kind = SubtitleKind::Image;
        subtitle.title = Some("External VobSub".to_string());
        subtitle.is_default = false;
        subtitle.is_forced = true;

        let plan = plan_playback(
            "phase17-external-vobsub-sidecar",
            &media,
            PlaybackSelection {
                subtitle_mode: SubtitleSelectionMode::Forced,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(plan.selected_subtitle_track, Some(-100_000));
        assert_eq!(plan.subtitle_action, StreamAction::BurnIn);
        let burn_in = plan
            .video_output
            .as_ref()
            .and_then(|output| output.burn_in.as_ref())
            .expect("external image subtitle should burn in");
        assert_eq!(burn_in.mode, SubtitleBurnInMode::Image);
        assert_eq!(burn_in.stream_index, 0);
        assert_eq!(burn_in.codec, "idx");
        assert_eq!(
            burn_in.external_path.as_deref(),
            Some("/media/Phase17.External.VobSub.idx")
        );
        assert!(
            plan.reasons
                .contains(&"subtitle_requires_burn_in".to_string()),
            "{:?}",
            plan.reasons
        );
    }

    #[test]
    fn phase18_hdr_actions_are_explicit_and_unknown_hdr_fails_closed() {
        let base = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let native = ClientPlaybackProfile::native_mpv();

        let mut dolby_vision = base.clone();
        let video = dolby_vision.video_streams.get_mut(0).unwrap();
        video.dolby_vision = true;
        video.dolby_vision_profile = Some(8);
        video.hdr10 = false;
        video.dolby_vision_has_hdr10_fallback = false;
        let mut dv_client = native.clone();
        dv_client.supports_dolby_vision = true;
        let dv_plan = plan_playback(
            "phase18-dv-direct",
            &dolby_vision,
            PlaybackSelection::default(),
            &dv_client,
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(dv_plan.hdr_action, HdrAction::DirectDolbyVision);
        assert!(
            dv_plan
                .reasons
                .contains(&"hdr_direct_dolby_vision".to_string()),
            "{:?}",
            dv_plan.reasons
        );

        dolby_vision.video_streams[0].dolby_vision_has_hdr10_fallback = true;
        let fallback_plan = plan_playback(
            "phase18-dv-hdr10-fallback",
            &dolby_vision,
            PlaybackSelection::default(),
            &native,
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(fallback_plan.hdr_action, HdrAction::DirectHdr10Fallback);
        assert!(
            fallback_plan
                .reasons
                .contains(&"hdr_direct_hdr10_fallback".to_string()),
            "{:?}",
            fallback_plan.reasons
        );
        assert!(
            fallback_plan
                .reasons
                .contains(&"dolby_vision_hdr10_fallback_selected".to_string()),
            "{:?}",
            fallback_plan.reasons
        );

        let mut dolby_vision_no_fallback = dolby_vision.clone();
        dolby_vision_no_fallback.video_streams[0].dolby_vision_has_hdr10_fallback = false;
        let dv_sdr_plan = plan_playback(
            "phase18-dv-no-fallback-sdr",
            &dolby_vision_no_fallback,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(
            dv_sdr_plan.hdr_action,
            HdrAction::Unsupported,
            "reasons={:?}",
            dv_sdr_plan.reasons
        );
        assert!(!dv_sdr_plan.playable);
        assert!(
            dv_sdr_plan.reasons.contains(&"hdr_unsupported".to_string()),
            "{:?}",
            dv_sdr_plan.reasons
        );
        assert!(
            dv_sdr_plan
                .reasons
                .contains(&"dolby_vision_hdr10_fallback_missing".to_string()),
            "{:?}",
            dv_sdr_plan.reasons
        );

        let mut dolby_vision_sdr_fallback = dolby_vision.clone();
        dolby_vision_sdr_fallback.video_streams[0].dolby_vision_has_hdr10_fallback = true;
        let dv_sdr_fallback_plan = plan_playback(
            "phase18-dv-fallback-tonemap-sdr",
            &dolby_vision_sdr_fallback,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(dv_sdr_fallback_plan.hdr_action, HdrAction::ToneMapToSdr);
        assert!(
            dv_sdr_fallback_plan
                .reasons
                .contains(&"dolby_vision_hdr10_fallback_tone_map_to_sdr".to_string()),
            "{:?}",
            dv_sdr_fallback_plan.reasons
        );
        assert!(
            dv_sdr_fallback_plan
                .video_output
                .as_ref()
                .and_then(|output| output.tone_map.as_ref())
                .is_some()
        );

        let mut hdr10_plus = base.clone();
        hdr10_plus.video_streams[0].hdr10 = true;
        hdr10_plus.video_streams[0].hdr10_plus = true;
        let mut hdr_client = native.clone();
        hdr_client.supports_hdr = true;
        hdr_client.supports_hdr10_plus = false;
        let hdr10_plus_plan = plan_playback(
            "phase18-hdr10-plus-fallback",
            &hdr10_plus,
            PlaybackSelection::default(),
            &hdr_client,
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(hdr10_plus_plan.hdr_action, HdrAction::DirectHdr10Fallback);
        assert!(
            hdr10_plus_plan
                .reasons
                .contains(&"hdr_direct_hdr10_fallback".to_string()),
            "{:?}",
            hdr10_plus_plan.reasons
        );
        assert!(
            hdr10_plus_plan
                .reasons
                .contains(&"hdr10_plus_hdr10_fallback_selected".to_string()),
            "{:?}",
            hdr10_plus_plan.reasons
        );

        let mut unknown_hdr = base;
        let video = unknown_hdr.video_streams.get_mut(0).unwrap();
        video.bit_depth = Some(10);
        video.color_transfer = Some("smpte2084".to_string());
        video.color_primaries = Some("bt2020".to_string());
        video.color_matrix = Some("bt2020nc".to_string());
        video.hdr10 = false;
        video.hdr10_plus = false;
        video.dolby_vision = false;
        let unknown_plan = plan_playback(
            "phase18-unknown-hdr",
            &unknown_hdr,
            PlaybackSelection::default(),
            &native,
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(unknown_plan.hdr_action, HdrAction::UnknownFailClosed);
        assert!(!unknown_plan.playable);
        assert!(
            unknown_plan
                .reasons
                .contains(&"hdr_unknown_fail_closed".to_string()),
            "{:?}",
            unknown_plan.reasons
        );
    }

    #[test]
    fn phase4_planner_decision_engine_acceptance_goldens() {
        let h264_aac = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let h264_dts_ass = capabilities(include_str!("fixtures/h264_dts_ass.json"));
        let hevc_hdr_pgs = capabilities(include_str!("fixtures/hevc_hdr_pgs.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(50_000_000);
        policy.max_resolution = Some("1080p".to_string());

        let native_plan = plan_playback(
            "native-file",
            &h264_aac,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::native_mpv(),
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(native_plan.mode, PlaybackMode::DirectPlay);
        assert_eq!(
            native_plan.reasons,
            vec!["direct_play_all_capabilities_satisfied".to_string()]
        );
        assert_eq!(
            native_plan.expected_outputs[0],
            ExpectedOutput::new("direct_file", "direct_file")
        );
        assert!(native_plan.compatibility_report.media_file_id_valid);
        assert!(
            native_plan
                .compatibility_report
                .checks
                .iter()
                .any(|check| check.category == "container" && check.passed)
        );

        let browser = ClientPlaybackProfile::browser_like();
        let browser_plan = plan_playback(
            "browser-file",
            &h264_aac,
            PlaybackSelection::default(),
            &browser,
            &policy,
        );
        assert_eq!(browser_plan.mode, PlaybackMode::DirectStream);
        assert_eq!(
            browser_plan.reasons,
            vec!["direct_stream_codecs_copyable_container_changed".to_string()]
        );
        assert!(browser_plan.expected_outputs.iter().any(|output| {
            output.name == "master.m3u8" && output.kind == "hls_master_playlist"
        }));
        assert!(
            browser_plan.expected_outputs.iter().any(|output| {
                output.name == "media.m3u8" && output.kind == "hls_media_playlist"
            })
        );
        assert!(
            browser_plan
                .expected_outputs
                .iter()
                .any(|output| { output.name == "init.mp4" && output.kind == "hls_init_segment" })
        );
        assert!(browser_plan.expected_outputs.iter().any(|output| {
            output.name == "segment_*.m4s" && output.kind == "hls_media_segment_pattern"
        }));
        assert_eq!(browser_plan.selected_video_track, Some(0));
        assert_eq!(browser_plan.selected_audio_track, Some(1));

        let mut mpegts_only_browser = browser.clone();
        mpegts_only_browser.supported_hls_segment_types = vec!["mpegts".to_string()];
        let mpegts_only_plan = plan_playback(
            "browser-mpegts-only-file",
            &h264_aac,
            PlaybackSelection::default(),
            &mpegts_only_browser,
            &policy,
        );
        assert_eq!(mpegts_only_plan.mode, PlaybackMode::DirectStream);
        assert_eq!(mpegts_only_plan.delivery, Delivery::HlsMpegts);
        assert!(mpegts_only_plan.expected_outputs.iter().any(|output| {
            output.name == "segment_*.ts" && output.kind == "hls_media_segment_pattern"
        }));

        let mut mpeg2_source = h264_aac.clone();
        mpeg2_source.video_streams[0].codec = Some("mpeg2video".to_string());
        let mut mpeg2_browser = browser.clone();
        mpeg2_browser
            .supported_video_codecs
            .push("mpeg2video".to_string());
        let mpeg2_plan = plan_playback(
            "mpeg2-browser-file",
            &mpeg2_source,
            PlaybackSelection::default(),
            &mpeg2_browser,
            &policy,
        );
        assert_eq!(mpeg2_plan.mode, PlaybackMode::DirectStream);
        assert_eq!(mpeg2_plan.delivery, Delivery::HlsMpegts);
        assert!(mpeg2_plan.expected_outputs.iter().any(|output| {
            output.name == "segment_*.ts" && output.kind == "hls_media_segment_pattern"
        }));
        assert!(
            !mpeg2_plan
                .expected_outputs
                .iter()
                .any(|output| output.name == "init.mp4")
        );

        let audio_plan = plan_playback(
            "audio-file",
            &h264_dts_ass,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: None,
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(audio_plan.mode, PlaybackMode::AudioTranscode);
        assert_eq!(audio_plan.video_action, StreamAction::Copy);
        assert_eq!(audio_plan.audio_action, StreamAction::Transcode);
        assert!(audio_plan.video_output.is_none());
        assert_eq!(
            audio_plan.reasons.first().map(String::as_str),
            Some("audio_transcode_video_copyable_audio_codec_not_supported")
        );
        assert!(
            audio_plan
                .reasons
                .contains(&"audio_codec_conversion_required".to_string()),
            "{:?}",
            audio_plan.reasons
        );

        let mut h264_aac_ass = h264_dts_ass.clone();
        h264_aac_ass.audio_streams[0].codec = Some("aac".to_string());
        h264_aac_ass.audio_streams[0].channels = Some(2);
        h264_aac_ass.audio_streams[0].bitrate_bps = Some(128_000);
        let subtitle_plan = plan_playback(
            "subtitle-file",
            &h264_aac_ass,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(subtitle_plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(subtitle_plan.video_action, StreamAction::Transcode);
        assert_eq!(subtitle_plan.audio_action, StreamAction::Transcode);
        assert_eq!(subtitle_plan.subtitle_action, StreamAction::BurnIn);
        assert!(subtitle_plan.audio_output.is_some());
        let subtitle_video = subtitle_plan.video_output.as_ref().unwrap();
        assert!(subtitle_video.burn_in.is_some());
        assert!(
            subtitle_plan
                .reasons
                .contains(&"subtitle_not_supported".to_string()),
            "{:?}",
            subtitle_plan.reasons
        );

        let burn_in_plan = plan_playback(
            "pgs-file",
            &hevc_hdr_pgs,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(burn_in_plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(burn_in_plan.subtitle_action, StreamAction::BurnIn);
        assert_eq!(
            burn_in_plan.video_transcode_reason.as_deref(),
            Some("hdr_tone_mapping_required")
        );
        let burn_in_video = burn_in_plan.video_output.as_ref().unwrap();
        assert_eq!(burn_in_video.codec, "h264");
        assert_eq!(burn_in_video.encoder, "libx264");
        assert_eq!(burn_in_video.profile.as_deref(), Some("high"));
        assert_eq!(burn_in_video.level.as_deref(), Some("4.1"));
        assert_eq!(burn_in_video.pixel_format.as_deref(), Some("yuv420p"));
        assert_eq!(burn_in_video.bitrate_bps, Some(8_000_000));
        assert_eq!(burn_in_video.maxrate_bps, Some(8_000_000));
        assert_eq!(burn_in_video.bufsize_bps, Some(16_000_000));
        assert_eq!(
            burn_in_video
                .scale
                .as_ref()
                .map(|scale| (scale.width, scale.height)),
            Some((1920, 1080))
        );
        assert_eq!(
            burn_in_video
                .tone_map
                .as_ref()
                .map(|tone| tone.algorithm.as_str()),
            Some("hable")
        );
        assert_eq!(burn_in_video.frame_rate.mode, VideoFrameRateMode::Source);
        assert_eq!(
            burn_in_video.burn_in.as_ref().map(|burn| burn.stream_index),
            Some(2)
        );
        assert!(
            burn_in_plan
                .reasons
                .contains(&"subtitle_requires_burn_in".to_string()),
            "{:?}",
            burn_in_plan.reasons
        );

        let no_subtitle_plan = plan_playback(
            "no-subtitle-file",
            &h264_aac,
            PlaybackSelection::default(),
            &browser,
            &policy,
        );
        assert!(
            no_subtitle_plan
                .reasons
                .iter()
                .all(|reason| !reason.contains("subtitle")),
            "{:?}",
            no_subtitle_plan.reasons
        );

        let mut wan_policy = policy.clone();
        wan_policy.max_bitrate_bps = Some(3_000_000);
        let wan_plan = plan_playback(
            "wan-file",
            &h264_aac,
            PlaybackSelection::default(),
            &browser,
            &wan_policy,
        );
        assert_eq!(wan_plan.mode, PlaybackMode::VideoTranscode);
        assert!(
            wan_plan
                .reasons
                .contains(&"source_bitrate_exceeds_policy".to_string()),
            "{:?}",
            wan_plan.reasons
        );

        let mut exhausted_policy = policy.clone();
        exhausted_policy.max_simultaneous_video_transcodes = Some(1);
        exhausted_policy.active_video_transcodes = 1;
        let exhausted_plan = plan_playback(
            "capacity-file",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &browser,
            &exhausted_policy,
        );
        assert!(!exhausted_plan.playable);
        assert!(
            exhausted_plan
                .reasons
                .contains(&"transcode_capacity_exhausted".to_string()),
            "{:?}",
            exhausted_plan.reasons
        );

        let mut automatic = browser.clone();
        automatic.quality_mode = QualityMode::Automatic;
        let mut adaptive_policy = policy.clone();
        adaptive_policy.allow_adaptive_transcode = true;
        let adaptive_plan = plan_playback(
            "adaptive-file",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &automatic,
            &adaptive_policy,
        );
        assert_eq!(adaptive_plan.mode, PlaybackMode::AdaptiveTranscode);
        assert!(adaptive_plan.adaptive);
        assert!(
            adaptive_plan
                .reasons
                .contains(&"adaptive_transcode_automatic_quality_requested".to_string()),
            "{:?}",
            adaptive_plan.reasons
        );
    }

    #[test]
    fn phase11_adaptive_quality_acceptance_gates() {
        let hevc_hdr_pgs = capabilities(include_str!("fixtures/hevc_hdr_pgs.json"));
        let mut automatic = ClientPlaybackProfile::browser_like();
        automatic.quality_mode = QualityMode::Automatic;
        automatic.max_bitrate_bps = Some(50_000_000);
        automatic.max_resolution = Some("2160p".to_string());

        let adaptive_policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            allow_adaptive_transcode: true,
            max_bitrate_bps: Some(50_000_000),
            abr_support_type: AbrSupportType::HlsJs,
            ..EffectivePlaybackPolicy::default()
        };

        let mut disabled_policy = adaptive_policy.clone();
        disabled_policy.allow_video_transcode = false;
        let disabled_plan = plan_playback(
            "adaptive-disabled-file",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &automatic,
            &disabled_policy,
        );
        assert_ne!(disabled_plan.mode, PlaybackMode::AdaptiveTranscode);
        assert!(!disabled_plan.playable);
        assert!(
            disabled_plan
                .reasons
                .contains(&"video_transcode_disabled_by_policy".to_string()),
            "{:?}",
            disabled_plan.reasons
        );

        let uncapped_plan = plan_playback(
            "adaptive-uncapped-file",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &automatic,
            &adaptive_policy,
        );
        assert_eq!(uncapped_plan.mode, PlaybackMode::AdaptiveTranscode);
        let uncapped_ladder = uncapped_plan
            .adaptive_ladder
            .as_ref()
            .expect("automatic playback should expose adaptive ladder");
        assert!(uncapped_ladder.rungs.len() >= 2, "{uncapped_ladder:?}");
        assert_eq!(
            uncapped_ladder.active_rung_id,
            uncapped_ladder.starting_rung_id
        );
        assert!(
            uncapped_ladder
                .rungs
                .iter()
                .any(|rung| rung.bandwidth_bps > 3_000_000),
            "{uncapped_ladder:?}"
        );
        assert!(
            uncapped_plan
                .expected_outputs
                .iter()
                .any(|output| output.name == "stream_0.m3u8"),
            "{:?}",
            uncapped_plan.expected_outputs
        );

        let mut capped_policy = adaptive_policy.clone();
        capped_policy.network_class = NetworkClass::Wan;
        capped_policy.max_remote_bitrate_bps = Some(3_000_000);
        capped_policy.server_upload_cap_bps = Some(4_000_000);
        let capped_plan = plan_playback(
            "adaptive-capped-file",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &automatic,
            &capped_policy,
        );
        assert_eq!(capped_plan.mode, PlaybackMode::AdaptiveTranscode);
        let capped_ladder = capped_plan
            .adaptive_ladder
            .as_ref()
            .expect("WAN-capped automatic playback should still expose ladder");
        assert!(
            capped_ladder
                .rungs
                .iter()
                .all(|rung| rung.bandwidth_bps <= 3_000_000),
            "{capped_ladder:?}"
        );
        assert!(
            capped_ladder.rungs.len() < uncapped_ladder.rungs.len(),
            "WAN caps should remove redundant capped rungs: uncapped={uncapped_ladder:?} capped={capped_ladder:?}"
        );
        assert!(
            capped_plan
                .reasons
                .contains(&"adaptive_remote_stream_cap_applied".to_string()),
            "{:?}",
            capped_plan.reasons
        );

        let mut exhausted_policy = adaptive_policy.clone();
        exhausted_policy.max_simultaneous_video_transcodes = Some(2);
        exhausted_policy.active_video_transcodes = 1;
        let exhausted_plan = plan_playback(
            "adaptive-capacity-file",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &automatic,
            &exhausted_policy,
        );
        assert!(!exhausted_plan.playable);
        assert!(
            exhausted_plan
                .reasons
                .contains(&"transcode_capacity_exhausted".to_string()),
            "{:?}",
            exhausted_plan.reasons
        );
        assert!(
            exhausted_plan
                .reasons
                .contains(&"adaptive_transcode_capacity_exhausted".to_string()),
            "{:?}",
            exhausted_plan.reasons
        );
    }

    #[test]
    fn phase21_adaptive_requires_explicit_automatic_quality_and_abr_support() {
        let hevc_hdr_pgs = capabilities(include_str!("fixtures/hevc_hdr_pgs.json"));
        let fixed = ClientPlaybackProfile::browser_like();
        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            allow_adaptive_transcode: true,
            max_bitrate_bps: Some(50_000_000),
            max_resolution: Some("2160p".to_string()),
            abr_support_type: AbrSupportType::HlsJs,
            ..EffectivePlaybackPolicy::default()
        };

        let fixed_plan = plan_playback(
            "phase21-fixed-not-adaptive",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &fixed,
            &policy,
        );
        assert_ne!(fixed_plan.mode, PlaybackMode::AdaptiveTranscode);
        assert!(!fixed_plan.adaptive);
        assert!(
            !fixed_plan
                .reasons
                .contains(&"adaptive_transcode_automatic_quality_requested".to_string()),
            "{:?}",
            fixed_plan.reasons
        );

        let mut no_abr = fixed.clone();
        no_abr.quality_mode = QualityMode::Automatic;
        no_abr.abr_support_type = AbrSupportType::None;
        let no_abr_plan = plan_playback(
            "phase21-no-abr-not-adaptive",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &no_abr,
            &policy,
        );
        assert_ne!(no_abr_plan.mode, PlaybackMode::AdaptiveTranscode);
        assert!(!no_abr_plan.adaptive);

        let mut automatic = fixed;
        automatic.quality_mode = QualityMode::Automatic;
        automatic.abr_support_type = AbrSupportType::HlsJs;
        let automatic_plan = plan_playback(
            "phase21-automatic-adaptive",
            &hevc_hdr_pgs,
            PlaybackSelection::default(),
            &automatic,
            &policy,
        );
        assert_eq!(automatic_plan.mode, PlaybackMode::AdaptiveTranscode);
        assert!(automatic_plan.adaptive);
        assert!(
            automatic_plan
                .reasons
                .contains(&"adaptive_transcode_automatic_quality_requested".to_string())
        );
    }

    #[test]
    fn phase21_fixed_lower_quality_selects_bounded_video_transcode() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let mut fixed = ClientPlaybackProfile::browser_like();
        fixed.quality_mode = QualityMode::Fixed;
        fixed.fixed_resolution = Some("720p".to_string());
        fixed.fixed_bitrate_bps = Some(2_000_000);
        let server = ServerPlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            max_resolution: Some("2160p".to_string()),
            max_simultaneous_video_transcodes: Some(2),
            ..ServerPlaybackPolicy::default()
        };
        let network = NetworkPlaybackPolicy {
            network_class: NetworkClass::Lan,
            max_bitrate_bps: None,
            max_remote_bitrate_bps: None,
            max_resolution: None,
            server_upload_cap_bps: None,
        };
        let policy = derive_effective_playback_policy(&fixed, &server, &network);

        let plan = plan_playback(
            "phase21-fixed-lower-quality",
            &media,
            PlaybackSelection::default(),
            &fixed,
            &policy,
        );

        assert_eq!(
            plan.mode,
            PlaybackMode::VideoTranscode,
            "{:?}",
            plan.reasons
        );
        let output = plan.video_output.as_ref().expect("video output");
        assert_eq!(output.scale.as_ref().map(|scale| scale.height), Some(720));
        assert_eq!(output.bitrate_bps, Some(2_000_000));
        assert_ne!(plan.mode, PlaybackMode::AdaptiveTranscode);
    }

    #[test]
    fn phase21_adaptive_ladder_metadata_respects_source_bounds_and_quality_caps() {
        let media = capabilities(include_str!("fixtures/hevc_hdr_pgs.json"));
        let mut automatic = ClientPlaybackProfile::browser_like();
        automatic.quality_mode = QualityMode::Automatic;
        automatic.abr_support_type = AbrSupportType::HlsJs;
        automatic.automatic_min_resolution = Some("480p".to_string());
        automatic.automatic_max_resolution = Some("1080p".to_string());
        automatic.automatic_min_bitrate_bps = Some(1_000_000);
        automatic.automatic_max_bitrate_bps = Some(5_000_000);
        automatic.max_resolution = Some("2160p".to_string());
        automatic.max_bitrate_bps = Some(50_000_000);
        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            allow_adaptive_transcode: true,
            max_bitrate_bps: Some(50_000_000),
            max_resolution: Some("2160p".to_string()),
            automatic_min_resolution: Some("480p".to_string()),
            automatic_max_resolution: Some("1080p".to_string()),
            automatic_min_bitrate_bps: Some(1_000_000),
            automatic_max_bitrate_bps: Some(5_000_000),
            abr_support_type: AbrSupportType::HlsJs,
            ..EffectivePlaybackPolicy::default()
        };

        let plan = plan_playback(
            "phase21-adaptive-metadata",
            &media,
            PlaybackSelection::default(),
            &automatic,
            &policy,
        );

        assert_eq!(
            plan.mode,
            PlaybackMode::AdaptiveTranscode,
            "{:?}",
            plan.reasons
        );
        let source_height = media
            .primary_video()
            .and_then(|video| video.height)
            .unwrap();
        let ladder = plan.adaptive_ladder.as_ref().expect("adaptive ladder");
        assert!(ladder.rungs.len() >= 2, "{ladder:?}");
        assert!(
            ladder.rungs.iter().all(|rung| rung.height <= source_height
                && rung.height <= 1080
                && rung.height >= 480),
            "{ladder:?}"
        );
        assert!(
            ladder
                .rungs
                .iter()
                .all(|rung| rung.bandwidth_bps <= 5_000_000),
            "{ladder:?}"
        );
        assert!(
            ladder.rungs.iter().all(|rung| {
                rung.average_bandwidth_bps > 0
                    && rung.average_bandwidth_bps <= rung.bandwidth_bps
                    && rung.resolution == format!("{}x{}", rung.width, rung.height)
                    && rung.codecs.contains("avc1.")
                    && rung.frame_rate.is_some()
            }),
            "{ladder:?}"
        );
    }

    #[test]
    fn phase10_planner_selects_videotoolbox_decode_encode_without_filters() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            hardware_capabilities: videotoolbox_capabilities(),
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(3_000_000);

        let plan = plan_playback(
            "hardware-file",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert!(plan.hardware_acceleration.enabled);
        assert_eq!(
            plan.hardware_acceleration.api.as_deref(),
            Some("videotoolbox")
        );
        assert_eq!(
            plan.hardware_acceleration.decoder.as_deref(),
            Some("videotoolbox")
        );
        assert_eq!(
            plan.hardware_acceleration.encoder.as_deref(),
            Some("h264_videotoolbox")
        );
        assert_eq!(
            plan.video_output
                .as_ref()
                .map(|output| output.encoder.as_str()),
            Some("h264_videotoolbox")
        );
    }

    #[test]
    fn phase10_planner_records_nvenc_decode_as_ffmpeg_cuda_token() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            hardware_capabilities: nvenc_capabilities(),
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(3_000_000);

        let plan = plan_playback(
            "hardware-file",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert!(plan.hardware_acceleration.enabled);
        assert_eq!(plan.hardware_acceleration.api.as_deref(), Some("nvenc"));
        assert_eq!(plan.hardware_acceleration.decoder.as_deref(), Some("cuda"));
        assert_eq!(
            plan.hardware_acceleration.encoder.as_deref(),
            Some("h264_nvenc")
        );
        assert_eq!(
            plan.hardware_acceleration.decode_status.as_deref(),
            Some("selected")
        );
        assert_eq!(
            plan.hardware_acceleration.encode_status.as_deref(),
            Some("selected")
        );
        assert!(plan.hardware_acceleration.warnings.is_empty());
    }

    #[test]
    fn phase10_planner_uses_matrix_over_flat_hardware_codec_lists() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let mut hardware_capabilities = nvenc_capabilities();
        hardware_capabilities.capability_matrices[0].encode[0].status =
            "unsupported_gpu".to_string();
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            allow_hardware_decode: false,
            hardware_capabilities,
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(3_000_000);

        let plan = plan_playback(
            "hardware-file",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert!(!plan.hardware_acceleration.enabled);
        assert_ne!(
            plan.video_output
                .as_ref()
                .map(|output| output.encoder.as_str()),
            Some("h264_nvenc")
        );
        assert_eq!(
            plan.hardware_acceleration.warnings,
            vec!["hardware_unavailable".to_string()]
        );
    }

    #[test]
    fn phase10_planner_falls_back_for_videotoolbox_h264_width_floor() {
        let mut media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let video = media.video_streams.first_mut().unwrap();
        video.width = Some(320);
        video.height = Some(180);
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            hardware_capabilities: videotoolbox_capabilities(),
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(3_000_000);

        let plan = plan_playback(
            "low-width-hardware-file",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert!(plan.playable);
        assert!(!plan.hardware_acceleration.enabled);
        assert_eq!(
            plan.video_output
                .as_ref()
                .map(|output| output.encoder.as_str()),
            Some("libx264")
        );
        assert!(
            plan.warnings
                .contains(&"hardware_encoder_min_width_unsupported:videotoolbox:h264".to_string()),
            "{:?}",
            plan.warnings
        );
    }

    #[test]
    fn phase10_planner_uses_hardware_encode_only_for_software_filter_graphs() {
        let media = capabilities(include_str!("fixtures/hevc_hdr_pgs.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            hardware_capabilities: videotoolbox_capabilities(),
            max_resolution: Some("1080p".to_string()),
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(50_000_000);

        let plan = plan_playback(
            "hardware-filter-file",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert!(plan.hardware_acceleration.enabled);
        assert_eq!(plan.hardware_acceleration.decoder, None);
        assert_eq!(
            plan.hardware_acceleration.encoder.as_deref(),
            Some("h264_videotoolbox")
        );
        assert!(
            plan.warnings
                .contains(&"hardware_decode_disabled_filter_graph".to_string()),
            "{:?}",
            plan.warnings
        );
        assert!(plan.video_output.as_ref().unwrap().tone_map.is_some());
        assert!(plan.video_output.as_ref().unwrap().burn_in.is_some());
    }

    #[test]
    fn phase10_explicit_unavailable_hardware_can_fail_instead_of_fallback() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            hardware_acceleration: "nvenc".to_string(),
            hardware_fallback: "fail".to_string(),
            ..EffectivePlaybackPolicy::default()
        };
        policy.max_bitrate_bps = Some(3_000_000);

        let plan = plan_playback(
            "hardware-fail-file",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &policy,
        );

        assert!(!plan.playable);
        assert!(
            plan.reasons
                .contains(&"hardware_unavailable:nvenc".to_string()),
            "{:?}",
            plan.reasons
        );
    }

    #[test]
    fn phase13_fixture_matrix_covers_required_media_shapes_and_profiles() {
        let h264_aac_mp4 = capabilities(include_str!("fixtures/h264_aac_mp4.json"));
        let h264_aac_mkv = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let h264_dts_ass = capabilities(include_str!("fixtures/h264_dts_ass.json"));
        let h264_aac_text_subtitles =
            capabilities(include_str!("fixtures/h264_aac_text_subtitles.json"));
        let hevc_hdr_aac_pgs_vobsub =
            capabilities(include_str!("fixtures/hevc_hdr_aac_pgs_vobsub.json"));
        let hevc_hdr_pgs = capabilities(include_str!("fixtures/hevc_hdr_pgs.json"));

        let browser = ClientPlaybackProfile::browser_like();
        let mut policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            max_bitrate_bps: Some(50_000_000),
            max_resolution: Some("2160p".to_string()),
            ..EffectivePlaybackPolicy::default()
        };

        let matrix_cases = [
            (
                "h264_aac_mp4",
                &h264_aac_mp4,
                PlaybackMode::DirectPlay,
                PlaybackMode::DirectPlay,
            ),
            (
                "h264_aac_mkv",
                &h264_aac_mkv,
                PlaybackMode::DirectPlay,
                PlaybackMode::DirectStream,
            ),
            (
                "h264_dts_ass",
                &h264_dts_ass,
                PlaybackMode::DirectPlay,
                PlaybackMode::AudioTranscode,
            ),
            (
                "h264_aac_text_subtitles",
                &h264_aac_text_subtitles,
                PlaybackMode::DirectPlay,
                PlaybackMode::DirectStream,
            ),
            (
                "hevc_hdr_aac_pgs_vobsub",
                &hevc_hdr_aac_pgs_vobsub,
                PlaybackMode::DirectPlay,
                PlaybackMode::VideoTranscode,
            ),
            (
                "hevc_hdr_pgs",
                &hevc_hdr_pgs,
                PlaybackMode::DirectPlay,
                PlaybackMode::VideoTranscode,
            ),
        ];
        for (fixture, media, native_mode, browser_mode) in matrix_cases {
            let native_plan = plan_playback(
                format!("phase13-matrix-{fixture}-native"),
                media,
                PlaybackSelection::default(),
                &ClientPlaybackProfile::native_mpv(),
                &EffectivePlaybackPolicy::default(),
            );
            assert_eq!(native_plan.mode, native_mode, "{fixture} native_mpv");

            let browser_plan = plan_playback(
                format!("phase13-matrix-{fixture}-browser"),
                media,
                PlaybackSelection::default(),
                &browser,
                &policy,
            );
            assert_eq!(browser_plan.mode, browser_mode, "{fixture} browser_like");
        }

        let mp4_native = plan_playback(
            "phase13-mp4-native",
            &h264_aac_mp4,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::native_mpv(),
            &EffectivePlaybackPolicy::default(),
        );
        assert_eq!(mp4_native.mode, PlaybackMode::DirectPlay);

        let mkv_browser = plan_playback(
            "phase13-mkv-browser",
            &h264_aac_mkv,
            PlaybackSelection::default(),
            &browser,
            &policy,
        );
        assert_eq!(mkv_browser.mode, PlaybackMode::DirectStream);
        assert_eq!(mkv_browser.video_action, StreamAction::Copy);
        assert_eq!(mkv_browser.audio_action, StreamAction::Copy);

        let dts_browser = plan_playback(
            "phase13-dts-browser",
            &h264_dts_ass,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: None,
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert_eq!(dts_browser.mode, PlaybackMode::AudioTranscode);
        assert_eq!(dts_browser.video_action, StreamAction::Copy);
        assert_eq!(dts_browser.audio_action, StreamAction::Transcode);

        policy.max_resolution = Some("1080p".to_string());
        let hdr_to_sdr = plan_playback(
            "phase13-hevc-hdr-browser",
            &hevc_hdr_aac_pgs_vobsub,
            PlaybackSelection::default(),
            &browser,
            &policy,
        );
        assert_eq!(hdr_to_sdr.mode, PlaybackMode::VideoTranscode);
        assert_eq!(
            hdr_to_sdr.video_transcode_reason.as_deref(),
            Some("hdr_tone_mapping_required")
        );
        assert!(hdr_to_sdr.video_output.as_ref().unwrap().tone_map.is_some());

        let mut remote_policy = policy.clone();
        remote_policy.network_class = NetworkClass::Wan;
        remote_policy.max_bitrate_bps = Some(4_000_000);
        remote_policy.max_remote_bitrate_bps = Some(4_000_000);
        let remote_cap_plan = plan_playback(
            "phase13-4k-remote-cap",
            &hevc_hdr_aac_pgs_vobsub,
            PlaybackSelection::default(),
            &browser,
            &remote_policy,
        );
        assert_eq!(remote_cap_plan.mode, PlaybackMode::VideoTranscode);
        assert!(
            remote_cap_plan
                .reasons
                .contains(&"source_bitrate_exceeds_policy".to_string())
                || remote_cap_plan
                    .reasons
                    .contains(&"source_bitrate_exceeds_bandwidth_policy".to_string()),
            "{:?}",
            remote_cap_plan.reasons
        );

        let mut text_policy = policy.clone();
        text_policy.max_bitrate_bps = Some(50_000_000);
        text_policy.max_resolution = Some("2160p".to_string());
        for subtitle_index in [2, 3] {
            let text_subtitle_plan = plan_playback(
                &format!("phase13-text-subtitle-{subtitle_index}"),
                &h264_aac_text_subtitles,
                PlaybackSelection {
                    audio_stream_index: Some(1),
                    subtitle_stream_index: Some(subtitle_index),
                    start_position_seconds: None,
                    ..PlaybackSelection::default()
                },
                &browser,
                &text_policy,
            );
            assert_ne!(text_subtitle_plan.mode, PlaybackMode::VideoTranscode);
            assert_eq!(text_subtitle_plan.video_action, StreamAction::Copy);
            assert!(matches!(
                text_subtitle_plan.subtitle_action,
                StreamAction::ConvertTextToWebvtt | StreamAction::Passthrough
            ));
        }

        let ass_subtitle_plan = plan_playback(
            "phase13-ass-subtitle-burn-in",
            &h264_aac_text_subtitles,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(4),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &text_policy,
        );
        assert_eq!(ass_subtitle_plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(ass_subtitle_plan.subtitle_action, StreamAction::BurnIn);
        assert!(
            ass_subtitle_plan
                .video_output
                .as_ref()
                .and_then(|output| output.burn_in.as_ref())
                .is_some()
        );
        assert_eq!(
            ass_subtitle_plan
                .video_output
                .as_ref()
                .and_then(|output| output.burn_in.as_ref())
                .and_then(|burn_in| burn_in.filter_stream_index),
            Some(2)
        );

        let no_subtitle_plan = plan_playback(
            "phase13-no-subtitle",
            &h264_aac_text_subtitles,
            PlaybackSelection::default(),
            &browser,
            &text_policy,
        );
        assert_eq!(no_subtitle_plan.mode, PlaybackMode::DirectStream);
        assert_eq!(no_subtitle_plan.subtitle_action, StreamAction::Disabled);

        for subtitle_index in [2, 3] {
            let image_subtitle_plan = plan_playback(
                &format!("phase13-image-subtitle-{subtitle_index}"),
                &hevc_hdr_aac_pgs_vobsub,
                PlaybackSelection {
                    audio_stream_index: Some(1),
                    subtitle_stream_index: Some(subtitle_index),
                    start_position_seconds: None,
                    ..PlaybackSelection::default()
                },
                &browser,
                &policy,
            );
            assert_eq!(image_subtitle_plan.mode, PlaybackMode::VideoTranscode);
            assert_eq!(image_subtitle_plan.subtitle_action, StreamAction::BurnIn);
            assert!(
                image_subtitle_plan
                    .video_output
                    .as_ref()
                    .and_then(|output| output.burn_in.as_ref())
                    .is_some()
            );
        }

        let corrupt_or_unreadable = MediaCapabilities::probe_failed("corrupt.mkv", "invalid data");
        let corrupt_plan = plan_playback(
            "phase13-corrupt",
            &corrupt_or_unreadable,
            PlaybackSelection::default(),
            &browser,
            &policy,
        );
        assert!(!corrupt_plan.playable);
        assert_eq!(corrupt_plan.reasons, vec!["probe_failed".to_string()]);
    }

    #[test]
    fn phase4_planner_validates_selected_tracks_and_start_position() {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let policy = EffectivePlaybackPolicy {
            allow_direct_stream: true,
            allow_audio_transcode: true,
            allow_video_transcode: true,
            ..EffectivePlaybackPolicy::default()
        };
        let browser = ClientPlaybackProfile::browser_like();

        let missing_audio = plan_playback(
            "file-missing-audio",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(99),
                subtitle_stream_index: None,
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert!(!missing_audio.playable);
        assert_eq!(
            missing_audio.reasons,
            vec!["selected_audio_track_not_found".to_string()]
        );

        let missing_subtitle = plan_playback(
            "file-missing-subtitle",
            &media,
            PlaybackSelection {
                audio_stream_index: None,
                subtitle_stream_index: Some(99),
                start_position_seconds: None,
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert!(!missing_subtitle.playable);
        assert_eq!(
            missing_subtitle.reasons,
            vec!["selected_subtitle_track_not_found".to_string()]
        );

        let bad_start = plan_playback(
            "file-bad-start",
            &media,
            PlaybackSelection {
                audio_stream_index: None,
                subtitle_stream_index: None,
                start_position_seconds: Some(-1),
                ..PlaybackSelection::default()
            },
            &browser,
            &policy,
        );
        assert!(!bad_start.playable);
        assert!(
            bad_start
                .reasons
                .contains(&"requested_start_position_invalid".to_string()),
            "{:?}",
            bad_start.reasons
        );
    }

    #[test]
    fn unprobed_or_failed_input_is_structured_not_playable() {
        let media = MediaCapabilities::probe_required("file-3");
        let plan = plan_playback(
            "file-3",
            &media,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &EffectivePlaybackPolicy::default(),
        );

        assert!(!plan.playable);
        assert_eq!(plan.reasons, vec!["probe_required".to_string()]);

        let failed = MediaCapabilities::probe_failed("bad.mkv", "ffprobe exited 1");
        let failed_plan = plan_playback(
            "file-4",
            &failed,
            PlaybackSelection::default(),
            &ClientPlaybackProfile::browser_like(),
            &EffectivePlaybackPolicy::default(),
        );
        assert!(!failed_plan.playable);
        assert_eq!(failed_plan.reasons, vec!["probe_failed".to_string()]);
    }
}
