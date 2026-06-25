use crate::playback::{
    hardware::{HardwareFallbackPolicy, HardwarePreference},
    plan::{
        AdaptiveAudioStrategy, AdaptiveLadderPlan, AdaptiveRungPlan, AudioOutputPlan,
        CompatibilityCheck, CompatibilityReport, Delivery, ExpectedOutput,
        HardwareAccelerationPlan, HdrAction, PLAYBACK_PLAN_VERSION, PlaybackMode, PlaybackPlan,
        SeekBehavior, StreamAction, SubtitleBurnInMode, SubtitleBurnInPlan, VideoFrameRateMode,
        VideoFrameRatePlan, VideoOutputPlan, VideoScalePlan, VideoToneMapPlan,
    },
    probe::{
        AudioStreamCapabilities, MediaCapabilities, ProbeStatus, SubtitleKind,
        SubtitleStreamCapabilities, VideoStreamCapabilities,
    },
    profile::{
        ClientKind, ClientPlaybackProfile, EffectivePlaybackPolicy, QualityMode, SubtitleBurnPolicy,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackSelection {
    pub audio_stream_index: Option<i32>,
    pub subtitle_stream_index: Option<i32>,
    pub start_position_seconds: Option<i32>,
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

    let subtitle = match selected.subtitle_stream_index {
        Some(index) => match media
            .subtitle_streams
            .iter()
            .find(|stream| stream.index == Some(index))
        {
            Some(stream) => Some(stream),
            None => {
                report.selected_subtitle_track = Some(index);
                report.checks.push(CompatibilityCheck::fail(
                    "subtitle_track",
                    "selected_subtitle_track_not_found",
                ));
                return not_playable_plan(
                    media_file_id,
                    vec!["selected_subtitle_track_not_found".to_string()],
                    report,
                );
            }
        },
        None => None,
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
        !matches!(hdr_action, HdrAction::ToneMapToSdr | HdrAction::Unsupported),
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
    if matches!(hdr_action, HdrAction::ToneMapToSdr | HdrAction::Unsupported) {
        push_unique(&mut blockers, "hdr_tone_mapping_required");
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

    let adaptive_ladder_candidate = if adaptive_transcode_can_be_considered(client, policy) {
        match planned_adaptive_ladder(
            video,
            subtitle,
            video_transcode_subtitle_action(subtitle, client),
            hdr_action,
            preferred_hls_delivery(client, true),
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
        let video_delivery = preferred_hls_delivery(client, true);
        let video_audio_output =
            audio.and_then(|audio| planned_audio_output(audio, client, video_delivery));
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
        return plan;
    }

    if blockers.is_empty() {
        return PlaybackPlan {
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
            compatibility_report: report,
            reasons: vec!["direct_play_all_capabilities_satisfied".to_string()],
            warnings: Vec::new(),
            expected_outputs: direct_file_outputs(),
            playable: true,
        };
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
        && !matches!(hdr_action, HdrAction::ToneMapToSdr | HdrAction::Unsupported);

    if let Some(direct_stream_delivery) = direct_stream_delivery.filter(|_| {
        policy.allow_direct_stream
            && source_within_bitrate_policy
            && source_within_resolution_policy
            && direct_play_video_supported
            && hls_audio_copyable
            && subtitle.is_none()
            && hls_policy_ok
    }) {
        return hls_plan(
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
        return hls_plan(
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
        return hls_plan(
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
    plan
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

fn planned_video_output(
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
    let tone_map =
        matches!(hdr_action, HdrAction::ToneMapToSdr | HdrAction::Unsupported).then(|| {
            VideoToneMapPlan {
                algorithm: "hable".to_string(),
                output_primaries: "bt709".to_string(),
                output_transfer: "bt709".to_string(),
                output_matrix: "bt709".to_string(),
            }
        });
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
    let burn_in = planned_burn_in(subtitle, subtitle_action)?;
    let mut output_reasons = Vec::new();
    for reason in reasons {
        push_unique(&mut output_reasons, reason);
    }
    if matches!(hdr_action, HdrAction::ToneMapToSdr | HdrAction::Unsupported) {
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

    Ok(VideoOutputPlan {
        codec: "h264".to_string(),
        encoder: "libx264".to_string(),
        preset: clean_or_default(&policy.video_encoder_preset, "veryfast"),
        profile: non_empty_string(&policy.video_encoder_profile),
        level: non_empty_string(&policy.video_encoder_level),
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
    video: &VideoStreamCapabilities,
    subtitle: Option<&SubtitleStreamCapabilities>,
    subtitle_action: StreamAction,
    hdr_action: HdrAction,
    delivery: Delivery,
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
    let (bitrate_cap_bps, mut ladder_reasons) =
        adaptive_bitrate_cap_bps(source_bitrate_bps, policy);
    let frame_rate = planned_frame_rate(video);
    let fps_for_gop = frame_rate
        .target_fps
        .as_deref()
        .or(frame_rate.source_fps.as_deref())
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(24.0);
    let segment_seconds = 4;
    let gop_frames = ((fps_for_gop * segment_seconds as f64).round() as i32).clamp(12, 300);
    let tone_map =
        matches!(hdr_action, HdrAction::ToneMapToSdr | HdrAction::Unsupported).then(|| {
            VideoToneMapPlan {
                algorithm: "hable".to_string(),
                output_primaries: "bt709".to_string(),
                output_transfer: "bt709".to_string(),
                output_matrix: "bt709".to_string(),
            }
        });

    let mut rungs = Vec::new();
    for (target_height, default_bitrate_bps) in adaptive_default_ladder() {
        if target_height > source_height {
            continue;
        }
        let target_bitrate_bps = default_bitrate_bps.min(bitrate_cap_bps);
        if target_bitrate_bps < 250_000 {
            continue;
        }
        if target_bitrate_bps >= source_bitrate_bps.saturating_mul(95) / 100 {
            continue;
        }
        if rungs.last().is_some_and(|previous: &AdaptiveRungPlan| {
            rungs_too_close(previous.bandwidth_bps, target_bitrate_bps)
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
        let burn_in = planned_burn_in(subtitle, subtitle_action)?;
        let mut output_reasons = Vec::new();
        for reason in reasons {
            push_unique(&mut output_reasons, reason);
        }
        push_unique(&mut output_reasons, "adaptive_ladder_rung");
        if target_bitrate_bps < default_bitrate_bps {
            push_unique(&mut output_reasons, "adaptive_ladder_bitrate_capped");
        }
        if matches!(hdr_action, HdrAction::ToneMapToSdr | HdrAction::Unsupported) {
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
        let video = VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "libx264".to_string(),
            preset: clean_or_default(&policy.video_encoder_preset, "veryfast"),
            profile: non_empty_string(&policy.video_encoder_profile),
            level: non_empty_string(&policy.video_encoder_level),
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
        rungs.push(AdaptiveRungPlan {
            id: rung_id,
            label,
            bandwidth_bps: target_bitrate_bps,
            width: target_width,
            height: even_dimension(target_height),
            video,
        });
    }

    if rungs.len() < 2 {
        return Err("adaptive_ladder_insufficient_useful_rungs".to_string());
    }

    push_unique(&mut ladder_reasons, "adaptive_ladder_source_aware");
    push_unique(&mut ladder_reasons, "adaptive_audio_strategy_per_rung");
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

    let decoder = if policy.allow_hardware_decode
        && !filter_graph_requires_software
        && capabilities.decode_support(api, source_codec).is_some()
    {
        Some(api.as_str().to_string())
    } else {
        if policy.allow_hardware_decode && filter_graph_requires_software {
            push_unique(&mut warnings, "hardware_decode_disabled_filter_graph");
        }
        None
    };

    let encoder = if policy.allow_hardware_encode {
        capabilities
            .encode_support(api, &output.codec)
            .map(|support| support.ffmpeg_name.clone())
    } else {
        None
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
    };
    Ok((plan, warnings))
}

fn hardware_unavailable_or_software(
    fail_if_unavailable: bool,
    fallback_policy: HardwareFallbackPolicy,
    software: HardwareAccelerationPlan,
    reason: String,
) -> Result<(HardwareAccelerationPlan, Vec<String>), String> {
    if fail_if_unavailable && fallback_policy == HardwareFallbackPolicy::Fail {
        Err(reason)
    } else {
        Ok((software, vec![reason]))
    }
}

fn video_filter_graph_requires_software(output: &VideoOutputPlan) -> bool {
    output.tone_map.is_some()
        || output.scale.is_some()
        || output.burn_in.is_some()
        || output.frame_rate.mode == VideoFrameRateMode::Convert
}

fn planned_burn_in(
    subtitle: Option<&SubtitleStreamCapabilities>,
    subtitle_action: StreamAction,
) -> Result<Option<SubtitleBurnInPlan>, String> {
    if subtitle_action != StreamAction::BurnIn {
        return Ok(None);
    }
    let subtitle = subtitle.ok_or_else(|| "subtitle_burn_in_stream_missing".to_string())?;
    if subtitle.external_path.is_some() {
        return Err("subtitle_burn_in_external_source_unsupported".to_string());
    }
    let stream_index = subtitle
        .index
        .ok_or_else(|| "subtitle_burn_in_stream_index_missing".to_string())?;
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

    Ok(Some(SubtitleBurnInPlan {
        stream_index,
        codec: codec_lower,
        mode,
        reason: "selected_subtitle_requires_video_burn_in".to_string(),
    }))
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
        Delivery::DirectFile => client
            .supported_subtitle_codecs
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(codec)),
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

fn not_playable_plan(
    media_file_id: String,
    reasons: Vec<String>,
    report: CompatibilityReport,
) -> PlaybackPlan {
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
        hdr_action: HdrAction::Unknown,
        hardware_acceleration: HardwareAccelerationPlan::default(),
        audio_output: None,
        video_output: None,
        adaptive_ladder: None,
        video_transcode_reason: None,
        compatibility_report: report,
        reasons,
        warnings: Vec::new(),
        expected_outputs: Vec::new(),
        playable: false,
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
    const ADAPTIVE_TRANSCODE_WEIGHT: u32 = 2;
    policy
        .max_simultaneous_video_transcodes
        .map(|max| {
            policy
                .active_video_transcodes
                .saturating_add(ADAPTIVE_TRANSCODE_WEIGHT)
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
        (SubtitleKind::Image, SubtitleBurnPolicy::Automatic | SubtitleBurnPolicy::ImageOnly) => {
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
            return HdrAction::Direct;
        }
        if video.dolby_vision_has_hdr10_fallback && client.supports_hdr {
            return HdrAction::Direct;
        }
        return if client.supports_hdr || client.supports_dolby_vision {
            HdrAction::Unsupported
        } else {
            HdrAction::ToneMapToSdr
        };
    }
    if unknown_hdr_metadata(video) {
        return HdrAction::Unsupported;
    }
    if video.hdr10_plus {
        return if client.supports_hdr10_plus {
            HdrAction::Direct
        } else {
            HdrAction::ToneMapToSdr
        };
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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::{
        media::ffprobe,
        playback::{
            hardware::{HardwareCapabilities, HardwareCodecSupport},
            plan::StreamAction,
            probe::{SubtitleKind, normalize_ffprobe_metadata},
            profile::{
                ClientPlaybackProfile, NetworkClass, NetworkPlaybackPolicy, ServerPlaybackPolicy,
                derive_effective_playback_policy,
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

    fn videotoolbox_capabilities() -> HardwareCapabilities {
        HardwareCapabilities {
            platform: "macos-x86_64".to_string(),
            ffmpeg_version: Some("ffmpeg fixture".to_string()),
            available_apis: vec!["videotoolbox".to_string()],
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
        let browser = ClientPlaybackProfile::browser_like();
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

        let browser = ClientPlaybackProfile::browser_like();
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
        let browser = ClientPlaybackProfile::browser_like();

        let plan = plan_playback(
            "file-2",
            &media,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
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
            },
            &browser,
            &policy,
        );
        assert_eq!(subtitle_plan.mode, PlaybackMode::SubtitleTranscode);
        assert_eq!(subtitle_plan.video_action, StreamAction::Copy);
        assert_eq!(subtitle_plan.audio_action, StreamAction::Copy);
        assert_eq!(
            subtitle_plan.subtitle_action,
            StreamAction::ConvertTextToWebvtt
        );
        assert!(subtitle_plan.audio_output.is_none());
        assert!(subtitle_plan.video_output.is_none());
        assert!(subtitle_plan.expected_outputs.iter().any(|output| {
            output.name == "sub_0.m3u8" && output.kind == "hls_subtitle_playlist"
        }));

        let burn_in_plan = plan_playback(
            "pgs-file",
            &hevc_hdr_pgs,
            PlaybackSelection {
                audio_stream_index: Some(1),
                subtitle_stream_index: Some(2),
                start_position_seconds: None,
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
        for subtitle_index in [2, 3, 4] {
            let text_subtitle_plan = plan_playback(
                &format!("phase13-text-subtitle-{subtitle_index}"),
                &h264_aac_text_subtitles,
                PlaybackSelection {
                    audio_stream_index: Some(1),
                    subtitle_stream_index: Some(subtitle_index),
                    start_position_seconds: None,
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
