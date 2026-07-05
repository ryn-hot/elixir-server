use once_cell::sync::Lazy;
use prometheus::{Encoder, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static PLAY_DECISIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_play_decisions_total",
        "Count of play decisions by mode and network",
    );
    IntCounterVec::new(opts, &["mode", "network", "container", "video_codec"])
        .expect("counter vec created")
});

pub static TRANSCODE_STARTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_transcode_starts_total",
        "Count of transcode start attempts",
    );
    IntCounterVec::new(opts, &["result", "container", "video_codec", "hardware"])
        .expect("counter vec created")
});

pub static REGISTRY_ACTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new("elixir_registry_actions_total", "Count of registry actions");
    IntCounterVec::new(opts, &["action", "result"]).expect("counter vec created")
});

pub static WAN_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new("elixir_wan_events_total", "Count of WAN setup events");
    IntCounterVec::new(opts, &["step", "result"]).expect("counter vec created")
});

pub static TRANSCODE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_transcode_duration_seconds",
        "Duration of transcode sessions",
    );
    HistogramVec::new(opts, &["result"]).expect("histogram vec created")
});

pub static TRANSCODE_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new("elixir_transcode_errors_total", "Count of transcode errors");
    IntCounterVec::new(opts, &["reason"]).expect("counter vec created")
});

pub static SEGMENT_SERVED: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_segments_served_total",
        "Count of HLS segments served",
    );
    IntCounterVec::new(opts, &["result"]).expect("counter vec created")
});

pub static DIRECT_STREAM_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_direct_stream_bytes_total",
        "Bytes read for direct file playback responses",
    );
    IntCounterVec::new(opts, &["session", "user", "media_file", "delivery"])
        .expect("counter vec created")
});

pub static DIRECT_STREAM_RANGE_REQUESTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_direct_stream_range_requests_total",
        "Direct file playback responses by byte range status",
    );
    IntCounterVec::new(opts, &["status", "delivery", "method"]).expect("counter vec created")
});

pub static PLAY_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new("elixir_play_errors_total", "Count of play errors");
    IntCounterVec::new(opts, &["reason"]).expect("counter vec created")
});

pub static PLAY_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_play_decision_latency_seconds",
        "Latency of play decision handler",
    );
    HistogramVec::new(opts, &["mode"]).expect("histogram vec created")
});

pub static PLAYBACK_DECISIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_decisions_total",
        "Playback decisions by mode, delivery, client, network, reason, and hardware path",
    );
    IntCounterVec::new(
        opts,
        &[
            "mode",
            "delivery",
            "client_kind",
            "network_kind",
            "decision_reason",
            "hardware",
        ],
    )
    .expect("counter vec created")
});

pub static PLAYBACK_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_errors_total",
        "Playback errors by mode, delivery, client, network, error class, and hardware path",
    );
    IntCounterVec::new(
        opts,
        &[
            "mode",
            "delivery",
            "client_kind",
            "network_kind",
            "error_class",
            "hardware",
        ],
    )
    .expect("counter vec created")
});

pub static PLAYBACK_CAPACITY_REJECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_capacity_rejections_total",
        "Playback starts rejected by capacity resource and error class",
    );
    IntCounterVec::new(
        opts,
        &[
            "resource",
            "mode",
            "delivery",
            "client_kind",
            "network_kind",
            "error_class",
        ],
    )
    .expect("counter vec created")
});

pub static PLAYBACK_CAPACITY_LEVELS: Lazy<IntGaugeVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_capacity_level",
        "Current playback capacity usage by resource, mode, and hardware path",
    );
    IntGaugeVec::new(opts, &["resource", "mode", "hardware"]).expect("gauge vec created")
});

pub static PLAYBACK_HLS_PLAYLIST_STARTUP_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_playback_hls_playlist_startup_latency_seconds",
        "Latency from HLS job start request to playlist response",
    );
    HistogramVec::new(opts, &["mode", "delivery", "hardware"]).expect("histogram vec created")
});

pub static PLAYBACK_SEGMENT_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_playback_segment_latency_seconds",
        "Latency of HLS artifact reads",
    );
    HistogramVec::new(opts, &["result", "artifact_kind"]).expect("histogram vec created")
});

pub static PLAYBACK_MISSING_SEGMENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_missing_segments_total",
        "Count of requested HLS artifacts that were missing or unregistered",
    );
    IntCounterVec::new(opts, &["artifact_kind"]).expect("counter vec created")
});

pub static PLAYBACK_ADAPTIVE_RENDITION_SWITCHES: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_adaptive_rendition_switches_total",
        "Count of observed adaptive rendition switches",
    );
    IntCounterVec::new(opts, &["direction", "reason"]).expect("counter vec created")
});

pub static PLAYBACK_SESSION_EXPIRATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_session_expirations_total",
        "Count of playback sessions expired by cleanup",
    );
    IntCounterVec::new(opts, &["reason"]).expect("counter vec created")
});

pub static PLAYBACK_CLEANUP_ACTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_cleanup_actions_total",
        "Playback cleanup actions by target, reason, and result",
    );
    IntCounterVec::new(opts, &["target", "reason", "result"]).expect("counter vec created")
});

pub static PLAYBACK_PROCESS_EXITS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_process_exits_total",
        "Playback FFmpeg process cleanup exits by reason, method, and result",
    );
    IntCounterVec::new(opts, &["reason", "method", "result"]).expect("counter vec created")
});

pub static PLAYBACK_HARDWARE_READINESS_STATUS: Lazy<IntGaugeVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_hardware_readiness_status",
        "Current playback hardware readiness status by API, OS family, and GPU vendor",
    );
    IntGaugeVec::new(opts, &["api", "status", "os_family", "gpu_vendor"])
        .expect("gauge vec created")
});

pub static PLAYBACK_HARDWARE_PROBE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_playback_hardware_probe_duration_seconds",
        "Duration of playback hardware capability probes",
    );
    HistogramVec::new(opts, &["api", "operation", "status"]).expect("histogram vec created")
});

pub static PLAYBACK_HARDWARE_PROBE_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_hardware_probe_failures_total",
        "Count of playback hardware capability probe failures",
    );
    IntCounterVec::new(opts, &["api", "operation", "reason"]).expect("counter vec created")
});

pub static PLAYBACK_FEASIBILITY_DECISIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_feasibility_decision_total",
        "Playback feasibility admission decisions by decision, reason, mode, and client",
    );
    IntCounterVec::new(opts, &["decision", "reason", "mode", "client"])
        .expect("counter vec created")
});

pub static PLAYBACK_PERFORMANCE_ENVELOPE_STATUS: Lazy<IntGaugeVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_performance_envelope_status",
        "Loaded playback performance envelope status by workload class, hardware API, support/performance status, and confidence",
    );
    IntGaugeVec::new(opts, &["class", "api", "status", "confidence"]).expect("gauge vec created")
});

pub static PLAYBACK_TRANSCODE_REALTIME_FACTOR: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_playback_transcode_realtime_factor",
        "Observed or certified transcode realtime factor for admitted playback workloads",
    );
    HistogramVec::new(opts, &["class", "api", "pipeline"]).expect("histogram vec created")
});

pub static PLAYBACK_TRANSCODE_REJECTED: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_transcode_rejected_total",
        "Playback transcode requests rejected by feasibility admission",
    );
    IntCounterVec::new(opts, &["reason", "class", "client"]).expect("counter vec created")
});

pub static PLAYBACK_TRANSCODE_DOWNGRADED: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_transcode_downgraded_total",
        "Playback transcode requests downgraded by feasibility admission",
    );
    IntCounterVec::new(opts, &["reason", "class", "client"]).expect("counter vec created")
});

pub static PLAYBACK_PERFORMANCE_PROBE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_playback_performance_probe_duration_seconds",
        "Duration of playback performance envelope probes",
    );
    HistogramVec::new(opts, &["class", "api", "status"]).expect("histogram vec created")
});

pub static RECONCILE_RUNS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_reconcile_runs_total",
        "Count of reconcile runs by result",
    );
    IntCounterVec::new(opts, &["result"]).expect("counter vec created")
});

pub static RECONCILE_ACTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_reconcile_actions_total",
        "Count of reconcile actions by type and result",
    );
    IntCounterVec::new(opts, &["action", "result"]).expect("counter vec created")
});

pub static OWNER_RELEASE_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_owner_release_events_total",
        "Count of media owner-release lifecycle events",
    );
    IntCounterVec::new(opts, &["action", "owner_type", "status"]).expect("counter vec created")
});

pub static MEDIA_SEGMENT_JOBS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_media_segment_jobs_total",
        "Count of media segment job lifecycle transitions",
    );
    IntCounterVec::new(opts, &["provider_kind", "job_type", "status"]).expect("counter vec created")
});

pub static MEDIA_SEGMENT_JOB_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_media_segment_job_duration_seconds",
        "Duration of completed media segment jobs",
    );
    HistogramVec::new(opts, &["provider_kind", "job_type", "status"])
        .expect("histogram vec created")
});

pub static MEDIA_SEGMENT_JOB_BACKLOG: Lazy<IntGaugeVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_media_segment_job_backlog",
        "Current media segment jobs by backlog status",
    );
    IntGaugeVec::new(opts, &["status"]).expect("gauge vec created")
});

pub static PLAYBACK_PROGRESS_UPDATES: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_playback_progress_updates_total",
        "Count of media interaction progress updates",
    );
    IntCounterVec::new(opts, &["event_type", "result"]).expect("counter vec created")
});

pub static USER_MEDIA_STATE_TRANSITIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_user_media_state_transitions_total",
        "Count of user media state transitions",
    );
    IntCounterVec::new(opts, &["item_type", "transition", "source"]).expect("counter vec created")
});

pub static MEDIA_SEGMENTS_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_media_segments_active_total",
        "Current active media segments by type and source",
    );
    IntGaugeVec::new(opts, &["segment_type", "source_label"]).expect("gauge vec created")
});

pub static MEDIA_SEGMENT_CANDIDATES: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_media_segment_candidates_total",
        "Count of media segment candidate validation outcomes",
    );
    IntCounterVec::new(opts, &["provider_kind", "validation_state"]).expect("counter vec created")
});

pub static SEGMENT_SKIP_ACTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_segment_skip_actions_total",
        "Count of segment skip actions reported by clients",
    );
    IntCounterVec::new(opts, &["segment_type", "behavior", "result"]).expect("counter vec created")
});

pub static AUTOPLAY_TRANSITIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_autoplay_transitions_total",
        "Count of Up Next autoplay state transitions",
    );
    IntCounterVec::new(opts, &["result", "reason"]).expect("counter vec created")
});

pub fn init_metrics() {
    REGISTRY.register(Box::new(PLAY_DECISIONS.clone())).ok();
    REGISTRY.register(Box::new(TRANSCODE_STARTS.clone())).ok();
    REGISTRY.register(Box::new(REGISTRY_ACTIONS.clone())).ok();
    REGISTRY.register(Box::new(WAN_EVENTS.clone())).ok();
    REGISTRY.register(Box::new(TRANSCODE_DURATION.clone())).ok();
    REGISTRY.register(Box::new(TRANSCODE_ERRORS.clone())).ok();
    REGISTRY.register(Box::new(SEGMENT_SERVED.clone())).ok();
    REGISTRY
        .register(Box::new(DIRECT_STREAM_BYTES.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DIRECT_STREAM_RANGE_REQUESTS.clone()))
        .ok();
    REGISTRY.register(Box::new(PLAY_ERRORS.clone())).ok();
    REGISTRY.register(Box::new(PLAY_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(PLAYBACK_DECISIONS.clone())).ok();
    REGISTRY.register(Box::new(PLAYBACK_ERRORS.clone())).ok();
    REGISTRY
        .register(Box::new(PLAYBACK_CAPACITY_REJECTIONS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_CAPACITY_LEVELS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_HLS_PLAYLIST_STARTUP_LATENCY.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_SEGMENT_LATENCY.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_MISSING_SEGMENTS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_ADAPTIVE_RENDITION_SWITCHES.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_SESSION_EXPIRATIONS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_CLEANUP_ACTIONS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_PROCESS_EXITS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_HARDWARE_READINESS_STATUS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_HARDWARE_PROBE_DURATION.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_HARDWARE_PROBE_FAILURES.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_FEASIBILITY_DECISIONS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_PERFORMANCE_ENVELOPE_STATUS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_TRANSCODE_REALTIME_FACTOR.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_TRANSCODE_REJECTED.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_TRANSCODE_DOWNGRADED.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_PERFORMANCE_PROBE_DURATION.clone()))
        .ok();
    REGISTRY.register(Box::new(RECONCILE_RUNS.clone())).ok();
    REGISTRY.register(Box::new(RECONCILE_ACTIONS.clone())).ok();
    REGISTRY
        .register(Box::new(OWNER_RELEASE_EVENTS.clone()))
        .ok();
    REGISTRY.register(Box::new(MEDIA_SEGMENT_JOBS.clone())).ok();
    REGISTRY
        .register(Box::new(MEDIA_SEGMENT_JOB_DURATION.clone()))
        .ok();
    REGISTRY
        .register(Box::new(MEDIA_SEGMENT_JOB_BACKLOG.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_PROGRESS_UPDATES.clone()))
        .ok();
    REGISTRY
        .register(Box::new(USER_MEDIA_STATE_TRANSITIONS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(MEDIA_SEGMENTS_ACTIVE.clone()))
        .ok();
    REGISTRY
        .register(Box::new(MEDIA_SEGMENT_CANDIDATES.clone()))
        .ok();
    REGISTRY
        .register(Box::new(SEGMENT_SKIP_ACTIONS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(AUTOPLAY_TRANSITIONS.clone()))
        .ok();
}

pub fn gather() -> Vec<u8> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buf = Vec::new();
    encoder.encode(&metric_families, &mut buf).ok();
    buf
}
