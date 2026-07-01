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
        .register(Box::new(PLAYBACK_HARDWARE_READINESS_STATUS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_HARDWARE_PROBE_DURATION.clone()))
        .ok();
    REGISTRY
        .register(Box::new(PLAYBACK_HARDWARE_PROBE_FAILURES.clone()))
        .ok();
    REGISTRY.register(Box::new(RECONCILE_RUNS.clone())).ok();
    REGISTRY.register(Box::new(RECONCILE_ACTIONS.clone())).ok();
    REGISTRY
        .register(Box::new(OWNER_RELEASE_EVENTS.clone()))
        .ok();
}

pub fn gather() -> Vec<u8> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buf = Vec::new();
    encoder.encode(&metric_families, &mut buf).ok();
    buf
}
