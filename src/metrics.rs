use once_cell::sync::Lazy;
use prometheus::{Encoder, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder};

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
    IntCounterVec::new(opts, &["result", "container", "video_codec"]).expect("counter vec created")
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
    REGISTRY.register(Box::new(PLAY_ERRORS.clone())).ok();
    REGISTRY.register(Box::new(PLAY_LATENCY.clone())).ok();
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
