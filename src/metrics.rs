use once_cell::sync::Lazy;
use prometheus::{Encoder, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static PLAY_DECISIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_play_decisions_total",
        "Count of play decisions by mode and network",
    );
    IntCounterVec::new(opts, &["mode", "network"]).expect("counter vec created")
});

pub static TRANSCODE_STARTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "elixir_transcode_starts_total",
        "Count of transcode start attempts",
    );
    IntCounterVec::new(opts, &["result"]).expect("counter vec created")
});

pub static REGISTRY_ACTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new("elixir_registry_actions_total", "Count of registry actions");
    IntCounterVec::new(opts, &["action", "result"]).expect("counter vec created")
});

pub static PLAY_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "elixir_play_decision_latency_seconds",
        "Latency of play decision handler",
    );
    HistogramVec::new(opts, &["mode"]).expect("histogram vec created")
});

pub fn init_metrics() {
    REGISTRY.register(Box::new(PLAY_DECISIONS.clone())).ok();
    REGISTRY.register(Box::new(TRANSCODE_STARTS.clone())).ok();
    REGISTRY.register(Box::new(REGISTRY_ACTIONS.clone())).ok();
    REGISTRY.register(Box::new(PLAY_LATENCY.clone())).ok();
}

pub fn gather() -> Vec<u8> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buf = Vec::new();
    encoder.encode(&metric_families, &mut buf).ok();
    buf
}
