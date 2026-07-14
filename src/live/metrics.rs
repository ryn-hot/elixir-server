//! Privacy-safe Live metrics with bounded label vocabularies.

use std::collections::BTreeMap;

use once_cell::sync::Lazy;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, core::Collector,
};
use serde::Serialize;
use sqlx::Row;

pub static PROVIDER_REQUESTS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("live_provider_requests_total", "Live provider requests"),
        &["operation", "outcome"],
    )
    .expect("Live provider request metric")
});
pub static PROVIDER_REQUEST_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "live_provider_request_duration_seconds",
            "Live provider request duration",
        ),
        &["operation", "outcome"],
    )
    .expect("Live provider duration metric")
});
pub static PROVIDER_CONTRACT_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "live_provider_contract_failures_total",
            "Rejected Live provider contract payloads",
        ),
        &["code"],
    )
    .expect("Live provider contract metric")
});
pub static CATALOG_CACHE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("live_catalog_cache_total", "Live catalog cache outcomes"),
        &["state"],
    )
    .expect("Live catalog cache metric")
});
pub static SESSIONS_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("live_sessions_active", "Active standalone Live sessions"),
        &["delivery_mode", "protocol", "egress_mode"],
    )
    .expect("Live active session metric")
});
pub static SESSIONS_STARTED: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("live_sessions_started_total", "Live session starts"),
        &["delivery_mode", "protocol", "outcome"],
    )
    .expect("Live session start metric")
});
pub static SESSION_START_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "live_session_start_duration_seconds",
            "Live session admission duration",
        ),
        &["delivery_mode"],
    )
    .expect("Live session duration metric")
});
pub static RELAY_REQUESTS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("live_relay_requests_total", "Live relay requests"),
        &["kind", "outcome"],
    )
    .expect("Live relay request metric")
});
pub static RELAY_UPSTREAM_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "live_relay_upstream_bytes_total",
            "Bytes received by the Live relay",
        ),
        &["kind"],
    )
    .expect("Live relay upstream byte metric")
});
pub static RELAY_CLIENT_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "live_relay_client_bytes_total",
            "Bytes emitted by the Live relay",
        ),
        &["kind"],
    )
    .expect("Live relay client byte metric")
});
pub static REMUX_JOBS_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("live_remux_jobs_active", "Active copy-only Live remux jobs"),
        &["profile"],
    )
    .expect("Live remux active metric")
});
pub static REMUX_JOBS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("live_remux_jobs_total", "Live copy-remux job outcomes"),
        &["profile", "outcome"],
    )
    .expect("Live remux total metric")
});
pub static RECONNECTS: Lazy<IntCounterVec> =
    Lazy::new(|| recovery_counter("live_reconnects_total", "Live reconnect outcomes"));
pub static REFRESHES: Lazy<IntCounterVec> =
    Lazy::new(|| recovery_counter("live_refreshes_total", "Live descriptor refresh outcomes"));
pub static FAILOVERS: Lazy<IntCounterVec> =
    Lazy::new(|| recovery_counter("live_failovers_total", "Live source failover outcomes"));
pub static EGRESS_BINDINGS_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "live_egress_bindings_active",
            "Ready session-scoped Live protected-egress bindings",
        ),
        &["mode"],
    )
    .expect("Live egress active metric")
});
pub static CLEANUP: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("live_cleanup_total", "Live resource cleanup outcomes"),
        &["resource", "outcome"],
    )
    .expect("Live cleanup metric")
});
pub static ADMISSION_REJECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "live_admission_rejections_total",
            "Bounded Live admission rejections",
        ),
        &["resource", "reason"],
    )
    .expect("Live admission rejection metric")
});
pub static DIAGNOSTIC_BUNDLES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "live_diagnostic_bundles_total",
            "Live support-bundle generation outcomes",
        ),
        &["outcome"],
    )
    .expect("Live diagnostic bundle metric")
});

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveSupportMetricSample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

fn recovery_counter(name: &str, help: &str) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), &["reason", "outcome"]).expect("Live recovery metric")
}

pub fn register(registry: &Registry) {
    for collector in [
        Box::new(PROVIDER_REQUESTS.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(PROVIDER_REQUEST_DURATION.clone()),
        Box::new(PROVIDER_CONTRACT_FAILURES.clone()),
        Box::new(CATALOG_CACHE.clone()),
        Box::new(SESSIONS_ACTIVE.clone()),
        Box::new(SESSIONS_STARTED.clone()),
        Box::new(SESSION_START_DURATION.clone()),
        Box::new(RELAY_REQUESTS.clone()),
        Box::new(RELAY_UPSTREAM_BYTES.clone()),
        Box::new(RELAY_CLIENT_BYTES.clone()),
        Box::new(REMUX_JOBS_ACTIVE.clone()),
        Box::new(REMUX_JOBS.clone()),
        Box::new(RECONNECTS.clone()),
        Box::new(REFRESHES.clone()),
        Box::new(FAILOVERS.clone()),
        Box::new(EGRESS_BINDINGS_ACTIVE.clone()),
        Box::new(CLEANUP.clone()),
        Box::new(ADMISSION_REJECTIONS.clone()),
        Box::new(DIAGNOSTIC_BUNDLES.clone()),
    ] {
        registry.register(collector).ok();
    }
}

pub fn support_snapshot() -> Vec<LiveSupportMetricSample> {
    let collectors: [&dyn Collector; 8] = [
        &*PROVIDER_REQUESTS,
        &*RELAY_REQUESTS,
        &*RELAY_UPSTREAM_BYTES,
        &*RELAY_CLIENT_BYTES,
        &*REMUX_JOBS,
        &*REFRESHES,
        &*FAILOVERS,
        &*CLEANUP,
    ];
    let mut samples = Vec::new();
    for collector in collectors {
        for family in collector.collect() {
            let name = family.get_name().to_string();
            for metric in family.get_metric() {
                let labels = metric
                    .get_label()
                    .iter()
                    .map(|label| (label.get_name().to_string(), label.get_value().to_string()))
                    .collect();
                samples.push(LiveSupportMetricSample {
                    name: name.clone(),
                    labels,
                    value: metric.get_counter().get_value(),
                });
            }
        }
    }
    samples.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.labels.cmp(&right.labels))
    });
    samples
}

pub async fn refresh_database_gauges(pool: &sqlx::AnyPool) -> Result<(), sqlx::Error> {
    let sessions = sqlx::query(
        "SELECT s.delivery_mode, s.protocol,
                CASE
                    WHEN b.state = 'ready' THEN 'protected'
                    WHEN b.state = 'failed'
                         AND b.failure_reason_redacted = 'runtime_readiness_failed'
                        THEN 'direct_fallback'
                    ELSE 'server_default'
                END AS egress_mode,
                COUNT(*) AS total
         FROM live_playback_sessions s
         LEFT JOIN live_egress_bindings b ON b.session_id = s.id
         WHERE s.state NOT IN ('ended', 'expired', 'failed')
         GROUP BY s.delivery_mode, s.protocol, egress_mode",
    )
    .fetch_all(pool)
    .await?;
    let bindings = sqlx::query(
        "SELECT mode, COUNT(*) AS total
         FROM live_egress_bindings WHERE state = 'ready' GROUP BY mode",
    )
    .fetch_all(pool)
    .await?;

    SESSIONS_ACTIVE.reset();
    for row in sessions {
        let delivery_mode: String = row.try_get("delivery_mode")?;
        let protocol: String = row.try_get("protocol")?;
        let egress_mode: String = row.try_get("egress_mode")?;
        let total: i64 = row.try_get("total")?;
        SESSIONS_ACTIVE
            .with_label_values(&[&delivery_mode, &protocol, &egress_mode])
            .set(total);
    }
    EGRESS_BINDINGS_ACTIVE.reset();
    for row in bindings {
        let mode: String = row.try_get("mode")?;
        let total: i64 = row.try_get("total")?;
        EGRESS_BINDINGS_ACTIVE
            .with_label_values(&[&mode])
            .set(total);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn o10_support_snapshot_exposes_only_frozen_bounded_metric_dimensions() {
        RELAY_REQUESTS
            .with_label_values(&["segment", "success"])
            .inc();
        RELAY_UPSTREAM_BYTES
            .with_label_values(&["segment"])
            .inc_by(1_024);
        let samples = support_snapshot();
        assert!(samples.iter().any(|sample| {
            sample.name == "live_relay_requests_total"
                && sample.labels.get("kind").map(String::as_str) == Some("segment")
                && sample.labels.get("outcome").map(String::as_str) == Some("success")
        }));
        let names = BTreeSet::from([
            "live_provider_requests_total",
            "live_relay_requests_total",
            "live_relay_upstream_bytes_total",
            "live_relay_client_bytes_total",
            "live_remux_jobs_total",
            "live_refreshes_total",
            "live_failovers_total",
            "live_cleanup_total",
        ]);
        let label_names = BTreeSet::from([
            "operation",
            "outcome",
            "kind",
            "profile",
            "reason",
            "resource",
        ]);
        for sample in samples {
            assert!(names.contains(sample.name.as_str()));
            assert!(sample.value.is_finite() && sample.value >= 0.0);
            assert!(sample.labels.len() <= 2);
            assert!(
                sample
                    .labels
                    .keys()
                    .all(|label| label_names.contains(label.as_str()))
            );
        }
    }
}
