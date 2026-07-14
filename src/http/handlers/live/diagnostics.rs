//! Redacted administrative support bundles for standalone Live sessions.

use axum::{
    Json,
    extract::{Path, RawQuery, State},
    http::{HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    extensions::{manifest::LIVE_CATALOG_PROVIDER_CONTRACT_VERSION, store::ExtensionStore},
    live::{
        metrics::{self, LiveSupportMetricSample},
        session::{
            DeliveryMode, SessionRecord, SessionRepositoryError, SessionState,
            StoredSessionDescriptor,
        },
    },
    state::AppState,
};

use super::{
    admin::LiveAdminPrincipal,
    catalog::{LiveHttpRejection, admit, error_response, reject_query, request_id},
};

const SUPPORT_BUNDLE_SCHEMA_VERSION: u32 = 1;
const MAX_TIMELINE_EVENTS: usize = 64;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveSupportBundle {
    schema_version: u32,
    partial: bool,
    session: SessionDiagnostics,
    provider: ProviderDiagnostics,
    planner: PlannerDiagnostics,
    feature_flags: Vec<FeatureFlagDiagnostics>,
    timeline: Vec<TimelineDiagnostics>,
    upstream: UpstreamDiagnostics,
    egress: EgressDiagnostics,
    remux: RemuxDiagnostics,
    cleanup: CleanupDiagnostics,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionDiagnostics {
    session_id: Uuid,
    profile_id: Uuid,
    provider_id: Uuid,
    delivery_mode: &'static str,
    protocol: &'static str,
    state: &'static str,
    revision: i64,
    source_index: i32,
    failover_count: i32,
    refresh_count: i32,
    created_at: String,
    last_heartbeat_at: String,
    expires_at: String,
    hard_expires_at: String,
    ended_at: Option<String>,
    error_code: Option<String>,
    error_detail: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderDiagnostics {
    provider_id: Uuid,
    health: &'static str,
    last_healthcheck_at: Option<String>,
    contract_version: u32,
    runtime_available: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlannerDiagnostics {
    descriptor_status: &'static str,
    decision_reason: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FeatureFlagDiagnostics {
    name: &'static str,
    configured_enabled: bool,
    effective_enabled: bool,
    dependency_ready: bool,
    disabled_reason: Option<&'static str>,
    certification_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimelineDiagnostics {
    at: String,
    revision: i64,
    state: String,
    reason: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UpstreamDiagnostics {
    scope: &'static str,
    samples: Vec<LiveSupportMetricSample>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EgressDiagnostics {
    requested_mode: Option<String>,
    policy_source: Option<String>,
    policy_revision: Option<i64>,
    fallback_allowed: Option<bool>,
    binding: Option<EgressBindingDiagnostics>,
    runtime: Option<EgressRuntimeDiagnostics>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EgressBindingDiagnostics {
    mode: String,
    state: String,
    policy_revision: i64,
    readiness_proven: bool,
    ready_at: Option<String>,
    last_health_at: Option<String>,
    released_at: Option<String>,
    failure_category: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EgressRuntimeDiagnostics {
    enabled: bool,
    ready: bool,
    active_bindings: usize,
    available_capacity: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemuxDiagnostics {
    applicable: bool,
    profile: Option<&'static str>,
    state: &'static str,
    exit_category: Option<&'static str>,
    stderr_tail: Option<String>,
    runtime: Option<RemuxRuntimeDiagnostics>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemuxRuntimeDiagnostics {
    active_jobs: usize,
    available_capacity: usize,
    jobs_started: u64,
    jobs_completed: u64,
    jobs_failed: u64,
    jobs_cancelled: u64,
    temp_bytes: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CleanupDiagnostics {
    terminal: bool,
    idempotency_replays_remaining: i64,
    egress_released: bool,
    remux_stopped: bool,
    delivery_resources_released: bool,
    complete: bool,
}

#[derive(Serialize)]
struct SupportEnvelope {
    data: Value,
    meta: SupportMeta,
    errors: Vec<Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportMeta {
    request_id: String,
    generated_at: String,
    cache_state: &'static str,
    partial: bool,
}

pub async fn session_bundle(
    State(state): State<AppState>,
    LiveAdminPrincipal(principal): LiveAdminPrincipal,
    RawQuery(raw_query): RawQuery,
    Path(session_id): Path<String>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let session_id = Uuid::parse_str(&session_id).map_err(|_| invalid_request())?;
        let repository = state
            .live
            .session_repository()
            .ok_or_else(service_unavailable)?;
        let session = repository
            .get_for_home(principal.home_id, session_id)
            .await
            .map_err(map_repository_error)?
            .ok_or_else(session_not_found)?;

        let (descriptor_status, descriptor) = load_descriptor(&repository, &session).await;
        let partial = !session.state.is_terminal() && descriptor.is_none();
        let provider = load_provider(&state, &session).await?;
        let feature_flags = state
            .live
            .snapshot()
            .await
            .features
            .into_iter()
            .map(|feature| FeatureFlagDiagnostics {
                name: feature.flag,
                configured_enabled: feature.raw_enabled,
                effective_enabled: feature.effective_enabled,
                dependency_ready: feature.dependency_ready,
                disabled_reason: feature.disabled_reason,
                certification_id: feature.certification_id,
            })
            .collect();
        let binding = load_egress_binding(&state, session.id).await?;
        let idempotency_replays_remaining = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM live_session_idempotency WHERE session_id = $1",
        )
        .bind(session.id.to_string())
        .fetch_one(&state.db_pool)
        .await
        .map_err(|_| service_unavailable())?;
        let egress_released = binding
            .as_ref()
            .is_none_or(|binding| matches!(binding.state.as_str(), "released" | "failed"));
        let remux_job = state
            .live
            .remux_service()
            .and_then(|service| service.diagnostics_for(&session));
        let remux_stopped = remux_job.is_none();
        let terminal = session.state.is_terminal();
        let delivery_resources_released = terminal && egress_released && remux_stopped;
        let bundle = LiveSupportBundle {
            schema_version: SUPPORT_BUNDLE_SCHEMA_VERSION,
            partial,
            session: session_diagnostics(&session),
            provider,
            planner: PlannerDiagnostics {
                descriptor_status,
                decision_reason: descriptor
                    .as_ref()
                    .map(|descriptor| descriptor.decision_reason.clone()),
            },
            feature_flags,
            timeline: timeline(&session, descriptor.as_ref()),
            upstream: UpstreamDiagnostics {
                scope: "server_aggregate",
                samples: metrics::support_snapshot(),
            },
            egress: egress_diagnostics(&state, descriptor.as_ref(), binding),
            remux: remux_diagnostics(&state, &session, remux_job).await,
            cleanup: CleanupDiagnostics {
                terminal,
                idempotency_replays_remaining,
                egress_released,
                remux_stopped,
                delivery_resources_released,
                complete: terminal
                    && idempotency_replays_remaining == 0
                    && delivery_resources_released,
            },
        };
        let value = serde_json::to_value(bundle).map_err(|_| service_unavailable())?;
        let redactor = state.live.redactor();
        let value = redactor.redact_json(&value);
        let scan = redactor.scan_json(&value);
        if !scan.is_clean() {
            let categories = scan
                .categories()
                .map(|category| category.as_str())
                .collect::<Vec<_>>();
            tracing::error!(?categories, "Live support bundle failed redaction scan");
            metrics::DIAGNOSTIC_BUNDLES
                .with_label_values(&["redaction_rejected"])
                .inc();
            return Err(service_unavailable());
        }
        metrics::DIAGNOSTIC_BUNDLES
            .with_label_values(&["completed"])
            .inc();
        Ok(support_response(value, partial, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

async fn load_descriptor(
    repository: &crate::live::session::LiveSessionRepository,
    session: &SessionRecord,
) -> (&'static str, Option<StoredSessionDescriptor>) {
    if session.state.is_terminal() {
        return ("terminal_tombstone", None);
    }
    match repository.decrypt_secrets(session.owner, session.id).await {
        Ok(material) => match serde_json::from_slice(material.descriptor.expose_secret()) {
            Ok(descriptor) => ("available", Some(descriptor)),
            Err(_) => ("invalid", None),
        },
        Err(_) => ("unavailable", None),
    }
}

async fn load_provider(
    state: &AppState,
    session: &SessionRecord,
) -> Result<ProviderDiagnostics, LiveHttpRejection> {
    let provider = ExtensionStore::new(&state.db_pool)
        .get_provider(session.owner.provider_id)
        .await
        .map_err(|_| service_unavailable())?
        .ok_or_else(service_unavailable)?;
    let runtime_available = match state.live.provider_client() {
        Some(client) => client
            .directory()
            .get(session.owner.provider_id)
            .await
            .is_ok(),
        None => false,
    };
    Ok(ProviderDiagnostics {
        provider_id: session.owner.provider_id,
        health: provider.health_state.as_str(),
        last_healthcheck_at: provider.last_healthcheck_at.map(timestamp),
        contract_version: LIVE_CATALOG_PROVIDER_CONTRACT_VERSION,
        runtime_available,
    })
}

async fn load_egress_binding(
    state: &AppState,
    session_id: Uuid,
) -> Result<Option<EgressBindingDiagnostics>, LiveHttpRejection> {
    let row = sqlx::query(
        "SELECT mode, state, policy_revision, failure_reason_redacted,
                CASE WHEN readiness_json IS NULL THEN 0 ELSE 1 END AS readiness_proven,
                CAST(ready_at AS TEXT) AS ready_at,
                CAST(last_health_at AS TEXT) AS last_health_at,
                CAST(released_at AS TEXT) AS released_at
         FROM live_egress_bindings WHERE session_id = $1 LIMIT 1",
    )
    .bind(session_id.to_string())
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|_| service_unavailable())?;
    row.map(|row| {
        Ok(EgressBindingDiagnostics {
            mode: row.try_get("mode").map_err(|_| service_unavailable())?,
            state: row.try_get("state").map_err(|_| service_unavailable())?,
            policy_revision: row
                .try_get("policy_revision")
                .map_err(|_| service_unavailable())?,
            readiness_proven: row
                .try_get::<i64, _>("readiness_proven")
                .map_err(|_| service_unavailable())?
                != 0,
            ready_at: row.try_get("ready_at").map_err(|_| service_unavailable())?,
            last_health_at: row
                .try_get("last_health_at")
                .map_err(|_| service_unavailable())?,
            released_at: row
                .try_get("released_at")
                .map_err(|_| service_unavailable())?,
            failure_category: row
                .try_get("failure_reason_redacted")
                .map_err(|_| service_unavailable())?,
        })
    })
    .transpose()
}

fn session_diagnostics(session: &SessionRecord) -> SessionDiagnostics {
    SessionDiagnostics {
        session_id: session.id,
        profile_id: session.owner.profile_id,
        provider_id: session.owner.provider_id,
        delivery_mode: session.delivery_mode.as_str(),
        protocol: session.protocol.as_str(),
        state: session.state.as_str(),
        revision: session.revision,
        source_index: session.source_index,
        failover_count: session.failover_count,
        refresh_count: session.refresh_count,
        created_at: timestamp(session.created_at),
        last_heartbeat_at: timestamp(session.last_heartbeat_at),
        expires_at: timestamp(session.expires_at),
        hard_expires_at: timestamp(session.hard_expires_at),
        ended_at: session.ended_at.map(timestamp),
        error_code: session.error_code.clone(),
        error_detail: session.error_detail_redacted.clone(),
    }
}

fn timeline(
    session: &SessionRecord,
    descriptor: Option<&StoredSessionDescriptor>,
) -> Vec<TimelineDiagnostics> {
    let mut events = vec![TimelineDiagnostics {
        at: timestamp(session.created_at),
        revision: 1,
        state: "resolving".to_string(),
        reason: Some("session_created".to_string()),
    }];
    if let Some(descriptor) = descriptor {
        let start = descriptor
            .recovery
            .events
            .len()
            .saturating_sub(MAX_TIMELINE_EVENTS.saturating_sub(2));
        events.extend(descriptor.recovery.events[start..].iter().map(|event| {
            TimelineDiagnostics {
                at: timestamp(event.at),
                revision: event.revision,
                state: event.action.state().to_string(),
                reason: Some(format!(
                    "{}:{}",
                    event.reason.as_str(),
                    event.outcome.as_str()
                )),
            }
        }));
    }
    events.push(TimelineDiagnostics {
        at: timestamp(session.ended_at.unwrap_or(session.last_heartbeat_at)),
        revision: session.revision,
        state: session.state.as_str().to_string(),
        reason: session.state.is_terminal().then(|| {
            session
                .error_code
                .clone()
                .unwrap_or_else(|| "session_ended".to_string())
        }),
    });
    events.sort_by(|left, right| {
        left.at
            .cmp(&right.at)
            .then_with(|| left.revision.cmp(&right.revision))
    });
    events.truncate(MAX_TIMELINE_EVENTS);
    events
}

fn egress_diagnostics(
    state: &AppState,
    descriptor: Option<&StoredSessionDescriptor>,
    binding: Option<EgressBindingDiagnostics>,
) -> EgressDiagnostics {
    let runtime = state.live.egress_service().map(|service| {
        let status = service.status();
        EgressRuntimeDiagnostics {
            enabled: status.enabled,
            ready: status.ready,
            active_bindings: status.active_bindings,
            available_capacity: status.available_capacity,
        }
    });
    EgressDiagnostics {
        requested_mode: descriptor.map(|descriptor| descriptor.egress.mode.clone()),
        policy_source: descriptor.map(|descriptor| descriptor.egress.source.clone()),
        policy_revision: descriptor.map(|descriptor| descriptor.egress.revision),
        fallback_allowed: descriptor.map(|descriptor| descriptor.egress.allow_fallback),
        binding,
        runtime,
    }
}

async fn remux_diagnostics(
    state: &AppState,
    session: &SessionRecord,
    job: Option<crate::live::remux::LiveRemuxJobDiagnostics>,
) -> RemuxDiagnostics {
    let applicable = session.delivery_mode == DeliveryMode::ServerRemux;
    let profile = applicable.then(|| remux_profile(session));
    let state_name = if !applicable {
        "not_applicable"
    } else if let Some(job) = job.as_ref() {
        job.state
    } else if session.state == SessionState::Failed {
        "failed"
    } else if session.state.is_terminal() {
        "stopped"
    } else {
        "unavailable"
    };
    let runtime = match state.live.remux_service() {
        Some(service) => {
            let snapshot = service.snapshot().await;
            Some(RemuxRuntimeDiagnostics {
                active_jobs: snapshot.active_jobs,
                available_capacity: snapshot.available_capacity,
                jobs_started: snapshot.jobs_started,
                jobs_completed: snapshot.jobs_completed,
                jobs_failed: snapshot.jobs_failed,
                jobs_cancelled: snapshot.jobs_cancelled,
                temp_bytes: snapshot.temp_bytes,
            })
        }
        None => None,
    };
    RemuxDiagnostics {
        applicable,
        profile: job.as_ref().map(|job| job.profile).or(profile),
        state: state_name,
        exit_category: (applicable && session.state.is_terminal())
            .then_some(session.state.as_str()),
        stderr_tail: job.and_then(|job| job.stderr_tail),
        runtime,
    }
}

fn remux_profile(session: &SessionRecord) -> &'static str {
    match session.protocol.as_str() {
        "mpeg_ts" => "mpeg_ts_to_hls_copy",
        "dash" => "dash_to_hls_copy",
        _ => "unsupported",
    }
}

fn support_response(value: Value, partial: bool, request_id: Uuid) -> Response {
    let mut response = Json(SupportEnvelope {
        data: value,
        meta: SupportMeta {
            request_id: request_id.to_string(),
            generated_at: timestamp(Utc::now()),
            cache_state: "none",
            partial,
        },
        errors: Vec::new(),
    })
    .into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("vary", HeaderValue::from_static("Authorization, Cookie"));
    response
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn invalid_request() -> LiveHttpRejection {
    LiveHttpRejection::invalid_request()
}

fn session_not_found() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::NOT_FOUND,
        "LIVE_SESSION_NOT_FOUND",
        "The Live session was not found.",
        false,
    )
}

fn service_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_PROVIDER_UNAVAILABLE",
        "The Live diagnostics service is unavailable.",
        true,
    )
}

fn map_repository_error(error: SessionRepositoryError) -> LiveHttpRejection {
    match error {
        SessionRepositoryError::NotFound => session_not_found(),
        SessionRepositoryError::InvalidInput => invalid_request(),
        _ => service_unavailable(),
    }
}
