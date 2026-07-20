//! Account-authenticated control API for standalone Live sessions.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};

use axum::{
    Json, async_trait,
    extract::{FromRequestParts, Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header::CACHE_CONTROL, request::Parts},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    authz::Capability,
    config::RunEnvironment,
    http::auth::{AccountAuthTransport, CurrentPrincipal},
    live::{
        admin::ActorSnapshot,
        catalog::{
            LiveProviderAccess, LiveProviderGrantRepository, LivePublicKeyCodec, LivePublicKeyScope,
        },
        contract::{
            ProviderOperation, ProviderRequestContext, RefreshFailure, RefreshFailureCategory,
            RefreshRequest as ProviderRefreshRequest, RefreshSessionContext, ResolveRequest,
            ResolvedSources, SensitiveString, SourceDescriptor, StreamProtocol,
        },
        crypto::{CorrelationHashPurpose, SecretBytes},
        egress::{
            EffectiveEgressPolicy, EgressPolicyMode, EgressPolicySource, LiveEgressOutcome,
            SessionEgressPolicyRequest,
        },
        planner::{
            ClientCapabilities, DeliveryPlan, DirectDisclosureRule, PlannerInput, PlannerPolicy,
            PlannerRejectionCode, PlaybackRequirements, plan_delivery,
        },
        service::LiveComponent,
        session::{
            DeliveryMode, IdempotencyRequest, LiveSessionRepository, LiveTrackPreferenceUpdate,
            LiveTrackSelection, NewSession, RecoveryAction, RecoveryOutcome, RecoveryPolicy,
            RecoveryPolicyError, RecoveryReason, RequestedEgressMode, SessionOwner,
            SessionProtocol, SessionRecord, SessionRecoveryFailure, SessionRecoveryReplacement,
            SessionRepositoryError, SessionState, StoredEgressPolicy, StoredRecoveryState,
            StoredSessionDescriptor, StoredSource, TerminalReason,
        },
        upstream::{
            DestinationPolicy, DestinationRule, FetchRequest, LocalDestinationDenylist,
            NetworkScope, PrivateLanGate, SystemDnsResolver, UpstreamFetcher, UpstreamLimits,
            UpstreamMethod,
        },
    },
    state::AppState,
};

use super::catalog::{LiveHttpRejection, access_context, admit};

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const CSRF_HEADER: &str = "x-elixir-csrf";
const ORIGIN_HEADER: &str = "origin";
const MAX_POLICY_ROWS: usize = 256;
const MAX_OBSERVED_SKEW_SECONDS: i64 = 300;
const SOURCE_KEY_MAX_SECONDS: i64 = 15 * 60;
const MAX_CREATE_LOCKS: usize = 10_000;

static CREATE_LOCKS: OnceLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();
static RECOVERY_LOCKS: OnceLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();

pub struct LivePlayPrincipal(pub(super) CurrentPrincipal);

#[async_trait]
impl FromRequestParts<AppState> for LivePlayPrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled || !state.live.config().playback_enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if !principal.has_capability(Capability::LivePlay) {
            return Err(LiveHttpRejection::new(
                StatusCode::FORBIDDEN,
                "LIVE_CAPABILITY_REQUIRED",
                "The active profile cannot play Live streams.",
                false,
            ));
        }
        let snapshot = state.live.snapshot().await;
        let playback_ready = snapshot
            .features
            .iter()
            .any(|feature| feature.flag == "playback_enabled" && feature.effective_enabled);
        if !playback_ready
            || state.live.session_repository().is_none()
            || state.live.control_fencing_token().await.is_none()
        {
            return Err(control_unavailable());
        }
        Ok(Self(principal))
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionCreateRequest {
    provider_id: Uuid,
    item_key: String,
    stream_option_key: String,
    client: ClientCapabilityRequest,
    #[serde(default)]
    egress_policy_id: Option<Uuid>,
    #[serde(default)]
    egress_mode: EgressModeRequest,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClientCapabilityRequest {
    platform: ClientPlatform,
    player: String,
    protocols: Vec<StreamProtocol>,
    video_codecs: Vec<String>,
    audio_codecs: Vec<String>,
    supports_request_headers: bool,
    supports_cookies: bool,
    supports_low_latency_hls: bool,
    supports_origin_time_shift: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ClientPlatform {
    Macos,
    Windows,
    Linux,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum EgressModeRequest {
    #[default]
    Inherit,
    Off,
    PreferProtected,
    RequireProtected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeartbeatRequest {
    expected_revision: i64,
    player_state: PlayerState,
    observed_at: String,
    #[serde(default)]
    distance_from_live_edge_seconds: Option<f64>,
    #[serde(default)]
    source_key: Option<String>,
    #[serde(default)]
    audio_track_id: Option<String>,
    #[serde(default)]
    audio_track_language: Option<String>,
    #[serde(default)]
    audio_track_title: Option<String>,
    #[serde(default)]
    subtitle_track_id: Option<String>,
    #[serde(default)]
    subtitle_track_language: Option<String>,
    #[serde(default)]
    subtitle_track_title: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PlayerState {
    Loading,
    Buffering,
    Playing,
    Paused,
    Ended,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EndQuery {
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionRecoveryRequest {
    expected_revision: i64,
    reason: RecoveryReason,
    #[serde(default)]
    requested_source_key: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionResponse {
    session_id: Uuid,
    revision: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_revision: Option<i64>,
    delivery_mode: &'static str,
    decision_reason: String,
    egress: SessionEgressDto,
    playback_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_token: Option<String>,
    expires_at: String,
    hard_expires_at: String,
    heartbeat_interval_seconds: u64,
    live: LiveWindowDto,
    selected_source: SelectedSourceDto,
    available_sources: Vec<SelectedSourceDto>,
    track_preferences: TrackPreferencesDto,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionEgressDto {
    mode: &'static str,
    fallback_reason: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveWindowDto {
    seekable: bool,
    window_seconds: Option<u32>,
    target_latency_seconds: Option<f64>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SelectedSourceDto {
    source_key: String,
    label: String,
    quality: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrackSelectionDto {
    track_id: String,
    language: Option<String>,
    title: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrackPreferencesDto {
    audio: Option<TrackSelectionDto>,
    subtitle: Option<TrackSelectionDto>,
}

#[derive(Serialize)]
struct SessionDetailEnvelope {
    data: SessionDetailDto,
    meta: SessionMetaDto,
    errors: Vec<Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionDetailDto {
    session_id: Uuid,
    revision: i64,
    state: &'static str,
    delivery_mode: &'static str,
    protocol: &'static str,
    selected_source: SelectedSourceDto,
    available_sources: Vec<SelectedSourceDto>,
    track_preferences: TrackPreferencesDto,
    expires_at: String,
    hard_expires_at: String,
    error_code: Option<String>,
    timeline: Vec<TimelineDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimelineDto {
    at: String,
    revision: i64,
    state: &'static str,
    reason: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionMetaDto {
    request_id: Uuid,
    generated_at: String,
    cache_state: &'static str,
    partial: bool,
}

struct LoadedPolicy {
    planner_rules: Vec<DirectDisclosureRule>,
    preflight_rules: Vec<DestinationRule>,
}

struct PreparedRecovery {
    resolved: ResolvedSources,
    plan: DeliveryPlan,
    egress: EffectiveEgressPolicy,
}

pub async fn create(
    State(state): State<AppState>,
    LivePlayPrincipal(principal): LivePlayPrincipal,
    headers: HeaderMap,
    payload: Result<Json<SessionCreateRequest>, JsonRejection>,
) -> Response {
    let started_at = std::time::Instant::now();
    let mut start_metric = None;
    let result = async {
        let _admission = admit(principal.user_id)?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let request = payload.map_err(|_| LiveHttpRejection::invalid_request())?.0;
        validate_create_request(&request)?;
        let idempotency_key = required_visible_header(&headers, IDEMPOTENCY_HEADER, 16, 128)?;
        let now = Utc::now();
        let access = access_context(&principal, &headers)?;
        let crypto = state.live.crypto().await.ok_or_else(control_unavailable)?;
        let idempotency_hash = crypto
            .hash_correlation(
                CorrelationHashPurpose::IdempotencyKey,
                idempotency_key.as_bytes(),
            )
            .map_err(|_| internal_error())?;
        let _create_guard = acquire_create_lock(format!(
            "{}:{}:{idempotency_hash}",
            principal.user_id, principal.profile_id
        ))
        .await?;
        let repository = state
            .live
            .session_repository()
            .ok_or_else(control_unavailable)?;
        let fencing_token = state
            .live
            .control_fencing_token()
            .await
            .ok_or_else(control_unavailable)?;
        let codec = LivePublicKeyCodec::new(crypto);
        let scope = LivePublicKeyScope {
            home_id: principal.home_id,
            profile_id: principal.profile_id,
            authorization_revision: principal.capability_revision,
        };
        let item_id = codec
            .open_item(&request.item_key, request.provider_id, scope, now)
            .map_err(|_| invalid_stream_key())?;
        let (stream_item_id, stream_id) = codec
            .open_stream(&request.stream_option_key, request.provider_id, scope, now)
            .map_err(|_| invalid_stream_key())?;
        if item_id != stream_item_id {
            return Err(invalid_stream_key());
        }
        require_play_grant(&state, &principal, request.provider_id).await?;
        let provider_client = state
            .live
            .provider_client()
            .ok_or_else(control_unavailable)?;
        let provider = provider_client
            .directory()
            .get(request.provider_id)
            .await
            .map_err(|_| provider_unavailable())?;
        let provider_revision = format!("{:?}", provider.revision);
        let request_identity = request_identity(&request, &principal, &provider_revision)?;
        let owner = SessionOwner {
            user_id: principal.user_id,
            home_id: principal.home_id,
            profile_id: principal.profile_id,
            account_session_id: principal.account_session_id,
            provider_id: request.provider_id,
        };
        let idempotency = IdempotencyRequest {
            key: SecretBytes::from_utf8(idempotency_key),
            request_identity: SecretBytes::new(request_identity),
        };
        if let Some(grant) = repository
            .lookup_idempotency(owner, &idempotency, now)
            .await
            .map_err(map_repository_error)?
        {
            let actor = playback_actor(&principal)?;
            let session =
                ensure_delivery_ready(&state, &repository, grant.session, &actor, now).await?;
            let response = session_response(
                &state,
                &repository,
                &session,
                Some(grant.token.expose_secret()),
                now,
            )
            .await?;
            return Ok(token_response(StatusCode::OK, response));
        }

        let context = ProviderRequestContext {
            locale: access.locale,
            timezone: access.timezone,
            now,
        };
        let cancellation = CancellationToken::new();
        let resolved = provider_client
            .resolve(
                &provider,
                principal.user_id,
                &context,
                &ResolveRequest {
                    item_id: item_id.clone(),
                    stream_id: stream_id.clone(),
                },
                &cancellation,
            )
            .await
            .map_err(map_provider_error)?;
        let selected_egress = select_session_egress_policy(&state, &principal, &request).await?;
        let loaded_policy = load_policy(&state, principal.home_id, request.provider_id).await?;
        let client = planner_capabilities(&request.client)?;
        let mut planner_policy = planner_policy(&state, &provider, &loaded_policy).await;
        planner_policy.protected_egress_mode = selected_egress.mode;
        let plan = plan_delivery(&PlannerInput {
            sources: &resolved,
            client: &client,
            policy: &planner_policy,
            requirements: PlaybackRequirements {
                require_time_shift: false,
                require_server_delivery: false,
            },
            now,
        })
        .map_err(map_planner_error)?;
        let selected =
            source_at(&resolved, plan.selected_source_index).ok_or_else(|| contract_invalid())?;
        start_metric = Some((
            plan.mode.as_str(),
            session_protocol(selected.protocol).as_str(),
        ));
        match plan.mode {
            DeliveryMode::ClientDirect => {
                preflight_direct(selected, loaded_policy.preflight_rules, {
                    #[cfg(test)]
                    {
                        state.live.allows_test_live_sources()
                    }
                    #[cfg(not(test))]
                    {
                        false
                    }
                })
                .await?;
            }
            DeliveryMode::ServerRelay
                if matches!(
                    selected.protocol,
                    StreamProtocol::Hls | StreamProtocol::HttpProgressive | StreamProtocol::MpegTs
                ) && state.live.relay_service().is_some() => {}
            DeliveryMode::ServerRemux
                if matches!(
                    selected.protocol,
                    StreamProtocol::Dash | StreamProtocol::MpegTs
                ) && state.live.remux_service().is_some() => {}
            DeliveryMode::ServerRelay | DeliveryMode::ServerRemux => {
                return Err(delivery_runtime_unavailable(plan.mode));
            }
        }
        require_play_grant(&state, &principal, request.provider_id).await?;
        let mut stored = StoredSessionDescriptor::from_resolved(
            &resolved,
            &provider_revision,
            &plan,
            (plan.mode == DeliveryMode::ClientDirect).then(|| selected.url.expose().to_string()),
        )
        .map_err(|_| contract_invalid())?;
        stored.egress = StoredEgressPolicy::from_effective(&selected_egress);
        stored.recovery = StoredRecoveryState::new(
            client.clone(),
            requested_egress_mode(request.egress_mode),
            &selected.stream_id,
        )
        .map_err(|_| contract_invalid())?;
        let descriptor = serde_json::to_vec(&stored).map_err(|_| internal_error())?;
        let item_snapshot = serde_json::to_vec(&json!({
            "providerId": request.provider_id,
            "itemId": item_id,
        }))
        .map_err(|_| internal_error())?;
        let created = {
            let _key_rotation = state.live.key_rotation_guard().await;
            repository
                .create(
                    NewSession {
                        owner,
                        item_key: SecretBytes::from_utf8(request.item_key),
                        stream_option_key: SecretBytes::from_utf8(request.stream_option_key),
                        item_snapshot: SecretBytes::new(item_snapshot),
                        descriptor: SecretBytes::new(descriptor),
                        delivery_mode: plan.mode,
                        protocol: session_protocol(selected.protocol),
                        source_index: i32::try_from(plan.selected_source_index)
                            .map_err(|_| contract_invalid())?,
                        control_fencing_token: fencing_token,
                        now,
                    },
                    Some(idempotency),
                )
                .await
                .map_err(map_repository_error)?
        };
        let replayed = created.replayed;
        let token = created.token.expose_secret().to_string();
        let actor = playback_actor(&principal)?;
        let session =
            ensure_delivery_ready(&state, &repository, created.session, &actor, now).await?;
        if replayed {
            start_metric = None;
        }
        let response = session_response(&state, &repository, &session, Some(&token), now).await?;
        Ok(token_response(
            if replayed {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            },
            response,
        ))
    }
    .await;
    if let Some((delivery_mode, protocol)) = start_metric {
        crate::live::metrics::SESSIONS_STARTED
            .with_label_values(&[
                delivery_mode,
                protocol,
                if result.is_ok() { "started" } else { "failed" },
            ])
            .inc();
        crate::live::metrics::SESSION_START_DURATION
            .with_label_values(&[delivery_mode])
            .observe(started_at.elapsed().as_secs_f64());
    }
    result.unwrap_or_else(IntoResponse::into_response)
}

pub async fn get(
    State(state): State<AppState>,
    LivePlayPrincipal(principal): LivePlayPrincipal,
    Path(session_id): Path<Uuid>,
) -> Response {
    let result = async {
        let _admission = admit(principal.user_id)?;
        let repository = state
            .live
            .session_repository()
            .ok_or_else(control_unavailable)?;
        let session = owned_session(&repository, &principal, session_id).await?;
        if session.state.is_terminal() {
            return Err(session_expired());
        }
        require_play_grant(&state, &principal, session.owner.provider_id).await?;
        let detail = session_detail(&state, &repository, &session, Utc::now()).await?;
        Ok(no_store(Json(detail).into_response()))
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

pub async fn heartbeat(
    State(state): State<AppState>,
    LivePlayPrincipal(principal): LivePlayPrincipal,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
    payload: Result<Json<HeartbeatRequest>, JsonRejection>,
) -> Response {
    let result = async {
        let _admission = admit(principal.user_id)?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let request = payload.map_err(|_| LiveHttpRejection::invalid_request())?.0;
        validate_heartbeat(&request)?;
        let track_update = track_preference_update(&request)?;
        let now = Utc::now();
        let repository = state
            .live
            .session_repository()
            .ok_or_else(control_unavailable)?;
        let session = owned_session(&repository, &principal, session_id).await?;
        require_play_grant(&state, &principal, session.owner.provider_id).await?;
        let stored = decrypt_descriptor(&repository, &session).await?;
        require_current_provider(&state, &session, &stored).await?;
        let selected = stored.selected().ok_or_else(contract_invalid)?;
        if let Some(source_key) = &request.source_key {
            let crypto = state.live.crypto().await.ok_or_else(control_unavailable)?;
            let opened_source = LivePublicKeyCodec::new(crypto)
                .open_source(
                    source_key,
                    session.id,
                    session.owner.provider_id,
                    &stored.provider_revision,
                    request.expected_revision,
                    now,
                )
                .map_err(|_| invalid_source_key())?;
            if opened_source != selected.stream_id {
                return Err(invalid_source_key());
            }
        }
        let fence = state
            .live
            .control_fencing_token()
            .await
            .ok_or_else(control_unavailable)?;
        let updated = match request.player_state {
            PlayerState::Ended => {
                repository
                    .terminate(
                        session.owner,
                        session.id,
                        request.expected_revision,
                        fence,
                        TerminalReason::ended(),
                        now,
                    )
                    .await
                    .map_err(map_repository_error)?
                    .session
            }
            PlayerState::Playing if session.state == SessionState::Ready => {
                let playing = repository
                    .transition(
                        session.owner,
                        session.id,
                        request.expected_revision,
                        fence,
                        SessionState::Playing,
                        now,
                    )
                    .await
                    .map_err(map_repository_error)?
                    .session;
                repository
                    .heartbeat_with_track_preferences(
                        playing.owner,
                        playing.id,
                        playing.revision,
                        fence,
                        now,
                        track_update.as_ref(),
                    )
                    .await
                    .map_err(map_repository_error)?
                    .session
            }
            _ => {
                repository
                    .heartbeat_with_track_preferences(
                        session.owner,
                        session.id,
                        request.expected_revision,
                        fence,
                        now,
                        track_update.as_ref(),
                    )
                    .await
                    .map_err(map_repository_error)?
                    .session
            }
        };
        if request.player_state == PlayerState::Ended {
            end_delivery_runtime(&state, session.id, fence).await?;
        }
        let detail = detail_from_stored(&state, &updated, &stored, now).await?;
        Ok(no_store(Json(detail).into_response()))
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

pub async fn refresh(
    State(state): State<AppState>,
    LivePlayPrincipal(principal): LivePlayPrincipal,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
    payload: Result<Json<SessionRecoveryRequest>, JsonRejection>,
) -> Response {
    recovery_response(
        state,
        principal,
        session_id,
        headers,
        payload,
        RecoveryAction::Refresh,
    )
    .await
}

pub async fn failover(
    State(state): State<AppState>,
    LivePlayPrincipal(principal): LivePlayPrincipal,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
    payload: Result<Json<SessionRecoveryRequest>, JsonRejection>,
) -> Response {
    recovery_response(
        state,
        principal,
        session_id,
        headers,
        payload,
        RecoveryAction::Failover,
    )
    .await
}

async fn recovery_response(
    state: AppState,
    principal: CurrentPrincipal,
    session_id: Uuid,
    headers: HeaderMap,
    payload: Result<Json<SessionRecoveryRequest>, JsonRejection>,
    action: RecoveryAction,
) -> Response {
    let metric_reason = payload
        .as_ref()
        .ok()
        .map(|request| request.reason.as_str())
        .unwrap_or("invalid_request");
    let result = async {
        let _admission = admit(principal.user_id)?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let request = payload.map_err(|_| LiveHttpRejection::invalid_request())?.0;
        validate_recovery_request(&request, action)?;
        let _recovery_guard = acquire_recovery_lock(format!(
            "{}:{}:{}",
            principal.user_id, principal.profile_id, session_id
        ))
        .await?;
        let now = Utc::now();
        let repository = state
            .live
            .session_repository()
            .ok_or_else(control_unavailable)?;
        let session = owned_session(&repository, &principal, session_id).await?;
        if session.state.is_terminal() {
            return Err(session_expired());
        }
        if session.revision != request.expected_revision {
            return Err(session_conflict());
        }
        if !matches!(
            session.state,
            SessionState::Ready | SessionState::Playing | SessionState::Reconnecting
        ) {
            return Err(session_conflict());
        }
        require_play_grant(&state, &principal, session.owner.provider_id).await?;
        let mut stored = decrypt_descriptor(&repository, &session).await?;
        require_current_provider(&state, &session, &stored).await?;
        stored
            .recovery
            .validate()
            .map_err(|_| recovery_unavailable())?;
        let policy = RecoveryPolicy::from(&state.live.config().recovery);
        if let Err(error) = stored.recovery.admit_transition(now, policy) {
            return exhaust_recovery(
                &state,
                &repository,
                &session,
                request.expected_revision,
                error,
                now,
            )
            .await;
        }
        let requested_source_id =
            open_requested_source(&state, &session, &stored, &request, action, now).await?;
        let provider_client = state
            .live
            .provider_client()
            .ok_or_else(control_unavailable)?;
        let provider = provider_client
            .directory()
            .get(session.owner.provider_id)
            .await
            .map_err(|_| provider_unavailable())?;
        let item_id = decrypt_item_id(&repository, &session).await?;
        let access = access_context(&principal, &headers)?;
        let context = ProviderRequestContext {
            locale: access.locale,
            timezone: access.timezone,
            now,
        };
        let loaded_policy =
            load_policy(&state, session.owner.home_id, session.owner.provider_id).await?;
        let mut planner_policy = planner_policy(&state, &provider, &loaded_policy).await;
        if session.delivery_mode == DeliveryMode::ServerRelay {
            planner_policy.relay_capacity_available = true;
        }
        if session.delivery_mode == DeliveryMode::ServerRemux {
            planner_policy.remux_capacity_available = true;
        }
        let client = stored
            .recovery
            .client
            .clone()
            .ok_or_else(recovery_unavailable)?;
        let cancellation = CancellationToken::new();
        let prepared = match action {
            RecoveryAction::Refresh => {
                prepare_same_source_refresh(
                    &state,
                    &provider_client,
                    &provider,
                    &principal,
                    &session,
                    &stored,
                    &item_id,
                    request.reason,
                    &context,
                    &client,
                    &planner_policy,
                    &loaded_policy,
                    &cancellation,
                )
                .await
            }
            RecoveryAction::Failover => {
                prepare_source_failover(
                    &state,
                    &provider_client,
                    &provider,
                    &principal,
                    &session,
                    &mut stored,
                    &item_id,
                    requested_source_id.as_deref(),
                    request.reason,
                    &context,
                    &client,
                    &planner_policy,
                    &loaded_policy,
                    policy,
                    &cancellation,
                )
                .await
            }
        };
        let prepared = match prepared {
            Ok(Some(prepared)) => prepared,
            Ok(None) => {
                let completed_at = Utc::now();
                return exhaust_recovery(
                    &state,
                    &repository,
                    &session,
                    request.expected_revision,
                    RecoveryPolicyError::SourceLimit,
                    completed_at,
                )
                .await;
            }
            Err(error) => {
                let completed_at = Utc::now();
                record_recovery_failure(
                    &state,
                    &repository,
                    &session,
                    &mut stored,
                    action,
                    request.reason,
                    policy,
                    completed_at,
                )
                .await?;
                return Err(error);
            }
        };
        let selected = source_at(&prepared.resolved, prepared.plan.selected_source_index)
            .ok_or_else(contract_invalid)?;
        let completed_at = Utc::now();
        if selected
            .expires_at
            .is_some_and(|expires_at| expires_at <= completed_at)
        {
            record_recovery_failure(
                &state,
                &repository,
                &session,
                &mut stored,
                action,
                request.reason,
                policy,
                completed_at,
            )
            .await?;
            return Err(stream_expired());
        }
        let final_revision = action
            .success_revision(request.expected_revision)
            .ok_or_else(internal_error)?;
        stored
            .recovery
            .record(
                action,
                request.reason,
                RecoveryOutcome::Succeeded,
                &selected.stream_id,
                final_revision,
                completed_at,
                policy,
            )
            .map_err(|_| recovery_exhausted())?;
        let playback_url = (prepared.plan.mode == DeliveryMode::ClientDirect)
            .then(|| selected.url.expose().to_string());
        let mut replacement = StoredSessionDescriptor::from_resolved(
            &prepared.resolved,
            &stored.provider_revision,
            &prepared.plan,
            playback_url,
        )
        .map_err(|_| contract_invalid())?;
        replacement.egress = StoredEgressPolicy::from_effective(&prepared.egress);
        replacement.recovery = stored.recovery;
        let descriptor = serde_json::to_vec(&replacement).map_err(|_| internal_error())?;
        let fence = state
            .live
            .control_fencing_token()
            .await
            .ok_or_else(control_unavailable)?;
        let grant = {
            let _key_rotation = state.live.key_rotation_guard().await;
            repository
                .replace_for_recovery(SessionRecoveryReplacement {
                    owner: session.owner,
                    session_id: session.id,
                    expected_revision: request.expected_revision,
                    control_fencing_token: fence,
                    descriptor: SecretBytes::new(descriptor),
                    delivery_mode: prepared.plan.mode,
                    protocol: session_protocol(selected.protocol),
                    source_index: i32::try_from(prepared.plan.selected_source_index)
                        .map_err(|_| contract_invalid())?,
                    action,
                    now: completed_at,
                })
                .await
                .map_err(map_repository_error)?
        };
        if grant.session.revision != final_revision {
            return Err(internal_error());
        }
        let actor = playback_actor(&principal)?;
        replace_delivery_runtime(
            &state,
            &repository,
            &grant.session,
            &actor,
            fence,
            completed_at,
        )
        .await?;
        let token = grant.token.expose_secret().to_string();
        let response = session_response(
            &state,
            &repository,
            &grant.session,
            Some(&token),
            completed_at,
        )
        .await?;
        Ok(token_response(StatusCode::OK, response))
    }
    .await;
    record_recovery_metric(action, metric_reason, result.is_ok());
    result.unwrap_or_else(IntoResponse::into_response)
}

fn record_recovery_metric(action: RecoveryAction, reason: &'static str, succeeded: bool) {
    let outcome = if succeeded { "succeeded" } else { "failed" };
    let action_counter = match action {
        RecoveryAction::Refresh => &*crate::live::metrics::REFRESHES,
        RecoveryAction::Failover => &*crate::live::metrics::FAILOVERS,
    };
    action_counter.with_label_values(&[reason, outcome]).inc();
    crate::live::metrics::RECONNECTS
        .with_label_values(&[reason, outcome])
        .inc();
}

pub async fn end(
    State(state): State<AppState>,
    LivePlayPrincipal(principal): LivePlayPrincipal,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
    Query(query): Query<EndQuery>,
) -> Response {
    let result = async {
        let _admission = admit(principal.user_id)?;
        validate_mutation_transport(&state, &principal, &headers)?;
        if query.expected_revision < 1 {
            return Err(LiveHttpRejection::invalid_request());
        }
        let repository = state
            .live
            .session_repository()
            .ok_or_else(control_unavailable)?;
        let session = owned_session(&repository, &principal, session_id).await?;
        let fence = state
            .live
            .control_fencing_token()
            .await
            .ok_or_else(control_unavailable)?;
        repository
            .terminate(
                session.owner,
                session.id,
                query.expected_revision,
                fence,
                TerminalReason::ended(),
                Utc::now(),
            )
            .await
            .map_err(map_repository_error)?;
        end_delivery_runtime(&state, session.id, fence).await?;
        refresh_live_gauges(&state).await;
        crate::live::metrics::CLEANUP
            .with_label_values(&["session", "completed"])
            .inc();
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

async fn acquire_recovery_lock(
    key: String,
) -> Result<tokio::sync::OwnedMutexGuard<()>, LiveHttpRejection> {
    let lock = {
        let registry = RECOVERY_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry = registry.lock().map_err(|_| internal_error())?;
        if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
            lock
        } else {
            if registry.len() >= MAX_CREATE_LOCKS {
                registry.retain(|_, value| value.strong_count() > 0);
                if registry.len() >= MAX_CREATE_LOCKS {
                    return Err(LiveHttpRejection::new(
                        StatusCode::TOO_MANY_REQUESTS,
                        "LIVE_RATE_LIMITED",
                        "Too many Live recovery operations are in progress.",
                        true,
                    ));
                }
            }
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            registry.insert(key, Arc::downgrade(&lock));
            lock
        }
    };
    Ok(lock.lock_owned().await)
}

fn validate_recovery_request(
    request: &SessionRecoveryRequest,
    action: RecoveryAction,
) -> Result<(), LiveHttpRejection> {
    let valid_key = request
        .requested_source_key
        .as_ref()
        .is_none_or(|key| (16..=2_048).contains(&key.len()));
    let valid_shape = match action {
        RecoveryAction::Refresh => {
            request.reason != RecoveryReason::ManualSourceSwitch
                && request.requested_source_key.is_none()
        }
        RecoveryAction::Failover => {
            (request.reason == RecoveryReason::ManualSourceSwitch)
                == request.requested_source_key.is_some()
        }
    };
    if request.expected_revision < 1 || !valid_key || !valid_shape {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(())
}

async fn open_requested_source(
    state: &AppState,
    session: &SessionRecord,
    stored: &StoredSessionDescriptor,
    request: &SessionRecoveryRequest,
    action: RecoveryAction,
    now: DateTime<Utc>,
) -> Result<Option<String>, LiveHttpRejection> {
    if action != RecoveryAction::Failover {
        return Ok(None);
    }
    let Some(key) = request.requested_source_key.as_deref() else {
        return Ok(None);
    };
    let crypto = state.live.crypto().await.ok_or_else(control_unavailable)?;
    let source_id = LivePublicKeyCodec::new(crypto)
        .open_source(
            key,
            session.id,
            session.owner.provider_id,
            &stored.provider_revision,
            request.expected_revision,
            now,
        )
        .map_err(|_| invalid_source_key())?;
    if stored
        .selected()
        .is_some_and(|source| source.stream_id == source_id)
        || !stored
            .sources
            .iter()
            .any(|source| source.stream_id == source_id)
    {
        return Err(invalid_source_key());
    }
    Ok(Some(source_id))
}

async fn decrypt_item_id(
    repository: &LiveSessionRepository,
    session: &SessionRecord,
) -> Result<String, LiveHttpRejection> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct ItemSnapshot {
        provider_id: Uuid,
        item_id: String,
    }

    let secrets = repository
        .decrypt_secrets(session.owner, session.id)
        .await
        .map_err(map_repository_error)?;
    let snapshot: ItemSnapshot = serde_json::from_slice(secrets.item_snapshot.expose_secret())
        .map_err(|_| internal_error())?;
    if snapshot.provider_id != session.owner.provider_id
        || snapshot.item_id.is_empty()
        || snapshot.item_id.len() > 512
        || snapshot.item_id.chars().any(char::is_control)
    {
        return Err(internal_error());
    }
    Ok(snapshot.item_id)
}

#[allow(clippy::too_many_arguments)]
async fn prepare_same_source_refresh(
    state: &AppState,
    provider_client: &crate::live::provider::LiveProviderClient,
    provider: &crate::live::provider::LiveProviderSnapshot,
    principal: &CurrentPrincipal,
    session: &SessionRecord,
    stored: &StoredSessionDescriptor,
    item_id: &str,
    reason: RecoveryReason,
    context: &ProviderRequestContext,
    client: &ClientCapabilities,
    planner_policy: &PlannerPolicy,
    loaded_policy: &LoadedPolicy,
    cancellation: &CancellationToken,
) -> Result<Option<PreparedRecovery>, LiveHttpRejection> {
    let selected = stored.selected().ok_or_else(contract_invalid)?;
    let resolved = refresh_provider_source(
        provider_client,
        provider,
        principal.user_id,
        session,
        stored,
        item_id,
        selected,
        reason,
        context,
        cancellation,
    )
    .await?;
    let prepared = prepare_recovery_plan(
        state,
        resolved,
        &selected.stream_id,
        stored.recovery.requested_egress,
        &stored.egress,
        client,
        planner_policy,
        loaded_policy,
        false,
        context.now,
    )
    .await?;
    Ok(Some(prepared))
}

#[allow(clippy::too_many_arguments)]
async fn prepare_source_failover(
    state: &AppState,
    provider_client: &crate::live::provider::LiveProviderClient,
    provider: &crate::live::provider::LiveProviderSnapshot,
    principal: &CurrentPrincipal,
    session: &SessionRecord,
    stored: &mut StoredSessionDescriptor,
    item_id: &str,
    requested_source_id: Option<&str>,
    reason: RecoveryReason,
    context: &ProviderRequestContext,
    client: &ClientCapabilities,
    planner_policy: &PlannerPolicy,
    loaded_policy: &LoadedPolicy,
    recovery_policy: RecoveryPolicy,
    cancellation: &CancellationToken,
) -> Result<Option<PreparedRecovery>, LiveHttpRejection> {
    let current = stored
        .selected()
        .ok_or_else(contract_invalid)?
        .stream_id
        .clone();
    if should_replan_direct_through_server(
        session.delivery_mode,
        reason,
        requested_source_id.is_some(),
    ) {
        let resolved = resolved_from_stored(stored, &current)?;
        if let Ok(prepared) = prepare_recovery_plan(
            state,
            resolved,
            &current,
            stored.recovery.requested_egress,
            &stored.egress,
            client,
            planner_policy,
            loaded_policy,
            true,
            context.now,
        )
        .await
        {
            return Ok(Some(prepared));
        }
    }
    stored
        .recovery
        .mark_source_failed(&current, context.now, recovery_policy)
        .map_err(|_| recovery_exhausted())?;
    let mut candidates = stored
        .sources
        .iter()
        .filter(|source| source.stream_id != current)
        .filter(|source| requested_source_id.is_none_or(|requested| source.stream_id == requested))
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.stream_id.cmp(&right.stream_id))
    });
    let explicit = requested_source_id.is_some();
    let refresh_lead =
        chrono::Duration::seconds(state.live.config().recovery.refresh_expiry_lead_seconds as i64);
    for candidate in candidates {
        if !stored
            .recovery
            .may_attempt_source(&candidate.stream_id, context.now, recovery_policy)
            .map_err(|_| recovery_exhausted())?
        {
            continue;
        }
        let needs_refresh = candidate
            .expires_at
            .is_some_and(|expires_at| expires_at <= context.now + refresh_lead);
        let resolved = if needs_refresh {
            match refresh_provider_source(
                provider_client,
                provider,
                principal.user_id,
                session,
                stored,
                item_id,
                &candidate,
                RecoveryReason::ExpiryThreshold,
                context,
                cancellation,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    stored
                        .recovery
                        .mark_source_failed(&candidate.stream_id, context.now, recovery_policy)
                        .map_err(|_| recovery_exhausted())?;
                    if explicit {
                        return Err(error);
                    }
                    continue;
                }
            }
        } else {
            resolved_from_stored(stored, &candidate.stream_id)?
        };
        match prepare_recovery_plan(
            state,
            resolved,
            &candidate.stream_id,
            stored.recovery.requested_egress,
            &stored.egress,
            client,
            planner_policy,
            loaded_policy,
            false,
            context.now,
        )
        .await
        {
            Ok(prepared) => return Ok(Some(prepared)),
            Err(error) => {
                stored
                    .recovery
                    .mark_source_failed(&candidate.stream_id, context.now, recovery_policy)
                    .map_err(|_| recovery_exhausted())?;
                if explicit {
                    return Err(error);
                }
            }
        }
    }
    if explicit {
        Err(stream_unavailable())
    } else {
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn refresh_provider_source(
    provider_client: &crate::live::provider::LiveProviderClient,
    provider: &crate::live::provider::LiveProviderSnapshot,
    user_id: Uuid,
    session: &SessionRecord,
    stored: &StoredSessionDescriptor,
    item_id: &str,
    source: &StoredSource,
    reason: RecoveryReason,
    context: &ProviderRequestContext,
    cancellation: &CancellationToken,
) -> Result<ResolvedSources, LiveHttpRejection> {
    if !provider.permits(ProviderOperation::Refresh) {
        return Err(recovery_unavailable());
    }
    let refresh_handle = source
        .refresh_handle
        .as_ref()
        .filter(|handle| !handle.is_empty())
        .ok_or_else(recovery_unavailable)?;
    let source_attempt = u8::try_from(stored.recovery.events.len().saturating_add(1))
        .unwrap_or(10)
        .clamp(1, 10);
    let resolved = provider_client
        .refresh(
            provider,
            user_id,
            context,
            &ProviderRefreshRequest {
                item_id: item_id.to_string(),
                stream_id: source.stream_id.clone(),
                refresh_handle: SensitiveString::new(refresh_handle.clone()),
                failure: RefreshFailure {
                    category: refresh_failure_category(reason),
                    http_status: refresh_http_status(reason),
                },
                session: RefreshSessionContext {
                    started_at: session.created_at,
                    source_attempt,
                },
            },
            cancellation,
        )
        .await
        .map_err(map_provider_error)?;
    if resolved.descriptor.stream_id != source.stream_id {
        return Err(contract_invalid());
    }
    Ok(order_resolved(resolved, &source.stream_id)?)
}

#[allow(clippy::too_many_arguments)]
async fn prepare_recovery_plan(
    state: &AppState,
    mut resolved: ResolvedSources,
    preferred_source_id: &str,
    requested_egress: RequestedEgressMode,
    stored_egress: &StoredEgressPolicy,
    client: &ClientCapabilities,
    planner_policy: &PlannerPolicy,
    loaded_policy: &LoadedPolicy,
    require_server_delivery: bool,
    now: DateTime<Utc>,
) -> Result<PreparedRecovery, LiveHttpRejection> {
    resolved = order_resolved(resolved, preferred_source_id)?;
    let _ = requested_egress;
    let egress = stored_egress
        .to_effective()
        .map_err(|_| contract_invalid())?;
    let mut planner_policy = planner_policy.clone();
    planner_policy.protected_egress_mode = egress.mode;
    let plan = plan_delivery(&PlannerInput {
        sources: &resolved,
        client,
        policy: &planner_policy,
        requirements: PlaybackRequirements {
            require_time_shift: false,
            require_server_delivery,
        },
        now,
    })
    .map_err(map_planner_error)?;
    let selected = source_at(&resolved, plan.selected_source_index).ok_or_else(contract_invalid)?;
    if selected.stream_id != preferred_source_id {
        return Err(stream_unavailable());
    }
    match plan.mode {
        DeliveryMode::ClientDirect => {
            preflight_direct(selected, loaded_policy.preflight_rules.clone(), {
                #[cfg(test)]
                {
                    state.live.allows_test_live_sources()
                }
                #[cfg(not(test))]
                {
                    false
                }
            })
            .await?;
        }
        DeliveryMode::ServerRelay
            if matches!(
                selected.protocol,
                StreamProtocol::Hls | StreamProtocol::HttpProgressive | StreamProtocol::MpegTs
            ) && state.live.relay_service().is_some() => {}
        DeliveryMode::ServerRemux
            if matches!(
                selected.protocol,
                StreamProtocol::Dash | StreamProtocol::MpegTs
            ) && state.live.remux_service().is_some() => {}
        DeliveryMode::ServerRelay | DeliveryMode::ServerRemux => {
            return Err(delivery_runtime_unavailable(plan.mode));
        }
    }
    Ok(PreparedRecovery {
        resolved,
        plan,
        egress,
    })
}

fn should_replan_direct_through_server(
    delivery_mode: DeliveryMode,
    reason: RecoveryReason,
    explicit_source: bool,
) -> bool {
    delivery_mode == DeliveryMode::ClientDirect
        && !explicit_source
        && matches!(reason, RecoveryReason::Transport | RecoveryReason::Stalled)
}

fn resolved_from_stored(
    stored: &StoredSessionDescriptor,
    preferred_source_id: &str,
) -> Result<ResolvedSources, LiveHttpRejection> {
    let mut sources = stored
        .sources
        .iter()
        .map(StoredSource::to_source_descriptor)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| contract_invalid())?;
    let selected_index = sources
        .iter()
        .position(|source| source.stream_id == preferred_source_id)
        .ok_or_else(stream_unavailable)?;
    let descriptor = sources.remove(selected_index);
    sources.sort_by(source_priority_order);
    Ok(ResolvedSources {
        descriptor,
        alternatives: sources,
    })
}

fn order_resolved(
    resolved: ResolvedSources,
    preferred_source_id: &str,
) -> Result<ResolvedSources, LiveHttpRejection> {
    let mut sources = std::iter::once(resolved.descriptor)
        .chain(resolved.alternatives)
        .collect::<Vec<_>>();
    let selected_index = sources
        .iter()
        .position(|source| source.stream_id == preferred_source_id)
        .ok_or_else(contract_invalid)?;
    let descriptor = sources.remove(selected_index);
    sources.sort_by(source_priority_order);
    Ok(ResolvedSources {
        descriptor,
        alternatives: sources,
    })
}

fn source_priority_order(left: &SourceDescriptor, right: &SourceDescriptor) -> std::cmp::Ordering {
    right
        .priority
        .cmp(&left.priority)
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}

async fn record_recovery_failure(
    state: &AppState,
    repository: &LiveSessionRepository,
    session: &SessionRecord,
    stored: &mut StoredSessionDescriptor,
    action: RecoveryAction,
    reason: RecoveryReason,
    policy: RecoveryPolicy,
    now: DateTime<Utc>,
) -> Result<(), LiveHttpRejection> {
    let source_id = stored
        .selected()
        .ok_or_else(contract_invalid)?
        .stream_id
        .clone();
    let final_revision = action
        .failure_revision(session.revision)
        .ok_or_else(internal_error)?;
    stored
        .recovery
        .record(
            action,
            reason,
            RecoveryOutcome::Failed,
            &source_id,
            final_revision,
            now,
            policy,
        )
        .map_err(|_| recovery_exhausted())?;
    let descriptor = serde_json::to_vec(stored).map_err(|_| internal_error())?;
    let mutation = {
        let _key_rotation = state.live.key_rotation_guard().await;
        repository
            .record_recovery_failure(SessionRecoveryFailure {
                owner: session.owner,
                session_id: session.id,
                expected_revision: session.revision,
                control_fencing_token: session.control_fencing_token,
                descriptor: SecretBytes::new(descriptor),
                action,
                now,
            })
            .await
            .map_err(map_repository_error)?
    };
    if mutation.session.revision != final_revision {
        return Err(internal_error());
    }
    Ok(())
}

async fn exhaust_recovery(
    state: &AppState,
    repository: &LiveSessionRepository,
    session: &SessionRecord,
    expected_revision: i64,
    _policy_error: RecoveryPolicyError,
    now: DateTime<Utc>,
) -> Result<Response, LiveHttpRejection> {
    repository
        .terminate(
            session.owner,
            session.id,
            expected_revision,
            session.control_fencing_token,
            TerminalReason {
                state: SessionState::Failed,
                error_code: Some("LIVE_FAILOVER_EXHAUSTED".to_string()),
                error_detail_redacted: None,
            },
            now,
        )
        .await
        .map_err(map_repository_error)?;
    end_delivery_runtime(state, session.id, session.control_fencing_token).await?;
    Err(recovery_exhausted())
}

async fn replace_delivery_runtime(
    state: &AppState,
    repository: &LiveSessionRepository,
    session: &SessionRecord,
    actor: &ActorSnapshot,
    fence: i64,
    now: DateTime<Utc>,
) -> Result<(), LiveHttpRejection> {
    end_delivery_runtime(state, session.id, fence).await?;
    if let Err(rejection) = admit_delivery_runtime(state, session, actor).await {
        let termination = repository
            .terminate(
                session.owner,
                session.id,
                session.revision,
                fence,
                TerminalReason {
                    state: SessionState::Failed,
                    error_code: Some(
                        match session.delivery_mode {
                            DeliveryMode::ServerRemux => "LIVE_REMUX_UNAVAILABLE",
                            _ => "LIVE_UPSTREAM_REJECTED",
                        }
                        .to_string(),
                    ),
                    error_detail_redacted: None,
                },
                now,
            )
            .await;
        let cleanup = end_delivery_runtime(state, session.id, fence).await;
        refresh_live_gauges(state).await;
        termination.map_err(map_repository_error)?;
        cleanup?;
        return Err(rejection);
    }
    refresh_live_gauges(state).await;
    Ok(())
}

async fn admit_delivery_runtime(
    state: &AppState,
    session: &SessionRecord,
    actor: &ActorSnapshot,
) -> Result<(), LiveHttpRejection> {
    let repository = state
        .live
        .session_repository()
        .ok_or_else(control_unavailable)?;
    let stored = decrypt_descriptor(&repository, session).await?;
    let egress_policy = stored
        .egress
        .to_effective()
        .map_err(|_| contract_invalid())?;
    if egress_policy.protected() {
        if session.delivery_mode == DeliveryMode::ClientDirect {
            return Err(contract_invalid());
        }
        state
            .live
            .egress_service()
            .ok_or_else(egress_unavailable)?
            .ensure_session(session, &egress_policy, actor)
            .await
            .map_err(|_| egress_unavailable())?;
    }
    match session.delivery_mode {
        DeliveryMode::ClientDirect => Ok(()),
        DeliveryMode::ServerRelay => state
            .live
            .relay_service()
            .ok_or_else(|| delivery_runtime_unavailable(DeliveryMode::ServerRelay))?
            .admit_session(session)
            .await
            .map_err(super::delivery::map_relay_error),
        DeliveryMode::ServerRemux => state
            .live
            .remux_service()
            .ok_or_else(|| delivery_runtime_unavailable(DeliveryMode::ServerRemux))?
            .admit_session(session)
            .await
            .map_err(super::delivery::map_remux_error),
    }
}

pub(super) async fn end_delivery_runtime(
    state: &AppState,
    session_id: Uuid,
    control_fencing_token: i64,
) -> Result<(), LiveHttpRejection> {
    let mut cleanup_failed = false;
    if let Some(relay) = state.live.relay_service() {
        relay.end_session(session_id);
    }
    if let Some(remux) = state.live.remux_service() {
        if let Err(error) = remux.end_session(session_id).await {
            cleanup_failed = true;
            tracing::error!(
                session_id = %session_id,
                error = %error,
                "Live remux session cleanup failed"
            );
        }
    }
    if let Some(egress) = state.live.egress_service() {
        if let Err(error) = egress.end_session(session_id, control_fencing_token).await {
            cleanup_failed = true;
            tracing::error!(
                session_id = %session_id,
                error = %error,
                "Live egress session cleanup failed"
            );
        }
    }
    if cleanup_failed {
        crate::live::metrics::CLEANUP
            .with_label_values(&["session", "failed"])
            .inc();
        Err(cleanup_incomplete())
    } else {
        Ok(())
    }
}

fn refresh_failure_category(reason: RecoveryReason) -> RefreshFailureCategory {
    match reason {
        RecoveryReason::ExpiryThreshold | RecoveryReason::ManualSourceSwitch => {
            RefreshFailureCategory::ExpiryThreshold
        }
        RecoveryReason::UpstreamUnauthorized => RefreshFailureCategory::UpstreamUnauthorized,
        RecoveryReason::UpstreamForbidden => RefreshFailureCategory::UpstreamForbidden,
        RecoveryReason::UpstreamGone => RefreshFailureCategory::UpstreamGone,
        RecoveryReason::Transport => RefreshFailureCategory::Transport,
        RecoveryReason::Stalled => RefreshFailureCategory::Stalled,
    }
}

fn refresh_http_status(reason: RecoveryReason) -> Option<u16> {
    match reason {
        RecoveryReason::UpstreamUnauthorized => Some(401),
        RecoveryReason::UpstreamForbidden => Some(403),
        RecoveryReason::UpstreamGone => Some(410),
        RecoveryReason::ExpiryThreshold
        | RecoveryReason::Transport
        | RecoveryReason::Stalled
        | RecoveryReason::ManualSourceSwitch => None,
    }
}

async fn acquire_create_lock(
    key: String,
) -> Result<tokio::sync::OwnedMutexGuard<()>, LiveHttpRejection> {
    let lock = {
        let registry = CREATE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry = registry.lock().map_err(|_| internal_error())?;
        if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
            lock
        } else {
            if registry.len() >= MAX_CREATE_LOCKS {
                registry.retain(|_, value| value.strong_count() > 0);
                if registry.len() >= MAX_CREATE_LOCKS {
                    return Err(LiveHttpRejection::new(
                        StatusCode::TOO_MANY_REQUESTS,
                        "LIVE_RATE_LIMITED",
                        "Too many Live session creates are in progress.",
                        true,
                    ));
                }
            }
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            registry.insert(key, Arc::downgrade(&lock));
            lock
        }
    };
    Ok(lock.lock_owned().await)
}

async fn owned_session(
    repository: &LiveSessionRepository,
    principal: &CurrentPrincipal,
    session_id: Uuid,
) -> Result<SessionRecord, LiveHttpRejection> {
    repository
        .get_for_account(
            principal.user_id,
            principal.home_id,
            principal.profile_id,
            principal.account_session_id,
            session_id,
        )
        .await
        .map_err(map_repository_error)?
        .ok_or_else(session_not_found)
}

async fn require_play_grant(
    state: &AppState,
    principal: &CurrentPrincipal,
    provider_id: Uuid,
) -> Result<(), LiveHttpRejection> {
    let decision = LiveProviderGrantRepository::new(state.db_pool.clone())
        .visibility(
            principal.home_id,
            principal.profile_id,
            principal.role,
            principal.profile_type,
            provider_id,
            LiveProviderAccess::Play,
        )
        .await
        .map_err(|_| provider_unavailable())?;
    if decision.authorization_revision != principal.capability_revision {
        return Err(session_conflict());
    }
    if !decision.allowed {
        return Err(LiveHttpRejection::new(
            StatusCode::FORBIDDEN,
            "LIVE_PROVIDER_FORBIDDEN",
            "The provider is not shared for Live playback.",
            false,
        ));
    }
    Ok(())
}

async fn ensure_delivery_ready(
    state: &AppState,
    repository: &LiveSessionRepository,
    mut session: SessionRecord,
    actor: &ActorSnapshot,
    now: DateTime<Utc>,
) -> Result<SessionRecord, LiveHttpRejection> {
    let protected = decrypt_descriptor(repository, &session)
        .await?
        .egress
        .to_effective()
        .map_err(|_| contract_invalid())?
        .protected();
    for _ in 0..3 {
        let next = match session.state {
            SessionState::Resolving => Some(SessionState::Planning),
            SessionState::Planning if protected => Some(SessionState::ProvisioningEgress),
            SessionState::Planning
            | SessionState::ProvisioningEgress
            | SessionState::Ready
            | SessionState::Playing => None,
            state if state.is_terminal() => return Err(session_expired()),
            _ => return Err(session_conflict()),
        };
        let Some(next) = next else {
            break;
        };
        session = transition_or_reload(repository, session, next, now).await?;
    }
    if let Err(rejection) = admit_delivery_runtime(state, &session, actor).await {
        let _ = repository
            .terminate(
                session.owner,
                session.id,
                session.revision,
                session.control_fencing_token,
                TerminalReason {
                    state: SessionState::Failed,
                    error_code: Some(
                        if protected {
                            "LIVE_EGRESS_UNAVAILABLE"
                        } else {
                            match session.delivery_mode {
                                DeliveryMode::ServerRemux => "LIVE_REMUX_UNAVAILABLE",
                                DeliveryMode::ServerRelay => "LIVE_UPSTREAM_REJECTED",
                                DeliveryMode::ClientDirect => "LIVE_PROVIDER_UNAVAILABLE",
                            }
                        }
                        .to_string(),
                    ),
                    error_detail_redacted: None,
                },
                now,
            )
            .await;
        let cleanup = end_delivery_runtime(state, session.id, session.control_fencing_token).await;
        refresh_live_gauges(state).await;
        cleanup?;
        return Err(rejection);
    }
    for _ in 0..6 {
        match session.state {
            SessionState::Planning | SessionState::ProvisioningEgress => {
                session =
                    transition_or_reload(repository, session, SessionState::Ready, now).await?;
            }
            SessionState::Ready | SessionState::Playing => {
                refresh_live_gauges(state).await;
                return Ok(session);
            }
            state if state.is_terminal() => return Err(session_expired()),
            _ => return Err(session_conflict()),
        }
    }
    Err(session_conflict())
}

async fn transition_or_reload(
    repository: &LiveSessionRepository,
    session: SessionRecord,
    next: SessionState,
    now: DateTime<Utc>,
) -> Result<SessionRecord, LiveHttpRejection> {
    match repository
        .transition(
            session.owner,
            session.id,
            session.revision,
            session.control_fencing_token,
            next,
            now,
        )
        .await
    {
        Ok(mutation) => Ok(mutation.session),
        Err(SessionRepositoryError::RevisionChanged) => repository
            .get_owned(session.owner, session.id)
            .await
            .map_err(map_repository_error)?
            .ok_or_else(session_not_found),
        Err(error) => Err(map_repository_error(error)),
    }
}

async fn session_response(
    state: &AppState,
    repository: &LiveSessionRepository,
    session: &SessionRecord,
    token: Option<&str>,
    now: DateTime<Utc>,
) -> Result<SessionResponse, LiveHttpRejection> {
    let stored = decrypt_descriptor(repository, session).await?;
    let selected = stored.selected().ok_or_else(contract_invalid)?;
    let available_sources = source_choices(state, session, &stored, now).await?;
    let selected_source = available_sources
        .get(stored.selected_source_index)
        .cloned()
        .ok_or_else(contract_invalid)?;
    let track_preferences = track_preferences(repository, session).await?;
    let effective_egress = stored
        .egress
        .to_effective()
        .map_err(|_| contract_invalid())?;
    let egress_outcome = if effective_egress.protected() {
        state
            .live
            .egress_service()
            .ok_or_else(egress_unavailable)?
            .outcome_for(session, &effective_egress)
            .map_err(|_| egress_unavailable())?
    } else {
        LiveEgressOutcome::ServerDefault
    };
    let server_delivery = session.delivery_mode != DeliveryMode::ClientDirect;
    let playback_url = match session.delivery_mode {
        DeliveryMode::ClientDirect => stored
            .playback_url
            .clone()
            .ok_or_else(delivery_runtime_unavailable_error)?,
        DeliveryMode::ServerRelay if session.protocol == SessionProtocol::Hls => format!(
            "/api/v1/live/sessions/{}/delivery/hls/manifest.m3u8",
            session.id
        ),
        DeliveryMode::ServerRelay
            if matches!(
                session.protocol,
                SessionProtocol::HttpProgressive | SessionProtocol::MpegTs
            ) =>
        {
            format!("/api/v1/live/sessions/{}/delivery/stream", session.id)
        }
        DeliveryMode::ServerRemux => format!(
            "/api/v1/live/sessions/{}/delivery/hls/manifest.m3u8",
            session.id
        ),
        DeliveryMode::ServerRelay => {
            return Err(delivery_runtime_unavailable_error());
        }
    };
    Ok(SessionResponse {
        session_id: session.id,
        revision: session.revision,
        token_revision: server_delivery.then_some(session.token_revision),
        delivery_mode: session.delivery_mode.as_str(),
        decision_reason: stored.decision_reason.clone(),
        egress: SessionEgressDto {
            mode: egress_outcome.as_str(),
            fallback_reason: (egress_outcome == LiveEgressOutcome::DirectFallback)
                .then_some("protected_egress_unavailable"),
        },
        playback_url,
        session_token: if server_delivery {
            Some(token.ok_or_else(internal_error)?.to_string())
        } else {
            None
        },
        expires_at: timestamp(session.expires_at),
        hard_expires_at: timestamp(session.hard_expires_at),
        heartbeat_interval_seconds: heartbeat_interval(state),
        live: LiveWindowDto {
            seekable: selected.time_shift_available,
            window_seconds: selected.time_shift_window_seconds,
            target_latency_seconds: None,
        },
        selected_source,
        available_sources,
        track_preferences,
    })
}

fn playback_actor(principal: &CurrentPrincipal) -> Result<ActorSnapshot, LiveHttpRejection> {
    ActorSnapshot::new(
        principal.user_id,
        principal.profile_display_name.clone(),
        principal.role,
    )
    .map_err(|_| internal_error())
}

async fn refresh_live_gauges(state: &AppState) {
    if let Err(error) = crate::live::metrics::refresh_database_gauges(&state.db_pool).await {
        tracing::warn!(error = %error, "Live metric gauge reconciliation failed");
    }
}

async fn session_detail(
    state: &AppState,
    repository: &LiveSessionRepository,
    session: &SessionRecord,
    now: DateTime<Utc>,
) -> Result<SessionDetailEnvelope, LiveHttpRejection> {
    let stored = decrypt_descriptor(repository, session).await?;
    require_current_provider(state, session, &stored).await?;
    detail_from_stored(state, session, &stored, now).await
}

async fn require_current_provider(
    state: &AppState,
    session: &SessionRecord,
    stored: &StoredSessionDescriptor,
) -> Result<(), LiveHttpRejection> {
    let provider = state
        .live
        .provider_client()
        .ok_or_else(control_unavailable)?
        .directory()
        .get(session.owner.provider_id)
        .await
        .map_err(|_| provider_unavailable())?;
    if format!("{:?}", provider.revision) != stored.provider_revision {
        return Err(session_conflict());
    }
    Ok(())
}

async fn detail_from_stored(
    state: &AppState,
    session: &SessionRecord,
    stored: &StoredSessionDescriptor,
    now: DateTime<Utc>,
) -> Result<SessionDetailEnvelope, LiveHttpRejection> {
    let available_sources = source_choices(state, session, stored, now).await?;
    let selected_source = available_sources
        .get(stored.selected_source_index)
        .cloned()
        .ok_or_else(contract_invalid)?;
    let repository = state
        .live
        .session_repository()
        .ok_or_else(control_unavailable)?;
    let track_preferences = track_preferences(&repository, session).await?;
    let mut timeline = stored
        .recovery
        .events
        .iter()
        .map(|event| TimelineDto {
            at: timestamp(event.at),
            revision: event.revision,
            state: event.action.state(),
            reason: Some(format!(
                "{}:{}",
                event.reason.as_str(),
                event.outcome.as_str()
            )),
        })
        .collect::<Vec<_>>();
    timeline.push(TimelineDto {
        at: timestamp(now),
        revision: session.revision,
        state: session.state.as_str(),
        reason: None,
    });
    Ok(SessionDetailEnvelope {
        data: SessionDetailDto {
            session_id: session.id,
            revision: session.revision,
            state: session.state.as_str(),
            delivery_mode: session.delivery_mode.as_str(),
            protocol: session.protocol.as_str(),
            selected_source,
            available_sources,
            track_preferences,
            expires_at: timestamp(session.expires_at),
            hard_expires_at: timestamp(session.hard_expires_at),
            error_code: session.error_code.clone(),
            timeline,
        },
        meta: SessionMetaDto {
            request_id: Uuid::new_v4(),
            generated_at: timestamp(now),
            cache_state: "none",
            partial: false,
        },
        errors: Vec::new(),
    })
}

async fn source_choices(
    state: &AppState,
    session: &SessionRecord,
    stored: &StoredSessionDescriptor,
    now: DateTime<Utc>,
) -> Result<Vec<SelectedSourceDto>, LiveHttpRejection> {
    if stored.sources.is_empty() || stored.sources.len() > 8 {
        return Err(contract_invalid());
    }
    let mut choices = Vec::with_capacity(stored.sources.len());
    for source in &stored.sources {
        choices.push(SelectedSourceDto {
            source_key: seal_source_key(state, session, stored, source, now).await?,
            label: source.label.clone(),
            quality: source.quality.clone(),
        });
    }
    Ok(choices)
}

async fn track_preferences(
    repository: &LiveSessionRepository,
    session: &SessionRecord,
) -> Result<TrackPreferencesDto, LiveHttpRejection> {
    let preferences = repository
        .track_preferences(session.owner.user_id, session.owner.provider_id)
        .await
        .map_err(map_repository_error)?;
    Ok(TrackPreferencesDto {
        audio: preferences
            .as_ref()
            .and_then(|value| value.audio.as_ref())
            .map(track_selection_dto),
        subtitle: preferences
            .as_ref()
            .and_then(|value| value.subtitle.as_ref())
            .map(track_selection_dto),
    })
}

fn track_selection_dto(selection: &LiveTrackSelection) -> TrackSelectionDto {
    TrackSelectionDto {
        track_id: selection.track_id.clone(),
        language: selection.language.clone(),
        title: selection.title.clone(),
    }
}

async fn decrypt_descriptor(
    repository: &LiveSessionRepository,
    session: &SessionRecord,
) -> Result<StoredSessionDescriptor, LiveHttpRejection> {
    let secrets = repository
        .decrypt_secrets(session.owner, session.id)
        .await
        .map_err(map_repository_error)?;
    serde_json::from_slice(secrets.descriptor.expose_secret()).map_err(|_| internal_error())
}

async fn seal_source_key(
    state: &AppState,
    session: &SessionRecord,
    stored: &StoredSessionDescriptor,
    selected: &StoredSource,
    now: DateTime<Utc>,
) -> Result<String, LiveHttpRejection> {
    let crypto = state.live.crypto().await.ok_or_else(control_unavailable)?;
    let key_expiry =
        (now + chrono::Duration::seconds(SOURCE_KEY_MAX_SECONDS)).min(session.hard_expires_at);
    LivePublicKeyCodec::new(crypto)
        .seal_source(
            session.id,
            session.owner.provider_id,
            &stored.provider_revision,
            &selected.stream_id,
            session.revision,
            key_expiry,
        )
        .map_err(|_| internal_error())
}

async fn load_policy(
    state: &AppState,
    home_id: Uuid,
    provider_id: Uuid,
) -> Result<LoadedPolicy, LiveHttpRejection> {
    let rows = sqlx::query(
        "SELECT scheme, normalized_host, port, exact_path, network_scope,
                CAST(CASE WHEN allow_fetch THEN 1 ELSE 0 END AS BIGINT) AS allow_fetch,
                CAST(CASE WHEN allow_client_disclosure THEN 1 ELSE 0 END AS BIGINT)
                    AS allow_client_disclosure
         FROM live_provider_destination_rules
         WHERE home_id = $1 AND provider_id = $2
         ORDER BY scheme, normalized_host, port, exact_path, network_scope",
    )
    .bind(home_id.to_string())
    .bind(provider_id.to_string())
    .fetch_all(&state.db_pool)
    .await
    .map_err(|_| internal_error())?;
    if rows.len() > MAX_POLICY_ROWS {
        return Err(internal_error());
    }
    let mut planner_rules = Vec::new();
    let mut preflight_rules = Vec::new();
    for row in rows {
        let scheme: String = row.try_get("scheme").map_err(|_| internal_error())?;
        let host: String = row
            .try_get("normalized_host")
            .map_err(|_| internal_error())?;
        let port = u16::try_from(
            row.try_get::<i64, _>("port")
                .map_err(|_| internal_error())?,
        )
        .map_err(|_| internal_error())?;
        let path: String = row.try_get("exact_path").map_err(|_| internal_error())?;
        let network_scope: String = row.try_get("network_scope").map_err(|_| internal_error())?;
        let allow_fetch: i64 = row.try_get("allow_fetch").map_err(|_| internal_error())?;
        let allow_disclosure: i64 = row
            .try_get("allow_client_disclosure")
            .map_err(|_| internal_error())?;
        let public = network_scope == "public";
        if allow_disclosure == 1 {
            #[cfg(test)]
            let rule = if state.live.allows_test_live_sources() {
                DirectDisclosureRule::new_for_test_loopback(
                    &scheme, &host, port, &path, public, true,
                )
            } else {
                DirectDisclosureRule::new(&scheme, &host, port, &path, public, true)
            };
            #[cfg(not(test))]
            let rule = DirectDisclosureRule::new(&scheme, &host, port, &path, public, true);
            if let Ok(rule) = rule {
                planner_rules.push(rule);
            }
        }
        if allow_fetch == 1 && public {
            if let Ok(rule) =
                DestinationRule::new(&scheme, &host, port, &path, NetworkScope::Public, true)
            {
                preflight_rules.push(rule);
            }
        }
    }
    Ok(LoadedPolicy {
        planner_rules,
        preflight_rules,
    })
}

async fn planner_policy(
    state: &AppState,
    provider: &crate::live::provider::LiveProviderSnapshot,
    loaded: &LoadedPolicy,
) -> PlannerPolicy {
    let snapshot = state.live.snapshot().await;
    let ready = |component: LiveComponent| {
        snapshot
            .features
            .iter()
            .any(|feature| feature.flag == component.flag_name() && feature.effective_enabled)
    };
    PlannerPolicy {
        client_direct_enabled: ready(LiveComponent::ClientDirect),
        relay_enabled: ready(LiveComponent::Relay),
        remux_enabled: ready(LiveComponent::Remux),
        relay_capacity_available: state
            .live
            .relay_service()
            .is_some_and(|relay| relay.available_capacity() > 0),
        remux_capacity_available: state
            .live
            .remux_service()
            .is_some_and(|remux| remux.available_capacity() > 0),
        protected_egress_mode: EgressPolicyMode::Off,
        protected_egress_ready: ready(LiveComponent::ProtectedEgress),
        allow_private_lan_sources: ready(LiveComponent::PrivateLanSources),
        provider_private_network_permission: provider.permits_private_network(),
        native_dash_relay_enabled: ready(LiveComponent::NativeDashRelay),
        rtmp_remux_enabled: ready(LiveComponent::RtmpRemux),
        srt_remux_enabled: ready(LiveComponent::SrtRemux),
        disclosure_rules: loaded.planner_rules.clone(),
    }
}

async fn preflight_direct(
    source: &SourceDescriptor,
    rules: Vec<DestinationRule>,
    allow_test_source: bool,
) -> Result<(), LiveHttpRejection> {
    #[cfg(test)]
    if allow_test_source {
        return Ok(());
    }
    #[cfg(not(test))]
    let _ = allow_test_source;
    if rules.is_empty() {
        return Err(upstream_rejected());
    }
    let resolver = SystemDnsResolver::new(Duration::from_secs(5))
        .map_err(|_| delivery_runtime_unavailable_error())?;
    let fetcher = UpstreamFetcher::new(
        Arc::new(resolver),
        UpstreamLimits {
            connect_timeout: Duration::from_secs(5),
            header_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(10),
            max_response_bytes: 1_024,
            max_response_headers: 64,
            max_response_header_bytes: 32 * 1_024,
            max_redirects: 5,
        },
    )
    .map_err(|_| delivery_runtime_unavailable_error())?;
    let policy = DestinationPolicy::new(
        rules,
        PrivateLanGate::default(),
        false,
        LocalDestinationDenylist::default(),
    )
    .map_err(|_| upstream_rejected())?;
    let response = fetcher
        .fetch(FetchRequest::new(
            source.url.clone(),
            UpstreamMethod::Head,
            policy,
            CancellationToken::new(),
        ))
        .await
        .map_err(|_| upstream_rejected())?;
    if !response.status().is_success() || response.stats().redirects() != 0 {
        return Err(upstream_rejected());
    }
    Ok(())
}

fn planner_capabilities(
    request: &ClientCapabilityRequest,
) -> Result<ClientCapabilities, LiveHttpRejection> {
    let protocols = request.protocols.iter().copied().collect::<BTreeSet<_>>();
    let video_codecs = normalize_capabilities(&request.video_codecs)?;
    let audio_codecs = normalize_capabilities(&request.audio_codecs)?;
    Ok(ClientCapabilities {
        protocols,
        containers: BTreeSet::from([
            "flv".to_string(),
            "matroska".to_string(),
            "mp4".to_string(),
            "mpegts".to_string(),
            "webm".to_string(),
        ]),
        video_codecs,
        audio_codecs,
    })
}

fn normalize_capabilities(values: &[String]) -> Result<BTreeSet<String>, LiveHttpRejection> {
    if values.len() > 32 {
        return Err(LiveHttpRejection::invalid_request());
    }
    let normalized = values
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if normalized.len() != values.len()
        || normalized.iter().any(|value| {
            value.is_empty()
                || value.len() > 64
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(normalized)
}

fn validate_create_request(request: &SessionCreateRequest) -> Result<(), LiveHttpRejection> {
    if request.provider_id.is_nil()
        || !(16..=2_048).contains(&request.item_key.len())
        || !(16..=2_048).contains(&request.stream_option_key.len())
        || request.client.player != "mpv"
        || request.client.protocols.is_empty()
        || request.client.protocols.len() > 6
        || request
            .client
            .protocols
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != request.client.protocols.len()
        || request.egress_policy_id.is_some()
            && !matches!(
                request.egress_mode,
                EgressModeRequest::PreferProtected | EgressModeRequest::RequireProtected
            )
    {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(())
}

fn validate_heartbeat(request: &HeartbeatRequest) -> Result<(), LiveHttpRejection> {
    let now = Utc::now();
    let observed_at = request
        .observed_at
        .strip_suffix('Z')
        .and_then(|_| DateTime::parse_from_rfc3339(&request.observed_at).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or_else(LiveHttpRejection::invalid_request)?;
    if request.expected_revision < 1
        || observed_at < now - chrono::Duration::seconds(MAX_OBSERVED_SKEW_SECONDS)
        || observed_at > now + chrono::Duration::seconds(30)
        || request
            .distance_from_live_edge_seconds
            .is_some_and(|value| !value.is_finite() || !(0.0..=86_400.0).contains(&value))
        || request
            .source_key
            .as_ref()
            .is_some_and(|value| !(16..=2_048).contains(&value.len()))
        || request.audio_track_id.as_ref().is_some_and(|_| {
            !valid_observed_track(
                request.audio_track_id.as_deref(),
                request.audio_track_language.as_deref(),
                request.audio_track_title.as_deref(),
            )
        })
        || request.audio_track_id.is_none()
            && (request.audio_track_language.is_some() || request.audio_track_title.is_some())
        || request.subtitle_track_id.as_ref().is_some_and(|_| {
            !valid_observed_track(
                request.subtitle_track_id.as_deref(),
                request.subtitle_track_language.as_deref(),
                request.subtitle_track_title.as_deref(),
            )
        })
        || request.subtitle_track_id.is_none()
            && (request.subtitle_track_language.is_some() || request.subtitle_track_title.is_some())
    {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(())
}

fn valid_observed_track(
    track_id: Option<&str>,
    language: Option<&str>,
    title: Option<&str>,
) -> bool {
    track_id.is_some_and(|value| valid_track_text(value, 256))
        && language.is_none_or(valid_track_language)
        && title.is_none_or(|value| valid_track_text(value, 256))
}

fn valid_track_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_track_language(value: &str) -> bool {
    let normalized = value.replace('_', "-");
    !normalized.is_empty()
        && normalized.len() <= 64
        && normalized.split('-').all(|part| {
            (1..=8).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
}

fn track_preference_update(
    request: &HeartbeatRequest,
) -> Result<Option<LiveTrackPreferenceUpdate>, LiveHttpRejection> {
    let selection = |track_id: &Option<String>,
                     language: &Option<String>,
                     title: &Option<String>|
     -> Option<LiveTrackSelection> {
        track_id.as_ref().map(|track_id| LiveTrackSelection {
            track_id: track_id.clone(),
            language: language
                .as_ref()
                .map(|value| value.replace('_', "-").to_ascii_lowercase()),
            title: title.clone(),
        })
    };
    let update = LiveTrackPreferenceUpdate {
        audio: selection(
            &request.audio_track_id,
            &request.audio_track_language,
            &request.audio_track_title,
        ),
        subtitle: selection(
            &request.subtitle_track_id,
            &request.subtitle_track_language,
            &request.subtitle_track_title,
        ),
    };
    Ok((!update.is_empty()).then_some(update))
}

async fn select_session_egress_policy(
    state: &AppState,
    principal: &CurrentPrincipal,
    request: &SessionCreateRequest,
) -> Result<EffectiveEgressPolicy, LiveHttpRejection> {
    state
        .live
        .refresh_builtin_egress()
        .await
        .map_err(|_| egress_unavailable())?;
    let requested = match request.egress_mode {
        EgressModeRequest::Inherit => None,
        EgressModeRequest::Off => Some(SessionEgressPolicyRequest {
            mode: EgressPolicyMode::Off,
            policy_id: None,
            allow_fallback: false,
        }),
        EgressModeRequest::PreferProtected => Some(SessionEgressPolicyRequest {
            mode: EgressPolicyMode::PreferProtected,
            policy_id: request.egress_policy_id.map(|value| value.to_string()),
            allow_fallback: false,
        }),
        EgressModeRequest::RequireProtected => Some(SessionEgressPolicyRequest {
            mode: EgressPolicyMode::RequireProtected,
            policy_id: request.egress_policy_id.map(|value| value.to_string()),
            allow_fallback: false,
        }),
    };
    let Some(egress) = state.live.egress_service() else {
        if request.egress_policy_id.is_some()
            || matches!(
                request.egress_mode,
                EgressModeRequest::PreferProtected | EgressModeRequest::RequireProtected
            )
        {
            return Err(egress_unavailable());
        }
        return Ok(EffectiveEgressPolicy {
            mode: EgressPolicyMode::Off,
            policy_id: None,
            allow_fallback: false,
            revision: 1,
            source: if matches!(request.egress_mode, EgressModeRequest::Off) {
                EgressPolicySource::Session
            } else {
                EgressPolicySource::ServerConfig
            },
        });
    };
    egress
        .select_policy(
            principal.home_id,
            principal.profile_id,
            request.provider_id,
            requested,
            request.egress_policy_id.is_some(),
        )
        .await
        .map_err(|_| egress_unavailable())
}

fn requested_egress_mode(requested: EgressModeRequest) -> RequestedEgressMode {
    match requested {
        EgressModeRequest::Inherit => RequestedEgressMode::Inherit,
        EgressModeRequest::Off => RequestedEgressMode::Off,
        EgressModeRequest::PreferProtected => RequestedEgressMode::PreferProtected,
        EgressModeRequest::RequireProtected => RequestedEgressMode::RequireProtected,
    }
}

fn request_identity(
    request: &SessionCreateRequest,
    principal: &CurrentPrincipal,
    provider_revision: &str,
) -> Result<Vec<u8>, LiveHttpRejection> {
    serde_json::to_vec(&json!({
        "contractVersion": "live_api_v1",
        "request": request,
        "principal": {
            "userId": principal.user_id,
            "homeId": principal.home_id,
            "profileId": principal.profile_id,
            "accountSessionId": principal.account_session_id,
            "role": format!("{:?}", principal.role).to_ascii_lowercase(),
            "capabilityRevision": principal.capability_revision,
        },
        "providerRevision": provider_revision,
    }))
    .map_err(|_| internal_error())
}

pub(super) fn validate_mutation_transport(
    state: &AppState,
    principal: &CurrentPrincipal,
    headers: &HeaderMap,
) -> Result<(), LiveHttpRejection> {
    if principal.transport == AccountAuthTransport::Bearer {
        return Ok(());
    }
    if principal.transport != AccountAuthTransport::Cookie {
        return Err(LiveHttpRejection::auth_required());
    }
    let origin = one_header(headers, ORIGIN_HEADER).ok_or_else(csrf_required)?;
    let csrf = one_header(headers, CSRF_HEADER).ok_or_else(csrf_required)?;
    let scheme = if state.settings.environment == RunEnvironment::Development {
        "http"
    } else {
        "https"
    };
    let host = state.settings.server.host.trim();
    if host.is_empty() || matches!(host, "0.0.0.0" | "::" | "[::]") {
        return Err(csrf_forbidden());
    }
    let expected_origin = format!("{scheme}://{host}:{}", state.settings.server.port);
    if normalize_origin(&origin).as_deref() != normalize_origin(&expected_origin).as_deref() {
        return Err(csrf_forbidden());
    }
    let expected = state
        .auth_service
        .csrf_token(principal.account_session_id, principal.csrf_revision)
        .map_err(|_| csrf_forbidden())?;
    if !constant_time_equal(csrf.as_bytes(), expected.expose_secret().as_bytes()) {
        return Err(csrf_forbidden());
    }
    Ok(())
}

fn normalize_origin(value: &str) -> Option<String> {
    let url = reqwest::Url::parse(value).ok()?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return None;
    }
    Some(format!(
        "{}://{}:{}",
        url.scheme(),
        url.host_str()?.trim_end_matches('.').to_ascii_lowercase(),
        url.port_or_known_default()?
    ))
}

fn one_header(headers: &HeaderMap, name: &str) -> Option<String> {
    let values = headers.get_all(name).iter().collect::<Vec<_>>();
    if values.len() != 1 {
        return None;
    }
    values[0].to_str().ok().map(str::to_string)
}

fn required_visible_header(
    headers: &HeaderMap,
    name: &str,
    min: usize,
    max: usize,
) -> Result<String, LiveHttpRejection> {
    let value = one_header(headers, name).ok_or_else(LiveHttpRejection::invalid_request)?;
    if !(min..=max).contains(&value.len()) || !value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
    {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(value)
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn source_at(resolved: &ResolvedSources, index: usize) -> Option<&SourceDescriptor> {
    if index == 0 {
        Some(&resolved.descriptor)
    } else {
        resolved.alternatives.get(index - 1)
    }
}

fn session_protocol(protocol: StreamProtocol) -> SessionProtocol {
    match protocol {
        StreamProtocol::Hls => SessionProtocol::Hls,
        StreamProtocol::Dash => SessionProtocol::Dash,
        StreamProtocol::HttpProgressive => SessionProtocol::HttpProgressive,
        StreamProtocol::MpegTs => SessionProtocol::MpegTs,
        StreamProtocol::Rtmp => SessionProtocol::Rtmp,
        StreamProtocol::Srt => SessionProtocol::Srt,
    }
}

fn heartbeat_interval(state: &AppState) -> u64 {
    (state.live.config().sessions.lease_seconds / 3).clamp(10, 60)
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn token_response(status: StatusCode, value: SessionResponse) -> Response {
    no_store((status, Json(value)).into_response())
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, "no-store".parse().expect("static header"));
    response
}

fn map_repository_error(error: SessionRepositoryError) -> LiveHttpRejection {
    if matches!(&error, SessionRepositoryError::Capacity) {
        crate::live::metrics::ADMISSION_REJECTIONS
            .with_label_values(&["session", "capacity_exhausted"])
            .inc();
    }
    match error {
        SessionRepositoryError::InvalidInput => LiveHttpRejection::invalid_request(),
        SessionRepositoryError::OwnerUnavailable => LiveHttpRejection::new(
            StatusCode::FORBIDDEN,
            "LIVE_SESSION_FORBIDDEN",
            "The Live session owner is no longer authorized.",
            false,
        ),
        SessionRepositoryError::Capacity => LiveHttpRejection::new(
            StatusCode::TOO_MANY_REQUESTS,
            "LIVE_RELAY_CAPACITY",
            "Live session capacity is exhausted.",
            true,
        ),
        SessionRepositoryError::IdempotencyConflict => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_IDEMPOTENCY_CONFLICT",
            "The idempotency key was already used for another request.",
            false,
        ),
        SessionRepositoryError::NotFound => session_not_found(),
        SessionRepositoryError::RevisionChanged | SessionRepositoryError::InvalidTransition => {
            session_conflict()
        }
        SessionRepositoryError::FenceLost => control_unavailable(),
        SessionRepositoryError::Expired => session_expired(),
        SessionRepositoryError::InvalidState
        | SessionRepositoryError::Crypto(_)
        | SessionRepositoryError::Storage(_) => internal_error(),
    }
}

fn map_provider_error(error: crate::live::provider::ProviderInvocationError) -> LiveHttpRejection {
    use crate::live::provider::ProviderInvocationError;
    match error {
        ProviderInvocationError::RequestTimeout | ProviderInvocationError::HardTimeout => {
            LiveHttpRejection::new(
                StatusCode::GATEWAY_TIMEOUT,
                "LIVE_PROVIDER_TIMEOUT",
                "The Live provider timed out.",
                true,
            )
        }
        ProviderInvocationError::InvalidRequest(_) | ProviderInvocationError::Contract(_) => {
            contract_invalid()
        }
        ProviderInvocationError::Cancelled => provider_unavailable(),
        _ => provider_unavailable(),
    }
}

fn map_planner_error(error: crate::live::planner::PlannerRejection) -> LiveHttpRejection {
    match error.code {
        PlannerRejectionCode::RelayCapacity => crate::live::metrics::ADMISSION_REJECTIONS
            .with_label_values(&["relay", "capacity_exhausted"])
            .inc(),
        PlannerRejectionCode::RemuxCapacity => crate::live::metrics::ADMISSION_REJECTIONS
            .with_label_values(&["remux", "capacity_exhausted"])
            .inc(),
        PlannerRejectionCode::ProtectedEgressUnavailable => {
            crate::live::metrics::ADMISSION_REJECTIONS
                .with_label_values(&["egress", "unavailable"])
                .inc()
        }
        _ => {}
    }
    match error.code {
        PlannerRejectionCode::DescriptorExpired => stream_expired(),
        PlannerRejectionCode::RelayCapacity => LiveHttpRejection::new(
            StatusCode::TOO_MANY_REQUESTS,
            "LIVE_RELAY_CAPACITY",
            "Live relay capacity is exhausted.",
            true,
        ),
        PlannerRejectionCode::RemuxCapacity => LiveHttpRejection::new(
            StatusCode::TOO_MANY_REQUESTS,
            "LIVE_REMUX_CAPACITY",
            "Live remux capacity is exhausted.",
            true,
        ),
        PlannerRejectionCode::ProtectedEgressUnavailable => LiveHttpRejection::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIVE_EGRESS_UNAVAILABLE",
            "Protected Live egress is unavailable.",
            true,
        ),
        PlannerRejectionCode::ClientProtocolUnsupported
        | PlannerRejectionCode::ClientCodecUnsupported
        | PlannerRejectionCode::ClientContainerUnsupported
        | PlannerRejectionCode::RemuxProfileUnsupported => LiveHttpRejection::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "LIVE_PROTOCOL_UNSUPPORTED",
            "The Live stream is not compatible with this client.",
            false,
        ),
        PlannerRejectionCode::PrivateNetworkForbidden => upstream_rejected(),
        PlannerRejectionCode::InvalidInput
        | PlannerRejectionCode::MalformedDescriptor
        | PlannerRejectionCode::ProtocolMismatch => contract_invalid(),
        PlannerRejectionCode::TimeShiftUnavailable
        | PlannerRejectionCode::RelayDisabled
        | PlannerRejectionCode::RemuxDisabled => delivery_runtime_unavailable_error(),
    }
}

fn stream_expired() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::GONE,
        "LIVE_STREAM_EXPIRED",
        "The Live stream descriptor expired.",
        false,
    )
}

fn delivery_runtime_unavailable(mode: DeliveryMode) -> LiveHttpRejection {
    let (code, message) = match mode {
        DeliveryMode::ServerRelay => (
            "LIVE_RELAY_CAPACITY",
            "The Live relay runtime is not ready.",
        ),
        DeliveryMode::ServerRemux => (
            "LIVE_REMUX_CAPACITY",
            "The Live remux runtime is not ready.",
        ),
        DeliveryMode::ClientDirect => (
            "LIVE_PROVIDER_UNAVAILABLE",
            "The Live playback runtime is unavailable.",
        ),
    };
    LiveHttpRejection::new(StatusCode::SERVICE_UNAVAILABLE, code, message, true)
}

fn egress_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_EGRESS_UNAVAILABLE",
        "Protected Live egress is unavailable.",
        true,
    )
}

fn control_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_CONTROL_LEASE_UNAVAILABLE",
        "The Live control service is unavailable.",
        true,
    )
}

fn cleanup_incomplete() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_CLEANUP_INCOMPLETE",
        "The Live session ended, but delivery cleanup is incomplete.",
        true,
    )
}

fn delivery_runtime_unavailable_error() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_PROVIDER_UNAVAILABLE",
        "The Live delivery runtime is unavailable.",
        true,
    )
}

fn provider_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::BAD_GATEWAY,
        "LIVE_PROVIDER_UNAVAILABLE",
        "The Live provider is unavailable.",
        true,
    )
}

fn upstream_rejected() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::BAD_GATEWAY,
        "LIVE_UPSTREAM_REJECTED",
        "The Live upstream source failed security preflight.",
        false,
    )
}

fn contract_invalid() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::BAD_GATEWAY,
        "LIVE_CONTRACT_INVALID",
        "The Live provider returned an invalid stream contract.",
        false,
    )
}

fn invalid_stream_key() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::NOT_FOUND,
        "LIVE_STREAM_UNAVAILABLE",
        "The Live stream choice is unavailable.",
        false,
    )
}

fn invalid_source_key() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::CONFLICT,
        "LIVE_SESSION_CONFLICT",
        "The Live source selection is stale.",
        false,
    )
}

fn stream_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "LIVE_STREAM_UNAVAILABLE",
        "The requested Live source is not currently eligible.",
        false,
    )
}

fn recovery_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::BAD_GATEWAY,
        "LIVE_STREAM_UNAVAILABLE",
        "The current Live source cannot be refreshed.",
        false,
    )
}

fn recovery_exhausted() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_FAILOVER_EXHAUSTED",
        "The bounded Live recovery policy is exhausted.",
        false,
    )
}

fn session_not_found() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::NOT_FOUND,
        "LIVE_SESSION_NOT_FOUND",
        "The Live session was not found.",
        false,
    )
}

fn session_conflict() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::CONFLICT,
        "LIVE_SESSION_CONFLICT",
        "The Live session changed before this request completed.",
        false,
    )
}

fn session_expired() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::GONE,
        "LIVE_SESSION_EXPIRED",
        "The Live session has ended or expired.",
        false,
    )
}

fn csrf_required() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::FORBIDDEN,
        "LIVE_CSRF_REQUIRED",
        "Cookie-authenticated Live mutations require origin and CSRF proof.",
        false,
    )
}

fn csrf_forbidden() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::FORBIDDEN,
        "LIVE_CSRF_REQUIRED",
        "The Live mutation origin or CSRF proof is invalid.",
        false,
    )
}

fn internal_error() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "LIVE_INTERNAL_ERROR",
        "The Live request could not be completed.",
        false,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        process::Command as StdCommand,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration as StdDuration, Instant},
    };

    use anyhow::Result;
    use axum::{
        Router,
        body::{self, Body},
        http::Request,
        routing::post,
    };
    use base64::{Engine as _, engine::general_purpose};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::Value;
    use tokio::{
        net::TcpListener,
        process::{Child, Command as TokioCommand},
        sync::oneshot,
        task::JoinHandle,
    };
    use tower::ServiceExt;

    use super::*;

    use crate::{
        artwork::ArtworkService,
        auth::AuthService,
        config::{
            AuthConfig, ClassifierConfig, DatabaseConfig, LibraryConfig, MediaInteractionsConfig,
            RunEnvironment, SecretsConfig, ServerConfig, Settings, TelemetryConfig,
        },
        db::Database,
        extensions::ExtensionManager,
        http::{handlers::live::delivery, router},
        library::LinkerService,
        live::{
            catalog::{LivePublicKeyCodec, LivePublicKeyScope},
            config::LiveConfig,
            crypto::SecretBytes,
            provider::tests::seed_provider,
            service::{LiveComponent, LiveService},
            session::{DeliveryMode, NewSession, SessionProtocol},
        },
        metadata::MetadataService,
        secrets::SecretsManager,
        state::AppState,
    };

    struct DirectProviderFixture {
        port: u16,
        calls: Arc<AtomicUsize>,
        shutdown: Option<oneshot::Sender<()>>,
        task: JoinHandle<()>,
    }

    struct DirectOriginFixture {
        temporary: tempfile::TempDir,
        child: Child,
        port: u16,
    }

    #[derive(Clone)]
    struct RelayOriginState {
        master_calls: Arc<AtomicUsize>,
        media_calls: Arc<AtomicUsize>,
        key_calls: Arc<AtomicUsize>,
        segment_calls: Arc<AtomicUsize>,
    }

    struct RelayOriginFixture {
        port: u16,
        state: RelayOriginState,
        shutdown: Option<oneshot::Sender<()>>,
        task: JoinHandle<()>,
    }

    #[test]
    fn direct_transport_recovery_escalates_without_overriding_source_selection() {
        assert!(should_replan_direct_through_server(
            DeliveryMode::ClientDirect,
            RecoveryReason::Transport,
            false,
        ));
        assert!(should_replan_direct_through_server(
            DeliveryMode::ClientDirect,
            RecoveryReason::Stalled,
            false,
        ));
        assert!(!should_replan_direct_through_server(
            DeliveryMode::ClientDirect,
            RecoveryReason::Transport,
            true,
        ));
        assert!(!should_replan_direct_through_server(
            DeliveryMode::ClientDirect,
            RecoveryReason::UpstreamUnauthorized,
            false,
        ));
        assert!(!should_replan_direct_through_server(
            DeliveryMode::ServerRelay,
            RecoveryReason::Transport,
            false,
        ));
    }

    impl DirectOriginFixture {
        async fn start() -> Result<Self> {
            Self::start_with_transport(true).await
        }

        async fn start_http() -> Result<Self> {
            Self::start_with_transport(false).await
        }

        async fn start_with_transport(tls: bool) -> Result<Self> {
            let temporary = tempfile::tempdir()?;
            let media = temporary.path().join("media");
            let certificates = temporary.path().join("certificates");
            let ready = temporary.path().join("origin-ready.json");
            let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
            let origin = repository.join("fixtures/live/origin-suite");
            let generated = TokioCommand::new("python3")
                .arg(origin.join("scripts/generate_synthetic_media.py"))
                .arg(&media)
                .arg("--package")
                .status()
                .await?;
            anyhow::ensure!(
                generated.success(),
                "synthetic Live media generation failed"
            );
            let certificates_generated = TokioCommand::new("sh")
                .arg(origin.join("scripts/generate_test_certificates.sh"))
                .arg(&certificates)
                .status()
                .await?;
            anyhow::ensure!(
                certificates_generated.success(),
                "Live fixture certificate generation failed"
            );
            let mut command = TokioCommand::new("python3");
            command
                .arg(origin.join("src/origin_server.py"))
                .arg("--host")
                .arg("127.0.0.1")
                .arg("--port")
                .arg("0")
                .arg("--ready-file")
                .arg(&ready)
                .arg("--media-root")
                .arg(&media);
            if tls {
                command
                    .arg("--tls-cert")
                    .arg(certificates.join("valid.crt"))
                    .arg("--tls-key")
                    .arg(certificates.join("valid.key"));
            }
            let child = command.kill_on_drop(true).spawn()?;
            let deadline = Instant::now() + StdDuration::from_secs(5);
            let port = loop {
                if let Ok(bytes) = tokio::fs::read(&ready).await {
                    let value: Value = serde_json::from_slice(&bytes)?;
                    break value["port"]
                        .as_u64()
                        .and_then(|port| u16::try_from(port).ok())
                        .ok_or_else(|| anyhow::anyhow!("invalid Live origin ready port"))?;
                }
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "Live TLS origin did not become ready"
                );
                tokio::time::sleep(StdDuration::from_millis(20)).await;
            };
            Ok(Self {
                temporary,
                child,
                port,
            })
        }

        fn ca_path(&self) -> PathBuf {
            self.temporary.path().join("certificates/ca.crt")
        }

        fn capture_path(&self) -> PathBuf {
            self.temporary.path().join("captures")
        }

        async fn snapshot(&self) -> Result<Value> {
            let response = reqwest::Client::builder()
                .no_proxy()
                .build()?
                .get(format!("http://127.0.0.1:{}/control/state", self.port))
                .send()
                .await?
                .error_for_status()?;
            Ok(response.json().await?)
        }

        async fn stop(mut self) -> Result<()> {
            self.child.kill().await?;
            let _ = self.child.wait().await;
            Ok(())
        }
    }

    impl RelayOriginFixture {
        async fn start() -> Result<Self> {
            let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
            let port = listener.local_addr()?.port();
            let state = RelayOriginState {
                master_calls: Arc::new(AtomicUsize::new(0)),
                media_calls: Arc::new(AtomicUsize::new(0)),
                key_calls: Arc::new(AtomicUsize::new(0)),
                segment_calls: Arc::new(AtomicUsize::new(0)),
            };
            let app = Router::new()
                .route("/master.m3u8", axum::routing::get(relay_master))
                .route("/media.m3u8", axum::routing::get(relay_media))
                .route("/key.bin", axum::routing::get(relay_key))
                .route("/segment.ts", axum::routing::get(relay_segment))
                .with_state(state.clone());
            let (shutdown, receiver) = oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = receiver.await;
                    })
                    .await;
            });
            Ok(Self {
                port,
                state,
                shutdown: Some(shutdown),
                task,
            })
        }

        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            let _ = self.task.await;
        }
    }

    async fn relay_master(State(state): State<RelayOriginState>) -> impl IntoResponse {
        state.master_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        (
            [("content-type", "application/vnd.apple.mpegurl")],
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=900000,CODECS=\"avc1.64001f,mp4a.40.2\"\nmedia.m3u8?sig=ELIXIR_LIVE_CANARY_CHILD\n",
        )
    }

    async fn relay_media(State(state): State<RelayOriginState>) -> impl IntoResponse {
        state.media_calls.fetch_add(1, Ordering::SeqCst);
        (
            [("content-type", "application/vnd.apple.mpegurl")],
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:1\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin?sig=ELIXIR_LIVE_CANARY_KEY\"\n#EXTINF:4.000,\nsegment.ts?sig=ELIXIR_LIVE_CANARY_SEGMENT\n",
        )
    }

    async fn relay_key(State(state): State<RelayOriginState>) -> impl IntoResponse {
        state.key_calls.fetch_add(1, Ordering::SeqCst);
        (
            [("content-type", "application/octet-stream")],
            [0x5a_u8; 16],
        )
    }

    async fn relay_segment(State(state): State<RelayOriginState>) -> impl IntoResponse {
        state.segment_calls.fetch_add(1, Ordering::SeqCst);
        ([("content-type", "video/mp2t")], vec![0x47_u8; 188 * 4])
    }

    #[derive(Serialize)]
    struct ExpiredAccessClaims {
        sub: String,
        sid: String,
        hid: Option<String>,
        pid: Option<String>,
        role: Option<String>,
        exp: usize,
        iat: usize,
    }

    impl DirectProviderFixture {
        async fn start() -> Result<Self> {
            Self::start_with_origin(None, false, false, false, false).await
        }

        async fn start_for_origin(port: u16) -> Result<Self> {
            Self::start_with_origin(Some(port), false, false, false, false).await
        }

        async fn start_for_relay(port: u16) -> Result<Self> {
            Self::start_with_origin(Some(port), true, false, false, false).await
        }

        async fn start_for_relay_media(port: u16) -> Result<Self> {
            Self::start_with_origin(Some(port), true, true, false, false).await
        }

        async fn start_for_recovery(port: u16) -> Result<Self> {
            Self::start_with_origin(Some(port), true, false, true, false).await
        }

        async fn start_for_remux(port: u16) -> Result<Self> {
            Self::start_with_origin(Some(port), false, false, false, true).await
        }

        async fn start_with_origin(
            origin_port: Option<u16>,
            relay_origin: bool,
            relay_media: bool,
            recovery: bool,
            remux_transport: bool,
        ) -> Result<Self> {
            let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
            let port = listener.local_addr()?.port();
            let calls = Arc::new(AtomicUsize::new(0));
            let route_calls = Arc::clone(&calls);
            let app = Router::new().route(
                "/v1/live/resolve",
                post(move |Json(request): Json<Value>| {
                    let route_calls = Arc::clone(&route_calls);
                    async move {
                        route_calls.fetch_add(1, Ordering::SeqCst);
                        let stream_id =
                            request["request"]["streamId"].as_str().unwrap_or("primary");
                        if recovery {
                            let port = origin_port.expect("recovery origin port");
                            return Json(json!({
                                "descriptor": recovery_descriptor(port, stream_id, 1, 100),
                                "alternatives": [
                                    recovery_descriptor(port, "backup", 1, 80),
                                    recovery_descriptor(port, "tertiary", 1, 60)
                                ]
                            }));
                        }
                        let (protocol, url, container, time_shift) = if remux_transport {
                            let port = origin_port.expect("remux origin port");
                            if stream_id == "dash" {
                                (
                                    "dash",
                                    format!(
                                        "http://127.0.0.1:{port}/dash/generated.mpd?sig=ELIXIR_M10_HTTP_CANARY"
                                    ),
                                    "mp4",
                                    json!({"available": false, "windowSeconds": null}),
                                )
                            } else {
                                (
                                    "mpeg_ts",
                                    format!(
                                        "http://127.0.0.1:{port}/transport/live.ts?sig=ELIXIR_M10_HTTP_CANARY"
                                    ),
                                    "mpeg",
                                    json!({"available": false, "windowSeconds": null}),
                                )
                            }
                        } else if relay_origin {
                            let path = if relay_media {
                                "/hls/generated.m3u8"
                            } else {
                                "/master.m3u8"
                            };
                            (
                                "hls",
                                format!(
                                    "http://127.0.0.1:{}{path}?sig=ELIXIR_LIVE_CANARY_ROOT",
                                    origin_port.expect("relay origin port"),
                                ),
                                "mpegts",
                                json!({"available": true, "windowSeconds": 8}),
                            )
                        } else if let Some(origin_port) = origin_port {
                            match stream_id {
                                "dash" => (
                                    "dash",
                                    format!("https://localhost:{origin_port}/dash/generated.mpd"),
                                    "mp4",
                                    json!({"available": false, "windowSeconds": null}),
                                ),
                                "progressive" => (
                                    "http_progressive",
                                    format!(
                                        "https://localhost:{origin_port}/transport/progressive.mp4"
                                    ),
                                    "mp4",
                                    json!({"available": false, "windowSeconds": null}),
                                ),
                                "mpegts" => (
                                    "mpeg_ts",
                                    format!("https://localhost:{origin_port}/transport/live.ts"),
                                    "mpegts",
                                    json!({"available": false, "windowSeconds": null}),
                                ),
                                _ => (
                                    "hls",
                                    format!("https://localhost:{origin_port}/hls/generated.m3u8"),
                                    "mpegts",
                                    json!({"available": true, "windowSeconds": 4}),
                                ),
                            }
                        } else {
                            (
                                "hls",
                                "https://public-direct.example.invalid/live/master.m3u8"
                                    .to_string(),
                                "mpegts",
                                json!({"available": true, "windowSeconds": 1800}),
                            )
                        };
                        let client_disclosure = if relay_origin || remux_transport {
                            "server_only"
                        } else {
                            "public"
                        };
                        let alternatives = if origin_port.is_some()
                            && !relay_origin
                            && !remux_transport
                            && stream_id == "hls"
                        {
                            vec![json!({
                                "streamId": "hls-backup",
                                "label": "Direct Backup",
                                "quality": "720p",
                                "language": "en",
                                "priority": 80,
                                "protocol": "http_progressive",
                                "url": format!(
                                    "https://localhost:{}/transport/progressive.mp4",
                                    origin_port.expect("direct origin port")
                                ),
                                "requestHeaders": {},
                                "cookies": [],
                                "origin": null,
                                "referer": null,
                                "credentialAuthorities": [],
                                "clientDisclosure": "public",
                                "expiresAt": null,
                                "refreshHandle": null,
                                "serverEgress": "not_required",
                                "privateNetwork": false,
                                "drm": {"kind": "none"},
                                "timeShift": {"available": false, "windowSeconds": null},
                                "media": {
                                    "container": "mp4",
                                    "videoCodec": "h264",
                                    "audioCodec": "aac"
                                }
                            })]
                        } else {
                            Vec::new()
                        };
                        Json(json!({
                            "descriptor": {
                                "streamId": stream_id,
                                "label": "Direct Primary",
                                "quality": "1080p",
                                "language": "en",
                                "priority": 100,
                                "protocol": protocol,
                                "url": url,
                                "requestHeaders": {},
                                "cookies": [],
                                "origin": null,
                                "referer": null,
                                "credentialAuthorities": [],
                                "clientDisclosure": client_disclosure,
                                "expiresAt": null,
                                "refreshHandle": null,
                                "serverEgress": "not_required",
                                "privateNetwork": false,
                                "drm": {"kind": "none"},
                                "timeShift": time_shift,
                                "media": {
                                    "container": container,
                                    "videoCodec": "h264",
                                    "audioCodec": "aac"
                                }
                            },
                            "alternatives": alternatives
                        }))
                    }
                }),
            );
            let app = if recovery {
                let refresh_calls = Arc::clone(&calls);
                app.route(
                    "/v1/live/refresh",
                    post(move |Json(request): Json<Value>| {
                        let refresh_calls = Arc::clone(&refresh_calls);
                        async move {
                            refresh_calls.fetch_add(1, Ordering::SeqCst);
                            let stream_id =
                                request["request"]["streamId"].as_str().unwrap_or("primary");
                            let port = origin_port.expect("recovery origin port");
                            let alternatives = ["primary", "backup", "tertiary"]
                                .into_iter()
                                .filter(|candidate| *candidate != stream_id)
                                .enumerate()
                                .map(|(index, candidate)| {
                                    recovery_descriptor(
                                        port,
                                        candidate,
                                        2,
                                        80 - i32::try_from(index).unwrap_or(0) * 20,
                                    )
                                })
                                .collect::<Vec<_>>();
                            Json(json!({
                                "descriptor": recovery_descriptor(port, stream_id, 2, 100),
                                "alternatives": alternatives
                            }))
                        }
                    }),
                )
            } else {
                app
            };
            let (shutdown, receiver) = oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = receiver.await;
                    })
                    .await;
            });
            Ok(Self {
                port,
                calls,
                shutdown: Some(shutdown),
                task,
            })
        }

        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            let _ = self.task.await;
        }
    }

    fn recovery_descriptor(port: u16, stream_id: &str, generation: u8, priority: i32) -> Value {
        json!({
            "streamId": stream_id,
            "label": format!("Recovery {stream_id}"),
            "quality": if stream_id == "primary" { "1080p" } else { "720p" },
            "language": "en",
            "priority": priority,
            "protocol": "hls",
            "url": format!(
                "http://127.0.0.1:{port}/master.m3u8?source={stream_id}&generation={generation}&sig=ELIXIR_R20_CANARY"
            ),
            "requestHeaders": {},
            "cookies": [],
            "origin": null,
            "referer": null,
            "credentialAuthorities": [],
            "clientDisclosure": "server_only",
            "expiresAt": (Utc::now() + chrono::Duration::minutes(30))
                .to_rfc3339_opts(SecondsFormat::Secs, true),
            "refreshHandle": format!("r20-refresh:{stream_id}:{generation}"),
            "serverEgress": "not_required",
            "privateNetwork": false,
            "drm": {"kind": "none"},
            "timeShift": {"available": true, "windowSeconds": 8},
            "media": {
                "container": "mpegts",
                "videoCodec": "h264",
                "audioCodec": "aac"
            }
        })
    }

    #[tokio::test]
    async fn m10_authenticated_mpeg_ts_remux_delivery_is_bounded_and_cleans_up() -> Result<()> {
        let origin = DirectOriginFixture::start_http().await?;
        let fixture = DirectProviderFixture::start_for_remux(origin.port).await?;
        let remux_temporary = tempfile::tempdir()?;
        let remux_root = remux_temporary.path().join("jobs");
        let mut settings = settings();
        settings.live.relay_enabled = true;
        settings.live.remux_enabled = true;
        settings.live.remux.max_concurrent = 1;
        settings.live.remux.temp_root = remux_root.to_string_lossy().into_owned();
        settings.live.remux.segment_seconds = 1;
        settings.live.remux.playlist_segments = 3;
        settings.live.remux.no_output_timeout_seconds = 5;
        settings.live.remux.minimum_free_bytes = 64 * 1_024 * 1_024;
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, fixture.port, json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let remux = state.live.remux_service().expect("remux service");
        let initial_capacity = remux.available_capacity();
        let app = router(state.clone());

        let (status, _, signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({
                    "email": format!("m10-{}@example.invalid", Uuid::new_v4()),
                    "password": "correct horse battery staple"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let access = signup["access_token"].as_str().unwrap().to_string();
        let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
        let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
        let user_id: Uuid =
            sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM homes WHERE id = $1")
                .bind(home_id.to_string())
                .fetch_one(&pool)
                .await?
                .parse()?;
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO live_provider_destination_rules (
                id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                revision, created_by_user_id, created_by_actor_snapshot
             ) VALUES ($1, $2, $3, 'http', '127.0.0.1', $4, '/transport/live.ts',
                       'public', TRUE, FALSE, FALSE, 1, $5, $6)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(i64::from(origin.port))
        .bind(user_id.to_string())
        .bind(json!({"userId": user_id, "role": "owner"}).to_string())
        .execute(&pool)
        .await?;

        let codec = LivePublicKeyCodec::new(state.live.crypto().await.expect("Live crypto"));
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision,
        };
        let now = Utc::now();
        let item_key = codec.seal_item(provider_id, "m10-event", scope, now)?;
        let stream_key = codec.seal_stream(provider_id, "m10-event", "mpegts", scope, now)?;
        let forbidden_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        let account_headers = [
            ("authorization", format!("Bearer {access}")),
            ("idempotency-key", "m10-remux-create-0001".to_string()),
        ];
        let (status, create_headers, created) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &account_headers,
                Some(json!({
                    "providerId": provider_id,
                    "itemKey": item_key,
                    "streamOptionKey": stream_key,
                    "client": {
                        "platform": "macos",
                        "player": "mpv",
                        "protocols": ["hls"],
                        "videoCodecs": ["h264"],
                        "audioCodecs": ["aac"],
                        "supportsRequestHeaders": true,
                        "supportsCookies": true,
                        "supportsLowLatencyHls": false,
                        "supportsOriginTimeShift": false
                    }
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED, "create body: {created}");
        assert_eq!(create_headers[CACHE_CONTROL], "no-store");
        assert_eq!(created["deliveryMode"], "server_remux");
        assert_eq!(created["decisionReason"], "client_protocol_requires_remux");
        assert!(!created.to_string().contains("ELIXIR_M10_HTTP_CANARY"));
        let session_id = Uuid::parse_str(created["sessionId"].as_str().unwrap())?;
        let session_token = created["sessionToken"].as_str().unwrap().to_string();
        let revision = created["revision"].as_i64().unwrap();
        let root_route = created["playbackUrl"].as_str().unwrap().to_string();
        assert_eq!(remux.available_capacity(), initial_capacity - 1);

        let (status, _, unauthenticated) =
            response_json(request(&app, "GET", &root_route, &[], None).await?).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(unauthenticated["errors"][0]["code"], "LIVE_AUTH_REQUIRED");
        let wrong_headers = [("authorization", "Bearer wrong-remux-token".to_string())];
        let (status, _, wrong) =
            response_json(request(&app, "GET", &root_route, &wrong_headers, None).await?).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(wrong["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        let delivery_headers = [
            ("authorization", format!("Bearer {session_token}")),
            ("range", "bytes=0-".to_string()),
        ];
        let (status, headers, manifest) = response_bytes(
            request(&app, "GET", &root_route, &delivery_headers, None).await?,
            1024 * 1024,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[CACHE_CONTROL], "no-store");
        assert_eq!(headers["vary"], "Authorization");
        let manifest = String::from_utf8(manifest)?;
        assert!(!manifest.contains("ELIXIR_M10_HTTP_CANARY"));
        assert!(!manifest.contains("127.0.0.1"));
        let resources = relay_resource_routes(&manifest);
        assert!(
            !resources.is_empty(),
            "rewritten remux manifest: {manifest}"
        );

        let segment_headers = [
            ("authorization", format!("Bearer {session_token}")),
            ("range", "bytes=0-187".to_string()),
        ];
        let (status, headers, segment) = response_bytes(
            request(&app, "GET", &resources[0], &segment_headers, None).await?,
            1024,
        )
        .await?;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(headers[CACHE_CONTROL], "no-store");
        assert!(
            headers["content-range"]
                .to_str()?
                .starts_with("bytes 0-187/")
        );
        assert_eq!(segment.len(), 188);
        assert_eq!(segment[0], 0x47);

        let query_headers = [("authorization", format!("Bearer {session_token}"))];
        let (status, _, query_error) = response_json(
            request(
                &app,
                "GET",
                &format!("{root_route}?token=ELIXIR_M10_HTTP_CANARY"),
                &query_headers,
                None,
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(query_error["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        let end_headers = [("authorization", format!("Bearer {access}"))];
        let ended = request(
            &app,
            "DELETE",
            &format!("/api/v1/live/sessions/{session_id}?expectedRevision={revision}"),
            &end_headers,
            None,
        )
        .await?;
        assert_eq!(ended.status(), StatusCode::NO_CONTENT);
        assert_eq!(remux.available_capacity(), initial_capacity);
        assert_eq!(remux.snapshot().await.active_jobs, 0);
        assert!(
            tokio::fs::read_dir(&remux_root)
                .await?
                .next_entry()
                .await?
                .is_none()
        );
        let plaintext: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_playback_sessions
             WHERE encrypted_descriptor LIKE '%ELIXIR_M10_HTTP_CANARY%'
                OR encrypted_descriptor LIKE '%127.0.0.1%'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(plaintext, 0);
        let forbidden_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_after, forbidden_before);

        fixture.stop().await;
        origin.stop().await?;
        let lease_shutdown = CancellationToken::new();
        lease_shutdown.cancel();
        state.live.clone().run_lease_heartbeat(lease_shutdown).await;
        Ok(())
    }

    #[tokio::test]
    async fn g40_real_qt_mpv_authenticated_hls_relay_has_moving_frames() -> Result<()> {
        let client_binary = std::env::var("ELIXIR_G40_CLIENT_TEST_BINARY")
            .map_err(|_| anyhow::anyhow!("ELIXIR_G40_CLIENT_TEST_BINARY is required"))?;
        anyhow::ensure!(
            Path::new(&client_binary).is_file(),
            "G40 Qt client test binary does not exist"
        );
        let origin = DirectOriginFixture::start_http().await?;
        let fixture = DirectProviderFixture::start_for_relay_media(origin.port).await?;
        let provider_calls = fixture.calls.clone();
        let mut settings = settings();
        settings.live.relay_enabled = true;
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, fixture.port, json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let relay = state.live.relay_service().expect("relay service");
        let initial_capacity = relay.available_capacity();
        let app = router(state.clone());

        let email = format!("g40-{}@example.invalid", Uuid::new_v4());
        let password = "correct horse battery staple";
        let (status, _, signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({"email": email, "password": password})),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "signup body: {signup}");
        let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
        let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
        let user_id: Uuid =
            sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM homes WHERE id = $1")
                .bind(home_id.to_string())
                .fetch_one(&pool)
                .await?
                .parse()?;
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO live_provider_destination_rules (
                id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                revision, created_by_user_id, created_by_actor_snapshot
             ) VALUES ($1, $2, $3, 'http', '127.0.0.1', $4,
                       '/hls/generated.m3u8', 'public', TRUE, FALSE, FALSE, 1, $5, $6)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(i64::from(origin.port))
        .bind(user_id.to_string())
        .bind(json!({"userId": user_id, "role": "owner"}).to_string())
        .execute(&pool)
        .await?;

        let codec = LivePublicKeyCodec::new(state.live.crypto().await.expect("Live crypto"));
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision,
        };
        let now = Utc::now();
        let item_key = codec.seal_item(provider_id, "g40-event", scope, now)?;
        let stream_key = codec.seal_stream(provider_id, "g40-event", "hls", scope, now)?;
        let forbidden_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;

        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let server_url = format!("http://{address}");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
                .await
        });
        let ca_path = origin.ca_path();
        let capture_path = origin.capture_path();
        let streams_json = serde_json::to_string(&vec![json!({
            "id": "hls",
            "key": stream_key,
        })])?;
        delivery::reset_test_delivery_counters();
        let output = tokio::task::spawn_blocking(move || {
            StdCommand::new(client_binary)
                .env("ELIXIR_G30_SERVER_URL", server_url)
                .env("ELIXIR_G30_EMAIL", email)
                .env("ELIXIR_G30_PASSWORD", password)
                .env("ELIXIR_G30_PROVIDER_ID", provider_id.to_string())
                .env("ELIXIR_G30_ITEM_KEY", item_key)
                .env("ELIXIR_G30_STREAMS_JSON", streams_json)
                .env("ELIXIR_G30_TLS_CA", ca_path)
                .env("ELIXIR_G30_CAPTURE_DIR", capture_path)
                .env("QT_QPA_PLATFORM", "offscreen")
                .env("QSG_RHI_BACKEND", "opengl")
                .output()
        })
        .await??;
        let origin_snapshot = origin
            .snapshot()
            .await
            .unwrap_or_else(|_| json!({"error": "snapshot_unavailable"}));
        let delivery_counters = delivery::test_delivery_counters();

        shutdown.cancel();
        let server_result = server.await?;
        fixture.stop().await;
        origin.stop().await?;
        let lease_shutdown = CancellationToken::new();
        lease_shutdown.cancel();
        state.live.clone().run_lease_heartbeat(lease_shutdown).await;
        server_result?;
        if !output.status.success() {
            anyhow::bail!(
                "G40 real Qt/mpv client failed with {}\ndelivery counters (attempted, authorized, succeeded, error): {:?}\norigin:\n{}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                delivery_counters,
                origin_snapshot,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("Totals: 3 passed, 0 failed"),
            "unexpected G40 Qt output: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        let sessions: (i64, i64, i64) = sqlx::query_as(
            "SELECT COUNT(*),
                    SUM(CASE WHEN state = 'ended' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state NOT IN ('ended', 'expired', 'failed') THEN 1 ELSE 0 END)
             FROM live_playback_sessions",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(sessions, (1, 1, 0));
        assert_eq!(relay.available_capacity(), initial_capacity);
        let plaintext_descriptors: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_playback_sessions
             WHERE encrypted_descriptor LIKE '%ELIXIR_LIVE_CANARY%'
                OR encrypted_descriptor LIKE '%127.0.0.1%'
                OR encrypted_item_snapshot LIKE '%g40-event%'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(plaintext_descriptors, 0);
        let forbidden_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_after, forbidden_before);
        Ok(())
    }

    #[tokio::test]
    async fn g50_real_qt_mpv_copy_remux_profiles_have_moving_frames_and_cleanup() -> Result<()> {
        let client_binary = std::env::var("ELIXIR_G50_CLIENT_TEST_BINARY")
            .or_else(|_| std::env::var("ELIXIR_G40_CLIENT_TEST_BINARY"))
            .map_err(|_| anyhow::anyhow!("ELIXIR_G50_CLIENT_TEST_BINARY is required"))?;
        anyhow::ensure!(
            Path::new(&client_binary).is_file(),
            "G50 Qt client test binary does not exist"
        );
        let origin = DirectOriginFixture::start_http().await?;
        let fixture = DirectProviderFixture::start_for_remux(origin.port).await?;
        let provider_calls = fixture.calls.clone();
        let remux_temporary = tempfile::tempdir()?;
        let remux_root = remux_temporary.path().join("jobs");
        let mut settings = settings();
        settings.live.relay_enabled = true;
        settings.live.remux_enabled = true;
        settings.live.remux.max_concurrent = 1;
        settings.live.remux.temp_root = remux_root.to_string_lossy().into_owned();
        settings.live.remux.segment_seconds = 1;
        settings.live.remux.playlist_segments = 3;
        settings.live.remux.no_output_timeout_seconds = 5;
        settings.live.remux.minimum_free_bytes = 64 * 1_024 * 1_024;
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, fixture.port, json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let remux = state.live.remux_service().expect("remux service");
        let initial_capacity = remux.available_capacity();
        let app = router(state.clone());

        let email = format!("g50-{}@example.invalid", Uuid::new_v4());
        let password = "correct horse battery staple";
        let (status, _, signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({"email": email, "password": password})),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "signup body: {signup}");
        let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
        let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
        let user_id: Uuid =
            sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM homes WHERE id = $1")
                .bind(home_id.to_string())
                .fetch_one(&pool)
                .await?
                .parse()?;
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;
        for path in ["/transport/live.ts", "/dash/generated.mpd"] {
            sqlx::query(
                "INSERT INTO live_provider_destination_rules (
                    id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                    network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                    revision, created_by_user_id, created_by_actor_snapshot
                 ) VALUES ($1, $2, $3, 'http', '127.0.0.1', $4, $5,
                           'public', TRUE, FALSE, FALSE, 1, $6, $7)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(home_id.to_string())
            .bind(provider_id.to_string())
            .bind(i64::from(origin.port))
            .bind(path)
            .bind(user_id.to_string())
            .bind(json!({"userId": user_id, "role": "owner"}).to_string())
            .execute(&pool)
            .await?;
        }

        let codec = LivePublicKeyCodec::new(state.live.crypto().await.expect("Live crypto"));
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision,
        };
        let now = Utc::now();
        let item_key = codec.seal_item(provider_id, "g50-event", scope, now)?;
        let streams_json = serde_json::to_string(&vec![
            json!({
                "id": "mpegts",
                "key": codec.seal_stream(provider_id, "g50-event", "mpegts", scope, now)?,
            }),
            json!({
                "id": "dash",
                "key": codec.seal_stream(provider_id, "g50-event", "dash", scope, now)?,
            }),
        ])?;
        let forbidden_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;

        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let server_url = format!("http://{address}");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
                .await
        });
        let ca_path = origin.ca_path();
        let capture_path = origin.capture_path();
        let output = tokio::task::spawn_blocking(move || {
            StdCommand::new(client_binary)
                .env("ELIXIR_G30_SERVER_URL", server_url)
                .env("ELIXIR_G30_EMAIL", email)
                .env("ELIXIR_G30_PASSWORD", password)
                .env("ELIXIR_G30_PROVIDER_ID", provider_id.to_string())
                .env("ELIXIR_G30_ITEM_KEY", item_key)
                .env("ELIXIR_G30_STREAMS_JSON", streams_json)
                .env("ELIXIR_G30_TLS_CA", ca_path)
                .env("ELIXIR_G30_CAPTURE_DIR", capture_path)
                .env("QT_QPA_PLATFORM", "offscreen")
                .env("QSG_RHI_BACKEND", "opengl")
                .output()
        })
        .await??;
        let origin_snapshot = origin
            .snapshot()
            .await
            .unwrap_or_else(|_| json!({"error": "snapshot_unavailable"}));

        shutdown.cancel();
        let server_result = server.await?;
        fixture.stop().await;
        origin.stop().await?;
        let lease_shutdown = CancellationToken::new();
        lease_shutdown.cancel();
        state.live.clone().run_lease_heartbeat(lease_shutdown).await;
        server_result?;
        if !output.status.success() {
            anyhow::bail!(
                "G50 real Qt/mpv client failed with {}\norigin:\n{}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                origin_snapshot,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("Totals: 3 passed, 0 failed"),
            "unexpected G50 Qt output: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), 2);
        let sessions: (i64, i64, i64) = sqlx::query_as(
            "SELECT COUNT(*),
                    SUM(CASE WHEN state = 'ended' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state NOT IN ('ended', 'expired', 'failed') THEN 1 ELSE 0 END)
             FROM live_playback_sessions",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(sessions, (2, 2, 0));
        let snapshot = remux.snapshot().await;
        assert_eq!(snapshot.active_jobs, 0);
        assert_eq!(snapshot.available_capacity, initial_capacity);
        assert_eq!(snapshot.jobs_started, 2);
        assert!(
            tokio::fs::read_dir(&remux_root)
                .await?
                .next_entry()
                .await?
                .is_none()
        );
        let plaintext_descriptors: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_playback_sessions
             WHERE encrypted_descriptor LIKE '%ELIXIR_M10_HTTP_CANARY%'
                OR encrypted_descriptor LIKE '%127.0.0.1%'
                OR encrypted_item_snapshot LIKE '%g50-event%'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(plaintext_descriptors, 0);
        let forbidden_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_after, forbidden_before);
        Ok(())
    }

    #[tokio::test]
    async fn g30_real_qt_mpv_direct_protocol_matrix_has_moving_frames_and_cleanup() -> Result<()> {
        let client_binary = std::env::var("ELIXIR_G30_CLIENT_TEST_BINARY")
            .map_err(|_| anyhow::anyhow!("ELIXIR_G30_CLIENT_TEST_BINARY is required"))?;
        anyhow::ensure!(
            Path::new(&client_binary).is_file(),
            "G30 Qt client test binary does not exist"
        );
        let origin = DirectOriginFixture::start().await?;
        let fixture = DirectProviderFixture::start_for_origin(origin.port).await?;
        let settings = settings();
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, fixture.port, json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let app = router(state.clone());

        let email = format!("g30-{}@example.invalid", Uuid::new_v4());
        let password = "correct horse battery staple";
        let (status, _, signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({"email": email, "password": password})),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "signup body: {signup}");
        let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
        let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
        let user_id: Uuid =
            sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM homes WHERE id = $1")
                .bind(home_id.to_string())
                .fetch_one(&pool)
                .await?
                .parse()?;
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;

        let sources = [
            ("hls", "/hls/generated.m3u8"),
            ("dash", "/dash/generated.mpd"),
            ("progressive", "/transport/progressive.mp4"),
            ("mpegts", "/transport/live.ts"),
        ];
        for (_, path) in sources {
            sqlx::query(
                "INSERT INTO live_provider_destination_rules (
                    id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                    network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                    revision, created_by_user_id, created_by_actor_snapshot
                 ) VALUES ($1, $2, $3, 'https', 'localhost', $4, $5, 'public',
                           TRUE, FALSE, TRUE, 1, $6, $7)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(home_id.to_string())
            .bind(provider_id.to_string())
            .bind(i64::from(origin.port))
            .bind(path)
            .bind(user_id.to_string())
            .bind(json!({"userId": user_id, "role": "owner"}).to_string())
            .execute(&pool)
            .await?;
        }

        let crypto = state.live.crypto().await.expect("Live crypto");
        let codec = LivePublicKeyCodec::new(crypto);
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision,
        };
        let now = Utc::now();
        let item_key = codec.seal_item(provider_id, "g30-event", scope, now)?;
        let stream_keys = sources
            .iter()
            .map(|(id, _)| {
                Ok(json!({
                    "id": id,
                    "key": codec.seal_stream(provider_id, "g30-event", id, scope, now)?,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        let forbidden_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;

        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let server_url = format!("http://{address}");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
                .await
        });
        let ca_path = origin.ca_path();
        let capture_path = origin.capture_path();
        let streams_json = serde_json::to_string(&stream_keys)?;
        let output = tokio::task::spawn_blocking(move || {
            StdCommand::new(client_binary)
                .env("ELIXIR_G30_SERVER_URL", server_url)
                .env("ELIXIR_G30_EMAIL", email)
                .env("ELIXIR_G30_PASSWORD", password)
                .env("ELIXIR_G30_PROVIDER_ID", provider_id.to_string())
                .env("ELIXIR_G30_ITEM_KEY", item_key)
                .env("ELIXIR_G30_STREAMS_JSON", streams_json)
                .env("ELIXIR_G30_TLS_CA", ca_path)
                .env("ELIXIR_G30_CAPTURE_DIR", capture_path)
                .env("QT_QPA_PLATFORM", "offscreen")
                .env("QSG_RHI_BACKEND", "opengl")
                .output()
        })
        .await?;

        shutdown.cancel();
        let server_result = server.await?;
        fixture.stop().await;
        origin.stop().await?;
        let lease_shutdown = CancellationToken::new();
        lease_shutdown.cancel();
        state.live.clone().run_lease_heartbeat(lease_shutdown).await;
        server_result?;
        let output = output?;
        if !output.status.success() {
            anyhow::bail!(
                "G30 real Qt/mpv client failed with {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("Totals: 3 passed, 0 failed"),
            "unexpected G30 Qt output: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let sessions: (i64, i64, i64) = sqlx::query_as(
            "SELECT COUNT(*),
                    SUM(CASE WHEN state = 'ended' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state NOT IN ('ended', 'expired', 'failed') THEN 1 ELSE 0 END)
             FROM live_playback_sessions",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(sessions, (4, 4, 0));
        let plaintext_descriptors: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_playback_sessions
             WHERE encrypted_descriptor LIKE '%localhost%'
                OR encrypted_item_snapshot LIKE '%g30-event%'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(plaintext_descriptors, 0);
        let forbidden_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_after, forbidden_before);
        Ok(())
    }

    fn settings() -> Settings {
        Settings {
            environment: RunEnvironment::Development,
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 44301,
            },
            database: DatabaseConfig {
                url: format!(
                    "sqlite:file:s20-live-session-{}?mode=memory&cache=shared",
                    Uuid::new_v4()
                ),
                max_connections: 8,
                connect_timeout_seconds: 5,
            },
            library: LibraryConfig::default(),
            extensions: crate::config::ExtensionsConfig::default(),
            auth: AuthConfig::default(),
            secrets: SecretsConfig {
                master_key: Some(general_purpose::STANDARD.encode([20u8; 32])),
            },
            telemetry: TelemetryConfig::default(),
            metadata: crate::config::MetadataConfig::default(),
            classifier: ClassifierConfig::default(),
            playback: crate::config::PlaybackConfig::default(),
            media_interactions: MediaInteractionsConfig::default(),
            live: LiveConfig {
                enabled: true,
                catalog_enabled: true,
                playback_enabled: true,
                client_direct_enabled: true,
                ..LiveConfig::default()
            },
            network: crate::config::NetworkConfig::default(),
        }
    }

    async fn request(
        app: &Router,
        method: &str,
        uri: &str,
        headers: &[(&str, String)],
        body_value: Option<Value>,
    ) -> Result<Response> {
        let mut builder = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, value);
        }
        let body = if let Some(value) = body_value {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        } else {
            Body::empty()
        };
        Ok(app.clone().oneshot(builder.body(body)?).await?)
    }

    async fn response_json(response: Response) -> Result<(StatusCode, HeaderMap, Value)> {
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = body::to_bytes(response.into_body(), 2 * 1024 * 1024).await?;
        Ok((status, headers, serde_json::from_slice(&bytes)?))
    }

    async fn response_bytes(
        response: Response,
        limit: usize,
    ) -> Result<(StatusCode, HeaderMap, Vec<u8>)> {
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = body::to_bytes(response.into_body(), limit).await?;
        Ok((status, headers, bytes.to_vec()))
    }

    fn relay_resource_routes(manifest: &str) -> Vec<String> {
        const PREFIX: &str = "/api/v1/live/sessions/";
        manifest
            .split(['\n', '"', ','])
            .filter_map(|part| {
                let start = part.find(PREFIX)?;
                let route = part[start..].trim();
                route
                    .contains("/delivery/hls/resources/")
                    .then(|| route.to_string())
            })
            .collect()
    }

    #[tokio::test]
    async fn r20_refresh_and_priority_failover_rotate_tokens_maps_and_replay() -> Result<()> {
        let origin = RelayOriginFixture::start().await?;
        let fixture = DirectProviderFixture::start_for_recovery(origin.port).await?;
        let mut settings = settings();
        settings.live.relay_enabled = true;
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, fixture.port, json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let relay = state.live.relay_service().expect("relay service");
        let initial_capacity = relay.available_capacity();
        let app = router(state.clone());

        let (status, _, signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({
                    "email": format!("r20-{}@example.invalid", Uuid::new_v4()),
                    "password": "correct horse battery staple"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let access = signup["access_token"].as_str().unwrap().to_string();
        let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
        let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
        let user_id: Uuid =
            sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM homes WHERE id = $1")
                .bind(home_id.to_string())
                .fetch_one(&pool)
                .await?
                .parse()?;
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO live_provider_destination_rules (
                id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                revision, created_by_user_id, created_by_actor_snapshot
             ) VALUES ($1, $2, $3, 'http', '127.0.0.1', $4, '/master.m3u8', 'public',
                       TRUE, FALSE, FALSE, 1, $5, $6)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(i64::from(origin.port))
        .bind(user_id.to_string())
        .bind(json!({"userId": user_id, "role": "owner"}).to_string())
        .execute(&pool)
        .await?;

        let codec = LivePublicKeyCodec::new(state.live.crypto().await.expect("Live crypto"));
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision,
        };
        let now = Utc::now();
        let item_key = codec.seal_item(provider_id, "r20-event", scope, now)?;
        let stream_key = codec.seal_stream(provider_id, "r20-event", "primary", scope, now)?;
        let create_body = json!({
            "providerId": provider_id,
            "itemKey": item_key,
            "streamOptionKey": stream_key,
            "client": {
                "platform": "macos",
                "player": "mpv",
                "protocols": ["hls"],
                "videoCodecs": ["h264"],
                "audioCodecs": ["aac"],
                "supportsRequestHeaders": true,
                "supportsCookies": true,
                "supportsLowLatencyHls": false,
                "supportsOriginTimeShift": true
            }
        });
        let create_headers = [
            ("authorization", format!("Bearer {access}")),
            ("idempotency-key", "r20-recovery-create-0001".to_string()),
        ];
        let forbidden_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        let (status, _, created) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &create_headers,
                Some(create_body.clone()),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED, "create body: {created}");
        assert_eq!(created["availableSources"].as_array().unwrap().len(), 3);
        let created_source_key = created["selectedSource"]["sourceKey"]
            .as_str()
            .unwrap()
            .to_string();
        let session_id = Uuid::parse_str(created["sessionId"].as_str().unwrap())?;
        let create_revision = created["revision"].as_i64().unwrap();
        let original_token = created["sessionToken"].as_str().unwrap().to_string();
        let playback_url = created["playbackUrl"].as_str().unwrap().to_string();
        let original_delivery_headers = [("authorization", format!("Bearer {original_token}"))];
        let (_, _, original_manifest) = response_bytes(
            request(&app, "GET", &playback_url, &original_delivery_headers, None).await?,
            1024 * 1024,
        )
        .await?;
        let original_manifest = String::from_utf8(original_manifest)?;
        let original_resource = relay_resource_routes(&original_manifest)
            .into_iter()
            .next()
            .expect("original child resource");

        let account_headers = [("authorization", format!("Bearer {access}"))];
        let (status, refresh_headers, refreshed) = response_json(
            request(
                &app,
                "POST",
                &format!("/api/v1/live/sessions/{session_id}/refresh"),
                &account_headers,
                Some(json!({
                    "expectedRevision": create_revision,
                    "reason": "upstream_unauthorized"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "refresh body: {refreshed}");
        assert_eq!(refresh_headers[CACHE_CONTROL], "no-store");
        assert_eq!(refreshed["revision"], create_revision + 2);
        assert_eq!(refreshed["tokenRevision"], 2);
        assert_eq!(refreshed["selectedSource"]["label"], "Recovery primary");
        assert_eq!(refreshed["availableSources"].as_array().unwrap().len(), 3);
        assert_ne!(
            refreshed["selectedSource"]["sourceKey"].as_str().unwrap(),
            created_source_key
        );
        let refresh_revision = refreshed["revision"].as_i64().unwrap();
        let refreshed_token = refreshed["sessionToken"].as_str().unwrap().to_string();
        assert_ne!(refreshed_token, original_token);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
        let (status, _, old_token_error) = response_json(
            request(&app, "GET", &playback_url, &original_delivery_headers, None).await?,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(old_token_error["errors"][0]["code"], "LIVE_AUTH_REQUIRED");
        let refreshed_delivery_headers = [("authorization", format!("Bearer {refreshed_token}"))];
        let (status, _, old_resource_error) = response_json(
            request(
                &app,
                "GET",
                &original_resource,
                &refreshed_delivery_headers,
                None,
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!old_resource_error.to_string().contains("ELIXIR_R20_CANARY"));

        let (status, _, stale) = response_json(
            request(
                &app,
                "POST",
                &format!("/api/v1/live/sessions/{session_id}/refresh"),
                &account_headers,
                Some(json!({
                    "expectedRevision": create_revision,
                    "reason": "stalled"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(stale["errors"][0]["code"], "LIVE_SESSION_CONFLICT");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);

        let (status, _, failed_over) = response_json(
            request(
                &app,
                "POST",
                &format!("/api/v1/live/sessions/{session_id}/failover"),
                &account_headers,
                Some(json!({
                    "expectedRevision": refresh_revision,
                    "reason": "transport"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "failover body: {failed_over}");
        assert_eq!(failed_over["revision"], refresh_revision + 3);
        assert_eq!(failed_over["tokenRevision"], 3);
        assert_eq!(failed_over["selectedSource"]["label"], "Recovery backup");
        assert_eq!(
            failed_over["availableSources"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|source| source["label"] == "Recovery backup")
                .count(),
            1
        );
        let failover_revision = failed_over["revision"].as_i64().unwrap();
        let failover_token = failed_over["sessionToken"].as_str().unwrap().to_string();
        assert_ne!(failover_token, refreshed_token);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
        let counters: (i64, i64, i64) = sqlx::query_as(
            "SELECT refresh_count, failover_count, source_index
             FROM live_playback_sessions WHERE id = $1",
        )
        .bind(session_id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(counters, (1, 1, 0));

        let (status, _, replay) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &create_headers,
                Some(create_body),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replay["revision"], failover_revision);
        assert_eq!(replay["sessionToken"], failover_token);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);

        let (status, _, detail) = response_json(
            request(
                &app,
                "GET",
                &format!("/api/v1/live/sessions/{session_id}"),
                &account_headers,
                None,
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(
            detail["data"]["timeline"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["state"] == "refreshing"
                    && entry["reason"] == "upstream_unauthorized:succeeded")
        );
        assert!(
            detail["data"]["timeline"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["state"] == "failing_over"
                    && entry["reason"] == "transport:succeeded")
        );
        for secret in ["127.0.0.1", "ELIXIR_R20_CANARY", "r20-refresh:"] {
            assert!(!detail.to_string().contains(secret));
        }
        let plaintext: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_playback_sessions
             WHERE encrypted_descriptor LIKE '%ELIXIR_R20_CANARY%'
                OR encrypted_descriptor LIKE '%r20-refresh:%'
                OR encrypted_descriptor LIKE '%127.0.0.1%'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(plaintext, 0);
        let forbidden_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_after, forbidden_before);

        let end_headers = [("authorization", format!("Bearer {access}"))];
        let ended = request(
            &app,
            "DELETE",
            &format!("/api/v1/live/sessions/{session_id}?expectedRevision={failover_revision}"),
            &end_headers,
            None,
        )
        .await?;
        assert_eq!(ended.status(), StatusCode::NO_CONTENT);
        assert_eq!(relay.available_capacity(), initial_capacity);
        fixture.stop().await;
        origin.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn r12_authenticated_hls_relay_needs_no_manual_destination_rules() -> Result<()> {
        let origin = RelayOriginFixture::start().await?;
        let fixture = DirectProviderFixture::start_for_relay(origin.port).await?;
        let mut settings = settings();
        settings.live.relay_enabled = true;
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, fixture.port, json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        let relay = state.live.relay_service().expect("relay service");
        let initial_capacity = relay.available_capacity();
        let app = router(state.clone());

        let (status, _, signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({
                    "email": format!("r12-{}@example.invalid", Uuid::new_v4()),
                    "password": "correct horse battery staple"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let access = signup["access_token"].as_str().unwrap().to_string();
        let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
        let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;
        let codec = LivePublicKeyCodec::new(state.live.crypto().await.expect("Live crypto"));
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision,
        };
        let now = Utc::now();
        let item_key = codec.seal_item(provider_id, "r12-event", scope, now)?;
        let stream_key = codec.seal_stream(provider_id, "r12-event", "primary", scope, now)?;
        let account_headers = [
            ("authorization", format!("Bearer {access}")),
            ("idempotency-key", "r12-relay-create-0001".to_string()),
        ];
        let forbidden_before: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        let (status, create_headers, created) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &account_headers,
                Some(json!({
                    "providerId": provider_id,
                    "itemKey": item_key,
                    "streamOptionKey": stream_key,
                    "client": {
                        "platform": "macos",
                        "player": "mpv",
                        "protocols": ["hls"],
                        "videoCodecs": ["h264"],
                        "audioCodecs": ["aac"],
                        "supportsRequestHeaders": true,
                        "supportsCookies": true,
                        "supportsLowLatencyHls": false,
                        "supportsOriginTimeShift": true
                    }
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED, "create body: {created}");
        assert_eq!(create_headers[CACHE_CONTROL], "no-store");
        assert_eq!(created["deliveryMode"], "server_relay");
        assert_eq!(created["decisionReason"], "sensitive_url_requires_relay");
        let session_id = Uuid::parse_str(created["sessionId"].as_str().unwrap())?;
        let session_token = created["sessionToken"].as_str().unwrap().to_string();
        let root_route = created["playbackUrl"].as_str().unwrap().to_string();
        assert_eq!(
            root_route,
            format!("/api/v1/live/sessions/{session_id}/delivery/hls/manifest.m3u8")
        );
        assert!(!created.to_string().contains("ELIXIR_LIVE_CANARY"));
        assert_eq!(relay.available_capacity(), initial_capacity - 1);

        let delivery_headers = [("authorization", format!("Bearer {session_token}"))];
        let (first, second) = tokio::join!(
            request(&app, "GET", &root_route, &delivery_headers, None),
            request(&app, "GET", &root_route, &delivery_headers, None)
        );
        let (first_status, first_headers, first_body) = response_bytes(first?, 1024 * 1024).await?;
        let (second_status, second_headers, second_body) =
            response_bytes(second?, 1024 * 1024).await?;
        assert_eq!(first_status, StatusCode::OK);
        assert_eq!(second_status, StatusCode::OK);
        assert_eq!(first_body, second_body);
        assert_eq!(first_headers[CACHE_CONTROL], "no-store");
        assert_eq!(second_headers[CACHE_CONTROL], "no-store");
        assert_eq!(first_headers["vary"], "Authorization");
        assert_eq!(first_headers["x-content-type-options"], "nosniff");
        assert_eq!(origin.state.master_calls.load(Ordering::SeqCst), 1);
        let master = String::from_utf8(first_body)?;
        assert!(!master.contains("ELIXIR_LIVE_CANARY"));
        assert!(!master.contains("127.0.0.1"));
        let master_routes = relay_resource_routes(&master);
        assert_eq!(master_routes.len(), 1, "rewritten master: {master}");

        let (status, headers, media_body) = response_bytes(
            request(&app, "GET", &master_routes[0], &delivery_headers, None).await?,
            1024 * 1024,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[CACHE_CONTROL], "no-store");
        assert_eq!(origin.state.media_calls.load(Ordering::SeqCst), 1);
        let media = String::from_utf8(media_body)?;
        assert!(!media.contains("ELIXIR_LIVE_CANARY"));
        let media_routes = relay_resource_routes(&media);
        assert_eq!(media_routes.len(), 2, "rewritten media: {media}");

        let (status, headers, key) = response_bytes(
            request(&app, "GET", &media_routes[0], &delivery_headers, None).await?,
            64,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[CACHE_CONTROL], "no-store");
        assert_eq!(key, vec![0x5a_u8; 16]);
        assert_eq!(origin.state.key_calls.load(Ordering::SeqCst), 1);

        let (status, headers, segment) = response_bytes(
            request(&app, "GET", &media_routes[1], &delivery_headers, None).await?,
            1024 * 1024,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[CACHE_CONTROL], "no-store");
        assert_eq!(headers["content-type"], "video/mp2t");
        assert_eq!(segment, vec![0x47_u8; 188 * 4]);
        assert_eq!(origin.state.segment_calls.load(Ordering::SeqCst), 1);

        let (status, _, query_error) = response_json(
            request(
                &app,
                "GET",
                &format!("{root_route}?%74oken=ELIXIR_LIVE_CANARY_QUERY_TOKEN"),
                &delivery_headers,
                None,
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(query_error["errors"][0]["code"], "LIVE_AUTH_REQUIRED");
        let wrong_delivery_headers = [("authorization", "Bearer wrong-session-token".to_string())];
        let (status, _, wrong_token_error) =
            response_json(request(&app, "GET", &root_route, &wrong_delivery_headers, None).await?)
                .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(wrong_token_error["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        relay.end_session(session_id);
        assert_eq!(relay.available_capacity(), initial_capacity);
        let (status, _, reconstructed) = response_bytes(
            request(&app, "GET", &root_route, &delivery_headers, None).await?,
            1024 * 1024,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(!reconstructed.is_empty());
        assert_eq!(origin.state.master_calls.load(Ordering::SeqCst), 2);
        assert_eq!(relay.available_capacity(), initial_capacity - 1);

        sqlx::query(
            "UPDATE live_control_server_leases
             SET fencing_token = fencing_token + 1
             WHERE lease_name = 'live-control-v1'",
        )
        .execute(&pool)
        .await?;
        let (status, _, stale_error) =
            response_json(request(&app, "GET", &root_route, &delivery_headers, None).await?)
                .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(stale_error["errors"][0]["code"], "LIVE_AUTH_REQUIRED");
        relay.reap_stale().await;
        assert_eq!(relay.available_capacity(), initial_capacity);

        let plaintext: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_playback_sessions
             WHERE encrypted_descriptor LIKE '%ELIXIR_LIVE_CANARY%'
                OR encrypted_descriptor LIKE '%127.0.0.1%'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(plaintext, 0);
        let forbidden_after: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM media_files),
                (SELECT COUNT(*) FROM acquisition_subscriptions),
                (SELECT COUNT(*) FROM playback_sessions)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(forbidden_after, forbidden_before);

        fixture.stop().await;
        origin.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn s20_real_router_direct_session_idempotency_control_and_delivery_auth() -> Result<()> {
        let fixture = DirectProviderFixture::start().await?;
        let settings = settings();
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let pool = database.pool.clone();
        let (_, provider_id) = seed_provider(&database, fixture.port, json!({})).await?;
        let mut state = AppState::new(
            settings.clone(),
            database,
            AuthService::new(settings.auth.clone())?,
            ExtensionManager::new(),
            MetadataService::new(settings.metadata.clone())?,
            LinkerService::new(settings.classifier.clone())?,
            ArtworkService::new(
                settings.library.artwork_cache_dir.clone(),
                settings.metadata.request_timeout_seconds,
            )?,
            SecretsManager::from_settings(&settings)?,
        );
        state.live = Arc::new(LiveService::new_for_test(
            settings.live.clone(),
            settings.environment,
            pool.clone(),
            state.secrets.clone(),
        ));
        state.live.initialize().await?;
        state
            .live
            .set_component_readiness(LiveComponent::Playback, true, None, None)
            .await;
        state
            .live
            .set_component_readiness(LiveComponent::ClientDirect, true, None, None)
            .await;
        let app = router(state.clone());

        let (status, _, unauthenticated) = response_json(
            request(&app, "POST", "/api/v1/live/sessions", &[], Some(json!({}))).await?,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(unauthenticated["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        let (status, _, signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({
                    "email": format!("s20-{}@example.invalid", Uuid::new_v4()),
                    "password": "correct horse battery staple"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let access = signup["access_token"].as_str().unwrap().to_string();
        let csrf = signup["csrf_token"].as_str().unwrap().to_string();
        let user_id = Uuid::parse_str(
            &sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM homes WHERE id = $1")
                .bind(signup["home_id"].as_str().unwrap())
                .fetch_one(&pool)
                .await?,
        )?;
        let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
        let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
        let account_session_id = Uuid::parse_str(signup["session_id"].as_str().unwrap())?;
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
        )
        .bind(profile_id.to_string())
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO live_provider_destination_rules (
                id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                revision, created_by_user_id, created_by_actor_snapshot
             ) VALUES ($1, $2, $3, 'https', 'public-direct.example.invalid', 443,
                       '/live/master.m3u8', 'public', TRUE, FALSE, TRUE, 1, $4, $5)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(user_id.to_string())
        .bind(json!({"userId": user_id, "role": "owner"}).to_string())
        .execute(&pool)
        .await?;

        let crypto = state.live.crypto().await.expect("Live crypto");
        let codec = LivePublicKeyCodec::new(crypto);
        let scope = LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision,
        };
        let now = Utc::now();
        let item_key = codec.seal_item(provider_id, "event-direct", scope, now)?;
        let stream_key = codec.seal_stream(provider_id, "event-direct", "primary", scope, now)?;
        let create_body = json!({
            "providerId": provider_id,
            "itemKey": item_key,
            "streamOptionKey": stream_key,
            "client": {
                "platform": "macos",
                "player": "mpv",
                "protocols": ["hls", "dash", "http_progressive", "mpeg_ts"],
                "videoCodecs": ["h264", "hevc"],
                "audioCodecs": ["aac", "ac3"],
                "supportsRequestHeaders": true,
                "supportsCookies": false,
                "supportsLowLatencyHls": false,
                "supportsOriginTimeShift": true
            }
        });
        let auth_headers = [
            ("authorization", format!("Bearer {access}")),
            ("idempotency-key", "s20-direct-create-0001".to_string()),
        ];
        let query_headers = [("idempotency-key", "s20-query-create-0001".to_string())];
        let (status, _, query_error) = response_json(
            request(
                &app,
                "POST",
                &format!("/api/v1/live/sessions?access_token={access}"),
                &query_headers,
                Some(create_body.clone()),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(query_error["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        let missing_csrf_headers = [
            ("cookie", format!("elixir_ui_token={access}")),
            ("idempotency-key", "s20-cookie-create-0001".to_string()),
        ];
        let (status, _, csrf_error) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &missing_csrf_headers,
                Some(create_body.clone()),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(csrf_error["errors"][0]["code"], "LIVE_CSRF_REQUIRED");

        let second_auth_headers = [
            ("authorization", format!("Bearer {access}")),
            ("idempotency-key", "s20-direct-create-0001".to_string()),
        ];
        let (first_response, second_response) = tokio::join!(
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &auth_headers,
                Some(create_body.clone()),
            ),
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &second_auth_headers,
                Some(create_body.clone()),
            )
        );
        let (first_status, first_headers, first_json) = response_json(first_response?).await?;
        let (second_status, second_headers, second_json) = response_json(second_response?).await?;
        assert_eq!(
            BTreeSet::from([first_status, second_status]),
            BTreeSet::from([StatusCode::OK, StatusCode::CREATED])
        );
        let (headers, created) = if first_status == StatusCode::CREATED {
            (&first_headers, &first_json)
        } else {
            (&second_headers, &second_json)
        };
        assert_eq!(headers[CACHE_CONTROL], "no-store");
        assert_eq!(created["deliveryMode"], "client_direct");
        assert_eq!(created["decisionReason"], "public_compatible_direct");
        assert_eq!(
            created["playbackUrl"],
            "https://public-direct.example.invalid/live/master.m3u8"
        );
        assert!(created.get("sessionToken").is_none());
        assert!(created.get("tokenRevision").is_none());
        assert_eq!(created["availableSources"].as_array().unwrap().len(), 1);
        assert!(created["trackPreferences"]["audio"].is_null());
        assert!(created["trackPreferences"]["subtitle"].is_null());
        assert_eq!(first_json["sessionId"], second_json["sessionId"]);
        let session_id = Uuid::parse_str(created["sessionId"].as_str().unwrap())?;
        let source_key = created["selectedSource"]["sourceKey"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);

        let cookie_headers = [
            ("cookie", format!("elixir_ui_token={access}")),
            ("origin", "http://127.0.0.1:44301".to_string()),
            ("x-elixir-csrf", csrf),
            ("idempotency-key", "s20-direct-create-0001".to_string()),
        ];
        let (status, _, replay) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/live/sessions",
                &cookie_headers,
                Some(create_body.clone()),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replay["sessionId"], session_id.to_string());
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);

        let bearer = [("authorization", format!("Bearer {access}"))];
        let (status, _, detail) = response_json(
            request(
                &app,
                "GET",
                &format!("/api/v1/live/sessions/{session_id}"),
                &bearer,
                None,
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["data"]["state"], "ready");
        let revision = detail["data"]["revision"].as_i64().unwrap();
        let (status, _, heartbeat) = response_json(
            request(
                &app,
                "POST",
                &format!("/api/v1/live/sessions/{session_id}/heartbeat"),
                &bearer,
                Some(json!({
                    "expectedRevision": revision,
                    "playerState": "playing",
                    "observedAt": timestamp(Utc::now()),
                    "distanceFromLiveEdgeSeconds": 2.5,
                    "sourceKey": source_key,
                    "audioTrackId": "audio-1",
                    "audioTrackLanguage": "EN_us",
                    "audioTrackTitle": "Main commentary",
                    "subtitleTrackId": "no"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(heartbeat["data"]["state"], "playing");
        assert_eq!(
            heartbeat["data"]["trackPreferences"]["audio"]["trackId"],
            "audio-1"
        );
        assert_eq!(
            heartbeat["data"]["trackPreferences"]["audio"]["language"],
            "en-us"
        );
        assert_eq!(
            heartbeat["data"]["trackPreferences"]["subtitle"]["trackId"],
            "no"
        );
        let persisted_track_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM live_track_preferences
             WHERE user_id = $1 AND provider_id = $2",
        )
        .bind(user_id.to_string())
        .bind(provider_id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(persisted_track_revision, 1);
        let end_revision = heartbeat["data"]["revision"].as_i64().unwrap();

        let (status, _, second_signup) = response_json(
            request(
                &app,
                "POST",
                "/api/v1/auth/signup",
                &[],
                Some(json!({
                    "email": format!("s20-other-{}@example.invalid", Uuid::new_v4()),
                    "password": "correct horse battery staple"
                })),
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let other_bearer = [(
            "authorization",
            format!("Bearer {}", second_signup["access_token"].as_str().unwrap()),
        )];
        let (status, _, ownership_error) = response_json(
            request(
                &app,
                "GET",
                &format!("/api/v1/live/sessions/{session_id}"),
                &other_bearer,
                None,
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            ownership_error["errors"][0]["code"],
            "LIVE_SESSION_NOT_FOUND"
        );

        let issued_at = usize::try_from((Utc::now() - chrono::Duration::minutes(5)).timestamp())?;
        let expired_access = encode(
            &Header::default(),
            &ExpiredAccessClaims {
                sub: user_id.to_string(),
                sid: account_session_id.to_string(),
                hid: Some(home_id.to_string()),
                pid: Some(profile_id.to_string()),
                role: Some("owner".to_string()),
                exp: issued_at + 60,
                iat: issued_at,
            },
            &EncodingKey::from_secret(settings.auth.access_token_secret.as_bytes()),
        )?;
        let expired_bearer = [("authorization", format!("Bearer {expired_access}"))];
        let (status, _, expired_error) = response_json(
            request(
                &app,
                "GET",
                &format!("/api/v1/live/sessions/{session_id}"),
                &expired_bearer,
                None,
            )
            .await?,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(expired_error["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

        let response = request(
            &app,
            "DELETE",
            &format!("/api/v1/live/sessions/{session_id}?expectedRevision={end_revision}"),
            &bearer,
            None,
        )
        .await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let repository = state.live.session_repository().expect("session repository");
        let fence = state.live.control_fencing_token().await.unwrap();
        let relay = repository
            .create(
                NewSession {
                    owner: SessionOwner {
                        user_id,
                        home_id,
                        profile_id,
                        account_session_id,
                        provider_id,
                    },
                    item_key: SecretBytes::from_utf8("relay-item".to_string()),
                    stream_option_key: SecretBytes::from_utf8("relay-stream".to_string()),
                    item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                    descriptor: SecretBytes::from_utf8("{}".to_string()),
                    delivery_mode: DeliveryMode::ServerRelay,
                    protocol: SessionProtocol::Hls,
                    source_index: 0,
                    control_fencing_token: fence,
                    now: Utc::now(),
                },
                None,
            )
            .await?;
        let mut delivery_headers = HeaderMap::new();
        delivery_headers.insert(
            "authorization",
            format!("Bearer {}", relay.token.expose_secret()).parse()?,
        );
        assert!(
            delivery::authenticate(&state, relay.session.id, &delivery_headers, None)
                .await
                .is_ok()
        );
        assert!(
            delivery::authenticate(
                &state,
                relay.session.id,
                &delivery_headers,
                Some("_HLS_msn=42&_HLS_part=1"),
            )
            .await
            .is_ok()
        );
        assert!(
            delivery::authenticate(&state, relay.session.id, &delivery_headers, Some("token=x"))
                .await
                .is_err()
        );
        delivery_headers.insert("cookie", "elixir_ui_token=ignored".parse()?);
        assert!(
            delivery::authenticate(&state, relay.session.id, &delivery_headers, None)
                .await
                .is_ok()
        );
        assert!(
            delivery::authenticate(&state, Uuid::new_v4(), &delivery_headers, None)
                .await
                .is_err()
        );
        delivery_headers.remove("authorization");
        assert!(
            delivery::authenticate(&state, relay.session.id, &delivery_headers, None)
                .await
                .is_err()
        );
        delivery_headers.insert(
            "authorization",
            format!("Bearer {}", relay.token.expose_secret()).parse()?,
        );
        delivery_headers.remove("cookie");
        sqlx::query(
            "UPDATE account_sessions
             SET revoked_at = $1, revoked_reason = 's20_test_revoke'
             WHERE id = $2",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(account_session_id.to_string())
        .execute(&pool)
        .await?;
        assert!(
            delivery::authenticate(&state, relay.session.id, &delivery_headers, None)
                .await
                .is_err()
        );

        let local_playback_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM playback_sessions")
                .fetch_one(&pool)
                .await?;
        let media_file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&pool)
            .await?;
        assert_eq!(local_playback_count, 0);
        assert_eq!(media_file_count, 0);
        fixture.stop().await;
        Ok(())
    }
}
