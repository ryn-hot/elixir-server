//! Account-authenticated administration API for standalone Live policy.

use std::collections::BTreeMap;

use axum::{
    Json, async_trait,
    extract::{FromRequestParts, Path, RawQuery, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL, request::Parts},
    response::{IntoResponse, Response},
};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    auth::home_profiles::HomeRole,
    authz::Capability,
    http::auth::{AccountAuthTransport, CurrentPrincipal},
    live::admin::{
        ActorSnapshot, DestinationNetworkScope, DestinationRuleInput, DestinationRuleMutation,
        LiveDestinationRuleError, LiveKeyAdminError, LiveKeyAdminService, LiveProviderAdminError,
        LiveProviderAdminRepository, LiveSessionAdminError, LiveSessionAdminRepository,
        ProviderDisableMutation,
    },
    live::catalog::LiveProviderGrantError,
    live::egress::{
        EgressPolicyMode, EgressPolicyRepository, EgressPolicyRepositoryError, PolicyScope,
        StoredPolicyAssignment,
    },
    live::session::SessionRepositoryError,
    state::AppState,
};

use super::{
    catalog::{LiveHttpRejection, admit, error_response, reject_query, request_id},
    sessions::validate_mutation_transport,
};

pub struct LiveDestinationAdminPrincipal(CurrentPrincipal);

pub struct LiveGrantAdminPrincipal(CurrentPrincipal);

pub struct LiveAdminPrincipal(pub(super) CurrentPrincipal);

pub struct LiveProviderDisablePrincipal(CurrentPrincipal);

pub struct LiveSecretsAdminPrincipal(CurrentPrincipal);

pub struct LiveEgressAdminPrincipal(CurrentPrincipal);

#[async_trait]
impl FromRequestParts<AppState> for LiveDestinationAdminPrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if principal.role != HomeRole::Owner
            || !principal.has_capability(Capability::LiveManage)
            || !principal.has_capability(Capability::SettingsManage)
        {
            return Err(capability_required());
        }
        if state.live.destination_rule_repository().is_none() {
            return Err(service_unavailable());
        }
        Ok(Self(principal))
    }
}

#[async_trait]
impl FromRequestParts<AppState> for LiveGrantAdminPrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if !principal.has_capability(Capability::SharingManage) {
            return Err(sharing_required());
        }
        if state.live.admin_audit().is_none() {
            return Err(service_unavailable());
        }
        Ok(Self(principal))
    }
}

#[async_trait]
impl FromRequestParts<AppState> for LiveAdminPrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if !principal.has_capability(Capability::LiveManage) {
            return Err(live_manage_required());
        }
        Ok(Self(principal))
    }
}

#[async_trait]
impl FromRequestParts<AppState> for LiveProviderDisablePrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if !principal.has_capability(Capability::ExtensionsManage) {
            return Err(extensions_manage_required());
        }
        if state.live.admin_audit().is_none() {
            return Err(service_unavailable());
        }
        Ok(Self(principal))
    }
}

#[async_trait]
impl FromRequestParts<AppState> for LiveSecretsAdminPrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if !principal.has_capability(Capability::SecretsManage) {
            return Err(secrets_manage_required());
        }
        if state.live.admin_audit().is_none() {
            return Err(service_unavailable());
        }
        Ok(Self(principal))
    }
}

#[async_trait]
impl FromRequestParts<AppState> for LiveEgressAdminPrincipal {
    type Rejection = LiveHttpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.live.config().enabled {
            return Err(LiveHttpRejection::not_found());
        }
        let principal = CurrentPrincipal::from_request_parts(parts, state)
            .await
            .map_err(LiveHttpRejection::from_auth_error)?;
        if principal.transport == AccountAuthTransport::Query {
            return Err(LiveHttpRejection::auth_required());
        }
        if principal.role != HomeRole::Owner
            || !principal.has_capability(Capability::LiveManage)
            || !principal.has_capability(Capability::SettingsManage)
        {
            return Err(capability_required());
        }
        Ok(Self(principal))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DestinationRuleCreateRequest {
    expected_provider_revision: i64,
    scheme: String,
    host: String,
    port: u16,
    path: String,
    network_scope: DestinationNetworkScope,
    allow_fetch: bool,
    allow_credentials: bool,
    allow_client_disclosure: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DestinationRuleUpdateRequest {
    expected_revision: i64,
    scheme: String,
    host: String,
    port: u16,
    path: String,
    network_scope: DestinationNetworkScope,
    allow_fetch: bool,
    allow_credentials: bool,
    allow_client_disclosure: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DestinationRuleDeleteRequest {
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderGrantRequest {
    can_browse: bool,
    can_play: bool,
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderDisableRequest {
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionTerminateRequest {
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyRotationRequest {
    expected_revision: i64,
    key_id: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EgressPolicyScopeRequest {
    ServerDefault,
    Profile,
    Provider,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressPolicyUpdateRequest {
    scope_type: EgressPolicyScopeRequest,
    #[serde(default)]
    scope_id: Option<Uuid>,
    mode: String,
    #[serde(default)]
    policy_id: Option<String>,
    #[serde(default)]
    allow_fallback: bool,
    expected_revision: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EgressStatusDto {
    enabled: bool,
    ready: bool,
    active_bindings: usize,
    available_capacity: usize,
    default_policy: EgressDefaultPolicyDto,
    profiles: Vec<EgressProfileDto>,
    assignments: Vec<EgressAssignmentDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EgressDefaultPolicyDto {
    mode: &'static str,
    policy_id: Option<String>,
    allow_fallback: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EgressProfileDto {
    id: String,
    name: String,
    kind: &'static str,
    selectable_by_profiles: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EgressAssignmentDto {
    id: Uuid,
    scope_type: &'static str,
    scope_key: String,
    mode: &'static str,
    policy_id: Option<String>,
    allow_fallback: bool,
    revision: i64,
}

impl From<StoredPolicyAssignment> for EgressAssignmentDto {
    fn from(value: StoredPolicyAssignment) -> Self {
        Self {
            id: value.id,
            scope_type: value.scope.scope_type(),
            scope_key: value.scope.scope_key(),
            mode: value.mode.as_str(),
            policy_id: value.policy_id,
            allow_fallback: value.allow_fallback,
            revision: value.revision,
        }
    }
}

pub async fn list_providers(
    State(state): State<AppState>,
    LiveAdminPrincipal(principal): LiveAdminPrincipal,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let ready = state
            .live
            .provider_client()
            .ok_or_else(service_unavailable)?
            .directory()
            .discover()
            .await
            .map_err(|error| {
                tracing::error!(error = %error, "Live admin provider discovery failed");
                service_unavailable()
            })?;
        let ready_protocols = ready
            .into_iter()
            .map(|provider| {
                (
                    provider.provider_id,
                    provider.contract.protocols().collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let providers = LiveProviderAdminRepository::new(state.db_pool.clone())
            .list(principal.home_id, &ready_protocols)
            .await
            .map_err(map_provider_admin_error)?;
        Ok(admin_response(StatusCode::OK, providers, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn egress_status(
    State(state): State<AppState>,
    LiveEgressAdminPrincipal(principal): LiveEgressAdminPrincipal,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        state
            .live
            .refresh_builtin_egress()
            .await
            .map_err(|_| service_unavailable())?;
        let assignments = EgressPolicyRepository::new(state.db_pool.clone())
            .assignments_for_home(principal.home_id)
            .await
            .map_err(map_egress_policy_error)?
            .into_iter()
            .map(EgressAssignmentDto::from)
            .collect();
        let config = &state.live.config().egress;
        let (
            enabled,
            ready,
            active_bindings,
            available_capacity,
            default_mode,
            default_policy_id,
            default_allow_fallback,
            profiles,
        ) = state
            .live
            .egress_service()
            .map(|service| {
                let status = service.status();
                (
                    status.enabled,
                    status.ready,
                    status.active_bindings,
                    status.available_capacity,
                    status.default_mode,
                    status.default_policy_id,
                    status.default_allow_fallback,
                    status
                        .profiles
                        .into_iter()
                        .map(|profile| EgressProfileDto {
                            id: profile.id,
                            name: profile.name,
                            kind: egress_profile_kind(profile.kind),
                            selectable_by_profiles: profile.selectable_by_profiles,
                        })
                        .collect(),
                )
            })
            .unwrap_or((
                state.live.config().protected_egress_enabled,
                false,
                0,
                0,
                config.default_mode,
                config.default_policy_id.clone(),
                config.default_allow_fallback,
                Vec::new(),
            ));
        let response = EgressStatusDto {
            enabled,
            ready,
            active_bindings,
            available_capacity,
            default_policy: EgressDefaultPolicyDto {
                mode: egress_default_mode(default_mode),
                policy_id: default_policy_id,
                allow_fallback: default_allow_fallback,
            },
            profiles,
            assignments,
        };
        Ok(admin_response(StatusCode::OK, response, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn update_egress_policy(
    State(state): State<AppState>,
    LiveEgressAdminPrincipal(principal): LiveEgressAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    payload: Result<Json<EgressPolicyUpdateRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let scope = match (request.scope_type, request.scope_id) {
            (EgressPolicyScopeRequest::ServerDefault, None) => PolicyScope::ServerDefault,
            (EgressPolicyScopeRequest::Profile, Some(id)) if !id.is_nil() => {
                PolicyScope::Profile(id)
            }
            (EgressPolicyScopeRequest::Provider, Some(id)) if !id.is_nil() => {
                PolicyScope::Provider(id)
            }
            _ => return Err(invalid_request()),
        };
        let mode = EgressPolicyMode::parse(&request.mode).map_err(|_| invalid_request())?;
        if mode != EgressPolicyMode::Off {
            let policy_id = request.policy_id.as_deref().ok_or_else(invalid_request)?;
            let egress = state
                .live
                .egress_service()
                .ok_or_else(service_unavailable)?;
            if egress.profile(policy_id).is_none() {
                return Err(invalid_request());
            }
        }
        let actor = actor(&principal)?;
        let audit = state.live.admin_audit().ok_or_else(service_unavailable)?;
        let assignment = EgressPolicyRepository::new(state.db_pool.clone())
            .upsert_audited(
                principal.home_id,
                scope,
                mode,
                request.policy_id.as_deref(),
                request.allow_fallback,
                request.expected_revision,
                &actor,
                &audit,
                Utc::now(),
            )
            .await
            .map_err(map_egress_policy_error)?;
        Ok(admin_response(
            StatusCode::OK,
            EgressAssignmentDto::from(assignment),
            request_id,
        ))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn disable_provider(
    State(state): State<AppState>,
    LiveProviderDisablePrincipal(principal): LiveProviderDisablePrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path(provider_id): Path<String>,
    payload: Result<Json<ProviderDisableRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let audit = state.live.admin_audit().ok_or_else(service_unavailable)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let mutation = LiveProviderAdminRepository::new(state.db_pool.clone())
            .disable(
                principal.home_id,
                provider_id,
                request.expected_revision,
                &actor,
                &audit,
            )
            .await
            .map_err(map_provider_admin_error)?;
        apply_provider_disable_revocations(&state, &mutation).await?;
        crate::http::handlers::extensions::trigger_extensions_reconcile(
            &state,
            "Live provider administrative disable",
        );
        Ok(admin_response(StatusCode::ACCEPTED, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn list_sessions(
    State(state): State<AppState>,
    LiveAdminPrincipal(principal): LiveAdminPrincipal,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let sessions = LiveSessionAdminRepository::new(
            state.db_pool.clone(),
            state
                .live
                .session_repository()
                .ok_or_else(service_unavailable)?,
        )
        .list(principal.home_id)
        .await
        .map_err(map_session_admin_error)?;
        Ok(admin_response(StatusCode::OK, sessions, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn terminate_session(
    State(state): State<AppState>,
    LiveAdminPrincipal(principal): LiveAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path(session_id): Path<String>,
    payload: Result<Json<SessionTerminateRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let session_id = parse_uuid(&session_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let audit = state.live.admin_audit().ok_or_else(service_unavailable)?;
        let fence = state
            .live
            .control_fencing_token()
            .await
            .ok_or_else(service_unavailable)?;
        let sessions = state
            .live
            .session_repository()
            .ok_or_else(service_unavailable)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let mutation = LiveSessionAdminRepository::new(state.db_pool.clone(), sessions)
            .terminate(
                principal.home_id,
                session_id,
                request.expected_revision,
                fence,
                &actor,
                &audit,
                Utc::now(),
            )
            .await
            .map_err(map_session_admin_error)?;
        super::sessions::end_delivery_runtime(&state, session_id, fence).await?;
        Ok(admin_response(StatusCode::OK, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn key_state(
    State(state): State<AppState>,
    LiveSecretsAdminPrincipal(principal): LiveSecretsAdminPrincipal,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let key_state = key_admin_service(&state)
            .await?
            .state()
            .await
            .map_err(map_key_admin_error)?;
        Ok(admin_response(StatusCode::OK, key_state, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn rotate_envelope_key(
    State(state): State<AppState>,
    LiveSecretsAdminPrincipal(principal): LiveSecretsAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    payload: Result<Json<KeyRotationRequest>, JsonRejection>,
) -> Response {
    rotate_key(
        state,
        principal,
        headers,
        raw_query,
        payload,
        KeyRotationRoute::Envelope,
    )
    .await
}

pub async fn rotate_token_hash_key(
    State(state): State<AppState>,
    LiveSecretsAdminPrincipal(principal): LiveSecretsAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    payload: Result<Json<KeyRotationRequest>, JsonRejection>,
) -> Response {
    rotate_key(
        state,
        principal,
        headers,
        raw_query,
        payload,
        KeyRotationRoute::TokenHash,
    )
    .await
}

pub async fn rotate_audit_key(
    State(state): State<AppState>,
    LiveSecretsAdminPrincipal(principal): LiveSecretsAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    payload: Result<Json<KeyRotationRequest>, JsonRejection>,
) -> Response {
    rotate_key(
        state,
        principal,
        headers,
        raw_query,
        payload,
        KeyRotationRoute::Audit,
    )
    .await
}

#[derive(Clone, Copy)]
enum KeyRotationRoute {
    Envelope,
    TokenHash,
    Audit,
}

async fn rotate_key(
    state: AppState,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    raw_query: Option<String>,
    payload: Result<Json<KeyRotationRequest>, JsonRejection>,
    route: KeyRotationRoute,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let service = key_admin_service(&state).await?;
        let now = Utc::now();
        let mutation = match route {
            KeyRotationRoute::Envelope => {
                let fence = state
                    .live
                    .control_fencing_token()
                    .await
                    .ok_or_else(service_unavailable)?;
                service
                    .rotate_envelope(
                        principal.home_id,
                        request.expected_revision,
                        &request.key_id,
                        fence,
                        &actor,
                        now,
                    )
                    .await
            }
            KeyRotationRoute::TokenHash => {
                let fence = state
                    .live
                    .control_fencing_token()
                    .await
                    .ok_or_else(service_unavailable)?;
                service
                    .rotate_token_hash(
                        principal.home_id,
                        request.expected_revision,
                        &request.key_id,
                        fence,
                        &actor,
                        now,
                    )
                    .await
            }
            KeyRotationRoute::Audit => {
                service
                    .rotate_audit(
                        principal.home_id,
                        request.expected_revision,
                        &request.key_id,
                        &actor,
                        now,
                    )
                    .await
            }
        }
        .map_err(map_key_admin_error)?;
        if matches!(route, KeyRotationRoute::TokenHash) && mutation.terminated_sessions > 0 {
            if let Some(relay) = state.live.relay_service() {
                relay.reap_stale().await;
            }
            if let Some(remux) = state.live.remux_service() {
                remux.reap_stale().await;
            }
        }
        Ok(admin_response(StatusCode::OK, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn list_destination_rules(
    State(state): State<AppState>,
    LiveDestinationAdminPrincipal(principal): LiveDestinationAdminPrincipal,
    RawQuery(raw_query): RawQuery,
    Path(provider_id): Path<String>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let rules = state
            .live
            .destination_rule_repository()
            .ok_or_else(service_unavailable)?
            .list(principal.home_id, provider_id)
            .await
            .map_err(map_destination_error)?;
        Ok(admin_response(StatusCode::OK, rules, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn create_destination_rule(
    State(state): State<AppState>,
    LiveDestinationAdminPrincipal(principal): LiveDestinationAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path(provider_id): Path<String>,
    payload: Result<Json<DestinationRuleCreateRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let mutation = state
            .live
            .destination_rule_repository()
            .ok_or_else(service_unavailable)?
            .create(
                principal.home_id,
                provider_id,
                request.expected_provider_revision,
                &actor,
                request.into_input(),
                Utc::now(),
            )
            .await
            .map_err(map_destination_error)?;
        Ok(admin_response(StatusCode::CREATED, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn update_destination_rule(
    State(state): State<AppState>,
    LiveDestinationAdminPrincipal(principal): LiveDestinationAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path((provider_id, rule_id)): Path<(String, String)>,
    payload: Result<Json<DestinationRuleUpdateRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let rule_id = parse_uuid(&rule_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let mutation = state
            .live
            .destination_rule_repository()
            .ok_or_else(service_unavailable)?
            .update(
                principal.home_id,
                provider_id,
                rule_id,
                request.expected_revision,
                &actor,
                request.into_input(),
                Utc::now(),
            )
            .await
            .map_err(map_destination_error)?;
        apply_revocation(&state, &mutation).await?;
        Ok(admin_response(StatusCode::OK, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn delete_destination_rule(
    State(state): State<AppState>,
    LiveDestinationAdminPrincipal(principal): LiveDestinationAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path((provider_id, rule_id)): Path<(String, String)>,
    payload: Result<Json<DestinationRuleDeleteRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let rule_id = parse_uuid(&rule_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let mutation = state
            .live
            .destination_rule_repository()
            .ok_or_else(service_unavailable)?
            .delete(
                principal.home_id,
                provider_id,
                rule_id,
                request.expected_revision,
                &actor,
                Utc::now(),
            )
            .await
            .map_err(map_destination_error)?;
        apply_revocation(&state, &mutation).await?;
        Ok(admin_response(StatusCode::OK, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn set_provider_grant(
    State(state): State<AppState>,
    LiveGrantAdminPrincipal(principal): LiveGrantAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path((provider_id, profile_id)): Path<(String, String)>,
    payload: Result<Json<ProviderGrantRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let profile_id = parse_uuid(&profile_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let audit = state.live.admin_audit().ok_or_else(service_unavailable)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let mutation =
            crate::live::catalog::LiveProviderGrantRepository::new(state.db_pool.clone())
                .set_grant_audited(
                    &actor,
                    profile_id,
                    provider_id,
                    request.can_browse,
                    request.can_play,
                    request.expected_revision,
                    None,
                    &audit,
                )
                .await
                .map_err(map_grant_error)?;
        apply_grant_revocation(&state, mutation.revocation_event_id).await?;
        Ok(admin_response(StatusCode::OK, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

pub async fn revoke_provider_grant(
    State(state): State<AppState>,
    LiveGrantAdminPrincipal(principal): LiveGrantAdminPrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path((provider_id, profile_id)): Path<(String, String)>,
    payload: Result<Json<DestinationRuleDeleteRequest>, JsonRejection>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        validate_mutation_transport(&state, &principal, &headers)?;
        let _admission = admit(principal.user_id)?;
        let provider_id = parse_uuid(&provider_id)?;
        let profile_id = parse_uuid(&profile_id)?;
        let request = payload.map_err(|_| invalid_request())?.0;
        let actor = actor(&principal)?;
        let audit = state.live.admin_audit().ok_or_else(service_unavailable)?;
        let _key_rotation = state.live.key_rotation_guard().await;
        let mutation =
            crate::live::catalog::LiveProviderGrantRepository::new(state.db_pool.clone())
                .set_grant_audited(
                    &actor,
                    profile_id,
                    provider_id,
                    false,
                    false,
                    request.expected_revision,
                    None,
                    &audit,
                )
                .await
                .map_err(map_grant_error)?;
        apply_grant_revocation(&state, mutation.revocation_event_id).await?;
        Ok(admin_response(StatusCode::OK, mutation, request_id))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

impl DestinationRuleCreateRequest {
    fn into_input(self) -> DestinationRuleInput {
        DestinationRuleInput {
            scheme: self.scheme,
            host: self.host,
            port: self.port,
            path: self.path,
            network_scope: self.network_scope,
            allow_fetch: self.allow_fetch,
            allow_credentials: self.allow_credentials,
            allow_client_disclosure: self.allow_client_disclosure,
        }
    }
}

impl DestinationRuleUpdateRequest {
    fn into_input(self) -> DestinationRuleInput {
        DestinationRuleInput {
            scheme: self.scheme,
            host: self.host,
            port: self.port,
            path: self.path,
            network_scope: self.network_scope,
            allow_fetch: self.allow_fetch,
            allow_credentials: self.allow_credentials,
            allow_client_disclosure: self.allow_client_disclosure,
        }
    }
}

async fn apply_revocation(
    state: &AppState,
    mutation: &DestinationRuleMutation,
) -> Result<(), LiveHttpRejection> {
    let Some(event) = mutation.revocation_event.as_ref() else {
        return Ok(());
    };
    state
        .auth_service
        .publish_authorization_revocation(event.id);
    state
        .live
        .drain_authorization_revocations()
        .await
        .map(|_| ())
        .map_err(|error| {
            tracing::error!(error = %error, "Live destination policy revocation failed closed");
            service_unavailable()
        })
}

async fn apply_grant_revocation(
    state: &AppState,
    event_id: Option<Uuid>,
) -> Result<(), LiveHttpRejection> {
    let Some(event_id) = event_id else {
        return Ok(());
    };
    state
        .auth_service
        .publish_authorization_revocation(event_id);
    state
        .live
        .drain_authorization_revocations()
        .await
        .map(|_| ())
        .map_err(|error| {
            tracing::error!(error = %error, "Live provider-grant revocation failed closed");
            service_unavailable()
        })
}

async fn apply_provider_disable_revocations(
    state: &AppState,
    mutation: &ProviderDisableMutation,
) -> Result<(), LiveHttpRejection> {
    if mutation.revocation_event_ids.is_empty() {
        return Ok(());
    }
    for event_id in &mutation.revocation_event_ids {
        state
            .auth_service
            .publish_authorization_revocation(*event_id);
    }
    state
        .live
        .drain_authorization_revocations()
        .await
        .map(|_| ())
        .map_err(|error| {
            tracing::error!(error = %error, "Live provider-disable revocation failed closed");
            service_unavailable()
        })
}

fn actor(principal: &CurrentPrincipal) -> Result<ActorSnapshot, LiveHttpRejection> {
    ActorSnapshot::new(
        principal.user_id,
        principal.profile_display_name.clone(),
        principal.role,
    )
    .map_err(|_| invalid_request())
}

fn parse_uuid(value: &str) -> Result<Uuid, LiveHttpRejection> {
    Uuid::parse_str(value).map_err(|_| invalid_request())
}

fn map_destination_error(error: LiveDestinationRuleError) -> LiveHttpRejection {
    match error {
        LiveDestinationRuleError::InvalidInput => invalid_request(),
        LiveDestinationRuleError::NotFound => LiveHttpRejection::new(
            StatusCode::NOT_FOUND,
            "LIVE_PROVIDER_NOT_FOUND",
            "The Live provider or destination rule was not found.",
            false,
        ),
        LiveDestinationRuleError::Forbidden => capability_required(),
        LiveDestinationRuleError::RevisionChanged => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_REVISION_CONFLICT",
            "The Live administrative revision changed.",
            false,
        ),
        LiveDestinationRuleError::Conflict => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_DESTINATION_RULE_CONFLICT",
            "The normalized Live destination rule already exists.",
            false,
        ),
        LiveDestinationRuleError::CapacityExceeded => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_DESTINATION_RULE_CONFLICT",
            "The Live destination rule limit was reached.",
            false,
        ),
        error @ (LiveDestinationRuleError::InvalidState
        | LiveDestinationRuleError::Storage(_)
        | LiveDestinationRuleError::Audit(_)
        | LiveDestinationRuleError::Revocation(_)
        | LiveDestinationRuleError::Serialization(_)) => {
            tracing::error!(error = %error, "Live destination rule operation failed");
            service_unavailable()
        }
    }
}

fn map_egress_policy_error(error: EgressPolicyRepositoryError) -> LiveHttpRejection {
    match error {
        EgressPolicyRepositoryError::Invalid => invalid_request(),
        EgressPolicyRepositoryError::RevisionChanged => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_SESSION_CONFLICT",
            "The Live egress policy changed. Refresh and retry.",
            true,
        ),
        EgressPolicyRepositoryError::ScopeForbidden => capability_required(),
        EgressPolicyRepositoryError::Database(_) | EgressPolicyRepositoryError::Audit(_) => {
            service_unavailable()
        }
    }
}

const fn egress_default_mode(mode: crate::live::config::LiveEgressDefaultMode) -> &'static str {
    match mode {
        crate::live::config::LiveEgressDefaultMode::Off => "off",
        crate::live::config::LiveEgressDefaultMode::PreferProtected => "prefer_protected",
        crate::live::config::LiveEgressDefaultMode::RequireProtected => "require_protected",
    }
}

const fn egress_profile_kind(kind: crate::live::config::LiveEgressProfileKind) -> &'static str {
    match kind {
        crate::live::config::LiveEgressProfileKind::Warp => "warp",
        crate::live::config::LiveEgressProfileKind::Wireguard => "wireguard",
        crate::live::config::LiveEgressProfileKind::Openvpn => "openvpn",
    }
}

fn map_grant_error(error: LiveProviderGrantError) -> LiveHttpRejection {
    match error {
        LiveProviderGrantError::InvalidInput => invalid_request(),
        LiveProviderGrantError::TargetUnavailable => LiveHttpRejection::new(
            StatusCode::NOT_FOUND,
            "LIVE_PROVIDER_NOT_FOUND",
            "The Live provider or target profile was not found.",
            false,
        ),
        LiveProviderGrantError::Forbidden => sharing_required(),
        LiveProviderGrantError::RevisionChanged => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_REVISION_CONFLICT",
            "The profile authorization revision changed.",
            false,
        ),
        error @ (LiveProviderGrantError::InvalidState
        | LiveProviderGrantError::Storage(_)
        | LiveProviderGrantError::Authorization(_)
        | LiveProviderGrantError::Revocation(_)
        | LiveProviderGrantError::Audit(_)
        | LiveProviderGrantError::Serialization(_)) => {
            tracing::error!(error = %error, "Live provider grant operation failed");
            service_unavailable()
        }
    }
}

fn map_provider_admin_error(error: LiveProviderAdminError) -> LiveHttpRejection {
    match error {
        LiveProviderAdminError::InvalidInput => invalid_request(),
        LiveProviderAdminError::NotFound => LiveHttpRejection::new(
            StatusCode::NOT_FOUND,
            "LIVE_PROVIDER_NOT_FOUND",
            "The Live provider was not found.",
            false,
        ),
        LiveProviderAdminError::Forbidden => extensions_manage_required(),
        LiveProviderAdminError::RevisionChanged => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_REVISION_CONFLICT",
            "The Live provider administrative revision changed.",
            false,
        ),
        error @ (LiveProviderAdminError::InvalidState
        | LiveProviderAdminError::Storage(_)
        | LiveProviderAdminError::Audit(_)
        | LiveProviderAdminError::Revocation(_)) => {
            tracing::error!(error = %error, "Live provider administration failed");
            service_unavailable()
        }
    }
}

fn map_session_admin_error(error: LiveSessionAdminError) -> LiveHttpRejection {
    match error {
        LiveSessionAdminError::InvalidInput
        | LiveSessionAdminError::Session(SessionRepositoryError::InvalidInput) => invalid_request(),
        LiveSessionAdminError::NotFound
        | LiveSessionAdminError::Session(SessionRepositoryError::NotFound) => {
            LiveHttpRejection::new(
                StatusCode::NOT_FOUND,
                "LIVE_SESSION_NOT_FOUND",
                "The Live session was not found.",
                false,
            )
        }
        LiveSessionAdminError::Forbidden => live_manage_required(),
        LiveSessionAdminError::RevisionChanged
        | LiveSessionAdminError::Session(SessionRepositoryError::RevisionChanged) => {
            LiveHttpRejection::new(
                StatusCode::CONFLICT,
                "LIVE_REVISION_CONFLICT",
                "The Live session administrative revision changed.",
                false,
            )
        }
        error @ (LiveSessionAdminError::InvalidState
        | LiveSessionAdminError::Storage(_)
        | LiveSessionAdminError::Audit(_)
        | LiveSessionAdminError::Session(_)) => {
            tracing::error!(error = %error, "Live session administration failed");
            service_unavailable()
        }
    }
}

fn map_key_admin_error(error: LiveKeyAdminError) -> LiveHttpRejection {
    match error {
        LiveKeyAdminError::InvalidInput => invalid_request(),
        LiveKeyAdminError::Forbidden => secrets_manage_required(),
        LiveKeyAdminError::RevisionChanged => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_REVISION_CONFLICT",
            "The Live key configuration revision changed.",
            false,
        ),
        LiveKeyAdminError::KeyNotConfigured => LiveHttpRejection::new(
            StatusCode::NOT_FOUND,
            "LIVE_KEY_NOT_CONFIGURED",
            "The requested Live key ID is not configured.",
            false,
        ),
        LiveKeyAdminError::CapacityExceeded => LiveHttpRejection::new(
            StatusCode::CONFLICT,
            "LIVE_KEY_ROTATION_INCOMPLETE",
            "The bounded Live key rotation could not complete.",
            true,
        ),
        error @ (LiveKeyAdminError::InvalidState
        | LiveKeyAdminError::Storage(_)
        | LiveKeyAdminError::Crypto(_)
        | LiveKeyAdminError::Session(_)
        | LiveKeyAdminError::Audit(_)
        | LiveKeyAdminError::SecretStore(_)) => {
            tracing::error!(error = %error, "Live key administration failed");
            service_unavailable()
        }
    }
}

fn invalid_request() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::BAD_REQUEST,
        "LIVE_INVALID_REQUEST",
        "The Live destination rule request is invalid.",
        false,
    )
}

fn capability_required() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::FORBIDDEN,
        "LIVE_CAPABILITY_REQUIRED",
        "Only an authorized home owner may manage Live destination rules.",
        false,
    )
}

fn sharing_required() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::FORBIDDEN,
        "LIVE_CAPABILITY_REQUIRED",
        "The active profile cannot manage Live provider sharing.",
        false,
    )
}

fn live_manage_required() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::FORBIDDEN,
        "LIVE_CAPABILITY_REQUIRED",
        "The active profile cannot administer Live providers.",
        false,
    )
}

fn extensions_manage_required() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::FORBIDDEN,
        "LIVE_CAPABILITY_REQUIRED",
        "The active profile cannot disable Live providers.",
        false,
    )
}

fn secrets_manage_required() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::FORBIDDEN,
        "LIVE_CAPABILITY_REQUIRED",
        "The active profile cannot rotate Live cryptographic keys.",
        false,
    )
}

async fn key_admin_service(state: &AppState) -> Result<LiveKeyAdminService, LiveHttpRejection> {
    Ok(LiveKeyAdminService::new(
        state.db_pool.clone(),
        state
            .live
            .session_repository()
            .ok_or_else(service_unavailable)?,
        state.live.crypto().await.ok_or_else(service_unavailable)?,
        state.live.admin_audit().ok_or_else(service_unavailable)?,
        state.secrets.clone(),
    ))
}

fn service_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_PROVIDER_UNAVAILABLE",
        "The Live administrative service is unavailable.",
        true,
    )
}

#[derive(Serialize)]
struct AdminEnvelope<T: Serialize> {
    data: T,
    meta: AdminMeta,
    errors: Vec<Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminMeta {
    request_id: String,
    generated_at: String,
    cache_state: &'static str,
    partial: bool,
}

fn admin_response<T: Serialize>(status: StatusCode, data: T, request_id: Uuid) -> Response {
    let mut response = (
        status,
        Json(AdminEnvelope {
            data,
            meta: AdminMeta {
                request_id: request_id.to_string(),
                generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                cache_state: "none",
                partial: false,
            },
            errors: Vec::new(),
        }),
    )
        .into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("vary", HeaderValue::from_static("Authorization, Cookie"));
    response
}
