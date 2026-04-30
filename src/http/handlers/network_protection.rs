use axum::{Json, extract::State};

use crate::{
    http::error::{ApiError, ApiResult},
    network::protection::{
        CloudflareWarpDiagnostics, CloudflareWarpDisclosure, CloudflareWarpProfileRequest,
        CloudflareWarpProfileResponse, CloudflareWarpResetRequest, CloudflareWarpResetResponse,
        DownloadNetworkEventRecord, DownloadNetworkProfileImportRequest,
        DownloadNetworkProfileImportResponse, DownloadProtectionFirstRunRequest,
        DownloadProtectionFirstRunResponse, DownloadProtectionProfilesResponse,
        DownloadProtectionStatus, DownloadProtectionSwitchRequest,
        DownloadProtectionSwitchResponse, DownloadProviderPresetCatalog,
        QbittorrentListenPortSyncApplyResponse, QbittorrentListenPortSyncPlan,
        QbittorrentListenPortSyncStatus,
        apply_download_protection_first_run_choice_with_orchestrated_apply,
        cloudflare_warp_diagnostics, cloudflare_warp_disclosure, download_provider_preset_catalog,
        ensure_cloudflare_warp_profile, import_openvpn_profile, import_wireguard_profile,
        list_download_network_events, list_download_network_profiles,
        observed_download_protection_status_with_evidence, qbittorrent_listen_port_sync_plan,
        reset_cloudflare_warp_profile, switch_download_protection_profile_with_orchestrated_apply,
    },
    orchestrator::executor::ExecutorAction,
    orchestrator::service::DownloadNetworkPreflightReport,
    state::AppState,
};

pub async fn protection(
    State(state): State<AppState>,
) -> ApiResult<Json<DownloadProtectionStatus>> {
    protection_status(State(state)).await
}

pub async fn protection_status(
    State(state): State<AppState>,
) -> ApiResult<Json<DownloadProtectionStatus>> {
    state
        .orchestrator
        .refresh_active_download_profile_runtime_status()
        .await?;
    let evidence = state
        .orchestrator
        .download_protection_runtime_evidence()
        .await?;
    let status = observed_download_protection_status_with_evidence(
        &state.settings,
        &state.db_pool,
        &state.secrets,
        Some(&evidence),
    )
    .await?;
    Ok(Json(status))
}

pub async fn preflight(
    State(state): State<AppState>,
) -> ApiResult<Json<DownloadNetworkPreflightReport>> {
    Ok(Json(
        state
            .orchestrator
            .preflight_active_download_profile_rehome()
            .await?,
    ))
}

pub async fn switch_profile(
    State(state): State<AppState>,
    Json(request): Json<DownloadProtectionSwitchRequest>,
) -> ApiResult<Json<DownloadProtectionSwitchResponse>> {
    let apply_state = state.clone();
    let evidence_state = state.clone();
    let response = switch_download_protection_profile_with_orchestrated_apply(
        &state.settings,
        &state.db_pool,
        &state.secrets,
        request,
        move || {
            let apply_state = apply_state.clone();
            async move {
                apply_state
                    .orchestrator
                    .apply_active_download_profile_to_managed_downloaders()
                    .await
                    .map(|_| ())
            }
        },
        move || {
            let evidence_state = evidence_state.clone();
            async move {
                evidence_state
                    .orchestrator
                    .refresh_active_download_profile_runtime_status()
                    .await?;
                evidence_state
                    .orchestrator
                    .download_protection_runtime_evidence()
                    .await
            }
        },
    )
    .await
    .map_err(|err| {
        let message = err.to_string();
        if message.contains("not found") {
            ApiError::not_found(message)
        } else if message.contains("targetProfileId is required") {
            ApiError::bad_request(message)
        } else {
            ApiError::internal(message)
        }
    })?;
    Ok(Json(response))
}

pub async fn first_run_setup(
    State(state): State<AppState>,
    Json(request): Json<DownloadProtectionFirstRunRequest>,
) -> ApiResult<Json<DownloadProtectionFirstRunResponse>> {
    let apply_state = state.clone();
    let evidence_state = state.clone();
    let response = apply_download_protection_first_run_choice_with_orchestrated_apply(
        &state.settings,
        &state.db_pool,
        &state.secrets,
        request,
        move || {
            let apply_state = apply_state.clone();
            async move {
                apply_state
                    .orchestrator
                    .apply_active_download_profile_to_managed_downloaders()
                    .await
                    .map(|_| ())
            }
        },
        move || {
            let evidence_state = evidence_state.clone();
            async move {
                evidence_state
                    .orchestrator
                    .refresh_active_download_profile_runtime_status()
                    .await?;
                evidence_state
                    .orchestrator
                    .download_protection_runtime_evidence()
                    .await
            }
        },
    )
    .await
    .map_err(|err| {
        let message = err.to_string();
        if message.contains("disclosure must be accepted") {
            ApiError::bad_request(message)
        } else if message.contains("not found") {
            ApiError::not_found(message)
        } else {
            ApiError::internal(message)
        }
    })?;
    Ok(Json(response))
}

pub async fn events(
    State(state): State<AppState>,
) -> ApiResult<Json<Vec<DownloadNetworkEventRecord>>> {
    Ok(Json(
        list_download_network_events(&state.db_pool, 50).await?,
    ))
}

pub async fn profiles(
    State(state): State<AppState>,
) -> ApiResult<Json<DownloadProtectionProfilesResponse>> {
    Ok(Json(list_download_network_profiles(&state.db_pool).await?))
}

pub async fn import_wireguard(
    State(state): State<AppState>,
    Json(request): Json<DownloadNetworkProfileImportRequest>,
) -> ApiResult<Json<DownloadNetworkProfileImportResponse>> {
    let response = import_wireguard_profile(&state.db_pool, &state.secrets, request)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    Ok(Json(response))
}

pub async fn import_openvpn(
    State(state): State<AppState>,
    Json(request): Json<DownloadNetworkProfileImportRequest>,
) -> ApiResult<Json<DownloadNetworkProfileImportResponse>> {
    let response = import_openvpn_profile(&state.db_pool, &state.secrets, request)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    Ok(Json(response))
}

pub async fn provider_presets() -> ApiResult<Json<DownloadProviderPresetCatalog>> {
    Ok(Json(download_provider_preset_catalog()))
}

pub async fn qbittorrent_listen_port_sync(
    State(state): State<AppState>,
) -> ApiResult<Json<QbittorrentListenPortSyncPlan>> {
    let plan =
        qbittorrent_listen_port_sync_plan(&state.settings, &state.db_pool, &state.secrets).await?;
    Ok(Json(plan))
}

pub async fn apply_qbittorrent_listen_port_sync(
    State(state): State<AppState>,
) -> ApiResult<Json<QbittorrentListenPortSyncApplyResponse>> {
    let plan =
        qbittorrent_listen_port_sync_plan(&state.settings, &state.db_pool, &state.secrets).await?;
    if plan.status != QbittorrentListenPortSyncStatus::Ready {
        return Ok(Json(QbittorrentListenPortSyncApplyResponse {
            applied: false,
            plan,
            notes: Vec::new(),
        }));
    }

    let target_provider_id = plan.target_provider_id.ok_or_else(|| {
        ApiError::bad_request(
            "qBittorrent listen-port sync is ready but no target provider was resolved.",
        )
    })?;
    let patch = plan.patch.clone().ok_or_else(|| {
        ApiError::bad_request("qBittorrent listen-port sync is ready but no patch was generated.")
    })?;

    let notes = state
        .orchestrator
        .apply_actions_with_notes(vec![
            ExecutorAction::TransportGate {
                provider_id: target_provider_id,
                timeout_seconds: 30,
            },
            ExecutorAction::BootstrapGate {
                provider_id: target_provider_id,
                timeout_seconds: 30,
            },
            ExecutorAction::HealthGate {
                provider_id: target_provider_id,
                timeout_seconds: 30,
            },
            ExecutorAction::ApplyDriverPatch {
                connector_extension_id: "elixir.network.protection.listen_port_sync".to_string(),
                target_provider_id,
                patch,
            },
        ])
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?;
    let refreshed_plan =
        qbittorrent_listen_port_sync_plan(&state.settings, &state.db_pool, &state.secrets).await?;

    Ok(Json(QbittorrentListenPortSyncApplyResponse {
        applied: true,
        plan: refreshed_plan,
        notes,
    }))
}

pub async fn warp_disclosure() -> ApiResult<Json<CloudflareWarpDisclosure>> {
    Ok(Json(cloudflare_warp_disclosure()))
}

pub async fn ensure_warp_profile(
    State(state): State<AppState>,
    Json(request): Json<CloudflareWarpProfileRequest>,
) -> ApiResult<Json<CloudflareWarpProfileResponse>> {
    let response = ensure_cloudflare_warp_profile(&state.db_pool, &state.secrets, request)
        .await
        .map_err(|err| {
            let message = err.to_string();
            if message.contains("disclosure must be accepted") {
                ApiError::bad_request(message)
            } else {
                ApiError::internal(message)
            }
        })?;
    Ok(Json(response))
}

pub async fn warp_diagnostics(
    State(state): State<AppState>,
) -> ApiResult<Json<CloudflareWarpDiagnostics>> {
    state
        .orchestrator
        .refresh_active_download_profile_runtime_status()
        .await?;
    Ok(Json(cloudflare_warp_diagnostics(&state.db_pool).await?))
}

pub async fn reset_warp_profile(
    State(state): State<AppState>,
    Json(request): Json<CloudflareWarpResetRequest>,
) -> ApiResult<Json<CloudflareWarpResetResponse>> {
    let response = reset_cloudflare_warp_profile(&state.db_pool, &state.secrets, request)
        .await
        .map_err(|err| {
            let message = err.to_string();
            if message.contains("confirmReset=true") {
                ApiError::bad_request(message)
            } else {
                ApiError::internal(message)
            }
        })?;
    Ok(Json(response))
}
