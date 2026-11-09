//! Owner choices and protected discovery. Provider setup saves explicit consent
//! for the existing startup owner; no handler registers/starts a provider, issues
//! paid credentials or turns a search result into install/runtime authority.
use crate::{
    auth::AuthService,
    db::Database,
    http::{
        auth::{AccountAuthTransport, CurrentPrincipal},
        error::{ApiError, ApiResult},
    },
    marketplace::{
        preferences::{self, Change, ChangeError, Snapshot},
        service::{RuntimePhase, RuntimeSnapshot},
        worker::PARTICIPATION_DISCLOSURE_VERSION,
    },
    state::AppState,
};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use chrono::Utc;
use serde::Serialize;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;
#[cfg(unix)]
mod installation;
#[cfg(all(test, unix))]
pub(crate) use installation::authenticated_fixture as installation_http_fixture;
#[cfg(unix)]
pub(crate) use installation::{
    installation_read_routes, installation_routes, private_response, targets,
};

#[derive(Serialize)]
pub struct SettingsResponse {
    settings: Snapshot,
    contribution_disclosure_version: u32,
    contribution_disclosure: &'static str,
    issuer_disclosure_version: u32,
    issuer_disclosure: &'static str,
    /// Saving a preference is not proof of any applied network role.
    runtime_status: RuntimePhase,
    runtime: RuntimeSnapshot,
    /// Applies only to participation roles, never to account-bound paid-support
    /// authorization or publication of endorsements.
    participation_applied: bool,
    /// Progress only: never protection or role authority.
    #[serde(skip_serializing_if = "Option::is_none")]
    startup: Option<crate::marketplace::service::StartupView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_setup: Option<crate::marketplace::provider_setup::View>,
}
fn response(settings: Snapshot, runtime: RuntimeSnapshot) -> SettingsResponse {
    let participation_applied = runtime.participation_applied(&settings);
    let runtime_status = runtime.phase;
    SettingsResponse {
        settings,
        contribution_disclosure_version: PARTICIPATION_DISCLOSURE_VERSION,
        contribution_disclosure: "Help power the extension marketplace: eligible servers may share verified installed extensions and bounded catalog/routing data through protected networking. Contribution is optional; off preserves owner-requested searches and downloads. Background limits: 256 MiB memory, 0.1 CPU core, 512 MiB automatic state, 32 KiB/s upload, 128 KiB/s download and 1 GiB combined monthly transfer. Unknown or metered connections do not contribute automatically. Availability and safe code are not guaranteed by popularity.",
        issuer_disclosure_version: preferences::ISSUER_DISCLOSURE_VERSION,
        issuer_disclosure: "Optional public paid support is separate from network contribution. Elixir's paid-account service learns which account requests a credential for which extension. Public peers receive an extension-specific support subject, tenure weight and timing, not account credentials or identifiers. Requests are not proof of a download, installation or execution. Search queries, installation reports and execution telemetry are not required for issuance. Published signed support may remain with other peers until expiry even after consent is withdrawn.",
        runtime_status,
        runtime,
        participation_applied,
        startup: None,
        provider_setup: None,
    }
}
fn database(state: &AppState) -> Database {
    Database {
        pool: state.db_pool.clone(),
        driver: state.db_driver,
    }
}
pub async fn import_preview(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<crate::marketplace::direct_import::ImportPreview>> {
    validate_mutation_transport(
        &state.auth_service,
        principal.transport,
        principal.account_session_id,
        principal.csrf_revision,
        &headers,
    )?;
    timeout(
        Duration::from_secs(10),
        preferences::load(&database(&state), &principal, Utc::now()),
    )
    .await
    .map_err(|_| ApiError::internal("marketplace owner authorization deadline"))?
    .map_err(map_error)?;
    let publication = elixir_marketplace::publication::PreparedPublication::parse(&bytes)
        .map_err(|_| ApiError::bad_request("invalid signed direct reference"))?;
    // One bounded pre-launch health cycle (72s), launch/join (10s/20s)
    // and ownership readback must fit without relaxing native retirement.
    let preview = timeout(Duration::from_secs(120), state.marketplace.import_preview(publication)).await
        .map_err(|_| ApiError::structured(StatusCode::SERVICE_UNAVAILABLE, "marketplace_unavailable",
            "Direct-reference checks timed out. Nothing was installed.", None))?
        .map_err(|error| {
            let (stage, reason) = preview_failure_diagnostic(&error);
            // Closed internal categories only. Never log raw errors, signed
            // input, credentials, paths, addresses or provider response data.
            tracing::warn!(stage, reason, "direct-reference preview unavailable");
            ApiError::structured(StatusCode::SERVICE_UNAVAILABLE, "marketplace_unavailable",
            "The reference could not be verified against protected networking and retained security state. Nothing was installed.", None)
        })?;
    // Authorization may have changed during the independent process/storage
    // work. A preview does not outlive the requesting owner/session revision.
    timeout(
        Duration::from_secs(10),
        preferences::load(&database(&state), &principal, Utc::now()),
    )
    .await
    .map_err(|_| ApiError::internal("marketplace owner reauthorization deadline"))?
    .map_err(map_error)?;
    Ok(Json(preview))
}
fn preview_failure_diagnostic(error: &anyhow::Error) -> (&'static str, &'static str) {
    let mut stage = "owner";
    let mut reason = "verification_not_confirmed";
    for cause in error.chain() {
        // Compare only fixed internal contexts; do not emit their data.
        let text = cause.to_string();
        match text.as_str() {
            "direct preview signature verification" => stage = "signatures",
            "direct preview native signature receipt" => stage = "signatures",
            "direct preview security reconciliation" => stage = "security_reconciliation",
            "direct preview retained security check" => stage = "retained_security",
            "source/verification slot is busy" => reason = "slot_busy",
            "direct-reference verification slot deadline" => reason = "slot_deadline",
            "source preflight admission deadline" => reason = "headroom_deadline",
            "source preflight needs headroom inside its existing worker allocation" => {
                reason = "headroom"
            }
            "marketplace helper protected readiness deadline" => {
                reason = "protection_wait_deadline"
            }
            "direct-reference verifier startup deadline" => reason = "native_start_deadline",
            "direct-reference verifier retirement deadline" => {
                reason = "native_retirement_deadline"
            }
            "foreground helper protection is not admitted (Expired)" => {
                reason = "protection_expired"
            }
            "foreground helper protection is not admitted (Failed)" => reason = "protection_failed",
            "foreground helper owner has failed" => reason = "owner_failed",
            _ => (),
        }
    }
    (stage, reason)
}

pub async fn start_search(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    validate_mutation_transport(
        &state.auth_service,
        principal.transport,
        principal.account_session_id,
        principal.csrf_revision,
        &headers,
    )?;
    timeout(
        Duration::from_secs(10),
        preferences::load(&database(&state), &principal, Utc::now()),
    )
    .await
    .map_err(|_| ApiError::internal("marketplace owner authorization deadline"))?
    .map_err(map_error)?;
    let request = crate::marketplace::search::SearchRequest::parse(&bytes)
        .map_err(|_| ApiError::bad_request("invalid marketplace search request"))?;
    let id = state.marketplace.start_search(request).map_err(|_| {
        ApiError::structured(
            StatusCode::SERVICE_UNAVAILABLE,
            "marketplace_unavailable",
            "Protected marketplace search is not connected.",
            None,
        )
    })?;
    Ok(Json(serde_json::json!({"schema_version":1,"query_id":id})))
}
pub async fn search_view(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<crate::marketplace::search::SearchView>> {
    timeout(
        Duration::from_secs(10),
        preferences::load(&database(&state), &principal, Utc::now()),
    )
    .await
    .map_err(|_| ApiError::internal("marketplace owner authorization deadline"))?
    .map_err(map_error)?;
    let view = timeout(Duration::from_secs(15), state.marketplace.search_view(id))
        .await
        .map_err(|_| {
            ApiError::structured(
                StatusCode::SERVICE_UNAVAILABLE,
                "marketplace_unavailable",
                "Marketplace search checks are unavailable.",
                None,
            )
        })?
        .map_err(|_| {
            ApiError::conflict(
                "Marketplace search was replaced, expired, stopped or is busy. Retry the search.",
            )
        })?;
    Ok(Json(view))
}
pub async fn settings(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
) -> ApiResult<Json<SettingsResponse>> {
    let snapshot = timeout(
        Duration::from_secs(10),
        preferences::load(&database(&state), &principal, Utc::now()),
    )
    .await
    .map_err(|_| ApiError::internal("marketplace settings deadline"))?
    .map_err(map_error)?;
    let mut reply = response(snapshot, state.marketplace.snapshot());
    reply.startup = Some(state.marketplace.startup_snapshot());
    reply.provider_setup = provider_setup_view(&state, &principal).await?;
    Ok(Json(reply))
}
pub async fn update_settings(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<SettingsResponse>> {
    validate_mutation_transport(
        &state.auth_service,
        principal.transport,
        principal.account_session_id,
        principal.csrf_revision,
        &headers,
    )?;
    let change = Change::parse(&bytes)
        .map_err(|_| ApiError::bad_request("invalid marketplace settings request"))?;
    let snapshot = timeout(
        Duration::from_secs(10),
        preferences::save(&database(&state), &principal, &change, Utc::now()),
    )
    .await
    .map_err(|_| {
        ApiError::internal("marketplace settings commit outcome is unknown; reload before retrying")
    })?
    .map_err(map_error)?;
    let mut reply = response(snapshot, state.marketplace.snapshot());
    reply.startup = Some(state.marketplace.startup_snapshot());
    reply.provider_setup = provider_setup_view(&state, &principal).await?;
    Ok(Json(reply))
}

async fn provider_setup_view(
    state: &AppState,
    principal: &CurrentPrincipal,
) -> ApiResult<Option<crate::marketplace::provider_setup::View>> {
    if !principal.has_capability(crate::authz::Capability::SecretsManage) {
        return Ok(None);
    }
    timeout(Duration::from_secs(10), async {
        let configuration =
            crate::marketplace::startup::customer_provider_configuration(state).await?;
        match configuration {
            Some(configuration) => crate::marketplace::provider_setup::load(
                &database(state),
                principal,
                configuration,
                Utc::now(),
            )
            .await
            .map(Some),
            None => Ok(None),
        }
    })
    .await
    .map_err(|_| {
        ApiError::structured(
            StatusCode::SERVICE_UNAVAILABLE,
            "marketplace_setup_unavailable",
            "Marketplace provider setup is unavailable. Reload before making changes.",
            None,
        )
    })?
    .map_err(|_| {
        ApiError::structured(
            StatusCode::SERVICE_UNAVAILABLE,
            "marketplace_setup_unavailable",
            "Marketplace provider setup is unavailable. Reload before making changes.",
            None,
        )
    })
}

/// Consent/configuration DATA only. No browser-selected provider/image/path or
/// native call. A changed decision invalidates the old protected client and
/// wakes only the already configured startup/recovery owner after commit.
pub async fn update_provider_setup(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<SettingsResponse>> {
    authorize_network_configuration(&state, &principal, &headers).await?;
    let change = crate::marketplace::provider_setup::Change::parse(&bytes).map_err(|_| {
        ApiError::bad_request("invalid provider setup choice or disclosure version")
    })?;
    let configuration = timeout(
        Duration::from_secs(5),
        crate::marketplace::startup::customer_provider_configuration(&state),
    )
    .await
    .map_err(|_| ApiError::conflict("Provider setup configuration is unavailable."))?
    .map_err(|_| ApiError::conflict("Provider setup configuration is unavailable."))?
    .ok_or_else(|| {
        ApiError::conflict("The server has not selected customer marketplace provider setup.")
    })?;
    let (_, changed) = timeout(
        Duration::from_secs(10),
        crate::marketplace::provider_setup::save(
            &database(&state),
            &principal,
            configuration,
            &change,
            Utc::now(),
        ),
    )
    .await
    .map_err(|_| {
        ApiError::internal("Provider choice outcome is unconfirmed. Reload before retrying.")
    })?
    .map_err(|_| {
        ApiError::conflict(
            "Provider choice was not confirmed. Reload current owner/settings before retrying.",
        )
    })?;
    if changed {
        state.marketplace.provider_configuration_changed();
    }
    // Returning the existing normal settings shape also performs fresh owner
    // authorization; an uncertain HTTP readback never repeats the mutation.
    settings(State(state), principal).await
}
/// Hint only the already configured retry owner. This cannot enroll a provider,
/// start another task, bypass backoff/retirement, or assert a protected path.
pub async fn retry_connection(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    authorize_network_configuration(&state, &principal, &headers).await?;
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        schema_version: u32,
    }
    let request: Request = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::bad_request("invalid marketplace connection retry"))?;
    if request.schema_version != 1 {
        return Err(ApiError::bad_request(
            "unsupported marketplace connection retry",
        ));
    }
    Ok(Json(serde_json::json!({"schema_version":1,
        "requested":state.marketplace.connection_configuration_changed()})))
}
fn map_error(error: anyhow::Error) -> ApiError {
    match error.downcast_ref::<ChangeError>() {
        Some(ChangeError::Invalid) => {
            ApiError::bad_request("invalid marketplace preferences or disclosure version")
        }
        Some(ChangeError::Conflict) => {
            ApiError::conflict("marketplace preferences changed; reload before retrying")
        }
        Some(ChangeError::Forbidden) => {
            ApiError::forbidden("current server owner authorization is required")
        }
        Some(ChangeError::OwnerSetupRequired) => ApiError::conflict(
            "marketplace owner setup is ambiguous; trusted local setup is required",
        ),
        None => {
            tracing::error!(error=%error, "marketplace settings are unavailable");
            ApiError::internal("marketplace settings are unavailable")
        }
    }
}
/// Marketplace-purpose credential/configuration writes reuse the exact owner,
/// home, profile/session and cookie-CSRF checks; legacy network routes do not
/// supply a blanket authentication layer on their own.
pub(super) async fn authorize_network_configuration(
    state: &AppState,
    principal: &CurrentPrincipal,
    headers: &HeaderMap,
) -> ApiResult<()> {
    principal.require(crate::authz::Capability::SecretsManage)?;
    validate_mutation_transport(
        &state.auth_service,
        principal.transport,
        principal.account_session_id,
        principal.csrf_revision,
        headers,
    )?;
    timeout(
        Duration::from_secs(10),
        preferences::load(&database(state), principal, Utc::now()),
    )
    .await
    .map_err(|_| ApiError::internal("marketplace connection authorization deadline"))?
    .map_err(map_error)?;
    Ok(())
}
fn validate_mutation_transport(
    auth: &AuthService,
    transport: AccountAuthTransport,
    session: Uuid,
    revision: i32,
    headers: &HeaderMap,
) -> ApiResult<()> {
    if transport == AccountAuthTransport::Bearer {
        return Ok(());
    }
    if transport != AccountAuthTransport::Cookie {
        return Err(ApiError::forbidden(
            "query tokens cannot change marketplace settings",
        ));
    }
    let mut values = headers.get_all("x-elixir-csrf").iter();
    let provided = values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::forbidden("cookie mutations require a CSRF token"))?;
    if values.next().is_some() {
        return Err(ApiError::forbidden("ambiguous CSRF token"));
    }
    let expected = auth
        .csrf_token(session, revision)
        .map_err(|_| ApiError::forbidden("invalid CSRF token"))?;
    // A non-ambient, session/revision-bound token is required even if a browser
    // can send the cookie. Never compare secrets with an early-exit byte loop.
    let expected = expected.expose_secret().as_bytes();
    if provided.len() != expected.len()
        || provided
            .as_bytes()
            .iter()
            .zip(expected)
            .fold(0u8, |difference, (a, b)| difference | (a ^ b))
            != 0
    {
        return Err(ApiError::forbidden("invalid CSRF token"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;
    #[test]
    fn preview_diagnostics_report_only_closed_stage_and_reason() {
        let error = anyhow::anyhow!("source/verification slot is busy")
            .context("direct preview signature verification");
        assert_eq!(
            preview_failure_diagnostic(&error),
            ("signatures", "slot_busy")
        );
        let error = anyhow::anyhow!("arbitrary private account, address, path or input")
            .context("direct preview retained security check");
        assert_eq!(
            preview_failure_diagnostic(&error),
            ("retained_security", "verification_not_confirmed")
        );
        assert_eq!(
            preview_failure_diagnostic(&anyhow::anyhow!("untrusted input")),
            ("owner", "verification_not_confirmed")
        );
    }

    #[test]
    fn preview_wait_and_retirement_diagnostics_remain_distinct() {
        for (context, expected) in [
            (
                "direct-reference verification slot deadline",
                "slot_deadline",
            ),
            (
                "marketplace helper protected readiness deadline",
                "protection_wait_deadline",
            ),
            (
                "direct-reference verifier startup deadline",
                "native_start_deadline",
            ),
            (
                "direct-reference verifier retirement deadline",
                "native_retirement_deadline",
            ),
            ("source preflight admission deadline", "headroom_deadline"),
        ] {
            let error = anyhow::anyhow!("private nested details")
                .context(context)
                .context("direct preview signature verification");
            assert_eq!(preview_failure_diagnostic(&error), ("signatures", expected));
        }
    }

    #[test]
    fn cookie_mutations_require_one_exact_session_and_revision_bound_csrf_token()
    -> anyhow::Result<()> {
        let auth = AuthService::new(AuthConfig::default())?;
        let session = Uuid::new_v4();
        let mut headers = HeaderMap::new();
        assert!(
            validate_mutation_transport(&auth, AccountAuthTransport::Bearer, session, 1, &headers)
                .is_ok()
        );
        assert!(
            validate_mutation_transport(&auth, AccountAuthTransport::Query, session, 1, &headers)
                .is_err()
        );
        assert!(
            validate_mutation_transport(&auth, AccountAuthTransport::Cookie, session, 1, &headers)
                .is_err()
        );
        headers.insert(
            "x-elixir-csrf",
            auth.csrf_token(session, 1)?.expose_secret().parse()?,
        );
        validate_mutation_transport(&auth, AccountAuthTransport::Cookie, session, 1, &headers)
            .map_err(|_| anyhow::anyhow!("valid CSRF fixture rejected"))?;
        assert!(
            validate_mutation_transport(&auth, AccountAuthTransport::Cookie, session, 2, &headers)
                .is_err()
        );
        assert!(
            validate_mutation_transport(
                &auth,
                AccountAuthTransport::Cookie,
                Uuid::new_v4(),
                1,
                &headers
            )
            .is_err()
        );
        headers.append("x-elixir-csrf", "duplicate".parse()?);
        assert!(
            validate_mutation_transport(&auth, AccountAuthTransport::Cookie, session, 1, &headers)
                .is_err()
        );
        Ok(())
    }
}
