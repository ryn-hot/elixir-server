//! Real owner-management handlers. Mutations are mounted only by explicit
//! trusted local prelaunch configuration, never an HTTP opt-in. Every handler
//! still requires current owner/transport and original operation checks.
use super::*;
use crate::marketplace::service::management::{
    self, ActionRequest, DirectInstallRequest, InstallRequest, TargetRequest,
};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderValue, header},
    middleware,
    response::Response,
    routing::{get, post},
};

pub(crate) async fn private_response(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    response
}

async fn authorize(state: &AppState, principal: &CurrentPrincipal) -> ApiResult<()> {
    timeout(
        Duration::from_secs(10),
        preferences::load(&database(state), principal, Utc::now()),
    )
    .await
    .map_err(|_| ApiError::internal("marketplace owner authorization deadline"))?
    .map_err(map_error)?;
    Ok(())
}
fn mutation(state: &AppState, principal: &CurrentPrincipal, headers: &HeaderMap) -> ApiResult<()> {
    validate_mutation_transport(
        &state.auth_service,
        principal.transport,
        principal.account_session_id,
        principal.csrf_revision,
        headers,
    )
}
pub(crate) async fn targets(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<management::TargetSnapshot>> {
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let request = TargetRequest::parse(&bytes)
        .map_err(|_| ApiError::bad_request("invalid bounded installation target request"))?;
    let mut view=timeout(Duration::from_secs(10),state.marketplace.installation_targets(&database(&state),&principal,request)).await
        .map_err(|_|ApiError::structured(StatusCode::SERVICE_UNAVAILABLE,"marketplace_targets_unavailable","Target checks timed out. No installation was started.",None))?
        .map_err(|error|if error.is::<management::TargetAuthorization>() {
            ApiError::forbidden("current server owner authorization is required")
        } else {ApiError::conflict("Target choices are busy, unmanaged, changed or need recovery. Refresh before installing.")})?;
    view.advertise_review(
        state
            .settings
            .marketplace
            .installation_management_configured()
            && state.marketplace.installation_review_available(),
    );
    Ok(Json(view))
}
async fn start(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<management::Started>> {
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let request = InstallRequest::parse(&bytes)
        .map_err(|_| ApiError::bad_request("invalid original installation request"))?;
    // Synchronous ownership before capture/selection; no timeout/drop can lose
    // a task after SQL or native work begins. No allocation comes from bytes.
    let started=state.marketplace.request_install(&principal,request,state.secrets.as_ref().clone())
        .map_err(|_|ApiError::structured(StatusCode::SERVICE_UNAVAILABLE,"marketplace_install_unavailable","Installation is not admitted, already owned or stopped. Poll an existing operation before retrying.",None))?;
    Ok(Json(started))
}
async fn start_direct(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<management::Started>> {
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let request = DirectInstallRequest::parse(&bytes).map_err(|_| {
        ApiError::bad_request(
            "explicit acceptance of the exact signed developer reference is required",
        )
    })?;
    let started = state.marketplace.request_direct_install(&principal, request, state.secrets.as_ref().clone())
        .map_err(|_| ApiError::structured(StatusCode::SERVICE_UNAVAILABLE,"marketplace_install_unavailable",
            "Direct installation is not admitted, already owned or stopped. Nothing is made searchable.",None))?;
    Ok(Json(started))
}
async fn publisher_seeding(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<management::PublisherSeedingView>> {
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let request = management::PublisherSeedingRequest::parse(&bytes).map_err(|_| {
        ApiError::bad_request("explicit acceptance of the exact publisher release is required")
    })?;
    state.marketplace.set_publisher_seeding(&database(&state),&principal,request).await.map(Json)
        .map_err(|_|ApiError::conflict("Publisher seeding could not be changed. Refresh its revision and check the installed release and protected connection. Nothing is made searchable."))
}
async fn submit_withdrawal(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<crate::marketplace::withdrawal::View>> {
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let request = crate::marketplace::withdrawal::Request::parse(&bytes).map_err(|_| {
        ApiError::bad_request("explicit acceptance of the exact root withdrawal is required")
    })?;
    timeout(Duration::from_secs(75),state.marketplace.submit_withdrawal(&database(&state),&principal,request)).await
        .map_err(|_|ApiError::structured(StatusCode::SERVICE_UNAVAILABLE,"marketplace_withdrawal_unavailable",
            "Withdrawal submission timed out and may have applied locally. Remote delivery is not confirmed. Recover the protected connection before checking the same submission.",None))?
        .map(Json).map_err(|_|ApiError::structured(StatusCode::SERVICE_UNAVAILABLE,"marketplace_withdrawal_unavailable",
            "Withdrawal could not be acknowledged and may have applied locally. Remote delivery is not confirmed. Check owner access and the original protected connection.",None))
}
async fn publisher_seeding_view(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    Path((extension, instance)): Path<(String, Uuid)>,
) -> ApiResult<Json<management::PublisherSeedingView>> {
    authorize(&state, &principal).await?;
    let extension = extension
        .parse()
        .map_err(|_| ApiError::bad_request("invalid extension identity"))?;
    state
        .marketplace
        .publisher_seeding_view(&database(&state), &principal, &extension, instance)
        .await
        .map(Json)
        .map_err(|_| {
            ApiError::conflict("Publisher choice is unavailable for this managed instance.")
        })
}
async fn upload_source(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    Path((operation, upload)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> ApiResult<Json<serde_json::Value>> {
    use futures_util::TryStreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let size = upload_length(&headers)?;
    let database = database(&state);
    let (mut claim,authorization)=state.marketplace.claim_source_upload(&database,&principal,operation,upload,size).await
        .map_err(|_|ApiError::conflict("The original signed upload is not waiting, belongs to another operation, or was already claimed. Poll the operation; do not replay a body."))?;
    let transfer = async {
        let stream = body
            .into_data_stream()
            .map_err(|_| std::io::Error::other("publisher upload body failed"));
        let mut input = tokio_util::io::StreamReader::new(stream);
        let mut buffer = vec![0u8; 64 * 1024];
        let mut count = 0u64;
        loop {
            let n = timeout(Duration::from_secs(10), input.read(&mut buffer)).await??;
            if n == 0 {
                break;
            }
            count = count
                .checked_add(n as u64)
                .ok_or_else(|| anyhow::anyhow!("upload length overflow"))?;
            anyhow::ensure!(count <= size, "upload exceeds the signed size");
            timeout(
                Duration::from_secs(30),
                claim.writer.write_all(&buffer[..n]),
            )
            .await??;
        }
        anyhow::ensure!(count == size, "upload ended before its signed size");
        claim.writer.shutdown().await?;
        anyhow::ensure!(
            timeout(Duration::from_secs(30), &mut claim.completion).await??,
            "native upload verification failed"
        );
        Ok::<_, anyhow::Error>(())
    };
    tokio::pin!(transfer);
    loop {
        tokio::select! {
            result=&mut transfer => {result.map_err(|_|ApiError::conflict("Upload did not complete native integrity verification. Poll the original operation for cleanup; no source or installation approval was issued."))?;break;},
            _=tokio::time::sleep(Duration::from_secs(5))=>{
                timeout(Duration::from_secs(10),state.marketplace.check_source_upload(&database,&authorization)).await
                    .map_err(|_|ApiError::forbidden("Upload owner authorization timed out"))?
                    .map_err(|_|ApiError::forbidden("Upload owner authorization changed"))?;
            }
        }
    }
    state
        .marketplace
        .check_source_upload(&database, &authorization)
        .await
        .map_err(|_| ApiError::forbidden("Upload owner authorization changed"))?;
    Ok(Json(
        serde_json::json!({"schema_version":1,"operation_id":operation,"upload_id":upload,
        "integrity_verified":true,"source_verified":false,"installation_ready":false}),
    ))
}
fn upload_length(headers: &HeaderMap) -> ApiResult<u64> {
    let values: Vec<_> = headers.get_all(header::CONTENT_LENGTH).iter().collect();
    let text = values.first().and_then(|v| v.to_str().ok()).unwrap_or("");
    let size = text.parse::<u64>().ok();
    if values.len() != 1
        || text.is_empty()
        || !text.bytes().all(|b| b.is_ascii_digit())
        || size.is_none_or(|n| n == 0 || n > elixir_marketplace::MAX_COMPILED_ARTIFACT_BYTES)
        || headers.contains_key(header::CONTENT_ENCODING)
        || headers.contains_key(header::TRANSFER_ENCODING)
        || headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/octet-stream")
    {
        return Err(ApiError::bad_request(
            "Upload requires one exact bounded Content-Length, application/octet-stream and no transfer/content encoding",
        ));
    }
    Ok(size.expect("checked upload length"))
}
async fn current(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
) -> ApiResult<Json<management::CurrentInstallation>> {
    authorize(&state, &principal).await?;
    timeout(
        Duration::from_secs(10),
        state
            .marketplace
            .current_installation(&database(&state), &principal),
    )
    .await
    .map_err(|_| ApiError::internal("current installation authorization deadline"))?
    .map(Json)
    .map_err(|_| {
        ApiError::conflict(
            "Current installation changed or needs recovery. Refresh before proceeding.",
        )
    })
}
async fn status(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<management::InstallationView>> {
    authorize(&state, &principal).await?;
    timeout(
        Duration::from_secs(10),
        state
            .marketplace
            .installation_status(&database(&state), &principal, id),
    )
    .await
    .map_err(|_| ApiError::internal("installation status authorization deadline"))?
    .map(Json)
    .map_err(|_| ApiError::conflict("Installation is unavailable or belongs to a different owner."))
}
async fn action(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<management::Acknowledgement>> {
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let request = ActionRequest::parse(&bytes)
        .map_err(|_| ApiError::bad_request("invalid bounded installation action"))?;
    state.marketplace.request_install_action(&principal,id,request).map(Json)
        .map_err(|_|ApiError::conflict("Action input, owner, original mutation or current operation changed. Poll the original operation before retrying."))
}
pub(crate) fn installation_read_routes() -> Router<AppState> {
    Router::new()
        .route("/api/v1/marketplace/installations/current", get(current))
        .route("/api/v1/marketplace/installations/:id", get(status))
        .layer(middleware::map_response(private_response))
}
/// Private mutation composition only; a boolean HTTP request cannot mount it.
pub(crate) fn installation_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/marketplace/withdrawals",
            post(submit_withdrawal).layer(DefaultBodyLimit::max(
                crate::marketplace::withdrawal::MAX_REQUEST_BYTES,
            )),
        )
        .route(
            "/api/v1/marketplace/installations/:id/uploads/:upload",
            post(upload_source).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/api/v1/marketplace/publisher-seeding",
            post(publisher_seeding).layer(DefaultBodyLimit::max(
                management::MAX_PUBLISHER_SEEDING_BYTES,
            )),
        )
        .route(
            "/api/v1/marketplace/publisher-seeding/:extension/:instance",
            get(publisher_seeding_view),
        )
        .route(
            "/api/v1/marketplace/installations/direct",
            post(start_direct).layer(DefaultBodyLimit::max(management::MAX_DIRECT_INSTALL_BYTES)),
        )
        .route(
            "/api/v1/marketplace/installations",
            post(start).layer(DefaultBodyLimit::max(management::MAX_INSTALL_BYTES)),
        )
        .route(
            "/api/v1/marketplace/installations/:id/actions",
            post(action).layer(DefaultBodyLimit::max(management::MAX_ACTION_BYTES)),
        )
        .layer(middleware::map_response(private_response))
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::{authenticated_first_run_fixture, authenticated_fixture};
