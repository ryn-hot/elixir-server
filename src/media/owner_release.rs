use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Method as ReqwestMethod, StatusCode as ReqwestStatusCode};
use serde::Serialize;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

use crate::{
    db::models::MediaType,
    extensions::{
        ExternalIds,
        manifest::ExtensionManifest,
        store::{ExtensionStore, MediaOwnership, NewMediaOwnerReleaseEvent},
    },
    http::handlers::extensions::{
        remove_managed_library_item_from_manager, resolve_control_provider_transport_base_url,
    },
    metrics,
    orchestrator::model::ProviderEndpoint,
    state::AppState,
};

const OWNER_RELEASE_TIMEOUT_SECONDS: u64 = 30;
const STATUS_PENDING: &str = "pending";
const STATUS_SUCCEEDED: &str = "succeeded";
const STATUS_UNSUPPORTED: &str = "unsupported";
const STATUS_SKIPPED: &str = "skipped";
const STATUS_FAILED: &str = "failed";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerReleaseAction {
    DeleteAndReleaseOwner,
    ReleaseOwnerOnly,
    BlockEpisode,
}

impl OwnerReleaseAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeleteAndReleaseOwner => "delete_and_release_owner",
            Self::ReleaseOwnerOnly => "release_owner_only",
            Self::BlockEpisode => "block_episode",
        }
    }

    pub fn scope(self) -> &'static str {
        match self {
            Self::BlockEpisode => "episode",
            Self::DeleteAndReleaseOwner | Self::ReleaseOwnerOnly => "media_item",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MediaOwnerReleaseRequest {
    pub action: OwnerReleaseAction,
    pub media_item_id: Uuid,
    pub media_type: MediaType,
    pub title: String,
    pub year: Option<i32>,
    pub external_ids: ExternalIds,
    pub episode: Option<OwnerReleaseEpisodeScope>,
    pub fail_on_owner_error: bool,
}

#[derive(Debug, Clone)]
pub struct OwnerReleaseEpisodeScope {
    pub episode_id: Uuid,
    pub season_number: i32,
    pub episode_number: i32,
    pub absolute_episode_number: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReleaseSummary {
    pub action: String,
    pub media_item_id: String,
    pub released_count: usize,
    pub unsupported_count: usize,
    pub pending_count: usize,
    pub skipped_count: usize,
    pub failed_count: usize,
    pub owners: Vec<OwnerReleaseOwnerResult>,
}

impl OwnerReleaseSummary {
    pub fn has_failures(&self) -> bool {
        self.failed_count > 0
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReleaseOwnerResult {
    pub ownership_id: String,
    pub owner_type: String,
    pub owner_label: Option<String>,
    pub owner_implementation: Option<String>,
    pub release_capability: String,
    pub status: String,
    pub status_reason: Option<String>,
    pub release_event_id: Option<String>,
}

#[derive(Debug, Clone)]
struct HandlerOutcome {
    status: &'static str,
    reason: String,
    response: Value,
}

impl HandlerOutcome {
    fn succeeded(reason: impl Into<String>, response: Value) -> Self {
        Self {
            status: STATUS_SUCCEEDED,
            reason: reason.into(),
            response,
        }
    }

    fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            status: STATUS_UNSUPPORTED,
            reason: reason.into(),
            response: json!({}),
        }
    }
}

pub async fn dispatch_media_owner_release(
    state: &AppState,
    store: &ExtensionStore<'_>,
    request: MediaOwnerReleaseRequest,
) -> Result<OwnerReleaseSummary> {
    let owners = store
        .ensure_media_ownerships_for_item(request.media_item_id)
        .await
        .context("loading media owners for release")?;
    let mut results = Vec::with_capacity(owners.len());

    for owner in owners {
        results.push(release_owner(state, store, &request, &owner).await?);
    }

    let released_count = results
        .iter()
        .filter(|result| result.status == STATUS_SUCCEEDED)
        .count();
    let unsupported_count = results
        .iter()
        .filter(|result| result.status == STATUS_UNSUPPORTED)
        .count();
    let pending_count = results
        .iter()
        .filter(|result| result.status == STATUS_PENDING)
        .count();
    let skipped_count = results
        .iter()
        .filter(|result| result.status == STATUS_SKIPPED)
        .count();
    let failed_count = results
        .iter()
        .filter(|result| result.status == STATUS_FAILED)
        .count();
    let summary = OwnerReleaseSummary {
        action: request.action.as_str().to_string(),
        media_item_id: request.media_item_id.to_string(),
        released_count,
        unsupported_count,
        pending_count,
        skipped_count,
        failed_count,
        owners: results,
    };

    if request.fail_on_owner_error && summary.has_failures() {
        bail!("owner release failed for {} owner(s)", summary.failed_count);
    }

    tracing::info!(
        action = %summary.action,
        media_item_id = %summary.media_item_id,
        released_count = summary.released_count,
        unsupported_count = summary.unsupported_count,
        pending_count = summary.pending_count,
        skipped_count = summary.skipped_count,
        failed_count = summary.failed_count,
        "media owner release dispatch completed"
    );

    Ok(summary)
}

async fn release_owner(
    state: &AppState,
    store: &ExtensionStore<'_>,
    request: &MediaOwnerReleaseRequest,
    owner: &MediaOwnership,
) -> Result<OwnerReleaseOwnerResult> {
    let prior = store
        .latest_media_owner_release_event(
            request.media_item_id,
            owner.ownership_id,
            request.action.as_str(),
        )
        .await?;
    if let Some(prior) = prior {
        if prior.status == STATUS_SUCCEEDED {
            let event_id = Uuid::new_v4();
            let reason = "Owner release already succeeded for this action.";
            record_owner_release_metric(request.action, &owner.owner_type, STATUS_SKIPPED);
            tracing::info!(
                action = request.action.as_str(),
                media_item_id = %request.media_item_id,
                ownership_id = %owner.ownership_id,
                owner_type = owner.owner_type.as_str(),
                "media owner release skipped after prior success"
            );
            store
                .create_media_owner_release_event(&NewMediaOwnerReleaseEvent {
                    release_event_id: event_id,
                    media_item_id: Some(request.media_item_id),
                    ownership_id: Some(owner.ownership_id),
                    requested_action: request.action.as_str().to_string(),
                    owner_type: owner.owner_type.clone(),
                    owner_label: owner.owner_label.clone(),
                    owner_provider_id: owner.owner_provider_id,
                    acquisition_subscription_id: owner.acquisition_subscription_id,
                    status: STATUS_SKIPPED.to_string(),
                    status_reason: Some(reason.to_string()),
                    request: Some(owner_release_event_request(request, owner)),
                    response: Some(json!({
                        "priorReleaseEventId": prior.release_event_id,
                        "priorUpdatedAt": prior.updated_at,
                    })),
                })
                .await?;
            return Ok(owner_result(
                owner,
                STATUS_SKIPPED,
                Some(reason),
                Some(event_id),
            ));
        }
    }

    let event_id = Uuid::new_v4();
    record_owner_release_metric(request.action, &owner.owner_type, "attempted");
    record_owner_release_metric(request.action, &owner.owner_type, STATUS_PENDING);
    tracing::info!(
        action = request.action.as_str(),
        media_item_id = %request.media_item_id,
        ownership_id = %owner.ownership_id,
        owner_type = owner.owner_type.as_str(),
        release_capability = owner.release_capability.as_str(),
        "media owner release attempted"
    );
    store
        .create_media_owner_release_event(&NewMediaOwnerReleaseEvent {
            release_event_id: event_id,
            media_item_id: Some(request.media_item_id),
            ownership_id: Some(owner.ownership_id),
            requested_action: request.action.as_str().to_string(),
            owner_type: owner.owner_type.clone(),
            owner_label: owner.owner_label.clone(),
            owner_provider_id: owner.owner_provider_id,
            acquisition_subscription_id: owner.acquisition_subscription_id,
            status: "pending".to_string(),
            status_reason: None,
            request: Some(owner_release_event_request(request, owner)),
            response: None,
        })
        .await?;

    let outcome = match execute_owner_release(state, store, request, owner, event_id).await {
        Ok(outcome) => outcome,
        Err(error) => {
            let reason = error.to_string();
            record_owner_release_metric(request.action, &owner.owner_type, STATUS_FAILED);
            tracing::warn!(
                action = request.action.as_str(),
                media_item_id = %request.media_item_id,
                ownership_id = %owner.ownership_id,
                owner_type = owner.owner_type.as_str(),
                error = %reason,
                "media owner release failed"
            );
            HandlerOutcome {
                status: STATUS_FAILED,
                reason,
                response: json!({
                    "error": error.to_string(),
                }),
            }
        }
    };

    store
        .update_media_owner_release_event_status(
            event_id,
            outcome.status,
            Some(&outcome.reason),
            Some(&outcome.response),
        )
        .await?;
    record_owner_release_metric(request.action, &owner.owner_type, outcome.status);
    tracing::info!(
        action = request.action.as_str(),
        media_item_id = %request.media_item_id,
        ownership_id = %owner.ownership_id,
        owner_type = owner.owner_type.as_str(),
        status = outcome.status,
        status_reason = outcome.reason.as_str(),
        "media owner release owner result recorded"
    );

    Ok(owner_result(
        owner,
        outcome.status,
        Some(&outcome.reason),
        Some(event_id),
    ))
}

async fn execute_owner_release(
    state: &AppState,
    store: &ExtensionStore<'_>,
    request: &MediaOwnerReleaseRequest,
    owner: &MediaOwnership,
    event_id: Uuid,
) -> Result<HandlerOutcome> {
    match owner.owner_type.as_str() {
        "external" => Ok(HandlerOutcome::unsupported(
            "External imports do not have an upstream owner to release.",
        )),
        "acquisition" => release_acquisition_owner(state, request, owner).await,
        "extension" => release_extension_owner(state, store, request, owner, event_id).await,
        "system" => Ok(HandlerOutcome::unsupported(
            "System owner release is not implemented for this owner.",
        )),
        other => Ok(HandlerOutcome::unsupported(format!(
            "Unsupported owner type '{other}'."
        ))),
    }
}

async fn release_extension_owner(
    state: &AppState,
    store: &ExtensionStore<'_>,
    request: &MediaOwnerReleaseRequest,
    owner: &MediaOwnership,
    event_id: Uuid,
) -> Result<HandlerOutcome> {
    match (owner.release_capability.as_str(), request.action) {
        ("manager.remove_item", OwnerReleaseAction::BlockEpisode) => {
            Ok(HandlerOutcome::unsupported(
                "Managed owner adapter does not support single-episode release.",
            ))
        }
        ("manager.remove_item", _) => release_arr_manager_owner(state, store, owner).await,
        ("media.owner_release", _) => {
            release_generic_extension_owner(state, store, request, owner, event_id).await
        }
        ("none", _) => Ok(HandlerOutcome::unsupported(
            "Extension owner does not advertise an owner-release capability.",
        )),
        (other, _) => Ok(HandlerOutcome::unsupported(format!(
            "Extension owner release capability '{other}' is not supported by this Elixir build."
        ))),
    }
}

async fn release_arr_manager_owner(
    state: &AppState,
    store: &ExtensionStore<'_>,
    owner: &MediaOwnership,
) -> Result<HandlerOutcome> {
    let Some(provider_id) = owner.owner_provider_id else {
        return Ok(HandlerOutcome::unsupported(
            "Managed owner provider is no longer available.",
        ));
    };
    let Some(provider) = store.get_provider(provider_id).await? else {
        return Ok(HandlerOutcome::unsupported(
            "Managed owner provider is no longer available.",
        ));
    };
    let manager_item_id = owner
        .owner_external_id
        .as_deref()
        .ok_or_else(|| anyhow!("managed owner is missing external item id"))?
        .parse::<i64>()
        .context("parsing managed owner item id")?;
    let message =
        remove_managed_library_item_from_manager(state, store, &provider, manager_item_id).await?;
    Ok(HandlerOutcome::succeeded(
        message.clone(),
        json!({
            "message": message,
            "deleteFiles": false,
            "providerId": provider_id,
            "managerItemId": manager_item_id,
        }),
    ))
}

async fn release_generic_extension_owner(
    _state: &AppState,
    store: &ExtensionStore<'_>,
    request: &MediaOwnerReleaseRequest,
    owner: &MediaOwnership,
    event_id: Uuid,
) -> Result<HandlerOutcome> {
    let Some(provider_id) = owner.owner_provider_id else {
        return Ok(HandlerOutcome::unsupported(
            "Extension owner provider is no longer available.",
        ));
    };
    let Some(provider) = store.get_provider(provider_id).await? else {
        return Ok(HandlerOutcome::unsupported(
            "Extension owner provider is no longer available.",
        ));
    };
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow!("extension owner provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing extension owner endpoint")?;
    let instance = store
        .get_instance(provider.instance_id)
        .await?
        .ok_or_else(|| anyhow!("extension owner instance is no longer available"))?;
    let extension = store
        .get_extension(&instance.extension_id)
        .await?
        .ok_or_else(|| anyhow!("extension owner package is no longer installed"))?;
    let manifest: ExtensionManifest = serde_json::from_value(extension.manifest_json)
        .context("parsing extension owner manifest")?;
    let Some(owner_release) = manifest.owner_release.as_ref() else {
        return Ok(HandlerOutcome::unsupported(
            "Extension manifest does not opt into owner release.",
        ));
    };
    let scope = request.action.scope();
    if !owner_release.scopes.is_empty() && !owner_release.scopes.iter().any(|value| value == scope)
    {
        return Ok(HandlerOutcome::unsupported(
            "Extension owner-release contract does not support this release scope.",
        ));
    }

    let base_url =
        resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?;
    let url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        owner_release.endpoint.trim_start_matches('/')
    );
    let payload = json!({
        "requestId": event_id,
        "idempotencyKey": owner_release_idempotency_key(request, owner),
        "action": request.action.as_str(),
        "scope": scope,
        "media": {
            "mediaItemId": request.media_item_id,
            "mediaType": request.media_type.as_str(),
            "title": request.title,
            "year": request.year,
            "externalIds": request.external_ids,
        },
        "episode": request.episode.as_ref().map(owner_release_episode_json),
        "owner": safe_owner_json(owner),
        "policy": owner_release_policy_json(request.action),
    });

    let client = Client::builder()
        .timeout(Duration::from_secs(OWNER_RELEASE_TIMEOUT_SECONDS))
        .build()
        .context("building owner-release HTTP client")?;
    let response = client
        .request(ReqwestMethod::POST, url)
        .json(&payload)
        .send()
        .await
        .context("sending owner-release request")?;
    if response.status() == ReqwestStatusCode::NOT_FOUND {
        return Ok(HandlerOutcome::unsupported(
            "Extension runtime does not expose the owner-release endpoint.",
        ));
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("extension owner-release failed ({status}): {}", body.trim());
    }
    let response_json =
        serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({ "body": body }));
    let response_status = response_json
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("succeeded")
        .trim()
        .to_ascii_lowercase();
    match response_status.as_str() {
        "accepted" => Ok(HandlerOutcome {
            status: STATUS_PENDING,
            reason: "Extension owner-release request was accepted for asynchronous processing."
                .to_string(),
            response: response_json,
        }),
        "unsupported" => Ok(HandlerOutcome {
            status: STATUS_UNSUPPORTED,
            reason: response_json
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Extension reported owner release is unsupported.")
                .to_string(),
            response: response_json,
        }),
        "failed" => bail!(
            "{}",
            response_json
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Extension reported owner-release failure.")
        ),
        _ => Ok(HandlerOutcome::succeeded(
            "Extension owner-release request succeeded.",
            response_json,
        )),
    }
}

async fn release_acquisition_owner(
    state: &AppState,
    request: &MediaOwnerReleaseRequest,
    owner: &MediaOwnership,
) -> Result<HandlerOutcome> {
    let subscription_id = owner
        .acquisition_subscription_id
        .ok_or_else(|| anyhow!("acquisition owner is missing subscription id"))?;

    if request.action == OwnerReleaseAction::BlockEpisode {
        return block_acquisition_episode_target(state, request, subscription_id).await;
    }

    let target_result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = 'excluded',
             state_reason = $1,
             next_search_after = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = $2
           AND state IN ('pending', 'searching', 'blocked', 'submitted')",
    )
    .bind("Released by media owner request.")
    .bind(subscription_id.to_string())
    .execute(&state.db_pool)
    .await
    .context("excluding acquisition targets for owner release")?;

    let subscription_result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_subscriptions
         SET status = 'paused',
             active = 0,
             candidate_search_after = CURRENT_TIMESTAMP,
             metadata_refresh_after = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = $1",
    )
    .bind(subscription_id.to_string())
    .execute(&state.db_pool)
    .await
    .context("stopping acquisition subscription for owner release")?;

    let job_result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET active = 0,
             state_reason = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE active = 1
           AND release_id IN (
               SELECT release_id
               FROM acquisition_releases
               WHERE subscription_id = $2
           )",
    )
    .bind("Owner release stopped monitoring for this media item.")
    .bind(subscription_id.to_string())
    .execute(&state.db_pool)
    .await
    .context("deactivating acquisition release jobs for owner release")?;

    Ok(HandlerOutcome::succeeded(
        "Elixir acquisition monitoring stopped for this item.",
        json!({
            "subscriptionId": subscription_id,
            "targetsExcluded": target_result.rows_affected(),
            "subscriptionsUpdated": subscription_result.rows_affected(),
            "releaseJobsDeactivated": job_result.rows_affected(),
        }),
    ))
}

async fn block_acquisition_episode_target(
    state: &AppState,
    request: &MediaOwnerReleaseRequest,
    subscription_id: Uuid,
) -> Result<HandlerOutcome> {
    let episode = request
        .episode
        .as_ref()
        .ok_or_else(|| anyhow!("episode owner-release request is missing episode scope"))?;
    let season_episode_key = format!(
        "S{season:02}E{episode_number:02}",
        season = episode.season_number,
        episode_number = episode.episode_number
    );
    let absolute_key = episode
        .absolute_episode_number
        .map(|absolute| format!("A{absolute:04}"));
    let result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = 'excluded',
             state_reason = $1,
             next_search_after = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = $2
           AND state IN ('pending', 'searching', 'blocked', 'submitted')
           AND (
               (season_number = $3 AND episode_number = $4)
               OR absolute_episode_number = $5
               OR target_key = $6
               OR target_key = $7
           )",
    )
    .bind("Excluded by episode owner-release request.")
    .bind(subscription_id.to_string())
    .bind(episode.season_number)
    .bind(episode.episode_number)
    .bind(episode.absolute_episode_number)
    .bind(&season_episode_key)
    .bind(absolute_key.as_deref())
    .execute(&state.db_pool)
    .await
    .context("excluding acquisition episode target for owner release")?;

    Ok(HandlerOutcome::succeeded(
        "Elixir acquisition monitoring excluded this episode target.",
        json!({
            "subscriptionId": subscription_id,
            "episodeId": episode.episode_id,
            "seasonNumber": episode.season_number,
            "episodeNumber": episode.episode_number,
            "absoluteEpisodeNumber": episode.absolute_episode_number,
            "targetsExcluded": result.rows_affected(),
        }),
    ))
}

fn owner_result(
    owner: &MediaOwnership,
    status: &str,
    status_reason: Option<&str>,
    event_id: Option<Uuid>,
) -> OwnerReleaseOwnerResult {
    OwnerReleaseOwnerResult {
        ownership_id: owner.ownership_id.to_string(),
        owner_type: owner.owner_type.clone(),
        owner_label: owner.owner_label.clone(),
        owner_implementation: owner.owner_implementation.clone(),
        release_capability: owner.release_capability.clone(),
        status: status.to_string(),
        status_reason: status_reason.map(str::to_string),
        release_event_id: event_id.map(|value| value.to_string()),
    }
}

fn owner_release_event_request(
    request: &MediaOwnerReleaseRequest,
    owner: &MediaOwnership,
) -> Value {
    json!({
        "idempotencyKey": owner_release_idempotency_key(request, owner),
        "action": request.action.as_str(),
        "scope": request.action.scope(),
        "media": {
            "mediaItemId": request.media_item_id,
            "mediaType": request.media_type.as_str(),
            "title": request.title,
            "year": request.year,
            "externalIds": request.external_ids,
        },
        "episode": request.episode.as_ref().map(owner_release_episode_json),
        "owner": safe_owner_json(owner),
        "policy": owner_release_policy_json(request.action),
    })
}

fn owner_release_episode_json(episode: &OwnerReleaseEpisodeScope) -> Value {
    json!({
        "episodeId": episode.episode_id,
        "seasonNumber": episode.season_number,
        "episodeNumber": episode.episode_number,
        "absoluteEpisodeNumber": episode.absolute_episode_number,
    })
}

fn owner_release_policy_json(action: OwnerReleaseAction) -> Value {
    json!({
        "deleteFiles": false,
        "stopMonitoring": matches!(
            action,
            OwnerReleaseAction::DeleteAndReleaseOwner | OwnerReleaseAction::ReleaseOwnerOnly
        ),
        "blockFutureImports": true,
    })
}

fn owner_release_idempotency_key(
    request: &MediaOwnerReleaseRequest,
    owner: &MediaOwnership,
) -> String {
    format!(
        "{}:{}:{}",
        request.media_item_id,
        owner.ownership_id,
        request.action.as_str()
    )
}

fn safe_owner_json(owner: &MediaOwnership) -> Value {
    json!({
        "ownershipId": owner.ownership_id,
        "ownerType": owner.owner_type,
        "ownerRole": owner.owner_role,
        "ownerLabel": owner.owner_label,
        "ownerImplementation": owner.owner_implementation,
        "ownerProviderId": owner.owner_provider_id,
        "ownerInstanceId": owner.owner_instance_id,
        "ownerExtensionId": owner.owner_extension_id,
        "ownerExternalId": owner.owner_external_id,
        "acquisitionSubscriptionId": owner.acquisition_subscription_id,
        "releaseCapability": owner.release_capability,
        "releasePolicy": owner.release_policy,
    })
}

fn record_owner_release_metric(action: OwnerReleaseAction, owner_type: &str, status: &str) {
    metrics::OWNER_RELEASE_EVENTS
        .with_label_values(&[action.as_str(), owner_type, status])
        .inc();
}
