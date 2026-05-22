use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use reqwest::Client;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};
use tracing::{info, warn};

use crate::db::models::{Extension, ExtensionInstance, Provider};
use crate::extensions::package::write_manifest_to_dir;
use crate::extensions::store::ExtensionStore;
use crate::http::handlers::extensions::{
    InstallPolicy, install_internal_extension_from_dir, list_prowlarr_indexer_proxy_names,
    resolve_control_provider_transport_base_url,
};
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::reconcile::ReconcileConfig;
use crate::runtime::docker::{DockerImageMetadata, DockerRuntimeManager};
use crate::state::AppState;
use uuid::Uuid;

const GITHUB_API_TIMEOUT: Duration = Duration::from_secs(20);
const RUNTIME_HEALTH_TIMEOUT: Duration = Duration::from_secs(120);
const RUNTIME_HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(2);
const RUNTIME_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PROXY_RUNTIME_UPDATE_STATE_PREFIX: &str = "extensions.proxy_runtime_auto_update.";
const OCI_VERSION_LABELS: [&str; 2] = [
    "org.opencontainers.image.version",
    "org.opencontainers.image.ref.name",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyRuntimeUpdateState {
    pub severity: String,
    pub status_code: String,
    pub label: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immutable_image: Option<String>,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
enum ImageTagPolicy {
    ReleaseTagOnly,
    ReleaseTagThenLatest,
}

#[derive(Debug, Clone, Copy)]
struct ProxyRuntimeDefinition {
    extension_id: &'static str,
    connector_extension_id: &'static str,
    github_repo: &'static str,
    image_repo: &'static str,
    proxy_name: &'static str,
    image_tag_policy: ImageTagPolicy,
}

const PROXY_RUNTIME_DEFINITIONS: [ProxyRuntimeDefinition; 2] = [
    ProxyRuntimeDefinition {
        extension_id: "elixir.modules.flaresolverr",
        connector_extension_id: "elixir.connectors.prowlarr_flaresolverr_proxy",
        github_repo: "FlareSolverr/FlareSolverr",
        image_repo: "ghcr.io/flaresolverr/flaresolverr",
        proxy_name: "FlareSolverr",
        image_tag_policy: ImageTagPolicy::ReleaseTagOnly,
    },
    ProxyRuntimeDefinition {
        extension_id: "elixir.modules.byparr",
        connector_extension_id: "elixir.connectors.prowlarr_byparr_proxy",
        github_repo: "ThePhaseless/Byparr",
        image_repo: "ghcr.io/thephaseless/byparr",
        proxy_name: "Byparr",
        image_tag_policy: ImageTagPolicy::ReleaseTagThenLatest,
    },
];

#[derive(Debug, Deserialize)]
struct GitHubLatestRelease {
    tag_name: String,
    draft: bool,
    prerelease: bool,
}

#[derive(Debug, Clone)]
struct ResolvedImageRelease {
    release_version: Version,
    immutable_image: String,
    source_image: String,
}

pub async fn start_proxy_runtime_update_loop(state: AppState, interval: Duration) {
    if interval.is_zero() {
        return;
    }

    let startup_delay = Duration::from_secs(
        state
            .settings
            .extensions
            .reconcile_startup_settle_seconds
            .max(1),
    );
    sleep(startup_delay).await;

    if let Err(err) = run_proxy_runtime_update_once(&state).await {
        warn!("proxy runtime auto-update startup pass failed: {err}");
    }

    loop {
        sleep(interval).await;
        if let Err(err) = run_proxy_runtime_update_once(&state).await {
            warn!("proxy runtime auto-update pass failed: {err}");
        }
    }
}

pub async fn run_proxy_runtime_update_once(state: &AppState) -> Result<()> {
    let client = build_github_client()?;
    let docker = DockerRuntimeManager::new(None);
    for definition in PROXY_RUNTIME_DEFINITIONS {
        if let Err(err) = maybe_update_proxy_runtime(state, &client, &docker, definition).await {
            warn!(
                extension_id = definition.extension_id,
                "proxy runtime auto-update failed: {err}"
            );
        }
    }
    Ok(())
}

async fn maybe_update_proxy_runtime(
    state: &AppState,
    client: &Client,
    docker: &DockerRuntimeManager,
    definition: ProxyRuntimeDefinition,
) -> Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    let Some(existing_extension) = store.get_extension(definition.extension_id).await? else {
        return Ok(());
    };
    if !existing_extension.enabled {
        return Ok(());
    }

    let current_version = Version::parse(&existing_extension.version).with_context(|| {
        format!(
            "installed extension '{}' version '{}' is not valid semver",
            existing_extension.extension_id, existing_extension.version
        )
    })?;

    let release = fetch_latest_release(client, definition.github_repo).await?;
    if release.draft || release.prerelease {
        persist_proxy_runtime_update_state(
            &store,
            definition,
            proxy_runtime_update_state(
                "ready",
                "waiting_for_stable_release",
                "Waiting for stable release",
                &format!(
                    "GitHub latest release '{}' is not a stable release, so Elixir left the current runtime in place.",
                    release.tag_name
                ),
                Some(current_version.to_string()),
                None,
            ),
        )
        .await?;
        return Ok(());
    }
    let resolved_release = match resolve_release_image(docker, definition, &release).await {
        Ok(value) => value,
        Err(err) => {
            persist_proxy_runtime_update_state(
                &store,
                definition,
                proxy_runtime_update_state(
                    "attention",
                    "immutable_image_unavailable",
                    "Auto-update blocked",
                    &format!(
                        "Elixir could not verify an immutable image for upstream release '{}': {err}",
                        release.tag_name
                    ),
                    Some(current_version.to_string()),
                    None,
                ),
            )
            .await?;
            return Ok(());
        }
    };

    if current_version > resolved_release.release_version {
        persist_proxy_runtime_update_state(
            &store,
            definition,
            proxy_runtime_update_state(
                "ready",
                "installed_newer_than_upstream",
                "Pinned newer than upstream",
                &format!(
                    "Installed runtime package {} is newer than the latest upstream release Elixir could verify ({}).",
                    current_version, resolved_release.release_version
                ),
                Some(current_version.to_string()),
                Some(resolved_release.immutable_image.clone()),
            ),
        )
        .await?;
        info!(
            extension_id = definition.extension_id,
            installed_version = %current_version,
            latest_release = %resolved_release.release_version,
            "skipping proxy runtime auto-update because installed version is newer than upstream release"
        );
        return Ok(());
    }

    if current_version == resolved_release.release_version {
        persist_proxy_runtime_update_state(
            &store,
            definition,
            proxy_runtime_update_state(
                "ready",
                "up_to_date",
                "Up to date",
                &format!(
                    "Already on the latest verified upstream release ({}).",
                    resolved_release.release_version
                ),
                Some(current_version.to_string()),
                Some(resolved_release.immutable_image.clone()),
            ),
        )
        .await?;
        return Ok(());
    }

    let prowlarr_provider =
        match resolve_prowlarr_provider_for_verification(state, &store, definition).await {
            Ok(value) => value,
            Err(err) => {
                persist_proxy_runtime_update_state(
                    &store,
                    definition,
                    proxy_runtime_update_state(
                        "attention",
                        "verification_blocked",
                        "Auto-update blocked",
                        &err.to_string(),
                        Some(current_version.to_string()),
                        Some(resolved_release.immutable_image.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
    let previous_extension = existing_extension.clone();
    let package_dir = build_generated_extension_package_dir(
        &state.settings.extensions.storage_root,
        &existing_extension,
        &resolved_release.release_version.to_string(),
        &resolved_release.immutable_image,
    )
    .await?;

    let install_result = install_internal_extension_from_dir(
        state,
        &package_dir,
        InstallPolicy {
            allow_internal_directory_install: true,
            allow_internal_unsigned: true,
            allow_downgrade: false,
            allow_same_version_replace: false,
            suppress_reconcile: false,
        },
    )
    .await;
    if install_result.is_err() {
        let _ = fs::remove_dir_all(&package_dir).await;
    }
    if let Err(err) = install_result {
        persist_proxy_runtime_update_state(
            &store,
            definition,
            proxy_runtime_update_state(
                "attention",
                "install_failed",
                "Auto-update failed",
                &format!(
                    "Elixir could not install runtime release {}: {err}",
                    resolved_release.release_version
                ),
                Some(current_version.to_string()),
                Some(resolved_release.immutable_image.clone()),
            ),
        )
        .await?;
        return Err(err);
    }

    let Some(instance) = resolve_enabled_module_instance(&store, definition.extension_id).await?
    else {
        rollback_proxy_runtime_update(state, definition, &previous_extension, Uuid::nil()).await?;
        persist_proxy_runtime_update_state(
            &store,
            definition,
            proxy_runtime_update_state(
                "attention",
                "missing_runtime_after_install",
                "Auto-update rolled back",
                &format!(
                    "Elixir installed runtime release {}, but no enabled runtime instance was available afterward. The previous version was restored.",
                    resolved_release.release_version
                ),
                Some(previous_extension.version.clone()),
                Some(manifest_runtime_image(&previous_extension.manifest_json)?),
            ),
        )
        .await?;
        bail!(
            "updated extension '{}' does not have an enabled runtime instance",
            definition.extension_id
        );
    };

    let reconcile_config = ReconcileConfig::from_settings(&state.settings);
    if let Err(err) = state.orchestrator.reconcile_once(&reconcile_config).await {
        rollback_proxy_runtime_update(state, definition, &previous_extension, instance.instance_id)
            .await?;
        persist_proxy_runtime_update_state(
            &store,
            definition,
            proxy_runtime_update_state(
                "attention",
                "reconcile_failed_rolled_back",
                "Auto-update rolled back",
                &format!(
                    "Elixir could not reconcile runtime release {} and restored the previous version: {err}",
                    resolved_release.release_version
                ),
                Some(previous_extension.version.clone()),
                Some(manifest_runtime_image(&previous_extension.manifest_json)?),
            ),
        )
        .await?;
        return Err(err);
    }

    if let Err(err) = wait_for_proxy_runtime_health(
        state,
        definition,
        instance.instance_id,
        &resolved_release.release_version,
        prowlarr_provider.as_ref(),
    )
    .await
    {
        warn!(
            extension_id = definition.extension_id,
            target_version = %resolved_release.release_version,
            source_image = %resolved_release.source_image,
            immutable_image = %resolved_release.immutable_image,
            "proxy runtime auto-update health gate failed, rolling back: {err}"
        );
        rollback_proxy_runtime_update(state, definition, &previous_extension, instance.instance_id)
            .await?;
        persist_proxy_runtime_update_state(
            &store,
            definition,
            proxy_runtime_update_state(
                "attention",
                "health_gate_failed_rolled_back",
                "Auto-update rolled back",
                &format!(
                    "Runtime release {} did not pass health checks, so Elixir restored the previous version.",
                    resolved_release.release_version
                ),
                Some(previous_extension.version.clone()),
                Some(manifest_runtime_image(&previous_extension.manifest_json)?),
            ),
        )
        .await?;
        return Ok(());
    }

    persist_proxy_runtime_update_state(
        &store,
        definition,
        proxy_runtime_update_state(
            "ready",
            "updated",
            "Updated automatically",
            &format!(
                "Elixir updated this runtime to verified upstream release {}.",
                resolved_release.release_version
            ),
            Some(resolved_release.release_version.to_string()),
            Some(resolved_release.immutable_image.clone()),
        ),
    )
    .await?;
    info!(
        extension_id = definition.extension_id,
        version = %resolved_release.release_version,
        image = %resolved_release.immutable_image,
        "proxy runtime auto-update applied successfully"
    );
    Ok(())
}

pub(crate) async fn load_proxy_runtime_update_state(
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> Result<Option<ProxyRuntimeUpdateState>> {
    let Some(value) = store
        .get_extension_setting(&proxy_runtime_update_state_key(extension_id))
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(
        serde_json::from_value(value).context("parsing proxy runtime update state")?,
    ))
}

async fn persist_proxy_runtime_update_state(
    store: &ExtensionStore<'_>,
    definition: ProxyRuntimeDefinition,
    state: ProxyRuntimeUpdateState,
) -> Result<()> {
    let value = serde_json::to_value(state).context("serializing proxy runtime update state")?;
    store
        .upsert_extension_setting(
            &proxy_runtime_update_state_key(definition.extension_id),
            &value,
        )
        .await
}

fn proxy_runtime_update_state(
    severity: &str,
    status_code: &str,
    label: &str,
    description: &str,
    release_version: Option<String>,
    immutable_image: Option<String>,
) -> ProxyRuntimeUpdateState {
    ProxyRuntimeUpdateState {
        severity: severity.to_string(),
        status_code: status_code.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        release_version,
        immutable_image,
        checked_at: Utc::now(),
    }
}

fn proxy_runtime_update_state_key(extension_id: &str) -> String {
    format!("{PROXY_RUNTIME_UPDATE_STATE_PREFIX}{extension_id}")
}

async fn rollback_proxy_runtime_update(
    state: &AppState,
    definition: ProxyRuntimeDefinition,
    previous_extension: &Extension,
    instance_id: Uuid,
) -> Result<()> {
    let package_dir = build_generated_extension_package_dir(
        &state.settings.extensions.storage_root,
        previous_extension,
        &previous_extension.version,
        &manifest_runtime_image(&previous_extension.manifest_json)?,
    )
    .await?;
    let install_result = install_internal_extension_from_dir(
        state,
        &package_dir,
        InstallPolicy {
            allow_internal_directory_install: true,
            allow_internal_unsigned: true,
            allow_downgrade: true,
            allow_same_version_replace: false,
            suppress_reconcile: false,
        },
    )
    .await;
    if install_result.is_err() {
        let _ = fs::remove_dir_all(&package_dir).await;
    }
    install_result?;

    let reconcile_config = ReconcileConfig::from_settings(&state.settings);
    let _ = state.orchestrator.reconcile_once(&reconcile_config).await;
    if instance_id.is_nil() {
        return Ok(());
    }
    let store = ExtensionStore::new(&state.db_pool);
    let prowlarr_provider =
        resolve_prowlarr_provider_for_verification(state, &store, definition).await?;
    wait_for_proxy_runtime_health(
        state,
        definition,
        instance_id,
        &Version::parse(&previous_extension.version)?,
        prowlarr_provider.as_ref(),
    )
    .await
}

async fn wait_for_proxy_runtime_health(
    state: &AppState,
    definition: ProxyRuntimeDefinition,
    instance_id: Uuid,
    target_version: &Version,
    prowlarr_provider: Option<&Provider>,
) -> Result<()> {
    let deadline = Instant::now() + RUNTIME_HEALTH_TIMEOUT;

    loop {
        let store = ExtensionStore::new(&state.db_pool);
        if store.get_instance(instance_id).await?.is_none() {
            bail!(
                "runtime instance '{}' disappeared during auto-update",
                instance_id
            );
        }
        let target_version_text = target_version.to_string();
        let Some(extension) = store.get_extension(definition.extension_id).await? else {
            bail!(
                "extension '{}' disappeared during auto-update",
                definition.extension_id
            );
        };
        if extension.version == target_version_text {
            let providers = store.list_providers(Some(instance_id)).await?;
            if let Some(provider) = providers
                .iter()
                .find(|provider| provider.capability == "indexer.proxy")
            {
                if proxy_runtime_endpoint_reachable(instance_id, provider).await? {
                    if let Some(prowlarr_provider) = prowlarr_provider {
                        let proxy_names =
                            list_prowlarr_indexer_proxy_names(state, &store, prowlarr_provider)
                                .await?;
                        if proxy_names.iter().any(|name| name == definition.proxy_name) {
                            return Ok(());
                        }
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        if Instant::now() >= deadline {
            bail!(
                "proxy runtime '{}' did not become healthy within {} seconds",
                definition.extension_id,
                RUNTIME_HEALTH_TIMEOUT.as_secs()
            );
        }
        sleep(RUNTIME_HEALTH_POLL_INTERVAL).await;
    }
}

async fn proxy_runtime_endpoint_reachable(instance_id: Uuid, provider: &Provider) -> Result<bool> {
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("proxy provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url = resolve_control_provider_transport_base_url(instance_id, &endpoint).await?;
    let url = reqwest::Url::parse(&base_url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("proxy runtime base url '{}' is missing a host", base_url))?
        .to_string();
    let port = url.port_or_known_default().ok_or_else(|| {
        anyhow::anyhow!("proxy runtime base url '{}' is missing a port", base_url)
    })?;
    match timeout(
        RUNTIME_CONNECT_TIMEOUT,
        TcpStream::connect((host.as_str(), port)),
    )
    .await
    {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(_)) | Err(_) => Ok(false),
    }
}

async fn resolve_enabled_module_instance(
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> Result<Option<ExtensionInstance>> {
    Ok(store
        .list_instances(Some(extension_id))
        .await?
        .into_iter()
        .find(|instance| instance.enabled))
}

async fn resolve_prowlarr_provider_for_verification(
    state: &AppState,
    store: &ExtensionStore<'_>,
    definition: ProxyRuntimeDefinition,
) -> Result<Option<Provider>> {
    let Some(connector) = store
        .get_extension(definition.connector_extension_id)
        .await?
    else {
        return Ok(None);
    };
    if !connector.enabled {
        return Ok(None);
    }

    let provider = find_enabled_prowlarr_provider(store)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Prowlarr is not available for proxy verification"))?;
    let proxy_names = list_prowlarr_indexer_proxy_names(state, store, &provider).await?;
    if !proxy_names.iter().any(|name| name == definition.proxy_name) {
        bail!(
            "Prowlarr proxy '{}' is not currently healthy; skipping auto-update for '{}'",
            definition.proxy_name,
            definition.extension_id
        );
    }
    Ok(Some(provider))
}

async fn find_enabled_prowlarr_provider(store: &ExtensionStore<'_>) -> Result<Option<Provider>> {
    for provider in store.list_providers(None).await? {
        if provider.capability != "indexer.registry" {
            continue;
        }
        let Some(instance) = store.get_instance(provider.instance_id).await? else {
            continue;
        };
        if !instance.enabled {
            continue;
        }
        let Some(extension) = store.get_extension(&instance.extension_id).await? else {
            continue;
        };
        if !extension.enabled {
            continue;
        }
        return Ok(Some(provider));
    }
    Ok(None)
}

async fn build_generated_extension_package_dir(
    storage_root: &str,
    extension: &Extension,
    version: &str,
    immutable_image: &str,
) -> Result<PathBuf> {
    let dir = Path::new(storage_root)
        .join("tmp")
        .join(format!("proxy-runtime-update-{}", Uuid::new_v4()));
    fs::create_dir_all(&dir).await.with_context(|| {
        format!(
            "creating proxy runtime update staging directory '{}'",
            dir.display()
        )
    })?;
    let mut raw_json = extension.manifest_json.clone();
    let root = raw_json
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("extension manifest root must be an object"))?;
    root.insert("version".to_string(), Value::String(version.to_string()));
    let runtime = root
        .get_mut("runtime")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("extension manifest runtime must be an object"))?;
    runtime.insert(
        "image".to_string(),
        Value::String(immutable_image.to_string()),
    );
    write_manifest_to_dir(&dir, &raw_json).await?;
    Ok(dir)
}

fn manifest_runtime_image(raw_json: &Value) -> Result<String> {
    raw_json
        .get("runtime")
        .and_then(|value| value.get("image"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("extension runtime.image is missing"))
}

async fn fetch_latest_release(client: &Client, repo: &str) -> Result<GitHubLatestRelease> {
    let url = format!("https://api.github.com/repos/{repo}/releases/latest");
    let response = client.get(&url).send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let detail = response.text().await.unwrap_or_default();
        bail!(
            "github latest release request failed ({status}): {}",
            detail.trim()
        );
    }
    response
        .json::<GitHubLatestRelease>()
        .await
        .context("parsing github latest release response")
}

async fn resolve_release_image(
    docker: &DockerRuntimeManager,
    definition: ProxyRuntimeDefinition,
    release: &GitHubLatestRelease,
) -> Result<ResolvedImageRelease> {
    let release_version = parse_release_version(&release.tag_name)?;
    let normalized_tag = normalized_release_tag(&release.tag_name);
    let candidates = image_candidates(definition, &release.tag_name);
    let mut errors = Vec::new();

    for candidate in candidates {
        match resolve_candidate_image(docker, definition, &candidate, &normalized_tag).await {
            Ok(immutable_image) => {
                return Ok(ResolvedImageRelease {
                    release_version: release_version.clone(),
                    immutable_image,
                    source_image: candidate,
                });
            }
            Err(err) => errors.push(format!("{candidate}: {err}")),
        }
    }

    bail!(
        "failed to resolve immutable image for '{}' release '{}': {}",
        definition.extension_id,
        release.tag_name,
        errors.join(" | ")
    );
}

async fn resolve_candidate_image(
    docker: &DockerRuntimeManager,
    definition: ProxyRuntimeDefinition,
    candidate: &str,
    normalized_release_tag: &str,
) -> Result<String> {
    docker.pull_image(candidate).await?;
    let metadata = docker.inspect_image_metadata(candidate).await?;
    if candidate.ends_with(":latest")
        && !image_metadata_matches_release(&metadata, normalized_release_tag)
    {
        bail!(
            "latest image does not advertise release '{}'",
            normalized_release_tag
        );
    }
    select_repo_digest(&metadata, definition.image_repo)
}

fn image_candidates(definition: ProxyRuntimeDefinition, release_tag: &str) -> Vec<String> {
    let mut candidates = vec![format!("{}:{}", definition.image_repo, release_tag)];
    if matches!(
        definition.image_tag_policy,
        ImageTagPolicy::ReleaseTagThenLatest
    ) {
        candidates.push(format!("{}:latest", definition.image_repo));
    }
    candidates
}

fn parse_release_version(tag: &str) -> Result<Version> {
    Version::parse(&normalized_release_tag(tag))
        .with_context(|| format!("release tag '{}' is not valid semver", tag))
}

fn normalized_release_tag(tag: &str) -> String {
    tag.trim()
        .trim_start_matches('v')
        .trim_start_matches('V')
        .to_string()
}

fn select_repo_digest(metadata: &DockerImageMetadata, image_repo: &str) -> Result<String> {
    metadata
        .repo_digests
        .iter()
        .find(|digest| digest.starts_with(&format!("{image_repo}@")))
        .cloned()
        .or_else(|| metadata.repo_digests.first().cloned())
        .ok_or_else(|| anyhow::anyhow!("image inspect returned no repo digests"))
}

fn image_metadata_matches_release(metadata: &DockerImageMetadata, normalized_tag: &str) -> bool {
    OCI_VERSION_LABELS.iter().any(|key| {
        metadata
            .labels
            .get(*key)
            .map(|value| normalized_release_tag(value) == normalized_tag)
            .unwrap_or(false)
    })
}

fn build_github_client() -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    Client::builder()
        .timeout(GITHUB_API_TIMEOUT)
        .default_headers(headers)
        .build()
        .context("building github release client")
}

#[cfg(test)]
mod tests {
    use super::{
        DockerImageMetadata, image_metadata_matches_release, normalized_release_tag,
        parse_release_version, select_repo_digest,
    };

    #[test]
    fn release_tag_normalization_strips_v_prefix() {
        assert_eq!(normalized_release_tag("v3.4.6"), "3.4.6");
        assert_eq!(normalized_release_tag("V2.1.0"), "2.1.0");
    }

    #[test]
    fn release_tag_parser_accepts_semver_tags() {
        let version = parse_release_version("v2.1.0").expect("semver tag");
        assert_eq!(version.to_string(), "2.1.0");
    }

    #[test]
    fn select_repo_digest_prefers_requested_repo() {
        let metadata = DockerImageMetadata {
            repo_digests: vec![
                "ghcr.io/other/image@sha256:bbb".to_string(),
                "ghcr.io/example/image@sha256:aaa".to_string(),
            ],
            labels: Default::default(),
        };
        let digest = select_repo_digest(&metadata, "ghcr.io/example/image").expect("repo digest");
        assert_eq!(digest, "ghcr.io/example/image@sha256:aaa");
    }

    #[test]
    fn latest_fallback_requires_matching_version_label() {
        let metadata = DockerImageMetadata {
            repo_digests: vec!["ghcr.io/example/image@sha256:aaa".to_string()],
            labels: std::collections::HashMap::from([(
                "org.opencontainers.image.version".to_string(),
                "v2.1.0".to_string(),
            )]),
        };
        assert!(image_metadata_matches_release(&metadata, "2.1.0"));
        assert!(!image_metadata_matches_release(&metadata, "2.1.1"));
    }
}
