#![allow(dead_code)]

use super::*;

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use tempfile::{Builder as TempDirBuilder, TempDir};
use tokio::time::sleep;

use crate::{
    acquisition::release_resolution::store::get_release_by_download_id,
    artwork::ArtworkService,
    auth::AuthService,
    config::{DatabaseConfig, Settings},
    db::Database,
    download_broker::{DownloadBrokerProviderKind, list_acquisition_routes},
    extensions::ExtensionManager,
    library::LinkerService,
    metadata::MetadataService,
};

const LIVE_DEBRID_OPT_IN_ENV: &str = "ELIXIR_LIVE_DEBRID_TESTS";
const LIVE_DEBRID_SERVICES_ENV: &str = "ELIXIR_LIVE_DEBRID_SERVICES";
const LIVE_DEBRID_OUTPUT_ROOT_ENV: &str = "ELIXIR_LIVE_DEBRID_OUTPUT_ROOT";
const LIVE_DEBRID_POLL_TIMEOUT_ENV: &str = "ELIXIR_LIVE_DEBRID_POLL_TIMEOUT_SECONDS";
const LIVE_DEBRID_POLL_INTERVAL_ENV: &str = "ELIXIR_LIVE_DEBRID_POLL_INTERVAL_SECONDS";

const LIVE_DEBRID_GLOBAL_SINGLE_MAGNET_ENV: &str = "ELIXIR_LIVE_DEBRID_SINGLE_MAGNET";
const LIVE_DEBRID_GLOBAL_MULTI_MAGNET_ENV: &str = "ELIXIR_LIVE_DEBRID_MULTI_MAGNET";
const LIVE_DEBRID_GLOBAL_HOSTER_URL_ENV: &str = "ELIXIR_LIVE_DEBRID_HOSTER_URL";

const DEFAULT_POLL_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 5;
const LIVE_REMOTE_CLEANUP_ATTEMPTS: usize = 4;
const LIVE_REMOTE_CLEANUP_RETRY_DELAY: Duration = Duration::from_secs(3);
const DP10B_SERVICES: &[DebridServiceKind] =
    &[DebridServiceKind::RealDebrid, DebridServiceKind::TorBox];
const DP10C_SERVICES: &[DebridServiceKind] =
    &[DebridServiceKind::AllDebrid, DebridServiceKind::Premiumize];

#[derive(Debug, Clone)]
struct LiveDebridConfig {
    enabled: bool,
    requested_services: Vec<DebridServiceKind>,
    services: Vec<LiveDebridServiceConfig>,
    output_root: Option<PathBuf>,
    poll_timeout: Duration,
    poll_interval: Duration,
}

#[derive(Debug, Clone)]
struct LiveDebridServiceConfig {
    service: DebridServiceKind,
    token_env: String,
    token: String,
    fixtures: LiveDebridFixtureSet,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LiveDebridFixtureSet {
    single_magnet: Option<String>,
    multi_file_magnet: Option<String>,
    hoster_url: Option<String>,
}

#[derive(Debug, Clone)]
struct LiveRemoteRelease {
    service: DebridServiceKind,
    instance_id: Uuid,
    remote_release_id: String,
}

#[derive(Debug, Default)]
struct LiveDebridCleanup {
    remote_releases: Vec<LiveRemoteRelease>,
}

struct LiveDebridTestRoot {
    root: PathBuf,
    _temp_dir: Option<TempDir>,
}

struct LiveDebridHarness {
    state: AppState,
    instance_id: Uuid,
    root: LiveDebridTestRoot,
    config: LiveDebridConfig,
    cleanup: LiveDebridCleanup,
}

impl LiveDebridConfig {
    fn from_env() -> Result<Self> {
        Self::from_vars(&collect_live_debrid_env())
    }

    fn enabled_from_env_or_skip() -> Result<Option<Self>> {
        Self::enabled_from_vars_or_skip(&collect_live_debrid_env())
    }

    fn enabled_from_vars_or_skip(vars: &BTreeMap<String, String>) -> Result<Option<Self>> {
        let config = Self::from_vars(vars)?;
        if config.enabled {
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    fn from_vars(vars: &BTreeMap<String, String>) -> Result<Self> {
        let enabled = parse_live_opt_in(vars.get(LIVE_DEBRID_OPT_IN_ENV))?;
        let requested_services = parse_requested_services(vars)?;
        let output_root = env_non_empty(vars, LIVE_DEBRID_OUTPUT_ROOT_ENV).map(PathBuf::from);
        let poll_timeout = Duration::from_secs(parse_duration_seconds(
            vars,
            LIVE_DEBRID_POLL_TIMEOUT_ENV,
            DEFAULT_POLL_TIMEOUT_SECONDS,
        )?);
        let poll_interval = Duration::from_secs(parse_duration_seconds(
            vars,
            LIVE_DEBRID_POLL_INTERVAL_ENV,
            DEFAULT_POLL_INTERVAL_SECONDS,
        )?);
        if poll_interval.is_zero() {
            bail!("{LIVE_DEBRID_POLL_INTERVAL_ENV} must be greater than zero");
        }

        let services = if enabled {
            requested_services
                .iter()
                .filter_map(|service| -> Option<Result<LiveDebridServiceConfig>> {
                    let token_env = live_token_env_key(*service);
                    let token = env_non_empty(vars, &token_env)?;
                    Some(LiveDebridServiceConfig::from_vars(
                        vars, *service, token_env, token,
                    ))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };

        Ok(Self {
            enabled,
            requested_services,
            services,
            output_root,
            poll_timeout,
            poll_interval,
        })
    }

    fn service(&self, service: DebridServiceKind) -> Option<&LiveDebridServiceConfig> {
        self.services.iter().find(|item| item.service == service)
    }

    fn is_service_enabled(&self, service: DebridServiceKind) -> bool {
        self.service(service).is_some()
    }

    fn redact_text(&self, value: &str) -> String {
        let mut redacted = value.to_string();
        for service in &self.services {
            let token = service.token.trim();
            if token.len() >= 4 {
                redacted = redacted.replace(token, "[redacted]");
            }
        }
        redacted
    }

    fn assert_text_redacted(&self, context: &str, value: &str) -> Result<()> {
        for service in &self.services {
            let token = service.token.trim();
            if token.len() >= 4 && value.contains(token) {
                bail!(
                    "unredacted {} token from {} found in {context}",
                    service.service.display_name(),
                    service.token_env
                );
            }
        }
        Ok(())
    }

    fn assert_json_redacted(&self, context: &str, value: &Value) -> Result<()> {
        self.assert_text_redacted(context, &serde_json::to_string(value)?)
    }
}

impl LiveDebridServiceConfig {
    fn from_vars(
        vars: &BTreeMap<String, String>,
        service: DebridServiceKind,
        token_env: String,
        token: String,
    ) -> Result<Self> {
        Ok(Self {
            service,
            token_env,
            token,
            fixtures: LiveDebridFixtureSet::from_vars(vars, service)?,
        })
    }
}

impl LiveDebridFixtureSet {
    fn from_vars(vars: &BTreeMap<String, String>, service: DebridServiceKind) -> Result<Self> {
        let single_magnet = env_non_empty(vars, &service_fixture_env_key(service, "SINGLE_MAGNET"))
            .or_else(|| env_non_empty(vars, LIVE_DEBRID_GLOBAL_SINGLE_MAGNET_ENV))
            .map(|value| validate_live_magnet_env("single magnet", &value))
            .transpose()?;
        let multi_file_magnet =
            env_non_empty(vars, &service_fixture_env_key(service, "MULTI_MAGNET"))
                .or_else(|| env_non_empty(vars, LIVE_DEBRID_GLOBAL_MULTI_MAGNET_ENV))
                .map(|value| validate_live_magnet_env("multi-file magnet", &value))
                .transpose()?;
        let hoster_url = env_non_empty(vars, &service_fixture_env_key(service, "HOSTER_URL"))
            .or_else(|| env_non_empty(vars, LIVE_DEBRID_GLOBAL_HOSTER_URL_ENV))
            .map(|value| validate_live_url_env("hoster URL", &value))
            .transpose()?;

        Ok(Self {
            single_magnet,
            multi_file_magnet,
            hoster_url,
        })
    }
}

impl LiveDebridCleanup {
    fn track_remote_release(
        &mut self,
        service: DebridServiceKind,
        instance_id: Uuid,
        remote_release_id: impl Into<String>,
    ) {
        self.remote_releases.push(LiveRemoteRelease {
            service,
            instance_id,
            remote_release_id: remote_release_id.into(),
        });
    }

    async fn cleanup_remote_releases(&mut self, state: &AppState) -> Result<()> {
        let store = ExtensionStore::new(&state.db_pool);
        let factory = DebridAdapterFactory::from_state(state);
        let releases = std::mem::take(&mut self.remote_releases);
        let mut warnings = Vec::new();

        for release in releases {
            let adapter = match factory
                .adapter_for_job_implementation(
                    &store,
                    release.instance_id,
                    Some(release.service.implementation_id()),
                )
                .await
            {
                Ok(adapter) => adapter,
                Err(err) => {
                    warnings.push(format!(
                        "{} adapter: {}",
                        release.service.display_name(),
                        redacted_body(&err.to_string())
                    ));
                    continue;
                }
            };
            let mut last_error = None;
            for attempt in 1..=LIVE_REMOTE_CLEANUP_ATTEMPTS {
                match adapter.delete_release(&release.remote_release_id).await {
                    Ok(_) => {
                        last_error = None;
                        break;
                    }
                    Err(err) => {
                        last_error = Some(err);
                        if attempt < LIVE_REMOTE_CLEANUP_ATTEMPTS {
                            sleep(LIVE_REMOTE_CLEANUP_RETRY_DELAY).await;
                        }
                    }
                }
            }
            if let Some(err) = last_error {
                warnings.push(format!(
                    "{} remote cleanup for {}: {}",
                    release.service.display_name(),
                    release.remote_release_id,
                    redacted_body(&err.to_string())
                ));
            }
        }

        if !warnings.is_empty() {
            eprintln!("live Debrid cleanup warning: {}", warnings.join("; "));
        }
        Ok(())
    }
}

impl LiveDebridTestRoot {
    async fn create(output_root: Option<&Path>) -> Result<Self> {
        if let Some(output_root) = output_root {
            let root = output_root.join(format!("elixir-live-debrid-{}", Uuid::new_v4()));
            tokio::fs::create_dir_all(&root)
                .await
                .with_context(|| format!("creating live Debrid output root {}", root.display()))?;
            Ok(Self {
                root,
                _temp_dir: None,
            })
        } else {
            let temp_dir = TempDirBuilder::new()
                .prefix("elixir-live-debrid-")
                .tempdir()
                .context("creating live Debrid temp root")?;
            Ok(Self {
                root: temp_dir.path().to_path_buf(),
                _temp_dir: Some(temp_dir),
            })
        }
    }

    fn path(&self) -> &Path {
        &self.root
    }

    fn downloads_root(&self) -> PathBuf {
        self.root.join("data").join("downloads")
    }

    async fn cleanup_local(&self) -> Result<()> {
        if self._temp_dir.is_none() && self.root.exists() {
            tokio::fs::remove_dir_all(&self.root)
                .await
                .with_context(|| format!("removing live Debrid root {}", self.root.display()))?;
        }
        Ok(())
    }
}

impl LiveDebridHarness {
    async fn create(config: LiveDebridConfig) -> Result<Self> {
        if !config.enabled {
            bail!("{LIVE_DEBRID_OPT_IN_ENV}=1 is required for live Debrid validation");
        }

        let root = LiveDebridTestRoot::create(config.output_root.as_deref()).await?;
        let mut settings = Settings::default();
        settings.database = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        settings.extensions.storage_root = root
            .path()
            .join("data")
            .join("extensions")
            .to_string_lossy()
            .to_string();
        settings.extensions.bundled_dir = root.path().join("bundled").to_string_lossy().to_string();
        settings.library.local_root = root.path().join("media").to_string_lossy().to_string();
        settings.library.artwork_cache_dir =
            root.path().join("artwork").to_string_lossy().to_string();

        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let auth_service = AuthService::new(settings.auth.clone())?;
        let metadata = MetadataService::new(settings.metadata.clone())?;
        let linkers = LinkerService::new(settings.classifier.clone())?;
        let artwork = ArtworkService::new(
            settings.library.artwork_cache_dir.clone(),
            settings.metadata.request_timeout_seconds,
        )?;
        let state = AppState::new(
            settings,
            database,
            auth_service,
            ExtensionManager::new(),
            metadata,
            linkers,
            artwork,
            SecretsManager::from_key_bytes([91u8; 32], true),
        );

        ensure_debrid_builtin(&state).await?;
        let store = ExtensionStore::new(&state.db_pool);
        let instance = store
            .list_instances(Some(DEBRID_EXTENSION_ID))
            .await?
            .into_iter()
            .next()
            .context("Debrid default instance should exist")?;

        for service in &config.services {
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Instance,
                    scope_id: Some(instance.instance_id),
                    key: service.service.secret_key().to_string(),
                    value_encrypted: state.secrets.encrypt(&service.token)?,
                    rotatable: false,
                })
                .await?;
        }

        Ok(Self {
            state,
            instance_id: instance.instance_id,
            root,
            config,
            cleanup: LiveDebridCleanup::default(),
        })
    }

    async fn set_active_service(&self, service: DebridServiceKind) -> Result<Uuid> {
        let store = ExtensionStore::new(&self.state.db_pool);
        let instance = store
            .get_instance(self.instance_id)
            .await?
            .context("Debrid instance should exist")?;
        let mut config = instance
            .config_json
            .clone()
            .unwrap_or_else(default_debrid_instance_config);
        if !config.is_object() {
            config = default_debrid_instance_config();
        }
        config[DEBRID_ACTIVE_SERVICE_CONFIG_KEY] = json!(service.implementation_id());
        store
            .update_instance_config(
                self.instance_id,
                Some(&normalized_debrid_instance_config(Some(config))),
            )
            .await?;
        reconcile_debrid_provider_for_instance(&self.state.db_pool, &store, self.instance_id).await
    }

    async fn adapter_for_service(
        &self,
        service: DebridServiceKind,
    ) -> Result<(Uuid, Box<dyn DebridProviderAdapter>)> {
        let provider_id = self.set_active_service(service).await?;
        let store = ExtensionStore::new(&self.state.db_pool);
        let adapter = DebridAdapterFactory::from_state(&self.state)
            .adapter_for_service(&store, self.instance_id, service)
            .await?;
        Ok((provider_id, adapter))
    }

    async fn track_job_remote_release(
        &mut self,
        service: DebridServiceKind,
        provider_id: Uuid,
        job_id: Uuid,
    ) -> Result<()> {
        if let Some(job) = load_debrid_job(&self.state.db_pool, job_id).await?
            && let Some(remote_release_id) = job
                .remote_release_id
                .as_deref()
                .or(job.remote_torrent_id.as_deref())
                .filter(|value| !value.trim().is_empty())
        {
            self.cleanup
                .track_remote_release(service, self.instance_id, remote_release_id);
        }

        for job in list_debrid_jobs_for_provider(&self.state.db_pool, provider_id).await? {
            let Some(remote_release_id) = job
                .remote_release_id
                .as_deref()
                .or(job.remote_torrent_id.as_deref())
                .filter(|value| !value.trim().is_empty())
            else {
                continue;
            };
            if !self.cleanup.remote_releases.iter().any(|release| {
                release.service == service && release.remote_release_id == remote_release_id
            }) {
                self.cleanup
                    .track_remote_release(service, self.instance_id, remote_release_id);
            }
        }

        Ok(())
    }

    async fn cleanup(&mut self) -> Result<()> {
        let remote_result = self.cleanup.cleanup_remote_releases(&self.state).await;
        let local_result = self.root.cleanup_local().await;
        remote_result?;
        local_result?;
        Ok(())
    }
}

async fn live_retry_until<T, F, Fut>(
    description: &str,
    timeout: Duration,
    interval: Duration,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let started = Instant::now();
    loop {
        if let Some(value) = operation().await? {
            return Ok(value);
        }
        if started.elapsed() >= timeout {
            bail!("timed out waiting for {description}");
        }
        sleep(interval.min(timeout.saturating_sub(started.elapsed()))).await;
    }
}

fn collect_live_debrid_env() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("ELIXIR_LIVE_DEBRID_"))
        .chain(
            DebridServiceKind::ALL
                .iter()
                .flat_map(|service| {
                    [
                        live_token_env_key(*service),
                        service_fixture_env_key(*service, "SINGLE_MAGNET"),
                        service_fixture_env_key(*service, "MULTI_MAGNET"),
                        service_fixture_env_key(*service, "HOSTER_URL"),
                    ]
                })
                .filter_map(|key| std::env::var(&key).ok().map(|value| (key, value))),
        )
        .collect()
}

fn parse_live_opt_in(value: Option<&String>) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "no" | "off" => Ok(false),
        "1" | "true" | "yes" | "on" => Ok(true),
        _ => bail!("{LIVE_DEBRID_OPT_IN_ENV} must be 1/true or 0/false"),
    }
}

fn parse_requested_services(vars: &BTreeMap<String, String>) -> Result<Vec<DebridServiceKind>> {
    let Some(raw) = env_non_empty(vars, LIVE_DEBRID_SERVICES_ENV) else {
        return Ok(DebridServiceKind::ALL.to_vec());
    };
    let mut seen = BTreeSet::new();
    let mut services = Vec::new();
    for item in raw.split(',') {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let service = DebridServiceKind::from_str(trimmed)?;
        if seen.insert(service.implementation_id()) {
            services.push(service);
        }
    }
    if services.is_empty() {
        bail!("{LIVE_DEBRID_SERVICES_ENV} did not contain any service ids");
    }
    Ok(services)
}

fn parse_duration_seconds(
    vars: &BTreeMap<String, String>,
    key: &str,
    default_value: u64,
) -> Result<u64> {
    let Some(value) = env_non_empty(vars, key) else {
        return Ok(default_value);
    };
    value
        .parse::<u64>()
        .with_context(|| format!("{key} must be a positive integer number of seconds"))
}

fn env_non_empty(vars: &BTreeMap<String, String>, key: &str) -> Option<String> {
    vars.get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn live_token_env_key(service: DebridServiceKind) -> String {
    format!("ELIXIR_LIVE_{}_TOKEN", live_service_env_slug(service))
}

fn service_fixture_env_key(service: DebridServiceKind, suffix: &str) -> String {
    format!("ELIXIR_LIVE_{}_{}", live_service_env_slug(service), suffix)
}

fn live_service_env_slug(service: DebridServiceKind) -> &'static str {
    match service {
        DebridServiceKind::RealDebrid => "REAL_DEBRID",
        DebridServiceKind::TorBox => "TORBOX",
        DebridServiceKind::AllDebrid => "ALL_DEBRID",
        DebridServiceKind::Premiumize => "PREMIUMIZE",
    }
}

fn validate_live_magnet_env(label: &str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    if !trimmed.to_ascii_lowercase().starts_with("magnet:?") {
        bail!("live Debrid {label} fixture must be a magnet URI");
    }
    if extract_magnet_info_hash(trimmed).is_none() {
        bail!("live Debrid {label} fixture magnet must include a btih info hash");
    }
    Ok(trimmed.to_string())
}

fn validate_live_url_env(label: &str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    let url = Url::parse(trimmed).with_context(|| format!("parsing live Debrid {label}"))?;
    match url.scheme() {
        "http" | "https" => Ok(trimmed.to_string()),
        _ => bail!("live Debrid {label} must be an HTTP(S) URL"),
    }
}

fn fixture_magnet(hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{hash}&dn=elixir-public-domain-fixture")
}

fn dp10b_service_configs(config: &LiveDebridConfig) -> Vec<LiveDebridServiceConfig> {
    config
        .services
        .iter()
        .filter(|service| DP10B_SERVICES.contains(&service.service))
        .cloned()
        .collect()
}

fn dp10c_service_configs(config: &LiveDebridConfig) -> Vec<LiveDebridServiceConfig> {
    config
        .services
        .iter()
        .filter(|service| DP10C_SERVICES.contains(&service.service))
        .cloned()
        .collect()
}

fn dp10d_service_configs(config: &LiveDebridConfig) -> Vec<LiveDebridServiceConfig> {
    config.services.clone()
}

fn live_fixture_magnet_for_service(service: &LiveDebridServiceConfig) -> Option<&str> {
    service
        .fixtures
        .single_magnet
        .as_deref()
        .or(service.fixtures.multi_file_magnet.as_deref())
}

fn live_fixture_hoster_for_service(service: &LiveDebridServiceConfig) -> Option<&str> {
    service.fixtures.hoster_url.as_deref()
}

fn live_selectable_media_file(inspection: &DebridReleaseInspection) -> Option<&DebridRemoteFile> {
    inspection
        .files
        .iter()
        .filter(|file| file.selectable)
        .find(|file| {
            is_debrid_media_file(&file.path) && !is_debrid_sample_or_extra_file(&file.path)
        })
}

async fn live_wait_for_file_listing(
    config: &LiveDebridConfig,
    service: DebridServiceKind,
    adapter: &dyn DebridProviderAdapter,
    remote_release_id: &str,
) -> Result<DebridReleaseInspection> {
    live_retry_until(
        &format!("{} file listing", service.display_name()),
        config.poll_timeout,
        config.poll_interval,
        || async {
            let inspection = adapter
                .inspect_release(remote_release_id)
                .await
                .map_err(|err| live_redacted_provider_error(config, service, "inspect", err))?;
            config.assert_json_redacted("live Debrid inspection", &json!(inspection))?;
            if !inspection.files.is_empty() {
                Ok(Some(inspection))
            } else if inspection.release.status == DebridReleaseStatus::Failed {
                bail!(
                    "{} live fixture failed during inspection: {:?}",
                    service.display_name(),
                    inspection.release.raw_status
                )
            } else {
                Ok(None)
            }
        },
    )
    .await
}

async fn live_wait_for_selected_links(
    config: &LiveDebridConfig,
    service: DebridServiceKind,
    adapter: &dyn DebridProviderAdapter,
    remote_release_id: &str,
    selected_file_ids: &[String],
    pool: &sqlx::AnyPool,
    job_id: Uuid,
) -> Result<DebridReleaseInspection> {
    live_retry_until(
        &format!("{} selected file links", service.display_name()),
        config.poll_timeout,
        config.poll_interval,
        || async {
            let selected = adapter
                .select_files(remote_release_id, selected_file_ids)
                .await
                .map_err(|err| {
                    live_redacted_provider_error(config, service, "select_files", err)
                })?;
            config.assert_json_redacted("live Debrid selected inspection", &json!(selected))?;
            update_debrid_job_from_inspection(pool, job_id, &selected).await?;

            if selected.release.status == DebridReleaseStatus::Failed {
                bail!(
                    "{} live fixture failed after file selection: {:?}",
                    service.display_name(),
                    selected.release.raw_status
                );
            }
            if selected.release.status == DebridReleaseStatus::Downloaded
                && !selected.links.is_empty()
            {
                Ok(Some(selected))
            } else {
                Ok(None)
            }
        },
    )
    .await
}

fn live_redacted_provider_error(
    config: &LiveDebridConfig,
    service: DebridServiceKind,
    stage: &str,
    err: anyhow::Error,
) -> anyhow::Error {
    let redacted = config.redact_text(&redacted_body(&err.to_string()));
    anyhow!("{} live {stage} failed: {redacted}", service.display_name())
}

async fn assert_dp10d_active_route(
    harness: &LiveDebridHarness,
    provider_id: Uuid,
    service: DebridServiceKind,
) -> Result<()> {
    let store = ExtensionStore::new(&harness.state.db_pool);
    let provider = store
        .get_provider(provider_id)
        .await?
        .context("active Debrid provider should exist")?;
    assert_eq!(provider.provider_id, provider_id);
    assert_eq!(
        provider.implementation.as_deref(),
        Some(service.implementation_id())
    );
    assert_eq!(provider.health_state, ProviderHealthState::Healthy);
    assert_eq!(
        provider
            .scope_json
            .as_ref()
            .and_then(|scope| scope.pointer("/download_broker/activeService"))
            .and_then(Value::as_str),
        Some(service.implementation_id())
    );

    let routes = list_acquisition_routes(&harness.state.db_pool, &store).await?;
    let route = routes
        .routes
        .iter()
        .find(|route| {
            route.logical_id == DEBRID_DEFAULT_LOGICAL_ID
                && route.owner_id == DEFAULT_ROUTE_OWNER_ID
        })
        .context("default Debrid acquisition route should exist")?;
    assert_eq!(route.selected_provider_id, Some(provider_id));
    assert_eq!(
        route.selected_provider_kind,
        Some(DownloadBrokerProviderKind::Debrid)
    );
    assert_eq!(
        route.selected_extension_id.as_deref(),
        Some(DEBRID_EXTENSION_ID)
    );
    assert!(
        route.blocker.is_none(),
        "unexpected Debrid route blocker: {:?}",
        route.blocker
    );
    assert!(route.candidates.iter().any(|candidate| {
        candidate.provider_id == provider_id
            && candidate.provider_kind == DownloadBrokerProviderKind::Debrid
            && candidate.implementation.as_deref() == Some(service.implementation_id())
            && candidate.selected
            && candidate.health_state == ProviderHealthState::Healthy
    }));

    Ok(())
}

fn dp10d_fake_config_vars() -> BTreeMap<String, String> {
    let mut vars = BTreeMap::new();
    vars.insert(LIVE_DEBRID_OPT_IN_ENV.to_string(), "1".to_string());
    vars.insert(
        LIVE_DEBRID_SERVICES_ENV.to_string(),
        DebridServiceKind::ALL
            .into_iter()
            .map(DebridServiceKind::implementation_id)
            .collect::<Vec<_>>()
            .join(","),
    );
    for service in DebridServiceKind::ALL {
        vars.insert(
            live_token_env_key(service),
            format!("dp10d-{}-secret-token", service.implementation_id()),
        );
    }
    vars
}

async fn run_live_account_validation(
    harness: &mut LiveDebridHarness,
    services: &[LiveDebridServiceConfig],
) -> Result<()> {
    for service in services {
        let (provider_id, _) = harness.adapter_for_service(service.service).await?;
        let store = ExtensionStore::new(&harness.state.db_pool);
        let account = test_debrid_service_account(
            &harness.state,
            &store,
            harness.instance_id,
            service.service,
        )
        .await
        .map_err(|err| {
            live_redacted_provider_error(
                &harness.config,
                service.service,
                "account validation",
                err,
            )
        })?;

        assert_eq!(
            account.provider_implementation,
            service.service.implementation_id()
        );
        harness
            .config
            .assert_json_redacted("live Debrid account response", &json!(account))?;

        let provider = store
            .get_provider(provider_id)
            .await?
            .context("live Debrid provider should exist after active service switch")?;
        assert_eq!(
            provider.implementation.as_deref(),
            Some(service.service.implementation_id())
        );
    }

    Ok(())
}

async fn run_live_magnet_lifecycle(
    harness: &mut LiveDebridHarness,
    services: &[LiveDebridServiceConfig],
    phase: &str,
) -> Result<()> {
    for service in services {
        let Some(magnet) = live_fixture_magnet_for_service(service) else {
            continue;
        };
        let (provider_id, adapter) = harness.adapter_for_service(service.service).await?;
        let name = format!(
            "elixir-{phase}-{}-fixture",
            service.service.implementation_id()
        );
        let job_id = {
            let store = ExtensionStore::new(&harness.state.db_pool);
            match submit_debrid(
                &harness.state,
                &store,
                provider_id,
                harness.instance_id,
                Some(service.service.implementation_id()),
                magnet,
                DebridSubmitOptions {
                    owner_id: "live.debrid.validation",
                    category: Some("live-debrid"),
                    name: Some(&name),
                    paused: false,
                    release_context: None,
                },
            )
            .await
            {
                Ok(job_id) => job_id,
                Err(err) => {
                    let _ = harness
                        .track_job_remote_release(service.service, provider_id, Uuid::nil())
                        .await;
                    return Err(live_redacted_provider_error(
                        &harness.config,
                        service.service,
                        "submit",
                        err,
                    ));
                }
            }
        };

        harness
            .track_job_remote_release(service.service, provider_id, job_id)
            .await?;
        let job = load_debrid_job(&harness.state.db_pool, job_id)
            .await?
            .context("live Debrid job should be persisted after submit")?;
        assert_eq!(
            job.provider_implementation.as_deref(),
            Some(service.service.implementation_id())
        );
        let remote_release_id = job
            .remote_release_id
            .as_deref()
            .or(job.remote_torrent_id.as_deref())
            .context("live Debrid job should persist a remote release id")?
            .to_string();

        let inspection = live_wait_for_file_listing(
            &harness.config,
            service.service,
            &*adapter,
            &remote_release_id,
        )
        .await?;
        let media_file = live_selectable_media_file(&inspection).with_context(|| {
            format!(
                "{} live fixture must expose at least one selectable media file",
                service.service.display_name()
            )
        })?;
        let selected_file_ids = vec![media_file.provider_file_id.clone()];
        let selected = live_wait_for_selected_links(
            &harness.config,
            service.service,
            &*adapter,
            &remote_release_id,
            &selected_file_ids,
            &harness.state.db_pool,
            job_id,
        )
        .await?;
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.selected_file_ids.as_slice()),
            Some(selected_file_ids.as_slice())
        );

        let store = ExtensionStore::new(&harness.state.db_pool);
        let progress =
            load_debrid_progress(&harness.state, &store, provider_id, harness.instance_id).await?;
        let item = progress
            .iter()
            .find(|item| item.id == job_id.to_string())
            .context("live Debrid progress item should include submitted job")?;
        let evidence = item
            .debrid
            .as_ref()
            .context("live Debrid progress should include provider evidence")?;
        assert_eq!(
            evidence.provider_implementation.as_deref(),
            Some(service.service.implementation_id())
        );
        assert_eq!(
            evidence.provider_name.as_deref(),
            Some(service.service.display_name())
        );
        assert!(evidence.selected_file_count >= 1);

        process_debrid_jobs_once(&harness.state)
            .await
            .map_err(|err| {
                live_redacted_provider_error(&harness.config, service.service, "materialize", err)
            })?;

        let job = load_debrid_job(&harness.state.db_pool, job_id)
            .await?
            .context("live Debrid job should load after materialization")?;
        assert_eq!(job.status, "completed");
        assert_eq!(
            job.provider_implementation.as_deref(),
            Some(service.service.implementation_id())
        );
        assert_eq!(job.progress, Some(1.0));
        let local_path = PathBuf::from(
            job.local_path
                .as_deref()
                .context("live Debrid materializer should persist local path")?,
        );
        assert!(local_path.exists());
        assert!(local_path.starts_with(harness.root.downloads_root()));
        let metadata = tokio::fs::metadata(&local_path).await?;
        if metadata.is_file() {
            assert!(
                metadata.len() > 0,
                "live materialized file should not be empty"
            );
        }

        if let Some(release) =
            get_release_by_download_id(&harness.state.db_pool, &job_id.to_string()).await?
        {
            harness
                .config
                .assert_json_redacted("live Debrid release provenance", &json!(release))?;
        }
    }

    Ok(())
}

async fn run_live_hoster_lifecycle(
    harness: &mut LiveDebridHarness,
    services: &[LiveDebridServiceConfig],
    phase: &str,
) -> Result<()> {
    for service in services {
        let Some(hoster_url) = live_fixture_hoster_for_service(service) else {
            continue;
        };
        let (provider_id, _) = harness.adapter_for_service(service.service).await?;
        let name = format!(
            "elixir-{phase}-{}-hoster-fixture",
            service.service.implementation_id()
        );
        let job_id = {
            let store = ExtensionStore::new(&harness.state.db_pool);
            submit_debrid(
                &harness.state,
                &store,
                provider_id,
                harness.instance_id,
                Some(service.service.implementation_id()),
                hoster_url,
                DebridSubmitOptions {
                    owner_id: "live.debrid.validation",
                    category: Some("live-debrid"),
                    name: Some(&name),
                    paused: false,
                    release_context: None,
                },
            )
            .await
            .map_err(|err| {
                live_redacted_provider_error(&harness.config, service.service, "hoster submit", err)
            })?
        };

        let job = load_debrid_job(&harness.state.db_pool, job_id)
            .await?
            .context("live Debrid hoster job should be persisted after submit")?;
        assert_eq!(
            job.provider_implementation.as_deref(),
            Some(service.service.implementation_id())
        );
        assert_eq!(job.source_kind, "hoster");
        assert!(
            !job.links.is_empty(),
            "{} live hoster fixture should expose a materializable link",
            service.service.display_name()
        );

        let store = ExtensionStore::new(&harness.state.db_pool);
        let progress =
            load_debrid_progress(&harness.state, &store, provider_id, harness.instance_id).await?;
        let item = progress
            .iter()
            .find(|item| item.id == job_id.to_string())
            .context("live Debrid hoster progress item should include submitted job")?;
        let evidence = item
            .debrid
            .as_ref()
            .context("live Debrid hoster progress should include provider evidence")?;
        assert_eq!(
            evidence.provider_implementation.as_deref(),
            Some(service.service.implementation_id())
        );
        assert_eq!(
            evidence.provider_name.as_deref(),
            Some(service.service.display_name())
        );
        harness
            .config
            .assert_text_redacted("live Debrid hoster progress", &format!("{progress:?}"))?;

        process_debrid_jobs_once(&harness.state)
            .await
            .map_err(|err| {
                live_redacted_provider_error(
                    &harness.config,
                    service.service,
                    "hoster materialize",
                    err,
                )
            })?;

        let job = load_debrid_job(&harness.state.db_pool, job_id)
            .await?
            .context("live Debrid hoster job should load after materialization")?;
        assert_eq!(job.status, "completed");
        assert_eq!(
            job.provider_implementation.as_deref(),
            Some(service.service.implementation_id())
        );
        assert_eq!(job.progress, Some(1.0));
        let local_path = PathBuf::from(
            job.local_path
                .as_deref()
                .context("live Debrid hoster materializer should persist local path")?,
        );
        assert!(local_path.exists());
        assert!(local_path.starts_with(harness.root.downloads_root()));
        let metadata = tokio::fs::metadata(&local_path).await?;
        if metadata.is_file() {
            assert!(
                metadata.len() > 0,
                "live hoster materialized file should not be empty"
            );
        }

        if let Some(release) =
            get_release_by_download_id(&harness.state.db_pool, &job_id.to_string()).await?
        {
            harness
                .config
                .assert_json_redacted("live Debrid hoster release provenance", &json!(release))?;
        }
    }

    Ok(())
}

#[test]
fn live_debrid_config_is_disabled_without_opt_in() -> Result<()> {
    let vars = BTreeMap::new();
    let config = LiveDebridConfig::from_vars(&vars)?;

    assert!(!config.enabled);
    assert!(LiveDebridConfig::enabled_from_vars_or_skip(&vars)?.is_none());
    assert!(config.services.is_empty());
    assert_eq!(config.requested_services, DebridServiceKind::ALL.to_vec());
    assert_eq!(
        config.poll_timeout,
        Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECONDS)
    );
    assert_eq!(
        config.poll_interval,
        Duration::from_secs(DEFAULT_POLL_INTERVAL_SECONDS)
    );
    Ok(())
}

#[test]
fn live_debrid_config_selects_requested_services_with_tokens_only() -> Result<()> {
    let mut vars = BTreeMap::new();
    vars.insert(LIVE_DEBRID_OPT_IN_ENV.to_string(), "1".to_string());
    vars.insert(
        LIVE_DEBRID_SERVICES_ENV.to_string(),
        "real_debrid,torbox,premiumize".to_string(),
    );
    vars.insert(
        live_token_env_key(DebridServiceKind::RealDebrid),
        "rd-live-token".to_string(),
    );
    vars.insert(
        live_token_env_key(DebridServiceKind::Premiumize),
        "pm-live-token".to_string(),
    );

    let config = LiveDebridConfig::from_vars(&vars)?;

    assert!(config.enabled);
    assert_eq!(config.requested_services.len(), 3);
    assert!(config.is_service_enabled(DebridServiceKind::RealDebrid));
    assert!(!config.is_service_enabled(DebridServiceKind::TorBox));
    assert!(config.is_service_enabled(DebridServiceKind::Premiumize));
    assert_eq!(config.services.len(), 2);
    Ok(())
}

#[test]
fn live_debrid_config_rejects_invalid_service_selector() {
    let mut vars = BTreeMap::new();
    vars.insert(LIVE_DEBRID_OPT_IN_ENV.to_string(), "1".to_string());
    vars.insert(
        LIVE_DEBRID_SERVICES_ENV.to_string(),
        "real_debrid,not_a_service".to_string(),
    );

    let err = LiveDebridConfig::from_vars(&vars).expect_err("invalid service should fail");
    assert!(err.to_string().contains("unsupported debrid service"));
}

#[test]
fn live_debrid_fixture_env_uses_service_override_and_validates_inputs() -> Result<()> {
    let mut vars = BTreeMap::new();
    vars.insert(LIVE_DEBRID_OPT_IN_ENV.to_string(), "1".to_string());
    vars.insert(
        live_token_env_key(DebridServiceKind::TorBox),
        "tb-live-token".to_string(),
    );
    vars.insert(
        LIVE_DEBRID_GLOBAL_SINGLE_MAGNET_ENV.to_string(),
        fixture_magnet("0123456789abcdef0123456789abcdef01234567"),
    );
    vars.insert(
        service_fixture_env_key(DebridServiceKind::TorBox, "SINGLE_MAGNET"),
        fixture_magnet("abcdefabcdefabcdefabcdefabcdefabcdefabcd"),
    );
    vars.insert(
        LIVE_DEBRID_GLOBAL_HOSTER_URL_ENV.to_string(),
        "https://example.invalid/public-domain-fixture.bin".to_string(),
    );

    let config = LiveDebridConfig::from_vars(&vars)?;
    let torbox = config
        .service(DebridServiceKind::TorBox)
        .context("TorBox should be configured")?;

    assert_eq!(
        torbox.fixtures.single_magnet.as_deref(),
        Some(
            "magnet:?xt=urn:btih:abcdefabcdefabcdefabcdefabcdefabcdefabcd&dn=elixir-public-domain-fixture"
        )
    );
    assert_eq!(
        torbox.fixtures.hoster_url.as_deref(),
        Some("https://example.invalid/public-domain-fixture.bin")
    );

    vars.insert(
        LIVE_DEBRID_GLOBAL_MULTI_MAGNET_ENV.to_string(),
        "https://example.invalid/not-a-magnet".to_string(),
    );
    let err = LiveDebridConfig::from_vars(&vars).expect_err("invalid magnet should fail");
    assert!(err.to_string().contains("must be a magnet URI"));
    Ok(())
}

#[test]
fn live_debrid_redaction_guard_detects_tokens_without_leaking_them() -> Result<()> {
    let mut vars = BTreeMap::new();
    vars.insert(LIVE_DEBRID_OPT_IN_ENV.to_string(), "1".to_string());
    vars.insert(
        live_token_env_key(DebridServiceKind::RealDebrid),
        "rd-secret-token".to_string(),
    );
    let config = LiveDebridConfig::from_vars(&vars)?;

    let err = config
        .assert_text_redacted("provider log", "provider returned rd-secret-token")
        .expect_err("token leak should fail redaction guard");
    let message = err.to_string();
    assert!(message.contains("Real-Debrid"));
    assert!(message.contains("provider log"));
    assert!(!message.contains("rd-secret-token"));

    let redacted = config.redact_text("provider returned rd-secret-token");
    assert_eq!(redacted, "provider returned [redacted]");
    config.assert_json_redacted(
        "safe UI payload",
        &json!({
            "service": "real_debrid",
            "state": "configured",
            "token": "[redacted]"
        }),
    )?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_harness_bootstraps_isolated_state_without_network_calls() -> Result<()> {
    let mut vars = BTreeMap::new();
    vars.insert(LIVE_DEBRID_OPT_IN_ENV.to_string(), "1".to_string());
    vars.insert(
        LIVE_DEBRID_SERVICES_ENV.to_string(),
        "real_debrid".to_string(),
    );
    vars.insert(
        live_token_env_key(DebridServiceKind::RealDebrid),
        "rd-live-token".to_string(),
    );
    let config = LiveDebridConfig::from_vars(&vars)?;
    let mut harness = LiveDebridHarness::create(config).await?;

    let provider_id = harness
        .set_active_service(DebridServiceKind::RealDebrid)
        .await?;
    let store = ExtensionStore::new(&harness.state.db_pool);
    assert!(
        store
            .get_extension(DEBRID_EXTENSION_ID)
            .await?
            .is_some_and(|extension| extension.enabled)
    );
    assert!(store.get_instance(harness.instance_id).await?.is_some());
    assert!(store.get_provider(provider_id).await?.is_some());

    let secret = store
        .get_secret(
            SecretScope::Instance,
            Some(harness.instance_id),
            DebridServiceKind::RealDebrid.secret_key(),
        )
        .await?
        .context("live token should be stored as an encrypted instance secret")?;
    assert_eq!(
        harness.state.secrets.decrypt(&secret.value_encrypted)?,
        "rd-live-token"
    );
    assert!(
        RuntimePaths::from_roots(
            &harness.state.settings.extensions.storage_root,
            &harness.state.settings.library.local_root,
        )
        .downloads_root
        .starts_with(&harness.root.downloads_root().to_string_lossy().to_string())
    );

    harness.config.assert_json_redacted(
        "provider config",
        &json!({
            "providerId": provider_id,
            "activeService": "real_debrid",
            "token": "[redacted]"
        }),
    )?;
    harness.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_dp10b_real_debrid_torbox_account_validation() -> Result<()> {
    let Some(config) = LiveDebridConfig::enabled_from_env_or_skip()? else {
        return Ok(());
    };
    let services = dp10b_service_configs(&config);
    if services.is_empty() {
        return Ok(());
    }
    let mut harness = LiveDebridHarness::create(config).await?;

    let result = run_live_account_validation(&mut harness, &services).await;
    let cleanup = harness.cleanup().await;
    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_dp10b_real_debrid_torbox_magnet_lifecycle() -> Result<()> {
    let Some(config) = LiveDebridConfig::enabled_from_env_or_skip()? else {
        return Ok(());
    };
    let services = dp10b_service_configs(&config)
        .into_iter()
        .filter(|service| live_fixture_magnet_for_service(service).is_some())
        .collect::<Vec<_>>();
    if services.is_empty() {
        return Ok(());
    }
    let mut harness = LiveDebridHarness::create(config).await?;

    let result = run_live_magnet_lifecycle(&mut harness, &services, "dp10b").await;
    let cleanup = harness.cleanup().await;
    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_dp10c_all_debrid_premiumize_account_validation() -> Result<()> {
    let Some(config) = LiveDebridConfig::enabled_from_env_or_skip()? else {
        return Ok(());
    };
    let services = dp10c_service_configs(&config);
    if services.is_empty() {
        return Ok(());
    }
    let mut harness = LiveDebridHarness::create(config).await?;

    let result = run_live_account_validation(&mut harness, &services).await;
    let cleanup = harness.cleanup().await;
    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_dp10c_all_debrid_premiumize_magnet_lifecycle() -> Result<()> {
    let Some(config) = LiveDebridConfig::enabled_from_env_or_skip()? else {
        return Ok(());
    };
    let services = dp10c_service_configs(&config)
        .into_iter()
        .filter(|service| live_fixture_magnet_for_service(service).is_some())
        .collect::<Vec<_>>();
    if services.is_empty() {
        return Ok(());
    }
    let mut harness = LiveDebridHarness::create(config).await?;

    let result = run_live_magnet_lifecycle(&mut harness, &services, "dp10c").await;
    let cleanup = harness.cleanup().await;
    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_dp10c_all_debrid_premiumize_hoster_lifecycle() -> Result<()> {
    let Some(config) = LiveDebridConfig::enabled_from_env_or_skip()? else {
        return Ok(());
    };
    let services = dp10c_service_configs(&config)
        .into_iter()
        .filter(|service| live_fixture_hoster_for_service(service).is_some())
        .collect::<Vec<_>>();
    if services.is_empty() {
        return Ok(());
    }
    let mut harness = LiveDebridHarness::create(config).await?;

    let result = run_live_hoster_lifecycle(&mut harness, &services, "dp10c").await;
    let cleanup = harness.cleanup().await;
    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_dp10d_product_switch_matrix_without_network_calls() -> Result<()> {
    let config = LiveDebridConfig::from_vars(&dp10d_fake_config_vars())?;
    let mut harness = LiveDebridHarness::create(config).await?;
    let mut stable_provider_id = None;

    for service in DebridServiceKind::ALL {
        let provider_id = harness.set_active_service(service).await?;
        if let Some(existing_provider_id) = stable_provider_id {
            assert_eq!(
                provider_id, existing_provider_id,
                "active Debrid service switches must keep the canonical provider id"
            );
        } else {
            stable_provider_id = Some(provider_id);
        }
        assert_dp10d_active_route(&harness, provider_id, service).await?;

        harness.config.assert_json_redacted(
            "DP-10D control payload",
            &json!({
                "extensionId": DEBRID_EXTENSION_ID,
                "title": "Debrid accounts",
                "activeService": service.implementation_id(),
                "activeServiceLabel": service.display_name(),
                "accountState": "configured",
                "providerId": provider_id,
                "token": "[redacted]",
                "actions": [
                    "set_active_debrid_service",
                    "remove_debrid_service_account"
                ]
            }),
        )?;
        harness.config.assert_json_redacted(
            "DP-10D candidate provider payload",
            &json!({
                "provider": {
                    "capability": "acquisition.candidate_provider",
                    "config": {
                        "baseUrl": "https://source.example/manifest.json",
                        "resultLimit": 25
                    }
                },
                "request": {
                    "mediaType": "series",
                    "title": "Example Show",
                    "preferences": {
                        "routePolicy": "debrid_first"
                    }
                },
                "routeOptions": [{
                    "logicalId": DEBRID_DEFAULT_LOGICAL_ID,
                    "label": "Debrid",
                    "selectedProviderId": provider_id
                }]
            }),
        )?;
    }

    harness.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_dp10d_cross_provider_account_switching() -> Result<()> {
    let Some(config) = LiveDebridConfig::enabled_from_env_or_skip()? else {
        return Ok(());
    };
    let services = dp10d_service_configs(&config);
    if services.is_empty() {
        return Ok(());
    }
    let mut harness = LiveDebridHarness::create(config).await?;

    let account_result = run_live_account_validation(&mut harness, &services).await;
    let route_result = async {
        let mut stable_provider_id = None;
        for service in services {
            let provider_id = harness.set_active_service(service.service).await?;
            if let Some(existing_provider_id) = stable_provider_id {
                assert_eq!(provider_id, existing_provider_id);
            } else {
                stable_provider_id = Some(provider_id);
            }
            assert_dp10d_active_route(&harness, provider_id, service.service).await?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let cleanup = harness.cleanup().await;
    account_result?;
    route_result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn live_debrid_retry_helper_waits_until_ready() -> Result<()> {
    let mut attempts = 0usize;
    let value = live_retry_until(
        "fixture readiness",
        Duration::from_secs(2),
        Duration::from_millis(10),
        || {
            attempts += 1;
            async move {
                if attempts >= 3 {
                    Ok(Some("ready"))
                } else {
                    Ok(None)
                }
            }
        },
    )
    .await?;

    assert_eq!(value, "ready");
    assert_eq!(attempts, 3);
    Ok(())
}
