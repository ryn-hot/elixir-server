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
    artwork::ArtworkService,
    auth::AuthService,
    config::{DatabaseConfig, Settings},
    db::Database,
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
        let mut failures = Vec::new();

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
                    failures.push(format!(
                        "{} adapter: {}",
                        release.service.display_name(),
                        redacted_body(&err.to_string())
                    ));
                    continue;
                }
            };
            if let Err(err) = adapter.delete_release(&release.remote_release_id).await {
                failures.push(format!(
                    "{} remote cleanup for {}: {}",
                    release.service.display_name(),
                    release.remote_release_id,
                    redacted_body(&err.to_string())
                ));
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            bail!("live Debrid cleanup failed: {}", failures.join("; "))
        }
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
