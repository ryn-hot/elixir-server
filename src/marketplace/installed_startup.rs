//! Trusted local startup of retained installed code, without peer/issuer/VPN
//! availability. No remote API can configure this profile or native owner.
use super::{
    application::Allocation,
    archive::ArchiveBudget,
    grant_store::installation::{InstallBudget, runtime::InstalledRuntime},
    quarantine::SourceJobBudget,
};
use crate::{db::Database, runtime::RuntimePaths, state::AppState};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{canonical, source_execution::SourceExecutionLimits};
use serde::{Deserialize, de::IntoDeserializer};
use std::path::Path;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod application_http;
pub(super) mod storage;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    schema_version: u32,
    worker_image: String,
    installations: Metadata,
    source: Source,
    retention: Retention,
    execution: SourceExecutionLimits,
    // Explicit trusted-operator opt-in only; no peer/admin request can set it.
    application_http: Option<application_http::Profile>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    entries: usize,
    bytes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retention {
    bytes: u64,
    releases: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    memory_bytes: u64,
    cpu_millicores: u32,
    disk_bytes: u64,
    input_bytes: u64,
    output_bytes: u64,
    wall_seconds: u64,
}
impl Profile {
    fn parse(bytes: &[u8]) -> Result<Self> {
        let value = canonical::parse(bytes, 32 * 1024)?;
        let mut ignored = false;
        let profile: Self =
            serde_ignored::deserialize(value.into_deserializer(), |_| ignored = true)?;
        if ignored
            || profile.schema_version != 1
            || !super::runtime::image_is_pinned(&profile.worker_image)
        {
            bail!("invalid closed installed startup profile");
        }
        profile.allocation().validate()?;
        Ok(profile)
    }
    fn allocation(&self) -> Allocation {
        Allocation {
            installations: InstallBudget {
                entries: self.installations.entries,
                bytes: self.installations.bytes,
            },
            source: SourceJobBudget {
                memory_bytes: self.source.memory_bytes,
                cpu_millicores: self.source.cpu_millicores,
                disk_bytes: self.source.disk_bytes,
                input_bytes: self.source.input_bytes,
                output_bytes: self.source.output_bytes,
                wall_seconds: self.source.wall_seconds,
            },
            retention: ArchiveBudget {
                bytes: self.retention.bytes,
                releases: self.retention.releases,
            },
            execution: self.execution.clone(),
        }
    }
}

fn configuration(state: &AppState, profile: &Profile) -> Result<InstalledRuntime> {
    let paths = RuntimePaths::from_roots(
        &state.settings.extensions.storage_root,
        &state.settings.library.local_root,
    );
    let directory = storage::prepare(Path::new(&paths.data_root))?;
    let allocation = profile.allocation();
    Ok(InstalledRuntime {
        runtime: state.orchestrator.runtime_manager(),
        playback: state.transcodes.clone(),
        local_http: state.marketplace.local_http_client(),
        application_http: None,
        compiled_toolchains: None,
        lane: directory.join("foreground-source"),
        archive: directory.join("source-archives"),
        deployment: format!("installed-{}", uuid::Uuid::new_v4().simple()),
        image: profile.worker_image.clone(),
        source_budget: allocation.source,
        retention: allocation.retention,
    })
}

async fn activate(
    state: &AppState,
    path: &Path,
    shutdown: &CancellationToken,
    stage: &mut &'static str,
) -> Result<()> {
    let path = path.to_owned();
    let profile = tokio::task::spawn_blocking(move || {
        Profile::parse(&super::startup::read_private_profile_bytes(&path)?)
    })
    .await??;
    if shutdown.is_cancelled() {
        return Ok(());
    }
    *stage = "private_storage";
    let mut configuration = configuration(state, &profile)?;
    // Validate all allocations before permitting even lineage initialization.
    *stage = "allocation_validation";
    super::application::Host::from_runtime(
        Database {
            pool: state.db_pool.clone(),
            driver: state.db_driver,
        },
        state.secrets.as_ref().clone(),
        configuration.clone(),
        profile.allocation().installations,
        profile.execution.clone(),
    )?;
    *stage = "runtime_readiness";
    state.orchestrator.ensure_runtime_ready().await?;
    if shutdown.is_cancelled() {
        return Ok(());
    }
    if let Some(http) = &profile.application_http {
        *stage = "application_http";
        let protected = http
            .configure(
                configuration.runtime.as_ref(),
                configuration
                    .lane
                    .parent()
                    .context("installed source directory unavailable")?,
            )
            .await;
        configuration.application_http = Some(match protected {
            Ok(configured) => configured,
            Err(_) => {
                // Optional networking must not prevent retained offline/LAN
                // execution. Keep an explicit unavailable selection, not None:
                // public HTTP must not fall back to the catalog's allowance.
                tracing::warn!(
                    "separate installed HTTP underlay unavailable; protected public requests disabled"
                );
                http.unavailable()
            }
        });
    }
    if shutdown.is_cancelled() {
        return Ok(());
    }
    *stage = "installation_allocation";
    state
        .marketplace
        .configure_installation(profile.allocation())?;
    *stage = "restoration_start";
    state.marketplace.begin_installed_restoration(
        Database {
            pool: state.db_pool.clone(),
            driver: state.db_driver,
        },
        state.secrets.as_ref().clone(),
        configuration.clone(),
    )?;
    // Cancelling this borrowed wait cannot abort the application's task.
    *stage = "restoration_join";
    state.marketplace.finish_installed_restoration().await?;
    if !shutdown.is_cancelled() {
        *stage = "installed_sharing";
        state.marketplace.begin_installed_sharing(
            Database {
                pool: state.db_pool.clone(),
                driver: state.db_driver,
            },
            &configuration,
        )?;
        *stage = "automatic_updates";
        state.marketplace.begin_automatic_updates(
            Database {
                pool: state.db_pool.clone(),
                driver: state.db_driver,
            },
            state.secrets.as_ref().clone(),
            configuration,
        )?;
    }
    Ok(())
}

/// None means no profile read, SQL/native inspection, startup or new allocation.
pub fn spawn_configured(
    state: &AppState,
    shutdown: CancellationToken,
) -> Option<JoinHandle<Result<()>>> {
    let path = state.settings.marketplace.installed_startup_file.clone()?;
    let state = state.clone();
    Some(tokio::spawn(async move {
        let mut stage = "private_profile";
        let result = activate(&state, &path, &shutdown, &mut stage).await;
        if result.is_err() && !shutdown.is_cancelled() {
            // Fixed stages only. Raw errors can contain paths, provider data
            // or attacker-controlled input; keep the original Result private.
            tracing::error!(
                stage,
                "installed source startup was not confirmed; execution remains unavailable"
            );
        }
        result
    }))
}

#[cfg(test)]
mod tests;
