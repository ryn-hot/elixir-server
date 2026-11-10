//! Trusted local installed-runtime wiring only. No downloader, image pull,
//! remote approval, HTTP field, environment-selected digest or default trust.
use crate::runtime::RuntimeManager;
use anyhow::{Result, bail};
use elixir_marketplace::{identity::Digest, reproduction::BuildPlatform};
use serde::Deserialize;
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use crate::marketplace::runtime::build::ApprovedToolchains;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Profile {
    guardian: Guardian,
    importer: Importer,
    toolchains: Vec<Digest>,
    // Separate explicit operator choice; absent never enables retained images.
    #[serde(default)]
    retain_image_cache: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Guardian {
    executable: PathBuf,
    sha256: Digest,
    platform: BuildPlatform,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Importer {
    executable: PathBuf,
    sha256: Digest,
    local_socket: PathBuf,
}
pub(super) struct Selection {
    pub(super) runtime: Arc<dyn RuntimeManager>,
    pub(super) toolchains: Arc<ApprovedToolchains>,
}
impl Profile {
    // Shape only. A parsed path/digest does not authenticate a local executable
    // or socket and never creates a usable native owner.
    pub(super) fn validate(&self) -> Result<()> {
        if !(1..=8).contains(&self.toolchains.len())
            || self
                .toolchains
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != self.toolchains.len()
        {
            bail!(
                "compiled startup requires one to eight distinct independently approved toolchains"
            );
        }
        for path in [
            &self.guardian.executable,
            &self.importer.executable,
            &self.importer.local_socket,
        ] {
            let Some(text) = path.to_str() else {
                bail!("compiled startup needs bounded local UTF-8 paths");
            };
            if !path.is_absolute()
                || text.len() > 4096
                || text.chars().any(|c| c == ':' || c.is_control())
            {
                bail!("compiled startup needs bounded absolute local paths");
            }
        }
        #[cfg(not(unix))]
        bail!("this host does not support the independently approved local compiled runtime");
        #[cfg(unix)]
        Ok(())
    }
}

pub(super) fn select(profile: Option<&Profile>) -> Result<Option<Selection>> {
    profile.map(configure).transpose()
}

#[cfg(unix)]
fn configure(profile: &Profile) -> Result<Selection> {
    profile.validate()?;
    // Use the existing independent file/ELF/hash and exact local socket
    // checks. Do not canonicalize aliases on an operator's behalf or infer
    // approval from a publisher record, image label or a working executable.
    let guardian = crate::runtime::container_guardian::Profile::from_operator_configuration(
        profile.guardian.executable.clone(),
        profile.guardian.sha256,
        profile.guardian.platform,
    )?;
    let importer = crate::runtime::image_import::Profile::from_operator_configuration(
        profile.importer.executable.clone(),
        profile.importer.sha256,
        profile.importer.local_socket.clone(),
    )?;
    let importer = if profile.retain_image_cache {
        importer.with_image_cache()
    } else {
        importer
    };
    let approved = ApprovedToolchains::from_operator_digests(profile.toolchains.iter().copied())?;
    // This optional profile explicitly selects the local Docker adapter, not
    // a caller-supplied daemon URL or a fallback from a different runtime.
    // Its existing observer still compares the actual CLI/Unix-daemon identity
    // before mutations. Construction launches no helper or container.
    let runtime = crate::runtime::docker::DockerRuntimeManager::new(None)
        .with_marketplace_container_guardian(guardian)
        .with_marketplace_import_profile(importer);
    Ok(Selection {
        runtime: Arc::new(runtime),
        toolchains: Arc::new(approved),
    })
}

#[cfg(not(unix))]
fn configure(_profile: &Profile) -> Result<Selection> {
    bail!("this host does not support the independently approved local compiled runtime")
}
