//! Local storage allocation only. All code execution uses the original owned
//! source-writer create/bind/start path, never an ad-hoc Docker run helper.
use super::*;
use crate::marketplace::runtime::build;

impl DockerRuntimeManager {
    pub(super) async fn create_build_output_volume(&self, slot: &str) -> Result<()> {
        let name = build::name(slot)?;
        let labels = build::labels(slot)?;
        if self.inspect_named_volume(&name).await?.is_some() {
            bail!("build output slot already exists; original owner must retire it first");
        }
        // --label values are trusted closed constants/validated local namespace.
        let mut args = vec!["volume".into(), "create".into(), "--driver=local".into()];
        let mut ordered: Vec<_> = labels.iter().collect();
        ordered.sort_by_key(|(key, _)| *key);
        for (key, value) in ordered {
            args.extend(["--label".into(), format!("{key}={value}")]);
        }
        args.push(name.clone());
        if self.run_stdout(&args).await?.trim() != name {
            bail!("build output allocation returned another name");
        }
        let value = self
            .inspect_named_volume(&name)
            .await?
            .context("build output volume missing after allocation")?;
        Self::verify_owned_local_volume(&value, &name, &labels)
    }
    pub(super) async fn remove_build_output_volume(&self, slot: &str) -> Result<()> {
        let name = build::name(slot)?;
        let labels = build::labels(slot)?;
        if let Some(value) = self.inspect_named_volume(&name).await? {
            Self::verify_owned_local_volume(&value, &name, &labels)?;
            // Native volume rm refuses live references. Never force deletion.
            self.remove_private_file_volume(&name, &labels).await?;
        }
        if self.inspect_named_volume(&name).await?.is_some() {
            bail!("build output volume remains after removal");
        }
        Ok(())
    }
}
