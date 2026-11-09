//! Fresh, authorized install/update work only. Search never constructs this
//! owner. Publisher data chooses neither the compiler nor the native platform.
use super::*;
use crate::marketplace::{
    grant_store::installation::runtime::InstalledRuntime,
    quarantine::{
        build::{Build, NativeBuildMatch},
        image_export::Export,
        image_import::Import,
        image_reference::RuntimeImage,
        local_build::LocalBuildRecord,
    },
    runtime::build::ApprovedToolchains,
    worker::PublicSourceObservation,
};
use crate::secrets::SecretsManager;
use elixir_marketplace::{
    records::{Release, RuntimeProfile},
    reproduction::BuildPlatform,
};

pub(super) struct Configuration {
    pub(super) artifact: usize,
    pub(super) budget: SourceJobBudget,
    approved: Arc<ApprovedToolchains>,
    secrets: SecretsManager,
}
impl Configuration {
    pub(super) fn select(
        release: &Release,
        runtime: &InstalledRuntime,
        secrets: SecretsManager,
    ) -> Result<Option<Self>> {
        if release.runtime_profile != RuntimeProfile::Reproducible {
            return Ok(None);
        }
        let approved = runtime
            .compiled_toolchains
            .as_ref()
            .context("compiled installation is not configured")?
            .clone();
        let guardian = runtime
            .runtime
            .marketplace_container_guardian()
            .context("compiled installation requires its trusted guardian")?;
        guardian.require()?;
        runtime
            .runtime
            .marketplace_import_profile()
            .context("compiled installation requires its trusted importer")?;
        let artifact = select_artifact(release, guardian.platform(), &approved)?;
        Ok(Some(Self {
            artifact,
            budget: runtime.source_budget.clone(),
            approved,
            secrets,
        }))
    }
}
fn select_artifact(
    release: &Release,
    platform: BuildPlatform,
    approved: &ApprovedToolchains,
) -> Result<usize> {
    if release.runtime_profile != RuntimeProfile::Reproducible {
        bail!("native artifact selection requires the compiled profile");
    }
    let mut candidates = release
        .artifacts
        .iter()
        .enumerate()
        .filter(|(_, artifact)| artifact.architecture == platform.name());
    let (index, artifact) = candidates
        .next()
        .context("no signed artifact for the trusted native platform")?;
    if candidates.next().is_some() {
        bail!("ambiguous signed artifacts for the trusted native platform");
    }
    approved.require(artifact.builder_image_digest)?;
    Ok(index)
}

// Every native owner is installed before the next await. Cancellation of an
// advance does not drop a source, restart a build or release its original lane.
pub(super) struct Fresh {
    stage: Stage,
    build: Option<NativeBuildMatch>,
}
enum Stage {
    Empty,
    Building(Box<Build>),
    Matched(Box<Option<SourceMatchedQuarantine>>),
    Exporting(Box<Export>),
    Importing(Box<Import>),
    Ready {
        source: Box<Option<SourceMatchedQuarantine>>,
        image: Box<RuntimeImage>,
    },
}
impl Fresh {
    pub(super) fn admission_retryable(&self) -> bool {
        matches!(self.stage, Stage::Matched(_))
    }
    pub(super) fn begin(
        source: &mut Option<SourceMatchedQuarantine>,
        configuration: &Configuration,
    ) -> Result<Self> {
        let build = SourceMatchedQuarantine::begin_fresh_build(source, &configuration.approved)?;
        Ok(Self {
            stage: Stage::Building(Box::new(build)),
            build: None,
        })
    }
    pub(super) async fn advance(&mut self, observation: PublicSourceObservation) -> Result<bool> {
        match &mut self.stage {
            Stage::Building(build) => {
                build.refresh_public_source(observation)?;
                Box::pin(build.run()).await?;
                let (source, evidence) = build.finish()?;
                self.stage = Stage::Matched(Box::new(Some(source)));
                self.build = Some(evidence);
                Ok(false)
            }
            Stage::Matched(source) => {
                let original = source
                    .as_mut()
                    .as_mut()
                    .context("fresh compiled source missing")?;
                original.refresh_public_source(observation)?;
                let build = self
                    .build
                    .as_ref()
                    .context("actual fresh reproduction missing")?;
                let filesystem = Box::pin(original.check_container_filesystem(build)).await?;
                let export =
                    SourceMatchedQuarantine::begin_image_export(source, build, &filesystem)?;
                self.stage = Stage::Exporting(Box::new(export));
                Ok(false)
            }
            Stage::Exporting(export) => {
                export.refresh_public_source(observation)?;
                Box::pin(export.run()).await?;
                let (source, sealed) = export.finish()?;
                self.stage = Stage::Matched(Box::new(Some(source)));
                let mut sealed = Some(sealed);
                let Stage::Matched(source) = &mut self.stage else {
                    unreachable!()
                };
                let import = SourceMatchedQuarantine::begin_image_import(source, &mut sealed)?;
                self.stage = Stage::Importing(Box::new(import));
                Ok(false)
            }
            Stage::Importing(import) => {
                import.refresh_public_source(observation)?;
                Box::pin(import.run()).await?;
                let (source, image) = Box::pin(import.adopt()).await?;
                self.stage = Stage::Ready {
                    source: Box::new(Some(source)),
                    image: Box::new(image),
                };
                Ok(true)
            }
            Stage::Ready { source, .. } => {
                source
                    .as_mut()
                    .as_mut()
                    .context("fresh compiled source missing")?
                    .refresh_public_source(observation)?;
                Ok(true)
            }
            Stage::Empty => bail!("fresh compiled owner already handed off or disposed"),
        }
    }
    pub(super) fn take_ready(
        &mut self,
        configuration: &Configuration,
    ) -> Result<(SourceMatchedQuarantine, LocalBuildRecord)> {
        let Stage::Ready { source, image } = &mut self.stage else {
            bail!("fresh compiled preparation is not ready");
        };
        let original = source
            .as_ref()
            .as_ref()
            .context("fresh compiled source missing")?;
        let record = LocalBuildRecord::from_actual_build(
            original,
            self.build
                .as_ref()
                .context("actual fresh reproduction missing")?,
            image,
            &configuration.secrets,
        )?;
        let source = source.take().expect("borrowed fresh source");
        // The source job owns the passive reference journal. Dropping the
        // borrowed image view does not retire it or authorize later execution.
        self.stage = Stage::Empty;
        self.build = None;
        Ok((source, record))
    }
    pub(super) async fn discard(&mut self) -> Result<()> {
        discard(&mut self.stage).await?;
        self.build = None;
        Ok(())
    }
}
async fn discard(stage: &mut Stage) -> Result<()> {
    match stage {
        Stage::Empty => {}
        Stage::Building(build) => {
            build.cancel().await?;
            // Return the joined source to a retained slot before disposing it.
            let source = build.take_cancelled_source()?;
            *stage = Stage::Matched(Box::new(Some(source)));
            let Stage::Matched(source) = stage else {
                unreachable!()
            };
            source
                .as_mut()
                .as_mut()
                .context("cancelled build source missing")?
                .discard_retained()
                .await?;
        }
        Stage::Matched(source) | Stage::Ready { source, .. } => {
            if let Some(source) = source.as_mut().as_mut() {
                source.discard_retained().await?;
            }
        }
        Stage::Exporting(export) => export.discard_source().await?,
        Stage::Importing(import) => import.discard_source().await?,
    }
    *stage = Stage::Empty;
    Ok(())
}
impl Drop for Fresh {
    fn drop(&mut self) {
        let mut stage = std::mem::replace(&mut self.stage, Stage::Empty);
        if matches!(stage, Stage::Empty) {
            return;
        }
        if let Ok(executor) = tokio::runtime::Handle::try_current() {
            executor.spawn(async move {
                let mut reported = false;
                loop {
                    match discard(&mut stage).await {
                        Ok(()) => return,
                        Err(error) if !reported => {
                            tracing::warn!(error=%error, "fresh compiled cleanup pending; retaining original owner");
                            reported = true;
                        }
                        Err(_) => {}
                    }
                    sleep(RETRY).await;
                }
            });
        }
        // Without an executor, keep durable native fences for recovery; never
        // manufacture disposal or grant execution from serialized history.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::records::CompiledArtifact;
    #[test]
    fn native_selection_requires_one_host_artifact_and_independent_compiler_trust() -> Result<()> {
        let (authorized, _, _) = crate::marketplace::quarantine::tests::fixture()?;
        let mut release = authorized.payload().clone();
        release.runtime_profile = RuntimeProfile::Reproducible;
        let compiler = Digest::of(b"independently approved test compiler");
        let artifact = CompiledArtifact {
            architecture: "linux/amd64".into(),
            blob: release.package.clone(),
            builder_image_digest: compiler,
            command: vec!["/compiler".into()],
            environment: Default::default(),
            inputs: vec![],
        };
        release.artifacts = vec![artifact.clone()];
        let approved = ApprovedToolchains::from_operator_digests([compiler])?;
        assert!(
            select_artifact(
                &release,
                BuildPlatform::LinuxAmd64,
                &ApprovedToolchains::default()
            )
            .is_err()
        );
        assert!(select_artifact(&release, BuildPlatform::LinuxArm64, &approved).is_err());
        assert_eq!(
            select_artifact(&release, BuildPlatform::LinuxAmd64, &approved)?,
            0
        );
        release.artifacts.push(artifact);
        assert!(select_artifact(&release, BuildPlatform::LinuxAmd64, &approved).is_err());
        release.runtime_profile = RuntimeProfile::SourceJs;
        assert!(select_artifact(&release, BuildPlatform::LinuxAmd64, &approved).is_err());
        Ok(())
    }
    #[test]
    fn fresh_native_phases_keep_bounded_async_frames() {
        fn size<F: std::future::Future>(
            _: impl FnOnce(&'static mut Fresh, PublicSourceObservation) -> F,
        ) -> usize {
            std::mem::size_of::<F>()
        }
        assert!(std::mem::size_of::<Fresh>() < 16 * 1024);
        let bytes = size(|fresh, observation| fresh.advance(observation));
        assert!(
            bytes <= 128 * 1024,
            "fresh compiled future too large: {bytes}"
        );
    }
}
