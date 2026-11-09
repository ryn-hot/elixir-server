//! Machine-local authenticated reproduction history, never an install/grant or
//! public-source observation. Reuse requires an actual newly rematched source,
//! signed retained publication, current trusted compiler and fresh native image
//! checks. No receipt can recreate the original job or extend its deadline.
use super::*;
use crate::{marketplace::runtime::build::ApprovedToolchains, secrets::SecretsManager};
use elixir_marketplace::{
    container_image::job::ContainerImageSummary,
    records::{Blob, RuntimeProfile},
    reproduction::BuildPlatform,
    retained_authority::VerifiedRetainedAuthority,
};
use serde::{Deserialize, Serialize};

const DOMAIN: &str = "elixir-marketplace/local-reproduction/v1";
const MARKER: &str = "source-reproduced-by-this-server/v1";
const MAX_BYTES: usize = 4096;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    schema_version: u32,
    release: Digest,
    source: Digest,
    artifact: Blob,
    platform: BuildPlatform,
    config: Digest,
    compiler: Digest,
}
impl Binding {
    fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        source.ensure_job_active()?;
        self.require_descriptor(&source.execution_descriptor(), source.summary())
    }
    fn require_descriptor(
        &self,
        source: &SourceMatchDescriptor,
        summary: &SourceMatchSummary,
    ) -> Result<()> {
        let d = super::super::runtime::build::execution(source, 1)?;
        summary.matches(&d.inputs.source)?;
        if self.schema_version != 1
            || source.release.runtime_profile != RuntimeProfile::Reproducible
            || self.release != source.workspace.release_id
            || self.source != Digest::of(canonical::to_vec(summary)?)
            || self.artifact.sha256 != d.output.artifact.sha256
            || self.artifact.size != d.output.artifact.size
            || self.platform != d.inputs.platform
            || self.compiler != d.toolchain_image_digest
        {
            bail!("local build record differs from actual rematched source");
        }
        Ok(())
    }
    fn require_image(&self, image: &ContainerImageSummary) -> Result<()> {
        if self.release != image.release
            || self.artifact.sha256 != image.artifact.sha256
            || self.artifact.size != image.artifact.size
            || self.platform != image.platform
            || self.config != image.config
        {
            bail!("local build record differs from checked image");
        }
        Ok(())
    }
    fn associated(&self) -> Result<Vec<u8>> {
        Ok(canonical::to_vec(
            &serde_json::json!({"domain":DOMAIN,"binding":self}),
        )?)
    }
}

/// Serializable LOCAL data only. No Debug/plaintext/fallback or constructor
/// from peer metadata. Parsing this value grants no native/SDK capability.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::marketplace) struct LocalBuildRecord {
    binding: Binding,
    seal: String,
}
impl LocalBuildRecord {
    pub(in crate::marketplace) fn from_actual_build(
        source: &SourceMatchedQuarantine,
        build: &build::NativeBuildMatch,
        image: &image_reference::RuntimeImage,
        secrets: &SecretsManager,
    ) -> Result<Self> {
        // These are private actual native capabilities, never publisher DTOs.
        build.require_source(source)?;
        image.require_source(source)?;
        source.require_public_source()?;
        let execution = super::super::runtime::build::execution(&source.execution_descriptor(), 1)?;
        let binding = Binding {
            schema_version: 1,
            release: source.workspace().release_id,
            source: Digest::of(canonical::to_vec(source.summary())?),
            artifact: execution.output.artifact,
            platform: execution.inputs.platform,
            config: image.image().config,
            compiler: execution.toolchain_image_digest,
        };
        binding.require_source(source)?;
        binding.require_image(image.image())?;
        let seal = secrets.encrypt_bound(MARKER, &binding.associated()?)?;
        let value = Self { binding, seal };
        value.to_bytes()?;
        Ok(value)
    }
    pub(in crate::marketplace) fn to_bytes(&self) -> Result<Vec<u8>> {
        let bytes = canonical::to_vec(self)?;
        if bytes.len() > MAX_BYTES || !SecretsManager::is_bound(&self.seal) {
            bail!("invalid bounded local build record");
        }
        Ok(bytes)
    }
    pub(in crate::marketplace) fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = canonical::decode(bytes, MAX_BYTES)?;
        if value.to_bytes()? != bytes || value.binding.schema_version != 1 {
            bail!("invalid canonical local build record");
        }
        Ok(value)
    }
    /// Closed DATA binding for installation journals. This does not authenticate
    /// the seal, prove native bytes or authorize execution; bind_retained must
    /// still check the machine key and an actual newly rematched native owner.
    pub(in crate::marketplace) fn require_descriptor_data(
        &self,
        source: &SourceMatchDescriptor,
        summary: &SourceMatchSummary,
    ) -> Result<()> {
        self.to_bytes()?;
        self.binding.require_descriptor(source, summary)
    }
    /// Fail-fast admission check only. Authentication of stored data cannot
    /// mint native byte/image authority; bind_retained still requires an actual
    /// newly rematched owner before any compiled export/import/execution.
    pub(in crate::marketplace) fn authenticate_data(
        &self,
        source: &SourceMatchDescriptor,
        summary: &SourceMatchSummary,
        approved: &ApprovedToolchains,
        secrets: &SecretsManager,
    ) -> Result<()> {
        self.require_descriptor_data(source, summary)?;
        approved.require(self.binding.compiler)?;
        if secrets.decrypt_bound(&self.seal, &self.binding.associated()?)? != MARKER {
            bail!("local build record authentication failed");
        }
        Ok(())
    }
    /// Check every prerequisite before changing the caller's original owner.
    /// This cannot turn an expired foreground fact into offline permission.
    pub(in crate::marketplace) fn bind_retained(
        &self,
        source: &mut SourceMatchedQuarantine,
        authority: &VerifiedRetainedAuthority,
        approved: &ApprovedToolchains,
        secrets: &SecretsManager,
    ) -> Result<VerifiedLocalBuild> {
        source.ensure_job_active()?;
        if source.public_source.is_some() || source.retained_build.is_some() {
            bail!("retained build requires a distinct newly rematched offline owner");
        }
        if canonical::to_vec(authority.release().payload())? != canonical::to_vec(source.release())?
        {
            bail!("retained build publication differs from signed installed authority");
        }
        self.authenticate_data(
            &source.execution_descriptor(),
            source.summary(),
            approved,
            secrets,
        )?;
        let owner = evidence_owner::EvidenceOwner::capture(source)?;
        source.retained_build = Some(Box::new(RetainedBuildBinding(self.binding.clone())));
        Ok(VerifiedLocalBuild {
            owner,
            binding: self.binding.clone(),
        })
    }
}

/// Only authenticated LOCAL record verification can create this field. It is
/// not serializable/cloneable and is not a public-source or permission fallback.
pub(super) struct RetainedBuildBinding(Binding);
impl RetainedBuildBinding {
    pub(super) fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        self.0.require_source(source)
    }
    pub(super) fn require_image(&self, image: &ContainerImageSummary) -> Result<()> {
        self.0.require_image(image)
    }
}
pub(in crate::marketplace) struct VerifiedLocalBuild {
    owner: evidence_owner::EvidenceOwner,
    binding: Binding,
}
impl VerifiedLocalBuild {
    pub(super) fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        self.owner.require(source)?;
        source.require_compiled_source()?;
        let current = source
            .retained_build
            .as_ref()
            .context("retained build binding missing")?;
        if canonical::to_vec(&current.0)? != canonical::to_vec(&self.binding)? {
            bail!("retained build belongs to another native source owner");
        }
        Ok(())
    }
    pub(super) fn require_image(&self, image: &ContainerImageSummary) -> Result<()> {
        self.binding.require_image(image)
    }
}

#[cfg(test)]
mod tests;
