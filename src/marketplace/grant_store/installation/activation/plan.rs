use super::*;
use crate::marketplace::quarantine::{SourceJobBudget, SourceQuarantinePlan};
use elixir_marketplace::{records::RuntimeProfile, security::AuthorizedRelease};

/// Trusted source-JS launch recipe DATA. A pinned image name and requested
/// limits are not proof of image presence, physical admission or readiness.
/// Other runtime profiles need their own verified executable-closure path.
pub(in crate::marketplace) struct SourceActivationPlan {
    image: String,
    source: SourceQuarantinePlan,
    review: Digest,
    id: Digest,
}
impl SourceActivationPlan {
    pub(in crate::marketplace) fn new(
        release: &AuthorizedRelease,
        review: &GrantReview,
        image: &str,
        budget: &SourceJobBudget,
    ) -> Result<Self> {
        if release.payload().runtime_profile != RuntimeProfile::SourceJs
            || release.release_id() != review.release_id()
            || &release.payload().extension_id != review.scope().extension_id()
            || !crate::marketplace::runtime::image_is_pinned(image)
        {
            bail!("invalid source activation release/review/image");
        }
        let source = SourceQuarantinePlan::for_release(release, None, budget)?;
        source.require_execution_resources(
            review
                .scope()
                .runtime()
                .context("source runtime review missing")?,
        )?;
        let review = review.review_id()?;
        let id = Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/source-activation-plan/v1","image":image,
            "review":review,"workspace":source.workspace(),
            "memory":budget.memory_bytes,"cpu":budget.cpu_millicores,"disk":budget.disk_bytes,
            "input":budget.input_bytes,"output":budget.output_bytes,"seconds":budget.wall_seconds
        }))?);
        Ok(Self {
            image: image.into(),
            source,
            review,
            id,
        })
    }
    pub(in crate::marketplace) fn id(&self) -> Digest {
        self.id
    }
    pub(in crate::marketplace) fn image(&self) -> &str {
        &self.image
    }
    pub(in crate::marketplace) fn source(&self) -> &SourceQuarantinePlan {
        &self.source
    }
    pub(super) fn probe_limits(
        &self,
    ) -> Result<elixir_marketplace::source_execution::SourceExecutionLimits> {
        use elixir_marketplace::source_execution::SourceExecutionLimits;
        const MIB: u64 = 1024 * 1024;
        let json_bytes = 1024;
        let usable = self
            .source
            .memory_reservation_bytes()
            .checked_sub(32 * MIB + 3 * json_bytes)
            .context("source probe has insufficient admitted memory")?;
        // Heap plus its maximum file must fit the ALREADY admitted profile.
        // Keep the evaluator's fixed/native overhead and JSON copies reserved.
        let heap_bytes = ((usable / 5 * 4 / MIB) * MIB).min(2 * 1024 * MIB);
        let limits = SourceExecutionLimits {
            profile: Default::default(),
            heap_bytes,
            max_file_bytes: (heap_bytes / 4).min(elixir_marketplace::MAX_SOURCE_PACKAGE_BYTES),
            json_bytes,
            timeout_millis: (self.source.workspace().timeout_seconds.min(30) * 1000) as u32,
            pending_jobs: 4096,
            sdk: None,
        };
        limits.validate()?;
        if limits.minimum_memory_bytes()? > self.source.memory_reservation_bytes() {
            bail!("source probe exceeds its admitted memory");
        }
        Ok(limits)
    }
    pub(super) fn require_material(&self, material: &Material) -> Result<()> {
        if self.review != material.review.review_id()?
            || self.source.workspace().release_id != material.review.release_id()
        {
            bail!("activation plan differs from the verified material");
        }
        Ok(())
    }
}
