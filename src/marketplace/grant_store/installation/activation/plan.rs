use super::super::runtime::InstalledRuntime;
use super::*;
use crate::marketplace::quarantine::{
    SourceJobBudget, SourceMatchedQuarantine, SourceQuarantinePlan, execution::CompiledPreparation,
    local_build::LocalBuildRecord,
};
use crate::marketplace::runtime::build::ApprovedToolchains;
use elixir_marketplace::{records::RuntimeProfile, security::AuthorizedRelease};

/// Trusted launch recipe/policy DATA. A pinned image name and requested
/// limits are not proof of image presence, physical admission or readiness.
/// Compiled profiles additionally require authenticated reproduction history.
pub(in crate::marketplace) struct SourceActivationPlan {
    image: String,
    source: SourceQuarantinePlan,
    review: Digest,
    id: Digest,
    compiled: Option<Box<CompiledPolicy>>,
    // Trusted local transport selection, never serialized or part of source/
    // grant authority. SourceJs keeps its existing plan identity.
    runtime: Option<Arc<dyn crate::runtime::RuntimeManager>>,
}
struct CompiledPolicy {
    authority: AuthorityRecord,
    record: LocalBuildRecord,
    toolchains: Arc<ApprovedToolchains>,
    secrets: SecretsManager,
    runtime: Arc<dyn crate::runtime::RuntimeManager>,
    summary: elixir_marketplace::source_job::SourceMatchSummary,
}
impl SourceActivationPlan {
    pub(in crate::marketplace) fn new(
        release: &AuthorizedRelease,
        review: &GrantReview,
        image: &str,
        budget: &SourceJobBudget,
    ) -> Result<Self> {
        if release.payload().runtime_profile != RuntimeProfile::SourceJs {
            bail!("source-only activation requires the SourceJs profile");
        }
        Self::build(release, review, image, budget, None)
    }
    fn build(
        release: &AuthorizedRelease,
        review: &GrantReview,
        image: &str,
        budget: &SourceJobBudget,
        artifact: Option<usize>,
    ) -> Result<Self> {
        if release.release_id() != review.release_id()
            || &release.payload().extension_id != review.scope().extension_id()
            || !crate::marketplace::runtime::image_is_pinned(image)
        {
            bail!("invalid source activation release/review/image");
        }
        let source = SourceQuarantinePlan::for_release(release, artifact, budget)?;
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
            compiled: None,
            runtime: None,
        })
    }
    pub(in crate::marketplace::grant_store::installation) fn for_material(
        material: &Material,
        configuration: &InstalledRuntime,
        secrets: &SecretsManager,
    ) -> Result<Self> {
        let facts = material.authority.verify()?;
        if facts.release().payload().runtime_profile == RuntimeProfile::SourceJs {
            let mut plan = Self::new(
                facts.release(),
                &material.review,
                &configuration.image,
                &configuration.source_budget,
            )?;
            plan.runtime = Some(configuration.runtime.clone());
            return Ok(plan);
        }
        if facts.release().payload().runtime_profile != RuntimeProfile::Reproducible {
            bail!("unsupported activation runtime profile");
        }
        let toolchains = configuration
            .compiled_toolchains
            .as_ref()
            .context("compiled activation is not configured")?;
        configuration
            .runtime
            .marketplace_container_guardian()
            .context("compiled activation guardian is not configured")?;
        configuration
            .runtime
            .marketplace_import_profile()
            .context("compiled activation import is not configured")?;
        let record = material
            .build
            .as_ref()
            .context("compiled activation history missing")?;
        let descriptor = material.source.descriptor(facts.release());
        record.authenticate_data(&descriptor, material.source.summary(), toolchains, secrets)?;
        let blob = descriptor
            .workspace
            .artifact
            .as_ref()
            .context("compiled activation artifact missing")?;
        let artifact = facts
            .release()
            .payload()
            .artifacts
            .iter()
            .position(|a| a.blob.sha256 == blob.sha256 && a.blob.size == blob.size)
            .context("compiled activation artifact is not signed")?;
        let mut plan = Self::build(
            facts.release(),
            &material.review,
            &configuration.image,
            &configuration.source_budget,
            Some(artifact),
        )?;
        plan.compiled = Some(Box::new(CompiledPolicy {
            authority: material.authority.clone(),
            record: record.clone(),
            toolchains: toolchains.clone(),
            secrets: secrets.clone(),
            runtime: configuration.runtime.clone(),
            summary: material.source.summary().clone(),
        }));
        plan.runtime = Some(configuration.runtime.clone());
        plan.require_material(material)?;
        Ok(plan)
    }
    pub(in crate::marketplace::grant_store::installation) fn for_capsule(
        capsule: &Capsule,
        configuration: &InstalledRuntime,
        secrets: &SecretsManager,
    ) -> Result<Self> {
        let facts = capsule.facts()?;
        let (manifest, review) = capsule.source.review(facts.release())?;
        Self::for_material(
            &Material {
                authority: capsule.authority.clone(),
                source: capsule.source.clone(),
                review,
                manifest,
                build: capsule.build.clone(),
            },
            configuration,
            secrets,
        )
    }
    pub(super) fn is_compiled(&self) -> bool {
        self.compiled.is_some()
    }
    pub(super) fn begin_compiled(
        &self,
        source: &mut Option<SourceMatchedQuarantine>,
    ) -> Result<CompiledPreparation> {
        let policy = self
            .compiled
            .as_ref()
            .context("compiled activation policy missing")?;
        if !Arc::ptr_eq(
            &source
                .as_ref()
                .context("compiled source missing")?
                .runtime(),
            &policy.runtime,
        ) {
            bail!("compiled activation source differs from its configured native owner");
        }
        CompiledPreparation::begin(
            source,
            &policy.record,
            policy.authority.verify()?,
            self.source.source_descriptor().clone(),
            policy.summary.clone(),
            &policy.toolchains,
            &policy.secrets,
        )
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
    pub(in crate::marketplace) fn native_runtime(
        &self,
    ) -> Option<Arc<dyn crate::runtime::RuntimeManager>> {
        self.runtime.clone()
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
        if let Some(policy) = &self.compiled {
            let facts = material.authority.verify()?;
            policy.record.authenticate_data(
                &material.source.descriptor(facts.release()),
                material.source.summary(),
                &policy.toolchains,
                &policy.secrets,
            )?;
            // The archive's storage horizon is not a new execution deadline.
            // Reuse the exact signed-slot comparison used by installed calls.
            crate::marketplace::quarantine::execution::require_retained_byte_plan(
                &facts,
                &material.source.descriptor(facts.release()),
                material.source.summary(),
                self.source.source_descriptor(),
                RuntimeProfile::Reproducible,
            )?;
        }
        Ok(())
    }
}
