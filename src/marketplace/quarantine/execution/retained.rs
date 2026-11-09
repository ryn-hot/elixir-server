//! Retained bytes and provenance, never an installed effect grant. Pending
//! bytes can proceed only through the actual executor's pre-argument verifier;
//! they are not promoted into a SourceMatchedQuarantine from stored summaries.
use super::*;
use elixir_marketplace::{records::RuntimeProfile, retained_authority::VerifiedRetainedAuthority};

pub(in crate::marketplace) struct RetainedCode {
    source: Provenance,
}
enum Provenance {
    Matched(SourceMatchedQuarantine),
    Pending {
        source: QuarantinedSource,
        expected: SourceMatchSummary,
    },
    Compiled {
        source: Box<SourceMatchedQuarantine>,
        image: Box<super::super::image_reference::RuntimeImage>,
    },
}
impl RetainedCode {
    /// Check while BORROWING the controller's real cleanup slot. A mismatch
    /// must not drop the native workspace/lane or lose its disposal owner.
    pub(in crate::marketplace) fn take(
        slot: &mut Option<SourceMatchedQuarantine>,
        authority: &VerifiedRetainedAuthority,
        expected: &SourceMatchDescriptor,
        summary: &SourceMatchSummary,
    ) -> Result<Self> {
        let source = slot
            .as_ref()
            .context("retained native source owner is missing")?;
        source.ensure_job_active()?;
        let actual = source.execution_descriptor();
        require_match(authority, expected, summary, &actual, source.summary())?;
        // Job timeout/descriptor ID may differ from the original enrollment;
        // release, every signed byte slot, manifest and complete native source
        // summary may not. No discovery/source receipt is synthesized here.
        Ok(Self {
            source: Provenance::Matched(slot.take().expect("borrowed retained source owner")),
        })
    }
    /// The original quarantined workspace remains owned across every failure.
    /// Only expected publication DATA is checked here, not native byte equality.
    pub(in crate::marketplace) fn take_pending(
        slot: &mut Option<QuarantinedSource>,
        authority: &VerifiedRetainedAuthority,
        expected: &SourceMatchDescriptor,
        summary: &SourceMatchSummary,
    ) -> Result<Self> {
        let source = slot
            .as_ref()
            .context("pending retained source owner is missing")?;
        if Instant::now() >= source.job.expires {
            bail!("source quarantine job has expired");
        }
        require_byte_plan(authority, expected, summary, &source.job.plan.source_match)?;
        Ok(Self {
            source: Provenance::Pending {
                source: slot.take().expect("borrowed original pending workspace"),
                expected: summary.clone(),
            },
        })
    }
    pub(in crate::marketplace) fn take_compiled(
        slot: &mut Option<SourceMatchedQuarantine>,
        image: &mut Option<super::super::image_reference::RuntimeImage>,
        authority: &VerifiedRetainedAuthority,
        expected: &SourceMatchDescriptor,
        summary: &SourceMatchSummary,
    ) -> Result<Self> {
        let source = slot.as_ref().context("compiled retained source missing")?;
        let actual_image = image.as_ref().context("compiled retained image missing")?;
        if source.public_source.is_some() || source.retained_build.is_none() {
            bail!("compiled installed code requires authenticated retained provenance");
        }
        require_byte_plan_for(
            authority,
            expected,
            summary,
            &source.execution_descriptor(),
            RuntimeProfile::Reproducible,
        )?;
        source.summary().matches(&source.execution_descriptor())?;
        if canonical::to_vec(source.summary())? != canonical::to_vec(summary)? {
            bail!("compiled native source differs from its enrolled summary");
        }
        actual_image.require_source(source)?;
        Ok(Self {
            source: Provenance::Compiled {
                source: Box::new(slot.take().expect("borrowed compiled source")),
                image: Box::new(image.take().expect("borrowed compiled image")),
            },
        })
    }
    fn job(&self) -> &SourceQuarantineJob {
        match &self.source {
            Provenance::Matched(source) => &source.source.job,
            Provenance::Pending { source, .. } => &source.job,
            Provenance::Compiled { source, .. } => &source.source.job,
        }
    }
    fn job_mut(&mut self) -> &mut SourceQuarantineJob {
        match &mut self.source {
            Provenance::Matched(source) => &mut source.source.job,
            Provenance::Pending { source, .. } => &mut source.job,
            Provenance::Compiled { source, .. } => &mut source.source.job,
        }
    }
    pub(in crate::marketplace) fn descriptor(&self) -> SourceMatchDescriptor {
        self.job().plan.source_match.clone()
    }
    pub(in crate::marketplace) fn require_execution_resources(
        &self,
        resources: &super::super::super::grants::RuntimeResources,
    ) -> Result<()> {
        self.job().plan.require_execution_resources(resources)
    }
    pub(in crate::marketplace) async fn invoke(
        &mut self,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        match &mut self.source {
            Provenance::Matched(source) => {
                source
                    .evaluate_native(SourceExecutionMode::Invoke, limits, arguments, broker)
                    .await
            }
            Provenance::Pending { source, expected } => {
                super::evaluate_verified(source, expected, limits, arguments, broker).await
            }
            Provenance::Compiled { source, image } => {
                source
                    .invoke_container(image, limits, arguments, broker)
                    .await
            }
        }
    }
    pub(in crate::marketplace) async fn discard(&mut self) -> Result<()> {
        match &mut self.source {
            Provenance::Matched(source) => source.discard_retained().await,
            Provenance::Compiled { source, .. } => source.discard_retained().await,
            Provenance::Pending { source, .. } => source.discard_retained().await,
        }
    }
    pub(in crate::marketplace) async fn park_bytes(&mut self) -> Result<bool> {
        if matches!(self.source, Provenance::Compiled { .. }) {
            // Do not put imported-image ownership into the SourceJs byte cache.
            // Compiled warm reuse needs its own bounded, measured lifetime.
            return Ok(false);
        }
        super::super::cache::park(self.job_mut()).await
    }
    /// Optional migration + local readiness, NEVER ordinary invoke. A parsed
    /// acknowledgement is not a commit/cleanup permit: the candidate owner
    /// still has to join its actual native task and recheck durable authority.
    pub(in crate::marketplace) async fn lifecycle(
        &mut self,
        limits: SourceExecutionLimits,
        context: &SourceLifecycleContext,
        execution: Digest,
        broker: &mut dyn CandidateBroker,
    ) -> Result<()> {
        context.require_target(&self.descriptor().release.version)?;
        if let Provenance::Compiled { source, image } = &mut self.source {
            return source
                .lifecycle_container(image, limits, context, execution, broker)
                .await;
        }
        let Provenance::Matched(source) = &mut self.source else {
            bail!("pending invocation bytes cannot become a lifecycle candidate");
        };
        let result = source
            .evaluate_native_as(
                SourceExecutionMode::Lifecycle,
                limits,
                &context.arguments()?,
                broker,
                execution,
            )
            .await?;
        if result.as_str().as_bytes() != SOURCE_LIFECYCLE_RESULT {
            bail!("native lifecycle did not return the runner's local acknowledgement");
        }
        Ok(())
    }
}

/// Data comparisons only, not a constructor for native ownership. Keep job
/// timeout out of byte identity while comparing every signed slot explicitly.
fn require_match(
    authority: &VerifiedRetainedAuthority,
    expected: &SourceMatchDescriptor,
    saved: &SourceMatchSummary,
    actual: &SourceMatchDescriptor,
    matched: &SourceMatchSummary,
) -> Result<()> {
    require_byte_plan(authority, expected, saved, actual)?;
    matched.matches(actual)?;
    if canonical::to_vec(matched)? != canonical::to_vec(saved)? {
        bail!("native retained bytes differ from the enrolled source publication");
    }
    Ok(())
}
fn require_byte_plan(
    authority: &VerifiedRetainedAuthority,
    expected: &SourceMatchDescriptor,
    saved: &SourceMatchSummary,
    actual: &SourceMatchDescriptor,
) -> Result<()> {
    require_byte_plan_for(authority, expected, saved, actual, RuntimeProfile::SourceJs)
}
fn require_byte_plan_for(
    authority: &VerifiedRetainedAuthority,
    expected: &SourceMatchDescriptor,
    saved: &SourceMatchSummary,
    actual: &SourceMatchDescriptor,
    profile: RuntimeProfile,
) -> Result<()> {
    expected.validate()?;
    actual.validate()?;
    saved.matches(expected)?;
    let expected_slots = expected.workspace.descriptors()?;
    let actual_slots = actual.workspace.descriptors()?;
    let same_slots = expected_slots.len() == actual_slots.len()
        && expected_slots.iter().zip(&actual_slots).all(|(a, b)| {
            a.schema_version == b.schema_version
                && a.release_id == b.release_id
                && a.purpose == b.purpose
                && a.blob.sha256 == b.blob.sha256
                && a.blob.size == b.blob.size
        });
    let compiled = profile == RuntimeProfile::Reproducible;
    if authority.release().payload().runtime_profile != profile
        || actual.workspace.artifact.is_some() != compiled
        || expected.workspace.artifact.is_some() != compiled
        || canonical::to_vec(authority.release().payload())?
            != canonical::to_vec(&expected.release)?
        || canonical::to_vec(&actual.release)? != canonical::to_vec(&expected.release)?
        || actual.workspace.release_id != authority.release().release_id()
        || !same_slots
    {
        bail!("native retained bytes differ from the enrolled source publication");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_byte_comparison_allows_new_job_time_but_not_changed_source_or_summary() -> Result<()>
    {
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))?;
        let descriptor = elixir_marketplace::admission::CandidateDescriptor::parse(
            &serde_json::to_vec(&serde_json::json!({
                "schema_version":1,"publisher":value["publisher"],"release":value["release"],"head":value["head"],"revocations":[],"gate_evidence":value["evidence"]["pairs"]
            }))?,
        )?;
        let authority =
            elixir_marketplace::retained_authority::AuthorityRecord::from_descriptor(&descriptor)?
                .verify()?;
        let (release, package, proof) = crate::marketplace::quarantine::tests::fixture()?;
        let plan = SourceQuarantinePlan::for_release(
            &release,
            None,
            &crate::marketplace::quarantine::tests::budget(),
        )?;
        let expected = plan.source_match;
        let summary =
            expected.verify(std::io::Cursor::new(package), std::io::Cursor::new(proof))?;
        let mut actual = expected.clone();
        actual.workspace.timeout_seconds += 1;
        require_match(&authority, &expected, &summary, &actual, &summary)?;
        let mut wrong_summary = summary.clone();
        wrong_summary
            .entrypoint
            .as_mut()
            .context("fixture entrypoint missing")?
            .sha256 = Digest::of(b"different entrypoint");
        assert!(require_match(&authority, &expected, &summary, &actual, &wrong_summary).is_err());
        actual.workspace.package.size += 1;
        assert!(require_match(&authority, &expected, &summary, &actual, &summary).is_err());
        Ok(())
    }
}
