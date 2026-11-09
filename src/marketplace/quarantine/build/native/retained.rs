//! Actual local disk round trip across TWO native jobs. This is not a server
//! process restart, installed generation, public-source or real paid-user test.
use super::*;
use crate::{
    marketplace::{
        archive::ArchiveBudget,
        quarantine::{Restoration, image_reference::RuntimeImage, local_build::LocalBuildRecord},
    },
    secrets::SecretsManager,
};
use elixir_marketplace::{
    admission::CandidateDescriptor,
    retained_authority::AuthorityRecord,
    source_execution::{
        RuntimeJson, SourceExecutionLimits, SourceExecutionMode, SourceLifecycleContext,
        SourceSdkReply,
    },
};

// Retains the actual revocable identity but grants NO SDK effects. Ordinary
// installed execution still needs its own enrollment/session/transaction gates.
struct NoEffects {
    source: SourceMatchDescriptor,
    native: Option<super::super::super::execution::NativeInvocation>,
    retired: bool,
    mode: SourceExecutionMode,
    execution: Option<Digest>,
}
impl NoEffects {
    fn new(
        source: &SourceMatchedQuarantine,
        mode: SourceExecutionMode,
        execution: Option<Digest>,
    ) -> Self {
        Self {
            source: source.execution_descriptor(),
            native: None,
            retired: false,
            mode,
            execution,
        }
    }
    fn require(&self, native: &super::super::super::execution::NativeInvocation) -> Result<()> {
        match self.mode {
            SourceExecutionMode::Invoke => native.require_invoke(&self.source),
            SourceExecutionMode::Probe => native.require_probe(&self.source),
            SourceExecutionMode::Lifecycle => native.require_candidate(
                &self.source,
                self.execution.context("candidate coordinate missing")?,
            ),
        }
    }
}
impl super::super::super::execution::CandidateBroker for NoEffects {}
#[async_trait::async_trait]
impl super::super::super::execution::Broker for NoEffects {
    fn native_started(
        &mut self,
        invocation: super::super::super::execution::NativeInvocation,
    ) -> Result<()> {
        if self.native.is_some() {
            bail!("retained fixture received duplicate native ownership");
        }
        self.require(&invocation)?;
        self.native = Some(invocation);
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        if let Some(native) = &self.native {
            self.require(native)?;
        }
        Ok(())
    }
    async fn request(&mut self, _: &RuntimeJson, _: usize) -> Result<SourceSdkReply> {
        bail!("retained integrity fixture grants no SDK effects")
    }
    async fn retire_effects(&mut self) -> Result<()> {
        if let Some(native) = self.native.take() {
            if native.check().is_ok() {
                bail!("retained invocation scope was not revoked");
            }
            self.retired = true;
        }
        Ok(())
    }
}

pub(super) async fn run(
    mut source: SourceMatchedQuarantine,
    image: &RuntimeImage,
    build: &NativeBuildMatch,
    value: &serde_json::Value,
    root: &Path,
) -> Result<()> {
    let runtime = source.runtime();
    let lane = source.job_lease().directory().to_owned();
    let deployment = source.source.job.deployment.clone();
    let plan = source.source.job.plan.clone();
    let worker = source.source.job.volume.image.clone();
    let old_view = image.runtime_view(&source)?;
    let secrets = SecretsManager::from_key_bytes([93; 32], false);
    let mut probe = NoEffects::new(&source, SourceExecutionMode::Probe, None);
    source
        .probe_container(image, small_limits(), &mut probe)
        .await?;
    if !probe.retired {
        bail!("fresh compiled probe scope was not retired");
    }
    let expected_summary = canonical::to_vec(source.summary())?;
    let expected_release = source.workspace().release_id;
    let archive_path = root.join("retained-archives");
    let retention = ArchiveBudget {
        bytes: 64 * MIB,
        releases: 4,
    };
    let (archive, record) =
        crate::marketplace::grant_store::invocation::native_tests::archive_compiled(
            source, build, image, value, root, &secrets,
        )
        .await
        .context("actual compiled source archive and original disposal")?;
    if archive.release_id() != expected_release || old_view.check().is_ok() {
        bail!("archive failed to retire the original source/image owner");
    }
    drop(archive); // Cold reuse below reads bytes/authority again, not this DTO.
    drop(
        ControllerLease::acquire(&lane)
            .context("original build/archive still pins its retired lane")?,
    );
    if value["fixture_variant"].as_str()==Some("container-image-installed") {
        return Box::pin(crate::marketplace::grant_store::installation::runtime::run_compiled_fixture(
            runtime,root,&lane,&worker,&deployment,value)).await;
    }
    let record = LocalBuildRecord::parse(&record.to_bytes()?)?;
    let candidate = CandidateDescriptor::parse(&serde_json::to_vec(&serde_json::json!({
        "schema_version":1,"publisher":value["publisher"],"release":value["release"],
        "head":value["head"],"revocations":[],"gate_evidence":value["evidence"]["pairs"]
    }))?)?;
    let authority = AuthorityRecord::from_descriptor(&candidate)?.verify()?;
    let cold = format!("{deployment}-cold");
    let admitted = SourceQuarantineJob::open(runtime.clone(), &lane, &cold, &worker, &plan)
        .await
        .context("opening distinct cold job after original disposal")?;
    let cold_expires = admitted.expires;
    let mut restoring = Restoration::new(admitted, archive_path, retention);
    let restored = Box::pin(async {
        let mut slots = 0;
        loop {
            slots += 1;
            if restoring.advance().await? {
                break;
            }
            if slots >= 3 {
                bail!("retained restore exceeded its three fixed byte slots");
            }
        }
        if slots != 3 {
            bail!("compiled restore omitted package, proof or artifact");
        }
        let ready = restoring.finish().await?;
        ready.match_source(runtime.as_ref()).await
    })
    .await;
    if restored.is_err() {
        restoring.cancel().await?;
    }
    let mut restored =
        restored.context("actual three-slot native archive restoration and source rematch")?;
    let operation=Box::pin(async {
        if restored.require_public_source().is_ok() || canonical::to_vec(restored.summary())?!=expected_summary {
            bail!("retained restore invented public freshness or changed actual native source bytes");
        }
        let compiler = authority.release().payload().artifacts[0].builder_image_digest;
        let approved = profile::ApprovedToolchains::from_operator_digests([compiler])?;
        let before = (restored.source.job.input_used,restored.source.job.output_used,restored.expires());
        if record.bind_retained(&mut restored,&authority,&approved,&SecretsManager::from_key_bytes([94;32],true)).is_ok() {
            bail!("wrong local machine key authenticated retained reproduction");
        }
        if before!=(restored.source.job.input_used,restored.source.job.output_used,restored.expires()) || restored.retained_build.is_some() {
            bail!("failed retained authentication mutated its actual source owner");
        }
        let local = record.bind_retained(&mut restored,&authority,&approved,&secrets)?;
        local.require_source(&restored)?;
        if restored.require_public_source().is_ok() || restored.require_foreground_manifest().is_ok() {
            bail!("authenticated retained integrity became foreground availability or approval");
        }
        let filesystem = restored.check_retained_container_filesystem(&local).await.context("retained native filesystem check")?;
        filesystem.require_source(&restored)?;
        let mut owner=Some(restored);
        let mut exporting=SourceMatchedQuarantine::begin_retained_image_export(&mut owner,&local,&filesystem).context("retained export construction")?;
        if owner.is_some() { bail!("retained export duplicated source ownership"); }
        exporting.run().await?;
        let (returned,sealed)=exporting.finish()?;
        let mut owner=Some(returned);
        let mut sealed=Some(sealed);
        let mut importing=SourceMatchedQuarantine::begin_image_import(&mut owner,&mut sealed)?;
        if owner.is_some() || sealed.is_some() { bail!("retained import duplicated its native source/export"); }
        importing.run().await?;
        let (mut returned,image)=importing.adopt().await?;
        local.require_source(&returned)?;
        image.require_source(&returned)?;
        if returned.expires()!=cold_expires || returned.require_public_source().is_ok() || build.require_source(&returned).is_ok() {
            bail!("cold reuse renewed its deadline or reused original foreground evidence");
        }
        let view=image.runtime_view(&returned)?;
        let mut probe=NoEffects::new(&returned,SourceExecutionMode::Probe,None);
        returned.probe_container(&image,small_limits(),&mut probe).await?;
        if !probe.retired { bail!("retained probe did not retire its actual scope"); }
        let execution=Digest::of(uuid::Uuid::new_v4().as_bytes());
        let context=SourceLifecycleContext::new(None,&returned.release().version)?;
        let before=(returned.source.job.input_used,returned.source.job.output_used);
        let mut candidate=NoEffects::new(&returned,SourceExecutionMode::Lifecycle,Some(execution));
        if returned.lifecycle_container(&image,small_limits(),&SourceLifecycleContext::new(None,"999.0.0")?,execution,&mut candidate).await.is_ok()
            || before!=(returned.source.job.input_used,returned.source.job.output_used) {
            bail!("wrong lifecycle target consumed native work or was accepted");
        }
        let wrong=returned.lifecycle_container(&image,small_limits(),&context,Digest::of(b"different execution"),&mut candidate).await
            .err().context("wrong candidate execution was admitted")?;
        if !format!("{wrong:#}").contains("different candidate execution") {
            return Err(wrong).context("lifecycle negative did not reach actual native identity check");
        }
        let mut candidate=NoEffects::new(&returned,SourceExecutionMode::Lifecycle,Some(execution));
        returned.lifecycle_container(&image,small_limits(),&context,execution,&mut candidate).await?;
        if !candidate.retired { bail!("lifecycle scope was not retired"); }
        let mut broker=NoEffects::new(&returned,SourceExecutionMode::Invoke,None);
        let limits=SourceExecutionLimits {profile:Default::default(),heap_bytes:32*MIB,timeout_millis:10_000,
            max_file_bytes:8*MIB,json_bytes:MIB,pending_jobs:128,sdk:None};
        let invoked=returned.invoke_container(&image,limits,&RuntimeJson::parse(b"null")?,&mut broker).await;
        let disposal=returned.discard_retained().await;
        disposal?;
        if local.require_source(&returned).is_ok() || image.require_source(&returned).is_ok() || view.check().is_ok() {
            bail!("disposed restored job retained usable integrity/image authority");
        }
        super::super::super::image_reference::require_absent(&returned.job_lease())?;
        super::super::super::image_import::require_control_absent(&returned.job_lease())?;
        let result:serde_json::Value=serde_json::from_str(invoked?.as_str())?;
        if result!=serde_json::json!({"fixture":true,"fraction":0.75}) || !broker.retired {
            bail!("retained native invocation failed its no-effects result/revocation check");
        }
        println!("retained build: actual record/SQL reopen, archive/disposal, disk restore/native rematch, native probe/lifecycle identity/no-effects invocation passed; no compiler rerun, public freshness or installed activation");
        Ok::<_,anyhow::Error>(())
    }).await;
    // Source ownership may have moved into export/import; those owners retain
    // their existing cancellation cleanup. The outer exact-role recovery also
    // covers this cold namespace on any earlier error, never foreign resources.
    operation
}

fn small_limits() -> SourceExecutionLimits {
    SourceExecutionLimits {
        profile: Default::default(),
        heap_bytes: 32 * MIB,
        timeout_millis: 10_000,
        max_file_bytes: 8 * MIB,
        json_bytes: 1024,
        pending_jobs: 128,
        sdk: None,
    }
}

#[test]
fn disk_restore_fixture_does_not_require_a_larger_native_thread_stack() {
    fn bytes<F: std::future::Future>(
        _: impl FnOnce(
            SourceMatchedQuarantine,
            &'static RuntimeImage,
            &'static NativeBuildMatch,
            &'static serde_json::Value,
            &'static Path,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    let size = bytes(|source, image, build, value, root| run(source, image, build, value, root));
    assert!(
        size <= 128 * 1024,
        "retained fixture future too large: {size}"
    );
}
