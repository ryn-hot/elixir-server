//! A view of one independently inspected executor, not a serialized receipt.
//! The scope bit revokes SDK access on return/cancellation. It is NEVER evidence
//! that native work stopped; the original job still owns confirmed disposal.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

mod http;
pub(in crate::marketplace) use http::HttpLaunch;
#[cfg(unix)]
mod gateway;
#[cfg(unix)]
pub(in crate::marketplace) use gateway::GatewayLaunch;

pub(super) struct NativeScope(Arc<AtomicBool>);

/// No constructor outside this module, Clone or Deserialize. Exact source/job,
/// immutable native identity, deadline and original locked inode stay bound.
pub(in crate::marketplace) struct NativeInvocation {
    source: Digest,
    _execution: Digest,
    invocation: Digest,
    mode: SourceExecutionMode,
    handle: ContainerHandle,
    runtime: Arc<dyn RuntimeManager>,
    lease: Arc<ControllerLease>,
    expires: Instant,
    active: Arc<AtomicBool>,
    #[cfg(unix)]
    image: Option<super::super::image_reference::RuntimeImageView>,
}
impl NativeScope {
    /// The caller has already inspected the actual source executor's kernel
    /// and immutable configuration. This is private to that lifecycle.
    pub(super) fn open(
        runtime: Arc<dyn RuntimeManager>,
        handle: ContainerHandle,
        descriptor: &SourceExecutionDescriptor,
        lease: Arc<ControllerLease>,
        expires: Instant,
    ) -> Result<(Self, NativeInvocation)> {
        descriptor.validate()?;
        Self::open_bound(
            runtime,
            handle,
            (
                Digest::of(canonical::to_vec(&descriptor.source)?),
                descriptor.job_id()?,
                descriptor.invocation_id,
                descriptor.mode,
            ),
            lease,
            expires,
            #[cfg(unix)]
            None,
        )
    }
    #[cfg(unix)]
    pub(super) fn open_container(
        runtime: Arc<dyn RuntimeManager>,
        handle: ContainerHandle,
        descriptor: &elixir_marketplace::container_execution::ContainerExecutionDescriptor,
        lease: Arc<ControllerLease>,
        expires: Instant,
        image: super::super::image_reference::RuntimeImageView,
    ) -> Result<(Self, NativeInvocation)> {
        image.require_descriptor(descriptor)?;
        Self::open_bound(
            runtime,
            handle,
            (
                Digest::of(canonical::to_vec(&descriptor.source)?),
                descriptor.job_id()?,
                descriptor.invocation_id,
                descriptor.mode,
            ),
            lease,
            expires,
            Some(image),
        )
    }
    fn open_bound(
        runtime: Arc<dyn RuntimeManager>,
        handle: ContainerHandle,
        coordinates: (Digest, Digest, Digest, SourceExecutionMode),
        lease: Arc<ControllerLease>,
        expires: Instant,
        #[cfg(unix)] image: Option<super::super::image_reference::RuntimeImageView>,
    ) -> Result<(Self, NativeInvocation)> {
        let active = Arc::new(AtomicBool::new(true));
        let invocation = NativeInvocation {
            source: coordinates.0,
            _execution: coordinates.1,
            invocation: coordinates.2,
            mode: coordinates.3,
            handle,
            runtime,
            lease,
            expires,
            active: active.clone(),
            #[cfg(unix)]
            image,
        };
        invocation.check()?;
        Ok((Self(active), invocation))
    }
}
impl Drop for NativeScope {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
impl NativeInvocation {
    /// Reference identity, not matching serialized IDs or a new capability.
    /// Used only to compose checks which already retain this SAME Arc owner.
    pub(in crate::marketplace) fn require_same(&self, other: &Self) -> Result<()> {
        if !std::ptr::eq(self, other) {
            bail!("HTTP authority belongs to another original native invocation");
        }
        self.check()
    }
    /// Bootstrap only. The installed caller must separately retain actual
    /// purpose consent and aggregate admission. No HTTP input, Ready flag or
    /// borrowed catalog/relay allowance is granted by this owner.
    #[cfg(unix)]
    pub(in crate::marketplace) async fn launch_application_gateway(
        self: &Arc<Self>,
        deployment: &str,
        network_id: &str,
        database: crate::db::Database,
        secrets: Arc<crate::secrets::SecretsManager>,
        authority: Arc<dyn crate::marketplace::worker::SdkHttpAuthority>,
    ) -> Result<Option<GatewayLaunch>> {
        use crate::marketplace::{
            application_egress::Protection, lease::activity::gateway::GatewayFence,
        };
        if self.mode != SourceExecutionMode::Invoke {
            bail!("gateway requires its original invoke executor");
        }
        self.current().await?;
        let Some(protection) = Protection::capture(database, secrets, self.runtime.clone()).await?
        else {
            return Ok(None);
        };
        let (profile, input) = protection
            .prepare(deployment, network_id, self.source, self.invocation)
            .await?;
        profile.require_invocation(self.source, self.invocation)?;
        self.current().await?;
        let executor = tokio::runtime::Handle::try_current()
            .context("gateway original owner executor unavailable")?;
        let fence = GatewayFence::begin(&self.lease, &self.handle.id, self.invocation, &profile)?;
        // No await after the marker and before retaining the original task.
        Ok(Some(GatewayLaunch::begin(
            executor,
            self.clone(),
            profile,
            input,
            protection,
            authority,
            fence,
        )))
    }
    /// Tracking only; the application owner must still admit resources, prove
    /// its gateway and authorize the installed effect before any helper input.
    pub(super) async fn begin_application_http(
        &self,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> Result<crate::marketplace::lease::activity::http::HttpFence> {
        if self.mode != SourceExecutionMode::Invoke {
            bail!("HTTP helper requires its original invoke executor");
        }
        profile.require_invocation(self.source, self.invocation)?;
        self.current().await?;
        let fence = crate::marketplace::lease::activity::http::HttpFence::begin(
            &self.lease,
            &self.handle.id,
            self.invocation,
            profile,
        )?;
        // Return the tracking owner even if the deadline elapsed during fsync.
        // It grants no effect; the next physical boundary must freshly check
        // authority. Losing this Prepared owner would only strand cleanup.
        Ok(fence)
    }
    /// Internal factory: application callers use the retained gateway's
    /// launch_http so identity/config authority cannot be replaced by strings.
    /// This launches no HTTP request and supplies no network/resource admission.
    pub(super) async fn launch_application_http(
        self: &Arc<Self>,
        deployment: &str,
        image: &str,
        gateway_id: &str,
        authority: Arc<dyn crate::marketplace::worker::SdkHttpAuthority>,
    ) -> Result<HttpLaunch> {
        self.launch_application_http_mode(
            deployment,
            image,
            gateway_id,
            authority,
            crate::marketplace::runtime::application_http::HttpMode::StreamLarge,
        )
        .await
    }
    pub(super) async fn launch_application_http_mode(
        self: &Arc<Self>,
        deployment: &str,
        image: &str,
        gateway_id: &str,
        authority: Arc<dyn crate::marketplace::worker::SdkHttpAuthority>,
        mode: crate::marketplace::runtime::application_http::HttpMode,
    ) -> Result<HttpLaunch> {
        // Only this original invocation supplies its source and random nonce.
        // Callers do not supply or override those scope coordinates.
        let profile =
            crate::marketplace::runtime::application_http::ApplicationHttpProfile::for_mode(
                deployment,
                image,
                gateway_id,
                self.source,
                self.invocation,
                mode,
            )?;
        let executor = tokio::runtime::Handle::try_current()
            .context("application HTTP launch owner unavailable")?;
        let fence = self.begin_application_http(&profile).await?;
        // No await after creating the marker and before retaining its task.
        Ok(HttpLaunch::begin(
            executor,
            self.clone(),
            profile,
            authority,
            fence,
        ))
    }
    pub(in crate::marketplace) fn require_probe(
        &self,
        descriptor: &SourceMatchDescriptor,
    ) -> Result<()> {
        self.require_source(descriptor)?;
        if self.mode != SourceExecutionMode::Probe {
            bail!("probe requires its actual no-effects executor");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn require_invoke(
        &self,
        descriptor: &SourceMatchDescriptor,
    ) -> Result<()> {
        self.require_source(descriptor)?;
        if self.mode != SourceExecutionMode::Invoke {
            bail!("installed SDK requires its actual invoke executor");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn require_candidate(
        &self,
        descriptor: &SourceMatchDescriptor,
        execution: Digest,
    ) -> Result<()> {
        self.require_lifecycle(descriptor)?;
        if self.invocation != execution {
            bail!("native lifecycle belongs to a different candidate execution");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn require_lifecycle(
        &self,
        descriptor: &SourceMatchDescriptor,
    ) -> Result<()> {
        self.require_source(descriptor)?;
        if self.mode != SourceExecutionMode::Lifecycle {
            bail!("candidate SDK requires its actual lifecycle executor");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn require_source(
        &self,
        descriptor: &SourceMatchDescriptor,
    ) -> Result<()> {
        self.check()?;
        if self.source != Digest::of(canonical::to_vec(descriptor)?) {
            bail!("native executor differs from the retained source owner");
        }
        Ok(())
    }
    /// Synchronous same-transaction restriction. Compiled calls also recheck
    /// their bounded private reference file; no network/daemon call occurs.
    /// The scope bit never substitutes for successful native cleanup.
    pub(in crate::marketplace) fn check(&self) -> Result<()> {
        #[cfg(unix)]
        if let Some(image) = &self.image {
            image.check()?;
        }
        if !self.active.load(Ordering::SeqCst)
            || Instant::now() >= self.expires
            || !self.lease.matches_directory(self.lease.directory())
        {
            bail!("native source invocation is no longer current");
        }
        Ok(())
    }
    /// Immediately before each effect, independently observe the same native
    /// identity. A cached Ready or a running container with the same NAME is
    /// not sufficient. The transaction additionally calls check().
    pub(in crate::marketplace) async fn current(&self) -> Result<()> {
        self.check()?;
        // RuntimeManager::inspect uses the handle's lookup name. As in the
        // source-job retirement path, supply the immutable ID in BOTH fields;
        // keep the expected human name only for checking the actual response.
        let identity = ContainerHandle {
            id: self.handle.id.clone(),
            name: self.handle.id.clone(),
        };
        let state = timeout_at(self.expires, self.runtime.inspect(&identity))
            .await
            .context("native effect inspection deadline")??;
        if state.id != self.handle.id || state.name != self.handle.name || !state.running {
            bail!("native source executor identity is not running");
        }
        self.check()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn native_scope_revokes_access_and_keeps_original_source_job_and_locked_inode()
    -> Result<()> {
        // Synthetic lifecycle fixture: this tests view restrictions only,
        // never Ready/kernel/running-state or native execution qualification.
        let root = tempfile::tempdir()?;
        let lane = root.path().canonicalize()?.join("expensive");
        let lease = Arc::new(ControllerLease::acquire(&lane)?);
        let (release, _, _) = crate::marketplace::quarantine::tests::fixture()?;
        let plan = SourceQuarantinePlan::for_release(
            &release,
            None,
            &crate::marketplace::quarantine::tests::budget(),
        )?;
        let descriptor = SourceExecutionDescriptor {
            mode: SourceExecutionMode::Invoke,
            schema_version: 1,
            invocation_id: Digest::of(b"native-view restriction fixture"),
            source: plan.source_match,
            limits: SourceExecutionLimits {
                profile: Default::default(),
                heap_bytes: 16 * MIB,
                timeout_millis: 3000,
                max_file_bytes: 4 * MIB,
                json_bytes: MIB,
                pending_jobs: 128,
                sdk: None,
            },
        };
        let (scope, invocation) = NativeScope::open(
            Arc::new(crate::runtime::docker::DockerRuntimeManager::new(None)),
            ContainerHandle {
                id: "synthetic-not-inspected".into(),
                name: "synthetic".into(),
            },
            &descriptor,
            lease,
            Instant::now() + Duration::from_secs(30),
        )?;
        invocation.require_source(&descriptor.source)?;
        invocation.require_invoke(&descriptor.source)?;
        invocation.require_same(&invocation)?;
        assert!(invocation.require_lifecycle(&descriptor.source).is_err());
        let mut lifecycle = descriptor.clone();
        lifecycle.mode = SourceExecutionMode::Lifecycle;
        let (lifecycle_scope, lifecycle_invocation) = NativeScope::open(
            invocation.runtime.clone(),
            invocation.handle.clone(),
            &lifecycle,
            invocation.lease.clone(),
            Instant::now() + Duration::from_secs(30),
        )?;
        assert!(invocation.require_same(&lifecycle_invocation).is_err());
        let (other_scope, other) = NativeScope::open(
            invocation.runtime.clone(),
            invocation.handle.clone(),
            &descriptor,
            invocation.lease.clone(),
            invocation.expires,
        )?;
        assert!(
            invocation.require_same(&other).is_err(),
            "identical serialized invocation fields cannot substitute for the original owner"
        );
        drop(other_scope);
        drop(other);
        lifecycle_invocation.require_lifecycle(&descriptor.source)?;
        let http_profile =
            crate::marketplace::runtime::application_http::ApplicationHttpProfile::new(
                "storage-native-view-fixture",
                &format!("sha256:{}", "a".repeat(64)),
                &"b".repeat(64),
                Digest::of(b"scope"),
                Digest::of(b"HTTP invocation"),
            )?;
        let denied = lifecycle_invocation
            .begin_application_http(&http_profile)
            .await;
        assert!(
            denied
                .err()
                .unwrap()
                .to_string()
                .contains("original invoke executor")
        );
        assert!(
            !lane
                .join(crate::marketplace::lease::activity::http::FILE_NAME)
                .exists()
        );
        assert!(
            lifecycle_invocation
                .require_invoke(&descriptor.source)
                .is_err()
        );
        lifecycle_invocation.require_candidate(&descriptor.source, lifecycle.invocation_id)?;
        assert!(
            lifecycle_invocation
                .require_candidate(
                    &descriptor.source,
                    Digest::of(b"different candidate attempt"),
                )
                .is_err()
        );
        drop(lifecycle_scope);
        assert!(
            lifecycle_invocation
                .require_lifecycle(&descriptor.source)
                .is_err()
        );
        drop(lifecycle_invocation);
        let mut different = descriptor.source.clone();
        different.workspace.timeout_seconds += 1;
        assert!(invocation.require_source(&different).is_err());
        drop(scope);
        assert!(invocation.check().is_err());
        assert!(
            ControllerLease::acquire(&lane).is_err(),
            "revoking SDK access is not native cleanup or lane release"
        );
        drop(invocation);
        drop(ControllerLease::acquire(&lane)?);
        Ok(())
    }
}
