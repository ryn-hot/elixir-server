//! Single local owner of installed calls. This does not add another native
//! service, job allowance, installed generation or permanent package store.
use super::super::activation::quiescence::CutoverScope;
use super::*;

const OWNER_DIRECTORY: &str = "installed-controller";
mod binding;
mod unstarted;
pub(in crate::marketplace::grant_store::installation) use unstarted::ClosedCandidate;
pub(in crate::marketplace) use unstarted::UnstartedCandidate;

struct RegisteredCall {
    generation: Generation,
    call: InstalledCall,
}

/// Original controller ownership retained by an actual queued/running task,
/// then by its joined retirement result. Not constructible from DB/status data.
pub(in crate::marketplace) struct RuntimeOwner {
    lease: Arc<ControllerLease>,
    binding: binding::Binding,
}
impl RuntimeOwner {
    pub(in crate::marketplace) fn check(&self) -> Result<()> {
        self.binding.require(&self.lease)
    }
    pub(in crate::marketplace::grant_store) async fn require(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        self.check()?;
        self.binding.require_database(tx, driver).await?;
        self.check()
    }
    pub(in crate::marketplace) fn lease(&self) -> Arc<ControllerLease> {
        self.lease.clone()
    }
    /// Runtime lineage only, never an installed grant or effect permission.
    pub(in crate::marketplace) async fn current(&self, database: &Database) -> Result<()> {
        self.check()?;
        let mut tx = database.pool.begin().await?;
        self.require(&mut tx, database.driver).await?;
        tx.commit().await?;
        self.check()
    }
}

/// All production installed starts go through this owner; raw InstalledCall
/// creation is private to the runtime module. The fixed child-directory OS
/// lease prevents a second process/controller for the SAME configured lane.
/// One cold call matches the existing one-expensive-job policy, NOT the final
/// warm-runtime/concurrency policy. Retained warm ownership remains required.
pub(in crate::marketplace) struct InstalledController {
    active: Option<RegisteredCall>,
    candidate: Option<super::candidate::CandidateCall>,
    lease: Arc<ControllerLease>,
    binding: binding::Binding,
    database: Database,
    secrets: SecretsManager,
    configuration: InstalledRuntime,
    #[cfg(unix)] image_cache:Option<Arc<crate::marketplace::quarantine::image_cache::Pool>>,
}
impl InstalledController {
    pub(in crate::marketplace) async fn open(
        database: Database,
        secrets: SecretsManager,
        mut configuration: InstalledRuntime,
    ) -> Result<Self> {
        configuration.retention.validate()?;
        if !crate::marketplace::runtime::image_is_pinned(&configuration.image) {
            bail!("installed controller requires an immutable worker image");
        }
        // Establish both owners before inspecting/reconciling anything. Never
        // recover a live foreign owner's startup or stop a name-based match.
        // Source jobs MUST use this same configured global lane. Its existing
        // write-ahead invariant covers every native create, including crashes
        // before an InstalledCall's acknowledgement reaches this controller.
        let (lease, binding) = binding::open(&database, &configuration).await?;
        #[cfg(unix)]
        let image_cache=if configuration.compiled_toolchains.is_some()
            && let Some(profile)=configuration.runtime.marketplace_import_profile()
            && profile.image_cache_enabled() {
                Some(crate::marketplace::quarantine::image_cache::Pool::open(configuration.runtime.clone(),profile,&configuration.lane).await?)
            } else {None};
        if let Some(connection) = configuration.application_http.as_mut() {
            if let Some((network, _)) = connection.network() {
                let pool = crate::marketplace::application_egress::WarmPool::new(
                    Arc::new(RuntimeOwner {
                        lease: lease.clone(),
                        binding: binding.clone(),
                    }),
                    configuration.runtime.clone(),
                    database.clone(),
                    secrets.clone(),
                    network.to_owned(),
                );
                connection.set_pool(Arc::new(pool));
            }
        }
        Ok(Self {
            active: None,
            candidate: None,
            lease,
            binding,
            database,
            secrets,
            configuration,
            #[cfg(unix)] image_cache,
        })
    }
    fn require_owner(&self) -> Result<()> {
        if !self
            .lease
            .matches_directory(&self.configuration.lane.join(OWNER_DIRECTORY))
        {
            bail!("installed runtime controller lost its original owner lock");
        }
        self.binding.require(&self.lease)?;
        Ok(())
    }
    /// Fail BEFORE moving this original controller or starting another command.
    /// Deployment is a fresh job nonce, deliberately not stable configuration.
    /// Runtime ownership and every SQL/native transition still recheck their
    /// original binding; this comparison is not a proof factory.
    pub(in crate::marketplace) fn require_configuration(
        &self,
        configuration: &InstalledRuntime,
        secrets: &SecretsManager,
    ) -> Result<()> {
        self.require_owner()?;
        let a = &self.configuration;
        let limits = |budget: &SourceJobBudget| {
            (
                budget.memory_bytes,
                budget.cpu_millicores,
                budget.disk_bytes,
                budget.input_bytes,
                budget.output_bytes,
                budget.wall_seconds,
            )
        };
        let key_context = b"elixir-marketplace/installed-controller-key/v1";
        let same_local_http = match (&a.local_http, &configuration.local_http) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        let same_compiled_toolchains =
            match (&a.compiled_toolchains, &configuration.compiled_toolchains) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
        if !Arc::ptr_eq(&a.runtime, &configuration.runtime)
            || !Arc::ptr_eq(&a.playback, &configuration.playback)
            || !same_local_http
            || !same_compiled_toolchains
            || a.lane != configuration.lane
            || a.archive != configuration.archive
            || a.image != configuration.image
            || limits(&a.source_budget) != limits(&configuration.source_budget)
            || (a.retention.bytes, a.retention.releases)
                != (
                    configuration.retention.bytes,
                    configuration.retention.releases,
                )
            || self.secrets.request_fingerprint(key_context, b"")?
                != secrets.request_fingerprint(key_context, b"")?
            || self.active.is_some()
            || self.candidate.is_some()
        {
            bail!("retained installed controller configuration changed or still owns work");
        }
        Ok(())
    }
    pub(super) fn retained_owner(&self) -> Result<Arc<RuntimeOwner>> {
        self.require_owner()?;
        Ok(Arc::new(RuntimeOwner {
            lease: self.lease.clone(),
            binding: self.binding.clone(),
        }))
    }
    #[cfg(test)]
    pub(super) fn candidate_fixture(&mut self, call: super::candidate::CandidateCall) {
        assert!(self.candidate.is_none() && self.active.is_none());
        self.candidate = Some(call);
    }
    pub(in crate::marketplace) fn begin(
        &mut self,
        binding: InstalledBinding,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
    ) -> Result<()> {
        self.begin_with_http(binding, limits, arguments, None)
    }
    fn begin_with_http(
        &mut self,
        binding: InstalledBinding,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
        http: Option<SdkHttpClient>,
    ) -> Result<()> {
        self.require_owner()?;
        if self.active.is_some() || self.candidate.is_some() {
            bail!("installed call is still owned; join it before another start");
        }
        let generation = binding.generation();
        let call = InstalledCall::begin_owned(
            self.database.clone(),
            self.secrets.clone(),
            binding,
            self.configuration.clone(),
            limits,
            arguments,
            self.lease.clone(),
            http,
            #[cfg(unix)] self.image_cache.clone(),
        )?;
        // No await between starting the task and retaining its ONLY owner.
        self.active = Some(RegisteredCall { generation, call });
        Ok(())
    }
    /// The actual retained controller, not a status/journal DTO, performs the
    /// fresh configured ACTIVE lookup. No discovery connection is required.
    pub(in crate::marketplace) async fn begin_configured(
        &mut self,
        home: Uuid,
        extension: &ExtensionId,
        instance: Uuid,
        budget: InstallBudget,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
        http: Option<SdkHttpClient>,
    ) -> Result<()> {
        self.require_idle()?;
        let binding =
            InstalledBinding::load_configured(&self.database, home, extension, instance, budget)
                .await?;
        self.begin_with_http(binding, limits, arguments, http)
    }
    pub(in crate::marketplace) fn require_idle(&self) -> Result<()> {
        self.require_owner()?;
        if self.active.is_some() || self.candidate.is_some() {
            bail!("original installed controller still owns native work");
        }
        Ok(())
    }
    #[cfg(all(test,unix))]
    pub(super) async fn image_cache_snapshot(&self)->Result<Option<(Digest,String,u64)>> {
        match &self.image_cache {Some(cache)=>cache.snapshot().await,None=>Ok(None)}
    }
    pub(in crate::marketplace) async fn begin_provider(
        &mut self,
        coordinates: &super::super::providers::Coordinates,
        revision: Digest,
        budget: InstallBudget,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
        http: Option<SdkHttpClient>,
    ) -> Result<()> {
        self.require_idle()?;
        let (binding, _) =
            InstalledBinding::load_provider(&self.database, coordinates, budget, Some(revision))
                .await?;
        self.begin_with_http(binding, limits, arguments, http)
    }
    /// Pure preflight precedes taking the original claim. No await occurs
    /// between its move into the task and retaining that task's only handle.
    pub(in crate::marketplace) fn begin_candidate(
        &mut self,
        claim: &mut Option<super::super::activation::candidate::CandidateClaim>,
        limits: SourceExecutionLimits,
    ) -> Result<()> {
        self.preflight_candidate(&limits)?;
        let original = claim.as_ref().context("original candidate claim missing")?;
        original.require_configuration(&self.configuration, &self.secrets)?;
        let executor =
            tokio::runtime::Handle::try_current().context("candidate executor unavailable")?;
        let owner = self.retained_owner()?;
        owner.check()?;
        let mut configuration = self.configuration.clone();
        configuration.deployment = original.deployment();
        let original = claim.take().expect("borrowed original candidate claim");
        self.candidate = Some(super::candidate::CandidateCall::start(
            executor,
            self.database.clone(),
            self.secrets.clone(),
            original,
            configuration,
            limits,
            owner,
        ));
        Ok(())
    }
    /// No claim is consumed and no native work is started by preflight.
    pub(in crate::marketplace) fn preflight_candidate(
        &self,
        limits: &SourceExecutionLimits,
    ) -> Result<()> {
        self.require_owner()?;
        if self.active.is_some() || self.candidate.is_some() {
            bail!("join the original installed/candidate owner before another start");
        }
        super::candidate::validate(&self.configuration, limits)
    }
    /// Consume only the original caller-owned claim, BEFORE any task received
    /// it. Configuration/source/session validity is not cleanup authority.
    /// Rejection leaves the caller's original slot untouched.
    pub(in crate::marketplace::grant_store::installation) fn seal_unstarted_candidate(
        &self,
        claim: &mut Option<super::super::activation::candidate::CandidateClaim>,
    ) -> Result<super::UnstartedCandidate> {
        self.require_owner()?;
        if self.active.is_some() || self.candidate.is_some() {
            bail!("candidate sealing requires the original idle controller");
        }
        let original = claim.as_ref().context("original candidate claim missing")?;
        if original.binding().reference().configured_instance.is_none() {
            bail!("unstarted candidate cleanup requires a configured owner");
        }
        let owner = self.retained_owner()?;
        let original = claim.take().expect("borrowed original candidate claim");
        Ok(super::UnstartedCandidate::new(
            original,
            owner,
            self.database.clone(),
            self.secrets.clone(),
        ))
    }
    /// Trusted local stop signal only. It neither joins a task nor proves
    /// disposal, authorizes SQL cleanup, or releases this controller's slot.
    pub(in crate::marketplace) fn request_stop(&self) {
        if let Some(active) = &self.active {
            active.call.stop.send_replace(true);
        }
        if let Some(candidate) = &self.candidate {
            candidate.signal_stop();
        }
    }
    /// Joins the same original task, never infers retirement from status or a
    /// missing container. A failed/panicked join leaves the slot fenced.
    pub(in crate::marketplace) async fn finish_candidate(
        &mut self,
    ) -> Result<super::RetiredCandidate> {
        let candidate = self.candidate.as_mut().context("no owned candidate call")?;
        candidate.join().await?;
        let retired = candidate.take()?;
        self.candidate = None;
        Ok(retired)
    }
    pub(in crate::marketplace) async fn finish(&mut self) -> Result<RuntimeJson> {
        self.require_owner()?;
        let active = self.active.as_mut().context("no owned installed call")?;
        // A normal source error still joins positive cleanup. A task panic or
        // uncertain join MUST keep the slot fenced, not permit a new begin.
        active.call.join().await?;
        let result = active.call.finish().await;
        self.active = None;
        result
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        // Cleanup remains possible after owner-path corruption. It can only
        // signal/join this controller's original task, never mint a new start.
        if let Some(active) = self.active.as_mut() {
            active.call.cancel().await?;
        }
        self.active = None;
        if let Some(candidate) = self.candidate.as_mut() {
            // Keep the joined result/claim until finish_candidate takes it.
            // Cancelling must not throw away the only failure-cleanup proof.
            candidate.cancel().await?;
        }
        #[cfg(unix)]
        if let Some(cache)=&self.image_cache {cache.evict().await?;}
        Ok(())
    }
    pub(in crate::marketplace) async fn shutdown_cache(&mut self) -> Result<()> {
        self.cancel().await?;
        if let Some(connection) = &self.configuration.application_http {
            connection.shutdown().await?;
        }
        // Cache bytes have no original task to join. Retire them under the
        // SAME global lane before a clean shutdown/quiescent handoff. Never
        // acquire replacement storage after owner-path corruption.
        if self.require_owner().is_ok() {
            let lane = ControllerLease::acquire(&self.configuration.lane)?;
            SourceQuarantineJob::evict_cached(self.configuration.runtime.as_ref(), &lane).await?;
        }
        Ok(())
    }
    /// Cleanup coordinates only, not permission to start a native job. The
    /// application may retain these across shutdown waits; eviction itself
    /// must acquire the actual global lane and reject any live native writer.
    pub(in crate::marketplace) fn cache_cleanup_coordinates(
        &self,
    ) -> Result<(Arc<dyn RuntimeManager>, PathBuf)> {
        self.require_owner()?;
        Ok((
            self.configuration.runtime.clone(),
            self.configuration.lane.clone(),
        ))
    }
    /// Fresh scope is coordination data only. Holding the returned borrow
    /// prevents this owner from starting/replacing any call during a future
    /// snapshot transaction. That transaction must revalidate the EXACT saved
    /// barrier, current owner/source/grants and both actual state roots again.
    pub(in crate::marketplace) async fn quiesce(
        &mut self,
        scope: CutoverScope,
    ) -> Result<QuiescentRuntime<'_>> {
        self.require_owner()?;
        if self.candidate.is_some() {
            bail!("candidate execution/retirement is still owned; no new quiescence");
        }
        scope.current(&self.database).await?;
        if let Some(active) = &self.active
            && &active.generation.extension == scope.extension()
            && active.generation.configured_instance == scope.configured_instance()
        {
            if scope.active() != Some(&active.generation) {
                bail!("owned native call differs from the exact old generation");
            }
            self.cancel().await?;
        }
        self.require_owner()?;
        scope.current(&self.database).await?;
        #[cfg(unix)]
        if let Some(cache)=&self.image_cache {cache.evict().await?;}
        if let Some(connection) = &self.configuration.application_http {
            connection.shutdown().await?;
        }
        Ok(QuiescentRuntime {
            controller: self,
            scope,
        })
    }
}
impl Drop for InstalledController {
    fn drop(&mut self) {
        // Signal before releasing the owner lease. The original task retains
        // the global native lane through confirmed cleanup. A new controller
        // cannot reconcile while that task (or its late-start owner) holds it.
        self.request_stop();
    }
}

/// Scoped ownership barrier, not serializable or clonable, and never obtained
/// from an unlocked lock, missing name, journal phase or "stopped" boolean.
/// Empty ownership is established only by controller startup reconciliation;
/// existing matching calls must join their original confirmed cleanup owner.
/// This is NOT state-copy, candidate-readiness, activation or serving proof.
pub(in crate::marketplace) struct QuiescentRuntime<'a> {
    controller: &'a mut InstalledController,
    scope: CutoverScope,
}
impl QuiescentRuntime<'_> {
    pub(in crate::marketplace::grant_store::installation) fn database(&self) -> &Database {
        &self.controller.database
    }
    pub(in crate::marketplace::grant_store::installation) fn secrets(&self) -> &SecretsManager {
        &self.controller.secrets
    }
    pub(in crate::marketplace::grant_store::installation) async fn require(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
        stored: &Stored,
    ) -> Result<()> {
        self.controller.require_owner()?;
        self.scope.require(stored)?;
        self.controller.binding.require_database(tx, driver).await?;
        self.controller.require_owner()
    }
    /// Original owner/DB lineage only. This does not extend the pre-migration
    /// scope, mint another claim, or prove candidate completion/retirement.
    pub(in crate::marketplace::grant_store::installation) async fn require_owner(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        self.controller.require_owner()?;
        self.controller.binding.require_database(tx, driver).await?;
        self.controller.require_owner()
    }
}

#[cfg(test)]
mod tests;
