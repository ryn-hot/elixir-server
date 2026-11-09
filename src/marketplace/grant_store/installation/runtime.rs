//! One cold installed SourceJs call: exact ACTIVE binding -> retained archive
//! rehash -> actual native rematch -> independently scoped SDK -> confirmed
//! disposal. No discovery/issuer/browser dependency or public entry point.
//! This is NOT the final warm-runtime/cache or production activation policy.
use super::*;
use crate::{
    marketplace::{
        archive::ArchiveBudget,
        lease::ControllerLease,
        quarantine::{
            QuarantinedSource, Restoration, SourceJobBudget, SourceMatchedQuarantine,
            SourceQuarantineJob, SourceQuarantinePlan,
            execution::{Broker, NativeInvocation, RetainedCode},
        },
        worker::{
            IndependentAdmission, ParticipationClient, SdkHttpAuthority, SdkHttpCall, SdkHttpClient,
        },
    },
    playback::PlaybackJobManager,
    runtime::RuntimeManager,
};
use elixir_marketplace::{
    records::RuntimeProfile,
    source_execution::{MAX_ARGUMENT_BYTES, RuntimeJson, SourceExecutionLimits, SourceSdkReply},
    source_job::SourceMatchDescriptor,
};
use standing::InstalledBinding;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant as Deadline, sleep, sleep_until, timeout},
};

const STOP_GRACE: Duration = Duration::from_secs(45);
const CHECK: Duration = Duration::from_millis(500);
const CLEANUP_RETRY: Duration = Duration::from_secs(10);
const RESOURCE_RETRY: Duration = Duration::from_secs(10);

/// Trusted server configuration, never a peer/HTTP body. The source lane must
/// be the SAME global expensive-job lane used by preparation/builds. No new
/// permanent volume/store, resource allowance or source URL is introduced.
#[derive(Clone)]
pub(in crate::marketplace) struct InstalledRuntime {
    pub runtime: Arc<dyn RuntimeManager>,
    pub playback: Arc<PlaybackJobManager>,
    pub local_http: Option<Arc<crate::marketplace::local_http::Client>>,
    pub application_http: Option<crate::marketplace::application_egress::Configuration>,
    pub lane: PathBuf,
    pub archive: PathBuf,
    pub deployment: String,
    pub image: String,
    pub source_budget: SourceJobBudget,
    pub retention: ArchiveBudget,
}

/// One owned call, not Clone/Deserialize or an installation ticket. Caller
/// cancellation signals the SAME task; it never aborts its cleanup owner.
struct InstalledCall {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<CallCompletion>>,
    result: Option<Result<RuntimeJson>>,
    http: Option<http::Call>,
    join_failed: bool,
}
// Carries the ORIGINAL outstanding HTTP owner when its retirement is unknown.
// A joined VM/result error alone must never clear that owner's controller slot.
struct CallCompletion {
    result: Result<RuntimeJson>,
    http: Option<http::Call>,
}
impl InstalledCall {
    fn begin_owned(
        database: Database,
        secrets: SecretsManager,
        binding: InstalledBinding,
        configuration: InstalledRuntime,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
        owner: Arc<ControllerLease>,
        http: Option<SdkHttpClient>,
    ) -> Result<Self> {
        Self::start(
            database,
            secrets,
            binding,
            configuration,
            limits,
            arguments,
            http,
            |stop| RunControl {
                stop,
                owner: Some(owner),
            },
        )
    }
    /// Raw-call component fixture only; production starts require the actual
    /// installed controller's retained OS owner above.
    #[cfg(test)]
    fn begin(
        database: Database,
        secrets: SecretsManager,
        binding: InstalledBinding,
        configuration: InstalledRuntime,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
    ) -> Result<Self> {
        Self::start(
            database,
            secrets,
            binding,
            configuration,
            limits,
            arguments,
            None,
            RunControl::fixture,
        )
    }
    fn start(
        database: Database,
        secrets: SecretsManager,
        binding: InstalledBinding,
        configuration: InstalledRuntime,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
        http: Option<SdkHttpClient>,
        control: impl FnOnce(watch::Receiver<bool>) -> RunControl,
    ) -> Result<Self> {
        configuration.retention.validate()?;
        limits.validate()?;
        if arguments.accounted_bytes() > MAX_ARGUMENT_BYTES
            || limits.minimum_memory_bytes()? > configuration.source_budget.memory_bytes
        {
            bail!("installed invocation exceeds its argument or memory reservation");
        }
        if configuration.source_budget.wall_seconds == 0
            || configuration.source_budget.wall_seconds
                > elixir_marketplace::quarantine::MAX_JOB_SECONDS
        {
            bail!("installed source requires a finite source-job deadline");
        }
        let (stop, stopped) = watch::channel(false);
        let task = tokio::runtime::Handle::try_current()
            .context("installed source executor unavailable")?
            .spawn(run(
                database,
                secrets,
                binding,
                configuration,
                limits,
                arguments,
                control(stopped),
                http,
            ));
        Ok(Self {
            stop,
            task: Some(task),
            result: None,
            http: None,
            join_failed: false,
        })
    }
    async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("installed source owner stopped before confirmed cleanup");
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            match result {
                Ok(completed) => {
                    self.result = Some(completed.result);
                    self.http = completed.http;
                }
                Err(error) => {
                    self.join_failed = true;
                    return Err(error).context("installed source owner task stopped");
                }
            }
        }
        if let Some(call) = self.http.as_mut() {
            call.cancel().await?;
            self.http = None;
        }
        Ok(())
    }
    /// A value is exposed only after positive native disposal AND a final
    /// durable enrollment check. Cancellation of this waiter keeps the handle.
    async fn finish(&mut self) -> Result<RuntimeJson> {
        self.join().await?;
        self.result
            .take()
            .context("installed source result already consumed")?
    }
    async fn cancel(&mut self) -> Result<()> {
        self.stop.send_replace(true);
        timeout(STOP_GRACE, self.join())
            .await
            .context("installed source cleanup is still pending")??;
        self.result = None;
        Ok(())
    }
}
impl Drop for InstalledCall {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

/// Exact generation DATA, not native or execution authority.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Generation {
    pub(super) configured_instance: Option<Uuid>,
    pub(super) home: Uuid,
    pub(super) extension: ExtensionId,
    pub(super) instance: Uuid,
    pub(super) capsule: Digest,
}
impl Generation {
    pub(super) fn from_capsule(
        home: Uuid,
        extension: &ExtensionId,
        configured_instance: Option<Uuid>,
        capsule: &Capsule,
    ) -> Result<Self> {
        Ok(Self {
            configured_instance,
            home,
            extension: extension.clone(),
            instance: capsule.instance,
            capsule: Digest::of(canonical::to_vec(capsule)?),
        })
    }
}

mod controller;
pub(in crate::marketplace::grant_store::installation) use controller::ClosedCandidate;
pub(in crate::marketplace) use controller::RuntimeOwner;
pub(in crate::marketplace) use controller::{
    InstalledController, QuiescentRuntime, UnstartedCandidate,
};
mod candidate;
pub(in crate::marketplace) use candidate::RetiredCandidate;

#[derive(Default)]
enum Stage {
    #[default]
    Empty,
    Opening(Box<Option<SourceQuarantineJob>>),
    Restoring(Box<Restoration>),
    Source(Box<Option<QuarantinedSource>>),
    Matched(Box<Option<SourceMatchedQuarantine>>),
    Executing(Box<RetainedCode>),
}
#[derive(Default)]
struct Owner(Stage);
impl Owner {
    async fn park_bytes(&mut self) -> Result<()> {
        if let Stage::Executing(source) = &mut self.0
            && source.park_bytes().await?
        {
            self.0 = Stage::Empty;
        }
        Ok(())
    }
    async fn discard(&mut self) -> Result<()> {
        match &mut self.0 {
            Stage::Empty => {}
            Stage::Opening(slot) => {
                if let Some(job) = slot.as_mut() {
                    job.discard_retained().await?;
                }
            }
            Stage::Restoring(source) => source.cancel().await?,
            Stage::Source(slot) => {
                if let Some(source) = slot.as_mut() {
                    source.discard_retained().await?;
                }
            }
            Stage::Matched(slot) => {
                if let Some(source) = slot.as_mut() {
                    source.discard_retained().await?;
                }
            }
            Stage::Executing(source) => source.discard().await?,
        }
        self.0 = Stage::Empty;
        Ok(())
    }
    async fn confirm_discard(&mut self) {
        let mut reported = false;
        loop {
            match self.discard().await {
                Ok(()) => return,
                Err(error) => {
                    if !reported {
                        tracing::warn!(error=%error, "installed source cleanup pending; retaining its native owner and lane");
                        reported = true;
                    }
                    sleep(CLEANUP_RETRY).await;
                }
            }
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let stage = std::mem::take(&mut self.0);
        if matches!(stage, Stage::Empty) {
            return;
        }
        if let Ok(executor) = tokio::runtime::Handle::try_current() {
            executor.spawn(async move {
                Owner(stage).confirm_discard().await;
            });
        }
        // Runtime shutdown cannot manufacture cleanup. The original native
        // activity fence remains durable; no replacement work may clear it.
    }
}

#[derive(Clone)]
struct RunControl {
    stop: watch::Receiver<bool>,
    // A queued call retains the registry owner BEFORE it takes the native lane.
    // None is constructed only by explicit raw-call component fixtures.
    owner: Option<Arc<ControllerLease>>,
}
impl RunControl {
    #[cfg(test)]
    fn fixture(stop: watch::Receiver<bool>) -> Self {
        Self { stop, owner: None }
    }
}
fn restrictions(stop: &RunControl, expires: Deadline) -> Result<()> {
    if stop
        .owner
        .as_ref()
        .is_some_and(|owner| !owner.matches_directory(owner.directory()))
    {
        bail!("installed call lost its original controller owner");
    }
    if *stop.stop.borrow() || stop.stop.has_changed().is_err() {
        bail!("installed source invocation cancelled");
    }
    if Deadline::now() >= expires {
        bail!("installed source invocation expired");
    }
    Ok(())
}
async fn stopped(stop: &mut RunControl) {
    loop {
        if *stop.stop.borrow() || stop.stop.changed().await.is_err() {
            return;
        }
    }
}
/// Cancellation/deadline only. A snapshot or effect transaction must never be
/// paused to start a SECOND database writer on its own scope.
async fn cancellable<T>(
    stop: &mut RunControl,
    expires: Deadline,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    restrictions(stop, expires)?;
    let result = tokio::select! {biased;
        _=stopped(stop)=>bail!("installed source invocation cancelled"),
        _=sleep_until(expires)=>bail!("installed source invocation expired"),
        result=future=>result,
    };
    restrictions(stop, expires)?;
    result
}
/// Used during source-only phases, never around SDK effect transactions. The
/// broker performs its own live checks during execution; pausing an in-flight
/// effect to acquire another SQLite writer would otherwise deadlock itself.
async fn guarded<T>(
    database: &Database,
    binding: &dyn Access,
    stop: &mut RunControl,
    expires: Deadline,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    restrictions(stop, expires)?;
    binding.current(database).await?;
    tokio::pin!(future);
    loop {
        tokio::select! {biased;
            _=stopped(stop)=>bail!("installed source invocation cancelled"),
            _=sleep_until(expires)=>bail!("installed source invocation expired"),
            _=sleep(CHECK)=>binding.current(database).await?,
            result=&mut future=>{ restrictions(stop, expires)?; return result; },
        }
    }
}
async fn resource_pause(
    database: &Database,
    binding: &dyn Access,
    stop: &mut RunControl,
    expires: Deadline,
    error: anyhow::Error,
) -> Result<()> {
    // Closed, trusted PRE-native scheduling outcome only. Never retry an
    // unknown native result, interpreter/SDK error, or message string.
    let Some(waiting) = ParticipationClient::source_resource_wait(&error) else {
        return Err(error);
    };
    tracing::debug!(
        ?waiting,
        "installed source retaining its owner while awaiting physical headroom"
    );
    guarded(database, binding, stop, expires, async {
        sleep(RESOURCE_RETRY).await;
        Ok(())
    })
    .await
}

#[async_trait::async_trait]
trait Access: Sync {
    fn review(&self) -> &GrantReview;
    async fn current(&self, database: &Database) -> Result<()>;
}
#[async_trait::async_trait]
impl Access for InstalledBinding {
    fn review(&self) -> &GrantReview {
        InstalledBinding::review(self)
    }
    async fn current(&self, database: &Database) -> Result<()> {
        InstalledBinding::current(self, database).await
    }
}

struct InstalledBroker<'a> {
    database: &'a Database,
    secrets: &'a SecretsManager,
    binding: Arc<InstalledBinding>,
    descriptor: SourceMatchDescriptor,
    native: Option<Arc<NativeInvocation>>,
    http: Option<&'a SdkHttpClient>,
    local_http: Option<&'a Arc<crate::marketplace::local_http::Client>>,
    application_http: Option<&'a crate::marketplace::application_egress::Configuration>,
    application_image: &'a str,
    application_deployment: &'a str,
    application_admission: Option<Arc<IndependentAdmission>>,
    http_call: &'a mut Option<http::Call>,
    stop: RunControl,
    expires: Deadline,
    checked: Option<Deadline>,
}
mod http;
impl InstalledBroker<'_> {
    fn check(&self) -> Result<()> {
        restrictions(&self.stop, self.expires)?;
        if let Some(native) = &self.native {
            native.check()?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl Broker for InstalledBroker<'_> {
    async fn retire_effects(&mut self) -> Result<()> {
        if let Some(call) = self.http_call.as_mut() {
            call.cancel().await?;
            *self.http_call = None;
        }
        Ok(())
    }
    fn native_started(&mut self, invocation: NativeInvocation) -> Result<()> {
        self.check()?;
        if self.native.is_some() {
            bail!("installed broker already bound to a native executor");
        }
        invocation.require_invoke(&self.descriptor)?;
        self.native = Some(Arc::new(invocation));
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        self.check()?;
        if self.checked.is_none_or(|last| last.elapsed() >= CHECK) {
            self.binding.current(self.database).await?;
            self.check()?;
            self.checked = Some(Deadline::now());
        }
        Ok(())
    }
    async fn request(&mut self, raw: &RuntimeJson, maximum: usize) -> Result<SourceSdkReply> {
        self.check()?;
        let native = self
            .native
            .as_ref()
            .context("installed SDK requires its actual native executor")?;
        native.current().await?;
        if elixir_marketplace::source_execution::SourceSdkRequest::parse(raw)?.is_http() {
            return self.request_http(raw, maximum).await;
        }
        super::super::sdk::InstalledScope {
            database: self.database,
            secrets: self.secrets,
            binding: &self.binding,
        }
        .execute(raw, maximum, || {
            self.check()?;
            native.check()
        })
        .await
    }
}

async fn run(
    database: Database,
    secrets: SecretsManager,
    binding: InstalledBinding,
    configuration: InstalledRuntime,
    limits: SourceExecutionLimits,
    arguments: RuntimeJson,
    mut stop: RunControl,
    http: Option<SdkHttpClient>,
) -> CallCompletion {
    let expires = Deadline::now() + Duration::from_secs(configuration.source_budget.wall_seconds);
    let mut owner = Owner::default();
    let binding = Arc::new(binding);
    let mut http_call = None;
    let mut result = async {
        restrictions(&stop, expires)?;
        let capsule = cancellable(&mut stop, expires, binding.material(&database)).await?;
        let preparation_started = Deadline::now();
        let prepared = load_retained(
            &mut owner,
            &database,
            binding.as_ref(),
            &configuration,
            &capsule,
            &mut stop,
            expires,
            elixir_marketplace::source_execution::SourceExecutionMode::Invoke,
        )
        .await;
        tracing::debug!(target: "elixir_server::marketplace::installed_call_timing",
            phase = "restore_match", elapsed_seconds = preparation_started.elapsed().as_secs_f64(),
            success = prepared.is_ok(), "installed call phase duration");
        prepared?;
        let Stage::Executing(code) = &mut owner.0 else {
            unreachable!()
        };
        let mut broker = InstalledBroker {
            database: &database,
            secrets: &secrets,
            binding: binding.clone(),
            descriptor: code.descriptor(),
            native: None,
            http: http.as_ref(),
            local_http: configuration.local_http.as_ref(),
            application_http: configuration.application_http.as_ref(),
            application_image: &configuration.image,
            application_deployment: &configuration.deployment,
            application_admission: if configuration.application_http.as_ref().is_some_and(|http| http.has_network()) {
                Some(IndependentAdmission::for_application(configuration.playback.clone(), configuration.archive.clone())?)
            } else { None },
            http_call: &mut http_call,
            stop: stop.clone(),
            expires,
            checked: None,
        };
        // Broker checks standing restrictions while reads stall and per effect;
        // this outer guard owns cancellation/deadline only, not another DB tx.
        loop {
            broker.checked = None;
            let invocation_started = Deadline::now();
            let invoked = cancellable(
                &mut stop,
                expires,
                code.invoke(limits.clone(), &arguments, &mut broker),
            )
            .await;
            tracing::debug!(target: "elixir_server::marketplace::installed_call_timing",
                phase = "invoke_attempt", elapsed_seconds = invocation_started.elapsed().as_secs_f64(),
                success = invoked.is_ok(), "installed call phase duration");
            match invoked {
                Ok(result) => break Ok(result),
                Err(error) => {
                    // Once actual native Ready bound this broker, never
                    // automatically rerun code or replay earlier SDK effects.
                    if broker.native.is_some() {
                        return Err(error);
                    }
                    resource_pause(&database, binding.as_ref(), &mut stop, expires, error).await?;
                }
            }
        }
    }
    .await;
    // Both cleanup owners live outside code.invoke's fallible future. Even an
    // unknown HTTP result must not skip source-VM disposal or lose that call.
    let retirement_started = Deadline::now();
    if let Some(call) = http_call.as_mut() {
        match call.cancel().await {
            Ok(()) => http_call = None,
            Err(error) => result = Err(error),
        }
    }
    if result.is_ok()
        && http_call.is_none()
        && let Err(error) = owner.park_bytes().await
    {
        result = Err(error);
    }
    owner.confirm_discard().await;
    // Diagnostics contain no invocation/instance/source identifiers or payloads
    // and are never consumed as execution, retirement or idle authority.
    tracing::debug!(target: "elixir_server::marketplace::installed_call_timing",
        phase = "retirement", elapsed_seconds = retirement_started.elapsed().as_secs_f64(),
        http_retirement_pending = http_call.is_some(), "installed call phase duration");
    let result = async {
        let result = result?;
        restrictions(&stop, expires)?;
        binding.current(&database).await?;
        restrictions(&stop, expires)?;
        Ok(result)
    }
    .await;
    CallCompletion {
        result,
        http: http_call,
    }
}

/// Restore/rematch and admission are identical for active calls and candidates.
/// This owns no second cleanup lane: all partial native work stays in owner.
async fn load_retained(
    owner: &mut Owner,
    database: &Database,
    binding: &dyn Access,
    configuration: &InstalledRuntime,
    capsule: &Capsule,
    stop: &mut RunControl,
    expires: Deadline,
    mode: elixir_marketplace::source_execution::SourceExecutionMode,
) -> Result<()> {
    let facts = capsule.facts()?;
    if facts.release().payload().runtime_profile != RuntimeProfile::SourceJs {
        bail!("installed cold execution supports SourceJs only");
    }
    let plan =
        SourceQuarantinePlan::for_release(facts.release(), None, &configuration.source_budget)?;
    let resources = binding
        .review()
        .scope()
        .runtime()
        .context("installed source has no reviewed runtime resources")?;
    plan.require_execution_resources(resources)?;
    let lease = ControllerLease::acquire(&configuration.lane)?;
    let admission = if configuration
        .application_http
        .as_ref()
        .is_some_and(|http| http.has_network())
    {
        IndependentAdmission::for_application(
            configuration.playback.clone(),
            configuration.archive.clone(),
        )?
    } else {
        IndependentAdmission::new(
            configuration.playback.clone(),
            configuration.archive.clone(),
        )?
    };
    owner.0 = Stage::Opening(Box::new(None));
    let Stage::Opening(slot) = &mut owner.0 else {
        unreachable!()
    };
    // All partial native work is retained in slot before the first native
    // side effect. A cancelled outer future cannot detach its cleanup.
    let reuse_allowed = stop.owner.is_some();
    let mut opened = guarded(database, binding, stop, expires, async {
        if reuse_allowed {
            SourceQuarantineJob::open_installed_retained(
                slot,
                configuration.runtime.clone(),
                &configuration.deployment,
                &configuration.image,
                &plan,
                lease,
                admission,
            )
            .await
        } else {
            SourceQuarantineJob::open_leased_retained(
                slot,
                configuration.runtime.clone(),
                &configuration.deployment,
                &configuration.image,
                &plan,
                lease,
                admission,
            )
            .await
            .map(|()| false)
        }
    })
    .await;
    let reuse = loop {
        match opened {
            Ok(reuse) => break reuse,
            Err(error) => {
                if !slot
                    .as_ref()
                    .as_ref()
                    .is_some_and(|job| job.admission_unstarted())
                {
                    return Err(error);
                }
                resource_pause(database, binding, stop, expires, error).await?;
                let job = slot
                    .as_mut()
                    .as_mut()
                    .context("original installed opening job is missing")?;
                opened = guarded(
                    database,
                    binding,
                    stop,
                    expires,
                    job.resume_unstarted_retained(expires),
                )
                .await
                .map(|()| false);
            }
        }
    };
    tracing::debug!(target: "elixir_server::marketplace::installed_call_timing",
        cache_hit = reuse, "installed byte workspace preparation");
    let source = if reuse {
        SourceQuarantineJob::take_cached_candidate(slot)?
    } else {
        let job = slot
            .take()
            .context("installed native source job is missing")?;
        owner.0 = Stage::Restoring(Box::new(Restoration::new(
            job,
            configuration.archive.clone(),
            configuration.retention,
        )));
        let Stage::Restoring(restoration) = &mut owner.0 else {
            unreachable!()
        };
        loop {
            match guarded(database, binding, stop, expires, restoration.advance()).await {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => resource_pause(database, binding, stop, expires, error).await?,
            }
        }
        loop {
            match guarded(database, binding, stop, expires, restoration.finish()).await {
                Ok(source) => break source,
                Err(error) => resource_pause(database, binding, stop, expires, error).await?,
            }
        }
    };
    owner.0 = Stage::Source(Box::new(Some(source)));
    let inline = mode == elixir_marketplace::source_execution::SourceExecutionMode::Invoke
        && configuration
            .runtime
            .image_labels(&configuration.image)
            .await?
            .get("elixir.marketplace.worker.source_eval_verified")
            .map(String::as_str)
            == Some("1");
    if inline {
        let Stage::Source(slot) = &mut owner.0 else {
            unreachable!()
        };
        owner.0 = Stage::Executing(Box::new(RetainedCode::take_pending(
            slot,
            &facts,
            &capsule.descriptor()?,
            capsule.source.summary(),
        )?));
    } else {
        let Stage::Source(slot) = &mut owner.0 else {
            unreachable!()
        };
        let matched = loop {
            match guarded(
                database,
                binding,
                stop,
                expires,
                QuarantinedSource::match_retained(slot),
            )
            .await
            {
                Ok(matched) => break matched,
                Err(error) => resource_pause(database, binding, stop, expires, error).await?,
            }
        };
        owner.0 = Stage::Matched(Box::new(Some(matched)));
        let Stage::Matched(slot) = &mut owner.0 else {
            unreachable!()
        };
        owner.0 = Stage::Executing(Box::new(RetainedCode::take(
            slot,
            &facts,
            &capsule.descriptor()?,
            capsule.source.summary(),
        )?));
    }
    restrictions(stop, expires)?;
    binding.current(database).await?;
    let Stage::Executing(code) = &mut owner.0 else {
        unreachable!()
    };
    code.require_execution_resources(resources)?;
    Ok(())
}

#[cfg(test)]
pub(in crate::marketplace::grant_store::installation) mod tests;
