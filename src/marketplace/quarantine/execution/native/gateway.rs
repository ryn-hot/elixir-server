//! ONE original startup/lifetime owner, retained outside the fallible source
//! exchange. Material acceptance is never a Ready/tunnel/admission capability.
use super::*;
use crate::marketplace::{
    application_egress::{ApplicationGatewayProfile, GatewayInput, MATERIAL_ACCEPTED, Protection},
    lease::activity::gateway::GatewayFence,
    worker::SdkHttpAuthority,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
    task::JoinHandle,
};

mod owner;
mod pipes;
use owner::Owner;

use std::sync::{OnceLock, atomic::AtomicBool};

const CHECK_INTERVAL: Duration = Duration::from_millis(500);
const AUTHORITY_TIMEOUT: Duration = Duration::from_secs(5);
const RETIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// The original accepted configuration and installed authority, retained by every
/// dependent HTTP helper. This binds lifetime/identity only, NOT VPN readiness,
/// aggregate admission, purpose consent or permission to send HTTP input.
struct Scope {
    owner: Owner,
    profile: Arc<ApplicationGatewayProfile>,
    protection: Protection,
    handle: OnceLock<ContainerHandle>,
    // Set once from the first complete native verification. Never reset after
    // drift/failure; this is expected policy, not a live-state/permission cache.
    policy: OnceLock<crate::marketplace::application_egress::ApplicationGatewayObservation>,
    closed: AtomicBool,
    failure: OnceLock<&'static str>,
    observing: AtomicBool,
    observation: tokio::sync::Mutex<
        Option<(
            Instant,
            crate::marketplace::application_egress::ApplicationGatewayObservation,
        )>,
    >,
}
impl Scope {
    fn check(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            bail!(
                "original application gateway authority is closed ({})",
                self.failure.get().copied().unwrap_or("owner_closed")
            );
        }
        self.owner.check()
    }
    fn revoke(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
    fn fail(&self, stage: &'static str, error: &anyhow::Error) {
        let _ = self.failure.set(stage);
        // This lifecycle contains only controller/provider/native checks, not
        // request URLs, headers, source responses or credential input. Keep
        // diagnostics finite and preserve the FIRST failure stage.
        let message: String = error.to_string().chars().take(384).collect();
        tracing::debug!(stage,error=%message,"original application tunnel stopped");
        self.revoke();
    }
    async fn configuration_current(&self) -> Result<()> {
        self.check()?;
        let owner = timeout(AUTHORITY_TIMEOUT, self.owner.current())
            .await
            .context("gateway authority deadline")
            .and_then(|result| result);
        if let Err(error) = owner {
            self.fail("owner_or_admission", &error);
            return Err(error);
        }
        if let Err(error) = self.protection.current().await {
            self.fail("vpn_configuration", &error);
            return Err(error);
        }
        self.check()
    }
    fn bind(&self, handle: ContainerHandle) -> Result<ContainerHandle> {
        self.check()?;
        if handle.name != self.profile.spec().name {
            bail!("original gateway material identity differs");
        }
        self.handle
            .set(handle.clone())
            .map_err(|_| anyhow::anyhow!("original gateway identity already bound"))?;
        self.check()?;
        Ok(handle)
    }
    async fn bound_current(&self) -> Result<()> {
        self.check()?;
        let handle = self
            .handle
            .get()
            .context("gateway material is not accepted")?;
        // A lookup alias is not native-name evidence. Compare the actual
        // inspected immutable ID/name, never recover or replace by that name.
        let identity = ContainerHandle {
            id: handle.id.clone(),
            name: handle.id.clone(),
        };
        let actual = timeout_at(
            self.owner.expires(),
            self.owner.runtime().inspect(&identity),
        )
        .await
        .context("retained application gateway inspection deadline")??;
        if actual.id != handle.id || actual.name != handle.name || !actual.running {
            bail!("original application gateway is not running");
        }
        self.configuration_current().await
    }
    async fn observe(
        &self,
    ) -> Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        self.check()?;
        // Share one bounded native observation across concurrent owner checks
        // and chunk operations. No caller can refresh its timestamp; failures
        // clear it before the await, and the owner supervises it independently.
        let mut cached = self.observation.lock().await;
        if let Some((at, observed)) = cached.as_ref()
            && at.elapsed() < Duration::from_secs(1)
        {
            return Ok(observed.clone());
        }
        *cached = None;
        if let Err(error) = self.configuration_current().await {
            self.revoke();
            return Err(error);
        }
        let handle = self
            .handle
            .get()
            .context("gateway material is not accepted")?;
        let observation_started = Instant::now();
        let result = timeout_at(self.owner.expires(), async {
            match self.policy.get() {
                Some(original) => {
                    self.owner
                        .runtime()
                        .observe_unchanged_application_gateway(handle, &self.profile, original)
                        .await
                }
                None => {
                    self.owner
                        .runtime()
                        .observe_application_gateway(handle, &self.profile)
                        .await
                }
            }
        })
        .await
        .context("original gateway kernel observation deadline")
        .and_then(|result| result);
        let observed = match result {
            Ok(observed) => observed,
            Err(error) => {
                // wait_tunnel() can observe this SAME original gateway before
                // its tunnel exists. No successful policy/health was accepted
                // yet and no HTTP input can be sent. Once bound, any failed
                // observation is fatal; never reset or re-bootstrap it.
                if self.policy.get().is_some() {
                    self.fail("native_policy_observation", &error);
                }
                return Err(error);
            }
        };
        if let Some(original) = self.policy.get()
            && !observed.same_guard(original)
        {
            self.revoke();
            bail!("original gateway policy or tunnel differs");
        }
        if let Err(error) = self.configuration_current().await {
            self.revoke();
            return Err(error);
        }
        // observation mutex serializes initial binding. No public DTO or peer
        // can supply this non-Deserialize, runtime-created native observation.
        if let Err(error) = self.owner.observed(&observed).await {
            self.fail("physical_headroom", &error);
            return Err(error);
        }
        if self.policy.get().is_none() {
            self.policy
                .set(observed.clone())
                .map_err(|_| anyhow::anyhow!("original gateway policy already bound"))?;
        }
        // Configuration/SQL rechecks can themselves take time. They cannot
        // re-date an older kernel/resource sample and extend its one-second
        // reuse window; slow observations are returned but never reused fresh.
        *cached = Some((observation_started, observed.clone()));
        self.observing.store(true, Ordering::SeqCst);
        Ok(observed)
    }
}
#[async_trait::async_trait]
impl SdkHttpAuthority for Scope {
    async fn current_native(&self, native: &NativeInvocation) -> Result<()> {
        self.owner.native()?.require_same(native)?;
        // bound_current() checks the original gateway, then composes the fresh
        // source/grant/config checks once. No native observation is reused here.
        self.bound_current().await?;
        native.check()
    }
    async fn current(&self) -> Result<()> {
        self.bound_current().await
    }
}
struct RevokeOnExit(Arc<Scope>);
impl Drop for RevokeOnExit {
    fn drop(&mut self) {
        self.0.revoke();
    }
}

enum Io {
    Unattached,
    Unknown,
    Attached(Box<pipes::Pipes>),
    Joined,
}
struct Owned {
    scope: Arc<Scope>,
    fence: Option<GatewayFence>,
    input: Option<GatewayInput>,
    io: Io,
    uncertain_call: bool,
}
impl Owned {
    fn live(&self, stop: &mut oneshot::Receiver<()>) -> Result<()> {
        self.scope.check()?;
        if !matches!(stop.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
            bail!("gateway original owner cancelled");
        }
        Ok(())
    }
    async fn current(&self, stop: &mut oneshot::Receiver<()>) -> Result<()> {
        self.scope.configuration_current().await?;
        self.live(stop)
    }
    async fn start(&mut self, stop: &mut oneshot::Receiver<()>) -> Result<ContainerHandle> {
        self.current(stop).await?;
        let attempt = self
            .fence
            .as_mut()
            .context("gateway original fence lost")?
            .creating(self.scope.owner.lease())?;
        // Cancellation never races CREATE/START/ATTACH. The SAME task receives
        // late outcomes. Only the original absolute deadline may leave an
        // uncertain operation for explicit recovery, with its marker intact.
        let created = match timeout_at(
            self.scope.owner.expires(),
            self.scope
                .owner
                .runtime()
                .create_application_gateway(&self.scope.profile, attempt),
        )
        .await
        {
            Ok(result) => result?,
            Err(error) => {
                self.uncertain_call = true;
                return Err(error).context("original gateway create deadline");
            }
        };
        let handle = match created {
            crate::runtime::gateway_creation::GatewayCreation::Created(handle) => handle,
            crate::runtime::gateway_creation::GatewayCreation::NotAttempted(outcome) => {
                let error = self
                    .fence
                    .as_mut()
                    .context("gateway rejection lost its fence")?
                    .not_attempted(self.scope.owner.lease(), outcome)?;
                return Err(error);
            }
        };
        if handle.name != self.scope.profile.spec().name {
            bail!("gateway create returned another actor");
        }
        self.fence
            .as_mut()
            .context("gateway original fence lost")?
            .bind(self.scope.owner.lease(), &handle.id)?;
        self.current(stop).await?;
        match timeout_at(
            self.scope.owner.expires(),
            self.scope
                .owner
                .runtime()
                .start_application_gateway(&handle, &self.scope.profile),
        )
        .await
        {
            Ok(result) => result?,
            Err(error) => {
                self.uncertain_call = true;
                return Err(error).context("original gateway start deadline");
            }
        }
        self.current(stop).await?;
        self.io = Io::Unknown;
        let process = timeout_at(
            self.scope.owner.expires(),
            self.scope
                .owner
                .runtime()
                .attach_application_gateway(&handle, &self.scope.profile),
        )
        .await
        .context("original gateway attach deadline")??;
        // Retain the actual Child/pipes BEFORE any post-await revocation check.
        self.io = Io::Attached(Box::new(pipes::Pipes::new(process)));
        self.current(stop).await?;
        let input = self
            .input
            .take()
            .context("original gateway material already consumed")?;
        self.write(input.body(), stop).await?;
        // Never let a rotation/revocation during prefix transmission start a
        // stale VPN: the fixed trailer is a separate, freshly checked effect.
        self.current(stop).await?;
        self.write(input.trailer(), stop).await?;
        let mut ack = [0; 36];
        let mut offset = 0;
        let mut tick = Instant::now() + CHECK_INTERVAL;
        while offset < ack.len() {
            self.live(stop)?;
            let progress = {
                let Io::Attached(pipes) = &mut self.io else {
                    bail!("gateway original pipes lost");
                };
                let output = pipes
                    .output
                    .as_mut()
                    .context("gateway original output closed")?;
                tokio::select! {biased;
                    _=&mut *stop=>bail!("gateway cancelled during material acknowledgement"),
                    _=tokio::time::sleep_until(self.scope.owner.expires())=>bail!("gateway original material deadline"),
                    _=tokio::time::sleep_until(tick)=>None,
                    result=output.read(&mut ack[offset..])=>Some(result),
                }
            };
            if let Some(result) = progress {
                let n = result?;
                if n == 0 {
                    bail!("gateway material acknowledgement truncated");
                }
                offset += n;
            } else {
                self.current(stop).await?;
                tick = Instant::now() + CHECK_INTERVAL;
            }
        }
        if ack != MATERIAL_ACCEPTED {
            bail!("gateway private material was not accepted");
        }
        drop(input); // Zeroize normalized credential bytes before publication.
        let Io::Attached(pipes) = &mut self.io else {
            bail!("gateway original pipes lost");
        };
        // Keep the original attach input owned/open for the full lifetime.
        // Closing it can detach Docker's CLI even though Gluetun is running.
        // No further bytes are sent; retirement closes it after native stop.
        pipes.drain_output();
        self.current(stop).await?;
        Ok(handle)
    }
    async fn write(&mut self, bytes: &[u8], stop: &mut oneshot::Receiver<()>) -> Result<()> {
        let mut offset = 0;
        let mut tick = Instant::now() + CHECK_INTERVAL;
        while offset < bytes.len() {
            self.live(stop)?;
            let end = bytes.len().min(offset + 16 * 1024);
            let progress = {
                let Io::Attached(pipes) = &mut self.io else {
                    bail!("gateway original pipes lost");
                };
                let input = pipes
                    .input
                    .as_mut()
                    .context("gateway original input closed")?;
                tokio::select! {biased;
                    _=&mut *stop=>bail!("gateway cancelled during private material input"),
                    _=tokio::time::sleep_until(self.scope.owner.expires())=>bail!("gateway original input deadline"),
                    _=tokio::time::sleep_until(tick)=>None,
                    result=input.write(&bytes[offset..end])=>Some(result),
                }
            };
            if let Some(result) = progress {
                let n = result?;
                if n == 0 {
                    bail!("gateway private input stopped");
                }
                offset += n;
            } else {
                self.current(stop).await?;
                tick = Instant::now() + CHECK_INTERVAL;
            }
        }
        Ok(())
    }
    fn pipe_ended(&mut self) -> bool {
        match &mut self.io {
            Io::Attached(pipes) => pipes.ended(),
            _ => true,
        }
    }
    async fn retire(&mut self) -> Result<()> {
        self.scope.revoke();
        drop(self.input.take());
        let Some(fence) = self.fence.as_mut() else {
            return Ok(());
        };
        // Known native identity can be stopped even while its original pipes
        // need joining. UNKNOWN attachment/deadline outcomes never clear data.
        if let Ok((profile, handle)) = fence.bound(self.scope.owner.lease()) {
            timeout(
                RETIRE_TIMEOUT,
                self.scope
                    .owner
                    .runtime()
                    .stop_application_gateway(&handle, &profile),
            )
            .await
            .context("original gateway stop remains pending")??;
        }
        if self.uncertain_call || matches!(self.io, Io::Unknown) {
            bail!("original gateway operation requires explicit recovery");
        }
        if let Io::Attached(pipes) = &mut self.io {
            timeout(RETIRE_TIMEOUT, pipes.join())
                .await
                .context("original gateway private pipes remain unjoined")??;
            self.io = Io::Joined;
        }
        if !self.scope.owner.can_remove() {
            bail!("warm gateway still retains an unretired HTTP borrower");
        }
        timeout(
            RETIRE_TIMEOUT,
            fence.retire(
                self.scope.owner.runtime().as_ref(),
                self.scope.owner.lease(),
            ),
        )
        .await
        .context("original gateway retirement remains pending")??;
        self.fence = None;
        Ok(())
    }
}

pub(in crate::marketplace) struct GatewayLaunch {
    // Retain the same OS/native owner even if its task panics. No Drop cleanup
    // or failed task may claim that its durable child has retired.
    scope: Arc<Scope>,
    stop: Option<oneshot::Sender<()>>,
    response: Option<oneshot::Receiver<Result<ContainerHandle>>>,
    materialized: Option<ContainerHandle>,
    failed: bool,
    task: Option<JoinHandle<Owned>>,
    completed: Option<Owned>,
    join_failed: bool,
}
impl GatewayLaunch {
    /// Separate bounded tunnel owner. No source/grant/HTTP authority survives
    /// in it; each borrower must supply its own original native invocation.
    pub(in crate::marketplace) async fn begin_warm(
        owner: Arc<crate::marketplace::grant_store::installation::runtime::RuntimeOwner>,
        runtime: Arc<dyn RuntimeManager>,
        database: crate::db::Database,
        secrets: Arc<crate::secrets::SecretsManager>,
        network_id: &str,
        admission: Arc<crate::marketplace::worker::IndependentAdmission>,
        busy: Arc<AtomicBool>,
    ) -> Result<Option<Self>> {
        timeout(AUTHORITY_TIMEOUT, owner.current(&database))
            .await
            .context("warm tunnel lineage deadline")??;
        admission.application_server_current().await?;
        let Some(protection) =
            Protection::capture(database.clone(), secrets, runtime.clone()).await?
        else {
            return Ok(None);
        };
        let lease = owner.lease();
        let deployment = lease.storage_namespace()?;
        let nonce = Digest::of(uuid::Uuid::new_v4().as_bytes());
        let scope = ApplicationGatewayProfile::warm_scope();
        let (profile, input) = protection
            .prepare(&deployment, network_id, scope, nonce)
            .await?;
        let executor = tokio::runtime::Handle::try_current()?;
        let fence = GatewayFence::begin_warm(&owner, &profile, nonce)?;
        // No await after the durable marker: the pool receives this SAME task.
        Ok(Some(Self::begin_owned(
            executor,
            Owner::Warm {
                owner,
                lease,
                database,
                runtime,
                expires: Instant::now() + Duration::from_secs(300),
                admission,
                busy,
            },
            profile,
            input,
            protection,
            fence,
        )))
    }
    pub(in crate::marketplace) fn can_borrow(&self, native: &NativeInvocation) -> bool {
        matches!(&self.scope.owner, Owner::Warm { .. })
            && self.scope.owner.check().is_ok()
            && self.scope.owner.expires() >= native.expires
    }
    pub(in crate::marketplace) async fn launch_borrowed_http(
        &mut self,
        image: &str,
        native: Arc<NativeInvocation>,
        authority: Arc<dyn SdkHttpAuthority>,
        mode: crate::marketplace::runtime::application_http::HttpMode,
    ) -> Result<HttpLaunch> {
        if !self.can_borrow(&native) {
            bail!("warm tunnel cannot cover the new original invocation");
        }
        let handle = self.materialized().await?.clone();
        self.current().await?;
        authority.current_native(&native).await?;
        native.current().await?;
        let combined: Arc<dyn SdkHttpAuthority> = Arc::new(Borrower {
            gateway: self.scope.clone(),
            native: native.clone(),
            authority,
        });
        native
            .launch_application_http_mode(
                self.scope.profile.owner_deployment(),
                image,
                &handle.id,
                combined,
                mode,
            )
            .await
    }
    pub(super) fn begin(
        executor: tokio::runtime::Handle,
        native: Arc<NativeInvocation>,
        profile: ApplicationGatewayProfile,
        input: GatewayInput,
        protection: Protection,
        authority: Arc<dyn SdkHttpAuthority>,
        fence: GatewayFence,
    ) -> Self {
        Self::begin_owned(
            executor,
            Owner::Invocation { native, authority },
            profile,
            input,
            protection,
            fence,
        )
    }
    fn begin_owned(
        executor: tokio::runtime::Handle,
        owner: Owner,
        profile: ApplicationGatewayProfile,
        input: GatewayInput,
        protection: Protection,
        fence: GatewayFence,
    ) -> Self {
        let scope = Arc::new(Scope {
            owner,
            profile: Arc::new(profile),
            protection,
            handle: OnceLock::new(),
            policy: OnceLock::new(),
            closed: AtomicBool::new(false),
            failure: OnceLock::new(),
            observing: AtomicBool::new(false),
            observation: tokio::sync::Mutex::new(None),
        });
        let (stop, mut cancelled) = oneshot::channel();
        let (response, receiver) = oneshot::channel();
        let mut owned = Owned {
            scope: scope.clone(),
            fence: Some(fence),
            input: Some(input),
            io: Io::Unattached,
            uncertain_call: false,
        };
        let original = scope.clone();
        let task = executor.spawn(async move {
            // Panic, startup failure and normal retirement all revoke the SAME
            // retained authority. No caller cancellation or retry reopens it.
            let _revoke = RevokeOnExit(original);
            let result = match owned.start(&mut cancelled).await {
                Ok(handle) => owned.scope.bind(handle),
                Err(error) => Err(error),
            };
            let started = result.is_ok();
            let delivered = response.send(result).is_ok();
            if started && delivered {
                loop {
                    tokio::select! {biased;
                        _=&mut cancelled=>break,
                        _=tokio::time::sleep_until(owned.scope.owner.expires())=>break,
                        _=tokio::time::sleep(owned.scope.owner.interval())=>{
                            if owned.pipe_ended() {
                                owned.scope.fail("private_transport_ended",&anyhow::anyhow!("original gateway private process/reader exited"));break;
                            }
                            if let Err(error)=owned.current(&mut cancelled).await {owned.scope.fail("owner_check",&error);break;}
                            if owned.scope.observing.load(Ordering::SeqCst) {
                                if let Err(error)=owned.scope.observe().await {owned.scope.fail("native_observation",&error);break;}
                            }
                        }
                    }
                }
            }
            let _ = owned.retire().await;
            owned
        });
        Self {
            scope,
            stop: Some(stop),
            response: Some(receiver),
            materialized: None,
            failed: false,
            task: Some(task),
            completed: None,
            join_failed: false,
        }
    }
    /// Fixed-file ACK only, not a protected route. Retain this original owner
    /// through later readiness/admission and all dependent HTTP retirement.
    pub(in crate::marketplace) async fn materialized(&mut self) -> Result<&ContainerHandle> {
        self.scope.check()?;
        if self.stop.is_none()
            || self.failed
            || self.task.as_ref().is_none_or(JoinHandle::is_finished)
        {
            bail!("original gateway startup cancelled or failed");
        }
        if let Some(response) = self.response.as_mut() {
            let result = response.await;
            self.response = None;
            match result {
                Ok(Ok(handle)) => self.materialized = Some(handle),
                Ok(Err(error)) => {
                    self.failed = true;
                    return Err(error);
                }
                Err(_) => {
                    self.failed = true;
                    bail!("original gateway startup task stopped");
                }
            }
        }
        self.scope.check()?;
        self.materialized
            .as_ref()
            .context("gateway has no original accepted-material identity")
    }
    /// Current original owner/configuration/identity, NOT a protected route.
    pub(in crate::marketplace) async fn current(&self) -> Result<()> {
        self.scope.bound_current().await
    }
    /// Independently observed guard, namespace and tunnel addresses. A tunnel
    /// can be UP without Internet connectivity; callers must probe separately.
    pub(in crate::marketplace) async fn observe(
        &self,
    ) -> Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        self.scope.observe().await
    }
    pub(in crate::marketplace) async fn wait_tunnel(
        &self,
    ) -> Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        loop {
            match self.observe().await {
                Ok(observation) => return Ok(observation),
                Err(error) => {
                    // Only wait for the SAME originally started VPN's tunnel.
                    // Never re-create, re-send material, or retry HTTP input.
                    self.scope.check()?;
                    self.current().await?;
                    if Instant::now() + Duration::from_millis(250) >= self.scope.owner.expires() {
                        return Err(error).context("original VPN tunnel did not become available");
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }
    /// Derive the HTTP child's gateway and storage owner from this original
    /// task. No caller-supplied CID or replacement installed authority. Retain
    /// this owner until the HTTP child retires; readiness, purpose consent and
    /// independent application resource/network admission remain separate.
    pub(in crate::marketplace) async fn launch_http(&mut self, image: &str) -> Result<HttpLaunch> {
        self.launch_http_mode(
            image,
            crate::marketplace::runtime::application_http::HttpMode::StreamLarge,
        )
        .await
    }
    pub(in crate::marketplace) async fn launch_http_mode(
        &mut self,
        image: &str,
        mode: crate::marketplace::runtime::application_http::HttpMode,
    ) -> Result<HttpLaunch> {
        let handle = self.materialized().await?.clone();
        self.current().await?;
        let authority: Arc<dyn SdkHttpAuthority> = self.scope.clone();
        self.scope
            .owner
            .native()?
            .launch_application_http_mode(
                self.scope.profile.owner_deployment(),
                image,
                &handle.id,
                authority,
                mode,
            )
            .await
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.scope.revoke();
        drop(self.stop.take());
        if self.join_failed {
            bail!("original gateway task failed without confirmed retirement");
        }
        if let Some(task) = self.task.as_mut() {
            let outcome = task.await;
            self.task = None;
            match outcome {
                Ok(owner) => self.completed = Some(owner),
                Err(error) => {
                    self.join_failed = true;
                    return Err(error).context("original gateway task join failed");
                }
            }
        }
        self.completed
            .as_mut()
            .context("original gateway lifetime owner unavailable")?
            .retire()
            .await
    }
}
/// Fresh per-call authority. The long-lived tunnel alone cannot send input.
struct Borrower {
    gateway: Arc<Scope>,
    native: Arc<NativeInvocation>,
    authority: Arc<dyn SdkHttpAuthority>,
}
#[async_trait::async_trait]
impl SdkHttpAuthority for Borrower {
    async fn current_native(&self, native: &NativeInvocation) -> Result<()> {
        self.native.require_same(native)?;
        self.authority.current_native(native).await?;
        self.gateway.bound_current().await?;
        native.check()
    }
    async fn current(&self) -> Result<()> {
        self.current_native(&self.native).await
    }
}
impl Drop for GatewayLaunch {
    fn drop(&mut self) {
        self.scope.revoke();
        drop(self.stop.take());
    }
}

#[cfg(test)]
mod tests;
