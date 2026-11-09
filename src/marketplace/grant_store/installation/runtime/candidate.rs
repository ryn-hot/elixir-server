//! Original claim -> owned native lifecycle -> confirmed workspace retirement.
//! No constructor from phase/status, and no activation or failure-discard API.
use super::super::activation::candidate::{CandidateBinding, CandidateClaim};
use super::*;
use crate::marketplace::quarantine::execution::CandidateBroker as CandidateTransport;
use elixir_marketplace::source_execution::SourceExecutionMode;

pub(super) fn validate(
    configuration: &InstalledRuntime,
    limits: &SourceExecutionLimits,
) -> Result<()> {
    configuration.retention.validate()?;
    limits.validate_for(SourceExecutionMode::Lifecycle)?;
    if configuration.source_budget.wall_seconds == 0
        || configuration.source_budget.wall_seconds
            > elixir_marketplace::quarantine::MAX_JOB_SECONDS
        || limits.minimum_memory_bytes_for(SourceExecutionMode::Lifecycle)?
            > configuration.source_budget.memory_bytes
    {
        bail!("candidate exceeds its finite source-job reservation");
    }
    Ok(())
}

pub(super) struct CandidateCall {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<RetiredCandidate>>,
    result: Option<RetiredCandidate>,
    join_failed: bool,
}
impl CandidateCall {
    pub(super) fn start(
        executor: tokio::runtime::Handle,
        database: Database,
        secrets: SecretsManager,
        claim: CandidateClaim,
        configuration: InstalledRuntime,
        limits: SourceExecutionLimits,
        owner: Arc<RuntimeOwner>,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let control = RunControl {
            stop: stopped,
            owner: Some(owner.lease()),
        };
        let task = executor.spawn(run_candidate(
            database,
            secrets,
            claim,
            configuration,
            limits,
            control,
            owner,
        ));
        Self {
            stop,
            task: Some(task),
            result: None,
            join_failed: false,
        }
    }
    pub(super) fn signal_stop(&self) {
        self.stop.send_replace(true);
    }
    pub(super) async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("candidate original task stopped before confirmed retirement");
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            match result {
                Ok(retired) => self.result = Some(retired),
                Err(error) => {
                    self.join_failed = true;
                    return Err(error).context("candidate original cleanup owner stopped");
                }
            }
        }
        Ok(())
    }
    pub(super) fn take(&mut self) -> Result<RetiredCandidate> {
        if self.join_failed || self.task.is_some() {
            bail!("candidate retirement is not joined");
        }
        self.result
            .take()
            .context("candidate retirement already consumed")
    }
    pub(super) async fn cancel(&mut self) -> Result<()> {
        self.signal_stop();
        timeout(STOP_GRACE, self.join())
            .await
            .context("candidate cleanup is still pending")??;
        Ok(())
    }
}
impl Drop for CandidateCall {
    fn drop(&mut self) {
        self.signal_stop();
    }
}

/// Constructed only after this original task has positively disposed of ALL
/// native/partial-start work; exposed only after joining that task. A normal
/// migration error still retains this result for separately authorized cleanup.
/// No Clone/Deserialize or owned-claim extraction (which could enable a replay).
pub(in crate::marketplace) struct RetiredCandidate {
    claim: CandidateClaim,
    result: Result<()>,
    owner: Arc<RuntimeOwner>,
    database: Database,
    secrets: SecretsManager,
}
impl RetiredCandidate {
    pub(in crate::marketplace) fn migration_result(&self) -> Result<(), &anyhow::Error> {
        self.result.as_ref().map(|_| ())
    }
    pub(in crate::marketplace::grant_store::installation) fn binding(&self) -> &CandidateBinding {
        self.claim.binding()
    }
    pub(in crate::marketplace::grant_store::installation) fn database(&self) -> &Database {
        &self.database
    }
    pub(in crate::marketplace::grant_store::installation) fn secrets(&self) -> &SecretsManager {
        &self.secrets
    }
    pub(in crate::marketplace::grant_store::installation) fn capsule(&self) -> &Capsule {
        self.claim.capsule()
    }
    pub(in crate::marketplace::grant_store::installation) async fn require_owner(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        self.owner.require(tx, driver).await
    }
}

struct CandidateAccess<'a> {
    claim: &'a CandidateClaim,
    owner: &'a RuntimeOwner,
}
#[async_trait::async_trait]
impl Access for CandidateAccess<'_> {
    fn review(&self) -> &GrantReview {
        self.claim.binding().review()
    }
    async fn current(&self, database: &Database) -> Result<()> {
        self.owner.check()?;
        let now = Utc::now();
        let started = Instant::now();
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let (_, valid) = self
            .claim
            .binding()
            .require_in_transaction(&mut tx, database.driver, now)
            .await?;
        self.owner.require(&mut tx, database.driver).await?;
        ensure_before_expiry(valid, now, started)?;
        tx.rollback().await?;
        self.owner.check()
    }
}

struct LifecycleBroker<'a> {
    database: &'a Database,
    secrets: &'a SecretsManager,
    access: CandidateAccess<'a>,
    descriptor: SourceMatchDescriptor,
    native: Option<NativeInvocation>,
    stop: RunControl,
    expires: Deadline,
    checked: Option<Deadline>,
}
impl LifecycleBroker<'_> {
    fn check(&self) -> Result<()> {
        restrictions(&self.stop, self.expires)?;
        self.access.owner.check()?;
        if let Some(native) = &self.native {
            native.check()?;
        }
        Ok(())
    }
}
impl CandidateTransport for LifecycleBroker<'_> {}
#[async_trait::async_trait]
impl Broker for LifecycleBroker<'_> {
    fn native_started(&mut self, invocation: NativeInvocation) -> Result<()> {
        self.check()?;
        if self.native.is_some() {
            bail!("candidate already owns its lifecycle executor");
        }
        invocation.require_candidate(&self.descriptor, self.access.claim.binding().execution()?)?;
        self.native = Some(invocation);
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        self.check()?;
        if self.checked.is_none_or(|last| last.elapsed() >= CHECK) {
            self.access.current(self.database).await?;
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
            .context("candidate SDK requires its original lifecycle executor")?;
        super::super::super::sdk::CandidateScope {
            database: self.database,
            secrets: self.secrets,
            binding: self.access.claim.binding(),
        }
        .execute(raw, maximum, native, self.access.owner, || self.check())
        .await
    }
}

async fn run_candidate(
    database: Database,
    secrets: SecretsManager,
    claim: CandidateClaim,
    configuration: InstalledRuntime,
    limits: SourceExecutionLimits,
    mut stop: RunControl,
    owner: Arc<RuntimeOwner>,
) -> RetiredCandidate {
    let expires = Deadline::now() + Duration::from_secs(configuration.source_budget.wall_seconds);
    let mut workspace = Owner::default();
    let result = async {
        let access = CandidateAccess {
            claim: &claim,
            owner: &owner,
        };
        restrictions(&stop, expires)?;
        access.current(&database).await?;
        claim.require_configuration(&configuration, &secrets)?;
        load_retained(
            &mut workspace,
            &database,
            &access,
            &configuration,
            claim.capsule(),
            &mut stop,
            expires,
            elixir_marketplace::source_execution::SourceExecutionMode::Lifecycle,
            &secrets,
        )
        .await?;
        let Stage::Executing(code) = &mut workspace.0 else {
            unreachable!()
        };
        let mut broker = LifecycleBroker {
            database: &database,
            secrets: &secrets,
            access,
            descriptor: code.descriptor(),
            native: None,
            stop: stop.clone(),
            expires,
            checked: None,
        };
        loop {
            broker.checked = None;
            match cancellable(
                &mut stop,
                expires,
                code.lifecycle(
                    limits.clone(),
                    claim.context(),
                    claim.binding().execution()?,
                    &mut broker,
                ),
            )
            .await
            {
                Ok(()) => break Ok(()),
                Err(error) => {
                    // Only typed pre-native admission waits can retry. Native
                    // Ready (even a caught SDK failure) makes execution final.
                    if broker.native.is_some() {
                        return Err(error);
                    }
                    resource_pause(&database, &broker.access, &mut stop, expires, error).await?;
                }
            }
        }
    }
    .await;
    workspace.confirm_discard().await;
    // Normal failures ALSO reach confirmed retirement, with the claim intact.
    // Uncertain/panicked original tasks never construct this result.
    let result = match result {
        Ok(()) => {
            async {
                restrictions(&stop, expires)?;
                CandidateAccess {
                    claim: &claim,
                    owner: &owner,
                }
                .current(&database)
                .await?;
                restrictions(&stop, expires)
            }
            .await
        }
        Err(error) => Err(error),
    };
    RetiredCandidate {
        claim,
        result,
        owner,
        database,
        secrets,
    }
}

#[cfg(test)]
mod tests;
