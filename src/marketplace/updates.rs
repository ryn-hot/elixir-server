//! One startup-owned automatic-update scheduler. O(1) cursor/job state, one
//! exact-identity lookup at a time, no new catalog/packet/VM allocation. Saved
//! consent schedules checks; every effect still needs sealed fresh authority.
use super::{
    application::{Allocation, Host},
    grant_store::installation::automatic::{Authorization, NoNewStableRelease},
    grant_store::installation::runtime::InstalledRuntime,
    search::{SearchPhase, SearchService},
    service::MarketplaceService,
};
use crate::{db::Database, secrets::SecretsManager};
use anyhow::{Context, Result, bail};
use elixir_marketplace::identity::ExtensionId;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use uuid::Uuid;

const CYCLE: u64 = 6 * 60 * 60;
const MAX_TARGETS: i64 = 4096;
const CHECK_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Default, Clone)]
pub(super) struct Service(Arc<Mutex<Option<Arc<Entry>>>>);
struct Entry {
    stop: watch::Sender<bool>,
    task: AsyncMutex<OwnedTask>,
    search: Arc<SearchService>,
    diagnostics: Arc<Mutex<Diagnostics>>,
}
#[derive(Default)]
struct Diagnostics {
    checks: u64,
    deferrals: u64,
    last_outcome: Option<&'static str>,
    operation: Option<Uuid>,
    failure: Option<Arc<anyhow::Error>>,
}
enum Outcome {
    Complete(Option<Uuid>),
    Deferred(&'static str),
}
impl Diagnostics {
    fn record(&mut self, outcome: Result<Outcome>) {
        self.checks = self.checks.saturating_add(1);
        self.operation = None;
        self.last_outcome = Some(match outcome {
            Ok(Outcome::Complete(operation)) => {
                self.failure = None;
                self.operation = operation;
                if operation.is_some() {
                    "started"
                } else {
                    "no_update"
                }
            }
            Ok(Outcome::Deferred(reason)) => {
                self.deferrals = self.deferrals.saturating_add(1);
                reason
            }
            Err(error) => {
                self.deferrals = self.deferrals.saturating_add(1);
                // Keep ONE original cause until an actual completed check.
                // Later empty/deferred retries must not erase the failure
                // that may explain why the candidate disappeared. No history.
                if self.failure.is_none() {
                    self.failure = Some(Arc::new(error));
                }
                "check_failed"
            }
        });
    }
}
#[derive(Default)]
struct Retry {
    consecutive: u8,
}
impl Retry {
    // Two short retries of the same target, then move on fairly. Persistent
    // unavailability backs off to at most a five-minute scan delay, not a
    // 30-minute blind spot after the first transient failure. No target map.
    fn after(&mut self, complete: bool, normal: Duration) -> (bool, Duration) {
        if complete {
            self.consecutive = 0;
            return (true, normal);
        }
        if self.consecutive < 2 {
            let delay = Duration::from_secs(30 << self.consecutive);
            self.consecutive += 1;
            (false, delay)
        } else {
            self.consecutive = 0;
            (true, normal.min(Duration::from_secs(300)))
        }
    }
}
struct OwnedTask {
    handle: Option<JoinHandle<Result<()>>>,
    failed: bool,
}

impl Service {
    pub(super) fn begin(
        &self,
        marketplace: MarketplaceService,
        database: Database,
        secrets: SecretsManager,
        runtime: InstalledRuntime,
        allocation: Allocation,
    ) -> Result<()> {
        allocation.validate()?;
        let _ = Host::from_runtime(
            database.clone(),
            secrets.clone(),
            runtime.clone(),
            allocation.installations,
            allocation.execution.clone(),
        )?;
        let mut slot = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("update owner unavailable"))?;
        if slot.is_some() {
            bail!("automatic-update scheduler already owned")
        }
        let executor =
            tokio::runtime::Handle::try_current().context("update executor unavailable")?;
        let (stop, stopped) = watch::channel(false);
        let search = Arc::new(SearchService::default());
        let diagnostics = Arc::new(Mutex::new(Diagnostics::default()));
        let task = executor.spawn(run(
            marketplace,
            database,
            secrets,
            runtime,
            allocation,
            stopped,
            search.clone(),
            diagnostics.clone(),
        ));
        *slot = Some(Arc::new(Entry {
            stop,
            task: AsyncMutex::new(OwnedTask {
                handle: Some(task),
                failed: false,
            }),
            search,
            diagnostics,
        }));
        Ok(())
    }
    pub(super) fn request_stop(&self) {
        let slot = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = slot.as_ref() {
            entry.stop.send_replace(true);
        }
    }
    #[cfg(test)]
    pub(super) fn fixture_diagnostics(&self) -> Result<serde_json::Value> {
        let slot = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("update owner unavailable"))?;
        let Some(entry) = slot.as_ref() else {
            return Ok(serde_json::Value::Null);
        };
        let d = entry
            .diagnostics
            .lock()
            .map_err(|_| anyhow::anyhow!("update diagnostics unavailable"))?;
        Ok(
            serde_json::json!({"checks":d.checks,"deferrals":d.deferrals,
            "last_outcome":d.last_outcome,"operation":d.operation,
            "original_failure_retained":d.failure.is_some()}),
        )
    }
    #[cfg(test)]
    pub(super) fn fixture_failure(&self) -> Result<Option<Arc<anyhow::Error>>> {
        let slot = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("update owner unavailable"))?;
        let Some(entry) = slot.as_ref() else {
            return Ok(None);
        };
        let failure = entry
            .diagnostics
            .lock()
            .map_err(|_| anyhow::anyhow!("update diagnostics unavailable"))?
            .failure
            .clone();
        Ok(failure)
    }
    pub(super) async fn shutdown(&self) -> Result<()> {
        self.request_stop();
        let entry = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("update owner unavailable"))?
            .clone();
        let Some(entry) = entry else { return Ok(()) };
        let mut owned = entry.task.lock().await;
        if let Some(task) = owned.handle.as_mut() {
            let terminal = timeout(Duration::from_secs(85), &mut *task)
                .await
                .context("automatic-update retirement pending; original task retained")?;
            owned.handle = None;
            owned.failed = !matches!(terminal, Ok(Ok(())));
        }
        // Retain the lookup OUTSIDE the fallible scheduler future as well.
        // Panic or cancelled wait cannot detach the original retirement handle.
        entry.search.request_stop();
        entry.search.join().await?;
        if owned.failed {
            bail!("automatic-update owner did not confirm retirement")
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Target {
    extension: ExtensionId,
    instance: Uuid,
    home: Uuid,
}
type Cursor = (String, String);
fn cadence(count: i64) -> Result<Duration> {
    if !(0..=MAX_TARGETS).contains(&count) {
        bail!("automatic-update target bound exceeded")
    }
    Ok(Duration::from_secs(
        (CYCLE / (count.max(1) as u64)).clamp(5, 30 * 60),
    ))
}
async fn next_target(
    database: &Database,
    after: Option<&Cursor>,
) -> Result<(Option<Target>, Duration)> {
    const FROM: &str = "FROM marketplace_installations m JOIN marketplace_extension_grants g ON g.extension_id=m.extension_id AND g.home_id=m.home_id JOIN extensions e ON e.extension_id=m.extension_id JOIN extension_instances i ON i.instance_id=m.instance_id AND i.extension_id=m.extension_id WHERE m.instance_id IS NOT NULL AND g.enabled=TRUE AND g.automatic_updates=TRUE AND e.enabled=TRUE AND i.enabled=TRUE";
    let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) {FROM}"))
        .fetch_one(&database.pool)
        .await?;
    let delay = cadence(count)?;
    let (extension, instance) = after
        .map(|v| (v.0.as_str(), v.1.as_str()))
        .unwrap_or(("", ""));
    let row: Option<(String,String,String)> = sqlx::query_as(&format!("SELECT SUBSTR(m.extension_id,1,129),SUBSTR(m.instance_id,1,37),SUBSTR(m.home_id,1,37) {FROM} AND (m.extension_id>$1 OR (m.extension_id=$1 AND m.instance_id>$2)) ORDER BY m.extension_id,m.instance_id LIMIT 1"))
        .bind(extension).bind(instance).fetch_optional(&database.pool).await?;
    Ok((
        row.map(|(extension, instance, home)| -> Result<Target> {
            Ok(Target {
                extension: extension.parse()?,
                instance: Uuid::parse_str(&instance)?,
                home: Uuid::parse_str(&home)?,
            })
        })
        .transpose()?,
        delay,
    ))
}
async fn wait(stopped: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    if *stopped.borrow() || stopped.has_changed().is_err() {
        return false;
    }
    tokio::select! { biased; _=stopped.changed()=>false, _=sleep(delay)=>true }
}

async fn attempt(
    marketplace: &MarketplaceService,
    search: &Arc<SearchService>,
    database: &Database,
    secrets: &SecretsManager,
    runtime: &InstalledRuntime,
    allocation: &Allocation,
    target: &Target,
    stopped: &watch::Receiver<bool>,
) -> Result<Outcome> {
    let client = marketplace.client()?;
    let query = search.start_update(client.clone(), &target.extension)?;
    let deadline = Instant::now() + CHECK_TIMEOUT;
    loop {
        if *stopped.borrow() || marketplace.stop_requested() {
            return Ok(Outcome::Complete(None));
        }
        let view = search.view(query).await?;
        match view.phase {
            SearchPhase::Checking if Instant::now() < deadline => {
                sleep(Duration::from_millis(500)).await
            }
            SearchPhase::Ready => {
                let Some(card) = view
                    .results
                    .iter()
                    .find(|card| card.extension_id == target.extension)
                else {
                    return Ok(Outcome::Deferred("no_admitted_remote_candidate"));
                };
                if !marketplace.automatic_update_idle()? || runtime.playback.startup_queue_len() > 0
                {
                    return Ok(Outcome::Deferred("installed_activity"));
                }
                let selected = search
                    .select(&client, query, &target.extension, card.release_id)
                    .await
                    .context("automatic-update fresh source selection")?;
                let authorization = match Authorization::capture(
                    database,
                    target.home,
                    target.instance,
                    selected.discovery_metadata()?,
                    allocation.installations,
                )
                .await
                {
                    Ok(value) => value,
                    Err(error) if error.downcast_ref::<NoNewStableRelease>().is_some() => {
                        return Ok(Outcome::Complete(None));
                    }
                    Err(error) => {
                        return Err(error.context("automatic-update standing authorization"));
                    }
                };
                let action = authorization
                    .action(database)
                    .await
                    .context("automatic-update original target action")?;
                let host = Host::from_runtime(
                    database.clone(),
                    secrets.clone(),
                    runtime.clone(),
                    allocation.installations,
                    allocation.execution.clone(),
                )
                .context("automatic-update native host")?;
                let operation = marketplace
                    .start_automatic_install(
                        query,
                        selected,
                        authorization,
                        action,
                        host,
                        search.clone(),
                    )
                    .context("automatic-update original operation start")?;
                return Ok(Outcome::Complete(Some(operation)));
            }
            SearchPhase::Checking => return Ok(Outcome::Deferred("lookup_deadline")),
            _ => return Ok(Outcome::Deferred("lookup_unavailable")),
        }
    }
}

async fn run(
    marketplace: MarketplaceService,
    database: Database,
    secrets: SecretsManager,
    runtime: InstalledRuntime,
    allocation: Allocation,
    mut stopped: watch::Receiver<bool>,
    search: Arc<SearchService>,
    diagnostics: Arc<Mutex<Diagnostics>>,
) -> Result<()> {
    let mut cursor: Option<Cursor> = None;
    let mut retry = Retry::default();
    // Stagger restarts; no enrollment, provider consent or network readiness is
    // inferred from a timer. A missing protected client simply defers this job.
    let mut delay = Duration::from_secs(15 + (Uuid::new_v4().as_u128() % 60) as u64);
    while wait(&mut stopped, delay).await {
        if marketplace.stop_requested() {
            break;
        }
        if !marketplace.automatic_update_idle()? || runtime.playback.startup_queue_len() > 0 {
            delay = Duration::from_secs(30);
            continue;
        }
        let (target, next_delay) = next_target(&database, cursor.as_ref()).await?;
        delay = next_delay;
        let target = match target {
            Some(target) => target,
            None => {
                cursor = None;
                let (target, _) = next_target(&database, None).await?;
                let Some(target) = target else {
                    retry = Retry::default();
                    continue;
                };
                target
            }
        };
        let outcome = attempt(
            &marketplace,
            &search,
            &database,
            &secrets,
            &runtime,
            &allocation,
            &target,
            &stopped,
        )
        .await;
        search.request_stop();
        // Retire the actual SAME lookup before any next poll. Timeout/failure
        // is terminal for this owner, never a reason to launch replacement work.
        search.join().await?;
        if *stopped.borrow() && matches!(outcome, Ok(Outcome::Complete(None))) {
            break;
        }
        let complete = matches!(outcome, Ok(Outcome::Complete(_)));
        let (advance, next) = retry.after(complete, next_delay);
        delay = next;
        if advance {
            cursor = Some((target.extension.to_string(), target.instance.to_string()));
        }
        let mut d = diagnostics
            .lock()
            .map_err(|_| anyhow::anyhow!("update diagnostics unavailable"))?;
        d.record(outcome);
        if !complete {
            tracing::debug!(
                reason = d.last_outcome,
                retry_seconds = delay.as_secs(),
                "automatic-update check deferred; current installed source unchanged"
            );
        }
    }
    search.request_stop();
    search.join().await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deferred_retries_keep_the_first_original_failure_until_a_check_really_completes() {
        let mut d = Diagnostics::default();
        d.record(Err(anyhow::anyhow!("first private failure")));
        let original = d.failure.clone().unwrap();
        d.record(Ok(Outcome::Deferred("no_admitted_remote_candidate")));
        assert!(Arc::ptr_eq(d.failure.as_ref().unwrap(), &original));
        d.record(Err(anyhow::anyhow!("later failure")));
        assert!(Arc::ptr_eq(d.failure.as_ref().unwrap(), &original));
        assert_eq!((d.checks, d.deferrals), (3, 3));
        d.record(Ok(Outcome::Complete(None)));
        assert!(d.failure.is_none());
        assert_eq!(d.last_outcome, Some("no_update"));
        let operation = Uuid::new_v4();
        d.record(Ok(Outcome::Complete(Some(operation))));
        assert_eq!(d.operation, Some(operation));
        assert_eq!((d.checks, d.deferrals), (5, 3));
    }
    #[test]
    fn transient_checks_retry_before_advancing_but_persistent_failure_cannot_starve_the_scan() {
        let mut retry = Retry::default();
        let normal = Duration::from_secs(1800);
        for _ in 0..10_000 {
            assert_eq!(retry.after(false, normal), (false, Duration::from_secs(30)));
            assert_eq!(retry.after(false, normal), (false, Duration::from_secs(60)));
            assert_eq!(retry.after(false, normal), (true, Duration::from_secs(300)));
            assert_eq!(retry.consecutive, 0);
        }
        let fast = Duration::from_secs(5);
        retry.after(false, fast);
        retry.after(false, fast);
        assert_eq!(retry.after(false, fast), (true, fast));
    }
    #[test]
    fn a_completed_check_restores_normal_cadence_and_clears_only_its_retry_counter() {
        let mut retry = Retry::default();
        let normal = Duration::from_secs(1800);
        retry.after(false, normal);
        assert_eq!(retry.after(true, normal), (true, normal));
        assert_eq!(retry.after(false, normal), (false, Duration::from_secs(30)));
        assert_eq!(CHECK_TIMEOUT, Duration::from_secs(300));
    }
    #[tokio::test(start_paused = true)]
    async fn cancellation_interrupts_the_original_backoff_without_starting_a_replacement() {
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move { wait(&mut stopped, Duration::from_secs(1800)).await });
        tokio::task::yield_now().await;
        stop.send_replace(true);
        assert!(!task.await.unwrap());
    }
    #[test]
    fn original_update_failure_is_retained_privately_and_closed_status_has_no_error_payload()
    -> Result<()> {
        let service = Service::default();
        let (stop, _) = watch::channel(false);
        let failure = Arc::new(anyhow::anyhow!("private original failure"));
        let diagnostics = Arc::new(Mutex::new(Diagnostics {
            checks: 1,
            deferrals: 1,
            last_outcome: Some("check_failed"),
            operation: None,
            failure: Some(failure.clone()),
        }));
        *service.0.lock().unwrap() = Some(Arc::new(Entry {
            stop,
            task: AsyncMutex::new(OwnedTask {
                handle: None,
                failed: false,
            }),
            search: Arc::new(SearchService::default()),
            diagnostics,
        }));
        assert!(Arc::ptr_eq(&failure, &service.fixture_failure()?.unwrap()));
        let view = service.fixture_diagnostics()?;
        assert_eq!(view["last_outcome"], "check_failed");
        assert_eq!(view["original_failure_retained"], true);
        assert!(!view.to_string().contains("private original failure"));
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn shutdown_timeout_retains_the_original_task_instead_of_detaching_it() -> Result<()> {
        let service = Service::default();
        let (stop, _) = watch::channel(false);
        let task = tokio::spawn(async {
            sleep(Duration::from_secs(100)).await;
            Ok(())
        });
        let entry = Arc::new(Entry {
            stop,
            task: AsyncMutex::new(OwnedTask {
                handle: Some(task),
                failed: false,
            }),
            search: Arc::new(SearchService::default()),
            diagnostics: Arc::new(Mutex::new(Diagnostics::default())),
        });
        *service.0.lock().unwrap() = Some(entry.clone());
        assert!(service.shutdown().await.is_err());
        assert!(entry.task.lock().await.handle.is_some());
        tokio::time::advance(Duration::from_secs(16)).await;
        service.shutdown().await?;
        assert!(entry.task.lock().await.handle.is_none());
        Ok(())
    }
    #[test]
    fn update_scan_is_bounded_and_does_not_grow_state_or_poll_every_target_each_tick() -> Result<()>
    {
        assert_eq!(cadence(0)?, Duration::from_secs(1800));
        assert_eq!(cadence(1)?, Duration::from_secs(1800));
        assert_eq!(cadence(100)?, Duration::from_secs(216));
        assert_eq!(cadence(4096)?, Duration::from_secs(5));
        assert!(cadence(4097).is_err());
        assert!(cadence(-1).is_err());
        Ok(())
    }
}
