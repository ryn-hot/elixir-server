//! One owned copy, not an abortable caller future. Blocking filesystem work
//! always joins before source cleanup; native export is borrowed and cancellable.
use super::*;
use crate::marketplace::search::Selection;
use std::time::Duration;
use tokio::{
    sync::watch,
    time::{sleep_until, timeout},
};

const STOP_GRACE: Duration = Duration::from_secs(45);
const DISPOSAL_RETRY: Duration = Duration::from_secs(10);
struct Outcome {
    selection: Option<Selection>,
    result: Option<Result<ArchivedSource>>,
}
pub(in crate::marketplace) struct Copy {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Outcome>>,
    outcome: Option<Outcome>,
    join_failed: bool,
}
impl Copy {
    pub(in crate::marketplace) fn begin(
        database: Database,
        source: GrantCheckedQuarantine,
        selection: Option<Selection>,
        path: PathBuf,
        budget: ArchiveBudget,
    ) -> Self {
        Self::begin_with_build(database, source, selection, path, budget, None)
    }
    // The same task owns the authenticated record AND original source until
    // copying/disposal join. A cancelled finish waiter cannot lose the record
    // between a successful copy and the installation-side handoff.
    pub(in crate::marketplace) fn begin_with_build(
        database: Database,
        source: GrantCheckedQuarantine,
        selection: Option<Selection>,
        path: PathBuf,
        budget: ArchiveBudget,
        build: Option<LocalBuildRecord>,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(
            database, source, selection, path, budget, build, stopped,
        ));
        Self {
            stop,
            task: Some(task),
            outcome: None,
            join_failed: false,
        }
    }
    async fn join(&mut self) -> Result<&mut Outcome> {
        if self.join_failed {
            bail!("archive owner cleanup is unconfirmed");
        }
        if self.outcome.is_none() {
            let joined = self
                .task
                .as_mut()
                .context("archive task owner missing")?
                .await;
            self.task = None;
            match joined {
                Ok(outcome) => self.outcome = Some(outcome),
                Err(error) => {
                    self.join_failed = true;
                    return Err(error).context("archive owner stopped before cleanup");
                }
            }
        }
        self.outcome
            .as_mut()
            .context("archive cleanup receipt missing")
    }
    pub(in crate::marketplace) async fn finish(
        &mut self,
    ) -> Result<(Option<Selection>, ArchivedSource)> {
        let outcome = self.join().await?;
        let result = outcome
            .result
            .take()
            .context("archive result already consumed")??;
        Ok((outcome.selection.take(), result))
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.stop.send_replace(true);
        timeout(STOP_GRACE, self.join())
            .await
            .context("archive cleanup is still pending")??;
        Ok(())
    }
}
impl Drop for Copy {
    fn drop(&mut self) {
        // Do not abort the owned task or a blocking write. The actual task and
        // its source/root leases survive a lost HTTP/finish/cancel waiter.
        self.stop.send_replace(true);
    }
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() || stop.changed().await.is_err() {
            return;
        }
    }
}
fn current(stop: &watch::Receiver<bool>, expires: Instant) -> Result<()> {
    if *stop.borrow() {
        bail!("archive copy cancelled");
    }
    active(expires)
}
async fn watch_selection(selection: &mut Option<Selection>) -> Result<()> {
    match selection {
        Some(selection) => selection.watch_current().await,
        None => std::future::pending().await,
    }
}
async fn check(
    source: &mut GrantCheckedQuarantine,
    selection: &mut Option<Selection>,
    database: &Database,
    stop: &mut watch::Receiver<bool>,
    expires: Instant,
) -> Result<()> {
    current(stop, expires)?;
    let checking = async {
        if let Some(selection) = selection {
            selection.maintain().await?;
            source.refresh_public_source(selection.public_source()?)?;
        }
        source.ensure_current(database).await
    };
    tokio::select! {biased;
        _=stopped(stop)=>bail!("archive copy cancelled"),
        _=sleep_until(expires)=>bail!("source archive job deadline exceeded"),
        result=checking=>result,
    }
}
struct Copied {
    archive: ArchivedSource,
    _root: Arc<Root>,
}
// Private scheduling seam, not a runtime or source capability constructor.
#[async_trait::async_trait]
trait Disposal: Send {
    async fn discard(&mut self) -> Result<()>;
}
#[async_trait::async_trait]
impl Disposal for GrantCheckedQuarantine {
    async fn discard(&mut self) -> Result<()> {
        self.discard_retained().await
    }
}
async fn confirm_disposal(source: &mut impl Disposal) {
    let mut reported = false;
    loop {
        match source.discard().await {
            Ok(()) => return,
            Err(error) => {
                if !reported {
                    tracing::warn!(error = %error, "archive source cleanup pending; retaining its owner and lane");
                    reported = true;
                }
                // A failed caller/shutdown wait cannot release this source or
                // fabricate cleanup. Retry only ownership-checked disposal,
                // never native work, with one task and no growing history.
                tokio::time::sleep(DISPOSAL_RETRY).await;
            }
        }
    }
}
async fn run(
    database: Database,
    mut source: GrantCheckedQuarantine,
    mut selection: Option<Selection>,
    path: PathBuf,
    budget: ArchiveBudget,
    build: Option<LocalBuildRecord>,
    mut stop: watch::Receiver<bool>,
) -> Outcome {
    let expires = source.source().source().expires();
    let result = copy(
        &database,
        &mut source,
        &mut selection,
        path,
        budget,
        &mut stop,
        expires,
    )
    .await;
    // Copy never drops a pending blocking-I/O handle. Even after cancellation
    // or the work deadline, disposal must join the actual native owner.
    // Keep the owning task itself alive after disposal errors. Returning a
    // source inside Outcome is insufficient: a dropped failed preparation
    // would drop that outcome and its real OS lease too.
    confirm_disposal(&mut source).await;
    let result = result.and_then(|mut copied| {
        current(&stop, expires)?;
        if let Some(record) = build {
            record.require_descriptor_data(
                &SourceMatchDescriptor {
                    schema_version: 1,
                    workspace: copied.archive.descriptor.workspace.clone(),
                    release: copied.archive.descriptor.release.clone(),
                },
                &copied.archive.descriptor.source,
            )?;
            copied.archive.build = Some(record);
        }
        Ok(copied.archive)
    });
    Outcome {
        selection,
        result: Some(result),
    }
}
async fn copy(
    database: &Database,
    source: &mut GrantCheckedQuarantine,
    selection: &mut Option<Selection>,
    path: PathBuf,
    budget: ArchiveBudget,
    stop: &mut watch::Receiver<bool>,
    expires: Instant,
) -> Result<Copied> {
    budget.validate()?;
    check(source, selection, database, stop, expires).await?;
    let matched = source.source().source();
    let runtime = matched.runtime();
    let mut descriptor = Descriptor {
        schema_version: 1,
        workspace: matched.workspace().clone(),
        release: matched.release().clone(),
        source: matched.summary().clone(),
    };
    descriptor.workspace.timeout_seconds = elixir_marketplace::quarantine::MAX_JOB_SECONDS;
    descriptor.validate()?;
    let source_job = matched.job_lease();
    let blob_bytes = descriptor
        .workspace
        .descriptors()?
        .iter()
        .map(|slot| slot.blob.size + 1)
        .sum::<u64>();
    let reads = budget.metadata_read_allowance()? + 2 * blob_bytes;
    source.source_mut().source_mut().reserve_archive(
        descriptor.reservation()? + ROOT_RESERVATION,
        reads,
        canonical::to_vec(&descriptor)?.len() as u64,
    )?;
    // No outer timeout/abort around these joins. A hung filesystem cannot be
    // declared stopped; its task/root/source leases keep the lane fenced.
    let store = tokio::task::spawn_blocking(move || {
        Store::open_with_job(&path, budget, expires, Some(source_job))
    })
    .await??;
    check(source, selection, database, stop, expires).await?;
    let root = store.root.clone();
    let recovered = store.recovered;
    let work = descriptor.clone();
    let (store, needed) = tokio::task::spawn_blocking(move || {
        let needed = store.begin(&work, expires)?;
        Ok::<_, anyhow::Error>((store, needed))
    })
    .await??;
    if needed {
        for slot in descriptor.workspace.descriptors()? {
            check(source, selection, database, stop, expires).await?;
            let target = store
                .releases()
                .join(PENDING)
                .join(slot.purpose.file_name());
            let held = root.clone();
            let file = tokio::task::spawn_blocking(move || {
                let _held = held;
                active(expires)?;
                create_file(&target)
            })
            .await??;
            let mut sink = ArchiveSink::new(file, slot.blob.size, root.clone(), expires);
            loop {
                let transfer = source.source_mut().source_mut().export_blob(
                    runtime.as_ref(),
                    slot.purpose,
                    &mut sink,
                );
                let result = tokio::select! {biased;
                    _=stopped(stop)=>Err(anyhow::anyhow!("archive copy cancelled")),
                    _=sleep_until(expires)=>Err(anyhow::anyhow!("source archive job deadline exceeded")),
                    result=watch_selection(selection)=>result.and_then(|()|Err(anyhow::anyhow!("archive authority watcher stopped"))),
                    result=transfer=>result,
                };
                let drained = sink.drain().await;
                if result.as_ref().err().is_some_and(
                    crate::marketplace::worker::ParticipationClient::source_resources_deferred,
                ) {
                    drained?;
                    // Retain this source reservation, sink, lane and deadline.
                    check(source, selection, database, stop, expires).await?;
                    tokio::select! {biased;
                        _=stopped(stop)=>bail!("archive copy cancelled"),
                        _=sleep_until(expires)=>bail!("source archive job deadline exceeded"),
                        result=watch_selection(selection)=>{result?;bail!("archive authority watcher stopped");}
                        _=tokio::time::sleep(Duration::from_secs(10))=>{}
                    }
                    continue;
                }
                result?;
                drained?;
                break;
            }
            sink.finish().await?;
        }
        check(source, selection, database, stop, expires).await?;
        let work = descriptor.clone();
        tokio::task::spawn_blocking(move || store.commit(&work, expires)).await??;
    }
    check(source, selection, database, stop, expires).await?;
    verify_committed(&root, &descriptor, expires).await?;
    check(source, selection, database, stop, expires).await?;
    Ok(Copied {
        archive: ArchivedSource {
            descriptor,
            recovered_incomplete_copy: recovered,
            build: None,
        },
        _root: root,
    })
}

#[cfg(test)]
mod tests {
    // Private scheduling fakes, not source/authority/installed-byte evidence.
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailedDisposal {
        _lease: ControllerLease,
        recovered: watch::Receiver<bool>,
        attempts: Arc<AtomicUsize>,
        attempted: Option<tokio::sync::oneshot::Sender<()>>,
    }
    #[async_trait::async_trait]
    impl Disposal for FailedDisposal {
        async fn discard(&mut self) -> Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if let Some(attempted) = self.attempted.take() {
                let _ = attempted.send(());
            }
            if !*self.recovered.borrow() {
                bail!("fake ownership-checked disposal unavailable");
            }
            Ok(())
        }
    }
    #[tokio::test(start_paused = true)]
    async fn failed_disposal_keeps_the_real_lane_after_lost_waiters_and_dropped_copy_until_recovery()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("foreground");
        let attempts = Arc::new(AtomicUsize::new(0));
        let (recovery, recovered) = watch::channel(false);
        let (attempted, first_attempt) = tokio::sync::oneshot::channel();
        let mut source = FailedDisposal {
            _lease: ControllerLease::acquire(&directory)?,
            recovered,
            attempts: attempts.clone(),
            attempted: Some(attempted),
        };
        let (stop, _) = watch::channel(false);
        let (done, completed) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            confirm_disposal(&mut source).await;
            drop(source);
            let _ = done.send(());
            Outcome {
                selection: None,
                result: Some(Err(anyhow::anyhow!("fake copy failed before disposal"))),
            }
        });
        let mut copy = Copy {
            stop,
            task: Some(task),
            outcome: None,
            join_failed: false,
        };
        first_attempt.await?;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(
            timeout(Duration::from_millis(1), copy.finish())
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(1), copy.cancel())
                .await
                .is_err()
        );
        assert!(ControllerLease::acquire(&directory).is_err());
        drop(copy);
        assert!(ControllerLease::acquire(&directory).is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "no busy cleanup retry");
        recovery.send_replace(true);
        tokio::time::advance(DISPOSAL_RETRY).await;
        completed.await?;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(ControllerLease::acquire(&directory).is_ok());
        Ok(())
    }
    #[tokio::test]
    async fn cancelled_copy_join_keeps_the_stored_handle_and_global_lane_until_actual_completion()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("foreground");
        let lease = ControllerLease::acquire(&directory)?;
        let (ready, started) = tokio::sync::oneshot::channel();
        let (finish, finishing) = tokio::sync::oneshot::channel();
        let (stop, _) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _held = lease;
            let _ = ready.send(());
            let _ = finishing.await;
            Outcome {
                selection: None,
                result: Some(Err(anyhow::anyhow!("fake copy stopped"))),
            }
        });
        let mut copy = Copy {
            stop,
            task: Some(task),
            outcome: None,
            join_failed: false,
        };
        started.await?;
        assert!(
            timeout(Duration::from_millis(10), copy.cancel())
                .await
                .is_err()
        );
        assert!(ControllerLease::acquire(&directory).is_err());
        finish.send(()).ok();
        copy.cancel().await?;
        copy.cancel().await?;
        assert!(copy.finish().await.is_err());
        assert!(ControllerLease::acquire(&directory).is_ok());
        Ok(())
    }
    #[tokio::test]
    async fn dropped_copy_signals_stop_without_aborting_its_cleanup_or_freeing_the_lane()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("foreground");
        let lease = ControllerLease::acquire(&directory)?;
        let (stop, mut stopped_signal) = watch::channel(false);
        let (cleaning, cleanup) = tokio::sync::oneshot::channel();
        let (finish, finishing) = tokio::sync::oneshot::channel();
        let (done, completed) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let held = lease;
            stopped(&mut stopped_signal).await;
            let _ = cleaning.send(());
            let _ = finishing.await;
            drop(held);
            let _ = done.send(());
            Outcome {
                selection: None,
                result: Some(Err(anyhow::anyhow!("fake copy stopped"))),
            }
        });
        drop(Copy {
            stop,
            task: Some(task),
            outcome: None,
            join_failed: false,
        });
        cleanup.await?;
        assert!(ControllerLease::acquire(&directory).is_err());
        finish.send(()).ok();
        completed.await?;
        assert!(ControllerLease::acquire(&directory).is_ok());
        Ok(())
    }
}
