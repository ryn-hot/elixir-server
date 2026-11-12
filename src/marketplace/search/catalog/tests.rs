//! Scheduling/data-bound tests with synthetic inventory/warm callbacks. The
//! ordinary first-page fixture verifies signatures locally, not native/VPN IO.
use super::*;
use crate::marketplace::search::tests::{Fake, descriptor, ready as finished, request};
use elixir_marketplace::catalog::{InventoryEntry, InventoryPage};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

struct Fixture {
    checks: Fake,
    template: CandidateDescriptor,
    count: usize,
    pages: AtomicUsize,
    warmed: AtomicUsize,
    metadata_pages: AtomicUsize,
    available: AtomicBool,
    hold: AtomicBool,
    started: Notify,
    resume: Notify,
    broken: u8,
    empty_until_prepared: bool,
    fail_early_admit: bool,
    hold_after_warmed: Option<usize>,
    hold_reset: bool,
    resets: AtomicUsize,
    browses: AtomicUsize,
    stopped: AtomicBool,
    queue_inventory: AtomicBool,
    queue_warm: AtomicBool,
    fail_population: bool,
    fail_reset: bool,
}
impl Fixture {
    fn new(count: usize, broken: u8) -> Result<Self> {
        let d = descriptor(1, 1)?;
        Ok(Self {
            checks: Fake::new(vec![d.clone()]),
            template: d,
            count,
            pages: AtomicUsize::new(0),
            warmed: AtomicUsize::new(0),
            metadata_pages: AtomicUsize::new(0),
            available: AtomicBool::new(true),
            hold: AtomicBool::new(false),
            started: Notify::new(),
            resume: Notify::new(),
            broken,
            empty_until_prepared: false,
            fail_early_admit: false,
            hold_after_warmed: None,
            hold_reset: false,
            resets: AtomicUsize::new(0),
            browses: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            queue_inventory: AtomicBool::new(false),
            queue_warm: AtomicBool::new(false),
            fail_population: false,
            fail_reset: false,
        })
    }
}
#[async_trait]
impl Backend for Fixture {
    async fn populate(&self, _: &Interest, _: Instant, _: &mut EarlyBrowse<'_>) -> Result<()> {
        if self.fail_population {
            bail!("synthetic population failure");
        }
        Ok(())
    }
    async fn warm_metadata(&self, descriptors: &[CandidateDescriptor], _: &Interest) -> Result<()> {
        assert!(descriptors.len() <= 20);
        self.metadata_pages.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn inventory(&self, after: Option<ExtensionId>, _: &Interest) -> Result<InventoryPage> {
        if self.queue_inventory.load(Ordering::SeqCst) {
            self.started.notify_one();
            return Err(ParticipationClient::fixture_queue_busy());
        }
        self.pages.fetch_add(1, Ordering::SeqCst);
        let first = if self.broken == 1 {
            0
        } else {
            after
                .as_ref()
                .map(|id| {
                    id.as_str()
                        .rsplit('-')
                        .next()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                        + 1
                })
                .unwrap_or(0)
        };
        let mut entries = vec![];
        for n in first..self.count.min(first + 20) {
            let mut d = self.template.clone();
            // Deliberately unverified scheduling data. The synthetic warm
            // callback below creates no real source, card or install capability.
            let id = ExtensionId::new(&d.publisher.payload.root_key, &format!("scan-{n:05}"))?;
            d.release.payload.extension_id = id.clone();
            entries.push(InventoryEntry {
                extension_id: id,
                descriptor_id: Digest::of(d.to_bytes()?),
                descriptor: (self.broken != 2).then_some(d),
            });
        }
        let next_after = (first + entries.len() < self.count)
            .then(|| entries.last().unwrap().extension_id.clone());
        if self.broken == 3
            && let Some(entry) = entries.first_mut()
        {
            entry.descriptor_id = Digest::of(b"wrong descriptor");
        }
        Ok(InventoryPage {
            schema_version: 1,
            entries,
            next_after,
            network_complete: self.broken == 4,
            metadata_only: true,
        })
    }
    fn preparation_ready(&self) -> Result<bool> {
        if self.stopped.load(Ordering::SeqCst) {
            bail!("synthetic preparation owner stopped");
        }
        Ok(self.available.load(Ordering::SeqCst))
    }
    async fn warm(&self, _: CandidateDescriptor, _: &Interest) -> Result<()> {
        if self.queue_warm.load(Ordering::SeqCst) {
            self.started.notify_one();
            return Err(ParticipationClient::fixture_queue_busy());
        }
        if self.hold.swap(false, Ordering::SeqCst)
            || self.hold_after_warmed == Some(self.warmed.load(Ordering::SeqCst))
        {
            self.started.notify_one();
            self.resume.notified().await;
        }
        self.warmed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn page(&self, r: &SearchRequest, i: &Interest) -> Result<SearchPage> {
        self.browses.fetch_add(1, Ordering::SeqCst);
        let mut page = self.checks.page(r, i).await?;
        if self.empty_until_prepared && self.warmed.load(Ordering::SeqCst) == 0 {
            page.results.clear();
        }
        Ok(page)
    }
    async fn reset_browse(&self, scope: BrowseScope) -> Result<()> {
        self.resets.fetch_add(1, Ordering::SeqCst);
        if self.hold_reset {
            self.started.notify_one();
            self.resume.notified().await;
        }
        if self.fail_reset && matches!(scope, BrowseScope::Ordinary) {
            bail!("synthetic browse refresh failure");
        }
        Ok(())
    }
    async fn admit(&self, d: CandidateDescriptor, i: &Interest) -> Result<Arc<Verified>> {
        if self.fail_early_admit && self.browses.load(Ordering::SeqCst) == 2 {
            bail!("synthetic first recovery source failure");
        }
        self.checks.admit(d, i).await
    }
    async fn current(&self, v: &Verified) -> Result<()> {
        self.checks.current(v).await
    }
}

#[tokio::test]
async fn failed_population_keeps_an_independent_checked_page_without_serializing_the_cause()
-> Result<()> {
    let mut fixture = Fixture::new(20, 0)?;
    fixture.fail_population = true;
    let backend = Arc::new(fixture);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    service.join().await?;
    let view = service.view(id).await?;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 1);
    assert!(matches!(
        view.preparation.as_ref().unwrap().phase,
        Phase::Unavailable
    ));
    assert!(!String::from_utf8(serde_json::to_vec(&view)?)?.contains("synthetic population"));
    assert_eq!(backend.pages.load(Ordering::SeqCst), 0);
    assert_eq!(backend.resets.load(Ordering::SeqCst), 0);
    assert_eq!(backend.browses.load(Ordering::SeqCst), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn failed_empty_browse_refresh_never_promotes_background_rows_or_restarts_the_pass()
-> Result<()> {
    let mut fixture = Fixture::new(20, 0)?;
    fixture.empty_until_prepared = true;
    fixture.fail_early_admit = true;
    fixture.fail_reset = true;
    let backend = Arc::new(fixture);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    service.join().await?;
    let view = service.view(id).await?;
    assert_eq!(view.phase, SearchPhase::Unavailable);
    assert!(view.results.is_empty());
    assert!(view.preparation.as_ref().unwrap().local_pass_complete);
    assert!(!String::from_utf8(serde_json::to_vec(&view)?)?.contains("synthetic browse"));
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 20);
    assert_eq!(backend.resets.load(Ordering::SeqCst), 2);
    assert_eq!(backend.browses.load(Ordering::SeqCst), 2);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn known_unsent_inventory_or_warm_pressure_does_not_spend_rows_or_restart_the_pass()
-> Result<()> {
    for inventory in [false, true] {
        let backend = Arc::new(Fixture::new(2, 0)?);
        if inventory {
            backend.queue_inventory.store(true, Ordering::SeqCst);
        } else {
            backend.queue_warm.store(true, Ordering::SeqCst);
        }
        let service = SearchService::default();
        let id = service.start_with(backend.clone(), initial())?;
        timeout(Duration::from_secs(3), backend.started.notified()).await?;
        let view = service.view(id).await?;
        assert_eq!(view.phase, SearchPhase::Ready);
        assert_eq!(view.results.len(), 1);
        let pending = view.preparation.unwrap();
        assert_eq!((pending.examined, pending.omitted), (0, 0));
        backend.queue_inventory.store(false, Ordering::SeqCst);
        backend.queue_warm.store(false, Ordering::SeqCst);
        let view = prepared(&service, id).await?;
        let complete = view.preparation.unwrap();
        assert!(complete.local_pass_complete);
        assert_eq!(
            (complete.examined, complete.available, complete.omitted),
            (2, 2, 0)
        );
        assert_eq!(backend.pages.load(Ordering::SeqCst), 1);
        assert_eq!(backend.metadata_pages.load(Ordering::SeqCst), 1);
        assert_eq!(backend.warmed.load(Ordering::SeqCst), 2);
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}
fn initial() -> SearchRequest {
    let mut value = request("");
    value.prepare_catalog = true;
    value
}
async fn prepared(service: &SearchService, id: Uuid) -> Result<SearchView> {
    timeout(Duration::from_secs(120), async {
        loop {
            let view = service.view(id).await?;
            if view.preparation.as_ref().is_some_and(|p| {
                matches!(
                    p.phase,
                    Phase::Complete | Phase::Limited | Phase::Unavailable
                )
            }) && view.phase != SearchPhase::Checking
            {
                return Ok(view);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn a_verified_page_is_ready_before_the_same_task_finishes_the_bounded_background_pass()
-> Result<()> {
    let backend = Arc::new(Fixture::new(83, 0)?);
    backend.hold.store(true, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    timeout(Duration::from_secs(3), backend.started.notified()).await?;
    let first = service.view(id).await?;
    assert_eq!(first.phase, SearchPhase::Ready);
    assert_eq!(first.results.len(), 1);
    assert!(!first.preparation.unwrap().local_pass_complete);
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 0);
    backend.resume.notify_one();
    let view = prepared(&service, id).await?;
    assert!(matches!(view.phase, SearchPhase::Ready));
    let progress = view.preparation.context("missing preparation")?;
    assert_eq!(
        (
            progress.examined,
            progress.available,
            progress.omitted,
            progress.pages
        ),
        (83, 83, 0, 5)
    );
    assert!(progress.local_pass_complete && !progress.network_complete && progress.metadata_only);
    assert_eq!(view.results.len(), 1);
    assert_eq!(
        view.checked, 1,
        "inventory progress is not the displayed first page's admission"
    );
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 83);
    assert_eq!(backend.metadata_pages.load(Ordering::SeqCst), 5);
    assert_eq!(
        backend.browses.load(Ordering::SeqCst),
        1,
        "do not replace a nonempty page or its cursor"
    );
    assert_eq!(backend.resets.load(Ordering::SeqCst), 0);
    assert_eq!(
        service
            .inner
            .slot
            .lock()
            .unwrap()
            .query
            .as_ref()
            .unwrap()
            .values
            .len(),
        1
    );
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test]
async fn resource_pause_does_not_skip_rows_or_hold_up_a_replacing_user_search() -> Result<()> {
    let backend = Arc::new(Fixture::new(40, 0)?);
    backend.available.store(false, Ordering::SeqCst);
    let service = SearchService::default();
    let old = service.start_with(backend.clone(), initial())?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let view = service.view(old).await?;
    assert!(matches!(view.preparation.unwrap().phase, Phase::Waiting));
    assert_eq!(backend.pages.load(Ordering::SeqCst), 0);
    let latest = service.start_with(backend.clone(), request(""))?;
    assert!(service.view(old).await.is_err());
    let view = finished(&service, latest).await?;
    assert!(view.preparation.is_none());
    assert_eq!(view.results.len(), 1);
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test]
async fn query_replacement_waits_for_the_original_owned_warm_work_instead_of_aborting_it()
-> Result<()> {
    let backend = Arc::new(Fixture::new(40, 0)?);
    backend.hold.store(true, Ordering::SeqCst);
    let service = SearchService::default();
    let old = service.start_with(backend.clone(), initial())?;
    timeout(Duration::from_secs(3), backend.started.notified()).await?;
    let latest = service.start_with(backend.clone(), request(""))?;
    assert!(service.view(old).await.is_err());
    assert!(matches!(
        service.view(latest).await?.phase,
        SearchPhase::Checking
    ));
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 0);
    backend.resume.notify_one();
    assert_eq!(finished(&service, latest).await?.results.len(), 1);
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test]
async fn replay_missing_bytes_changed_bindings_and_global_claims_cannot_complete_preparation()
-> Result<()> {
    for broken in 1..=4 {
        let backend = Arc::new(Fixture::new(40, broken)?);
        let service = SearchService::default();
        let id = service.start_with(backend, initial())?;
        let view = prepared(&service, id).await?;
        assert_eq!(view.phase, SearchPhase::Ready);
        assert!(!view.preparation.unwrap().local_pass_complete);
        assert_eq!(
            view.results.len(),
            1,
            "bad inventory cannot erase an independently verified first page"
        );
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}
#[tokio::test]
async fn ten_thousand_rows_are_a_pass_ceiling_not_an_unbounded_retained_history() -> Result<()> {
    let backend = Arc::new(Fixture::new(10_001, 0)?);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    let view = prepared(&service, id).await?;
    let progress = view.preparation.unwrap();
    assert_eq!(view.phase, SearchPhase::Ready);
    assert!(matches!(progress.phase, Phase::Limited));
    assert_eq!(progress.examined, 10_000);
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 10_000);
    assert!(!progress.local_pass_complete && view.results.len() == 1);
    assert_eq!(
        service
            .inner
            .slot
            .lock()
            .unwrap()
            .query
            .as_ref()
            .unwrap()
            .values
            .len(),
        1
    );
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn an_empty_bootstrap_rechecks_the_first_warmed_row_without_promoting_warmed_rows()
-> Result<()> {
    let mut fixture = Fixture::new(20, 0)?;
    fixture.empty_until_prepared = true;
    fixture.hold_reset = true;
    fixture.hold_after_warmed = Some(1);
    fixture.hold.store(true, Ordering::SeqCst);
    let backend = Arc::new(fixture);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    timeout(Duration::from_secs(3), backend.started.notified()).await?;
    let empty = service.view(id).await?;
    assert!(empty.results.is_empty());
    assert!(!empty.preparation.unwrap().local_pass_complete);
    backend.resume.notify_one();
    timeout(Duration::from_secs(3), backend.started.notified()).await?;
    let rechecking = service.view(id).await?;
    assert!(rechecking.results.is_empty());
    assert_eq!(rechecking.phase, SearchPhase::Checking);
    assert!(!rechecking.preparation.unwrap().local_pass_complete);
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 1);
    backend.resume.notify_one();
    timeout(Duration::from_secs(3), backend.started.notified()).await?;
    let first = service.view(id).await?;
    assert_eq!(first.phase, SearchPhase::Ready);
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.checked, 1);
    assert!(!first.preparation.unwrap().local_pass_complete);
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 1);
    backend.resume.notify_one();
    let view = prepared(&service, id).await?;
    service.join().await?;
    assert_eq!(view.results.len(), 1);
    assert_eq!(
        view.checked, 1,
        "the 20 synthetic warmed rows do not become checked cards"
    );
    assert_eq!(backend.resets.load(Ordering::SeqCst), 1);
    assert_eq!(backend.browses.load(Ordering::SeqCst), 2);
    assert!(
        service
            .inner
            .slot
            .lock()
            .unwrap()
            .query
            .as_ref()
            .unwrap()
            .preparation_until
            .is_none()
    );
    service.request_stop();
    Ok(())
}

#[tokio::test]
async fn a_missed_early_card_keeps_the_single_final_browse_after_preparation() -> Result<()> {
    let mut fixture = Fixture::new(3, 0)?;
    fixture.empty_until_prepared = true;
    fixture.fail_early_admit = true;
    fixture.hold_after_warmed = Some(1);
    let backend = Arc::new(fixture);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    timeout(Duration::from_secs(3), backend.started.notified()).await?;
    let pending = service.view(id).await?;
    assert_eq!(pending.phase, SearchPhase::Ready);
    assert!(pending.results.is_empty() && pending.next_cursor.is_none());
    assert_eq!((pending.checked, pending.omitted), (0, 0));
    assert!(!pending.preparation.unwrap().local_pass_complete);
    backend.resume.notify_one();
    service.join().await?;
    let view = service.view(id).await?;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 1);
    assert!(view.preparation.unwrap().local_pass_complete);
    assert_eq!(backend.resets.load(Ordering::SeqCst), 2);
    assert_eq!(backend.browses.load(Ordering::SeqCst), 3);
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 3);
    service.request_stop();
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn explicit_background_preparation_survives_view_departure_but_cannot_renew_its_hour_limit()
-> Result<()> {
    let backend = Arc::new(Fixture::new(20, 0)?);
    backend.available.store(false, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    let page = finished(&service, id).await?;
    assert_eq!(page.results.len(), 1);
    assert!(matches!(page.preparation.unwrap().phase, Phase::Waiting));
    let until = service
        .inner
        .slot
        .lock()
        .unwrap()
        .query
        .as_ref()
        .unwrap()
        .preparation_until
        .unwrap();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    let interest = Interest {
        inner: Arc::downgrade(&service.inner),
        id,
    };
    assert!(
        interest.active(),
        "a setup-requested pass continues behind other app screens"
    );
    assert_eq!(
        backend.pages.load(Ordering::SeqCst),
        0,
        "no work while resource-deferred"
    );
    let _ = service.view(id).await?;
    assert_eq!(
        service
            .inner
            .slot
            .lock()
            .unwrap()
            .query
            .as_ref()
            .unwrap()
            .preparation_until,
        Some(until)
    );
    tokio::time::advance(Duration::from_secs(3601)).await;
    service.join().await?;
    let query = service.inner.slot.lock().unwrap();
    let query = query.query.as_ref().unwrap();
    assert!(matches!(
        query.preparation.as_ref().unwrap().phase,
        Phase::Limited | Phase::Unavailable
    ));
    assert!(query.preparation_until.is_none());
    assert_eq!(backend.warmed.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn stopping_the_owner_ends_background_preparation_and_never_grants_readiness() -> Result<()> {
    let backend = Arc::new(Fixture::new(20, 0)?);
    backend.available.store(false, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), initial())?;
    let _ = finished(&service, id).await?;
    backend.stopped.store(true, Ordering::SeqCst);
    let view = prepared(&service, id).await?;
    assert!(matches!(
        view.preparation.unwrap().phase,
        Phase::Unavailable
    ));
    assert_eq!(backend.pages.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    assert!(service.view(id).await.is_err());
    Ok(())
}
#[test]
fn preparation_is_only_an_explicit_initial_default_request() -> Result<()> {
    assert!(
        SearchRequest::parse(
            br#"{"query":{"text":"","tags":[],"limit":20},"prepare_catalog":true}"#
        )
        .is_ok()
    );
    for changed in [
        serde_json::json!({"text":"movies","tags":[],"limit":20}),
        serde_json::json!({"text":"","tags":["anime"],"limit":20}),
        serde_json::json!({"text":"","tags":[],"limit":1}),
    ] {
        assert!(
            SearchRequest::parse(&serde_json::to_vec(
                &serde_json::json!({"query":changed,"prepare_catalog":true})
            )?)
            .is_err()
        );
    }
    assert!(
        SearchRequest::parse(
            br#"{"query":{"text":"","tags":[],"limit":20},"prepare_catalog":"true"}"#
        )
        .is_err()
    );
    Ok(())
}
