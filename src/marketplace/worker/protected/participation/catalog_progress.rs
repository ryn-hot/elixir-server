//! One current, metadata-only progress observation. No event log, new polling
//! actor, readiness authority or claim of global catalog coverage.
use super::*;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    NotStarted,
    Refreshing,
    Waiting,
    Paused,
    Unavailable,
    Unsupported,
}

#[derive(Clone, Debug, Serialize)]
pub struct CatalogSnapshot {
    pub phase: Phase,
    pub observed_seconds: Option<u64>,
    pub last_peer_page_seconds: Option<u64>,
    pub last_peer_scan_seconds: Option<u64>,
    pub last_page_examined: Option<usize>,
    #[serde(skip)]
    captured: Instant,
}
impl CatalogSnapshot {
    pub(super) fn age(&mut self) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.captured).as_secs();
        for age in [
            &mut self.observed_seconds,
            &mut self.last_peer_page_seconds,
            &mut self.last_peer_scan_seconds,
        ]
        .into_iter()
        .flatten()
        {
            *age = age.saturating_add(elapsed);
        }
        self.captured += Duration::from_secs(elapsed);
        if matches!(self.phase, Phase::Refreshing | Phase::Waiting)
            && self.observed_seconds.is_none_or(|age| age > 30)
        {
            self.phase = Phase::Unavailable;
        }
    }
}

#[derive(Default)]
pub(super) struct Tracker {
    observed: Option<Instant>,
    phase: Option<Phase>,
    pending: Option<PeerKey>,
    page: Option<(Instant, usize)>,
    scan: Option<Instant>,
}
impl Tracker {
    pub(super) fn paused(&mut self) {
        self.pending = None;
        self.phase = Some(Phase::Paused);
    }
    pub(super) fn started(&mut self, state: &CatalogJobState) {
        self.observed = Some(Instant::now());
        // PrivateWorkerClient already correlates operation, kind, peer and
        // running phase. Keep the completion fence explicit here as well.
        self.pending = (state.kind == Some(CatalogJobKind::Population)
            && state.phase == CatalogJobPhase::Running)
            .then(|| state.peer.clone())
            .flatten();
        self.phase = Some(if self.pending.is_some() {
            Phase::Refreshing
        } else {
            Phase::Unavailable
        });
    }
    pub(super) fn observed(&mut self, state: &CatalogJobState) {
        let now = Instant::now();
        self.observed = Some(now);
        if state.kind != Some(CatalogJobKind::Population) {
            if self.pending.take().is_some() {
                self.phase = Some(Phase::Unavailable);
            }
            return;
        }
        self.phase = Some(match state.phase {
            CatalogJobPhase::Running => Phase::Refreshing,
            CatalogJobPhase::Progress | CatalogJobPhase::Complete => Phase::Waiting,
            CatalogJobPhase::Limited => Phase::Paused,
            _ => Phase::Unavailable,
        });
        if state.phase == CatalogJobPhase::Running {
            return;
        }
        // A residual completed job polled repeatedly cannot refresh age.
        // Nor can another peer's result finish the request we actually started.
        let pending = self.pending.take();
        let matching = pending
            .as_ref()
            .is_some_and(|peer| state.peer.as_ref() == Some(peer));
        if pending.is_some() && !matching {
            self.phase = Some(Phase::Unavailable);
        }
        if matching
            && matches!(
                state.phase,
                CatalogJobPhase::Progress | CatalogJobPhase::Complete
            )
        {
            self.page = Some((now, state.admitted + state.unchanged + state.rejected));
            if state.phase == CatalogJobPhase::Complete {
                self.scan = Some(now);
            }
        }
    }
    pub(super) fn view(&self, supported: bool, allowed: bool) -> CatalogSnapshot {
        let now = Instant::now();
        let age = |time: Instant| now.saturating_duration_since(time).as_secs();
        let mut value = CatalogSnapshot {
            phase: if !supported {
                Phase::Unsupported
            } else if !allowed && self.pending.is_none() {
                Phase::Paused
            } else {
                self.phase.unwrap_or(Phase::NotStarted)
            },
            observed_seconds: self.observed.map(age),
            last_peer_page_seconds: self.page.map(|(time, _)| age(time)),
            last_peer_scan_seconds: self.scan.map(age),
            last_page_examined: self.page.map(|(_, count)| count),
            captured: now,
        };
        value.age();
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state(phase: CatalogJobPhase) -> CatalogJobState {
        CatalogJobState {
            phase,
            kind: Some(CatalogJobKind::Population),
            peer: Some(PeerKey::try_from("1".repeat(64)).unwrap()),
            admitted: 0,
            unchanged: 0,
            rejected: 0,
            indexed: 0,
            removed: 0,
        }
    }
    #[tokio::test(start_paused = true)]
    async fn only_a_correlated_new_peer_completion_advances_progress() {
        let mut tracker = Tracker::default();
        let running = state(CatalogJobPhase::Running);
        tracker.started(&running);
        let mut page = state(CatalogJobPhase::Progress);
        page.admitted = 3;
        page.rejected = 1;
        tracker.observed(&page);
        assert_eq!(tracker.view(true, true).last_page_examined, Some(4));
        assert_eq!(tracker.view(true, true).last_peer_scan_seconds, None);
        tokio::time::advance(Duration::from_secs(17)).await;
        tracker.observed(&page);
        assert_eq!(tracker.view(true, true).last_peer_page_seconds, Some(17));
        tracker.started(&running);
        tracker.observed(&state(CatalogJobPhase::Complete));
        let value = tracker.view(true, true);
        assert_eq!(value.last_peer_page_seconds, Some(0));
        assert_eq!(value.last_peer_scan_seconds, Some(0));
        assert_eq!(value.last_page_examined, Some(0)); // empty peer scan is still not global coverage
        tokio::time::advance(Duration::from_secs(9)).await;
        tracker.observed(&state(CatalogJobPhase::Complete));
        assert_eq!(tracker.view(true, true).last_peer_scan_seconds, Some(9));
    }
    #[tokio::test(start_paused = true)]
    async fn failures_limits_wrong_peers_and_maintenance_never_count_as_refresh() {
        for phase in [
            CatalogJobPhase::Failed,
            CatalogJobPhase::Limited,
            CatalogJobPhase::Cancelled,
        ] {
            let mut tracker = Tracker::default();
            tracker.started(&state(CatalogJobPhase::Running));
            tracker.observed(&state(phase));
            assert_eq!(tracker.view(true, true).last_peer_page_seconds, None);
        }
        let mut tracker = Tracker::default();
        tracker.started(&state(CatalogJobPhase::Running));
        let mut wrong = state(CatalogJobPhase::Complete);
        wrong.peer = Some(PeerKey::try_from("2".repeat(64)).unwrap());
        tracker.observed(&wrong);
        assert_eq!(tracker.view(true, true).last_peer_page_seconds, None);
        tracker.started(&state(CatalogJobPhase::Running));
        let mut maintenance = state(CatalogJobPhase::Complete);
        maintenance.kind = Some(CatalogJobKind::Maintenance);
        maintenance.peer = None;
        tracker.observed(&maintenance);
        assert_eq!(tracker.view(true, true).phase, Phase::Unavailable);
        assert_eq!(tracker.view(true, true).last_peer_scan_seconds, None);
    }
    #[tokio::test(start_paused = true)]
    async fn cached_watch_copies_age_without_polling_or_inventing_readiness() {
        let mut tracker = Tracker::default();
        assert_eq!(tracker.view(true, true).phase, Phase::NotStarted);
        assert_eq!(tracker.view(false, true).phase, Phase::Unsupported);
        assert_eq!(tracker.view(true, false).phase, Phase::Paused);
        tracker.started(&state(CatalogJobPhase::Running));
        tracker.observed(&state(CatalogJobPhase::Complete));
        let mut copy = tracker.view(true, true);
        tokio::time::advance(Duration::from_secs(31)).await;
        copy.age();
        assert_eq!(copy.phase, Phase::Unavailable);
        assert_eq!(copy.last_peer_scan_seconds, Some(31));
        assert_eq!(copy.observed_seconds, Some(31));
        assert_eq!(tracker.view(true, false).phase, Phase::Paused);
        let serialized = serde_json::to_value(copy).unwrap();
        assert_eq!(serialized.as_object().unwrap().len(), 5);
        assert!(serialized.get("peer").is_none());
        assert!(serialized.get("ready").is_none());
    }
}
