//! A single aggregate for each explicitly observed setup pass. Ordinary view
//! polling does not consume its budget, and a missing checkpoint stays null.
use serde::Serialize;
use std::{
    cell::RefCell,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::time::Instant;
use uuid::Uuid;

const MAX_PASSES: usize = 4;
static PASSES: AtomicUsize = AtomicUsize::new(0);
tokio::task_local! { static PASS: RefCell<Pass>; }

pub(in crate::marketplace) enum Checkpoint {
    DiscoveryStarted,
    DiscoveryFinished(usize),
    FirstRemoteHint,
    SourceStarted,
    SourceFinished(bool),
    FirstAdmitted,
}

#[derive(Default, Serialize)]
struct Marks {
    discovery_started_ms: Option<u64>,
    discovery_finished_ms: Option<u64>,
    discovered_peers: Option<usize>,
    first_remote_hint_ms: Option<u64>,
    source_started_ms: Option<u64>,
    source_finished_ms: Option<u64>,
    source_succeeded: Option<bool>,
    first_admitted_ms: Option<u64>,
}
struct Pass {
    query_id: Uuid,
    started: Instant,
    marks: Marks,
    emitted: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    FirstAdmitted,
    NoCard,
    Failed,
}
#[derive(Serialize)]
struct Event<'a> {
    stage: &'static str,
    query_id: Uuid,
    marks: &'a Marks,
    elapsed_ms: u64,
    outcome: Outcome,
}
impl Pass {
    fn new(query_id: Uuid) -> Self {
        Self {
            query_id,
            started: Instant::now(),
            marks: Marks::default(),
            emitted: false,
        }
    }
    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
    fn record(&mut self, checkpoint: Checkpoint) {
        let ms = self.elapsed_ms();
        match checkpoint {
            Checkpoint::DiscoveryStarted => {
                self.marks.discovery_started_ms.get_or_insert(ms);
            }
            Checkpoint::DiscoveryFinished(peers) => {
                self.marks.discovery_finished_ms.get_or_insert(ms);
                self.marks.discovered_peers.get_or_insert(peers);
            }
            Checkpoint::FirstRemoteHint => {
                self.marks.first_remote_hint_ms.get_or_insert(ms);
            }
            Checkpoint::SourceStarted => {
                self.marks.source_started_ms.get_or_insert(ms);
            }
            Checkpoint::SourceFinished(succeeded) => {
                self.marks.source_finished_ms.get_or_insert(ms);
                self.marks.source_succeeded.get_or_insert(succeeded);
            }
            Checkpoint::FirstAdmitted => {
                self.marks.first_admitted_ms.get_or_insert(ms);
                self.emit(Outcome::FirstAdmitted);
            }
        }
    }
    fn emit(&mut self, outcome: Outcome) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        super::write_event(&mut std::io::stderr().lock(), &self.event(outcome));
    }
    fn event(&self, outcome: Outcome) -> Event<'_> {
        Event {
            stage: "cold_setup_search_pass",
            query_id: self.query_id,
            marks: &self.marks,
            elapsed_ms: self.elapsed_ms(),
            outcome,
        }
    }
}

pub(in crate::marketplace) fn record(checkpoint: Checkpoint) {
    let _ = PASS.try_with(|pass| pass.borrow_mut().record(checkpoint));
}

pub(in crate::marketplace) async fn observe(
    query_id: Uuid,
    work: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    if std::env::var_os("ELIXIR_MARKETPLACE_COLD_SETUP_TIMING").as_deref()
        != Some(std::ffi::OsStr::new("1"))
        || PASSES
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < MAX_PASSES).then_some(n + 1)
            })
            .is_err()
    {
        return work.await;
    }
    let pass = RefCell::new(Pass::new(query_id));
    PASS.scope(pass, async {
        let result = work.await;
        PASS.with(|pass| {
            pass.borrow_mut().emit(if result.is_ok() {
                Outcome::NoCard
            } else {
                Outcome::Failed
            })
        });
        result
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn cold_setup_reports_first_attempt_and_preserves_missing_stages() {
        let query_id = Uuid::new_v4();
        let mut pass = Pass::new(query_id);
        let failed = serde_json::to_value(pass.event(Outcome::Failed)).unwrap();
        assert_eq!(failed["query_id"], query_id.to_string());
        assert!(failed["marks"]["discovered_peers"].is_null());
        pass.record(Checkpoint::DiscoveryStarted);
        tokio::time::advance(Duration::from_millis(8)).await;
        pass.record(Checkpoint::DiscoveryFinished(1));
        tokio::time::advance(Duration::from_millis(3)).await;
        pass.record(Checkpoint::FirstRemoteHint);
        tokio::time::advance(Duration::from_millis(2)).await;
        pass.record(Checkpoint::SourceStarted);
        tokio::time::advance(Duration::from_millis(7)).await;
        pass.record(Checkpoint::SourceFinished(false));
        tokio::time::advance(Duration::from_millis(9)).await;
        pass.record(Checkpoint::SourceStarted);
        pass.record(Checkpoint::SourceFinished(true));
        let event = serde_json::to_value(pass.event(Outcome::NoCard)).unwrap();
        assert_eq!(event["elapsed_ms"], 29);
        assert_eq!(
            event["marks"],
            serde_json::json!({
                "discovery_started_ms":0, "discovery_finished_ms":8, "discovered_peers":1,
                "first_remote_hint_ms":11, "source_started_ms":13, "source_finished_ms":20,
                "source_succeeded":false, "first_admitted_ms":null
            })
        );
    }
}
