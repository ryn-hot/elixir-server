use std::{future::Future, sync::Arc};

use axum::http::HeaderMap;
use dashmap::{DashMap, mapref::entry::Entry};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{LiveRelayError, hls::HlsResourceId};

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct ManifestFlightKey {
    session_id: Uuid,
    control_fencing_token: i64,
    resource_id: Option<HlsResourceId>,
}

impl ManifestFlightKey {
    pub(super) fn new(
        session_id: Uuid,
        control_fencing_token: i64,
        resource_id: Option<HlsResourceId>,
    ) -> Self {
        Self {
            session_id,
            control_fencing_token,
            resource_id,
        }
    }
}

#[derive(Clone)]
pub(super) struct CoalescedManifest {
    pub(super) headers: HeaderMap,
    pub(super) body: Vec<u8>,
}

type LoadResult = Result<Arc<CoalescedManifest>, LiveRelayError>;

struct Flight {
    sender: watch::Sender<Option<LoadResult>>,
    upstream: CancellationToken,
    waiters: std::sync::atomic::AtomicUsize,
    completed: std::sync::atomic::AtomicBool,
}

impl Flight {
    fn add_waiter(&self) {
        self.waiters
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    fn remove_waiter(&self) {
        if self
            .waiters
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
            && !self.completed.load(std::sync::atomic::Ordering::Acquire)
        {
            self.upstream.cancel();
        }
    }
}

struct WaiterGuard(Arc<Flight>);

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        self.0.remove_waiter();
    }
}

#[derive(Clone, Default)]
pub(super) struct ManifestRequestCoalescer {
    flights: Arc<DashMap<ManifestFlightKey, Arc<Flight>>>,
}

impl ManifestRequestCoalescer {
    pub(super) async fn run<F, Fut>(
        &self,
        key: ManifestFlightKey,
        session_cancellation: &CancellationToken,
        loader: F,
    ) -> LoadResult
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = LoadResult> + Send + 'static,
    {
        let (flight, leader) = match self.flights.entry(key.clone()) {
            Entry::Occupied(entry) => {
                let flight = entry.get().clone();
                flight.add_waiter();
                (flight, false)
            }
            Entry::Vacant(entry) => {
                let (sender, _) = watch::channel(None);
                let flight = Arc::new(Flight {
                    sender,
                    upstream: session_cancellation.child_token(),
                    waiters: std::sync::atomic::AtomicUsize::new(1),
                    completed: std::sync::atomic::AtomicBool::new(false),
                });
                entry.insert(flight.clone());
                (flight, true)
            }
        };
        let _guard = WaiterGuard(flight.clone());
        let mut receiver = flight.sender.subscribe();

        if leader {
            let flights = self.flights.clone();
            let task_flight = flight.clone();
            tokio::spawn(async move {
                let result = loader(task_flight.upstream.clone()).await;
                task_flight
                    .completed
                    .store(true, std::sync::atomic::Ordering::Release);
                task_flight.sender.send_replace(Some(result));
                flights.remove(&key);
            });
        }

        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            tokio::select! {
                _ = session_cancellation.cancelled() => {
                    return Err(LiveRelayError::SessionExpired);
                }
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return Err(LiveRelayError::Unavailable);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn active_flights(&self) -> usize {
        self.flights.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn r12_manifest_coalescer_deduplicates_and_cancels_orphaned_loads() {
        let coalescer = ManifestRequestCoalescer::default();
        let session_cancellation = CancellationToken::new();
        let key = ManifestFlightKey::new(Uuid::new_v4(), 7, None);
        let calls = Arc::new(AtomicUsize::new(0));

        let first_coalescer = coalescer.clone();
        let first_cancellation = session_cancellation.clone();
        let first_key = key.clone();
        let first_calls = calls.clone();
        let first = tokio::spawn(async move {
            first_coalescer
                .run(first_key, &first_cancellation, move |_| async move {
                    first_calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                    Ok(Arc::new(CoalescedManifest {
                        headers: HeaderMap::new(),
                        body: b"manifest".to_vec(),
                    }))
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while coalescer.active_flights() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("manifest leader admission");
        let second = coalescer.run(key, &session_cancellation, |_| async move {
            panic!("a coalesced waiter must not invoke its loader")
        });
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            first
                .expect("leader task")
                .expect("leader load")
                .body
                .as_slice(),
            b"manifest"
        );
        assert_eq!(second.expect("waiter load").body.as_slice(), b"manifest");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(coalescer.active_flights(), 0);

        let orphan_key = ManifestFlightKey::new(Uuid::new_v4(), 8, None);
        let loader_cancelled = Arc::new(AtomicBool::new(false));
        let orphan_coalescer = coalescer.clone();
        let orphan_session = CancellationToken::new();
        let task_session = orphan_session.clone();
        let task_cancelled = loader_cancelled.clone();
        let orphan = tokio::spawn(async move {
            orphan_coalescer
                .run(orphan_key, &task_session, move |upstream| async move {
                    upstream.cancelled().await;
                    task_cancelled.store(true, Ordering::SeqCst);
                    Err(LiveRelayError::SessionExpired)
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while coalescer.active_flights() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("orphaned manifest leader admission");
        orphan.abort();
        let _ = orphan.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !loader_cancelled.load(Ordering::SeqCst) || coalescer.active_flights() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("orphaned manifest loader cancellation");
        assert!(!orphan_session.is_cancelled());
    }
}
