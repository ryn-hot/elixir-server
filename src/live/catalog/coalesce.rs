use std::{future::Future, sync::Arc};

use dashmap::{DashMap, mapref::entry::Entry};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::cache::{CacheKey, CatalogCacheValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoalescedLoadError {
    Cancelled,
    Failed(&'static str),
}

type LoadResult = Result<Arc<CatalogCacheValue>, CoalescedLoadError>;

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
pub struct CatalogRequestCoalescer {
    flights: Arc<DashMap<CacheKey, Arc<Flight>>>,
}

impl CatalogRequestCoalescer {
    pub async fn run<F, Fut>(
        &self,
        key: CacheKey,
        waiter_cancellation: &CancellationToken,
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
                    upstream: CancellationToken::new(),
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
                _ = waiter_cancellation.cancelled() => {
                    return Err(CoalescedLoadError::Cancelled);
                }
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return Err(CoalescedLoadError::Failed("catalog_coalescer_closed"));
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn active_flights(&self) -> usize {
        self.flights.len()
    }
}
