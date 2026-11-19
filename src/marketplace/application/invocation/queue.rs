//! Bounded FIFO admission, NOT native ownership or retirement authority.
//! One active consumer/result plus up to 4,095 waiting callers. A waiter has
//! no deadline of its own: only its caller's deadline or cancellation ends it.
//! Cancellation
//! releases only this coordination slot; the original application/controller
//! must still prove native cleanup before it can start another invocation.
use anyhow::{Context, Result, bail};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, watch};

const CAPACITY: usize = 4096;

struct Inner {
    slots: Arc<Semaphore>,
    serial: Arc<Mutex<()>>,
    stopped: watch::Sender<bool>,
}
pub(in crate::marketplace) struct Queue(Arc<Inner>);
// No Clone/Deserialize/public constructor. This is a scheduling turn, never a
// source/grant/executor capability, and never proof that dropped work stopped.
pub(crate) struct Turn {
    owner: Arc<Inner>,
    _serial: OwnedMutexGuard<()>,
    _slot: OwnedSemaphorePermit,
}
impl Default for Queue {
    fn default() -> Self {
        let (stopped, _) = watch::channel(false);
        Self(Arc::new(Inner {
            slots: Arc::new(Semaphore::new(CAPACITY)),
            serial: Arc::new(Mutex::new(())),
            stopped,
        }))
    }
}
impl Queue {
    pub(in crate::marketplace) fn is_idle(&self) -> bool {
        !*self.0.stopped.borrow() && self.0.slots.available_permits() == CAPACITY
    }
    // Test coordination witness only, never native execution/cleanup evidence.
    #[cfg(test)]
    pub(in crate::marketplace) fn slots_used(&self) -> usize {
        CAPACITY - self.0.slots.available_permits()
    }
    fn open(&self) -> Result<()> {
        if *self.0.stopped.borrow() {
            bail!("installed call queue is stopping");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn require(&self, turn: &Turn) -> Result<()> {
        self.open()?;
        if !Arc::ptr_eq(&self.0, &turn.owner) {
            bail!("installed call turn belongs to a different queue");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn close(&self) {
        self.0.stopped.send_replace(true);
        self.0.slots.close();
    }
    pub(in crate::marketplace) fn try_acquire(&self) -> Result<Turn> {
        self.open()?;
        let slot = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("installed call queue is full")?;
        let serial = self
            .0
            .serial
            .clone()
            .try_lock_owned()
            .context("installed call slot busy")?;
        let turn = Turn {
            owner: self.0.clone(),
            _serial: serial,
            _slot: slot,
        };
        self.require(&turn)?;
        Ok(turn)
    }
    pub(in crate::marketplace) async fn acquire(&self) -> Result<Turn> {
        let mut stopped = self.0.stopped.subscribe();
        self.open()?;
        // Never enqueue on this semaphore: excess requests fail immediately.
        let slot = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("installed call queue is full")?;
        let serial = tokio::select! {biased;
            _ = stopped.changed() => bail!("installed call queue is stopping"),
            serial = self.0.serial.clone().lock_owned() => serial,
        };
        let turn = Turn {
            owner: self.0.clone(),
            _serial: serial,
            _slot: slot,
        };
        self.require(&turn)?;
        Ok(turn)
    }
}
impl Drop for Queue {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::{Instant, timeout};

    #[tokio::test(start_paused = true)]
    async fn all_slots_are_fifo_and_overflow_never_waits() -> Result<()> {
        let queue = Queue::default();
        let first = queue.acquire().await?;
        let mut waiting: Vec<_> = (1..CAPACITY).map(|_| Box::pin(queue.acquire())).collect();
        for pending in waiting.iter_mut() {
            assert!(
                timeout(Duration::from_millis(1), pending.as_mut())
                    .await
                    .is_err()
            );
        }
        assert_eq!(queue.0.slots.available_permits(), 0);
        let before = Instant::now();
        assert!(queue.acquire().await.is_err());
        assert_eq!(Instant::now(), before);
        drop(first);
        // The next turn goes to the earliest waiter, never a later one.
        assert!(
            timeout(Duration::from_millis(1), waiting[1].as_mut())
                .await
                .is_err()
        );
        for pending in waiting.iter_mut() {
            let turn = timeout(Duration::from_secs(1), pending.as_mut()).await??;
            drop(turn);
        }
        assert_eq!(queue.0.slots.available_permits(), CAPACITY);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_releases_its_slot_and_does_not_steal_a_turn() -> Result<()> {
        let queue = Queue::default();
        let active = queue.acquire().await?;
        let mut waiting = Box::pin(queue.acquire());
        assert!(
            timeout(Duration::from_millis(1), waiting.as_mut())
                .await
                .is_err()
        );
        assert_eq!(queue.0.slots.available_permits(), CAPACITY - 2);
        drop(waiting);
        assert_eq!(queue.0.slots.available_permits(), CAPACITY - 1);
        drop(active);
        drop(queue.try_acquire()?);
        assert_eq!(queue.0.slots.available_permits(), CAPACITY);
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn queue_wait_has_no_deadline_of_its_own_and_never_replaces_the_active_turn()
    -> Result<()> {
        let queue = Queue::default();
        let active = queue.acquire().await?;
        let mut waiting = Box::pin(queue.acquire());
        // Hours of paused time pass; the waiter is still waiting, not failed.
        assert!(
            timeout(Duration::from_secs(4 * 3600), waiting.as_mut())
                .await
                .is_err()
        );
        assert_eq!(queue.0.slots.available_permits(), CAPACITY - 2);
        queue.require(&active)?;
        drop(active);
        drop(timeout(Duration::from_secs(1), waiting.as_mut()).await??);
        drop(queue.try_acquire()?);
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn shutdown_wakes_waiters_rejects_old_turns_and_never_reopens() -> Result<()> {
        let queue = Queue::default();
        let active = queue.acquire().await?;
        let mut waiting = Box::pin(queue.acquire());
        assert!(
            timeout(Duration::from_millis(1), waiting.as_mut())
                .await
                .is_err()
        );
        queue.close();
        assert!(waiting.await.is_err());
        assert!(queue.require(&active).is_err());
        assert!(queue.try_acquire().is_err());
        drop(active);
        queue.close();
        assert!(queue.acquire().await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn turns_cannot_cross_service_queues_and_drop_signals_closure() -> Result<()> {
        let queue = Queue::default();
        let other = Queue::default();
        let active = queue.acquire().await?;
        assert!(other.require(&active).is_err());
        drop(queue);
        assert!(*active.owner.stopped.borrow());
        Ok(())
    }
}
