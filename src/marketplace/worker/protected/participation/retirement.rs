//! One bounded, private observation of the ORIGINAL protected owner's joined
//! cleanup. Neither public status nor a replacement owner can issue this proof.
use anyhow::{Context, Result, bail};
use std::sync::Arc;
use tokio::{
    sync::watch,
    time::{Instant, timeout_at},
};

#[derive(Clone, Copy)]
enum State {
    Pending,
    Confirmed,
    Unconfirmed,
}

pub(super) struct Owner {
    state: watch::Sender<State>,
}

#[derive(Clone)]
pub(super) struct Observation {
    original: Arc<()>,
    state: watch::Receiver<State>,
}

/// No serialization, public constructor, network result or installation grant.
pub(in crate::marketplace) struct Confirmed {
    original: Arc<()>,
}

pub(super) fn channel() -> (Owner, Observation) {
    let (state, observed) = watch::channel(State::Pending);
    (
        Owner { state },
        Observation {
            original: Arc::new(()),
            state: observed,
        },
    )
}

impl Owner {
    /// Call ONLY after consuming the original network.shutdown dependency.
    pub(super) fn complete(self, exit: &crate::marketplace::lifecycle::Exit) {
        self.state.send_replace(if exit.cleanup_confirmed() {
            State::Confirmed
        } else {
            State::Unconfirmed
        });
    }
}

impl Observation {
    pub(super) async fn wait(&self, expires: Instant) -> Result<Confirmed> {
        let mut state = self.state.clone();
        timeout_at(expires, async {
            loop {
                let current = *state.borrow_and_update();
                match current {
                    State::Confirmed => {
                        return Ok(Confirmed {
                            original: self.original.clone(),
                        });
                    }
                    State::Unconfirmed => bail!("original protected owner cleanup is unconfirmed"),
                    State::Pending => (),
                }
                state
                    .changed()
                    .await
                    .context("original protected cleanup owner stopped without proof")?;
            }
        })
        .await
        .context("original protected cleanup observation deadline")?
    }

    pub(super) fn accepts(&self, proof: &Confirmed) -> bool {
        Arc::ptr_eq(&self.original, &proof.original)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marketplace::lifecycle::Exit;
    use std::time::Duration;

    #[tokio::test]
    async fn only_original_joined_cleanup_issues_a_proof_even_if_the_operation_failed() -> Result<()>
    {
        let (owner, original) = channel();
        let (_, replacement) = channel();
        owner.complete(&Exit::new(
            Err(anyhow::anyhow!("original operation failed")),
            Ok(()),
        ));
        let proof = original
            .wait(Instant::now() + Duration::from_secs(1))
            .await?;
        assert!(original.accepts(&proof));
        assert!(original.clone().accepts(&proof));
        assert!(!replacement.accepts(&proof));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn uncertainty_owner_loss_and_cancelled_waiters_do_not_manufacture_or_renew_cleanup()
    -> Result<()> {
        let (owner, original) = channel();
        let expires = Instant::now() + Duration::from_secs(10);
        assert!(
            timeout_at(
                Instant::now() + Duration::from_secs(1),
                original.wait(expires)
            )
            .await
            .is_err()
        );
        owner.complete(&Exit::new(
            Ok(()),
            Err(anyhow::anyhow!("physical cleanup unknown")),
        ));
        assert!(original.wait(expires).await.is_err());
        let (owner, lost) = channel();
        drop(owner);
        assert!(lost.wait(expires).await.is_err());
        let (_owner, pending) = channel();
        assert!(pending.wait(expires).await.is_err());
        let now = Instant::now();
        assert!(pending.wait(expires).await.is_err());
        assert_eq!(
            Instant::now(),
            now,
            "an expired original deadline is never renewed"
        );
        Ok(())
    }
}
