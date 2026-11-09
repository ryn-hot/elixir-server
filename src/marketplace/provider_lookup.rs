//! One owned metadata-only rendezvous lookup. Dropping its waiter cannot
//! release the existing expensive-job lane before the actual RPC completes.
use super::{
    lease::ControllerLease,
    worker::{ParticipationClient, Retirement},
};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{identity::Digest, worker_network::PeerKey};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// Only the original local request owner constructs this. A peer cannot send
/// cleanup flags. A before-dispatch rejection or an actual private RPC reply
/// finishes metadata work; queue/IPC timeout and lost owners do not prove that.
pub(in crate::marketplace) struct Completion {
    reply: Option<Result<Vec<PeerKey>>>,
    uncertainty: Option<Arc<anyhow::Error>>,
    retired: bool,
}
impl Completion {
    pub(in crate::marketplace) fn not_dispatched(error: anyhow::Error) -> Self {
        Self::replied(Err(error))
    }
    pub(in crate::marketplace) fn replied(reply: Result<Vec<PeerKey>>) -> Self {
        Self {
            reply: Some(reply),
            uncertainty: None,
            retired: false,
        }
    }
    pub(in crate::marketplace) fn unconfirmed(error: anyhow::Error) -> Self {
        Self {
            reply: None,
            uncertainty: Some(Arc::new(error)),
            retired: false,
        }
    }
    /// Authority may disappear after a real reply. Reject its hints without
    /// turning the already-finished RPC into an unknown cleanup outcome.
    pub(in crate::marketplace) fn revalidate(mut self, current: Result<()>) -> Self {
        if let Some(reply) = self.reply.take() {
            self.reply = Some(reply.and_then(|peers| current.map(|()| peers)));
        }
        self
    }
    fn cleanup(&self) -> Result<()> {
        if let Some(error) = &self.uncertainty
            && !self.retired
        {
            bail!("provider lookup cleanup remains unconfirmed: {error:#}");
        }
        Ok(())
    }
    fn take_reply(&mut self) -> Result<Vec<PeerKey>> {
        // Retirement proves cleanup ONLY. The original failed operation must
        // never turn into an empty or successful provider result afterwards.
        if let Some(error) = &self.uncertainty {
            bail!("provider lookup outcome remains unconfirmed: {error:#}");
        }
        self.cleanup()?;
        self.reply
            .take()
            .context("provider lookup result was already consumed")?
    }
}
enum Joined {
    Running(JoinHandle<Completion>),
    Done(Completion),
}
impl Joined {
    async fn join(&mut self) -> &mut Completion {
        if let Self::Running(task) = self {
            let result = task.await;
            *self = Self::Done(match result {
                Ok(completion) => completion,
                Err(error) => Completion::unconfirmed(
                    anyhow::Error::new(error).context("owned provider lookup task stopped"),
                ),
            });
        }
        let Self::Done(completion) = self else {
            unreachable!("original task was joined")
        };
        completion
    }
}
pub(super) struct Lookup {
    client: ParticipationClient,
    sha256: Digest,
    task: Joined,
    retirement_deadline: Option<Instant>,
}
impl Lookup {
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_unconfirmed(client: &ParticipationClient) -> Self {
        Self {
            client: client.clone(),
            sha256: Digest::of(b"synthetic interrupted metadata lookup"),
            task: Joined::Done(Completion::unconfirmed(anyhow::anyhow!(
                "original synthetic RPC outcome unknown"
            ))),
            retirement_deadline: None,
        }
    }
    pub(super) fn begin(
        client: &ParticipationClient,
        sha256: Digest,
        lease: Arc<ControllerLease>,
    ) -> Self {
        let owner = client.clone();
        let task = tokio::spawn(async move {
            let _lease = lease;
            owner.find_blob_providers(sha256).await
        });
        Self {
            client: client.clone(),
            sha256,
            task: Joined::Running(task),
            retirement_deadline: None,
        }
    }
    pub(super) fn matches(&self, client: &ParticipationClient, sha256: Digest) -> bool {
        self.sha256 == sha256 && self.client.same_owner(client)
    }
    pub(super) async fn wait(&mut self) -> Result<Vec<PeerKey>> {
        let peers = self.task.join().await.take_reply()?;
        elixir_marketplace::worker_blob::validate_provider_hints(&peers)?;
        Ok(peers)
    }
    pub(super) async fn finish(&mut self) -> Result<()> {
        let completion = self.task.join().await;
        if let Some(original) = &completion.uncertainty
            && !completion.retired
        {
            // One original deadline survives cancellation/repeated cleanup.
            // Waiting observes the same owner; it starts no RPC/worker/credit.
            let expires = *self
                .retirement_deadline
                .get_or_insert_with(|| Instant::now() + Duration::from_secs(72));
            let proof = self
                .client
                .original_retirement(expires)
                .await
                .with_context(|| {
                    format!("provider lookup cleanup remains unconfirmed: {original:#}")
                })?;
            if !self.client.accepts_retirement(&proof) {
                bail!("provider lookup retirement belongs to a different owner");
            }
            completion.confirm_retirement(proof);
        }
        completion.cleanup()?;
        // Cleanup seals any unread hints; it is not a second result consumer
        // or permission to use an old reply after disposing the source job.
        completion.reply = None;
        Ok(())
    }
    // No Drop abort. The task retains the SAME lane across waiter loss until
    // its bounded RPC completes; unknown results cannot become positive cleanup.
}

impl Completion {
    fn confirm_retirement(&mut self, _proof: Retirement) {
        self.retired = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_retirement_preserves_original_rpc_cause_across_repeated_cleanup() {
        let owner = crate::marketplace::worker::synthetic_blob_owner();
        let mut lookup = Lookup::fixture_unconfirmed(&owner.client);
        let first = format!("{:#}", lookup.finish().await.unwrap_err());
        assert!(
            first.contains("original synthetic RPC outcome unknown"),
            "{first}"
        );
        assert!(
            first.contains("original protected cleanup owner stopped without proof"),
            "{first}"
        );
        assert_eq!(format!("{:#}", lookup.finish().await.unwrap_err()), first);
        assert!(
            format!("{:#}", lookup.wait().await.unwrap_err())
                .contains("original synthetic RPC outcome unknown")
        );
    }

    #[test]
    fn operation_failure_and_completion_provenance_are_independent() -> Result<()> {
        for mut completion in [
            Completion::not_dispatched(anyhow::anyhow!("rejected before IPC")),
            Completion::replied(Err(anyhow::anyhow!("actual reply rejected"))),
            Completion::replied(Ok(vec![]))
                .revalidate(Err(anyhow::anyhow!("authority withdrawn after reply"))),
        ] {
            assert!(completion.take_reply().is_err());
            completion.cleanup()?;
            assert!(completion.take_reply().is_err(), "one result, not replay");
            completion.cleanup()?;
        }
        let mut unknown = Completion::unconfirmed(anyhow::anyhow!("actual IPC outcome unknown"));
        for _ in 0..2 {
            assert!(unknown.take_reply().is_err());
            assert!(
                format!("{:#}", unknown.cleanup().unwrap_err())
                    .contains("actual IPC outcome unknown")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_join_waiter_keeps_the_original_completion_and_never_repolls_it() -> Result<()>
    {
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut joined = Joined::Running(tokio::spawn(async move {
            receive.await.expect("fixture must retain its sender")
        }));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), joined.join())
                .await
                .is_err()
        );
        send.send(Completion::not_dispatched(anyhow::anyhow!(
            "original before-dispatch error"
        )))
        .ok();
        assert!(
            format!("{:#}", joined.join().await.take_reply().unwrap_err())
                .contains("original before-dispatch error")
        );
        joined.join().await.cleanup()?;
        joined.join().await.cleanup()?;
        Ok(())
    }

    #[tokio::test]
    async fn panicked_lookup_join_never_acknowledges_cleanup_or_loses_its_original_failure() {
        let mut joined = Joined::Running(tokio::spawn(async { panic!("original lookup panic") }));
        let first = joined.join().await.uncertainty.as_ref().unwrap().clone();
        let second = joined.join().await.uncertainty.as_ref().unwrap().clone();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(format!("{first:#}").contains("original lookup panic"));
        assert!(joined.join().await.cleanup().is_err());
        assert!(joined.join().await.take_reply().is_err());
    }
}
