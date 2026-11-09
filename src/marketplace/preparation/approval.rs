//! One owner request, not a queue, grant, execution token or approval history.
use super::*;
use crate::{
    http::auth::CurrentPrincipal,
    marketplace::grant_store::{Actor, ApprovalSelection},
};
use tokio::sync::oneshot;

pub(super) enum Request {
    Owner {
        actor: Actor,
        selection: ApprovalSelection,
    },
    #[cfg(unix)]
    Automatic {
        authorization: crate::marketplace::grant_store::installation::automatic::Authorization,
    },
}
struct Accepted {
    id: Digest,
    secrets: bool,
    automatic: bool,
}
pub(super) struct Slot {
    sender: Option<oneshot::Sender<Request>>,
    accepted: Option<Accepted>,
}
impl Slot {
    pub(super) fn channel() -> (Self, oneshot::Receiver<Request>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender: Some(sender),
                accepted: None,
            },
            receiver,
        )
    }
}
impl Service {
    /// Acknowledges ownership of ONE request, not a successful approval. The
    /// owned task commits against current authorization; callers poll its view.
    pub(in crate::marketplace) fn approve(
        &self,
        id: Uuid,
        principal: &CurrentPrincipal,
        selection: ApprovalSelection,
    ) -> Result<()> {
        self.enqueue_approval(id, selection, |secrets| {
            Actor::from_principal(principal, secrets)
        })
    }
    fn enqueue_approval(
        &self,
        id: Uuid,
        selection: ApprovalSelection,
        actor: impl FnOnce(bool) -> Result<Actor>,
    ) -> Result<()> {
        let entry = self.entry(Some(id))?;
        let mut slot = entry
            .approval
            .lock()
            .map_err(|_| anyhow::anyhow!("approval owner unavailable"))?;
        let secrets = if let Some(accepted) = &slot.accepted {
            if accepted.automatic {
                bail!("preparation already owns standing update authorization");
            }
            accepted.secrets
        } else {
            let view = self.view(id)?;
            if view.phase != Phase::AwaitingApproval {
                bail!("preparation is not awaiting approval");
            }
            let review = view.review.context("current permission review missing")?;
            if review.review_id()? != selection.review_id {
                bail!("owner selection does not match the current review");
            }
            !review.scope().secret_keys().is_empty()
        };
        let actor = actor(secrets)?;
        let request_id = actor.approval_id(&selection)?;
        if let Some(accepted) = &slot.accepted {
            if accepted.id == request_id {
                return Ok(());
            }
            bail!("one owner approval request is already owned");
        }
        slot.sender
            .take()
            .context("approval owner stopped")?
            .send(Request::Owner { actor, selection })
            .map_err(|_| anyhow::anyhow!("approval owner stopped"))?;
        slot.accepted = Some(Accepted {
            id: request_id,
            secrets,
            automatic: false,
        });
        Ok(())
    }
    /// Reuse an existing exact owner grant. This cannot create or widen one.
    #[cfg(unix)]
    pub(in crate::marketplace) fn approve_automatic(
        &self,
        id: Uuid,
        authorization: &crate::marketplace::grant_store::installation::automatic::Authorization,
    ) -> Result<()> {
        let entry = self.entry(Some(id))?;
        let mut slot = entry
            .approval
            .lock()
            .map_err(|_| anyhow::anyhow!("approval owner unavailable"))?;
        let request_id = authorization.request_id()?;
        if let Some(accepted) = &slot.accepted {
            if accepted.automatic && accepted.id == request_id {
                return Ok(());
            }
            bail!("one different preparation permission decision is already owned");
        }
        let view = self.view(id)?;
        if view.phase != Phase::AwaitingApproval || view.release_id != authorization.release() {
            bail!("preparation is not awaiting this automatic-update review");
        }
        authorization.require_review(&view.review.context("current permission review missing")?)?;
        slot.sender
            .take()
            .context("approval owner stopped")?
            .send(Request::Automatic {
                authorization: authorization.clone(),
            })
            .map_err(|_| anyhow::anyhow!("approval owner stopped"))?;
        slot.accepted = Some(Accepted {
            id: request_id,
            secrets: false,
            automatic: true,
        });
        Ok(())
    }
    /// Synthetic actor only in local composition tests. No production request
    /// accepts a serialized Actor or bypasses CurrentPrincipal's capabilities.
    #[cfg(test)]
    pub(super) fn approve_as(
        &self,
        id: Uuid,
        actor: Actor,
        selection: ApprovalSelection,
    ) -> Result<()> {
        self.enqueue_approval(id, selection, |_| Ok(actor))
    }
}
