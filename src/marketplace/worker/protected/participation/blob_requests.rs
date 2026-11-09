//! One unsent local queue reservation, not a second request or transport retry.
//! Capacity wait and acknowledgement share the original thirty-second budget.
use super::*;
use tokio::time::timeout_at;

/// Private, non-clonable capacity. Dropping it sends nothing. The transfer owner
/// dispatches START while holding its synchronous start/cancel decision lock.
pub(in crate::marketplace) struct Slot {
    client: ParticipationClient,
    permit: mpsc::OwnedPermit<Request>,
    operation: WorkerControlOperation,
    deadline: Instant,
}
pub(in crate::marketplace) struct Reply {
    received: oneshot::Receiver<Result<BlobReadState>>,
    deadline: Instant,
}
impl ParticipationClient {
    pub(in crate::marketplace) async fn reserve_blob(
        &self,
        operation: WorkerControlOperation,
    ) -> Result<Slot> {
        if !matches!(
            operation,
            WorkerControlOperation::BlobStart { .. }
                | WorkerControlOperation::BlobRead { .. }
                | WorkerControlOperation::BlobCancel { .. }
        ) {
            bail!("invalid private blob operation");
        }
        let checked = elixir_marketplace::worker_control::WorkerControlRequest {
            schema_version: 1,
            request_id: 1,
            control: operation,
        };
        checked.validate()?;
        if !matches!(checked.control, WorkerControlOperation::BlobCancel { .. }) {
            self.require_blob_owner()?;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        let queue = if transfer_io(&checked.control) {
            &self.transfer_requests
        } else {
            &self.requests
        };
        let permit = timeout_at(deadline, queue.clone().reserve_owned())
            .await
            .context("blob local capacity deadline before dispatch")?
            .context("blob request owner stopped before dispatch")?;
        Ok(Slot {
            client: self.clone(),
            permit,
            operation: checked.control,
            deadline,
        })
    }
}
impl Slot {
    pub(in crate::marketplace) fn send(self) -> Result<Reply> {
        if Instant::now() >= self.deadline {
            bail!("blob original deadline elapsed before dispatch");
        }
        if !matches!(self.operation, WorkerControlOperation::BlobCancel { .. }) {
            self.client.require_blob_owner()?;
        }
        let (send, received) = oneshot::channel();
        self.permit.send(Request::Blob(self.operation, send));
        Ok(Reply {
            received,
            deadline: self.deadline,
        })
    }
}
impl Reply {
    pub(in crate::marketplace) async fn wait(self) -> Result<BlobReadState> {
        timeout_at(self.deadline, self.received)
            .await
            .context("blob acknowledgement deadline")?
            .context("blob owner stopped")?
    }
}
