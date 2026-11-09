//! Bounded private-channel reader for an already reserved quarantine job.
//! Neither this stream nor EOF grants source, installation or sharing trust.
use super::worker::ParticipationClient;
use anyhow::{Context as _, Result, bail};
use elixir_marketplace::{
    identity::Digest,
    records::Blob,
    worker_blob::{BlobDownloadSpec, BlobReadState, decode_chunk},
    worker_control::WorkerControlOperation,
    worker_network::PeerKey,
};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
    time::{Sleep, sleep},
};

type Reply = Pin<Box<dyn Future<Output = Result<BlobReadState>> + Send>>;
#[derive(Clone, Copy, PartialEq, Eq)]
enum Requested {
    Waiting,
    Cancel,
    Complete,
}
struct Decision {
    armed: bool,
    requested: Requested,
    lease: Option<Arc<super::lease::ControllerLease>>,
}
enum Cleanup {
    Running(JoinHandle<Completion>),
    Done(Completion),
}
struct Completion {
    // A positive retirement proves cleanup only, never a successful transfer
    // or cancellation RPC. Keep the first operation failure independently.
    original: Option<Arc<anyhow::Error>>,
    cleanup: std::result::Result<(), Arc<anyhow::Error>>,
}
impl Cleanup {
    async fn join(&mut self) -> std::result::Result<(), Arc<anyhow::Error>> {
        if let Self::Running(task) = self {
            let completion = match task.await {
                Ok(result) => result,
                Err(error) => {
                    let error = Arc::new(
                        anyhow::Error::new(error).context("original blob cleanup task stopped"),
                    );
                    Completion {
                        original: Some(error.clone()),
                        cleanup: Err(error),
                    }
                }
            };
            tracing::debug!(
                original_failure = completion.original.is_some(),
                cleanup_confirmed = completion.cleanup.is_ok(),
                "original blob cleanup task joined"
            );
            *self = Self::Done(completion);
        }
        let Self::Done(completion) = self else {
            unreachable!("original cleanup task joined")
        };
        completion.cleanup.clone()
    }
}
struct Owner {
    client: ParticipationClient,
    job: Digest,
    decision: Arc<Mutex<Decision>>,
    wake: watch::Sender<()>,
    cleanup: AsyncMutex<Cleanup>,
}
impl Owner {
    fn request_cancel(&self) {
        let mut decision = self
            .decision
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if decision.requested != Requested::Complete {
            decision.requested = Requested::Cancel;
        }
        self.wake.send_replace(());
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.request_cancel();
    }
}
/// ONE retained cancellation task, never a second transfer or packet grant.
/// The workspace can join it even after a Reader/open/read future was dropped.
#[derive(Clone)]
pub(super) struct Transfer(Arc<Owner>);
impl Transfer {
    pub(super) fn retained(
        client: &ParticipationClient,
        lease: Arc<super::lease::ControllerLease>,
    ) -> Self {
        Self::begin(client, Some(lease))
    }
    fn begin(
        client: &ParticipationClient,
        lease: Option<Arc<super::lease::ControllerLease>>,
    ) -> Self {
        let job = Digest::of(uuid::Uuid::new_v4().as_bytes());
        let decision = Arc::new(Mutex::new(Decision {
            armed: false,
            requested: Requested::Waiting,
            lease,
        }));
        let (wake, mut changed) = watch::channel(());
        let source = decision.clone();
        let owner = client.clone();
        let task = tokio::spawn(async move {
            // Drop/cancellation of the caller cannot release the SAME global
            // lane before cancellation or ORIGINAL joined network retirement.
            let result = async {
                loop {
                    let (armed, requested) = {
                        let decision = source
                            .lock()
                            .map_err(|_| anyhow::anyhow!("blob cleanup decision unavailable"))?;
                        (decision.armed, decision.requested)
                    };
                    if requested == Requested::Complete {
                        return Ok(());
                    }
                    if requested == Requested::Cancel || changed.has_changed().is_err() {
                        if !armed {
                            return Ok(());
                        }
                        if !matches!(
                            owner
                                .control_blob(WorkerControlOperation::BlobCancel { job })
                                .await?,
                            BlobReadState::Cancelled
                        ) {
                            bail!("blob cancellation was not acknowledged");
                        }
                        return Ok(());
                    }
                    let _ = changed.changed().await;
                }
            }
            .await;
            let completion = match result {
                Err(original) => {
                    let original = Arc::new(original);
                    crate::marketplace::lifecycle::trace_failure("blob_cancellation", &original);
                    owner.fail_blob_cleanup();
                    // This SAME task retains its lane until original physical
                    // retirement. No retry/new job/credit/transfer authority.
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(72);
                    let cleanup = async {
                        let proof =
                            owner.original_retirement(deadline).await.with_context(|| {
                                format!("original blob cancellation failed: {original:#}")
                            })?;
                        if !owner.accepts_retirement(&proof) {
                            bail!("blob retirement belongs to another original owner");
                        }
                        Ok(())
                    }
                    .await
                    .map_err(Arc::new);
                    Completion {
                        original: Some(original),
                        cleanup,
                    }
                }
                Ok(()) => Completion {
                    original: None,
                    cleanup: Ok(()),
                },
            };
            if completion.cleanup.is_ok() {
                source
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .lease
                    .take();
            }
            // Unknown cleanup retains the same lane in the original owner.
            // Dropping a join waiter or seeing a late status cannot release it.
            completion
        });
        Self(Arc::new(Owner {
            client: client.clone(),
            job,
            decision,
            wake,
            cleanup: AsyncMutex::new(Cleanup::Running(task)),
        }))
    }
    async fn start(&self, peer: PeerKey, transfer: BlobDownloadSpec) -> Result<BlobReadState> {
        // Wait only for LOCAL unsent capacity, before the decision lock. A
        // concurrent cancel can still seal the original unstarted transfer;
        // acquiring capacity later cannot resurrect it or send a late START.
        let slot = self
            .0
            .client
            .reserve_blob(WorkerControlOperation::BlobStart {
                job: self.0.job,
                peer,
                transfer,
            })
            .await?;
        let reply = {
            let mut decision = self
                .0
                .decision
                .lock()
                .map_err(|_| anyhow::anyhow!("blob start decision unavailable"))?;
            if decision.armed || decision.requested != Requested::Waiting {
                bail!("blob transfer was already started or cancelled");
            }
            // Start+arm are one synchronous critical section. Cleanup cannot observe
            // an unarmed transfer after its start command was actually queued.
            let reply = slot.send()?;
            decision.armed = true;
            reply
        };
        reply.wait().await
    }
    fn complete(&self) -> Result<()> {
        let mut decision = self
            .0
            .decision
            .lock()
            .map_err(|_| anyhow::anyhow!("blob completion decision unavailable"))?;
        if !decision.armed || decision.requested != Requested::Waiting {
            bail!("blob completion lost its owner");
        }
        decision.requested = Requested::Complete;
        self.0.wake.send_replace(());
        Ok(())
    }
    pub(super) fn needs_cleanup(&self) -> bool {
        self.0
            .decision
            .lock()
            .map(|d| d.armed || d.requested != Requested::Waiting)
            .unwrap_or(true)
    }
    fn active(&self) -> bool {
        self.0
            .decision
            .lock()
            .is_ok_and(|d| d.armed && d.requested == Requested::Waiting)
    }
    pub(super) async fn cancel(&self) -> Result<()> {
        self.0.request_cancel();
        // Borrow the one join handle: cancellation of THIS wait never consumes
        // it or sends another cancel for a replacement job.
        let mut cleanup = self.0.cleanup.lock().await;
        if let Err(error) = cleanup.join().await {
            self.0.client.fail_blob_cleanup();
            bail!("protected blob cleanup is unconfirmed: {error:#}");
        }
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::marketplace) async fn original_cancellation_failure(
        &self,
    ) -> Option<Arc<anyhow::Error>> {
        let cleanup = self.0.cleanup.lock().await;
        match &*cleanup {
            Cleanup::Done(completion) => completion.original.clone(),
            Cleanup::Running(_) => None,
        }
    }
}
struct Cancellation {
    transfer: Transfer,
}
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.transfer.0.request_cancel();
    }
}
pub(super) struct Reader {
    cancellation: Cancellation,
    expected: Blob,
    offset: u64,
    buffer: Vec<u8>,
    cursor: usize,
    pending: Option<Reply>,
    backoff: Option<Pin<Box<Sleep>>>,
    eof: bool,
    failed: bool,
}
impl Reader {
    #[cfg(test)]
    pub(super) async fn open(
        client: &ParticipationClient,
        peer: PeerKey,
        transfer: BlobDownloadSpec,
    ) -> Result<Self> {
        Self::open_retained(Transfer::begin(client, None), peer, transfer).await
    }
    pub(super) async fn open_retained(
        transfer_owner: Transfer,
        peer: PeerKey,
        transfer: BlobDownloadSpec,
    ) -> Result<Self> {
        transfer.validate()?;
        let cancellation = Cancellation {
            transfer: transfer_owner,
        };
        let client = &cancellation.transfer.0.client;
        client.require_blob_owner()?;
        let expected = transfer.blob.clone();
        if !matches!(
            cancellation.transfer.start(peer, transfer).await?,
            BlobReadState::Started
        ) {
            bail!("blob start was not acknowledged");
        }
        client.require_blob_owner()?;
        Ok(Self {
            cancellation,
            expected,
            offset: 0,
            buffer: vec![],
            cursor: 0,
            pending: None,
            backoff: None,
            eof: false,
            failed: false,
        })
    }
    #[cfg(test)]
    pub(super) async fn cancel(&mut self) -> Result<()> {
        self.pending = None;
        self.backoff = None;
        self.failed = true;
        self.buffer.clear();
        self.cancellation.transfer.cancel().await
    }
    fn error(
        &mut self,
        reason: &'static str,
        source: Option<anyhow::Error>,
    ) -> Poll<io::Result<()>> {
        self.failed = true;
        self.pending = None;
        self.buffer.clear();
        self.cancellation.transfer.0.request_cancel();
        let message = format!("protected marketplace blob transfer failed: {reason}");
        let error = match source {
            Some(source) => source.context(message),
            None => anyhow::anyhow!(message),
        };
        Poll::Ready(Err(io::Error::other(error)))
    }
}
impl AsyncRead for Reader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.failed {
            return this.error("reader already failed", None);
        }
        if let Err(error) = this.cancellation.transfer.0.client.require_blob_owner() {
            return this.error("consumer owner unavailable", Some(error));
        }
        if this.eof {
            return Poll::Ready(Ok(()));
        }
        if !this.cancellation.transfer.active() {
            return this.error("original transfer inactive", None);
        }
        loop {
            if this.cursor < this.buffer.len() {
                let count = (this.buffer.len() - this.cursor).min(output.remaining());
                output.put_slice(&this.buffer[this.cursor..this.cursor + count]);
                this.cursor += count;
                this.offset += count as u64;
                if this.cursor == this.buffer.len() {
                    this.buffer.clear();
                    this.cursor = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if let Some(delay) = &mut this.backoff {
                if delay.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.backoff = None;
            }
            if this.pending.is_none() {
                let client = this.cancellation.transfer.0.client.clone();
                let job = this.cancellation.transfer.0.job;
                let offset = this.offset;
                this.pending = Some(Box::pin(async move {
                    client
                        .control_blob(WorkerControlOperation::BlobRead { job, offset })
                        .await
                }));
            }
            let state = match this.pending.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    this.pending = None;
                    match result {
                        Ok(state) => state,
                        Err(error) => return this.error("read acknowledgement", Some(error)),
                    }
                }
            };
            match state {
                BlobReadState::Data { offset, data } if offset == this.offset => {
                    let Ok(bytes) = decode_chunk(&data) else {
                        return this.error("invalid chunk encoding", None);
                    };
                    if bytes.len() as u64 > this.expected.size.saturating_sub(this.offset) {
                        return this.error("chunk exceeds signed size", None);
                    }
                    this.buffer = bytes;
                    this.cursor = 0;
                }
                BlobReadState::Pending { offset } if offset == this.offset => {
                    // A compromised child cannot spin the parent by returning
                    // empty pending replies instead of its normal long poll.
                    this.backoff = Some(Box::pin(sleep(Duration::from_millis(100))));
                }
                BlobReadState::Complete {
                    blob,
                    integrity_only: true,
                } if this.offset == this.expected.size
                    && blob.size == this.expected.size
                    && blob.sha256 == this.expected.sha256 =>
                {
                    this.eof = true;
                    if let Err(error) = this.cancellation.transfer.complete() {
                        return this.error("completion owner lost", Some(error));
                    }
                    return Poll::Ready(Ok(()));
                }
                BlobReadState::Failed { .. } => return this.error("remote transfer failed", None),
                _ => return this.error("receipt phase, offset or descriptor differs", None),
            }
        }
    }
}
