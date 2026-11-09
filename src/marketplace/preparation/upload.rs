//! One original native quarantine input, not a package cache or HTTP task owner.
//! The native owner retains the reader/cleanup. Dropping an HTTP writer closes
//! input; it cannot mark bytes verified, retry a slot or reallocate a job.
use super::*;
use elixir_marketplace::records::Blob;
use tokio::{io::DuplexStream, sync::oneshot};

const BUFFER: usize = 64 * 1024;
#[derive(Clone, Serialize)]
pub struct UploadView {
    pub upload_id: Uuid,
    pub purpose: QuarantinePurpose,
    pub blob: Blob,
    pub claimed: bool,
}
#[derive(Clone)]
pub(super) struct Slot {
    state: Arc<Mutex<State>>,
    changed: watch::Sender<u64>,
}
#[derive(Default)]
struct State {
    waiting: Option<UploadView>,
    reader: Option<Input>,
    closed: bool,
}
pub(super) struct Input {
    id: Uuid,
    pub stream: DuplexStream,
    completion: oneshot::Sender<bool>,
}
pub(crate) struct UploadClaim {
    pub writer: DuplexStream,
    pub completion: oneshot::Receiver<bool>,
}
impl Default for Slot {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            changed: watch::channel(0).0,
        }
    }
}
impl Slot {
    fn notify(&self) {
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }
    pub(super) fn view(&self) -> Option<UploadView> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed {
            None
        } else {
            state.waiting.clone()
        }
    }
    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closed = true;
        state.reader = None;
        state.waiting = None;
        drop(state);
        self.notify();
    }
    pub(super) async fn receive(&self, purpose: QuarantinePurpose, blob: Blob) -> Result<Input> {
        blob.validate(purpose.maximum())?;
        let mut changed = self.changed.subscribe();
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("upload owner unavailable"))?;
            if state.closed || state.waiting.is_some() {
                bail!("original upload input is closed or already owned");
            }
            state.waiting = Some(UploadView {
                upload_id: Uuid::new_v4(),
                purpose,
                blob,
                claimed: false,
            });
        }
        self.notify();
        let waiting = async {
            loop {
                {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("upload owner unavailable"))?;
                    if state.closed {
                        bail!("original upload closed");
                    }
                    if let Some(input) = state.reader.take() {
                        return Ok(input);
                    }
                }
                changed
                    .changed()
                    .await
                    .context("original upload owner stopped")?;
            }
        };
        match tokio::time::timeout(Duration::from_secs(300), waiting).await {
            Ok(result) => result,
            Err(_) => {
                self.close();
                bail!("publisher did not supply its original input before the upload deadline");
            }
        }
    }
    pub(super) fn claim(&self, id: Uuid, size: u64) -> Result<UploadClaim> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("upload owner unavailable"))?;
        if state.closed {
            bail!("original upload closed");
        }
        let waiting = state
            .waiting
            .as_mut()
            .context("no source upload is waiting")?;
        if waiting.upload_id != id || waiting.claimed || waiting.blob.size != size {
            bail!("upload does not match the original signed slot or was already claimed");
        }
        let (writer, reader) = tokio::io::duplex(BUFFER);
        let (completion, receipt) = oneshot::channel();
        waiting.claimed = true;
        state.reader = Some(Input {
            id,
            stream: reader,
            completion,
        });
        drop(state);
        self.notify();
        Ok(UploadClaim {
            writer,
            completion: receipt,
        })
    }
    pub(super) fn complete(&self, input: Input, verified: bool) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("upload owner unavailable"))?;
        if state.closed
            || state
                .waiting
                .as_ref()
                .is_none_or(|v| v.upload_id != input.id || !v.claimed)
        {
            bail!("original upload acknowledgement changed");
        }
        state.waiting = None;
        state.closed = !verified;
        drop(state);
        let _ = input.completion.send(verified);
        self.notify();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
