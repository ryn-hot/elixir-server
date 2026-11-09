//! One actual attach task and its original private pipes. No network/grant
//! authority, SDK codec, retry, detached reader or caller-supplied joined flag.
use super::*;
use crate::runtime::PrivateProcessStdio;
use elixir_marketplace::sdk_http::{
    MAX_HTTP_REPLY, NATIVE_EXIT_MARKER, NativeHttpExit,
    stream::{MAX_STREAM_COMMAND, MAX_STREAM_COMMANDS, MAX_STREAM_OUTPUT},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroize::Zeroizing;

const UNUSED: u8 = 0;
const PENDING: u8 = 1;
const JOINED: u8 = 2;
const SEALED: u8 = 3;
/// Private atomic admission/retirement gate, not native or VPN evidence. A
/// retire claim and a new attachment can never both win the UNUSED state.
pub(super) struct Tracking(std::sync::atomic::AtomicU8);
impl Tracking {
    pub(super) fn new() -> Self {
        Self(std::sync::atomic::AtomicU8::new(UNUSED))
    }
    fn start(&self) -> Result<()> {
        self.0
            .compare_exchange(UNUSED, PENDING, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                anyhow::anyhow!("application HTTP attachment is already started or sealed")
            })?;
        Ok(())
    }
    fn complete(&self) -> Result<()> {
        self.0
            .compare_exchange(PENDING, JOINED, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                anyhow::anyhow!("application HTTP join does not own the original attachment")
            })?;
        Ok(())
    }
    #[cfg(test)]
    fn pending(&self) -> bool {
        self.0.load(Ordering::SeqCst) == PENDING
    }
    pub(super) fn seal_for_retirement(&self) -> bool {
        loop {
            let state = self.0.load(Ordering::SeqCst);
            match state {
                PENDING => return false,
                SEALED => return true,
                UNUSED | JOINED => {
                    if self
                        .0
                        .compare_exchange(state, SEALED, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        return true;
                    }
                }
                _ => return false,
            }
        }
    }
}

enum Stage {
    Empty,
    Starting(JoinHandle<Result<PrivateProcessStdio>>),
    Connected(Box<Pipes>),
    Failed,
    Joined(bool),
}
pub(super) struct Owner {
    stage: Stage,
    tracking: Arc<Tracking>,
}
impl Owner {
    pub(super) fn new(tracking: Arc<Tracking>) -> Self {
        Self {
            stage: Stage::Empty,
            tracking,
        }
    }
    pub(super) fn empty(&self) -> bool {
        matches!(self.stage, Stage::Empty)
    }
    pub(super) fn poison(&mut self) {
        if let Stage::Connected(pipes) = &mut self.stage {
            pipes.poisoned = true;
        }
    }
    pub(super) fn begin(
        &mut self,
        runtime: Arc<dyn RuntimeManager>,
        handle: ContainerHandle,
        profile: Arc<ApplicationHttpProfile>,
        expires: Instant,
    ) -> Result<()> {
        if !self.empty() {
            bail!("application HTTP attachment already attempted");
        }
        let executor = tokio::runtime::Handle::try_current()?;
        self.tracking.start()?;
        self.stage = Stage::Starting(executor.spawn(async move {
            timeout_at(expires, runtime.attach_application_http(&handle, &profile))
                .await
                .context("original application HTTP attachment deadline")?
        }));
        Ok(())
    }
    pub(super) async fn connect(&mut self) -> Result<()> {
        if let Stage::Starting(task) = &mut self.stage {
            let outcome = task.await;
            self.stage = Stage::Failed;
            // A failed/unknown attachment never pretends that pipes joined.
            // The durable native fence remains for explicit recovery.
            let process = outcome.context("original HTTP attach task failed")??;
            self.stage = Stage::Connected(Box::new(Pipes::new(process)));
        }
        if !matches!(self.stage, Stage::Connected(_)) {
            bail!("original application HTTP attachment unavailable");
        }
        Ok(())
    }
    pub(super) async fn exchange(&mut self, bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        let Stage::Connected(pipes) = &mut self.stage else {
            bail!("application HTTP pipes are not attached");
        };
        pipes.exchange(bytes).await
    }
    pub(super) async fn retire(&mut self) -> Result<Option<bool>> {
        if matches!(self.stage, Stage::Empty) {
            return Ok(None);
        }
        if let Stage::Joined(valid) = self.stage {
            return Ok(Some(valid));
        }
        self.connect().await?;
        let Stage::Connected(pipes) = &mut self.stage else {
            unreachable!()
        };
        let valid = pipes.join().await?;
        self.tracking.complete()?;
        self.stage = Stage::Joined(valid);
        Ok(Some(valid))
    }
}
struct Pipes {
    child: tokio::process::Child,
    input: Option<tokio::process::ChildStdin>,
    output: Option<tokio::process::ChildStdout>,
    diagnostics: Option<JoinHandle<bool>>,
    diagnostic_ok: Option<bool>,
    status: Option<std::process::ExitStatus>,
    tail: Zeroizing<Vec<u8>>,
    total: usize,
    commands: u32,
    poisoned: bool,
    final_start: Option<usize>,
}
impl Pipes {
    fn new(process: PrivateProcessStdio) -> Self {
        let PrivateProcessStdio {
            child,
            input,
            output,
            diagnostics,
        } = process;
        let diagnostics = tokio::spawn(async move {
            let Some(stderr) = diagnostics else {
                return false;
            };
            let mut bytes = Zeroizing::new(Vec::new());
            stderr.take(1025).read_to_end(&mut bytes).await.is_ok() && bytes.len() <= 1024
        });
        Self {
            child,
            input: Some(input),
            output: Some(output),
            diagnostics: Some(diagnostics),
            diagnostic_ok: None,
            status: None,
            tail: Zeroizing::new(Vec::new()),
            total: 0,
            commands: 0,
            poisoned: false,
            final_start: None,
        }
    }
    async fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
        let n = self
            .output
            .as_mut()
            .context("original HTTP stdout closed")?
            .read(bytes)
            .await?;
        if n > (MAX_STREAM_OUTPUT + NATIVE_EXIT_MARKER.len() + 1).saturating_sub(self.total) {
            self.poisoned = true;
            bail!("application HTTP aggregate output exceeded");
        }
        self.total += n;
        let keep = NATIVE_EXIT_MARKER.len() + 1;
        let skip = (self.tail.len() + n).saturating_sub(keep);
        let from_tail = skip.min(self.tail.len());
        self.tail.drain(..from_tail);
        self.tail.extend_from_slice(&bytes[skip - from_tail..n]);
        Ok(n)
    }
    async fn exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        let mut read = 0;
        while read < bytes.len() {
            let n = self.read(&mut bytes[read..]).await?;
            if n == 0 {
                bail!("application HTTP truncated frame");
            }
            read += n;
        }
        Ok(())
    }
    async fn exchange(&mut self, bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        if self.poisoned
            || self.final_start.is_some()
            || bytes.is_empty()
            || bytes.len() > MAX_STREAM_COMMAND
            || self.commands >= MAX_STREAM_COMMANDS
        {
            bail!("application HTTP exchange unavailable or over budget");
        }
        self.poisoned = true;
        self.commands += 1;
        let input = self
            .input
            .as_mut()
            .context("application HTTP stdin closed")?;
        input.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        input.write_all(bytes).await?;
        input.flush().await?;
        let mut header = [0; 4];
        self.exact(&mut header).await?;
        if header == NATIVE_EXIT_MARKER[..4] {
            // A failed child can emit ONLY its post-wait trailer. This is not
            // a length-prefixed response or success/retirement proof. Preserve
            // the SAME poisoned owner and drain/join it through normal cleanup.
            bail!("application HTTP native process ended before its framed reply");
        }
        let length = u32::from_be_bytes(header) as usize;
        if length == 0 || length > MAX_HTTP_REPLY {
            bail!("application HTTP reply frame bounds");
        }
        let mut bytes = Zeroizing::new(vec![0; length]);
        self.exact(&mut bytes).await?;
        if self
            .diagnostics
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            self.join_diagnostics().await;
            if self.diagnostic_ok != Some(true) {
                bail!("application HTTP diagnostics invalid");
            }
        }
        self.poisoned = false;
        Ok(bytes)
    }
    async fn join_diagnostics(&mut self) {
        if let Some(task) = self.diagnostics.as_mut() {
            let joined = task.await;
            self.diagnostics = None;
            self.diagnostic_ok = Some(matches!(joined, Ok(true)));
        }
    }
    async fn join(&mut self) -> Result<bool> {
        self.final_start.get_or_insert(self.total);
        drop(self.input.take());
        let mut bytes = Zeroizing::new([0u8; 16384]);
        while self.output.is_some() {
            match self.read(bytes.as_mut_slice()).await {
                Ok(0) => {
                    self.output = None;
                }
                Ok(_) => (),
                Err(_) => {
                    self.poisoned = true;
                    self.output = None;
                }
            }
        }
        if self.status.is_none() {
            self.status = Some(
                self.child
                    .wait()
                    .await
                    .context("original HTTP attach process wait failed")?,
            );
        }
        self.join_diagnostics().await;
        // Actual wait and every original pipe are joined even for invalid
        // frames/markers/diagnostics. Such completion allows cleanup, NOT data.
        Ok(!self.poisoned
            && self.status.is_some_and(|s| s.success())
            && self.diagnostic_ok == Some(true)
            && self.total.saturating_sub(self.final_start.unwrap()) == NATIVE_EXIT_MARKER.len() + 1
            && NativeHttpExit::parse(&self.tail).is_ok_and(|v| v.succeeded && v.output.is_empty()))
    }
}

#[cfg(test)]
mod tests;
