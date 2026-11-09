//! Owned original CLI and bounded/discarded private streams. Reader completion
//! is bookkeeping only; retirement additionally confirms native stop/removal.
use super::*;
use crate::runtime::PrivateProcessStdio;
use tokio::io::AsyncRead;
use zeroize::Zeroizing;

const OUTPUT_LIMIT: usize = 1024 * 1024;
pub(super) async fn discard(mut stream: impl AsyncRead + Unpin, maximum: usize) -> bool {
    let mut buffer = Zeroizing::new([0u8; 4096]);
    let mut total = 0usize;
    loop {
        let n = match stream.read(&mut buffer[..]).await {
            Ok(n) => n,
            Err(_) => return false,
        };
        if n == 0 {
            return true;
        }
        total = total.saturating_add(n);
        if total > maximum {
            return false;
        }
    }
}
pub(super) struct Pipes {
    child: tokio::process::Child,
    pub(super) input: Option<tokio::process::ChildStdin>,
    pub(super) output: Option<tokio::process::ChildStdout>,
    stdout: Option<JoinHandle<bool>>,
    stderr: Option<JoinHandle<bool>>,
    stdout_done: Option<bool>,
    stderr_done: Option<bool>,
    status: Option<std::process::ExitStatus>,
    join_failed: bool,
}
impl Pipes {
    #[cfg(test)]
    pub(super) fn status_code(&self) -> Option<i32> {
        self.status
            .as_ref()
            .and_then(std::process::ExitStatus::code)
    }
    pub(super) fn new(process: PrivateProcessStdio) -> Self {
        let PrivateProcessStdio {
            child,
            input,
            output,
            diagnostics,
        } = process;
        let stderr = tokio::spawn(async move {
            if let Some(stream) = diagnostics {
                discard(stream, OUTPUT_LIMIT).await
            } else {
                false
            }
        });
        Self {
            child,
            input: Some(input),
            output: Some(output),
            stdout: None,
            stderr: Some(stderr),
            stdout_done: None,
            stderr_done: None,
            status: None,
            join_failed: false,
        }
    }
    pub(super) fn drain_output(&mut self) {
        if let Some(output) = self.output.take() {
            self.stdout = Some(tokio::spawn(discard(output, OUTPUT_LIMIT)));
        }
    }
    pub(super) fn ended(&mut self) -> bool {
        self.stdout.as_ref().is_some_and(JoinHandle::is_finished)
            || self.stderr.as_ref().is_some_and(JoinHandle::is_finished)
            || !matches!(self.child.try_wait(), Ok(None))
    }
    pub(super) async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("original gateway reader task failed without confirmed join");
        }
        drop(self.input.take());
        self.drain_output();
        if self.stdout_done.is_none() {
            let result = self
                .stdout
                .as_mut()
                .context("original gateway stdout owner lost")?
                .await;
            self.stdout = None;
            match result {
                Ok(valid) => self.stdout_done = Some(valid),
                Err(error) => {
                    self.join_failed = true;
                    return Err(error).context("original gateway stdout join failed");
                }
            }
        }
        if self.stderr_done.is_none() {
            let result = self
                .stderr
                .as_mut()
                .context("original gateway stderr owner lost")?
                .await;
            self.stderr = None;
            match result {
                Ok(valid) => self.stderr_done = Some(valid),
                Err(error) => {
                    self.join_failed = true;
                    return Err(error).context("original gateway stderr join failed");
                }
            }
        }
        if self.status.is_none() {
            self.status = Some(self.child.wait().await?);
        }
        // A read/budget/protocol failure can be cleaned only after these ACTUAL
        // joins plus native stop. It can never publish a usable HTTP response.
        Ok(())
    }
}
