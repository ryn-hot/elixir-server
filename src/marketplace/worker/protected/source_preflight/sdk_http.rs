//! Fixed owned HTTP effect, not installed-grant/native-execution authority.
//! Shares the existing helper slot and worker allocation. No host fallback,
//! persistent queue, cache, retry, cookie jar or new packet allowance.
use super::*;
use elixir_marketplace::{
    sdk_http::{HttpReply, HttpRequest, MAX_NATIVE_HTTP_OUTPUT, NativeHttpExit},
    source_execution::{RuntimeJson, SourceSdkReply},
};
use tokio::task::JoinHandle;
use zeroize::Zeroizing;

const HTTP_RETIRE_DEADLINE: Duration = Duration::from_secs(25);
const AUTHORITY_DEADLINE: Duration = Duration::from_secs(5);
const AUTHORITY_INTERVAL: Duration = Duration::from_millis(500);
const READY_DEADLINE: Duration = Duration::from_secs(8);

mod stream;

/// Trusted server lifecycle only. Implementations must retain the ORIGINAL
/// native/installed authority and freshly check it, not accept a DTO flag.
/// Worker image/protection ownership remains independently checked here.
#[async_trait::async_trait]
pub(in crate::marketplace) trait SdkHttpAuthority: Send + Sync {
    async fn current(&self) -> Result<()>;

    /// Fresh composition for an exact original native owner, not a cached
    /// approval/inspection token. The conservative default checks both.
    /// An implementation that already inspects this SAME native owner in
    /// current() may combine the checks, but must reject a different owner.
    async fn current_native(
        &self,
        native: &crate::marketplace::quarantine::execution::NativeInvocation,
    ) -> Result<()> {
        native.current().await?;
        self.current().await?;
        native.check()
    }
}
async fn authorized(authority: &dyn SdkHttpAuthority) -> Result<()> {
    timeout(AUTHORITY_DEADLINE, authority.current())
        .await
        .context("SDK HTTP authority check deadline")?
}

/// Only the held protected worker constructs this; data/DTOs cannot do so.
#[derive(Clone)]
pub(in crate::marketplace) struct SdkHttpClient(Arc<Owner>);
impl Client {
    pub(in crate::marketplace::worker::protected) fn sdk_http(&self) -> Option<SdkHttpClient> {
        self.0
            .binding
            .sdk_http
            .then(|| SdkHttpClient(self.0.clone()))
    }
}
impl Owner {
    fn admit_http(&self, ready: bool) -> Result<()> {
        if !self.binding.sdk_http {
            bail!("protected worker does not support SDK HTTP");
        }
        self.admit_foreground(ready)
    }

    /// Routine renewal is a scheduling wait, not permission to send while
    /// Checking. Recheck authority across every await and require Ready at the
    /// final input boundary. Callers bound this wait and retain the same call.
    async fn http_ready(
        &self,
        cancel: &mut oneshot::Sender<()>,
        authority: &dyn SdkHttpAuthority,
    ) -> Result<bool> {
        let mut protection = self.protection.clone();
        loop {
            self.admit_http(false)?;
            if cancel.is_closed() {
                return Ok(false);
            }
            if observed_state(&protection) == ProtectionState::Ready {
                authorized(authority).await?;
                self.admit_http(false)?;
                if cancel.is_closed() {
                    return Ok(false);
                }
                if observed_state(&protection) == ProtectionState::Ready {
                    return Ok(true);
                }
            }
            tokio::select! {biased;
                _ = cancel.closed() => return Ok(false),
                result = protection.changed() => result.context("SDK HTTP protection owner stopped")?,
            }
        }
    }
}
struct Completion {
    // This is set by the original task only after the actual CLI wait AND the
    // native parent's post-wait marker on its private pipe. Never infer it from
    // absence, elapsed time, a status DTO or a fabricated successful result.
    retired: bool,
    result: Option<Result<SourceSdkReply>>,
}
/// Keep outside the fallible/cancellable source invocation, then cancel/join it
/// before reusing that invocation's controller. A failed/unknown original join
/// is sticky: no caller may turn it into an idle/no-effect acknowledgement.
pub(in crate::marketplace) struct SdkHttpCall {
    lifeline: Option<oneshot::Receiver<()>>,
    task: Option<JoinHandle<Completion>>,
    completed: Option<Completion>,
    join_failed: bool,
}
impl SdkHttpClient {
    /// Trusted controller only: must have independently checked the installed
    /// instance, approved network.egress, actual native executor and request.
    /// This function does NOT confer those authorities or accept their DTOs.
    pub(in crate::marketplace) fn start(
        &self,
        raw: &RuntimeJson,
        maximum: usize,
        authority: Arc<dyn SdkHttpAuthority>,
    ) -> Result<SdkHttpCall> {
        // Reserving the one owned slot starts no network effect. A routine
        // renewal may finish inside this call's bounded pre-input wait.
        self.0.admit_http(false)?;
        let request = HttpRequest::new(
            raw,
            maximum,
            Digest::of(uuid::Uuid::new_v4().as_bytes()),
            self.0.binding.family,
        )?;
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("protected HTTP helper slot is busy")?;
        let (mut cancel, lifeline) = oneshot::channel();
        let owner = self.0.clone();
        let task = tokio::runtime::Handle::try_current()
            .context("SDK HTTP owner unavailable")?
            .spawn(async move {
                let _permit = permit;
                let mut retired = true; // No helper has been launched yet.
                let result = owner
                    .run_http(request, &mut cancel, &mut retired, authority.as_ref())
                    .await;
                Completion {
                    retired,
                    result: Some(result),
                }
            });
        Ok(SdkHttpCall {
            lifeline: Some(lifeline),
            task: Some(task),
            completed: None,
            join_failed: false,
        })
    }
}
impl SdkHttpCall {
    fn request_stop(&mut self) {
        drop(self.lifeline.take());
    }
    async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("original SDK HTTP owner join failed");
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            match result {
                Ok(result) => self.completed = Some(result),
                Err(_) => {
                    self.join_failed = true;
                    bail!("original SDK HTTP owner join failed");
                }
            }
        }
        if !self.completed.as_ref().is_some_and(|c| c.retired) {
            bail!("original SDK HTTP native retirement is unconfirmed");
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn finish(&mut self) -> Result<SourceSdkReply> {
        self.join().await?;
        self.completed
            .as_mut()
            .and_then(|c| c.result.take())
            .context("SDK HTTP result already consumed")?
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.request_stop();
        self.join().await?;
        self.completed
            .as_mut()
            .expect("joined original HTTP owner")
            .result = None;
        Ok(())
    }
}
impl Drop for SdkHttpCall {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl Owner {
    async fn run_http(
        self: &Arc<Self>,
        request: HttpRequest,
        cancel: &mut oneshot::Sender<()>,
        retired: &mut bool,
        authority: &dyn SdkHttpAuthority,
    ) -> Result<SourceSdkReply> {
        if cancel.is_closed() {
            return Ok(SourceSdkReply::Unavailable);
        }
        authorized(authority).await?;
        self.headroom().await?;
        if !timeout(READY_DEADLINE, self.http_ready(cancel, authority))
            .await
            .context("SDK HTTP pre-launch readiness deadline")??
        {
            return Ok(SourceSdkReply::Unavailable);
        }
        let bytes = Zeroizing::new(request.to_bytes()?);
        // Unknown launch/retirement is not safe to retry. Stop the independent
        // lease owner and preserve unconfirmed retirement in the original call.
        let mut fault = FaultOnDrop {
            owner: self.clone(),
            armed: true,
        };
        *retired = false;
        let process = timeout(
            Duration::from_secs(10),
            self.runtime
                .exec_marketplace_sdk_http(&self.handle, &self.profile.container_spec().labels),
        )
        .await
        .context("SDK HTTP launch deadline")??;
        let (output, diagnostic, status, cancelled) = timeout(
            HTTP_RETIRE_DEADLINE,
            exchange_http(process, &bytes, self, cancel, authority),
        )
        .await
        .context("SDK HTTP retirement deadline")??;
        let exit = NativeHttpExit::parse(&output)?;
        *retired = true;
        fault.armed = false;
        if cancelled || !exit.succeeded || !status.success() {
            tracing::debug!(
                cancelled,
                native_success = exit.succeeded,
                cli_success = status.success(),
                "SDK HTTP retired without a usable reply"
            );
            return Ok(SourceSdkReply::Unavailable);
        }
        // Retired HTTP plus a revoked grant is a normal denial, not a reason
        // to permanently fault the shared marketplace actor. Only actual
        // protocol/kernel/ownership integrity failures arm the fail-stop here.
        let mut integrity = FaultOnDrop {
            owner: self.clone(),
            armed: true,
        };
        diagnostic?;
        let reply = HttpReply::parse(
            exit.report()?.context("SDK HTTP report missing")?,
            &request,
            &self.binding.context(&self.profile),
        )?;
        timeout(
            Duration::from_secs(5),
            inspect_fixed_worker(self.runtime.as_ref(), &self.handle, &self.profile),
        )
        .await
        .context("SDK HTTP ownership readback deadline")??;
        integrity.armed = false;
        self.admit_http(false)?;
        authorized(authority).await?;
        Ok(reply)
    }
}
// One bounded stream through EOF plus actual original CLI wait. Only the native
// parent's trailer later confirms its HTTP child was reaped, even on failure.
async fn exchange_http(
    process: PrivateProcessStdio,
    bytes: &[u8],
    owner: &Owner,
    cancel: &mut oneshot::Sender<()>,
    authority: &dyn SdkHttpAuthority,
) -> Result<(
    Zeroizing<Vec<u8>>,
    Result<()>,
    std::process::ExitStatus,
    bool,
)> {
    let PrivateProcessStdio {
        child,
        mut input,
        output,
        diagnostics,
    } = process;
    let mut child = Child(child);
    // A spawned helper is still owned even if permission disappears here.
    // Deny the input, close its lifeline, and join its actual native marker.
    let allowed = match timeout(READY_DEADLINE, owner.http_ready(cancel, authority)).await {
        Ok(Ok(allowed)) => allowed,
        Ok(Err(error)) => {
            tracing::debug!(reason = %error, "SDK HTTP input admission denied");
            false
        }
        Err(_) => {
            tracing::debug!("SDK HTTP input readiness deadline");
            false
        }
    };
    let written = if allowed {
        timeout(Duration::from_secs(3), async {
            input.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
            input.write_all(bytes).await?;
            input.flush().await
        })
        .await
    } else {
        Ok(Err(std::io::Error::other("SDK HTTP input not authorized")))
    };
    let mut cancelled = !matches!(written, Ok(Ok(())));
    let mut input = Some(input);
    if cancelled {
        drop(input.take());
    }
    let read = async {
        let mut output = output.take((MAX_NATIVE_HTTP_OUTPUT + 1) as u64);
        let mut bytes = Zeroizing::new(Vec::new());
        output.read_to_end(&mut bytes).await?;
        Ok::<_, anyhow::Error>(bytes)
    };
    let diagnostics = async {
        let mut stream = diagnostics
            .context("SDK HTTP diagnostic pipe missing")?
            .take(1025);
        let mut bytes = Zeroizing::new(Vec::new());
        stream.read_to_end(&mut bytes).await?;
        if bytes.len() > 1024 {
            bail!("SDK HTTP diagnostic bounds");
        }
        Ok(())
    };
    let retired = async { tokio::join!(read, diagnostics, child.0.wait()) };
    tokio::pin!(retired);
    let mut protection = owner.protection.clone();
    let mut check = tokio::time::interval(AUTHORITY_INTERVAL);
    let (output, diagnostic, status) = loop {
        tokio::select! {biased;
            _=cancel.closed(),if !cancelled=>{cancelled=true;drop(input.take());},
            _=protection.changed(),if !cancelled=>{if owner.admit_http(false).is_err(){cancelled=true;drop(input.take());}},
            _=check.tick(),if !cancelled=>{if owner.admit_http(false).is_err() || authorized(authority).await.is_err(){cancelled=true;drop(input.take());}},
            result=&mut retired=>break result,
        }
    };
    drop(input);
    Ok((
        output?,
        diagnostic,
        status.context("SDK HTTP original CLI wait failed")?,
        cancelled,
    ))
}

#[cfg(test)]
mod tests;
