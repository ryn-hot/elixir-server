//! Independent private-pipe DNS/HTTPS observations, not public-worker authority.
//! The fixed executable runs as UID 10005, separate from worker UID 10004.
//! Packet leases, namespace/ownership inspection and supervision remain required.
mod bootstrap;
pub(super) mod refresh;
pub use bootstrap::BOOTSTRAP_PROBE_BYTES;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    connectivity::{
        ConnectivityReply, ConnectivityRequest, MAX_CONNECTIVITY_REPLY, ProbeContext, ProbeFamily,
    },
    identity::Digest,
    network_probe::WireguardIdentity,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

use super::{
    packet::PacketGuardController, runtime::inspect_fixed_worker, storage::FixedWorkerSpec,
};
use crate::runtime::{PrivateProcessStdio, RuntimeManager, model::ContainerHandle};

pub(super) const MAX_OBSERVATION_AGE: Duration = Duration::from_secs(600);

struct ProbeJob(tokio::process::Child);

// Negative diagnostic only. A worker can claim any failure category; none
// grants health/protection authority. Never forward arbitrary stderr text.
fn probe_diagnostic(bytes: &[u8]) -> &'static str {
    // A confirmed nonzero child exit adds this exact fixed native-parent
    // trailer. Accept no other suffix/prefix and never print the raw text.
    const PARENT: &[u8] =
        b"marketplace kernel guard unavailable (invalid marketplace kernel profile)\n";
    if bytes == PARENT {
        return "native_guard";
    }
    let bytes = bytes.strip_suffix(PARENT).unwrap_or(bytes);
    match bytes {
        b"ELXPROBE1:context\n" => "context",
        b"ELXPROBE1:input\n" => "input",
        b"ELXPROBE1:dns\n" => "dns",
        b"ELXPROBE1:https_request\n" => "https_request",
        b"ELXPROBE1:https_response\n" => "https_response",
        b"ELXPROBE1:trace\n" => "trace",
        b"ELXPROBE1:deadline\n" => "deadline",
        b"ELXPROBE1:output\n" => "output",
        b"ELXPROBE1:unavailable\n" | b"marketplace connectivity probe unavailable\n" => {
            "unavailable"
        }
        b"" => "no_report",
        _ => "unclassified",
    }
}
impl Drop for ProbeJob {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

async fn exchange(
    process: PrivateProcessStdio,
    request: &ConnectivityRequest,
    context: &ProbeContext<'_>,
    deadline: Duration,
) -> Result<ConnectivityReply> {
    let PrivateProcessStdio {
        child,
        mut input,
        mut output,
        diagnostics,
    } = process;
    let mut job = ProbeJob(child);
    let operation = async {
        let bytes = request.to_bytes()?;
        input.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        input.write_all(&bytes).await?;
        input.flush().await?;
        // Docker attach to OpenStdin does not forward EOF. The version-2
        // helper starts on exactly one complete frame, keeping this original
        // input alive until the exchange is actually complete/cancelled.
        let diagnostic = diagnostics.context("missing connectivity diagnostics")?;
        let reply = async {
            let length = output.read_u32().await? as usize;
            if length == 0 || length > MAX_CONNECTIVITY_REPLY {
                bail!("connectivity reply bounds");
            }
            let mut body = vec![0; length];
            output.read_exact(&mut body).await?;
            if output.read(&mut [0; 1]).await? != 0 {
                bail!("trailing connectivity output");
            }
            Ok::<_, anyhow::Error>(body)
        };
        let diagnostics = async {
            let mut bytes = Vec::new();
            diagnostic.take(1025).read_to_end(&mut bytes).await?;
            if bytes.len() > 1024 {
                bail!("connectivity diagnostic overflow");
            }
            Ok::<_, anyhow::Error>(probe_diagnostic(&bytes))
        };
        let exit = async { Ok::<_, anyhow::Error>(job.0.wait().await?) };
        let (bytes, diagnostics, status) = tokio::join!(reply, diagnostics, exit);
        let diagnostic = diagnostics?;
        let status = status?;
        if !status.success() {
            bail!("independent connectivity probe failed ({status}; diagnostic={diagnostic})");
        }
        let bytes = bytes?;
        drop(input);
        Ok(ConnectivityReply::parse(&bytes, request, context)?)
    };
    timeout(deadline, operation)
        .await
        .context("independent connectivity probe deadline")?
}

/// Not Clone/Deserialize: only successful private process exchange can construct
/// it here. Still not authorization to renew a lease or open public sockets.
pub struct ConnectivityObservation {
    guard_id: String,
    epoch: Digest,
    started: Instant,
    report: ConnectivityReply,
}

/// Trusted local provider policy, not a peer-controlled destination/config.
#[derive(Clone, Copy)]
pub struct ProbePolicy {
    pub family: ProbeFamily,
    pub require_warp: bool,
}
impl ConnectivityObservation {
    pub(super) fn started_at(&self) -> Instant {
        self.started
    }
    pub fn report(&self) -> &ConnectivityReply {
        &self.report
    }
    pub fn is_recent_for(&self, guard: &PacketGuardController) -> bool {
        self.guard_id == guard.handle().id
            && self.epoch == guard.profile().policy().epoch
            && self.report.kernel.network_namespace_inode == guard.namespace_inode()
            && self.started.elapsed() <= MAX_OBSERVATION_AGE
            && guard.channel_usable()
    }
    fn check_policy(&self, guard: &PacketGuardController, policy: ProbePolicy) -> Result<()> {
        if !self.is_recent_for(guard)
            || self.report.family != policy.family
            || (policy.require_warp
                && self.report.warp == elixir_marketplace::connectivity::WarpState::Off)
        {
            bail!("independent connectivity evidence or provider policy differs");
        }
        Ok(())
    }
}

/// Immutable expectations, not a borrowed controller. The private probe must be
/// polled while the controller independently checks the tunnel and renews its
/// short kernel lease; holding `&guard` across this exchange prevents that.
pub(super) struct ProbeBinding {
    guard_id: String,
    epoch: Digest,
    namespace_inode: u64,
    tunnel_addresses: Vec<std::net::IpAddr>,
}
impl ProbeBinding {
    pub(super) fn capture(
        guard: &PacketGuardController,
        profile: &FixedWorkerSpec,
    ) -> Result<Self> {
        let expected_network = format!("container:{}", guard.handle().id);
        if profile.container_spec().network_mode.as_deref() != Some(expected_network.as_str())
            || profile.limits.memory_bytes != guard.profile().budget().worker_limits().memory_bytes
            || profile.limits.cpu_millicores
                != guard.profile().budget().worker_limits().cpu_millicores
            || !guard.channel_usable()
        {
            bail!("connectivity probe requires the owned protected worker and reserved budget");
        }
        Ok(Self {
            guard_id: guard.handle().id.clone(),
            epoch: guard.profile().policy().epoch,
            namespace_inode: guard.namespace_inode(),
            tunnel_addresses: guard.profile().policy().tunnel_addresses.clone(),
        })
    }
}

/// Private periodic stage. No controller borrow, tunnel creation or new credit.
pub(super) async fn observe_fixed_worker(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    profile: &FixedWorkerSpec,
    binding: ProbeBinding,
    policy: ProbePolicy,
) -> Result<ConnectivityObservation> {
    let started = Instant::now();
    let process = timeout(
        Duration::from_secs(10),
        runtime.exec_marketplace_connectivity_probe(handle, &profile.container_spec().labels),
    )
    .await
    .context("connectivity process startup deadline")??;
    let request = ConnectivityRequest {
        schema_version: 1,
        nonce: Digest::of(uuid::Uuid::new_v4().as_bytes()),
        family: policy.family,
        require_warp: policy.require_warp,
    };
    let context = ProbeContext {
        namespace_inode: binding.namespace_inode,
        memory_bytes: profile.limits.memory_bytes,
        cpu_millicores: profile.limits.cpu_millicores,
        pids: super::runtime::WORKER_PIDS,
        tunnel_addresses: &binding.tunnel_addresses,
    };
    let report = exchange(process, &request, &context, Duration::from_secs(18)).await?;
    timeout(
        Duration::from_secs(10),
        inspect_fixed_worker(runtime, handle, profile),
    )
    .await
    .context("connectivity ownership reinspection deadline")??;
    Ok(ConnectivityObservation {
        guard_id: binding.guard_id,
        epoch: binding.epoch,
        started,
        report,
    })
}

/// One controller-owned probe slot with no hidden queue. Clearing before the
/// first await invalidates old evidence even if the new attempt is cancelled.
#[derive(Default)]
pub struct ConnectivityMonitor {
    current: Option<ConnectivityObservation>,
}
impl ConnectivityMonitor {
    pub fn current(&self, guard: &PacketGuardController) -> Option<&ConnectivityObservation> {
        self.current
            .as_ref()
            .filter(|sample| sample.is_recent_for(guard))
    }
    pub fn invalidate(&mut self) {
        self.current = None;
    }
    pub(super) fn require_current(
        &self,
        guard: &PacketGuardController,
        policy: ProbePolicy,
    ) -> Result<&ConnectivityObservation> {
        let sample = self
            .current
            .as_ref()
            .context("missing current independent connectivity evidence")?;
        sample.check_policy(guard, policy)?;
        Ok(sample)
    }
    /// No cached sample survives cancellation or a failed renewal. This creates
    /// no credit and is not the contribution/installation admission policy.
    pub(super) async fn renew_checked(
        &mut self,
        runtime: &dyn RuntimeManager,
        guard: &mut PacketGuardController,
        identity: &WireguardIdentity,
        policy: ProbePolicy,
    ) -> Result<()> {
        let sample = self
            .current
            .take()
            .context("missing connectivity renewal sample")?;
        let result = async {
            sample.check_policy(guard, policy)?;
            refresh::renew_once(runtime, guard, identity, &sample).await
        }
        .await;
        match result {
            Ok(()) => {
                self.current = Some(sample);
                Ok(())
            }
            Err(error) => {
                let _ = guard.pause(runtime).await;
                Err(error)
            }
        }
    }
    /// Refresh a previously successful bootstrap/periodic sample. Kernel tunnel
    /// checks and short lease renewals continue while DNS/TLS is in flight.
    /// Failure clears the sample and pauses traffic; cancellation leaves no
    /// reusable sample and the independent <=15-second kernel lease expires.
    pub async fn observe_fixed_worker(
        &mut self,
        runtime: &dyn RuntimeManager,
        handle: &ContainerHandle,
        profile: &FixedWorkerSpec,
        guard: &mut PacketGuardController,
        identity: &WireguardIdentity,
        policy: ProbePolicy,
    ) -> Result<()> {
        let previous = self.current.take();
        match refresh::refresh(runtime, handle, profile, guard, identity, policy, previous).await {
            Ok(sample) => {
                self.current = Some(sample);
                Ok(())
            }
            Err(error) => {
                let _ = guard.pause(runtime).await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    #[test]
    fn probe_diagnostics_are_negative_closed_labels_not_untrusted_worker_text() {
        assert_eq!(probe_diagnostic(b"ELXPROBE1:dns\n"), "dns");
        assert_eq!(probe_diagnostic(b"ELXPROBE1:deadline\n"), "deadline");
        let parent = b"marketplace kernel guard unavailable (invalid marketplace kernel profile)\n";
        assert_eq!(probe_diagnostic(parent), "native_guard");
        for (marker, label) in [
            (b"ELXPROBE1:dns\n".as_slice(), "dns"),
            (b"ELXPROBE1:deadline\n".as_slice(), "deadline"),
            (
                b"marketplace connectivity probe unavailable\n".as_slice(),
                "unavailable",
            ),
        ] {
            assert_eq!(probe_diagnostic(&[marker, parent].concat()), label);
            assert_eq!(
                probe_diagnostic(&[marker, parent, parent].concat()),
                "unclassified"
            );
        }
        assert_eq!(
            probe_diagnostic(b"marketplace connectivity probe unavailable\n"),
            "unavailable"
        );
        assert_eq!(probe_diagnostic(b""), "no_report");
        for bytes in [
            b"ELXPROBE1:dns\nprivate account".as_slice(),
            b"ELXPROBE1:private\n",
            b"private address",
            b"ELXPROBE1:dns\nELXPROBE1:dns\n",
        ] {
            assert_eq!(probe_diagnostic(bytes), "unclassified");
        }
    }
    fn fake(mode: &str) -> Result<PrivateProcessStdio> {
        let script = r#"
          const mode=process.argv[1];let input=Buffer.alloc(0);
          let started=false;
          process.stdin.on('data',part=>{
            input=Buffer.concat([input,part]);
            if(started || input.length<4 || input.length<4+input.readUInt32BE())return;
            started=true;setTimeout(()=>{
            // Regression: no EOF may be required or sent before this reply.
            if(process.stdin.readableEnded){process.exitCode=1;return;}
            if(mode==='stall'){setInterval(()=>{},1000);return;}
            if(mode==='diagnostics'){process.stderr.write(Buffer.alloc(1025,120));process.stdin.destroy();return;}
            if(mode==='classified_failure'){process.stderr.write('ELXPROBE1:dns\nmarketplace kernel guard unavailable (invalid marketplace kernel profile)\n');process.exitCode=1;process.stdin.destroy();return;}
            const request=JSON.parse(input.subarray(4));
            let reply={schema_version:1,nonce:request.nonce,family:'ipv4',elapsed_millis:100,
              kernel:{memory_bytes:234881024,swap_bytes:0,cpu_quota_us:9000,cpu_period_us:100000,pids:64,network_namespace_inode:12345,seccomp_mode:2},
              resolved_addresses:['104.16.132.229'],connected_address:'104.16.132.229',source_address:'10.66.0.2',exit_address:'8.8.4.4',warp:'on'};
            if(mode==='nonce')reply.nonce='sha256:'+'0'.repeat(64);
            let body=Buffer.from(JSON.stringify(reply));if(mode==='duplicate')body=Buffer.from(JSON.stringify(reply).replace('{','{"schema_version":1,'));
            const header=Buffer.alloc(4);header.writeUInt32BE(mode==='oversize'?4097:body.length);
            const frame=Buffer.concat([header,body]);
            if(mode==='partial'){process.stdout.write(frame.subarray(0,8));process.stdin.destroy();return;}
            for(let i=0;i<frame.length;i+=3)process.stdout.write(frame.subarray(i,i+3));
            if(mode==='extra')process.stdout.write(Buffer.from([0]));
            if(mode==='status')process.exitCode=1;
            process.stdin.destroy();
            },25);
          });
        "#;
        let mut child = tokio::process::Command::new("node")
            .args(["-e", script, mode])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(PrivateProcessStdio {
            input: child.stdin.take().unwrap(),
            output: child.stdout.take().unwrap(),
            diagnostics: child.stderr.take(),
            child,
        })
    }
    #[tokio::test]
    async fn private_probe_requires_complete_correlated_bounded_output_and_successful_exit()
    -> Result<()> {
        let request = ConnectivityRequest {
            schema_version: 1,
            nonce: Digest::of(b"test-only"),
            family: ProbeFamily::Ipv4,
            require_warp: true,
        };
        let addresses = ["10.66.0.2".parse()?];
        let context = ProbeContext {
            namespace_inode: 12345,
            memory_bytes: 224 * 1024 * 1024,
            cpu_millicores: 90,
            pids: 64,
            tunnel_addresses: &addresses,
        };
        exchange(fake("valid")?, &request, &context, Duration::from_secs(3)).await?;
        let failure = exchange(
            fake("classified_failure")?,
            &request,
            &context,
            Duration::from_secs(3),
        )
        .await
        .err()
        .context("negative probe diagnostic became success")?;
        assert!(failure.to_string().contains("diagnostic=dns"));
        for mode in [
            "nonce",
            "duplicate",
            "oversize",
            "partial",
            "extra",
            "status",
            "diagnostics",
        ] {
            assert!(
                exchange(fake(mode)?, &request, &context, Duration::from_secs(3))
                    .await
                    .is_err(),
                "{mode}"
            );
        }
        assert!(
            exchange(
                fake("stall")?,
                &request,
                &context,
                Duration::from_millis(100)
            )
            .await
            .is_err()
        );
        Ok(())
    }
}
