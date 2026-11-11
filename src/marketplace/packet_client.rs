//! Private native-actor RPC only. An acknowledgement proves neither paid debit
//! authority nor a working VPN. This channel never carries account secrets.
use crate::runtime::PrivateProcessStdio;
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    canonical,
    guard_control::{
        GuardCommand, GuardKernelReport, GuardOutcome, GuardReply, GuardRequest,
        MAX_GUARD_FRAME_BYTES, MAX_GUARD_REPLY_BYTES, MAX_HOST_RESOURCES_BYTES,
    },
    identity::Digest,
    packet_guard::{MAX_CREDIT_REVISION, PacketCredit},
    packet_usage::PacketUsage,
    wireguard_setup::{SECRET_FRAME_BYTES, WireguardSecrets, WireguardSetup},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    time::timeout,
};

pub(super) struct PacketClient {
    process: Option<PrivateProcessStdio>,
    diagnostics: tokio::task::JoinHandle<()>,
    failure_stage: Arc<AtomicU16>,
    generation_mismatch: Arc<AtomicU64>,
    overflow: mpsc::Receiver<()>,
    epoch: Digest,
    sequence: u64,
    poisoned: bool,
    deadline: Duration,
    cpu_millicores: u32,
    memory_bytes: u64,
    pids: u64,
    namespace: Option<u64>,
    credit: Option<PacketCredit>,
    last_usage: Option<PacketUsage>,
    lease_seconds: Option<u8>,
    host_sampled: bool,
    provider_initial_credit: bool,
}
const STAGES: [&str; 16] = [
    "frame",
    "initial_read",
    "initial_shape",
    "integrity",
    "command",
    "credit_read",
    "kernel",
    "probe",
    "usage",
    "reply",
    "host_resources",
    "firewall_apply",
    "firewall_commit",
    "wireguard_secret",
    "wireguard_config",
    "command_expired",
];
const ERROR_KINDS: [&str; 11] = [
    "Other",
    "TimedOut",
    "Interrupted",
    "InvalidData",
    "PermissionDenied",
    "WouldBlock",
    "UnexpectedEof",
    "BrokenPipe",
    "InvalidInput",
    "NotFound",
    "OutOfMemory",
];
fn diagnostic_stage(bytes: &[u8]) -> u16 {
    bytes
        .split(|b| *b == b'\n')
        .filter_map(|line| {
            let text = std::str::from_utf8(line)
                .ok()?
                .strip_prefix("marketplace packet stage=")?;
            let (name, kind) = text.split_once(" kind=")?;
            let kind = ERROR_KINDS
                .iter()
                .position(|value| *value == kind)
                .map_or(0, |n| (n as u16 + 1) << 8);
            STAGES
                .iter()
                .position(|stage| *stage == name)
                .map(|n| (n as u16 + 1) | kind)
        })
        .last()
        .unwrap_or(0)
}
// Only a complete, exact numeric line is portable into server diagnostics.
// This is failure context, never firewall authority or permission to retry.
// Zero means absent: an actual mismatch cannot have both counters equal zero.
fn diagnostic_generation(bytes: &[u8]) -> u64 {
    fn counter(text: &str) -> Option<u32> {
        if text.is_empty()
            || text.len() > 10
            || !text.bytes().all(|byte| byte.is_ascii_digit())
            || (text.len() > 1 && text.starts_with('0'))
        {
            return None;
        }
        text.parse().ok()
    }
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .filter_map(|line| {
            let text = std::str::from_utf8(line.strip_suffix(b"\n")?)
                .ok()?
                .strip_prefix("marketplace packet generation expected=")?;
            let (expected, actual) = text.split_once(" actual=")?;
            let (expected, actual) = (counter(expected)?, counter(actual)?);
            (expected != actual).then_some((u64::from(expected) << 32) | u64::from(actual))
        })
        .last()
        .unwrap_or(0)
}
fn command_deadline(base: Duration, command: &GuardCommand) -> Duration {
    // Full initial/credit snapshots have longer bounds, while traffic remains
    // closed. Steady-state renewals stay at 8 s and
    // never extend the independent 15 s maximum kernel lease.
    match command {
        GuardCommand::Initialize { .. } | GuardCommand::InitializeProvider { .. } => base * 3,
        GuardCommand::Credit { .. } => base * 2,
        _ => base,
    }
}
impl PacketClient {
    pub(super) async fn new_joined(
        mut process: PrivateProcessStdio,
        epoch: Digest,
        memory_bytes: u64,
        cpu_millicores: u32,
        pids: u64,
    ) -> Result<Self> {
        if process.diagnostics.is_none() {
            let _ = process.child.start_kill();
            timeout(Duration::from_secs(10), process.child.wait())
                .await
                .context("rejected packet attachment termination unconfirmed")??;
            bail!("missing packet actor diagnostics");
        }
        Self::new(process, epoch, memory_bytes, cpu_millicores, pids)
    }
    pub(super) fn new(
        mut process: PrivateProcessStdio,
        epoch: Digest,
        memory_bytes: u64,
        cpu_millicores: u32,
        pids: u64,
    ) -> Result<Self> {
        let (sender, overflow) = mpsc::channel(1);
        let failure_stage = Arc::new(AtomicU16::new(0));
        let stage = failure_stage.clone();
        let generation_mismatch = Arc::new(AtomicU64::new(0));
        let generation = generation_mismatch.clone();
        let mut diagnostic = process
            .diagnostics
            .take()
            .context("missing packet actor diagnostics")?;
        let diagnostics = tokio::spawn(async move {
            let mut buffer = [0u8; 512];
            let mut count = 0;
            let mut transcript = Vec::new();
            while count <= 4096 {
                let size = (4097 - count).min(buffer.len());
                match diagnostic.read(&mut buffer[..size]).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => {
                        count += read;
                        if count <= 4096 {
                            transcript.extend_from_slice(&buffer[..read]);
                            let value = diagnostic_stage(&transcript);
                            if value != 0 {
                                stage.store(value, Ordering::Relaxed);
                            }
                            let value = diagnostic_generation(&transcript);
                            if value != 0 {
                                generation.store(value, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            let _ = sender.send(()).await;
        });
        Ok(Self {
            process: Some(process),
            diagnostics,
            failure_stage,
            generation_mismatch,
            overflow,
            epoch,
            sequence: 0,
            poisoned: false,
            deadline: Duration::from_secs(8),
            cpu_millicores,
            memory_bytes,
            pids,
            namespace: None,
            credit: None,
            last_usage: None,
            lease_seconds: None,
            host_sampled: false,
            provider_initial_credit: false,
        })
    }
    pub(super) fn namespace_inode(&self) -> Option<u64> {
        self.namespace
    }
    pub(super) fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub(super) async fn request(&mut self, command: GuardCommand) -> Result<GuardReply> {
        if matches!(command, GuardCommand::ConfigureWireguard { .. }) {
            bail!("WireGuard setup requires the dedicated private secret transport");
        }
        self.exchange(command, None).await
    }
    pub(super) async fn configure_wireguard(
        &mut self,
        setup: WireguardSetup,
        secrets: &WireguardSecrets,
    ) -> Result<GuardReply> {
        secrets.check_device(&setup)?;
        self.exchange(GuardCommand::ConfigureWireguard { setup }, Some(secrets))
            .await
    }
    async fn exchange(
        &mut self,
        command: GuardCommand,
        secrets: Option<&WireguardSecrets>,
    ) -> Result<GuardReply> {
        if self.poisoned || self.sequence >= MAX_CREDIT_REVISION {
            bail!("packet actor channel is unavailable");
        }
        if matches!(command, GuardCommand::HostResources) {
            if self.host_sampled || (self.credit.is_some() && !self.provider_initial_credit) {
                bail!("host-resource sampling requires its one unused precredit slot");
            }
            self.host_sampled = true;
        }
        self.sequence += 1;
        let request = GuardRequest {
            schema_version: 1,
            sequence: self.sequence,
            epoch: self.epoch,
            command,
        };
        let bytes = canonical::to_vec(&request)?;
        if bytes.is_empty() || bytes.len() > MAX_GUARD_FRAME_BYTES {
            bail!("packet actor request is oversized");
        }
        // Keep poisoned across cancellation/unknown apply. Never retry a debit.
        self.poisoned = true;
        let process = self
            .process
            .as_mut()
            .context("packet attachment is closed")?;
        let mut cancellation = Cancel {
            child: &mut process.child,
            armed: true,
        };
        let deadline = command_deadline(self.deadline, &request.command);
        let operation = async {
            process
                .input
                .write_all(&(bytes.len() as u32).to_be_bytes())
                .await?;
            process.input.write_all(&bytes).await?;
            if let Some(secrets) = secrets {
                process
                    .input
                    .write_all(&(SECRET_FRAME_BYTES as u32).to_be_bytes())
                    .await?;
                process.input.write_all(secrets.private_frame()).await?;
            }
            process.input.flush().await?;
            let length = process.output.read_u32().await? as usize;
            if length == 0 || length > MAX_GUARD_REPLY_BYTES {
                bail!("invalid packet actor response length");
            }
            let mut bytes = vec![0; length];
            process.output.read_exact(&mut bytes).await?;
            let reply: GuardReply = canonical::decode(&bytes, MAX_GUARD_REPLY_BYTES)?;
            if reply.schema_version != 1
                || reply.sequence != self.sequence
                || reply.epoch != self.epoch
            {
                bail!("packet actor response correlation differs");
            }
            let matches = match (&request.command, &reply.outcome) {
                (GuardCommand::Initialize { .. }, GuardOutcome::Initialized)
                | (GuardCommand::Pause, GuardOutcome::Paused)
                | (GuardCommand::Observe, GuardOutcome::Observed)
                | (GuardCommand::CreditStatus, GuardOutcome::CreditObserved) => true,
                (
                    GuardCommand::InitializeProvider { credit, .. },
                    GuardOutcome::ProviderInitialized {
                        revision,
                        prepaid_bytes,
                    },
                ) => *revision == credit.revision && *prepaid_bytes == credit.bytes,
                (GuardCommand::HostResources, GuardOutcome::HostResourcesObserved) => true,
                (GuardCommand::ConfigureWireguard { .. }, GuardOutcome::WireguardConfigured) => {
                    true
                }
                (
                    GuardCommand::Credit { credit },
                    GuardOutcome::Credited {
                        revision,
                        prepaid_bytes,
                    },
                ) => *revision == credit.revision && *prepaid_bytes == credit.bytes,
                (
                    GuardCommand::Heartbeat,
                    GuardOutcome::LeaseRenewed {
                        revision,
                        lease_seconds,
                    },
                ) => {
                    Some(*revision) == self.credit.map(|v| v.revision)
                        && Some(*lease_seconds) == self.lease_seconds
                }
                _ => false,
            };
            if !matches {
                bail!("packet actor acknowledged a different operation");
            }
            if matches!(request.command, GuardCommand::Observe) != reply.network.is_some() {
                bail!("packet actor network observation presence differs");
            }
            if matches!(request.command, GuardCommand::CreditStatus) != reply.usage.is_some() {
                bail!("packet actor usage observation presence differs");
            }
            if matches!(request.command, GuardCommand::HostResources)
                != reply.host_resources.is_some()
                || reply.host_resources.as_ref().is_some_and(|report| {
                    report.is_empty()
                        || report.len() > MAX_HOST_RESOURCES_BYTES
                        || report.contains('\0')
                })
            {
                bail!("packet actor host-resource observation differs");
            }
            if let Some(usage) = reply.usage {
                usage.validate(
                    self.credit
                        .context("packet actor has no committed credit")?,
                )?;
                if self.last_usage.is_some_and(|previous| {
                    usage.charged_bytes < previous.charged_bytes
                        || usage.valid_for_millis > previous.valid_for_millis
                }) {
                    bail!("packet actor usage regressed or expiry grew");
                }
            }
            let report = reply
                .kernel
                .as_ref()
                .context("packet actor omitted kernel evidence")?;
            verify_kernel(
                report,
                self.memory_bytes,
                self.cpu_millicores,
                self.pids,
                self.namespace,
            )?;
            Ok(reply)
        };
        let mut result = tokio::select! {
            result=timeout(deadline,operation)=>result.context("packet actor deadline exceeded").and_then(|result|result),
            Some(())=self.overflow.recv()=>Err(anyhow::anyhow!("packet actor diagnostic limit exceeded")),
        };
        if result.is_err() {
            // stdout EOF can race the final stderr read. Allow a bounded drain
            // on failure only; this does not join/retire the native actor,
            // unpoison the channel or extend a kernel traffic lease. Timeout
            // leaves this same diagnostics task owned until normal retirement.
            let _ = timeout(Duration::from_millis(100), &mut self.diagnostics).await;
            let counters = self.generation_mismatch.load(Ordering::Relaxed);
            if counters != 0 {
                let expected = (counters >> 32) as u32;
                let actual = counters as u32;
                result = result.with_context(|| {
                    format!(
                        "packet actor firewall generation changed (expected {expected}, actual {actual})"
                    )
                });
            }
            let index = self.failure_stage.load(Ordering::Relaxed);
            if let Some(stage) = (index & 0xff)
                .checked_sub(1)
                .and_then(|n| STAGES.get(n as usize))
            {
                // Only fixed enum names and the validated counters above are
                // exposed, never raw diagnostics, vendor output, configuration
                // or account/connection data.
                let kind = (index >> 8)
                    .checked_sub(1)
                    .and_then(|n| ERROR_KINDS.get(n as usize))
                    .copied()
                    .unwrap_or("unavailable");
                result = result.with_context(|| format!("packet actor failed at {stage} ({kind})"));
            }
        }
        if let Ok(reply) = &result {
            match &request.command {
                GuardCommand::Initialize { policy } => {
                    self.lease_seconds = Some(policy.lease_seconds)
                }
                GuardCommand::InitializeProvider { policy, credit } => {
                    self.lease_seconds = Some(policy.lease_seconds);
                    self.credit = Some(*credit);
                    self.last_usage = None;
                    self.provider_initial_credit = true;
                }
                GuardCommand::Credit { credit } => {
                    self.credit = Some(*credit);
                    self.last_usage = None;
                    self.provider_initial_credit = false;
                }
                GuardCommand::Heartbeat => self.provider_initial_credit = false,
                GuardCommand::CreditStatus => self.last_usage = reply.usage,
                _ => {}
            }
            self.namespace = Some(reply.kernel.as_ref().unwrap().network_namespace_inode);
            self.poisoned = false;
            cancellation.armed = false;
        }
        result
    }
    pub(super) async fn close(mut self) {
        let _ = self.close_attachment().await;
        self.diagnostics.abort();
    }
    async fn close_attachment(&mut self) -> Result<std::process::ExitStatus> {
        self.poisoned = true;
        let mut process = self.process.take().context("packet attachment is closed")?;
        // ChildStdin::shutdown does not close the Unix pipe. Deliver EOF to
        // this original actor before waiting, retaining bounded forced cleanup.
        drop(process.input);
        let mut cancellation = Cancel {
            child: &mut process.child,
            armed: true,
        };
        let status = match timeout(Duration::from_secs(2), cancellation.child.wait()).await {
            Ok(result) => result?,
            Err(_) => {
                let _ = cancellation.child.start_kill();
                timeout(Duration::from_secs(2), cancellation.child.wait()).await??
            }
        };
        cancellation.armed = false;
        self.diagnostics.abort();
        Ok(status)
    }
    /// Native parent retirement requires the original attachment's actual join.
    pub(super) async fn close_joined(mut self) -> Result<()> {
        self.poisoned = true;
        let mut process = self.process.take().context("packet attachment is closed")?;
        let mut cancellation = Cancel {
            child: &mut process.child,
            armed: true,
        };
        let _ = cancellation.child.start_kill();
        timeout(Duration::from_secs(10), cancellation.child.wait())
            .await
            .context("original packet attachment termination unconfirmed")??;
        cancellation.armed = false;
        Ok(())
    }
}
fn verify_kernel(
    report: &GuardKernelReport,
    memory_bytes: u64,
    cpu_millicores: u32,
    pids: u64,
    namespace: Option<u64>,
) -> Result<()> {
    if report.memory_bytes != memory_bytes
        || report.swap_bytes != 0
        || report.pids != pids
        || report.seccomp_mode != 2
        || report.cpu_period_us == 0
        || report.cpu_quota_us == 0
        || u128::from(report.cpu_quota_us) * 1000
            != u128::from(report.cpu_period_us) * u128::from(cpu_millicores)
        || report.network_namespace_inode == 0
        || namespace.is_some_and(|expected| expected != report.network_namespace_inode)
    {
        bail!("packet actor kernel isolation differs from profile");
    }
    Ok(())
}
struct Cancel<'a> {
    child: &'a mut tokio::process::Child,
    armed: bool,
}
impl Drop for Cancel<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.child.start_kill();
        }
    }
}
impl Drop for PacketClient {
    fn drop(&mut self) {
        self.diagnostics.abort();
        if let Some(process) = &mut self.process {
            let _ = process.child.start_kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::packet_guard::{PacketCredit, PacketPolicy, VpnEndpoint};
    use std::process::Stdio;
    #[test]
    fn diagnostics_expose_only_fixed_failure_stages_not_raw_output() {
        assert_eq!(
            diagnostic_stage(b"marketplace packet stage=initial_read kind=Other\n"),
            2 | (1 << 8)
        );
        assert_eq!(
            diagnostic_stage(b"marketplace packet stage=credit_read kind=Interrupted\n"),
            6 | (3 << 8)
        );
        assert_eq!(
            diagnostic_stage(
                b"marketplace packet stage=credit_read kind=Interrupted private-data\n"
            ),
            6
        );
        assert_eq!(
            diagnostic_stage(b"marketplace packet stage=host_resources kind=TimedOut\n"),
            11 | (2 << 8)
        );
        assert_eq!(
            diagnostic_stage(b"marketplace packet stage=firewall_apply kind=TimedOut\n"),
            12 | (2 << 8)
        );
        assert_eq!(
            diagnostic_stage(b"marketplace packet stage=command_expired kind=TimedOut\n"),
            16 | (2 << 8)
        );
        assert_eq!(
            diagnostic_stage(b"ignored\nmarketplace packet stage=probe kind=secret-value\n"),
            8
        );
        for bytes in [
            b"stage=initial_read".as_slice(),
            b"marketplace packet stage=private-account-id kind=Other\n",
            b"marketplace packet stage=initial_read.private kind=Other\n",
            b"\xff\0",
        ] {
            assert_eq!(diagnostic_stage(bytes), 0);
        }
    }
    #[test]
    fn generation_diagnostics_require_complete_exact_bounded_mismatching_counters() {
        let line = b"marketplace packet generation expected=42 actual=43\n";
        let pair = (42_u64 << 32) | 43;
        assert_eq!(diagnostic_generation(line), pair);
        for length in 0..line.len() {
            assert_eq!(diagnostic_generation(&line[..length]), 0);
        }
        assert_eq!(
            diagnostic_generation(b"marketplace packet generation expected=4294967295 actual=1\n"),
            (u64::from(u32::MAX) << 32) | 1
        );
        assert_eq!(
            diagnostic_generation(b"marketplace packet generation expected=0 actual=1\n"),
            1
        );
        for invalid in [
            "marketplace packet generation expected=1 actual=1\n",
            "marketplace packet generation expected=0 actual=0\n",
            "marketplace packet generation expected=01 actual=2\n",
            "marketplace packet generation expected=1 actual=02\n",
            "marketplace packet generation expected=+1 actual=2\n",
            "marketplace packet generation expected=-1 actual=2\n",
            "marketplace packet generation expected=1 actual=4294967296\n",
            "marketplace packet generation expected=99999999999 actual=2\n",
            "marketplace packet generation expected=1 actual=\n",
            "marketplace packet generation expected=private-account actual=2\n",
            "marketplace packet generation expected=1 actual=2 private-account\n",
            "marketplace packet generation expected=1 actual=2\r\n",
            "prefix marketplace packet generation expected=1 actual=2\n",
        ] {
            assert_eq!(diagnostic_generation(invalid.as_bytes()), 0, "{invalid:?}");
        }
        let transcript = [
            b"untrusted ignored output\n".as_slice(),
            line,
            b"marketplace packet generation expected=99 actual=100 private-account\n",
        ]
        .concat();
        assert_eq!(diagnostic_generation(&transcript), pair);
        assert_eq!(diagnostic_generation(b"\xff\n"), 0);
    }
    fn policy() -> PacketPolicy {
        PacketPolicy {
            schema_version: 1,
            epoch: Digest::of(b"client"),
            tunnel_interface: "wg0".to_owned(),
            tunnel_addresses: vec!["10.66.0.2".parse().unwrap()],
            endpoints: vec![VpnEndpoint {
                address: "8.8.8.8".parse().unwrap(),
                port: 51820,
                local_port: 51821,
            }],
            lease_seconds: 3,
            upload_bytes_per_second: 32768,
            download_bytes_per_second: 131072,
        }
    }
    #[test]
    fn cold_initialization_deadline_does_not_relax_steady_state_or_kernel_leases() {
        let base = Duration::from_secs(8);
        assert_eq!(
            command_deadline(base, &GuardCommand::Initialize { policy: policy() }),
            Duration::from_secs(24)
        );
        assert_eq!(
            command_deadline(
                base,
                &GuardCommand::InitializeProvider {
                    policy: policy(),
                    credit: PacketCredit {
                        revision: 1,
                        bytes: 65536
                    },
                }
            ),
            Duration::from_secs(24)
        );
        for command in [
            GuardCommand::Heartbeat,
            GuardCommand::Pause,
            GuardCommand::Observe,
            GuardCommand::HostResources,
        ] {
            assert_eq!(command_deadline(base, &command), base);
        }
        assert_eq!(
            command_deadline(
                base,
                &GuardCommand::Credit {
                    credit: PacketCredit {
                        revision: 1,
                        bytes: 8192
                    }
                }
            ),
            Duration::from_secs(16)
        );
        assert_eq!(elixir_marketplace::packet_guard::MAX_LEASE_SECONDS, 15);
    }
    fn fake(mode: &str, deadline: Duration) -> Result<PacketClient> {
        let script = r#"
const mode=process.argv[1];let input=Buffer.alloc(0),credit=0,prepaid=0,reads=0;
process.stdin.on('data',chunk=>{
input=Buffer.concat([input,chunk]);if(input.length<4||input.length<4+input.readUInt32BE(0))return;
const n=input.readUInt32BE(0),request=JSON.parse(input.subarray(4,4+n));input=input.subarray(4+n);
if(mode==='stall')return;if(mode==='diagnostics'){process.stderr.write(Buffer.alloc(5000));return;}
if(mode==='generation_failure'||mode==='split_generation_failure'||mode==='invalid_generation_failure'){
const stage='marketplace packet stage=credit_read kind=Interrupted\n';
const finish=()=>process.stderr.end(stage,()=>process.stdout.end());
if(mode==='invalid_generation_failure')process.stderr.write('marketplace packet generation expected=42 actual=43 private-account\n',finish);
else if(mode==='split_generation_failure'){process.stderr.write('marketplace packet generation expected=42 actual=');setTimeout(()=>process.stderr.write('43\n',finish),5);}
else process.stderr.write('marketplace packet generation expected=42 actual=43\n',finish);
return;
}
if(mode==='oversize'){const h=Buffer.alloc(4);h.writeUInt32BE(4097);process.stdout.write(h);return;}
if(mode==='partial'){process.stdout.write(Buffer.from([0,0]));return;}
let outcome={type:'paused'};
if(request.command.type==='initialize')outcome={type:'initialized'};
if(request.command.type==='initialize_provider'){credit=request.command.credit.revision;prepaid=request.command.credit.bytes;reads=0;outcome={type:'provider_initialized',revision:credit,prepaid_bytes:prepaid};if(mode==='provider_credit')outcome.prepaid_bytes++;}
if(request.command.type==='credit'){credit=request.command.credit.revision;prepaid=request.command.credit.bytes;reads=0;outcome={type:'credited',revision:credit,prepaid_bytes:prepaid};}
if(request.command.type==='heartbeat')outcome={type:'lease_renewed',revision:credit,lease_seconds:3};
if(request.command.type==='observe')outcome={type:'observed'};
if(request.command.type==='credit_status')outcome={type:'credit_observed'};
if(request.command.type==='host_resources')outcome={type:'host_resources_observed'};
const reply={schema_version:1,sequence:request.sequence,epoch:request.epoch,outcome,kernel:{memory_bytes:33554432,swap_bytes:0,cpu_quota_us:1000,cpu_period_us:100000,pids:16,network_namespace_inode:12345,seccomp_mode:2}};
if(request.command.type==='host_resources'){
reply.host_resources='test-only-native-resource-report';
if(mode==='missing_host')delete reply.host_resources;
if(mode==='empty_host')reply.host_resources='';
if(mode==='oversized_host')reply.host_resources='x'.repeat(2049);
if(mode==='nul_host')reply.host_resources='x\u0000';
if(mode==='wrong_host_kind')reply.outcome={type:'paused'};
}
if(mode==='unsolicited_host')reply.host_resources='test-only-unsolicited';
if(request.command.type==='observe')reply.network={status:'unavailable',reason:'missing_tunnel'};
if(request.command.type==='credit_status'){
reads++;reply.usage={revision:credit,prepaid_bytes:prepaid,charged_bytes:128*reads,valid_for_millis:3600000-reads};
if(mode==='usage_revision')reply.usage.revision++;
if(mode==='usage_bytes')reply.usage.prepaid_bytes++;
if(mode==='usage_overdrawn')reply.usage.charged_bytes=prepaid;
if(mode==='usage_expiry')reply.usage.valid_for_millis=3600001;
if(mode==='usage_regressed'&&reads>1)reply.usage.charged_bytes=1;
if(mode==='usage_extended'&&reads>1)reply.usage.valid_for_millis=3600000;
if(mode==='missing_usage')delete reply.usage;
}
if(mode==='unsolicited_usage')reply.usage={revision:1,prepaid_bytes:8192,charged_bytes:0,valid_for_millis:1};
if(mode==='unsolicited_network')reply.network={status:'unavailable',reason:'missing_tunnel'};
if(mode==='missing_network')delete reply.network;
if(mode==='wrong_sequence')reply.sequence++;
if(mode==='wrong_epoch')reply.epoch='sha256:'+'0'.repeat(64);
if(mode==='wrong_kind')reply.outcome={type:'paused'};
if(mode==='missing_kernel')delete reply.kernel;
if(mode==='memory')reply.kernel.memory_bytes++;
if(mode==='cpu')reply.kernel.cpu_quota_us++;
if(mode==='swap')reply.kernel.swap_bytes=1;
if(mode==='pids')reply.kernel.pids++;
if(mode==='seccomp')reply.kernel.seccomp_mode=0;
if(mode==='zero_namespace')reply.kernel.network_namespace_inode=0;
if(mode==='extra_field')reply.kernel.authorized=true;
if(mode==='namespace'&&request.sequence>1)reply.kernel.network_namespace_inode++;
if(mode==='credit'&&request.command.type==='credit')reply.outcome.prepaid_bytes++;
if(mode==='lease'&&request.command.type==='heartbeat')reply.outcome.revision++;
if(mode==='lease_duration'&&request.command.type==='heartbeat')reply.outcome.lease_seconds++;
let body=JSON.stringify(reply);if(mode==='duplicate')body=body.replace('{','{"schema_version":1,');
const bytes=Buffer.from(body),header=Buffer.alloc(4);header.writeUInt32BE(bytes.length);const frame=Buffer.concat([header,bytes]);
for(let i=0;i<frame.length;i+=3)process.stdout.write(frame.subarray(i,i+3));
});setInterval(()=>{},1000);
"#;
        let mut child = tokio::process::Command::new("node")
            .args(["-e", script, mode])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stream = PrivateProcessStdio {
            input: child.stdin.take().unwrap(),
            output: child.stdout.take().unwrap(),
            diagnostics: child.stderr.take(),
            child,
        };
        let mut client = PacketClient::new(stream, policy().epoch, 33554432, 10, 16)?;
        client.deadline = deadline;
        Ok(client)
    }
    #[tokio::test]
    async fn closing_packet_attachment_delivers_eof_before_wait_without_granting_health()
    -> Result<()> {
        let mut child = tokio::process::Command::new("node")
            .args(["-e", "const timer=setInterval(()=>{},1000);process.stdin.resume();process.stdin.once('end',()=>clearInterval(timer));process.stdout.write('READY');"])
            .env_clear().env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
        let mut client = PacketClient::new(
            PrivateProcessStdio {
                input: child.stdin.take().context("stdin")?,
                output: child.stdout.take().context("stdout")?,
                diagnostics: child.stderr.take(),
                child,
            },
            Digest::of(b"EOF test only"),
            128 * 1024 * 1024,
            100,
            16,
        )?;
        let mut ready = [0; 5];
        timeout(
            Duration::from_secs(3),
            client
                .process
                .as_mut()
                .context("attachment")?
                .output
                .read_exact(&mut ready),
        )
        .await??;
        assert_eq!(&ready, b"READY");
        assert!(client.close_attachment().await?.success());
        assert!(client.process.is_none() && client.poisoned);
        assert!(client.namespace.is_none() && client.credit.is_none());
        assert!(client.close_attachment().await.is_err());
        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn forced_packet_retirement_still_joins_the_original_attachment() -> Result<()> {
        fake("stall", Duration::from_secs(3))?.close_joined().await
    }

    #[tokio::test]
    async fn private_client_correlates_operations_credit_and_kernel_identity() -> Result<()> {
        let mut client = fake("good", Duration::from_secs(2))?;
        client
            .request(GuardCommand::Initialize { policy: policy() })
            .await?;
        let observed = client.request(GuardCommand::Observe).await?;
        assert!(matches!(
            observed.network,
            Some(elixir_marketplace::network_probe::NetworkObservation::Unavailable { .. })
        ));
        client
            .request(GuardCommand::Credit {
                credit: PacketCredit {
                    revision: 9,
                    bytes: 8192,
                },
            })
            .await?;
        client.request(GuardCommand::Heartbeat).await?;
        let usage = client
            .request(GuardCommand::CreditStatus)
            .await?
            .usage
            .unwrap();
        assert_eq!(usage.revision, 9);
        assert_eq!(usage.charged_bytes, 128);
        let later = client
            .request(GuardCommand::CreditStatus)
            .await?
            .usage
            .unwrap();
        assert_eq!(later.charged_bytes, 256);
        assert!(later.valid_for_millis < usage.valid_for_millis);
        client.request(GuardCommand::Pause).await?;
        assert_eq!(client.namespace_inode(), Some(12345));
        client.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn failure_counters_survive_chunked_stderr_but_never_reopen_the_channel() -> Result<()> {
        for mode in [
            "generation_failure",
            "split_generation_failure",
            "invalid_generation_failure",
        ] {
            let mut client = fake(mode, Duration::from_secs(2))?;
            let error = client
                .request(GuardCommand::Initialize { policy: policy() })
                .await
                .unwrap_err();
            let report = format!("{error:#}");
            assert!(
                report.contains("credit_read (Interrupted)"),
                "{mode}: {report}"
            );
            assert_eq!(
                report.contains("firewall generation changed (expected 42, actual 43)"),
                mode != "invalid_generation_failure",
                "{mode}: {report}"
            );
            assert!(!report.contains("private-account"));
            assert!(client.is_poisoned());
            assert!(client.request(GuardCommand::Pause).await.is_err());
            client.close_joined().await?;
        }
        Ok(())
    }
    #[tokio::test]
    async fn provider_initial_credit_is_correlated_and_does_not_bypass_one_use_host_sampling()
    -> Result<()> {
        let mut p = policy();
        p.tunnel_interface = elixir_marketplace::packet_guard::PROVIDER_TUN_INTERFACE.into();
        for action in ["sample", "credit", "heartbeat"] {
            let mut client = fake("good", Duration::from_secs(2))?;
            client
                .request(GuardCommand::InitializeProvider {
                    policy: p.clone(),
                    credit: PacketCredit {
                        revision: 1,
                        bytes: 65536,
                    },
                })
                .await?;
            match action {
                "sample" => {
                    client.request(GuardCommand::HostResources).await?;
                }
                "credit" => {
                    client
                        .request(GuardCommand::Credit {
                            credit: PacketCredit {
                                revision: 2,
                                bytes: 65536,
                            },
                        })
                        .await?;
                }
                _ => {
                    client.request(GuardCommand::Heartbeat).await?;
                }
            }
            assert!(client.request(GuardCommand::HostResources).await.is_err());
            client.close().await;
        }
        let mut client = fake("provider_credit", Duration::from_secs(2))?;
        assert!(
            client
                .request(GuardCommand::InitializeProvider {
                    policy: p,
                    credit: PacketCredit {
                        revision: 1,
                        bytes: 65536
                    }
                })
                .await
                .is_err()
        );
        assert!(client.is_poisoned());
        client.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn malformed_replies_unknown_isolation_and_replay_poison_the_channel() -> Result<()> {
        for (mode, expected) in [
            ("wrong_sequence", "response correlation differs"),
            ("wrong_epoch", "response correlation differs"),
            ("wrong_kind", "acknowledged a different operation"),
            ("missing_kernel", "omitted kernel evidence"),
            ("unsolicited_usage", "usage observation presence differs"),
            ("unsolicited_host", "host-resource observation differs"),
            (
                "unsolicited_network",
                "network observation presence differs",
            ),
            ("memory", "kernel isolation differs"),
            ("cpu", "kernel isolation differs"),
            ("swap", "kernel isolation differs"),
            ("pids", "kernel isolation differs"),
            ("seccomp", "kernel isolation differs"),
            ("zero_namespace", "kernel isolation differs"),
            ("extra_field", "unknown field"),
            ("duplicate", "duplicate"),
            ("oversize", "invalid packet actor response length"),
            ("partial", "deadline exceeded"),
        ] {
            let mut client = fake(mode, Duration::from_secs(2))?;
            let error = client
                .request(GuardCommand::Initialize { policy: policy() })
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains(expected),
                "{mode}: expected {expected}, got {error:#}"
            );
            assert!(
                client
                    .request(GuardCommand::Pause)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("channel is unavailable")
            );
            client.close().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_or_unsolicited_network_observations_cannot_be_treated_as_health() -> Result<()>
    {
        let mut client = fake("missing_network", Duration::from_secs(2))?;
        client
            .request(GuardCommand::Initialize { policy: policy() })
            .await?;
        let error = client.request(GuardCommand::Observe).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("network observation presence differs")
        );
        assert!(client.is_poisoned());
        client.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn host_resource_reply_is_one_use_and_never_grants_credit_or_a_second_sample()
    -> Result<()> {
        let mut client = fake("good", Duration::from_secs(2))?;
        client
            .request(GuardCommand::Initialize { policy: policy() })
            .await?;
        let report = client.request(GuardCommand::HostResources).await?;
        assert_eq!(
            report.host_resources.as_deref(),
            Some("test-only-native-resource-report")
        );
        assert!(client.credit.is_none());
        assert!(client.request(GuardCommand::HostResources).await.is_err());
        client
            .request(GuardCommand::Credit {
                credit: PacketCredit {
                    revision: 1,
                    bytes: 8192,
                },
            })
            .await?;
        client.request(GuardCommand::Pause).await?;
        assert!(
            !client.is_poisoned(),
            "local duplicate refusal need not dispatch a native command"
        );
        client.close().await;
        let mut credited = fake("good", Duration::from_secs(2))?;
        credited
            .request(GuardCommand::Initialize { policy: policy() })
            .await?;
        credited
            .request(GuardCommand::Credit {
                credit: PacketCredit {
                    revision: 1,
                    bytes: 8192,
                },
            })
            .await?;
        assert!(credited.request(GuardCommand::HostResources).await.is_err());
        credited.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn host_resource_missing_empty_oversized_nul_or_wrong_kind_poison_the_original_pipe()
    -> Result<()> {
        for mode in [
            "missing_host",
            "empty_host",
            "oversized_host",
            "nul_host",
            "wrong_host_kind",
        ] {
            let mut client = fake(mode, Duration::from_secs(2))?;
            client
                .request(GuardCommand::Initialize { policy: policy() })
                .await?;
            assert!(
                client.request(GuardCommand::HostResources).await.is_err(),
                "{mode}"
            );
            assert!(client.is_poisoned(), "{mode}");
            assert!(client.request(GuardCommand::Pause).await.is_err(), "{mode}");
            client.close().await;
        }
        Ok(())
    }
    #[tokio::test]
    async fn renewed_leases_and_debits_cannot_change_acknowledged_revision_or_namespace()
    -> Result<()> {
        for mode in ["namespace", "credit", "lease", "lease_duration"] {
            let mut client = fake(mode, Duration::from_secs(2))?;
            client
                .request(GuardCommand::Initialize { policy: policy() })
                .await?;
            let result = client
                .request(GuardCommand::Credit {
                    credit: PacketCredit {
                        revision: 3,
                        bytes: 8192,
                    },
                })
                .await;
            if matches!(mode, "lease" | "lease_duration") {
                result?;
                let error = client.request(GuardCommand::Heartbeat).await.unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("acknowledged a different operation")
                );
            } else {
                let expected = if mode == "namespace" {
                    "kernel isolation differs"
                } else {
                    "acknowledged a different operation"
                };
                assert!(result.unwrap_err().to_string().contains(expected));
            }
            assert!(client.request(GuardCommand::Pause).await.is_err());
            client.close().await;
        }
        Ok(())
    }
    #[tokio::test]
    async fn missing_forged_regressed_or_extended_usage_poison_the_channel() -> Result<()> {
        for mode in [
            "missing_usage",
            "usage_revision",
            "usage_bytes",
            "usage_overdrawn",
            "usage_expiry",
            "usage_regressed",
            "usage_extended",
        ] {
            let mut client = fake(mode, Duration::from_secs(2))?;
            client
                .request(GuardCommand::Initialize { policy: policy() })
                .await?;
            client
                .request(GuardCommand::Credit {
                    credit: PacketCredit {
                        revision: 7,
                        bytes: 8192,
                    },
                })
                .await?;
            let first = client.request(GuardCommand::CreditStatus).await;
            if matches!(mode, "usage_regressed" | "usage_extended") {
                first?;
                assert!(
                    client.request(GuardCommand::CreditStatus).await.is_err(),
                    "{mode}"
                );
            } else {
                assert!(first.is_err(), "{mode}");
            }
            assert!(
                client.request(GuardCommand::Pause).await.is_err(),
                "no reuse after {mode}"
            );
            client.close().await;
        }
        let mut client = fake("good", Duration::from_secs(2))?;
        client
            .request(GuardCommand::Initialize { policy: policy() })
            .await?;
        assert!(
            client.request(GuardCommand::CreditStatus).await.is_err(),
            "no usage may be accepted without credit"
        );
        client.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn cancellation_timeout_and_diagnostic_flood_never_leave_a_reusable_stream() -> Result<()>
    {
        for mode in ["stall", "diagnostics"] {
            let duration = if mode == "stall" {
                Duration::from_millis(150)
            } else {
                Duration::from_secs(2)
            };
            let mut client = fake(mode, duration)?;
            let expected = if mode == "stall" {
                "deadline exceeded"
            } else {
                "diagnostic limit exceeded"
            };
            let error = client
                .request(GuardCommand::Initialize { policy: policy() })
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{mode}: expected {expected}, got {error:#}"
            );
            assert!(client.request(GuardCommand::Pause).await.is_err());
            client.close().await;
        }
        let mut client = fake("stall", Duration::from_secs(10))?;
        assert!(
            timeout(
                Duration::from_millis(30),
                client.request(GuardCommand::Initialize { policy: policy() })
            )
            .await
            .is_err()
        );
        assert!(client.request(GuardCommand::Pause).await.is_err());
        client.close().await;
        Ok(())
    }
}
