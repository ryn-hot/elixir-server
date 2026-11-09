//! Private provider session + encrypted state publication. This transport is
//! not an owned container/namespace capability or permission to open a packet
//! lease. Its caller MUST retain exact native ownership and recovery fencing.
use super::{lease::ControllerLease, provider_state};
use crate::{runtime::PrivateProcessStdio, secrets::SecretsManager};
use anyhow::{Context, Result, ensure};
use elixir_marketplace::{
    canonical,
    identity::Digest,
    warp_provider::{Command, MAX_CONTROL_BYTES, MAX_STATE_BYTES, Outcome, Reply, Request, Start},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use zeroize::Zeroizing;

pub(super) struct Client {
    stream: PrivateProcessStdio,
    start: Start,
    sequence: u64,
    poisoned: bool,
    connected: bool,
    last_reply: tokio::time::Instant,
    overflow: Arc<AtomicBool>,
    diagnostics: tokio::task::JoinHandle<()>,
    _lease: Arc<ControllerLease>,
}
impl Client {
    /// Restore only the expected scoped record. A missing/corrupt/foreign
    /// record in resume mode NEVER falls back to registration new. State is
    /// committed + fsynced before the native durable-save acknowledgement.
    pub(super) async fn start(
        stream: PrivateProcessStdio,
        start: Start,
        image: Digest,
        lease: ControllerLease,
        secrets: Arc<SecretsManager>,
    ) -> Result<Self> {
        Self::start_shared(stream, start, image, Arc::new(lease), secrets).await
    }
    pub(super) async fn start_shared(
        mut stream: PrivateProcessStdio,
        start: Start,
        image: Digest,
        lease: Arc<ControllerLease>,
        secrets: Arc<SecretsManager>,
    ) -> Result<Self> {
        let preparation = async {
            start.validate()?;
            let owner = lease.clone();
            let key = secrets.clone();
            let scope = start.scope;
            let saved = tokio::task::spawn_blocking(move || {
                provider_state::load(&owner, &key, scope, image)
            })
            .await
            .context("provider state load task")??;
            ensure!(
                saved.is_some() == start.resume,
                "provider resume intent differs from saved identity"
            );
            let diagnostic = stream
                .diagnostics
                .take()
                .context("provider diagnostics channel missing")?;
            Ok::<_, anyhow::Error>((saved, diagnostic))
        }
        .await;
        let (saved, mut diagnostic) = match preparation {
            Ok(prepared) => prepared,
            Err(error) => {
                Self::kill_join(&mut stream)
                    .await
                    .context("rejected provider attachment termination unconfirmed")?;
                return Err(error);
            }
        };
        let overflow = Arc::new(AtomicBool::new(false));
        let flag = overflow.clone();
        let diagnostics = tokio::spawn(async move {
            let mut bytes = [0; 512];
            let mut count = 0;
            loop {
                match diagnostic.read(&mut bytes).await {
                    Ok(0) | Err(_) => break,
                    Ok(length) => {
                        count += length;
                        if count > 4096 {
                            flag.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
        });
        let mut client = Self {
            stream,
            start,
            sequence: 3,
            poisoned: true,
            connected: false,
            last_reply: tokio::time::Instant::now(),
            overflow,
            diagnostics,
            _lease: lease,
        };
        let result = timeout(Duration::from_secs(120), async {
            let header = canonical::to_vec(&client.start)?;
            client.write_frame(&header).await?;
            let expected = saved.as_ref().map(|saved| saved.digest);
            if let Some(saved) = saved {
                client.write_frame(&saved.bytes).await?;
            }
            let checkpoint = client.read_reply(1).await?;
            let Outcome::State {
                bytes,
                digest,
                resumed,
            } = checkpoint
            else {
                anyhow::bail!("provider omitted its private checkpoint");
            };
            ensure!(
                resumed == client.start.resume && (1..=MAX_STATE_BYTES).contains(&bytes),
                "provider checkpoint differs from original startup"
            );
            let private = client.read_frame(MAX_STATE_BYTES).await?;
            ensure!(
                private.len() == bytes && Digest::of(private.as_slice()) == digest,
                "provider checkpoint framing differs"
            );
            // Disk I/O, encryption and fsync must not block a playback/event
            // executor thread. The same OS lease survives task cancellation.
            let owner = client._lease.clone();
            let key = secrets.clone();
            let scope = client.start.scope;
            let committed = tokio::task::spawn_blocking(move || {
                provider_state::save(&owner, &key, scope, image, expected, private)
            })
            .await
            .context("provider state commit task")??;
            committed.require(client.start.scope, image, digest)?;
            let acknowledgement = canonical::to_vec(&Request {
                schema_version: 1,
                session: client.start.session,
                sequence: 2,
                command: Command::AcknowledgeState { digest },
            })?;
            client.write_frame(&acknowledgement).await?;
            ensure!(
                matches!(client.read_reply(2).await?, Outcome::StateAcknowledged),
                "provider did not acknowledge saved state"
            );
            ensure!(
                !client.overflow.load(Ordering::Relaxed),
                "provider diagnostics exceeded its bound"
            );
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("provider startup deadline")
        .and_then(|value| value);
        if let Err(error) = result {
            client
                .abandon()
                .await
                .context("original provider attachment termination unconfirmed")?;
            return Err(error);
        }
        client.poisoned = false;
        client.last_reply = tokio::time::Instant::now();
        Ok(client)
    }
    async fn write_frame(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            !bytes.is_empty() && bytes.len() <= MAX_STATE_BYTES,
            "provider frame bounds"
        );
        self.stream.input.write_u32(bytes.len() as u32).await?;
        self.stream.input.write_all(bytes).await?;
        self.stream.input.flush().await?;
        Ok(())
    }
    async fn read_frame(&mut self, limit: usize) -> Result<Zeroizing<Vec<u8>>> {
        let length = self.stream.output.read_u32().await? as usize;
        ensure!(
            length > 0 && length <= limit,
            "provider response frame bounds"
        );
        let mut bytes = Zeroizing::new(vec![0; length]);
        self.stream.output.read_exact(&mut bytes).await?;
        Ok(bytes)
    }
    async fn read_reply(&mut self, sequence: u64) -> Result<Outcome> {
        let bytes = self.read_frame(MAX_CONTROL_BYTES).await?;
        let reply: Reply = canonical::decode(&bytes, MAX_CONTROL_BYTES)?;
        ensure!(
            reply.schema_version == 1
                && reply.session == self.start.session
                && reply.sequence == sequence,
            "provider response correlation differs"
        );
        ensure!(
            !self.overflow.load(Ordering::Relaxed),
            "provider diagnostics exceeded its bound"
        );
        Ok(reply.outcome)
    }
    async fn command(&mut self, command: Command) -> Result<Outcome> {
        ensure!(
            !self.poisoned && !self.overflow.load(Ordering::Relaxed),
            "provider session is poisoned"
        );
        let sequence = self.sequence;
        let result = timeout(Duration::from_secs(12), async {
            // The native supervisor bounds request frequency. Pace from its
            // previous reply, not from a send that may still be buffered.
            tokio::time::sleep_until(self.last_reply + Duration::from_millis(300)).await;
            let bytes = canonical::to_vec(&Request {
                schema_version: 1,
                session: self.start.session,
                sequence,
                command,
            })?;
            // Cancellation during pacing cannot have touched the pipe. From
            // the first possible write onward, uncertainty still poisons the
            // session until the entire correlated reply has been consumed.
            self.poisoned = true;
            self.write_frame(&bytes).await?;
            self.read_reply(sequence).await
        })
        .await
        .context("provider command deadline")?;
        match result {
            Ok(outcome) => {
                self.sequence = self
                    .sequence
                    .checked_add(1)
                    .context("provider sequence exhausted")?;
                self.poisoned = false;
                self.last_reply = tokio::time::Instant::now();
                Ok(outcome)
            }
            Err(error) => Err(error),
        }
    }
    pub(super) async fn connect(&mut self) -> Result<()> {
        ensure!(!self.connected, "provider connection already requested");
        let outcome = self.command(Command::Connect).await?;
        if !matches!(outcome, Outcome::ConnectionRequested) {
            self.poisoned = true;
            anyhow::bail!("provider acknowledged a different operation");
        }
        self.connected = true;
        Ok(())
    }
    /// Raw interface presence, NOT DNS/TLS or provider/worker health.
    pub(super) async fn inspect(&mut self) -> Result<bool> {
        match self.command(Command::Inspect).await? {
            Outcome::Inspected { interface_present } => Ok(interface_present),
            _ => {
                self.poisoned = true;
                anyhow::bail!("provider acknowledged a different operation");
            }
        }
    }
    pub(super) async fn stop(self) -> Result<()> {
        self.finish(false).await
    }
    pub(super) async fn revoke(self) -> Result<()> {
        self.finish(true).await
    }
    async fn finish(mut self, revoke: bool) -> Result<()> {
        let result = self
            .command(if revoke {
                Command::Revoke
            } else {
                Command::Stop
            })
            .await;
        if let Err(error) = result {
            self.abandon().await?;
            return Err(error);
        }
        let completion = async {
            ensure!(
                matches!(
                    (result?, revoke),
                    (Outcome::Stopped, false) | (Outcome::Revoked, true)
                ),
                "provider did not acknowledge stop"
            );
            let status = timeout(Duration::from_secs(10), self.stream.child.wait())
                .await
                .context("provider attachment join deadline")??;
            ensure!(
                status.success(),
                "provider attachment exited unsuccessfully"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = completion {
            self.abandon().await?;
            return Err(error);
        }
        Ok(())
    }
    async fn abandon(&mut self) -> Result<()> {
        self.poisoned = true;
        Self::kill_join(&mut self.stream).await
    }
    async fn kill_join(stream: &mut PrivateProcessStdio) -> Result<()> {
        let _ = stream.child.start_kill();
        timeout(Duration::from_secs(10), stream.child.wait())
            .await
            .context("provider attachment abort join deadline")??;
        Ok(())
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.poisoned = true;
        self.diagnostics.abort();
        let _ = self.stream.child.start_kill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::warp_provider::{CONSENT_VERSION, Scope, State, StateFile};
    use std::process::Stdio;
    fn scope() -> Scope {
        Scope {
            deployment: Digest::of(b"installation"),
            enrollment: Digest::of(b"enrollment"),
        }
    }
    fn attachment_joined(pid: u32) {
        // A read-only supplemental check: production cleanup awaits the
        // ORIGINAL Child::wait, never infers completion from PID absence.
        assert_eq!(unsafe { libc::kill(pid as libc::pid_t, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
    #[tokio::test]
    async fn rejected_start_joins_original_attachment_before_returning() -> Result<()> {
        let root = tempfile::tempdir()?;
        for mode in 0..3 {
            let directory = root.path().canonicalize()?.join(format!("provider-{mode}"));
            let mut child = tokio::process::Command::new("node")
                .args(["-e", "setInterval(()=>{},1000)"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()?;
            let pid = child.id().context("fixture child missing")?;
            let diagnostics = child.stderr.take();
            let stream = PrivateProcessStdio {
                input: child.stdin.take().unwrap(),
                output: child.stdout.take().unwrap(),
                diagnostics: if mode == 2 { None } else { diagnostics },
                child,
            };
            let result = Client::start(
                stream,
                Start {
                    schema_version: if mode == 0 { 2 } else { 1 },
                    session: Digest::of(b"rejected-session"),
                    scope: scope(),
                    consent_version: CONSENT_VERSION.into(),
                    resume: mode == 1,
                },
                Digest::of(b"image"),
                ControllerLease::acquire(&directory)?,
                Arc::new(SecretsManager::from_key_bytes([93; 32], false)),
            )
            .await;
            assert!(result.is_err());
            attachment_joined(pid);
            let _ = ControllerLease::acquire(&directory)?;
        }
        Ok(())
    }
    #[tokio::test]
    async fn private_checkpoint_is_encrypted_before_acknowledgement_and_reloads_without_registration_fallback()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let secrets = SecretsManager::from_key_bytes([94; 32], true);
        let image = Digest::of(b"image");
        let state = State::new(
            scope(),
            Digest::of(b"identity"),
            vec![StateFile::new(
                "reg.json".into(),
                Zeroizing::new(b"private fixture".to_vec()),
            )?],
        )?
        .encode();
        let encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, state.as_slice());
        for (resume, bad_stop) in [(false, false), (true, false), (true, true)] {
            let script = r#"
const fs=require('node:fs'),crypto=require('node:crypto'),state=Buffer.from(process.argv[1],'base64'),path=process.argv[2],resume=process.argv[3]==='true',badStop=process.argv[4]==='true';
let buffer=Buffer.alloc(0),start,waiting=false;
const send=value=>{const body=Buffer.isBuffer(value)?value:Buffer.from(JSON.stringify(value)),head=Buffer.alloc(4);head.writeUInt32BE(body.length);process.stdout.write(head);process.stdout.write(body);};
const reply=(sequence,outcome)=>send({schema_version:1,session:start.session,sequence,outcome});
process.stdin.on('data',chunk=>{buffer=Buffer.concat([buffer,chunk]);while(buffer.length>=4&&buffer.length>=buffer.readUInt32BE(0)+4){const size=buffer.readUInt32BE(0),bytes=buffer.subarray(4,size+4);buffer=buffer.subarray(size+4);
if(!start){start=JSON.parse(bytes);if(resume){waiting=true;continue;}checkpoint();continue;}
if(waiting){waiting=false;if(!bytes.equals(state))process.exit(2);checkpoint();continue;}
const request=JSON.parse(bytes);if(request.command.type==='acknowledge_state'){
const record=JSON.parse(fs.readFileSync(path));if(!record.ciphertext.startsWith('enc:v1:bound:')||JSON.stringify(record).includes('private fixture'))process.exit(3);
reply(request.sequence,{type:'state_acknowledged'});
}else if(request.command.type==='stop'){reply(request.sequence,{type:badStop?'state_acknowledged':'stopped'});if(!badStop)process.exit(0);}else process.exit(4);
}});
function checkpoint(){reply(1,{type:'state',bytes:state.length,digest:'sha256:'+crypto.createHash('sha256').update(state).digest('hex'),resumed:resume});send(state);}
"#;
            let mut child = tokio::process::Command::new("node")
                .args([
                    "-e",
                    script,
                    &encoded,
                    &directory.join("provider-state.v1").to_string_lossy(),
                    if resume { "true" } else { "false" },
                    if bad_stop { "true" } else { "false" },
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()?;
            let pid = child.id().context("fixture child missing")?;
            let stream = PrivateProcessStdio {
                input: child.stdin.take().unwrap(),
                output: child.stdout.take().unwrap(),
                diagnostics: child.stderr.take(),
                child,
            };
            let start = Start {
                schema_version: 1,
                session: Digest::of(if resume { b"session2" } else { b"session1" }),
                scope: scope(),
                consent_version: CONSENT_VERSION.into(),
                resume,
            };
            let client = Client::start(
                stream,
                start,
                image,
                ControllerLease::acquire(&directory)?,
                Arc::new(secrets.clone()),
            )
            .await?;
            let result = client.stop().await;
            if bad_stop {
                assert!(result.is_err(), "wrong stop acknowledgement was accepted");
            } else {
                result?;
            }
            attachment_joined(pid);
        }
        Ok(())
    }

    fn cancellation_fixture(
        hold_inspection: bool,
    ) -> Result<(tempfile::TempDir, Client, tokio::process::ChildStderr, u32)> {
        let root = tempfile::tempdir()?;
        let lease = Arc::new(ControllerLease::acquire(
            &root.path().canonicalize()?.join("provider"),
        )?);
        // Private framed-pipe fixture only: no registration, provider, VPN,
        // network authority or real secret. Holding a reply exposes the exact
        // point after which cancellation cannot safely reuse the channel.
        let script = r#"
const hold=process.argv[1]==='true';let pending=Buffer.alloc(0),sequence=3;
const reply=(request,outcome,exit=false)=>{const body=Buffer.from(JSON.stringify({schema_version:1,session:request.session,sequence:request.sequence,outcome})),header=Buffer.alloc(4);header.writeUInt32BE(body.length);process.stdout.write(Buffer.concat([header,body]),()=>{if(exit)process.exit(0);});};
process.stdin.on('data',chunk=>{pending=Buffer.concat([pending,chunk]);while(pending.length>=4&&pending.length>=pending.readUInt32BE(0)+4){const size=pending.readUInt32BE(0),request=JSON.parse(pending.subarray(4,size+4));pending=pending.subarray(size+4);if(request.sequence!==sequence++)process.exit(7);
if(request.command.type==='inspect'){if(hold)process.stderr.write('S');else reply(request,{type:'inspected',interface_present:true});}
else if(request.command.type==='stop'||request.command.type==='revoke')reply(request,{type:request.command.type==='stop'?'stopped':'revoked'},true);
else process.exit(8);
}});
"#;
        let mut child = tokio::process::Command::new("node")
            .args(["-e", script, if hold_inspection { "true" } else { "false" }])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().context("fixture attachment missing")?;
        let seen = child.stderr.take().context("fixture observation missing")?;
        let client = Client {
            stream: PrivateProcessStdio {
                input: child.stdin.take().unwrap(),
                output: child.stdout.take().unwrap(),
                diagnostics: None,
                child,
            },
            start: Start {
                schema_version: 1,
                session: Digest::of(b"cancellation-fixture"),
                scope: scope(),
                consent_version: CONSENT_VERSION.into(),
                resume: false,
            },
            sequence: 3,
            poisoned: false,
            connected: false,
            last_reply: tokio::time::Instant::now(),
            overflow: Arc::new(AtomicBool::new(false)),
            diagnostics: tokio::spawn(async {}),
            _lease: lease,
        };
        Ok((root, client, seen, pid))
    }

    #[tokio::test]
    async fn cancellation_during_local_pacing_keeps_the_original_unwritten_sequence_usable()
    -> Result<()> {
        for revoke in [false, true] {
            let (_root, mut client, _seen, pid) = cancellation_fixture(false)?;
            // Force a purely local wait, then poll exactly once; no wall-clock
            // timing race or native reply can put this case past the write.
            client.last_reply = tokio::time::Instant::now() + Duration::from_secs(60);
            {
                let mut call = Box::pin(client.inspect());
                std::future::poll_fn(|cx| {
                    assert!(std::future::Future::poll(call.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
            }
            assert!(!client.poisoned);
            assert_eq!(client.sequence, 3);
            client.last_reply = tokio::time::Instant::now() - Duration::from_secs(1);
            assert!(client.inspect().await?);
            assert_eq!(client.sequence, 4);
            client.finish(revoke).await?;
            attachment_joined(pid);
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_after_a_written_request_stays_poisoned_and_joins_the_original_attachment()
    -> Result<()> {
        for revoke in [false, true] {
            let (_root, mut client, mut seen, pid) = cancellation_fixture(true)?;
            client.last_reply = tokio::time::Instant::now() - Duration::from_secs(1);
            {
                let mut call = Box::pin(client.inspect());
                let mut marker = [0];
                tokio::select! {
                    result = &mut call => panic!("held reply unexpectedly completed: {result:?}"),
                    result = timeout(Duration::from_secs(10), seen.read_exact(&mut marker)) => { result??; }
                }
                assert_eq!(marker, [b'S']);
            }
            assert!(client.poisoned);
            assert_eq!(client.sequence, 3);
            assert!(
                client
                    .inspect()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("poisoned")
            );
            assert!(client.finish(revoke).await.is_err());
            attachment_joined(pid);
        }
        Ok(())
    }

    struct NativeFixture {
        owner: String,
        image: String,
        ids: Vec<String>,
    }
    impl Drop for NativeFixture {
        fn drop(&mut self) {
            for id in self.ids.iter().rev() {
                // Best-effort safety only. The passing path independently
                // verifies stop/removal; failures here are not retirement proof.
                let Ok(output) = std::process::Command::new("docker")
                    .args(["inspect", id])
                    .output()
                else {
                    continue;
                };
                let Ok(values) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
                    continue;
                };
                let Some(value) = values.get(0) else {
                    continue;
                };
                if value["Id"] != *id
                    || value["Config"]["Image"] != self.image
                    || value["Config"]["Labels"]["elixir.marketplace.warp_server_test"]
                        != self.owner
                {
                    continue;
                }
                if value["State"]["Running"] == true {
                    let _ = std::process::Command::new("docker")
                        .args(["kill", id])
                        .output();
                }
                let _ = std::process::Command::new("docker")
                    .args(["rm", id])
                    .output();
            }
        }
    }
    async fn docker(args: &[&str]) -> Result<String> {
        let output = timeout(
            Duration::from_secs(20),
            tokio::process::Command::new("docker")
                .args(args)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("native provider fixture command deadline")??;
        ensure!(
            output.status.success() && output.stdout.len() <= 65536 && output.stderr.len() <= 4096,
            "native provider fixture command failed"
        );
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }
    impl NativeFixture {
        async fn create(&mut self) -> Result<String> {
            let name = format!("elixir-marketplace-warp-server-{}", uuid::Uuid::new_v4());
            let label = format!("elixir.marketplace.warp_server_test={}", self.owner);
            let id = docker(&[
                "create",
                "--name",
                &name,
                "--label",
                &label,
                "--pull=never",
                "--network=bridge",
                "--interactive",
                "--init",
                "--read-only",
                "--user=0:0",
                "--no-healthcheck",
                "--cap-drop=ALL",
                "--cap-add=NET_ADMIN",
                "--cap-add=SETUID",
                "--cap-add=SETGID",
                "--cap-add=CHOWN",
                "--cap-add=KILL",
                "--device=/dev/net/tun:/dev/net/tun",
                "--sysctl=net.ipv6.conf.all.disable_ipv6=0",
                "--sysctl=net.ipv4.conf.all.src_valid_mark=1",
                "--security-opt=no-new-privileges",
                "--memory=256m",
                "--memory-swap=256m",
                "--cpus=0.2",
                "--pids-limit=64",
                "--log-driver=none",
                "--ulimit=core=0:0",
                "--ulimit=nofile=256:256",
                "--ulimit=fsize=33554432:33554432",
                "--shm-size=1m",
                "--tmpfs=/var/lib/cloudflare-warp:rw,noexec,nosuid,nodev,size=32m,mode=0700",
                "--tmpfs=/run:rw,noexec,nosuid,nodev,size=8m,mode=0755",
                "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=8m,mode=1777",
                "--tmpfs=/var/log:rw,noexec,nosuid,nodev,size=8m,mode=0700",
                &self.image,
            ])
            .await?;
            ensure!(
                crate::runtime::packet_network::immutable_id(&id),
                "fixture container ID is not immutable"
            );
            self.ids.push(id.clone());
            let value: serde_json::Value = serde_json::from_str(&docker(&["inspect", &id]).await?)?;
            let value = &value[0];
            ensure!(
                value["Config"]["Image"] == self.image
                    && value["HostConfig"]["NetworkMode"] == "bridge"
                    && value["HostConfig"]["ReadonlyRootfs"] == true
                    && value["HostConfig"]["Privileged"] == false
                    && value["HostConfig"]["Memory"] == 256 * 1024 * 1024
                    && value["HostConfig"]["MemorySwap"] == 256 * 1024 * 1024
                    && value["HostConfig"]["NanoCpus"] == 200_000_000
                    && value["HostConfig"]["PidsLimit"] == 64
                    && value["HostConfig"]["PortBindings"]
                        .as_object()
                        .is_some_and(|ports| ports.is_empty())
                    && value["Mounts"]
                        .as_array()
                        .is_some_and(|mounts| mounts.iter().all(|mount| mount["Type"] == "tmpfs")),
                "fixture native profile differs"
            );
            Ok(id)
        }
        async fn stopped(&self, id: &str) -> Result<()> {
            let values: serde_json::Value = serde_json::from_str(&docker(&["inspect", id]).await?)?;
            let value = &values[0];
            ensure!(
                value["Id"] == id
                    && value["Config"]["Image"] == self.image
                    && value["Config"]["Labels"]["elixir.marketplace.warp_server_test"]
                        == self.owner
                    && value["State"]["Running"] == false
                    && value["State"]["ExitCode"] == 0
                    && value["State"]["OOMKilled"] == false,
                "original provider did not terminate cleanly"
            );
            Ok(())
        }
    }
    async fn native_stream(id: &str) -> Result<PrivateProcessStdio> {
        let mut child = tokio::process::Command::new("docker")
            .args(["start", "--attach", "--interactive", id])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(PrivateProcessStdio {
            input: child.stdin.take().context("missing native stdin")?,
            output: child.stdout.take().context("missing native stdout")?,
            diagnostics: child.stderr.take(),
            child,
        })
    }
    #[tokio::test]
    #[ignore = "explicit owner-approved free Cloudflare provider + encrypted server-store integration"]
    async fn real_free_provider_uses_encrypted_server_state_before_ack_and_resumes_one_identity()
    -> Result<()> {
        ensure!(
            std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1"),
            "explicit live free-provider consent required"
        );
        let image = std::env::var("ELIXIR_MARKETPLACE_WARP_PROVIDER_IMAGE")?;
        ensure!(
            image
                .strip_prefix("sha256:")
                .is_some_and(crate::runtime::packet_network::immutable_id),
            "immutable inspected native provider image required"
        );
        let image_digest: Digest = image.parse()?;
        let mut fixture = NativeFixture {
            owner: uuid::Uuid::new_v4().to_string(),
            image: image.clone(),
            ids: vec![],
        };
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let key = Arc::new(SecretsManager::from_key_bytes([95; 32], false));
        let scope = Scope {
            deployment: Digest::of(uuid::Uuid::new_v4().as_bytes()),
            enrollment: Digest::of(uuid::Uuid::new_v4().as_bytes()),
        };
        let mut prior_identity = None;
        for resume in [false, true] {
            let id = fixture.create().await?;
            let start = Start {
                schema_version: 1,
                session: Digest::of(uuid::Uuid::new_v4().as_bytes()),
                scope,
                consent_version: CONSENT_VERSION.into(),
                resume,
            };
            let mut client = Client::start(
                native_stream(&id).await?,
                start,
                image_digest,
                ControllerLease::acquire(&directory)?,
                key.clone(),
            )
            .await?;
            client.connect().await?;
            let mut protected = false;
            for _ in 0..6 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                client.inspect().await?;
                let value=docker(&["exec",&id,"/bin/bash","-c","set -o pipefail; curl --noproxy '*' --fail --silent --connect-timeout 2 --max-time 4 https://www.cloudflare.com/cdn-cgi/trace 2>/dev/null | grep -q '^warp=on$' && printf protected || printf unavailable"]).await?;
                if value == "protected" {
                    protected = true;
                    break;
                }
            }
            ensure!(protected, "native fixed-domain TLS probe was not protected");
            if resume {
                tokio::time::sleep(Duration::from_millis(300)).await;
                client.revoke().await?;
            } else {
                tokio::time::sleep(Duration::from_millis(300)).await;
                client.stop().await?;
            }
            fixture.stopped(&id).await?;
            let lease = ControllerLease::acquire(&directory)?;
            let saved = provider_state::load(&lease, &key, scope, image_digest)?
                .context("server did not retain encrypted provider state")?;
            let state = State::decode(saved.bytes, scope)?;
            if let Some(prior) = prior_identity {
                ensure!(
                    state.identity() == prior,
                    "provider identity changed across restart"
                );
            } else {
                prior_identity = Some(state.identity());
            }
            let record = std::fs::read_to_string(directory.join("provider-state.v1"))?;
            ensure!(
                record.contains("enc:v1:bound:") && !record.contains("private_key"),
                "provider state was not encrypted at rest"
            );
        }
        for id in &fixture.ids {
            ensure!(
                docker(&["rm", id]).await? == *id,
                "owned provider removal unconfirmed"
            );
        }
        fixture.ids.clear();
        ensure!(
            docker(&[
                "ps",
                "-a",
                "--filter",
                &format!(
                    "label=elixir.marketplace.warp_server_test={}",
                    fixture.owner
                ),
                "--format",
                "{{.ID}}"
            ])
            .await?
            .is_empty(),
            "owned provider fixtures remain"
        );
        eprintln!(
            "{}",
            serde_json::json!({"image":image,"registration_calls":1,"provider_starts":2,"encrypted_server_state":true,"same_identity_after_restart":true,
            "commit_before_ack":true,"protected_fixed_tls_both_starts":true,"all_owned_containers_retired":true,
            "excludes":"production native owner/fence factory, packet guard/protected DNS/worker/public/playback/redistribution qualification; revoke does not prove remote retention erasure"})
        );
        Ok(())
    }
}
