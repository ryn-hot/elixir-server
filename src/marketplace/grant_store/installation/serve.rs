//! Long-lived installed executors: one per installed instance, started on its
//! first call and reused (docs/architecture/arr-stack-marketplace-extension.md).
//! Every call rechecks the instance's ACTIVE binding; calls to one instance run
//! one at a time. An executor stops after 10 idle minutes, before its one-hour
//! lifetime, when its release changes, after a failure, and at shutdown.
use super::*;
use crate::marketplace::{
    archive::{ArchiveBudget, Replay},
    lease::ControllerLease,
    quarantine::execution::{Broker, ServeOutcome, ServeSession},
    runtime::SourceJobLimits,
    stack::{StackMemberEndpoint, StackMembers},
};
use elixir_marketplace::{
    canonical,
    quarantine::QuarantinePurpose,
    records::RuntimeProfile,
    source_execution::{
        RuntimeJson, SdkHttpMethod, SourceExecutionLimits, SourceSdkLimits, SourceSdkReply,
        SourceSdkRequest, SourceServeDescriptor,
    },
};
use runtime::InstalledRuntime;
use standing::InstalledBinding;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, time::Instant};

pub(in crate::marketplace) const IDLE_STOP: Duration = Duration::from_secs(10 * 60);
const SESSION_SECONDS: u32 = 3600;
/// Replace an executor this long before its hard lifetime ends.
const RENEW_BEFORE: Duration = Duration::from_secs(10 * 60);
const CHECK: Duration = Duration::from_millis(500);
const MIB: u64 = 1024 * 1024;
/// A member's reply body, as text, at most this large.
const MEMBER_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Per-call limits of a long-lived call. The heap is shared by the session.
pub(in crate::marketplace) fn serve_limits() -> SourceExecutionLimits {
    SourceExecutionLimits {
        profile: Default::default(),
        // Prowlarr's indexer catalog alone is about 6 MB and growing.
        heap_bytes: 64 * MIB,
        timeout_millis: 30_000,
        max_file_bytes: 4 * MIB,
        json_bytes: 16 * MIB,
        pending_jobs: 4096,
        sdk: Some(SourceSdkLimits {
            calls: 256,
            request_bytes: 4 * MIB,
            response_bytes: 64 * MIB,
        }),
    }
}

struct Slot {
    session: ServeSession,
    /// The installed release's source descriptor; a change means an update.
    release: Digest,
}

pub(in crate::marketplace) struct ServeHost {
    database: Database,
    secrets: SecretsManager,
    configuration: InstalledRuntime,
    members: Option<Arc<dyn StackMembers>>,
    slots: std::sync::Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<Option<Slot>>>>>,
}

impl ServeHost {
    pub(in crate::marketplace) fn new(
        database: Database,
        secrets: SecretsManager,
        configuration: InstalledRuntime,
        members: Option<Arc<dyn StackMembers>>,
    ) -> Self {
        Self {
            database,
            secrets,
            configuration,
            members,
            slots: Default::default(),
        }
    }

    fn slot(&self, instance: Uuid) -> Result<Arc<tokio::sync::Mutex<Option<Slot>>>> {
        Ok(self
            .slots
            .lock()
            .map_err(|_| anyhow::anyhow!("extension executors unavailable"))?
            .entry(instance)
            .or_default()
            .clone())
    }

    /// Crash leftovers: executors this server no longer holds.
    pub(in crate::marketplace) async fn remove_orphans(&self) -> Result<usize> {
        let mut keep = Vec::new();
        let slots: Vec<_> = self
            .slots
            .lock()
            .map_err(|_| anyhow::anyhow!("extension executors unavailable"))?
            .values()
            .cloned()
            .collect();
        for slot in slots {
            if let Ok(held) = slot.try_lock()
                && let Some(id) = held.as_ref().and_then(|slot| slot.session.container_id())
            {
                keep.push(id.to_owned());
            }
        }
        self.configuration
            .runtime
            .remove_marketplace_serve_executors(&keep)
            .await
    }

    /// One call to an installed instance's long-lived executor.
    pub(in crate::marketplace) async fn call(
        &self,
        home: Uuid,
        extension: &ExtensionId,
        instance: Uuid,
        budget: InstallBudget,
        arguments: RuntimeJson,
    ) -> Result<ServeOutcome> {
        let binding =
            InstalledBinding::load_configured(&self.database, home, extension, instance, budget)
                .await?;
        let slot = self.slot(instance)?;
        let mut held = slot.lock().await;
        let capsule = binding.material(&self.database).await?;
        let descriptor = capsule.descriptor()?;
        let release = Digest::of(canonical::to_vec(&descriptor)?);
        let stale = held.as_ref().is_some_and(|slot| {
            slot.release != release
                || !slot.session.healthy()
                || slot.session.age() + RENEW_BEFORE
                    >= Duration::from_secs(u64::from(SESSION_SECONDS))
        });
        if stale && let Some(old) = held.take() {
            old.session.stop().await?;
        }
        let mut broker = ServeBroker {
            database: &self.database,
            secrets: &self.secrets,
            binding: &binding,
            stack: extension.as_str(),
            members: self.members.as_deref(),
            checked: None,
        };
        if held.is_none() {
            let session = self
                .start(&binding, &capsule, descriptor, &mut broker)
                .await?;
            *held = Some(Slot { session, release });
        }
        let slot = held.as_mut().expect("an executor is held");
        let outcome = slot.session.call(&arguments, &mut broker).await;
        if outcome.is_err() || !slot.session.healthy() {
            if let Some(old) = held.take() {
                let _ = old.session.stop().await;
            }
        }
        outcome
    }

    async fn start(
        &self,
        binding: &InstalledBinding,
        capsule: &Capsule,
        descriptor: elixir_marketplace::source_job::SourceMatchDescriptor,
        broker: &mut ServeBroker<'_>,
    ) -> Result<ServeSession> {
        let facts = capsule.facts()?;
        if facts.release().payload().runtime_profile != RuntimeProfile::SourceJs {
            bail!("only source modules can run as long-lived executors");
        }
        let serve = SourceServeDescriptor {
            schema_version: 1,
            session_id: Digest::of(Uuid::new_v4().as_bytes()),
            source: descriptor.clone(),
            limits: serve_limits(),
            session_seconds: SESSION_SECONDS,
        };
        // Exactly what the executor needs: its heap, file and JSON working set,
        // and the package and proof while it verifies them.
        let memory = serve.minimum_memory_bytes()?.div_ceil(MIB) * MIB;
        let resources = binding
            .review()
            .scope()
            .runtime()
            .context("installed source has no reviewed runtime resources")?;
        if memory > resources.memory_bytes {
            bail!(
                "this extension's approved memory ({} MiB) is below what a long-lived executor needs ({} MiB)",
                resources.memory_bytes / MIB,
                memory / MIB
            );
        }
        let limits = SourceJobLimits::new(memory, self.configuration.source_budget.cpu_millicores)?;
        crate::marketplace::worker::serve_start_admitted(&self.configuration.playback, memory)
            .await?;
        let (package, proof) = self.material(&descriptor).await?;
        ServeSession::start(
            self.configuration.runtime.clone(),
            &self.configuration.image,
            serve,
            &limits,
            &package,
            &proof,
            capsule.source.summary(),
            broker,
        )
        .await
    }

    /// The signed package and proof, read from the retained archive under a
    /// lease of this host's own; each byte is hashed against the signed blob.
    async fn material(
        &self,
        descriptor: &elixir_marketplace::source_job::SourceMatchDescriptor,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let lease = Arc::new(ControllerLease::acquire(
            &self.configuration.lane.join("serve"),
        )?);
        let budget: ArchiveBudget = self.configuration.retention;
        let allowance = Replay::read_allowance(budget, descriptor)?;
        let mut last = None;
        // The archive's own lock is held only while another job reads it.
        for _ in 0..20 {
            let path = self.configuration.archive.clone();
            let expected = descriptor.clone();
            let lease = lease.clone();
            let opened = tokio::task::spawn_blocking(move || {
                Replay::open(
                    &path,
                    budget,
                    &expected,
                    lease,
                    allowance,
                    Instant::now() + Duration::from_secs(60),
                )
            })
            .await?;
            match opened {
                Ok(mut replay) => {
                    let mut package = Vec::new();
                    replay
                        .take(QuarantinePurpose::Package)?
                        .read_to_end(&mut package)
                        .await?;
                    let mut proof = Vec::new();
                    replay
                        .take(QuarantinePurpose::Proof)?
                        .read_to_end(&mut proof)
                        .await?;
                    return Ok((package, proof));
                }
                Err(error) => {
                    last = Some(error);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("installed archive unavailable")))
    }

    /// Stops executors idle for 10 minutes or near their lifetime; busy ones
    /// are left alone.
    pub(in crate::marketplace) async fn reap(&self) -> Result<()> {
        let slots: Vec<_> = self
            .slots
            .lock()
            .map_err(|_| anyhow::anyhow!("extension executors unavailable"))?
            .values()
            .cloned()
            .collect();
        for slot in slots {
            let Ok(mut held) = slot.try_lock() else {
                continue;
            };
            let expired = held.as_ref().is_some_and(|slot| {
                slot.session.idle() >= IDLE_STOP
                    || slot.session.age() + RENEW_BEFORE
                        >= Duration::from_secs(u64::from(SESSION_SECONDS))
            });
            if expired && let Some(old) = held.take() {
                old.session.stop().await?;
            }
        }
        Ok(())
    }

    pub(in crate::marketplace) async fn shutdown(&self) -> Result<()> {
        let slots: Vec<_> = self
            .slots
            .lock()
            .map_err(|_| anyhow::anyhow!("extension executors unavailable"))?
            .drain()
            .map(|(_, slot)| slot)
            .collect();
        for slot in slots {
            if let Some(old) = slot.lock().await.take() {
                old.session.stop().await?;
            }
        }
        Ok(())
    }
}

/// SDK answers for a long-lived call: its stack members over Elixir's own
/// HTTP client, each member's API key, and the instance's secrets and state.
/// Every other network request is refused.
struct ServeBroker<'a> {
    database: &'a Database,
    secrets: &'a SecretsManager,
    binding: &'a InstalledBinding,
    stack: &'a str,
    members: Option<&'a dyn StackMembers>,
    checked: Option<Instant>,
}

#[async_trait::async_trait]
impl Broker for ServeBroker<'_> {
    async fn current(&mut self) -> Result<()> {
        if self.checked.is_none_or(|last| last.elapsed() >= CHECK) {
            self.binding.current(self.database).await?;
            self.checked = Some(Instant::now());
        }
        Ok(())
    }
    async fn request(&mut self, raw: &RuntimeJson, maximum: usize) -> Result<SourceSdkReply> {
        match SourceSdkRequest::parse(raw)? {
            SourceSdkRequest::Fetch {
                url,
                method,
                headers,
                body,
            } => {
                self.member_fetch(&url, method, headers, body, maximum)
                    .await
            }
            request if request.is_http() => Ok(SourceSdkReply::Denied),
            SourceSdkRequest::Secret { key } if key.ends_with(".api_key") => {
                let member = key.trim_end_matches(".api_key");
                match self.member(member).await? {
                    Some(endpoint) => Ok(SourceSdkReply::Value(RuntimeJson::from_value(
                        endpoint
                            .api_key
                            .map_or(serde_json::Value::Null, serde_json::Value::String),
                    )?)),
                    None => Ok(SourceSdkReply::Value(RuntimeJson::from_value(
                        serde_json::Value::Null,
                    )?)),
                }
            }
            _ => {
                super::super::sdk::InstalledScope {
                    database: self.database,
                    secrets: self.secrets,
                    binding: self.binding,
                }
                .execute(raw, maximum, || Ok(()))
                .await
            }
        }
    }
}

impl ServeBroker<'_> {
    async fn member(&self, member: &str) -> Result<Option<StackMemberEndpoint>> {
        let Some(members) = self.members else {
            return Ok(None);
        };
        members.member(self.stack, member).await
    }

    /// `http://<member>.stack/<path>` only: Elixir finds that member of this
    /// stack and makes the request itself. No other host is reachable.
    async fn member_fetch(
        &self,
        url: &str,
        method: SdkHttpMethod,
        headers: Vec<elixir_marketplace::source_execution::SdkHeader>,
        body: Option<String>,
        maximum: usize,
    ) -> Result<SourceSdkReply> {
        let parsed = reqwest::Url::parse(url)?;
        let Some(member) = parsed
            .host_str()
            .and_then(|host| host.strip_suffix(".stack"))
            .filter(|member| !member.is_empty() && !member.contains('.'))
        else {
            return Ok(SourceSdkReply::Denied);
        };
        if parsed.scheme() != "http" || parsed.port().is_some() {
            return Ok(SourceSdkReply::Denied);
        }
        let Some(endpoint) = self.member(member).await? else {
            return Ok(SourceSdkReply::Unavailable);
        };
        let mut target = reqwest::Url::parse(&endpoint.base_url)?;
        let base = target.path().trim_end_matches('/').to_owned();
        target.set_path(&format!("{base}{}", parsed.path()));
        target.set_query(parsed.query());
        let method = match method {
            SdkHttpMethod::Get => reqwest::Method::GET,
            SdkHttpMethod::Head => reqwest::Method::HEAD,
            SdkHttpMethod::Post => reqwest::Method::POST,
            SdkHttpMethod::Put => reqwest::Method::PUT,
            SdkHttpMethod::Delete => reqwest::Method::DELETE,
        };
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(25))
            .no_proxy()
            .build()?;
        let mut request = client.request(method, target);
        for header in headers {
            request = request.header(header.name, header.value);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(_) => return Ok(SourceSdkReply::Unavailable),
        };
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .filter(|value| value.len() <= 256 && !value.bytes().any(|c| c < 32 || c == 127))
            .map(str::to_owned);
        let limit = maximum.saturating_sub(4096).min(MEMBER_BODY_BYTES);
        let mut bytes = Vec::new();
        let mut stream = response;
        while let Some(chunk) = stream.chunk().await? {
            if bytes.len() + chunk.len() > limit {
                bail!("stack member reply is larger than this call allows");
            }
            bytes.extend_from_slice(&chunk);
        }
        let headers = content_type
            .map(|value| vec![serde_json::json!({"name": "content-type", "value": value})])
            .unwrap_or_default();
        Ok(SourceSdkReply::Value(RuntimeJson::from_value(serde_json::json!({
            "status": status,
            "headers": headers,
            "body": String::from_utf8_lossy(&bytes),
        }))?))
    }
}
