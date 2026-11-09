//! Fresh read-only observations through the independently pinned local daemon.
//! Only the HTTP client is reused: never responses, running state or authority.
//! No endpoint discovery, TCP, proxy, DNS, redirect or CLI fallback on failure.
use super::*;
use elixir_marketplace::{
    canonical,
    container_image::import::{MAX_INSPECTION_BYTES, SocketBinding},
    identity::Digest,
};
use std::os::unix::fs::{FileTypeExt, MetadataExt};

const DEADLINE: Duration = Duration::from_secs(5);
const MAX_CONCURRENT: usize = 4;

pub(super) enum Query<'a> {
    Container(&'a str),
    Image(Digest),
    Info,
}
impl Query<'_> {
    fn exact_absence(&self, value: &Value) -> bool {
        let expected = match self {
            Self::Container(reference) => format!("No such container: {reference}"),
            Self::Image(image) => format!("No such image: {image}"),
            Self::Info => return false,
        };
        value.as_object().is_some_and(|object| object.len() == 1)
            && value["message"].as_str() == Some(&expected)
    }
    fn path(&self) -> Result<String> {
        Ok(match self {
            Self::Container(reference) => {
                // One exact Docker identifier, never a URL/path/query supplied
                // by an extension. No prefix lookup or arbitrary API methods.
                if reference.is_empty()
                    || reference.len() > 255
                    || !reference.as_bytes()[0].is_ascii_alphanumeric()
                    || !reference
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
                {
                    bail!("invalid exact local container reference");
                }
                format!("/v1.45/containers/{reference}/json")
            }
            Self::Image(image) => format!("/v1.45/images/{image}/json"),
            Self::Info => "/v1.45/info".into(),
        })
    }
}

pub(super) struct Observer {
    binding: SocketBinding,
    client: reqwest::Client,
    slots: tokio::sync::Semaphore,
}
impl Observer {
    fn new(binding: SocketBinding) -> Result<Self> {
        Self::with_deadline(binding, DEADLINE)
    }
    fn with_deadline(binding: SocketBinding, deadline: Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .http1_only()
            .unix_socket(Path::new(&binding.path))
            // Each check reconnects to the pinned pathname. An idle connection
            // must not survive daemon/socket replacement as stale authority.
            .pool_max_idle_per_host(0)
            .timeout(deadline)
            .build()?;
        let value = Self {
            binding,
            client,
            slots: tokio::sync::Semaphore::new(MAX_CONCURRENT),
        };
        value.require_socket()?;
        Ok(value)
    }
    fn require_socket(&self) -> Result<()> {
        let path = Path::new(&self.binding.path);
        let m = std::fs::symlink_metadata(path)?;
        if !m.file_type().is_socket()
            || m.dev() != self.binding.device
            || m.ino() != self.binding.inode
            || path.canonicalize()? != path
        {
            bail!("independently pinned local daemon socket changed");
        }
        Ok(())
    }
    pub(super) async fn get(&self, query: Query<'_>) -> Result<Option<Value>> {
        timeout(DEADLINE, async {
            let _slot = self.slots.acquire().await?;
            let path = query.path()?;
            self.require_socket()?;
            // unix_socket forces local IPC, including for this literal host;
            // it does not resolve or connect to localhost through IP.
            let mut response = self
                .client
                .get(format!("http://localhost{path}"))
                .send()
                .await?;
            self.require_socket()?;
            let missing = response.status() == reqwest::StatusCode::NOT_FOUND;
            if (response.status() != reqwest::StatusCode::OK && !missing)
                || response
                    .content_length()
                    .is_some_and(|n| n > MAX_INSPECTION_BYTES as u64)
            {
                bail!("bounded local daemon observation unavailable");
            }
            let mut bytes = Vec::new();
            while let Some(part) = response.chunk().await? {
                if part.len() > MAX_INSPECTION_BYTES.saturating_sub(bytes.len()) {
                    bail!("local daemon observation exceeds its bound");
                }
                bytes.extend_from_slice(&part);
            }
            self.require_socket()?;
            let value = canonical::parse(&bytes, MAX_INSPECTION_BYTES)?;
            if !value.is_object() {
                bail!("local daemon observation is not an object");
            }
            if missing {
                // A routing/version/proxy 404 is not positive absence. Cleanup
                // may return its fence only for Docker's exact-object result.
                if !query.exact_absence(&value) {
                    bail!("local daemon did not confirm exact object absence");
                }
                return Ok(None);
            }
            Ok(Some(value))
        })
        .await
        .context("local daemon observation deadline")?
    }
}

impl DockerRuntimeManager {
    pub(super) async fn local_observer(&self) -> Result<&Observer> {
        self.local_observer
            .get_or_try_init(|| async {
                Observer::new(
                    self.import_profile
                        .as_ref()
                        .context("local daemon observation is not independently configured")?
                        .socket(),
                )
            })
            .await
    }
    pub(super) fn use_local_observation(&self, reference: &str) -> bool {
        // The first container lookup uses the CLI path, which validates its
        // daemon pairing before any absence can be reported. Subsequent exact
        // marketplace/immutable lookups can avoid the CLI process. Unconfigured
        // remote contexts and named legacy lookup behavior stay unchanged.
        self.import_profile.is_some()
            && self.local_cli_binding.get().is_some()
            && (immutable_id(reference) || reference.starts_with("elixir-marketplace-"))
    }
    pub(super) async fn observe_local_container(&self, reference: &str) -> Result<Option<Value>> {
        if self.local_cli_binding.get().is_none() {
            bail!("local container observation requires its validated CLI pairing");
        }
        let value = self
            .local_observer()
            .await?
            .get(Query::Container(reference))
            .await?;
        if let Some(value) = &value {
            let id = value["Id"]
                .as_str()
                .context("local container identity absent")?;
            if !immutable_id(id)
                || (immutable_id(reference) && reference != id)
                || (!immutable_id(reference)
                    && value["Name"].as_str() != Some(&format!("/{reference}")))
            {
                bail!("local container lookup did not return its exact immutable identity");
            }
        }
        Ok(value)
    }
    pub(super) async fn local_docker_command(&self) -> Result<Command> {
        self.local_cli_binding
            .get_or_try_init(|| async {
                // First prove the existing CLI context and explicit local import
                // socket identify the same daemon. Do not silently move core media
                // operations to a different engine when installing this profile.
                let bytes = self
                    .import_observe(&["info".into(), "--format".into(), "{{json .ID}}".into()])
                    .await?;
                let cli = Self::require_import_daemon_id(&canonical::parse(&bytes, 1024)?)?;
                if cli != self.image_import_daemon_id().await? {
                    bail!("Docker CLI and pinned import socket identify different daemons");
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?;
        self.local_observer().await?.require_socket()?;
        let binding = self
            .import_profile
            .as_ref()
            .context("local daemon profile absent")?
            .socket();
        let mut command = Command::new(&self.docker_bin);
        // Every subsequent CLI operation, including cleanup and stdio attach,
        // uses that same explicit endpoint even if the saved context changes.
        command
            .arg("--host")
            .arg(format!("unix://{}", binding.path));
        for key in [
            "DOCKER_HOST",
            "DOCKER_CONTEXT",
            "DOCKER_TLS_VERIFY",
            "DOCKER_CERT_PATH",
        ] {
            command.env_remove(key);
        }
        Ok(command)
    }
}

#[cfg(test)]
mod tests;
