//! Trusted local configuration only. No peer/HTTP DTO may choose these paths.
use anyhow::{Context, Result, bail};
use elixir_marketplace::{container_image::import::SocketBinding, identity::Digest};
use std::{
    fs::{self, OpenOptions},
    io::Read,
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::Stdio,
};

#[async_trait::async_trait]
pub(crate) trait CacheControl: Send + Sync {
    fn directory(&self) -> &Path;
    async fn evict(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct Profile {
    executable: PathBuf,
    digest: Digest,
    socket: SocketBinding,
    cache: std::sync::Arc<std::sync::Mutex<Option<std::sync::Weak<dyn CacheControl>>>>,
    cache_enabled: bool,
}
impl Profile {
    pub fn from_operator_configuration(
        executable: PathBuf,
        digest: Digest,
        socket: PathBuf,
    ) -> Result<Self> {
        if !executable.is_absolute()
            || executable.canonicalize()? != executable
            || !socket.is_absolute()
            || socket.canonicalize()? != socket
        {
            bail!("import profile needs canonical local host paths");
        }
        let m = fs::symlink_metadata(&socket)?;
        if !m.file_type().is_socket() {
            bail!("import profile requires a local Unix socket");
        }
        let profile = Self {
            executable,
            digest,
            socket: SocketBinding {
                path: socket.to_str().context("import socket UTF-8")?.into(),
                device: m.dev(),
                inode: m.ino(),
            },
            cache: Default::default(),
            cache_enabled: false,
        };
        profile.require()?;
        Ok(profile)
    }
    pub(crate) fn socket(&self) -> SocketBinding {
        self.socket.clone()
    }
    /// Trusted prelaunch opt-in only; the independently approved companion
    /// must implement the passive-reference keeper. Never a peer/HTTP option.
    pub(crate) fn with_image_cache(mut self) -> Self {
        self.cache_enabled = true;
        self
    }
    pub(crate) fn image_cache_enabled(&self) -> bool {
        self.cache_enabled
    }
    pub(crate) fn register_cache(&self, cache: std::sync::Arc<dyn CacheControl>) -> Result<()> {
        let mut slot = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("image cache hook poisoned"))?;
        if slot.as_ref().and_then(std::sync::Weak::upgrade).is_some() {
            bail!("image cache already has an actual owner");
        }
        *slot = Some(std::sync::Arc::downgrade(&cache));
        Ok(())
    }
    pub(crate) fn cache(
        &self,
        directory: &Path,
    ) -> Result<Option<std::sync::Arc<dyn CacheControl>>> {
        let slot = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("image cache hook poisoned"))?;
        let value = slot.as_ref().and_then(std::sync::Weak::upgrade);
        if value
            .as_ref()
            .is_some_and(|cache| cache.directory() != directory)
        {
            bail!("image cache belongs to another foreground lane");
        }
        Ok(value)
    }
    fn require(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(&self.executable)?;
        let m = file.metadata()?;
        if !m.is_file()
            || m.len() == 0
            || m.len() > 64 * 1024 * 1024
            || m.mode() & 0o022 != 0
            || m.mode() & 0o111 == 0
        {
            bail!("import companion must be a bounded independently approved executable");
        }
        use sha2::{Digest as _, Sha256};
        let mut hash = Sha256::new();
        let mut buffer = [0; 65536];
        let mut remaining = m.len();
        while remaining > 0 {
            let n = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..n])?;
            hash.update(&buffer[..n]);
            remaining -= n as u64;
        }
        if file.read(&mut [0; 1])? != 0
            || format!("sha256:{:x}", hash.finalize()) != self.digest.to_string()
        {
            bail!("import companion differs from its independent approval");
        }
        let current = fs::symlink_metadata(&self.executable)?;
        if current.dev() != m.dev() || current.ino() != m.ino() || current.len() != m.len() {
            bail!("import companion changed during approval");
        }
        Ok(())
    }
    pub(crate) fn spawn(&self, lane: &Path, cleanup: bool) -> Result<tokio::process::Child> {
        self.spawn_role(lane, if cleanup { Some("--cleanup") } else { None })
    }
    pub(crate) fn spawn_reference_cleanup(&self, lane: &Path) -> Result<tokio::process::Child> {
        self.spawn_role(lane, Some("--cleanup-reference"))
    }
    pub(crate) fn spawn_cache(&self, lane: &Path, cleanup: bool) -> Result<tokio::process::Child> {
        self.spawn_role(
            lane,
            Some(if cleanup {
                "--cleanup-image-cache"
            } else {
                "--retain-image-cache"
            }),
        )
    }
    fn spawn_role(&self, lane: &Path, mode: Option<&'static str>) -> Result<tokio::process::Child> {
        self.require()?;
        let mut command = tokio::process::Command::new(&self.executable);
        command
            .arg(lane)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        if let Some(mode) = mode {
            command.arg(mode);
        }
        Ok(command.spawn()?)
    }
}
