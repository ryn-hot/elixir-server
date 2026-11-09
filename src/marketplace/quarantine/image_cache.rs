//! One passive image slot, independently locked from foreground source work.
//! Only actual in-process import provenance can fill it. Disk journals recover
//! cleanup, never verified code, SDK authority or an old source-job deadline.
use super::*;
use crate::runtime::image_import::{CacheControl, Profile};
use elixir_marketplace::container_image::{
    import,
    job::ContainerImageSummary,
    reference::{MAX_REQUEST_BYTES, ReferenceRequest},
};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::{
    process::Child,
    sync::{Mutex, OwnedMutexGuard, watch},
};

const DIRECTORY: &str = "compiled-image-cache";
const IDLE: Duration = Duration::from_secs(300);
const TICK: Duration = Duration::from_secs(5);
const REQUEST: &str = "image-cache.request";
const CLEARING: &str = "image-cache.clearing";
const CONTROLS: [&str; 3] = [
    "image-cache.attempt",
    "image-cache.result",
    "image-cache.retired",
];

struct Core {
    lease: Arc<ControllerLease>,
    profile: Arc<Profile>,
    runtime: Arc<dyn RuntimeManager>,
}
impl Core {
    fn current(&self) -> Result<()> {
        if !self.lease.matches_directory(self.lease.directory()) {
            bail!("image cache lost its actual private owner");
        }
        Ok(())
    }
    fn read(&self, name: &str, max: usize) -> Result<(File, Vec<u8>)> {
        self.current()?;
        if name != REQUEST && name != CLEARING && !CONTROLS.contains(&name) {
            bail!("invalid image cache control role");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(self.lease.directory().join(name))?;
        let m = file.metadata()?;
        if !m.is_file()
            || m.nlink() != 1
            || m.uid() != unsafe { libc::geteuid() }
            || m.mode() & 0o077 != 0
            || m.len() > max as u64
        {
            bail!("invalid private image cache control");
        }
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(max as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > max {
            bail!("image cache control exceeds bound");
        }
        Ok((file, bytes))
    }
    fn same_request(&self, held: &File, request: &ReferenceRequest) -> Result<()> {
        self.validate_request(request)?;
        let (file, bytes) = self.read(REQUEST, MAX_REQUEST_BYTES)?;
        let a = file.metadata()?;
        let b = held.metadata()?;
        if a.dev() != b.dev() || a.ino() != b.ino() || bytes != canonical::to_vec(request)? {
            bail!("image cache request inode/binding changed");
        }
        Ok(())
    }
    fn write(&self, request: &ReferenceRequest) -> Result<File> {
        self.validate_request(request)?;
        self.write_body(REQUEST, &canonical::to_vec(request)?)
    }
    fn validate_request(&self, request: &ReferenceRequest) -> Result<()> {
        request.validate()?;
        if request.storage_namespace.to_string()
            != format!("sha256:{}", self.lease.storage_namespace()?)
        {
            bail!("image cache control belongs to another actual lane");
        }
        Ok(())
    }
    fn write_body(&self, name: &str, bytes: &[u8]) -> Result<File> {
        self.current()?;
        if ![REQUEST, CLEARING].contains(&name) || bytes.len() > MAX_REQUEST_BYTES {
            bail!("invalid cache control write");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o400)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.lease.directory().join(name))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        File::open(self.lease.directory())?.sync_all()?;
        Ok(file)
    }
}

struct Pending {
    request: ReferenceRequest,
    file: Option<File>,
    child: Option<Child>,
}
struct Slot {
    core: Arc<Core>,
    pending: Option<Pending>,
    entry: Option<Arc<Anchor>>,
    cleanup: Option<Child>,
}
impl Slot {
    async fn settle(&mut self) -> Result<()> {
        if let Some(pending) = self.pending.as_mut()
            && let Some(child) = pending.child.as_mut()
        {
            child.wait().await?;
            pending.child = None;
        }
        Ok(())
    }
    async fn clear(&mut self) -> Result<()> {
        self.settle().await?;
        if let Some(entry) = &self.entry {
            entry.active.store(false, Ordering::Release);
        }
        let Some(pending) = self.pending.as_ref() else {
            return Ok(());
        };
        self.core.validate_request(&pending.request)?;
        let request_bytes = canonical::to_vec(&pending.request)?;
        let expected = pending.request.receipt()?;
        let clearing = match self.core.read(CLEARING, MAX_REQUEST_BYTES) {
            Ok((_, bytes)) => Some(bytes),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        let committed = clearing.as_ref() == Some(&request_bytes);
        if !committed {
            self.core.same_request(
                pending
                    .file
                    .as_ref()
                    .context("cache request missing without a terminal clearing record")?,
                &pending.request,
            )?;
            let retired = match self.core.read("image-cache.retired", 4096) {
                Ok((_, bytes)) => Some(bytes),
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    None
                }
                Err(error) => return Err(error),
            };
            if retired.is_none() {
                if clearing.is_some() {
                    bail!("incomplete cache clearing has no original retired proof");
                }
                if self.cleanup.is_none() {
                    self.cleanup = Some(
                        self.core
                            .profile
                            .spawn_cache(self.core.lease.directory(), true)?,
                    );
                }
                let success = self
                    .cleanup
                    .as_mut()
                    .context("cache cleanup child missing")?
                    .wait()
                    .await?
                    .success();
                self.cleanup = None;
                if !success {
                    bail!("original image cache retirement remains unconfirmed");
                }
            }
            if self.core.read("image-cache.retired", 4096)?.1 != expected {
                bail!("cache retirement receipt changed");
            }
        }
        // Same keeper lock excludes late starts and preserves interrupted
        // cleanup. A full-request tombstone survives request deletion and
        // blocks late normal keepers until every removal is durably synced.
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(self.core.lease.directory().join("image-cache.lock"))?;
        let m = lock.metadata()?;
        if !m.is_file()
            || m.len() != 0
            || m.nlink() != 1
            || m.mode() & 0o077 != 0
            || m.uid() != unsafe { libc::geteuid() }
        {
            bail!("invalid image cache keeper lock");
        }
        lock.try_lock()?;
        struct Unlock(File);
        impl Drop for Unlock {
            fn drop(&mut self) {
                let _ = self.0.unlock();
            }
        }
        let _lock = Unlock(lock);
        if !committed {
            self.core.same_request(
                pending
                    .file
                    .as_ref()
                    .context("original cache request missing")?,
                &pending.request,
            )?;
            if let Some(bytes) = clearing {
                // Only an interrupted prefix of OUR terminal write can be
                // replaced, and only with intact request + native retirement.
                if !request_bytes.starts_with(&bytes) {
                    bail!("foreign cache clearing record");
                }
                std::fs::remove_file(self.core.lease.directory().join(CLEARING))?;
            }
            self.core.write_body(CLEARING, &request_bytes)?;
        }
        if self.core.read(CLEARING, MAX_REQUEST_BYTES)?.1 != request_bytes {
            bail!("cache clearing transition changed");
        }
        for name in CONTROLS {
            match std::fs::symlink_metadata(self.core.lease.directory().join(name)) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
                Ok(_) => {}
            }
            if self.core.read(name, 4096)?.1 != expected {
                bail!("foreign image cache control");
            }
            std::fs::remove_file(self.core.lease.directory().join(name))?;
        }
        match std::fs::symlink_metadata(self.core.lease.directory().join(REQUEST)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
            Ok(_) => {
                if let Some(file) = &pending.file {
                    self.core.same_request(file, &pending.request)?;
                }
                if self.core.read(REQUEST, MAX_REQUEST_BYTES)?.1 != request_bytes {
                    bail!("foreign cache request during clearing");
                }
                std::fs::remove_file(self.core.lease.directory().join(REQUEST))?;
            }
        }
        File::open(self.core.lease.directory())?.sync_all()?;
        std::fs::remove_file(self.core.lease.directory().join(CLEARING))?;
        File::open(self.core.lease.directory())?.sync_all()?;
        self.entry = None;
        self.pending = None;
        Ok(())
    }
}

pub(in crate::marketplace) struct Pool {
    core: Arc<Core>,
    slot: Arc<Mutex<Slot>>,
    stop: watch::Sender<bool>,
    worker: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Pool {
    pub(in crate::marketplace) async fn open(
        runtime: Arc<dyn RuntimeManager>,
        profile: Arc<Profile>,
        lane: &std::path::Path,
    ) -> Result<Arc<Self>> {
        let directory = lane.join(DIRECTORY);
        let lease = Arc::new(ControllerLease::acquire_for_recovery(&directory)?);
        let core = Arc::new(Core {
            lease,
            profile: profile.clone(),
            runtime,
        });
        for entry in std::fs::read_dir(&directory)? {
            let name = entry?.file_name();
            if !["controller.lock", "image-cache.lock", REQUEST, CLEARING]
                .iter()
                .any(|allowed| name == *allowed)
                && !CONTROLS.iter().any(|allowed| name == *allowed)
            {
                bail!("unrelated data in the image cache lane");
            }
        }
        let pending = match std::fs::symlink_metadata(directory.join(REQUEST)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                match core.read(CLEARING, MAX_REQUEST_BYTES) {
                    Ok((_, bytes)) => Some(Pending {
                        request: ReferenceRequest::parse(&bytes)?,
                        file: None,
                        child: None,
                    }),
                    Err(error)
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        for name in CONTROLS {
                            match std::fs::symlink_metadata(directory.join(name)) {
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                Err(e) => return Err(e.into()),
                                Ok(_) => bail!("orphan image cache controls require recovery"),
                            }
                        }
                        None
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(e) => return Err(e.into()),
            Ok(_) => {
                let (file, bytes) = core.read(REQUEST, MAX_REQUEST_BYTES)?;
                Some(Pending {
                    request: ReferenceRequest::parse(&bytes)?,
                    file: Some(file),
                    child: None,
                })
            }
        };
        let mut slot = Slot {
            core: core.clone(),
            pending,
            entry: None,
            cleanup: None,
        };
        slot.clear().await?; // Recovery can NEVER restore an executable entry.
        let (stop, mut stopped) = watch::channel(false);
        let pool = Arc::new(Self {
            core,
            slot: Arc::new(Mutex::new(slot)),
            stop,
            worker: Default::default(),
        });
        profile.register_cache(pool.clone())?;
        let weak = Arc::downgrade(&pool);
        let worker = tokio::spawn(async move {
            let mut reported = false;
            loop {
                tokio::select! {_ = tokio::time::sleep(if reported {Duration::from_secs(60)} else {TICK})=>{},_ = stopped.changed()=>return}
                let Some(pool) = weak.upgrade() else {
                    return;
                };
                if let Ok(mut slot) = pool.slot.try_lock() {
                    let dirty = slot.entry.as_ref().is_some_and(|entry| {
                        entry.created.elapsed() >= IDLE || !entry.active.load(Ordering::Acquire)
                    }) || slot.pending.is_some() && slot.entry.is_none();
                    if dirty {
                        match slot.clear().await {
                            Ok(()) => reported = false,
                            Err(error) if !reported => {
                                tracing::warn!(error=%error,"image cache cleanup pending; preserving its original owner");
                                reported = true;
                            }
                            Err(_) => {}
                        }
                    } else {
                        reported = false;
                    }
                }
            }
        });
        *pool
            .worker
            .lock()
            .map_err(|_| anyhow::anyhow!("cache timer owner poisoned"))? = Some(worker);
        Ok(pool)
    }
    pub(in crate::marketplace) async fn claim(
        self: &Arc<Self>,
        plan: &mut SourceQuarantinePlan,
    ) -> Result<Arc<Borrow>> {
        let mut guard = self.slot.clone().lock_owned().await;
        let key = key(plan)?;
        if guard.entry.as_ref().is_some_and(|entry| {
            entry.key != key
                || !entry.active.load(Ordering::Acquire)
                || entry.created.elapsed() >= IDLE
                || !plan.fits_cached_image(entry.bytes)
        }) || guard.pending.is_some() && guard.entry.is_none()
        {
            guard.clear().await?;
        }
        let disk = guard.entry.as_ref().map_or(0, |entry| entry.bytes);
        plan.reserve_cached_image(disk)?;
        Ok(Arc::new(Borrow {
            core: self.core.clone(),
            slot: Mutex::new(guard),
            disk,
            key,
            reserved: plan.disk_reservation,
            ceiling: plan.disk_limit,
        }))
    }
    pub(in crate::marketplace) async fn evict(&self) -> Result<()> {
        self.slot.lock().await.clear().await
    }
    #[cfg(test)]
    pub(in crate::marketplace) async fn snapshot(&self) -> Result<Option<(Digest, String, u64)>> {
        let slot = self.slot.lock().await;
        slot.entry
            .as_ref()
            .map(|entry| {
                entry.check()?;
                Ok((
                    entry.request.image.config,
                    entry.request.operation_id.clone(),
                    entry.bytes,
                ))
            })
            .transpose()
    }
}
#[async_trait::async_trait]
impl CacheControl for Pool {
    fn directory(&self) -> &std::path::Path {
        self.core.lease.directory()
    }
    async fn evict(&self) -> Result<()> {
        self.evict().await
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        let dirty = self
            .slot
            .try_lock()
            .map_or(true, |slot| slot.pending.is_some());
        if dirty && let Ok(executor) = tokio::runtime::Handle::try_current() {
            let slot = self.slot.clone();
            executor.spawn(async move {
                loop {
                    if slot.lock().await.clear().await.is_ok() {
                        return;
                    }
                    tokio::time::sleep(TICK).await;
                }
            });
        }
    }
}

pub(in crate::marketplace) struct Borrow {
    core: Arc<Core>,
    slot: Mutex<OwnedMutexGuard<Slot>>,
    disk: u64,
    key: Digest,
    reserved: u64,
    ceiling: u64,
}
impl Borrow {
    pub(super) fn require_lane(&self, lane: &std::path::Path) -> Result<()> {
        self.core.current()?;
        if self.core.lease.directory() != lane.join(DIRECTORY) {
            bail!("cached image belongs to another foreground lane");
        }
        Ok(())
    }
    pub(super) fn require_plan(
        &self,
        plan: &SourceQuarantinePlan,
        lane: &std::path::Path,
    ) -> Result<()> {
        self.require_lane(lane)?;
        if self.key != key(plan)?
            || self.reserved != plan.disk_reservation
            || self.ceiling != plan.disk_limit
        {
            bail!("cached image lost its exact shared job reservation");
        }
        Ok(())
    }
    pub(super) async fn retire(&self, remove: bool) -> Result<()> {
        let mut slot = self.slot.lock().await;
        slot.settle().await?;
        if remove || slot.entry.is_none() {
            slot.clear().await?;
        }
        Ok(())
    }
    pub(super) async fn get(
        &self,
        source: &SourceMatchedQuarantine,
        proof: &local_build::VerifiedLocalBuild,
        filesystem: &container_filesystem::NativeContainerFilesystemMatch,
    ) -> Result<Option<Arc<Anchor>>> {
        let mut slot = self.slot.lock().await;
        let Some(entry) = slot.entry.as_ref().cloned() else {
            return Ok(None);
        };
        entry.require_source(source, proof, filesystem)?;
        if self.disk != entry.bytes {
            bail!("cache image lacks its original disk reservation");
        }
        match self
            .core
            .runtime
            .inspect_marketplace_import_image(entry.request.image.config)
            .await
        {
            Ok(bytes) => import::inspect(&bytes, &entry.request.image)?,
            Err(_) => {
                slot.clear().await?;
                return Ok(None);
            }
        }
        entry.check()?;
        Ok(Some(entry))
    }
    pub(super) async fn put(
        &self,
        source: &SourceMatchedQuarantine,
        image: &image_reference::RuntimeImage,
        filesystem: &container_filesystem::NativeContainerFilesystemMatch,
    ) -> Result<()> {
        image.require_source(source)?;
        filesystem.require_source(source)?;
        let mut slot = self.slot.lock().await;
        if slot.entry.is_some() || slot.pending.is_some() {
            bail!("image cache slot is not empty");
        }
        let bytes = footprint(filesystem.summary())?;
        if !source
            .source
            .job
            .plan
            .fits_cached_image(bytes.saturating_sub(self.disk))
        {
            return Ok(());
        }
        let profile = &self.core.profile;
        let daemon_id = self.core.runtime.marketplace_daemon_id().await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let remaining = source
            .expires()
            .saturating_duration_since(Instant::now())
            .as_millis();
        let request = ReferenceRequest {
            schema_version: 1,
            operation_id: uuid::Uuid::new_v4().to_string(),
            daemon_id,
            storage_namespace: format!("sha256:{}", self.core.lease.storage_namespace()?)
                .parse()?,
            image: image.image().clone(),
            socket: profile.socket(),
            expires_unix_millis: u64::try_from(now + remaining)?,
        };
        request.validate()?;
        let file = self.core.write(&request)?;
        slot.pending = Some(Pending {
            request: request.clone(),
            file: Some(file),
            child: None,
        });
        // Install the only child owner in the retained slot BEFORE an await.
        slot.pending.as_mut().unwrap().child =
            Some(profile.spawn_cache(self.core.lease.directory(), false)?);
        slot.settle().await?;
        if self.core.read("image-cache.result", 4096)?.1 != request.receipt()? {
            bail!("image cache adoption lacks its settled original result");
        }
        image.require_source(source)?;
        import::inspect(
            &self
                .core
                .runtime
                .inspect_marketplace_import_image(request.image.config)
                .await?,
            &request.image,
        )?;
        let original = slot
            .pending
            .as_ref()
            .context("original cache fill owner missing")?
            .file
            .as_ref()
            .context("original cache request inode missing")?;
        self.core.same_request(original, &request)?;
        let file = original.try_clone()?;
        let anchor = Arc::new(Anchor {
            core: self.core.clone(),
            file,
            request,
            source: Digest::of(canonical::to_vec(source.summary())?),
            key: self.key,
            bytes,
            created: Instant::now(),
            active: AtomicBool::new(true),
        });
        anchor.check()?;
        slot.entry = Some(anchor);
        Ok(())
    }
}

pub(super) struct Anchor {
    core: Arc<Core>,
    file: File,
    request: ReferenceRequest,
    source: Digest,
    key: Digest,
    bytes: u64,
    created: Instant,
    active: AtomicBool,
}
impl Anchor {
    pub(super) fn require_source(
        &self,
        source: &SourceMatchedQuarantine,
        proof: &local_build::VerifiedLocalBuild,
        filesystem: &container_filesystem::NativeContainerFilesystemMatch,
    ) -> Result<()> {
        self.check()?;
        proof.require_source(source)?;
        proof.require_image(&self.request.image)?;
        filesystem.require_source(source)?;
        if self.source != Digest::of(canonical::to_vec(source.summary())?)
            || canonical::to_vec(&self.request.image)?
                != canonical::to_vec(&filesystem.summary().image)?
        {
            bail!("cache differs from the fresh native source/artifact match");
        }
        Ok(())
    }
    pub(super) fn check(&self) -> Result<()> {
        if !self.active.load(Ordering::Acquire) {
            bail!("cached image is retiring");
        }
        self.core.same_request(&self.file, &self.request)?;
        if self.core.read("image-cache.result", 4096)?.1 != self.request.receipt()? {
            bail!("cached image lost its private settled reference");
        }
        Ok(())
    }
    pub(super) fn image(&self) -> &ContainerImageSummary {
        &self.request.image
    }
}
fn key(plan: &SourceQuarantinePlan) -> Result<Digest> {
    Ok(Digest::of(canonical::to_vec(&(
        "compiled-image-cache-v1",
        &plan.source_match,
    ))?))
}
fn footprint(
    fs: &elixir_marketplace::container_image::filesystem::job::ContainerFilesystemSummary,
) -> Result<u64> {
    // The caller holds the actual NativeContainerFilesystemMatch, checked
    // against this source before these accounting-only fields are read.
    fs.image
        .layers
        .iter()
        .try_fold(8 * MIB, |sum, layer| {
            sum.checked_add(layer.size)
                .context("cached image footprint overflow")
        })?
        .checked_add(
            u64::from(fs.entries)
                .checked_mul(8192)
                .context("image metadata reservation overflow")?,
        )
        .context("cached image footprint overflow")
}

struct Admission {
    inner: Arc<dyn ResourceAdmission>,
    borrow: Arc<Borrow>,
}
#[async_trait::async_trait]
impl ResourceAdmission for Admission {
    fn cached_image(&self) -> Option<Arc<Borrow>> {
        Some(self.borrow.clone())
    }
    fn needs_measurement(&self) -> bool {
        self.inner.needs_measurement()
    }
    async fn before_measurement(&self, plan: &SourceQuarantinePlan) -> Result<()> {
        self.inner.before_measurement(plan).await
    }
    async fn check(
        &self,
        plan: &SourceQuarantinePlan,
        provisioned: bool,
        observation: Option<&HostObservation>,
    ) -> Result<()> {
        self.inner.check(plan, provisioned, observation).await
    }
}
pub(in crate::marketplace) fn admission(
    inner: Arc<dyn ResourceAdmission>,
    borrow: Arc<Borrow>,
) -> Arc<dyn ResourceAdmission> {
    Arc::new(Admission { inner, borrow })
}

pub(super) async fn before_source(
    runtime: Arc<dyn RuntimeManager>,
    lease: &ControllerLease,
) -> Result<()> {
    let directory = lease.directory().join(DIRECTORY);
    let profile = runtime.marketplace_import_profile();
    if let Some(profile) = &profile
        && let Some(cache) = profile.cache(&directory)?
    {
        return cache.evict().await;
    }
    match std::fs::symlink_metadata(&directory) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    }
    let Some(profile) = profile else {
        let _lease = ControllerLease::acquire_for_recovery(&directory)?;
        for entry in std::fs::read_dir(&directory)? {
            let name = entry?.file_name();
            if name != "controller.lock" && name != "image-cache.lock" {
                bail!("existing image cache requires its trusted cleanup profile");
            }
        }
        return Ok(());
    };
    let pool = Pool::open(runtime, profile, lease.directory()).await?;
    pool.evict().await
}

#[cfg(test)]
mod tests;
