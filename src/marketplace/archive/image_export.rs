//! One fixed disposable image-export file per original foreground lane. Not an
//! installed archive, source/build proof, daemon image or activation capability.
use super::*;
use crate::marketplace::quarantine::SourceMatchedQuarantine;
use std::os::fd::AsRawFd;

const DIRECTORY: &str = "image-export";
const FILE: &str = "image.blob";
enum Work {
    Opened(Arc<Root>, File),
    Sealed(Blob),
    Retired,
}

pub(in crate::marketplace) struct ImageExportStaging {
    job: Arc<ControllerLease>,
    expires: Instant,
    limit: u64,
    pending: Option<JoinHandle<Result<Work>>>,
    root: Option<Arc<Root>>,
    sink: Option<ArchiveSink>,
    sealed: Option<Blob>,
    started: bool,
    sealing: bool,
}
impl ImageExportStaging {
    pub(in crate::marketplace) fn new(source: &SourceMatchedQuarantine) -> Result<Self> {
        source.require_compiled_source()?;
        let artifact = source
            .selected_artifact()
            .context("image export requires a selected artifact")?;
        Ok(Self {
            job: source.job_lease(),
            expires: source.expires(),
            limit: elixir_marketplace::container_image::export::output_limit(artifact)?,
            pending: None,
            root: None,
            sink: None,
            sealed: None,
            started: false,
            sealing: false,
        })
    }
    async fn join(&mut self) -> Result<Work> {
        let result = self
            .pending
            .as_mut()
            .context("image export I/O owner missing")?
            .await;
        self.pending = None;
        result.context("image export I/O task stopped")?
    }
    async fn work(&mut self, f: impl FnOnce() -> Result<Work> + Send + 'static) -> Result<Work> {
        if self.pending.is_some() {
            bail!("original image export I/O must be joined first");
        }
        self.pending = Some(tokio::task::spawn_blocking(f));
        self.join().await
    }
    pub(in crate::marketplace) async fn open(&mut self) -> Result<()> {
        if self.started {
            bail!("image export staging cannot restart");
        }
        self.started = true;
        let job = self.job.clone();
        let limit = self.limit;
        let expires = self.expires;
        let Work::Opened(root, file) = self
            .work(move || {
                active(expires)?;
                let root = open_root(job, true)?.context("image export root was not created")?;
                clear_file(&root)?;
                free_space(&root.path, limit + ROOT_RESERVATION)?;
                let file = create_file(&root.path.join(FILE))?;
                preallocate(&file, limit)?;
                file.sync_all()?;
                sync_directory(&root.path)?;
                Ok(Work::Opened(root, file))
            })
            .await?
        else {
            bail!("unexpected image export open outcome");
        };
        self.sink = Some(ArchiveSink::new(
            file,
            self.limit,
            root.clone(),
            self.expires,
        ));
        self.root = Some(root);
        Ok(())
    }
    pub(in crate::marketplace) async fn drain(&mut self) -> Result<()> {
        if let Some(sink) = self.sink.as_mut() {
            sink.drain().await?;
        }
        Ok(())
    }
    /// Complete footer plus clean native exit are independently required by
    /// the controller. This rehash proves only the exact actual staging file.
    pub(in crate::marketplace) async fn seal(&mut self, expected: &Blob) -> Result<()> {
        active(self.expires)?;
        expected
            .validate(elixir_marketplace::container_image::docker_save::MAX_DOCKER_SAVE_BYTES)?;
        if self.sealing || self.sealed.is_some() || expected.size > self.limit {
            bail!("image export seal cannot restart");
        }
        self.sealing = true;
        self.drain().await?;
        let sink = self.sink.as_mut().context("image export sink missing")?;
        if self.limit - sink.remaining != expected.size {
            bail!("image export file is incomplete");
        }
        let file = sink.file.take().context("image export file missing")?;
        let root = self
            .root
            .as_ref()
            .context("image export root missing")?
            .clone();
        let expected = expected.clone();
        let expires = self.expires;
        let limit = self.limit;
        let Work::Sealed(blob) = self
            .work(move || {
                require_root(&root)?;
                let path = root.path.join(FILE);
                same_file(&path, &file, limit, 0o600)?;
                file.set_len(expected.size)?;
                seal_file(file)?;
                regular(&path, expected.size, Some(0o400))?;
                hash_file(&path, &expected, expires)?;
                sync_directory(&root.path)?;
                Ok(Work::Sealed(expected))
            })
            .await?
        else {
            bail!("unexpected image export seal outcome");
        };
        self.sink = None;
        self.sealed = Some(blob);
        Ok(())
    }
    pub(in crate::marketplace) fn require_sealed(&self, expected: &Blob) -> Result<()> {
        active(self.expires)?;
        let actual = self
            .sealed
            .as_ref()
            .context("image export file is not sealed")?;
        if actual.sha256 != expected.sha256
            || actual.size != expected.size
            || self.pending.is_some()
        {
            bail!("image export file differs from its joined original outcome");
        }
        let root = self.root.as_ref().context("image export root missing")?;
        require_root(root)?;
        if regular(&root.path.join(FILE), expected.size, Some(0o400))?.len() != expected.size {
            bail!("sealed image export file changed length");
        }
        Ok(())
    }
    /// Same file only. Pending blocking work and its real FD/root/OS locks are
    /// retained through every cancelled join; no recursive Drop task is used.
    pub(in crate::marketplace) async fn retire(&mut self) -> Result<()> {
        if self.pending.is_some() {
            match self.join().await? {
                Work::Opened(root, file) => {
                    self.sink = Some(ArchiveSink::new(
                        file,
                        self.limit,
                        root.clone(),
                        self.expires,
                    ));
                    self.root = Some(root);
                }
                Work::Sealed(blob) => self.sealed = Some(blob),
                Work::Retired => {}
            }
        }
        self.drain().await?;
        self.sink = None;
        if let Some(root) = self.root.take() {
            let root = match Arc::try_unwrap(root) {
                Ok(root) => root,
                Err(root) => {
                    self.root = Some(root);
                    bail!("original image export I/O still owns its staging root");
                }
            };
            self.work(move || {
                clear_file(&root)?;
                Ok(Work::Retired)
            })
            .await?;
        }
        self.sealed = None;
        Ok(())
    }
    pub(in crate::marketplace) async fn cleanup_idle(job: Arc<ControllerLease>) -> Result<()> {
        // Moving the real source lease into the blocking operation prevents a
        // cancelled startup from releasing it before filesystem cleanup ends.
        tokio::task::spawn_blocking(move || {
            if let Some(root) = open_root(job, false)? {
                clear_file(&root)?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("image export idle cleanup task stopped")?
    }
}
impl AsyncWrite for ImageExportStaging {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.sealing {
            return Poll::Ready(Err(io::Error::other(
                "image export staging is not writable",
            )));
        }
        match self.sink.as_mut() {
            Some(sink) => Pin::new(sink).poll_write(cx, bytes),
            _ => Poll::Ready(Err(io::Error::other(
                "image export staging is not writable",
            ))),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.sink.as_mut() {
            Some(sink) => Pin::new(sink).poll_flush(cx),
            None => Poll::Ready(Err(io::Error::other("image export sink missing"))),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
fn require_root(root: &Root) -> Result<()> {
    if !root._lease.matches_directory(&root.path)
        || !root
            ._source_job
            .as_ref()
            .is_some_and(|job| job.matches_directory(job.directory()))
    {
        bail!("image export staging lost its original locked inodes");
    }
    Ok(())
}
fn open_root(job: Arc<ControllerLease>, create: bool) -> Result<Option<Arc<Root>>> {
    if !job.matches_directory(job.directory()) {
        bail!("image export lost its source job lease");
    }
    let path = job.directory().join(DIRECTORY);
    if !create {
        match fs::symlink_metadata(&path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
    }
    let lease = ControllerLease::acquire(&path)?;
    let root = Arc::new(Root {
        path,
        _lease: lease,
        _source_job: Some(job),
    });
    require_root(&root)?;
    if physical(&fs::symlink_metadata(&root.path)?)?
        + physical(&fs::symlink_metadata(root.path.join("controller.lock"))?)?
        > ROOT_RESERVATION
    {
        bail!("image export root exceeds its metadata reservation");
    }
    if names(&root.path, 2)?
        .iter()
        .any(|n| n != "controller.lock" && n != FILE)
    {
        bail!("unknown entry in image export staging");
    }
    Ok(Some(root))
}
fn same_file(path: &Path, file: &File, limit: u64, mode: u32) -> Result<()> {
    let before = regular(path, limit, Some(mode))?;
    let held = file.metadata()?;
    if before.dev() != held.dev()
        || before.ino() != held.ino()
        || held.nlink() != 1
        || held.uid() != unsafe { libc::geteuid() }
        || held.mode() & 0o7777 != mode
    {
        bail!("image export staging file was replaced");
    }
    Ok(())
}
fn clear_file(root: &Root) -> Result<()> {
    require_root(root)?;
    if names(&root.path, 2)?
        .iter()
        .any(|n| n != "controller.lock" && n != FILE)
    {
        bail!("unknown image export staging entry; refusing removal");
    }
    let path = root.path.join(FILE);
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
        Ok(_) => {
            let metadata = regular(
                &path,
                elixir_marketplace::container_image::docker_save::MAX_DOCKER_SAVE_BYTES,
                None,
            )?;
            if physical(&metadata)?
                > elixir_marketplace::container_image::docker_save::MAX_DOCKER_SAVE_BYTES
                    + ROOT_RESERVATION
            {
                bail!("image export file exceeds its physical reservation");
            }
            fs::remove_file(&path)?;
            sync_directory(&root.path)?;
        }
    }
    Ok(())
}
fn preallocate(file: &File, limit: u64) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let code = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, limit.try_into()?) };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code))
                .context("image export allocation failed");
        }
    }
    #[cfg(target_os = "macos")]
    {
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: limit.try_into()?,
            fst_bytesalloc: 0,
        };
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } != 0 {
            return Err(io::Error::last_os_error()).context("image export allocation failed");
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    bail!("image export physical allocation is unsupported on this host");
    file.set_len(limit)?;
    let allocated = physical(&file.metadata()?)?;
    if allocated < limit || allocated > limit + ROOT_RESERVATION {
        bail!("image export physical allocation differs");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    // Filesystem primitive only. This fixture mints no source/build/native
    // evidence or paid/public-source observation.
    async fn fixture() -> Result<(tempfile::TempDir, Arc<ControllerLease>, ImageExportStaging)> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().canonicalize()?.join("lane");
        let source = Arc::new(ControllerLease::acquire(&path)?);
        let staging = ImageExportStaging {
            job: source.clone(),
            expires: Instant::now() + Duration::from_secs(60),
            limit: 2 * MIB + 512,
            pending: None,
            root: None,
            sink: None,
            sealed: None,
            started: false,
            sealing: false,
        };
        Ok((directory, source, staging))
    }
    #[tokio::test]
    async fn image_export_staging_preallocates_bounds_seals_rehashes_and_retires_only_its_file()
    -> Result<()> {
        let (_directory, source, mut staging) = fixture().await?;
        staging.open().await?;
        let root = source.directory().join(DIRECTORY);
        assert!(physical(&fs::metadata(root.join(FILE))?)? >= staging.limit);
        let bytes = vec![7; 80 * 1024];
        let blob = Blob {
            size: bytes.len() as u64,
            sha256: Digest::of(&bytes),
        };
        staging.write_all(&bytes).await?;
        staging.seal(&blob).await?;
        staging.require_sealed(&blob)?;
        assert_eq!(fs::metadata(root.join(FILE))?.len(), blob.size);
        assert_eq!(fs::metadata(root.join(FILE))?.mode() & 0o7777, 0o400);
        assert!(staging.write_all(b"extra").await.is_err());
        assert!(staging.seal(&blob).await.is_err());
        staging.retire().await?;
        assert!(!root.join(FILE).exists());
        assert!(root.join("controller.lock").is_file());
        ImageExportStaging::cleanup_idle(source.clone()).await?;
        Ok(())
    }
    #[tokio::test]
    async fn partial_or_wrong_digest_cannot_seal_or_retry_and_cleanup_still_joins() -> Result<()> {
        for partial in [false, true] {
            let (_directory, source, mut staging) = fixture().await?;
            staging.open().await?;
            let bytes = b"staging primitive, not a native image proof";
            staging.write_all(bytes).await?;
            let blob = Blob {
                size: if partial { 1024 } else { bytes.len() as u64 },
                sha256: Digest::of(b"wrong"),
            };
            assert!(staging.seal(&blob).await.is_err());
            assert!(staging.require_sealed(&blob).is_err());
            assert!(staging.open().await.is_err());
            staging.retire().await?;
            assert!(!source.directory().join(DIRECTORY).join(FILE).exists());
        }
        Ok(())
    }
    #[tokio::test]
    async fn foreign_names_symlinks_and_active_root_owners_are_never_removed_as_idle() -> Result<()>
    {
        let (directory, source, mut staging) = fixture().await?;
        staging.open().await?;
        assert!(
            ImageExportStaging::cleanup_idle(source.clone())
                .await
                .is_err()
        );
        staging.retire().await?;
        let path = source.directory().join(DIRECTORY);
        let preserved = directory.path().join("preserved");
        fs::write(&preserved, b"must survive")?;
        std::os::unix::fs::symlink(&preserved, path.join(FILE))?;
        assert!(
            ImageExportStaging::cleanup_idle(source.clone())
                .await
                .is_err()
        );
        assert_eq!(fs::read(&preserved)?, b"must survive");
        fs::remove_file(path.join(FILE))?;
        fs::write(path.join("foreign"), b"preserve this too")?;
        assert!(
            ImageExportStaging::cleanup_idle(source.clone())
                .await
                .is_err()
        );
        assert_eq!(fs::read(path.join("foreign"))?, b"preserve this too");
        Ok(())
    }
    #[tokio::test]
    async fn dropped_staging_waiter_cannot_release_original_lease_while_blocking_io_remains()
    -> Result<()> {
        let (_directory, source, mut staging) = fixture().await?;
        let path = source.directory().to_owned();
        let held = source.clone();
        let (release, wait) = std::sync::mpsc::channel();
        staging.pending = Some(tokio::task::spawn_blocking(move || {
            let _held = held;
            wait.recv_timeout(Duration::from_secs(5))?;
            Ok(Work::Retired)
        }));
        drop(source);
        drop(staging);
        assert!(ControllerLease::acquire(&path).is_err());
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if ControllerLease::acquire(&path).is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        Ok(())
    }
}
