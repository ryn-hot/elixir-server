//! Disposable pre-build bytes, NOT the grant-complete immutable archive.
//! One .pending pair per original job lane, never committed as an installed
//! release. The candidate artifact is neither copied nor opened here.
use super::super::quarantine::SourceMatchedQuarantine;
use super::*;
use tokio::time::Instant;

enum Work {
    Opened(Store),
    File(File),
    Done,
    Readers(Replay),
}

/// Kept in the original build owner across every await. A dropped waiter does
/// not drop a blocking task, a native source owner, or reset an allowance.
pub(in crate::marketplace) struct BuildInputs {
    descriptor: Descriptor,
    source_job: Arc<ControllerLease>,
    budget: ArchiveBudget,
    expires: Instant,
    started: bool,
    pending: Option<JoinHandle<Result<Work>>>,
    store: Option<Store>,
    sink: Option<ArchiveSink>,
    replay: Option<Replay>,
}
impl BuildInputs {
    pub(in crate::marketplace) fn new(source: &SourceMatchedQuarantine) -> Result<Self> {
        source.require_public_source()?;
        if source.release().runtime_profile
            != elixir_marketplace::records::RuntimeProfile::Reproducible
            || source.workspace().artifact.is_none()
        {
            bail!("pre-build staging requires a selected compiled release");
        }
        let mut descriptor = Descriptor {
            schema_version: 1,
            workspace: source.workspace().clone(),
            release: source.release().clone(),
            source: source.summary().clone(),
        };
        // Raw public inputs only. This cannot collide with or become the later
        // three-slot immutable source/candidate archive for the same release.
        descriptor.workspace.artifact = None;
        descriptor.workspace.timeout_seconds = elixir_marketplace::quarantine::MAX_JOB_SECONDS;
        descriptor.validate()?;
        let budget = ArchiveBudget {
            bytes: descriptor.reservation()? + ROOT_RESERVATION,
            releases: 1,
        };
        Ok(Self {
            descriptor,
            source_job: source.job_lease(),
            budget,
            expires: source.expires(),
            started: false,
            pending: None,
            store: None,
            sink: None,
            replay: None,
        })
    }
    fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        source.require_public_source()?;
        if !Arc::ptr_eq(&self.source_job, &source.job_lease())
            || source.expires() != self.expires
            || canonical::to_vec(source.release())? != canonical::to_vec(&self.descriptor.release)?
        {
            bail!("build staging changed its original source owner");
        }
        self.descriptor.source.matches(&SourceMatchDescriptor {
            schema_version: 1,
            workspace: source.workspace().clone(),
            release: source.release().clone(),
        })?;
        Ok(())
    }
    async fn join(&mut self) -> Result<Work> {
        let result = self
            .pending
            .as_mut()
            .context("build staging I/O owner missing")?
            .await;
        self.pending = None;
        result.context("build staging I/O task stopped")?
    }
    async fn work(&mut self, f: impl FnOnce() -> Result<Work> + Send + 'static) -> Result<Work> {
        if self.pending.is_some() {
            bail!("original build staging I/O must be joined first");
        }
        self.pending = Some(tokio::task::spawn_blocking(f));
        self.join().await
    }
    pub(in crate::marketplace) async fn prepare(
        &mut self,
        source: &mut SourceMatchedQuarantine,
    ) -> Result<()> {
        if self.started {
            bail!("build staging attempt cannot resume or refund work");
        }
        self.started = true;
        self.require_source(source)?;
        let input_bytes =
            self.descriptor.workspace.package.size + self.descriptor.workspace.proof.size;
        // This suballocation must already be included in the original plan's
        // admission. It cannot increase RAM/disk/time credit after opening.
        source.reserve_build_staging(
            self.budget.bytes,
            self.budget.metadata_read_allowance()? + 2 * (input_bytes + 2),
            input_bytes + canonical::to_vec(&self.descriptor)?.len() as u64,
        )?;
        let path = self.source_job.directory().join("build-inputs");
        let lease = self.source_job.clone();
        let budget = self.budget;
        let expires = self.expires;
        let descriptor = self.descriptor.clone();
        let Work::Opened(store) = self
            .work(move || {
                let store = Store::open_build_inputs(&path, budget, expires, lease)?;
                if !store.begin(&descriptor, expires)? {
                    bail!("build inputs must remain disposable pending data");
                }
                Ok(Work::Opened(store))
            })
            .await?
        else {
            bail!("unexpected build staging open result");
        };
        self.store = Some(store);
        for purpose in [QuarantinePurpose::Package, QuarantinePurpose::Proof] {
            self.require_source(source)?;
            let slot = self.descriptor.workspace.descriptor(purpose)?;
            let root = self
                .store
                .as_ref()
                .context("build staging store missing")?
                .root
                .clone();
            let held = root.clone();
            let Work::File(file) = self
                .work(move || {
                    active(expires)?;
                    let target = held
                        .path
                        .join("releases")
                        .join(PENDING)
                        .join(purpose.file_name());
                    Ok(Work::File(create_file(&target)?))
                })
                .await?
            else {
                bail!("unexpected build staging file result");
            };
            self.sink = Some(ArchiveSink::new(file, slot.blob.size, root, expires));
            let runtime = source.runtime();
            let copied = source
                .export_blob(
                    runtime.as_ref(),
                    purpose,
                    self.sink.as_mut().context("build staging sink missing")?,
                )
                .await;
            // Even a rejected/cancelled export must join its original write.
            let drained = self
                .sink
                .as_mut()
                .context("build staging sink missing")?
                .drain()
                .await;
            copied?;
            drained?;
            let mut sink = self.sink.take().context("build staging sink missing")?;
            if sink.remaining != 0 || sink.pending.is_some() {
                bail!("incomplete build staging file");
            }
            let file = sink.file.take().context("build staging file missing")?;
            let root = sink.root.clone();
            match self
                .work(move || {
                    let _held = root;
                    active(expires)?;
                    seal_file(file)?;
                    Ok(Work::Done)
                })
                .await?
            {
                Work::Done => {}
                _ => bail!("unexpected build staging seal result"),
            }
        }
        self.require_source(source)?;
        let root = self
            .store
            .as_ref()
            .context("build staging store missing")?
            .root
            .clone();
        let descriptor = self.descriptor.clone();
        let Work::Readers(replay) = self
            .work(move || Replay::from_build_inputs(root, &descriptor, expires).map(Work::Readers))
            .await?
        else {
            bail!("unexpected build staging reader result");
        };
        self.replay = Some(replay);
        self.require_source(source)
    }
    pub(in crate::marketplace) fn take_readers(&mut self) -> Result<Replay> {
        active(self.expires)?;
        self.replay
            .take()
            .context("build input readers are unavailable or already consumed")
    }
    /// Native export/compile owners and taken Reader I/O must be stopped/joined
    /// FIRST. Keep this object through cancelled cleanup waits as well.
    pub(in crate::marketplace) async fn retire(&mut self) -> Result<()> {
        if self.pending.is_some() {
            match self.join().await? {
                Work::Opened(store) => self.store = Some(store),
                Work::Readers(replay) => self.replay = Some(replay),
                Work::File(file) => drop(file),
                Work::Done => {}
            }
        }
        if let Some(sink) = self.sink.as_mut() {
            sink.drain().await?;
        }
        self.sink = None;
        self.replay = None;
        if let Some(store) = self.store.as_ref() {
            let root = store.root.clone();
            match self
                .work(move || {
                    let pending = root.path.join("releases").join(PENDING);
                    match fs::symlink_metadata(&pending) {
                        Ok(_) => {
                            discard_pending(&pending)?;
                            sync_directory(&root.path.join("releases"))?;
                        }
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                    Ok(Work::Done)
                })
                .await?
            {
                Work::Done => {}
                _ => bail!("unexpected build staging retirement result"),
            }
            self.store = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn pending() -> Result<(tempfile::TempDir, Store, Descriptor, Vec<u8>, Vec<u8>)> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let lease = Arc::new(ControllerLease::acquire(&root.join("lane"))?);
        let (_, descriptor, package, proof) = super::super::tests::fixture()?;
        let store = Store::open_build_inputs(
            &root.join("inputs"),
            super::super::tests::budget(),
            super::super::tests::expires(),
            lease,
        )?;
        if !store.begin(&descriptor, super::super::tests::expires())? {
            bail!("new fixture unexpectedly reused bytes");
        }
        for (purpose, body) in [
            (QuarantinePurpose::Package, &package),
            (QuarantinePurpose::Proof, &proof),
        ] {
            let mut file = create_file(&store.releases().join(PENDING).join(purpose.file_name()))?;
            file.write_all(body)?;
            seal_file(file)?;
        }
        Ok((directory, store, descriptor, package, proof))
    }
    #[tokio::test]
    async fn pending_source_pair_streams_once_without_committing_or_exposing_a_candidate()
    -> Result<()> {
        let (_directory, store, descriptor, package, proof) = pending()?;
        let mut replay = Replay::from_build_inputs(
            store.root.clone(),
            &descriptor,
            super::super::tests::expires(),
        )?;
        assert!(replay.take(QuarantinePurpose::Artifact).is_err());
        for (purpose, expected) in [
            (QuarantinePurpose::Package, package),
            (QuarantinePurpose::Proof, proof),
        ] {
            let mut reader = replay.take(purpose)?;
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await?;
            assert_eq!(actual, expected);
            assert!(replay.take(purpose).is_err());
            reader.drain().await?;
        }
        assert!(store.releases().join(PENDING).is_dir());
        assert!(!store.releases().join(descriptor.name()).exists());
        assert!(ControllerLease::acquire(&store.root.path).is_err());
        Ok(())
    }
    #[test]
    fn candidate_or_committed_data_is_rejected_before_staging_cleanup() -> Result<()> {
        for committed in [false, true] {
            let (directory, store, descriptor, _package, _proof) = pending()?;
            let path = store.root.path.clone();
            let lane = directory.path().canonicalize()?.join("lane");
            let protected = if committed {
                store.commit(&descriptor, super::super::tests::expires())?;
                store.releases().join(descriptor.name())
            } else {
                let path = store.releases().join(PENDING).join("artifact.blob");
                create_file(&path)?.write_all(b"candidate")?;
                path
            };
            drop(store);
            let result = Store::open_build_inputs(
                &path,
                super::super::tests::budget(),
                super::super::tests::expires(),
                Arc::new(ControllerLease::acquire(&lane)?),
            );
            assert!(result.is_err());
            assert!(protected.exists());
        }
        Ok(())
    }
    #[tokio::test]
    async fn changed_pending_source_cannot_produce_a_successful_verified_eof() -> Result<()> {
        let (_directory, store, descriptor, mut package, _proof) = pending()?;
        let path = store.releases().join(PENDING).join("package.blob");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        package[0] ^= 1;
        fs::write(&path, &package)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400))?;
        let mut replay = Replay::from_build_inputs(
            store.root.clone(),
            &descriptor,
            super::super::tests::expires(),
        )?;
        let mut reader = replay.take(QuarantinePurpose::Package)?;
        assert!(reader.read_to_end(&mut Vec::new()).await.is_err());
        assert!(reader.read_u8().await.is_err());
        reader.drain().await?;
        Ok(())
    }
}
