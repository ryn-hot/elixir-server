//! Bounded raw readback for a future native restore owner. This never parses
//! package/Git input, authorizes execution, or claims native source equality.
//! Blocking opens/reads retain BOTH archive and foreground-job OS leases.
use super::*;
use tokio::io::{AsyncRead, ReadBuf};

const CHUNK: usize = 64 * 1024;
type ReadTask = JoinHandle<(File, io::Result<Vec<u8>>)>;

/// No Clone/Deserialize/path getter. At most three fixed, already-opened slots
/// may be taken once. The owner must join its blocking open and each reader's
/// drain before reporting restoration/cleanup complete.
pub(in crate::marketplace) struct Replay {
    root: Arc<Root>,
    slots: Vec<(QuarantinePurpose, Blob, File)>,
    expires: Instant,
}
impl Replay {
    /// Data-only readers from a disposable source-only pending pair. Never
    /// accepts an artifact slot, commits an archive, or authorizes execution.
    pub(super) fn from_build_inputs(
        root: Arc<Root>,
        descriptor: &Descriptor,
        expires: Instant,
    ) -> Result<Self> {
        active(expires)?;
        descriptor.validate()?;
        if descriptor.workspace.artifact.is_some() || !root._lease.matches_directory(&root.path) {
            bail!("build inputs require an owned source-only pair");
        }
        let path = root.path.join("releases").join(PENDING);
        if inspect_pending_complete(&path, descriptor)? > descriptor.reservation()? {
            bail!("build staging exceeds its physical reservation");
        }
        let mut slots = Vec::with_capacity(2);
        for purpose in [QuarantinePurpose::Package, QuarantinePurpose::Proof] {
            let expected = descriptor.workspace.descriptor(purpose)?;
            let path = path.join(purpose.file_name());
            let before = regular(&path, expected.blob.size, Some(0o400))?;
            let file = open_file(&path)?;
            let held = file.metadata()?;
            if held.dev() != before.dev()
                || held.ino() != before.ino()
                || held.nlink() != 1
                || held.uid() != unsafe { libc::geteuid() }
                || held.mode() & 0o7777 != 0o400
                || held.len() != expected.blob.size
            {
                bail!("build input changed while opening");
            }
            slots.push((purpose, expected.blob, file));
        }
        Ok(Self {
            root,
            slots,
            expires,
        })
    }
    pub(in crate::marketplace) fn read_allowance(
        budget: ArchiveBudget,
        expected: &SourceMatchDescriptor,
    ) -> Result<u64> {
        expected.validate()?;
        expected.workspace.descriptors()?.iter().try_fold(
            budget.metadata_read_allowance()?,
            |used, slot| {
                used.checked_add(slot.blob.size + 1)
                    .context("archive replay read allowance overflow")
            },
        )
    }
    /// Run on an owned blocking task, never the async executor. The caller
    /// reserves this read allowance separately from native receive/verification
    /// I/O. A cancelled waiter cannot release the captured foreground lease.
    pub(in crate::marketplace) fn open(
        path: &Path,
        budget: ArchiveBudget,
        expected: &SourceMatchDescriptor,
        source_job: Arc<ControllerLease>,
        read_allowance: u64,
        expires: Instant,
    ) -> Result<Self> {
        expected.validate()?;
        active(expires)?;
        if !source_job.matches_directory(source_job.directory()) {
            bail!("archive replay lost its original foreground lease");
        }
        let slots = expected.workspace.descriptors()?;
        let required = Self::read_allowance(budget, expected)?;
        if read_allowance < required {
            bail!("archive replay needs an explicit sufficient read allowance");
        }
        let store = Store::open_with_job(path, budget, expires, Some(source_job))?;
        let target = store
            .releases()
            .join(&expected.workspace.release_id.to_string()[7..]);
        // Only bounded metadata is inspected here. Each actual byte is hashed
        // while streaming, and a native receiver/verifier must independently
        // rehash/rematch it before any execution is considered.
        let found = inspect(&target, false, expires)?;
        let found_slots = found.workspace.descriptors()?;
        if canonical::to_vec(&found.release)? != canonical::to_vec(&expected.release)?
            || found.workspace.release_id != expected.workspace.release_id
            || found_slots.len() != slots.len()
            || found_slots.iter().zip(&slots).any(|(found, expected)| {
                found.purpose != expected.purpose
                    || found.blob.sha256 != expected.blob.sha256
                    || found.blob.size != expected.blob.size
            })
        {
            bail!("archive replay differs from its exact source plan");
        }
        found.source.matches(expected)?;
        let mut opened = Vec::with_capacity(slots.len());
        for slot in slots {
            active(expires)?;
            let path = target.join(slot.purpose.file_name());
            let before = regular(&path, slot.blob.size, Some(0o400))?;
            let file = open_file(&path)?;
            let held = file.metadata()?;
            if !held.is_file()
                || held.dev() != before.dev()
                || held.ino() != before.ino()
                || held.nlink() != 1
                || held.uid() != unsafe { libc::geteuid() }
                || held.mode() & 0o7777 != 0o400
                || held.len() != slot.blob.size
            {
                bail!("archive replay slot changed while opening");
            }
            opened.push((slot.purpose, slot.blob, file));
        }
        Ok(Self {
            root: store.root,
            slots: opened,
            expires,
        })
    }
    pub(in crate::marketplace) fn take(&mut self, purpose: QuarantinePurpose) -> Result<Reader> {
        active(self.expires)?;
        let index = self
            .slots
            .iter()
            .position(|(slot, _, _)| *slot == purpose)
            .context("archive replay slot is absent or already consumed")?;
        let (_, expected, file) = self.slots.remove(index);
        Ok(Reader {
            file: Some(file),
            pending: None,
            root: self.root.clone(),
            remaining: expected.size,
            expected: expected.sha256,
            hash: Sha256::new(),
            buffered: Vec::new(),
            offset: 0,
            expires: self.expires,
            done: false,
            failed: false,
        })
    }
}

/// One <=64-KiB blocking read, no read-ahead or unbounded buffering. EOF is
/// exposed only after an extra-byte probe and the exact signed content hash.
pub(in crate::marketplace) struct Reader {
    file: Option<File>,
    pending: Option<ReadTask>,
    root: Arc<Root>,
    remaining: u64,
    expected: Digest,
    hash: Sha256,
    buffered: Vec<u8>,
    offset: usize,
    expires: Instant,
    done: bool,
    failed: bool,
}
impl Reader {
    fn accept(&mut self, file: File, result: io::Result<Vec<u8>>) -> io::Result<()> {
        self.file = Some(file);
        let bytes = result?;
        if Instant::now() >= self.expires || bytes.len() as u64 > self.remaining {
            return Err(io::Error::other(
                "archive replay deadline or length exceeded",
            ));
        }
        if bytes.is_empty() {
            if self.remaining != 0
                || Digest::from_bytes(self.hash.clone().finalize().into()) != self.expected
            {
                return Err(io::Error::other(
                    "archive replay content hash or length differs",
                ));
            }
            self.done = true;
        } else {
            self.remaining -= bytes.len() as u64;
            self.hash.update(&bytes);
            self.buffered = bytes;
            self.offset = 0;
        }
        Ok(())
    }
    /// Cancellation of this await leaves the SAME pending read in this owner.
    /// It is not a successful EOF/integrity receipt; a native restore owner must
    /// still complete the stream and independently verify its native workspace.
    pub(in crate::marketplace) async fn drain(&mut self) -> Result<()> {
        if let Some(task) = self.pending.as_mut() {
            let result = task.await;
            self.pending = None;
            let result = result
                .map_err(io::Error::other)
                .and_then(|(file, bytes)| self.accept(file, bytes));
            if let Err(error) = result {
                self.failed = true;
                return Err(error.into());
            }
        }
        Ok(())
    }
}
impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if self.failed || Instant::now() >= self.expires {
                self.failed = true;
                return Poll::Ready(Err(io::Error::other("archive replay is unavailable")));
            }
            if self.offset < self.buffered.len() {
                let size = output.remaining().min(self.buffered.len() - self.offset);
                output.put_slice(&self.buffered[self.offset..self.offset + size]);
                self.offset += size;
                return Poll::Ready(Ok(()));
            }
            if self.done {
                return Poll::Ready(Ok(()));
            }
            if let Some(task) = self.pending.as_mut() {
                let result = match Pin::new(task).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result,
                };
                self.pending = None;
                let result = result
                    .map_err(io::Error::other)
                    .and_then(|(file, bytes)| self.accept(file, bytes));
                if let Err(error) = result {
                    self.failed = true;
                    return Poll::Ready(Err(error));
                }
            } else {
                let Some(mut file) = self.file.take() else {
                    self.failed = true;
                    return Poll::Ready(Err(io::Error::other("archive replay lost its file")));
                };
                let size = (self.remaining + 1).min(CHUNK as u64) as usize;
                let root = self.root.clone();
                let expires = self.expires;
                self.pending = Some(tokio::task::spawn_blocking(move || {
                    let _held = root;
                    let result = (|| {
                        active(expires).map_err(io::Error::other)?;
                        let mut bytes = vec![0; size];
                        let size = file.read(&mut bytes)?;
                        bytes.truncate(size);
                        Ok(bytes)
                    })();
                    (file, result)
                }));
            }
        }
    }
}

#[cfg(test)]
mod tests;
