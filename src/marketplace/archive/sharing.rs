//! Read only an exact committed archive slot, concurrently with foreground
//! work. No writer/global-job lease is held for a slow network upload. The
//! immutable opened inode, exact descriptor and streaming hash stay owned.
use super::*;
use std::os::unix::fs::MetadataExt;

type Stamp = (u64, u64, u64, i64, i64, i64, i64);
fn stamp(file: &File, expected: u64) -> Result<Stamp> {
    let m = file.metadata()?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != unsafe { libc::geteuid() }
        || m.mode() & 0o7777 != 0o400
        || m.len() != expected
    {
        bail!("installed sharing file changed");
    }
    Ok((
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{budget, expires, fixture, populate};
    use super::*;
    fn expected(descriptor: &Descriptor) -> SourceMatchDescriptor {
        SourceMatchDescriptor {
            schema_version: 1,
            workspace: descriptor.workspace.clone(),
            release: descriptor.release.clone(),
        }
    }
    #[test]
    fn installed_sharing_streams_exact_archive_without_taking_the_writer_or_foreground_lease()
    -> Result<()> {
        let (_, descriptor, package, proof) = fixture()?;
        let root = tempfile::tempdir()?;
        let path = root.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        // Keep the actual writer alive. An upload neither owns nor waits on it.
        for (blob, bytes) in [
            (&descriptor.workspace.package, &package),
            (&descriptor.workspace.proof, &proof),
        ] {
            let mut reader = SharingReader::open(&path, budget(), &expected(&descriptor), blob)?;
            let mut actual = vec![];
            while (actual.len() as u64) < blob.size {
                let maximum = (blob.size - actual.len() as u64).min(64 * 1024) as usize;
                actual.extend(reader.read(actual.len() as u64, maximum)?);
            }
            assert_eq!(&actual, bytes);
            assert!(reader.read(blob.size, 1).is_err());
        }
        let mut wrong = descriptor.workspace.package.clone();
        wrong.sha256 = Digest::of(b"uninstalled");
        assert!(SharingReader::open(&path, budget(), &expected(&descriptor), &wrong).is_err());
        Ok(())
    }
    #[test]
    fn changed_or_corrupt_archive_never_gets_a_complete_stream() -> Result<()> {
        let (_, descriptor, package, proof) = fixture()?;
        let root = tempfile::tempdir()?;
        let path = root.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        let blob = &descriptor.workspace.package;
        let target = store
            .releases()
            .join(descriptor.name())
            .join("package.blob");
        let mut held = SharingReader::open(&path, budget(), &expected(&descriptor), blob)?;
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600))?;
        assert!(held.read(0, blob.size.min(64 * 1024) as usize).is_err());
        fs::write(&target, vec![7; package.len()])?;
        fs::set_permissions(&target, fs::Permissions::from_mode(0o400))?;
        let mut corrupt = SharingReader::open(&path, budget(), &expected(&descriptor), blob)?;
        let mut offset = 0;
        while offset + 64 * 1024 < blob.size {
            corrupt.read(offset, 64 * 1024)?;
            offset += 64 * 1024;
        }
        assert!(corrupt.read(offset, (blob.size - offset) as usize).is_err());
        fs::hard_link(&target, root.path().join("hardlink"))?;
        assert!(SharingReader::open(&path, budget(), &expected(&descriptor), blob).is_err());
        Ok(())
    }
}
pub(in crate::marketplace) struct SharingReader {
    file: File,
    blob: Blob,
    stamp: Stamp,
    offset: u64,
    hash: Sha256,
}
impl SharingReader {
    /// On an owned blocking task. No package parser, pathname from a peer,
    /// copy/cache, recovery write, session or execution claim is introduced.
    pub(in crate::marketplace) fn open(
        path: &Path,
        budget: ArchiveBudget,
        expected: &SourceMatchDescriptor,
        blob: &Blob,
    ) -> Result<Self> {
        budget.validate()?;
        expected.validate()?;
        if !path.is_absolute() || path.canonicalize()? != path {
            bail!("sharing archive root must be canonical");
        }
        directory(path, 0o700)?;
        let releases = path.join("releases");
        directory(&releases, 0o700)?;
        let expires = Instant::now() + std::time::Duration::from_secs(10);
        let target = releases.join(&expected.workspace.release_id.to_string()[7..]);
        let found = inspect(&target, false, expires)?;
        found.source.matches(expected)?;
        if canonical::to_vec(&found.release)? != canonical::to_vec(&expected.release)?
            || canonical::to_vec(&found.workspace)? != canonical::to_vec(&expected.workspace)?
        {
            bail!("sharing archive differs from the exact installed source");
        }
        let slot = found
            .workspace
            .descriptors()?
            .into_iter()
            .find(|slot| slot.blob.sha256 == blob.sha256 && slot.blob.size == blob.size)
            .context("requested blob is not in the installed archive")?;
        let path = target.join(slot.purpose.file_name());
        let before = regular(&path, blob.size, Some(0o400))?;
        let file = open_file(&path)?;
        let held = stamp(&file, blob.size)?;
        if held.0 != before.dev() || held.1 != before.ino() {
            bail!("sharing file replaced while opening");
        }
        Ok(Self {
            file,
            blob: blob.clone(),
            stamp: held,
            offset: 0,
            hash: Sha256::new(),
        })
    }
    pub(in crate::marketplace) fn read(&mut self, offset: u64, maximum: usize) -> Result<Vec<u8>> {
        if self.offset != offset
            || offset >= self.blob.size
            || maximum
                != (self.blob.size - offset).min(elixir_marketplace::blob::BLOB_CHUNK_BYTES as u64)
                    as usize
            || stamp(&self.file, self.blob.size)? != self.stamp
        {
            bail!("sharing read changed its exact file or offset");
        }
        let mut bytes = vec![0; maximum];
        self.file.read_exact(&mut bytes)?;
        self.hash.update(&bytes);
        self.offset += bytes.len() as u64;
        if self.offset == self.blob.size {
            let mut extra = [0];
            if self.file.read(&mut extra)? != 0
                || Digest::from_bytes(self.hash.clone().finalize().into()) != self.blob.sha256
            {
                bail!("sharing archive content hash differs");
            }
        }
        if stamp(&self.file, self.blob.size)? != self.stamp {
            bail!("sharing archive changed during read");
        }
        // A corrupt stream never receives its final chunk/integrity EOF. The
        // consumer must still rehash and verify source before installation.
        Ok(bytes)
    }
}
