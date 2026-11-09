//! One encrypted provider identity under the original OS controller lease.
//! Committed is durability DATA only, never provider consent, native ownership,
//! a healthy tunnel or a packet lease. No downloader state or plaintext fallback.
use super::lease::ControllerLease;
use crate::secrets::SecretsManager;
use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use elixir_marketplace::{
    canonical,
    identity::Digest,
    warp_provider::{CONSENT_VERSION, MAX_STATE_BYTES, Scope, State},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
use zeroize::Zeroizing;

const CURRENT: &str = "provider-state.v1";
const PENDING: &str = "provider-state.next";
const MAX_RECORD_BYTES: usize = 2 * MAX_STATE_BYTES + 8192;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    scope: Scope,
    image: Digest,
    consent_version: String,
    revision: u64,
    previous: Option<Digest>,
    digest: Digest,
    ciphertext: String,
}
pub(super) struct Committed {
    scope: Scope,
    image: Digest,
    digest: Digest,
}
impl Committed {
    pub(super) fn require(&self, scope: Scope, image: Digest, digest: Digest) -> Result<()> {
        ensure!(
            self.scope == scope && self.image == image && self.digest == digest,
            "provider state commit differs from the original checkpoint"
        );
        Ok(())
    }
}
pub(super) struct Loaded {
    pub(super) bytes: Zeroizing<Vec<u8>>,
    pub(super) digest: Digest,
    revision: u64,
}
fn associated(record: &Record) -> Result<Vec<u8>> {
    Ok(canonical::to_vec(&(
        "elixir/marketplace/provider-state/v1",
        record.schema_version,
        record.scope,
        record.image,
        &record.consent_version,
        record.revision,
        record.previous,
        record.digest,
    ))?)
}
fn current_owner(lease: &ControllerLease) -> Result<()> {
    ensure!(
        lease.matches_directory(lease.directory()),
        "provider state lost its original controller lease"
    );
    Ok(())
}
fn read(path: &Path) -> Result<Option<Record>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("opening private provider state"),
    };
    let meta = file.metadata()?;
    ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.mode() & 0o077 == 0
            && meta.len() <= MAX_RECORD_BYTES as u64,
        "provider state file must be private, unshared and bounded"
    );
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take((MAX_RECORD_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_RECORD_BYTES,
        "provider state record exceeds its bound"
    );
    Ok(Some(canonical::decode(&bytes, MAX_RECORD_BYTES)?))
}
fn open_record(
    record: &Record,
    secrets: &SecretsManager,
    scope: Scope,
    image: Digest,
) -> Result<Loaded> {
    ensure!(
        record.schema_version == 1
            && record.scope == scope
            && record.image == image
            && record.consent_version == CONSENT_VERSION
            && (1..=(1u64 << 53) - 1).contains(&record.revision)
            && record.ciphertext.len() <= MAX_RECORD_BYTES,
        "provider state context differs"
    );
    scope.validate()?;
    let plaintext =
        Zeroizing::new(secrets.decrypt_bound(&record.ciphertext, &associated(record)?)?);
    ensure!(
        plaintext.len() <= 4 * MAX_STATE_BYTES.div_ceil(3),
        "provider state encoding exceeds its bound"
    );
    let bytes = Zeroizing::new(
        STANDARD
            .decode(plaintext.as_bytes())
            .map_err(|_| anyhow::anyhow!("invalid provider state encoding"))?,
    );
    ensure!(
        bytes.len() <= MAX_STATE_BYTES && Digest::of(bytes.as_slice()) == record.digest,
        "provider state digest differs"
    );
    // Validate opaque binary shape, paths and exact purpose/enrollment before
    // returning any bytes for a fresh native process. No repair/new enrollment.
    let state = State::decode(bytes, scope)?;
    Ok(Loaded {
        bytes: state.encode(),
        digest: record.digest,
        revision: record.revision,
    })
}
fn absent(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error).context("inspecting provider checkpoint"),
        Ok(_) => Ok(false),
    }
}
pub(super) fn sync(file: &File) -> Result<()> {
    file.sync_all()?;
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        ensure!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == 0,
            "provider state full synchronization failed"
        );
    }
    Ok(())
}
pub(super) fn load(
    lease: &ControllerLease,
    secrets: &SecretsManager,
    scope: Scope,
    image: Digest,
) -> Result<Option<Loaded>> {
    let _guard = lease.activity_guard()?;
    current_owner(lease)?;
    ensure!(
        absent(&lease.directory().join(PENDING))?,
        "interrupted provider state publication requires recovery"
    );
    let result = read(&lease.directory().join(CURRENT))?
        .map(|record| open_record(&record, secrets, scope, image))
        .transpose()?;
    current_owner(lease)?;
    Ok(result)
}
pub(super) fn save(
    lease: &ControllerLease,
    secrets: &SecretsManager,
    scope: Scope,
    image: Digest,
    expected: Option<Digest>,
    bytes: Zeroizing<Vec<u8>>,
) -> Result<Committed> {
    let state = State::decode(bytes, scope)?;
    let bytes = state.encode();
    let digest = Digest::of(bytes.as_slice());
    let _guard = lease.activity_guard()?;
    current_owner(lease)?;
    let pending = lease.directory().join(PENDING);
    let current = lease.directory().join(CURRENT);
    ensure!(
        absent(&pending)?,
        "interrupted provider state publication requires recovery"
    );
    let prior = read(&current)?
        .map(|record| open_record(&record, secrets, scope, image))
        .transpose()?;
    if prior.as_ref().is_some_and(|prior| prior.digest == digest) {
        // A previous rename/fsync may have had an uncertain outcome. Seeing
        // matching bytes in the page cache is not a durable acknowledgement.
        sync(&File::open(&current)?)?;
        File::open(lease.directory())?.sync_all()?;
        current_owner(lease)?;
        return Ok(Committed {
            scope,
            image,
            digest,
        });
    }
    ensure!(
        prior.as_ref().map(|prior| prior.digest) == expected,
        "provider state changed before publication"
    );
    let revision = prior.as_ref().map_or(Ok(1), |prior| {
        prior
            .revision
            .checked_add(1)
            .filter(|n| *n < (1u64 << 53))
            .context("provider state revision exhausted")
    })?;
    let mut record = Record {
        schema_version: 1,
        scope,
        image,
        consent_version: CONSENT_VERSION.into(),
        revision,
        previous: expected,
        digest,
        ciphertext: String::new(),
    };
    let plaintext = Zeroizing::new(STANDARD.encode(bytes.as_slice()));
    record.ciphertext = secrets.encrypt_bound(&plaintext, &associated(&record)?)?;
    let encoded = canonical::to_vec(&record)?;
    ensure!(
        encoded.len() <= MAX_RECORD_BYTES,
        "provider state record exceeds its bound"
    );
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&pending)?;
    file.write_all(&encoded)?;
    sync(&file)?;
    current_owner(lease)?;
    fs::rename(&pending, &current)?;
    File::open(lease.directory())?.sync_all()?;
    current_owner(lease)?;
    // Read/decrypt the published record before acknowledging its exact digest.
    let published = read(&current)?.context("published provider state disappeared")?;
    ensure!(
        open_record(&published, secrets, scope, image)?.digest == digest,
        "published provider state differs"
    );
    Ok(Committed {
        scope,
        image,
        digest,
    })
}
/// Recover only one authenticated DATA publication. This does not reconcile or
/// adopt native work, grant consent, start a process or authorize any traffic.
pub(super) fn recover(
    lease: &ControllerLease,
    secrets: &SecretsManager,
    scope: Scope,
    image: Digest,
) -> Result<()> {
    let _guard = lease.activity_guard()?;
    current_owner(lease)?;
    let pending = lease.directory().join(PENDING);
    let current = lease.directory().join(CURRENT);
    let Some(record) = read(&pending)? else {
        return Ok(());
    };
    let candidate = open_record(&record, secrets, scope, image)?;
    let prior = read(&current)?
        .map(|record| open_record(&record, secrets, scope, image))
        .transpose()?;
    if prior.as_ref().is_some_and(|prior| {
        prior.digest == candidate.digest && prior.revision == candidate.revision
    }) {
        // Only this verified private pending inode is removed, never provider
        // identities or an unknown native ownership/recovery fence.
        sync(&File::open(&current)?)?;
        current_owner(lease)?;
        fs::remove_file(&pending)?;
        File::open(lease.directory())?.sync_all()?;
        return Ok(());
    }
    ensure!(
        prior.as_ref().map(|prior| prior.digest) == record.previous
            && prior.as_ref().map_or(1, |prior| prior.revision + 1) == record.revision,
        "provider state recovery conflicts with current identity"
    );
    sync(&File::open(&pending)?)?;
    current_owner(lease)?;
    fs::rename(pending, current)?;
    File::open(lease.directory())?.sync_all()?;
    current_owner(lease)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::warp_provider::StateFile;
    fn scope() -> Scope {
        Scope {
            deployment: Digest::of(b"installation"),
            enrollment: Digest::of(b"enrollment"),
        }
    }
    fn state(value: &[u8]) -> Zeroizing<Vec<u8>> {
        State::new(
            scope(),
            Digest::of(b"registration"),
            vec![StateFile::new("reg.json".into(), Zeroizing::new(value.to_vec())).unwrap()],
        )
        .unwrap()
        .encode()
    }
    #[test]
    fn committed_state_is_encrypted_bound_durable_and_never_silently_rotated() -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let secrets = SecretsManager::from_key_bytes([91; 32], true);
        let image = Digest::of(b"client-image");
        let lease = ControllerLease::acquire(&directory)?;
        let bytes = state(b"private-registration-fixture");
        let digest = Digest::of(bytes.as_slice());
        let receipt = save(&lease, &secrets, scope(), image, None, bytes)?;
        receipt.require(scope(), image, digest)?;
        assert!(
            receipt
                .require(scope(), Digest::of(b"other-image"), digest)
                .is_err()
        );
        let raw = fs::read(directory.join(CURRENT))?;
        assert!(
            !raw.windows(b"private-registration-fixture".len())
                .any(|part| part == b"private-registration-fixture")
        );
        assert!(
            save(
                &lease,
                &secrets,
                scope(),
                image,
                None,
                state(b"other-state")
            )
            .is_err()
        );
        assert!(
            load(
                &lease,
                &SecretsManager::from_key_bytes([92; 32], true),
                scope(),
                image
            )
            .is_err()
        );
        assert!(load(&lease, &secrets, scope(), Digest::of(b"different-image")).is_err());
        drop(lease);
        let lease = ControllerLease::acquire(&directory)?;
        let restored = load(&lease, &secrets, scope(), image)?.unwrap();
        assert!(restored.bytes.as_slice() == state(b"private-registration-fixture").as_slice());
        save(
            &lease,
            &secrets,
            scope(),
            image,
            Some(digest),
            state(b"updated-state"),
        )?;
        assert!(load(&lease, &secrets, scope(), image)?.unwrap().digest != digest);
        Ok(())
    }
    #[test]
    fn interrupted_authenticated_publication_recovers_but_foreign_or_plaintext_state_does_not()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let lease = ControllerLease::acquire(&directory)?;
        let secrets = SecretsManager::from_key_bytes([93; 32], true);
        let image = Digest::of(b"image");
        save(&lease, &secrets, scope(), image, None, state(b"fixture"))?;
        fs::rename(directory.join(CURRENT), directory.join(PENDING))?;
        assert!(load(&lease, &secrets, scope(), image).is_err());
        recover(&lease, &secrets, scope(), image)?;
        assert!(load(&lease, &secrets, scope(), image)?.is_some());
        let mut record = read(&directory.join(CURRENT))?.unwrap();
        record.ciphertext = "plaintext".into();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join(PENDING))?;
        file.write_all(&canonical::to_vec(&record)?)?;
        assert!(recover(&lease, &secrets, scope(), image).is_err());
        assert!(directory.join(PENDING).is_file());
        Ok(())
    }
}
