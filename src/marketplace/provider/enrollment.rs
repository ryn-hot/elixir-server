//! One enrollment attempt per explicitly consented local setup. This marker is
//! durable DATA, not consent, native ownership, an account or VPN health.
use crate::marketplace::{lease::ControllerLease, provider_state};
use anyhow::{Context, Result, ensure};
use elixir_marketplace::{
    canonical,
    identity::Digest,
    warp_provider::{CONSENT_VERSION, Scope},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};

const NAME: &str = "provider-enrollment.v1";
const MAX_BYTES: usize = 1024;
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    scope: Scope,
    image: Digest,
    consent_version: String,
}

// In a first-run qualification, only the UI can choose the enrollment ID.
// After its startup has joined, read the native marker for exact recovery.
// It cannot create an intent, prove a retained account or allow registration.
#[cfg(test)]
pub(super) fn fixture_recovery_scope(
    directory: &std::path::Path,
    deployment: Digest,
    image: Digest,
) -> Result<Option<Scope>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(directory.join(NAME))
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("reading first-run enrollment marker"),
    };
    let meta = file.metadata()?;
    ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.mode() & 0o077 == 0
            && meta.len() <= MAX_BYTES as u64,
        "first-run enrollment marker must be private, unshared and bounded"
    );
    let mut bytes = Vec::new();
    file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    let record: Record = canonical::decode(&bytes, MAX_BYTES)?;
    record.scope.validate()?;
    ensure!(
        record.schema_version == 1
            && record.scope.deployment == deployment
            && record.image == image
            && record.consent_version == CONSENT_VERSION,
        "first-run enrollment marker is from another owner context"
    );
    Ok(Some(record.scope))
}

pub(super) fn intent(
    lease: &ControllerLease,
    scope: Scope,
    image: Digest,
    checkpoint: bool,
) -> Result<bool> {
    let _transition = lease.activity_guard()?;
    lease.require_provider_lane()?;
    scope.validate()?;
    let record = Record {
        schema_version: 1,
        scope,
        image,
        consent_version: CONSENT_VERSION.into(),
    };
    let path = lease.directory().join(NAME);
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(file) => {
            let meta = file.metadata()?;
            ensure!(
                meta.is_file()
                    && meta.nlink() == 1
                    && meta.uid() == unsafe { libc::geteuid() }
                    && meta.mode() & 0o077 == 0
                    && meta.len() <= MAX_BYTES as u64,
                "provider enrollment marker must be private, unshared and bounded"
            );
            let mut bytes = Vec::new();
            (&file).take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
            let retained: Record = canonical::decode(&bytes, MAX_BYTES)?;
            ensure!(
                retained == record,
                "provider enrollment context changed; explicit recovery required"
            );
            ensure!(
                checkpoint,
                "provider enrollment was attempted without a recoverable identity; no new registration permitted"
            );
            // Seeing the marker after an uncertain prior fsync is not enough.
            provider_state::sync(&file)?;
            File::open(lease.directory())?.sync_all()?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ensure!(
                !checkpoint,
                "retained provider identity has no enrollment intent; explicit recovery required"
            );
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)
                .context("creating one-shot provider enrollment intent")?;
            file.write_all(&canonical::to_vec(&record)?)?;
            provider_state::sync(&file)?;
            File::open(lease.directory())?.sync_all()?;
            // No networking, native create or registration preceded durability.
            Ok(false)
        }
        Err(error) => Err(error).context("reading provider enrollment intent"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope() -> Scope {
        Scope {
            deployment: Digest::of(b"deployment"),
            enrollment: Digest::of(b"enrollment"),
        }
    }
    #[test]
    fn enrollment_is_one_shot_and_only_retained_identity_can_resume() -> Result<()> {
        let root = tempfile::tempdir()?;
        let lease = ControllerLease::acquire(&root.path().canonicalize()?.join("owner"))?;
        let image = Digest::of(b"image");
        assert!(!intent(&lease, scope(), image, false)?);
        assert!(
            intent(&lease, scope(), image, false).is_err(),
            "missing identity must never re-enroll"
        );
        assert!(intent(&lease, scope(), image, true)?);
        assert!(intent(&lease, scope(), Digest::of(b"other"), true).is_err());
        let changed = Scope {
            enrollment: Digest::of(b"changed"),
            ..scope()
        };
        assert!(intent(&lease, changed, image, true).is_err());
        drop(lease);
        let lease =
            ControllerLease::acquire_for_recovery(&root.path().canonicalize()?.join("owner"))?;
        assert!(
            intent(&lease, scope(), image, false).is_err(),
            "restart cannot reset enrollment"
        );
        assert!(intent(&lease, scope(), image, true)?);
        Ok(())
    }
    #[test]
    fn first_run_recovery_reads_only_the_original_native_marker() -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("owner");
        let lease = ControllerLease::acquire(&directory)?;
        let image = Digest::of(b"image");
        assert!(fixture_recovery_scope(&directory, scope().deployment, image)?.is_none());
        assert!(!intent(&lease, scope(), image, false)?);
        drop(lease);
        assert_eq!(
            fixture_recovery_scope(&directory, scope().deployment, image)?,
            Some(scope())
        );
        assert!(
            fixture_recovery_scope(&directory, Digest::of(b"other deployment"), image).is_err()
        );
        assert!(
            fixture_recovery_scope(&directory, scope().deployment, Digest::of(b"other image"))
                .is_err()
        );
        Ok(())
    }
    #[test]
    fn orphaned_or_corrupt_or_linked_intent_fails_closed() -> Result<()> {
        use std::os::unix::{fs::PermissionsExt, fs::symlink};
        let root = tempfile::tempdir()?;
        let lease = ControllerLease::acquire(&root.path().canonicalize()?.join("owner"))?;
        let image = Digest::of(b"image");
        assert!(intent(&lease, scope(), image, true).is_err());
        let path = lease.directory().join(NAME);
        assert!(!path.exists());
        assert!(!intent(&lease, scope(), image, false)?);
        let bytes = std::fs::read(&path)?;
        std::fs::write(&path, b"{")?;
        assert!(intent(&lease, scope(), image, true).is_err());
        std::fs::write(&path, &bytes)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
        assert!(intent(&lease, scope(), image, true).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let retained = lease.directory().join("retained");
        std::fs::rename(&path, &retained)?;
        symlink(&retained, &path)?;
        assert!(intent(&lease, scope(), image, true).is_err());
        Ok(())
    }
}
