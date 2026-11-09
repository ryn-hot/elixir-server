//! One local controller owns a deployment's private worker stream at a time.
//! Ownership is an OS lock on a persistent inode, never a PID or stale-file
//! heuristic. The trusted caller supplies the same private state directory on
//! every start; placing that directory on an unsupported filesystem fails closed.
use std::{
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

pub(super) mod activity;

pub(super) struct ControllerLease {
    file: File,
    directory: PathBuf,
    activity: std::sync::Mutex<()>,
}

impl ControllerLease {
    #[cfg(unix)]
    pub(super) fn require_provider_lane(&self) -> Result<()> {
        if !self.matches_directory(&self.directory) {
            bail!("provider lost its original OS lease");
        }
        for name in [
            activity::FILE_NAME,
            activity::http::FILE_NAME,
            activity::GATEWAY_FILE_NAME,
            "image-import.request",
            "image-import.attempt",
            "image-import.result",
            "image-import.retired",
            "image-reference.pending",
            "image-reference.next",
        ] {
            match std::fs::symlink_metadata(self.directory.join(name)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                _ => bail!(
                    "provider cannot enter a directory with unrelated interrupted native work"
                ),
            }
        }
        Ok(())
    }
    /// Stable DATA namespace for one disposable workspace, not a runtime grant.
    /// Reopening the same locked inode preserves it; replacing the storage does
    /// not silently adopt another controller's volumes.
    pub(super) fn storage_namespace(&self) -> Result<String> {
        if !self.matches_directory(&self.directory) {
            bail!("cache storage lost its original controller lock");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let lock = self.file.metadata()?;
            let directory = std::fs::symlink_metadata(&self.directory)?;
            let encoded = elixir_marketplace::canonical::to_vec(&(
                "elixir-source-cache-v1",
                self.directory.as_os_str().as_encoded_bytes(),
                directory.dev(),
                directory.ino(),
                lock.dev(),
                lock.ino(),
            ))?;
            Ok(elixir_marketplace::identity::Digest::of(encoded).to_string()[7..].to_owned())
        }
        #[cfg(not(unix))]
        bail!("source cache requires stable local inode identities")
    }
    pub(super) fn directory(&self) -> &Path {
        &self.directory
    }
    pub(super) fn acquire(directory: &Path) -> Result<Self> {
        Self::acquire_inner(directory, false)
    }
    /// Recovery may inspect an interrupted native operation while retaining
    /// the same OS lock. It must not start replacement work or erase its fence
    /// without proving cleanup of the recorded immutable identity.
    pub(super) fn acquire_for_recovery(directory: &Path) -> Result<Self> {
        Self::acquire_inner(directory, true)
    }
    fn acquire_inner(directory: &Path, recovering: bool) -> Result<Self> {
        if !directory.is_absolute() {
            bail!("marketplace controller state requires an absolute private directory");
        }
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error).context("creating marketplace controller directory"),
        }
        let metadata = std::fs::symlink_metadata(directory)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || directory.canonicalize()? != directory
        {
            bail!("marketplace controller directory is not a canonical private directory");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
                bail!("marketplace controller directory must be private to the server account");
            }
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // Open a reparse point itself, not its potentially foreign target.
            options.custom_flags(0x0020_0000);
        }
        let file = options.open(directory.join("controller.lock"))?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 4096 {
            bail!("invalid marketplace controller lock inode");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.mode() & 0o077 != 0
            {
                bail!("marketplace controller lock must be private and unshared");
            }
        }
        file.try_lock()
            .context("marketplace controller is already owned or OS locking is unavailable")?;
        let lease = Self {
            file,
            directory: directory.to_owned(),
            activity: std::sync::Mutex::new(()),
        };
        if !recovering {
            for name in [
                activity::FILE_NAME,
                activity::http::FILE_NAME,
                activity::GATEWAY_FILE_NAME,
                "provider-native.pending",
                "provider-native.next",
                "image-import.request",
                "image-import.attempt",
                "image-import.result",
                "image-import.retired",
                "image-reference.pending",
                "image-reference.next",
            ] {
                match std::fs::symlink_metadata(directory.join(name)) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    _ => bail!("interrupted native marketplace work requires confirmed cleanup"),
                }
            }
        }
        Ok(lease)
    }
    /// Short synchronous journal transitions only; never hold across an await.
    pub(super) fn activity_guard(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.activity
            .lock()
            .map_err(|_| anyhow::anyhow!("native activity transition was poisoned"))
    }
    /// Handoffs must retain the same private path and locked inode, not merely
    /// any live lock from another deployment or a replaced pathname.
    pub(super) fn matches_directory(&self, directory: &Path) -> bool {
        if self.directory != directory
            || directory.canonicalize().ok().as_deref() != Some(directory)
        {
            return false;
        }
        let Ok(path) = std::fs::symlink_metadata(directory.join("controller.lock")) else {
            return false;
        };
        let Ok(held) = self.file.metadata() else {
            return false;
        };
        if !path.is_file() || path.file_type().is_symlink() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if path.dev() != held.dev() || path.ino() != held.ino() || path.nlink() != 1 {
                return false;
            }
        }
        #[cfg(not(unix))]
        let _ = held;
        true
    }
}

impl Drop for ControllerLease {
    fn drop(&mut self) {
        // A concurrent process spawn can briefly inherit a close-on-exec FD
        // before exec. Explicit unlock releases the shared lock description
        // immediately; relying only on close can spuriously retain ownership.
        // A crashed owner still releases the lock when its handles close.
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_handoff_keeps_the_same_lock_and_rejects_another_directory() -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("controller");
        let lease = ControllerLease::acquire(&directory)?;
        assert!(lease.matches_directory(&directory));
        assert!(!lease.matches_directory(&directory.join("other")));
        let transferred = Some(lease);
        assert!(
            ControllerLease::acquire(&directory).is_err(),
            "moving the lease must not unlock it"
        );
        assert!(transferred.as_ref().unwrap().matches_directory(&directory));
        drop(transferred);
        assert!(ControllerLease::acquire(&directory).is_ok());
        Ok(())
    }
    #[cfg(unix)]
    #[test]
    fn controller_handoff_rejects_a_replaced_lock_inode() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("controller");
        let lease = ControllerLease::acquire(&directory)?;
        let path = directory.join("controller.lock");
        std::fs::rename(&path, directory.join("retired.lock"))?;
        File::create(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        assert!(!lease.matches_directory(&directory));
        assert!(
            ControllerLease::acquire(&directory).is_ok(),
            "the old descriptor cannot claim the replacement lock"
        );
        Ok(())
    }
    #[test]
    fn lease_requires_live_ownership_and_never_deletes_its_inode() -> Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = parent.path().canonicalize()?.join("controller");
        let lease = ControllerLease::acquire(&directory)?;
        assert!(ControllerLease::acquire(&directory).is_err());
        let path = directory.join("controller.lock");
        let before = std::fs::metadata(&path)?;
        // Like a descriptor inherited during a concurrent spawn, this clone
        // keeps the open description alive after the controller is dropped.
        // Ownership must still be explicitly released, not delayed until the
        // unrelated description disappears.
        let _inherited_description = lease.file.try_clone()?;
        drop(lease);
        assert!(
            path.is_file(),
            "releasing ownership must not unlink the lock inode"
        );
        let _next = ControllerLease::acquire(&directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(before.ino(), std::fs::metadata(path)?.ino());
        }
        Ok(())
    }

    #[test]
    #[ignore = "child-only OS-lock crash fixture; invoked by the ownership test"]
    fn lease_process_fixture() -> Result<()> {
        use std::io::Write;
        let directory = std::env::var_os("ELIXIR_MARKETPLACE_LOCK_FIXTURE")
            .context("child-only lock fixture requires its private directory")?;
        let directory = Path::new(&directory);
        let _lease = ControllerLease::acquire(directory)?;
        File::create(directory.join("owner.ready"))?.write_all(b"owned")?;
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn independent_process_ownership_is_released_by_a_crash() -> Result<()> {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let parent = tempfile::tempdir()?;
        let directory = parent.path().canonicalize()?.join("controller");
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "marketplace::lease::tests::lease_process_fixture",
                    "--ignored",
                    "--nocapture",
                ])
                .env("ELIXIR_MARKETPLACE_LOCK_FIXTURE", &directory)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?,
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !directory.join("owner.ready").is_file() {
            if std::time::Instant::now() >= deadline || child.0.try_wait()?.is_some() {
                bail!("lock fixture did not acquire ownership");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(ControllerLease::acquire(&directory).is_err());
        child.0.kill()?;
        child.0.wait()?;
        let _lease = ControllerLease::acquire(&directory)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn shared_directories_and_symlink_or_hardlink_lock_files_are_rejected() -> Result<()> {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let parent = tempfile::tempdir()?;
        let directory = parent.path().canonicalize()?.join("controller");
        std::fs::create_dir(&directory)?;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755))?;
        assert!(ControllerLease::acquire(&directory).is_err());
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        let target = parent.path().join("target");
        File::create(&target)?;
        symlink(&target, directory.join("controller.lock"))?;
        assert!(ControllerLease::acquire(&directory).is_err());
        std::fs::remove_file(directory.join("controller.lock"))?;
        std::fs::hard_link(&target, directory.join("controller.lock"))?;
        assert!(ControllerLease::acquire(&directory).is_err());
        let alias = parent.path().join("alias");
        symlink(&directory, &alias)?;
        assert!(ControllerLease::acquire(&alias).is_err());
        Ok(())
    }
}
