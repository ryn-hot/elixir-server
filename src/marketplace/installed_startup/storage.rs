//! Only the two shared private ancestors. Do not acquire the provider's worker
//! lease, create execution lineage, repair old state or follow child symlinks.
use anyhow::{Context, Result, bail};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

pub(in crate::marketplace) fn prepare(data_root: &Path) -> Result<PathBuf> {
    let root = data_root
        .canonicalize()
        .context("installed data root unavailable")?;
    validate(&root, false)?;
    let mut parent = root;
    for name in ["marketplace", "worker"] {
        let child = parent.join(name);
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&child) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error).context("creating installed private ancestor"),
        }
        validate(&child, true)?;
        // Persist both concurrent first creation and our own first creation.
        // No chmod, deletion or marker/lock changes on existing directories.
        File::open(&parent)?.sync_all()?;
        parent = child;
    }
    File::open(&parent)?.sync_all()?;
    Ok(parent)
}

fn validate(path: &Path, private: bool) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() || path.canonicalize()? != path {
        bail!("installed ancestor is not a canonical directory");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let forbidden = if private { 0o077 } else { 0o022 };
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & forbidden != 0 {
            bail!("installed ancestor is not protected by the server account");
        }
    }
    #[cfg(not(unix))]
    let _ = private;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    #[test]
    fn cold_ancestors_are_private_persistent_and_do_not_create_any_owner_or_repair_state()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let worker = prepare(root.path())?;
        assert_eq!(
            worker,
            root.path().canonicalize()?.join("marketplace/worker")
        );
        for directory in [worker.parent().unwrap(), worker.as_path()] {
            assert_eq!(std::fs::symlink_metadata(directory)?.mode() & 0o777, 0o700);
        }
        assert_eq!(std::fs::read_dir(&worker)?.count(), 0);
        let marker = worker.join("provider-native.pending");
        std::fs::write(&marker, b"unconfirmed original work")?;
        let inode = std::fs::symlink_metadata(&worker)?.ino();
        assert_eq!(prepare(root.path())?, worker);
        assert_eq!(std::fs::symlink_metadata(&worker)?.ino(), inode);
        assert_eq!(std::fs::read(marker)?, b"unconfirmed original work");
        assert_eq!(std::fs::read_dir(&worker)?.count(), 1);
        Ok(())
    }

    #[test]
    fn linked_or_non_directory_ancestors_are_rejected_without_touching_the_target() -> Result<()> {
        for level in ["marketplace", "worker"] {
            for linked in [true, false] {
                let root = tempfile::tempdir()?;
                let outside = tempfile::tempdir()?;
                let parent = if level == "worker" {
                    let worker = prepare(root.path())?;
                    // Remove only our fresh, empty test directory, not history.
                    std::fs::remove_dir(&worker)?;
                    worker.parent().unwrap().to_owned()
                } else {
                    root.path().canonicalize()?
                };
                let path = parent.join(level);
                if linked {
                    symlink(outside.path(), &path)?;
                } else {
                    std::fs::write(&path, b"not a directory")?;
                }
                assert!(prepare(root.path()).is_err());
                assert_eq!(std::fs::read_dir(outside.path())?.count(), 0);
                assert!(std::fs::symlink_metadata(path).is_ok());
            }
        }
        Ok(())
    }

    #[test]
    fn unsafe_permissions_are_rejected_not_silently_repaired() -> Result<()> {
        for level in ["root", "marketplace", "worker"] {
            let root = tempfile::tempdir()?;
            let worker = prepare(root.path())?;
            let path = match level {
                "root" => root.path(),
                "marketplace" => worker.parent().unwrap(),
                _ => worker.as_path(),
            };
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))?;
            assert!(prepare(root.path()).is_err());
            assert_eq!(std::fs::symlink_metadata(path)?.mode() & 0o777, 0o777);
        }
        Ok(())
    }
}
