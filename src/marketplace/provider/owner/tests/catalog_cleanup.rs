//! Retire only sealed release directories in an already-stopped test fixture.
use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, OpenOptions, Permissions},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

pub(super) fn unseal_completed_archives(root: &Path) -> Result<()> {
    let owner = unsafe { libc::geteuid() };
    let owned_directory = |path: &Path| -> Result<_> {
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_dir() && metadata.uid() == owner && metadata.mode() & 0o022 == 0,
            "catalog fixture archive is not an owned directory"
        );
        Ok(metadata)
    };
    owned_directory(root)?;
    let mut releases = root.to_owned();
    for component in ["marketplace", "worker", "source-archives", "releases"] {
        releases.push(component);
        owned_directory(&releases)?;
    }
    let mut sealed = Vec::new();
    for release in fs::read_dir(releases)?.take(17) {
        ensure!(sealed.len() < 16, "catalog fixture release count exceeded");
        let release = release?;
        let name = release.file_name();
        let id = name
            .to_str()
            .context("invalid catalog fixture release name")?;
        ensure!(
            id.len() == 64 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
            "unknown or incomplete catalog fixture release"
        );
        let metadata = owned_directory(&release.path())?;
        ensure!(
            metadata.mode() & 0o7777 == 0o500,
            "unsealed catalog fixture release"
        );
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(release.path())?;
        let opened = directory.metadata()?;
        ensure!(
            (
                metadata.dev(),
                metadata.ino(),
                metadata.mode(),
                metadata.uid()
            ) == (opened.dev(), opened.ino(), opened.mode(), opened.uid()),
            "catalog fixture release changed during inspection"
        );
        let mut names = Vec::new();
        for entry in fs::read_dir(release.path())?.take(5) {
            let entry = entry?;
            let file = fs::symlink_metadata(entry.path())?;
            ensure!(
                file.is_file() && file.uid() == owner && file.mode() & 0o7777 == 0o400,
                "catalog fixture capsule entry differs"
            );
            names.push(entry.file_name());
        }
        names.sort();
        ensure!(
            names == ["descriptor.json", "package.blob", "proof.blob"]
                || names
                    == [
                        "artifact.blob",
                        "descriptor.json",
                        "package.blob",
                        "proof.blob"
                    ],
            "catalog fixture capsule layout differs"
        );
        sealed.push(directory);
    }
    ensure!(
        !sealed.is_empty(),
        "catalog fixture did not retain a sealed release"
    );
    // Never broaden permissions until the complete layout has passed. Use the
    // opened directory handles, not a path that might now name something else.
    for directory in sealed {
        directory.set_permissions(Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::DirBuilderExt;

    fn make_release(root: &Path, name: &str, artifact: bool) -> Result<std::path::PathBuf> {
        let release = root
            .join("marketplace/worker/source-archives/releases")
            .join(name);
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&release)?;
        let mut names = vec!["descriptor.json", "package.blob", "proof.blob"];
        if artifact {
            names.push("artifact.blob");
        }
        for name in names {
            let file = release.join(name);
            fs::write(&file, b"synthetic catalog fixture")?;
            fs::set_permissions(file, Permissions::from_mode(0o400))?;
        }
        fs::set_permissions(&release, Permissions::from_mode(0o500))?;
        Ok(release)
    }

    #[test]
    fn sealed_source_and_artifact_capsules_can_be_retired() -> Result<()> {
        let root = tempfile::tempdir()?;
        make_release(root.path(), &"0".repeat(64), false)?;
        make_release(root.path(), &"a".repeat(64), true)?;
        unseal_completed_archives(root.path())?;
        let path = root.path().to_owned();
        root.close()?;
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn unknown_release_preserves_sealed_permissions() -> Result<()> {
        let root = tempfile::tempdir()?;
        let sealed = make_release(root.path(), &"b".repeat(64), false)?;
        fs::create_dir(sealed.parent().unwrap().join(".pending"))?;
        assert!(unseal_completed_archives(root.path()).is_err());
        assert_eq!(fs::metadata(&sealed)?.mode() & 0o7777, 0o500);
        fs::set_permissions(sealed, Permissions::from_mode(0o700))?;
        root.close()?;
        Ok(())
    }

    #[test]
    fn linked_release_is_never_unsealed() -> Result<()> {
        let root = tempfile::tempdir()?;
        let foreign = tempfile::tempdir()?;
        let releases = root
            .path()
            .join("marketplace/worker/source-archives/releases");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&releases)?;
        let link = releases.join("c".repeat(64));
        std::os::unix::fs::symlink(foreign.path(), link)?;
        let original = fs::metadata(foreign.path())?.mode();
        assert!(unseal_completed_archives(root.path()).is_err());
        assert_eq!(fs::metadata(foreign.path())?.mode(), original);
        Ok(())
    }
}
