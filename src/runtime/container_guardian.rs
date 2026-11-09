//! Independently provisioned STATIC Linux guardian, never publisher/HTTP data.
//! No default binary, download, image pull, source proof or execution authority.
use anyhow::{Context, Result, bail};
use elixir_marketplace::{identity::Digest, reproduction::BuildPlatform};
use sha2::{Digest as _, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub struct Profile {
    executable: PathBuf,
    digest: Digest,
    platform: BuildPlatform,
}
impl Profile {
    pub fn from_operator_configuration(
        executable: PathBuf,
        digest: Digest,
        platform: BuildPlatform,
    ) -> Result<Self> {
        if !executable.is_absolute() || executable.canonicalize()? != executable {
            bail!("guardian requires a canonical independently approved local binary");
        }
        let value = Self {
            executable,
            digest,
            platform,
        };
        value.require()?;
        Ok(value)
    }
    pub(crate) fn path(&self) -> &Path {
        &self.executable
    }
    pub(crate) fn digest(&self) -> Digest {
        self.digest
    }
    pub(crate) fn platform(&self) -> BuildPlatform {
        self.platform
    }
    pub(crate) fn require(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(&self.executable)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.len() < 64
            || metadata.len() > 16 * 1024 * 1024
            || metadata.nlink() != 1
            || metadata.mode() & 0o022 != 0
            || metadata.mode() & 0o111 == 0
            || metadata.uid() != 0 && metadata.uid() != unsafe { libc::geteuid() }
        {
            bail!("guardian must be bounded, executable, unshared and independently provisioned");
        }
        // Reject a publisher-controlled dynamic interpreter BEFORE execution,
        // not after its loader has already had a chance to run.
        let mut header = [0; 64];
        file.read_exact(&mut header)?;
        let machine = match self.platform {
            BuildPlatform::LinuxAmd64 => 62,
            BuildPlatform::LinuxArm64 => 183,
        };
        let offset = u64::from_le_bytes(header[32..40].try_into().unwrap());
        let count = u16::from_le_bytes(header[56..58].try_into().unwrap());
        if &header[..6] != b"\x7fELF\x02\x01"
            || u16::from_le_bytes(header[18..20].try_into().unwrap()) != machine
            || !matches!(
                u16::from_le_bytes(header[16..18].try_into().unwrap()),
                2 | 3
            )
            || u16::from_le_bytes(header[54..56].try_into().unwrap()) != 56
            || count == 0
            || count > 128
            || offset > 64 * 1024
            || offset + u64::from(count) * 56 > metadata.len()
        {
            bail!("guardian ELF/platform differs from its independent approval");
        }
        file.seek(SeekFrom::Start(offset))?;
        for _ in 0..count {
            let mut program = [0; 56];
            file.read_exact(&mut program)?;
            if u32::from_le_bytes(program[..4].try_into().unwrap()) == 3 {
                bail!("guardian may not use a publisher-image ELF interpreter");
            }
        }
        file.rewind()?;
        let mut hash = Sha256::new();
        let mut block = [0; 65536];
        let mut left = metadata.len();
        while left > 0 {
            let count = left.min(block.len() as u64) as usize;
            file.read_exact(&mut block[..count])?;
            hash.update(&block[..count]);
            left -= count as u64;
        }
        if file.read(&mut [0])? != 0 || Digest::from_bytes(hash.finalize().into()) != self.digest {
            bail!("guardian differs from its independent binary approval");
        }
        let named =
            fs::symlink_metadata(&self.executable).context("guardian binding disappeared")?;
        if named.dev() != metadata.dev()
            || named.ino() != metadata.ino()
            || named.len() != metadata.len()
        {
            bail!("guardian changed during independent approval");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn elf() -> Vec<u8> {
        let mut bytes = vec![0; 120];
        bytes[..6].copy_from_slice(b"\x7fELF\x02\x01");
        bytes[16..18].copy_from_slice(&2u16.to_le_bytes());
        bytes[18..20].copy_from_slice(&62u16.to_le_bytes());
        bytes[32..40].copy_from_slice(&64u64.to_le_bytes());
        bytes[54..56].copy_from_slice(&56u16.to_le_bytes());
        bytes[56..58].copy_from_slice(&1u16.to_le_bytes());
        bytes
    }
    #[test]
    fn explicit_binary_approval_rejects_dynamic_foreign_changed_and_linked_files() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().canonicalize()?.join("guardian");
        for variant in 0..6 {
            let mut bytes = elf();
            match variant {
                1 => bytes[64..68].copy_from_slice(&3u32.to_le_bytes()),
                2 => bytes[18..20].copy_from_slice(&183u16.to_le_bytes()),
                3 => bytes[32..40].copy_from_slice(&65537u64.to_le_bytes()),
                4 => bytes.truncate(64),
                _ => {}
            }
            fs::write(&path, &bytes)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o555))?;
            let digest = if variant == 5 {
                Digest::of(b"different")
            } else {
                Digest::of(&bytes)
            };
            let value = Profile::from_operator_configuration(
                path.clone(),
                digest,
                BuildPlatform::LinuxAmd64,
            );
            assert_eq!(value.is_ok(), variant == 0, "variant {variant}");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
        }
        let bytes = elf();
        fs::write(&path, &bytes)?;
        let profile = Profile::from_operator_configuration(
            path.clone(),
            Digest::of(&bytes),
            BuildPlatform::LinuxAmd64,
        )?;
        fs::hard_link(&path, root.path().join("alias"))?;
        assert!(profile.require().is_err());
        Ok(())
    }
}
