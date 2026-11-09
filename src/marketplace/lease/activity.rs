//! One bounded durable fence, not a queue/history. Cancellation or a crashed
//! controller must not turn a released OS lock into proof that native work
//! stopped. Ordinary lease acquisition rejects even a malformed fence.
use super::ControllerLease;
use anyhow::{Context, Result, bail};
use elixir_marketplace::canonical;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};
use uuid::Uuid;

pub(super) const FILE_NAME: &str = "native-job.pending";
pub(super) const GATEWAY_FILE_NAME: &str = "gateway-helper.pending";
const MAX_BYTES: usize = 4096;
#[cfg(unix)]
pub(in crate::marketplace) mod gateway;
pub(in crate::marketplace) mod http;

fn require_gateway_absent(lease: &ControllerLease) -> Result<()> {
    match fs::symlink_metadata(lease.directory.join(GATEWAY_FILE_NAME)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => bail!("the original application gateway still requires confirmed cleanup"),
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::marketplace) enum Target {
    /// One passive daemon load, independently owned across server death. A lost
    /// keeper/transport remains fenced: image presence is not load completion.
    ImageImport {
        intent: elixir_marketplace::container_image::import::ImportIntent,
    },
    /// Actual helper cleanup was joined and its durable receipt matched this
    /// original operation. Recovery now removes only bounded local control data.
    ImageImportRetired {
        intent: elixir_marketplace::container_image::import::ImportIntent,
    },
    /// Passive image ownership was persisted separately. No publisher process
    /// starts in this phase; the original source owner may enter its next phase.
    ImageReferenceAdopted {
        intent: elixir_marketplace::container_image::import::ImportIntent,
    },
    /// Storage-only allocation, not an executable actor or a stopped-process
    /// claim. One inode-derived slot bounds even an uncertain late allocation.
    BuildOutputAllocation {
        deployment: String,
    },
    SourceProvisioner {
        deployment: String,
    },
    /// New create-only boundary. Unlike legacy unbound SourceWriter records,
    /// this operation is never permitted to start before a durable CID bind.
    #[serde(rename = "source_writer_create")]
    WriterCreate {
        deployment: String,
    },
    /// Unlike the older deployment-only marker, this captures the exact desired
    /// configuration's creation fingerprint. Recovery is cleanup-only/Created.
    #[serde(rename = "source_writer_create_v2")]
    WriterCreateIntent {
        intent: crate::marketplace::runtime::source_writer::SourceWriterIntent,
    },
    SourceWriter {
        deployment: String,
        container_id: Option<String>,
    },
    /// This role is CREATE ONLY until its CID is fsynced. No source bytes,
    /// SDK, network, mounts or writable application state are involved.
    Measurement {
        deployment: String,
        container_id: Option<String>,
    },
}
impl Target {
    fn validate(&self) -> Result<()> {
        let deployment: &str = match self {
            Self::ImageImport { intent } | Self::ImageImportRetired { intent } | Self::ImageReferenceAdopted { intent } => {
                intent.validate()?;
                &intent.deployment
            }
            Self::WriterCreateIntent { intent } => {
                intent.validate()?;
                intent.deployment()
            }
            Self::SourceProvisioner { deployment }
            | Self::WriterCreate { deployment }
            | Self::BuildOutputAllocation { deployment } => deployment,
            Self::SourceWriter {
                deployment,
                container_id,
            }
            | Self::Measurement {
                deployment,
                container_id,
            } => {
                if container_id.as_ref().is_some_and(|id| {
                    id.len() != 64
                        || !id
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                }) {
                    bail!("invalid native activity immutable identity");
                }
                deployment
            }
        };
        if deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            bail!("invalid native activity deployment");
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    operation_id: Uuid,
    target: Target,
}

pub(in crate::marketplace) struct Fence {
    file: File,
    record: Record,
}
impl Fence {
    pub(in crate::marketplace) fn begin(lease: &ControllerLease, target: Target) -> Result<Self> {
        let _guard = lease.activity_guard()?;
        http::require_absent(lease)?;
        require_gateway_absent(lease)?;
        target.validate()?;
        if !lease.matches_directory(&lease.directory) {
            bail!("native activity lost its controller lock identity");
        }
        let record = Record {
            schema_version: 1,
            operation_id: Uuid::new_v4(),
            target,
        };
        let mut options = options();
        options.create_new(true);
        let mut file = options.open(lease.directory.join(FILE_NAME))?;
        file.write_all(&canonical::to_vec(&record)?)?;
        file.sync_all()?;
        File::open(&lease.directory)?.sync_all()?;
        // The global lane itself may have been created for this first job.
        // Persist its parent entry too, not only the fence inside that lane.
        #[cfg(unix)]
        File::open(
            lease
                .directory
                .parent()
                .context("native job lane has no parent")?,
        )?
        .sync_all()?;
        Ok(Self { file, record })
    }
    pub(in crate::marketplace) fn load(lease: &ControllerLease) -> Result<Option<Self>> {
        let mut file = match options().open(lease.directory.join(FILE_NAME)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let record = read(&mut file)?;
        let value = Self { file, record };
        value.require_same_file(lease)?;
        Ok(Some(value))
    }
    pub(in crate::marketplace) fn target(&self) -> &Target {
        &self.record.target
    }
    pub(in crate::marketplace) fn operation_id(&self)->Uuid {self.record.operation_id}
    #[cfg(unix)]
    pub(in crate::marketplace) fn current_bytes(&self,lease:&ControllerLease)->Result<Vec<u8>> {
        let _guard=lease.activity_guard()?;self.require_same_file(lease)?;
        if read(&mut self.file.try_clone()?)?!=self.record {bail!("native activity changed before ownership handoff");}
        Ok(canonical::to_vec(&self.record)?)
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn adopt_image_reference(&mut self,lease:&ControllerLease)->Result<()> {
        let _guard=lease.activity_guard()?;self.require_same_file(lease)?;
        if read(&mut self.file)?!=self.record {bail!("native import changed before image reference handoff");}
        let Target::ImageImport{intent}=&self.record.target else {bail!("only an actual active import can hand off its image");};
        crate::marketplace::quarantine::image_reference::require_active(lease,self.record.operation_id,intent)?;
        self.write_bound(Target::ImageReferenceAdopted{intent:intent.clone()})
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn retire_import(&mut self,lease:&ControllerLease)->Result<()> {
        let _guard=lease.activity_guard()?;self.require_same_file(lease)?;
        if read(&mut self.file)?!=self.record {bail!("import journal changed before retirement");}
        let Target::ImageImport{intent}=&self.record.target else {bail!("not an active image import");};
        let receipt=crate::marketplace::quarantine::image_import::read_control(lease,"image-import.retired",4096)?;
        if receipt!=canonical::to_vec(&self.record)? {bail!("import retirement receipt differs from actual root operation");}
        self.write_bound(Target::ImageImportRetired{intent:intent.clone()})
    }
    pub(in crate::marketplace) fn bind_writer(
        &mut self,
        lease: &ControllerLease,
        id: &str,
    ) -> Result<()> {
        let _guard = lease.activity_guard()?;
        self.require_same_file(lease)?;
        if read(&mut self.file)? != self.record {
            bail!("native activity record changed before process binding");
        }
        let deployment: &str = match &self.record.target {
            Target::WriterCreateIntent { intent } => intent.deployment(),
            Target::WriterCreate { deployment }
            | Target::SourceWriter {
                deployment,
                container_id: None,
            } => deployment,
            _ => bail!("native activity cannot bind a different process"),
        };
        let target = Target::SourceWriter {
            deployment: deployment.to_owned(),
            container_id: Some(id.to_owned()),
        };
        self.write_bound(target)
    }
    /// Called only under the same live OS lease after the original create task
    /// is joined or its process has died. Empty names and older unknown-start
    /// records are not completion evidence. Save the observed CID before stop.
    pub(in crate::marketplace) async fn reconcile_writer_create(
        &mut self,
        runtime: &dyn crate::runtime::RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<bool> {
        let intent = {
            let _guard = lease.activity_guard()?;
            self.require_same_file(lease)?;
            if read(&mut self.file)? != self.record {
                bail!("source creation record changed");
            }
            let Target::WriterCreateIntent { intent } = &self.record.target else {
                return Ok(false);
            };
            intent.clone()
        };
        let handle = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            runtime.inspect_unstarted_quarantine_marketplace_writer(&intent),
        )
        .await
        .context("unbound source creation inspection deadline")??;
        if handle.name != intent.name() || !crate::runtime::packet_network::immutable_id(&handle.id)
        {
            bail!("unbound source inspection returned another identity");
        }
        self.bind_writer(lease, &handle.id)?;
        Ok(true)
    }
    pub(in crate::marketplace) fn bind_measurement(
        &mut self,
        lease: &ControllerLease,
        id: &str,
    ) -> Result<()> {
        let _guard = lease.activity_guard()?;
        self.require_same_file(lease)?;
        if read(&mut self.file)? != self.record {
            bail!("native measurement record changed");
        }
        let Target::Measurement {
            deployment,
            container_id: None,
        } = &self.record.target
        else {
            bail!("measurement cannot bind a different native role");
        };
        self.write_bound(Target::Measurement {
            deployment: deployment.clone(),
            container_id: Some(id.to_owned()),
        })
    }
    fn write_bound(&mut self, target: Target) -> Result<()> {
        target.validate()?;
        let record = Record {
            schema_version: 1,
            operation_id: self.record.operation_id,
            target,
        };
        self.file.seek(SeekFrom::Start(0))?;
        self.file.set_len(0)?;
        self.file.write_all(&canonical::to_vec(&record)?)?;
        self.file.sync_all()?;
        self.record = record;
        Ok(())
    }
    /// No Drop cleanup. Only an explicit, confirmed native completion/stop may
    /// remove this exact inode and operation; cancellation leaves it durable.
    pub(in crate::marketplace) fn clear(&mut self, lease: &ControllerLease) -> Result<()> {
        let _guard = lease.activity_guard()?;
        http::require_absent(lease)?;
        require_gateway_absent(lease)?;
        if matches!(self.record.target,Target::ImageImport{..}) {bail!("active image import cannot be cleared without joined retirement");}
        if let Target::ImageReferenceAdopted{intent}=&self.record.target {
            #[cfg(unix)]
            crate::marketplace::quarantine::image_reference::require_active(lease,self.record.operation_id,intent)?;
            #[cfg(not(unix))]
            bail!("image reference handoff requires a supported local host");
        }
        if matches!(self.record.target,Target::ImageImportRetired{..}) {
            #[cfg(unix)]
            crate::marketplace::quarantine::image_import::require_control_absent(lease)?;
            #[cfg(not(unix))]
            bail!("image import retirement requires a supported local host");
        }
        self.require_same_file(lease)?;
        if read(&mut self.file)? != self.record {
            bail!("native activity record changed");
        }
        fs::remove_file(lease.directory.join(FILE_NAME))?;
        File::open(&lease.directory)?.sync_all()?;
        Ok(())
    }
    fn require_same_file(&self, lease: &ControllerLease) -> Result<()> {
        if !lease.matches_directory(&lease.directory) {
            bail!("native activity lost its controller lease");
        }
        let path = fs::symlink_metadata(lease.directory.join(FILE_NAME))?;
        let held = self.file.metadata()?;
        private_file(&path)?;
        private_file(&held)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if path.dev() != held.dev() || path.ino() != held.ino() {
                bail!("native activity inode was replaced");
            }
        }
        Ok(())
    }
}
fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    options
}
fn private_file(metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_file() || metadata.len() > MAX_BYTES as u64 {
        bail!("invalid bounded native activity file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
            || metadata.mode() & 0o077 != 0
        {
            bail!("native activity must be private and unshared");
        }
    }
    Ok(())
}
fn read(file: &mut File) -> Result<Record> {
    private_file(&file.metadata()?)?;
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    let record: Record =
        canonical::decode(&bytes, MAX_BYTES).context("native activity record is corrupt")?;
    if record.schema_version != 1
        || record.operation_id.is_nil()
        || canonical::to_vec(&record)? != bytes
    {
        bail!("invalid canonical native activity record");
    }
    record.target.validate()?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_bound_creation_is_distinct_bounded_and_binds_once() -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("creation");
        let lease = ControllerLease::acquire(&directory)?;
        let intent = serde_json::from_value(
            serde_json::json!({"deployment":"first","fingerprint":"a".repeat(64)}),
        )?;
        let mut fence = Fence::begin(&lease, Target::WriterCreateIntent { intent })?;
        let bytes = fs::read(directory.join(FILE_NAME))?;
        assert!(bytes.len() <= MAX_BYTES);
        assert!(matches!(
            Fence::load(&lease)?.unwrap().target(),
            Target::WriterCreateIntent { .. }
        ));
        assert!(fence.bind_measurement(&lease, &"b".repeat(64)).is_err());
        assert!(fence.bind_writer(&lease, "mutable-name").is_err());
        fence.bind_writer(&lease, &"b".repeat(64))?;
        assert!(fence.bind_writer(&lease, &"c".repeat(64)).is_err());
        fence.clear(&lease)?;
        Ok(())
    }

    #[test]
    fn measurement_identity_binding_cannot_cross_roles_rebind_or_accept_mutable_names() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("measurement");
        let lease = ControllerLease::acquire(&directory)?;
        let mut fence = Fence::begin(
            &lease,
            Target::Measurement {
                deployment: "first".into(),
                container_id: None,
            },
        )?;
        let original = fs::read(directory.join(FILE_NAME))?;
        assert!(fence.bind_writer(&lease, &"a".repeat(64)).is_err());
        assert!(fence.bind_measurement(&lease, "mutable-name").is_err());
        assert_eq!(fs::read(directory.join(FILE_NAME))?, original);
        fence.bind_measurement(&lease, &"a".repeat(64))?;
        assert!(fence.bind_measurement(&lease, &"b".repeat(64)).is_err());
        assert!(
            matches!(Fence::load(&lease)?.unwrap().target(), Target::Measurement { container_id: Some(id), .. } if id == &"a".repeat(64))
        );
        fence.clear(&lease)?;
        let mut writer = Fence::begin(
            &lease,
            Target::WriterCreate {
                deployment: "first".into(),
            },
        )?;
        assert!(writer.bind_measurement(&lease, &"a".repeat(64)).is_err());
        writer.clear(&lease)?;
        Ok(())
    }

    #[test]
    fn create_only_fence_is_distinct_from_legacy_unknown_start_and_binds_durably() -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let lease = ControllerLease::acquire(&directory)?;
        let mut fence = Fence::begin(
            &lease,
            Target::WriterCreate {
                deployment: "first".into(),
            },
        )?;
        let original = fs::read(directory.join(FILE_NAME))?;
        assert!(matches!(
            Fence::load(&lease)?.unwrap().target(),
            Target::WriterCreate { .. }
        ));
        assert!(fence.bind_writer(&lease, "mutable-name").is_err());
        assert_eq!(fs::read(directory.join(FILE_NAME))?, original);
        fence.bind_writer(&lease, &"a".repeat(64))?;
        let loaded = Fence::load(&lease)?.unwrap();
        assert!(
            matches!(loaded.target(), Target::SourceWriter { container_id: Some(id), .. } if id == &"a".repeat(64))
        );
        assert!(fence.bind_writer(&lease, &"b".repeat(64)).is_err());
        drop(loaded);
        fence.clear(&lease)?;
        Ok(())
    }

    #[test]
    fn interrupted_activity_blocks_other_owners_until_exact_confirmed_cleanup() -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let lease = ControllerLease::acquire(&directory)?;
        let mut fence = Fence::begin(
            &lease,
            Target::SourceWriter {
                deployment: "first".into(),
                container_id: None,
            },
        )?;
        fence.bind_writer(&lease, &"a".repeat(64))?;
        drop(fence);
        drop(lease);
        assert!(ControllerLease::acquire(&directory).is_err());
        let lease = ControllerLease::acquire_for_recovery(&directory)?;
        let mut fence = Fence::load(&lease)?.unwrap();
        assert!(matches!(
            fence.target(),
            Target::SourceWriter {
                container_id: Some(_),
                ..
            }
        ));
        fence.clear(&lease)?;
        drop(lease);
        drop(ControllerLease::acquire(&directory)?);
        Ok(())
    }

    #[tokio::test]
    async fn native_fence_survives_controller_process_exit_without_drop_cleanup() -> Result<()> {
        const CHILD: &str = "ELIXIR_MARKETPLACE_NATIVE_FENCE_PROCESS_FIXTURE";
        if let Some(directory) = std::env::var_os(CHILD) {
            let lease = ControllerLease::acquire(std::path::Path::new(&directory))?;
            let mut fence = Fence::begin(
                &lease,
                Target::SourceWriter {
                    deployment: "crashed".into(),
                    container_id: None,
                },
            )?;
            fence.bind_writer(&lease, &"c".repeat(64))?;
            std::process::exit(0);
        }
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let status = tokio::time::timeout(std::time::Duration::from_secs(30),
            tokio::process::Command::new(std::env::current_exe()?)
                .args(["marketplace::lease::activity::tests::native_fence_survives_controller_process_exit_without_drop_cleanup", "--exact", "--nocapture"])
                .env(CHILD, &directory).kill_on_drop(true).status()
        ).await??;
        assert!(status.success());
        assert!(ControllerLease::acquire(&directory).is_err());
        let lease = ControllerLease::acquire_for_recovery(&directory)?;
        let fence = Fence::load(&lease)?.context("process exit lost its native fence")?;
        assert!(
            matches!(fence.target(), Target::SourceWriter { deployment, container_id: Some(id) } if deployment == "crashed" && id == &"c".repeat(64))
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn replacement_symlinks_hardlinks_and_corrupt_records_cannot_clear_the_fence() -> Result<()> {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let lease = ControllerLease::acquire(&directory)?;
        let mut fence = Fence::begin(
            &lease,
            Target::SourceProvisioner {
                deployment: "first".into(),
            },
        )?;
        let path = directory.join(FILE_NAME);
        fs::hard_link(&path, directory.join("alias"))?;
        assert!(fence.clear(&lease).is_err());
        fs::remove_file(directory.join("alias"))?;
        fs::rename(&path, directory.join("retired"))?;
        symlink(directory.join("retired"), &path)?;
        assert!(fence.clear(&lease).is_err());
        assert!(Fence::load(&lease).is_err());
        fs::remove_file(&path)?;
        let other = Fence::begin(
            &lease,
            Target::SourceProvisioner {
                deployment: "second".into(),
            },
        )?;
        assert!(fence.clear(&lease).is_err());
        drop(other);
        fs::write(&path, b"corrupt")?;
        assert!(Fence::load(&lease).is_err());
        drop(lease);
        assert!(ControllerLease::acquire(&directory).is_err());
        Ok(())
    }
}
