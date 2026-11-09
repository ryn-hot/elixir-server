//! One bounded passive image-reference owner per original source lane. This is
//! a cleanup journal, NOT persisted build verification or execution authority.
use super::*;
use elixir_marketplace::container_image::{
    import::{ImportIntent, ImportRequest},
    job::ContainerImageSummary,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};
use uuid::Uuid;

pub(in crate::marketplace) const FILE: &str = "image-reference.pending";
const NEXT: &str = "image-reference.next";
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    operation_id: Uuid,
    target: Kind,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Kind {
    ImageImport { intent: ImportIntent },
    ImageImportRetired { intent: ImportIntent },
}
impl Record {
    fn intent(&self) -> &ImportIntent {
        match &self.target {
            Kind::ImageImport { intent } | Kind::ImageImportRetired { intent } => intent,
        }
    }
    fn retired(&self) -> bool {
        matches!(self.target, Kind::ImageImportRetired { .. })
    }
    fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = canonical::decode(bytes, 4096)?;
        value.intent().validate()?;
        if value.schema_version != 1
            || value.operation_id.is_nil()
            || canonical::to_vec(&value)? != bytes
        {
            bail!("invalid bounded image reference record");
        }
        Ok(value)
    }
}
fn absent(lease: &ControllerLease, name: &str) -> Result<bool> {
    if !lease.matches_directory(lease.directory()) {
        bail!("image reference lost its original lane");
    }
    match fs::symlink_metadata(lease.directory().join(name)) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Ok(_) => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn private(file: &File) -> Result<()> {
    let m = file.metadata()?;
    if !m.is_file()
        || m.len() > 4096
        || m.nlink() != 1
        || m.uid() != unsafe { libc::geteuid() }
        || m.mode() & 0o077 != 0
    {
        bail!("image reference must be bounded private and unshared");
    }
    Ok(())
}
fn read(lease: &ControllerLease, name: &str) -> Result<(File, Record)> {
    if !lease.matches_directory(lease.directory()) || !matches!(name, FILE | NEXT) {
        bail!("invalid image reference role/lane");
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(lease.directory().join(name))?;
    private(&file)?;
    let mut bytes = vec![];
    Read::by_ref(&mut file).take(4097).read_to_end(&mut bytes)?;
    Ok((file, Record::parse(&bytes)?))
}
fn write_new(lease: &ControllerLease, name: &str, record: &Record) -> Result<()> {
    if !lease.matches_directory(lease.directory()) || !matches!(name, FILE | NEXT) {
        bail!("invalid image reference write role/lane");
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(lease.directory().join(name))?;
    file.write_all(&canonical::to_vec(record)?)?;
    file.sync_all()?;
    File::open(lease.directory())?.sync_all()?;
    Ok(())
}
pub(in crate::marketplace) fn require_absent(lease: &ControllerLease) -> Result<()> {
    if !absent(lease, FILE)? || !absent(lease, NEXT)? {
        bail!("original passive image reference still requires retirement");
    }
    Ok(())
}
pub(in crate::marketplace) fn require_active(
    lease: &ControllerLease,
    operation: Uuid,
    intent: &ImportIntent,
) -> Result<()> {
    if !absent(lease, NEXT)? {
        bail!("image reference is entering retirement");
    }
    let (_, record) = read(lease, FILE)?;
    if record.retired() || record.operation_id != operation || record.intent() != intent {
        bail!("image reference does not match its original import");
    }
    Ok(())
}
pub(super) fn prepare(lease: &ControllerLease, fence: &Fence) -> Result<()> {
    require_absent(lease)?;
    let record = Record::parse(&fence.current_bytes(lease)?)?;
    if record.retired() {
        bail!("a retired import cannot adopt a runtime image");
    }
    write_new(lease, FILE, &record)
}
fn settle_next(lease: &ControllerLease) -> Result<()> {
    if absent(lease, NEXT)? {
        return Ok(());
    }
    let (_, next) = read(lease, NEXT)?;
    if !next.retired() {
        bail!("image reference transition is not a confirmed retirement");
    }
    if !absent(lease, FILE)? {
        let (_, current) = read(lease, FILE)?;
        if current.operation_id != next.operation_id || current.intent() != next.intent() {
            bail!("foreign image reference retirement transition");
        }
    }
    fs::rename(lease.directory().join(NEXT), lease.directory().join(FILE))?;
    File::open(lease.directory())?.sync_all()?;
    Ok(())
}
fn mark_retired(lease: &ControllerLease, record: &Record) -> Result<()> {
    if record.retired() {
        return Ok(());
    }
    let retired = Record {
        schema_version: 1,
        operation_id: record.operation_id,
        target: Kind::ImageImportRetired {
            intent: record.intent().clone(),
        },
    };
    write_new(lease, NEXT, &retired)?;
    settle_next(lease)
}
/// Crash between copying the active import journal and its root handoff: the
/// old root still retires the image. Mirror that confirmed terminal state BEFORE
/// old request/result files can be removed; no second daemon load/cleanup.
pub(super) fn mirror_retired(
    lease: &ControllerLease,
    operation: Uuid,
    intent: &ImportIntent,
) -> Result<()> {
    settle_next(lease)?;
    if absent(lease, FILE)? {
        return Ok(());
    }
    let (_, record) = read(lease, FILE)?;
    if record.operation_id != operation || record.intent() != intent {
        bail!("unrelated passive image reference");
    }
    mark_retired(lease, &record)
}
/// Native actors must already be retired. This only releases a passive local
/// image reference, never force-deletes a shared CID or manufactures a grant.
pub(in crate::marketplace) async fn recover(
    runtime: &dyn RuntimeManager,
    lease: &ControllerLease,
) -> Result<bool> {
    if absent(lease, FILE)? && absent(lease, NEXT)? {
        return Ok(false);
    }
    if Fence::load(lease)?.is_some() {
        bail!("retire original native actor before its image reference");
    }
    settle_next(lease)?;
    let (_, record) = read(lease, FILE)?;
    if !record.retired() {
        match fs::symlink_metadata(lease.directory().join("image-import.retired")) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let profile = runtime
                    .marketplace_import_profile()
                    .context("image reference requires its trusted original host profile")?;
                let mut child = profile.spawn_reference_cleanup(lease.directory())?;
                if !child.wait().await?.success() {
                    bail!("passive image reference cleanup is unconfirmed");
                }
            }
            Err(error) => return Err(error.into()),
            Ok(_) => (),
        }
        let _keeper = super::image_import::keeper_lock(lease)?;
        if super::image_import::read_control(lease, "image-import.retired", 4096)?
            != canonical::to_vec(&record)?
        {
            bail!("image reference cleanup receipt changed identity");
        }
        mark_retired(lease, &record)?;
    }
    let (_, retired) = read(lease, FILE)?;
    if !retired.retired() {
        bail!("image reference has no durable terminal state");
    }
    super::image_import::clear_controls(lease, retired.operation_id, retired.intent())?;
    fs::remove_file(lease.directory().join(FILE))?;
    File::open(lease.directory())?.sync_all()?;
    Ok(true)
}

/// Actual in-memory import provenance plus an exact held passive-reference
/// inode. No Clone/Deserialize; reopening the cleanup JSON cannot construct it.
pub(in crate::marketplace) struct RuntimeImage {
    owner: evidence_owner::EvidenceOwner,
    file: File,
    record: Record,
    image: ContainerImageSummary,
}
impl RuntimeImage {
    pub(super) fn capture(
        source: &SourceMatchedQuarantine,
        imported: &super::image_import::ImportedImage,
        request: &ImportRequest,
    ) -> Result<Self> {
        imported.require_source(source)?;
        if imported.config() != request.filesystem.image.config
            || imported.request() != request.intent()?.request
        {
            bail!("runtime image differs from actual import provenance");
        }
        let lease = source.job_lease();
        let (file, record) = read(&lease, FILE)?;
        if record.intent() != &request.intent()?
            || record.retired()
            || source.source.job.activity.as_ref().map(Fence::operation_id)
                != Some(record.operation_id)
        {
            bail!("runtime image reference is not the active import");
        }
        Ok(Self {
            owner: evidence_owner::EvidenceOwner::capture(source)?,
            file,
            record,
            image: request.filesystem.image.clone(),
        })
    }
    pub(in crate::marketplace) fn require_source(
        &self,
        source: &SourceMatchedQuarantine,
    ) -> Result<()> {
        self.owner.require(source)?;
        source.require_compiled_source()?;
        source.require_compiled_image(&self.image)?;
        let lease = source.job_lease();
        require_active(&lease, self.record.operation_id, self.record.intent())?;
        private(&self.file)?;
        let (actual, record) = read(&lease, FILE)?;
        let held = self.file.metadata()?;
        let named = actual.metadata()?;
        if held.dev() != named.dev() || held.ino() != named.ino() || record != self.record {
            bail!("runtime image reference inode/identity changed");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn image(&self) -> &ContainerImageSummary {
        &self.image
    }
    pub(in crate::marketplace) fn runtime_view(
        &self,
        source: &SourceMatchedQuarantine,
    ) -> Result<RuntimeImageView> {
        self.require_source(source)?;
        Ok(RuntimeImageView {
            owner: evidence_owner::EvidenceOwner::capture(source)?,
            file: self.file.try_clone()?,
            record: self.record.clone(),
            image: self.image.clone(),
            source: Digest::of(canonical::to_vec(&source.execution_descriptor())?),
        })
    }
}

/// A live private view for the SAME invocation's SDK checks. No Clone,
/// Deserialize or constructor from disk/summary; it cannot keep a job alive.
pub(in crate::marketplace) struct RuntimeImageView {
    owner: evidence_owner::EvidenceOwner,
    file: File,
    record: Record,
    image: ContainerImageSummary,
    source: Digest,
}
impl RuntimeImageView {
    pub(in crate::marketplace) fn check(&self) -> Result<()> {
        let lease = self.owner.current()?;
        require_active(&lease, self.record.operation_id, self.record.intent())?;
        private(&self.file)?;
        let (named, record) = read(&lease, FILE)?;
        let held = self.file.metadata()?;
        let actual = named.metadata()?;
        if held.dev() != actual.dev() || held.ino() != actual.ino() || record != self.record {
            bail!("compiled invocation lost its actual imported-image reference");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn require_descriptor(
        &self,
        descriptor: &elixir_marketplace::container_execution::ContainerExecutionDescriptor,
    ) -> Result<()> {
        self.check()?;
        descriptor.validate()?;
        if self.source != Digest::of(canonical::to_vec(&descriptor.source)?)
            || canonical::to_vec(&self.image)? != canonical::to_vec(&descriptor.image)?
        {
            bail!("compiled invocation differs from its private source/image provenance");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
