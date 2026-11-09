//! Private immutable source archives, not installed/executable/shared releases.
//! Only the trusted controller and a live grant-checked quarantine can commit.
//! Persisted descriptors are data: every reopen rehashes their fixed blob slots.
//! No ZIP/Git parser, extraction, peer pathname or executable runs on this host.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    canonical,
    identity::Digest,
    quarantine::{QuarantinePurpose, QuarantineWorkspace},
    records::{Blob, Release},
    security::AuthorizedRelease,
    source_job::{SourceMatchDescriptor, SourceMatchSummary},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::{
    io::AsyncWrite,
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use super::{grant_store::GrantCheckedQuarantine, lease::ControllerLease};
use crate::{db::Database, runtime::RuntimeManager};
use super::quarantine::local_build::LocalBuildRecord;

const MIB: u64 = 1024 * 1024;
const MAX_DESCRIPTOR: usize = 64 * 1024;
const ROOT_RESERVATION: u64 = MIB;
const PENDING: &str = ".pending";
const DESCRIPTOR: &str = "descriptor.json";
mod owned;
pub(super) use owned::Copy;
mod build_inputs;
mod image_export;
pub(in crate::marketplace) use image_export::ImageExportStaging;
mod replay;
pub(in crate::marketplace) use build_inputs::BuildInputs;
mod sharing;
pub(super) use replay::{Reader, Replay};
pub(in crate::marketplace) use sharing::SharingReader;

/// Explicit owner storage allocation, separate from automatic-worker storage.
/// A release-count limit also bounds directory scans and tiny-file overhead.
#[derive(Debug, Clone, Copy)]
pub struct ArchiveBudget {
    pub bytes: u64,
    pub releases: usize,
}
impl ArchiveBudget {
    pub(super) fn validate(self) -> Result<()> {
        if self.bytes <= ROOT_RESERVATION || !(1..=4096).contains(&self.releases) {
            bail!("source archive requires positive bounded owner storage and release limits");
        }
        Ok(())
    }
    pub(super) fn metadata_read_allowance(self) -> Result<u64> {
        self.validate()?;
        Ok((self.releases as u64 + 3) * (MAX_DESCRIPTOR as u64 + 1))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    schema_version: u32,
    workspace: QuarantineWorkspace,
    release: Release,
    source: SourceMatchSummary,
}
impl Descriptor {
    fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = canonical::decode(bytes, MAX_DESCRIPTOR)?;
        value.validate()?;
        if canonical::to_vec(&value)? != bytes {
            bail!("noncanonical source archive descriptor");
        }
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        if self.schema_version != 1 || canonical::to_vec(self)?.len() > MAX_DESCRIPTOR {
            bail!("invalid source archive descriptor");
        }
        self.source.matches(&SourceMatchDescriptor {
            schema_version: 1,
            workspace: self.workspace.clone(),
            release: self.release.clone(),
        })?;
        self.source.manifest()?.bind_release(&self.release)?;
        Ok(())
    }
    fn reservation(&self) -> Result<u64> {
        self.validate()?;
        // Includes physical filesystem/descriptor/directory headroom, not merely
        // package lengths. Source and destination coexist until durable commit.
        Ok(self.workspace.volume_reservation_bytes()? + MIB)
    }
    fn name(&self) -> String {
        self.workspace.release_id.to_string()[7..].to_owned()
    }
}

/// Integrity-checked archive information, never installation/runner authority.
/// No public filesystem path, deserializer or conversion to a runtime grant.
pub struct ArchivedSource {
    descriptor: Descriptor,
    recovered_incomplete_copy: bool,
    // Local verification packet for the subsequent journal commit only. Never
    // part of the immutable/shareable archive descriptor: randomized encryption
    // must not change byte identity or break idempotent archive retries.
    build: Option<LocalBuildRecord>,
}
impl ArchivedSource {
    pub(super) fn build_record(&self) -> Option<&LocalBuildRecord> {
        self.build.as_ref()
    }
    pub fn release_id(&self) -> Digest {
        self.descriptor.workspace.release_id
    }
    pub fn recovered_incomplete_copy(&self) -> bool {
        self.recovered_incomplete_copy
    }
    /// Copy only authenticated metadata from this actual rehashed archive.
    /// The serialized record alone never proves that disk bytes still match.
    pub(super) fn record_for(&self, release: &AuthorizedRelease) -> Result<RetainedSourceRecord> {
        if canonical::to_vec(&self.descriptor.release)? != canonical::to_vec(release.payload())? {
            bail!("retained source differs from signed installation release");
        }
        let record = RetainedSourceRecord {
            workspace: self.descriptor.workspace.clone(),
            source: self.descriptor.source.clone(),
        };
        record.review(release)?;
        Ok(record)
    }
}

/// Persisted source identity, not a byte-verification or activation token.
/// Fresh archive reads and runtime admission remain mandatory before execution.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedSourceRecord {
    workspace: QuarantineWorkspace,
    source: SourceMatchSummary,
}
impl RetainedSourceRecord {
    pub(super) fn descriptor(&self, release: &AuthorizedRelease) -> SourceMatchDescriptor {
        SourceMatchDescriptor {
            schema_version: 1,
            workspace: self.workspace.clone(),
            release: release.payload().clone(),
        }
    }
    pub(super) fn summary(&self) -> &SourceMatchSummary {
        &self.source
    }
    pub(super) fn id(&self) -> Result<Digest> {
        Ok(Digest::of(canonical::to_vec(self)?))
    }
    pub(super) fn review(
        &self,
        release: &AuthorizedRelease,
    ) -> Result<(
        crate::extensions::manifest::ExtensionManifest,
        super::grants::GrantReview,
    )> {
        self.source.matches(&self.descriptor(release))?;
        let (manifest, _, _) = super::manifest::check_document(
            self.source.manifest()?,
            release.payload(),
            &self.source,
            self.workspace.artifact.as_ref(),
        )?;
        let review = super::grants::build_review(
            &manifest,
            release.payload().runtime_profile,
            release.release_id(),
            release.payload().manifest_sha256,
            &release.payload().artifacts,
        )?;
        Ok((manifest, review))
    }
}

struct Root {
    path: PathBuf,
    // Every blocking I/O task owns an Arc<Root>. Cancelled async callers cannot
    // release this lock while a detached filesystem operation is still running.
    _lease: ControllerLease,
    // Persist's native source job owns the same global lane. Detached blocking
    // readers/writers must retain it until real I/O joins, not async timeout.
    _source_job: Option<Arc<ControllerLease>>,
}
struct Store {
    root: Arc<Root>,
    budget: ArchiveBudget,
    used: u64,
    count: usize,
    recovered: bool,
}
impl Store {
    fn open(path: &Path, budget: ArchiveBudget, expires: Instant) -> Result<Self> {
        Self::open_with_job(path, budget, expires, None)
    }
    fn open_with_job(
        path: &Path,
        budget: ArchiveBudget,
        expires: Instant,
        source_job: Option<Arc<ControllerLease>>,
    ) -> Result<Self> {
        Self::open_kind(path, budget, expires, source_job, false)
    }
    fn open_build_inputs(
        path: &Path,
        budget: ArchiveBudget,
        expires: Instant,
        source_job: Arc<ControllerLease>,
    ) -> Result<Self> {
        Self::open_kind(path, budget, expires, Some(source_job), true)
    }
    fn open_kind(
        path: &Path,
        budget: ArchiveBudget,
        expires: Instant,
        source_job: Option<Arc<ControllerLease>>,
        build_inputs: bool,
    ) -> Result<Self> {
        budget.validate()?;
        active(expires)?;
        let lease = ControllerLease::acquire(path)?;
        let root = Arc::new(Root {
            path: path.to_owned(),
            _lease: lease,
            _source_job: source_job,
        });
        let entries = names(path, 2)?;
        if entries
            .iter()
            .any(|name| name != "controller.lock" && name != "releases")
        {
            bail!("unexpected private archive root entry");
        }
        let releases = path.join("releases");
        if !entries.iter().any(|name| name == "releases") {
            fs::DirBuilder::new().mode(0o700).create(&releases)?;
            sync_directory(path)?;
        }
        directory(&releases, 0o700)?;
        if physical(&fs::symlink_metadata(path)?)?
            + physical(&fs::symlink_metadata(&releases)?)?
            + physical(&fs::symlink_metadata(path.join("controller.lock"))?)?
            > ROOT_RESERVATION
        {
            bail!("source archive root exceeds its physical reservation");
        }
        let mut store = Self {
            root,
            budget,
            used: ROOT_RESERVATION,
            count: 0,
            recovered: false,
        };
        let entries = names(&releases, budget.releases + 1)?;
        // Validate everything before deleting a known incomplete copy. Foreign
        // names, symlinks, hardlinks and over-limit state fail without repair.
        for name in &entries {
            active(expires)?;
            if build_inputs && name != PENDING {
                bail!("build-input staging must never contain a committed release");
            }
            if name == PENDING {
                let files = validate_pending(&releases.join(PENDING))?;
                if build_inputs
                    && files
                        .iter()
                        .any(|name| name == QuarantinePurpose::Artifact.file_name())
                {
                    bail!("candidate artifact is forbidden in build-input staging");
                }
            } else {
                if !release_name(name) {
                    bail!("unknown source archive release entry");
                }
                let descriptor = inspect(&releases.join(name), false, expires)?;
                if descriptor.name() != *name {
                    bail!("source archive directory identity mismatch");
                }
                store.count += 1;
                store.used = store
                    .used
                    .checked_add(descriptor.reservation()?)
                    .context("source archive quota overflow")?;
            }
        }
        if store.count > budget.releases || store.used > budget.bytes {
            bail!("existing source archive exceeds the configured owner budget");
        }
        if entries.iter().any(|name| name == PENDING) {
            discard_pending(&releases.join(PENDING))?;
            sync_directory(&releases)?;
            store.recovered = true;
        }
        Ok(store)
    }
    fn releases(&self) -> PathBuf {
        self.root.path.join("releases")
    }
    fn begin(&self, descriptor: &Descriptor, expires: Instant) -> Result<bool> {
        active(expires)?;
        let target = self.releases().join(descriptor.name());
        match fs::symlink_metadata(&target) {
            Ok(_) => {
                let existing = inspect(&target, true, expires)?;
                if canonical::to_vec(&existing)? != canonical::to_vec(descriptor)? {
                    bail!("immutable archive release already has different metadata");
                }
                return Ok(false);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if self.count >= self.budget.releases
            || self
                .used
                .checked_add(descriptor.reservation()?)
                .is_none_or(|bytes| bytes > self.budget.bytes)
        {
            bail!("owner source archive quota exhausted");
        }
        free_space(&self.root.path, descriptor.reservation()? + 64 * MIB)?;
        let pending = self.releases().join(PENDING);
        fs::DirBuilder::new().mode(0o700).create(&pending)?;
        sync_directory(&self.releases())?;
        let bytes = canonical::to_vec(descriptor)?;
        let mut metadata = create_file(&pending.join(DESCRIPTOR))?;
        metadata.write_all(&bytes)?;
        seal_file(metadata)?;
        sync_directory(&pending)?;
        Ok(true)
    }
    fn commit(&self, descriptor: &Descriptor, expires: Instant) -> Result<()> {
        active(expires)?;
        let pending = self.releases().join(PENDING);
        let found = inspect_pending_complete(&pending, descriptor)?;
        if found > descriptor.reservation()? {
            bail!("archive copy exceeds physical reservation");
        }
        let target = self.releases().join(descriptor.name());
        if fs::symlink_metadata(&target).is_ok() {
            bail!("source archive release already exists");
        }
        fs::set_permissions(&pending, fs::Permissions::from_mode(0o500))?;
        sync_directory(&pending)?;
        active(expires)?;
        fs::rename(&pending, &target)?;
        sync_directory(&self.releases())?;
        Ok(())
    }
}

/// Durable integrity archive of an already source/manifest/grant-checked job.
/// Public-source availability, dependency closure, builds, current publisher
/// authority and runtime admission remain independent mandatory install gates.
pub async fn persist(
    database: &Database,
    runtime: &dyn RuntimeManager,
    source: GrantCheckedQuarantine,
    path: &Path,
    budget: ArchiveBudget,
) -> Result<ArchivedSource> {
    let actual = source.source().source().runtime();
    if !std::ptr::addr_eq(actual.as_ref(), runtime) {
        bail!("archive runtime does not match its verified source owner");
    }
    let mut copy = Copy::begin(database.clone(), source, None, path.to_owned(), budget);
    let (_, archive) = copy.finish().await?;
    Ok(archive)
}

/// Archive the same actual fresh reproduced/imported owner, retaining its
/// machine-local proof for the installation journal. Preflight failure leaves
/// the caller's original source slot intact; the existing Copy owns all later
/// cancellation/cleanup. No saved receipt can replace either native capability.
pub(in crate::marketplace) async fn persist_compiled(
    database: &Database,
    source: &mut Option<GrantCheckedQuarantine>,
    build: &super::quarantine::build::NativeBuildMatch,
    image: &super::quarantine::image_reference::RuntimeImage,
    secrets: &crate::secrets::SecretsManager,
    path: &Path,
    budget: ArchiveBudget,
) -> Result<ArchivedSource> {
    let checked = source.as_ref().context("compiled archive source owner missing")?;
    let record = LocalBuildRecord::from_actual_build(checked.source().source(), build, image, secrets)?;
    budget.validate()?;
    tokio::runtime::Handle::try_current().context("compiled archive executor unavailable")?;
    let mut copy = Copy::begin(database.clone(), source.take().expect("borrowed source"), None, path.to_owned(), budget);
    let (_, mut archive) = copy.finish().await?;
    record.require_descriptor_data(&SourceMatchDescriptor {
        schema_version: 1, workspace: archive.descriptor.workspace.clone(), release: archive.descriptor.release.clone(),
    }, &archive.descriptor.source)?;
    archive.build = Some(record);
    Ok(archive)
}

// Rehash committed disk bytes, including after an idempotent retry or crash.
// The returned information still cannot authorize installation or public seeding.
pub async fn verify_retained(
    path: &Path,
    budget: ArchiveBudget,
    release: &AuthorizedRelease,
    job_directory: &Path,
    input_bytes: u64,
    wall_seconds: u64,
) -> Result<ArchivedSource> {
    budget.validate()?;
    if wall_seconds == 0 || wall_seconds > 8 * 60 * 60 {
        bail!("invalid archive verification deadline");
    }
    let path = path.to_owned();
    let expected = release.payload().clone();
    let id = release.release_id();
    let artifact = expected
        .artifacts
        .iter()
        .map(|artifact| artifact.blob.size)
        .max()
        .unwrap_or(0);
    let required = (budget.releases as u64 + 1) * (MAX_DESCRIPTOR as u64 + 1)
        + expected.package.size
        + expected.source.proof.size
        + artifact
        + 3;
    if input_bytes < required {
        bail!("retained source verification needs an explicit sufficient read allowance");
    }
    let job_directory = job_directory.to_owned();
    let expires = Instant::now() + std::time::Duration::from_secs(wall_seconds);
    timeout_at(
        expires,
        tokio::task::spawn_blocking(move || {
            // Same lock ordering as persist; cancellation retains this global
            // lane until blocking reads actually finish, not until timeout.
            let _job = ControllerLease::acquire(&job_directory)?;
            let store = Store::open(&path, budget, expires)?;
            let found = inspect(&store.releases().join(&id.to_string()[7..]), true, expires)?;
            if canonical::to_vec(&found.release)? != canonical::to_vec(&expected)? {
                bail!("retained archive does not match the authorized release");
            }
            Ok(ArchivedSource {
                descriptor: found,
                recovered_incomplete_copy: store.recovered,
                build: None,
            })
        }),
    )
    .await
    .context("retained source verification deadline exceeded")??
}

/// Real package/Git equality and durable archive I/O, but no native isolation,
/// public repository availability, owner consent or installed authority claim.
#[cfg(test)]
pub(in crate::marketplace) async fn fixture_from_package(
    path: &Path,
    release: &AuthorizedRelease,
    package: &[u8],
    proof: &[u8],
) -> Result<ArchivedSource> {
    let expires = Instant::now() + std::time::Duration::from_secs(30);
    let workspace = QuarantineWorkspace {
        schema_version: 1,
        release_id: release.release_id(),
        package: release.payload().package.clone(),
        proof: release.payload().source.proof.clone(),
        artifact: None,
        timeout_seconds: elixir_marketplace::quarantine::MAX_JOB_SECONDS,
    };
    let matching = SourceMatchDescriptor {
        schema_version: 1,
        workspace: workspace.clone(),
        release: release.payload().clone(),
    };
    let descriptor = Descriptor {
        schema_version: 1,
        workspace,
        release: release.payload().clone(),
        source: matching.verify(io::Cursor::new(package), io::Cursor::new(proof))?,
    };
    let budget = ArchiveBudget {
        bytes: 128 * MIB,
        releases: 4,
    };
    let store = Store::open(path, budget, expires)?;
    if store.begin(&descriptor, expires)? {
        for (purpose, bytes) in [
            (QuarantinePurpose::Package, package),
            (QuarantinePurpose::Proof, proof),
        ] {
            let mut file = create_file(&store.releases().join(PENDING).join(purpose.file_name()))?;
            file.write_all(bytes)?;
            seal_file(file)?;
        }
        store.commit(&descriptor, expires)?;
    }
    drop(store);
    verify_retained(
        path,
        budget,
        release,
        &path
            .parent()
            .context("fixture parent")?
            .join("fixture-lane"),
        MIB,
        30,
    )
    .await
}

async fn verify_committed(
    root: &Arc<Root>,
    descriptor: &Descriptor,
    expires: Instant,
) -> Result<()> {
    let root = root.clone();
    let descriptor = descriptor.clone();
    tokio::task::spawn_blocking(move || {
        let found = inspect(
            &root.path.join("releases").join(descriptor.name()),
            true,
            expires,
        )?;
        if canonical::to_vec(&found)? != canonical::to_vec(&descriptor)? {
            bail!("archive commit changed");
        }
        Ok(())
    })
    .await?
}

fn active(expires: Instant) -> Result<()> {
    if Instant::now() >= expires {
        bail!("source archive deadline exceeded");
    }
    Ok(())
}
fn release_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn names(path: &Path, maximum: usize) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(path)? {
        if result.len() == maximum {
            bail!("source archive entry limit exceeded");
        }
        result.push(
            entry?
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("non-UTF8 archive entry"))?,
        );
    }
    result.sort();
    Ok(result)
}
fn physical(metadata: &fs::Metadata) -> Result<u64> {
    metadata
        .blocks()
        .checked_mul(512)
        .context("source archive physical allocation overflow")
}
fn directory(path: &Path, mode: u32) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o7777 != mode
    {
        bail!("unsafe source archive directory");
    }
    Ok(metadata)
}
fn regular(path: &Path, maximum: u64, mode: Option<u32>) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.len() > maximum
        || !matches!(metadata.mode() & 0o7777, 0o600 | 0o400)
        || mode.is_some_and(|mode| metadata.mode() & 0o7777 != mode)
    {
        bail!("unsafe source archive file");
    }
    Ok(metadata)
}
fn open_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?)
}
fn create_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?)
}
fn seal_file(file: File) -> Result<()> {
    file.set_permissions(fs::Permissions::from_mode(0o400))?;
    file.sync_all()?;
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } != 0 {
            return Err(io::Error::last_os_error()).context("source archive full fsync");
        }
    }
    Ok(())
}
fn sync_directory(path: &Path) -> Result<()> {
    open_file(path)?.sync_all()?;
    Ok(())
}
fn free_space(path: &Path, required: u64) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut data = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), data.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let data = unsafe { data.assume_init() };
    if (data.f_bavail as u64)
        .checked_mul(data.f_frsize as u64)
        .is_none_or(|available| available < required)
    {
        bail!("insufficient source archive free-space headroom");
    }
    Ok(())
}
fn read_descriptor(path: &Path) -> Result<Descriptor> {
    regular(path, MAX_DESCRIPTOR as u64, Some(0o400))?;
    let mut bytes = Vec::new();
    open_file(path)?
        .take(MAX_DESCRIPTOR as u64 + 1)
        .read_to_end(&mut bytes)?;
    Descriptor::parse(&bytes)
}
fn expected_names(descriptor: &Descriptor) -> Result<Vec<String>> {
    let mut expected = vec![DESCRIPTOR.to_owned()];
    expected.extend(
        descriptor
            .workspace
            .descriptors()?
            .iter()
            .map(|slot| slot.purpose.file_name().to_owned()),
    );
    expected.sort();
    Ok(expected)
}
fn inspect(path: &Path, hash: bool, expires: Instant) -> Result<Descriptor> {
    active(expires)?;
    directory(path, 0o500)?;
    let descriptor = read_descriptor(&path.join(DESCRIPTOR))?;
    if names(path, 4)? != expected_names(&descriptor)? {
        bail!("unexpected source archive files");
    }
    let mut allocated = physical(&fs::symlink_metadata(path)?)?
        + physical(&fs::symlink_metadata(path.join(DESCRIPTOR))?)?;
    for slot in descriptor.workspace.descriptors()? {
        active(expires)?;
        let file = path.join(slot.purpose.file_name());
        let metadata = regular(&file, slot.blob.size, Some(0o400))?;
        if metadata.len() != slot.blob.size {
            bail!("source archive blob length mismatch");
        }
        allocated = allocated
            .checked_add(physical(&metadata)?)
            .context("source archive allocation overflow")?;
        if hash {
            hash_file(&file, &slot.blob, expires)?;
        }
    }
    if allocated > descriptor.reservation()? {
        bail!("source archive physical reservation exceeded");
    }
    Ok(descriptor)
}
fn hash_file(path: &Path, expected: &Blob, expires: Instant) -> Result<()> {
    let mut file = open_file(path)?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 64 * 1024];
    let mut total = 0;
    loop {
        active(expires)?;
        let maximum = (expected.size - total + 1).min(bytes.len() as u64) as usize;
        let size = file.read(&mut bytes[..maximum])?;
        if size == 0 {
            break;
        }
        total += size as u64;
        if total > expected.size {
            bail!("source archive grew during verification");
        }
        hash.update(&bytes[..size]);
    }
    if total != expected.size || Digest::from_bytes(hash.finalize().into()) != expected.sha256 {
        bail!("source archive content hash mismatch");
    }
    Ok(())
}
fn validate_pending(path: &Path) -> Result<Vec<String>> {
    let metadata = fs::symlink_metadata(path)?;
    if !matches!(metadata.mode() & 0o7777, 0o700 | 0o500) {
        bail!("unsafe pending source archive");
    }
    directory(path, metadata.mode() & 0o7777)?;
    let entries = names(path, 4)?;
    for name in &entries {
        let maximum = match name.as_str() {
            DESCRIPTOR => MAX_DESCRIPTOR as u64,
            "package.blob" => QuarantinePurpose::Package.maximum(),
            "proof.blob" => QuarantinePurpose::Proof.maximum(),
            "artifact.blob" => QuarantinePurpose::Artifact.maximum(),
            _ => bail!("unexpected incomplete source archive entry"),
        };
        let metadata = regular(&path.join(name), maximum, None)?;
        if physical(&metadata)? > maximum + 16 * MIB {
            bail!("unsafe pending source allocation");
        }
    }
    Ok(entries)
}
fn discard_pending(path: &Path) -> Result<()> {
    let entries = validate_pending(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    for entry in entries {
        fs::remove_file(path.join(entry))?;
    }
    fs::remove_dir(path)?;
    Ok(())
}
fn inspect_pending_complete(path: &Path, descriptor: &Descriptor) -> Result<u64> {
    directory(path, 0o700)?;
    if names(path, 4)? != expected_names(descriptor)?
        || canonical::to_vec(&read_descriptor(&path.join(DESCRIPTOR))?)?
            != canonical::to_vec(descriptor)?
    {
        bail!("incomplete source archive copy");
    }
    let mut allocated = physical(&fs::symlink_metadata(path)?)?
        + physical(&fs::symlink_metadata(path.join(DESCRIPTOR))?)?;
    for slot in descriptor.workspace.descriptors()? {
        let metadata = regular(
            &path.join(slot.purpose.file_name()),
            slot.blob.size,
            Some(0o400),
        )?;
        if metadata.len() != slot.blob.size {
            bail!("incomplete source archive blob");
        }
        allocated += physical(&metadata)?;
    }
    Ok(allocated)
}

/// Blocking writes never run on the async executor and retain the archive OS
/// lease even if their caller is cancelled. One <=64 KiB write may be in flight.
struct ArchiveSink {
    file: Option<File>,
    pending: Option<JoinHandle<(File, io::Result<usize>)>>,
    root: Arc<Root>,
    remaining: u64,
    expires: Instant,
}
impl ArchiveSink {
    fn new(file: File, size: u64, root: Arc<Root>, expires: Instant) -> Self {
        Self {
            file: Some(file),
            pending: None,
            root,
            remaining: size,
            expires,
        }
    }
    async fn finish(mut self) -> Result<()> {
        if self.remaining != 0 || self.pending.is_some() {
            bail!("incomplete source archive sink");
        }
        let file = self
            .file
            .take()
            .context("source archive sink lost its file")?;
        let root = self.root.clone();
        let expires = self.expires;
        tokio::task::spawn_blocking(move || {
            let _held = root;
            active(expires)?;
            seal_file(file)
        })
        .await?
    }
    /// Join a write after a cancelled native export. Never drop this handle
    /// and call cleanup complete while the blocking write is still running.
    async fn drain(&mut self) -> Result<()> {
        if let Some(task) = self.pending.as_mut() {
            let (file, result) = task.await.context("archive write task stopped")?;
            self.pending = None;
            self.file = Some(file);
            let size = result?;
            self.remaining = self
                .remaining
                .checked_sub(size as u64)
                .context("archive write count changed")?;
        }
        Ok(())
    }
}
impl AsyncWrite for ArchiveSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(task) = self.pending.as_mut() {
            let (file, result) = match Pin::new(task).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(result)) => result,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
            };
            self.file = Some(file);
            self.pending = None;
            if let Ok(size) = result {
                self.remaining -= size as u64;
            }
            return Poll::Ready(result);
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if Instant::now() >= self.expires || self.remaining == 0 {
            return Poll::Ready(Err(io::Error::other(
                "source archive write budget exhausted",
            )));
        }
        let size = bytes.len().min(64 * 1024).min(self.remaining as usize);
        let bytes = bytes[..size].to_vec();
        let Some(mut file) = self.file.take() else {
            return Poll::Ready(Err(io::Error::other("source archive file unavailable")));
        };
        let root = self.root.clone();
        let expires = self.expires;
        self.pending = Some(tokio::task::spawn_blocking(move || {
            let _held = root;
            let result = if Instant::now() >= expires {
                Err(io::Error::other("source archive write expired"))
            } else {
                file.write_all(&bytes).map(|_| size)
            };
            (file, result)
        }));
        self.poll_write(cx, &[])
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        if self.pending.is_some() {
            Poll::Ready(Err(io::Error::other("source archive write still pending")))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use elixir_marketplace::{
        records::{Publisher, SignedRecord},
        security::AuthorizedPublisher,
    };
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt;

    pub(super) fn expires() -> Instant {
        Instant::now() + std::time::Duration::from_secs(30)
    }
    pub(super) fn budget() -> ArchiveBudget {
        ArchiveBudget {
            bytes: 128 * MIB,
            releases: 2,
        }
    }
    #[tokio::test]
    async fn cancelled_archive_drain_retains_its_write_and_joins_before_releasing_locks()
    -> Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = parent.path().canonicalize()?;
        let lane = directory.join("foreground");
        let source_job = Arc::new(ControllerLease::acquire(&lane)?);
        let store = Store::open_with_job(
            &directory.join("archive"),
            budget(),
            expires(),
            Some(source_job.clone()),
        )?;
        drop(source_job);
        let mut sink = ArchiveSink::new(
            create_file(&directory.join("chunk"))?,
            2,
            store.root.clone(),
            expires(),
        );
        let mut file = sink.file.take().unwrap();
        let held = store.root.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let (finish, finishing) = std::sync::mpsc::sync_channel(1);
        sink.pending = Some(tokio::task::spawn_blocking(move || {
            let _held = held;
            let _ = ready.send(());
            finishing.recv().unwrap();
            let result = file.write_all(&[1, 2]).map(|()| 2);
            (file, result)
        }));
        started.await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), sink.drain())
                .await
                .is_err()
        );
        assert!(ControllerLease::acquire(&lane).is_err());
        finish.send(())?;
        sink.drain().await?;
        assert_eq!(sink.remaining, 0);
        assert_eq!(std::fs::read(directory.join("chunk"))?, vec![1, 2]);
        drop(sink);
        drop(store);
        assert!(ControllerLease::acquire(&lane).is_ok());
        Ok(())
    }
    #[tokio::test]
    async fn cancelled_blocking_archive_work_keeps_the_source_jobs_global_lane_until_actual_completion()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?;
        let job_directory = directory.join("expensive");
        let source_job = Arc::new(ControllerLease::acquire(&job_directory)?);
        let store = Store::open_with_job(
            &directory.join("archive"),
            budget(),
            expires(),
            Some(source_job.clone()),
        )?;
        let held = store.root.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let (finish, wait) = std::sync::mpsc::sync_channel(1);
        let (complete, completed) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _ = ready.send(());
            wait.recv().unwrap();
            drop(held);
            let _ = complete.send(());
        });
        started.await?;
        drop(task); // dropping an async JoinHandle does not stop blocking I/O
        drop(store);
        drop(source_job);
        assert!(ControllerLease::acquire(&job_directory).is_err());
        finish.send(())?;
        completed.await?;
        drop(ControllerLease::acquire(&job_directory)?);
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_native_work_blocks_retained_archive_reads_before_opening_a_second_store()
    -> Result<()> {
        use super::super::lease::activity::{Fence, Target};
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?;
        let job_directory = directory.join("expensive");
        let lease = ControllerLease::acquire(&job_directory)?;
        let fence = Fence::begin(
            &lease,
            Target::SourceProvisioner {
                deployment: "interrupted".into(),
            },
        )?;
        drop(fence);
        drop(lease);
        let (release, _, _, _) = fixture()?;
        let archive = directory.join("archive");
        let result = verify_retained(&archive, budget(), &release, &job_directory, MIB, 10).await;
        let error = result
            .err()
            .context("retained archive skipped the native fence")?;
        assert!(format!("{error:#}").contains("interrupted native marketplace work"));
        assert!(
            !archive.exists(),
            "the rejected job must not create store state"
        );
        Ok(())
    }
    pub(super) fn fixture() -> Result<(AuthorizedRelease, Descriptor, Vec<u8>, Vec<u8>)> {
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))?;
        let publisher =
            SignedRecord::<Publisher>::parse(&serde_json::to_vec(&value["publisher"])?)?;
        let publisher = AuthorizedPublisher::verify(&publisher)?;
        let signed = SignedRecord::<Release>::parse(&serde_json::to_vec(&value["release"])?)?;
        let authorized = AuthorizedRelease::verify(&signed, &publisher, &publisher)?;
        let package = base64::engine::general_purpose::STANDARD
            .decode(value["package_base64"].as_str().context("package")?)?;
        let proof = base64::engine::general_purpose::STANDARD
            .decode(value["source_proof_base64"].as_str().context("proof")?)?;
        let workspace = QuarantineWorkspace {
            schema_version: 1,
            release_id: authorized.release_id(),
            package: authorized.payload().package.clone(),
            proof: authorized.payload().source.proof.clone(),
            artifact: None,
            timeout_seconds: elixir_marketplace::quarantine::MAX_JOB_SECONDS,
        };
        let matching = SourceMatchDescriptor {
            schema_version: 1,
            workspace: workspace.clone(),
            release: authorized.payload().clone(),
        };
        let source = matching.verify(Cursor::new(&package), Cursor::new(&proof))?;
        let descriptor = Descriptor {
            schema_version: 1,
            workspace,
            release: authorized.payload().clone(),
            source,
        };
        descriptor.validate()?;
        Ok((authorized, descriptor, package, proof))
    }
    pub(super) fn populate(
        store: &Store,
        descriptor: &Descriptor,
        package: &[u8],
        proof: &[u8],
    ) -> Result<()> {
        assert!(store.begin(descriptor, expires())?);
        for (purpose, bytes) in [
            (QuarantinePurpose::Package, package),
            (QuarantinePurpose::Proof, proof),
        ] {
            let mut file = create_file(&store.releases().join(PENDING).join(purpose.file_name()))?;
            file.write_all(bytes)?;
            seal_file(file)?;
        }
        store.commit(descriptor, expires())
    }
    #[tokio::test]
    async fn immutable_archive_survives_reopen_and_idempotent_commit_without_becoming_install_authority()
    -> Result<()> {
        let (authorized, descriptor, package, proof) = fixture()?;
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        assert!(!store.begin(&descriptor, expires())?);
        assert!(
            Store::open(&path, budget(), expires()).is_err(),
            "one controller owns the archive"
        );
        let target = store.releases().join(descriptor.name());
        assert_eq!(fs::metadata(&target)?.mode() & 0o7777, 0o500);
        assert_eq!(
            fs::metadata(target.join("package.blob"))?.mode() & 0o7777,
            0o400
        );
        assert!(create_file(&target.join("package.blob")).is_err());
        drop(store);
        let retained = verify_retained(
            &path,
            budget(),
            &authorized,
            &parent.path().canonicalize()?.join("expensive"),
            MIB,
            10,
        )
        .await?;
        assert_eq!(retained.release_id(), authorized.release_id());
        assert!(!retained.recovered_incomplete_copy());
        let reopened = Store::open(&path, budget(), expires())?;
        assert_eq!(reopened.count, 1);
        assert_eq!(reopened.used, ROOT_RESERVATION + descriptor.reservation()?);
        Ok(())
    }
    #[test]
    fn crash_recovery_discards_only_fixed_incomplete_files_and_preserves_committed_release()
    -> Result<()> {
        let (_, descriptor, package, proof) = fixture()?;
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        drop(store);
        for mode in [0o700, 0o500] {
            let pending = path.join("releases").join(PENDING);
            fs::DirBuilder::new().mode(0o700).create(&pending)?;
            create_file(&pending.join(DESCRIPTOR))?.write_all(b"interrupted JSON")?;
            create_file(&pending.join("proof.blob"))?.write_all(b"partial")?;
            fs::set_permissions(&pending, fs::Permissions::from_mode(mode))?;
            let reopened = Store::open(&path, budget(), expires())?;
            assert!(reopened.recovered);
            assert!(!pending.exists());
            assert_eq!(reopened.count, 1);
            inspect(
                &reopened.releases().join(descriptor.name()),
                true,
                expires(),
            )?;
            drop(reopened);
        }
        Ok(())
    }
    #[test]
    fn incomplete_copy_never_commits_and_quotas_charge_both_files_and_metadata() -> Result<()> {
        let (_, descriptor, package, proof) = fixture()?;
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let insufficient = ArchiveBudget {
            bytes: ROOT_RESERVATION + descriptor.reservation()? - 1,
            releases: 1,
        };
        let store = Store::open(&path, insufficient, expires())?;
        assert!(store.begin(&descriptor, expires()).is_err());
        assert!(!store.releases().join(PENDING).exists());
        drop(store);
        let store = Store::open(&path, budget(), expires())?;
        assert!(store.begin(&descriptor, expires())?);
        assert!(store.commit(&descriptor, expires()).is_err());
        assert!(!store.releases().join(descriptor.name()).exists());
        drop(store);
        let store = Store::open(&path, budget(), expires())?;
        assert!(store.recovered);
        populate(&store, &descriptor, &package, &proof)?;
        drop(store);
        assert!(Store::open(&path, insufficient, expires()).is_err());
        assert!(
            path.join("releases").join(descriptor.name()).exists(),
            "quota change never deletes valid archives"
        );
        for bad in [
            ArchiveBudget {
                bytes: 0,
                releases: 1,
            },
            ArchiveBudget {
                bytes: 128 * MIB,
                releases: 0,
            },
            ArchiveBudget {
                bytes: 128 * MIB,
                releases: 4097,
            },
        ] {
            assert!(bad.validate().is_err());
        }
        Ok(())
    }
    #[tokio::test]
    async fn bit_rot_is_rejected_and_never_silently_overwritten_on_retry() -> Result<()> {
        let (authorized, descriptor, package, proof) = fixture()?;
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        let target = store
            .releases()
            .join(descriptor.name())
            .join("package.blob");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600))?;
        let mut bytes = package.clone();
        bytes[0] ^= 1;
        OpenOptions::new()
            .write(true)
            .open(&target)?
            .write_all(&bytes)?;
        fs::set_permissions(&target, fs::Permissions::from_mode(0o400))?;
        assert!(store.begin(&descriptor, expires()).is_err());
        assert_eq!(fs::read(&target)?, bytes);
        drop(store);
        assert!(
            verify_retained(
                &path,
                budget(),
                &authorized,
                &parent.path().canonicalize()?.join("expensive"),
                MIB,
                10
            )
            .await
            .is_err()
        );
        assert_eq!(fs::read(target)?, bytes);
        Ok(())
    }
    #[tokio::test]
    async fn retained_verification_cannot_bypass_the_global_job_lane_or_read_allowance()
    -> Result<()> {
        let (authorized, descriptor, package, proof) = fixture()?;
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let private = parent.path().canonicalize()?.join("expensive");
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        drop(store);
        let job = ControllerLease::acquire(&private)?;
        assert!(
            verify_retained(&path, budget(), &authorized, &private, MIB, 10)
                .await
                .is_err()
        );
        drop(job);
        assert!(
            verify_retained(&path, budget(), &authorized, &private, 1, 10)
                .await
                .is_err()
        );
        assert!(
            verify_retained(&path, budget(), &authorized, &private, MIB, 0)
                .await
                .is_err()
        );
        assert_eq!(
            verify_retained(&path, budget(), &authorized, &private, MIB, 10)
                .await?
                .release_id(),
            authorized.release_id()
        );
        Ok(())
    }
    #[test]
    fn foreign_names_symlinks_hardlinks_and_writable_commits_fail_closed() -> Result<()> {
        use std::os::unix::fs::symlink;
        for hard in [false, true] {
            let parent = tempfile::tempdir()?;
            let path = parent.path().canonicalize()?.join("archive");
            let store = Store::open(&path, budget(), expires())?;
            let pending = store.releases().join(PENDING);
            fs::DirBuilder::new().mode(0o700).create(&pending)?;
            let outside = parent.path().join("outside");
            let mut target = create_file(&outside)?;
            target.write_all(b"never touch")?;
            drop(target);
            if hard {
                fs::hard_link(&outside, pending.join("package.blob"))?;
            } else {
                symlink(&outside, pending.join("package.blob"))?;
            }
            drop(store);
            assert!(Store::open(&path, budget(), expires()).is_err());
            assert_eq!(fs::read(&outside)?, b"never touch");
            assert!(fs::symlink_metadata(pending.join("package.blob")).is_ok());
        }
        let (_, descriptor, package, proof) = fixture()?;
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        let file = store.releases().join(descriptor.name()).join("proof.blob");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600))?;
        drop(store);
        assert!(Store::open(&path, budget(), expires()).is_err());
        Ok(())
    }
    #[tokio::test]
    async fn sink_bounds_each_write_total_bytes_and_requires_complete_durable_finish() -> Result<()>
    {
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        let target = store.root.path.join("sink-only-fixture");
        let mut sink = ArchiveSink::new(
            create_file(&target)?,
            128 * 1024,
            store.root.clone(),
            expires(),
        );
        sink.write_all(&vec![17; 128 * 1024]).await?;
        assert!(sink.write_all(&[1]).await.is_err());
        sink.finish().await?;
        assert_eq!(fs::read(&target)?, vec![17; 128 * 1024]);
        let partial = store.root.path.join("partial-only-fixture");
        let mut sink = ArchiveSink::new(create_file(&partial)?, 2, store.root.clone(), expires());
        sink.write_all(&[1]).await?;
        assert!(sink.finish().await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn cancelled_blocking_io_retains_controller_lock_until_operation_exits() -> Result<()> {
        let parent = tempfile::tempdir()?;
        let path = parent.path().canonicalize()?.join("archive");
        let store = Store::open(&path, budget(), expires())?;
        let held = store.root.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let io = tokio::task::spawn_blocking(move || {
            let _held = held;
            let _ = started.send(());
            wait.recv()
        });
        ready.await?;
        drop(store);
        io.abort();
        assert!(Store::open(&path, budget(), expires()).is_err());
        release.send(())?;
        io.await??;
        assert!(Store::open(&path, budget(), expires()).is_ok());
        Ok(())
    }
    #[test]
    fn descriptor_is_closed_canonical_and_bound_to_release_source_and_manifest() -> Result<()> {
        let (_, descriptor, _, _) = fixture()?;
        let bytes = canonical::to_vec(&descriptor)?;
        Descriptor::parse(&bytes)?;
        let mut value = serde_json::to_value(&descriptor)?;
        value["install_authorized"] = serde_json::json!(true);
        assert!(Descriptor::parse(&canonical::to_vec(&value)?).is_err());
        let mut wrong = descriptor.clone();
        wrong.workspace.release_id = Digest::of(b"other");
        assert!(wrong.validate().is_err());
        let mut wrong = descriptor.clone();
        wrong.source.manifest_sha256 = Digest::of(b"other");
        assert!(wrong.validate().is_err());
        assert!(Descriptor::parse(&vec![b' '; MAX_DESCRIPTOR + 1]).is_err());
        Ok(())
    }
}
