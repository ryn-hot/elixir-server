//! Original verified-source/export owner. Import results are never execution,
//! permissions or activation grants. Unknown daemon outcomes remain fenced.
use super::image_export::VerifiedImageExport;
use super::*;
use crate::runtime::image_import::Profile;
use elixir_marketplace::container_image::import::{
    ImportOutcome, ImportRequest, ImportStatus, MAX_REQUEST_BYTES,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};

const CONTROL: [(&str, usize); 4] = [
    ("image-import.result", 4096),
    ("image-import.attempt", 4096),
    ("image-import.retired", 4096),
    ("image-import.request", MAX_REQUEST_BYTES),
];
fn absent(path: &std::path::Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Ok(_) => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn private_file(file: &File, max: usize) -> Result<()> {
    let m = file.metadata()?;
    if !m.is_file()
        || m.len() > max as u64
        || m.uid() != unsafe { libc::geteuid() }
        || m.nlink() != 1
        || m.mode() & 0o077 != 0
    {
        bail!("invalid private import control file");
    }
    Ok(())
}
pub(in crate::marketplace) fn read_control(
    lease: &ControllerLease,
    name: &str,
    max: usize,
) -> Result<Vec<u8>> {
    if !lease.matches_directory(lease.directory())
        || !CONTROL.iter().any(|(n, b)| *n == name && max <= *b)
    {
        bail!("import control lost its actual source lease/role");
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(lease.directory().join(name))?;
    private_file(&file, max)?;
    let mut bytes = vec![];
    Read::by_ref(&mut file)
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        bail!("import control exceeds its bound");
    }
    Ok(bytes)
}
fn write_request(lease: &ControllerLease, request: &ImportRequest) -> Result<()> {
    request.validate()?;
    require_control_absent(lease)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o400)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(lease.directory().join("image-import.request"))?;
    file.write_all(&canonical::to_vec(request)?)?;
    file.sync_all()?;
    File::open(lease.directory())?.sync_all()?;
    Ok(())
}
pub(in crate::marketplace) fn require_control_absent(lease: &ControllerLease) -> Result<()> {
    if !lease.matches_directory(lease.directory()) {
        bail!("import control lease changed");
    }
    for (name, _) in CONTROL {
        match fs::symlink_metadata(lease.directory().join(name)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            _ => bail!("original import control still requires retirement"),
        }
    }
    Ok(())
}
pub(super) fn keeper_lock(lease: &ControllerLease) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(lease.directory().join("image-import.lock"))?;
    private_file(&file, 0)?;
    file.try_lock()
        .context("original image importer still owns its actual OS lock")?;
    Ok(file)
}
fn clear_retired(lease: &ControllerLease, fence: &Fence) -> Result<()> {
    let Target::ImageImportRetired { intent } = fence.target() else {
        bail!("image import has no durable retired phase");
    };
    clear_controls(lease, fence.operation_id(), intent)
}
pub(super) fn clear_controls(
    lease: &ControllerLease,
    operation: uuid::Uuid,
    intent: &elixir_marketplace::container_image::import::ImportIntent,
) -> Result<()> {
    let _keeper = keeper_lock(lease)?;
    // Request is removed LAST, so partially completed cleanup can still bind
    // all result/attempt files on restart. This never invokes the daemon.
    let mut validated = vec![];
    for (name, max) in CONTROL {
        match fs::symlink_metadata(lease.directory().join(name)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
            Ok(_) => (),
        }
        let bytes = read_control(lease, name, max)?;
        if name == "image-import.request" {
            if ImportRequest::parse(&bytes)?.intent()? != *intent {
                bail!("retired import request identity changed");
            }
        } else {
            let value = canonical::parse(&bytes, max)?;
            if value
                .get("operation_id")
                .and_then(serde_json::Value::as_str)
                != Some(operation.to_string().as_str())
            {
                bail!("foreign import control operation");
            }
            let binding = if name == "image-import.result" {
                value.get("request")
            } else {
                value.pointer("/target/intent/request")
            };
            if binding != Some(&serde_json::to_value(intent.request)?) {
                bail!("foreign import control request");
            }
        }
        validated.push(name);
    }
    for name in validated {
        fs::remove_file(lease.directory().join(name))?;
        File::open(lease.directory())?.sync_all()?;
    }
    Ok(())
}
pub(in crate::marketplace) fn cleanup_idle(lease: &ControllerLease) -> Result<()> {
    super::image_reference::require_absent(lease)?;
    if Fence::load(lease)?.is_some() {
        bail!("image import control is not idle");
    }
    let _keeper = keeper_lock(lease)?;
    for (name, _) in &CONTROL[..3] {
        if !absent(&lease.directory().join(name))? {
            bail!("unbound attempted import requires recovery");
        }
    }
    let path = lease.directory().join("image-import.request");
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
        Ok(_) => (),
    }
    ImportRequest::parse(&read_control(
        lease,
        "image-import.request",
        MAX_REQUEST_BYTES,
    )?)?;
    // No root fence and no attempted/result record: the caller died before
    // authorization to spawn. A late helper cannot match a missing root.
    fs::remove_file(path)?;
    File::open(lease.directory())?.sync_all()?;
    Ok(())
}
pub(in crate::marketplace) async fn retire_activity(
    runtime: &dyn RuntimeManager,
    lease: &ControllerLease,
    fence: &mut Fence,
) -> Result<bool> {
    match fence.target() {
        Target::ImageImport { .. } => {
            if absent(&lease.directory().join("image-import.retired"))? {
                let profile = runtime
                    .marketplace_import_profile()
                    .context("original import requires its trusted host profile")?;
                let mut child = profile.spawn(lease.directory(), true)?;
                // No kill-on-drop. A lost recovery waiter leaves the independent
                // helper and root fence, never a fabricated stopped result.
                if !child.wait().await?.success() {
                    bail!("original image import cleanup remains unconfirmed");
                }
            }
            let _keeper = keeper_lock(lease)?;
            fence.retire_import(lease)?;
            if let Target::ImageImportRetired { intent } = fence.target() {
                super::image_reference::mirror_retired(lease, fence.operation_id(), intent)?;
            }
        }
        Target::ImageReferenceAdopted { intent } => {
            super::image_reference::require_active(lease, fence.operation_id(), intent)?;
            return Ok(true);
        }
        Target::ImageImportRetired { intent } => {
            let _keeper = keeper_lock(lease)?;
            super::image_reference::mirror_retired(lease, fence.operation_id(), intent)?;
        }
        _ => return Ok(false),
    }
    clear_retired(lease, fence)?;
    Ok(true)
}

/// Conservative peak daemon work estimate, not a hard filesystem quota. Actual
/// supported-daemon accounting and playback coexistence must still be qualified.
fn allocation(
    summary: &elixir_marketplace::container_image::export::ContainerExportSummary,
) -> Result<u64> {
    let fs = &summary.filesystem;
    summary
        .archive
        .size
        .checked_mul(3)
        .and_then(|n| n.checked_add(2 * fs.reserved_unpacked_bytes))
        .and_then(|n| n.checked_add(fs.entries as u64 * 4096 + 16 * MIB))
        .context("daemon image allocation overflow")
}

pub(in crate::marketplace) struct ImportedImage {
    owner: evidence_owner::EvidenceOwner,
    config: Digest,
    request: Digest,
}
impl ImportedImage {
    pub(in crate::marketplace) fn require_source(
        &self,
        source: &SourceMatchedQuarantine,
    ) -> Result<()> {
        self.owner.require(source)?;
        source.require_public_source()?;
        if !matches!(source.source.job.activity.as_ref().map(Fence::target),Some(Target::ImageImport{intent}) if intent.request==self.request)
        {
            bail!("imported image lost its original active root owner");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn config(&self) -> Digest {
        self.config
    }
    pub(super) fn request(&self) -> Digest {
        self.request
    }
}
pub(in crate::marketplace) struct Import {
    source: Option<SourceMatchedQuarantine>,
    export: Option<VerifiedImageExport>,
    profile: Arc<Profile>,
    request: Option<ImportRequest>,
    child: Option<tokio::process::Child>,
    started: bool,
    cancelled: bool,
    verified: Option<ImportedImage>,
}
impl SourceMatchedQuarantine {
    pub(in crate::marketplace) fn begin_image_import(
        source: &mut Option<Self>,
        export: &mut Option<VerifiedImageExport>,
    ) -> Result<Import> {
        let original = source.as_ref().context("image import source missing")?;
        super::image_reference::require_absent(&original.job_lease())?;
        let sealed = export.as_ref().context("verified image export missing")?;
        sealed.require_source(original)?;
        let profile = original
            .runtime()
            .marketplace_import_profile()
            .context("image import has no trusted host profile")?;
        let execution =
            super::super::runtime::build::execution(&original.source.job.plan.source_match, 1)?;
        let required = allocation(sealed.summary())?
            .checked_add(sealed.summary().archive.size + MIB)
            .context("import/staging allocation overflow")?;
        if required > super::super::runtime::build::extra_disk(&execution)? {
            bail!("original retired build allocation cannot cover image staging plus daemon work");
        }
        Ok(Import {
            source: source.take(),
            export: export.take(),
            profile,
            request: None,
            child: None,
            started: false,
            cancelled: false,
            verified: None,
        })
    }
}
impl Import {
    /// Preserve actual import provenance while handing cleanup ownership to a
    /// separate bounded passive ledger. This does NOT start publisher code.
    pub(in crate::marketplace) async fn adopt(
        &mut self,
    ) -> Result<(
        SourceMatchedQuarantine,
        super::image_reference::RuntimeImage,
    )> {
        let imported = self
            .verified
            .as_ref()
            .context("image adoption needs an actual import result")?;
        imported.require_source(self.source()?)?;
        let lease = self.source()?.job_lease();
        let fence = self
            .source()?
            .source
            .job
            .activity
            .as_ref()
            .context("image adoption lost its original import fence")?;
        super::image_reference::prepare(&lease, fence)?;
        let image = super::image_reference::RuntimeImage::capture(
            self.source()?,
            imported,
            self.request
                .as_ref()
                .context("image adoption lost request")?,
        )?;
        {
            let job = &mut self
                .source
                .as_mut()
                .context("image source missing")?
                .source
                .job;
            job.activity
                .as_mut()
                .context("image import fence missing")?
                .adopt_image_reference(&lease)?;
            job.complete_activity()?;
        }
        // Retain this same owner through cancelled host I/O. On failure/drop,
        // source retirement cleans the passive ledger before releasing its lane.
        if let Some(export) = self.export.as_mut() {
            export.discard().await?;
        }
        image.require_source(self.source()?)?;
        self.verified = None;
        self.export = None;
        Ok((
            self.source.take().context("adopted image source missing")?,
            image,
        ))
    }
    pub(in crate::marketplace) fn source(&self) -> Result<&SourceMatchedQuarantine> {
        self.source
            .as_ref()
            .context("original image source missing")
    }
    fn current(&self) -> Result<()> {
        self.export
            .as_ref()
            .context("original image export missing")?
            .require_source(self.source()?)
    }
    pub(in crate::marketplace) async fn run(&mut self) -> Result<()> {
        if self.started || self.cancelled {
            bail!("image import cannot restart");
        }
        self.started = true;
        self.current()?;
        let runtime = self.source()?.runtime();
        let expires = self.source()?.expires();
        {
            let job = &mut self
                .source
                .as_mut()
                .context("image import source missing")?
                .source
                .job;
            job.cleanup_activity(runtime.as_ref()).await?;
            job.recover_writer(runtime.as_ref()).await?;
            job.cached_measurement = None;
            job.admit_native(expires).await?;
            charge_bytes(&mut job.input_used, MIB, job.plan.input_limit)?;
            charge_bytes(&mut job.output_used, MIB, job.plan.output_limit)?;
        }
        self.current()?;
        let id = timeout_at(expires, runtime.marketplace_daemon_id()).await??;
        let source = self.source()?;
        let summary = self
            .export
            .as_ref()
            .context("image export missing")?
            .summary();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let remaining = expires
            .saturating_duration_since(Instant::now())
            .as_millis();
        if remaining == 0 {
            bail!("original image import deadline expired");
        }
        let request = ImportRequest {
            schema_version: 1,
            daemon_id: id,
            storage_namespace: format!("sha256:{}", source.source.job._lease.storage_namespace()?)
                .parse()?,
            deployment: source.source.job.deployment.clone(),
            archive: summary.archive.clone(),
            filesystem: summary.filesystem.clone(),
            socket: self.profile.socket(),
            expires_unix_millis: u64::try_from(now + remaining)?,
        };
        write_request(&source.source.job._lease, &request)?;
        self.request = Some(request.clone());
        self.source
            .as_mut()
            .context("image import source missing")?
            .source
            .job
            .begin_activity(Target::ImageImport {
                intent: request.intent()?,
            })?;
        self.child = Some(
            self.profile
                .spawn(self.source()?.source.job._lease.directory(), false)?,
        );
        let status = self
            .child
            .as_mut()
            .context("original import child missing")?
            .wait()
            .await?;
        self.child = None;
        if !status.success() {
            bail!("native image import did not produce a successful joined outcome");
        }
        self.current()?;
        let source = self.source()?;
        let fence = source
            .source
            .job
            .activity
            .as_ref()
            .context("import root missing")?;
        let outcome = ImportOutcome::parse(
            &read_control(&source.source.job._lease, "image-import.result", 4096)?,
            &request,
            &fence.operation_id().to_string(),
        )?;
        if outcome.status == ImportStatus::Rejected {
            bail!("daemon rejected selected image");
        }
        if timeout_at(expires, runtime.marketplace_daemon_id()).await?? != request.daemon_id {
            bail!("actual runtime daemon changed during import");
        }
        let actual = timeout_at(
            expires,
            runtime.inspect_marketplace_import_image(outcome.image),
        )
        .await??;
        elixir_marketplace::container_image::import::inspect(&actual, &request.filesystem.image)?;
        if timeout_at(expires, runtime.marketplace_daemon_id()).await?? != request.daemon_id {
            bail!("actual runtime daemon changed during inspection");
        }
        self.current()?;
        self.verified = Some(ImportedImage {
            owner: evidence_owner::EvidenceOwner::capture(self.source()?)?,
            config: outcome.image,
            request: request.intent()?.request,
        });
        Ok(())
    }
    pub(in crate::marketplace) fn verified(&self) -> Result<&ImportedImage> {
        let result = self
            .verified
            .as_ref()
            .context("no actual imported-image result")?;
        result.require_source(self.source()?)?;
        Ok(result)
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        if let Some(child) = self.child.as_mut() {
            child.wait().await?;
            self.child = None;
        }
        let source = self
            .source
            .as_mut()
            .context("image import source missing")?;
        let runtime = source.runtime();
        source.source.job.cleanup_activity(runtime.as_ref()).await?;
        super::image_reference::recover(runtime.as_ref(), &source.job_lease()).await?;
        cleanup_idle(&source.source.job._lease)?;
        if let Some(export) = self.export.as_mut() {
            export.discard().await?;
        }
        self.verified = None;
        self.cancelled = true;
        Ok(())
    }
    pub(in crate::marketplace) fn take_cancelled_source(
        &mut self,
    ) -> Result<SourceMatchedQuarantine> {
        if !self.cancelled || self.child.is_some() || self.verified.is_some() {
            bail!("image import is not retired");
        }
        self.source.take().context("original import source missing")
    }
}
impl Drop for Import {
    fn drop(&mut self) {
        let Some(mut source) = self.source.take() else {
            return;
        };
        let mut child = self.child.take();
        let mut export = self.export.take();
        let Ok(executor) = tokio::runtime::Handle::try_current() else {
            return;
        };
        executor.spawn(async move {
            let mut delay = 2;
            loop {
                let result = async {
                    if let Some(child) = child.as_mut() {
                        child.wait().await?;
                    }
                    child = None;
                    let runtime = source.runtime();
                    source.source.job.cleanup_activity(runtime.as_ref()).await?;
                    super::image_reference::recover(runtime.as_ref(), &source.job_lease()).await?;
                    cleanup_idle(&source.source.job._lease)?;
                    if let Some(export) = export.as_mut() {
                        export.discard().await?;
                    }
                    source.discard_retained().await
                }
                .await;
                if result.is_ok() {
                    return;
                }
                tracing::warn!("original image import cleanup remains fenced");
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay = (delay * 2).min(60);
            }
        });
    }
}

#[cfg(test)]
mod tests;
