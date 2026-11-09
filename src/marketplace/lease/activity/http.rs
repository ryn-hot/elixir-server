//! One bounded child record under the SAME global lane/OS lease as its native
//! executor. Neither an intent nor a stopped VM proves this child has retired.
use super::*;
use crate::{
    marketplace::runtime::application_http::{ApplicationHttpIntent, ApplicationHttpProfile},
    runtime::{RuntimeManager, model::ContainerHandle, packet_network::immutable_id},
};
use elixir_marketplace::identity::Digest;

pub(in crate::marketplace) const FILE_NAME: &str = "http-helper.pending";

/// One linear create call minted ONLY by its original journal's Creating
/// transition. No Clone, Deserialize or production constructor outside here.
pub struct HttpCreateAttempt {
    operation: Uuid,
    fingerprint: String,
}
impl HttpCreateAttempt {
    pub(crate) fn require(&self, operation: Uuid, profile: &ApplicationHttpProfile) -> Result<()> {
        if self.operation.is_nil()
            || self.operation != operation
            || self.fingerprint
                != profile.spec().labels[crate::runtime::model::CONTAINER_SPEC_HASH_LABEL]
        {
            bail!("HTTP create outcome belongs to another original attempt");
        }
        Ok(())
    }
    pub(crate) fn require_profile(&self, profile: &ApplicationHttpProfile) -> Result<()> {
        self.require(self.operation, profile)
    }
    #[cfg(test)]
    pub(crate) fn fixture(profile: &ApplicationHttpProfile) -> Self {
        Self {
            operation: Uuid::new_v4(),
            fingerprint: profile.spec().labels[crate::runtime::model::CONTAINER_SPEC_HASH_LABEL]
                .clone(),
        }
    }
}
pub(super) fn require_absent(lease: &ControllerLease) -> Result<()> {
    match fs::symlink_metadata(lease.directory.join(FILE_NAME)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => bail!("the original HTTP helper still requires confirmed cleanup"),
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum Phase {
    Prepared,
    Creating,
    NotAttempted,
    Bound { container_id: String },
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct HttpRecord {
    schema_version: u32,
    operation_id: Uuid,
    parent_operation_id: Uuid,
    parent_container_id: String,
    source_invocation: Digest,
    intent: ApplicationHttpIntent,
    state: Phase,
}
pub(in crate::marketplace) struct HttpFence {
    file: File,
    record: HttpRecord,
}
impl HttpFence {
    /// Tracking only, not permission to launch. The actual native invocation
    /// additionally checks source/mode/deadline and current installed authority.
    pub(in crate::marketplace) fn begin(
        lease: &ControllerLease,
        parent: &str,
        invocation: Digest,
        profile: &ApplicationHttpProfile,
    ) -> Result<Self> {
        let _guard = lease.activity_guard()?;
        let root = Fence::load(lease)?.context("HTTP child requires its recorded native parent")?;
        if !matches!(root.target(),Target::SourceWriter{container_id:Some(id),..} if id==parent)
            || !immutable_id(parent)
            || invocation.as_bytes() == &[0; 32]
        {
            bail!("HTTP child differs from the original native parent");
        }
        let record = HttpRecord {
            schema_version: 1,
            operation_id: Uuid::new_v4(),
            parent_operation_id: root.record.operation_id,
            parent_container_id: parent.to_owned(),
            source_invocation: invocation,
            intent: profile.intent(),
            state: Phase::Prepared,
        };
        let bytes = canonical::to_vec(&record)?;
        if bytes.len() > MAX_BYTES {
            bail!("HTTP child journal bounds");
        }
        let mut options = options();
        options.create_new(true);
        let mut file = options.open(lease.directory.join(FILE_NAME))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        File::open(&lease.directory)?.sync_all()?;
        Ok(Self { file, record })
    }
    pub(in crate::marketplace) fn load(lease: &ControllerLease) -> Result<Option<Self>> {
        let _guard = lease.activity_guard()?;
        Self::load_locked(lease)
    }
    fn load_locked(lease: &ControllerLease) -> Result<Option<Self>> {
        if !lease.matches_directory(&lease.directory) {
            bail!("HTTP child lost its original global lease");
        }
        let mut file = match options().open(lease.directory.join(FILE_NAME)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let record = read_http(&mut file)?;
        let value = Self { file, record };
        value.require(lease)?;
        Ok(Some(value))
    }
    fn require(&self, lease: &ControllerLease) -> Result<()> {
        if !lease.matches_directory(&lease.directory) {
            bail!("HTTP child lost its original global lease");
        }
        let path = fs::symlink_metadata(lease.directory.join(FILE_NAME))?;
        let held = self.file.metadata()?;
        private_file(&path)?;
        private_file(&held)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if path.dev() != held.dev() || path.ino() != held.ino() {
                bail!("HTTP child journal inode changed");
            }
        }
        let root = Fence::load(lease)?.context("HTTP child lost its native parent record")?;
        if root.record.operation_id != self.record.parent_operation_id
            || !matches!(root.target(),Target::SourceWriter{container_id:Some(id),..} if id==&self.record.parent_container_id)
        {
            bail!("HTTP child belongs to a different native operation");
        }
        Ok(())
    }
    fn current(&mut self, lease: &ControllerLease) -> Result<()> {
        self.require(lease)?;
        if read_http(&mut self.file)? != self.record {
            bail!("HTTP child journal changed");
        }
        Ok(())
    }
    fn write(&mut self, state: Phase) -> Result<()> {
        let record = HttpRecord {
            schema_version: self.record.schema_version,
            operation_id: self.record.operation_id,
            parent_operation_id: self.record.parent_operation_id,
            parent_container_id: self.record.parent_container_id.clone(),
            source_invocation: self.record.source_invocation,
            intent: self.record.intent.clone(),
            state,
        };
        let bytes = canonical::to_vec(&record)?;
        if bytes.len() > MAX_BYTES {
            bail!("HTTP child journal bounds");
        }
        self.file.seek(SeekFrom::Start(0))?;
        self.file.set_len(0)?;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        self.record = record;
        Ok(())
    }
    /// MUST complete before issuing CREATE. Prepared recovery has no possible
    /// native effect; uncertain Creating recovery may never infer absence.
    pub(in crate::marketplace) fn creating(
        &mut self,
        lease: &ControllerLease,
    ) -> Result<HttpCreateAttempt> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        if !matches!(self.record.state, Phase::Prepared) {
            bail!("HTTP creation already attempted");
        }
        let profile = self.record.intent.profile()?;
        self.write(Phase::Creating)?;
        Ok(HttpCreateAttempt {
            operation: self.record.operation_id,
            fingerprint: profile.spec().labels[crate::runtime::model::CONTAINER_SPEC_HASH_LABEL]
                .clone(),
        })
    }
    /// Only the completed original runtime call can return its consumed attempt
    /// as definitively pre-CREATE. Persist that terminal fact before cleanup;
    /// cancelled waiters/restart may retire it without a name-absence heuristic.
    pub(in crate::marketplace) fn not_attempted(
        &mut self,
        lease: &ControllerLease,
        outcome: crate::runtime::http_creation::HttpNotAttempted,
    ) -> Result<anyhow::Error> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        if !matches!(self.record.state, Phase::Creating) {
            bail!("HTTP rejection cannot replace a bound or unattempted operation");
        }
        outcome.require(self.record.operation_id, &self.record.intent.profile()?)?;
        self.write(Phase::NotAttempted)?;
        Ok(outcome.into_error())
    }
    /// MUST fsync the actual CID before the separate inspected START.
    pub(in crate::marketplace) fn bind(&mut self, lease: &ControllerLease, id: &str) -> Result<()> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        if !matches!(self.record.state, Phase::Creating)
            || !immutable_id(id)
            || id == self.record.parent_container_id
        {
            bail!("HTTP child cannot bind this native identity");
        }
        self.write(Phase::Bound {
            container_id: id.to_owned(),
        })
    }
    pub(in crate::marketplace) fn bound(
        &mut self,
        lease: &ControllerLease,
    ) -> Result<(ApplicationHttpProfile, ContainerHandle)> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        let Phase::Bound { container_id } = &self.record.state else {
            bail!("HTTP startup has no durable native identity");
        };
        let profile = self.record.intent.profile()?;
        let handle = ContainerHandle {
            id: container_id.clone(),
            name: profile.spec().name.clone(),
        };
        Ok((profile, handle))
    }
    /// Retain this SAME object across cancelled/error cleanup waiters. No Drop
    /// cleanup. Normal owners must first join their original creation/HTTP task;
    /// restart recovery holds the OS lease after every old owner has died.
    pub(in crate::marketplace) async fn retire(
        &mut self,
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<()> {
        let (profile, prepared, creating) = {
            let _guard = lease.activity_guard()?;
            self.current(lease)?;
            let profile = self.record.intent.profile()?;
            (
                profile,
                matches!(self.record.state, Phase::Prepared | Phase::NotAttempted),
                matches!(self.record.state, Phase::Creating),
            )
        };
        if !prepared {
            if creating {
                let handle = runtime.inspect_unstarted_application_http(&profile).await?;
                if handle.name != profile.spec().name {
                    bail!("unstarted HTTP recovery returned a different helper");
                }
                // Persist the observed CID BEFORE removing it. A crash after
                // removal can then prove absence of that exact immutable ID.
                self.bind(lease, &handle.id)?;
            }
            let (profile, handle) = self.bound(lease)?;
            if let Some(actual) = runtime.get_container_handle(&handle.id).await? {
                // Generic lookup echoes its lookup reference in `name`; it
                // is NOT an inspection of the native human name. Keep the
                // frozen profile handle for the sealed stop/remove methods,
                // which inspect actual CID/name/ownership before any effect.
                if actual.id != handle.id {
                    bail!("HTTP recovery identity changed");
                }
                runtime.stop_application_http(&handle, &profile).await?;
                runtime.remove_application_http(&handle, &profile).await?;
            }
        }
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        fs::remove_file(lease.directory.join(FILE_NAME))?;
        File::open(&lease.directory)?.sync_all()?;
        Ok(())
    }
    pub(in crate::marketplace) async fn recover(
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<bool> {
        let Some(mut fence) = Self::load(lease)? else {
            return Ok(false);
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            fence.retire(runtime, lease),
        )
        .await
        .context("HTTP child recovery deadline")??;
        Ok(true)
    }
}
fn read_http(file: &mut File) -> Result<HttpRecord> {
    private_file(&file.metadata()?)?;
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = vec![];
    file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    let record: HttpRecord =
        canonical::decode(&bytes, MAX_BYTES).context("corrupt HTTP child journal")?;
    if record.schema_version != 1
        || record.operation_id.is_nil()
        || record.parent_operation_id.is_nil()
        || !immutable_id(&record.parent_container_id)
        || record.source_invocation.as_bytes() == &[0; 32]
        || canonical::to_vec(&record)? != bytes
        || matches!(&record.state,Phase::Bound{container_id} if !immutable_id(container_id) || container_id==&record.parent_container_id)
    {
        bail!("invalid HTTP child journal");
    }
    record.intent.profile()?;
    Ok(record)
}

#[cfg(test)]
mod tests;
