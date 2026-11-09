//! A bounded child journal under the ORIGINAL source's global OS lease. A
//! decoded record authorizes cleanup only, never START, material or sockets.
use super::*;
use crate::{
    marketplace::application_egress::{ApplicationGatewayIntent, ApplicationGatewayProfile},
    runtime::{
        RuntimeManager,
        model::{CONTAINER_SPEC_HASH_LABEL, ContainerHandle},
        packet_network::immutable_id,
    },
};
use elixir_marketplace::identity::Digest;

pub struct GatewayCreateAttempt {
    operation: Uuid,
    fingerprint: String,
}
impl GatewayCreateAttempt {
    pub(crate) fn require(
        &self,
        operation: Uuid,
        profile: &ApplicationGatewayProfile,
    ) -> Result<()> {
        if operation.is_nil()
            || self.operation != operation
            || self.fingerprint != profile.spec().labels[CONTAINER_SPEC_HASH_LABEL]
        {
            bail!("gateway create outcome belongs to another original attempt");
        }
        Ok(())
    }
    pub(crate) fn require_profile(&self, profile: &ApplicationGatewayProfile) -> Result<()> {
        self.require(self.operation, profile)
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
struct GatewayRecord {
    schema_version: u32,
    operation_id: Uuid,
    parent_operation_id: Uuid,
    parent_container_id: String,
    source_invocation: Digest,
    intent: ApplicationGatewayIntent,
    state: Phase,
    #[serde(default, skip_serializing_if = "is_false")]
    standalone: bool,
}
pub(in crate::marketplace) struct GatewayFence {
    file: File,
    record: GatewayRecord,
}
fn is_false(value: &bool) -> bool {
    !*value
}
impl GatewayFence {
    /// Tracking only. The actual original NativeInvocation must check its
    /// source/mode/deadline and installed authority before physical effects.
    pub(in crate::marketplace) fn begin(
        lease: &ControllerLease,
        parent: &str,
        invocation: Digest,
        profile: &ApplicationGatewayProfile,
    ) -> Result<Self> {
        let _guard = lease.activity_guard()?;
        http::require_absent(lease)?;
        require_gateway_absent(lease)?;
        profile.require_nonce(invocation)?;
        let root =
            Fence::load(lease)?.context("gateway child requires its recorded native parent")?;
        if !immutable_id(parent)
            || invocation.as_bytes() == &[0; 32]
            || !matches!(root.target(),Target::SourceWriter{container_id:Some(id),..} if id==parent)
        {
            bail!("gateway child differs from the original native parent");
        }
        let record = GatewayRecord {
            schema_version: 1,
            operation_id: Uuid::new_v4(),
            parent_operation_id: root.record.operation_id,
            parent_container_id: parent.into(),
            source_invocation: invocation,
            intent: profile.intent(),
            state: Phase::Prepared,
            standalone: false,
        };
        let bytes = canonical::to_vec(&record)?;
        if bytes.len() > MAX_BYTES {
            bail!("gateway child journal bounds");
        }
        let mut options = options();
        options.create_new(true);
        let mut file = options.open(lease.directory.join(GATEWAY_FILE_NAME))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        File::open(&lease.directory)?.sync_all()?;
        Ok(Self { file, record })
    }
    /// A separate, actual installed-controller OS owner, not a source call.
    /// Deserializing this journal can authorize recovery only, never creation.
    #[cfg(unix)]
    pub(in crate::marketplace) fn begin_warm(
        owner: &crate::marketplace::grant_store::installation::runtime::RuntimeOwner,
        profile: &ApplicationGatewayProfile,
        nonce: Digest,
    ) -> Result<Self> {
        owner.check()?;
        if !profile.is_warm() {
            bail!("warm owner requires a separately compiled tunnel profile");
        }
        let lease = owner.lease();
        let _guard = lease.activity_guard()?;
        http::require_absent(&lease)?;
        require_gateway_absent(&lease)?;
        if Fence::load(&lease)?.is_some() {
            bail!("warm gateway cannot adopt a source parent");
        }
        profile.require_nonce(nonce)?;
        let operation_id = Uuid::new_v4();
        let record = GatewayRecord {
            schema_version: 1,
            operation_id,
            parent_operation_id: operation_id,
            parent_container_id: String::new(),
            source_invocation: nonce,
            intent: profile.intent(),
            state: Phase::Prepared,
            standalone: true,
        };
        let bytes = canonical::to_vec(&record)?;
        if bytes.len() > MAX_BYTES {
            bail!("warm gateway journal bounds");
        }
        let mut file = options()
            .create_new(true)
            .open(lease.directory.join(GATEWAY_FILE_NAME))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        File::open(&lease.directory)?.sync_all()?;
        Ok(Self { file, record })
    }
    pub(in crate::marketplace) fn load(lease: &ControllerLease) -> Result<Option<Self>> {
        let _guard = lease.activity_guard()?;
        let mut file = match options().open(lease.directory.join(GATEWAY_FILE_NAME)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let record = read_gateway(&mut file)?;
        let fence = Self { file, record };
        fence.require(lease)?;
        Ok(Some(fence))
    }
    fn require(&self, lease: &ControllerLease) -> Result<()> {
        if !lease.matches_directory(&lease.directory) {
            bail!("gateway lost its original OS lease");
        }
        let path = fs::symlink_metadata(lease.directory.join(GATEWAY_FILE_NAME))?;
        let held = self.file.metadata()?;
        private_file(&path)?;
        private_file(&held)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if path.dev() != held.dev() || path.ino() != held.ino() {
                bail!("gateway journal inode changed");
            }
        }
        if self.record.standalone {
            if Fence::load(lease)?.is_some() {
                bail!("warm gateway acquired an unexpected source parent");
            }
            return Ok(());
        }
        let root = Fence::load(lease)?.context("gateway child lost its native parent record")?;
        if root.record.operation_id != self.record.parent_operation_id
            || !matches!(root.target(),Target::SourceWriter{container_id:Some(id),..} if id==&self.record.parent_container_id)
        {
            bail!("gateway child belongs to a different native operation");
        }
        Ok(())
    }
    fn current(&mut self, lease: &ControllerLease) -> Result<()> {
        self.require(lease)?;
        if read_gateway(&mut self.file)? != self.record {
            bail!("gateway child journal changed");
        }
        Ok(())
    }
    fn write(&mut self, state: Phase) -> Result<()> {
        let record = GatewayRecord {
            schema_version: 1,
            operation_id: self.record.operation_id,
            parent_operation_id: self.record.parent_operation_id,
            parent_container_id: self.record.parent_container_id.clone(),
            source_invocation: self.record.source_invocation,
            intent: self.record.intent.clone(),
            state,
            standalone: self.record.standalone,
        };
        let bytes = canonical::to_vec(&record)?;
        if bytes.len() > MAX_BYTES {
            bail!("gateway child journal bounds");
        }
        self.file.seek(SeekFrom::Start(0))?;
        self.file.set_len(0)?;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        self.record = record;
        Ok(())
    }
    /// Single-use token only after the original intent has been synchronized.
    pub(in crate::marketplace) fn creating(
        &mut self,
        lease: &ControllerLease,
    ) -> Result<GatewayCreateAttempt> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        http::require_absent(lease)?;
        if !matches!(self.record.state, Phase::Prepared) {
            bail!("gateway creation already attempted");
        }
        let profile = self.record.intent.profile()?;
        self.write(Phase::Creating)?;
        Ok(GatewayCreateAttempt {
            operation: self.record.operation_id,
            fingerprint: profile.spec().labels[CONTAINER_SPEC_HASH_LABEL].clone(),
        })
    }
    pub(in crate::marketplace) fn not_attempted(
        &mut self,
        lease: &ControllerLease,
        outcome: crate::runtime::gateway_creation::GatewayNotAttempted,
    ) -> Result<anyhow::Error> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        if !matches!(self.record.state, Phase::Creating) {
            bail!("gateway rejection differs from its original phase");
        }
        outcome.require(self.record.operation_id, &self.record.intent.profile()?)?;
        self.write(Phase::NotAttempted)?;
        Ok(outcome.into_error())
    }
    /// Synchronize the returned immutable CID BEFORE any separate START.
    pub(in crate::marketplace) fn bind(&mut self, lease: &ControllerLease, id: &str) -> Result<()> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        if !matches!(self.record.state, Phase::Creating)
            || !immutable_id(id)
            || id == self.record.parent_container_id
        {
            bail!("gateway cannot bind this native identity");
        }
        self.write(Phase::Bound {
            container_id: id.into(),
        })
    }
    pub(in crate::marketplace) fn bound(
        &mut self,
        lease: &ControllerLease,
    ) -> Result<(ApplicationGatewayProfile, ContainerHandle)> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        let Phase::Bound { container_id } = &self.record.state else {
            bail!("gateway has no durable native identity");
        };
        let profile = self.record.intent.profile()?;
        let handle = ContainerHandle {
            id: container_id.clone(),
            name: profile.spec().name.clone(),
        };
        Ok((profile, handle))
    }
    /// Caller retains this SAME object across cancellation/error. Normal
    /// owners first join ORIGINAL create/start/material/CLI tasks; recovery
    /// holds the OS lease after those old process owners have died. No Drop
    /// clearing, name-absence proof or automatic replacement is allowed.
    pub(in crate::marketplace) async fn retire(
        &mut self,
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<()> {
        let (profile, prepared, creating) = {
            let _guard = lease.activity_guard()?;
            self.current(lease)?;
            (
                self.record.intent.profile()?,
                matches!(self.record.state, Phase::Prepared | Phase::NotAttempted),
                matches!(self.record.state, Phase::Creating),
            )
        };
        if !prepared {
            if creating {
                let handle = runtime
                    .inspect_unstarted_application_gateway(&profile)
                    .await?;
                if handle.name != profile.spec().name {
                    bail!("gateway recovery returned a different actor");
                }
                self.bind(lease, &handle.id)?;
            }
            let (profile, handle) = self.bound(lease)?;
            if let Some(actual) = runtime.get_container_handle(&handle.id).await? {
                if actual.id != handle.id {
                    bail!("gateway recovery identity changed");
                }
                runtime.stop_application_gateway(&handle, &profile).await?;
                // Stop may revoke service immediately, but do NOT remove a
                // namespace owner while an original HTTP child is unretired.
                {
                    let _guard = lease.activity_guard()?;
                    self.current(lease)?;
                    http::require_absent(lease)?;
                }
                runtime
                    .remove_application_gateway(&handle, &profile)
                    .await?;
            }
        }
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        http::require_absent(lease)?;
        fs::remove_file(lease.directory.join(GATEWAY_FILE_NAME))?;
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
        .context("original gateway recovery deadline")??;
        Ok(true)
    }
    /// Invoked only after recovery of the global source/HTTP lane and while
    /// holding the SAME installed-controller OS lease and DB lineage.
    pub(in crate::marketplace) async fn recover_warm(
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<bool> {
        {
            let _guard = lease.activity_guard()?;
            http::require_absent(lease)?;
            if Fence::load(lease)?.is_some() {
                bail!("unexpected source work under warm controller");
            }
        }
        let Some(mut fence) = Self::load(lease)? else {
            return Ok(false);
        };
        if !fence.record.standalone {
            bail!("source-bound gateway cannot become a warm tunnel");
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            fence.retire(runtime, lease),
        )
        .await
        .context("warm gateway recovery deadline")??;
        Ok(true)
    }
}
fn read_gateway(file: &mut File) -> Result<GatewayRecord> {
    private_file(&file.metadata()?)?;
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = vec![];
    file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    let record: GatewayRecord =
        canonical::decode(&bytes, MAX_BYTES).context("corrupt gateway child journal")?;
    if record.schema_version != 1
        || record.operation_id.is_nil()
        || if record.standalone {
            record.parent_operation_id != record.operation_id
                || !record.parent_container_id.is_empty()
        } else {
            record.parent_operation_id.is_nil() || !immutable_id(&record.parent_container_id)
        }
        || record.source_invocation.as_bytes() == &[0; 32]
        || (record.standalone && !record.intent.profile()?.is_warm())
        || canonical::to_vec(&record)? != bytes
        || matches!(&record.state,Phase::Bound{container_id} if !immutable_id(container_id) || container_id==&record.parent_container_id)
    {
        bail!("invalid gateway child journal");
    }
    record
        .intent
        .profile()?
        .require_nonce(record.source_invocation)?;
    Ok(record)
}

#[cfg(test)]
mod tests;
