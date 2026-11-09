//! One original provider operation. Persist before CREATE; persist returned CID
//! before START. Recovery is cleanup-only, never a new start token. Drop never
//! removes a fence, and unknown CREATE never becomes "not attempted" by absence.
use super::*;
use crate::{
    marketplace::lease::ControllerLease,
    runtime::{
        RuntimeManager,
        model::{CONTAINER_SPEC_HASH_LABEL, ContainerHandle},
    },
};
use anyhow::{Context, Result, ensure};
use elixir_marketplace::canonical;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};
use uuid::Uuid;
const LIMIT: usize = 4096;

pub struct CreateAttempt {
    operation: Uuid,
    fingerprint: String,
}
impl CreateAttempt {
    pub(crate) fn require(&self, operation: Uuid, profile: &Profile) -> Result<()> {
        ensure!(
            !operation.is_nil() && self.operation == operation,
            "provider creation belongs to another operation"
        );
        self.require_profile(profile)
    }
    pub(crate) fn require_profile(&self, profile: &Profile) -> Result<()> {
        ensure!(
            self.fingerprint == profile.spec().labels[CONTAINER_SPEC_HASH_LABEL],
            "provider creation profile differs"
        );
        Ok(())
    }
}
pub struct StartAttempt {
    fingerprint: String,
    id: String,
}
impl StartAttempt {
    pub(crate) fn require(&self, profile: &Profile, handle: &ContainerHandle) -> Result<()> {
        ensure!(
            self.fingerprint == profile.spec().labels[CONTAINER_SPEC_HASH_LABEL]
                && self.id == handle.id
                && handle.name == profile.spec().name,
            "provider start differs from original bound create"
        );
        Ok(())
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum Phase {
    Prepared,
    Creating,
    NotAttempted,
    Bound { container_id: String },
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    operation: Uuid,
    intent: Intent,
    state: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    guard: Option<GuardRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    worker: Option<WorkerRecord>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GuardRecord {
    intent: super::guard::Intent,
    state: Phase,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRecord {
    intent: super::worker::Intent,
    state: Phase,
}
pub(super) struct Fence {
    file: File,
    record: Record,
}
fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    options
}
fn private(file: &File) -> Result<()> {
    let meta = file.metadata()?;
    ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.mode() & 0o077 == 0
            && meta.len() <= LIMIT as u64,
        "provider fence must be private, unshared and bounded"
    );
    Ok(())
}
fn read(file: &File) -> Result<Record> {
    use std::io::{Seek, SeekFrom};
    private(file)?;
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = vec![];
    file.take((LIMIT + 1) as u64).read_to_end(&mut bytes)?;
    let record: Record = canonical::decode(&bytes, LIMIT)?;
    ensure!(
        record.schema_version == 1
            && !record.operation.is_nil()
            && canonical::to_vec(&record)? == bytes
            && !matches!(&record.state,Phase::Bound{container_id} if !immutable_id(container_id)),
        "invalid provider fence"
    );
    let parent = record.intent.profile()?;
    if let Some(guard) = &record.guard {
        let Phase::Bound { container_id } = &record.state else {
            anyhow::bail!("guard requires a bound provider");
        };
        guard.intent.profile(&parent, container_id)?;
        ensure!(
            !matches!(&guard.state, Phase::Bound {container_id:id} if !immutable_id(id) || id == container_id),
            "invalid guard native identity"
        );
    }
    if let Some(worker) = &record.worker {
        let Some(GuardRecord {
            state: Phase::Bound { container_id },
            ..
        }) = &record.guard
        else {
            anyhow::bail!("worker requires a bound guard");
        };
        let Phase::Bound {
            container_id: parent_id,
        } = &record.state
        else {
            anyhow::bail!("worker requires bound provider");
        };
        let guard = record
            .guard
            .as_ref()
            .unwrap()
            .intent
            .profile(&parent, parent_id)?;
        worker.intent.profile(guard, container_id)?;
        ensure!(
            !matches!(&worker.state, Phase::Bound {container_id:id} if !immutable_id(id) || id == container_id || id == parent_id),
            "invalid worker native identity"
        );
    }
    Ok(record)
}
fn absent(lease: &ControllerLease, name: &str) -> Result<bool> {
    match fs::symlink_metadata(lease.directory().join(name)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
        Ok(_) => Ok(false),
    }
}
fn current_owner(lease: &ControllerLease) -> Result<()> {
    ensure!(
        lease.matches_directory(lease.directory()),
        "provider lost original OS lease"
    );
    Ok(())
}
impl Fence {
    pub(super) fn require_deployment(&self, deployment: &str) -> Result<()> {
        ensure!(
            self.record.intent.deployment == deployment,
            "provider journal belongs to another deployment"
        );
        Ok(())
    }
    pub(super) fn require_setup(&self, deployment: &str, scope: Scope, image: &str) -> Result<()> {
        self.require_deployment(deployment)?;
        ensure!(
            self.record.intent.scope == scope && self.record.intent.image == image,
            "provider identity/image change requires explicit migration before native recovery"
        );
        Ok(())
    }
    pub(super) fn begin(lease: &ControllerLease, profile: &Profile) -> Result<Self> {
        let _guard = lease.activity_guard()?;
        current_owner(lease)?;
        lease.require_provider_lane()?;
        ensure!(
            absent(lease, FENCE)? && absent(lease, NEXT)?,
            "provider recovery must finish before replacement"
        );
        let record = Record {
            schema_version: 1,
            operation: Uuid::new_v4(),
            intent: profile.intent(),
            state: Phase::Prepared,
            guard: None,
            worker: None,
        };
        let bytes = canonical::to_vec(&record)?;
        ensure!(bytes.len() <= LIMIT, "provider fence bounds");
        let mut file = options()
            .create_new(true)
            .open(lease.directory().join(FENCE))?;
        file.write_all(&bytes)?;
        crate::marketplace::provider_state::sync(&file)?;
        File::open(lease.directory())?.sync_all()?;
        Ok(Self { file, record })
    }
    pub(super) fn load(lease: &ControllerLease) -> Result<Option<Self>> {
        let _guard = lease.activity_guard()?;
        current_owner(lease)?;
        // A pending atomic replacement is not ignored or a start permission.
        // Recover only a complete same-operation allowed successor.
        let file = match options().open(lease.directory().join(FENCE)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ensure!(
                    absent(lease, NEXT)?,
                    "provider has an orphaned publication; explicit review required"
                );
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let record = read(&file)?;
        let mut fence = Self { file, record };
        if !absent(lease, NEXT)? {
            let next = options().open(lease.directory().join(NEXT))?;
            let candidate = read(&next)?;
            ensure!(
                candidate.operation == fence.record.operation
                    && candidate.intent == fence.record.intent
                    && successor(&fence.record, &candidate),
                "provider pending transition conflicts"
            );
            crate::marketplace::provider_state::sync(&next)?;
            fs::rename(lease.directory().join(NEXT), lease.directory().join(FENCE))?;
            File::open(lease.directory())?.sync_all()?;
            fence.file = options().open(lease.directory().join(FENCE))?;
            fence.record = candidate;
        }
        fence.current(lease)?;
        Ok(Some(fence))
    }
    pub(super) fn current(&self, lease: &ControllerLease) -> Result<()> {
        current_owner(lease)?;
        private(&self.file)?;
        let path = fs::symlink_metadata(lease.directory().join(FENCE))?;
        let held = self.file.metadata()?;
        ensure!(
            path.is_file()
                && path.dev() == held.dev()
                && path.ino() == held.ino()
                && path.nlink() == 1,
            "provider fence inode changed"
        );
        ensure!(
            read(&self.file)? == self.record,
            "provider fence contents changed"
        );
        Ok(())
    }
    fn write(&mut self, lease: &ControllerLease, state: Phase) -> Result<()> {
        self.publish(
            lease,
            Record {
                state,
                ..self.record.clone()
            },
        )
    }
    fn publish(&mut self, lease: &ControllerLease, record: Record) -> Result<()> {
        self.current(lease)?;
        ensure!(
            absent(lease, NEXT)?,
            "provider publication is already pending"
        );
        let bytes = canonical::to_vec(&record)?;
        ensure!(bytes.len() <= LIMIT, "provider fence bounds");
        let mut next = options()
            .create_new(true)
            .open(lease.directory().join(NEXT))?;
        next.write_all(&bytes)?;
        crate::marketplace::provider_state::sync(&next)?;
        current_owner(lease)?;
        fs::rename(lease.directory().join(NEXT), lease.directory().join(FENCE))?;
        File::open(lease.directory())?.sync_all()?;
        self.file = options().open(lease.directory().join(FENCE))?;
        self.record = record;
        self.current(lease)
    }
    pub(super) fn begin_guard(
        &mut self,
        lease: &ControllerLease,
        profile: &super::guard::Profile,
    ) -> Result<()> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        let Phase::Bound { container_id } = &self.record.state else {
            anyhow::bail!("guard requires bound parent");
        };
        ensure!(
            self.record.guard.is_none(),
            "guard recovery required before replacement"
        );
        profile
            .intent
            .profile(&self.record.intent.profile()?, container_id)?;
        self.publish(
            lease,
            Record {
                guard: Some(GuardRecord {
                    intent: profile.intent.clone(),
                    state: Phase::Prepared,
                }),
                ..self.record.clone()
            },
        )
    }
    fn guard_profile(&self) -> Result<super::guard::Profile> {
        let Phase::Bound { container_id } = &self.record.state else {
            anyhow::bail!("guard lost bound parent");
        };
        self.record
            .guard
            .as_ref()
            .context("guard record missing")?
            .intent
            .profile(&self.record.intent.profile()?, container_id)
    }
    fn guard_state(&mut self, lease: &ControllerLease, state: Phase) -> Result<()> {
        let mut record = self.record.clone();
        record.guard.as_mut().context("guard record missing")?.state = state;
        self.publish(lease, record)
    }
    pub(super) fn creating_guard(
        &mut self,
        lease: &ControllerLease,
    ) -> Result<super::guard::GuardCreateAttempt> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        ensure!(
            matches!(
                self.record.guard.as_ref().map(|g| &g.state),
                Some(Phase::Prepared)
            ),
            "guard create already attempted"
        );
        let profile = self.guard_profile()?;
        self.guard_state(lease, Phase::Creating)?;
        Ok(super::guard::GuardCreateAttempt {
            operation: self.record.operation,
            fingerprint: profile.spec().labels[CONTAINER_SPEC_HASH_LABEL].clone(),
        })
    }
    pub(super) fn bind_guard(
        &mut self,
        lease: &ControllerLease,
        handle: &ContainerHandle,
    ) -> Result<super::guard::GuardStartAttempt> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        let profile = self.guard_profile()?;
        ensure!(
            matches!(
                self.record.guard.as_ref().map(|g| &g.state),
                Some(Phase::Creating)
            ) && immutable_id(&handle.id)
                && handle.name == profile.spec().name
                && handle.id != profile.parent().1.id,
            "guard bind differs from original create"
        );
        self.guard_state(
            lease,
            Phase::Bound {
                container_id: handle.id.clone(),
            },
        )?;
        Ok(super::guard::GuardStartAttempt {
            id: handle.id.clone(),
            fingerprint: profile.spec().labels[CONTAINER_SPEC_HASH_LABEL].clone(),
        })
    }
    pub(super) fn guard_not_attempted(
        &mut self,
        lease: &ControllerLease,
        outcome: crate::runtime::provider_creation::GuardNotAttempted,
    ) -> Result<anyhow::Error> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        ensure!(
            matches!(
                self.record.guard.as_ref().map(|g| &g.state),
                Some(Phase::Creating)
            ),
            "guard rejection phase differs"
        );
        outcome.require(self.record.operation, &self.guard_profile()?)?;
        self.guard_state(lease, Phase::NotAttempted)?;
        Ok(outcome.into_error())
    }
    pub(super) fn guard_namespace(&self, lease: &ControllerLease) -> Result<u64> {
        self.current(lease)?;
        let guard = self
            .record
            .guard
            .as_ref()
            .context("guard native record missing")?;
        ensure!(
            matches!(guard.state, Phase::Bound { .. }),
            "guard has no original bound native identity"
        );
        Ok(guard.intent.namespace)
    }
    pub(super) fn require_guard_binding(
        &self,
        lease: &ControllerLease,
        handle: &ContainerHandle,
        fingerprint: &str,
        namespace: u64,
    ) -> Result<()> {
        self.current(lease)?;
        let guard = self
            .record
            .guard
            .as_ref()
            .context("guard original record missing")?;
        let profile = self.guard_profile()?;
        ensure!(
            matches!(&guard.state,Phase::Bound{container_id} if container_id==&handle.id)
                && handle.name == profile.spec().name
                && namespace == guard.intent.namespace
                && profile.spec().labels[CONTAINER_SPEC_HASH_LABEL] == fingerprint,
            "guard handoff differs from original bound child"
        );
        Ok(())
    }
    pub(super) async fn retire_guard(
        &mut self,
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<()> {
        self.current(lease)?;
        // The worker is another namespace holder. Failed/unknown child cleanup
        // MUST preserve both guard and provider, not merely their journal.
        self.retire_worker(runtime, lease).await?;
        let Some(guard) = &self.record.guard else {
            return Ok(());
        };
        let profile = self.guard_profile()?;
        if matches!(guard.state, Phase::Creating) {
            // Unknown CREATE cannot be resolved by a missing name, even when
            // the provider has already exited. Only exact unstarted recovery.
            let handle = runtime.inspect_unstarted_provider_guard(&profile).await?;
            let _ = self.bind_guard(lease, &handle)?;
        }
        if let Phase::Bound { container_id } = &self.record.guard.as_ref().unwrap().state {
            let handle = ContainerHandle {
                id: container_id.clone(),
                name: profile.spec().name.clone(),
            };
            if let Some(actual) = runtime.get_container_handle(&handle.id).await? {
                ensure!(actual.id == handle.id, "guard recovery identity differs");
                runtime.stop_provider_guard(&handle, &profile).await?;
                runtime.remove_provider_guard(&handle, &profile).await?;
            }
        }
        let _lock = lease.activity_guard()?;
        self.publish(
            lease,
            Record {
                guard: None,
                ..self.record.clone()
            },
        )
    }
    fn worker_profile(&self) -> Result<super::worker::Profile> {
        let Some(GuardRecord {
            state: Phase::Bound { container_id },
            ..
        }) = &self.record.guard
        else {
            anyhow::bail!("worker lost bound guard");
        };
        self.record
            .worker
            .as_ref()
            .context("worker record missing")?
            .intent
            .profile(self.guard_profile()?, container_id)
    }
    pub(super) fn begin_worker(
        &mut self,
        lease: &ControllerLease,
        image: &str,
        issuer_keys: &[elixir_marketplace::identity::PublicKey],
    ) -> Result<super::worker::Profile> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        ensure!(
            self.record.worker.is_none(),
            "worker recovery required before replacement"
        );
        let Some(GuardRecord {
            state: Phase::Bound { container_id },
            ..
        }) = &self.record.guard
        else {
            anyhow::bail!("worker requires bound guard");
        };
        let profile = super::worker::Profile::compile(
            self.guard_profile()?,
            container_id,
            image,
            issuer_keys,
        )?;
        self.publish(
            lease,
            Record {
                worker: Some(WorkerRecord {
                    intent: profile.intent.clone(),
                    state: Phase::Prepared,
                }),
                ..self.record.clone()
            },
        )?;
        Ok(profile)
    }
    fn worker_state(&mut self, lease: &ControllerLease, state: Phase) -> Result<()> {
        let mut record = self.record.clone();
        record
            .worker
            .as_mut()
            .context("worker record missing")?
            .state = state;
        self.publish(lease, record)
    }
    pub(super) fn creating_worker(
        &mut self,
        lease: &ControllerLease,
    ) -> Result<super::worker::WorkerCreateAttempt> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        ensure!(
            matches!(
                self.record.worker.as_ref().map(|w| &w.state),
                Some(Phase::Prepared)
            ),
            "worker create already attempted"
        );
        let profile = self.worker_profile()?;
        self.worker_state(lease, Phase::Creating)?;
        Ok(super::worker::WorkerCreateAttempt {
            operation: self.record.operation,
            fingerprint: profile.spec().labels[CONTAINER_SPEC_HASH_LABEL].clone(),
        })
    }
    pub(super) fn bind_worker(
        &mut self,
        lease: &ControllerLease,
        handle: &ContainerHandle,
    ) -> Result<super::worker::WorkerStartAttempt> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        let profile = self.worker_profile()?;
        ensure!(
            matches!(
                self.record.worker.as_ref().map(|w| &w.state),
                Some(Phase::Creating)
            ) && immutable_id(&handle.id)
                && handle.name == profile.spec().name
                && handle.id != profile.parent().1.id
                && handle.id != profile.parent().0.parent().1.id,
            "worker bind differs from original create"
        );
        self.worker_state(
            lease,
            Phase::Bound {
                container_id: handle.id.clone(),
            },
        )?;
        Ok(super::worker::WorkerStartAttempt {
            id: handle.id.clone(),
            fingerprint: profile.spec().labels[CONTAINER_SPEC_HASH_LABEL].clone(),
        })
    }
    pub(super) fn worker_not_attempted(
        &mut self,
        lease: &ControllerLease,
        outcome: crate::runtime::provider_creation::WorkerNotAttempted,
    ) -> Result<anyhow::Error> {
        let _lock = lease.activity_guard()?;
        self.current(lease)?;
        ensure!(
            matches!(
                self.record.worker.as_ref().map(|w| &w.state),
                Some(Phase::Creating)
            ),
            "worker rejection phase differs"
        );
        outcome.require(self.record.operation, &self.worker_profile()?)?;
        self.worker_state(lease, Phase::NotAttempted)?;
        Ok(outcome.into_error())
    }
    pub(super) async fn retire_worker(
        &mut self,
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<()> {
        self.current(lease)?;
        let Some(worker) = &self.record.worker else {
            return Ok(());
        };
        let profile = self.worker_profile()?;
        if matches!(worker.state, Phase::Creating) {
            let handle = runtime.inspect_unstarted_provider_worker(&profile).await?;
            let _ = self.bind_worker(lease, &handle)?;
        }
        if let Phase::Bound { container_id } = &self.record.worker.as_ref().unwrap().state {
            let handle = ContainerHandle {
                id: container_id.clone(),
                name: profile.spec().name.clone(),
            };
            if let Some(actual) = runtime.get_container_handle(&handle.id).await? {
                ensure!(actual.id == handle.id, "worker recovery identity differs");
                runtime.stop_provider_worker(&handle, &profile).await?;
                runtime.remove_provider_worker(&handle, &profile).await?;
            }
        }
        let _lock = lease.activity_guard()?;
        self.publish(
            lease,
            Record {
                worker: None,
                ..self.record.clone()
            },
        )
    }
    pub(super) fn creating(&mut self, lease: &ControllerLease) -> Result<CreateAttempt> {
        let _guard = lease.activity_guard()?;
        ensure!(
            matches!(self.record.state, Phase::Prepared),
            "provider create already attempted"
        );
        self.write(lease, Phase::Creating)?;
        Ok(CreateAttempt {
            operation: self.record.operation,
            fingerprint: self.record.intent.fingerprint.clone(),
        })
    }
    pub(super) fn bind(
        &mut self,
        lease: &ControllerLease,
        handle: &ContainerHandle,
    ) -> Result<StartAttempt> {
        let _guard = lease.activity_guard()?;
        ensure!(
            matches!(self.record.state, Phase::Creating)
                && immutable_id(&handle.id)
                && handle.name == self.record.intent.profile()?.spec().name,
            "provider bind differs from original create"
        );
        self.write(
            lease,
            Phase::Bound {
                container_id: handle.id.clone(),
            },
        )?;
        Ok(StartAttempt {
            fingerprint: self.record.intent.fingerprint.clone(),
            id: handle.id.clone(),
        })
    }
    pub(super) fn not_attempted(
        &mut self,
        lease: &ControllerLease,
        outcome: crate::runtime::provider_creation::NotAttempted,
    ) -> Result<anyhow::Error> {
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        ensure!(
            matches!(self.record.state, Phase::Creating),
            "provider rejection phase differs"
        );
        outcome.require(self.record.operation, &self.record.intent.profile()?)?;
        self.write(lease, Phase::NotAttempted)?;
        Ok(outcome.into_error())
    }
    pub(super) async fn retire(
        &mut self,
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<()> {
        self.current(lease)?;
        self.retire_guard(runtime, lease).await?;
        let profile = self.record.intent.profile()?;
        if matches!(self.record.state, Phase::Creating) {
            // Never clear an uncertain create by name absence. Only an exact,
            // never-started native result can supply its previously unknown CID.
            let handle = runtime.inspect_unstarted_provider(&profile).await?;
            let _ = self.bind(lease, &handle)?;
        }
        if let Phase::Bound { container_id } = &self.record.state {
            let handle = ContainerHandle {
                id: container_id.clone(),
                name: profile.spec().name.clone(),
            };
            if let Some(actual) = runtime.get_container_handle(&handle.id).await? {
                ensure!(
                    actual.id == handle.id,
                    "provider recovery returned another native identity"
                );
                runtime.stop_provider(&handle, &profile).await?;
                runtime.remove_provider(&handle, &profile).await?;
            }
        }
        let _guard = lease.activity_guard()?;
        self.current(lease)?;
        ensure!(absent(lease, NEXT)?, "provider publication unresolved");
        fs::remove_file(lease.directory().join(FENCE))?;
        File::open(lease.directory())?.sync_all()?;
        Ok(())
    }
    pub(super) async fn recover(
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<bool> {
        let Some(mut fence) = Self::load(lease)? else {
            return Ok(false);
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(40),
            fence.retire(runtime, lease),
        )
        .await
        .context("provider recovery deadline")??;
        Ok(true)
    }
}

fn phase_successor(before: &Phase, after: &Phase) -> bool {
    matches!(
        (before, after),
        (Phase::Prepared, Phase::Creating)
            | (Phase::Creating, Phase::NotAttempted)
            | (Phase::Creating, Phase::Bound { .. })
    )
}
fn successor(before: &Record, after: &Record) -> bool {
    if before.worker != after.worker {
        if before.state != after.state
            || before.guard != after.guard
            || !matches!(
                before.guard.as_ref().map(|g| &g.state),
                Some(Phase::Bound { .. })
            )
        {
            return false;
        }
        return match (&before.worker, &after.worker) {
            (None, Some(next)) => matches!(next.state, Phase::Prepared),
            (Some(old), Some(next)) => {
                old.intent == next.intent && phase_successor(&old.state, &next.state)
            }
            (Some(old), None) => matches!(
                old.state,
                Phase::Prepared | Phase::NotAttempted | Phase::Bound { .. }
            ),
            _ => false,
        };
    }
    // Neither ancestor can advance/retire while the worker record is retained.
    if before.worker.is_some() {
        return false;
    }
    if before.guard.is_none()
        && after.guard.is_none()
        && phase_successor(&before.state, &after.state)
    {
        return true;
    }
    if before.state != after.state || !matches!(before.state, Phase::Bound { .. }) {
        return false;
    }
    match (&before.guard, &after.guard) {
        (None, Some(next)) => matches!(next.state, Phase::Prepared),
        (Some(old), Some(next)) => {
            old.intent == next.intent && phase_successor(&old.state, &next.state)
        }
        (Some(old), None) => matches!(
            old.state,
            Phase::Prepared | Phase::NotAttempted | Phase::Bound { .. }
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn worker_unknown_create_holds_both_ancestors_and_tokens_bind_only_original_cid()
    -> Result<()> {
        use crate::runtime::{docker::DockerRuntimeManager, provider_creation::WorkerCreation};
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let lease = ControllerLease::acquire(&directory)?;
        let (mut fence, parent, handle) = bound(&lease)?;
        let guard = guard(&parent, &handle)?;
        fence.begin_guard(&lease, &guard)?;
        let _ = fence.creating_guard(&lease)?;
        let child = ContainerHandle {
            id: "e".repeat(64),
            name: guard.spec().name.clone(),
        };
        let _ = fence.bind_guard(&lease, &child)?;
        let keys = (0..4)
            .map(|i| {
                elixir_marketplace::identity::PublicKey::from_bytes(
                    ed25519_dalek::SigningKey::from_bytes(&[i + 1; 32])
                        .verifying_key()
                        .to_bytes(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let worker = fence.begin_worker(&lease, &format!("sha256:{}", "f".repeat(64)), &keys)?;
        assert_eq!(
            worker.spec().network_mode,
            Some(format!("container:{}", child.id))
        );
        assert!(canonical::to_vec(&fence.record)?.len() <= LIMIT);
        let attempt = fence.creating_worker(&lease)?;
        assert!(fence.creating_worker(&lease).is_err());
        let unavailable = DockerRuntimeManager::new(Some(
            root.path().join("missing-docker").to_string_lossy().into(),
        ));
        assert!(fence.retire_guard(&unavailable, &lease).await.is_err());
        assert!(matches!(
            fence.record.guard.as_ref().unwrap().state,
            Phase::Bound { .. }
        ));
        assert!(matches!(fence.record.state, Phase::Bound { .. }));
        let WorkerCreation::NotAttempted(outcome) =
            unavailable.create_provider_worker(&worker, attempt).await?
        else {
            anyhow::bail!("unexpected worker fixture create");
        };
        let _ = fence.worker_not_attempted(&lease, outcome)?;
        fence.retire_worker(&unavailable, &lease).await?;
        assert!(fence.record.worker.is_none());
        let _ = fence.begin_worker(&lease, &worker.spec().image, &keys)?;
        let _ = fence.creating_worker(&lease)?;
        assert!(fence.bind_worker(&lease, &child).is_err());
        let worker_id = ContainerHandle {
            id: "1".repeat(64),
            name: worker.spec().name.clone(),
        };
        let start = fence.bind_worker(&lease, &worker_id)?;
        start.require(&worker, &worker_id)?;
        assert!(
            start
                .require(
                    &worker,
                    &ContainerHandle {
                        id: "2".repeat(64),
                        name: worker_id.name.clone()
                    }
                )
                .is_err()
        );
        let loaded = Fence::load(&lease)?.context("worker record lost")?;
        assert!(matches!(
            loaded.record.worker.as_ref().unwrap().state,
            Phase::Bound { .. }
        ));
        assert!(!successor(
            &loaded.record,
            &Record {
                guard: None,
                worker: None,
                ..loaded.record.clone()
            }
        ));
        let mut unknown = loaded.record.clone();
        unknown.worker.as_mut().unwrap().state = Phase::Creating;
        assert!(!successor(
            &unknown,
            &Record {
                worker: None,
                ..unknown.clone()
            }
        ));
        let mut mismatch = loaded.record.clone();
        mismatch.guard = None;
        let bytes = canonical::to_vec(&mismatch)?;
        let bad = root.path().join("bad-record");
        let mut file = options().create_new(true).open(bad)?;
        file.write_all(&bytes)?;
        assert!(read(&file).is_err());
        Ok(())
    }
    fn profile() -> Result<Profile> {
        Profile::fixture(
            "fence-fixture",
            &format!("sha256:{}", "a".repeat(64)),
            &"b".repeat(64),
        )
    }
    fn bound(lease: &ControllerLease) -> Result<(Fence, Profile, ContainerHandle)> {
        let p = profile()?;
        let mut f = Fence::begin(lease, &p)?;
        let _ = f.creating(lease)?;
        let parent = ContainerHandle {
            id: "c".repeat(64),
            name: p.spec().name.clone(),
        };
        let _ = f.bind(lease, &parent)?;
        Ok((f, p, parent))
    }
    fn guard(parent: &Profile, handle: &ContainerHandle) -> Result<super::super::guard::Profile> {
        super::super::guard::Profile::compile(
            parent,
            &handle.id,
            12345,
            &format!("sha256:{}", "d".repeat(64)),
            super::super::guard::tests::policy(),
            elixir_marketplace::policy::OrdinaryLimits::default(),
        )
    }
    #[tokio::test]
    async fn child_guard_unknown_outcomes_preserve_parent_and_start_tokens_bind_only_original_child()
    -> Result<()> {
        use crate::runtime::{docker::DockerRuntimeManager, provider_creation::GuardCreation};
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let lease = ControllerLease::acquire(&directory)?;
        let (mut f, p, parent) = bound(&lease)?;
        let g = guard(&p, &parent)?;
        f.begin_guard(&lease, &g)?;
        let attempt = f.creating_guard(&lease)?;
        assert!(f.creating_guard(&lease).is_err());
        let unavailable =
            DockerRuntimeManager::new(Some(root.path().join("no-docker").to_string_lossy().into()));
        assert!(f.retire(&unavailable, &lease).await.is_err());
        assert!(matches!(f.record.state, Phase::Bound { .. }));
        assert!(matches!(
            f.record.guard.as_ref().unwrap().state,
            Phase::Creating
        ));
        let GuardCreation::NotAttempted(outcome) =
            unavailable.create_provider_guard(&g, attempt).await?
        else {
            anyhow::bail!("unexpected fixture create");
        };
        let _ = f.guard_not_attempted(&lease, outcome)?;
        f.retire_guard(&unavailable, &lease).await?;
        assert!(f.record.guard.is_none());
        assert!(directory.join(FENCE).is_file());
        f.begin_guard(&lease, &g)?;
        let _ = f.creating_guard(&lease)?;
        assert!(f.bind_guard(&lease, &parent).is_err());
        let child = ContainerHandle {
            id: "e".repeat(64),
            name: g.spec().name.clone(),
        };
        let start = f.bind_guard(&lease, &child)?;
        start.require(&g, &child)?;
        f.require_guard_binding(
            &lease,
            &child,
            &g.spec().labels[CONTAINER_SPEC_HASH_LABEL],
            12345,
        )?;
        assert!(
            f.require_guard_binding(
                &lease,
                &child,
                &g.spec().labels[CONTAINER_SPEC_HASH_LABEL],
                12346
            )
            .is_err()
        );
        assert!(
            f.require_guard_binding(
                &lease,
                &ContainerHandle {
                    id: "f".repeat(64),
                    name: child.name.clone()
                },
                &g.spec().labels[CONTAINER_SPEC_HASH_LABEL],
                12345
            )
            .is_err()
        );
        assert!(
            start
                .require(
                    &g,
                    &ContainerHandle {
                        id: "f".repeat(64),
                        name: child.name.clone()
                    }
                )
                .is_err()
        );
        assert_eq!(f.guard_namespace(&lease)?, 12345);
        assert!(canonical::to_vec(&f.record)?.len() < LIMIT);
        drop(f);
        drop(lease);
        assert!(ControllerLease::acquire(&directory).is_err());
        let lease = ControllerLease::acquire_for_recovery(&directory)?;
        let mut f = Fence::load(&lease)?.unwrap();
        assert!(f.retire(&unavailable, &lease).await.is_err());
        assert!(f.record.guard.is_some());
        assert!(directory.join(FENCE).is_file());
        Ok(())
    }
    #[test]
    fn child_guard_atomic_successors_cannot_clear_an_uncertain_create() -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let lease = ControllerLease::acquire(&directory)?;
        let (mut f, p, parent) = bound(&lease)?;
        f.begin_guard(&lease, &guard(&p, &parent)?)?;
        let mut next = f.record.clone();
        next.guard.as_mut().unwrap().state = Phase::Creating;
        let mut file = options().create_new(true).open(directory.join(NEXT))?;
        file.write_all(&canonical::to_vec(&next)?)?;
        file.sync_all()?;
        let loaded = Fence::load(&lease)?.unwrap();
        assert!(matches!(
            loaded.record.guard.as_ref().unwrap().state,
            Phase::Creating
        ));
        let mut invalid = loaded.record.clone();
        invalid.guard = None;
        let mut file = options().create_new(true).open(directory.join(NEXT))?;
        file.write_all(&canonical::to_vec(&invalid)?)?;
        file.sync_all()?;
        assert!(Fence::load(&lease).is_err());
        assert!(directory.join(NEXT).is_file());
        Ok(())
    }
    #[test]
    fn create_and_start_are_one_use_original_profile_tokens_and_restart_requires_recovery()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let lease = ControllerLease::acquire(&directory)?;
        let profile = profile()?;
        let mut fence = Fence::begin(&lease, &profile)?;
        let attempt = fence.creating(&lease)?;
        assert!(attempt.require(Uuid::new_v4(), &profile).is_err());
        assert!(fence.creating(&lease).is_err());
        let handle = ContainerHandle {
            id: "c".repeat(64),
            name: profile.spec().name.clone(),
        };
        let start = fence.bind(&lease, &handle)?;
        start.require(&profile, &handle)?;
        assert!(
            start
                .require(
                    &profile,
                    &ContainerHandle {
                        id: "d".repeat(64),
                        name: handle.name.clone()
                    }
                )
                .is_err()
        );
        drop(fence);
        drop(lease);
        assert!(ControllerLease::acquire(&directory).is_err());
        let lease = ControllerLease::acquire_for_recovery(&directory)?;
        let recovered = Fence::load(&lease)?.unwrap();
        assert!(matches!(recovered.record.state, Phase::Bound { .. }));
        assert!(Fence::begin(&lease, &profile).is_err());
        Ok(())
    }
    #[test]
    fn complete_atomic_successors_recover_but_conflicts_corruption_and_unrelated_work_are_preserved()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("provider");
        let lease = ControllerLease::acquire(&directory)?;
        let p = profile()?;
        let fence = Fence::begin(&lease, &p)?;
        let successor = Record {
            state: Phase::Creating,
            ..fence.record.clone()
        };
        let mut next = options().create_new(true).open(directory.join(NEXT))?;
        next.write_all(&canonical::to_vec(&successor)?)?;
        next.sync_all()?;
        let loaded = Fence::load(&lease)?.unwrap();
        assert!(matches!(loaded.record.state, Phase::Creating));
        assert!(!directory.join(NEXT).exists());
        let mut bad = successor;
        bad.operation = Uuid::new_v4();
        bad.state = Phase::NotAttempted;
        let mut next = options().create_new(true).open(directory.join(NEXT))?;
        next.write_all(&canonical::to_vec(&bad)?)?;
        assert!(Fence::load(&lease).is_err());
        assert!(directory.join(NEXT).exists());
        let root2 = tempfile::tempdir()?;
        let directory2 = root2.path().canonicalize()?.join("provider");
        let lease2 = ControllerLease::acquire(&directory2)?;
        fs::write(
            directory2.join("native-job.pending"),
            b"unrelated interrupted work",
        )?;
        assert!(Fence::begin(&lease2, &p).is_err());
        assert!(directory2.join("native-job.pending").is_file());
        Ok(())
    }
}
