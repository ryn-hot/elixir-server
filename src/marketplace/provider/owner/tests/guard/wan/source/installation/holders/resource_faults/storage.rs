//! A real OS permission failure, restricted to this fixture's retained archive.
//! This is not ENOSPC or a replacement for physical-storage qualification.
use super::*;
use std::{
    fs::{File, Permissions},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};

struct UnwritableDirectory {
    directory: File,
    original: Permissions,
    restored: bool,
}
impl UnwritableDirectory {
    fn apply(root: &Path, path: &Path) -> Result<Self> {
        let root = root.canonicalize()?;
        let owner = std::fs::symlink_metadata(&root)?;
        ensure!(
            owner.is_dir()
                && owner.uid() == unsafe { libc::geteuid() }
                && owner.mode() & 0o077 == 0,
            "fault requires the original private fixture root"
        );
        ensure!(
            path.is_absolute() && path.starts_with(&root) && path != root,
            "fault must be strictly inside the original private fixture"
        );
        ensure!(
            path.canonicalize()? == path,
            "fault cannot follow a linked directory"
        );
        let before = std::fs::symlink_metadata(path)?;
        ensure!(
            before.is_dir()
                && !before.file_type().is_symlink()
                && before.uid() == unsafe { libc::geteuid() }
                && before.uid() != 0
                && before.mode() & 0o7777 == 0o700,
            "fault requires an owned private directory and non-root user"
        );
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let opened = directory.metadata()?;
        ensure!(
            opened.ino() == before.ino() && opened.dev() == before.dev(),
            "fault directory changed"
        );
        let mut fault = Self {
            directory,
            original: opened.permissions(),
            restored: false,
        };
        fault
            .directory
            .set_permissions(Permissions::from_mode(0o500))?;
        let probe = path.join(format!(".owned-storage-probe-{}", Uuid::new_v4()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&probe)
        {
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(fault),
            result => {
                fault.restore()?;
                if let Ok(file) = result {
                    drop(file);
                    std::fs::remove_file(probe)?;
                }
                bail!("fixture did not produce a real OS storage permission failure")
            }
        }
    }
    fn restore(&mut self) -> Result<()> {
        self.directory.set_permissions(self.original.clone())?;
        ensure!(
            self.directory.metadata()?.mode() & 0o7777 == self.original.mode() & 0o7777,
            "original fixture directory mode was not restored"
        );
        self.restored = true;
        Ok(())
    }
}
impl Drop for UnwritableDirectory {
    fn drop(&mut self) {
        if !self.restored
            && self
                .directory
                .set_permissions(self.original.clone())
                .is_err()
        {
            eprintln!("fixture storage permission restoration failed");
        }
    }
}

fn private_directory(root: &tempfile::TempDir, name: &str) -> Result<PathBuf> {
    let path = root.path().canonicalize()?.join(name);
    std::fs::create_dir(&path)?;
    std::fs::set_permissions(&path, Permissions::from_mode(0o700))?;
    Ok(path)
}

#[test]
fn storage_permission_fault_is_real_and_restores_original_inode_on_unwind() -> Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::set_permissions(root.path(), Permissions::from_mode(0o700))?;
    let path = private_directory(&root, "retained")?;
    let original = std::fs::metadata(&path)?;
    {
        let mut fault = UnwritableDirectory::apply(root.path(), &path)?;
        assert_eq!(std::fs::metadata(&path)?.mode() & 0o777, 0o500);
        fault.restore()?;
    }
    let unwind = std::panic::catch_unwind(|| {
        let _fault = UnwritableDirectory::apply(root.path(), &path).unwrap();
        panic!("deliberate fixture unwind");
    });
    assert!(unwind.is_err());
    let restored = std::fs::metadata(&path)?;
    assert_eq!(
        (restored.dev(), restored.ino(), restored.mode()),
        (original.dev(), original.ino(), original.mode())
    );
    assert_eq!(std::fs::read_dir(path)?.count(), 0);
    Ok(())
}

#[test]
fn storage_permission_fault_refuses_foreign_linked_or_nonprivate_directories() -> Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::set_permissions(root.path(), Permissions::from_mode(0o700))?;
    let foreign = tempfile::tempdir()?;
    let outside = private_directory(&foreign, "outside")?;
    assert!(UnwritableDirectory::apply(root.path(), &outside).is_err());
    let linked = root.path().canonicalize()?.join("linked");
    std::os::unix::fs::symlink(&outside, &linked)?;
    assert!(UnwritableDirectory::apply(root.path(), &linked).is_err());
    let public = private_directory(&root, "public")?;
    std::fs::set_permissions(&public, Permissions::from_mode(0o755))?;
    assert!(UnwritableDirectory::apply(root.path(), &public).is_err());
    assert_eq!(std::fs::metadata(outside)?.mode() & 0o777, 0o700);
    assert_eq!(std::fs::metadata(public)?.mode() & 0o777, 0o755);
    Ok(())
}

async fn installed_record(
    actor: &Participant,
    source: &SourceFixture,
    instance: Uuid,
) -> Result<(i64, String)> {
    sqlx::query_as("SELECT revision,record_json FROM marketplace_installations WHERE extension_id=$1 AND instance_id=$2")
        .bind(source.publication.release.payload.extension_id.as_str()).bind(instance.to_string())
        .fetch_one(&actor.rig.database.pool).await.map_err(Into::into)
}

async fn begin_update(
    actor: &Participant,
    source: &SourceFixture,
    instance: Uuid,
) -> Result<(Uuid, Value)> {
    let publication = &source.publication;
    let targets = target_choices(
        actor.app()?,
        actor.token()?,
        publication.release.payload.extension_id.as_str(),
        Duration::from_secs(30),
    )
    .await?;
    let row = targets["instances"]
        .as_array()
        .context("instance choices missing")?
        .iter()
        .find(|row| row["instance_id"] == json!(instance))
        .context("original configured instance missing")?;
    ensure!(
        row["pending"] == false,
        "original configured instance still pending"
    );
    let request = json!({"schema_version":1,"request_id":Uuid::new_v4(),"publication":publication,
        "accepted_reference":Digest::of(publication.to_bytes()?),"accept_unqualified":true,"upload_source":true,
        "target":{"kind":"existing","instance_id":instance},"expected_revision":row["expected_revision"],
        "mutation_id":Uuid::new_v4(),"controls":row["controls"]});
    let started = http(
        actor.app()?,
        actor.token()?,
        "POST",
        "/api/v1/marketplace/installations/direct",
        Some(request),
    )
    .await?;
    let id = serde_json::from_value(started["operation_id"].clone())?;
    let review = phase(
        actor.app()?,
        actor.token()?,
        id,
        "awaiting_approval",
        Some(source),
    )
    .await?;
    ensure!(
        review["review_id"].is_string() && review["approval_revision"].is_i64(),
        "real update review missing"
    );
    Ok((id, review))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "standing-approved one free original identity; approved dummy only; real storage permission failure and authenticated cancellation/retry"]
async fn unavailable_archive_preserves_installed_source_and_manual_update_retries() -> Result<()> {
    for flag in ["WARP_LIFECYCLE", "WAN", "PUBLIC_SOURCE", "STORAGE_UPDATE"] {
        ensure!(
            std::env::var(format!("ELIXIR_MARKETPLACE_{flag}_LIVE_TEST")).as_deref() == Ok("1"),
            "explicit isolated storage-fault qualification required"
        );
    }
    let sources = configured_fixtures()?;
    let image = immutable_image("ELIXIR_MARKETPLACE_WARP_PROVIDER_IMAGE")?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let mut actor = Participant::new(runtime, &image).await?;
    eprintln!(
        "{}",
        json!({"stage":"resource_fault_layout","deployment":actor.rig.deployment,"recovery_root":actor.rig.root.path()})
    );
    let operation = std::panic::AssertUnwindSafe(Box::pin(async {
        let mut allocation = crate::marketplace::application::fixture_allocation();
        allocation.source.cpu_millicores = 200; allocation.source.wall_seconds = 600; allocation.validate()?;
        actor.start(&guard, &worker, Digest::of(Uuid::new_v4().as_bytes()), false, &allocation).await?;
        let instance = install_with_updates(actor.app()?, actor.state()?, actor.token()?, &sources[0], None, true, false).await?;
        let user = actor.principal.as_ref().context("original owner missing")?.user_id;
        provider(actor.state()?, &actor.rig.database, user, instance, "1.0.0").await?;
        let original = installed_record(&actor, &sources[0], instance).await?;
        let original_grant = grant(&actor, &sources[0]).await?;
        ensure!(original_grant.2 == 0, "storage fixture must not enable automatic updates");
        let (id, review) = begin_update(&actor, &sources[1], instance).await?;
        eprintln!("{}", json!({"stage":"storage_update_review_ready"}));
        let paths = crate::runtime::RuntimePaths::from_roots(&actor.state()?.settings.extensions.storage_root, &actor.state()?.settings.library.local_root);
        let path = Path::new(&paths.data_root).canonicalize()?.join("marketplace/worker/source-archives/releases");
        let mut fault = UnwritableDirectory::apply(actor.rig.root.path(), &path)?;
        eprintln!("{}", json!({"stage":"storage_update_fault_applied","actual_permission_denied":true,"physical_disk_full":false}));
        action(actor.app()?, actor.token()?, id, json!({"action":"approve","review_id":review["review_id"],
            "expected_revision":review["approval_revision"],"mutation_id":Uuid::new_v4(),"automatic_updates":false})).await?;
        phase(actor.app()?, actor.token()?, id, "recovery_required", None).await?;
        let failure = actor.state()?.marketplace.fixture_original_preparation_failure()?.context("original storage failure missing")?;
        ensure!(failure.chain().any(|cause| cause.to_string() == "unsafe source archive directory"), "update failed for a different cause than the owned archive fault");
        fault.restore()?;
        ensure!(installed_record(&actor, &sources[0], instance).await? == original, "failed archive copy changed the installed capsule/permissions");
        // A real owner approval is durable even when later copying fails. Do
        // not pretend it never happened or roll back a concurrent revocation.
        let approved = grant(&actor, &sources[1]).await?;
        ensure!(approved.0 == original_grant.0 + 1 && approved.2 == 0, "exact manual approval or opt-in changed");
        eprintln!("{}", json!({"stage":"storage_update_failed_closed","original_installed_record_unchanged":true,"original_mode_restored":true}));
        // The original command deliberately owns the single execution
        // controller until cancellation joins its unresolved cleanup. Do not
        // open a second controller or invoke through a copied DB observation.
        action(actor.app()?, actor.token()?, id, json!({"action":"cancel","mutation_id":Uuid::new_v4()})).await?;
        phase(actor.app()?, actor.token()?, id, "cancelled", None).await?;
        actor.state()?.marketplace.fixture_original_installation().fixture_join_original(id).await?;
        ensure!(installed_record(&actor, &sources[0], instance).await? == original && grant(&actor, &sources[1]).await? == approved,
            "cancellation changed installed source or lost the real owner approval");
        provider(actor.state()?, &actor.rig.database, user, instance, "1.0.0").await?;
        eprintln!("{}", json!({"stage":"storage_update_original_cancel_joined","old_version_invoked_after_cleanup":true}));
        let updated = install_with_updates(actor.app()?, actor.state()?, actor.token()?, &sources[1], Some(instance), true, false).await?;
        ensure!(updated == instance, "retry replaced configured instance");
        provider(actor.state()?, &actor.rig.database, user, instance, "1.1.0").await?;
        ensure!(grant(&actor, &sources[1]).await?.2 == 0, "manual retry enabled unattended updates");
        eprintln!("{}", json!({"stage":"storage_update_recovery_verified","original_installed_record_unchanged":true,
            "old_version_invoked_after_cleanup":true,"original_cancel_joined":true,"original_mode_restored":true,
            "same_instance_updated":true,"updated_version_invoked":true,"automatic_updates_disabled":true,"same_original_identity":true}));
        Ok::<_, anyhow::Error>(())
    })).catch_unwind().await;
    let cleanup = actor.close().await;
    if cleanup.is_ok() {
        eprintln!(
            "{}",
            json!({"stage":"resource_fault_original_identity_retired"})
        );
    }
    cleanup?;
    match operation {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    Ok(())
}
