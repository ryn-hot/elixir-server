use super::*;
use crate::{
    marketplace::provider::FENCE,
    runtime::{
        docker::DockerRuntimeManager,
        model::ELIXIR_DEPLOYMENT_ID_LABEL,
        packet_network::{PacketNetworkInspection, PacketNetworkSpec},
    },
};
use elixir_marketplace::warp_provider::State;
use serde_json::Value;
use std::{collections::HashMap, fs::File};
use tokio::time::{Duration, timeout};
mod catalog_cleanup;
mod guard;

enum CatalogArchives {
    Required,
    Absent,
}

/// Preserve recovery evidence on failed/uncertain native tests. Never erase a
/// CREATE fence merely because a fixture scope is unwinding.
struct PreservedRoot(
    Option<tempfile::TempDir>,
    std::cell::Cell<bool>,
    std::cell::Cell<bool>,
);
impl PreservedRoot {
    fn new() -> Result<Self> {
        Ok(Self(
            Some(tempfile::tempdir()?),
            std::cell::Cell::new(false),
            std::cell::Cell::new(false),
        ))
    }
    fn path(&self) -> &std::path::Path {
        self.0.as_ref().unwrap().path()
    }
    fn complete(&self) {
        self.1.set(true);
    }
    fn preserve(&self) {
        // Installed-source cleanup has journals independent of the provider.
        // Successful VPN retirement must not delete an uncertain source owner.
        self.2.set(true);
    }
    fn describe_uplink_attempt(&self, spec: &PacketNetworkSpec) -> Result<()> {
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.path().join("provider-uplink-attempt.json"))?;
        file.write_all(&elixir_marketplace::canonical::to_vec(&serde_json::json!({
            "schema_version":1,"name":spec.name(),"labels":spec.labels(),"options":spec.options(),
            "ipv6":spec.ipv6(),"internal":spec.internal(),
        }))?)?;
        file.sync_all()?;
        Ok(())
    }
    fn describe(
        &self,
        deployment: &str,
        scope: Scope,
        image: &str,
        network: &str,
        fixture: &str,
    ) -> Result<()> {
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.path().join("provider-fixture.json"))?;
        // Public recovery context only. No private bundle or encryption key.
        file.write_all(&elixir_marketplace::canonical::to_vec(&serde_json::json!({"schema_version":1,
            "deployment":deployment,"scope":scope,"image":image,"network":network,"fixture":fixture}))?)?;
        file.sync_all()?;
        Ok(())
    }
    fn must_preserve(&self) -> bool {
        let directories = ["provider", "marketplace/worker"];
        let identity = !self.1.get()
            && directories.iter().any(|directory| {
                ["provider-state.v1", "provider-state.next"]
                    .iter()
                    .any(|name| {
                        std::fs::symlink_metadata(self.path().join(directory).join(name)).is_ok()
                    })
            });
        let fenced = directories.iter().any(|directory| {
            [super::super::FENCE, super::super::NEXT]
                .iter()
                .any(|name| {
                    match std::fs::symlink_metadata(self.path().join(directory).join(name)) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                        _ => true,
                    }
                })
        });
        self.2.get() || identity || fenced
    }
    fn close_completed_catalog(&mut self, archives: CatalogArchives) -> Result<()> {
        ensure!(
            self.1.get() && !self.must_preserve(),
            "catalog fixture still requires recovery"
        );
        let checked = match archives {
            CatalogArchives::Required => catalog_cleanup::unseal_completed_archives(self.path()),
            CatalogArchives::Absent => catalog_cleanup::confirm_no_archives(self.path()),
        };
        if let Err(error) = checked {
            self.preserve();
            return Err(error.context("catalog fixture sealed archive cleanup"));
        }
        self.0
            .take()
            .unwrap()
            .close()
            .context("completed catalog fixture directory cleanup")
    }
}
impl Drop for PreservedRoot {
    fn drop(&mut self) {
        if self.0.is_some() && self.must_preserve() {
            let path = self.0.take().unwrap().keep();
            eprintln!("provider fixture recovery retained at {}", path.display());
        }
    }
}

#[test]
fn catalog_cleanup_requires_completion_and_refuses_recovery_fences() -> Result<()> {
    let mut root = PreservedRoot::new()?;
    assert!(
        root.close_completed_catalog(CatalogArchives::Absent)
            .is_err()
    );
    let worker = root.path().join("marketplace/worker");
    std::fs::create_dir_all(&worker)?;
    let fence = worker.join(FENCE);
    std::fs::write(&fence, b"synthetic unresolved fixture")?;
    root.complete();
    assert!(
        root.close_completed_catalog(CatalogArchives::Absent)
            .is_err()
    );
    assert!(fence.is_file());
    std::fs::remove_file(fence)?;
    let path = root.path().to_owned();
    root.close_completed_catalog(CatalogArchives::Absent)?;
    assert!(!path.exists());
    Ok(())
}

async fn docker(args: &[&str]) -> Result<String> {
    let output = timeout(
        Duration::from_secs(20),
        tokio::process::Command::new("docker")
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("provider fixture deadline")??;
    ensure!(
        output.status.success() && output.stdout.len() <= 65536 && output.stderr.len() <= 4096,
        "provider fixture command failed"
    );
    Ok(String::from_utf8(output.stdout)?.trim().into())
}
struct Emergency {
    actors: Vec<(ContainerHandle, HashMap<String, String>)>,
    network: PacketNetworkSpec,
    network_id: String,
}
impl Drop for Emergency {
    fn drop(&mut self) {
        for (handle, expected) in self.actors.iter().rev() {
            if let Ok(output) = std::process::Command::new("docker")
                .args(["inspect", "--format", "{{json .Config.Labels}}", &handle.id])
                .output()
                && output.status.success()
                && serde_json::from_slice::<HashMap<String, String>>(&output.stdout).is_ok_and(
                    |actual| {
                        expected
                            .iter()
                            .all(|(key, value)| actual.get(key) == Some(value))
                    },
                )
            {
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", &handle.id])
                    .output();
            }
        }
        if let Ok(output) = std::process::Command::new("docker")
            .args(["network", "inspect", &self.network_id])
            .output()
            && output.status.success()
            && serde_json::from_slice::<Vec<Value>>(&output.stdout)
                .ok()
                .and_then(|values| {
                    values
                        .first()
                        .and_then(|value| PacketNetworkInspection::parse(value).ok())
                })
                .is_some_and(|actual| {
                    self.network.verify(&actual).is_ok() && actual.members.is_empty()
                })
        {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.network_id])
                .output();
        }
    }
}
fn image() -> Result<String> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WARP_PROVIDER_IMAGE")?;
    ensure!(
        image
            .strip_prefix("sha256:")
            .is_some_and(crate::runtime::packet_network::immutable_id),
        "immutable inspected provider image required"
    );
    Ok(image)
}

#[tokio::test]
#[ignore = "explicit isolated provider CREATE/recovery test; no enrollment or public traffic"]
async fn unknown_create_absence_stays_fenced_and_exact_unstarted_recovery_never_starts_provider()
-> Result<()> {
    let runtime = DockerRuntimeManager::new(None);
    let image = image()?;
    let deployment = format!("provider-recovery-{}", uuid::Uuid::new_v4().simple());
    let spec = PacketNetworkSpec::isolated_fixture(&Profile::network_owner(&deployment), false)?;
    let network = runtime.ensure_packet_uplink(&spec).await?;
    spec.verify(&network)?;
    let mut emergency = Emergency {
        actors: vec![],
        network: spec,
        network_id: network.id.clone(),
    };
    let root = PreservedRoot::new()?;
    let directory = root.path().canonicalize()?.join("provider");
    let lease = ControllerLease::acquire(&directory)?;
    let profile = Profile::fixture(&deployment, &image, &network.id)?;
    let mut fence = Fence::begin(&lease, &profile)?;
    let attempt = fence.creating(&lease)?;
    assert!(Fence::recover(&runtime, &lease).await.is_err());
    assert!(
        directory.join(FENCE).is_file(),
        "unknown name absence must not clear original intent"
    );
    // The original unused token is still retained here. A failed pure preflight
    // supplies typed NotAttempted, not a synthetic absence/timeout heuristic.
    let unavailable = DockerRuntimeManager::new(Some(
        root.path()
            .join("must-not-execute")
            .to_string_lossy()
            .into(),
    ));
    let ProviderCreation::NotAttempted(outcome) =
        unavailable.create_provider(&profile, attempt).await?
    else {
        anyhow::bail!("fixture unexpectedly created native work");
    };
    let _ = fence.not_attempted(&lease, outcome)?;
    fence.retire(&runtime, &lease).await?;
    drop(fence);
    let profile = Profile::fixture(&deployment, &image, &network.id)?;
    let mut fence = Fence::begin(&lease, &profile)?;
    let ProviderCreation::Created(handle) = runtime
        .create_provider(&profile, fence.creating(&lease)?)
        .await?
    else {
        anyhow::bail!("provider fixture was not created");
    };
    emergency
        .actors
        .push((handle.clone(), profile.spec().labels.clone()));
    let actual = runtime.inspect_provider(&handle, &profile).await?;
    ensure!(
        actual.status == "created" && !actual.running,
        "CREATE unexpectedly started provider"
    );
    // Lose the returned result before binding: restart may recover only this
    // exact never-started native object, never manufacture a START token.
    drop(fence);
    drop(lease);
    assert!(ControllerLease::acquire(&directory).is_err());
    let lease = ControllerLease::acquire_for_recovery(&directory)?;
    assert!(Fence::recover(&runtime, &lease).await?);
    assert!(runtime.get_container_handle(&handle.id).await?.is_none());
    assert!(!directory.join(FENCE).exists());
    let profile = Profile::fixture(&deployment, &image, &network.id)?;
    let foreign_label = format!("provider.foreign_fixture={}", uuid::Uuid::new_v4());
    let foreign = docker(&[
        "create",
        "--pull=never",
        "--name",
        &profile.spec().name,
        "--label",
        &foreign_label,
        "--network=none",
        "--entrypoint=/bin/sleep",
        &image,
        "30",
    ])
    .await?;
    emergency.actors.push((
        ContainerHandle {
            id: foreign.clone(),
            name: profile.spec().name.clone(),
        },
        HashMap::from([(
            foreign_label.split_once('=').unwrap().0.into(),
            foreign_label.split_once('=').unwrap().1.into(),
        )]),
    ));
    let mut fence = Fence::begin(&lease, &profile)?;
    let ProviderCreation::NotAttempted(outcome) = runtime
        .create_provider(&profile, fence.creating(&lease)?)
        .await?
    else {
        anyhow::bail!("foreign name was replaced");
    };
    let _ = fence.not_attempted(&lease, outcome)?;
    fence.retire(&runtime, &lease).await?;
    ensure!(
        runtime.get_container_handle(&foreign).await?.is_some(),
        "foreign fixture was modified"
    );
    docker(&["rm", &foreign]).await?;
    runtime
        .remove_packet_uplink(&emergency.network, &emergency.network_id)
        .await?;
    emergency.actors.clear();
    root.complete();
    eprintln!(
        "{}",
        serde_json::json!({"unknown_create_absence_preserved":true,"typed_not_attempted_only":true,"exact_unstarted_recovery":true,"foreign_name_preserved":true,"registration_calls":0,"excludes":"provider boot, public networking and health"})
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit owner-approved live free provider factory + durable native recovery"]
async fn native_owner_factory_reuses_encrypted_identity_and_recovers_dropped_owner_before_replacement()
-> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1"),
        "explicit live free-provider consent required"
    );
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let image = image()?;
    let image_digest: Digest = image.parse()?;
    let root = PreservedRoot::new()?;
    let directory = root.path().canonicalize()?.join("provider");
    let deployment = format!("provider-owner-{}", uuid::Uuid::new_v4().simple());
    let scope = Scope {
        deployment: Profile::deployment_scope(&deployment),
        enrollment: Digest::of(uuid::Uuid::new_v4().as_bytes()),
    };
    let secrets = Arc::new(SecretsManager::from_key_bytes([96; 32], false));
    let network_spec = Profile::network_spec(&deployment)?;
    let network = runtime.ensure_packet_uplink(&network_spec).await?;
    network_spec.verify(&network)?;
    root.describe(
        &deployment,
        scope,
        &image,
        &network.id,
        "native-owner-factory",
    )?;
    let mut emergency = Emergency {
        actors: vec![],
        network: network_spec,
        network_id: network.id.clone(),
    };
    let mut identity = None;
    let mut interrupted: Option<String> = None;
    for phase in 0..3 {
        let mut owner = Owner::launch(
            runtime.clone(),
            directory.clone(),
            deployment.clone(),
            image.clone(),
            scope,
            phase != 0,
            secrets.clone(),
        )
        .wait()
        .await?;
        emergency.actors.push((
            owner.handle().clone(),
            owner.profile().spec().labels.clone(),
        ));
        if let Some(id) = interrupted.take() {
            ensure!(
                runtime.get_container_handle(&id).await?.is_none(),
                "replacement did not retire original interrupted native owner"
            );
        }
        ensure!(
            directory.join(FENCE).is_file(),
            "active provider has no native recovery fence"
        );
        owner.connect().await?;
        let mut protected = false;
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(300)).await;
            owner.inspect().await?;
            let value=docker(&["exec",&owner.handle().id,"/bin/bash","-c","set -o pipefail; curl --noproxy '*' --fail --silent --connect-timeout 2 --max-time 4 https://www.cloudflare.com/cdn-cgi/trace 2>/dev/null | grep -q '^warp=on$' && printf protected || printf unavailable"]).await?;
            if value == "protected" {
                protected = true;
                break;
            }
        }
        ensure!(
            protected,
            "owned provider fixed TLS probe was not protected"
        );
        let saved = provider_state::load(&owner.lease, &secrets, scope, image_digest)?
            .context("encrypted owner state missing")?;
        let state = State::decode(saved.bytes, scope)?;
        if let Some(prior) = identity {
            ensure!(state.identity() == prior, "provider identity changed");
        } else {
            identity = Some(state.identity());
        }
        if phase == 1 {
            interrupted = Some(owner.handle().id.clone());
            drop(owner);
            ensure!(
                directory.join(FENCE).is_file(),
                "dropping owner silently cleared native fence"
            );
            assert!(
                ControllerLease::acquire(&directory).is_err(),
                "ordinary restart bypassed native recovery"
            );
        } else {
            tokio::time::sleep(Duration::from_millis(300)).await;
            owner.close(phase == 2).await?;
            ensure!(
                !directory.join(FENCE).exists(),
                "confirmed retirement left native fence"
            );
        }
    }
    runtime
        .remove_packet_uplink(&emergency.network, &emergency.network_id)
        .await?;
    emergency.actors.clear();
    ensure!(
        docker(&[
            "ps",
            "-a",
            "--filter",
            &format!("label={ELIXIR_DEPLOYMENT_ID_LABEL}={deployment}"),
            "--format",
            "{{.ID}}"
        ])
        .await?
        .is_empty(),
        "owned provider actors remain"
    );
    let _ = ControllerLease::acquire(&directory)?;
    let _ = File::open(directory)?;
    root.complete();
    eprintln!(
        "{}",
        serde_json::json!({"image":image,"provider_starts":3,"registration_calls":1,"same_encrypted_identity":true,"native_create_bind_before_start":true,
        "dropped_owner_stays_fenced":true,"native_recovery_before_replacement":true,"fixed_protected_tls_all_starts":true,"owned_retirement":true,
        "excludes":"actual server process/power loss, packet-guard namespace handoff, protected DNS/public workers, continuous accounting/playback and redistribution qualification"})
    );
    Ok(())
}
