//! Real OS ownership with synthetic task completions, NOT native stop proof.
//! The separate explicit Docker quiescence test exercises actual native work.
use super::super::super::{standing::tests as installed, tests::Fixture};
use super::super::tests::{configuration, limits};
use super::*;
use crate::marketplace::lease::activity::{Fence, Target};

// Actual protected transport / native VM composition, separate from the
// synthetic completion fixtures in this module.
#[cfg(unix)]
mod native_http;

fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}

#[tokio::test]
async fn joined_vm_result_cannot_release_a_controller_with_unconfirmed_http_retirement()
-> Result<()> {
    let f = Fixture::new().await?;
    let mut owner = controller(&f).await?;
    let instance = installed::enroll(&f).await?;
    let generation = installed::binding(&f, instance).await?.generation();
    let (http, _process_owner) = SdkHttpCall::fixture_unconfirmed_retirement()?;
    let (stop, _stopped) = watch::channel(false);
    owner.active = Some(RegisteredCall {
        generation,
        call: InstalledCall {
            stop,
            task: Some(tokio::spawn(async move {
                CallCompletion {
                    result: Ok(RuntimeJson::parse(b"null").unwrap()),
                    http: Some(super::super::http::Call::Protected(http)),
                }
            })),
            result: None,
            http: None,
            join_failed: false,
        },
    });
    assert!(owner.finish().await.is_err());
    assert!(owner.active.is_some());
    assert!(owner.require_idle().is_err());
    assert!(owner.cancel().await.is_err());
    assert!(owner.active.as_ref().unwrap().call.http.is_some());
    assert!(
        owner
            .begin(
                installed::binding(&f, instance).await?,
                limits(),
                RuntimeJson::parse(b"null")?
            )
            .is_err()
    );
    Ok(())
}
fn config(f: &Fixture) -> Result<InstalledRuntime> {
    configuration(
        f,
        &format!("sha256:{}", "1".repeat(64)),
        "controller-fixture",
    )
}
async fn controller(f: &Fixture) -> Result<InstalledController> {
    InstalledController::open(f.db.clone(), manager(), config(f)?).await
}

async fn warm_profile() -> Result<crate::marketplace::application_egress::ApplicationGatewayProfile>
{
    use crate::{
        live::{
            config::LiveEgressProfileKind, egress::prepare_gateway_material_from_secret_values,
        },
        network::protection::DownloadNetworkProfileKind,
    };
    let config=b"[Interface]\nPrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=\nAddress = 10.64.0.2/32\n[Peer]\nPublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=\nAllowedIPs = 0.0.0.0/0\nEndpoint = 1.1.1.1:51820\n";
    let files = prepare_gateway_material_from_secret_values(
        LiveEgressProfileKind::Wireguard,
        config,
        None,
        None,
    )
    .await?;
    crate::marketplace::application_egress::ApplicationGatewayProfile::warm_fixture(
        "controller-warm",
        &"b".repeat(64),
        &format!("sha256:{}", "a".repeat(64)),
        &DownloadNetworkProfileKind::WireguardConfig,
        &files,
    )
}

#[tokio::test]
async fn warm_prepared_gateway_recovers_under_runtime_lineage_without_adopting_source_authority()
-> Result<()> {
    use crate::marketplace::lease::activity::gateway::GatewayFence;
    let f = Fixture::new().await?;
    let controller = controller(&f).await?;
    let owner = controller.retained_owner()?;
    owner.current(&f.db).await?;
    let profile = warm_profile().await?;
    assert!(profile.is_warm());
    assert_eq!(profile.spec().command[3], "330");
    let nonce = Digest::of("controller-warm");
    let fence = GatewayFence::begin_warm(&owner, &profile, nonce)?;
    assert!(GatewayFence::begin_warm(&owner, &profile, nonce).is_err());
    let path = owner.lease().directory().join("gateway-helper.pending");
    let bytes = std::fs::read(&path)?;
    assert!(String::from_utf8_lossy(&bytes).contains("\"standalone\":true"));
    assert!(!String::from_utf8_lossy(&bytes).contains("yAnzdtF2"));
    drop(fence);
    drop(owner);
    drop(controller);
    let reopened = InstalledController::open(f.db.clone(), manager(), config(&f)?).await?;
    assert!(
        !path.exists(),
        "a prepared-only original warm marker needs no native creation to retire"
    );
    reopened.require_idle()?;
    Ok(())
}

#[tokio::test]
async fn warm_unknown_create_survives_restart_and_cannot_become_a_new_start() -> Result<()> {
    use crate::marketplace::lease::activity::gateway::GatewayFence;
    let f = Fixture::new().await?;
    let controller = controller(&f).await?;
    let owner = controller.retained_owner()?;
    let mut fence = GatewayFence::begin_warm(
        &owner,
        &warm_profile().await?,
        Digest::of("controller-warm"),
    )?;
    let lost = fence.creating(&owner.lease())?;
    drop(lost);
    let path = owner.lease().directory().join("gateway-helper.pending");
    let bytes = std::fs::read(&path)?;
    drop(fence);
    drop(owner);
    drop(controller);
    assert!(
        InstalledController::open(f.db.clone(), manager(), config(&f)?)
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(&path)?,
        bytes,
        "unknown native work cannot be cleared by an idle/drop/restart inference"
    );
    assert!(
        InstalledController::open(f.db.clone(), manager(), config(&f)?)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn one_controller_per_configured_lane_and_invalid_image_cannot_create_an_owner() -> Result<()>
{
    let f = Fixture::new().await?;
    let mut invalid = config(&f)?;
    invalid.image = "mutable:latest".into();
    assert!(
        InstalledController::open(f.db.clone(), manager(), invalid)
            .await
            .is_err()
    );
    assert!(!config(&f)?.lane.exists());
    let first = controller(&f).await?;
    assert!(controller(&f).await.is_err());
    let lane = config(&f)?.lane;
    // Idle ownership does not monopolize preparation's expensive-job lane.
    drop(ControllerLease::acquire(&lane)?);
    drop(first);
    drop(controller(&f).await?);
    Ok(())
}

#[tokio::test]
async fn uncertain_or_corrupt_interrupted_native_work_cannot_initialize_an_empty_registry()
-> Result<()> {
    for corrupt in [false, true] {
        let f = Fixture::new().await?;
        let lane = config(&f)?.lane;
        let held = ControllerLease::acquire(&lane)?;
        let fence = Fence::begin(
            &held,
            Target::WriterCreate {
                deployment: "unknown-late-create".into(),
            },
        )?;
        drop(fence);
        drop(held);
        if corrupt {
            std::fs::write(lane.join("native-job.pending"), b"{}")?;
        }
        let before = std::fs::read(lane.join("native-job.pending"))?;
        assert!(controller(&f).await.is_err());
        assert_eq!(before, std::fs::read(lane.join("native-job.pending"))?);
        assert!(ControllerLease::acquire(&lane).is_err());
        assert!(controller(&f).await.is_err(), "retry cannot invent cleanup");
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_join_wait_retains_the_original_slot_and_task_panics_never_unlock_a_start()
-> Result<()> {
    let f = Fixture::new().await?;
    let mut owner = controller(&f).await?;
    let instance = installed::enroll(&f).await?;
    let generation = installed::binding(&f, instance).await?.generation();
    let (stop, _stopped) = watch::channel(false);
    let (done, complete) = tokio::sync::oneshot::channel();
    owner.active = Some(RegisteredCall {
        generation: generation.clone(),
        call: InstalledCall {
            stop,
            task: Some(tokio::spawn(async move {
                let _ = complete.await;
                CallCompletion {
                    result: Err(anyhow::anyhow!("synthetic source error after cleanup")),
                    http: None,
                }
            })),
            result: None,
            http: None,
            join_failed: false,
        },
    });
    assert!(
        timeout(Duration::from_millis(20), owner.cancel())
            .await
            .is_err()
    );
    assert!(owner.active.is_some());
    assert!(
        owner
            .begin(
                installed::binding(&f, instance).await?,
                limits(),
                RuntimeJson::parse(b"null")?
            )
            .is_err()
    );
    assert!(controller(&f).await.is_err());
    let _ = done.send(());
    owner.cancel().await?;
    assert!(owner.active.is_none());
    async fn unknown_owner() -> CallCompletion {
        panic!("synthetic unknown owner outcome")
    }
    let (stop, _stopped) = watch::channel(false);
    owner.active = Some(RegisteredCall {
        generation,
        call: InstalledCall {
            stop,
            task: Some(tokio::spawn(unknown_owner())),
            result: None,
            http: None,
            join_failed: false,
        },
    });
    assert!(owner.finish().await.is_err());
    assert!(owner.active.is_some());
    assert!(owner.cancel().await.is_err());
    assert!(
        owner
            .begin(
                installed::binding(&f, instance).await?,
                limits(),
                RuntimeJson::parse(b"null")?
            )
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn dropped_owner_signals_before_unlock_but_reopen_waits_for_the_original_global_lane()
-> Result<()> {
    let f = Fixture::new().await?;
    let mut owner = controller(&f).await?;
    let instance = installed::enroll(&f).await?;
    let generation = installed::binding(&f, instance).await?.generation();
    let held = ControllerLease::acquire(&config(&f)?.lane)?;
    let (stop, mut stopped) = watch::channel(false);
    let (release, wait) = tokio::sync::oneshot::channel();
    let (joined, acknowledged) = tokio::sync::oneshot::channel();
    owner.active = Some(RegisteredCall {
        generation,
        call: InstalledCall {
            stop,
            task: Some(tokio::spawn(async move {
                stopped.changed().await.unwrap();
                assert!(*stopped.borrow());
                let _ = wait.await;
                drop(held);
                let _ = joined.send(());
                CallCompletion {
                    result: Err(anyhow::anyhow!("synthetic original cleanup joined")),
                    http: None,
                }
            })),
            result: None,
            http: None,
            join_failed: false,
        },
    });
    drop(owner);
    assert!(
        controller(&f).await.is_err(),
        "a free registry lock is not native cleanup"
    );
    let _ = release.send(());
    timeout(Duration::from_secs(5), acknowledged).await??;
    drop(controller(&f).await?);
    Ok(())
}

#[tokio::test]
async fn replaced_owner_inode_denies_new_starts_without_disabling_owned_cleanup() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new().await?;
    let mut owner = controller(&f).await?;
    let instance = installed::enroll(&f).await?;
    let (_stop, stopped) = watch::channel(false);
    let control = RunControl {
        stop: stopped,
        owner: Some(owner.lease.clone()),
    };
    restrictions(&control, Deadline::now() + Duration::from_secs(10))?;
    let path = config(&f)?
        .lane
        .join(OWNER_DIRECTORY)
        .join("controller.lock");
    std::fs::rename(&path, path.with_file_name("old-owner.lock"))?;
    std::fs::File::create(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    assert!(restrictions(&control, Deadline::now() + Duration::from_secs(10)).is_err());
    assert!(
        owner
            .begin(
                installed::binding(&f, instance).await?,
                limits(),
                RuntimeJson::parse(b"null")?
            )
            .is_err()
    );
    owner.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn prior_installed_history_cannot_be_silently_adopted_without_a_durable_runtime_binding()
-> Result<()> {
    for orphan_only in [false, true] {
        let f = Fixture::new().await?;
        installed::enroll(&f).await?;
        if orphan_only {
            // Persisted corruption in this disposable fixture: the marker is
            // the only remaining installed history, not native-stop proof.
            sqlx::query("DELETE FROM marketplace_installations")
                .execute(&f.db.pool)
                .await?;
            sqlx::query("DELETE FROM marketplace_instance_sdk_state")
                .execute(&f.db.pool)
                .await?;
            sqlx::query("UPDATE marketplace_installation_budget SET entries=0,encoded_bytes=0")
                .execute(&f.db.pool)
                .await?;
        }
        let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installations")
            .fetch_one(&f.db.pool)
            .await?;
        assert!(controller(&f).await.is_err(), "orphan_only={orphan_only}");
        assert!(!config(&f)?.lane.exists());
        assert_eq!(
            before,
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_installations")
                .fetch_one(&f.db.pool)
                .await?
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
                .fetch_one(&f.db.pool)
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM marketplace_installed_runtime_owner"
            )
            .fetch_one(&f.db.pool)
            .await?,
            0
        );
    }
    Ok(())
}

#[tokio::test]
async fn missing_runtime_history_paths_are_not_recreated_even_after_a_clean_process_exit()
-> Result<()> {
    for relative in [
        "",
        "controller.lock",
        "installed-controller/controller.lock",
        "installed-controller/owner.binding",
    ] {
        let f = Fixture::new().await?;
        let owner = controller(&f).await?;
        installed::enroll(&f).await?;
        drop(owner);
        let lane = config(&f)?.lane;
        let path = if relative.is_empty() {
            lane.clone()
        } else {
            lane.join(relative)
        };
        std::fs::rename(&path, f.root.path().join("preserved-owner-history"))?;
        assert!(controller(&f).await.is_err(), "{relative}");
        assert!(
            !path.exists(),
            "missing history must not be silently reinitialized: {relative}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn orphan_desired_metadata_alone_cannot_initialize_a_replacement_runtime_owner() -> Result<()>
{
    let f = Fixture::new().await?;
    installed::enroll(&f).await?;
    sqlx::query("INSERT INTO marketplace_installation_metadata(extension_id,home_id,metadata_sha256) SELECT extension_id,home_id,$1 FROM marketplace_installations")
        .bind(Digest::of(b"orphan metadata fixture").to_string()).execute(&f.db.pool).await?;
    sqlx::query("DELETE FROM marketplace_installations")
        .execute(&f.db.pool)
        .await?;
    sqlx::query("DELETE FROM marketplace_sdk_namespaces")
        .execute(&f.db.pool)
        .await?;
    sqlx::query("UPDATE marketplace_installation_budget SET entries=0,encoded_bytes=0")
        .execute(&f.db.pool)
        .await?;
    assert!(controller(&f).await.is_err());
    assert!(!config(&f)?.lane.exists());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installed_runtime_owner")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn a_ready_marker_with_missing_or_changed_database_binding_is_never_reinitialized()
-> Result<()> {
    for fault in ["missing", "nonce", "digest"] {
        let f = Fixture::new().await?;
        drop(controller(&f).await?);
        let marker = config(&f)?.lane.join(OWNER_DIRECTORY).join("owner.binding");
        let before = std::fs::read(&marker)?;
        match fault {
            "missing" => {
                sqlx::query("DELETE FROM marketplace_installed_runtime_owner")
                    .execute(&f.db.pool)
                    .await?;
            }
            "nonce" => {
                sqlx::query("UPDATE marketplace_installed_runtime_owner SET nonce=$1")
                    .bind(Uuid::new_v4().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "digest" => {
                sqlx::query("UPDATE marketplace_installed_runtime_owner SET binding_sha256=$1")
                    .bind(Digest::of(b"wrong owner identity").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => unreachable!(),
        }
        let changed: Option<(String, String)> =
            sqlx::query_as("SELECT nonce,binding_sha256 FROM marketplace_installed_runtime_owner")
                .fetch_optional(&f.db.pool)
                .await?;
        assert!(controller(&f).await.is_err(), "{fault}");
        assert_eq!(before, std::fs::read(&marker)?);
        assert_eq!(
            changed,
            sqlx::query_as::<_, (String, String)>(
                "SELECT nonce,binding_sha256 FROM marketplace_installed_runtime_owner"
            )
            .fetch_optional(&f.db.pool)
            .await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn exact_pending_first_initialization_recovers_without_history_growth_or_runtime_authority()
-> Result<()> {
    use std::os::unix::fs::FileExt;
    for database_committed in [false, true] {
        let f = Fixture::new().await?;
        drop(controller(&f).await?);
        let marker = config(&f)?.lane.join(OWNER_DIRECTORY).join("owner.binding");
        let before = std::fs::read(&marker)?;
        // TEST-ONLY crash boundary. No installation/native call ever existed.
        let file = std::fs::OpenOptions::new().write(true).open(&marker)?;
        file.write_all_at(b"P", 0)?;
        file.sync_all()?;
        if !database_committed {
            sqlx::query("DELETE FROM marketplace_installed_runtime_owner")
                .execute(&f.db.pool)
                .await?;
        }
        drop(controller(&f).await?);
        assert_eq!(before, std::fs::read(&marker)?);
        assert_eq!(
            1,
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM marketplace_installed_runtime_owner"
            )
            .fetch_one(&f.db.pool)
            .await?
        );
        assert_eq!(
            0,
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_installations")
                .fetch_one(&f.db.pool)
                .await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn queued_pre_native_call_keeps_the_registry_lock_until_its_original_task_joins() -> Result<()>
{
    let f = Fixture::new().await?;
    let mut owner = controller(&f).await?;
    let instance = installed::enroll(&f).await?;
    let generation = installed::binding(&f, instance).await?.generation();
    let (stop, receiver) = watch::channel(false);
    let mut control = RunControl {
        stop: receiver,
        owner: Some(owner.lease.clone()),
    };
    let (release, wait) = tokio::sync::oneshot::channel();
    let (done, joined) = tokio::sync::oneshot::channel();
    owner.active = Some(RegisteredCall {
        generation,
        call: InstalledCall {
            stop,
            task: Some(tokio::spawn(async move {
                stopped(&mut control).await;
                let _ = wait.await;
                drop(control);
                let _ = done.send(());
                CallCompletion {
                    result: Err(anyhow::anyhow!("synthetic pre-native task joined")),
                    http: None,
                }
            })),
            result: None,
            http: None,
            join_failed: false,
        },
    });
    drop(owner);
    // No native lane/fence exists in this fixture. The original QUEUED task
    // still retains its real registry lease, so restart cannot invent an empty
    // registry from a free global lane before that task finishes.
    assert!(controller(&f).await.is_err());
    let _ = release.send(());
    timeout(Duration::from_secs(5), joined).await??;
    drop(controller(&f).await?);
    Ok(())
}
