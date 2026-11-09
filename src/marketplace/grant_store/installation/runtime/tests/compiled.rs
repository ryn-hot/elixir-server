//! Actual compiled retained load/controller/SQL/AES SDK. The initial ACTIVE
//! journal transition below is EXPLICITLY synthetic, not activation proof.
use super::*;
use crate::marketplace::runtime::build::ApprovedToolchains;
use std::path::Path;

const MIB: u64 = 1024 * 1024;

// Establish the actual durable runtime owner while the database is empty,
// before any source job or installed history exists. Never synthesize/adopt
// lineage after staging: the production controller deliberately rejects that.
pub(in crate::marketplace) async fn initialize_compiled_fixture(
    runtime: Arc<dyn RuntimeManager>,
    root: &Path,
    lane: &Path,
    worker: &str,
    deployment: &str,
) -> Result<()> {
    let db = grant_tests::connect(&root.join("retained-build.sqlite")).await?;
    let controller = InstalledController::open(
        db.clone(),
        SecretsManager::from_key_bytes([93; 32], false),
        compiled_fixture_configuration(&db, runtime, root, lane, worker, deployment)?,
    )
    .await?;
    controller.require_idle()?;
    drop(controller);
    db.pool.close().await;
    Ok(())
}

pub(in crate::marketplace) async fn run_compiled_fixture(
    runtime: Arc<dyn RuntimeManager>,
    root: &Path,
    lane: &Path,
    worker: &str,
    deployment: &str,
    value: &serde_json::Value,
) -> Result<()> {
    let db = grant_tests::connect(&root.join("retained-build.sqlite")).await?;
    let id: ExtensionId = value["release"]["payload"]["extension_id"]
        .as_str()
        .context("compiled fixture extension missing")?
        .parse()?;
    let (home, instance) = enroll_fixture(&db, &id).await?;
    let secrets = SecretsManager::from_key_bytes([93; 32], false);
    grant_tests::secret(&db, "global", None, "source_api", "never-global", &secrets).await?;
    grant_tests::secret(
        &db,
        "instance",
        Some(instance),
        "source_api",
        "own-native-instance",
        &secrets,
    )
    .await?;
    let config =
        compiled_fixture_configuration(&db, runtime.clone(), root, lane, worker, deployment)?;
    // Fail before source admission; stored metadata/history cannot approve a
    // compiler, enable a missing runtime policy or substitute another key.
    for wrong_key in [false, true] {
        let mut disabled = config.clone();
        if !wrong_key {
            disabled.compiled_toolchains = None;
        }
        let manager = if wrong_key {
            SecretsManager::from_key_bytes([94; 32], true)
        } else {
            secrets.clone()
        };
        let mut controller = InstalledController::open(db.clone(), manager, disabled).await?;
        let binding =
            InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
        controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
        let error = controller
            .finish()
            .await
            .err()
            .context("compiled preflight accepted missing policy/wrong key")?;
        let text = format!("{error:#}");
        if !(text.contains("not configured")
            || text.contains("decrypt")
            || text.contains("authentication"))
        {
            return Err(error).context("negative did not reach compiled preflight");
        }
        controller.shutdown_cache().await?;
        drop(controller);
        assert!(
            runtime
                .get_container_handle(&format!(
                    "elixir-marketplace-source-writer-{}",
                    config.deployment
                ))
                .await?
                .is_none()
        );
    }
    let mut controller =
        InstalledController::open(db.clone(), secrets.clone(), config.clone()).await?;
    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
    assert_eq!(controller.finish().await?.as_str(), "true");
    assert!(revision(&db, instance).await? > 0);

    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    controller.begin(
        binding,
        limits(),
        RuntimeJson::from_value(json!({"cancel":true}))?,
    )?;
    wait_key(&db, instance, "cancel_started").await?;
    controller.cancel().await?;
    controller.require_idle()?;
    assert!(
        runtime
            .get_container_handle(&format!(
                "elixir-marketplace-source-writer-{}",
                config.deployment
            ))
            .await?
            .is_none()
    );

    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    controller.begin(
        binding,
        limits(),
        RuntimeJson::from_value(json!({"revoke":true}))?,
    )?;
    let revoke = async {
        wait_key(&db, instance, "started").await?;
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,revision=revision+1,revocation_revision=revision+1 WHERE extension_id=$1")
            .bind(id.as_str()).execute(&db.pool).await?;
        Ok::<_, anyhow::Error>(())
    };
    let (result, revoked) = tokio::join!(controller.finish(), revoke);
    revoked?;
    assert!(
        result.is_err(),
        "revoked installed binding retained native effects"
    );
    controller.require_idle()?;
    controller.shutdown_cache().await?;
    drop(controller);
    drop(ControllerLease::acquire(lane)?);
    assert!(
        runtime
            .get_container_handle(&format!(
                "elixir-marketplace-source-writer-{}",
                config.deployment
            ))
            .await?
            .is_none()
    );
    println!(
        "installed compiled: actual stored-proof authentication/native rematch/reimport/controller/instance SQL-AES SDK, denied network/unapproved secret, original cancellation and live grant revocation passed; ACTIVE enrollment synthetic, no activation qualification"
    );
    Ok(())
}

pub(in crate::marketplace) fn compiled_fixture_configuration(
    db: &Database,
    runtime: Arc<dyn RuntimeManager>,
    root: &Path,
    lane: &Path,
    worker: &str,
    deployment: &str,
) -> Result<InstalledRuntime> {
    Ok(InstalledRuntime {
        runtime,
        playback: Arc::new(PlaybackJobManager::new(db.pool.clone(), None)),
        local_http: None,
        application_http: None,
        compiled_toolchains: Some(Arc::new(ApprovedToolchains::from_operator_digests([
            std::env::var("ELIXIR_MARKETPLACE_TOOLCHAIN_IMAGE")?.parse()?,
        ])?)),
        lane: lane.into(),
        archive: root.join("retained-archives"),
        deployment: format!("{deployment}-installed"),
        image: worker.into(),
        source_budget: SourceJobBudget {
            memory_bytes: 256 * MIB,
            cpu_millicores: 200,
            disk_bytes: 512 * MIB,
            input_bytes: 64 * MIB,
            output_bytes: 64 * MIB,
            wall_seconds: 120,
        },
        retention: ArchiveBudget {
            bytes: 64 * MIB,
            releases: 4,
        },
    })
}

// Same explicit SQL enrollment seam as the established SourceJs standing
// fixtures. This is NOT a native lifecycle/ready/activation capability.
async fn enroll_fixture(database: &Database, id: &ExtensionId) -> Result<(Uuid, Uuid)> {
    let mut tx = database.pool.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let ledger = store::lock_budget(&mut tx, standing_tests::limit()).await?;
    let old = store::load(&mut tx, database.driver, id)
        .await?
        .context("compiled fixture journal missing")?;
    let mut journal: Journal =
        canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
    let pending = journal
        .pending
        .take()
        .context("compiled fixture pending source missing")?;
    pending.facts()?;
    let home = journal.home;
    let instance = pending.instance;
    journal.active = Some(pending);
    journal.revision += 1;
    journal.mutation = Uuid::new_v4();
    journal.request = Digest::of(b"synthetic compiled ACTIVE enrollment only");
    store::write(
        &mut tx,
        ledger,
        Some(&old),
        journal,
        old.floor,
        standing_tests::limit(),
    )
    .await?;
    sqlx::query("UPDATE extensions SET enabled=TRUE WHERE extension_id=$1")
        .bind(id.as_str())
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(instance.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((home, instance))
}
async fn revision(database: &Database, instance: Uuid) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(instance.to_string())
    .fetch_one(&database.pool)
    .await?)
}
async fn wait_key(database: &Database, instance: Uuid, key: &str) -> Result<()> {
    timeout(Duration::from_secs(60),async {
        loop {
            let found:i64=sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key=$2")
                .bind(instance.to_string()).bind(key).fetch_one(&database.pool).await?;
            if found==1 {return Ok::<_,anyhow::Error>(());}
            sleep(Duration::from_millis(20)).await;
        }
    }).await.context("actual installed native SDK marker did not commit")?
}
