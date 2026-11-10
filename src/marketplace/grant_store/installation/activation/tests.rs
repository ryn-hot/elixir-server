//! Actual archive/crypto/SQL, synthetic discovery/protection observations.
//! No native start, physical admission, readiness or activation-commit claim.
use super::*;
use crate::marketplace::quarantine::SourceJobBudget;
use crate::{
    config::DatabaseConfig,
    marketplace::grant_store::installation::{cancellation, tests::Fixture},
};
mod atomic_cancel;
mod choices;
pub(super) mod compiled_native;
mod configured_runtime;
mod credentials;
mod pre_candidate;
mod source_cancel;
mod unstarted_candidate;

const MIB: u64 = 1024 * 1024;
const IMAGE: &str = "sha256:5d1db50d7282d14d23179c2ff687d1afe9ce94b5335588935cb506b84b6b4fa0";
fn limit() -> InstallBudget {
    InstallBudget {
        entries: 4,
        bytes: 4 * MIB,
    }
}
fn source_budget() -> SourceJobBudget {
    SourceJobBudget {
        memory_bytes: 64 * MIB,
        cpu_millicores: 100,
        disk_bytes: 128 * MIB,
        input_bytes: 16 * MIB,
        output_bytes: 64 * MIB,
        wall_seconds: 120,
    }
}
fn plan(f: &Fixture) -> Result<SourceActivationPlan> {
    let material = f.material()?;
    SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        IMAGE,
        &source_budget(),
    )
}
#[tokio::test]
async fn configured_source_js_plans_preserve_their_existing_identity_without_compiled_policy()
-> Result<()> {
    let f = Fixture::new().await?;
    let configuration =
        super::super::runtime::tests::configuration(&f, IMAGE, "source-plan-compatibility")?;
    let material = f.material()?;
    let original = SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        &configuration.image,
        &configuration.source_budget,
    )?;
    let configured = SourceActivationPlan::for_material(
        &material,
        &configuration,
        &SecretsManager::from_key_bytes([57; 32], false),
    )?;
    assert_eq!(original.id(), configured.id());
    assert!(original.native_runtime().is_none());
    assert!(Arc::ptr_eq(
        &configured
            .native_runtime()
            .context("configured native adapter")?,
        &configuration.runtime
    ));
    assert!(!configured.is_compiled());
    assert!(configured.source().workspace().artifact.is_none());
    Ok(())
}
fn intent(
    f: &Fixture,
    staged: &StagedInstallation,
    mutation: Uuid,
) -> Result<OwnerActivationIntent> {
    OwnerActivationIntent::for_actor(
        f.actor.clone(),
        staged,
        &f.material()?.review,
        f.revision,
        mutation,
    )
}
fn step<'a>(
    f: &'a Fixture,
    staged: &StagedInstallation,
    mutation: Uuid,
    plan: &'a SourceActivationPlan,
) -> Result<Step<'a>> {
    Ok(Step {
        database: &f.db,
        metadata: &f.metadata,
        public: &f.public,
        material: f.material()?,
        intent: Arc::new(intent(f, staged, mutation)?),
        plan,
        budget: limit(),
    })
}
async fn prepared(
    f: &Fixture,
    staged: &StagedInstallation,
    mutation: Uuid,
    plan: &SourceActivationPlan,
) -> Result<PreparedActivation> {
    match prepare_checked(step(f, staged, mutation, plan)?).await? {
        PreparedOrRecovery::Prepared(p) => Ok(p),
        PreparedOrRecovery::Recovery(_) => bail!("unexpected recovery before start claim"),
    }
}
async fn reopen(f: &mut Fixture) -> Result<()> {
    f.db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            f.root.path().join("install.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    Ok(())
}
async fn snapshot(f: &Fixture) -> Result<(String, String, i64, i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT i.record_json,i.record_sha256,i.encoded_bytes,b.revision,b.entries,b.encoded_bytes FROM marketplace_installations i CROSS JOIN marketplace_installation_budget b")
        .fetch_one(&f.db.pool).await?)
}
async fn unused(f: &Fixture) -> Result<()> {
    let enabled:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM extensions WHERE enabled=TRUE)+(SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE)+(SELECT COUNT(*) FROM providers) AS BIGINT)")
        .fetch_one(&f.db.pool).await?;
    assert_eq!(
        enabled, 0,
        "fences/claims never enable code or register providers"
    );
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn preparation_is_durable_bound_and_retriable_but_not_an_unused_stage_receipt() -> Result<()>
{
    let mut f = Fixture::new().await?;
    let stage_mutation = Uuid::new_v4();
    let staged = f.stage(stage_mutation, 0, limit()).await?;
    let plan = plan(&f)?;
    let mutation = Uuid::new_v4();
    let p = prepared(&f, &staged, mutation, &plan).await?;
    assert_eq!(p.revision(), 2);
    let before = snapshot(&f).await?;
    let r = prepared(&f, &staged, mutation, &plan).await?;
    assert_eq!(p.attempt(), r.attempt());
    assert_eq!(before, snapshot(&f).await?);
    assert!(f.stage(stage_mutation, 0, limit()).await.is_err());
    let forged_stage = StagedInstallation {
        choices: staged.choices,
        configured_instance: None,
        extension: staged.extension.clone(),
        release: staged.release,
        instance: staged.instance,
        revision: p.revision(),
    };
    let cancel =
        cancellation::OwnerCancelIntent::for_actor(f.actor.clone(), &forged_stage, Uuid::new_v4())?;
    assert!(cancellation::cancel(&f.db, cancel, limit()).await.is_err());
    assert!(prepared(&f, &staged, Uuid::new_v4(), &plan).await.is_err());
    let mut tx = f.db.pool.begin().await?;
    assert!(
        store::load(&mut tx, f.db.driver, staged.extension())
            .await?
            .unwrap()
            .receipt()
            .is_err()
    );
    tx.rollback().await?;
    reopen(&mut f).await?;
    let recovered = prepared(&f, &staged, mutation, &plan).await?;
    assert_eq!(recovered.attempt(), p.attempt());
    assert_eq!(recovered.revision(), p.revision());
    unused(&f).await?;
    Ok(())
}

#[tokio::test]
async fn start_claim_is_single_use_and_replays_only_reconcile_even_after_reopen() -> Result<()> {
    let mut f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(&f)?;
    let mutation = Uuid::new_v4();
    let p = prepared(&f, &staged, mutation, &plan).await?;
    let started = match claim_checked(step(&f, &staged, mutation, &plan)?, &p).await? {
        StartOrRecovery::Start(s) => s,
        _ => bail!("fresh claim not issued"),
    };
    assert_eq!(started.attempt(), p.attempt());
    assert_eq!(started.revision(), 3);
    assert_eq!(
        started.deployment(),
        format!("activation-{}", p.attempt().simple())
    );
    let before = snapshot(&f).await?;
    for _ in 0..2 {
        match claim_checked(step(&f, &staged, mutation, &plan)?, &p).await? {
            StartOrRecovery::Recovery(r) => {
                assert_eq!(r.attempt(), p.attempt());
                assert_eq!(r.revision(), 3);
            }
            StartOrRecovery::Start(_) => bail!("retry issued another native start claim"),
        }
    }
    reopen(&mut f).await?;
    match prepare_checked(step(&f, &staged, mutation, &plan)?).await? {
        PreparedOrRecovery::Recovery(r) => {
            assert_eq!(r.attempt(), p.attempt());
            assert_eq!(r.revision(), 3);
        }
        PreparedOrRecovery::Prepared(_) => bail!("starting cannot be reissued as prepared"),
    }
    assert!(matches!(
        claim_checked(step(&f, &staged, mutation, &plan)?, &p).await?,
        StartOrRecovery::Recovery(_)
    ));
    let cancel =
        cancellation::OwnerCancelIntent::for_prepared_actor(f.actor.clone(), &p, Uuid::new_v4())?;
    assert!(
        cancellation::cancel(&f.db, cancel, limit()).await.is_err(),
        "starting is never unused cleanup"
    );
    assert_eq!(snapshot(&f).await?, before);
    unused(&f).await?;
    Ok(())
}

#[tokio::test]
async fn prepared_cancellation_is_offline_exact_and_cannot_erase_a_starting_fence() -> Result<()> {
    let mut f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(&f)?;
    let mutation = Uuid::new_v4();
    let p = prepared(&f, &staged, mutation, &plan).await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    super::super::super::revoke_as(
        &f.db,
        &f.actor,
        staged.extension(),
        1,
        Uuid::new_v4(),
        Utc::now(),
    )
    .await?;
    sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
        .execute(&f.db.pool)
        .await?;
    let cancel_mutation = Uuid::new_v4();
    let cancellation = || {
        cancellation::OwnerCancelIntent::for_prepared_actor(f.actor.clone(), &p, cancel_mutation)
    };
    assert_eq!(
        cancellation::cancel(&f.db, cancellation()?, limit())
            .await?
            .revision(),
        3
    );
    assert_eq!(
        cancellation::cancel(&f.db, cancellation()?, limit())
            .await?
            .revision(),
        3
    );
    assert_eq!(f.counts().await?, (1, 0, 1));
    let original =
        cancellation::OwnerCancelIntent::for_actor(f.actor.clone(), &staged, cancel_mutation)?;
    assert!(
        cancellation::cancel(&f.db, original, limit())
            .await
            .is_err(),
        "action kinds cannot share an idempotency key"
    );
    reopen(&mut f).await?;
    let retry =
        cancellation::OwnerCancelIntent::for_prepared_actor(f.actor.clone(), &p, cancel_mutation)?;
    assert_eq!(
        cancellation::cancel(&f.db, retry, limit())
            .await?
            .revision(),
        3
    );
    Ok(())
}

#[tokio::test]
async fn claim_and_prepared_cancel_serialize_to_one_winner_without_enabling_anything() -> Result<()>
{
    let f = Fixture::new().await?;
    race_claim_cancel(&f).await
}
async fn race_claim_cancel(f: &Fixture) -> Result<()> {
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(f)?;
    let mutation = Uuid::new_v4();
    let p = prepared(f, &staged, mutation, &plan).await?;
    let cancellation =
        cancellation::OwnerCancelIntent::for_prepared_actor(f.actor.clone(), &p, Uuid::new_v4())?;
    let (claim, cancel) = tokio::join!(
        claim_checked(step(f, &staged, mutation, &plan)?, &p),
        cancellation::cancel(&f.db, cancellation, limit())
    );
    match (claim, cancel) {
        (Ok(StartOrRecovery::Start(_)), Err(_)) => {
            unused(f).await?;
        }
        (Err(_), Ok(c)) => {
            assert_eq!(c.revision(), 3);
            assert_eq!(f.counts().await?, (1, 0, 1));
        }
        _ => bail!("exactly one start/cancel must commit"),
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_claims_issue_one_token_and_the_other_is_recovery_data() -> Result<()> {
    let f = Fixture::new().await?;
    race_claims(&f).await?;
    Ok(())
}
async fn race_claims(f: &Fixture) -> Result<NativeStartCheck> {
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(f)?;
    let mutation = Uuid::new_v4();
    let p = prepared(f, &staged, mutation, &plan).await?;
    let (a, b) = tokio::join!(
        claim_checked(step(f, &staged, mutation, &plan)?, &p),
        claim_checked(step(f, &staged, mutation, &plan)?, &p)
    );
    let mut starts = 0;
    let mut check = None;
    for outcome in [a, b] {
        match outcome? {
            StartOrRecovery::Start(start) => {
                starts += 1;
                check = Some(start.native_check());
            }
            StartOrRecovery::Recovery(r) => assert_eq!(r.attempt(), p.attempt()),
        }
    }
    assert_eq!(starts, 1);
    unused(f).await?;
    check.context("race lost the original claimant's read-only check")
}

#[tokio::test]
async fn preparation_and_claim_recheck_authority_source_metadata_and_capacity_without_partial_writes()
-> Result<()> {
    for claiming in [false, true] {
        for fault in [
            "metadata", "public", "grant", "epoch", "session", "home", "budget", "journal",
            "manifest",
        ] {
            let f = Fixture::new().await?;
            let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
            let plan = plan(&f)?;
            let mutation = Uuid::new_v4();
            let p = if claiming {
                Some(prepared(&f, &staged, mutation, &plan).await?)
            } else {
                None
            };
            let mut action = step(&f, &staged, mutation, &plan)?;
            match fault {
                "metadata" => (f.meta_owner)(false),
                "public" => (f.public_owner)(false),
                "grant" => Arc::get_mut(&mut action.intent).unwrap().grant_revision += 1,
                "epoch" => {
                    sqlx::query(
                        "UPDATE marketplace_extension_grants SET revocation_revision=revision+1",
                    )
                    .execute(&f.db.pool)
                    .await?;
                }
                "session" => {
                    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='offline activation fixture'").execute(&f.db.pool).await?;
                }
                "home" => {
                    sqlx::query("UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL").execute(&f.db.pool).await?;
                }
                "budget" => {
                    let n: i64 = sqlx::query_scalar(
                        "SELECT encoded_bytes FROM marketplace_installation_budget",
                    )
                    .fetch_one(&f.db.pool)
                    .await?;
                    action.budget.bytes = if claiming { n as u64 - 1 } else { n as u64 };
                }
                "journal" => {
                    sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                        .bind(Digest::of(b"bad").to_string())
                        .execute(&f.db.pool)
                        .await?;
                }
                "manifest" => {
                    sqlx::query("UPDATE extensions SET manifest_json='{}'")
                        .execute(&f.db.pool)
                        .await?;
                }
                _ => unreachable!(),
            }
            let before = snapshot(&f).await?;
            assert!(
                action.advance(p.as_ref()).await.is_err(),
                "claim={claiming} {fault}"
            );
            assert_eq!(snapshot(&f).await?, before, "claim={claiming} {fault}");
            unused(&f).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn status_survives_lost_source_and_bad_revoked_grant_but_never_proves_cleanup() -> Result<()>
{
    for starting in [false, true] {
        let f = Fixture::new().await?;
        let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
        let plan = plan(&f)?;
        let mutation = Uuid::new_v4();
        let p = prepared(&f, &staged, mutation, &plan).await?;
        if starting {
            assert!(matches!(
                claim_checked(step(&f, &staged, mutation, &plan)?, &p).await?,
                StartOrRecovery::Start(_)
            ));
        }
        (f.meta_owner)(false);
        (f.public_owner)(false);
        super::super::super::revoke_as(
            &f.db,
            &f.actor,
            staged.extension(),
            1,
            Uuid::new_v4(),
            Utc::now(),
        )
        .await?;
        sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
            .execute(&f.db.pool)
            .await?;
        if starting {
            // A process/candidate could have written state. Inspect is status,
            // not a false claim that disabled flags prove an unused instance.
            sqlx::query("INSERT INTO marketplace_instance_sdk_values (instance_id,key,revision,value_encrypted,encoded_bytes) VALUES ($1,'candidate',1,'opaque',6)")
                .bind(staged.instance().to_string()).execute(&f.db.pool).await?;
        }
        let before = snapshot(&f).await?;
        match inspect_as(&f.db, f.actor.clone(), staged.extension(), limit()).await? {
            ActivationStatus::Prepared(r) => {
                assert!(!starting);
                assert_eq!(r.attempt(), p.attempt());
            }
            ActivationStatus::Starting(r) => {
                assert!(starting);
                assert_eq!(r.attempt(), p.attempt());
            }
            ActivationStatus::SourcePrepared(_) => bail!("unprobed source cannot be prepared"),
            ActivationStatus::Quiescing(_) => bail!("unprobed source cannot quiesce"),
            ActivationStatus::StateCopied(_) => bail!("unprobed source cannot copy state"),
            ActivationStatus::CandidateStarting(_) => {
                bail!("unprobed source cannot start candidate code")
            }
        }
        assert_eq!(before, snapshot(&f).await?);
    }
    Ok(())
}

#[tokio::test]
async fn recipe_binding_rejects_excess_resources_changed_image_and_mutation_reuse() -> Result<()> {
    let f = Fixture::new().await?;
    let probe = plan(&f)?.probe_limits()?;
    probe.validate()?;
    assert!(
        probe.sdk.is_none(),
        "interface probes cannot enable effects"
    );
    assert!(
        probe.minimum_memory_bytes()? <= source_budget().memory_bytes,
        "probe fits the actual original allocation, not a larger worker"
    );
    assert!(
        probe.json_bytes >= elixir_marketplace::source_execution::SOURCE_PROBE_RESULT.len() as u64
    );
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let material = f.material()?;
    let authority = material.authority.verify()?;
    for what in ["image", "memory", "cpu", "disk", "input", "output", "time"] {
        let mut budget = source_budget();
        let image = if what == "image" {
            "worker:latest"
        } else {
            IMAGE
        };
        match what {
            "memory" => budget.memory_bytes = 256 * MIB,
            "cpu" => budget.cpu_millicores = 101,
            "disk" => budget.disk_bytes = 1,
            "input" => budget.input_bytes = 1,
            "output" => budget.output_bytes = 1,
            "time" => budget.wall_seconds = 0,
            _ => {}
        }
        assert!(
            SourceActivationPlan::new(authority.release(), &material.review, image, &budget)
                .is_err(),
            "{what}"
        );
    }
    let plan = plan(&f)?;
    assert_eq!(plan.image(), IMAGE);
    assert_eq!(plan.source().workspace().release_id, staged.release());
    let mutation = Uuid::new_v4();
    let p = prepared(&f, &staged, mutation, &plan).await?;
    let changed = SourceActivationPlan::new(
        authority.release(),
        &material.review,
        IMAGE,
        &SourceJobBudget {
            memory_bytes: 128 * MIB,
            ..source_budget()
        },
    )?;
    assert_ne!(changed.id(), plan.id());
    assert!(
        prepare_checked(step(&f, &staged, mutation, &changed)?)
            .await
            .is_err()
    );
    assert!(
        claim_checked(step(&f, &staged, mutation, &changed)?, &p)
            .await
            .is_err()
    );
    unused(&f).await?;
    Ok(())
}

#[tokio::test]
async fn phase_records_are_closed_bounded_and_do_not_normalize_explicit_nulls() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let json = snapshot(&f).await?.0;
    assert!(!json.contains("activation"));
    let mut value: serde_json::Value = serde_json::from_str(&json)?;
    value["activation"] = serde_json::Value::Null;
    assert!(
        canonical::decode::<Journal>(&canonical::to_vec(&value)?, record::MAX_RECORD_BYTES)
            .is_err()
    );
    let plan = plan(&f)?;
    prepared(&f, &staged, Uuid::new_v4(), &plan).await?;
    let mut record: Journal =
        canonical::decode(snapshot(&f).await?.0.as_bytes(), record::MAX_RECORD_BYTES)?;
    let fence = record.activation.as_ref().unwrap();
    assert!(
        canonical::to_vec(fence)?.len() + 64 < 1024,
        "staging leaves enough fence headroom"
    );
    record.activation.as_mut().unwrap().phase = ActivationPhase::Starting;
    assert!(
        record.validate().is_err(),
        "phase and revision must advance atomically"
    );
    record.revision += 1;
    record.validate()?;
    record.pending = None;
    assert!(
        record.validate().is_err(),
        "a fence without its candidate is corrupt"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh isolated loopback PostgreSQL activation-fence races/reopen; not native activation"]
async fn postgres_activation_claim_cancel_and_duplicate_claim_races_are_durable() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_ACTIVATION_POSTGRES_URL")?;
    let cancel_url = std::env::var("ELIXIR_TEST_MARKETPLACE_ACTIVATION_CANCEL_POSTGRES_URL")?;
    if url == cancel_url {
        bail!("two distinct fresh fixture databases required");
    }
    let db = fresh_postgres(&url).await?;
    let cancel_db = fresh_postgres(&cancel_url).await?;
    let first_name: String = sqlx::query_scalar("SELECT CAST(current_database() AS TEXT)")
        .fetch_one(&db.pool)
        .await?;
    let second_name: String = sqlx::query_scalar("SELECT CAST(current_database() AS TEXT)")
        .fetch_one(&cancel_db.pool)
        .await?;
    if first_name == second_name {
        bail!("distinct fresh fixture database names required");
    }
    let mut f = Fixture::with_database(tempfile::tempdir()?, db).await?;
    let check = race_claims(&f).await?;
    require_native_start(
        &f.db,
        &f.metadata,
        &f.public,
        &f.archive,
        &check,
        &plan(&f)?,
        limit(),
    )
    .await?;
    let id = f.descriptor.release.payload.extension_id.clone();
    let status = inspect_as(&f.db, f.actor.clone(), &id, limit()).await?;
    let expected = match status {
        ActivationStatus::Starting(r) => r.attempt(),
        _ => bail!("claim fence missing"),
    };
    f.db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    require_native_start(
        &f.db,
        &f.metadata,
        &f.public,
        &f.archive,
        &check,
        &plan(&f)?,
        limit(),
    )
    .await?;
    match inspect_as(&f.db, f.actor.clone(), &id, limit()).await? {
        ActivationStatus::Starting(r) => assert_eq!(r.attempt(), expected),
        _ => bail!("reopen lost fence"),
    }
    f.db.pool.close().await;
    let cancel_fixture = Fixture::with_database(tempfile::tempdir()?, cancel_db).await?;
    race_claim_cancel(&cancel_fixture).await?;
    cancel_fixture.db.pool.close().await;
    Ok(())
}

pub(in crate::marketplace::grant_store::installation) async fn fresh_postgres(
    url: &str,
) -> Result<Database> {
    fixture_url(url)?;
    let db = Database::connect(&DatabaseConfig {
        url: url.to_owned(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    if db.driver != DatabaseDriver::Postgres {
        bail!("PostgreSQL fixture required");
    }
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')",
    )
    .fetch_one(&db.pool)
    .await?;
    if tables != 0 {
        bail!("activation fixture database must be empty; no reset");
    }
    Ok(db)
}

fn fixture_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url)?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql")
        || !matches!(
            parsed.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
        )
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.path().starts_with("/elixir_marketplace_activation_")
    {
        bail!("dedicated loopback activation fixture database required");
    }
    Ok(())
}

#[test]
fn postgres_fixture_targets_cannot_override_host_or_connect_to_a_normal_application_database()
-> Result<()> {
    for url in [
        "postgresql://user@127.0.0.1:54933/elixir_marketplace_activation_claim",
        "postgres://user@localhost:54933/elixir_marketplace_activation_cancel",
        "postgresql://user@[::1]:54933/elixir_marketplace_activation_claim",
    ] {
        fixture_url(url)?;
    }
    for url in [
        "sqlite://localhost/elixir_marketplace_activation_claim",
        "postgresql://user@example.com/elixir_marketplace_activation_claim",
        "postgresql://user@127.0.0.1/elixir",
        "postgresql://user@127.0.0.1/elixir_marketplace_activation_claim?host=example.com",
        "postgresql://user@127.0.0.1/elixir_marketplace_activation_claim#override",
    ] {
        assert!(fixture_url(url).is_err(), "{url}");
    }
    Ok(())
}

#[tokio::test]
async fn a_start_claim_needs_no_extra_allocation_when_the_encoded_record_does_not_grow()
-> Result<()> {
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(&f)?;
    let mutation = Uuid::new_v4();
    let p = prepared(&f, &staged, mutation, &plan).await?;
    let before = snapshot(&f).await?;
    let mut action = step(&f, &staged, mutation, &plan)?;
    action.budget.bytes = before.5 as u64;
    assert!(matches!(
        claim_checked(action, &p).await?,
        StartOrRecovery::Start(_)
    ));
    let after = snapshot(&f).await?;
    assert_eq!(before.2, after.2);
    assert_eq!(before.5, after.5);
    assert_eq!(after.4, 1);
    assert_eq!(after.3, before.3 + 1);
    unused(&f).await?;
    Ok(())
}

mod native_gate;
#[cfg(unix)]
pub(in crate::marketplace::grant_store::installation) mod quiescence;
#[cfg(unix)]
mod source_prepared;
