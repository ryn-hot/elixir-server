//! Actual scoped journals and owned runtime lifecycle/commit. The no-effects
//! probe/discovery observations remain fixtures; ACTIVE is never synthesized.
use super::super::candidate::{self, CandidateClaim, CandidateOrRecovery};
use super::super::commit::{self, OwnerCommitIntent};
use super::super::quiescence;
use super::*;
use crate::marketplace::grant_store::installation::runtime::tests::{
    assert_disposed, configuration, limits,
};
use crate::marketplace::grant_store::{
    installation::{
        InstallTarget,
        runtime::{InstalledController, InstalledRuntime, RetiredCandidate},
        standing::InstalledBinding,
    },
    sdk,
};
use elixir_marketplace::source_execution::RuntimeJson;
use serde_json::{Value, json};

fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
async fn controller(f: &Fixture, image: &str) -> Result<(InstalledController, InstalledRuntime)> {
    let config = configuration(f, image, "configured-native")?;
    Ok((
        InstalledController::open(f.db.clone(), manager(), config.clone()).await?,
        config,
    ))
}
async fn claim(
    f: &Fixture,
    owner: &mut InstalledController,
    config: &InstalledRuntime,
    staged: &StagedInstallation,
) -> Result<CandidateClaim> {
    claim_with_credentials(f, owner, config, staged, None).await
}
pub(super) fn principal(f: &Fixture) -> CurrentPrincipal {
    CurrentPrincipal::fixture_owner(
        f.actor.user,
        f.actor.home,
        f.actor.profile,
        f.actor.session,
        f.actor.authorization_revision,
        f.actor.expires,
        crate::authz::CapabilitySet::all(),
    )
}
pub(super) async fn claim_with_credentials(
    f: &Fixture,
    owner: &mut InstalledController,
    config: &InstalledRuntime,
    staged: &StagedInstallation,
    changes: Option<Vec<CredentialChange>>,
) -> Result<CandidateClaim> {
    let material = f.material()?;
    let plan = SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        &config.image,
        &config.source_budget,
    )?;
    let mutation = Uuid::new_v4();
    let intent = Arc::new(match changes {
        Some(changes) => OwnerActivationIntent::new_with_credentials(
            &principal(f),
            staged,
            &material.review,
            f.revision,
            mutation,
            &manager(),
            changes,
        )?,
        None => intent(f, staged, mutation)?,
    });
    let checked = || -> Result<Step<'_>> {
        Ok(Step {
            database: &f.db,
            metadata: &f.metadata,
            public: &f.public,
            material: f.material()?,
            intent: intent.clone(),
            plan: &plan,
            budget: limit(),
        })
    };
    let prepared = match prepare_checked(checked()?).await? {
        PreparedOrRecovery::Prepared(prepared) => prepared,
        _ => bail!("original preparation required"),
    };
    let start = match claim_checked(checked()?, &prepared).await? {
        StartOrRecovery::Start(start) => start,
        _ => bail!("original source claim required"),
    };
    // Only this source-only probe boundary is synthetic, NOT candidate native
    // execution/readiness/retirement, ACTIVE enrollment or activation commit.
    let retained = source_prepared::retain(f, &start.native_check(), &plan, limit()).await?;
    let fenced = quiescence::quiesce_checked(checked()?, &retained).await?;
    let scope = quiescence::observe_checked(checked()?, &fenced).await?;
    let guard = owner.quiesce(scope).await?;
    let mut copy_step = checked()?;
    copy_step.database = guard.database();
    let copied = super::super::state_copy::copy_checked(copy_step, &guard).await?;
    let mut claim_step = checked()?;
    claim_step.database = guard.database();
    match candidate::claim_checked(claim_step, &guard, &copied).await? {
        CandidateOrRecovery::Claim(claim) => Ok(claim),
        _ => bail!("original candidate claim required"),
    }
}
async fn stage(f: &Fixture, target: InstallTarget, expected: i64) -> Result<StagedInstallation> {
    f.stage_configured(target, Uuid::new_v4(), expected, limit())
        .await
}
fn new(name: &str) -> InstallTarget {
    InstallTarget::New { name: name.into() }
}
async fn stored(f: &Fixture, owner: Uuid) -> Result<Stored> {
    let mut tx = f.db.pool.begin().await?;
    let stored = store::load_configured(
        &mut tx,
        f.db.driver,
        f.material()?.review.scope().extension_id(),
        Some(owner),
    )
    .await?
    .context("configured journal")?;
    tx.rollback().await?;
    Ok(stored)
}
async fn native_retire(
    owner: &mut InstalledController,
    claim: CandidateClaim,
) -> Result<RetiredCandidate> {
    let mut slot = Some(claim);
    owner.begin_candidate(&mut slot, limits())?;
    assert!(slot.is_none());
    owner.finish_candidate().await
}
async fn invoke(f: &Fixture, owner: &mut InstalledController, id: Uuid) -> Result<Value> {
    let binding = InstalledBinding::load_configured(
        &f.db,
        f.actor.home,
        f.material()?.review.scope().extension_id(),
        id,
        limit(),
    )
    .await?;
    owner.begin(binding, limits(), RuntimeJson::from_value(json!({}))?)?;
    Ok(serde_json::from_str(owner.finish().await?.as_str())?)
}
fn source(version: &str) -> String {
    format!(
        "import {{state,getSecret}} from '@elixir/sdk/v1';export async function invoke(a){{if(a.hold){{await state.set('held',true);for(;;){{}}}}return{{version:'{version}',cursor:await state.get('cursor'),key:await getSecret('source_api')}}}}export async function migrate(){{await state.set('cursor',(await state.get('cursor')??0)+1)}}export async function checkReady(){{return (await state.get('cursor'))>0 && (await getSecret('source_api'))?.endsWith('-private')===true}}"
    )
}

#[tokio::test]
async fn configured_candidate_effects_bind_owner_and_namespace_but_cannot_activate_from_status()
-> Result<()> {
    let f = Fixture::new().await?;
    let (mut owner, config) = controller(&f, &format!("sha256:{}", "1".repeat(64))).await?;
    let a = stage(&f, new("A"), 0).await?;
    let claim = claim(&f, &mut owner, &config, &a).await?;
    let id = a.configured_instance.context("owner")?;
    let b = stage(&f, new("B"), 0).await?;
    let request =
        RuntimeJson::from_value(json!({"operation":"state_set","key":"cursor","value":17}))?;
    sdk::CandidateScope {
        database: &f.db,
        secrets: &manager(),
        binding: claim.binding(),
    }
    .fixture_execute(&request, 65536, || Ok(()))
    .await?;
    let cipher: String = sqlx::query_scalar(
        "SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1",
    )
    .bind(a.instance().to_string())
    .fetch_one(&f.db.pool)
    .await?;
    let envelope: Value = serde_json::from_str(&manager().decrypt(&cipher)?)?;
    assert_eq!(envelope["owner_instance_id"], json!(id));
    assert_eq!(envelope["instance_id"], json!(a.instance()));
    let other: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1",
    )
    .bind(b.instance().to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(other, 0);
    assert!(
        InstalledBinding::load_configured(&f.db, f.actor.home, a.extension(), id, limit())
            .await
            .is_err()
    );
    let status =
        inspect_configured_as(&f.db, f.actor.clone(), a.extension(), Some(id), limit()).await?;
    assert!(matches!(status, ActivationStatus::CandidateStarting(_)));
    let before = canonical::to_vec(&stored(&f, id).await?.journal)?;
    // Occupy the expensive lane: this original task fails BEFORE native open.
    let lane = crate::marketplace::lease::ControllerLease::acquire(&config.lane)?;
    let retired = native_retire(&mut owner, claim).await?;
    assert!(retired.migration_result().is_err());
    assert!(
        commit::activate(
            &retired,
            OwnerCommitIntent::fixture(f.actor.clone(), Uuid::new_v4())?
        )
        .await
        .is_err()
    );
    assert_eq!(canonical::to_vec(&stored(&f, id).await?.journal)?, before);
    drop(lane);
    let pending = super::super::retirement::discard(
        &retired,
        super::super::retirement::OwnerDiscardIntent::fixture(f.actor.clone(), Uuid::new_v4())?,
    )
    .await?;
    assert_eq!(pending.configured_instance, Some(id));
    Ok(())
}

#[tokio::test]
async fn configured_reference_cannot_cross_owners_even_with_the_same_release_and_plan() -> Result<()>
{
    let f = Fixture::new().await?;
    let a = stage(&f, new("A"), 0).await?;
    let b = stage(&f, new("B"), 0).await?;
    let plan = plan(&f)?;
    let mutation = Uuid::new_v4();
    let prepared = prepared(&f, &a, mutation, &plan).await?;
    let mut forged = PreparedActivation {
        reference: reference(&stored(&f, a.configured_instance.unwrap()).await?)?,
    };
    forged.reference.configured_instance = b.configured_instance;
    assert!(
        claim_checked(step(&f, &a, mutation, &plan)?, &forged)
            .await
            .is_err()
    );
    assert!(matches!(
        claim_checked(step(&f, &a, mutation, &plan)?, &prepared).await?,
        StartOrRecovery::Start(_)
    ));
    Ok(())
}

async fn native_workflow(f: &mut Fixture, image: &str) -> Result<()> {
    let (mut owner, config) = controller(f, image).await?;
    let first = stage(f, new("Main"), 0).await?;
    // The same real source must refuse first readiness without its required
    // credential. Only the original joined failure can reset the private fork.
    let missing = claim(f, &mut owner, &config, &first).await?;
    let deployment = missing.deployment();
    let failed = native_retire(&mut owner, missing).await?;
    assert_disposed(f, image, &deployment).await?;
    assert!(failed.migration_result().is_err());
    let migrated: String = sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
        .bind(first.instance().to_string()).fetch_one(&f.db.pool).await?;
    let envelope: Value = serde_json::from_str(&manager().decrypt(&migrated)?)?;
    assert_eq!(
        envelope["value"],
        json!(1),
        "actual native migration ran before readiness refused the missing credential"
    );
    assert!(
        commit::activate(
            &failed,
            OwnerCommitIntent::fixture(f.actor.clone(), Uuid::new_v4())?
        )
        .await
        .is_err()
    );
    let a = super::super::retirement::discard(
        &failed,
        super::super::retirement::OwnerDiscardIntent::fixture(f.actor.clone(), Uuid::new_v4())?,
    )
    .await?;
    assert_eq!(a.instance(), first.instance());
    assert_eq!(a.configured_instance, first.configured_instance);
    let head: (i64,i64,i64) = sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(a.instance().to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(head, (1, 0, 0));
    eprintln!(
        "configured native: missing credential refused readiness/activation; original joined owner discarded its private migration"
    );
    let id_a = a.configured_instance.context("A")?;
    let claim_a = claim_with_credentials(
        f,
        &mut owner,
        &config,
        &a,
        Some(vec![CredentialChange::set(
            "source_api",
            "A-private",
            false,
        )]),
    )
    .await?;
    let deployment = claim_a.deployment();
    let retired_a = native_retire(&mut owner, claim_a).await?;
    assert_disposed(f, image, &deployment).await?;
    retired_a
        .migration_result()
        .map_err(|e| anyhow::anyhow!("first native readiness: {e:#}"))?;
    let mutation = Uuid::new_v4();
    let active_a = commit::activate(
        &retired_a,
        OwnerCommitIntent::fixture(f.actor.clone(), mutation)?,
    )
    .await?;
    let retry = commit::activate(
        &retired_a,
        OwnerCommitIntent::fixture(f.actor.clone(), mutation)?,
    )
    .await?;
    assert_eq!(active_a.revision, retry.revision);
    assert_eq!(active_a.namespace, a.instance());
    eprintln!("configured native: A v1 activated and exact commit retry acknowledged");
    let b = stage(f, new("Family"), 0).await?;
    let id_b = b.configured_instance.context("B")?;
    let claim_b = claim_with_credentials(
        f,
        &mut owner,
        &config,
        &b,
        Some(vec![CredentialChange::set(
            "source_api",
            "B-private",
            false,
        )]),
    )
    .await?;
    let deployment = claim_b.deployment();
    let retired_b = native_retire(&mut owner, claim_b).await?;
    assert_disposed(f, image, &deployment).await?;
    commit::activate(
        &retired_b,
        OwnerCommitIntent::fixture(f.actor.clone(), Uuid::new_v4())?,
    )
    .await?;
    assert_eq!(
        invoke(f, &mut owner, id_a).await?,
        json!({"version":"1.0.0","cursor":1,"key":"A-private"})
    );
    assert_eq!(
        invoke(f, &mut owner, id_b).await?,
        json!({"version":"1.0.0","cursor":1,"key":"B-private"})
    );
    eprintln!("configured native: A and B execute their own v1 state and credentials");
    let b_before = canonical::to_vec(&stored(f, id_b).await?.journal)?;
    let b_cipher: String = sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
        .bind(b.instance().to_string()).fetch_one(&f.db.pool).await?;
    let provider = Uuid::new_v4();
    let media = Uuid::new_v4();
    sqlx::query("INSERT INTO providers(provider_id,instance_id,capability,cardinality) VALUES($1,$2,'media.fixture','many')")
        .bind(provider.to_string()).bind(id_a.to_string()).execute(&f.db.pool).await?;
    sqlx::query(
        "INSERT INTO media_items(id,type,title) VALUES($1,'movie','preserved configured media')",
    )
    .bind(media.to_string())
    .execute(&f.db.pool)
    .await?;
    sqlx::query("INSERT INTO media_ownerships(ownership_id,media_item_id,owner_type,owner_instance_id,owner_provider_id) VALUES($1,$2,'extension',$3,$4)")
        .bind(Uuid::new_v4().to_string()).bind(media.to_string()).bind(id_a.to_string()).bind(provider.to_string()).execute(&f.db.pool).await?;
    // A package/proof pair reserves about 34 MiB, even for these tiny sources.
    // Three releases fit the unchanged 128 MiB owner allocation; four do not.
    // Two updates exercise both creating and collecting the previous slot.
    for version in ["2.0.0", "3.0.0"] {
        let old = stored(f, id_a).await?;
        f.upgrade(version, Some(&source(version))).await?;
        let pending = stage(
            f,
            InstallTarget::Existing { instance: id_a },
            old.journal.revision,
        )
        .await?;
        if version == "2.0.0" {
            let binding_b = InstalledBinding::load_configured(
                &f.db,
                f.actor.home,
                b.extension(),
                id_b,
                limit(),
            )
            .await?;
            owner.begin(
                binding_b,
                limits(),
                RuntimeJson::from_value(json!({"hold":true}))?,
            )?;
            tokio::time::timeout(std::time::Duration::from_secs(120),async {
                loop {
                    let started: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='held'")
                        .bind(b.instance().to_string()).fetch_one(&f.db.pool).await?;
                    if started == 1 { return Ok::<_,anyhow::Error>(()); }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }).await??;
        }
        let claim_a = claim(f, &mut owner, &config, &pending).await?;
        let claim_a = if version == "2.0.0" {
            let mut slot = Some(claim_a);
            assert!(owner.begin_candidate(&mut slot, limits()).is_err());
            assert!(
                slot.is_some(),
                "waiting behind B must not consume/recreate A's original claim"
            );
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(10), owner.finish())
                    .await
                    .is_err(),
                "A quiescence must leave the original B native task running, not signal or join it"
            );
            owner.cancel().await?; // explicitly join B before using the same native lane
            eprintln!(
                "configured native: A quiescence left B running; B explicitly cancelled and joined"
            );
            slot.take().unwrap()
        } else {
            claim_a
        };
        // B remains executable while A alone is fenced, despite desired/global
        // metadata now showing a newer source. This is actual native SDK use.
        assert_eq!(
            invoke(f, &mut owner, id_b).await?,
            json!({"version":"1.0.0","cursor":1,"key":"B-private"})
        );
        let deployment = claim_a.deployment();
        let retired = native_retire(&mut owner, claim_a).await?;
        assert_disposed(f, image, &deployment).await?;
        retired
            .migration_result()
            .map_err(|e| anyhow::anyhow!("native {version} readiness: {e:#}"))?;
        let committed = commit::activate(
            &retired,
            OwnerCommitIntent::fixture(f.actor.clone(), Uuid::new_v4())?,
        )
        .await?;
        assert_eq!(committed.configured_instance, id_a);
        assert_eq!(
            invoke(f, &mut owner, id_a).await?,
            json!({"version":version,"cursor":version.as_bytes()[0]-b'0',"key":"A-private"})
        );
        assert_eq!(
            canonical::to_vec(&stored(f, id_b).await?.journal)?,
            b_before
        );
        let unchanged: String = sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
            .bind(b.instance().to_string()).fetch_one(&f.db.pool).await?;
        assert_eq!(unchanged, b_cipher);
        let namespaces: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE instance_id=$1",
        )
        .bind(id_a.to_string())
        .fetch_one(&f.db.pool)
        .await?;
        assert_eq!(
            namespaces, 2,
            "active/previous only, older private generation collected without touching operational IDs"
        );
        eprintln!("configured native: A {version} committed with two namespaces; B unchanged");
    }
    let a_before = canonical::to_vec(&stored(f, id_a).await?.journal)?;
    let quota_error = f
        .upgrade("4.0.0", Some(&source("4.0.0")))
        .await
        .err()
        .context("a fourth archive must not bypass the unchanged owner allocation")?;
    assert!(
        quota_error
            .to_string()
            .contains("owner source archive quota exhausted"),
        "unexpected fourth-release failure: {quota_error:#}"
    );
    assert_eq!(
        canonical::to_vec(&stored(f, id_a).await?.journal)?,
        a_before
    );
    assert_eq!(
        canonical::to_vec(&stored(f, id_b).await?.journal)?,
        b_before
    );
    let namespace_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(
        namespace_count, 3,
        "quota denial cannot allocate a generation"
    );
    let releases = f.root.path().join("archive/releases");
    assert!(!releases.join(".pending").exists());
    assert_eq!(std::fs::read_dir(releases)?.count(), 3);
    assert_eq!(
        invoke(f, &mut owner, id_a).await?,
        json!({"version":"3.0.0","cursor":3,"key":"A-private"})
    );
    eprintln!(
        "configured native: fourth-release quota denial preserved both journals and executable A v3"
    );
    assert_eq!(f.counts().await?, (1, 2, 2));
    let ownership: (String, String) = sqlx::query_as(
        "SELECT owner_instance_id,owner_provider_id FROM media_ownerships WHERE media_item_id=$1",
    )
    .bind(media.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(ownership, (id_a.to_string(), provider.to_string()));
    assert!(
        InstalledBinding::load(&f.db, f.actor.home, a.extension(), id_a, limit())
            .await
            .is_err(),
        "legacy API must not fall back to a configured journal"
    );
    assert!(
        InstalledBinding::load_configured(
            &f.db,
            f.actor.home,
            a.extension(),
            a.instance(),
            limit()
        )
        .await
        .is_err(),
        "namespace IDs are not configured selectors"
    );
    assert_disposed(f, image, &config.deployment).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "explicit offline native two-configured-instance lifecycle/atomic commit/SDK secrets/state/previous GC; discovery and probe observations synthetic"]
async fn native_configured_instances_activate_and_update_independently_without_operational_id_replacement()
-> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    anyhow::ensure!(
        crate::marketplace::runtime::image_is_pinned(&image),
        "immutable worker image required"
    );
    native_workflow(
        &mut Fixture::with_source(Some(&source("1.0.0"))).await?,
        &image,
    )
    .await
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL plus offline native configured generations/commit/SDK/GC; no public install qualification"]
async fn postgres_native_configured_instances_activate_and_update_independently() -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    anyhow::ensure!(
        crate::marketplace::runtime::image_is_pinned(&image),
        "immutable worker image required"
    );
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_CONFIGURED_NATIVE_POSTGRES_URL")?;
    let db = super::fresh_postgres(&url).await?;
    let mut f = Fixture::with_database_and_source(
        tempfile::tempdir()?,
        db,
        None,
        "fixture-provider",
        Some(&source("1.0.0")),
    )
    .await?;
    native_workflow(&mut f, &image).await
}
