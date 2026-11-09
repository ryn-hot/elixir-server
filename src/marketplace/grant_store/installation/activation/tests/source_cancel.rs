//! Actual crypto/authentication/SQL, synthetic probe-disposal completion ONLY.
//! Production cancellation accepts the original PreparedSource, not these refs.
use super::super::quiescence as cutover;
use super::*;
use crate::marketplace::grant_store::installation::{
    InstallTarget,
    runtime::{InstalledController, tests::configuration},
};
use cancellation::{CancelledInstallation, OwnerCancelIntent};

async fn stage(f: &Fixture) -> Result<StagedInstallation> {
    let mutation = Uuid::new_v4();
    f.stage_configured(
        InstallTarget::New {
            name: format!("Source cancel {mutation}"),
        },
        mutation,
        0,
        limit(),
    )
    .await
}
fn checked<'a>(
    f: &'a Fixture,
    plan: &'a SourceActivationPlan,
    intent: &Arc<OwnerActivationIntent>,
) -> Result<Step<'a>> {
    Ok(Step {
        database: &f.db,
        metadata: &f.metadata,
        public: &f.public,
        material: f.material()?,
        intent: intent.clone(),
        plan,
        budget: limit(),
    })
}
async fn retained(
    f: &Fixture,
    staged: &StagedInstallation,
) -> Result<(
    SourceActivationPlan,
    Arc<OwnerActivationIntent>,
    ActivationReference,
)> {
    let plan = plan(f)?;
    let intent = Arc::new(intent(f, staged, Uuid::new_v4())?);
    let prepared = match prepare_checked(checked(f, &plan, &intent)?).await? {
        PreparedOrRecovery::Prepared(value) => value,
        _ => bail!("original preparation required"),
    };
    let started = match claim_checked(checked(f, &plan, &intent)?, &prepared).await? {
        StartOrRecovery::Start(value) => value,
        _ => bail!("original source start required"),
    };
    let reference = source_prepared::retain(f, &started.native_check(), &plan, limit()).await?;
    Ok((plan, intent, reference))
}
fn request(
    principal: &CurrentPrincipal,
    reference: &ActivationReference,
    mutation: Uuid,
) -> Result<OwnerCancelIntent> {
    OwnerCancelIntent::fixture_disposed_source(principal, reference, mutation)
}
async fn cancel(
    f: &Fixture,
    principal: &CurrentPrincipal,
    reference: &ActivationReference,
    mutation: Uuid,
) -> Result<CancelledInstallation> {
    cancellation::cancel(&f.db, request(principal, reference, mutation)?, limit()).await
}
async fn record(f: &Fixture, owner: Option<Uuid>) -> Result<Stored> {
    let mut tx = f.db.pool.begin().await?;
    let stored = store::load_configured(
        &mut tx,
        f.db.driver,
        &f.descriptor.release.payload.extension_id,
        owner,
    )
    .await?
    .context("journal")?;
    tx.rollback().await?;
    Ok(stored)
}

#[tokio::test]
async fn disposed_source_cancel_is_offline_exact_reserved_and_preserves_other_instances()
-> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let (_, _, reference) = retained(&f, &a).await?;
    let b = stage(&f).await?;
    let before_b = canonical::to_vec(&record(&f, b.configured_instance).await?.journal)?;
    let owner = a.configured_instance.context("owner")?;
    sqlx::query("UPDATE extension_instances SET config_json=$1 WHERE instance_id=$2")
        .bind(r#"{"owner":"preserved"}"#)
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    let before_owner: (String,Option<String>,String) = sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    let secret_id = Uuid::new_v4();
    let legacy_cipher = crate::secrets::SecretsManager::from_key_bytes([57; 32], false)
        .encrypt("legacy-config-private")?;
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'source_api',$3,FALSE)")
        .bind(secret_id.to_string()).bind(owner.to_string()).bind(&legacy_cipher).execute(&f.db.pool).await?;
    let mut caps = crate::authz::CapabilitySet::all();
    caps.remove(Capability::SecretsManage);
    let principal = CurrentPrincipal::fixture_owner(
        f.actor.user,
        f.actor.home,
        f.actor.profile,
        f.actor.session,
        f.actor.authorization_revision,
        f.actor.expires,
        caps,
    );
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}'")
        .execute(&f.db.pool)
        .await?;
    // Exact, non-growing cleanup can still use its reserved final writer slot.
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
        .bind(MAX_REVISION - 1)
        .execute(&f.db.pool)
        .await?;
    let mutation = Uuid::new_v4();
    assert_eq!(
        cancel(&f, &principal, &reference, mutation)
            .await?
            .revision(),
        reference.revision() + 1
    );
    assert_eq!(
        cancel(&f, &principal, &reference, mutation)
            .await?
            .revision(),
        reference.revision() + 1
    );
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    let stored = record(&f, Some(owner)).await?;
    assert!(stored.journal.activation.is_none() && stored.journal.pending.is_none());
    assert_eq!(
        stored
            .journal
            .cancelled
            .as_ref()
            .context("cancelled")?
            .instance,
        a.instance()
    );
    assert_eq!(
        canonical::to_vec(&record(&f, b.configured_instance).await?.journal)?,
        before_b
    );
    let after_owner: (String,Option<String>,String) = sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(after_owner, before_owner);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT value_encrypted FROM secrets WHERE secret_id=$1")
            .bind(secret_id.to_string())
            .fetch_one(&f.db.pool)
            .await?,
        legacy_cipher
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
            .fetch_one(&f.db.pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM extension_instances")
            .fetch_one(&f.db.pool)
            .await?,
        2
    );
    assert!(f.root.path().join("archive/releases").is_dir());
    Ok(())
}

#[tokio::test]
async fn disposed_source_cancel_requires_fresh_owner_but_not_the_launch_session_revision()
-> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let (_, _, reference) = retained(&f, &a).await?;
    let mut principal = f.actor.fixture_principal();
    let before = canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?;
    sqlx::query(
        "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
    )
    .bind(principal.profile_id.to_string())
    .execute(&f.db.pool)
    .await?;
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(
        canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?,
        before
    );
    principal.capability_revision += 1; // fixture reauthentication, never restore stale DB revision
    let old_role = principal.role;
    principal.role = HomeRole::Admin;
    assert!(request(&principal, &reference, Uuid::new_v4()).is_err());
    principal.role = old_role;
    principal.transport = crate::http::auth::AccountAuthTransport::Query;
    assert!(request(&principal, &reference, Uuid::new_v4()).is_err());
    principal.transport = crate::http::auth::AccountAuthTransport::Bearer;
    assert_eq!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await?
            .revision(),
        reference.revision() + 1
    );
    Ok(())
}

#[tokio::test]
async fn disposed_source_cancel_rejects_changed_attempt_plan_owner_namespace_and_revision()
-> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let (_, _, mut reference) = retained(&f, &a).await?;
    let before = canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?;
    let principal = f.actor.fixture_principal();
    let attempt = reference.attempt;
    reference.attempt = Uuid::new_v4();
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    reference.attempt = attempt;
    let plan = reference.plan;
    reference.plan = Digest::of(b"foreign source plan");
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    reference.plan = plan;
    let owner = reference.configured_instance;
    reference.configured_instance = Some(Uuid::new_v4());
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    reference.configured_instance = owner;
    let namespace = reference.instance;
    reference.instance = Uuid::new_v4();
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    reference.instance = namespace;
    reference.revision += 1;
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(
        canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?,
        before
    );
    Ok(())
}

#[tokio::test]
async fn disposed_source_cancel_refuses_unexpected_private_data_without_deleting_it() -> Result<()>
{
    for kind in ["state", "secret", "head"] {
        let f = Fixture::new().await?;
        let a = stage(&f).await?;
        let (_, _, reference) = retained(&f, &a).await?;
        match kind {
            "state" => {
                sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,'foreign',1,'invalid',7)")
                .bind(a.instance().to_string()).execute(&f.db.pool).await?;
            }
            "secret" => {
                sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,'source_api',$2,'invalid',7,FALSE)")
                .bind(a.instance().to_string()).bind(Uuid::new_v4().to_string()).execute(&f.db.pool).await?;
            }
            _ => {
                sqlx::query(
                    "UPDATE marketplace_instance_sdk_state SET revision=2 WHERE instance_id=$1",
                )
                .bind(a.instance().to_string())
                .execute(&f.db.pool)
                .await?;
            }
        }
        let before = canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?;
        assert!(
            cancel(&f, &f.actor.fixture_principal(), &reference, Uuid::new_v4())
                .await
                .is_err()
        );
        assert_eq!(
            canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?,
            before
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
                .fetch_one(&f.db.pool)
                .await?,
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn disposed_source_cancel_cannot_erase_quiescence_copy_or_candidate_claim() -> Result<()> {
    let f = Fixture::new().await?;
    let config = configuration(&f, IMAGE, "cancel-scope")?;
    let mut owner = InstalledController::open(
        f.db.clone(),
        crate::secrets::SecretsManager::from_key_bytes([57; 32], false),
        config,
    )
    .await?;
    let a = stage(&f).await?;
    let (plan, intent, reference) = retained(&f, &a).await?;
    let principal = f.actor.fixture_principal();
    let fenced = cutover::quiesce_checked(checked(&f, &plan, &intent)?, &reference).await?;
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    let scope = cutover::observe_checked(checked(&f, &plan, &intent)?, &fenced).await?;
    let guard = owner.quiesce(scope).await?;
    let mut copy = checked(&f, &plan, &intent)?;
    copy.database = guard.database();
    let copied = state_copy::copy_checked(copy, &guard).await?;
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    let mut next = checked(&f, &plan, &intent)?;
    next.database = guard.database();
    assert!(matches!(
        candidate::claim_checked(next, &guard, &copied).await?,
        candidate::CandidateOrRecovery::Claim(_)
    ));
    let before = canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?;
    assert!(
        cancel(&f, &principal, &reference, Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(
        canonical::to_vec(&record(&f, a.configured_instance).await?.journal)?,
        before
    );
    Ok(())
}
