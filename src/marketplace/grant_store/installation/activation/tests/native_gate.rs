//! Actual archive/signature/SQL gate checks, not native resource qualification.
use super::*;

pub(super) async fn claimed(f: &Fixture, plan: &SourceActivationPlan) -> Result<ActivationStart> {
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let mutation = Uuid::new_v4();
    let prepared = prepared(f, &staged, mutation, plan).await?;
    match claim_checked(step(f, &staged, mutation, plan)?, &prepared).await? {
        StartOrRecovery::Start(start) => Ok(start),
        _ => bail!("fixture did not get its original claim"),
    }
}
pub(super) async fn check(
    f: &Fixture,
    check: &NativeStartCheck,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<()> {
    require_native_start(
        &f.db,
        &f.metadata,
        &f.public,
        &f.archive,
        check,
        plan,
        budget,
    )
    .await
}

#[tokio::test]
async fn native_checks_are_read_only_and_reopen_does_not_mint_a_replacement_claim() -> Result<()> {
    let mut f = Fixture::new().await?;
    let plan = plan(&f)?;
    let start = claimed(&f, &plan).await?;
    let check_data = start.native_check();
    let before = snapshot(&f).await?;
    check(&f, &check_data, &plan, limit()).await?;
    assert_eq!(before, snapshot(&f).await?);
    reopen(&mut f).await?;
    check(&f, &check_data, &plan, limit()).await?;
    assert_eq!(before, snapshot(&f).await?);
    let id = f.descriptor.release.payload.extension_id.clone();
    match inspect_as(&f.db, f.actor.clone(), &id, limit()).await? {
        ActivationStatus::Starting(reference) => assert_eq!(reference.attempt(), start.attempt()),
        _ => bail!("native check changed the durable phase"),
    }
    unused(&f).await?;
    Ok(())
}

#[tokio::test]
async fn a_prepared_record_and_read_only_coordinates_cannot_pass_the_native_gate() -> Result<()> {
    let f = Fixture::new().await?;
    let plan = plan(&f)?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let mutation = Uuid::new_v4();
    let prepared = prepared(&f, &staged, mutation, &plan).await?;
    let r = prepared.reference();
    // Test-only fabricated CHECK DATA, never an ActivationStart. Even these
    // coordinates cannot pass the SQL phase gate, much less the native factory.
    let check_data = NativeStartCheck {
        starting_journal: Digest::of(b"fabricated-check-data-not-a-native-owner"),
        reference: ActivationReference {
            configured_instance: r.configured_instance,
            extension: r.extension.clone(),
            release: r.release,
            instance: r.instance,
            revision: r.revision,
            attempt: r.attempt,
            plan: r.plan,
        },
        intent: Arc::new(intent(&f, &staged, mutation)?),
    };
    let before = snapshot(&f).await?;
    assert!(check(&f, &check_data, &plan, limit()).await.is_err());
    assert_eq!(before, snapshot(&f).await?);
    unused(&f).await?;
    Ok(())
}

#[tokio::test]
async fn native_handoff_rechecks_every_current_binding_without_repair_or_partial_writes()
-> Result<()> {
    for fault in [
        "metadata",
        "public",
        "grant_disabled",
        "grant_revision",
        "grant_json",
        "reapproved",
        "session",
        "home",
        "budget",
        "journal",
        "manifest",
        "instance",
        "configured",
        "attempt",
        "revision",
        "plan",
        "release",
        "instance_id",
    ] {
        let f = Fixture::new().await?;
        let plan = plan(&f)?;
        let start = claimed(&f, &plan).await?;
        let mut data = start.native_check();
        let mut budget = limit();
        match fault {
            "metadata" => (f.meta_owner)(false),
            "public" => (f.public_owner)(false),
            "grant_disabled" => {
                sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "grant_revision" => {
                sqlx::query("UPDATE marketplace_extension_grants SET revision=revision+1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "grant_json" => {
                sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "reapproved" => {
                sqlx::query("UPDATE marketplace_extension_grants SET revision=revision+2,revocation_revision=revision+1,enabled=TRUE")
                .execute(&f.db.pool).await?;
            }
            "session" => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='native gate fixture'")
                .execute(&f.db.pool).await?;
            }
            "home" => {
                sqlx::query("UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL")
                .execute(&f.db.pool).await?;
            }
            "budget" => {
                let used: i64 =
                    sqlx::query_scalar("SELECT encoded_bytes FROM marketplace_installation_budget")
                        .fetch_one(&f.db.pool)
                        .await?;
                budget.bytes = used as u64 - 1;
            }
            "journal" => {
                sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                    .bind(Digest::of(b"corrupt").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "manifest" => {
                sqlx::query("UPDATE extensions SET manifest_json='{}'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "instance" => {
                sqlx::query("UPDATE extension_instances SET enabled=TRUE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "configured" => {
                sqlx::query("UPDATE extension_instances SET config_json='{}'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "attempt" => data.reference.attempt = Uuid::new_v4(),
            "revision" => data.reference.revision -= 1,
            "plan" => data.reference.plan = Digest::of(b"different plan"),
            "release" => data.reference.release = Digest::of(b"different release"),
            "instance_id" => data.reference.instance = Uuid::new_v4(),
            _ => unreachable!(),
        }
        let before = snapshot(&f).await?;
        let counts = f.counts().await?;
        assert!(check(&f, &data, &plan, budget).await.is_err(), "{fault}");
        #[cfg(unix)]
        assert!(
            super::source_prepared::retain(&f, &data, &plan, budget)
                .await
                .is_err(),
            "source-prepared {fault}"
        );
        assert_eq!(before, snapshot(&f).await?, "{fault}");
        assert_eq!(counts, f.counts().await?, "{fault}");
    }
    Ok(())
}
