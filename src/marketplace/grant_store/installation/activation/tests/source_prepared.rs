//! Real crypto/archive/SQL; the completion proof is synthetic in this module.
//! Actual probe/disposal/lane release is covered by the explicit native fixture.
use super::*;

#[tokio::test]
async fn journal_and_transition_ownership_are_stack_bounded_without_changing_wire_bytes()
-> Result<()> {
    fn future_size<F>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(&f)?;
    let journal: Journal =
        canonical::decode(snapshot(&f).await?.0.as_bytes(), record::MAX_RECORD_BYTES)?;
    let mut inline = serde_json::to_value(&journal)?;
    inline["active"] = serde_json::to_value(journal.active.as_deref())?;
    inline["previous"] = serde_json::to_value(journal.previous.as_deref())?;
    inline["pending"] = serde_json::to_value(journal.pending.as_deref())?;
    assert_eq!(
        canonical::to_vec(&inline)?,
        canonical::to_vec(&journal)?,
        "heap ownership must preserve the old unboxed capsule wire format"
    );
    let journal_size = std::mem::size_of::<Journal>();
    assert!(
        journal_size <= 1024,
        "three source trees must not be inline in every journal: {journal_size}"
    );
    let transition = step(&f, &staged, Uuid::new_v4(), &plan)?;
    let inner_size = future_size(|| transition.transition_owned(Action::Prepare));
    assert!(
        inner_size <= 128 * 1024,
        "SQL transition future exceeded its stack-construction bound: {inner_size}"
    );
    let public = step(&f, &staged, Uuid::new_v4(), &plan)?.transition(Action::Prepare);
    assert_eq!(
        std::mem::size_of_val(&public),
        2 * std::mem::size_of::<usize>(),
        "native callers retain only the boxed transition pointer"
    );
    println!(
        "bounded activation ownership: journal={journal_size}, transition={inner_size}, caller={}",
        std::mem::size_of_val(&public)
    );
    Ok(())
}

pub(super) async fn retain(
    f: &Fixture,
    check: &NativeStartCheck,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<ActivationReference> {
    let proof = source::DisposedProbe::fixture(check);
    match (Step {
        database: &f.db,
        metadata: &f.metadata,
        public: &f.public,
        material: f.material()?,
        intent: check.intent.clone(),
        plan,
        budget,
    })
    .transition(Action::Retain(&proof))
    .await?
    {
        Transition::SourcePrepared(reference) => Ok(reference),
        _ => bail!("fixture source preparation did not commit"),
    }
}

#[tokio::test]
async fn retained_source_preparation_is_durable_one_way_and_does_not_enable_a_runtime() -> Result<()>
{
    let mut f = Fixture::new().await?;
    durable(&mut f, None).await
}

async fn durable(f: &mut Fixture, postgres_url: Option<&str>) -> Result<()> {
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(f)?;
    let mutation = Uuid::new_v4();
    let prepared = prepared(f, &staged, mutation, &plan).await?;
    let start = match claim_checked(step(f, &staged, mutation, &plan)?, &prepared).await? {
        StartOrRecovery::Start(start) => start,
        _ => bail!("fresh fixture claim required"),
    };
    let check = start.native_check();
    let reference = retain(f, &check, &plan, limit()).await?;
    assert_eq!(reference.revision(), 4);
    assert_eq!(reference.attempt(), start.attempt());
    let before = snapshot(f).await?;
    // There is no repeat native/SQL completion capability. If a commit reply
    // was lost, inspect the durable phase instead of reissuing any operation.
    assert!(retain(f, &check, &plan, limit()).await.is_err());
    assert!(native_gate::check(f, &check, &plan, limit()).await.is_err());
    assert!(matches!(
        claim_checked(step(f, &staged, mutation, &plan)?, &prepared).await?,
        StartOrRecovery::Recovery(r) if r.revision() == 4 && r.attempt() == start.attempt()
    ));
    assert!(matches!(
        prepare_checked(step(f, &staged, mutation, &plan)?).await?,
        PreparedOrRecovery::Recovery(r) if r.revision() == 4
    ));
    let cancel = cancellation::OwnerCancelIntent::for_prepared_actor(
        f.actor.clone(),
        &prepared,
        Uuid::new_v4(),
    )?;
    assert!(cancellation::cancel(&f.db, cancel, limit()).await.is_err());
    assert_eq!(snapshot(f).await?, before);
    unused(f).await?;

    if let Some(url) = postgres_url {
        f.db.pool.close().await;
        f.db = Database::connect(&DatabaseConfig {
            url: url.to_owned(),
            max_connections: 6,
            connect_timeout_seconds: 5,
        })
        .await?;
    } else {
        reopen(f).await?;
    }
    // Authenticated status must survive offline discovery, revoked permission
    // and unavailable/corrupt grant JSON. It never authorizes execution.
    (f.meta_owner)(false);
    (f.public_owner)(false);
    super::super::super::super::revoke_as(
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
    match inspect_as(&f.db, f.actor.clone(), staged.extension(), limit()).await? {
        ActivationStatus::SourcePrepared(r) => {
            assert_eq!(r.revision(), 4);
            assert_eq!(r.attempt(), reference.attempt());
        }
        _ => bail!("reopen lost the source-prepared phase"),
    }
    assert_eq!(snapshot(f).await?, before);
    unused(f).await
}

#[tokio::test]
async fn source_preparation_cannot_skip_start_and_phase_changes_need_the_exact_revision()
-> Result<()> {
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let plan = plan(&f)?;
    let mutation = Uuid::new_v4();
    let prepared = prepared(&f, &staged, mutation, &plan).await?;
    let r = prepared.reference();
    let fake_check = NativeStartCheck {
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
    assert!(retain(&f, &fake_check, &plan, limit()).await.is_err());
    assert_eq!(snapshot(&f).await?, before);
    let mut journal: Journal = canonical::decode(before.0.as_bytes(), record::MAX_RECORD_BYTES)?;
    journal.activation.as_mut().unwrap().phase = ActivationPhase::SourcePrepared;
    assert!(journal.validate().is_err());
    journal.revision += 1;
    assert!(journal.validate().is_err());
    journal.revision += 1;
    journal.validate()?;
    journal.revision += 1;
    assert!(journal.validate().is_err());
    unused(&f).await
}

#[tokio::test]
async fn source_preparation_budget_denial_does_not_partially_advance_or_repair_anything()
-> Result<()> {
    let f = Fixture::new().await?;
    let plan = plan(&f)?;
    let start = native_gate::claimed(&f, &plan).await?;
    let before = snapshot(&f).await?;
    let budget = InstallBudget {
        entries: 4,
        bytes: before.5 as u64,
    };
    assert!(
        retain(&f, &start.native_check(), &plan, budget)
            .await
            .is_err()
    );
    assert_eq!(snapshot(&f).await?, before);
    let reference = retain(&f, &start.native_check(), &plan, limit()).await?;
    assert_eq!(reference.revision(), 4);
    let after = snapshot(&f).await?;
    assert!(after.5 > before.5);
    assert_eq!(after.3, before.3 + 1);
    assert_eq!(after.4, before.4);
    unused(&f).await
}

#[tokio::test]
async fn source_preparation_and_revocation_serialize_without_enabling_or_restoring_a_claim()
-> Result<()> {
    let f = Fixture::new().await?;
    let plan = plan(&f)?;
    let start = native_gate::claimed(&f, &plan).await?;
    let check = start.native_check();
    let id = f.descriptor.release.payload.extension_id.clone();
    let (prepared, revoked) = tokio::join!(
        retain(&f, &check, &plan, limit()),
        super::super::super::super::revoke_as(&f.db, &f.actor, &id, 1, Uuid::new_v4(), Utc::now(),),
    );
    revoked?;
    match inspect_as(&f.db, f.actor.clone(), &id, limit()).await? {
        ActivationStatus::SourcePrepared(_) => assert!(prepared.is_ok()),
        ActivationStatus::Starting(_) => assert!(prepared.is_err()),
        _ => bail!("source preparation race regressed its phase"),
    }
    assert!(
        native_gate::check(&f, &check, &plan, limit())
            .await
            .is_err()
    );
    assert!(retain(&f, &check, &plan, limit()).await.is_err());
    unused(&f).await
}

#[tokio::test]
#[ignore = "explicit fresh isolated loopback PostgreSQL source-prepared transition/reopen; synthetic native completion proof"]
async fn postgres_source_prepared_is_durable_and_never_reissues_a_native_claim() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_SOURCE_PREPARED_POSTGRES_URL")?;
    let db = fresh_postgres(&url).await?;
    let mut f = Fixture::with_database(tempfile::tempdir()?, db).await?;
    let result = durable(&mut f, Some(&url)).await;
    f.db.pool.close().await;
    result
}
