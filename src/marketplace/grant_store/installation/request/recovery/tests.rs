//! Actual SQL/archive/crypto input; synthetic discovery/protection only.
//! These exercise DATA acknowledgement, not original native-task evidence.
use super::*;
use crate::extensions::store::ExtensionStore;
use crate::marketplace::grant_store::installation::{
    self as install,
    tests::{Fixture, budget},
};

struct Original {
    intent: OwnerInstallIntent,
    extension: ExtensionId,
    request: Digest,
    source: Digest,
}
async fn original(f: &Fixture) -> Result<Original> {
    let material = f.material()?;
    let captured = CapturedTarget::capture(
        &f.db,
        &f.actor.fixture_principal(),
        InstallAction {
            extension: material.review.scope().extension_id().clone(),
            release: material.review.release_id(),
            target: InstallTarget::New {
                name: "Original pending request".into(),
            },
            expected_revision: 0,
            mutation: Uuid::new_v4(),
            controls: ExpectedControls::Absent {},
        },
    )
    .await?;
    let intent = OwnerInstallIntent::new_captured(
        &f.actor.fixture_principal(),
        &material.review,
        f.revision,
        &captured,
    )?;
    Ok(Original {
        extension: material.review.scope().extension_id().clone(),
        request: intent.request_id(&material)?,
        source: material.source.id()?,
        intent,
    })
}
async fn stage(f: &Fixture, o: &Original) -> Result<StagedInstallation> {
    install::stage_borrowed(
        &f.db,
        &f.metadata,
        &f.public,
        f.material()?,
        &o.intent,
        budget(),
    )
    .await
}
async fn recover(
    f: &Fixture,
    o: &Original,
    p: &CurrentPrincipal,
) -> Result<Option<StagedInstallation>> {
    read(&f.db, p, &o.intent, &o.extension, o.request, o.source).await
}
async fn journal(f: &Fixture) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
            .fetch_one(&f.db.pool)
            .await?,
    )
}

#[tokio::test]
async fn exact_staging_acknowledgement_survives_source_loss_and_a_later_revocation_floor_for_cleanup_only()
-> Result<()> {
    let f = Fixture::new().await?;
    let o = original(&f).await?;
    let staged = stage(&f, &o).await?;
    crate::marketplace::grant_store::revoke(
        &f.db,
        &f.actor.fixture_principal(),
        &o.extension,
        f.revision,
        Uuid::new_v4(),
        Utc::now(),
    )
    .await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    let before = journal(&f).await?;
    let recovered = recover(&f, &o, &f.actor.fixture_principal())
        .await?
        .context("original receipt")?;
    assert_eq!(recovered.instance(), staged.instance());
    assert_eq!(
        recovered.configured_instance(),
        staged.configured_instance()
    );
    assert_eq!(
        journal(&f).await?,
        before,
        "acknowledging data never resets a floor or changes a journal"
    );
    let cancelled = install::cancellation::cancel(
        &f.db,
        install::cancellation::OwnerCancelIntent::new(
            &f.actor.fixture_principal(),
            &recovered,
            Uuid::new_v4(),
        )?,
        budget(),
    )
    .await?;
    assert_eq!(cancelled.revision(), 2);
    let private:i64=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_generation_secrets)").fetch_one(&f.db.pool).await?;
    assert_eq!(private, 0);
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn recovered_receipt_keeps_the_original_choices_after_real_later_off_writes() -> Result<()> {
    let f = Fixture::new().await?;
    let o = original(&f).await?;
    let staged = stage(&f, &o).await?;
    let owner = staged.configured_instance().context("configured owner")?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(owner, false)
        .await?;
    ExtensionStore::new(&f.db.pool)
        .set_extension_enabled(o.extension.as_str(), false)
        .await?;
    let before = journal(&f).await?;
    let recovered = recover(&f, &o, &f.actor.fixture_principal())
        .await?
        .context("exact receipt")?;
    assert!(recovered.choices == staged.choices);
    assert_eq!(journal(&f).await?, before);
    let counters:(i64,i64)=sqlx::query_as("SELECT (SELECT revision FROM marketplace_extension_choices),(SELECT revision FROM marketplace_instance_choices)").fetch_one(&f.db.pool).await?;
    assert_eq!(
        counters,
        (2, 2),
        "readback must not adopt or reset later controls"
    );
    Ok(())
}

#[tokio::test]
async fn wrong_users_homes_and_revoked_sessions_cannot_acknowledge_the_original_request()
-> Result<()> {
    let f = Fixture::new().await?;
    let o = original(&f).await?;
    let _ = stage(&f, &o).await?;
    let before = journal(&f).await?;
    let other = crate::marketplace::grant_store::tests::owner(&f.db).await?;
    assert!(recover(&f, &o, &other.fixture_principal()).await.is_err());
    let mut changed = f.actor.fixture_principal();
    changed.home_id = other.home;
    assert!(recover(&f, &o, &changed).await.is_err());
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='original staging readback fixture' WHERE user_id=$1").bind(f.actor.user.to_string()).execute(&f.db.pool).await?;
    assert!(recover(&f, &o, &f.actor.fixture_principal()).await.is_err());
    assert_eq!(journal(&f).await?, before);
    Ok(())
}

#[tokio::test]
async fn changed_journals_or_source_request_hashes_cannot_become_an_original_receipt() -> Result<()>
{
    for fault in ["request", "mutation", "revision", "activation"] {
        let f = Fixture::new().await?;
        let o = original(&f).await?;
        let _ = stage(&f, &o).await?;
        let mut record: serde_json::Value = serde_json::from_str(&journal(&f).await?)?;
        match fault {
            "request" => record["request"] = serde_json::json!(Digest::of(b"another request")),
            "mutation" => record["mutation"] = serde_json::json!(Uuid::new_v4()),
            "revision" => record["revision"] = serde_json::json!(2),
            "activation" => {
                record["activation"] = serde_json::json!({"unexpected":"not native evidence"})
            }
            _ => unreachable!(),
        }
        let changed = String::from_utf8(canonical::to_vec(&record)?)?;
        sqlx::query("UPDATE marketplace_installations SET record_json=$1")
            .bind(&changed)
            .execute(&f.db.pool)
            .await?;
        assert!(
            recover(&f, &o, &f.actor.fixture_principal()).await.is_err(),
            "{fault}"
        );
        assert_eq!(journal(&f).await?, changed);
        assert_eq!(f.counts().await?, (1, 1, 1));
    }
    let f = Fixture::new().await?;
    let o = original(&f).await?;
    let _ = stage(&f, &o).await?;
    assert!(
        read(
            &f.db,
            &f.actor.fixture_principal(),
            &o.intent,
            &o.extension,
            Digest::of(b"wrong request"),
            o.source
        )
        .await
        .is_err()
    );
    assert!(
        read(
            &f.db,
            &f.actor.fixture_principal(),
            &o.intent,
            &o.extension,
            o.request,
            Digest::of(b"wrong source")
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn no_committed_staging_result_is_only_absence_of_data_not_permission_to_allocate_or_run()
-> Result<()> {
    let f = Fixture::new().await?;
    let o = original(&f).await?;
    assert!(
        recover(&f, &o, &f.actor.fixture_principal())
            .await?
            .is_none()
    );
    assert_eq!(f.counts().await?, (0, 0, 0));
    Ok(())
}

#[tokio::test]
async fn a_later_real_cancellation_cannot_be_reinterpreted_as_the_original_pending_receipt()
-> Result<()> {
    let f = Fixture::new().await?;
    let o = original(&f).await?;
    let staged = stage(&f, &o).await?;
    install::cancellation::cancel(
        &f.db,
        install::cancellation::OwnerCancelIntent::new(
            &f.actor.fixture_principal(),
            &staged,
            Uuid::new_v4(),
        )?,
        budget(),
    )
    .await?;
    let before = journal(&f).await?;
    assert!(recover(&f, &o, &f.actor.fixture_principal()).await.is_err());
    assert_eq!(journal(&f).await?, before);
    Ok(())
}
