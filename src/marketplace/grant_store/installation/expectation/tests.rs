//! Real SQL/source archives and authorization; no native execution evidence.
use super::super::tests::{Fixture, budget};
use super::*;
use crate::extensions::store::ExtensionStore;
use serde_json::json;

async fn capture(
    f: &Fixture,
    target: InstallTarget,
    revision: i64,
    controls: ExpectedControls,
) -> Result<CapturedTarget> {
    let review = f.material()?.review;
    CapturedTarget::capture(
        &f.db,
        &f.actor.fixture_principal(),
        InstallAction {
            extension: review.scope().extension_id().clone(),
            release: review.release_id(),
            target,
            expected_revision: revision,
            mutation: Uuid::new_v4(),
            controls,
        },
    )
    .await
}
async fn stage(f: &Fixture, captured: &CapturedTarget) -> Result<StagedInstallation> {
    let material = f.material()?;
    let intent = OwnerInstallIntent::new_captured(
        &f.actor.fixture_principal(),
        &material.review,
        f.revision,
        captured,
    )?;
    stage_checked(&f.db, &f.metadata, &f.public, material, intent, budget()).await
}
async fn existing(f: &Fixture) -> Result<Uuid> {
    let staged = f
        .stage_configured(
            InstallTarget::New {
                name: "Existing A".into(),
            },
            Uuid::new_v4(),
            0,
            budget(),
        )
        .await?;
    cancellation::cancel(
        &f.db,
        cancellation::OwnerCancelIntent::new(
            &f.actor.fixture_principal(),
            &staged,
            Uuid::new_v4(),
        )?,
        budget(),
    )
    .await?;
    staged.configured_instance().context("owner")
}
async fn record(f: &Fixture, id: Uuid) -> Result<String> {
    Ok(
        sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(id.to_string())
        .fetch_one(&f.db.pool)
        .await?,
    )
}
fn original_existing() -> ExpectedControls {
    ExpectedControls::Existing {
        extension_revision: 1,
        instance_revision: 1,
    }
}

#[tokio::test]
async fn original_absent_target_stages_once_and_retries_ack_without_restamping_later_off()
-> Result<()> {
    let f = Fixture::new().await?;
    let captured = capture(
        &f,
        InstallTarget::New { name: "A".into() },
        0,
        ExpectedControls::Absent {},
    )
    .await?;
    let staged = stage(&f, &captured).await?;
    let id = staged.configured_instance().context("owner")?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(id, false)
        .await?;
    ExtensionStore::new(&f.db.pool)
        .set_extension_enabled(staged.extension().as_str(), false)
        .await?;
    let before = record(&f, id).await?;
    let retry = stage(&f, &captured).await?;
    assert_eq!(retry.instance(), staged.instance());
    assert_eq!(retry.configured_instance(), Some(id));
    assert!(retry.choices == staged.choices);
    assert_eq!(record(&f, id).await?, before);
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn later_real_controls_between_capture_and_staging_cannot_be_rebased_or_partially_written()
-> Result<()> {
    for action in [
        "instance_off",
        "extension_off",
        "config",
        "rename",
        "change_back",
    ] {
        let f = Fixture::new().await?;
        let id = existing(&f).await?;
        let captured = capture(
            &f,
            InstallTarget::Existing { instance: id },
            2,
            original_existing(),
        )
        .await?;
        let store = ExtensionStore::new(&f.db.pool);
        match action {
            "instance_off" => {
                store.set_instance_enabled(id, false).await?;
            }
            "extension_off" => {
                store
                    .set_extension_enabled(
                        f.material()?.review.scope().extension_id().as_str(),
                        false,
                    )
                    .await?;
            }
            "config" => {
                store
                    .update_instance_config(id, Some(&json!({"owner":"new"})))
                    .await?;
            }
            "rename" => {
                sqlx::query(
                    "UPDATE extension_instances SET instance_name='Renamed' WHERE instance_id=$1",
                )
                .bind(id.to_string())
                .execute(&f.db.pool)
                .await?;
            }
            "change_back" => {
                store.set_instance_enabled(id, true).await?;
                store.set_instance_enabled(id, false).await?;
            }
            _ => unreachable!(),
        }
        let before = record(&f, id).await?;
        let ledger: i64 = sqlx::query_scalar(
            "SELECT revision FROM marketplace_installation_budget WHERE singleton=1",
        )
        .fetch_one(&f.db.pool)
        .await?;
        assert!(stage(&f, &captured).await.is_err(), "{action}");
        assert_eq!(record(&f, id).await?, before, "{action}");
        assert_eq!(f.counts().await?, (1, 1, 1));
        let roots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
            .fetch_one(&f.db.pool)
            .await?;
        assert_eq!(roots, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM marketplace_installation_budget WHERE singleton=1"
            )
            .fetch_one(&f.db.pool)
            .await?,
            ledger
        );
    }
    Ok(())
}

#[tokio::test]
async fn capture_requires_the_callers_original_versions_and_an_idle_managed_journal() -> Result<()>
{
    let f = Fixture::new().await?;
    let id = existing(&f).await?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(id, false)
        .await?;
    assert!(
        capture(
            &f,
            InstallTarget::Existing { instance: id },
            2,
            original_existing()
        )
        .await
        .is_err()
    );
    assert!(
        capture(
            &f,
            InstallTarget::Existing { instance: id },
            1,
            ExpectedControls::Existing {
                extension_revision: 1,
                instance_revision: 2
            }
        )
        .await
        .is_err()
    );
    let fresh = capture(
        &f,
        InstallTarget::Existing { instance: id },
        2,
        ExpectedControls::Existing {
            extension_revision: 1,
            instance_revision: 2,
        },
    )
    .await?;
    let staged = stage(&f, &fresh).await?;
    assert_eq!(staged.revision(), 3);
    assert!(
        capture(
            &f,
            InstallTarget::Existing { instance: id },
            3,
            ExpectedControls::Existing {
                extension_revision: 1,
                instance_revision: 2
            }
        )
        .await
        .is_err(),
        "pending work is already owned"
    );
    Ok(())
}

#[tokio::test]
async fn a_new_instance_binds_the_original_parent_and_cannot_adopt_an_absent_or_changed_parent()
-> Result<()> {
    let f = Fixture::new().await?;
    let _a = existing(&f).await?;
    assert!(
        capture(
            &f,
            InstallTarget::New { name: "B".into() },
            0,
            ExpectedControls::Absent {}
        )
        .await
        .is_err()
    );
    let b = capture(
        &f,
        InstallTarget::New { name: "B".into() },
        0,
        ExpectedControls::NewInstance {
            extension_revision: 1,
        },
    )
    .await?;
    ExtensionStore::new(&f.db.pool)
        .set_extension_enabled(f.material()?.review.scope().extension_id().as_str(), false)
        .await?;
    assert!(stage(&f, &b).await.is_err());
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn captured_action_cannot_change_user_home_release_or_reuse_a_newer_control_stamp()
-> Result<()> {
    let f = Fixture::new().await?;
    let original = capture(
        &f,
        InstallTarget::New { name: "A".into() },
        0,
        ExpectedControls::Absent {},
    )
    .await?;
    let other = crate::marketplace::grant_store::tests::owner(&f.db).await?;
    assert!(
        OwnerInstallIntent::new_captured(
            &other.fixture_principal(),
            &f.material()?.review,
            f.revision,
            &original
        )
        .is_err()
    );
    let mut changed = serde_json::to_value(&f.material()?.review)?;
    changed["release_id"] = json!(Digest::of(b"a different selected release"));
    let changed: GrantReview = serde_json::from_value(changed)?;
    assert!(
        OwnerInstallIntent::new_captured(
            &f.actor.fixture_principal(),
            &changed,
            f.revision,
            &original
        )
        .is_err()
    );
    let staged = stage(&f, &original).await?;
    let id = staged.configured_instance().context("owner")?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(id, false)
        .await?;
    let encoded: serde_json::Value = serde_json::from_str(&record(&f, id).await?)?;
    assert_eq!(encoded["choices"]["instance_revision"], 1);
    Ok(())
}

#[tokio::test]
async fn missing_saturated_or_foreign_choice_state_fences_capture_without_resetting_it()
-> Result<()> {
    for fault in ["missing", "saturated", "foreign"] {
        let f = Fixture::new().await?;
        let id = existing(&f).await?;
        match fault {
            "missing" => {
                sqlx::query("DELETE FROM marketplace_instance_choices WHERE instance_id=$1")
                    .bind(id.to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "saturated" => {
                sqlx::query(
                    "UPDATE marketplace_instance_choices SET revision=$1 WHERE instance_id=$2",
                )
                .bind(MAX_REVISION)
                .bind(id.to_string())
                .execute(&f.db.pool)
                .await?;
            }
            "foreign" => {
                let foreign = crate::marketplace::grant_store::tests::owner(&f.db).await?;
                sqlx::query("UPDATE marketplace_installation_metadata SET home_id=$1")
                    .bind(foreign.home.to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => unreachable!(),
        }
        assert!(
            capture(
                &f,
                InstallTarget::Existing { instance: id },
                2,
                original_existing()
            )
            .await
            .is_err(),
            "{fault}"
        );
        assert_eq!(f.counts().await?, (1, 1, 1));
    }
    Ok(())
}
