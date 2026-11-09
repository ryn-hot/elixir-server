//! Real SQL/archive/crypto data checks; no native or public-network proof.
use super::super::tests::{Fixture, budget};
use super::*;
use crate::extensions::store::ExtensionStore;
use serde_json::json;

fn request(f: &Fixture, after: Option<&TargetCursor>) -> Result<TargetRequest> {
    TargetRequest::parse(&serde_json::to_vec(
        &json!({"schema_version":1,"extension_id":f.material()?.review.scope().extension_id(),"after":after}),
    )?)
}
async fn read(f: &Fixture, after: Option<&TargetCursor>) -> Result<TargetSnapshot> {
    snapshot(&f.db, &f.actor.fixture_principal(), request(f, after)?).await
}
async fn staged(f: &Fixture, name: &str, limit: InstallBudget) -> Result<StagedInstallation> {
    f.stage_configured(
        InstallTarget::New { name: name.into() },
        Uuid::new_v4(),
        0,
        limit,
    )
    .await
}
async fn cancel(f: &Fixture, staged: &StagedInstallation, limit: InstallBudget) -> Result<()> {
    cancellation::cancel(
        &f.db,
        cancellation::OwnerCancelIntent::new(&f.actor.fixture_principal(), staged, Uuid::new_v4())?,
        limit,
    )
    .await?;
    Ok(())
}
async fn preferences(f: &Fixture) -> Result<(String, String, String, String, String, String)> {
    Ok(sqlx::query_as("SELECT CAST(revision AS TEXT),COALESCE(home_id,''),preferences_json,COALESCE(mutation_id,''),COALESCE(approved_by_user_id,''),CAST(updated_at AS TEXT) FROM marketplace_preferences")
        .fetch_one(&f.db.pool).await?)
}

#[tokio::test]
async fn target_requests_are_closed_exact_and_bounded_before_sql() -> Result<()> {
    let f = Fixture::new().await?;
    let valid =
        json!({"schema_version":1,"extension_id":f.material()?.review.scope().extension_id()});
    TargetRequest::parse(&serde_json::to_vec(&valid)?)?;
    for value in [
        json!({"schema_version":2,"extension_id":valid["extension_id"]}),
        json!({"schema_version":1,"extension_id":valid["extension_id"],"budget":4096}),
        json!({"schema_version":1,"extension_id":valid["extension_id"],"after":{"instance_id":Uuid::nil(),"extension_revision":1}}),
        json!({"schema_version":1,"extension_id":valid["extension_id"],"after":{"instance_id":Uuid::new_v4(),"extension_revision":0}}),
        json!({"schema_version":1,"extension_id":valid["extension_id"],"after":{"instance_id":Uuid::new_v4(),"extension_revision":1,"authority":true}}),
    ] {
        assert!(TargetRequest::parse(&serde_json::to_vec(&value)?).is_err());
    }
    assert!(TargetRequest::parse(b"{\"schema_version\":1,\"schema_version\":1}").is_err());
    assert!(TargetRequest::parse(&vec![b' '; MAX_REQUEST_BYTES + 1]).is_err());
    Ok(())
}
#[tokio::test]
async fn absent_target_view_is_data_and_changes_no_preferences_budget_or_installation() -> Result<()>
{
    let f = Fixture::new().await?;
    let before = preferences(&f).await?;
    let ledger: (i64, i64, i64) = sqlx::query_as(
        "SELECT revision,entries,encoded_bytes FROM marketplace_installation_budget",
    )
    .fetch_one(&f.db.pool)
    .await?;
    let mut view = read(&f, None).await?;
    assert!(matches!(
        view.new_instance.controls,
        ExpectedControls::Absent {}
    ));
    assert!(view.instances.is_empty());
    assert!(view.next_cursor.is_none());
    assert!(!view.installation_ready);
    assert!(
        serde_json::to_value(&view)?
            .get("review_available")
            .is_none()
    );
    view.advertise_review(true);
    assert_eq!(serde_json::to_value(&view)?["review_available"], true);
    assert!(
        !view.installation_ready,
        "review availability is never installation authority"
    );
    view.advertise_review(false);
    assert_eq!(preferences(&f).await?, before);
    assert_eq!(f.counts().await?, (0, 0, 0));
    let after: (i64, i64, i64) = sqlx::query_as(
        "SELECT revision,entries,encoded_bytes FROM marketplace_installation_budget",
    )
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(after, ledger);
    let encoded = serde_json::to_string(&view)?;
    for forbidden in [
        "record_json",
        "archive",
        "path",
        "credential",
        "native_owner",
    ] {
        assert!(!encoded.contains(forbidden));
    }
    Ok(())
}
#[tokio::test]
async fn pending_target_has_no_update_controls_and_real_cancellation_exposes_only_current_data()
-> Result<()> {
    let f = Fixture::new().await?;
    let original = staged(&f, "Owner A", budget()).await?;
    let view = read(&f, None).await?;
    assert_eq!(view.instances.len(), 1);
    assert!(view.instances[0].pending);
    assert!(view.instances[0].controls.is_none());
    assert_eq!(view.instances[0].expected_revision, 1);
    assert!(!view.instances[0].enabled);
    cancel(&f, &original, budget()).await?;
    let view = read(&f, None).await?;
    assert!(!view.instances[0].pending);
    assert!(view.instances[0].controls.is_some());
    assert_eq!(view.instances[0].expected_revision, 2);
    assert_eq!(
        view.instances[0].instance_id,
        original.configured_instance().unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn displayed_original_versions_cannot_be_silently_rebased_after_real_off() -> Result<()> {
    let f = Fixture::new().await?;
    let original = staged(&f, "A", budget()).await?;
    cancel(&f, &original, budget()).await?;
    let view = read(&f, None).await?;
    let choice = &view.instances[0];
    let id = choice.instance_id;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(id, false)
        .await?;
    let release = f.material()?.review.release_id();
    assert!(
        CapturedTarget::capture(
            &f.db,
            &f.actor.fixture_principal(),
            InstallAction {
                extension: f.material()?.review.scope().extension_id().clone(),
                release,
                target: InstallTarget::Existing { instance: id },
                expected_revision: choice.expected_revision,
                mutation: Uuid::new_v4(),
                controls: choice.controls.unwrap(),
            }
        )
        .await
        .is_err()
    );
    let next = read(&f, None).await?;
    match (
        choice.controls.unwrap(),
        next.instances[0].controls.unwrap(),
    ) {
        (
            ExpectedControls::Existing {
                instance_revision: a,
                ..
            },
            ExpectedControls::Existing {
                instance_revision: b,
                ..
            },
        ) => assert_eq!(b, a + 1),
        _ => bail!("expected observed existing controls"),
    }
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}
#[tokio::test]
async fn foreign_home_expired_and_revoked_sessions_cannot_read_target_data() -> Result<()> {
    let f = Fixture::new().await?;
    let other = crate::marketplace::grant_store::tests::owner(&f.db).await?;
    let error = match snapshot(&f.db, &other.fixture_principal(), request(&f, None)?).await {
        Err(error) => error,
        Ok(_) => bail!("foreign target read accepted"),
    };
    assert!(error.is::<TargetAuthorization>());
    let mut expired = f.actor.fixture_principal();
    expired.access_expires_at = Utc::now() - chrono::Duration::seconds(1);
    assert!(snapshot(&f.db, &expired, request(&f, None)?).await.is_err());
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='test' WHERE user_id=$1")
        .bind(f.actor.user.to_string())
        .execute(&f.db.pool)
        .await?;
    assert!(read(&f, None).await.is_err());
    assert_eq!(f.counts().await?, (0, 0, 0));
    Ok(())
}
#[tokio::test]
async fn unmanaged_changed_or_missing_targets_are_not_adopted_or_repaired() -> Result<()> {
    for fault in ["counter", "marker", "metadata", "name", "journal", "extra"] {
        let f = Fixture::new().await?;
        let original = staged(&f, "A", budget()).await?;
        cancel(&f, &original, budget()).await?;
        match fault {
            "counter" => {
                sqlx::query("DELETE FROM marketplace_instance_choices")
                    .execute(&f.db.pool)
                    .await?;
            }
            "marker" => {
                sqlx::query("DELETE FROM marketplace_installation_metadata")
                    .execute(&f.db.pool)
                    .await?;
            }
            "metadata" => {
                sqlx::query("UPDATE extensions SET name='changed without marker'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "name" => {
                sqlx::query("UPDATE extension_instances SET instance_name=$1")
                    .bind("x".repeat(129))
                    .execute(&f.db.pool)
                    .await?;
            }
            "journal" => {
                sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                    .bind(Digest::of(b"incorrect journal checksum").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "extra" => {
                sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,'unmanaged',FALSE)")
                .bind(Uuid::new_v4().to_string()).bind(f.material()?.review.scope().extension_id().as_str()).execute(&f.db.pool).await?;
            }
            _ => unreachable!(),
        }
        let before = preferences(&f).await?;
        assert!(read(&f, None).await.is_err(), "{fault}");
        assert_eq!(preferences(&f).await?, before);
    }
    Ok(())
}
#[tokio::test]
async fn target_pages_are_twenty_rows_and_a_later_parent_change_invalidates_the_cursor()
-> Result<()> {
    let f = Fixture::new().await?;
    let limit = InstallBudget {
        entries: 32,
        bytes: 16 * 1024 * 1024,
    };
    for n in 0..21 {
        staged(&f, &format!("Instance {n}"), limit).await?;
    }
    let first = read(&f, None).await?;
    assert_eq!(first.instances.len(), 20);
    let cursor = first.next_cursor.as_ref().context("next page")?;
    let second = read(&f, Some(cursor)).await?;
    assert_eq!(second.instances.len(), 1);
    assert!(second.next_cursor.is_none());
    assert!(
        first
            .instances
            .iter()
            .all(|row| row.instance_id != second.instances[0].instance_id)
    );
    assert!(serde_json::to_vec(&first)?.len() < 32 * 1024);
    ExtensionStore::new(&f.db.pool)
        .set_extension_enabled(f.material()?.review.scope().extension_id().as_str(), false)
        .await?;
    assert!(read(&f, Some(cursor)).await.is_err());
    assert_eq!(f.counts().await?, (1, 21, 21));
    Ok(())
}
