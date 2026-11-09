//! Actual SQL/current account authorization and durable decisions only. No
//! provider registration, health, network, native process or paid credentials.
use super::*;
use crate::{config::DatabaseConfig, marketplace::preferences};
use serde_json::json;

async fn database() -> Result<(tempfile::TempDir, Database, Actor)> {
    database_with_connections(1).await
}
async fn database_with_connections(
    max_connections: u32,
) -> Result<(tempfile::TempDir, Database, Actor)> {
    let root = tempfile::tempdir()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", root.path().join("setup.sqlite").display()),
        max_connections,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let actor = crate::marketplace::grant_store::tests::owner(&database).await?;
    preferences::local_setup::bind_initial_home(&database, actor.home, Uuid::new_v4()).await?;
    Ok((root, database, actor))
}
fn configuration() -> Digest {
    Digest::of(b"synthetic immutable operator profile")
}

#[tokio::test]
async fn standing_consent_capture_waits_for_a_current_writer_without_read_upgrade_failure()
-> Result<()> {
    let (_root, database, actor) = database_with_connections(2).await?;
    save_as(
        &database,
        &actor,
        configuration(),
        &change(0, true),
        Utc::now(),
    )
    .await?;
    let original = capture(&database, configuration()).await?;
    let mut writer = durability::connection(&database).await?;
    let mut transaction = writer.begin().await?;
    sqlx::query("UPDATE profiles SET updated_at=updated_at WHERE id=$1")
        .bind(actor.profile_authorization().0.to_string())
        .execute(&mut *transaction)
        .await?;
    // The actual capture must wait before reading, not take a shared snapshot
    // and fail SQLITE_BUSY immediately when upgrading its authorization lock.
    let mut pending = Box::pin(capture(&database, configuration()));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), pending.as_mut())
            .await
            .is_err(),
        "a reserved writer must defer capture, not manufacture a consent failure"
    );
    transaction.commit().await?;
    let retained = tokio::time::timeout(std::time::Duration::from_secs(5), pending).await??;
    assert_eq!(retained.revision, original.revision);
    assert_eq!(retained.enrollment(), original.enrollment());
    retained.current().await?;
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_provider_setup")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(rows, 1);
    Ok(())
}
async fn save_as(
    database: &Database,
    actor: &Actor,
    configuration: Digest,
    change: &Change,
    now: DateTime<Utc>,
) -> Result<View> {
    super::save_as(database, actor, configuration, change, now)
        .await
        .map(|(view, _)| view)
}
fn change(revision: i64, enabled: bool) -> Change {
    Change {
        schema_version: 1,
        expected_revision: revision,
        mutation_id: Uuid::new_v4(),
        enabled,
        consent_version: enabled.then(|| CONSENT_VERSION.into()),
    }
}

#[test]
fn closed_provider_choice_cannot_select_credentials_or_imply_terms_acceptance() -> Result<()> {
    let valid = json!({"schema_version":1,"expected_revision":0,"mutation_id":Uuid::new_v4(),"enabled":true,"consent_version":CONSENT_VERSION});
    Change::parse(&canonical::to_vec(&valid)?)?;
    for (field, value) in [
        ("image", json!("sha256:invented")),
        ("enrollment", json!(Digest::of(b"caller identity"))),
        ("resume", json!(false)),
        ("provider", json!("other")),
        ("protected", json!(true)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(Change::parse(&canonical::to_vec(&invalid)?).is_err());
    }
    for (field, value) in [
        ("consent_version", json!(null)),
        ("consent_version", json!("old")),
        ("expected_revision", json!(-1)),
        ("mutation_id", json!(Uuid::nil())),
        ("enabled", json!(false)),
        ("schema_version", json!(2)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(Change::parse(&canonical::to_vec(&invalid)?).is_err());
    }
    let duplicate = String::from_utf8(canonical::to_vec(&valid)?)?.replacen(
        "\"enabled\":true",
        "\"enabled\":true,\"enabled\":false",
        1,
    );
    assert!(Change::parse(duplicate.as_bytes()).is_err());
    Ok(())
}

#[tokio::test]
async fn provider_decision_is_one_row_and_preserves_enrollment_across_toggle_retry_and_restart()
-> Result<()> {
    let (root, database, actor) = database().await?;
    assert_eq!(
        load_as(&database, &actor, configuration(), Utc::now())
            .await?
            .revision,
        0
    );
    assert!(capture(&database, configuration()).await.is_err());
    let accept = change(0, true);
    let saved = save_as(&database, &actor, configuration(), &accept, Utc::now()).await?;
    assert!(saved.enabled && saved.consent_current);
    let original = capture(&database, configuration()).await?;
    assert_eq!(
        save_as(&database, &actor, configuration(), &accept, Utc::now()).await?,
        saved
    );
    let disable = change(1, false);
    save_as(&database, &actor, configuration(), &disable, Utc::now()).await?;
    assert!(original.current().await.is_err());
    assert!(capture(&database, configuration()).await.is_err());
    let enable = change(2, true);
    save_as(&database, &actor, configuration(), &enable, Utc::now()).await?;
    let resumed = capture(&database, configuration()).await?;
    assert_eq!(resumed.enrollment(), original.enrollment());
    assert_eq!(resumed.revision, 3);
    assert!(
        save_as(&database, &actor, configuration(), &accept, Utc::now())
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_provider_setup")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(count, 1);
    drop(original);
    drop(resumed);
    database.pool.close().await;
    let reopened = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", root.path().join("setup.sqlite").display()),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    let retained = capture(&reopened, configuration()).await?;
    assert_eq!(retained.revision, 3);
    assert_eq!(
        save_as(&reopened, &actor, configuration(), &enable, Utc::now())
            .await?
            .revision,
        3
    );
    assert_eq!(
        capture(&reopened, configuration()).await?.enrollment(),
        retained.enrollment()
    );
    Ok(())
}

#[tokio::test]
async fn setup_rejects_changed_mutations_configurations_and_current_owner_authorization()
-> Result<()> {
    let (_root, database, actor) = database().await?;
    let accept = change(0, true);
    save_as(&database, &actor, configuration(), &accept, Utc::now()).await?;
    let mut altered = change(0, false);
    altered.mutation_id = accept.mutation_id;
    assert!(
        save_as(&database, &actor, configuration(), &altered, Utc::now())
            .await
            .is_err()
    );
    assert!(
        save_as(
            &database,
            &actor,
            Digest::of(b"changed operator image/config"),
            &accept,
            Utc::now()
        )
        .await
        .is_err()
    );
    assert!(
        capture(&database, Digest::of(b"changed operator image/config"))
            .await
            .is_err()
    );
    let foreign = crate::marketplace::grant_store::tests::owner(&database).await?;
    assert!(
        load_as(&database, &foreign, configuration(), Utc::now())
            .await
            .is_err()
    );
    assert!(
        save_as(
            &database,
            &foreign,
            configuration(),
            &change(1, false),
            Utc::now()
        )
        .await
        .is_err()
    );
    let bound = capture(&database, configuration()).await?;
    sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE home_id=$1")
        .bind(actor.home.to_string())
        .execute(&database.pool)
        .await?;
    assert!(bound.current().await.is_err());
    assert!(
        save_as(
            &database,
            &actor,
            configuration(),
            &change(1, false),
            Utc::now()
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn standing_provider_consent_survives_logout_but_not_owner_revocation() -> Result<()> {
    let (_root, database, actor) = database().await?;
    save_as(
        &database,
        &actor,
        configuration(),
        &change(0, true),
        Utc::now(),
    )
    .await?;
    let binding = capture(&database, configuration()).await?;
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='setup fixture logout' WHERE user_id=$1").bind(actor.user.to_string()).execute(&database.pool).await?;
    binding.current().await?;
    assert!(
        load_as(&database, &actor, configuration(), Utc::now())
            .await
            .is_err()
    );
    assert!(
        save_as(
            &database,
            &actor,
            configuration(),
            &change(1, false),
            Utc::now()
        )
        .await
        .is_err()
    );
    sqlx::query(
        "UPDATE home_members SET role='viewer',status='suspended' WHERE home_id=$1 AND user_id=$2",
    )
    .bind(actor.home.to_string())
    .bind(actor.user.to_string())
    .execute(&database.pool)
    .await?;
    assert!(binding.current().await.is_err());
    Ok(())
}

#[tokio::test]
async fn missing_or_corrupt_provider_decisions_are_not_reset_or_recreated() -> Result<()> {
    let (_root, database, actor) = database().await?;
    save_as(
        &database,
        &actor,
        configuration(),
        &change(0, true),
        Utc::now(),
    )
    .await?;
    sqlx::query("UPDATE marketplace_provider_setup SET setup_json='{}'")
        .execute(&database.pool)
        .await?;
    assert!(
        load_as(&database, &actor, configuration(), Utc::now())
            .await
            .is_err()
    );
    assert!(
        save_as(
            &database,
            &actor,
            configuration(),
            &change(1, true),
            Utc::now()
        )
        .await
        .is_err()
    );
    assert!(capture(&database, configuration()).await.is_err());
    sqlx::query("DELETE FROM marketplace_provider_setup")
        .execute(&database.pool)
        .await?;
    assert!(
        save_as(
            &database,
            &actor,
            configuration(),
            &change(0, true),
            Utc::now()
        )
        .await
        .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_provider_setup")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn exact_retries_do_not_request_restart_and_competing_mutations_have_one_winner() -> Result<()>
{
    let (_root, database, actor) = database().await?;
    let accept = change(0, true);
    let (first, changed) =
        super::save_as(&database, &actor, configuration(), &accept, Utc::now()).await?;
    assert!(changed);
    let enrollment = capture(&database, configuration()).await?.enrollment();
    let (same, changed) =
        super::save_as(&database, &actor, configuration(), &accept, Utc::now()).await?;
    assert_eq!(same, first);
    assert!(
        !changed,
        "the HTTP owner must not restart networking for an exact retry"
    );
    let enable = change(1, true);
    let disable = change(1, false);
    let (one, two) = tokio::join!(
        super::save_as(&database, &actor, configuration(), &enable, Utc::now()),
        super::save_as(&database, &actor, configuration(), &disable, Utc::now())
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let stored = Stored::parse(sqlx::query(SELECT).fetch_one(&database.pool).await?)?;
    assert_eq!(stored.revision, 2);
    assert_eq!(stored.decision.enrollment, Some(enrollment));
    Ok(())
}
