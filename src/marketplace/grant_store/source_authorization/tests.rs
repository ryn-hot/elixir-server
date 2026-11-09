//! Real owner/session rows. No source/native/network receipt is manufactured.
use super::*;

async fn fixture() -> Result<(tempfile::TempDir, Database, Actor)> {
    let root = tempfile::tempdir()?;
    let database =
        crate::marketplace::grant_store::tests::connect(&root.path().join("source-auth.sqlite"))
            .await?;
    let actor = crate::marketplace::grant_store::tests::owner(&database).await?;
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string()).bind(String::from_utf8(canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?)
        .bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&database.pool).await?;
    Ok((root, database, actor))
}
#[tokio::test]
async fn only_fresh_owner_authentication_can_capture_a_source_continuation() -> Result<()> {
    let (_root, database, actor) = fixture().await?;
    let mut expired = actor.fixture_principal();
    expired.access_expires_at = Utc::now() - chrono::Duration::seconds(1);
    assert!(
        SourceAuthorization::capture(&database, &expired)
            .await
            .is_err()
    );
    sqlx::query("UPDATE account_sessions SET csrf_revision=csrf_revision+1 WHERE id=$1")
        .bind(actor.session.to_string())
        .execute(&database.pool)
        .await?;
    assert!(
        SourceAuthorization::capture(&database, &actor.fixture_principal())
            .await
            .is_err(),
        "an already stale request cannot create new owned authority"
    );
    Ok(())
}
#[tokio::test]
async fn an_owned_source_survives_routine_refresh_but_does_not_relax_actor_actions() -> Result<()> {
    let (_root, database, actor) = fixture().await?;
    let mut held = SourceAuthorization::capture(&database, &actor.fixture_principal()).await?;
    let original = held.expires;
    let monotonic = held.monotonic_expires;
    // Simulate the already-accepted access token expiring, without changing
    // the active login or asking a wall-clock sleep to stand in for ownership.
    held.actor.expires = Utc::now() - chrono::Duration::seconds(1);
    sqlx::query(
        "UPDATE account_sessions SET csrf_revision=csrf_revision+1,expires_at=$1 WHERE id=$2",
    )
    .bind((Utc::now() + chrono::Duration::days(30)).to_rfc3339())
    .bind(actor.session.to_string())
    .execute(&database.pool)
    .await?;
    held.current(&database).await?;
    let cloned = held.clone();
    assert_eq!(cloned.expires, original);
    assert_eq!(cloned.monotonic_expires, monotonic);
    let mut tx = database.pool.begin().await?;
    assert!(
        held.actor.authenticate(&mut tx, Utc::now()).await.is_err(),
        "grant/setup/activation Actor must still need fresh access"
    );
    tx.rollback().await?;
    assert!(
        SourceAuthorization::capture(&database, &actor.fixture_principal())
            .await
            .is_err(),
        "old CSRF cannot start another operation after refresh"
    );
    Ok(())
}
#[tokio::test]
async fn logout_owner_revision_and_session_expiry_still_stop_owned_source_work() -> Result<()> {
    for change in ["logout", "owner", "revision", "session"] {
        let (_root, database, actor) = fixture().await?;
        let held = SourceAuthorization::capture(&database, &actor.fixture_principal()).await?;
        match change {
            "logout" => {
                sqlx::query(
                    "UPDATE account_sessions SET revoked_at=$1,revoked_reason='logout' WHERE id=$2",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(actor.session.to_string())
                .execute(&database.pool)
                .await?;
            }
            "owner" => {
                sqlx::query("UPDATE home_members SET role='admin' WHERE home_id=$1 AND user_id=$2")
                    .bind(actor.home.to_string())
                    .bind(actor.user.to_string())
                    .execute(&database.pool)
                    .await?;
            }
            "revision" => {
                sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1")
                .bind(actor.profile.to_string()).execute(&database.pool).await?;
            }
            "session" => {
                sqlx::query("UPDATE account_sessions SET expires_at=$1 WHERE id=$2")
                    .bind((Utc::now() - chrono::Duration::seconds(1)).to_rfc3339())
                    .bind(actor.session.to_string())
                    .execute(&database.pool)
                    .await?;
            }
            _ => unreachable!(),
        }
        assert!(held.current(&database).await.is_err(), "{change}");
    }
    Ok(())
}
#[tokio::test]
async fn refresh_and_clone_never_extend_the_original_monotonic_job_deadline() -> Result<()> {
    let (_root, database, actor) = fixture().await?;
    let held = SourceAuthorization::capture(&database, &actor.fixture_principal()).await?;
    tokio::time::pause();
    let cloned = held.clone();
    assert!(
        held.valid_for()
            <= std::time::Duration::from_secs(elixir_marketplace::quarantine::MAX_JOB_SECONDS)
    );
    tokio::time::advance(std::time::Duration::from_secs(
        elixir_marketplace::quarantine::MAX_JOB_SECONDS + 1,
    ))
    .await;
    assert!(held.check().is_err());
    assert!(cloned.check().is_err());
    assert_eq!(held.valid_for(), std::time::Duration::ZERO);
    assert!(cloned.current(&database).await.is_err());
    Ok(())
}
