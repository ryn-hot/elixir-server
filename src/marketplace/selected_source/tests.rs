//! Real SQLite owner/session/security transactions and signed public records;
//! synthetic native receipt/protection owner, not VPN or process isolation.
use super::*;
use crate::config::DatabaseConfig;
use crate::marketplace::grant_store::Actor;
use elixir_marketplace::{canonical, publication::PreparedPublication};

async fn fixture() -> Result<(tempfile::TempDir, Database, Actor, PreparedPublication)> {
    let directory = tempfile::tempdir()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            directory.path().join("direct.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let actor = crate::marketplace::grant_store::tests::owner(&database).await?;
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string())
        .bind(String::from_utf8(canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?)
        .bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&database.pool).await?;
    let vector: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let publication = PreparedPublication::parse(&serde_json::to_vec(&serde_json::json!({
        "schema_version":1,"publisher":vector["publisher"],"release":vector["release"],"head":vector["head"],
    }))?)?;
    Ok((directory, database, actor, publication))
}
#[tokio::test]
async fn zero_support_direct_acceptance_allocates_only_owner_bound_security_not_discovery()
-> Result<()> {
    let (_directory, database, actor, publication) = fixture().await?;
    let reference = Digest::of(publication.to_bytes()?);
    let (observation, _owner) =
        PublicationObservation::fixture_for_verified_publication(publication.clone())?;
    let accepted = AcceptedPublication::capture(
        &database,
        observation,
        &actor.fixture_principal(),
        reference,
    )
    .await?;
    assert!(accepted.discovery().is_none());
    assert!(accepted.require_actor(actor.user, actor.home).is_ok());
    assert!(accepted.require_actor(Uuid::new_v4(), actor.home).is_err());
    assert!(accepted.require_actor(actor.user, Uuid::new_v4()).is_err());
    assert!(
        accepted.require_current(&database).await.is_err(),
        "a preview cannot become enrolled authority"
    );
    let mutation =
        crate::marketplace::security_state::catalog_accept_publication(&database, &accepted, 0)
            .await?;
    assert!(mutation.tracked());
    assert_eq!(mutation.revision(), 1);
    assert!(mutation.into_batch().is_some());
    accepted.require_current(&database).await?;
    let authority = accepted.authority_record()?.verify()?;
    assert_eq!(authority.release().release_id(), publication.release_id()?);
    let fake = serde_json::json!({"schema_version":1,"publisher":publication.publisher,"release":publication.release,
        "head":publication.head,"revocations":[],"gate_evidence":[]});
    assert!(
        elixir_marketplace::admission::CandidateDescriptor::parse(&canonical::to_vec(&fake)?)
            .is_err(),
        "direct acceptance must not loosen the closed five-proof discovery descriptor"
    );
    for table in [
        "extensions",
        "extension_instances",
        "marketplace_extension_grants",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(
            count, 0,
            "{table}: acceptance is not permission, installation or execution"
        );
    }
    sqlx::query("UPDATE account_sessions SET revoked_at=$1,revoked_reason='logout' WHERE id=$2")
        .bind(Utc::now().to_rfc3339())
        .bind(actor.fixture_principal().account_session_id.to_string())
        .execute(&database.pool)
        .await?;
    assert!(accepted.require_current(&database).await.is_err());
    assert!(
        crate::marketplace::security_state::catalog_accept_publication(&database, &accepted, 1)
            .await
            .is_err()
    );
    assert_eq!(
        crate::marketplace::security_state::catalog_baseline(&database)
            .await?
            .revision(),
        1
    );
    Ok(())
}
#[tokio::test]
async fn wrong_reference_expired_access_and_withdrawn_owner_cannot_allocate_direct_authority()
-> Result<()> {
    let (_directory, database, actor, publication) = fixture().await?;
    let reference = Digest::of(publication.to_bytes()?);
    let (observation, _owner) =
        PublicationObservation::fixture_for_verified_publication(publication.clone())?;
    assert!(
        AcceptedPublication::capture(
            &database,
            observation,
            &actor.fixture_principal(),
            Digest::of(b"other reference")
        )
        .await
        .is_err()
    );
    let (observation, _owner) =
        PublicationObservation::fixture_for_verified_publication(publication.clone())?;
    let mut principal = actor.fixture_principal();
    principal.access_expires_at = Utc::now() - chrono::Duration::seconds(1);
    assert!(
        AcceptedPublication::capture(&database, observation, &principal, reference)
            .await
            .is_err()
    );
    let (observation, _owner) =
        PublicationObservation::fixture_for_verified_publication(publication)?;
    let accepted = AcceptedPublication::capture(
        &database,
        observation,
        &actor.fixture_principal(),
        reference,
    )
    .await?;
    sqlx::query("UPDATE home_members SET role='admin' WHERE home_id=$1 AND user_id=$2")
        .bind(actor.home.to_string())
        .bind(actor.user.to_string())
        .execute(&database.pool)
        .await?;
    assert!(
        crate::marketplace::security_state::catalog_accept_publication(&database, &accepted, 0)
            .await
            .is_err()
    );
    assert_eq!(
        crate::marketplace::security_state::catalog_baseline(&database)
            .await?
            .revision(),
        0
    );
    Ok(())
}
