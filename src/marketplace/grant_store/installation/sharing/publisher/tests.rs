//! Real SDK signatures, archives and SQL; receipt/protection and initial ACTIVE
//! enrollment are synthetic. Packaged transfer is checked separately.
use super::*;
use crate::marketplace::grant_store::installation::{
    standing::tests::local_http::configured,
    tests::{Fixture, budget},
};
use serde_json::{Value, json};

pub(in crate::marketplace::grant_store::installation::sharing) fn publication(
    f: &Fixture,
) -> PreparedPublication {
    PreparedPublication {
        schema_version: 1,
        publisher: f.descriptor.publisher.clone(),
        release: f.descriptor.release.clone(),
        head: f.descriptor.head.clone(),
    }
}
fn input(f: &Fixture, instance: Uuid, revision: i64, mutation: Uuid) -> Result<Value> {
    let p = publication(f);
    Ok(
        json!({"schema_version":1,"instance_id":instance,"extension_id":p.release.payload.extension_id,
        "expected_revision":revision,"mutation_id":mutation,"action":{"type":"enable",
        "accepted_reference":Digest::of(p.to_bytes()?),"publication":p,"accept_unqualified_seeding":true}}),
    )
}
pub(in crate::marketplace::grant_store::installation::sharing) async fn enable(
    f: &Fixture,
    instance: Uuid,
) -> Result<()> {
    let (receipt, _owner) =
        PublicationObservation::fixture_for_verified_publication(publication(f))?;
    change(
        &f.db,
        &f.actor.fixture_principal(),
        Request::parse(&canonical::to_vec(&input(f, instance, 0, Uuid::new_v4())?)?)?,
        Some(&receipt),
        budget(),
    )
    .await?;
    Ok(())
}
async fn source(f: &Fixture, instance: Uuid) -> Result<(SharingSource, Box<dyn Fn(bool)>)> {
    let (receipt, owner) =
        PublicationObservation::fixture_for_verified_publication(publication(f))?;
    let consent = load(
        &f.db,
        f.actor.home,
        &f.descriptor.release.payload.extension_id,
        instance,
    )
    .await?
    .context("explicit seed choice")?;
    let source =
        SharingSource::capture_publisher(&f.db, f.actor.home, instance, consent, receipt, budget())
            .await?;
    Ok((source, Box::new(owner)))
}
#[tokio::test]
async fn exact_owner_consent_seeds_without_paid_metadata_and_survives_logout() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, _) = configured(&f, "Publisher").await?;
    assert!(
        load(
            &f.db,
            f.actor.home,
            &f.descriptor.release.payload.extension_id,
            instance
        )
        .await?
        .is_none()
    );
    assert!(
        !view(
            &f.db,
            &f.actor.fixture_principal(),
            &f.descriptor.release.payload.extension_id,
            instance
        )
        .await?
        .enabled
    );
    enable(&f, instance).await?;
    // No MetadataObservation enters capture_publisher; loss of its unrelated
    // initial-fixture paid receipt cannot manufacture/withdraw publisher consent.
    (f.meta_owner)(false);
    let (held, _owner) = source(&f, instance).await?;
    assert!(matches!(&held.admission, Admission::Publisher { .. }));
    assert_eq!(held.inventory()?.blobs.len(), 2);
    sqlx::query("UPDATE account_sessions SET revoked_at=$1,revoked_reason='logout' WHERE id=$2")
        .bind(Utc::now().to_rfc3339())
        .bind(f.actor.fixture_principal().account_session_id.to_string())
        .execute(&f.db.pool)
        .await?;
    held.current(&f.db).await?;
    assert!(
        view(
            &f.db,
            &f.actor.fixture_principal(),
            &f.descriptor.release.payload.extension_id,
            instance
        )
        .await
        .is_err(),
        "a standing seed does not authorize fresh HTTP actions after logout"
    );
    Ok(())
}
#[tokio::test]
async fn withdrawal_is_offline_idempotent_and_invalidates_a_retained_seed() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, _) = configured(&f, "Publisher").await?;
    let mutation = Uuid::new_v4();
    let initial = input(&f, instance, 0, mutation)?;
    let (receipt, _owner) =
        PublicationObservation::fixture_for_verified_publication(publication(&f))?;
    let bytes = canonical::to_vec(&initial)?;
    assert_eq!(
        change(
            &f.db,
            &f.actor.fixture_principal(),
            Request::parse(&bytes)?,
            Some(&receipt),
            budget()
        )
        .await?
        .revision,
        1
    );
    assert_eq!(
        change(
            &f.db,
            &f.actor.fixture_principal(),
            Request::parse(&bytes)?,
            Some(&receipt),
            budget()
        )
        .await?
        .revision,
        1
    );
    let (held, protection) = source(&f, instance).await?;
    held.current(&f.db).await?;
    let mut forged_retry = initial.clone();
    forged_retry["action"] = json!({"type":"disable"});
    assert!(
        change(
            &f.db,
            &f.actor.fixture_principal(),
            Request::parse(&canonical::to_vec(&forged_retry)?)?,
            None,
            budget()
        )
        .await
        .is_err()
    );
    protection(false);
    sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1")
        .bind(instance.to_string())
        .execute(&f.db.pool)
        .await?;
    let disable = json!({"schema_version":1,"instance_id":instance,"extension_id":f.descriptor.release.payload.extension_id,
        "expected_revision":1,"mutation_id":Uuid::new_v4(),"action":{"type":"disable"}});
    for _ in 0..2 {
        let saved = change(
            &f.db,
            &f.actor.fixture_principal(),
            Request::parse(&canonical::to_vec(&disable)?)?,
            None,
            budget(),
        )
        .await?;
        assert!(!saved.enabled);
        assert_eq!(saved.revision, 2);
    }
    protection(true);
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(instance.to_string())
        .execute(&f.db.pool)
        .await?;
    assert!(held.current(&f.db).await.is_err());
    assert!(
        load(
            &f.db,
            f.actor.home,
            &f.descriptor.release.payload.extension_id,
            instance
        )
        .await?
        .is_none()
    );
    assert!(
        change(
            &f.db,
            &f.actor.fixture_principal(),
            Request::parse(&bytes)?,
            Some(&receipt),
            budget()
        )
        .await
        .is_err(),
        "old enable retry must not undo a newer disable"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_publisher_seeding")
            .fetch_one(&f.db.pool)
            .await?,
        1
    );
    Ok(())
}
#[tokio::test]
async fn publisher_choice_cannot_substitute_another_release_instance_or_reference() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, _) = configured(&f, "Publisher").await?;
    let (other, _) = configured(&f, "Other").await?;
    enable(&f, instance).await?;
    let (held, _owner) = source(&f, instance).await?;
    let consent = load(
        &f.db,
        f.actor.home,
        &f.descriptor.release.payload.extension_id,
        instance,
    )
    .await?
    .unwrap();
    let (receipt, _protection) =
        PublicationObservation::fixture_for_verified_publication(publication(&f))?;
    assert!(
        SharingSource::capture_publisher(&f.db, f.actor.home, other, consent, receipt, budget())
            .await
            .is_err()
    );
    assert!(
        load(
            &f.db,
            Uuid::new_v4(),
            &f.descriptor.release.payload.extension_id,
            instance
        )
        .await
        .is_err()
    );
    let mut bad = input(&f, instance, 1, Uuid::new_v4())?;
    bad["action"]["accepted_reference"] = json!(Digest::of(b"wrong"));
    assert!(Request::parse(&canonical::to_vec(&bad)?).is_err());
    // An exact source binding also rejects a new signed release/head, even if
    // the name/keys match. A stale choice never follows an update implicitly.
    let mut updated = Fixture::new().await?;
    let (owner, _) = configured(&updated, "Original").await?;
    enable(&updated, owner).await?;
    updated.upgrade("1.1.0", None).await?;
    let (new_receipt, _new_owner) =
        PublicationObservation::fixture_for_verified_publication(publication(&updated))?;
    let request = Request::parse(&canonical::to_vec(&input(
        &updated,
        owner,
        1,
        Uuid::new_v4(),
    )?)?)?;
    assert!(
        change(
            &updated.db,
            &updated.actor.fixture_principal(),
            request,
            Some(&new_receipt),
            budget()
        )
        .await
        .is_err()
    );
    held.current(&f.db).await?;
    Ok(())
}
#[tokio::test]
async fn standing_seed_rechecks_owner_revision_grant_and_original_protection() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, _) = configured(&f, "Publisher").await?;
    enable(&f, instance).await?;
    let (held, protection) = source(&f, instance).await?;
    protection(false);
    assert!(held.current(&f.db).await.is_err());
    protection(true);
    held.current(&f.db).await?;
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE WHERE extension_id=$1")
        .bind(f.descriptor.release.payload.extension_id.as_str())
        .execute(&f.db.pool)
        .await?;
    assert!(held.current(&f.db).await.is_err());
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=TRUE WHERE extension_id=$1")
        .bind(f.descriptor.release.payload.extension_id.as_str())
        .execute(&f.db.pool)
        .await?;
    held.current(&f.db).await?;
    sqlx::query(
        "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
    )
    .bind(f.actor.fixture_principal().profile_id.to_string())
    .execute(&f.db.pool)
    .await?;
    assert!(held.current(&f.db).await.is_err());
    Ok(())
}
#[tokio::test]
async fn parsing_requires_bounded_closed_explicit_consent() -> Result<()> {
    let f = Fixture::new().await?;
    let instance = Uuid::new_v4();
    let original = input(&f, instance, 0, Uuid::new_v4())?;
    Request::parse(&canonical::to_vec(&original)?)?;
    for (field, value) in [
        ("accept_unqualified_seeding", json!(false)),
        ("verified", json!(true)),
    ] {
        let mut bad = original.clone();
        bad["action"][field] = value;
        assert!(Request::parse(&canonical::to_vec(&bad)?).is_err());
    }
    let mut bad = original.clone();
    bad["instance_id"] = json!(Uuid::nil());
    assert!(Request::parse(&canonical::to_vec(&bad)?).is_err());
    let mut bad = original.clone();
    bad["expected_revision"] = json!(MAX_REVISION);
    assert!(Request::parse(&canonical::to_vec(&bad)?).is_err());
    let mut bad = original.clone();
    bad["allocation"] = json!("unbounded");
    assert!(Request::parse(&canonical::to_vec(&bad)?).is_err());
    assert!(Request::parse(&vec![b' '; MAX_REQUEST_BYTES + 1]).is_err());
    let duplicate = format!(
        "{{\"schema_version\":1,{}",
        &serde_json::to_string(&original)?[1..]
    );
    assert!(Request::parse(duplicate.as_bytes()).is_err());
    Ok(())
}
