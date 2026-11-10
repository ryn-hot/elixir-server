//! Local cryptography + actual database transactions. Synthetic receipt owners
//! are not evidence of native process isolation or protected public retrieval.
use super::*;
use crate::config::DatabaseConfig;
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor,
    evidence::IssuerPolicy,
    records::Payload,
    records::{Revocation, SignedRecord},
};
use std::sync::Arc;

mod catalog_export;
mod current_batch;
mod direct_import;
mod preview;
mod retained;
mod unchanged;

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}
fn fixture() -> Result<(CandidateDescriptor, IssuerPolicy)> {
    let value: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let mut d = CandidateDescriptor::parse(&serde_json::to_vec(
        &serde_json::json!({"schema_version":1,
        "publisher":value["publisher"],"release":value["release"],"head":value["head"],
        "revocations":[],"gate_evidence":value["evidence"]["pairs"]}),
    )?)?;
    let now = chrono::Utc::now().timestamp() as u64;
    for (index, pair) in d.gate_evidence.iter_mut().enumerate() {
        let mut credential = pair.credential.payload.clone();
        credential.issued_at = now;
        credential.expires_at = now + 3600;
        pair.credential = SignedRecord::sign(credential, None, &key(3))?;
        let mut endorsement = pair.endorsement.payload.clone();
        endorsement.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(endorsement, None, &key(10 + index as u8))?;
    }
    Ok((
        d,
        IssuerPolicy::new([value["issuer_key"].as_str().unwrap().parse()?])?,
    ))
}
fn incoming(d: &CandidateDescriptor) -> Result<Incoming> {
    let records = SecurityRecords {
        schema_version: 1,
        extension_id: d.release.payload.extension_id.clone(),
        publisher: d.publisher.clone(),
        head: Some(d.head.clone()),
        revocations: d.revocations.clone(),
    };
    let summary = records.verify(chrono::Utc::now().timestamp() as u64)?;
    Ok(Incoming { records, summary })
}
async fn database() -> Result<(tempfile::TempDir, Database)> {
    let directory = tempfile::tempdir()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            directory.path().join("security.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    Ok((directory, database))
}
fn resign<T: Payload>(record: &mut SignedRecord<T>, key: &SigningKey) -> Result<()> {
    *record = SignedRecord::sign(record.payload.clone(), record.authority_record_id, key)?;
    Ok(())
}

#[tokio::test]
async fn durable_fences_survive_restart_reject_old_heads_and_ignore_unknown_signed_only_ids()
-> Result<()> {
    let (directory, db) = database().await?;
    let (d, policy) = fixture()?;
    assert!(!mutate(&db, incoming(&d)?, false).await?);
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    assert!(require_current(&db, &receipt).await.is_err());
    admit(&db, &receipt).await?;
    require_current(&db, &receipt).await?;
    let mut advanced = d.clone();
    advanced.head.payload.sequence += 1;
    resign(&mut advanced.head, &key(2))?;
    assert!(mutate(&db, incoming(&advanced)?, false).await?);
    assert!(require_current(&db, &receipt).await.is_err());
    assert!(
        admit(&db, &receipt).await.is_err(),
        "replaying valid old metadata cannot erase a newer fence"
    );
    let (new_receipt, _new_owner) =
        MetadataObservation::fixture_for_verified_descriptor(advanced, &policy)?;
    require_current(&db, &new_receipt).await?;
    db.pool.close().await;
    let reopened = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            directory.path().join("security.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    require_current(&reopened, &new_receipt).await?;
    assert!(require_current(&reopened, &receipt).await.is_err());
    Ok(())
}

#[tokio::test]
async fn publisher_and_head_forks_are_sticky_and_only_strictly_newer_state_resolves_them()
-> Result<()> {
    let (_directory, db) = database().await?;
    let (d, policy) = fixture()?;
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    let mut fork = d.clone();
    fork.head.payload.timestamp -= 1;
    resign(&mut fork.head, &key(2))?;
    mutate(&db, incoming(&fork)?, false).await?;
    assert!(require_current(&db, &receipt).await.is_err());
    assert!(admit(&db, &receipt).await.is_err());
    let mut newer = d.clone();
    newer.head.payload.sequence += 1;
    resign(&mut newer.head, &key(2))?;
    mutate(&db, incoming(&newer)?, false).await?;
    let (receipt2, _owner2) =
        MetadataObservation::fixture_for_verified_descriptor(newer.clone(), &policy)?;
    require_current(&db, &receipt2).await?;
    let mut publisher_fork = newer.clone();
    publisher_fork
        .publisher
        .payload
        .release_keys
        .push(PublicKey::from_bytes(key(99).verifying_key().to_bytes())?);
    resign(&mut publisher_fork.publisher, &key(1))?;
    mutate(&db, incoming(&publisher_fork)?, false).await?;
    assert!(admit(&db, &receipt2).await.is_err());
    let old_authority = newer.publisher.clone();
    newer.publisher.payload.sequence += 1;
    resign(&mut newer.publisher, &key(1))?;
    newer.release_authority = Some(old_authority);
    let authority = Digest::of(newer.publisher.to_bytes()?);
    newer.head.authority_record_id = Some(authority);
    newer.head.payload.publisher_state_id = authority;
    newer.head.payload.sequence = 0;
    resign(&mut newer.head, &key(2))?;
    let (receipt3, _owner3) = MetadataObservation::fixture_for_verified_descriptor(newer, &policy)?;
    admit(&db, &receipt3).await?;
    assert!(require_current(&db, &receipt2).await.is_err());
    Ok(())
}

#[tokio::test]
async fn withdrawal_without_adoption_or_valid_head_is_retained_and_cannot_be_omitted_later()
-> Result<()> {
    let (_directory, db) = database().await?;
    let (d, policy) = fixture()?;
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    let mut withdrawal = d.clone();
    withdrawal.head.payload.sequence += 999; // invalid signature, no head observation
    withdrawal.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: d.publisher.payload.root_key,
            extension_id: d.release.payload.extension_id.clone(),
            release_ids: vec![Digest::of(canonical::to_vec(&d.release.payload)?)],
            release_keys: vec![],
            reason: "security withdrawal".into(),
        },
        None,
        &key(1),
    )?);
    let update = incoming(&withdrawal)?;
    assert!(update.summary.head.is_none());
    mutate(&db, update, false).await?;
    assert!(require_current(&db, &receipt).await.is_err());
    assert!(admit(&db, &receipt).await.is_err());
    Ok(())
}

#[tokio::test]
async fn row_loss_corruption_or_missing_budget_never_resets_security_authority() -> Result<()> {
    let (_directory, db) = database().await?;
    let (d, policy) = fixture()?;
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    sqlx::query("DELETE FROM marketplace_extension_security")
        .execute(&db.pool)
        .await?;
    assert!(require_current(&db, &receipt).await.is_err());
    assert!(admit(&db, &receipt).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_security")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 0);
    sqlx::query("DELETE FROM marketplace_security_budget")
        .execute(&db.pool)
        .await?;
    db.run_migrations().await?;
    assert!(admit(&db, &receipt).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_security_budget")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_owners_cannot_revert_a_newer_publisher_or_head() -> Result<()> {
    let (_directory, db) = database().await?;
    let (d, policy) = fixture()?;
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    let baseline = catalog_baseline(&db).await?;
    assert_eq!(baseline.revision(), 1);
    baseline.require_current(&db).await?;
    let unchanged = catalog_admit(&db, &receipt, 1).await?;
    assert_eq!(unchanged.revision(), 1);
    assert!(unchanged.into_batch().is_none());
    let mut next = d.clone();
    next.head.payload.sequence += 1;
    resign(&mut next.head, &key(2))?;
    let db = Arc::new(db);
    let mut tasks = vec![];
    for index in 0..8 {
        let database = db.clone();
        let input = incoming(if index % 2 == 0 { &d } else { &next })?;
        tasks.push(tokio::spawn(async move {
            mutate(&database, input, false).await
        }));
    }
    for task in tasks {
        task.await??;
    }
    assert!(require_current(&db, &receipt).await.is_err());
    let (receipt2, _owner2) = MetadataObservation::fixture_for_verified_descriptor(next, &policy)?;
    require_current(&db, &receipt2).await?;
    Ok(())
}

#[test]
fn revocation_retention_plateaus_and_saturates_instead_of_forgetting_old_ids() -> Result<()> {
    let (d, _) = fixture()?;
    let mut input = incoming(&d)?;
    let mut p = PublisherState {
        schema_version: 1,
        root_key: input.summary.root_key,
        sequence: 0,
        payload_id: input.summary.publisher_payload_id,
        conflicted: false,
    };
    let mut e = ExtensionState {
        schema_version: 1,
        extension_id: input.summary.extension_id.clone(),
        root_key: p.root_key,
        head: None,
        revoked_releases: BTreeSet::new(),
        revoked_keys: BTreeSet::new(),
        saturated: false,
    };
    for i in 0..132u64 {
        input.records.revocations = vec![SignedRecord::sign(
            Revocation {
                root_key: p.root_key,
                extension_id: e.extension_id.clone(),
                release_ids: vec![Digest::of(i.to_be_bytes())],
                release_keys: vec![],
                reason: "test".into(),
            },
            None,
            &key(1),
        )?];
        input.summary = input
            .records
            .verify(chrono::Utc::now().timestamp() as u64)?;
        merge(&mut p, &mut e, &input);
    }
    assert_eq!(e.revoked_releases.len(), 128);
    assert!(e.saturated);
    assert!(e.revoked_releases.contains(&Digest::of(0u64.to_be_bytes())));
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL security-state database"]
async fn postgres_security_state_serializes_replays_reopens_and_retains_withdrawals() -> Result<()>
{
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_SECURITY_POSTGRES_URL")
        .context("explicit fresh loopback security database required")?;
    let parsed = reqwest::Url::parse(&url)?;
    if parsed.host_str() != Some("127.0.0.1")
        || DatabaseDriver::from_url(&url)? != DatabaseDriver::Postgres
    {
        bail!("security fixture requires a fresh loopback PostgreSQL database");
    }
    let config = DatabaseConfig {
        url,
        max_connections: 4,
        connect_timeout_seconds: 5,
    };
    let db = Database::connect(&config).await?;
    let existing: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")
            .fetch_one(&db.pool)
            .await?;
    if existing != 0 {
        bail!("security fixture refuses an existing database");
    }
    db.run_migrations().await?;
    let (d, policy) = fixture()?;
    let (direct, _direct_owner) = direct_import::receipt(&d)?;
    assert!(!require_publication_current(&db, &direct).await?);
    assert!(
        !catalog_observe_publication(&db, &direct, 0)
            .await?
            .tracked()
    );
    assert!(!mutate(&db, incoming(&d)?, false).await?);
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    assert!(require_publication_current(&db, &direct).await?);
    assert_eq!(require_current_batch(&db, &[&receipt]).await?, vec![true]);
    let baseline = catalog_baseline(&db).await?;
    assert_eq!(baseline.revision(), 1);
    baseline.require_current(&db).await?;
    let unchanged = catalog_admit(&db, &receipt, 1).await?;
    assert_eq!(unchanged.revision(), 1);
    assert!(unchanged.into_batch().is_none());
    let mut next = d.clone();
    next.head.payload.sequence += 1;
    resign(&mut next.head, &key(2))?;
    let (a, b) = tokio::join!(
        mutate(&db, incoming(&d)?, false),
        mutate(&db, incoming(&next)?, false)
    );
    a?;
    b?;
    assert!(require_current(&db, &receipt).await.is_err());
    assert_eq!(require_current_batch(&db, &[&receipt]).await?, vec![false]);
    let (next_receipt, _next_owner) =
        MetadataObservation::fixture_for_verified_descriptor(next.clone(), &policy)?;
    let (next_direct, _next_direct_owner) = direct_import::receipt(&next)?;
    assert!(require_publication_current(&db, &next_direct).await?);
    require_current(&db, &next_receipt).await?;
    assert!(baseline.require_current(&db).await.is_err());
    let advanced = catalog_baseline(&db).await?;
    assert_eq!(advanced.revision(), 2);
    assert!(catalog_admit(&db, &next_receipt, 1).await.is_err());
    db.pool.close().await;
    let reopened = Database::connect(&config).await?;
    require_current(&reopened, &next_receipt).await?;
    assert!(require_publication_current(&reopened, &next_direct).await?);
    next.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: d.publisher.payload.root_key,
            extension_id: d.release.payload.extension_id.clone(),
            release_ids: vec![],
            release_keys: vec![d.release.signer_key],
            reason: "withdrawn signing key".into(),
        },
        None,
        &key(1),
    )?);
    mutate(&reopened, incoming(&next)?, false).await?;
    assert!(require_current(&reopened, &next_receipt).await.is_err());
    assert!(
        require_publication_current(&reopened, &next_direct)
            .await
            .is_err()
    );
    assert!(admit(&reopened, &next_receipt).await.is_err());
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM marketplace_security_budget WHERE singleton=1")
            .fetch_one(&reopened.pool)
            .await?;
    assert!(revision >= 3);
    let baseline = catalog_baseline(&reopened).await?;
    assert_eq!(baseline.revision(), revision as u64);
    let extension = baseline
        .batches()
        .find_map(|batch| batch.extensions.into_iter().next())
        .context("missing PostgreSQL exported extension")?;
    assert!(extension.revoked_keys.contains(&d.release.signer_key));
    baseline.require_current(&reopened).await?;
    let (page, _page_owners) = current_batch::full_page(&reopened).await?;
    let references = page.iter().rev().collect::<Vec<_>>();
    assert_eq!(
        require_current_batch(&reopened, &references).await?,
        vec![true; 20]
    );
    for value in references {
        require_current(&reopened, value).await?;
    }
    reopened.pool.close().await;
    Ok(())
}
