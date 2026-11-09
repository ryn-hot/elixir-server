//! Actual SQLite transactions, local cryptography and (in the final test) real
//! Node/native-Rust private pipes. Synthetic receipt owners do not prove the
//! protected public/UID10005 path or aggregate cgroup resource qualification.
use super::*;
use elixir_marketplace::{
    catalog_fence::baseline_digest,
    worker_control::{WorkerControlOperation, WorkerControlOutcome},
};

#[tokio::test]
async fn baseline_and_transaction_deltas_bind_exact_rows_revisions_and_digest() -> Result<()> {
    let (_directory, db) = database().await?;
    let empty = catalog_baseline(&db).await?;
    assert_eq!(empty.revision(), 0);
    assert_eq!(empty.batches().count(), 0);
    empty.require_current(&db).await?;
    let (descriptor, policy) = fixture()?;
    let (metadata, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let first = catalog_admit(&db, &metadata, 0).await?;
    assert_eq!(first.revision(), 1);
    let batch = first.into_batch().context("new admission has no delta")?;
    assert_eq!(batch.publishers.len(), 1);
    assert_eq!(batch.extensions.len(), 1);
    assert!(empty.require_current(&db).await.is_err());
    assert!(
        catalog_admit(&db, &metadata, 0).await.is_err(),
        "an old mirror cannot skip an unseen revision"
    );
    let same = catalog_admit(&db, &metadata, 1).await?;
    assert_eq!(same.revision(), 1);
    assert!(same.into_batch().is_none());
    let current = catalog_baseline(&db).await?;
    assert_eq!(current.revision(), 1);
    let generation = Digest::of(b"baseline export test");
    assert!(
        matches!(current.begin(generation),WorkerControlOperation::CatalogSecurityBegin { revision:1,publishers:1,extensions:1,digest,.. }
        if digest==baseline_digest(&batch.publishers,&batch.extensions)?)
    );
    assert!(matches!(
        current.finish(generation),
        WorkerControlOperation::CatalogSecurityFinish { revision: 1, .. }
    ));
    let snapshot = current.batches().collect::<Vec<_>>();
    assert_eq!(
        canonical::to_vec(&snapshot[0].publishers)?,
        canonical::to_vec(&batch.publishers)?
    );
    assert_eq!(
        canonical::to_vec(&snapshot[1].extensions)?,
        canonical::to_vec(&batch.extensions)?
    );
    current.require_current(&db).await?;
    Ok(())
}

#[tokio::test]
async fn baseline_rejects_missing_blocked_or_corrupt_authority_without_resetting_it() -> Result<()>
{
    for failure in ["hash", "accounting", "blocked", "missing"] {
        let (_directory, db) = database().await?;
        let (descriptor, policy) = fixture()?;
        let (metadata, _owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
        admit(&db, &metadata).await?;
        match failure {
            "hash" => {
                sqlx::query("UPDATE marketplace_extension_security SET state_sha256=$1")
                    .bind(Digest::of(b"wrong state hash").to_string())
                    .execute(&db.pool)
                    .await?;
            }
            "accounting" => {
                sqlx::query("UPDATE marketplace_security_budget SET encoded_bytes=encoded_bytes+1")
                    .execute(&db.pool)
                    .await?;
            }
            "blocked" => {
                sqlx::query("UPDATE marketplace_security_budget SET blocked=1")
                    .execute(&db.pool)
                    .await?;
            }
            _ => {
                sqlx::query("DELETE FROM marketplace_security_budget")
                    .execute(&db.pool)
                    .await?;
            }
        }
        assert!(catalog_baseline(&db).await.is_err(), "{failure}");
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_security")
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(rows, 1, "{failure}: an export must not reset authority");
        if failure == "missing" {
            let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_security_budget")
                .fetch_one(&db.pool)
                .await?;
            assert_eq!(rows, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_snapshot_never_claims_a_later_transaction_or_an_unmirrored_external_update() -> Result<()>
{
    let (_directory, db) = database().await?;
    let (descriptor, policy) = fixture()?;
    let (metadata, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    catalog_admit(&db, &metadata, 0).await?;
    let before = catalog_baseline(&db).await?;
    let mut newer = descriptor;
    newer.head.payload.sequence += 1;
    resign(&mut newer.head, &key(2))?;
    mutate(&db, incoming(&newer)?, false).await?;
    assert!(before.require_current(&db).await.is_err());
    assert!(catalog_admit(&db, &metadata, 1).await.is_err());
    let after = catalog_baseline(&db).await?;
    assert_eq!(after.revision(), 2);
    assert_eq!(
        after.batches().last().unwrap().extensions[0]
            .head
            .as_ref()
            .unwrap()
            .sequence,
        1
    );
    Ok(())
}

#[tokio::test]
async fn durable_export_and_runtime_withdrawal_round_trip_through_a_real_private_worker()
-> Result<()> {
    use crate::{marketplace::client::PrivateWorkerClient, runtime::PrivateProcessStdio};
    use elixir_marketplace::{
        catalog::SearchQuery,
        peer::{PeerOperation, PeerOutcome},
    };
    use std::{path::PathBuf, process::Stdio};
    let (directory, db) = database().await?;
    let (mut descriptor, policy) = fixture()?;
    let (metadata, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let mut child = tokio::process::Command::new("node")
        .arg(root.join("crates/elixir_marketplace/worker/src/worker.mjs"))
        .args(["--state"])
        .arg(directory.path().canonicalize()?.join("catalog.sqlite"))
        .arg("--verifier")
        .arg(root.join("target/debug/elixir-marketplace-verifier"))
        .arg("--issuer-key")
        .arg(
            descriptor.gate_evidence[0]
                .credential
                .signer_key
                .to_string(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut worker = PrivateWorkerClient::new(PrivateProcessStdio {
        input: child.stdin.take().unwrap(),
        output: child.stdout.take().unwrap(),
        diagnostics: child.stderr.take(),
        child,
    })?;
    let generation = Digest::of(b"actual private worker reconciliation");
    let baseline = catalog_baseline(&db).await?;
    assert!(matches!(
        worker
            .control(baseline.begin(generation))
            .await?
            .control_response,
        WorkerControlOutcome::CatalogSecurity { ready: false, .. }
    ));
    for batch in baseline.batches() {
        worker
            .control(WorkerControlOperation::CatalogSecurityApply { generation, batch })
            .await?;
    }
    assert!(matches!(
        worker
            .control(baseline.finish(generation))
            .await?
            .control_response,
        WorkerControlOutcome::CatalogSecurity { ready: true, .. }
    ));
    let mutation = catalog_admit(&db, &metadata, 0).await?;
    worker
        .control(WorkerControlOperation::CatalogSecurityApply {
            generation,
            batch: mutation.into_batch().unwrap(),
        })
        .await?;
    assert!(matches!(
        worker
            .request(PeerOperation::Offer {
                descriptor: Box::new(descriptor.clone())
            })
            .await?
            .response,
        PeerOutcome::Offered { .. }
    ));
    let query = || PeerOperation::Search {
        query: SearchQuery {
            text: String::new(),
            tags: vec![],
            limit: 20,
        },
        cursor: None,
    };
    assert!(
        matches!(worker.request(query()).await?.response,PeerOutcome::Search {page} if page.results.len()==1)
    );
    descriptor.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: descriptor.publisher.payload.root_key,
            extension_id: descriptor.release.payload.extension_id.clone(),
            release_ids: vec![Digest::of(canonical::to_vec(&descriptor.release.payload)?)],
            release_keys: vec![],
            reason: "withdrawn".into(),
        },
        None,
        &key(1),
    )?);
    let withdrawal = mutate_at(&db, incoming(&descriptor)?, false, Some(1)).await?;
    assert_eq!(withdrawal.revision(), 2);
    assert!(matches!(
        worker
            .control(WorkerControlOperation::CatalogSecurityApply {
                generation,
                batch: withdrawal.into_batch().unwrap()
            })
            .await?
            .control_response,
        WorkerControlOutcome::CatalogSecurity {
            ready: true,
            revision: 2,
            ..
        }
    ));
    assert!(
        matches!(worker.request(query()).await?.response,PeerOutcome::Search {page} if page.results.is_empty())
    );
    descriptor.revocations.clear();
    worker
        .request(PeerOperation::Offer {
            descriptor: Box::new(descriptor),
        })
        .await?;
    assert!(
        matches!(worker.request(query()).await?.response,PeerOutcome::Search {page} if page.results.is_empty())
    );
    let baseline = catalog_baseline(&db).await?;
    let next = Digest::of(b"another owned baseline generation");
    worker.control(baseline.begin(next)).await?;
    for batch in baseline.batches() {
        worker
            .control(WorkerControlOperation::CatalogSecurityApply {
                generation: next,
                batch,
            })
            .await?;
    }
    assert!(matches!(
        worker
            .control(baseline.finish(next))
            .await?
            .control_response,
        WorkerControlOutcome::CatalogSecurity {
            ready: true,
            revision: 2,
            ..
        }
    ));
    assert!(
        matches!(worker.request(query()).await?.response,PeerOutcome::Search {page} if page.results.is_empty())
    );
    worker.close().await;
    Ok(())
}

#[tokio::test]
async fn baseline_orders_and_batches_more_than_one_frame_without_losing_fields() -> Result<()> {
    let (_directory, db) = database().await?;
    let (template, policy) = fixture()?;
    for index in 0..17 {
        let mut descriptor = template.clone();
        let prefix = descriptor
            .release
            .payload
            .extension_id
            .as_str()
            .split_once('/')
            .unwrap()
            .0;
        let id: ExtensionId = format!("{prefix}/baseline-{index}").parse()?;
        descriptor.release.payload.extension_id = id.clone();
        resign(&mut descriptor.release, &key(2))?;
        let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
        descriptor.head.payload.extension_id = id.clone();
        descriptor.head.payload.stable_release_id = release;
        resign(&mut descriptor.head, &key(2))?;
        for (n, pair) in descriptor.gate_evidence.iter_mut().enumerate() {
            pair.credential.payload.extension_id = id.clone();
            resign(&mut pair.credential, &key(3))?;
            pair.endorsement.payload.extension_id = id.clone();
            pair.endorsement.payload.release_id = release;
            pair.endorsement.payload.credential_id = Digest::of(pair.credential.to_bytes()?);
            resign(&mut pair.endorsement, &key(10 + n as u8))?;
        }
        let (metadata, _owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
        let mutation = catalog_admit(&db, &metadata, index).await?;
        assert_eq!(mutation.revision(), index + 1);
    }
    let snapshot = catalog_baseline(&db).await?;
    let batches = snapshot.batches().collect::<Vec<_>>();
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].publishers.len(), 1);
    assert_eq!(batches[1].extensions.len(), 16);
    assert_eq!(batches[2].extensions.len(), 1);
    let publishers = batches
        .iter()
        .flat_map(|b| b.publishers.clone())
        .collect::<Vec<_>>();
    let extensions = batches
        .iter()
        .flat_map(|b| b.extensions.clone())
        .collect::<Vec<_>>();
    for batch in &batches {
        batch.validate()?;
        assert_eq!(batch.revision, 17);
    }
    assert!(
        extensions
            .windows(2)
            .all(|rows| rows[0].extension_id.as_str() < rows[1].extension_id.as_str())
    );
    let expected = baseline_digest(&publishers, &extensions)?;
    assert!(
        matches!(snapshot.begin(Digest::of(b"multi-frame baseline")),WorkerControlOperation::CatalogSecurityBegin {revision:17,digest,publishers:1,extensions:17,..} if digest==expected)
    );
    Ok(())
}
