//! Real signatures and SQLite with synthetic protection/private-command owners.
//! These test commit/ACK ordering, not actual protected process provenance.
use super::retention::RetentionFixture;
use super::*;
use crate::marketplace::worker::protected::{MetadataObservation, source_preflight};
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    identity::Digest, records::SignedRecord, worker_control::WorkerControlOutcome,
};

async fn prepared() -> Result<(RetentionFixture, Arc<security_mirror::Mirror>, impl Send)> {
    let mut fixture = RetentionFixture::new().await?;
    let (source, owner) = source_preflight::Client::fixture_for_security_storage()?;
    fixture.client.source_preflight = Some(source);
    let mirror = Arc::new(security_mirror::Mirror::fixture(
        Digest::of(b"synthetic private mirror"),
        1,
    ));
    fixture.client.security_mirror = Some(mirror.clone());
    Ok((fixture, mirror, owner))
}

#[tokio::test]
async fn warm_search_admissions_are_exact_owner_local_and_revision_bound() -> Result<()> {
    use crate::marketplace::security_state::{Rejection, catalog_admit, catalog_observe};
    use crate::marketplace::worker::{PublicSourceObservation, SecurityObservation};
    use elixir_marketplace::{records::Revocation, security_job::SecurityRecords};

    let (fixture, mirror, _owner) = prepared().await?;
    let first = &fixture.inputs.metadata;
    let mut descriptor = first.descriptor().clone();
    let id = ExtensionId::new(&descriptor.publisher.payload.root_key, "warm-sibling")?;
    let release_key = SigningKey::from_bytes(&[2; 32]);
    descriptor.release.payload.extension_id = id.clone();
    descriptor.release = SignedRecord::sign(
        descriptor.release.payload,
        descriptor.release.authority_record_id,
        &release_key,
    )?;
    let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
    descriptor.head.payload.extension_id = id.clone();
    descriptor.head.payload.stable_release_id = release;
    descriptor.head = SignedRecord::sign(
        descriptor.head.payload,
        descriptor.head.authority_record_id,
        &release_key,
    )?;
    for (index, pair) in descriptor.gate_evidence.iter_mut().enumerate() {
        pair.credential.payload.extension_id = id.clone();
        pair.credential = SignedRecord::sign(
            pair.credential.payload.clone(),
            None,
            &SigningKey::from_bytes(&[3; 32]),
        )?;
        pair.endorsement.payload.extension_id = id.clone();
        pair.endorsement.payload.release_id = release;
        pair.endorsement.payload.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(
            pair.endorsement.payload.clone(),
            None,
            &SigningKey::from_bytes(&[10 + index as u8; 32]),
        )?;
    }
    let (second, _second_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &fixture.policy)?;
    let client = &fixture.client;
    let preflight = client.source_preflight.as_ref().unwrap();
    preflight.fixture_cache_metadata(first)?;
    preflight.fixture_cache_metadata(&second)?;
    let database = &client.database;
    let current = catalog_admit(database, &second, 1).await?.revision();
    *mirror.revision.lock().await = current;
    let descriptors = [first.descriptor().clone(), descriptor.clone()];
    let first_id = Digest::of(descriptors[0].to_bytes()?);
    let second_id = Digest::of(descriptor.to_bytes()?);
    let mut admissions = client.warm_local_search_admissions(&descriptors).await?;
    assert_eq!(admissions.len(), 2);
    let mut other = client.clone();
    other.security_mirror = Some(Arc::new(security_mirror::Mirror::fixture(
        Digest::of(b"another owner"),
        current,
    )));
    assert!(
        other
            .use_warm_search_admission(admissions.remove(&first_id).unwrap())
            .await?
            .is_none()
    );
    let (received, source) = client
        .use_warm_search_admission(admissions.remove(&second_id).unwrap())
        .await?
        .unwrap();
    assert_eq!(received.descriptor().to_bytes()?, descriptor.to_bytes()?);
    assert!(
        source.is_none(),
        "a cache miss must stay on the ordinary source path"
    );
    let (native, source_owner) =
        PublicSourceObservation::fixture_for_source(release, Digest::of(b"fixture license"));
    preflight.fixture_cache_search_source(&second, &native)?;
    let mut admissions = client.warm_local_search_admissions(&descriptors).await?;
    let (received, fact) = client
        .use_warm_search_admission(admissions.remove(&second_id).unwrap())
        .await?
        .context("expected an exact warm hit")?;
    let fact = fact.context("positive source cache hit missing")?;
    assert_eq!(received.descriptor().to_bytes()?, descriptor.to_bytes()?);
    assert_eq!(fact.release_id(), release);
    assert!(!fact.valid_for().is_zero());
    let mut admissions = client.warm_local_search_admissions(&descriptors).await?;
    let withdrawal = SignedRecord::sign(
        Revocation {
            root_key: descriptor.publisher.payload.root_key,
            extension_id: id.clone(),
            release_ids: vec![release],
            release_keys: vec![],
            reason: "withdrawn".into(),
        },
        None,
        &SigningKey::from_bytes(&[1; 32]),
    )?;
    let (updated, _updated_owner) = SecurityObservation::fixture_for_withdrawal(SecurityRecords {
        schema_version: elixir_marketplace::SCHEMA_VERSION,
        extension_id: id,
        publisher: descriptor.publisher,
        head: None,
        revocations: vec![withdrawal],
    })?;
    let next = catalog_observe(database, &updated, current)
        .await?
        .revision();
    assert_eq!(next, current + 1);
    *mirror.revision.lock().await = next;
    assert!(
        client
            .use_warm_search_admission(admissions.remove(&second_id).unwrap())
            .await?
            .is_none()
    );
    let mut admissions = client.warm_local_search_admissions(&descriptors).await?;
    let error = client
        .use_warm_search_admission(admissions.remove(&second_id).unwrap())
        .await
        .err()
        .context("withdrawn no-op must not count as current")?;
    assert!(matches!(
        error.downcast_ref::<Rejection>(),
        Some(Rejection::Withdrawn)
    ));
    assert!(
        !fact.valid_for().is_zero(),
        "withdrawal must reject even with a live source fact"
    );
    source_owner(false);
    assert!(
        fact.valid_for().is_zero(),
        "warm path must not refresh the original owner"
    );
    Ok(())
}
#[tokio::test]
async fn setup_page_uses_the_same_existing_request_slot_and_checked_client_admission() -> Result<()>
{
    // Synthetic protection/private RPC only, not native network provenance.
    let (mut fixture, _mirror, _owner) = prepared().await?;
    let peer = PeerKey::try_from("1".repeat(64))?;
    let client = fixture.client.clone();
    let task = tokio::spawn(async move {
        client
            .prepare_catalog_page(PeerKey::try_from("1".repeat(64))?)
            .await
    });
    let Request::CatalogSetup(page) = timeout(Duration::from_secs(3), fixture.requests.recv())
        .await?
        .context("no original page request")?
    else {
        bail!("wrong private request");
    };
    assert!(!page.started);
    assert!(page.reconnect_until.is_none());
    assert_eq!(page.peer, peer);
    assert!(page.admission.catalog_preparation_ready()?);
    let state = CatalogJobState {
        phase: CatalogJobPhase::Progress,
        kind: Some(CatalogJobKind::Population),
        peer: Some(peer),
        admitted: 1,
        unchanged: 0,
        rejected: 0,
        indexed: 0,
        removed: 0,
    };
    page.reply
        .send(Ok(Some(state)))
        .map_err(|_| anyhow::anyhow!("page waiter lost"))?;
    assert_eq!(task.await??.unwrap().phase, CatalogJobPhase::Progress);
    Ok(())
}
#[tokio::test]
async fn only_catalog_setup_has_time_for_discovery_plus_a_page_and_both_waiters_remain_bounded()
-> Result<()> {
    // Private ACK timing only. Synthetic watches/channel, no running worker,
    // network, protected lease renewal, or source/install capability.
    let (mut fixture, _mirror, _owner) = prepared().await?;
    let client = fixture.client.clone();
    let page = tokio::spawn(async move {
        client
            .prepare_catalog_page(PeerKey::try_from("1".repeat(64))?)
            .await
    });
    let Request::CatalogSetup(original) = timeout(Duration::from_secs(3), fixture.requests.recv())
        .await?
        .context("no original catalog request")?
    else {
        bail!("wrong catalog request");
    };
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(35)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        !page.is_finished(),
        "a valid discovery wait must not inherit the ordinary 30s RPC ceiling"
    );
    tokio::time::advance(Duration::from_secs(56)).await;
    assert!(page.await?.is_err());
    assert!(original.reply.is_closed());
    let client = fixture.client.clone();
    let ordinary = tokio::spawn(async move {
        client
            .request(PeerOperation::Inventory {
                after: None,
                limit: 1,
                known: vec![],
            })
            .await
    });
    let Request::Cached(_, reply) = fixture
        .requests
        .recv()
        .await
        .context("no original cached request")?
    else {
        bail!("wrong cached request");
    };
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(ordinary.await?.is_err());
    assert!(reply.is_closed());
    tokio::time::resume();
    Ok(())
}
fn next(
    fixture: &RetentionFixture,
    fork: bool,
) -> Result<(Arc<MetadataObservation>, impl Fn(bool) + use<>)> {
    let mut descriptor = fixture.inputs.metadata.descriptor().clone();
    if fork {
        descriptor.head.payload.timestamp -= 1;
    } else {
        descriptor.head.payload.sequence += 1;
    }
    descriptor.head = SignedRecord::sign(
        descriptor.head.payload.clone(),
        descriptor.head.authority_record_id,
        &SigningKey::from_bytes(&[2; 32]),
    )?;
    let (metadata, owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &fixture.policy)?;
    Ok((Arc::new(metadata), owner))
}
fn start(fixture: &RetentionFixture, metadata: Arc<MetadataObservation>) -> JoinHandle<Result<()>> {
    let client = fixture.client.clone();
    tokio::spawn(async move { client.persist_metadata_security(&metadata).await })
}
async fn pending(
    fixture: &mut RetentionFixture,
) -> Result<(Digest, oneshot::Sender<Result<WorkerControlResponse>>)> {
    match timeout(Duration::from_secs(5), fixture.requests.recv())
        .await?
        .context("no mirror update")?
    {
        Request::Security(
            WorkerControlOperation::CatalogSecurityApply { generation, batch },
            reply,
        ) => {
            assert_eq!(batch.revision, 2);
            assert_eq!(batch.publishers.len(), 1);
            assert_eq!(batch.extensions.len(), 1);
            batch.validate()?;
            Ok((generation, reply))
        }
        _ => panic!("a committed security change requires exactly its private mirror delta"),
    }
}
fn ack(reply: oneshot::Sender<Result<WorkerControlResponse>>, generation: Digest, ready: bool) {
    assert!(
        reply
            .send(Ok(WorkerControlResponse {
                schema_version: 1,
                request_id: 1,
                control_response: WorkerControlOutcome::CatalogSecurity {
                    generation,
                    revision: 2,
                    ready
                }
            }))
            .is_ok()
    );
}

#[tokio::test]
async fn committed_security_delta_waits_for_local_capacity_without_deadlocking_or_replaying()
-> Result<()> {
    let (mut fixture, mirror, _owner) = prepared().await?;
    let (metadata, _metadata_owner) = next(&fixture, false)?;
    let (fill, _held) = oneshot::channel();
    fixture
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill slot")?;
    let mut task = start(&fixture, metadata);
    assert!(timeout(Duration::from_millis(30), &mut task).await.is_err());
    assert!(!fixture.client.source_preflight.as_ref().unwrap().failed());
    assert!(matches!(
        fixture.requests.recv().await,
        Some(Request::Peers(_))
    ));
    let (generation, reply) = pending(&mut fixture).await?;
    // The actor's current-state check must defer, not wait on the revision
    // lock held by this same committed delta awaiting its acknowledgement.
    assert!(!mirror.current(&fixture.client.database).await?);
    ack(reply, generation, true);
    task.await??;
    assert_eq!(*mirror.revision.lock().await, 2);
    assert!(mirror.current(&fixture.client.database).await?);
    assert!(
        fixture.requests.try_recv().is_err(),
        "one committed delta only"
    );
    Ok(())
}

#[tokio::test]
async fn direct_reference_changes_wait_for_the_same_exact_mirror_ack_and_fail_stop_on_mismatch()
-> Result<()> {
    for correct in [true, false] {
        let (mut fixture, mirror, _owner) = prepared().await?;
        let (metadata, _metadata_owner) = next(&fixture, false)?;
        let descriptor = metadata.descriptor();
        let (publication, _publication_owner) =
            super::super::super::PublicationObservation::fixture_for_verified_publication(
                elixir_marketplace::publication::PreparedPublication {
                    schema_version: 1,
                    publisher: descriptor.publisher.clone(),
                    release: descriptor.release.clone(),
                    head: descriptor.head.clone(),
                },
            )?;
        let publication = Arc::new(publication);
        let client = fixture.client.clone();
        let task =
            tokio::spawn(async move { client.persist_publication_security(&publication).await });
        let (generation, reply) = pending(&mut fixture).await?;
        assert!(!task.is_finished());
        ack(
            reply,
            if correct {
                generation
            } else {
                Digest::of(b"wrong direct-reference generation")
            },
            true,
        );
        if correct {
            assert!(task.await??);
            assert_eq!(*mirror.revision.lock().await, 2);
            assert!(!fixture.client.source_preflight.as_ref().unwrap().failed());
        } else {
            assert!(task.await?.is_err());
            assert_eq!(*mirror.revision.lock().await, 1);
            assert!(fixture.client.source_preflight.as_ref().unwrap().failed());
        }
    }
    Ok(())
}

#[tokio::test]
async fn metadata_is_not_returned_until_the_exact_committed_delta_is_acknowledged() -> Result<()> {
    let (mut fixture, mirror, _owner) = prepared().await?;
    let (metadata, _metadata_owner) = next(&fixture, false)?;
    let task = start(&fixture, metadata.clone());
    let (generation, reply) = pending(&mut fixture).await?;
    assert!(!task.is_finished());
    assert!(
        crate::marketplace::security_state::require_current(
            &fixture.client.database,
            &fixture.inputs.metadata
        )
        .await
        .is_err()
    );
    crate::marketplace::security_state::require_current(&fixture.client.database, &metadata)
        .await?;
    ack(reply, generation, true);
    task.await??;
    assert_eq!(*mirror.revision.lock().await, 2);
    assert!(!fixture.client.source_preflight.as_ref().unwrap().failed());
    assert!(matches!(
        fixture.requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    Ok(())
}

#[tokio::test]
async fn wrong_or_unfinished_ack_and_cancellation_fail_stop_after_a_durable_commit() -> Result<()> {
    for failure in ["generation", "ready", "cancel"] {
        let (mut fixture, mirror, _owner) = prepared().await?;
        let (metadata, _metadata_owner) = next(&fixture, false)?;
        let task = start(&fixture, metadata);
        let (generation, reply) = pending(&mut fixture).await?;
        if failure == "cancel" {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(reply);
        } else {
            ack(
                reply,
                if failure == "generation" {
                    Digest::of(b"wrong generation")
                } else {
                    generation
                },
                failure != "ready",
            );
            assert!(task.await?.is_err());
        }
        assert!(
            fixture.client.source_preflight.as_ref().unwrap().failed(),
            "{failure}"
        );
        assert_eq!(
            fixture.client.snapshot().phase,
            ParticipationPhase::Failed,
            "{failure}"
        );
        assert_eq!(
            *mirror.revision.lock().await,
            1,
            "{failure}: no uncertain acknowledgement advances the mirror"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_committed_conflict_is_mirrored_even_when_its_candidate_is_rejected() -> Result<()> {
    let (mut fixture, mirror, _owner) = prepared().await?;
    let (metadata, _metadata_owner) = next(&fixture, true)?;
    let task = start(&fixture, metadata);
    let (generation, reply) = pending(&mut fixture).await?;
    ack(reply, generation, true);
    assert!(crate::marketplace::security_state::is_expected_rejection(
        &task.await?.unwrap_err()
    ));
    assert_eq!(*mirror.revision.lock().await, 2);
    assert!(!fixture.client.source_preflight.as_ref().unwrap().failed());
    Ok(())
}

#[tokio::test]
async fn unchanged_metadata_does_not_repeat_a_mirror_job_or_spend_another_revision() -> Result<()> {
    let (mut fixture, mirror, _owner) = prepared().await?;
    fixture
        .client
        .persist_metadata_security(&fixture.inputs.metadata)
        .await?;
    assert_eq!(*mirror.revision.lock().await, 1);
    assert!(matches!(
        fixture.requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    Ok(())
}

#[tokio::test]
async fn external_changes_require_replay_and_busy_local_commits_defer_checks_without_deadlocking()
-> Result<()> {
    let (fixture, mirror, _owner) = prepared().await?;
    assert!(mirror.current(&fixture.client.database).await?);
    {
        let _held = mirror.revision.lock().await;
        assert!(!mirror.current(&fixture.client.database).await?);
    }
    fixture.advance_head().await?;
    assert!(mirror.current(&fixture.client.database).await.is_err());
    assert!(
        fixture
            .client
            .persist_metadata_security(&fixture.inputs.metadata)
            .await
            .is_err()
    );
    assert!(fixture.client.source_preflight.as_ref().unwrap().failed());
    assert_eq!(*mirror.revision.lock().await, 1);
    Ok(())
}
