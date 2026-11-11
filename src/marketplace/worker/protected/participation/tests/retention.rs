//! Real signatures and durable SQLite fences with a synthetic private worker
//! channel. These tests do not establish protected native-process provenance.
use super::*;
use crate::marketplace::worker::protected::{
    EvidenceObservation, MetadataObservation, PublicSourceObservation, SearchSourceObservation,
};
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor,
    evidence::IssuerPolicy,
    identity::Digest,
    peer::{PeerOutcome, RejectionCode},
    records::SignedRecord,
    worker_control::WorkerControlOutcome,
};

pub(super) struct Inputs {
    pub(super) metadata: MetadataObservation,
    source: PublicSourceObservation,
    batch: EvidenceBatch,
    observation: EvidenceObservation,
}
pub(super) struct RetentionFixture {
    pub(super) client: ParticipationClient,
    pub(super) requests: mpsc::Receiver<Request>,
    pub(super) inputs: Arc<Inputs>,
    pub(super) policy: IssuerPolicy,
    source_owner: Box<dyn Fn(bool)>,
    _owners: Vec<Box<dyn Fn(bool)>>,
    _status: watch::Sender<ParticipationSnapshot>,
    _protection: watch::Sender<Snapshot>,
    _directory: tempfile::TempDir,
}
impl RetentionFixture {
    pub(super) async fn new() -> Result<Self> {
        let data: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        let mut descriptor =
            CandidateDescriptor::parse(&serde_json::to_vec(&serde_json::json!({
                "schema_version":1,"publisher":data["publisher"],"release":data["release"],
                "head":data["head"],"revocations":[],"gate_evidence":data["evidence"]["pairs"]
            }))?)?;
        let now = chrono::Utc::now().timestamp() as u64;
        for (index, pair) in descriptor.gate_evidence.iter_mut().enumerate() {
            pair.credential.payload.issued_at = now;
            pair.credential.payload.expires_at = now + 3600;
            pair.credential = SignedRecord::sign(
                pair.credential.payload.clone(),
                None,
                &SigningKey::from_bytes(&[3; 32]),
            )?;
            pair.endorsement.payload.credential_id = Digest::of(pair.credential.to_bytes()?);
            pair.endorsement = SignedRecord::sign(
                pair.endorsement.payload.clone(),
                None,
                &SigningKey::from_bytes(&[10 + index as u8; 32]),
            )?;
        }
        let policy = IssuerPolicy::new([data["issuer_key"].as_str().unwrap().parse()?])?;
        let (metadata, metadata_owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
        let release = Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?);
        let mut batch = EvidenceBatch {
            schema_version: 1,
            release_id: release,
            snapshot_id: Digest::of(b"upstream fixture"),
            pairs: metadata.descriptor().gate_evidence.clone(),
            next_cursor: Some("foreign-next".into()),
        };
        batch.pairs[0].credential.signature = "A".repeat(86);
        let (observation, observation_owner) =
            EvidenceObservation::fixture_for_batch(&metadata, &batch, &policy)?;
        let (source, source_owner) =
            PublicSourceObservation::fixture_for_source(release, Digest::of(b"fixture license"));
        let directory = tempfile::tempdir()?;
        let database = Database::connect(&crate::config::DatabaseConfig {
            url: format!(
                "sqlite://{}",
                directory.path().join("retention.sqlite").display()
            ),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        crate::marketplace::security_state::admit(&database, &metadata).await?;
        let (requests, receiver) = mpsc::channel(1);
        let snapshot = ParticipationSnapshot {
            phase: ParticipationPhase::Running,
            applied_roles: Some(NetworkRoles::default()),
            ..ParticipationSnapshot::default()
        };
        let (status, seen) = watch::channel(snapshot);
        let (protection, protected) = watch::channel(Snapshot::timed(
            ProtectionState::Ready,
            Duration::from_secs(3600),
        ));
        let (credits, _) = mpsc::channel(1);
        let (faults, _) = mpsc::channel(1);
        Ok(Self {
            client: ParticipationClient {
                blob_discovery: false,
                blob_sharing: false,
                database,
                transfer_requests: requests.clone(),
                local_reads: requests.clone(),
                requests,
                status: seen,
                protection: protected,
                credits,
                source_preflight: None,
                source_jobs: None,
                security_mirror: None,
                faults,
                retirement: retirement::channel().1,
            },
            requests: receiver,
            inputs: Arc::new(Inputs {
                metadata,
                source,
                batch,
                observation,
            }),
            policy,
            source_owner: Box::new(source_owner),
            _owners: vec![Box::new(metadata_owner), Box::new(observation_owner)],
            _status: status,
            _protection: protection,
            _directory: directory,
        })
    }
    fn start(&self) -> JoinHandle<Result<bool>> {
        let client = self.client.clone();
        let inputs = self.inputs.clone();
        tokio::spawn(async move {
            client
                .retain_evidence(
                    &inputs.metadata,
                    &inputs.source,
                    &inputs.batch,
                    &inputs.observation,
                )
                .await
        })
    }
    fn start_metadata(&self, search_only: bool) -> JoinHandle<Result<bool>> {
        let client = self.client.clone();
        let inputs = self.inputs.clone();
        tokio::spawn(async move {
            if search_only {
                let source = SearchSourceObservation::from_source(&inputs.source)?;
                client
                    .retain_admitted_metadata(&inputs.metadata, &source)
                    .await
            } else {
                client
                    .retain_admitted_metadata(&inputs.metadata, &inputs.source)
                    .await
            }
        })
    }
    async fn offered(&mut self) -> Result<oneshot::Sender<Result<PeerResponse>>> {
        match timeout(Duration::from_secs(5), self.requests.recv())
            .await?
            .context("no offer")?
        {
            Request::Cached(PeerOperation::Offer { descriptor }, reply) => {
                assert_eq!(
                    canonical::to_vec(&descriptor)?,
                    canonical::to_vec(self.inputs.metadata.descriptor())?
                );
                Ok(reply)
            }
            _ => panic!(
                "retention must first offer authenticated metadata, never alter network roles"
            ),
        }
    }
    fn reply_offer(&self, reply: oneshot::Sender<Result<PeerResponse>>, accept: bool) {
        let response = if accept {
            PeerOutcome::Offered {
                release_id: self.inputs.batch.release_id,
            }
        } else {
            PeerOutcome::Rejected {
                code: RejectionCode::Limit,
            }
        };
        assert!(
            reply
                .send(Ok(PeerResponse {
                    schema_version: 1,
                    request_id: Some(1),
                    response
                }))
                .is_ok()
        );
    }
    async fn merged(&mut self) -> Result<oneshot::Sender<Result<WorkerControlResponse>>> {
        match timeout(Duration::from_secs(5), self.requests.recv())
            .await?
            .context("no merge")?
        {
            Request::Merge(id, batch, reply) => {
                assert_eq!(
                    id,
                    self.inputs
                        .metadata
                        .descriptor()
                        .release
                        .payload
                        .extension_id
                );
                assert_eq!(batch.release_id, self.inputs.batch.release_id);
                assert!(batch.next_cursor.is_none());
                assert_eq!(batch.pairs.len(), 4);
                assert_eq!(
                    canonical::to_vec(&batch.pairs)?,
                    canonical::to_vec(&&self.inputs.batch.pairs[1..])?
                );
                Ok(reply)
            }
            _ => panic!("retention may only issue the bounded private proof merge"),
        }
    }
    fn reply_merge(reply: oneshot::Sender<Result<WorkerControlResponse>>) {
        // A zero receipt can mean exact proofs were already retained. It is
        // deliberately not treated as a verified supporter count.
        assert!(
            reply
                .send(Ok(WorkerControlResponse {
                    schema_version: 1,
                    request_id: 1,
                    control_response: WorkerControlOutcome::EvidenceMerged { accepted: 0 },
                }))
                .is_ok()
        );
    }
    fn no_more_requests(&mut self) {
        assert!(matches!(
            self.requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!self.client.snapshot().applied_roles.unwrap().catalog);
    }
    pub(super) async fn advance_head(&self) -> Result<()> {
        let mut newer = self.inputs.metadata.descriptor().clone();
        newer.head.payload.sequence += 1;
        newer.head = SignedRecord::sign(
            newer.head.payload.clone(),
            newer.head.authority_record_id,
            &SigningKey::from_bytes(&[2; 32]),
        )?;
        let (newer, _owner) =
            MetadataObservation::fixture_for_verified_descriptor(newer, &self.policy)?;
        crate::marketplace::security_state::admit(&self.client.database, &newer).await
    }
}

#[tokio::test]
async fn cached_catalog_veto_is_local_read_only_and_missing_is_not_a_remote_ban() -> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let (source, _source_owner) =
        super::super::super::source_preflight::Client::fixture_for_catalog_snapshot(false)?;
    fixture.client.source_preflight = Some(source);
    let d = fixture.inputs.metadata.descriptor();
    let state = elixir_marketplace::catalog::HeadResponse {
        schema_version: 1,
        publisher: d.publisher.clone(),
        head: Some(d.head.clone()),
        revocations: vec![],
        metadata_only: true,
    };
    let mut withdrawn = state.clone();
    withdrawn.head = None;
    for (response, allowed) in [
        (
            PeerOutcome::Head {
                state: Box::new(state),
            },
            true,
        ),
        (
            PeerOutcome::Head {
                state: Box::new(withdrawn),
            },
            false,
        ),
        (
            PeerOutcome::Rejected {
                code: RejectionCode::Invalid,
            },
            true,
        ),
        (
            PeerOutcome::Rejected {
                code: RejectionCode::Io,
            },
            false,
        ),
        (
            PeerOutcome::Rejected {
                code: RejectionCode::Stale,
            },
            false,
        ),
    ] {
        let client = fixture.client.clone();
        let inputs = fixture.inputs.clone();
        let pending =
            tokio::spawn(async move { client.check_catalog_current(&inputs.metadata).await });
        let Some(Request::Cached(PeerOperation::GetHead { extension_id }, reply)) =
            timeout(Duration::from_secs(5), fixture.requests.recv()).await?
        else {
            bail!("expected exactly one local head read")
        };
        assert_eq!(
            extension_id,
            fixture
                .inputs
                .metadata
                .descriptor()
                .release
                .payload
                .extension_id
        );
        reply
            .send(Ok(PeerResponse {
                schema_version: 1,
                request_id: Some(1),
                response,
            }))
            .map_err(|_| anyhow::anyhow!("head read retired"))?;
        assert_eq!(pending.await?.is_ok(), allowed);
        assert!(fixture.requests.try_recv().is_err());
        // A worker veto must not become a durable, unsigned revocation.
        fixture
            .client
            .check_metadata_current(&fixture.inputs.metadata)
            .await?;
    }
    Ok(())
}

fn batch_head_row(
    metadata: &MetadataObservation,
) -> elixir_marketplace::worker_control::CatalogHeadReadback {
    let d = metadata.descriptor();
    elixir_marketplace::worker_control::CatalogHeadReadback {
        extension_id: d.release.payload.extension_id.clone(),
        response: PeerResponse {
            schema_version: 1,
            request_id: Some(1),
            response: PeerOutcome::Head {
                state: Box::new(elixir_marketplace::catalog::HeadResponse {
                    schema_version: 1,
                    publisher: d.publisher.clone(),
                    head: Some(d.head.clone()),
                    revocations: vec![],
                    metadata_only: true,
                }),
            },
        },
    }
}

// Real metadata signatures/fences, synthetic source and private-command owners.
// The old native cache's stale reply is independently covered by the worker's
// catalog-restrictions tests. This boundary must repair, then still veto.
async fn remote_release_fixture() -> Result<(
    RetentionFixture,
    Arc<MetadataObservation>,
    Arc<PublicSourceObservation>,
    impl Fn(bool) + use<>,
    impl Fn(bool) + use<>,
)> {
    let mut fixture = RetentionFixture::new().await?;
    let mut descriptor = fixture.inputs.metadata.descriptor().clone();
    descriptor.release.payload.version = "2.0.0".into();
    descriptor.release = SignedRecord::sign(
        descriptor.release.payload.clone(),
        descriptor.release.authority_record_id,
        &SigningKey::from_bytes(&[2; 32]),
    )?;
    let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
    descriptor.head.payload.sequence += 1;
    descriptor.head.payload.stable_release_id = release;
    descriptor.head = SignedRecord::sign(
        descriptor.head.payload.clone(),
        descriptor.head.authority_record_id,
        &SigningKey::from_bytes(&[2; 32]),
    )?;
    for (index, pair) in descriptor.gate_evidence.iter_mut().enumerate() {
        pair.endorsement.payload.release_id = release;
        pair.endorsement = SignedRecord::sign(
            pair.endorsement.payload.clone(),
            None,
            &SigningKey::from_bytes(&[10 + index as u8; 32]),
        )?;
    }
    let (metadata, metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &fixture.policy)?;
    crate::marketplace::security_state::admit(&fixture.client.database, &metadata).await?;
    let (source, source_owner) =
        PublicSourceObservation::fixture_for_source(release, Digest::of(b"next source license"));
    let (preflight, preflight_owner) =
        super::super::super::source_preflight::Client::fixture_for_catalog_snapshot(false)?;
    fixture.client.source_preflight = Some(preflight);
    fixture._owners.push(Box::new(move |_| {
        let _ = &preflight_owner;
    }));
    Ok((
        fixture,
        Arc::new(metadata),
        Arc::new(source),
        metadata_owner,
        source_owner,
    ))
}
async fn remote_offer(
    fixture: &mut RetentionFixture,
    metadata: &MetadataObservation,
) -> Result<oneshot::Sender<Result<PeerResponse>>> {
    let Some(Request::Cached(PeerOperation::Offer { descriptor }, reply)) =
        timeout(Duration::from_secs(5), fixture.requests.recv()).await?
    else {
        bail!("source-admitted remote metadata must be retained before the veto");
    };
    assert_eq!(
        canonical::to_vec(&descriptor)?,
        canonical::to_vec(metadata.descriptor())?
    );
    Ok(reply)
}
fn acknowledge_remote_offer(
    reply: oneshot::Sender<Result<PeerResponse>>,
    metadata: &MetadataObservation,
) -> Result<()> {
    reply
        .send(Ok(PeerResponse {
            schema_version: 1,
            request_id: Some(1),
            response: PeerOutcome::Offered {
                release_id: Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?),
            },
        }))
        .map_err(|_| anyhow::anyhow!("original remote retention waiter lost"))
}

#[tokio::test]
async fn remote_update_retains_verified_source_metadata_before_the_same_mandatory_head_veto()
-> Result<()> {
    let (mut fixture, metadata, source, _metadata_owner, _source_owner) =
        remote_release_fixture().await?;
    // Reproduce the circular OLD order: the parent has adopted v2, but the
    // worker cannot serve a private head for its now-superseded v1 cache.
    let client = fixture.client.clone();
    let next = metadata.clone();
    let old_order = tokio::spawn(async move { client.check_catalog_current(&next).await });
    let Some(Request::Cached(PeerOperation::GetHead { .. }, reply)) = fixture.requests.recv().await
    else {
        bail!("expected the old pre-retention veto");
    };
    reply
        .send(Ok(PeerResponse {
            schema_version: 1,
            request_id: Some(1),
            response: PeerOutcome::Rejected {
                code: RejectionCode::Stale,
            },
        }))
        .ok();
    assert!(old_order.await?.is_err());
    let client = fixture.client.clone();
    let next = metadata.clone();
    let pending = tokio::spawn(async move {
        client
            .retain_current_search_metadata(&next, source.as_ref())
            .await
    });
    let offer = remote_offer(&mut fixture, &metadata).await?;
    acknowledge_remote_offer(offer, &metadata)?;
    let Some(Request::Cached(PeerOperation::GetHead { extension_id }, reply)) =
        fixture.requests.recv().await
    else {
        bail!("mandatory post-retention head veto was skipped");
    };
    assert_eq!(
        extension_id,
        metadata.descriptor().release.payload.extension_id
    );
    reply.send(Ok(batch_head_row(&metadata).response)).ok();
    pending.await??;
    fixture.client.check_metadata_current(&metadata).await?;
    fixture.no_more_requests();
    Ok(())
}

#[tokio::test]
async fn source_retention_never_bypasses_a_post_retention_withdrawal_conflict_or_unknown_head()
-> Result<()> {
    for case in ["withdrawal", "newer", "fork", "stale", "io"] {
        let (mut fixture, metadata, source, _metadata_owner, _source_owner) =
            remote_release_fixture().await?;
        let client = fixture.client.clone();
        let next = metadata.clone();
        let pending = tokio::spawn(async move {
            client
                .retain_current_search_metadata(&next, source.as_ref())
                .await
        });
        let offer = remote_offer(&mut fixture, &metadata).await?;
        acknowledge_remote_offer(offer, &metadata)?;
        let Some(Request::Cached(PeerOperation::GetHead { .. }, reply)) =
            fixture.requests.recv().await
        else {
            bail!("post-retention veto was skipped");
        };
        let mut response = batch_head_row(&metadata).response;
        match case {
            "stale" | "io" => {
                response.response = PeerOutcome::Rejected {
                    code: if case == "stale" {
                        RejectionCode::Stale
                    } else {
                        RejectionCode::Io
                    },
                }
            }
            _ => {
                let PeerOutcome::Head { state } = &mut response.response else {
                    unreachable!()
                };
                match case {
                    "withdrawal" => state.head = None,
                    "newer" => state.head.as_mut().unwrap().payload.sequence += 1,
                    "fork" => state.head.as_mut().unwrap().payload.timestamp -= 1,
                    _ => unreachable!(),
                }
            }
        }
        reply.send(Ok(response)).ok();
        assert!(
            pending.await?.is_err(),
            "{case}: retention is not authority"
        );
        fixture.no_more_requests();
    }
    Ok(())
}

#[tokio::test]
async fn remote_retention_rechecks_native_source_owner_and_durable_fences_without_late_dispatch()
-> Result<()> {
    for case in [
        "source_before",
        "source_after",
        "metadata_before",
        "owner_after",
        "head_after",
    ] {
        let (mut fixture, metadata, source, metadata_owner, source_owner) =
            remote_release_fixture().await?;
        if case == "source_before" {
            source_owner(false);
        }
        if case == "metadata_before" {
            metadata_owner(false);
        }
        let client = fixture.client.clone();
        let next = metadata.clone();
        let pending = tokio::spawn(async move {
            client
                .retain_current_search_metadata(&next, source.as_ref())
                .await
        });
        if !case.ends_with("before") {
            let offer = remote_offer(&mut fixture, &metadata).await?;
            match case {
                "source_after" => source_owner(false),
                "owner_after" => {
                    let mut failed = fixture._status.borrow().clone();
                    failed.phase = ParticipationPhase::Failed;
                    failed.applied_roles = None;
                    fixture._status.send_replace(failed);
                }
                "head_after" => {
                    let mut descriptor = metadata.descriptor().clone();
                    descriptor.head.payload.sequence += 1;
                    descriptor.head = SignedRecord::sign(
                        descriptor.head.payload.clone(),
                        descriptor.head.authority_record_id,
                        &SigningKey::from_bytes(&[2; 32]),
                    )?;
                    let (newer, _owner) = MetadataObservation::fixture_for_verified_descriptor(
                        descriptor,
                        &fixture.policy,
                    )?;
                    crate::marketplace::security_state::admit(&fixture.client.database, &newer)
                        .await?;
                }
                _ => unreachable!(),
            }
            acknowledge_remote_offer(offer, &metadata)?;
        }
        assert!(
            timeout(Duration::from_secs(5), pending).await??.is_err(),
            "{case}"
        );
        assert!(
            fixture.requests.try_recv().is_err(),
            "{case}: no late head/download/role operation"
        );
    }
    Ok(())
}

#[tokio::test]
async fn remote_retention_waits_for_one_slot_without_replaying_or_bypassing_the_final_veto()
-> Result<()> {
    for case in ["full", "rejected", "lost"] {
        let (mut fixture, metadata, source, _metadata_owner, _source_owner) =
            remote_release_fixture().await?;
        let mut filler = None;
        if case == "full" {
            let (send, held) = oneshot::channel();
            fixture
                .client
                .requests
                .try_send(Request::Peers(send))
                .ok()
                .context("fill local slot")?;
            filler = Some(held);
        }
        let client = fixture.client.clone();
        let next = metadata.clone();
        let mut pending = tokio::spawn(async move {
            client
                .retain_current_search_metadata(&next, source.as_ref())
                .await
        });
        if case == "full" {
            assert!(
                timeout(Duration::from_millis(30), &mut pending)
                    .await
                    .is_err()
            );
            assert!(matches!(
                fixture.requests.recv().await,
                Some(Request::Peers(_))
            ));
        }
        let offer = remote_offer(&mut fixture, &metadata).await?;
        if case == "lost" {
            drop(offer);
            assert!(pending.await?.is_err());
            fixture.no_more_requests();
            continue;
        } else if case == "full" {
            acknowledge_remote_offer(offer, &metadata)?;
        } else {
            offer
                .send(Ok(PeerResponse {
                    schema_version: 1,
                    request_id: Some(1),
                    response: PeerOutcome::Rejected {
                        code: RejectionCode::Limit,
                    },
                }))
                .ok();
        }
        let Some(Request::Cached(PeerOperation::GetHead { .. }, reply)) =
            timeout(Duration::from_secs(5), fixture.requests.recv()).await?
        else {
            bail!("capacity refusal must still require the same final head veto");
        };
        let response = if case == "full" {
            batch_head_row(&metadata).response
        } else {
            PeerResponse {
                schema_version: 1,
                request_id: Some(1),
                response: PeerOutcome::Rejected {
                    code: RejectionCode::Stale,
                },
            }
        };
        reply.send(Ok(response)).ok();
        assert_eq!(
            pending.await?.is_ok(),
            case == "full",
            "{case}: ordinary pressure waits, rejection stays rejected"
        );
        drop(filler);
        fixture.no_more_requests();
    }
    Ok(())
}

#[tokio::test]
async fn unsent_remote_retention_rechecks_facts_and_cancellation_before_dispatch() -> Result<()> {
    for case in ["source", "metadata", "owner", "cancel"] {
        let (mut fixture, metadata, source, metadata_owner, source_owner) =
            remote_release_fixture().await?;
        let (send, _held) = oneshot::channel();
        fixture
            .client
            .requests
            .try_send(Request::Peers(send))
            .ok()
            .context("fill local slot")?;
        let mut pending = Box::pin(
            fixture
                .client
                .retain_current_search_metadata(&metadata, source.as_ref()),
        );
        assert!(
            timeout(Duration::from_millis(30), &mut pending)
                .await
                .is_err()
        );
        match case {
            "source" => source_owner(false),
            "metadata" => metadata_owner(false),
            "owner" => {
                let mut failed = fixture._status.borrow().clone();
                failed.phase = ParticipationPhase::Failed;
                failed.applied_roles = None;
                fixture._status.send_replace(failed);
            }
            "cancel" => (),
            _ => unreachable!(),
        }
        if case == "cancel" {
            drop(pending);
            assert!(matches!(
                fixture.requests.recv().await,
                Some(Request::Peers(_))
            ));
        } else {
            assert!(matches!(
                fixture.requests.recv().await,
                Some(Request::Peers(_))
            ));
            assert!(pending.await.is_err(), "{case}");
        }
        tokio::task::yield_now().await;
        assert!(
            fixture.requests.try_recv().is_err(),
            "{case}: no late offer/head/RPC"
        );
    }
    Ok(())
}

#[tokio::test]
async fn selected_catalog_head_waits_for_local_capacity_without_replaying_or_mutating() -> Result<()>
{
    let mut fixture = RetentionFixture::new().await?;
    let (source, _source_owner) =
        super::super::super::source_preflight::Client::fixture_for_catalog_snapshot(false)?;
    fixture.client.source_preflight = Some(source);
    let (fill, _held) = oneshot::channel();
    fixture
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill slot")?;
    let client = fixture.client.clone();
    let inputs = fixture.inputs.clone();
    let mut pending =
        tokio::spawn(async move { client.check_catalog_current(&inputs.metadata).await });
    assert!(
        timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    assert!(matches!(
        fixture.requests.recv().await,
        Some(Request::Peers(_))
    ));
    let Some(Request::Cached(PeerOperation::GetHead { extension_id }, reply)) =
        timeout(Duration::from_secs(5), fixture.requests.recv()).await?
    else {
        bail!("expected the one selected head read");
    };
    let row = batch_head_row(&fixture.inputs.metadata);
    assert_eq!(extension_id, row.extension_id);
    reply.send(Ok(row.response)).ok();
    pending.await??;
    fixture.no_more_requests();
    fixture
        .client
        .check_metadata_current(&fixture.inputs.metadata)
        .await?;
    Ok(())
}

fn selected_remote_source(
    fixture: &mut RetentionFixture,
    metadata: &MetadataObservation,
    source: &PublicSourceObservation,
) -> Result<crate::marketplace::search::Selection> {
    let mut running = fixture._status.borrow().clone();
    running.applied_roles = Some(NetworkRoles {
        consumer: true,
        catalog: false,
        routing: false,
    });
    fixture._status.send_replace(running);
    crate::marketplace::search::Selection::fixture_for_native_preparation(
        fixture.client.clone(),
        metadata.retained_copy(),
        source.retained_copy(),
        vec![],
    )
}

#[tokio::test]
async fn selected_source_guard_allows_the_existing_control_wait_without_renewing_authority()
-> Result<()> {
    let (mut fixture, metadata, source, _metadata_owner, _source_owner) =
        remote_release_fixture().await?;
    let mut selection = selected_remote_source(&mut fixture, &metadata, &source)?;
    let pending = tokio::spawn(async move {
        let result = selection.current().await;
        (selection, result)
    });
    let Some(Request::Cached(PeerOperation::GetHead { extension_id }, reply)) =
        timeout(Duration::from_secs(5), fixture.requests.recv()).await?
    else {
        bail!("expected the SAME current-head check");
    };
    assert_eq!(
        extension_id,
        metadata.descriptor().release.payload.extension_id
    );
    // The durable read has finished: pause only while the actual control reply
    // is pending, never while SQLite work might still need real I/O scheduling.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(20)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        !pending.is_finished(),
        "an outer 10s guard must not cancel the original allowed 30s control wait"
    );
    tokio::time::resume();
    reply.send(Ok(batch_head_row(&metadata).response)).ok();
    let (selection, result) = pending.await?;
    result?;
    assert_eq!(selection.metadata()?.release_payload().version, "2.0.0");
    fixture.no_more_requests();
    Ok(())
}

#[tokio::test]
async fn selected_source_guard_keeps_withdrawal_owner_freshness_and_original_control_deadlines()
-> Result<()> {
    for case in ["withdrawal", "source", "metadata", "owner", "timeout"] {
        let (mut fixture, metadata, source, metadata_owner, source_owner) =
            remote_release_fixture().await?;
        let mut selection = selected_remote_source(&mut fixture, &metadata, &source)?;
        let pending = tokio::spawn(async move {
            let result = selection.current().await;
            (selection, result)
        });
        let Some(Request::Cached(PeerOperation::GetHead { .. }, reply)) =
            timeout(Duration::from_secs(5), fixture.requests.recv()).await?
        else {
            bail!("expected original current-head check");
        };
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(if case == "timeout" { 31 } else { 20 })).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        tokio::time::resume();
        if case == "timeout" {
            let (selection, result) = pending.await?;
            assert!(
                format!("{:#}", result.unwrap_err())
                    .contains("original request acknowledgement deadline")
            );
            assert!(selection.metadata().is_err());
            assert!(
                reply.send(Ok(batch_head_row(&metadata).response)).is_err(),
                "a late reply cannot repair failed authority"
            );
        } else {
            match case {
                "source" => source_owner(false),
                "metadata" => metadata_owner(false),
                "owner" => {
                    let mut failed = fixture._status.borrow().clone();
                    failed.phase = ParticipationPhase::Failed;
                    failed.applied_roles = None;
                    fixture._status.send_replace(failed);
                }
                _ => (),
            }
            let mut response = batch_head_row(&metadata).response;
            if case == "withdrawal" {
                let PeerOutcome::Head { state } = &mut response.response else {
                    unreachable!()
                };
                state.head = None;
            }
            reply.send(Ok(response)).ok();
            let (selection, result) = pending.await?;
            assert!(result.is_err(), "{case}");
            assert!(
                selection.metadata().is_err(),
                "{case}: failed checks cannot become a transfer/install grant"
            );
        }
        assert!(
            fixture.requests.try_recv().is_err(),
            "{case}: no replay, replacement or payload operation"
        );
    }
    Ok(())
}
fn second_batch_metadata(
    base: &MetadataObservation,
    policy: &IssuerPolicy,
) -> Result<(MetadataObservation, impl Fn(bool) + use<>)> {
    let mut d = base.descriptor().clone();
    let id = ExtensionId::new(&d.publisher.payload.root_key, "batch-second")?;
    d.release.payload.extension_id = id.clone();
    d.release = SignedRecord::sign(
        d.release.payload.clone(),
        d.release.authority_record_id,
        &SigningKey::from_bytes(&[2; 32]),
    )?;
    let release = Digest::of(canonical::to_vec(&d.release.payload)?);
    d.head.payload.extension_id = id.clone();
    d.head.payload.stable_release_id = release;
    d.head = SignedRecord::sign(
        d.head.payload.clone(),
        d.head.authority_record_id,
        &SigningKey::from_bytes(&[2; 32]),
    )?;
    for (index, pair) in d.gate_evidence.iter_mut().enumerate() {
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
    MetadataObservation::fixture_for_verified_descriptor(d, policy)
}

#[tokio::test]
async fn current_ranking_batch_uses_ordered_bounded_prefixes_and_keeps_exact_native_support()
-> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let (source, _owner) =
        super::super::super::source_preflight::Client::fixture_for_catalog_snapshot(true)?;
    fixture.client.source_preflight = Some(source);
    let (second, _second_owner) = second_batch_metadata(&fixture.inputs.metadata, &fixture.policy)?;
    crate::marketplace::security_state::admit(&fixture.client.database, &second).await?;
    let second = Arc::new(second);
    let values = vec![&fixture.inputs.metadata, second.as_ref()];
    let client = fixture.client.clone();
    let task_inputs = fixture.inputs.clone();
    let task_second = second.clone();
    let pending = tokio::spawn(async move {
        client
            .ranking_summaries_current(&[&task_inputs.metadata, task_second.as_ref()])
            .await
    });
    for n in 0..2 {
        let Some(Request::Heads(ids, reply)) =
            timeout(Duration::from_secs(5), fixture.requests.recv()).await?
        else {
            bail!("expected local head batch")
        };
        assert_eq!(
            ids,
            values[n..]
                .iter()
                .map(|m| m.descriptor().release.payload.extension_id.clone())
                .collect::<Vec<_>>()
        );
        reply
            .send(Ok(WorkerControlResponse {
                schema_version: 1,
                request_id: 1,
                control_response: WorkerControlOutcome::CatalogHeads {
                    rows: vec![batch_head_row(values[n])],
                },
            }))
            .map_err(|_| anyhow::anyhow!("readback cancelled"))?;
    }
    let result = pending.await??;
    assert_eq!(result.len(), 2);
    for (summary, value) in result.into_iter().zip(&values) {
        let summary = summary.context("current native summary")?;
        assert_eq!(
            summary.verified_supporters,
            value.support()?.verified_supporters
        );
        assert_eq!(summary.weighted_score, value.support()?.weighted_score);
    }
    assert!(fixture.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn head_batch_veto_missing_and_wrong_scope_never_grant_or_persist_security_authority()
-> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let (source, _owner) =
        super::super::super::source_preflight::Client::fixture_for_catalog_snapshot(true)?;
    fixture.client.source_preflight = Some(source);
    for mode in 0..3 {
        let inputs = fixture.inputs.clone();
        let client = fixture.client.clone();
        let pending =
            tokio::spawn(
                async move { client.ranking_summaries_current(&[&inputs.metadata]).await },
            );
        let Some(Request::Heads(_, reply)) =
            timeout(Duration::from_secs(5), fixture.requests.recv()).await?
        else {
            bail!("expected local batch")
        };
        let mut row = batch_head_row(&fixture.inputs.metadata);
        match mode {
            0 => {
                row.response.response = PeerOutcome::Rejected {
                    code: RejectionCode::Invalid,
                }
            }
            1 => {
                let PeerOutcome::Head { state } = &mut row.response.response else {
                    unreachable!()
                };
                state.head = None;
            }
            _ => {
                row.extension_id = ExtensionId::new(
                    &fixture
                        .inputs
                        .metadata
                        .descriptor()
                        .publisher
                        .payload
                        .root_key,
                    "wrong-scope",
                )?
            }
        }
        reply
            .send(Ok(WorkerControlResponse {
                schema_version: 1,
                request_id: 1,
                control_response: WorkerControlOutcome::CatalogHeads { rows: vec![row] },
            }))
            .map_err(|_| anyhow::anyhow!("readback cancelled"))?;
        let result = pending.await?;
        if mode == 2 {
            assert!(result.is_err());
        } else {
            assert_eq!(result?[0].is_some(), mode == 0);
        }
        fixture
            .client
            .check_metadata_current(&fixture.inputs.metadata)
            .await?;
    }
    fixture.advance_head().await?;
    assert!(
        fixture
            .client
            .ranking_summaries_current(&[&fixture.inputs.metadata])
            .await?[0]
            .is_none()
    );
    assert!(
        fixture.requests.try_recv().is_err(),
        "known denials cannot allocate a private readback"
    );
    Ok(())
}

#[tokio::test]
async fn an_older_worker_keeps_the_existing_single_head_protocol_for_current_summaries()
-> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let (source, _owner) =
        super::super::super::source_preflight::Client::fixture_for_catalog_snapshot(false)?;
    fixture.client.source_preflight = Some(source);
    let inputs = fixture.inputs.clone();
    let client = fixture.client.clone();
    let pending =
        tokio::spawn(async move { client.ranking_summaries_current(&[&inputs.metadata]).await });
    let Some(Request::Cached(PeerOperation::GetHead { extension_id }, reply)) =
        timeout(Duration::from_secs(5), fixture.requests.recv()).await?
    else {
        bail!("older worker must use the single protocol")
    };
    assert_eq!(
        extension_id,
        fixture
            .inputs
            .metadata
            .descriptor()
            .release
            .payload
            .extension_id
    );
    reply
        .send(Ok(batch_head_row(&fixture.inputs.metadata).response))
        .map_err(|_| anyhow::anyhow!("readback cancelled"))?;
    assert!(pending.await??[0].is_some());
    assert!(fixture.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn a_new_durable_head_during_batch_readback_invalidates_the_old_summary() -> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let (source, _owner) =
        super::super::super::source_preflight::Client::fixture_for_catalog_snapshot(true)?;
    fixture.client.source_preflight = Some(source);
    let inputs = fixture.inputs.clone();
    let client = fixture.client.clone();
    let pending =
        tokio::spawn(async move { client.ranking_summaries_current(&[&inputs.metadata]).await });
    let Some(Request::Heads(_, reply)) =
        timeout(Duration::from_secs(5), fixture.requests.recv()).await?
    else {
        bail!("expected local batch")
    };
    fixture.advance_head().await?;
    reply
        .send(Ok(WorkerControlResponse {
            schema_version: 1,
            request_id: 1,
            control_response: WorkerControlOutcome::CatalogHeads {
                rows: vec![batch_head_row(&fixture.inputs.metadata)],
            },
        }))
        .map_err(|_| anyhow::anyhow!("readback cancelled"))?;
    assert!(pending.await??[0].is_none());
    assert!(
        fixture
            .client
            .check_metadata_current(&fixture.inputs.metadata)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn cached_progress_ages_and_protection_loss_removes_it_without_new_rpc() -> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    // SQLite opens on a real worker thread. Pause only AFTER its async setup,
    // otherwise auto-advancing test time can race the connection deadline.
    tokio::time::pause();
    let mut tracker = catalog_progress::Tracker::default();
    let mut job = CatalogJobState {
        phase: CatalogJobPhase::Running,
        kind: Some(CatalogJobKind::Population),
        peer: Some(PeerKey::try_from("1".repeat(64))?),
        admitted: 0,
        unchanged: 0,
        rejected: 0,
        indexed: 0,
        removed: 0,
    };
    tracker.started(&job);
    job.phase = CatalogJobPhase::Complete;
    tracker.observed(&job);
    let mut snapshot = fixture._status.borrow().clone();
    snapshot.catalog = Some(tracker.view(true, true));
    fixture._status.send_replace(snapshot);
    assert_eq!(
        fixture
            .client
            .snapshot()
            .catalog
            .unwrap()
            .last_peer_scan_seconds,
        Some(0)
    );
    tokio::time::advance(Duration::from_secs(31)).await;
    let value = fixture.client.snapshot().catalog.unwrap();
    assert_eq!(value.phase, catalog_progress::Phase::Unavailable);
    assert_eq!(value.last_peer_scan_seconds, Some(31));
    fixture
        ._protection
        .send_replace(Snapshot::inactive(ProtectionState::Failed));
    assert!(fixture.client.snapshot().catalog.is_none());
    assert!(fixture.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn checked_metadata_is_retained_without_optional_evidence_or_role_promotion() -> Result<()> {
    for search_only in [false, true] {
        let mut fixture = RetentionFixture::new().await?;
        let task = fixture.start_metadata(search_only);
        let offer = fixture.offered().await?;
        fixture.reply_offer(offer, true);
        assert!(task.await??);
        // Exactly one Offer, no proof merge, listener, role or package grant.
        fixture.no_more_requests();
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn peer_and_refresh_acknowledgements_have_one_fixed_health_plus_queue_deadline() -> Result<()>
{
    for peer_request in [false, true] {
        // This tests queue deadlines, not SQL. A paused Tokio clock can expire
        // a real SQLite connection before its blocking I/O thread is scheduled.
        // Reuse the existing lazy, synthetic owner and a private one-slot queue;
        // enqueue never touches that owner's database or a native process.
        let mut owner = super::blob::owner();
        let (send, mut requests) = mpsc::channel(1);
        owner.client.requests = send;
        let client = owner.client.clone();
        let task = tokio::spawn(async move {
            if peer_request {
                client
                    .request_peer(PeerKey::try_from("1".repeat(64))?, query())
                    .await
                    .map(|_| ())
            } else {
                client.refresh_network().await.map(|_| ())
            }
        });
        // Hold the actual original request as if its pre-dispatch check stayed
        // pending. No actor, native process, network operation or second send.
        let original = requests.recv().await.context("queued request")?;
        step(31).await;
        assert!(
            !task.is_finished(),
            "the old 30s acknowledgement must not preempt health admission"
        );
        step(70).await;
        assert!(!task.is_finished());
        step(1).await;
        assert!(format!("{:#}", task.await?.unwrap_err()).contains("acknowledgement deadline"));
        match original {
            Request::Peer(_, _, reply) => assert!(peer_request && reply.is_closed()),
            Request::Refresh(reply) => assert!(!peer_request && reply.is_closed()),
            _ => bail!("unexpected queued request"),
        }
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!owner.client.snapshot().applied_roles.unwrap().catalog);
    }
    Ok(())
}

#[tokio::test]
async fn only_an_actual_full_local_queue_has_busy_provenance() -> Result<()> {
    let mut owner = super::blob::owner();
    let (send, mut requests) = mpsc::channel(1);
    owner.client.requests = send;
    let (reply, _original) = oneshot::channel();
    assert!(
        owner
            .client
            .requests
            .try_send(Request::Refresh(reply))
            .is_ok()
    );
    let error = owner.client.refresh_network().await.unwrap_err();
    assert!(ParticipationClient::queue_busy(&error));
    assert_eq!(
        requests.len(),
        1,
        "no second operation entered the private queue"
    );
    assert!(!ParticipationClient::queue_busy(&anyhow::anyhow!(
        error.to_string()
    )));
    assert!(!ParticipationClient::queue_busy(&anyhow::anyhow!(
        "private acknowledgement outcome unknown"
    )));
    requests.close();
    let error = owner.client.refresh_network().await.unwrap_err();
    assert!(
        !ParticipationClient::queue_busy(&error),
        "a closed owner is not safe retry provenance"
    );
    Ok(())
}

#[tokio::test]
async fn local_queue_pressure_can_leave_checked_metadata_uncached_without_extra_work() -> Result<()>
{
    let mut fixture = RetentionFixture::new().await?;
    let (reply, _original) = oneshot::channel();
    assert!(
        fixture
            .client
            .requests
            .try_send(Request::Refresh(reply))
            .is_ok()
    );
    assert!(!fixture.start_metadata(true).await??);
    assert!(matches!(
        fixture.requests.try_recv(),
        Ok(Request::Refresh(_))
    ));
    fixture.no_more_requests();
    Ok(())
}

#[tokio::test]
async fn metadata_cache_transport_failure_is_not_disguised_as_local_capacity() -> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let task = fixture.start_metadata(true);
    let reply = fixture.offered().await?;
    assert!(
        reply
            .send(Err(anyhow::anyhow!("original unconfirmed cache transport")))
            .is_ok()
    );
    let error = task.await?.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("original unconfirmed cache transport")
    );
    assert!(!ParticipationClient::queue_busy(&error));
    fixture.no_more_requests();
    Ok(())
}

#[tokio::test]
async fn metadata_cache_refusal_is_not_an_admission_or_an_extra_attempt() -> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let task = fixture.start_metadata(true);
    let offer = fixture.offered().await?;
    fixture.reply_offer(offer, false);
    assert!(!task.await??);
    fixture.no_more_requests();
    Ok(())
}

#[tokio::test]
async fn metadata_retention_rechecks_source_owner_and_durable_head_after_the_write() -> Result<()> {
    for head_changed in [false, true] {
        let mut fixture = RetentionFixture::new().await?;
        let task = fixture.start_metadata(true);
        let offer = fixture.offered().await?;
        if head_changed {
            fixture.advance_head().await?;
        } else {
            (fixture.source_owner)(false);
        }
        fixture.reply_offer(offer, true);
        let result = task.await?;
        if head_changed {
            assert!(crate::marketplace::security_state::is_expected_rejection(
                &result.unwrap_err()
            ));
        } else {
            assert!(!result?);
        }
        fixture.no_more_requests();
    }
    Ok(())
}

#[tokio::test]
async fn wrong_scope_or_lost_source_cannot_enqueue_metadata_retention() -> Result<()> {
    for wrong_scope in [false, true] {
        let mut fixture = RetentionFixture::new().await?;
        if wrong_scope {
            let (other, owner) = PublicSourceObservation::fixture_for_source(
                Digest::of(b"another release"),
                Digest::of(b"fixture license"),
            );
            Arc::get_mut(&mut fixture.inputs).unwrap().source = other;
            fixture._owners.push(Box::new(owner));
        } else {
            (fixture.source_owner)(false);
        }
        assert!(fixture.start_metadata(true).await?.is_err());
        fixture.no_more_requests();
    }
    Ok(())
}

#[tokio::test]
async fn authenticated_retention_uses_existing_commands_without_promoting_consumer_roles()
-> Result<()> {
    let mut fixture = RetentionFixture::new().await?;
    let task = fixture.start();
    let offer = fixture.offered().await?;
    fixture.reply_offer(offer, true);
    RetentionFixture::reply_merge(fixture.merged().await?);
    assert!(task.await??);
    fixture.no_more_requests();
    Ok(())
}

#[tokio::test]
async fn rejected_offer_or_expired_source_cannot_proceed_to_proof_merge() -> Result<()> {
    for source_lost in [false, true] {
        let mut fixture = RetentionFixture::new().await?;
        let task = fixture.start();
        let offer = fixture.offered().await?;
        if source_lost {
            (fixture.source_owner)(false);
        }
        fixture.reply_offer(offer, source_lost);
        assert!(!task.await??);
        fixture.no_more_requests();
    }
    Ok(())
}

#[tokio::test]
async fn durable_head_changes_are_rechecked_after_each_cache_write() -> Result<()> {
    for after_merge in [false, true] {
        let mut fixture = RetentionFixture::new().await?;
        let task = fixture.start();
        let offer = fixture.offered().await?;
        let merge = if after_merge {
            fixture.reply_offer(offer, true);
            Some(fixture.merged().await?)
        } else {
            fixture.advance_head().await?;
            fixture.reply_offer(offer, true);
            None
        };
        if let Some(merge) = merge {
            fixture.advance_head().await?;
            RetentionFixture::reply_merge(merge);
        }
        assert!(crate::marketplace::security_state::is_expected_rejection(
            &task.await?.unwrap_err()
        ));
        fixture.no_more_requests();
    }
    Ok(())
}

#[tokio::test]
async fn unavailable_source_or_empty_authenticated_batch_issues_no_cache_commands() -> Result<()> {
    for empty in [false, true] {
        let mut fixture = RetentionFixture::new().await?;
        if empty {
            let inputs = Arc::get_mut(&mut fixture.inputs).unwrap();
            for pair in &mut inputs.batch.pairs {
                pair.credential.signature = "A".repeat(86);
            }
            let (observation, owner) = EvidenceObservation::fixture_for_batch(
                &inputs.metadata,
                &inputs.batch,
                &fixture.policy,
            )?;
            inputs.observation = observation;
            fixture._owners.push(Box::new(owner));
            assert!(!fixture.start().await??);
        } else {
            (fixture.source_owner)(false);
            assert!(fixture.start().await?.is_err());
        }
        fixture.no_more_requests();
    }
    Ok(())
}
