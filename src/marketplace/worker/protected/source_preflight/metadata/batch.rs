//! Finite cache warming, not a second verifier actor or an installation grant.
use super::*;
use elixir_marketplace::verifier::MAX_METADATA_BATCH;

const MAX_BATCH_REPLY: usize = MAX_METADATA_BATCH * MAX_REPLY;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Row {
    descriptor_id: Digest,
    outcome: Outcome,
}

fn parse_batch(
    bytes: &[u8],
    id: u64,
    descriptors: &[CandidateDescriptor],
) -> Result<Result<Vec<Option<SupportSummary>>>> {
    if !(1..=MAX_METADATA_BATCH).contains(&descriptors.len()) {
        bail!("metadata batch input bound");
    }
    let reply: Reply = canonical::decode(bytes, MAX_BATCH_REPLY)?;
    if reply.schema_version != SCHEMA_VERSION || reply.request_id != id {
        bail!("metadata batch response scope differs");
    }
    match reply.outcome {
        Outcome::MetadataBatch {
            results,
            metadata_only,
        } => {
            if !metadata_only
                || results.len() != descriptors.len()
                || !(1..=MAX_METADATA_BATCH).contains(&results.len())
            {
                bail!("metadata batch response bounds differ");
            }
            let mut checked = Vec::with_capacity(results.len());
            for (row, descriptor) in results.into_iter().zip(descriptors) {
                if row.descriptor_id != Digest::of(descriptor.to_bytes()?) {
                    bail!("metadata batch input binding differs");
                }
                checked.push(parse_outcome(row.outcome, descriptor)?.ok());
            }
            Ok(Ok(checked))
        }
        rejected @ Outcome::Rejected { .. } => {
            let error = parse_outcome(rejected, &descriptors[0])?
                .err()
                .context("metadata batch rejection required")?;
            Ok(Err(error))
        }
        _ => bail!("metadata batch outcome differs"),
    }
}

impl Client {
    /// Only warms exact positive native metadata facts. Does not persist
    /// publisher/head state, check source, count a candidate or approve a card.
    pub(in crate::marketplace::worker::protected) async fn warm_metadata(
        &self,
        descriptors: &[CandidateDescriptor],
    ) -> Result<()> {
        self.0.cache_admit()?;
        if descriptors.len() > MAX_METADATA_BATCH {
            bail!("metadata warming page bound");
        }
        // Existing images keep their single-descriptor path. Never send a new
        // command based on a peer/UI capability or inferred version string.
        if !self.0.binding.metadata_batch || descriptors.len() < 2 {
            return Ok(());
        }
        if !self.0.binding.metadata_verifier {
            bail!("independent metadata verification is unavailable");
        }
        let policy = IssuerPolicy::new(self.0.binding.issuer_keys.iter().copied())?.fingerprint();
        let mut selected = Vec::new();
        // Leave a fixed envelope margin within the SAME 256-KiB frame. A large
        // individual descriptor can still use the normal single-input path.
        let mut size = 1024usize;
        {
            let mut cache = self
                .0
                .metadata_cache
                .lock()
                .map_err(|_| anyhow::anyhow!("metadata receipt cache is unavailable"))?;
            for (index, descriptor) in descriptors.iter().enumerate() {
                let Ok(input) = CheckedDescriptor::new(descriptor) else {
                    continue;
                };
                if cache.get(&input, policy).is_some() {
                    if index == 0 {
                        #[cfg(test)]
                        eprintln!(
                            "{}",
                            serde_json::json!({"stage":"metadata_prefetch_first_cached", "rows":descriptors.len()})
                        );
                        return Ok(());
                    } // no O(page²) work on warm browse
                    continue;
                }
                if size + input.bytes().len() + 1 > MAX_CONTROL_FRAME_BYTES {
                    break;
                }
                size += input.bytes().len() + 1;
                selected.push(descriptor.clone());
            }
        }
        #[cfg(test)]
        eprintln!(
            "{}",
            serde_json::json!({"stage":"metadata_prefetch_selected", "requested":descriptors.len(), "selected":selected.len()})
        );
        if selected.len() < 2 {
            return Ok(());
        }
        Work::Foreground.admit_start(&self.0)?;
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("metadata/source slot is busy")?;
        let (id, _) = descriptor_request(&selected[0])?;
        let bytes = canonical::to_vec(&VerificationRequest::VerifyDescriptorBatch {
            schema_version: SCHEMA_VERSION,
            request_id: id,
            descriptors: selected.clone(),
        })?;
        if bytes.len() > MAX_CONTROL_FRAME_BYTES {
            bail!("metadata warming frame bound");
        }
        let owner = self.0.clone();
        let (mut completion, response) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = run(&owner, selected, id, bytes, policy, &mut completion).await;
            let _ = completion.send(result);
        });
        response.await.context("metadata warming owner stopped")?
    }
}

async fn run(
    owner: &Arc<Owner>,
    descriptors: Vec<CandidateDescriptor>,
    id: u64,
    bytes: Vec<u8>,
    policy: Digest,
    completion: &mut oneshot::Sender<Result<()>>,
) -> Result<()> {
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"metadata_prefetch_native_begin", "rows":descriptors.len()})
    );
    if completion.is_closed() {
        bail!("metadata warming cancelled before admission");
    }
    owner.headroom().await?;
    Work::Foreground
        .ready_before_launch(owner, completion)
        .await?;
    if completion.is_closed() {
        bail!("metadata warming cancelled before launch");
    }
    let observed = Instant::now();
    let observed_utc = observation_utc()?;
    let mut cleanup = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    let process = timeout(
        Duration::from_secs(10),
        owner.runtime.exec_marketplace_metadata_verifier(
            &owner.handle,
            &owner.profile.container_spec().labels,
            &owner.binding.issuer_keys,
        ),
    )
    .await
    .context("metadata warming startup deadline")??;
    let result = timeout(
        Duration::from_secs(20),
        exchange_bytes(
            process,
            &bytes,
            MAX_BATCH_REPLY,
            owner,
            completion,
            true,
            Work::Foreground,
        ),
    )
    .await
    .context("metadata warming retirement deadline")??;
    let result = match result {
        Ok(body) => parse_batch(&body, id, &descriptors)?,
        Err(error) => Err(error),
    };
    cleanup.armed = false;
    let results = result?;
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"metadata_prefetch_native_checked", "rows":results.len(), "positive":results.iter().filter(|row| row.is_some()).count()})
    );
    Work::Foreground.admit(owner, false)?;
    let mut integrity = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    timeout(
        Duration::from_secs(5),
        inspect_fixed_worker(owner.runtime.as_ref(), &owner.handle, &owner.profile),
    )
    .await
    .context("metadata warming ownership readback deadline")??;
    integrity.armed = false;
    Work::Foreground.admit(owner, false)?;
    // No partial cache writes before the entire report, original exit and
    // actual worker ownership pass. Invalid rows never become capabilities.
    let mut cache = owner
        .metadata_cache
        .lock()
        .map_err(|_| anyhow::anyhow!("metadata receipt cache is unavailable"))?;
    for (descriptor, support) in descriptors.into_iter().zip(results) {
        if let Some(support) = support {
            cache.insert(&MetadataObservation {
                descriptor,
                support,
                issuer_policy: policy,
                observed,
                observed_utc,
                protection: owner.protection.clone(),
            })?;
        }
    }
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"metadata_prefetch_cache_committed"})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reports_bind_every_original_row_and_reject_missing_extra_nested_or_forged_results()
    -> Result<()> {
        let (descriptor, _, policy) = super::super::super::evidence::tests::fixture()?;
        let mut invalid = descriptor.clone();
        invalid.head.payload.sequence += 1;
        let inputs = vec![descriptor.clone(), invalid];
        let mut verifier = elixir_marketplace::verifier::Verifier::new(policy)?;
        let request = VerificationRequest::VerifyDescriptorBatch {
            schema_version: 1,
            request_id: 7,
            descriptors: inputs.clone(),
        };
        let report = serde_json::to_value(
            verifier.process(&canonical::to_vec(&request)?, observation_utc()?),
        )?;
        let rows = parse_batch(&canonical::to_vec(&report)?, 7, &inputs)??;
        assert!(rows[0].is_some() && rows[1].is_none());
        for change in [
            "id", "missing", "extra", "binding", "nested", "scope", "score",
        ] {
            let mut value = report.clone();
            match change {
                "id" => value["request_id"] = json!(8),
                "missing" => {
                    value["outcome"]["results"].as_array_mut().unwrap().pop();
                }
                "extra" => value["outcome"]["results"]
                    .as_array_mut()
                    .unwrap()
                    .push(report["outcome"]["results"][0].clone()),
                "binding" => {
                    value["outcome"]["results"][0]["descriptor_id"] =
                        json!(Digest::of(b"other input"))
                }
                "nested" => value["outcome"]["results"][0]["outcome"] = report["outcome"].clone(),
                "scope" => value["outcome"]["metadata_only"] = json!(false),
                "score" => {
                    value["outcome"]["results"][0]["outcome"]["support"]["verified_supporters"] =
                        json!(4096)
                }
                _ => unreachable!(),
            }
            assert!(
                parse_batch(&canonical::to_vec(&value)?, 7, &inputs).is_err(),
                "{change}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn warming_keeps_the_original_single_slot_feature_and_playback_guards() -> Result<()> {
        let (descriptor, _, policy) = super::super::super::evidence::tests::fixture()?;
        let mut fixture = super::super::super::tests::fixture()?;
        let inputs = vec![descriptor.clone(); 2];
        let permit = fixture.owner.slots.clone().try_acquire_owned()?;
        assert!(
            Client(fixture.owner.clone())
                .warm_metadata(&inputs)
                .await
                .unwrap_err()
                .to_string()
                .contains("slot is busy")
        );
        Arc::get_mut(&mut fixture.owner)
            .context("unshared fixture")?
            .binding
            .metadata_batch = false;
        Client(fixture.owner.clone()).warm_metadata(&inputs).await?; // old image: no new command
        Arc::get_mut(&mut fixture.owner)
            .context("unshared fixture")?
            .binding
            .metadata_batch = true;
        assert!(
            Client(fixture.owner.clone())
                .warm_metadata(&vec![descriptor.clone(); 21])
                .await
                .is_err()
        );
        let (fact, _fact_owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
        fixture.owner.metadata_cache.lock().unwrap().insert(&fact)?;
        Client(fixture.owner.clone()).warm_metadata(&inputs).await?;
        let cached = Client(fixture.owner.clone())
            .verify_metadata(&descriptor)
            .await?;
        assert_eq!(cached.observed, fact.observed);
        assert_eq!(cached.observed_utc, fact.observed_utc);
        *fixture.owner.metadata_cache.lock().unwrap() = ReceiptCache::default();
        drop(permit);
        fixture.owner.playback.set_test_startup_queue_len(1);
        assert!(
            Client(fixture.owner.clone())
                .warm_metadata(&inputs)
                .await
                .unwrap_err()
                .to_string()
                .contains("playback startup")
        );
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));
        Ok(())
    }
}
