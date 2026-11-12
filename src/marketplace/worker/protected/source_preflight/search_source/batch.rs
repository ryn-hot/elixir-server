//! Search-only positive cache warming. Same owned slot/native UID/watchdog,
//! <=20 authenticated recipes, <=256KiB frames, no install-fact cache writes.
use super::*;
use elixir_marketplace::{canonical, records::RepositoryKind, source_preflight::SourceAccess};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MAX_BATCH: usize = 20;
const MAX_BYTES: usize = 256 * 1024;
#[cfg(test)]
mod native;
struct Input {
    metadata: MetadataObservation,
    location: usize,
    request: SourcePreflightRequest,
}
#[derive(Serialize)]
struct Request<'a> {
    schema_version: u32,
    nonce: Digest,
    requests: Vec<&'a SourcePreflightRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostics: Option<bool>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    schema_version: u32,
    nonce: Digest,
    results: Vec<Row>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Row {
    nonce: Digest,
    plan_id: Digest,
    report: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<SourceFailure>,
}

// Availability diagnostics only. No URL, address, status text or exception can
// cross this enum, and no category grants a source observation or cache write.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SourceFailure {
    Dns,
    HttpsRequest,
    HttpStatus,
    Tls,
    Destination,
    Encoding,
    ContentType,
    Size,
    Body,
    SourceText,
    ManifestDigest,
    Deadline,
    FrameLimit,
    Unavailable,
}

fn parse(
    bytes: &[u8],
    nonce: Digest,
    inputs: &[Input],
    owner: &Owner,
) -> Result<Vec<Option<Digest>>> {
    let reply: Reply = canonical::decode(bytes, MAX_BYTES)?;
    if reply.schema_version != 1
        || reply.nonce != nonce
        || reply.results.len() != inputs.len()
        || !(2..=MAX_BATCH).contains(&inputs.len())
    {
        bail!("source warming reply scope differs");
    }
    let mut checked = Vec::with_capacity(inputs.len());
    #[cfg(test)]
    let mut diagnostics = Vec::with_capacity(inputs.len());
    for (row, input) in reply.results.into_iter().zip(inputs) {
        #[cfg(test)]
        let index = checked.len();
        if row.nonce != input.request.nonce || row.plan_id != input.request.plan_id()? {
            bail!("source warming row binding differs");
        }
        if (row.report.is_some() && row.failure.is_some())
            || (!owner.binding.source_batch_diagnostics && row.failure.is_some())
            || (owner.binding.source_batch_diagnostics
                && row.report.is_none()
                && row.failure.is_none())
        {
            bail!("source warming availability diagnostic differs");
        }
        #[cfg(test)]
        let provided = row.report.is_some();
        #[cfg(test)]
        let elapsed = row
            .report
            .as_ref()
            .and_then(|r| r.get("elapsed_millis"))
            .and_then(|n| n.as_u64())
            .filter(|n| *n <= 30_000);
        #[cfg(test)]
        let mut rejection = None;
        let license = if let Some(report) = row.report {
            let parsed = SourcePreflightReply::parse_with_policy(
                &canonical::to_vec(&report)?,
                &input.request,
                &input.metadata.descriptor().release.payload,
                input.location,
                &owner.binding.context(&owner.profile),
            )?;
            #[cfg(test)]
            {
                rejection = parsed.as_ref().err().map(|error| error.code());
            }
            parsed
                .ok()
                .map(|report| report.license_sha256())
                .transpose()?
        } else {
            None
        };
        #[cfg(test)]
        diagnostics.push(serde_json::json!({"row":index,"provided":provided,"accepted":license.is_some(),"elapsed_ms":elapsed,"policy_rejection":rejection,"failure":row.failure}));
        checked.push(license);
    }
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_rows","rows":diagnostics})
    );
    Ok(checked)
}

fn select(owner: &Owner, metadata: Vec<MetadataObservation>) -> Result<Vec<Input>> {
    if metadata.len() > MAX_BATCH {
        bail!("source warming page bound");
    }
    let policy =
        elixir_marketplace::evidence::IssuerPolicy::new(owner.binding.issuer_keys.iter().copied())?
            .fingerprint();
    let mut inputs = Vec::new();
    let mut extensions = BTreeSet::new();
    let mut cache = owner
        .search_source_cache
        .lock()
        .map_err(|_| anyhow::anyhow!("search source cache unavailable"))?;
    for metadata in metadata {
        if !metadata.is_current() || metadata.issuer_policy() != policy {
            continue;
        }
        let release = &metadata.descriptor().release.payload;
        let key = Digest::of(release.extension_id.as_str().as_bytes());
        let id = Digest::of(canonical::to_vec(release)?);
        if !extensions.insert(key) || cache.get(key, id).is_some() {
            continue;
        }
        for (location, source) in release.source.locations.iter().enumerate() {
            // This helper only warms HTTPS forge rows. Radicle is verified
            // separately; a missing seed must not abort another row or mirror.
            if matches!(source.kind, RepositoryKind::Radicle) {
                continue;
            }
            let request = SourcePreflightRequest::for_release(
                release,
                location,
                Digest::of(uuid::Uuid::new_v4().as_bytes()),
                owner.binding.family,
            )?;
            if matches!(request.plan.access, SourceAccess::Https { .. }) {
                inputs.push(Input {
                    metadata,
                    location,
                    request,
                });
                break;
            }
        }
    }
    Ok(inputs)
}
impl Client {
    pub(in crate::marketplace::worker::protected) async fn warm_search_sources(
        &self,
        metadata: Vec<MetadataObservation>,
    ) -> Result<()> {
        self.0.cache_admit()?;
        #[cfg(test)]
        eprintln!(
            "{}",
            serde_json::json!({"stage":"source_batch_requested", "supported":self.0.binding.source_batch, "metadata":metadata.len()})
        );
        if !self.0.binding.source_batch {
            return Ok(());
        }
        let inputs = select(&self.0, metadata)?;
        #[cfg(test)]
        eprintln!(
            "{}",
            serde_json::json!({"stage":"source_batch_selected", "rows":inputs.len()})
        );
        if inputs.len() < 2 {
            return Ok(());
        }
        Work::Foreground.admit_start(&self.0)?;
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("source warming slot busy")?;
        let nonce = Digest::of(uuid::Uuid::new_v4().as_bytes());
        let bytes = canonical::to_vec(&Request {
            schema_version: 1,
            nonce,
            requests: inputs.iter().map(|input| &input.request).collect(),
            diagnostics: self.0.binding.source_batch_diagnostics.then_some(true),
        })?;
        if bytes.len() > MAX_BYTES {
            bail!("source warming frame bound");
        }
        let owner = self.0.clone();
        let (mut completion, response) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = run(&owner, inputs, nonce, bytes, &mut completion).await;
            let _ = completion.send(result);
        });
        response.await.context("source warming owner stopped")?
    }
}
async fn run(
    owner: &Arc<Owner>,
    inputs: Vec<Input>,
    nonce: Digest,
    bytes: Vec<u8>,
    completion: &mut oneshot::Sender<Result<()>>,
) -> Result<()> {
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_native_begin", "rows":inputs.len(), "input_bytes":bytes.len()})
    );
    if completion.is_closed() {
        bail!("source warming cancelled before admission");
    }
    owner.headroom().await?;
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_headroom_checked"})
    );
    Work::Foreground
        .ready_before_launch(owner, completion)
        .await?;
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_protection_ready"})
    );
    if completion.is_closed() {
        bail!("source warming cancelled before launch");
    }
    // Earliest observation, never grant an extra TTL for batching/slow peers.
    let observed = Instant::now();
    let observed_utc = observation_utc()?;
    let mut cleanup = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    let process = timeout(
        Duration::from_secs(10),
        owner.runtime.exec_marketplace_source_preflight(
            &owner.handle,
            &owner.profile.container_spec().labels,
        ),
    )
    .await
    .context("source warming startup deadline")??;
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_helper_started"})
    );
    let result = timeout(
        RETIRE_DEADLINE,
        exchange_bytes(
            process,
            &bytes,
            MAX_BYTES,
            owner,
            completion,
            false,
            Work::Foreground,
        ),
    )
    .await
    .context("source warming retirement deadline")??;
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_helper_retired", "controlled_reply":result.is_ok()})
    );
    let results = match result {
        Ok(body) => parse(&body, nonce, &inputs, owner)?,
        Err(error) => {
            cleanup.armed = false;
            return Err(error);
        }
    };
    cleanup.armed = false;
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_native_checked", "rows":results.len(), "positive":results.iter().filter(|row| row.is_some()).count()})
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
    .context("source warming ownership readback deadline")??;
    integrity.armed = false;
    Work::Foreground.admit(owner, false)?;
    if completion.is_closed() {
        bail!("source warming caller stopped");
    }
    let mut cache = owner
        .search_source_cache
        .lock()
        .map_err(|_| anyhow::anyhow!("search source cache unavailable"))?;
    for (input, license) in inputs.into_iter().zip(results) {
        if let Some(license_sha256) = license.filter(|_| input.metadata.is_current()) {
            cache.insert(
                Digest::of(
                    input
                        .metadata
                        .descriptor()
                        .release
                        .payload
                        .extension_id
                        .as_str()
                        .as_bytes(),
                ),
                &SearchSourceObservation {
                    release_id: input.request.plan.release_id,
                    location_id: input.request.plan.location_id,
                    license_sha256,
                    observed,
                    observed_utc,
                    protection: owner.protection.clone(),
                },
            );
        }
    }
    #[cfg(test)]
    eprintln!(
        "{}",
        serde_json::json!({"stage":"source_batch_cache_committed"})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use ed25519_dalek::SigningKey;
    use elixir_marketplace::{identity::ExtensionId, records::SignedRecord};
    use serde_json::{Value, json};

    // Real signatures, synthetic protection/process reports; no billing or
    // public repository claim. The native image/actual WAN are separate gates.
    fn metadata(
        index: usize,
        bad_manifest: bool,
    ) -> Result<(MetadataObservation, Box<dyn Fn(bool)>, Vec<u8>)> {
        metadata_with(index, bad_manifest, |_| {})
    }

    fn metadata_with(
        index: usize,
        bad_manifest: bool,
        edit: impl FnOnce(&mut Release),
    ) -> Result<(MetadataObservation, Box<dyn Fn(bool)>, Vec<u8>)> {
        let (mut d, _, policy) = super::super::super::evidence::tests::fixture()?;
        let id = ExtensionId::new(
            &d.publisher.payload.root_key,
            &format!("source-batch-{index}"),
        )?;
        d.release.payload.extension_id = id.clone();
        let manifest = canonical::to_vec(
            &json!({"id":if bad_manifest{"wrong".to_owned()}else{id.to_string()},"name":d.release.payload.display_name,"kind":d.release.payload.kind,"version":d.release.payload.version}),
        )?;
        d.release.payload.manifest_sha256 = Digest::of(&manifest);
        edit(&mut d.release.payload);
        let key = SigningKey::from_bytes(&[2; 32]);
        d.release = SignedRecord::sign(
            d.release.payload.clone(),
            d.release.authority_record_id,
            &key,
        )?;
        let release = Digest::of(canonical::to_vec(&d.release.payload)?);
        d.head.payload.extension_id = id.clone();
        d.head.payload.stable_release_id = release;
        d.head = SignedRecord::sign(d.head.payload.clone(), d.head.authority_record_id, &key)?;
        for (i, pair) in d.gate_evidence.iter_mut().enumerate() {
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
                &SigningKey::from_bytes(&[10 + i as u8; 32]),
            )?;
        }
        let (metadata, owner) = MetadataObservation::fixture_for_verified_descriptor(d, &policy)?;
        Ok((metadata, Box::new(owner), manifest))
    }

    #[tokio::test]
    async fn a_seedless_radicle_hint_does_not_block_forge_prefetch() -> Result<()> {
        use elixir_marketplace::records::SourceLocation;

        let fixture = super::super::super::tests::fixture()?;
        let radicle = SourceLocation {
            kind: RepositoryKind::Radicle,
            repository_identity: "rad:z3gqcJUoA1n9HaHKufZs5FCSGazv5".into(),
            url: None,
        };
        let (only, _only, _) =
            metadata_with(0, false, |r| r.source.locations = vec![radicle.clone()])?;
        let (mixed, _mixed, _) =
            metadata_with(1, false, |r| r.source.locations.insert(0, radicle))?;
        let (forge, _forge, _) = metadata(2, false)?;
        let rows = select(&fixture.owner, vec![only, mixed, forge])?;
        assert_eq!(
            rows.iter().map(|row| row.location).collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert!(
            rows.iter()
                .all(|row| matches!(row.request.plan.access, SourceAccess::Https { .. }))
        );
        Ok(())
    }
    fn row(input: &Input, manifest: &[u8]) -> Result<Value> {
        let SourceAccess::Https {
            manifest: m,
            license: l,
        } = &input.request.plan.access
        else {
            unreachable!()
        };
        let text = |url: &str, body: &[u8]| json!({"url":url,"resolved_addresses":["104.16.132.229"],"connected_address":"104.16.132.229","source_address":"10.66.0.2","body_base64":STANDARD.encode(body)});
        Ok(
            json!({"nonce":input.request.nonce,"plan_id":input.request.plan_id()?,"report":{
            "schema_version":1,"nonce":input.request.nonce,"plan_id":input.request.plan_id()?,"elapsed_millis":10,
            "kernel":super::super::super::tests::kernel_fixture()?,
            "manifest":text(&m.url,manifest),"license":text(&l.url,b"SPDX-License-Identifier: MIT\n")}}),
        )
    }
    #[tokio::test]
    async fn every_batch_row_retains_exact_native_source_checks_and_policy_rejections() -> Result<()>
    {
        let fixture = super::super::super::tests::fixture()?;
        let (a, _a, am) = metadata(0, false)?;
        let (b, _b, bm) = metadata(1, true)?;
        let inputs = select(&fixture.owner, vec![a, b])?;
        assert_eq!(inputs.len(), 2);
        let nonce = Digest::of(b"batch nonce");
        let valid = json!({"schema_version":1,"nonce":nonce,"results":[row(&inputs[0],&am)?,row(&inputs[1],&bm)?]});
        let result = parse(&canonical::to_vec(&valid)?, nonce, &inputs, &fixture.owner)?;
        assert_eq!(
            result,
            vec![Some(Digest::of(b"SPDX-License-Identifier: MIT\n")), None]
        );
        for change in [
            "nonce", "count", "order", "missing", "extra", "row", "kernel", "url", "bytes", "dns",
            "scope",
        ] {
            let mut bad = valid.clone();
            match change {
                "nonce" => bad["nonce"] = json!(Digest::of(b"other")),
                "count" => {
                    bad["results"].as_array_mut().unwrap().pop();
                }
                "order" => bad["results"].as_array_mut().unwrap().swap(0, 1),
                "missing" => {
                    bad["results"][0].as_object_mut().unwrap().remove("report");
                }
                "extra" => bad["results"][0]["approved"] = json!(true),
                "row" => bad["results"][0]["report"]["nonce"] = json!(Digest::of(b"wrong")),
                "kernel" => {
                    bad["results"][0]["report"]["kernel"]["network_namespace_inode"] = json!(12346)
                }
                "url" => {
                    bad["results"][0]["report"]["manifest"]["url"] =
                        json!("https://attacker.example/manifest.yaml")
                }
                "bytes" => {
                    bad["results"][0]["report"]["manifest"]["body_base64"] =
                        json!(STANDARD.encode(b"different"))
                }
                "dns" => {
                    bad["results"][0]["report"]["manifest"]["resolved_addresses"] =
                        json!(["127.0.0.1"])
                }
                "scope" => bad["schema_version"] = json!(2),
                _ => unreachable!(),
            }
            assert!(
                parse(&canonical::to_vec(&bad)?, nonce, &inputs, &fixture.owner).is_err(),
                "{change}"
            );
        }
        let mut empty = valid;
        empty["results"][0]["report"] = Value::Null;
        empty["results"][1]["report"] = Value::Null;
        assert_eq!(
            parse(&canonical::to_vec(&empty)?, nonce, &inputs, &fixture.owner)?,
            vec![None, None]
        );
        assert!(parse(&vec![b' '; MAX_BYTES + 1], nonce, &inputs, &fixture.owner).is_err());
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));
        Ok(())
    }
    #[tokio::test]
    async fn source_failure_categories_are_negotiated_closed_and_never_positive() -> Result<()> {
        let mut fixture = super::super::super::tests::fixture()?;
        let (a, _a, am) = metadata(0, false)?;
        let (b, _b, bm) = metadata(1, false)?;
        let inputs = select(&fixture.owner, vec![a, b])?;
        let nonce = Digest::of(b"closed source failure diagnostics");
        let valid = json!({"schema_version":1,"nonce":nonce,"results":[row(&inputs[0],&am)?,row(&inputs[1],&bm)?]});
        let mut failures = valid.clone();
        for row in failures["results"].as_array_mut().unwrap() {
            row["report"] = Value::Null;
            row["failure"] = json!("dns");
        }
        assert!(
            parse(
                &canonical::to_vec(&failures)?,
                nonce,
                &inputs,
                &fixture.owner
            )
            .is_err()
        );
        Arc::get_mut(&mut fixture.owner)
            .context("unshared fixture")?
            .binding
            .source_batch_diagnostics = true;
        assert_eq!(
            parse(
                &canonical::to_vec(&failures)?,
                nonce,
                &inputs,
                &fixture.owner
            )?,
            vec![None, None]
        );
        for code in [
            "dns",
            "https_request",
            "http_status",
            "tls",
            "destination",
            "encoding",
            "content_type",
            "size",
            "body",
            "source_text",
            "manifest_digest",
            "deadline",
            "frame_limit",
            "unavailable",
        ] {
            failures["results"][0]["failure"] = json!(code);
            assert_eq!(
                parse(
                    &canonical::to_vec(&failures)?,
                    nonce,
                    &inputs,
                    &fixture.owner
                )?,
                vec![None, None]
            );
        }
        failures["results"][0]["failure"] = json!("https://secret-address.example/account");
        assert!(
            parse(
                &canonical::to_vec(&failures)?,
                nonce,
                &inputs,
                &fixture.owner
            )
            .is_err()
        );
        let mut contradictory = valid.clone();
        contradictory["results"][0]["failure"] = json!("dns");
        assert!(
            parse(
                &canonical::to_vec(&contradictory)?,
                nonce,
                &inputs,
                &fixture.owner
            )
            .is_err()
        );
        let mut missing = valid.clone();
        missing["results"][0]["report"] = Value::Null;
        assert!(
            parse(
                &canonical::to_vec(&missing)?,
                nonce,
                &inputs,
                &fixture.owner
            )
            .is_err()
        );
        assert!(
            parse(&canonical::to_vec(&valid)?, nonce, &inputs, &fixture.owner)?
                .iter()
                .all(Option::is_some)
        );
        let requests = inputs
            .iter()
            .map(|input| &input.request)
            .collect::<Vec<_>>();
        let old = serde_json::to_value(Request {
            schema_version: 1,
            nonce,
            requests: requests.clone(),
            diagnostics: None,
        })?;
        assert!(old.get("diagnostics").is_none());
        let new = serde_json::to_value(Request {
            schema_version: 1,
            nonce,
            requests,
            diagnostics: Some(true),
        })?;
        assert_eq!(new["diagnostics"], json!(true));
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));
        Ok(())
    }
    #[tokio::test]
    async fn warming_never_bypasses_original_slot_playback_feature_or_scope() -> Result<()> {
        let mut fixture = super::super::super::tests::fixture()?;
        let (a, _a, _) = metadata(0, false)?;
        let (b, _b, _) = metadata(1, false)?;
        let inputs = || vec![a.retained_copy(), b.retained_copy()];
        let held = fixture.owner.slots.clone().try_acquire_owned()?;
        let error = Client(fixture.owner.clone())
            .warm_search_sources(inputs())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("slot busy"));
        drop(held);
        fixture.owner.playback.set_test_startup_queue_len(1);
        assert!(
            Client(fixture.owner.clone())
                .warm_search_sources(inputs())
                .await
                .is_err()
        );
        fixture.owner.playback.set_test_startup_queue_len(0);
        Arc::get_mut(&mut fixture.owner)
            .context("unshared source fixture")?
            .binding
            .source_batch = false;
        Client(fixture.owner.clone())
            .warm_search_sources(inputs())
            .await?; // no new command to old image
        assert!(
            !Client(fixture.owner.clone()).search_batch_needed(&a.descriptor().release.payload)?
        );
        Arc::get_mut(&mut fixture.owner)
            .context("unshared source fixture")?
            .binding
            .source_batch = true;
        assert!(
            Client(fixture.owner.clone())
                .warm_search_sources((0..21).map(|_| a.retained_copy()).collect())
                .await
                .is_err()
        );
        assert_eq!(
            select(&fixture.owner, vec![a.retained_copy(), a.retained_copy()])?.len(),
            1
        );
        _a(false);
        assert!(select(&fixture.owner, vec![a.retained_copy()])?.is_empty());
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));
        assert!(fixture.owner.slots.clone().try_acquire_owned().is_ok());
        Ok(())
    }
}
