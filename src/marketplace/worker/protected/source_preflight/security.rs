//! Private native security observations, separate from paid discovery gates.
use super::*;
use elixir_marketplace::{
    MAX_CONTROL_FRAME_BYTES, SCHEMA_VERSION, canonical,
    security_job::{SecurityRecords, SecuritySummary},
    verifier::VerificationRequest,
};
use serde::{Deserialize, Serialize};

pub struct SecurityObservation {
    records: SecurityRecords,
    summary: SecuritySummary,
    observed: Instant,
    observed_utc: u64,
    protection: watch::Receiver<Snapshot>,
}
impl SecurityObservation {
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_withdrawal(
        records: SecurityRecords,
    ) -> Result<(Self, impl Fn(bool) + Send + Sync)> {
        let summary = records.verify(observation_utc()?)?;
        let (owner, protection) =
            watch::channel(Snapshot::timed(ProtectionState::Ready, OBSERVATION_TTL));
        Ok((
            Self {
                records,
                summary,
                observed: Instant::now(),
                observed_utc: observation_utc()?,
                protection,
            },
            move |ready| {
                owner.send_replace(Snapshot::timed(
                    if ready {
                        ProtectionState::Ready
                    } else {
                        ProtectionState::Failed
                    },
                    OBSERVATION_TTL,
                ));
            },
        ))
    }
    // Authentication of immutable signed records survives transport shutdown.
    // This does not assert live freshness or permit allocating unknown IDs.
    pub(in crate::marketplace) fn authenticated_summary(&self) -> &SecuritySummary {
        &self.summary
    }
    pub fn records(&self) -> &SecurityRecords {
        &self.records
    }
    pub fn summary(&self) -> Result<&SecuritySummary> {
        if !self.is_current() {
            bail!("security observation is no longer current");
        }
        Ok(&self.summary)
    }
    pub fn is_current(&self) -> bool {
        self.observed.elapsed() <= OBSERVATION_TTL
            && wall_clock_current(self.observed_utc, chrono::Utc::now().timestamp())
            && matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            )
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    schema_version: u32,
    request_id: u64,
    outcome: Outcome,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Outcome {
    SecurityState {
        summary: SecuritySummary,
        metadata_only: bool,
    },
    Rejected {
        code: String,
    },
}
fn parse(bytes: &[u8], id: u64, records: &SecurityRecords) -> Result<Result<SecuritySummary>> {
    let reply: Reply = canonical::decode(bytes, 4096)?;
    if reply.schema_version != SCHEMA_VERSION || reply.request_id != id {
        bail!("security response scope differs");
    }
    match reply.outcome {
        Outcome::Rejected { code } => {
            if !matches!(
                code.as_str(),
                "limit"
                    | "signature"
                    | "unauthorized"
                    | "expired"
                    | "conflict"
                    | "stale"
                    | "io"
                    | "invalid"
            ) {
                bail!("invalid security rejection code");
            }
            Ok(Err(anyhow::anyhow!(
                "security records were not authenticated"
            )))
        }
        Outcome::SecurityState {
            summary,
            metadata_only,
        } => {
            let expected_revocations = records
                .revocations
                .iter()
                .map(|r| r.to_bytes().map(Digest::of))
                .collect::<elixir_marketplace::Result<Vec<_>>>()?;
            let head_matches = match (&summary.head, &records.head) {
                (None, _) => true,
                (Some(actual), Some(input)) => {
                    actual.sequence == input.payload.sequence
                        && actual.payload_id == Digest::of(canonical::to_vec(&input.payload)?)
                }
                _ => false,
            };
            if !metadata_only
                || summary.extension_id != records.extension_id
                || summary.root_key != records.publisher.payload.root_key
                || summary.publisher_sequence != records.publisher.payload.sequence
                || summary.publisher_payload_id
                    != Digest::of(canonical::to_vec(&records.publisher.payload)?)
                || summary.revocation_ids != expected_revocations
                || !head_matches
            {
                bail!("security response differs from checked records");
            }
            Ok(Ok(summary))
        }
    }
}
impl Client {
    pub(in crate::marketplace::worker::protected) async fn verify_security(
        &self,
        records: &SecurityRecords,
    ) -> Result<SecurityObservation> {
        self.verify_security_mode(records, Work::Restrictions).await
    }
    pub(in crate::marketplace::worker::protected) async fn verify_owner_security(
        &self,
        records: &SecurityRecords,
    ) -> Result<SecurityObservation> {
        self.verify_security_mode(records, Work::Foreground).await
    }
    async fn verify_security_mode(
        &self,
        records: &SecurityRecords,
        work: Work,
    ) -> Result<SecurityObservation> {
        work.admit_start(&self.0)?;
        if !self.0.binding.security_verifier {
            bail!("independent security verification is unavailable");
        }
        let permit = admission::try_slot(&self.0, work, "metadata/source/security slot is busy")?;
        records.validate_shape()?;
        let records = records.clone();
        let random = u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into()?);
        let id = (random & ((1u64 << 53) - 1)).max(1);
        let bytes = canonical::to_vec(&VerificationRequest::VerifySecurity {
            schema_version: SCHEMA_VERSION,
            request_id: id,
            records: Box::new(records.clone()),
        })?;
        if bytes.len() > MAX_CONTROL_FRAME_BYTES {
            bail!("security request bounds");
        }
        let owner = self.0.clone();
        let (mut completion, response) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = run(&owner, records, id, bytes, &mut completion, work).await;
            let _ = completion.send(result);
        });
        response.await.context("security verifier owner stopped")?
    }
}
async fn run(
    owner: &Arc<Owner>,
    records: SecurityRecords,
    id: u64,
    bytes: Vec<u8>,
    completion: &mut oneshot::Sender<Result<SecurityObservation>>,
    work: Work,
) -> Result<SecurityObservation> {
    if completion.is_closed() {
        bail!("security verification cancelled before admission");
    }
    owner.headroom().await?;
    work.ready_before_launch(owner, completion).await?;
    if completion.is_closed() {
        bail!("security verification cancelled before launch");
    }
    let observed_utc = observation_utc()?;
    let mut cleanup = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    let process = timeout(
        Duration::from_secs(10),
        owner.runtime.exec_marketplace_security_verifier(
            &owner.handle,
            &owner.profile.container_spec().labels,
            &owner.binding.issuer_keys,
        ),
    )
    .await
    .context("security verifier startup deadline")??;
    let result = timeout(
        Duration::from_secs(20),
        exchange_bytes(process, &bytes, 4096, owner, completion, true, work),
    )
    .await
    .context("security verifier retirement deadline")??;
    let result = match result {
        Ok(body) => parse(&body, id, &records)?,
        Err(error) => Err(error),
    };
    cleanup.armed = false;
    let summary = result?;
    work.admit(owner, false)?;
    let mut integrity = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    timeout(
        Duration::from_secs(5),
        inspect_fixed_worker(owner.runtime.as_ref(), &owner.handle, &owner.profile),
    )
    .await
    .context("security verifier ownership readback deadline")??;
    integrity.armed = false;
    work.admit(owner, false)?;
    let observation = SecurityObservation {
        records,
        summary,
        observed: Instant::now(),
        observed_utc,
        protection: owner.protection.clone(),
    };
    if !observation.is_current() {
        bail!("security observation expired during verification");
    }
    Ok(observation)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_security_parser_rejects_replayed_or_changed_scopes() -> Result<()> {
        let data: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        let records = SecurityRecords {
            schema_version: 1,
            extension_id: serde_json::from_value(data["extension_id"].clone())?,
            publisher: serde_json::from_value(data["publisher"].clone())?,
            head: Some(serde_json::from_value(data["head"].clone())?),
            revocations: vec![],
        };
        let summary = records.verify(data["now"].as_u64().unwrap())?;
        let report = serde_json::json!({"schema_version":1,"request_id":7,"outcome":{"type":"security_state","summary":summary,"metadata_only":true}});
        assert!(parse(&serde_json::to_vec(&report)?, 7, &records)?.is_ok());
        for path in [
            "/request_id",
            "/schema_version",
            "/outcome/summary/publisher_sequence",
            "/outcome/summary/head/sequence",
        ] {
            let mut changed = report.clone();
            let value = changed.pointer_mut(path).unwrap();
            *value = serde_json::json!(value.as_u64().unwrap() + 1);
            assert!(parse(&serde_json::to_vec(&changed)?, 7, &records).is_err());
        }
        let mut changed = report.clone();
        changed["outcome"]["metadata_only"] = serde_json::json!(false);
        assert!(parse(&serde_json::to_vec(&changed)?, 7, &records).is_err());
        let mut changed = report;
        changed["outcome"]["summary"]["revocation_ids"] =
            serde_json::json!([Digest::of(b"not verified")]);
        assert!(parse(&serde_json::to_vec(&changed)?, 7, &records).is_err());
        Ok(())
    }
}
