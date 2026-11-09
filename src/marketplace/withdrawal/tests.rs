//! Ordering/control tests with real signatures but synthetic native/protection
//! receipts. No public network, native helper/kernel or HTTP proof is implied.
use super::*;
use elixir_marketplace::{
    catalog::HeadResponse,
    records::{Revocation, SignedRecord},
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

pub(super) fn input() -> serde_json::Value {
    let data: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))
    .unwrap();
    let notice = SignedRecord::sign(
        Revocation {
            root_key: serde_json::from_value(data["publisher"]["payload"]["root_key"].clone())
                .unwrap(),
            extension_id: serde_json::from_value(data["extension_id"].clone()).unwrap(),
            release_ids: vec![serde_json::from_value(data["release_id"].clone()).unwrap()],
            release_keys: vec![],
            reason: "withdrawal test".into(),
        },
        None,
        &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
    )
    .unwrap();
    let descriptor: CandidateDescriptor =
        serde_json::from_value(serde_json::json!({"schema_version":1,
        "publisher":data["publisher"],"release":data["release"],"head":data["head"],
        "revocations":[notice],"gate_evidence":data["evidence"]["pairs"]}))
        .unwrap();
    serde_json::json!({"schema_version":1,"accepted_descriptor_id":Digest::of(descriptor.to_bytes().unwrap()),
        "revocation_id":Digest::of(notice.to_bytes().unwrap()),"accept_permanent_withdrawal":true,"descriptor":descriptor})
}
fn parsed(value: &serde_json::Value) -> Request {
    Request::parse(&serde_json::to_vec(value).unwrap()).unwrap()
}
struct Fake {
    events: Mutex<Vec<&'static str>>,
    auth: AtomicUsize,
    fail_auth: usize,
    known: bool,
    missing: bool,
    bad_offer: bool,
    wrong_receipt: bool,
    failed: AtomicBool,
    records: Mutex<Option<SecurityRecords>>,
    owners: Mutex<Vec<Box<dyn Fn(bool) + Send + Sync>>>,
}
impl Default for Fake {
    fn default() -> Self {
        Self {
            events: Mutex::default(),
            auth: AtomicUsize::new(0),
            fail_auth: 0,
            known: true,
            missing: false,
            bad_offer: false,
            wrong_receipt: false,
            failed: AtomicBool::new(false),
            records: Mutex::default(),
            owners: Mutex::default(),
        }
    }
}
impl Fake {
    fn event(&self, name: &'static str) {
        self.events.lock().unwrap().push(name);
    }
}
#[async_trait]
impl Backend for Fake {
    async fn authorize(&self) -> Result<()> {
        self.event("authorize");
        let n = self.auth.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_auth == n {
            bail!("owner revoked");
        }
        Ok(())
    }
    async fn authenticate(&self, records: &SecurityRecords) -> Result<SecurityObservation> {
        self.event("authenticate");
        let mut value = records.clone();
        if self.wrong_receipt {
            value.revocations.clear();
        }
        let (value, owner) = SecurityObservation::fixture_for_withdrawal(value)?;
        self.owners.lock().unwrap().push(Box::new(owner));
        Ok(value)
    }
    async fn retain(&self, value: &SecurityObservation) -> Result<bool> {
        self.event("retain");
        *self.records.lock().unwrap() = Some(value.records().clone());
        Ok(self.known)
    }
    async fn request(&self, operation: PeerOperation) -> Result<PeerResponse> {
        let response = match operation {
            PeerOperation::Offer { descriptor } => {
                self.event("offer");
                if self.bad_offer {
                    PeerOutcome::Offered {
                        release_id: descriptor.head.payload.stable_release_id,
                    }
                } else {
                    PeerOutcome::Rejected {
                        code: elixir_marketplace::peer::RejectionCode::Unauthorized,
                    }
                }
            }
            PeerOperation::GetHead { .. } => {
                self.event("head");
                if self.missing {
                    PeerOutcome::Rejected {
                        code: elixir_marketplace::peer::RejectionCode::Invalid,
                    }
                } else {
                    let records = self.records.lock().unwrap().clone().unwrap();
                    PeerOutcome::Head {
                        state: Box::new(HeadResponse {
                            schema_version: 1,
                            publisher: records.publisher,
                            head: None,
                            revocations: records.revocations,
                            metadata_only: true,
                        }),
                    }
                }
            }
            _ => bail!("unexpected operation"),
        };
        Ok(PeerResponse {
            schema_version: 1,
            request_id: Some(1),
            response,
        })
    }
    fn fail_worker(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }
}
#[test]
fn exact_confirmation_and_restrictive_target_are_required_without_trusting_signatures() {
    let original = input();
    assert!(Request::parse(&canonical::to_vec(&original).unwrap()).is_ok());
    for field in [
        "accept_permanent_withdrawal",
        "accepted_descriptor_id",
        "revocation_id",
        "schema_version",
    ] {
        let mut value = original.clone();
        value[field] = match field {
            "accept_permanent_withdrawal" => false.into(),
            "schema_version" => 2.into(),
            _ => serde_json::json!(Digest::of(b"other")),
        };
        assert!(
            Request::parse(&serde_json::to_vec(&value).unwrap()).is_err(),
            "{field}"
        );
    }
    let mut value = original.clone();
    value["verified"] = true.into();
    assert!(Request::parse(&serde_json::to_vec(&value).unwrap()).is_err());
    let bytes = String::from_utf8(canonical::to_vec(&original).unwrap()).unwrap();
    assert!(Request::parse(bytes.replacen('{', "{\"schema_version\":1,", 1).as_bytes()).is_err());
    assert!(Request::parse(&vec![b' '; MAX_REQUEST_BYTES + 1]).is_err());
    let mut forged = original;
    forged["descriptor"]["revocations"][0]["signature"] = "A".repeat(86).into();
    forged["revocation_id"] = serde_json::json!(Digest::of(
        canonical::to_vec(&forged["descriptor"]["revocations"][0]).unwrap()
    ));
    forged["accepted_descriptor_id"] = serde_json::json!(Digest::of(
        canonical::to_vec(&forged["descriptor"]).unwrap()
    ));
    assert!(
        Request::parse(&canonical::to_vec(&forged).unwrap()).is_ok(),
        "parsing cannot confer root authority"
    );
}
#[tokio::test]
async fn native_authentication_and_owner_recheck_precede_durable_restrictions_and_offer()
-> Result<()> {
    let backend = Fake::default();
    let view = serde_json::to_value(submit(&backend, parsed(&input())).await?)?;
    for field in [
        "signatures_verified",
        "security_tracked",
        "security_applied",
        "local_catalog_retained",
        "metadata_only",
    ] {
        assert_eq!(view[field], true);
    }
    assert_eq!(view["peer_delivery_confirmed"], false);
    assert_eq!(
        *backend.events.lock().unwrap(),
        [
            "authorize",
            "authenticate",
            "authorize",
            "retain",
            "authorize",
            "offer",
            "head",
            "authorize"
        ]
    );
    assert!(!backend.failed.load(Ordering::SeqCst));
    Ok(())
}
#[tokio::test]
async fn unknown_identity_and_missing_catalog_proof_are_not_reported_as_delivery() -> Result<()> {
    let unknown = Fake {
        known: false,
        ..Fake::default()
    };
    let view = serde_json::to_value(submit(&unknown, parsed(&input())).await?)?;
    assert_eq!(view["security_tracked"], false);
    assert_eq!(view["security_applied"], false);
    assert_eq!(view["local_catalog_retained"], false);
    assert!(!unknown.events.lock().unwrap().contains(&"offer"));
    let missing = Fake {
        missing: true,
        ..Fake::default()
    };
    let view = serde_json::to_value(submit(&missing, parsed(&input())).await?)?;
    assert_eq!(view["security_applied"], true);
    assert_eq!(view["local_catalog_retained"], false);
    assert_eq!(view["peer_delivery_confirmed"], false);
    Ok(())
}
#[tokio::test]
async fn lost_authority_mismatched_receipts_and_forged_roots_never_reach_storage() {
    for backend in [
        Fake {
            fail_auth: 2,
            ..Fake::default()
        },
        Fake {
            wrong_receipt: true,
            ..Fake::default()
        },
    ] {
        assert!(submit(&backend, parsed(&input())).await.is_err());
        assert!(!backend.events.lock().unwrap().contains(&"retain"));
    }
    let mut value = input();
    value["descriptor"]["revocations"][0]["signature"] = "A".repeat(86).into();
    value["revocation_id"] = serde_json::json!(Digest::of(
        canonical::to_vec(&value["descriptor"]["revocations"][0]).unwrap()
    ));
    value["accepted_descriptor_id"] =
        serde_json::json!(Digest::of(canonical::to_vec(&value["descriptor"]).unwrap()));
    let backend = Fake::default();
    assert!(submit(&backend, parsed(&value)).await.is_err());
    assert!(!backend.events.lock().unwrap().contains(&"retain"));
}
#[tokio::test]
async fn admitting_withdrawn_code_is_terminal_not_a_positive_acknowledgement() {
    let backend = Fake {
        bad_offer: true,
        ..Fake::default()
    };
    assert!(submit(&backend, parsed(&input())).await.is_err());
    assert!(backend.failed.load(Ordering::SeqCst));
    assert!(!backend.events.lock().unwrap().contains(&"head"));
}
