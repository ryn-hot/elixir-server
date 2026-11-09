//! An unsigned availability veto over an already native-verified descriptor.
//! This cannot grant installation, advance durable security state, or prove
//! that a peer's view is globally latest. Omitted/evicted local state is unknown.
use anyhow::Result;
use elixir_marketplace::{
    SCHEMA_VERSION, admission::CandidateDescriptor, canonical, catalog::HeadResponse,
    identity::Digest,
};

pub(super) fn allows(descriptor: &CandidateDescriptor, state: &HeadResponse) -> Result<bool> {
    if state.schema_version != SCHEMA_VERSION
        || !state.metadata_only
        || state.publisher.payload.root_key != descriptor.publisher.payload.root_key
        || state.publisher.payload.sequence > descriptor.publisher.payload.sequence
    {
        return Ok(false);
    }
    // This unsigned cache cannot overrule a newer NATIVE-verified generation.
    // Durable signature/anti-rollback/revocation checks still surround this
    // function; stale availability data grants no new authority. Otherwise a
    // retained old listing can permanently hide its own legitimate update.
    if let Some(head) = &state.head {
        if head.payload.extension_id != descriptor.release.payload.extension_id
            || head.payload.publisher_state_id != Digest::of(canonical::to_vec(&state.publisher)?)
        {
            return Ok(false);
        }
    }
    if state.publisher.payload.sequence == descriptor.publisher.payload.sequence {
        if canonical::to_vec(&state.publisher)? != canonical::to_vec(&descriptor.publisher)? {
            return Ok(false);
        }
        let Some(head) = &state.head else {
            return Ok(false);
        };
        if head.payload.sequence > descriptor.head.payload.sequence
            || (head.payload.sequence == descriptor.head.payload.sequence
                && canonical::to_vec(head)? != canonical::to_vec(&descriptor.head)?)
        {
            return Ok(false);
        }
    }
    let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
    Ok(!state.revocations.iter().any(|record| {
        let payload = &record.payload;
        payload.extension_id == descriptor.release.payload.extension_id
            && (payload.release_ids.contains(&release)
                || payload
                    .release_keys
                    .contains(&descriptor.release.signer_key)
                || payload.release_keys.contains(&descriptor.head.signer_key))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::records::{Revocation, SignedRecord};
    fn fixture() -> Result<(CandidateDescriptor, HeadResponse)> {
        let value: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        let descriptor = CandidateDescriptor::parse(&serde_json::to_vec(&serde_json::json!({
            "schema_version":1,"publisher":value["publisher"],"release":value["release"],
            "head":value["head"],"revocations":[],"gate_evidence":value["evidence"]["pairs"]
        }))?)?;
        let state = HeadResponse {
            schema_version: 1,
            publisher: descriptor.publisher.clone(),
            head: Some(descriptor.head.clone()),
            revocations: vec![],
            metadata_only: true,
        };
        Ok((descriptor, state))
    }
    fn advanced(publisher: bool) -> Result<(CandidateDescriptor, HeadResponse)> {
        let (mut descriptor, state) = fixture()?;
        let delegate = ed25519_dalek::SigningKey::from_bytes(&[2; 32]);
        if publisher {
            let mut payload = descriptor.publisher.payload.clone();
            payload.sequence += 1;
            descriptor.publisher = SignedRecord::sign(
                payload,
                None,
                &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
            )?;
            descriptor.publisher.verify()?;
            let authority = descriptor.publisher.verify()?.record_id();
            descriptor.release = SignedRecord::sign(
                descriptor.release.payload.clone(),
                Some(authority),
                &delegate,
            )?;
            descriptor.head.payload.publisher_state_id = authority;
            descriptor.head.authority_record_id = Some(authority);
        }
        let mut head = descriptor.head.payload.clone();
        head.sequence += 1;
        descriptor.head = SignedRecord::sign(head, descriptor.head.authority_record_id, &delegate)?;
        descriptor.release.verify()?;
        descriptor.head.verify()?;
        Ok((descriptor, state))
    }
    #[test]
    fn an_older_local_head_cannot_hide_a_newer_independently_checked_head() -> Result<()> {
        let (descriptor, state) = advanced(false)?;
        assert!(allows(&descriptor, &state)?);
        // The same stale observation must not veto an update at admission,
        // page ranking, selection or an installed update's subsequent checks.
        assert!(!allows(
            &fixture()?.0,
            &HeadResponse {
                head: Some(descriptor.head.clone()),
                ..state
            }
        )?);
        Ok(())
    }
    #[test]
    fn a_superseded_publisher_generation_is_unknown_not_authority_against_a_newer_one() -> Result<()>
    {
        let (descriptor, mut state) = advanced(true)?;
        assert!(allows(&descriptor, &state)?);
        // A missing head under an older delegation cannot describe this newer
        // native-checked delegation. Same-generation withdrawal still vetoes.
        state.head = None;
        assert!(allows(&descriptor, &state)?);
        state.publisher = descriptor.publisher.clone();
        assert!(!allows(&descriptor, &state)?);
        Ok(())
    }
    #[test]
    fn outdated_snapshots_still_veto_known_release_or_signer_withdrawals() -> Result<()> {
        for publisher in [false, true] {
            let (descriptor, state) = advanced(publisher)?;
            for signer in [
                None,
                Some(descriptor.release.signer_key),
                Some(descriptor.head.signer_key),
            ] {
                let mut state = state.clone();
                state.revocations = vec![SignedRecord::sign(
                    Revocation {
                        root_key: descriptor.publisher.payload.root_key,
                        extension_id: descriptor.release.payload.extension_id.clone(),
                        release_ids: if signer.is_none() {
                            vec![Digest::of(canonical::to_vec(&descriptor.release.payload)?)]
                        } else {
                            vec![]
                        },
                        release_keys: signer.into_iter().collect(),
                        reason: "withdrawn".into(),
                    },
                    None,
                    &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
                )?];
                assert!(!allows(&descriptor, &state)?);
            }
        }
        Ok(())
    }
    #[test]
    fn older_snapshots_with_wrong_identity_or_publisher_binding_never_relax_the_veto() -> Result<()>
    {
        let (descriptor, state) = advanced(false)?;
        let mut changed = state.clone();
        changed.head.as_mut().unwrap().payload.extension_id =
            elixir_marketplace::identity::ExtensionId::new(
                &descriptor.publisher.payload.root_key,
                "other",
            )?;
        assert!(!allows(&descriptor, &changed)?);
        let mut changed = state.clone();
        changed.head.as_mut().unwrap().payload.publisher_state_id =
            Digest::of(b"different publisher");
        assert!(!allows(&descriptor, &changed)?);
        let mut changed = state;
        changed.publisher.payload.root_key = elixir_marketplace::identity::PublicKey::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[9; 32])
                .verifying_key()
                .to_bytes(),
        )?;
        assert!(!allows(&descriptor, &changed)?);
        Ok(())
    }
    #[test]
    fn snapshot_only_vetoes_and_requires_exact_publisher_head() -> Result<()> {
        let (descriptor, state) = fixture()?;
        assert!(allows(&descriptor, &state)?);
        let mut changed = state.clone();
        changed.head = None;
        assert!(!allows(&descriptor, &changed)?);
        let mut changed = state.clone();
        changed.publisher.payload.sequence += 1;
        assert!(!allows(&descriptor, &changed)?);
        let mut changed = state.clone();
        changed.head.as_mut().unwrap().payload.sequence += 1;
        assert!(!allows(&descriptor, &changed)?);
        let mut changed = state.clone();
        changed.metadata_only = false;
        assert!(!allows(&descriptor, &changed)?);
        let mut changed = state;
        changed.schema_version += 1;
        assert!(!allows(&descriptor, &changed)?);
        Ok(())
    }
    #[test]
    fn withdrawals_of_release_or_either_signer_veto_but_unrelated_do_not() -> Result<()> {
        let (descriptor, mut state) = fixture()?;
        let mut revocation = Revocation {
            root_key: descriptor.publisher.payload.root_key,
            extension_id: descriptor.release.payload.extension_id.clone(),
            release_ids: vec![Digest::of(b"different release")],
            release_keys: vec![],
            reason: "withdrawn".into(),
        };
        let key = ed25519_dalek::SigningKey::from_bytes(&[1; 32]);
        state.revocations = vec![SignedRecord::sign(revocation.clone(), None, &key)?];
        assert!(allows(&descriptor, &state)?);
        revocation.release_ids = vec![Digest::of(canonical::to_vec(&descriptor.release.payload)?)];
        state.revocations = vec![SignedRecord::sign(revocation.clone(), None, &key)?];
        assert!(!allows(&descriptor, &state)?);
        for signer in [descriptor.release.signer_key, descriptor.head.signer_key] {
            revocation.release_ids.clear();
            revocation.release_keys = vec![signer];
            state.revocations = vec![SignedRecord::sign(revocation.clone(), None, &key)?];
            assert!(!allows(&descriptor, &state)?);
        }
        Ok(())
    }
}
