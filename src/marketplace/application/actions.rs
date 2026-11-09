//! One in-flight request and one latest acknowledgement, never action history.
//! Correlation data cannot authorize SQL, native execution or cleanup.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::marketplace) enum ActionKind {
    Approve,
    RetryStage,
    Setup,
    Activate,
    Cancel,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::marketplace) enum ActionStatus {
    Queued,
    Running,
    /// The coordinator handled the request. The operation's actual phase is
    /// separate: this is NOT approval, installation or native-disposal proof.
    Acknowledged,
    Rejected,
    RecoveryRequired,
    Unconfirmed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(in crate::marketplace) struct ActionReceipt {
    pub request_id: Uuid,
    pub kind: ActionKind,
    pub status: ActionStatus,
}
struct Tracked {
    receipt: ActionReceipt,
    fingerprint: Digest,
}
#[derive(Default)]
pub(super) struct Tracker(Mutex<Option<Tracked>>);
pub(super) fn retain_mutation(original: &mut Option<Uuid>, requested: Uuid) -> Result<()> {
    if requested.is_nil() || original.is_some_and(|id| id != requested) {
        bail!("install decision must retry its original nonzero mutation");
    }
    *original = Some(requested);
    Ok(())
}
impl Tracker {
    pub(super) fn current(&self, closed: bool) -> Result<Option<ActionReceipt>> {
        let record = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("action owner unavailable"))?;
        Ok(record.as_ref().map(|record| {
            let mut receipt = record.receipt.clone();
            if closed && matches!(receipt.status, ActionStatus::Queued | ActionStatus::Running) {
                receipt.status = ActionStatus::Unconfirmed;
            }
            receipt
        }))
    }
    /// Hold only a short synchronous lock across the original channel handoff.
    /// Identical in-flight/handled requests acknowledge, never enqueue twice.
    /// A rejected/recovery request may explicitly retry the SAME payload with
    /// renewed same-user authorization; the command rechecks actual owners.
    pub(super) fn enqueue(
        &self,
        request_id: Uuid,
        kind: ActionKind,
        fingerprint: Digest,
        send: impl FnOnce() -> Result<()>,
    ) -> Result<ActionReceipt> {
        let mut record = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("action owner unavailable"))?;
        if let Some(previous) = record.as_ref() {
            if previous.receipt.request_id == request_id {
                if previous.fingerprint != fingerprint || previous.receipt.kind != kind {
                    bail!("install action request id was reused with different input");
                }
                if !matches!(
                    previous.receipt.status,
                    ActionStatus::Rejected | ActionStatus::RecoveryRequired
                ) {
                    return Ok(previous.receipt.clone());
                }
            } else if matches!(
                previous.receipt.status,
                ActionStatus::Queued | ActionStatus::Running
            ) {
                bail!("install command action slot busy");
            }
        }
        send()?;
        let receipt = ActionReceipt {
            request_id,
            kind,
            status: ActionStatus::Queued,
        };
        *record = Some(Tracked {
            receipt: receipt.clone(),
            fingerprint,
        });
        Ok(receipt)
    }
    pub(super) fn mark(&self, request_id: Uuid, status: ActionStatus) -> Result<()> {
        let mut record = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("action owner unavailable"))?;
        let record = record
            .as_mut()
            .filter(|record| record.receipt.request_id == request_id)
            .context("original install action acknowledgement missing")?;
        let expected = match status {
            ActionStatus::Running => ActionStatus::Queued,
            ActionStatus::Acknowledged
            | ActionStatus::Rejected
            | ActionStatus::RecoveryRequired => ActionStatus::Running,
            _ => bail!("invalid coordinator acknowledgement transition"),
        };
        if record.receipt.status != expected {
            bail!("original install action acknowledgement changed");
        }
        record.receipt.status = status;
        Ok(())
    }
}

impl Action {
    pub(super) fn require_original_mutation(
        &self,
        activation: Option<Uuid>,
        cancellation: Option<Uuid>,
    ) -> Result<()> {
        let original = match self.kind() {
            ActionKind::Activate => activation,
            ActionKind::Cancel => cancellation,
            _ => None,
        };
        if original.is_some_and(|id| id != self.request_id()) {
            bail!("install decision must retry its original mutation");
        }
        Ok(())
    }
    pub(super) fn kind(&self) -> ActionKind {
        match self {
            Self::Approve { .. } => ActionKind::Approve,
            Self::RetryStage { .. } => ActionKind::RetryStage,
            Self::Setup { .. } => ActionKind::Setup,
            Self::Activate { .. } => ActionKind::Activate,
            Self::Cancel { .. } => ActionKind::Cancel,
        }
    }
    pub(super) fn request_id(&self) -> Uuid {
        match self {
            Self::Approve { selection, .. } => selection.mutation_id,
            Self::RetryStage { mutation, .. }
            | Self::Setup { mutation, .. }
            | Self::Activate { mutation, .. }
            | Self::Cancel { mutation, .. } => *mutation,
        }
    }
    pub(super) fn fingerprint(&self, operation: Uuid, secrets: &SecretsManager) -> Result<Digest> {
        let principal = self.principal();
        let approval = match self {
            Self::Approve { selection, .. } => serde_json::json!({
                "review":selection.review_id,"revision":selection.expected_revision,
                "automatic_updates":selection.automatic_updates,
            }),
            _ => serde_json::Value::Null,
        };
        // Deliberately omit session/revision/expiry: an exact retry may renew
        // authorization, never user/home, action, mutation or credential input.
        let context = canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/application-action/v1",
            "operation":operation,"user":principal.user_id,"home":principal.home_id,
            "request":self.request_id(),"kind":self.kind(),"approval":approval,
        }))?;
        let input = match self {
            Self::Setup { credentials, .. } => CredentialChange::canonical_input(credentials)?,
            _ => zeroize::Zeroizing::new(Vec::new()),
        };
        Ok(Digest::from_bytes(
            secrets.request_fingerprint(&context, &input)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn an_original_sql_mutation_cannot_be_replaced_on_recovery() -> Result<()> {
        let mut original = None;
        assert!(retain_mutation(&mut original, Uuid::nil()).is_err());
        assert!(original.is_none());
        let id = Uuid::new_v4();
        retain_mutation(&mut original, id)?;
        retain_mutation(&mut original, id)?;
        assert!(retain_mutation(&mut original, Uuid::new_v4()).is_err());
        assert_eq!(original, Some(id));
        Ok(())
    }

    #[test]
    fn identical_actions_acknowledge_one_original_handoff_and_keep_only_the_latest_record()
    -> Result<()> {
        let tracker = Tracker::default();
        let id = Uuid::new_v4();
        let fingerprint = Digest::of(b"bounded input");
        let sends = Cell::new(0);
        let send = || {
            sends.set(sends.get() + 1);
            Ok(())
        };
        assert_eq!(
            tracker
                .enqueue(id, ActionKind::Setup, fingerprint, send)?
                .status,
            ActionStatus::Queued
        );
        assert_eq!(
            tracker
                .enqueue(id, ActionKind::Setup, fingerprint, send)?
                .status,
            ActionStatus::Queued
        );
        assert!(
            tracker
                .enqueue(id, ActionKind::Setup, Digest::of(b"different"), send)
                .is_err()
        );
        assert!(
            tracker
                .enqueue(id, ActionKind::Cancel, fingerprint, send)
                .is_err()
        );
        assert!(
            tracker
                .enqueue(Uuid::new_v4(), ActionKind::Cancel, fingerprint, send)
                .is_err()
        );
        tracker.mark(id, ActionStatus::Running)?;
        assert_eq!(
            tracker
                .enqueue(id, ActionKind::Setup, fingerprint, send)?
                .status,
            ActionStatus::Running
        );
        tracker.mark(id, ActionStatus::Acknowledged)?;
        assert_eq!(
            tracker
                .enqueue(id, ActionKind::Setup, fingerprint, send)?
                .status,
            ActionStatus::Acknowledged
        );
        assert_eq!(sends.get(), 1);
        let next = Uuid::new_v4();
        tracker.enqueue(next, ActionKind::Cancel, fingerprint, send)?;
        assert_eq!(sends.get(), 2);
        assert_eq!(tracker.current(false)?.unwrap().request_id, next);
        assert!(tracker.mark(id, ActionStatus::Running).is_err());
        Ok(())
    }
    #[test]
    fn exact_recovery_or_rejected_input_can_retry_but_never_change_its_payload() -> Result<()> {
        for outcome in [ActionStatus::Rejected, ActionStatus::RecoveryRequired] {
            let tracker = Tracker::default();
            let id = Uuid::new_v4();
            let input = Digest::of(b"original");
            tracker.enqueue(id, ActionKind::Activate, input, || Ok(()))?;
            tracker.mark(id, ActionStatus::Running)?;
            tracker.mark(id, outcome)?;
            assert!(
                tracker
                    .enqueue(id, ActionKind::Activate, Digest::of(b"replacement"), || Ok(
                        ()
                    ))
                    .is_err()
            );
            assert_eq!(tracker.current(false)?.unwrap().status, outcome);
            assert_eq!(
                tracker
                    .enqueue(id, ActionKind::Activate, input, || Ok(()))?
                    .status,
                ActionStatus::Queued
            );
            assert!(tracker.mark(id, ActionStatus::Acknowledged).is_err());
            tracker.mark(id, ActionStatus::Running)?;
            tracker.mark(id, ActionStatus::Acknowledged)?;
            assert!(tracker.mark(id, ActionStatus::Acknowledged).is_err());
        }
        Ok(())
    }
    #[test]
    fn closed_transport_is_unconfirmed_and_a_failed_handoff_never_overwrites_acknowledged_input()
    -> Result<()> {
        let tracker = Tracker::default();
        let id = Uuid::new_v4();
        let input = Digest::of(b"input");
        tracker.enqueue(id, ActionKind::Cancel, input, || Ok(()))?;
        assert_eq!(
            tracker.current(true)?.unwrap().status,
            ActionStatus::Unconfirmed
        );
        assert_eq!(
            tracker.current(false)?.unwrap().status,
            ActionStatus::Queued
        );
        tracker.mark(id, ActionStatus::Running)?;
        assert_eq!(
            tracker.current(true)?.unwrap().status,
            ActionStatus::Unconfirmed
        );
        tracker.mark(id, ActionStatus::Acknowledged)?;
        assert!(
            tracker
                .enqueue(Uuid::new_v4(), ActionKind::Setup, input, || bail!("closed"))
                .is_err()
        );
        assert_eq!(tracker.current(true)?.unwrap().request_id, id);
        assert_eq!(
            tracker.current(true)?.unwrap().status,
            ActionStatus::Acknowledged
        );
        Ok(())
    }
    fn principal() -> CurrentPrincipal {
        CurrentPrincipal::fixture_owner(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
            chrono::Utc::now() + chrono::Duration::minutes(5),
            crate::authz::CapabilitySet::all(),
        )
    }
    #[test]
    fn keyed_setup_correlation_is_order_independent_private_and_allows_same_user_session_renewal()
    -> Result<()> {
        let p = principal();
        let mutation = Uuid::new_v4();
        let operation = Uuid::new_v4();
        let secrets = SecretsManager::from_key_bytes([92; 32], false);
        let make = |principal: CurrentPrincipal, reverse: bool, value: &str, rotatable: bool| {
            let mut credentials = vec![
                CredentialChange::set("a", value, rotatable),
                CredentialChange::Remove { key: "z".into() },
            ];
            if reverse {
                credentials.reverse();
            }
            Action::Setup {
                principal,
                mutation,
                credentials,
            }
        };
        let original = make(p.clone(), false, "private low entropy fixture", false)
            .fingerprint(operation, &secrets)?;
        let mut fresh = p.clone();
        fresh.capability_revision += 1;
        fresh.account_session_id = Uuid::new_v4();
        assert_eq!(
            original,
            make(fresh, true, "private low entropy fixture", false)
                .fingerprint(operation, &secrets)?
        );
        assert_ne!(
            original,
            make(p.clone(), false, "replacement", false).fingerprint(operation, &secrets)?
        );
        assert_ne!(
            original,
            make(p.clone(), false, "private low entropy fixture", true)
                .fingerprint(operation, &secrets)?
        );
        assert_ne!(
            original,
            make(p.clone(), false, "private low entropy fixture", false)
                .fingerprint(Uuid::new_v4(), &secrets)?
        );
        assert_ne!(
            original,
            make(principal(), false, "private low entropy fixture", false)
                .fingerprint(operation, &secrets)?
        );
        assert_ne!(
            original,
            make(p, false, "private low entropy fixture", false)
                .fingerprint(operation, &SecretsManager::from_key_bytes([93; 32], false))?
        );
        let tracker = Tracker::default();
        let receipt = tracker.enqueue(mutation, ActionKind::Setup, original, || Ok(()))?;
        let encoded = serde_json::to_string(&receipt)?;
        assert!(!encoded.contains("private low entropy fixture"));
        assert!(!encoded.contains(&original.to_string()));
        Ok(())
    }
    #[test]
    fn shared_credential_encoding_preserves_the_existing_length_delimited_setup_bytes() -> Result<()>
    {
        let input = CredentialChange::canonical_input(&[
            CredentialChange::Remove { key: "z".into() },
            CredentialChange::set("a", "bc", true),
        ])?;
        assert_eq!(
            input.as_slice(),
            &[
                1, 0, 1, b'a', 1, 1, 0, 0, 0, 0, 0, 0, 0, 2, b'b', b'c', 0, 1, b'z', 0
            ]
        );
        assert_eq!(CredentialChange::canonical_input(&[])?.as_slice(), &[1]);
        Ok(())
    }
}
