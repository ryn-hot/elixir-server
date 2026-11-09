//! Owner-provided credential DATA for the initial atomic copy. Plaintext is
//! zeroized after preparation, never persisted in the journal or sent to a
//! probe. The parent transition retains all fresh actor/grant/runtime checks.
use super::*;
use crate::marketplace::grant_store::sdk::{generation_secrets, namespaces::Coordinates};
use zeroize::Zeroizing;

/// No Debug/Serialize: request diagnostics must not expose input credentials.
pub(in crate::marketplace) enum CredentialChange {
    Set {
        key: String,
        value: Zeroizing<String>,
        rotatable: bool,
    },
    Remove {
        key: String,
    },
}
impl CredentialChange {
    /// DATA bounds before accepting a queued setup; the selected review and
    /// fresh owner still independently restrict which keys may be written.
    pub(in crate::marketplace) fn validate_input(changes: &[Self]) -> Result<usize> {
        if changes.len() > 64 {
            bail!("credential setup exceeds its key bound");
        }
        let mut size = 1usize;
        for (index, change) in changes.iter().enumerate() {
            let key = change.key();
            if key.is_empty()
                || key.len() > 64
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            {
                bail!("invalid credential input key");
            }
            if changes[..index].iter().any(|earlier| earlier.key() == key) {
                bail!("credential setup repeats a key");
            }
            size = size
                .checked_add(2 + key.len() + 1)
                .context("credential setup byte overflow")?;
            if let Self::Set { value, .. } = change {
                if value.len() > generation_secrets::MAX_PLAINTEXT {
                    bail!("credential input exceeds its plaintext bound");
                }
                size = size
                    .checked_add(1 + 8 + value.len())
                    .context("credential setup byte overflow")?;
            }
            if size > generation_secrets::MAX_SETUP_BYTES {
                bail!("credential setup exceeds its byte bound");
            }
        }
        Ok(size)
    }
    /// Private, bounded retry bytes. This representation is shared with the
    /// existing encrypted setup fingerprint; values never enter JSON/status.
    pub(in crate::marketplace) fn canonical_input(changes: &[Self]) -> Result<Zeroizing<Vec<u8>>> {
        let size = Self::validate_input(changes)?;
        let mut ordered: Vec<_> = changes.iter().collect();
        ordered.sort_unstable_by(|a, b| a.key().cmp(b.key()));
        let mut input = Zeroizing::new(Vec::with_capacity(size));
        input.push(1);
        for change in ordered {
            input.extend_from_slice(&(change.key().len() as u16).to_be_bytes());
            input.extend_from_slice(change.key().as_bytes());
            match change {
                Self::Remove { .. } => input.push(0),
                Self::Set {
                    value, rotatable, ..
                } => {
                    input.push(1);
                    input.push(u8::from(*rotatable));
                    input.extend_from_slice(&(value.len() as u64).to_be_bytes());
                    input.extend_from_slice(value.as_bytes());
                }
            }
        }
        Ok(input)
    }
    pub(in crate::marketplace) fn set(
        key: impl Into<String>,
        value: impl Into<String>,
        rotatable: bool,
    ) -> Self {
        Self::Set {
            key: key.into(),
            value: Zeroizing::new(value.into()),
            rotatable,
        }
    }
    fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Remove { key } => key,
        }
    }
}

pub(super) struct Patch {
    id: Digest,
    changes: Vec<generation_secrets::PreparedChange>,
}
impl Patch {
    pub(super) fn prepare(
        manager: &SecretsManager,
        intent: &OwnerActivationIntent,
        review: &GrantReview,
        mut changes: Vec<CredentialChange>,
    ) -> Result<Self> {
        let owner = intent
            .configured_instance
            .context("credentials require an explicit configured owner")?;
        if intent.review != review.review_id()?
            || intent.release != review.release_id()
            || &intent.extension != review.scope().extension_id()
            || changes.is_empty()
            || changes.len() > 64
        {
            bail!("credential setup differs from the selected source");
        }
        let input = CredentialChange::canonical_input(&changes)?;
        changes.sort_by(|a, b| a.key().cmp(b.key()));
        for change in &changes {
            let key = change.key();
            if !review
                .scope()
                .secret_keys()
                .iter()
                .any(|declared| declared == key)
            {
                bail!("credential key is not declared by the selected source");
            }
        }
        let context = canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/owner-credential-setup/v1",
            "user":intent.actor.user,"home":intent.actor.home,"extension":intent.extension,
            "owner":owner,"namespace":intent.instance,"release":intent.release,
            "review":intent.review,"grant_revision":intent.grant_revision,
            "staged_revision":intent.staged_revision,"mutation":intent.mutation,
        }))?;
        let id = Digest::from_bytes(manager.request_fingerprint(&context, &input)?);
        let scope = Coordinates {
            home: intent.actor.home,
            extension: &intent.extension,
            instance: owner,
            namespace: intent.instance,
        };
        let mut prepared = Vec::with_capacity(changes.len());
        let mut encoded = 0usize;
        for change in changes {
            encoded = encoded
                .checked_add(change.key().len())
                .context("credential setup byte overflow")?;
            prepared.push(match change {
                CredentialChange::Remove { key } => {
                    generation_secrets::PreparedChange::Remove { key }
                }
                CredentialChange::Set {
                    key,
                    value,
                    rotatable,
                } => {
                    let cipher = generation_secrets::seal(manager, scope, &key, &value)?;
                    encoded = encoded
                        .checked_add(cipher.len())
                        .context("credential setup byte overflow")?;
                    if encoded > generation_secrets::MAX_SETUP_BYTES {
                        bail!("encrypted credential setup exceeds its byte bound");
                    }
                    generation_secrets::PreparedChange::Set {
                        key,
                        cipher,
                        rotatable,
                    }
                }
            });
        }
        if encoded > generation_secrets::MAX_SETUP_BYTES {
            bail!("encrypted credential setup exceeds its byte bound");
        }
        Ok(Self {
            id,
            changes: prepared,
        })
    }
    pub(super) fn id(&self) -> Digest {
        self.id
    }
    pub(super) fn changes(&self) -> &[generation_secrets::PreparedChange] {
        &self.changes
    }
}
