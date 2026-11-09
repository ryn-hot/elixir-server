use super::*;
use crate::{extensions::manifest::ExtensionManifest, marketplace::archive::RetainedSourceRecord};
use elixir_marketplace::{
    retained_authority::VerifiedRetainedAuthority, source_job::SourceMatchDescriptor,
};

pub(super) const MAX_REVISION: i64 = 9_007_199_254_740_991;
pub(super) const MAX_RECORD_BYTES: usize = 400 * 1024;
pub(super) const ROW_BYTES: i64 = 128 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ActivationPhase {
    Prepared,
    Starting,
    /// Archive retained after the actual no-effects probe AND confirmed
    /// disposal of its temporary native workspace. Not runtime readiness.
    SourcePrepared,
    /// Durable old-generation effect barrier. NOT proof that native work has
    /// stopped, state was copied, or the candidate is ready/enabled.
    Quiescing,
    /// Exact encrypted fork only. Not migration/candidate readiness or serving.
    StateCopied,
    /// One original candidate lifecycle claim was issued. Code MAY have run;
    /// never recreate the claim or use pre-migration discard/resume here.
    CandidateStarting,
}
/// Bounded recovery data, never proof that a runtime exists or is stopped.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ActivationFence {
    pub attempt: Uuid,
    pub staged_revision: i64,
    pub grant_revision: i64,
    pub plan: Digest,
    pub phase: ActivationPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_copy: Option<super::super::sdk::snapshot::CopySeal>,
}

pub(super) struct Material {
    pub authority: AuthorityRecord,
    pub source: RetainedSourceRecord,
    pub review: GrantReview,
    pub manifest: ExtensionManifest,
}
impl Material {
    pub(super) fn from_archive(
        archive: &ArchivedSource,
        metadata: &dyn crate::marketplace::selected_source::SelectedSource,
    ) -> Result<Self> {
        let authority = metadata.authority_record()?;
        let verified = authority.verify()?;
        let source = archive.record_for(verified.release())?;
        let (manifest, review) = source.review(verified.release())?;
        Ok(Self {
            authority,
            source,
            review,
            manifest,
        })
    }
    pub(super) fn current(
        &self,
        metadata: &dyn crate::marketplace::selected_source::SelectedSource,
        public: &PublicSourceObservation,
    ) -> Result<()> {
        let verified = self.authority.verify()?;
        if self.authority.to_bytes()? != metadata.authority_record()?.to_bytes()? {
            bail!("installation selection changed");
        }
        public.require_matches_source(
            &self.source.descriptor(verified.release()),
            self.source.summary(),
        )
    }
}

/// Serialized journal data, not a runtime/installation capability. Reloading
/// validates signatures/source/manifest/scope binding but must still rehash the
/// actual archived bytes before any activation or serving.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Capsule {
    pub authority: AuthorityRecord,
    pub source: RetainedSourceRecord,
    pub review: Digest,
    pub grant_revision: i64,
    pub instance: Uuid,
}
impl Capsule {
    pub(super) fn metadata_id(&self) -> Result<Digest> {
        let facts = self.facts()?;
        let (manifest, _) = self.source.review(facts.release())?;
        OperationalMetadata::from_release(facts.release(), &manifest)?.id()
    }
    pub(super) fn facts(&self) -> Result<VerifiedRetainedAuthority> {
        if !(1..=MAX_REVISION).contains(&self.grant_revision) || self.instance.is_nil() {
            bail!("invalid installation capsule identity");
        }
        let authority = self.authority.verify()?;
        let (_, review) = self.source.review(authority.release())?;
        if review.review_id()? != self.review {
            bail!("installed source review differs");
        }
        Ok(authority)
    }
    pub(super) fn descriptor(&self) -> Result<SourceMatchDescriptor> {
        Ok(self.source.descriptor(self.authority.verify()?.release()))
    }
}

/// Compact latest-action tombstone, never source or runtime authority. Retain
/// just enough identity to acknowledge cancellation and verify the disabled
/// extension metadata before a new, independently checked staging action.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CancelledCandidate {
    pub release: Digest,
    pub instance: Uuid,
    pub metadata: Digest,
}
impl CancelledCandidate {
    pub(super) fn from_capsule(capsule: &Capsule) -> Result<Self> {
        let facts = capsule.facts()?;
        Ok(Self {
            release: facts.release().release_id(),
            instance: capsule.instance,
            metadata: capsule.metadata_id()?,
        })
    }
}

#[derive(Serialize)]
pub(super) struct OperationalMetadata {
    pub extension: ExtensionId,
    pub name: String,
    pub version: String,
    pub kind: String,
    pub publisher: String,
    pub key: String,
    pub trust: String,
    pub manifest: String,
    pub package: Digest,
}
impl OperationalMetadata {
    pub(super) fn from_release(
        release: &elixir_marketplace::security::AuthorizedRelease,
        manifest: &ExtensionManifest,
    ) -> Result<Self> {
        let payload = release.payload();
        let manifest = serde_json::to_string(manifest)?;
        if manifest.len() > 64 * 1024 {
            bail!("operational manifest exceeds metadata reservation");
        }
        let kind = match payload.kind {
            elixir_marketplace::records::ExtensionKind::Module => "module",
            elixir_marketplace::records::ExtensionKind::Connector => "connector",
            elixir_marketplace::records::ExtensionKind::Blueprint => "blueprint",
        };
        Ok(Self {
            extension: payload.extension_id.clone(),
            name: payload.display_name.clone(),
            version: payload.version.clone(),
            kind: kind.into(),
            publisher: payload.publisher_name.clone(),
            key: release.signer_key().to_string(),
            trust: "community".into(),
            manifest,
            package: payload.package.sha256,
        })
    }
    pub(super) fn id(&self) -> Result<Digest> {
        if self.manifest.len() > 64 * 1024 {
            bail!("operational manifest exceeds metadata reservation");
        }
        // Some typed manifest fields are HashMaps. Preserve their values but
        // normalize object ordering before hashing; randomized serializer order
        // must not break restart/retry. Keep the normalized manifest as a string
        // because normal operational JSON can contain fractional defaults.
        let mut manifest: serde_json::Value = serde_json::from_str(&self.manifest)?;
        manifest.sort_all_objects();
        let mut metadata = serde_json::to_value(self)?;
        metadata["manifest"] = serde_json::Value::String(serde_json::to_string(&manifest)?);
        Ok(Digest::of(canonical::to_vec(&metadata)?))
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Journal {
    // Absent legacy bytes are preserved. Only fresh configured staging binds
    // an immutable choice snapshot; old pending records are never auto-adopted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub choices: Option<super::choices::Snapshot>,
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_instance: Option<Uuid>,
    pub extension: ExtensionId,
    pub home: Uuid,
    pub revision: i64,
    pub mutation: Uuid,
    pub request: Digest,
    pub approved_by: Uuid,
    // Keep the three bounded source/authority trees off nested decoder and
    // native-controller stack frames. Box is wire-transparent: old journal
    // bytes/hashes and the existing metadata accounting remain unchanged.
    pub active: Option<Box<Capsule>>,
    pub previous: Option<Box<Capsule>>,
    pub pending: Option<Box<Capsule>>,
    // Missing in older staging records. No migration or silent rewrite of a
    // valid old record is needed; only a cancellation writes this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled: Option<CancelledCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<ActivationFence>,
}
impl Journal {
    pub(super) fn validate(&self) -> Result<()> {
        if let Some(choices) = self.choices {
            choices.validate()?;
            if self.configured_instance.is_none() {
                bail!("legacy journal cannot adopt configured choices");
            }
        }
        if !matches!(
            (self.schema_version, self.configured_instance),
            (1, None) | (2, Some(_))
        ) || self.configured_instance.is_some_and(|id| id.is_nil())
            || self.home.is_nil()
            || self.approved_by.is_nil()
            || self.mutation.is_nil()
            || !(1..=MAX_REVISION).contains(&self.revision)
            || (self.active.is_none() && self.previous.is_some())
            || (self.active.is_none() && self.pending.is_none() && self.cancelled.is_none())
            || (self.pending.is_some() && self.cancelled.is_some())
        {
            bail!("invalid installation journal");
        }
        let mut instances = std::collections::BTreeSet::new();
        for capsule in self.slots() {
            let facts = capsule.facts()?;
            if facts.extension_id() != &self.extension
                || !instances.insert(capsule.instance)
                || self.configured_instance == Some(capsule.instance)
            {
                bail!("installation capsule belongs to a different identity or generation");
            }
        }
        if let Some(cancelled) = &self.cancelled
            && (cancelled.instance.is_nil()
                || instances.contains(&cancelled.instance)
                || self.configured_instance == Some(cancelled.instance))
        {
            bail!("invalid cancelled installation identity");
        }
        if let Some(activation) = &self.activation {
            let step = match activation.phase {
                ActivationPhase::Prepared => 1,
                ActivationPhase::Starting => 2,
                ActivationPhase::SourcePrepared => 3,
                ActivationPhase::Quiescing => 4,
                ActivationPhase::StateCopied => 5,
                ActivationPhase::CandidateStarting => 6,
            };
            match (&activation.state_copy, activation.phase) {
                (Some(seal), ActivationPhase::StateCopied | ActivationPhase::CandidateStarting) => {
                    seal.validate(self.active.is_some())?
                }
                (
                    None,
                    ActivationPhase::Prepared
                    | ActivationPhase::Starting
                    | ActivationPhase::SourcePrepared
                    | ActivationPhase::Quiescing,
                ) => {}
                _ => bail!("state copy seal differs from activation phase"),
            }
            if self.pending.is_none()
                || self.cancelled.is_some()
                || activation.attempt.is_nil()
                || !(1..MAX_REVISION - 1).contains(&activation.staged_revision)
                || !(1..=MAX_REVISION).contains(&activation.grant_revision)
                || activation.staged_revision.checked_add(step) != Some(self.revision)
            {
                bail!("invalid activation fence or phase revision");
            }
        }
        if canonical::to_vec(self)?.len() > MAX_RECORD_BYTES {
            bail!("installation journal exceeds metadata reservation");
        }
        Ok(())
    }
    pub(super) fn slots(&self) -> impl Iterator<Item = &Capsule> {
        self.active
            .iter()
            .chain(self.previous.iter())
            .chain(self.pending.iter())
            .map(|capsule| capsule.as_ref())
    }
}
pub(super) struct Stored {
    pub journal: Journal,
    pub floor: i64,
    pub encoded: i64,
}
impl Stored {
    pub(super) fn parse(row: AnyRow) -> Result<Self> {
        let json: String = row.try_get("record_json")?;
        let bytes: i64 = row.try_get("record_bytes")?;
        let encoded: i64 = row.try_get("encoded_bytes")?;
        let revoked: i64 = row.try_get("revocation_revision")?;
        if !(1..=MAX_RECORD_BYTES as i64).contains(&bytes)
            || bytes != json.len() as i64
            || encoded != bytes + ROW_BYTES
            || revoked < 0
            || revoked > row.try_get::<i64, _>("grant_revision")?
            || row.try_get::<String, _>("record_sha256")? != Digest::of(json.as_bytes()).to_string()
        {
            bail!("installation journal accounting or integrity differs");
        }
        let journal: Journal = canonical::decode(json.as_bytes(), MAX_RECORD_BYTES)?;
        journal.validate()?;
        if journal.extension.as_str() != row.try_get::<String, _>("extension_id")?
            || journal.home.to_string() != row.try_get::<String, _>("home_id")?
            || journal.revision != row.try_get::<i64, _>("revision")?
            || journal.configured_instance.map(|id| id.to_string())
                != row.try_get::<Option<String>, _>("instance_id")?
        {
            bail!("installation journal scope differs");
        }
        Ok(Self {
            journal,
            floor: revoked.max(1),
            encoded,
        })
    }
    pub(super) fn receipt(&self) -> Result<StagedInstallation> {
        if self.journal.activation.is_some() {
            bail!("activation is fenced; not an unused staging receipt");
        }
        let pending = self
            .journal
            .pending
            .as_ref()
            .context("installation has no pending candidate")?;
        Ok(StagedInstallation {
            choices: self.journal.choices,
            configured_instance: self.journal.configured_instance,
            extension: self.journal.extension.clone(),
            revision: self.journal.revision,
            instance: pending.instance,
            release: pending.authority.verify()?.release().release_id(),
        })
    }
}
