//! Query-scoped no-op admission for an already verified local page. It never
//! applies a restriction, establishes public source or grants display/install.
use super::*;
use crate::marketplace::security_state;
use elixir_marketplace::{MAX_SEARCH_RESULTS, admission::CandidateDescriptor, identity::Digest};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Weak;

pub(in crate::marketplace) struct WarmSearchAdmission {
    owner: Weak<security_mirror::Mirror>,
    revision: u64,
    metadata: super::super::MetadataObservation,
}

impl ParticipationClient {
    pub(in crate::marketplace) async fn warm_local_search_admissions(
        &self,
        descriptors: &[CandidateDescriptor],
    ) -> Result<BTreeMap<Digest, WarmSearchAdmission>> {
        if !(2..=MAX_SEARCH_RESULTS).contains(&descriptors.len())
            || self.snapshot().phase != ParticipationPhase::Running
        {
            return Ok(BTreeMap::new());
        }
        let preflight = self
            .source_preflight
            .as_ref()
            .context("metadata verification unavailable")?;
        let mut seen = BTreeSet::new();
        let mut cached = Vec::new();
        for descriptor in descriptors {
            // Ambiguous or cold rows go through ordinary admission when
            // actually emitted. This method never launches a native helper.
            if let Ok(Some((id, metadata))) = preflight.cached_metadata_with_id(descriptor)
                && seen.insert(&descriptor.release.payload.extension_id)
            {
                cached.push((id, metadata));
            }
        }
        if cached.len() < 2 {
            return Ok(BTreeMap::new());
        }
        self.security_result(
            async {
                let mirror = self
                    .security_mirror
                    .as_ref()
                    .context("catalog security reconciliation unavailable")?;
                let revision = mirror.revision.lock().await;
                let values = cached.iter().map(|(_, value)| value).collect::<Vec<_>>();
                let unchanged =
                    security_state::warm_no_change_page(&self.database, &values, *revision).await?;
                Ok(cached
                    .into_iter()
                    .zip(unchanged)
                    .filter_map(|((id, metadata), unchanged)| {
                        unchanged.then(|| {
                            (
                                id,
                                WarmSearchAdmission {
                                    owner: Arc::downgrade(mirror),
                                    revision: *revision,
                                    metadata,
                                },
                            )
                        })
                    })
                    .collect())
            },
            false,
        )
        .await
    }

    /// The query has matched the exact checked descriptor digest of a local
    /// emitted row. The original fact keeps its issuer, owner and expiry.
    /// A newer mirror revision falls back to the ordinary verify/merge path.
    pub(in crate::marketplace) async fn use_warm_search_admission(
        &self,
        checked: WarmSearchAdmission,
    ) -> Result<Option<super::super::MetadataObservation>> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        let preflight = self
            .source_preflight
            .as_ref()
            .context("metadata verification unavailable")?;
        let mirror = self
            .security_mirror
            .as_ref()
            .context("catalog security reconciliation unavailable")?;
        if !checked.owner.ptr_eq(&Arc::downgrade(mirror))
            || !preflight.warm_metadata_current(&checked.metadata)?
        {
            return Ok(None);
        }
        self.security_result(
            async {
                let revision = mirror.revision.lock().await;
                if *revision != checked.revision {
                    return Ok(None);
                }
                security_state::require_current_at(
                    &self.database,
                    &checked.metadata,
                    Some(*revision),
                )
                .await?;
                Ok(Some(checked.metadata))
            },
            false,
        )
        .await
    }
}
