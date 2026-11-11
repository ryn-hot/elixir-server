//! One page of exact bytes, owned by a single private pipe. This dictionary
//! stores untrusted hint data, not a verification result, lifetime, or grant.
use super::*;
use elixir_marketplace::{
    admission::CandidateDescriptor,
    catalog::{SearchPage, SearchQuery, SearchResult},
    identity::Digest,
    worker_control::CatalogSearchResult,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct SearchDictionary(BTreeMap<Digest, Descriptor>);

struct Descriptor {
    value: CandidateDescriptor,
    canonical_bytes: usize,
}

impl Descriptor {
    fn from_hint(value: CandidateDescriptor) -> Result<(Digest, Self)> {
        // The caller already strictly validated the envelope and descriptor
        // shape. Hash its exact canonical content, not a release/version ID.
        let encoded = canonical::to_vec(&value)?;
        Ok((
            Digest::of(&encoded),
            Self {
                value,
                canonical_bytes: encoded.len(),
            },
        ))
    }
}

impl SearchDictionary {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn from_response(response: &PeerResponse) -> Result<Self> {
        let PeerOutcome::Search { page } = &response.response else {
            return Ok(Self::default());
        };
        Ok(Self(
            page.results
                .iter()
                .map(|row| Descriptor::from_hint(row.descriptor.clone()))
                .collect::<Result<_>>()?,
        ))
    }

    fn expand(mut self, result: WorkerControlResponse) -> Result<(Self, PeerResponse)> {
        let request_id = Some(result.request_id);
        let page = match result.control_response {
            WorkerControlOutcome::CatalogSearch { page } => *page,
            WorkerControlOutcome::Rejected { code } => {
                return Ok((
                    Self::default(),
                    PeerResponse {
                        schema_version: 1,
                        request_id,
                        response: PeerOutcome::Rejected { code },
                    },
                ));
            }
            _ => bail!("private compact search response differs"),
        };
        let mut response = PeerResponse {
            schema_version: 1,
            request_id,
            response: PeerOutcome::Search {
                page: Box::new(SearchPage {
                    schema_version: page.schema_version,
                    snapshot_id: page.snapshot_id,
                    results: Vec::with_capacity(page.results.len()),
                    next_cursor: page.next_cursor,
                    network_complete: page.network_complete,
                    metadata_only: page.metadata_only,
                }),
            },
        };
        let mut bytes = canonical::to_vec(&response)?.len();
        let PeerOutcome::Search { page: full } = &mut response.response else {
            unreachable!()
        };
        let mut next = Self::default();
        let mut extensions = BTreeSet::new();
        for row in page.results {
            let (digest, descriptor, support) = match row {
                CatalogSearchResult::Reference(row) => (
                    row.descriptor_id,
                    self.0
                        .remove(&row.descriptor_id)
                        .context("private search did not supply this exact descriptor")?,
                    row.retained_support,
                ),
                CatalogSearchResult::Full(row) => {
                    let (digest, descriptor) = Descriptor::from_hint(row.descriptor)?;
                    (digest, descriptor, row.retained_support)
                }
            };
            // Charge the exact expanded JSON length without re-serializing
            // every cached descriptor. The normal frame ceiling still holds.
            bytes += usize::from(!full.results.is_empty())
                + br#"{"descriptor":,"retained_support":}"#.len()
                + descriptor.canonical_bytes
                + canonical::to_vec(&support)?.len();
            if bytes > MAX_CONTROL_FRAME_BYTES
                || !extensions.insert(descriptor.value.release.payload.extension_id.clone())
            {
                bail!("private search expansion exceeds page bounds");
            }
            full.results.push(SearchResult {
                descriptor: descriptor.value.clone(),
                retained_support: support,
            });
            next.0.insert(digest, descriptor);
        }
        Ok((next, response))
    }
}

impl PrivateWorkerClient {
    pub(in crate::marketplace) fn enable_compact_search(&mut self) {
        self.compact_search = Some(SearchDictionary::default());
    }

    pub(super) async fn request_compact_search(
        &mut self,
        query: SearchQuery,
        cursor: Option<String>,
    ) -> Result<PeerResponse> {
        let previous = std::mem::take(
            self.compact_search
                .as_mut()
                .context("private compact search is disabled")?,
        );
        let known = previous.0.keys().copied().collect();
        let reply = self
            .control(WorkerControlOperation::CatalogSearch {
                query,
                cursor,
                known,
            })
            .await?;
        let (next, result) = previous.expand(reply)?;
        self.compact_search = Some(next);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::{evidence::SupportSummary, worker_control::CatalogSearchReference};

    fn reference(descriptor_id: Digest, support: SupportSummary) -> WorkerControlResponse {
        WorkerControlResponse {
            schema_version: 1,
            request_id: 2,
            control_response: WorkerControlOutcome::CatalogSearch {
                page: Box::new(SearchPage {
                    schema_version: 1,
                    snapshot_id: Digest::of(b"current page"),
                    results: vec![CatalogSearchResult::Reference(CatalogSearchReference {
                        descriptor_id,
                        retained_support: support,
                    })],
                    next_cursor: Some("next".into()),
                    network_complete: false,
                    metadata_only: true,
                }),
            },
        }
    }

    #[test]
    fn exact_descriptor_expands_with_fresh_support_and_no_extra_frame_budget() -> Result<()> {
        let f: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        let value = CandidateDescriptor::parse(&canonical::to_vec(&serde_json::json!({
            "schema_version": 1, "publisher": f["publisher"], "release": f["release"],
            "head": f["head"], "revocations": [], "gate_evidence": f["evidence"]["pairs"]
        }))?)?;
        let (id, descriptor) = Descriptor::from_hint(value)?;
        let support = SupportSummary {
            verified_supporters: 7,
            weighted_score: 17,
            discovery_qualified: true,
            capped: false,
            is_global_total: false,
            valid_until: Some(1800000000),
        };
        let reply = || reference(id, support.clone());
        assert!(SearchDictionary::default().expand(reply()).is_err());
        let dictionary = SearchDictionary(BTreeMap::from([(id, descriptor)]));
        let (mut dictionary, expanded) = dictionary.expand(reply())?;
        let encoded = canonical::to_vec(&expanded)?;
        assert!(PeerResponse::parse(&encoded).is_ok());
        let PeerOutcome::Search { page } = expanded.response else {
            bail!("expected reconstructed page");
        };
        assert_eq!(Digest::of(page.results[0].descriptor.to_bytes()?), id);
        assert_eq!(page.results[0].retained_support.verified_supporters, 7);
        assert_eq!(page.next_cursor.as_deref(), Some("next"));
        // A hostile compact frame must not make the expanded page exceed the
        // ordinary cap. Inject the accounting edge without huge fixtures.
        dictionary.0.get_mut(&id).unwrap().canonical_bytes +=
            MAX_CONTROL_FRAME_BYTES - encoded.len();
        let (mut boundary, _) = dictionary.expand(reply())?;
        boundary.0.get_mut(&id).unwrap().canonical_bytes += 1;
        assert!(boundary.expand(reply()).is_err());
        Ok(())
    }
}
