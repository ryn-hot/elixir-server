//! Batched unsigned availability vetoes, never cached security authority.
//! Exact native facts and durable checks still surround every returned value.
use super::*;
use elixir_marketplace::{
    evidence::SupportSummary,
    peer::{PeerOutcome, RejectionCode},
};

impl ParticipationClient {
    pub(in crate::marketplace) async fn ranking_summaries_current(
        &self,
        metadata: &[&super::super::MetadataObservation],
    ) -> Result<Vec<Option<SupportSummary>>> {
        if metadata.len() > elixir_marketplace::MAX_SEARCH_RESULTS
            || metadata
                .iter()
                .map(|m| &m.descriptor().release.payload.extension_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != metadata.len()
        {
            bail!("current ranking page bounds or duplicate identity");
        }
        if metadata.is_empty() {
            return Ok(vec![]);
        }
        if !self
            .source_preflight
            .as_ref()
            .is_some_and(|c| c.catalog_head_batch_supported())
        {
            // Compatibility only; absence of an immutable capability never
            // authorizes a new operation or weakens the existing single path.
            let mut result = Vec::with_capacity(metadata.len());
            for value in metadata {
                let checked = async {
                    let summary = self.ranking_summary(value).await?;
                    self.check_catalog_current(value).await?;
                    Ok::<_, anyhow::Error>(summary)
                }
                .await;
                result.push(checked.ok());
            }
            return Ok(result);
        }
        #[cfg(test)]
        let began = Instant::now();
        let mut summaries = Vec::with_capacity(metadata.len());
        for value in metadata {
            let summary = if self.check_metadata_current(value).await.is_ok() {
                // Immutable native evidence only. This synchronous read grants
                // no I/O or execution, and durable checks bracket the readback.
                self.source_preflight
                    .as_ref()
                    .context("ranking unavailable")?
                    .ranking_summary(value)
                    .ok()
            } else {
                None
            };
            summaries.push(summary);
        }
        let indices = summaries
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.is_some().then_some(i))
            .collect::<Vec<_>>();
        #[cfg(test)]
        let metadata_rank_ms = began.elapsed().as_millis();
        #[cfg(test)]
        let head_began = Instant::now();
        #[cfg(test)]
        let mut requests = 0usize;
        #[cfg(test)]
        let mut rpc_ids = Vec::new();
        let mut offset = 0;
        while offset < indices.len() {
            let ids = indices[offset..]
                .iter()
                .map(|&i| {
                    metadata[i]
                        .descriptor()
                        .release
                        .payload
                        .extension_id
                        .clone()
                })
                .collect::<Vec<_>>();
            let (send, reply) = oneshot::channel();
            #[cfg(test)]
            let waiting = std::time::Instant::now();
            let response = self
                .enqueue_after_capacity(Request::Heads(ids.clone(), send), reply)
                .await?;
            #[cfg(test)]
            {
                requests += 1;
                rpc_ids.push(response.request_id);
                crate::marketplace::search_timing::emit(
                    crate::marketplace::search_timing::Event::SearchHeadWaitTiming {
                        context: crate::marketplace::search_timing::context(),
                        request_id: response.request_id,
                        total_us: crate::marketplace::search_timing::micros(waiting),
                    },
                );
            }
            let WorkerControlOutcome::CatalogHeads { rows } = response.control_response else {
                bail!("local head batch unavailable");
            };
            if rows.is_empty()
                || rows.len() > ids.len()
                || rows.iter().zip(&ids).any(|(r, id)| &r.extension_id != id)
            {
                bail!("local head batch order or scope differs");
            }
            let count = rows.len();
            for (n, row) in rows.into_iter().enumerate() {
                let index = indices[offset + n];
                let allowed = match row.response.response {
                    PeerOutcome::Head { state } => {
                        catalog_snapshot::allows(metadata[index].descriptor(), &state)?
                    }
                    // Missing local metadata is unknown, not a veto against
                    // independently verified remote metadata (same as singles).
                    PeerOutcome::Rejected {
                        code: RejectionCode::Invalid,
                    } => true,
                    _ => false,
                };
                if !allowed {
                    summaries[index] = None;
                }
            }
            // Replies are a nonempty prefix when bytes fill first: at most 20
            // reads total, no cursor, retry, event history or cross-view cache.
            offset += count;
        }
        #[cfg(test)]
        let heads_ms = head_began.elapsed().as_millis();
        #[cfg(test)]
        let after_began = Instant::now();
        for (index, value) in metadata.iter().enumerate() {
            if summaries[index].is_some() && self.check_metadata_current(value).await.is_err() {
                summaries[index] = None;
            }
        }
        #[cfg(test)]
        crate::marketplace::search_timing::emit(
            crate::marketplace::search_timing::Event::SearchCurrentBatchTiming {
                context: crate::marketplace::search_timing::context(),
                rows: metadata.len(),
                requests,
                rpc_ids,
                metadata_rank_ms,
                heads_ms,
                after_ms: after_began.elapsed().as_millis(),
                total_ms: began.elapsed().as_millis(),
            },
        );
        Ok(summaries)
    }
}
