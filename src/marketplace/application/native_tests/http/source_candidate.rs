//! Actual acquisition candidate search -> verified marketplace source module,
//! through the SAME installed-source controller (no HTTP endpoint, container or
//! legacy provider). Offline fixture: the module returns one synthetic candidate.
use super::*;
use crate::http::handlers::acquisition_sources::{
    CandidateSearchRequest, MarketplaceSources, search_candidates_with_store,
};

pub(super) async fn qualify(state: &crate::state::AppState, instance: Uuid) -> Result<()> {
    let sources = MarketplaceSources::from_state(state);
    let request: CandidateSearchRequest = serde_json::from_value(json!({
        "mediaType": "movie",
        "title": "Example Movie",
        "year": 2026,
        "externalIds": {"imdb": "tt1234567"},
        "limit": 5
    }))?;
    let started = std::time::Instant::now();
    let response = search_candidates_with_store(&state.db_pool, request, Some(&sources)).await?;
    eprintln!(
        "native source candidate search: elapsed_ms={} candidates={}",
        started.elapsed().as_millis(),
        response.candidates.len()
    );
    assert_eq!(response.provider.instance_id, instance, "the installed source instance answered");
    assert_eq!(response.candidates.len(), 1, "one synthetic candidate is normalized");
    assert!(
        response
            .warnings
            .iter()
            .any(|warning| warning.contains("synthetic source candidate for Example Movie")),
        "the module received the v1 envelope with the original request"
    );
    // Without a marketplace handle the same row is never offered: there is no
    // HTTP endpoint or legacy fallback for a source-js provider.
    let request: CandidateSearchRequest = serde_json::from_value(json!({
        "mediaType": "movie",
        "title": "Example Movie",
        "limit": 5
    }))?;
    assert!(
        search_candidates_with_store(&state.db_pool, request, None)
            .await
            .is_err(),
        "a source-js provider must not be reachable without the installed controller"
    );
    Ok(())
}
