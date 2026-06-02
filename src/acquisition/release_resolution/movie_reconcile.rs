use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    acquisition::release_resolution::{
        models::ReleaseConfidence,
        movie_graph::{
            MovieExternalIdConflict, MovieExternalIdEvidenceKind, MovieExternalIdProvider,
            MovieIdentityGraph, MovieTitleEvidence, MovieTitleEvidenceKind, MovieYearEvidence,
            normalize_movie_title,
        },
        movie_radarr_parser::MovieParsedRelease,
    },
    extensions::ExternalIds,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieReconciliationOutcome {
    Planned,
    ReviewRequired,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieReviewReason {
    UnparseableReleaseTitle,
    MissingParsedMovieTitle,
    MissingParsedYear,
    MissingGraphYear,
    WeakYearEvidence,
    NearYearMismatch,
    GraphIdentityConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieRejectionReason {
    ExternalIdConflict,
    WrongTitle,
    WrongYear,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieExternalIdComparisonSourceKind {
    Parser,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieExternalIdMatch {
    pub provider: MovieExternalIdProvider,
    pub id: String,
    pub source: String,
    pub source_kind: MovieExternalIdComparisonSourceKind,
    pub target_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieExternalIdMismatch {
    pub provider: MovieExternalIdProvider,
    pub id: String,
    pub source: String,
    pub source_kind: MovieExternalIdComparisonSourceKind,
    pub target_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieTitleMatch {
    pub parsed_title: String,
    pub normalized_title: String,
    pub graph_title: String,
    pub graph_title_kind: MovieTitleEvidenceKind,
    pub graph_title_source: String,
    pub graph_title_language: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieYearMatch {
    pub parsed_year: i32,
    pub graph_year: i32,
    pub graph_year_source: String,
    pub exact: bool,
    pub delta: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieReconciliation {
    pub outcome: MovieReconciliationOutcome,
    pub confidence: ReleaseConfidence,
    pub title_match: Option<MovieTitleMatch>,
    pub year_match: Option<MovieYearMatch>,
    pub external_id_matches: Vec<MovieExternalIdMatch>,
    pub external_id_conflicts: Vec<MovieExternalIdMismatch>,
    pub graph_id_conflicts: Vec<MovieExternalIdConflict>,
    pub review_reasons: Vec<MovieReviewReason>,
    pub rejection_reasons: Vec<MovieRejectionReason>,
    pub diagnostics: Vec<String>,
}

pub fn reconcile_movie_release(
    parsed: Option<&MovieParsedRelease>,
    graph: &MovieIdentityGraph,
) -> MovieReconciliation {
    let graph_id_conflicts = graph.id_conflicts.clone();
    let mut result = MovieReconciliation {
        outcome: MovieReconciliationOutcome::ReviewRequired,
        confidence: ReleaseConfidence::ReviewRequired,
        title_match: None,
        year_match: None,
        external_id_matches: Vec::new(),
        external_id_conflicts: Vec::new(),
        graph_id_conflicts,
        review_reasons: Vec::new(),
        rejection_reasons: Vec::new(),
        diagnostics: Vec::new(),
    };

    if !result.graph_id_conflicts.is_empty() {
        result
            .review_reasons
            .push(MovieReviewReason::GraphIdentityConflict);
    }

    let Some(parsed) = parsed else {
        result
            .review_reasons
            .push(MovieReviewReason::UnparseableReleaseTitle);
        return result.finish();
    };

    compare_external_ids(parsed, graph, &mut result);
    if !result.external_id_conflicts.is_empty() {
        result
            .rejection_reasons
            .push(MovieRejectionReason::ExternalIdConflict);
        return result.finish();
    }

    if !result.external_id_matches.is_empty() && result.graph_id_conflicts.is_empty() {
        result.outcome = MovieReconciliationOutcome::Planned;
        result.confidence = ReleaseConfidence::High;
        result.diagnostics.push("external_id_match".to_string());
        return result.finish();
    }

    result.title_match = best_title_match(parsed, graph);
    if result.title_match.is_none() {
        if parsed.movie_titles.is_empty()
            || parsed
                .movie_titles
                .iter()
                .all(|title| normalize_movie_title(title).is_none())
        {
            result
                .review_reasons
                .push(MovieReviewReason::MissingParsedMovieTitle);
        } else {
            result
                .rejection_reasons
                .push(MovieRejectionReason::WrongTitle);
        }
        return result.finish();
    }

    result.year_match = best_year_match(parsed.year, graph);
    evaluate_title_year_policy(parsed, graph, &mut result);
    result.finish()
}

impl MovieReconciliation {
    fn finish(mut self) -> Self {
        self.review_reasons = dedupe_reasons(self.review_reasons);
        self.rejection_reasons = dedupe_reasons(self.rejection_reasons);
        self.external_id_matches = dedupe_external_id_matches(self.external_id_matches);
        self.external_id_conflicts = dedupe_external_id_conflicts(self.external_id_conflicts);

        if !self.rejection_reasons.is_empty() {
            self.outcome = MovieReconciliationOutcome::Rejected;
            self.confidence = ReleaseConfidence::Low;
        } else if !self.review_reasons.is_empty()
            && self.outcome != MovieReconciliationOutcome::Rejected
        {
            self.outcome = MovieReconciliationOutcome::ReviewRequired;
            self.confidence = ReleaseConfidence::ReviewRequired;
        }

        self
    }
}

fn compare_external_ids(
    parsed: &MovieParsedRelease,
    graph: &MovieIdentityGraph,
    result: &mut MovieReconciliation,
) {
    if let Some(imdb) = parsed.imdb_id.as_deref() {
        compare_external_id(
            graph,
            result,
            MovieExternalIdProvider::Imdb,
            imdb,
            "radarr_parser.imdb_id",
            MovieExternalIdComparisonSourceKind::Parser,
        );
    }
    if let Some(tmdb) = parsed.tmdb_id {
        compare_external_id(
            graph,
            result,
            MovieExternalIdProvider::Tmdb,
            &tmdb.to_string(),
            "radarr_parser.tmdb_id",
            MovieExternalIdComparisonSourceKind::Parser,
        );
    }

    for entry in &graph.remote_ids {
        if entry.kind != MovieExternalIdEvidenceKind::Candidate {
            continue;
        }
        if !identity_provider(entry.provider) {
            continue;
        }
        compare_external_id(
            graph,
            result,
            entry.provider,
            &entry.normalized_id,
            &entry.source,
            MovieExternalIdComparisonSourceKind::Candidate,
        );
    }
}

fn compare_external_id(
    graph: &MovieIdentityGraph,
    result: &mut MovieReconciliation,
    provider: MovieExternalIdProvider,
    id: &str,
    source: &str,
    source_kind: MovieExternalIdComparisonSourceKind,
) {
    let Some(target_id) = graph_identity_id(&graph.external_ids, provider) else {
        result.diagnostics.push(format!(
            "external_id_without_target_identity:{provider:?}:{source}"
        ));
        return;
    };

    if graph.has_identity_external_id(provider, id) {
        result.external_id_matches.push(MovieExternalIdMatch {
            provider,
            id: id.to_string(),
            source: source.to_string(),
            source_kind,
            target_id: target_id.to_string(),
        });
    } else {
        result.external_id_conflicts.push(MovieExternalIdMismatch {
            provider,
            id: id.to_string(),
            source: source.to_string(),
            source_kind,
            target_id: target_id.to_string(),
        });
    }
}

fn best_title_match(
    parsed: &MovieParsedRelease,
    graph: &MovieIdentityGraph,
) -> Option<MovieTitleMatch> {
    let mut parsed_titles = Vec::new();
    for parsed_title in &parsed.movie_titles {
        if let Some(normalized) = normalize_movie_title(parsed_title) {
            parsed_titles.push((parsed_title.as_str(), normalized));
        }
    }

    let mut best = None;
    let mut best_rank = i32::MIN;
    for (parsed_title, normalized) in parsed_titles {
        for graph_title in &graph.titles {
            let Some(graph_normalized) = graph_title.normalized.as_deref() else {
                continue;
            };
            if normalized != graph_normalized {
                continue;
            }
            let rank = title_kind_rank(graph_title.kind);
            if rank > best_rank {
                best_rank = rank;
                best = Some(title_match(parsed_title, &normalized, graph_title));
            }
        }
    }

    best
}

fn title_match(
    parsed_title: &str,
    normalized_title: &str,
    graph_title: &MovieTitleEvidence,
) -> MovieTitleMatch {
    MovieTitleMatch {
        parsed_title: parsed_title.to_string(),
        normalized_title: normalized_title.to_string(),
        graph_title: graph_title.title.clone(),
        graph_title_kind: graph_title.kind,
        graph_title_source: graph_title.source.clone(),
        graph_title_language: graph_title.language.clone(),
    }
}

fn title_kind_rank(kind: MovieTitleEvidenceKind) -> i32 {
    match kind {
        MovieTitleEvidenceKind::TvdbCanonical => 40,
        MovieTitleEvidenceKind::Target => 30,
        MovieTitleEvidenceKind::TvdbAlias => 20,
        MovieTitleEvidenceKind::TvdbTranslation => 10,
    }
}

fn best_year_match(parsed_year: Option<i32>, graph: &MovieIdentityGraph) -> Option<MovieYearMatch> {
    let parsed_year = parsed_year?;
    let mut best: Option<(&MovieYearEvidence, i32)> = None;

    for graph_year in &graph.years {
        let delta = (parsed_year - graph_year.year).abs();
        match best {
            None => best = Some((graph_year, delta)),
            Some((current, current_delta)) => {
                if delta < current_delta
                    || (delta == current_delta
                        && year_source_rank(graph_year) > year_source_rank(current))
                {
                    best = Some((graph_year, delta));
                }
            }
        }
    }

    best.map(|(graph_year, delta)| MovieYearMatch {
        parsed_year,
        graph_year: graph_year.year,
        graph_year_source: graph_year.source.clone(),
        exact: delta == 0,
        delta,
    })
}

fn year_source_rank(year: &MovieYearEvidence) -> i32 {
    if year.source == "target_metadata" {
        return 40;
    }
    if year.source.contains(".year") {
        return 30;
    }
    20
}

fn evaluate_title_year_policy(
    parsed: &MovieParsedRelease,
    graph: &MovieIdentityGraph,
    result: &mut MovieReconciliation,
) {
    if parsed.year.is_none() {
        result
            .review_reasons
            .push(MovieReviewReason::MissingParsedYear);
        return;
    }

    let Some(year_match) = result.year_match.as_ref() else {
        result
            .review_reasons
            .push(MovieReviewReason::MissingGraphYear);
        return;
    };

    if !year_match.exact {
        if year_match.delta <= 1 {
            result
                .review_reasons
                .push(MovieReviewReason::NearYearMismatch);
        } else {
            result
                .rejection_reasons
                .push(MovieRejectionReason::WrongYear);
        }
        return;
    }

    if graph.years.is_empty() || graph.canonical_year.is_none() {
        result
            .review_reasons
            .push(MovieReviewReason::WeakYearEvidence);
        return;
    }

    result.outcome = MovieReconciliationOutcome::Planned;
    result.confidence = title_year_confidence(result.title_match.as_ref());
}

fn title_year_confidence(title_match: Option<&MovieTitleMatch>) -> ReleaseConfidence {
    match title_match.map(|matched| matched.graph_title_kind) {
        Some(MovieTitleEvidenceKind::TvdbTranslation) => ReleaseConfidence::Medium,
        Some(_) => ReleaseConfidence::High,
        None => ReleaseConfidence::ReviewRequired,
    }
}

fn graph_identity_id(ids: &ExternalIds, provider: MovieExternalIdProvider) -> Option<&str> {
    match provider {
        MovieExternalIdProvider::Tvdb => ids.tvdb.as_deref(),
        MovieExternalIdProvider::TvdbMovie => ids.tvdb_movie.as_deref(),
        MovieExternalIdProvider::Imdb => ids.imdb.as_deref(),
        MovieExternalIdProvider::Tmdb => ids.tmdb.as_deref(),
        MovieExternalIdProvider::Eidr | MovieExternalIdProvider::Other => None,
    }
}

fn identity_provider(provider: MovieExternalIdProvider) -> bool {
    matches!(
        provider,
        MovieExternalIdProvider::Tvdb
            | MovieExternalIdProvider::TvdbMovie
            | MovieExternalIdProvider::Imdb
            | MovieExternalIdProvider::Tmdb
    )
}

fn dedupe_reasons<T>(reasons: Vec<T>) -> Vec<T>
where
    T: Ord,
{
    BTreeSet::from_iter(reasons).into_iter().collect()
}

fn dedupe_external_id_matches(matches: Vec<MovieExternalIdMatch>) -> Vec<MovieExternalIdMatch> {
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for item in matches {
        let key = (
            item.provider,
            item.id.clone(),
            item.source.clone(),
            item.source_kind,
        );
        if seen.insert(key) {
            output.push(item);
        }
    }
    output
}

fn dedupe_external_id_conflicts(
    conflicts: Vec<MovieExternalIdMismatch>,
) -> Vec<MovieExternalIdMismatch> {
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for item in conflicts {
        let key = (
            item.provider,
            item.id.clone(),
            item.source.clone(),
            item.source_kind,
        );
        if seen.insert(key) {
            output.push(item);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        acquisition::release_resolution::{
            movie_graph::{
                MovieIdentityGraphInput, MovieSourceExternalIds, build_movie_identity_graph,
            },
            movie_radarr_parser::parse_movie_title,
        },
        extensions::ExternalIds,
    };

    use super::*;

    fn graph_for_matrix() -> MovieIdentityGraph {
        build_movie_identity_graph(MovieIdentityGraphInput::new(
            "The Matrix",
            Some(1999),
            ExternalIds {
                tvdb_movie: Some("170".to_string()),
                imdb: Some("tt0133093".to_string()),
                tmdb: Some("603".to_string()),
                ..Default::default()
            },
            Some(json!({
                "id": "170",
                "name": "The Matrix",
                "year": 1999,
                "aliases": ["Matrix"],
                "remoteIds": [
                    {"sourceName": "IMDB", "id": "tt0133093"},
                    {"sourceName": "TheMovieDB.com", "id": "603"}
                ]
            })),
        ))
    }

    fn parse(title: &str) -> MovieParsedRelease {
        parse_movie_title(title, false)
            .unwrap_or_else(|| panic!("expected parser to parse release title: {title}"))
    }

    #[test]
    fn movie_reconciliation_exact_external_id_match_is_high_confidence() {
        let graph = graph_for_matrix();
        let parsed = parse("Wrong Movie 2020 tt0133093 1080p BluRay x264-GRP");

        let result = reconcile_movie_release(Some(&parsed), &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::Planned);
        assert_eq!(result.confidence, ReleaseConfidence::High);
        assert!(result.title_match.is_none());
        assert_eq!(result.external_id_matches.len(), 1);
        assert_eq!(
            result.external_id_matches[0].provider,
            MovieExternalIdProvider::Imdb
        );
        assert!(result.rejection_reasons.is_empty());
    }

    #[test]
    fn movie_reconciliation_exact_title_and_year_match_is_high_confidence() {
        let graph = graph_for_matrix();
        let parsed = parse("The Matrix 1999 1080p BluRay x264-GRP");

        let result = reconcile_movie_release(Some(&parsed), &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::Planned);
        assert_eq!(result.confidence, ReleaseConfidence::High);
        assert_eq!(
            result
                .title_match
                .as_ref()
                .map(|value| value.graph_title_kind),
            Some(MovieTitleEvidenceKind::TvdbCanonical)
        );
        assert_eq!(
            result.year_match.as_ref().map(|value| value.exact),
            Some(true)
        );
        assert!(result.review_reasons.is_empty());
        assert!(result.rejection_reasons.is_empty());
    }

    #[test]
    fn movie_reconciliation_alias_title_and_year_match_is_high_confidence() {
        let graph = graph_for_matrix();
        let parsed = parse("Matrix 1999 2160p UHD BluRay x265-GRP");

        let result = reconcile_movie_release(Some(&parsed), &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::Planned);
        assert_eq!(result.confidence, ReleaseConfidence::High);
        assert_eq!(
            result
                .title_match
                .as_ref()
                .map(|value| value.graph_title_kind),
            Some(MovieTitleEvidenceKind::TvdbAlias)
        );
    }

    #[test]
    fn movie_reconciliation_title_match_with_missing_year_requires_review() {
        let graph = graph_for_matrix();
        let mut parsed = parse("The Matrix 1999 1080p BluRay x264-GRP");
        parsed.year = None;

        let result = reconcile_movie_release(Some(&parsed), &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::ReviewRequired);
        assert_eq!(result.confidence, ReleaseConfidence::ReviewRequired);
        assert!(result.title_match.is_some());
        assert!(
            result
                .review_reasons
                .contains(&MovieReviewReason::MissingParsedYear)
        );
        assert!(result.rejection_reasons.is_empty());
    }

    #[test]
    fn movie_reconciliation_wrong_year_rejects_without_external_id_match() {
        let graph = graph_for_matrix();
        let parsed = parse("The Matrix 2003 1080p BluRay x264-GRP");

        let result = reconcile_movie_release(Some(&parsed), &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::Rejected);
        assert_eq!(result.confidence, ReleaseConfidence::Low);
        assert!(result.title_match.is_some());
        assert!(
            result
                .rejection_reasons
                .contains(&MovieRejectionReason::WrongYear)
        );
    }

    #[test]
    fn movie_reconciliation_wrong_external_id_rejects_even_with_title_year_match() {
        let graph = graph_for_matrix();
        let parsed = parse("The Matrix 1999 tt9999999 1080p BluRay x264-GRP");

        let result = reconcile_movie_release(Some(&parsed), &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::Rejected);
        assert_eq!(result.confidence, ReleaseConfidence::Low);
        assert_eq!(result.external_id_conflicts.len(), 1);
        assert_eq!(
            result.external_id_conflicts[0].provider,
            MovieExternalIdProvider::Imdb
        );
        assert!(
            result
                .rejection_reasons
                .contains(&MovieRejectionReason::ExternalIdConflict)
        );
    }

    #[test]
    fn movie_reconciliation_candidate_id_conflict_rejects() {
        let mut input = MovieIdentityGraphInput::new(
            "The Matrix",
            Some(1999),
            ExternalIds {
                imdb: Some("tt0133093".to_string()),
                ..Default::default()
            },
            Some(json!({
                "id": "170",
                "name": "The Matrix",
                "year": 1999,
            })),
        );
        input.candidate_external_ids.push(MovieSourceExternalIds {
            source: "source_provider.candidate".to_string(),
            external_ids: ExternalIds {
                imdb: Some("tt9999999".to_string()),
                ..Default::default()
            },
        });
        let graph = build_movie_identity_graph(input);
        let parsed = parse("The Matrix 1999 1080p BluRay x264-GRP");

        let result = reconcile_movie_release(Some(&parsed), &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::Rejected);
        assert_eq!(result.external_id_conflicts.len(), 1);
        assert_eq!(
            result.external_id_conflicts[0].source,
            "source_provider.candidate"
        );
    }

    #[test]
    fn movie_reconciliation_unparseable_title_requires_review() {
        let graph = graph_for_matrix();

        let result = reconcile_movie_release(None, &graph);

        assert_eq!(result.outcome, MovieReconciliationOutcome::ReviewRequired);
        assert_eq!(result.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            result
                .review_reasons
                .contains(&MovieReviewReason::UnparseableReleaseTitle)
        );
    }
}
