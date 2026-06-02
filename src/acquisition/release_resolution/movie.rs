use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::{
    acquisition::release_resolution::{
        models::{ReleaseConfidence, ReleaseCoverageKind, ReleaseKind, ReleaseResolverKind},
        movie_graph::{
            MovieIdentityGraph, MovieIdentityGraphInput, MovieSourceExternalIds,
            build_movie_identity_graph,
        },
        movie_radarr_parser::{MovieParsedRelease, parse_movie_title},
        movie_reconcile::{
            MovieReconciliation, MovieReconciliationOutcome, reconcile_movie_release,
        },
    },
    extensions::ExternalIds,
};

pub const MOVIE_RADARR_STYLE_RESOLVER_VERSION: &str = "rrm-movie-radarr-style-v0";
pub const RADARR_REFERENCE_REPOSITORY: &str = "https://github.com/Radarr/Radarr";
pub const RADARR_REFERENCE_COMMIT: &str = "520bf4215a13223433ef6c77ad7e822cd8359c94";
pub const RADARR_MOVIE_FIXTURE_SET: &str = "rrm0-radarr-movie-parser-inventory";
pub const MOVIE_MAIN_FILE_SELECTION_POLICY_VERSION: &str = "rrm5-movie-main-file-selection-v1";

const MOVIE_DOMINANT_MAIN_FILE_RATIO: f64 = 3.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieTarget {
    pub target_id: Uuid,
    pub target_key: String,
    pub title: String,
    pub year: Option<i32>,
    #[serde(default)]
    pub external_ids: ExternalIds,
    #[serde(default)]
    pub tvdb_movie: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieCandidateInput {
    pub title: String,
    #[serde(default)]
    pub source_external_ids: Vec<MovieSourceExternalIds>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieReleaseFileSelectionInput {
    pub file_id: String,
    pub path: String,
    pub size_bytes: Option<i64>,
    pub selectable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieReleaseFileRole {
    MainCandidate,
    SampleOrExtra,
    NonMedia,
    NonSelectable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieReleaseFileSelectionDiagnostic {
    pub file_id: String,
    pub path: String,
    pub size_bytes: Option<i64>,
    pub selectable: bool,
    pub role: MovieReleaseFileRole,
    pub reason: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieMainFileSelectionStatus {
    Approved,
    ReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieMainFileSelection {
    pub policy_version: String,
    pub status: MovieMainFileSelectionStatus,
    pub selected_file_id: Option<String>,
    pub skipped_file_ids: Vec<String>,
    pub review_reasons: Vec<String>,
    pub main_candidate_count: usize,
    pub diagnostics: Vec<MovieReleaseFileSelectionDiagnostic>,
}

impl MovieMainFileSelection {
    pub fn is_approved(&self) -> bool {
        self.status == MovieMainFileSelectionStatus::Approved
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieCoveragePlan {
    pub release_kind: ReleaseKind,
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    pub confidence: ReleaseConfidence,
    pub target_id: Uuid,
    pub target_key: String,
    pub coverage_kind: ReleaseCoverageKind,
    pub parsed_release: Option<MovieParsedRelease>,
    pub graph: MovieIdentityGraph,
    pub reconciliation: MovieReconciliation,
}

impl MovieCoveragePlan {
    pub fn is_planned(&self) -> bool {
        self.reconciliation.outcome == MovieReconciliationOutcome::Planned
            && !matches!(
                self.confidence,
                ReleaseConfidence::ReviewRequired | ReleaseConfidence::Low
            )
    }
}

#[derive(Debug, Default)]
pub struct MovieRadarrStyleResolver;

impl MovieRadarrStyleResolver {
    pub fn plan_candidate(
        &self,
        target: MovieTarget,
        candidate: MovieCandidateInput,
    ) -> MovieCoveragePlan {
        let parsed_release = parse_movie_title(&candidate.title, false);
        let graph = build_movie_identity_graph(MovieIdentityGraphInput {
            target_title: target.title.clone(),
            target_year: target.year,
            target_external_ids: target.external_ids,
            tvdb_movie: target.tvdb_movie,
            candidate_external_ids: candidate.source_external_ids,
        });
        let reconciliation = reconcile_movie_release(parsed_release.as_ref(), &graph);
        MovieCoveragePlan {
            release_kind: ReleaseKind::Single,
            resolver_kind: ReleaseResolverKind::MovieRadarrStyle,
            resolver_version: MOVIE_RADARR_STYLE_RESOLVER_VERSION.to_string(),
            confidence: reconciliation.confidence,
            target_id: target.target_id,
            target_key: target.target_key,
            coverage_kind: ReleaseCoverageKind::Movie,
            parsed_release,
            graph,
            reconciliation,
        }
    }
}

pub fn select_movie_main_file(files: &[MovieReleaseFileSelectionInput]) -> MovieMainFileSelection {
    let mut review_reasons = BTreeSet::new();
    let mut seen_file_ids = BTreeSet::new();
    let mut duplicate_file_ids = BTreeSet::new();
    for file in files {
        if file.file_id.trim().is_empty() {
            review_reasons.insert("missing_file_id".to_string());
        } else if !seen_file_ids.insert(file.file_id.clone()) {
            duplicate_file_ids.insert(file.file_id.clone());
        }
    }
    if !duplicate_file_ids.is_empty() {
        review_reasons.insert("duplicate_file_ids".to_string());
    }
    if files.is_empty() {
        review_reasons.insert("missing_file_list".to_string());
    }

    let classified = files
        .iter()
        .enumerate()
        .map(|(index, file)| (index, file, classify_movie_release_file(file)))
        .collect::<Vec<_>>();
    let main_candidates = classified
        .iter()
        .filter(|(_, _, (role, _))| *role == MovieReleaseFileRole::MainCandidate)
        .map(|(index, file, _)| (*index, *file))
        .collect::<Vec<_>>();

    let selected_file_id = match main_candidates.len() {
        0 => {
            if !files.is_empty() {
                review_reasons.insert("no_movie_media_files".to_string());
            }
            None
        }
        1 => main_candidates
            .first()
            .map(|(_, file)| file.file_id.clone()),
        _ => dominant_movie_main_file(&main_candidates)
            .map(|(_, file)| file.file_id.clone())
            .or_else(|| {
                if main_candidates
                    .iter()
                    .any(|(_, file)| file.size_bytes.filter(|size| *size >= 0).is_none())
                {
                    review_reasons.insert("movie_multi_file_missing_size".to_string());
                } else {
                    review_reasons.insert("ambiguous_movie_main_file".to_string());
                }
                None
            }),
    };

    let selected_file_id = selected_file_id.filter(|_| review_reasons.is_empty());
    if selected_file_id.is_none() && !files.is_empty() {
        review_reasons.insert("no_selected_movie_file".to_string());
    }

    let selected_set = selected_file_id.iter().cloned().collect::<BTreeSet<_>>();
    let mut skipped_file_ids = files
        .iter()
        .filter(|file| !file.file_id.trim().is_empty())
        .filter(|file| !selected_set.contains(&file.file_id))
        .map(|file| file.file_id.clone())
        .collect::<Vec<_>>();
    skipped_file_ids.sort();
    skipped_file_ids.dedup();

    let diagnostics = classified
        .into_iter()
        .map(
            |(_, file, (role, reason))| MovieReleaseFileSelectionDiagnostic {
                file_id: file.file_id.clone(),
                path: file.path.clone(),
                size_bytes: file.size_bytes,
                selectable: file.selectable,
                role,
                reason,
                selected: selected_set.contains(&file.file_id),
            },
        )
        .collect::<Vec<_>>();
    let review_reasons = review_reasons.into_iter().collect::<Vec<_>>();
    let status = if review_reasons.is_empty() && selected_file_id.is_some() {
        MovieMainFileSelectionStatus::Approved
    } else {
        MovieMainFileSelectionStatus::ReviewRequired
    };

    MovieMainFileSelection {
        policy_version: MOVIE_MAIN_FILE_SELECTION_POLICY_VERSION.to_string(),
        status,
        selected_file_id,
        skipped_file_ids,
        review_reasons,
        main_candidate_count: main_candidates.len(),
        diagnostics,
    }
}

fn dominant_movie_main_file<'a>(
    candidates: &[(usize, &'a MovieReleaseFileSelectionInput)],
) -> Option<(usize, &'a MovieReleaseFileSelectionInput)> {
    if candidates.len() <= 1 {
        return candidates.first().copied();
    }
    if candidates
        .iter()
        .any(|(_, file)| file.size_bytes.filter(|size| *size >= 0).is_none())
    {
        return None;
    }
    let mut sorted = candidates.to_vec();
    sorted.sort_by(|(left_index, left), (right_index, right)| {
        right
            .size_bytes
            .unwrap_or_default()
            .cmp(&left.size_bytes.unwrap_or_default())
            .then_with(|| left.file_id.cmp(&right.file_id))
            .then_with(|| left_index.cmp(right_index))
    });
    let largest = sorted[0];
    let second = sorted[1];
    let largest_size = largest.1.size_bytes.unwrap_or_default();
    let second_size = second.1.size_bytes.unwrap_or_default();
    if largest_size <= 0 {
        return None;
    }
    let ratio = largest_size as f64 / second_size.max(1) as f64;
    (ratio >= MOVIE_DOMINANT_MAIN_FILE_RATIO).then_some(largest)
}

fn classify_movie_release_file(
    file: &MovieReleaseFileSelectionInput,
) -> (MovieReleaseFileRole, String) {
    if !file.selectable {
        return (
            MovieReleaseFileRole::NonSelectable,
            "provider_marked_non_selectable".to_string(),
        );
    }
    if !movie_path_has_media_extension(&file.path) {
        return (
            MovieReleaseFileRole::NonMedia,
            "non_movie_media_extension".to_string(),
        );
    }
    if is_movie_sample_or_extra_path(&file.path) {
        return (
            MovieReleaseFileRole::SampleOrExtra,
            "sample_or_extra_media".to_string(),
        );
    }
    (
        MovieReleaseFileRole::MainCandidate,
        "movie_media_candidate".to_string(),
    )
}

pub fn movie_path_has_media_extension(path: &str) -> bool {
    let extension = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(
        extension.as_str(),
        "mkv"
            | "mp4"
            | "m4v"
            | "avi"
            | "mov"
            | "wmv"
            | "ts"
            | "m2ts"
            | "webm"
            | "flv"
            | "mpg"
            | "mpeg"
    )
}

pub fn is_movie_sample_or_extra_path(path: &str) -> bool {
    let lower = path.replace('\\', "/").to_ascii_lowercase();
    let segments = lower
        .split('/')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.iter().any(|segment| {
        matches!(
            *segment,
            "sample"
                | "samples"
                | "extra"
                | "extras"
                | "trailer"
                | "trailers"
                | "featurette"
                | "featurettes"
                | "bonus"
                | "proof"
        ) || *segment == "behind the scenes"
            || *segment == "deleted scenes"
    }) {
        return true;
    }

    let basename = segments.last().copied().unwrap_or(lower.as_str());
    let stem = basename
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(basename);
    let tokens = stem
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect::<BTreeSet<_>>();
    tokens.contains("sample")
        || tokens.contains("samples")
        || tokens.contains("trailer")
        || tokens.contains("trailers")
        || tokens.contains("featurette")
        || tokens.contains("featurettes")
        || tokens.contains("extra")
        || tokens.contains("extras")
        || tokens.contains("bonus")
        || tokens.contains("proof")
        || tokens.contains("interview")
        || tokens.contains("interviews")
        || (tokens.contains("deleted") && (tokens.contains("scene") || tokens.contains("scenes")))
        || (tokens.contains("behind") && tokens.contains("scenes"))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::Value as JsonValue;

    use super::{RADARR_MOVIE_FIXTURE_SET, RADARR_REFERENCE_COMMIT, RADARR_REFERENCE_REPOSITORY};

    const EXPECTED_TOTAL_CASES: u64 = 1098;
    const EXPECTED_ASSERTED_CASES: u64 = 1042;
    const EXPECTED_UNSUPPORTED_POLICY_CASES: u64 = 56;

    fn load_radarr_inventory() -> JsonValue {
        serde_json::from_str(include_str!(
            "fixtures/radarr_rrm0_movie_parser_inventory.json"
        ))
        .expect("valid RR-M0 Radarr movie parser inventory")
    }

    fn string_set(value: &JsonValue, field: &str) -> BTreeSet<String> {
        value[field]
            .as_array()
            .unwrap_or_else(|| panic!("{field} must be an array"))
            .iter()
            .map(|entry| {
                entry
                    .as_str()
                    .unwrap_or_else(|| panic!("{field} entries must be strings"))
                    .to_string()
            })
            .collect()
    }

    fn pending_movie_fixture_rows(payload: &JsonValue) -> Vec<String> {
        let cases = payload["cases"].as_array().expect("fixture cases array");
        let mut pending = Vec::new();

        for case in cases {
            for field in ["classification", "skipReason"] {
                if let Some(value) = case[field].as_str()
                    && value.to_ascii_lowercase().contains("pending")
                {
                    pending.push(format!("{}:{field}={value}", case["id"]));
                }
            }
        }

        pending
    }

    #[test]
    fn rrm0_radarr_movie_parser_inventory_is_pinned_and_classified() {
        let payload = load_radarr_inventory();

        assert_eq!(payload["radarrRepository"], RADARR_REFERENCE_REPOSITORY);
        assert_eq!(payload["radarrCommit"], RADARR_REFERENCE_COMMIT);
        assert_eq!(payload["fixtureSet"], RADARR_MOVIE_FIXTURE_SET);
        assert_eq!(payload["fixtureSchemaVersion"], 1);
        assert_eq!(
            payload["generatedBy"].as_str().expect("generator command"),
            "scripts/extract_radarr_movie_parser_fixtures.py --radarr-root /tmp/radarr-inspect --output elixir-server/src/acquisition/release_resolution/fixtures/radarr_rrm0_movie_parser_inventory.json"
        );
        assert_eq!(
            payload["sourceRoot"].as_str().expect("source root"),
            "src/NzbDrone.Core.Test/ParserTests"
        );

        let fixture_files = string_set(&payload, "fixtureFiles");
        assert_eq!(
            fixture_files,
            [
                "AnimeVersionFixture.cs",
                "CrapParserFixture.cs",
                "EditionParserFixture.cs",
                "ExtendedQualityParserRegex.cs",
                "HashedReleaseFixture.cs",
                "IsoLanguagesFixture.cs",
                "LanguageParserFixture.cs",
                "NormalizeTitleFixture.cs",
                "ParserFixture.cs",
                "QualityParserFixture.cs",
                "ReleaseGroupParserFixture.cs",
                "SceneCheckerFixture.cs",
                "SlugParserFixture.cs",
                "UrlFixture.cs",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        );

        let allowed = string_set(&payload, "allowedClassifications");
        assert_eq!(
            allowed,
            ["movie_rrm_asserted", "unsupported_by_product_policy"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );

        let cases = payload["cases"].as_array().expect("fixture cases array");
        assert_eq!(payload["counts"]["total"], EXPECTED_TOTAL_CASES);
        assert_eq!(
            payload["counts"]["movieRrmAsserted"],
            EXPECTED_ASSERTED_CASES
        );
        assert_eq!(
            payload["counts"]["unsupportedByProductPolicy"],
            EXPECTED_UNSUPPORTED_POLICY_CASES
        );
        assert_eq!(payload["counts"]["movieRrmPending"], 0);
        assert_eq!(cases.len() as u64, EXPECTED_TOTAL_CASES);

        let mut ids = BTreeSet::new();
        let mut counted_asserted = 0_u64;
        let mut counted_unsupported = 0_u64;
        let mut by_fixture = BTreeMap::<String, u64>::new();
        let mut by_test_kind = BTreeMap::<String, u64>::new();

        for case in cases {
            let id = case["id"].as_str().expect("case id");
            assert!(ids.insert(id.to_string()), "duplicate RR-M0 case id {id}");
            assert!(
                case["sourcePath"].as_str().is_some(),
                "{id} missing source path"
            );
            assert!(case["fixture"].as_str().is_some(), "{id} missing fixture");
            assert!(case["method"].as_str().is_some(), "{id} missing method");
            assert!(case["line"].as_u64().is_some(), "{id} missing source line");
            assert!(case["origin"].as_str().is_some(), "{id} missing origin");
            assert!(case["input"].is_string(), "{id} missing input");
            assert!(
                case["testKind"].as_str().is_some(),
                "{id} missing test kind"
            );
            assert!(case["expected"].is_object(), "{id} missing expected object");

            let classification = case["classification"]
                .as_str()
                .expect("classification string");
            assert!(
                allowed.contains(classification),
                "{id} has invalid classification {classification}"
            );
            assert!(
                !classification.contains("pending"),
                "{id} contains pending classification"
            );

            match classification {
                "movie_rrm_asserted" => {
                    counted_asserted += 1;
                    assert!(
                        case.get("skipReason").is_none(),
                        "{id} asserted row must not have skip reason"
                    );
                }
                "unsupported_by_product_policy" => {
                    counted_unsupported += 1;
                    assert_eq!(
                        case["skipReason"], "unsupported_by_product_policy",
                        "{id} unsupported row must carry policy skip reason"
                    );
                    assert!(
                        case["skipNote"]
                            .as_str()
                            .is_some_and(|note| !note.trim().is_empty()),
                        "{id} unsupported row must explain policy boundary"
                    );
                }
                other => panic!("{id} unexpected classification {other}"),
            }

            *by_fixture
                .entry(case["fixture"].as_str().expect("fixture").to_string())
                .or_default() += 1;
            *by_test_kind
                .entry(case["testKind"].as_str().expect("test kind").to_string())
                .or_default() += 1;
        }

        assert_eq!(counted_asserted, EXPECTED_ASSERTED_CASES);
        assert_eq!(counted_unsupported, EXPECTED_UNSUPPORTED_POLICY_CASES);

        for (fixture, count) in &by_fixture {
            assert_eq!(
                payload["counts"]["byFixture"][fixture].as_u64(),
                Some(*count),
                "fixture count mismatch for {fixture}"
            );
        }
        for (test_kind, count) in &by_test_kind {
            assert_eq!(
                payload["counts"]["byTestKind"][test_kind].as_u64(),
                Some(*count),
                "test kind count mismatch for {test_kind}"
            );
        }

        for required_kind in [
            "movie_title",
            "movie_year",
            "movie_external_id",
            "quality",
            "language",
            "release_group",
            "edition",
            "hashed_path",
            "reject_title",
            "scene_title",
            "title_normalization",
        ] {
            assert!(
                by_test_kind.get(required_kind).copied().unwrap_or_default() > 0,
                "RR-M0 inventory missing required test kind {required_kind}"
            );
        }
    }

    #[test]
    fn rrm0_radarr_movie_parser_inventory_has_no_pending_rows() {
        let payload = load_radarr_inventory();
        let pending = pending_movie_fixture_rows(&payload);

        assert!(
            pending.is_empty(),
            "RR-M0 Radarr inventory contains pending rows:\n{}",
            pending.join("\n")
        );
    }

    #[test]
    fn rrmt_movie_production_gate_rejects_pending_fixture_rows() {
        let payload = load_radarr_inventory();
        let cases = payload["cases"].as_array().expect("fixture cases array");
        let allowed = string_set(&payload, "allowedClassifications");

        assert_eq!(payload["radarrRepository"], RADARR_REFERENCE_REPOSITORY);
        assert_eq!(payload["radarrCommit"], RADARR_REFERENCE_COMMIT);
        assert_eq!(payload["fixtureSet"], RADARR_MOVIE_FIXTURE_SET);
        assert_eq!(payload["counts"]["total"], EXPECTED_TOTAL_CASES);
        assert_eq!(
            payload["counts"]["movieRrmAsserted"],
            EXPECTED_ASSERTED_CASES
        );
        assert_eq!(
            payload["counts"]["unsupportedByProductPolicy"],
            EXPECTED_UNSUPPORTED_POLICY_CASES
        );
        assert_eq!(payload["counts"]["movieRrmPending"], 0);
        assert_eq!(cases.len() as u64, EXPECTED_TOTAL_CASES);

        let invalid_classifications = cases
            .iter()
            .filter_map(|case| {
                let id = case["id"].as_str().unwrap_or("<missing-id>");
                let classification = case["classification"].as_str().unwrap_or("<missing>");
                (!allowed.contains(classification)).then(|| format!("{id}:{classification}"))
            })
            .collect::<Vec<_>>();
        assert!(
            invalid_classifications.is_empty(),
            "RR-MT movie fixture gate found non-production classifications:\n{}",
            invalid_classifications.join("\n")
        );

        let pending = pending_movie_fixture_rows(&payload);
        assert!(
            pending.is_empty(),
            "RR-MT movie production fixture gate contains pending rows:\n{}",
            pending.join("\n")
        );
    }
}
