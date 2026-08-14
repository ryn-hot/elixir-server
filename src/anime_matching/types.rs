use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

pub const ANIME_MATCH_SCHEMA_VERSION: u32 = 1;
pub const ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION: u32 = 1;
/// Model-only batch bound qualified on the minimum Intel host. Acquisition
/// may still search and resolve a larger deterministic candidate set; only
/// the highest-ranked difficult candidates cross the local-model boundary.
pub const ANIME_MATCH_MAX_CANDIDATES: usize = 6;
/// Coarse, tokenizer-independent guard for the 4,096-token V1 worker envelope.
/// ALM-6 also applies the bundle's exact tokenizer limit before inference.
pub const ANIME_MATCH_MAX_REQUEST_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AnimeMatchMediaType {
    Anime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeMatchScope {
    Subscription,
    Episode,
    Season,
    Range,
    Missing,
    SelectedTargets,
    AnimeArc,
    Series,
    Movie,
    Special,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeMatchAudioPreferenceMode {
    #[default]
    Any,
    Prefer,
    Require,
    PreferDub,
    RequireDub,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchAudioPreference {
    #[serde(default)]
    pub mode: AnimeMatchAudioPreferenceMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtitle_languages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepted_profiles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchTarget {
    pub media_type: AnimeMatchMediaType,
    pub canonical_title: String,
    pub scope: AnimeMatchScope,
    pub wanted_target_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub episode_numbers: Vec<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absolute_episode_numbers: Vec<i32>,
    #[serde(default)]
    pub audio_preference: AnimeMatchAudioPreference,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AnimeMatchAliasKind {
    Canonical,
    English,
    Romaji,
    Native,
    Synonym,
    Generated,
}

/// Classify an alias consistently across acquisition and library adapters.
/// Explicit language/source evidence wins; script inspection is only a native
/// fallback and deliberately does not treat accented Latin text as Japanese.
pub fn classify_anime_match_alias(
    language: Option<&str>,
    source: Option<&str>,
    value: &str,
) -> AnimeMatchAliasKind {
    let source = source.unwrap_or_default().trim().to_ascii_lowercase();
    if source.starts_with("generated_season") {
        return AnimeMatchAliasKind::Generated;
    }

    let language = language
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-");
    if matches!(language.as_str(), "en" | "eng" | "english") || language.starts_with("en-") {
        AnimeMatchAliasKind::English
    } else if matches!(
        language.as_str(),
        "romaji" | "x-jat" | "ja-latn" | "jpn-latn"
    ) {
        AnimeMatchAliasKind::Romaji
    } else if matches!(
        language.as_str(),
        "ja" | "jpn" | "japanese" | "x-jpn" | "native"
    ) || value.chars().any(is_japanese_script)
    {
        AnimeMatchAliasKind::Native
    } else {
        AnimeMatchAliasKind::Synonym
    }
}

fn is_japanese_script(value: char) -> bool {
    matches!(
        value,
        '\u{3040}'..='\u{30ff}'
            | '\u{31f0}'..='\u{31ff}'
            | '\u{3400}'..='\u{4dbf}'
            | '\u{4e00}'..='\u{9fff}'
            | '\u{f900}'..='\u{faff}'
    )
}

/// NFKC, case, whitespace, and punctuation-insensitive identity used only for
/// alias deduplication/association. Display values remain unchanged on wire.
pub fn anime_match_alias_equivalence_key(value: &str) -> String {
    value
        .nfkc()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchAlias {
    pub value: String,
    pub kind: AnimeMatchAliasKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchContextTarget {
    pub target_key: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absolute_episode_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tvdb_episode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anidb_episode_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchSeasonContext {
    pub season_number: i32,
    pub anilist_id: String,
    pub aliases: Vec<AnimeMatchAlias>,
    pub targets: Vec<AnimeMatchContextTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchContext {
    pub graph_fingerprint: String,
    pub seasons: Vec<AnimeMatchSeasonContext>,
}

/// Keep only wanted targets plus the closest numbering boundaries. This makes
/// the same production-shaped context bound available to every adapter while
/// retaining enough adjacent evidence to distinguish seasonal and absolute
/// numbering.
pub fn scope_anime_match_context(
    mut context: AnimeMatchContext,
    target: &AnimeMatchTarget,
) -> AnimeMatchContext {
    let wanted = target
        .wanted_target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    context.seasons.sort_by(|left, right| {
        left.season_number
            .cmp(&right.season_number)
            .then_with(|| left.anilist_id.cmp(&right.anilist_id))
    });
    for season in &mut context.seasons {
        season.targets.sort_by(anime_context_target_order);
    }

    let preferred_season = target.season_number;
    let mut wanted_season_indexes = BTreeSet::new();
    for (season_index, season) in context.seasons.iter().enumerate() {
        let contains_preferred_wanted = season.targets.iter().any(|candidate| {
            wanted.contains(candidate.target_key.as_str())
                && anime_context_target_matches_preferred_season(
                    season,
                    candidate,
                    preferred_season,
                )
        });
        if contains_preferred_wanted {
            wanted_season_indexes.insert(season_index);
        }
    }
    let ambiguous_wanted_keys = wanted
        .iter()
        .filter_map(|wanted_key| {
            let occurrences = context
                .seasons
                .iter()
                .enumerate()
                .filter(|(season_index, _)| wanted_season_indexes.contains(season_index))
                .map(|(_, season)| {
                    season
                        .targets
                        .iter()
                        .filter(|candidate| {
                            candidate.target_key.as_str() == *wanted_key
                                && anime_context_target_matches_preferred_season(
                                    season,
                                    candidate,
                                    preferred_season,
                                )
                        })
                        .count()
                })
                .sum::<usize>();
            (occurrences > 1).then(|| (*wanted_key).to_string())
        })
        .collect::<BTreeSet<_>>();

    let mut retained_season_indexes = BTreeSet::new();
    for season_index in &wanted_season_indexes {
        retained_season_indexes.insert(*season_index);
        if let Some(previous) = season_index.checked_sub(1) {
            retained_season_indexes.insert(previous);
        }
        if *season_index + 1 < context.seasons.len() {
            retained_season_indexes.insert(*season_index + 1);
        }
    }

    let mut seen_target_keys = BTreeSet::new();
    let seasons = context
        .seasons
        .into_iter()
        .enumerate()
        .filter(|(index, _)| retained_season_indexes.contains(index))
        .map(|(season_index, mut season)| {
            let wanted_target_indexes = if wanted_season_indexes.contains(&season_index) {
                season
                    .targets
                    .iter()
                    .enumerate()
                    .filter_map(|(target_index, candidate)| {
                        (wanted.contains(candidate.target_key.as_str())
                            && anime_context_target_matches_preferred_season(
                                &season,
                                candidate,
                                preferred_season,
                            ))
                        .then_some(target_index)
                    })
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            let mut retained_target_indexes = wanted_target_indexes.clone();
            if let (Some(first), Some(last)) = (
                wanted_target_indexes.iter().next().copied(),
                wanted_target_indexes.iter().next_back().copied(),
            ) {
                if let Some(previous) = first.checked_sub(1) {
                    retained_target_indexes.insert(previous);
                }
                if last + 1 < season.targets.len() {
                    retained_target_indexes.insert(last + 1);
                }
            } else if !season.targets.is_empty() {
                if season_index
                    .checked_sub(1)
                    .is_some_and(|previous| wanted_season_indexes.contains(&previous))
                {
                    retained_target_indexes.insert(0);
                }
                if season_index
                    .checked_add(1)
                    .is_some_and(|next| wanted_season_indexes.contains(&next))
                {
                    retained_target_indexes.insert(season.targets.len() - 1);
                }
            }

            season.targets = season
                .targets
                .into_iter()
                .enumerate()
                .filter(|(index, _)| retained_target_indexes.contains(index))
                .filter_map(|(target_index, candidate)| {
                    let is_wanted = wanted_target_indexes.contains(&target_index);
                    if !is_wanted && wanted.contains(candidate.target_key.as_str()) {
                        return None;
                    }
                    if is_wanted && ambiguous_wanted_keys.contains(&candidate.target_key) {
                        seen_target_keys.insert(candidate.target_key.clone());
                        return Some(candidate);
                    }
                    seen_target_keys
                        .insert(candidate.target_key.clone())
                        .then_some(candidate)
                })
                .collect();
            season
        })
        .collect();

    AnimeMatchContext {
        graph_fingerprint: context.graph_fingerprint,
        seasons,
    }
}

fn anime_context_target_matches_preferred_season(
    season: &AnimeMatchSeasonContext,
    target: &AnimeMatchContextTarget,
    preferred_season: Option<i32>,
) -> bool {
    preferred_season
        .map(|number| {
            target.season_number == Some(number)
                || (target.season_number.is_none() && season.season_number == number)
        })
        .unwrap_or(true)
}

fn anime_context_target_order(
    left: &AnimeMatchContextTarget,
    right: &AnimeMatchContextTarget,
) -> std::cmp::Ordering {
    (
        left.season_number.unwrap_or(i32::MAX),
        left.episode_number.unwrap_or(i32::MAX),
        left.absolute_episode_number.unwrap_or(i32::MAX),
        &left.target_key,
    )
        .cmp(&(
            right.season_number.unwrap_or(i32::MAX),
            right.episode_number.unwrap_or(i32::MAX),
            right.absolute_episode_number.unwrap_or(i32::MAX),
            &right.target_key,
        ))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchParseFacts {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub title_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub season_numbers: Vec<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub episode_numbers: Vec<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absolute_episode_numbers: Vec<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audio_profiles: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchFile {
    pub file_key: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchCandidate {
    pub candidate_key: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<AnimeMatchFile>,
    #[serde(default)]
    pub parse_facts: AnimeMatchParseFacts,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchRequest {
    pub schema_version: u32,
    pub request_id: String,
    pub target: AnimeMatchTarget,
    pub context: AnimeMatchContext,
    pub candidates: Vec<AnimeMatchCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeCandidateMatch {
    pub candidate_key: String,
    pub matched_target_keys: Vec<String>,
    pub audio_profile: AnimeMatchAudioProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_file_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AnimeMatchAudioProfile {
    Unknown,
    DualAudio,
    Subbed,
    Dubbed,
    JaAudioEnSubs,
    EnAudio,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeMatchResponse {
    pub schema_version: u32,
    pub matches: Vec<AnimeCandidateMatch>,
}

/// The only inference-owned decision in the semantic-evidence path. Every
/// entity, coordinate, and target key is authored by Elixir before inference;
/// the model can select one complete interpretation or abstain.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AnimeSemanticMediaKind {
    Episode,
    Range,
    SeasonPack,
    SeriesPack,
    Movie,
    Special,
    Ova,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AnimeSemanticNumbering {
    Seasonal,
    Absolute,
    EntityOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeSemanticEntity {
    pub index: usize,
    pub season_number: i32,
    /// Season numbers release names may use for this provider-owned entity.
    /// This includes the canonical Elixir season plus explicit season markers
    /// found in the entity's own aliases (for example an AniList "Season 3"
    /// alias on canonical season 4).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub release_season_numbers: Vec<i32>,
    pub aliases: Vec<String>,
    /// Private canonical join key. The model selects `index`; it never needs
    /// or returns provider identity.
    #[serde(skip)]
    pub anilist_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeSemanticHypothesis {
    pub index: usize,
    pub entity_index: usize,
    pub numbering: AnimeSemanticNumbering,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub episode_numbers: Vec<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absolute_episode_numbers: Vec<i32>,
    pub media_kind: AnimeSemanticMediaKind,
    /// Private canonical join carried through the service but never serialized
    /// into the model payload. The model sees semantic coordinates, not opaque
    /// database or request target identifiers.
    #[serde(skip)]
    pub target_keys: Vec<String>,
}

/// The user request projected into the semantic selector contract. Provider
/// identifiers remain private; the model sees only the title, coordinates,
/// scope, and audio policy it must compare with one raw release.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeSemanticTarget {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub canonical_title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<AnimeMatchScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub episode_numbers: Vec<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absolute_episode_numbers: Vec<i32>,
    #[serde(default)]
    pub audio_preference: AnimeMatchAudioPreference,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeSemanticEvidenceRequest {
    pub schema_version: u32,
    pub request_id: String,
    pub candidate_key: String,
    pub raw: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_release: Option<String>,
    /// Optional only for backward compatibility with the original 18-case
    /// selector fixture. Every production-built request supplies this field.
    #[serde(default)]
    pub target: AnimeSemanticTarget,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub title_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_season_numbers: Vec<i32>,
    pub graph_fingerprint: String,
    pub entities: Vec<AnimeSemanticEntity>,
    pub hypotheses: Vec<AnimeSemanticHypothesis>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeSemanticEvidenceResponse {
    pub schema_version: u32,
    pub hypothesis_index: Option<usize>,
}

/// Non-serializable adapter envelope. `source` is retained only inside the
/// process and is never sent to an inference engine.
#[derive(Debug, Clone)]
pub struct AnimeMatchFileInput<F> {
    pub source: F,
    pub path: String,
}

/// Non-serializable adapter envelope. Acquisition and library can retain
/// different private source types while producing the same wire request.
#[derive(Debug, Clone)]
pub struct AnimeMatchCandidateInput<C, F> {
    pub source: C,
    pub title: String,
    pub files: Vec<AnimeMatchFileInput<F>>,
    pub parse_facts: AnimeMatchParseFacts,
}

#[derive(Debug, Clone)]
pub struct AnimeMatchBatchInput<C, F> {
    pub request_id: String,
    pub target: AnimeMatchTarget,
    pub context: AnimeMatchContext,
    pub candidates: Vec<AnimeMatchCandidateInput<C, F>>,
}

#[derive(Debug)]
struct AnimeMatchFileSource<F> {
    candidate_key: String,
    source: F,
}

/// Request-local references back to adapter-owned source values. The fields
/// are deliberately private and this type does not implement `Serialize`.
#[derive(Debug)]
pub struct AnimeMatchSourceMap<C, F> {
    candidate_sources: BTreeMap<String, C>,
    file_sources: BTreeMap<String, AnimeMatchFileSource<F>>,
}

impl<C, F> AnimeMatchSourceMap<C, F> {
    pub(crate) fn new(
        candidate_sources: BTreeMap<String, C>,
        file_sources: BTreeMap<String, (String, F)>,
    ) -> Self {
        Self {
            candidate_sources,
            file_sources: file_sources
                .into_iter()
                .map(|(file_key, (candidate_key, source))| {
                    (
                        file_key,
                        AnimeMatchFileSource {
                            candidate_key,
                            source,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn candidate_source(&self, candidate_key: &str) -> Option<&C> {
        self.candidate_sources.get(candidate_key)
    }

    pub fn file_source(&self, candidate_key: &str, file_key: &str) -> Option<&F> {
        self.file_sources
            .get(file_key)
            .filter(|source| source.candidate_key == candidate_key)
            .map(|source| &source.source)
    }

    pub(crate) fn file_candidate_key(&self, file_key: &str) -> Option<&str> {
        self.file_sources
            .get(file_key)
            .map(|source| source.candidate_key.as_str())
    }

    pub fn candidate_count(&self) -> usize {
        self.candidate_sources.len()
    }

    pub fn file_count(&self) -> usize {
        self.file_sources.len()
    }
}

#[derive(Debug)]
pub struct PreparedAnimeMatchRequest<C, F> {
    pub(crate) request: AnimeMatchRequest,
    pub(crate) source_map: AnimeMatchSourceMap<C, F>,
}

impl<C, F> PreparedAnimeMatchRequest<C, F> {
    pub fn request(&self) -> &AnimeMatchRequest {
        &self.request
    }

    pub fn source_map(&self) -> &AnimeMatchSourceMap<C, F> {
        &self.source_map
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn alm5_response_json_round_trip_preserves_optional_file_selection() {
        let response = AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![AnimeCandidateMatch {
                candidate_key: "candidate-0".to_string(),
                matched_target_keys: vec!["S02E01".to_string()],
                audio_profile: AnimeMatchAudioProfile::DualAudio,
                selected_file_keys: None,
            }],
        };

        let encoded = serde_json::to_value(&response).expect("serialize response");
        assert_eq!(encoded["schemaVersion"], ANIME_MATCH_SCHEMA_VERSION);
        assert!(encoded["matches"][0].get("selectedFileKeys").is_none());
        let decoded: AnimeMatchResponse =
            serde_json::from_value(encoded).expect("deserialize response");
        assert_eq!(decoded, response);
    }

    #[test]
    fn alm5_response_wire_contract_rejects_unknown_fields_and_missing_matches() {
        let unknown = json!({
            "schemaVersion": ANIME_MATCH_SCHEMA_VERSION,
            "matches": [],
            "confidence": 0.99
        });
        assert!(serde_json::from_value::<AnimeMatchResponse>(unknown).is_err());

        let missing_matches = json!({ "schemaVersion": ANIME_MATCH_SCHEMA_VERSION });
        assert!(serde_json::from_value::<AnimeMatchResponse>(missing_matches).is_err());

        let nested_unknown = json!({
            "schemaVersion": ANIME_MATCH_SCHEMA_VERSION,
            "matches": [{
                "candidateKey": "candidate-0",
                "matchedTargetKeys": ["S02E01"],
                "audioProfile": "dual_audio",
                "confidence": 0.99
            }]
        });
        assert!(serde_json::from_value::<AnimeMatchResponse>(nested_unknown).is_err());

        let unknown_audio_profile = json!({
            "schemaVersion": ANIME_MATCH_SCHEMA_VERSION,
            "matches": [{
                "candidateKey": "candidate-0",
                "matchedTargetKeys": ["S02E01"],
                "audioProfile": "invented_profile"
            }]
        });
        assert!(serde_json::from_value::<AnimeMatchResponse>(unknown_audio_profile).is_err());
    }

    #[test]
    fn alm5_alias_classification_uses_language_and_script_without_mislabeling_romaji() {
        assert_eq!(
            classify_anime_match_alias(Some("x-jat"), Some("anizip_title"), "Tōkyō Ghoul"),
            AnimeMatchAliasKind::Romaji
        );
        assert_eq!(
            classify_anime_match_alias(None, Some("anilist_season_title"), "Tōkyō Ghoul"),
            AnimeMatchAliasKind::Synonym
        );
        assert_eq!(
            classify_anime_match_alias(None, Some("anilist_season_title"), "東京喰種"),
            AnimeMatchAliasKind::Native
        );
        assert_eq!(
            classify_anime_match_alias(None, Some("generated_season_short"), "Tokyo Ghoul S02"),
            AnimeMatchAliasKind::Generated
        );
        assert_eq!(
            anime_match_alias_equivalence_key("Tokyo Ghoul:Re"),
            anime_match_alias_equivalence_key("Tokyo Ghoul Re"),
            "punctuation variants must share one alias identity"
        );
    }

    #[test]
    fn alm5_context_scope_keeps_wanted_targets_and_only_immediate_numbering_boundaries() {
        let season = |season_number: i32, targets: Vec<(&str, i32, i32)>| AnimeMatchSeasonContext {
            season_number,
            anilist_id: format!("anilist-{season_number}"),
            aliases: vec![AnimeMatchAlias {
                value: format!("Series Season {season_number}"),
                kind: AnimeMatchAliasKind::Generated,
                source: Some("generated_season_ordinal".to_string()),
                language: None,
            }],
            targets: targets
                .into_iter()
                .map(|(target_key, episode, absolute)| AnimeMatchContextTarget {
                    target_key: target_key.to_string(),
                    title: target_key.to_string(),
                    season_number: Some(season_number),
                    episode_number: Some(episode),
                    absolute_episode_number: Some(absolute),
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                })
                .collect(),
        };
        let context = AnimeMatchContext {
            graph_fingerprint: "bounded-context".to_string(),
            seasons: vec![
                season(
                    1,
                    vec![("S01E01", 1, 1), ("S01E02", 2, 2), ("S01E03", 3, 3)],
                ),
                season(
                    2,
                    vec![("S02E01", 1, 4), ("S02E02", 2, 5), ("S02E03", 3, 6)],
                ),
                season(
                    3,
                    vec![("S03E01", 1, 7), ("S03E02", 2, 8), ("S03E03", 3, 9)],
                ),
                season(4, vec![("S04E01", 1, 10)]),
            ],
        };
        let target = AnimeMatchTarget {
            media_type: AnimeMatchMediaType::Anime,
            canonical_title: "Series".to_string(),
            scope: AnimeMatchScope::Episode,
            wanted_target_keys: vec!["S02E02".to_string()],
            season_number: Some(2),
            episode_numbers: vec![2],
            absolute_episode_numbers: vec![5],
            audio_preference: AnimeMatchAudioPreference::default(),
        };

        let scoped = scope_anime_match_context(context, &target);
        assert_eq!(
            scoped
                .seasons
                .iter()
                .map(|season| season.season_number)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            scoped.seasons[0]
                .targets
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S01E03"]
        );
        assert_eq!(
            scoped.seasons[1]
                .targets
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E01", "S02E02", "S02E03"]
        );
        assert_eq!(
            scoped.seasons[2]
                .targets
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S03E01"]
        );
    }

    #[test]
    fn alm5_context_scope_reserves_wanted_key_across_neighbor_collisions() {
        let target_entry = |season_number: i32| AnimeMatchContextTarget {
            target_key: "A0001".to_string(),
            title: format!("Season {season_number} absolute one"),
            season_number: Some(season_number),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            tvdb_episode_id: None,
            anidb_episode_id: None,
        };
        let context = AnimeMatchContext {
            graph_fingerprint: "collision-context".to_string(),
            seasons: vec![
                AnimeMatchSeasonContext {
                    season_number: 1,
                    anilist_id: "one".to_string(),
                    aliases: Vec::new(),
                    targets: vec![target_entry(1)],
                },
                AnimeMatchSeasonContext {
                    season_number: 2,
                    anilist_id: "two".to_string(),
                    aliases: Vec::new(),
                    targets: vec![target_entry(2)],
                },
            ],
        };
        let target = AnimeMatchTarget {
            media_type: AnimeMatchMediaType::Anime,
            canonical_title: "Series".to_string(),
            scope: AnimeMatchScope::Episode,
            wanted_target_keys: vec!["A0001".to_string()],
            season_number: Some(2),
            episode_numbers: vec![1],
            absolute_episode_numbers: vec![1],
            audio_preference: AnimeMatchAudioPreference::default(),
        };

        let scoped = scope_anime_match_context(context, &target);
        let retained = scoped
            .seasons
            .iter()
            .flat_map(|season| season.targets.iter())
            .collect::<Vec<_>>();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].season_number, Some(2));
        assert_eq!(retained[0].target_key, "A0001");
    }

    #[test]
    fn alm5_context_scope_does_not_rebind_wanted_key_to_wrong_preferred_season() {
        let entry = |target_key: &str, season_number: i32| AnimeMatchContextTarget {
            target_key: target_key.to_string(),
            title: target_key.to_string(),
            season_number: Some(season_number),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            tvdb_episode_id: None,
            anidb_episode_id: None,
        };
        let scoped = scope_anime_match_context(
            AnimeMatchContext {
                graph_fingerprint: "wrong-season-key".to_string(),
                seasons: vec![
                    AnimeMatchSeasonContext {
                        season_number: 1,
                        anilist_id: "one".to_string(),
                        aliases: Vec::new(),
                        targets: vec![entry("A0001", 1)],
                    },
                    AnimeMatchSeasonContext {
                        season_number: 2,
                        anilist_id: "two".to_string(),
                        aliases: Vec::new(),
                        targets: vec![entry("S02E01", 2)],
                    },
                ],
            },
            &AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Series".to_string(),
                scope: AnimeMatchScope::Episode,
                wanted_target_keys: vec!["A0001".to_string()],
                season_number: Some(2),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![1],
                audio_preference: AnimeMatchAudioPreference::default(),
            },
        );

        assert!(
            scoped
                .seasons
                .iter()
                .flat_map(|season| season.targets.iter())
                .all(|target| target.target_key != "A0001")
        );
    }

    #[test]
    fn alm5_context_scope_filters_wrong_season_collision_in_multi_key_request() {
        let entry = |target_key: &str, season_number: i32, title: &str| AnimeMatchContextTarget {
            target_key: target_key.to_string(),
            title: title.to_string(),
            season_number: Some(season_number),
            episode_number: Some(1),
            absolute_episode_number: Some(14),
            tvdb_episode_id: None,
            anidb_episode_id: None,
        };
        let scoped = scope_anime_match_context(
            AnimeMatchContext {
                graph_fingerprint: "multi-key-season-collision".to_string(),
                seasons: vec![
                    AnimeMatchSeasonContext {
                        season_number: 2,
                        anilist_id: "relation-two".to_string(),
                        aliases: Vec::new(),
                        targets: vec![
                            entry("S03E01", 3, "valid relation-two target"),
                            entry("A0014", 4, "wrong preferred season"),
                        ],
                    },
                    AnimeMatchSeasonContext {
                        season_number: 3,
                        anilist_id: "relation-three".to_string(),
                        aliases: Vec::new(),
                        targets: vec![entry("A0014", 3, "valid absolute target")],
                    },
                ],
            },
            &AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Series".to_string(),
                scope: AnimeMatchScope::SelectedTargets,
                wanted_target_keys: vec!["S03E01".to_string(), "A0014".to_string()],
                season_number: Some(3),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![14],
                audio_preference: AnimeMatchAudioPreference::default(),
            },
        );

        let retained = scoped
            .seasons
            .iter()
            .flat_map(|season| season.targets.iter())
            .collect::<Vec<_>>();
        assert_eq!(retained.len(), 2);
        assert!(retained.iter().any(|target| target.target_key == "S03E01"));
        assert!(retained.iter().any(|target| {
            target.target_key == "A0014" && target.title == "valid absolute target"
        }));
        assert!(
            retained
                .iter()
                .all(|target| target.title != "wrong preferred season")
        );
    }

    #[test]
    fn alm5_context_scope_keeps_same_season_wanted_collisions_invalid() {
        let target_entry = |title: &str| AnimeMatchContextTarget {
            target_key: "A0001".to_string(),
            title: title.to_string(),
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            tvdb_episode_id: None,
            anidb_episode_id: None,
        };
        let scoped = scope_anime_match_context(
            AnimeMatchContext {
                graph_fingerprint: "same-season-collision".to_string(),
                seasons: vec![AnimeMatchSeasonContext {
                    season_number: 1,
                    anilist_id: "one".to_string(),
                    aliases: Vec::new(),
                    targets: vec![target_entry("first"), target_entry("second")],
                }],
            },
            &AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Series".to_string(),
                scope: AnimeMatchScope::Episode,
                wanted_target_keys: vec!["A0001".to_string()],
                season_number: Some(1),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![1],
                audio_preference: AnimeMatchAudioPreference::default(),
            },
        );

        assert_eq!(scoped.seasons[0].targets.len(), 2);
    }

    #[test]
    fn alm5_context_scope_keeps_both_boundaries_between_wanted_seasons() {
        let season = |season_number: i32| AnimeMatchSeasonContext {
            season_number,
            anilist_id: format!("season-{season_number}"),
            aliases: Vec::new(),
            targets: (1..=3)
                .map(|episode| AnimeMatchContextTarget {
                    target_key: format!("S{season_number:02}E{episode:02}"),
                    title: format!("Season {season_number} Episode {episode}"),
                    season_number: Some(season_number),
                    episode_number: Some(episode),
                    absolute_episode_number: None,
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                })
                .collect(),
        };
        let scoped = scope_anime_match_context(
            AnimeMatchContext {
                graph_fingerprint: "split-wanted-seasons".to_string(),
                seasons: vec![season(1), season(2), season(3), season(4)],
            },
            &AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Series".to_string(),
                scope: AnimeMatchScope::SelectedTargets,
                wanted_target_keys: vec!["S01E02".to_string(), "S03E02".to_string()],
                season_number: None,
                episode_numbers: vec![2],
                absolute_episode_numbers: Vec::new(),
                audio_preference: AnimeMatchAudioPreference::default(),
            },
        );

        let middle = scoped
            .seasons
            .iter()
            .find(|season| season.season_number == 2)
            .expect("middle boundary season");
        assert_eq!(
            middle
                .targets
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E01", "S02E03"]
        );
    }
}
