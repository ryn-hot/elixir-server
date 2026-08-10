use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::{Result, bail};

use crate::anime_matching::{
    AnimeMatchAlias, AnimeMatchAliasKind, AnimeMatchAudioPreference, AnimeMatchBatchInput,
    AnimeMatchCandidateInput, AnimeMatchContext, AnimeMatchContextTarget, AnimeMatchFileInput,
    AnimeMatchParseFacts, AnimeMatchSeasonContext, AnimeMatchTarget, AnimeMatchingService,
    PreparedAnimeMatchRequest, anime_match_alias_equivalence_key, classify_anime_match_alias,
    scope_anime_match_context,
};

use super::{
    AniListSeasonChainEntry, AniZipMapping, anizip_prefers_mainline_numbering,
    resolve_anizip_target_numbers,
};

/// Library-owned metadata for one season in a model request. Scan execution
/// and episode persistence remain library concerns on the private side of the
/// shared matching boundary.
#[derive(Debug, Clone)]
pub(crate) struct LibraryAnimeMatchSeasonInput {
    pub season: AniListSeasonChainEntry,
    pub mapping: Option<AniZipMapping>,
}

/// One local file becomes one model candidate. V1 cannot safely express a
/// many-file library group where every file may map to a different episode.
#[derive(Debug, Clone)]
pub(crate) struct LibraryAnimeMatchFileInput {
    /// Original library path. It is retained in the private source map.
    pub path: String,
    /// Optional release-level label when a file source supplied one. The
    /// basename is used when this is absent.
    pub candidate_title: Option<String>,
    pub parse_facts: AnimeMatchParseFacts,
}

#[derive(Debug, Clone)]
pub(crate) struct LibraryAnimeMatchRequestInput {
    pub request_id: String,
    pub target: AnimeMatchTarget,
    pub graph_fingerprint: String,
    pub seasons: Vec<LibraryAnimeMatchSeasonInput>,
    pub files: Vec<LibraryAnimeMatchFileInput>,
}

pub(crate) type PreparedLibraryAnimeMatchRequest = PreparedAnimeMatchRequest<String, String>;

/// Construct the private-source batch consumed by `AnimeMatchingService`.
/// Keeping this as the only production adapter guarantees that contract tests
/// and the live scan path assign identical candidate/file keys and context.
pub(crate) fn library_anime_match_batch_input(
    mut input: LibraryAnimeMatchRequestInput,
) -> Result<AnimeMatchBatchInput<String, String>> {
    normalize_target(&mut input.target);
    let canonical_title = input.target.canonical_title.clone();
    let seasons = build_season_contexts(&canonical_title, input.seasons)?;
    let context = scope_anime_match_context(
        AnimeMatchContext {
            graph_fingerprint: input.graph_fingerprint.trim().to_string(),
            seasons,
        },
        &input.target,
    );

    input.files.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.candidate_title.cmp(&right.candidate_title))
    });
    input.files.dedup_by(|left, right| left.path == right.path);

    let candidates = input
        .files
        .into_iter()
        .map(|mut file| {
            let private_path = file.path.trim().to_string();
            let wire_path = library_match_filename(&private_path);
            let title = file
                .candidate_title
                .take()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| wire_path.clone());
            normalize_parse_facts(&mut file.parse_facts);
            AnimeMatchCandidateInput {
                source: private_path.clone(),
                title,
                files: vec![AnimeMatchFileInput {
                    source: private_path,
                    path: wire_path,
                }],
                parse_facts: file.parse_facts,
            }
        })
        .collect();

    Ok(AnimeMatchBatchInput {
        request_id: input.request_id.trim().to_string(),
        target: input.target,
        context,
        candidates,
    })
}

/// Construct the library side of the shared V1 matching contract without
/// executing inference or changing deterministic scan state.
pub(crate) fn prepare_library_anime_match_request(
    input: LibraryAnimeMatchRequestInput,
) -> Result<PreparedLibraryAnimeMatchRequest> {
    AnimeMatchingService::prepare_request(library_anime_match_batch_input(input)?)
        .map_err(Into::into)
}

fn build_season_contexts(
    canonical_title: &str,
    seasons: Vec<LibraryAnimeMatchSeasonInput>,
) -> Result<Vec<AnimeMatchSeasonContext>> {
    let mut unique = BTreeMap::<(i32, String), LibraryAnimeMatchSeasonInput>::new();
    let mut relation_season_by_anilist_id = BTreeMap::<String, i32>::new();
    let mut anilist_id_by_relation_season = BTreeMap::<i32, String>::new();
    for mut input in seasons {
        let mapping_anilist_id = input
            .mapping
            .as_ref()
            .and_then(|mapping| mapping.ids.anilist.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        input.season.anilist_id = input.season.anilist_id.trim().to_string();
        if !input.season.anilist_id.is_empty()
            && mapping_anilist_id.as_deref().is_some_and(|mapping_id| {
                !mapping_id.eq_ignore_ascii_case(&input.season.anilist_id)
            })
        {
            bail!(
                "library anime matcher season {} has conflicting AniList identities",
                input.season.season_number
            );
        }
        if input.season.anilist_id.is_empty() {
            input.season.anilist_id = mapping_anilist_id.unwrap_or_default();
        }
        if input.season.anilist_id.is_empty() || input.season.season_number < 0 {
            continue;
        }
        let normalized_anilist_id = input.season.anilist_id.to_ascii_lowercase();
        if let Some(existing_season) = relation_season_by_anilist_id
            .insert(normalized_anilist_id.clone(), input.season.season_number)
            && existing_season != input.season.season_number
        {
            bail!(
                "library anime matcher AniList identity '{}' is assigned to relation seasons {} and {}",
                input.season.anilist_id,
                existing_season,
                input.season.season_number
            );
        }
        if let Some(existing_anilist_id) =
            anilist_id_by_relation_season.get(&input.season.season_number)
            && !existing_anilist_id.eq_ignore_ascii_case(&input.season.anilist_id)
        {
            bail!(
                "library anime matcher relation season {} is assigned conflicting AniList identities '{}' and '{}'",
                input.season.season_number,
                existing_anilist_id,
                input.season.anilist_id
            );
        }
        anilist_id_by_relation_season
            .entry(input.season.season_number)
            .or_insert_with(|| input.season.anilist_id.clone());
        let key = (input.season.season_number, normalized_anilist_id);
        match unique.get_mut(&key) {
            Some(current) => {
                match (&current.mapping, &input.mapping) {
                    (Some(current_mapping), Some(input_mapping))
                        if serde_json::to_value(current_mapping)?
                            != serde_json::to_value(input_mapping)? =>
                    {
                        bail!(
                            "library anime matcher relation season {} AniList identity '{}' has conflicting ani.zip mappings",
                            input.season.season_number,
                            input.season.anilist_id
                        );
                    }
                    (None, Some(_)) => current.mapping = input.mapping,
                    _ => {}
                }
                if current.season.title.trim().is_empty() && !input.season.title.trim().is_empty() {
                    current.season.title = input.season.title;
                }
            }
            None => {
                unique.insert(key, input);
            }
        }
    }

    Ok(unique
        .into_values()
        .map(|input| build_season_context(canonical_title, input))
        .collect())
}

fn build_season_context(
    canonical_title: &str,
    input: LibraryAnimeMatchSeasonInput,
) -> AnimeMatchSeasonContext {
    let season_number = input.season.season_number;
    let mut aliases = BTreeMap::<String, AnimeMatchAlias>::new();

    insert_alias(
        &mut aliases,
        AnimeMatchAlias {
            value: canonical_title.trim().to_string(),
            kind: AnimeMatchAliasKind::Canonical,
            source: Some("canonical_title".to_string()),
            language: None,
        },
    );

    if let Some(mapping) = input.mapping.as_ref() {
        let mut localized_titles = mapping.titles.iter().collect::<Vec<_>>();
        localized_titles
            .sort_by(|left, right| left.0.cmp(right.0).then_with(|| left.1.cmp(right.1)));
        for (language, value) in localized_titles {
            insert_alias(
                &mut aliases,
                AnimeMatchAlias {
                    value: value.trim().to_string(),
                    kind: classify_anime_match_alias(Some(language), Some("anizip_title"), value),
                    source: Some("anizip_title".to_string()),
                    language: nonempty_string(language),
                },
            );
        }
    }

    let season_title = input.season.title.trim();
    if !season_title.is_empty() {
        let kind = if anime_match_alias_equivalence_key(season_title)
            == anime_match_alias_equivalence_key(canonical_title)
        {
            AnimeMatchAliasKind::Canonical
        } else {
            classify_anime_match_alias(None, Some("anilist_season_title"), season_title)
        };
        insert_alias(
            &mut aliases,
            AnimeMatchAlias {
                value: season_title.to_string(),
                kind,
                source: Some("anilist_season_title".to_string()),
                language: None,
            },
        );
    }

    if season_number > 1 && !canonical_title.trim().is_empty() {
        for (value, source) in [
            (
                format!("{} Season {season_number}", canonical_title.trim()),
                "generated_season_ordinal",
            ),
            (
                format!("{} S{season_number}", canonical_title.trim()),
                "generated_season_short",
            ),
            (
                format!("{} S{season_number:02}", canonical_title.trim()),
                "generated_season_short",
            ),
        ] {
            insert_alias(
                &mut aliases,
                AnimeMatchAlias {
                    value,
                    kind: AnimeMatchAliasKind::Generated,
                    source: Some(source.to_string()),
                    language: None,
                },
            );
        }
    }

    let mut aliases = aliases.into_values().collect::<Vec<_>>();
    aliases.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| {
                anime_match_alias_equivalence_key(&left.value)
                    .cmp(&anime_match_alias_equivalence_key(&right.value))
            })
            .then_with(|| left.source.cmp(&right.source))
    });

    let targets = input
        .mapping
        .as_ref()
        .map(|mapping| build_mapping_targets(canonical_title, season_number, mapping))
        .unwrap_or_default();

    AnimeMatchSeasonContext {
        season_number,
        anilist_id: input.season.anilist_id,
        aliases,
        targets,
    }
}

fn insert_alias(aliases: &mut BTreeMap<String, AnimeMatchAlias>, alias: AnimeMatchAlias) {
    let key = anime_match_alias_equivalence_key(&alias.value);
    if key.is_empty() {
        return;
    }
    let replace = aliases
        .get(&key)
        .map(|current| alias_precedence(alias.kind) > alias_precedence(current.kind))
        .unwrap_or(true);
    if replace {
        aliases.insert(key, alias);
    }
}

fn alias_precedence(kind: AnimeMatchAliasKind) -> u8 {
    match kind {
        AnimeMatchAliasKind::Canonical => 6,
        AnimeMatchAliasKind::English => 5,
        AnimeMatchAliasKind::Romaji => 4,
        AnimeMatchAliasKind::Native => 3,
        AnimeMatchAliasKind::Generated => 2,
        AnimeMatchAliasKind::Synonym => 1,
    }
}

pub(crate) fn build_mapping_targets(
    canonical_title: &str,
    context_season: i32,
    mapping: &AniZipMapping,
) -> Vec<AnimeMatchContextTarget> {
    let prefer_mainline_numbering = anizip_prefers_mainline_numbering(mapping);
    let mut targets = BTreeMap::<String, AnimeMatchContextTarget>::new();

    for episode in &mapping.episodes {
        let (season_number, episode_number, absolute_episode_number) =
            resolve_anizip_target_numbers(context_season, prefer_mainline_numbering, episode);
        let Some(target_key) =
            anime_match_target_key(season_number, episode_number, absolute_episode_number)
        else {
            continue;
        };
        let target = AnimeMatchContextTarget {
            title: episode
                .title
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("{} {target_key}", canonical_title.trim())),
            target_key: target_key.clone(),
            season_number,
            episode_number,
            absolute_episode_number,
            tvdb_episode_id: normalized_optional_id(episode.tvdb_id.as_deref()),
            anidb_episode_id: normalized_optional_id(episode.anidb_eid.as_deref()),
        };
        let replace = targets
            .get(&target_key)
            .map(|current| {
                target_evidence_score(&target) > target_evidence_score(current)
                    || (target_evidence_score(&target) == target_evidence_score(current)
                        && target_tie_key(&target) < target_tie_key(current))
            })
            .unwrap_or(true);
        if replace {
            targets.insert(target_key, target);
        }
    }

    let mut targets = targets.into_values().collect::<Vec<_>>();
    targets.sort_by_key(|target| {
        (
            target.season_number.unwrap_or(i32::MAX),
            target.episode_number.unwrap_or(i32::MAX),
            target.absolute_episode_number.unwrap_or(i32::MAX),
            target.target_key.clone(),
        )
    });
    targets
}

/// Keep this convention aligned with acquisition's canonical graph target
/// keys. Seasonal numbering wins when the season is non-negative (including
/// canonical season-zero specials) and the episode is positive.
fn anime_match_target_key(
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
) -> Option<String> {
    if let (Some(season), Some(episode)) = (season_number, episode_number)
        && season >= 0
        && episode > 0
    {
        return Some(format!("S{season:02}E{episode:02}"));
    }
    absolute_episode_number
        .filter(|number| *number > 0)
        .map(|number| format!("A{number:04}"))
}

fn target_evidence_score(target: &AnimeMatchContextTarget) -> u8 {
    u8::from(target.tvdb_episode_id.is_some()) * 4
        + u8::from(target.anidb_episode_id.is_some()) * 5
        + u8::from(target.absolute_episode_number.is_some())
}

fn target_tie_key(target: &AnimeMatchContextTarget) -> (String, String, String) {
    (
        target.title.clone(),
        target.tvdb_episode_id.clone().unwrap_or_default(),
        target.anidb_episode_id.clone().unwrap_or_default(),
    )
}

fn normalize_target(target: &mut AnimeMatchTarget) {
    target.canonical_title = target.canonical_title.trim().to_string();
    normalize_string_vec(&mut target.wanted_target_keys);
    normalize_positive_numbers(&mut target.episode_numbers);
    normalize_positive_numbers(&mut target.absolute_episode_numbers);
    normalize_audio_preference(&mut target.audio_preference);
}

fn normalize_audio_preference(preference: &mut AnimeMatchAudioPreference) {
    normalize_string_vec(&mut preference.languages);
    normalize_string_vec(&mut preference.subtitle_languages);
    normalize_string_vec(&mut preference.accepted_profiles);
}

fn normalize_parse_facts(facts: &mut AnimeMatchParseFacts) {
    normalize_string_vec(&mut facts.title_candidates);
    facts.season_numbers.retain(|number| *number >= 0);
    facts.season_numbers.sort_unstable();
    facts.season_numbers.dedup();
    normalize_positive_numbers(&mut facts.episode_numbers);
    normalize_positive_numbers(&mut facts.absolute_episode_numbers);
    normalize_string_vec(&mut facts.audio_profiles);
    normalize_string_vec(&mut facts.languages);
    facts.release_kind = facts
        .release_kind
        .take()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    facts.batch_kind = facts
        .batch_kind
        .take()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
}

fn normalize_string_vec(values: &mut Vec<String>) {
    let normalized = std::mem::take(values)
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    *values = normalized.into_iter().collect();
}

fn normalize_positive_numbers(values: &mut Vec<i32>) {
    values.retain(|number| *number > 0);
    values.sort_unstable();
    values.dedup();
}

fn library_match_filename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(path.trim())
        .to_string()
}

fn normalized_optional_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn nonempty_string(value: &str) -> Option<String> {
    normalized_optional_id(Some(value))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use crate::{
        anime_matching::{
            AnimeMatchAudioPreferenceMode, AnimeMatchMediaType, AnimeMatchRequest, AnimeMatchScope,
        },
        extensions::ExternalIds,
        library::AniZipEpisodeRecord,
    };

    use super::*;

    fn tokyo_ghoul_season() -> AniListSeasonChainEntry {
        AniListSeasonChainEntry {
            season_number: 2,
            anilist_id: "1002".to_string(),
            title: "Tokyo Ghoul Root A".to_string(),
            format: Some("TV".to_string()),
            season_year: Some(2015),
            start_year: Some(2015),
            status: Some("FINISHED".to_string()),
            episodes: Some(12),
            next_airing_episode: None,
            next_airing_at: None,
            confidence: 1.0,
        }
    }

    fn tokyo_ghoul_mapping() -> AniZipMapping {
        AniZipMapping {
            ids: ExternalIds {
                anilist: Some("1002".to_string()),
                tvdb_series: Some("305014".to_string()),
                ..Default::default()
            },
            episodes: vec![AniZipEpisodeRecord {
                season_number: Some(2),
                episode_number: Some(1),
                absolute_episode_number: Some(13),
                episode_label: Some("13".to_string()),
                mainline_episode_number: Some(13),
                title: Some("New Surge".to_string()),
                overview: None,
                runtime_minutes: Some(24),
                image: None,
                tvdb_id: Some("2013".to_string()),
                anidb_eid: Some("3013".to_string()),
                raw: json!({
                    "episode": "13",
                    "seasonNumber": 2,
                    "episodeNumber": 1,
                    "absoluteEpisodeNumber": 13
                }),
            }],
            images: Vec::new(),
            titles: HashMap::from([
                ("en".to_string(), "Tokyo Ghoul Root A".to_string()),
                ("x-jat".to_string(), "Tokyo Ghoul √A".to_string()),
                ("ja".to_string(), "東京喰種トーキョーグール√A".to_string()),
            ]),
        }
    }

    fn tokyo_ghoul_input() -> LibraryAnimeMatchRequestInput {
        LibraryAnimeMatchRequestInput {
            request_id: "search-group-id".to_string(),
            target: AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Tokyo Ghoul".to_string(),
                scope: AnimeMatchScope::Episode,
                wanted_target_keys: vec!["S02E01".to_string()],
                season_number: Some(2),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![13],
                audio_preference: AnimeMatchAudioPreference {
                    mode: AnimeMatchAudioPreferenceMode::PreferDub,
                    languages: vec!["en".to_string()],
                    subtitle_languages: Vec::new(),
                    accepted_profiles: vec![
                        "en_audio".to_string(),
                        "dual_audio".to_string(),
                        "dubbed".to_string(),
                    ],
                },
            },
            graph_fingerprint: "rr3-scoped-tokyo-ghoul".to_string(),
            seasons: vec![LibraryAnimeMatchSeasonInput {
                season: tokyo_ghoul_season(),
                mapping: Some(tokyo_ghoul_mapping()),
            }],
            files: vec![LibraryAnimeMatchFileInput {
                path: "/private/library/Tokyo Ghoul Root A - 01.mkv".to_string(),
                candidate_title: Some(
                    "[Group] Tokyo Ghoul Root A - 01 [1080p] [Dual Audio]".to_string(),
                ),
                parse_facts: AnimeMatchParseFacts {
                    title_candidates: vec![
                        "Tokyo Ghoul Root A".to_string(),
                        "Tokyo Ghoul Root A - 01 [ ] [Dual Audio]".to_string(),
                        "tokyoghoulroota".to_string(),
                    ],
                    season_numbers: Vec::new(),
                    episode_numbers: vec![1],
                    absolute_episode_numbers: vec![1],
                    release_kind: None,
                    batch_kind: Some("single".to_string()),
                    audio_profiles: vec!["dual_audio".to_string()],
                    languages: Vec::new(),
                },
            }],
        }
    }

    /// Reusable by the acquisition-side cross-adapter assertion.
    pub(crate) fn tokyo_ghoul_library_request_fixture() -> AnimeMatchRequest {
        prepare_library_anime_match_request(tokyo_ghoul_input())
            .expect("valid Tokyo Ghoul library request")
            .request()
            .clone()
    }

    /// Relation ordering and provider episode numbering are independent. This
    /// fixture keeps Root A in relation season 2 while its mapped target uses
    /// TVDB-style S03 numbering.
    pub(crate) fn tokyo_ghoul_relation_season_mismatch_request_fixture() -> AnimeMatchRequest {
        let mut input = tokyo_ghoul_input();
        input.target.wanted_target_keys = vec!["S03E01".to_string()];
        input.target.season_number = Some(3);
        input.seasons[0]
            .mapping
            .as_mut()
            .expect("fixture mapping")
            .episodes[0]
            .season_number = Some(3);
        input.seasons[0]
            .mapping
            .as_mut()
            .expect("fixture mapping")
            .episodes[0]
            .mainline_episode_number = None;
        prepare_library_anime_match_request(input)
            .expect("valid relation/episode-season mismatch request")
            .request()
            .clone()
    }

    #[test]
    fn alm5_library_adapter_builds_scoped_tokyo_ghoul_request() -> Result<()> {
        let prepared = prepare_library_anime_match_request(tokyo_ghoul_input())?;
        let request = prepared.request();

        assert_eq!(request.target.canonical_title, "Tokyo Ghoul");
        assert_eq!(request.target.scope, AnimeMatchScope::Episode);
        assert_eq!(request.target.wanted_target_keys, vec!["S02E01"]);
        assert_eq!(request.target.season_number, Some(2));
        assert_eq!(request.target.episode_numbers, vec![1]);
        assert_eq!(request.target.absolute_episode_numbers, vec![13]);
        assert_eq!(request.context.graph_fingerprint, "rr3-scoped-tokyo-ghoul");
        assert_eq!(
            request.target.audio_preference.mode,
            AnimeMatchAudioPreferenceMode::PreferDub
        );
        assert_eq!(request.target.audio_preference.languages, vec!["en"]);
        assert_eq!(
            request.target.audio_preference.accepted_profiles,
            vec!["dual_audio", "dubbed", "en_audio"]
        );

        let season = request.context.seasons.first().expect("season context");
        assert_eq!(season.season_number, 2);
        assert_eq!(season.anilist_id, "1002");
        assert!(season.aliases.iter().any(|alias| {
            alias.value == "Tokyo Ghoul Root A" && alias.kind == AnimeMatchAliasKind::English
        }));
        assert!(season.aliases.iter().any(|alias| {
            alias.value == "Tokyo Ghoul √A" && alias.kind == AnimeMatchAliasKind::Romaji
        }));
        assert!(season.aliases.iter().any(|alias| {
            alias.value == "東京喰種トーキョーグール√A" && alias.kind == AnimeMatchAliasKind::Native
        }));
        assert!(season.aliases.iter().any(|alias| {
            alias.value == "Tokyo Ghoul Season 2" && alias.kind == AnimeMatchAliasKind::Generated
        }));
        assert!(season.aliases.iter().any(|alias| {
            alias.value == "Tokyo Ghoul"
                && alias.kind == AnimeMatchAliasKind::Canonical
                && alias.source.as_deref() == Some("canonical_title")
        }));
        assert_eq!(season.targets.len(), 1);
        assert_eq!(season.targets[0].target_key, "S02E01");
        assert_eq!(season.targets[0].title, "New Surge");
        assert_eq!(season.targets[0].season_number, Some(2));
        assert_eq!(season.targets[0].episode_number, Some(1));
        assert_eq!(season.targets[0].absolute_episode_number, Some(13));
        assert_eq!(season.targets[0].tvdb_episode_id.as_deref(), Some("2013"));
        assert_eq!(season.targets[0].anidb_episode_id.as_deref(), Some("3013"));

        assert_eq!(request.candidates.len(), 1);
        assert_eq!(request.candidates[0].candidate_key, "candidate-0");
        assert_eq!(
            request.candidates[0].title,
            "[Group] Tokyo Ghoul Root A - 01 [1080p] [Dual Audio]"
        );
        assert_eq!(
            request.candidates[0].files[0].path,
            "Tokyo Ghoul Root A - 01.mkv"
        );
        assert_eq!(
            request.candidates[0].parse_facts.audio_profiles,
            vec!["dual_audio"]
        );
        assert!(request.candidates[0].parse_facts.languages.is_empty());

        let original_path = "/private/library/Tokyo Ghoul Root A - 01.mkv";
        assert_eq!(
            prepared
                .source_map()
                .candidate_source("candidate-0")
                .map(String::as_str),
            Some(original_path)
        );
        assert_eq!(
            prepared
                .source_map()
                .file_source("candidate-0", "candidate-0-file-0")
                .map(String::as_str),
            Some(original_path)
        );
        let serialized = serde_json::to_string(request)?;
        assert!(!serialized.contains("/private/library"));
        Ok(())
    }

    #[test]
    fn alm5_library_adapter_keeps_relation_season_when_episode_numbering_differs() {
        let request = tokyo_ghoul_relation_season_mismatch_request_fixture();
        assert_eq!(request.target.season_number, Some(3));
        assert_eq!(request.target.wanted_target_keys, vec!["S03E01"]);
        assert_eq!(request.context.seasons.len(), 1);
        assert_eq!(request.context.seasons[0].season_number, 2);
        assert_eq!(request.context.seasons[0].anilist_id, "1002");
        assert_eq!(request.context.seasons[0].targets.len(), 1);
        assert_eq!(request.context.seasons[0].targets[0].target_key, "S03E01");
        assert_eq!(request.context.seasons[0].targets[0].season_number, Some(3));
    }

    #[test]
    fn alm5_library_adapter_assigns_keys_after_stable_path_ordering() -> Result<()> {
        let mut input = tokyo_ghoul_input();
        input.files = vec![
            LibraryAnimeMatchFileInput {
                path: "/media/z-last.mkv".to_string(),
                candidate_title: None,
                parse_facts: AnimeMatchParseFacts::default(),
            },
            LibraryAnimeMatchFileInput {
                path: "/media/a-first.mkv".to_string(),
                candidate_title: None,
                parse_facts: AnimeMatchParseFacts::default(),
            },
        ];
        let prepared = prepare_library_anime_match_request(input)?;

        assert_eq!(prepared.request().candidates[0].title, "a-first.mkv");
        assert_eq!(prepared.request().candidates[1].title, "z-last.mkv");
        assert_eq!(
            prepared
                .source_map()
                .candidate_source("candidate-0")
                .map(String::as_str),
            Some("/media/a-first.mkv")
        );
        Ok(())
    }

    #[test]
    fn alm5_library_adapter_preserves_absolute_mainline_target_keys() -> Result<()> {
        let mut mapping = tokyo_ghoul_mapping();
        mapping.episodes = vec![
            mapping.episodes[0].clone(),
            AniZipEpisodeRecord {
                season_number: None,
                episode_number: None,
                absolute_episode_number: Some(14),
                episode_label: Some("14".to_string()),
                mainline_episode_number: Some(14),
                title: Some("Mainline 14".to_string()),
                ..mapping.episodes[0].clone()
            },
            AniZipEpisodeRecord {
                season_number: None,
                episode_number: None,
                absolute_episode_number: Some(15),
                episode_label: Some("15".to_string()),
                mainline_episode_number: Some(15),
                title: Some("Mainline 15".to_string()),
                ..mapping.episodes[0].clone()
            },
        ];
        let targets = build_mapping_targets("Tokyo Ghoul", 2, &mapping);
        let keys = targets
            .iter()
            .map(|target| target.target_key.as_str())
            .collect::<Vec<_>>();

        assert_eq!(keys, vec!["A0013", "A0014", "A0015"]);
        assert!(targets.iter().all(|target| target.season_number.is_none()));
        assert!(targets.iter().all(|target| target.episode_number.is_none()));
        Ok(())
    }

    #[test]
    fn alm5_library_adapter_passes_audio_preference_without_new_settings() -> Result<()> {
        let mut input = tokyo_ghoul_input();
        input.target.audio_preference.mode = AnimeMatchAudioPreferenceMode::PreferDub;
        input.target.audio_preference.languages = vec![" en ".to_string(), "en".to_string()];
        input.target.audio_preference.accepted_profiles = vec![
            "dual_audio".to_string(),
            "en_audio".to_string(),
            "dubbed".to_string(),
        ];
        let prepared = prepare_library_anime_match_request(input)?;

        assert_eq!(
            prepared.request().target.audio_preference.mode,
            AnimeMatchAudioPreferenceMode::PreferDub
        );
        assert_eq!(
            prepared.request().target.audio_preference.languages,
            vec!["en"]
        );
        assert_eq!(
            prepared.request().target.audio_preference.accepted_profiles,
            vec!["dual_audio", "dubbed", "en_audio"]
        );
        Ok(())
    }

    #[test]
    fn alm5_library_adapter_rejects_conflicting_anilist_identity_sources() {
        let mut input = tokyo_ghoul_input();
        input.seasons[0]
            .mapping
            .as_mut()
            .expect("fixture mapping")
            .ids
            .anilist = Some("different-id".to_string());
        assert!(prepare_library_anime_match_request(input).is_err());
    }

    #[test]
    fn alm5_library_adapter_rejects_one_anilist_id_in_multiple_relation_seasons() {
        let mut input = tokyo_ghoul_input();
        let mut duplicate_relation = input.seasons[0].clone();
        duplicate_relation.season.season_number = 3;
        input.seasons.push(duplicate_relation);
        assert!(prepare_library_anime_match_request(input).is_err());
    }

    #[test]
    fn alm8_library_adapter_rejects_multiple_anilist_ids_for_one_relation_season() {
        let mut input = tokyo_ghoul_input();
        let mut conflicting_identity = input.seasons[0].clone();
        conflicting_identity.season.anilist_id = "different-id".to_string();
        conflicting_identity
            .mapping
            .as_mut()
            .expect("fixture mapping")
            .ids
            .anilist = Some("different-id".to_string());
        input.seasons.push(conflicting_identity);

        let error = prepare_library_anime_match_request(input)
            .expect_err("one relation season cannot have multiple AniList identities");
        assert!(
            error
                .to_string()
                .contains("relation season 2 is assigned conflicting AniList identities")
        );
    }

    #[test]
    fn alm8_library_adapter_rejects_conflicting_non_null_mappings() {
        let mut input = tokyo_ghoul_input();
        let mut conflicting_mapping = input.seasons[0].clone();
        conflicting_mapping
            .mapping
            .as_mut()
            .expect("fixture mapping")
            .episodes[0]
            .tvdb_id = Some("different-tvdb-episode".to_string());
        input.seasons.push(conflicting_mapping);

        let error = prepare_library_anime_match_request(input)
            .expect_err("duplicate relation identity cannot carry conflicting mappings");
        assert!(error.to_string().contains(
            "relation season 2 AniList identity '1002' has conflicting ani.zip mappings"
        ));
    }

    #[test]
    fn alm8_library_adapter_preserves_canonical_s00_special_target() -> Result<()> {
        let mut input = tokyo_ghoul_input();
        input.target.wanted_target_keys = vec!["S00E01".to_string()];
        input.target.season_number = Some(0);
        input.target.episode_numbers = vec![1];
        let episode = &mut input.seasons[0]
            .mapping
            .as_mut()
            .expect("fixture mapping")
            .episodes[0];
        episode.season_number = Some(0);
        episode.episode_number = Some(1);

        let prepared = prepare_library_anime_match_request(input)?;
        let request = prepared.request();
        let season = request.context.seasons.first().expect("season context");
        let target = season.targets.first().expect("special target");

        assert_eq!(request.target.wanted_target_keys, vec!["S00E01"]);
        assert_eq!(
            season.season_number, 2,
            "relation season remains AniList-scoped"
        );
        assert_eq!(target.target_key, "S00E01");
        assert_eq!(target.season_number, Some(0));
        assert_eq!(target.episode_number, Some(1));
        assert_eq!(target.absolute_episode_number, Some(13));
        Ok(())
    }
}
