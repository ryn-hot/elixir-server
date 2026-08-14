use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use regex::Regex;

use super::{
    ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION, AnimeMatchContext, AnimeMatchRequest,
    AnimeMatchRuntimeProvenance, AnimeMatchScope, AnimeSemanticEntity,
    AnimeSemanticEvidenceRequest, AnimeSemanticEvidenceResponse, AnimeSemanticHypothesis,
    AnimeSemanticMediaKind, AnimeSemanticNumbering, AnimeSemanticTarget,
    anime_match_alias_equivalence_key,
};

pub const ANIME_SEMANTIC_EVIDENCE_MAX_HYPOTHESES: usize = 48;

static EXPLICIT_ALIAS_SEASON_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)(?:\bS0*(?P<short>\d{1,3})\b|\bSeason[\s._-]*0*(?P<word>\d{1,3})\b|\b0*(?P<ordinal>\d{1,3})(?:st|nd|rd|th)\s+Season\b|第\s*0*(?P<native>\d{1,3})\s*期)",
    )
    .expect("valid explicit anime alias season regex")
});

#[async_trait]
pub trait AnimeSemanticEvidenceEngine: Send + Sync {
    async fn select_hypothesis(
        &self,
        request: AnimeSemanticEvidenceRequest,
    ) -> Result<AnimeSemanticEvidenceResponse>;

    async fn select_hypothesis_with_provenance(
        &self,
        request: AnimeSemanticEvidenceRequest,
    ) -> Result<AnimeSemanticEvidenceEngineOutput> {
        Ok(AnimeSemanticEvidenceEngineOutput {
            response: self.select_hypothesis(request).await?,
            runtime: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnimeSemanticEvidenceEngineOutput {
    pub response: AnimeSemanticEvidenceResponse,
    pub runtime: Option<AnimeMatchRuntimeProvenance>,
}

/// Build complete interpretations for exactly one release or basename. The
/// generator deliberately takes its numeric observations as input: adapters
/// may union several fallible parsers, but the model never invents a coordinate
/// and never receives candidate rank, audio, quality, route, or file ownership.
pub fn build_semantic_evidence_request(
    request: &AnimeMatchRequest,
    candidate_key: impl Into<String>,
    raw: impl Into<String>,
    parent_release: Option<String>,
    observed_titles: impl IntoIterator<Item = String>,
    observed_seasons: impl IntoIterator<Item = i32>,
    observed_episodes: impl IntoIterator<Item = i32>,
    observed_absolute_episodes: impl IntoIterator<Item = i32>,
    media_kinds: impl IntoIterator<Item = AnimeSemanticMediaKind>,
) -> Result<Option<AnimeSemanticEvidenceRequest>> {
    let candidate_key = candidate_key.into();
    let raw = raw.into();
    ensure!(
        !raw.trim().is_empty(),
        "semantic evidence raw value is empty"
    );

    let mut title_candidates = observed_titles
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    title_candidates.insert(raw.clone());
    let observed_seasons = positive_set(
        observed_seasons
            .into_iter()
            .chain(request.target.season_number),
    );
    let observed_episodes = positive_set(
        observed_episodes
            .into_iter()
            .chain(request.target.episode_numbers.iter().copied()),
    );
    let observed_absolute = positive_set(
        observed_absolute_episodes
            .into_iter()
            .chain(request.target.absolute_episode_numbers.iter().copied()),
    );
    // A bare anime number is inherently ambiguous, but an explicit SxxEyy
    // coordinate is not. Preserve both interpretations only for bare numbers;
    // otherwise keep seasonal and independently observed absolute facts apart.
    let observed_numbers = observed_episodes
        .union(&observed_absolute)
        .copied()
        .collect::<BTreeSet<_>>();
    let seasonal_numbers = if observed_seasons.is_empty() {
        &observed_numbers
    } else {
        &observed_episodes
    };
    let absolute_numbers = if observed_seasons.is_empty() {
        &observed_numbers
    } else {
        &observed_absolute
    };
    let mut media_kinds = media_kinds.into_iter().collect::<BTreeSet<_>>();
    media_kinds.extend(semantic_media_kinds_for_target_scope(request.target.scope));
    if media_kinds.is_empty() {
        return Ok(None);
    }

    // Context is already bounded to wanted targets and their closest numbering
    // neighbours. Do not require deterministic alias overlap here: cross-script
    // names and irregular sequel aliases are exactly why the semantic selector
    // exists. Wanted entities sort first, followed by title relevance and
    // season proximity, so a bounded request always retains the target while
    // still showing the model plausible adjacent-season alternatives.
    let wanted_target_keys = request
        .target
        .wanted_target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut entities = semantic_entities(&request.context)
        .into_iter()
        .map(|entity| {
            let wanted = request.context.seasons.iter().any(|season| {
                season.season_number == entity.season_number
                    && season.anilist_id == entity.anilist_id
                    && season
                        .targets
                        .iter()
                        .any(|target| wanted_target_keys.contains(target.target_key.as_str()))
            });
            let relevance = semantic_entity_relevance(&entity, &title_candidates).unwrap_or(0);
            let distance = request
                .target
                .season_number
                .map(|target| target.abs_diff(entity.season_number))
                .unwrap_or(u32::MAX);
            (wanted, relevance, distance, entity)
        })
        .collect::<Vec<_>>();
    entities.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| {
                left.2
                    .cmp(&right.2)
                    .then_with(|| left.3.index.cmp(&right.3.index))
            })
    });
    let worst_case_hypotheses_per_entity = media_kinds.len().saturating_mul(3).max(1);
    let entity_limit =
        (ANIME_SEMANTIC_EVIDENCE_MAX_HYPOTHESES / worst_case_hypotheses_per_entity).max(1);
    let mut entities = entities
        .into_iter()
        .take(entity_limit)
        .map(|(_, _, _, entity)| entity)
        .collect::<Vec<_>>();
    if entities.is_empty() {
        return Ok(None);
    }
    for (index, entity) in entities.iter_mut().enumerate() {
        entity.index = index;
    }

    let mut hypotheses = Vec::new();

    for kind in media_kinds {
        for entity in &entities {
            if matches!(
                kind,
                AnimeSemanticMediaKind::SeasonPack | AnimeSemanticMediaKind::SeriesPack
            ) {
                hypotheses.push(AnimeSemanticHypothesis {
                    index: hypotheses.len(),
                    entity_index: entity.index,
                    numbering: AnimeSemanticNumbering::EntityOnly,
                    episode_numbers: Vec::new(),
                    absolute_episode_numbers: Vec::new(),
                    media_kind: kind,
                    target_keys: Vec::new(),
                });
                continue;
            }
            let mut seasonal_targets = BTreeMap::<i32, Vec<String>>::new();
            let mut absolute_targets = BTreeMap::<i32, Vec<String>>::new();
            let Some(context_season) = request.context.seasons.iter().find(|season| {
                season.season_number == entity.season_number
                    && season.anilist_id == entity.anilist_id
            }) else {
                continue;
            };
            for target in &context_season.targets {
                if let Some(episode) = target
                    .episode_number
                    .filter(|value| seasonal_numbers.contains(value))
                {
                    seasonal_targets
                        .entry(episode)
                        .or_default()
                        .push(target.target_key.clone());
                }
                if let Some(absolute) = target
                    .absolute_episode_number
                    .filter(|value| absolute_numbers.contains(value))
                {
                    absolute_targets
                        .entry(absolute)
                        .or_default()
                        .push(target.target_key.clone());
                }
            }

            push_numbered_hypothesis(
                &mut hypotheses,
                entity.index,
                AnimeSemanticNumbering::Seasonal,
                kind,
                seasonal_targets,
            );
            push_numbered_hypothesis(
                &mut hypotheses,
                entity.index,
                AnimeSemanticNumbering::Absolute,
                kind,
                absolute_targets,
            );

            // Identity and coordinate interpretation are separable. EntityOnly
            // means the model affirms the title/media entity while the existing
            // deterministic parser retains responsibility for observed numbers.
            hypotheses.push(AnimeSemanticHypothesis {
                index: hypotheses.len(),
                entity_index: entity.index,
                numbering: AnimeSemanticNumbering::EntityOnly,
                episode_numbers: Vec::new(),
                absolute_episode_numbers: Vec::new(),
                media_kind: kind,
                target_keys: Vec::new(),
            });
        }
    }

    hypotheses.sort_by(|left, right| {
        (
            left.entity_index,
            left.numbering,
            &left.episode_numbers,
            &left.absolute_episode_numbers,
            left.media_kind,
        )
            .cmp(&(
                right.entity_index,
                right.numbering,
                &right.episode_numbers,
                &right.absolute_episode_numbers,
                right.media_kind,
            ))
    });
    hypotheses.dedup_by(|left, right| {
        left.entity_index == right.entity_index
            && left.numbering == right.numbering
            && left.episode_numbers == right.episode_numbers
            && left.absolute_episode_numbers == right.absolute_episode_numbers
            && left.media_kind == right.media_kind
    });
    if hypotheses.is_empty() || hypotheses.len() > ANIME_SEMANTIC_EVIDENCE_MAX_HYPOTHESES {
        return Ok(None);
    }
    for (index, hypothesis) in hypotheses.iter_mut().enumerate() {
        hypothesis.index = index;
        hypothesis.target_keys.sort();
        hypothesis.target_keys.dedup();
    }

    let file_names = request
        .candidates
        .iter()
        .find(|candidate| candidate.candidate_key == candidate_key)
        .into_iter()
        .flat_map(|candidate| candidate.files.iter())
        .map(|file| file.path.trim())
        .filter(|path| !path.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    Ok(Some(AnimeSemanticEvidenceRequest {
        schema_version: ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
        request_id: request.request_id.clone(),
        candidate_key,
        raw,
        parent_release,
        target: AnimeSemanticTarget {
            canonical_title: request.target.canonical_title.clone(),
            scope: Some(request.target.scope),
            season_number: request.target.season_number,
            episode_numbers: request.target.episode_numbers.clone(),
            absolute_episode_numbers: request.target.absolute_episode_numbers.clone(),
            audio_preference: request.target.audio_preference.clone(),
        },
        file_names,
        title_candidates: title_candidates.into_iter().collect(),
        observed_season_numbers: observed_seasons.into_iter().collect(),
        graph_fingerprint: request.context.graph_fingerprint.clone(),
        entities,
        hypotheses,
    }))
}

fn semantic_entity_relevance(
    entity: &AnimeSemanticEntity,
    titles: &BTreeSet<String>,
) -> Option<u8> {
    let title_keys = titles
        .iter()
        .map(|title| anime_match_alias_equivalence_key(title))
        .filter(|key| !key.is_empty())
        .collect::<BTreeSet<_>>();
    let mut relevance = None;
    for alias in &entity.aliases {
        let alias = anime_match_alias_equivalence_key(alias);
        if alias.is_empty() {
            continue;
        }
        for title in &title_keys {
            let score = if title == &alias {
                Some(3)
            } else if title.len().min(alias.len()) >= 4
                && (title.contains(&alias) || alias.contains(title))
            {
                Some(2)
            } else {
                None
            };
            relevance = relevance.max(score);
        }
    }
    relevance
}

pub fn validate_semantic_evidence_response<'a>(
    request: &'a AnimeSemanticEvidenceRequest,
    response: &AnimeSemanticEvidenceResponse,
) -> Result<Option<&'a AnimeSemanticHypothesis>> {
    ensure!(
        response.schema_version == ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
        "unsupported semantic evidence response schema version {}",
        response.schema_version
    );
    let Some(index) = response.hypothesis_index else {
        return Ok(None);
    };
    let hypothesis = request
        .hypotheses
        .get(index)
        .filter(|hypothesis| hypothesis.index == index)
        .ok_or_else(|| anyhow::anyhow!("semantic evidence selected unknown hypothesis {index}"))?;
    ensure!(
        request
            .entities
            .get(hypothesis.entity_index)
            .is_some_and(|entity| entity.index == hypothesis.entity_index),
        "semantic evidence hypothesis references unknown entity"
    );
    Ok(Some(hypothesis))
}

fn semantic_entities(context: &AnimeMatchContext) -> Vec<AnimeSemanticEntity> {
    let mut seasons = context.seasons.iter().collect::<Vec<_>>();
    seasons.sort_by(|left, right| {
        (left.season_number, &left.anilist_id).cmp(&(right.season_number, &right.anilist_id))
    });
    seasons
        .into_iter()
        .enumerate()
        .map(|(index, season)| {
            let mut aliases = BTreeSet::new();
            aliases.extend(
                season
                    .aliases
                    .iter()
                    .map(|alias| alias.value.trim())
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
            );
            aliases.extend(
                season
                    .targets
                    .iter()
                    .map(|target| target.title.trim())
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
            );
            let mut release_season_numbers = BTreeSet::from([season.season_number]);
            for alias in &aliases {
                release_season_numbers.extend(explicit_alias_season_numbers(alias));
            }
            AnimeSemanticEntity {
                index,
                season_number: season.season_number,
                release_season_numbers: release_season_numbers.into_iter().collect(),
                aliases: aliases.into_iter().collect(),
                anilist_id: season.anilist_id.clone(),
            }
        })
        .collect()
}

fn explicit_alias_season_numbers(alias: &str) -> impl Iterator<Item = i32> + '_ {
    EXPLICIT_ALIAS_SEASON_RE
        .captures_iter(alias)
        .filter_map(|captures| {
            ["short", "word", "ordinal", "native"]
                .into_iter()
                .find_map(|name| captures.name(name))
                .and_then(|value| value.as_str().parse::<i32>().ok())
                .filter(|value| *value >= 0)
        })
}

fn positive_set(values: impl IntoIterator<Item = i32>) -> BTreeSet<i32> {
    values.into_iter().filter(|value| *value > 0).collect()
}

fn semantic_media_kinds_for_target_scope(
    scope: AnimeMatchScope,
) -> impl Iterator<Item = AnimeSemanticMediaKind> {
    let kinds: &[AnimeSemanticMediaKind] = match scope {
        AnimeMatchScope::Movie => &[AnimeSemanticMediaKind::Movie],
        AnimeMatchScope::Special => &[AnimeSemanticMediaKind::Special, AnimeSemanticMediaKind::Ova],
        AnimeMatchScope::Season => &[AnimeSemanticMediaKind::SeasonPack],
        AnimeMatchScope::Series | AnimeMatchScope::Subscription => {
            &[AnimeSemanticMediaKind::SeriesPack]
        }
        AnimeMatchScope::Range | AnimeMatchScope::AnimeArc => &[AnimeSemanticMediaKind::Range],
        AnimeMatchScope::Episode | AnimeMatchScope::Missing | AnimeMatchScope::SelectedTargets => {
            &[AnimeSemanticMediaKind::Episode]
        }
    };
    kinds.iter().copied()
}

fn push_numbered_hypothesis(
    hypotheses: &mut Vec<AnimeSemanticHypothesis>,
    entity_index: usize,
    numbering: AnimeSemanticNumbering,
    media_kind: AnimeSemanticMediaKind,
    targets: BTreeMap<i32, Vec<String>>,
) {
    if targets.is_empty() {
        return;
    }
    let numbers = targets.keys().copied().collect::<Vec<_>>();
    let target_keys = targets.into_values().flatten().collect();
    hypotheses.push(AnimeSemanticHypothesis {
        index: hypotheses.len(),
        entity_index,
        numbering,
        episode_numbers: (numbering == AnimeSemanticNumbering::Seasonal)
            .then_some(numbers.clone())
            .unwrap_or_default(),
        absolute_episode_numbers: (numbering == AnimeSemanticNumbering::Absolute)
            .then_some(numbers)
            .unwrap_or_default(),
        media_kind,
        target_keys,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anime_matching::{
        AnimeMatchAlias, AnimeMatchAliasKind, AnimeMatchAudioPreference, AnimeMatchContextTarget,
        AnimeMatchMediaType, AnimeMatchScope, AnimeMatchSeasonContext, AnimeMatchTarget,
    };

    fn tokyo_ghoul_request() -> AnimeMatchRequest {
        let season = |number, alias: &str, absolute| AnimeMatchSeasonContext {
            season_number: number,
            anilist_id: format!("anilist-{number}"),
            aliases: vec![AnimeMatchAlias {
                value: alias.to_string(),
                kind: AnimeMatchAliasKind::English,
                source: None,
                language: Some("en".to_string()),
            }],
            targets: vec![AnimeMatchContextTarget {
                target_key: format!("S{number:02}E01"),
                title: alias.to_string(),
                season_number: Some(number),
                episode_number: Some(1),
                absolute_episode_number: Some(absolute),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        AnimeMatchRequest {
            schema_version: 1,
            request_id: "tokyo-ghoul".to_string(),
            target: AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Tokyo Ghoul".to_string(),
                scope: AnimeMatchScope::Episode,
                wanted_target_keys: vec!["S02E01".to_string()],
                season_number: Some(2),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![13],
                audio_preference: AnimeMatchAudioPreference::default(),
            },
            context: AnimeMatchContext {
                graph_fingerprint: "tg".to_string(),
                seasons: vec![
                    season(1, "Tokyo Ghoul", 1),
                    season(2, "Tokyo Ghoul Root A", 13),
                ],
            },
            candidates: Vec::new(),
        }
    }

    #[test]
    fn alm9_semantic_request_preserves_named_sequel_and_numbering_alternatives() {
        let request = build_semantic_evidence_request(
            &tokyo_ghoul_request(),
            "candidate-0",
            "[Group] Tokyo Ghoul Root A - 01",
            None,
            ["Tokyo Ghoul Root A".to_string()],
            [],
            [1],
            [13],
            [AnimeSemanticMediaKind::Episode],
        )
        .expect("request generation")
        .expect("ambiguous evidence request");

        assert_eq!(request.entities.len(), 2);
        let root_a = request
            .entities
            .iter()
            .find(|entity| {
                entity
                    .aliases
                    .iter()
                    .any(|value| value == "Tokyo Ghoul Root A")
            })
            .expect("Root A entity");
        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.entity_index == root_a.index
                && hypothesis.numbering == AnimeSemanticNumbering::Seasonal
                && hypothesis.episode_numbers == vec![1]
                && hypothesis.target_keys == vec!["S02E01"]
        }));
        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.entity_index == root_a.index
                && hypothesis.numbering == AnimeSemanticNumbering::Absolute
                && hypothesis.absolute_episode_numbers == vec![13]
        }));
    }

    #[test]
    fn alm9_semantic_response_accepts_only_request_owned_index_or_null() {
        let request = build_semantic_evidence_request(
            &tokyo_ghoul_request(),
            "candidate-0",
            "Tokyo Ghoul Root A - 01",
            None,
            ["Tokyo Ghoul Root A".to_string()],
            [],
            [1],
            [13],
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .unwrap();
        assert!(
            validate_semantic_evidence_response(
                &request,
                &AnimeSemanticEvidenceResponse {
                    schema_version: 1,
                    hypothesis_index: None
                }
            )
            .unwrap()
            .is_none()
        );
        assert!(
            validate_semantic_evidence_response(
                &request,
                &AnimeSemanticEvidenceResponse {
                    schema_version: 1,
                    hypothesis_index: Some(999)
                }
            )
            .is_err()
        );
    }

    #[test]
    fn alm9_bare_number_preserves_seasonal_and_absolute_interpretations() {
        let request = build_semantic_evidence_request(
            &tokyo_ghoul_request(),
            "candidate-0",
            "[Group] Tokyo Ghoul Root A - 13",
            None,
            ["Tokyo Ghoul Root A".to_string()],
            [],
            [13],
            [],
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .unwrap();

        let root_a_index = request
            .entities
            .iter()
            .find(|entity| entity.season_number == 2)
            .map(|entity| entity.index)
            .unwrap();
        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.entity_index == root_a_index
                && hypothesis.numbering == AnimeSemanticNumbering::Absolute
                && hypothesis.absolute_episode_numbers == vec![13]
                && hypothesis.target_keys == vec!["S02E01"]
        }));
    }

    #[test]
    fn alm9_numeric_coincidence_still_builds_a_target_aware_request() {
        let request = build_semantic_evidence_request(
            &tokyo_ghoul_request(),
            "candidate-0",
            "[Group] Ishuzoku Reviewers - 13",
            None,
            ["Ishuzoku Reviewers".to_string()],
            [],
            [13],
            [],
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .expect("bounded target context remains available");

        assert_eq!(request.target.canonical_title, "Tokyo Ghoul");
        assert_eq!(request.target.season_number, Some(2));
        assert!(
            request
                .entities
                .iter()
                .any(|entity| entity.season_number == 2)
        );
        assert!(
            request
                .title_candidates
                .iter()
                .any(|title| title == "Ishuzoku Reviewers")
        );
    }

    #[test]
    fn alm9_explicit_season_preserves_target_owned_numbering_alternatives() {
        let request = build_semantic_evidence_request(
            &tokyo_ghoul_request(),
            "candidate-0",
            "Tokyo Ghoul Root A S02E01",
            None,
            ["Tokyo Ghoul Root A".to_string()],
            [2],
            [1],
            [],
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .unwrap();

        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.numbering == AnimeSemanticNumbering::Seasonal
                && hypothesis.episode_numbers == vec![1]
        }));
        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.numbering == AnimeSemanticNumbering::Absolute
                && hypothesis.absolute_episode_numbers == vec![13]
        }));
    }

    #[test]
    fn alm9_ova_can_select_a_nonzero_canonical_entity() {
        let request = build_semantic_evidence_request(
            &tokyo_ghoul_request(),
            "candidate-0",
            "Tokyo Ghoul Root A OVA",
            None,
            ["Tokyo Ghoul Root A".to_string()],
            [],
            [],
            [],
            [AnimeSemanticMediaKind::Ova],
        )
        .unwrap()
        .unwrap();

        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.media_kind == AnimeSemanticMediaKind::Ova
                && hypothesis.numbering == AnimeSemanticNumbering::EntityOnly
                && request.entities[hypothesis.entity_index].season_number == 2
        }));
    }

    #[test]
    fn alm9_target_scope_adds_missing_special_media_interpretations() {
        let mut match_request = tokyo_ghoul_request();
        match_request.target.scope = AnimeMatchScope::Special;
        let request = build_semantic_evidence_request(
            &match_request,
            "candidate-0",
            "Tokyo Ghoul Root A OVA",
            None,
            ["Tokyo Ghoul Root A".to_string()],
            [],
            [],
            [],
            // Simulate an incomplete deterministic classification. The exact
            // request scope must still expose special/OVA choices to the model.
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .unwrap();

        let kinds = request
            .hypotheses
            .iter()
            .map(|hypothesis| hypothesis.media_kind)
            .collect::<BTreeSet<_>>();
        assert!(kinds.contains(&AnimeSemanticMediaKind::Episode));
        assert!(kinds.contains(&AnimeSemanticMediaKind::Special));
        assert!(kinds.contains(&AnimeSemanticMediaKind::Ova));
    }

    #[test]
    fn alm9_entity_exposes_only_explicit_alias_season_translations() {
        let mut request = tokyo_ghoul_request();
        request.context.seasons[1].season_number = 4;
        request.context.seasons[1].aliases.push(AnimeMatchAlias {
            value: "Tokyo Ghoul Root A Season 3".to_string(),
            kind: AnimeMatchAliasKind::English,
            source: None,
            language: Some("en".to_string()),
        });
        let semantic = build_semantic_evidence_request(
            &request,
            "candidate-0",
            "Tokyo Ghoul Root A Season 3",
            None,
            ["Tokyo Ghoul Root A Season 3".to_string()],
            [3],
            [1],
            [],
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .unwrap();
        let entity = semantic
            .entities
            .iter()
            .find(|entity| entity.season_number == 4)
            .unwrap();

        assert_eq!(entity.release_season_numbers, vec![3, 4]);
    }
}
