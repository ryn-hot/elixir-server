use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use async_trait::async_trait;

use super::{
    ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION, AnimeMatchContext, AnimeMatchRequest,
    AnimeMatchRuntimeProvenance, AnimeSemanticEntity, AnimeSemanticEvidenceRequest,
    AnimeSemanticEvidenceResponse, AnimeSemanticHypothesis, AnimeSemanticMediaKind,
    AnimeSemanticNumbering,
};

pub const ANIME_SEMANTIC_EVIDENCE_MAX_HYPOTHESES: usize = 48;

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
    observed_seasons: impl IntoIterator<Item = i32>,
    observed_episodes: impl IntoIterator<Item = i32>,
    observed_absolute_episodes: impl IntoIterator<Item = i32>,
    media_kinds: impl IntoIterator<Item = AnimeSemanticMediaKind>,
) -> Result<Option<AnimeSemanticEvidenceRequest>> {
    let raw = raw.into();
    ensure!(
        !raw.trim().is_empty(),
        "semantic evidence raw value is empty"
    );

    let entities = semantic_entities(&request.context);
    if entities.is_empty() {
        return Ok(None);
    }
    // Season observations are deliberately not a filter. A bad inferred
    // season is one of the ambiguities this evidence path exists to resolve;
    // explicit SxxEyy contradictions are enforced later by the resolver.
    let _observed_seasons = positive_set(observed_seasons);
    let observed_episodes = positive_set(observed_episodes);
    let observed_absolute = positive_set(observed_absolute_episodes);
    // A bare anime number is inherently ambiguous: parser A may call `13`
    // seasonal while parser B calls it absolute. Preserve both complete graph
    // interpretations and let explicit SxxEyy contradiction checks decide
    // when only seasonal numbering is legal.
    let observed_numbers = observed_episodes
        .union(&observed_absolute)
        .copied()
        .collect::<BTreeSet<_>>();
    let media_kinds = media_kinds.into_iter().collect::<BTreeSet<_>>();
    if media_kinds.is_empty() {
        return Ok(None);
    }

    let mut hypotheses = Vec::new();

    for kind in media_kinds {
        for entity in &entities {
            if matches!(
                kind,
                AnimeSemanticMediaKind::Special | AnimeSemanticMediaKind::Ova
            ) && entity.season_number != 0
            {
                continue;
            }
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
                    .filter(|value| observed_numbers.contains(value))
                {
                    seasonal_targets
                        .entry(episode)
                        .or_default()
                        .push(target.target_key.clone());
                }
                if let Some(absolute) = target
                    .absolute_episode_number
                    .filter(|value| observed_numbers.contains(value))
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

            if observed_episodes.is_empty() && observed_absolute.is_empty() {
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

    Ok(Some(AnimeSemanticEvidenceRequest {
        schema_version: ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
        request_id: request.request_id.clone(),
        candidate_key: candidate_key.into(),
        raw,
        parent_release,
        graph_fingerprint: request.context.graph_fingerprint.clone(),
        entities,
        hypotheses,
    }))
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
            AnimeSemanticEntity {
                index,
                season_number: season.season_number,
                aliases: aliases.into_iter().collect(),
                anilist_id: season.anilist_id.clone(),
            }
        })
        .collect()
}

fn positive_set(values: impl IntoIterator<Item = i32>) -> BTreeSet<i32> {
    values.into_iter().filter(|value| *value > 0).collect()
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
            [],
            [1],
            [13],
            [AnimeSemanticMediaKind::Episode],
        )
        .expect("request generation")
        .expect("ambiguous evidence request");

        assert_eq!(request.entities.len(), 2);
        assert!(
            request.entities[1]
                .aliases
                .iter()
                .any(|value| value == "Tokyo Ghoul Root A")
        );
        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.entity_index == 1
                && hypothesis.numbering == AnimeSemanticNumbering::Seasonal
                && hypothesis.episode_numbers == vec![1]
                && hypothesis.target_keys == vec!["S02E01"]
        }));
        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.entity_index == 1
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
            [],
            [13],
            [],
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .unwrap();

        assert!(request.hypotheses.iter().any(|hypothesis| {
            hypothesis.entity_index == 1
                && hypothesis.numbering == AnimeSemanticNumbering::Absolute
                && hypothesis.absolute_episode_numbers == vec![13]
                && hypothesis.target_keys == vec!["S02E01"]
        }));
    }
}
