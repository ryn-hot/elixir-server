use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, anyhow, bail};

use crate::{
    acquisition::{
        automation::anime_coverage_options_for_candidate,
        language_policy::{
            AcquisitionLanguagePreference, AnimeAudioPreference, AnimeAudioPreferenceMode,
            CandidateLanguageEvidence, LanguagePreferenceAssessment,
            LanguagePreferenceAssessmentState, LanguagePreferenceMode, add_language_evidence_text,
            add_language_evidence_value, add_subtitle_language_evidence_value,
            assess_language_preference, language_preference_from_quality_profile,
        },
        release_resolution::{
            anime::{
                ANIME_SHOKO_STYLE_RESOLVER_VERSION, AnimeBatchKind, AnimeCandidateInput,
                AnimeCandidateScoringContext, AnimeFileCoverageEntry, AnimeFileCoveragePlan,
                AnimeReleaseFileInput, AnimeScopedAlias, anime_coverage_kind,
                anime_release_kind_for_coverage, parse_anime_release_title, score_anime_candidate,
            },
            models::{ReleaseConfidence, ReleaseCoverageState, ReleaseKind, ReleaseResolverKind},
        },
        subscriptions::{
            AcquisitionRequestScope, AcquisitionRoutePolicy, AcquisitionSubscription,
            AcquisitionTarget,
        },
    },
    anime_matching::{
        AnimeCandidateMatch, AnimeMatchAlias, AnimeMatchAliasKind, AnimeMatchAudioPreference,
        AnimeMatchAudioPreferenceMode, AnimeMatchAudioProfile, AnimeMatchBatchInput,
        AnimeMatchCandidateInput, AnimeMatchContext, AnimeMatchContextTarget, AnimeMatchFileInput,
        AnimeMatchMediaType, AnimeMatchParseFacts, AnimeMatchRequest, AnimeMatchScope,
        AnimeMatchSeasonContext, AnimeMatchSourceMap, DeterministicMatchState,
        anime_match_alias_equivalence_key, classify_anime_match_alias, scope_anime_match_context,
    },
    db::models::MediaType,
    http::handlers::acquisition_sources::{AcquisitionCandidate, AcquisitionCandidateFile},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcquisitionAnimeCandidateSource {
    pub candidate_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcquisitionAnimeFileSource {
    pub candidate_index: usize,
    pub file_index: usize,
}

pub(crate) type AcquisitionAnimeMatchBatchInput =
    AnimeMatchBatchInput<AcquisitionAnimeCandidateSource, AcquisitionAnimeFileSource>;
pub(crate) type AcquisitionAnimeMatchSourceMap =
    AnimeMatchSourceMap<AcquisitionAnimeCandidateSource, AcquisitionAnimeFileSource>;

/// Adapts the acquisition graph, requested targets, and provider candidates to
/// the shared logical V1 matcher contract. Provider identity, routes, URLs,
/// hashes, and download state stay in the private acquisition pipeline.
pub(crate) fn acquisition_anime_match_batch_input(
    request_id: impl Into<String>,
    subscription: &AcquisitionSubscription,
    wanted_targets: &[AcquisitionTarget],
    context: &AnimeCandidateScoringContext,
    candidates: &[AcquisitionCandidate],
) -> Result<AcquisitionAnimeMatchBatchInput> {
    if subscription.media_type != MediaType::Anime {
        bail!("anime matcher acquisition adapter requires an anime subscription");
    }
    if wanted_targets.is_empty() {
        bail!("anime matcher acquisition adapter requires at least one wanted target");
    }
    if wanted_targets
        .iter()
        .any(|target| target.media_type != MediaType::Anime)
    {
        bail!("anime matcher acquisition adapter received a non-anime target");
    }

    let mut sorted_wanted = wanted_targets.iter().collect::<Vec<_>>();
    sorted_wanted.sort_by_key(|target| target_sort_key(target));
    let wanted_target_keys =
        sorted_unique_strings(sorted_wanted.iter().map(|target| target.target_key.clone()));
    let season_number =
        one_shared_present_i32(sorted_wanted.iter().map(|target| target.season_number));
    let episode_numbers = sorted_unique_i32(
        sorted_wanted
            .iter()
            .filter_map(|target| target.episode_number),
    );
    let absolute_episode_numbers = sorted_unique_i32(
        sorted_wanted
            .iter()
            .filter_map(|target| target.absolute_episode_number),
    );

    let target = crate::anime_matching::AnimeMatchTarget {
        media_type: AnimeMatchMediaType::Anime,
        canonical_title: subscription.title.trim().to_string(),
        scope: acquisition_match_scope(subscription.request_scope),
        wanted_target_keys,
        season_number,
        episode_numbers,
        absolute_episode_numbers,
        audio_preference: acquisition_audio_preference(subscription),
    };
    let context = acquisition_match_context(&subscription.title, context, &target)?;
    let candidates = candidates
        .iter()
        .enumerate()
        .map(|(candidate_index, candidate)| AnimeMatchCandidateInput {
            source: AcquisitionAnimeCandidateSource { candidate_index },
            title: candidate.title.clone(),
            files: candidate
                .files
                .iter()
                .enumerate()
                .filter(|(_, file)| selectable_anime_media_file(file))
                .map(|(file_index, file)| AnimeMatchFileInput {
                    source: AcquisitionAnimeFileSource {
                        candidate_index,
                        file_index,
                    },
                    path: file.path.clone(),
                })
                .collect(),
            parse_facts: acquisition_candidate_parse_facts(candidate),
        })
        .collect();

    Ok(AnimeMatchBatchInput {
        request_id: request_id.into(),
        target,
        context,
        candidates,
    })
}

/// Mirrors the current acquisition acceptance gate without teaching the
/// shared matching service about resolver-specific confidence or coverage.
pub(crate) fn acquisition_anime_deterministic_state(
    plan: &AnimeFileCoveragePlan,
) -> DeterministicMatchState {
    if matches!(
        plan.confidence,
        ReleaseConfidence::High | ReleaseConfidence::Medium
    ) && !plan.entries.is_empty()
        && plan.rejection_reasons.is_empty()
    {
        DeterministicMatchState::Definitive
    } else {
        DeterministicMatchState::Difficult
    }
}

/// Bind a deterministic single-episode plan to the sole real provider media
/// file only after independently proving that filename resolves to the same
/// target. This keeps the no-model fast path while preserving provider-file
/// ownership for later selection and import.
pub(crate) fn bind_exact_single_anime_provider_file(
    mut plan: AnimeFileCoveragePlan,
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
) -> AnimeFileCoveragePlan {
    if plan.release_kind != ReleaseKind::Single
        || plan.confidence != ReleaseConfidence::High
        || plan.entries.len() != 1
        || !plan.review_reasons.is_empty()
        || !plan.rejection_reasons.is_empty()
    {
        return plan;
    }
    let media_files = files
        .iter()
        .filter(|file| {
            file.selectable
                && is_anime_provider_media_file(&file.path)
                && !is_anime_provider_sample_or_extra(&file.path)
        })
        .collect::<Vec<_>>();
    if media_files.len() != 1 {
        return plan;
    }
    let file = media_files[0];
    let file_candidate = AnimeCandidateInput {
        title: file
            .path
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(file.path.as_str())
            .to_string(),
        source_kind: candidate.source_kind.clone(),
        quality: candidate.quality.clone(),
        size_bytes: file.size_bytes.and_then(|value| u64::try_from(value).ok()),
        seeders: candidate.seeders,
        cached_debrid: candidate.cached_debrid,
        rank: candidate.rank,
        source_score: candidate.source_score,
        supported_routes: candidate.supported_routes.clone(),
        default_route: candidate.default_route.clone(),
    };
    let file_score = score_anime_candidate(context, &file_candidate);
    if file_score.confidence != ReleaseConfidence::High
        || file_score.target_matches.len() != 1
        || !file_score.review_reasons.is_empty()
        || !file_score.rejection_reasons.is_empty()
        || file_score.target_matches[0].target_key != plan.entries[0].target_key
    {
        return plan;
    }

    let file_target = &file_score.target_matches[0];
    plan.selected_file_keys = vec![file.file_key.clone()];
    let entry = &mut plan.entries[0];
    entry.release_file_key = Some(file.file_key.clone());
    entry.file_id = file.file_id.clone();
    entry.file_index = file.file_index;
    entry.path = Some(file.path.clone());
    entry.confidence = ReleaseConfidence::High;
    entry.score = Some(file_target.score);
    entry.reason = file_target.match_reason.clone();
    plan
}

fn is_anime_provider_media_file(path: &str) -> bool {
    let lower = path.trim().to_ascii_lowercase();
    [
        ".mkv", ".mp4", ".m4v", ".avi", ".mov", ".wmv", ".ts", ".m2ts", ".webm",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension))
}

fn is_anime_provider_sample_or_extra(path: &str) -> bool {
    let lower = path.replace('\\', "/").to_ascii_lowercase();
    let basename = lower.rsplit('/').next().unwrap_or(&lower);
    basename.contains("sample")
        || basename.contains("trailer")
        || basename.contains("extra")
        || lower.contains("/sample")
        || lower.contains("/extras/")
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AcquisitionAnimeModelCoverage {
    pub candidate_index: usize,
    pub audio_profile: AnimeMatchAudioProfile,
    pub plan: AnimeFileCoveragePlan,
}

/// Convert every already reference-validated model mapping into the existing
/// acquisition coverage type. The whole ordered response is checked before it
/// is returned, so one unsafe pack/file mapping rejects the response atomically.
pub(crate) fn model_derived_anime_coverage_plans(
    request: &AnimeMatchRequest,
    context: &AnimeCandidateScoringContext,
    candidates: &[AcquisitionCandidate],
    route_policy: AcquisitionRoutePolicy,
    matches: &[AnimeCandidateMatch],
    source_map: &AcquisitionAnimeMatchSourceMap,
) -> Result<Vec<AcquisitionAnimeModelCoverage>> {
    model_derived_anime_coverage_plans_with_selection_resolver(
        request,
        context,
        candidates,
        matches,
        source_map,
        |_, candidate| {
            anime_coverage_options_for_candidate(route_policy, candidate).file_selection_supported
        },
    )
}

/// Convert mappings after a downstream inspection surface has established its
/// actual file-selection capability. qBittorrent and debrid inspection must
/// use this instead of inferring capability from the subscription route.
pub(crate) fn model_derived_anime_coverage_plans_with_file_selection_support(
    request: &AnimeMatchRequest,
    context: &AnimeCandidateScoringContext,
    candidates: &[AcquisitionCandidate],
    file_selection_supported: bool,
    matches: &[AnimeCandidateMatch],
    source_map: &AcquisitionAnimeMatchSourceMap,
) -> Result<Vec<AcquisitionAnimeModelCoverage>> {
    model_derived_anime_coverage_plans_with_selection_resolver(
        request,
        context,
        candidates,
        matches,
        source_map,
        |_, _| file_selection_supported,
    )
}

pub(crate) fn model_derived_anime_coverage_plans_with_selection_resolver(
    request: &AnimeMatchRequest,
    context: &AnimeCandidateScoringContext,
    candidates: &[AcquisitionCandidate],
    matches: &[AnimeCandidateMatch],
    source_map: &AcquisitionAnimeMatchSourceMap,
    file_selection_supported: impl Fn(usize, &AcquisitionCandidate) -> bool,
) -> Result<Vec<AcquisitionAnimeModelCoverage>> {
    matches
        .iter()
        .map(|matched| {
            let candidate_source = source_map
                .candidate_source(&matched.candidate_key)
                .ok_or_else(|| {
                    anyhow!("unknown model candidate key '{}'", matched.candidate_key)
                })?;
            let candidate = candidates
                .get(candidate_source.candidate_index)
                .ok_or_else(|| anyhow!("model candidate source index is out of bounds"))?;
            let plan = model_derived_anime_coverage_plan(
                request,
                context,
                candidates,
                file_selection_supported(candidate_source.candidate_index, candidate),
                matched,
                source_map,
            )?;
            Ok(AcquisitionAnimeModelCoverage {
                candidate_index: candidate_source.candidate_index,
                audio_profile: matched.audio_profile,
                plan,
            })
        })
        .collect()
}

fn model_derived_anime_coverage_plan(
    request: &AnimeMatchRequest,
    context: &AnimeCandidateScoringContext,
    candidates: &[AcquisitionCandidate],
    file_selection_supported: bool,
    matched: &AnimeCandidateMatch,
    source_map: &AcquisitionAnimeMatchSourceMap,
) -> Result<AnimeFileCoveragePlan> {
    let candidate_source = source_map
        .candidate_source(&matched.candidate_key)
        .ok_or_else(|| anyhow!("unknown model candidate key '{}'", matched.candidate_key))?;
    let candidate = candidates
        .get(candidate_source.candidate_index)
        .ok_or_else(|| anyhow!("model candidate source index is out of bounds"))?;
    let file_identities = candidate
        .files
        .iter()
        .enumerate()
        .map(|(file_index, file)| acquisition_release_file_identity(file, file_index))
        .collect::<Vec<_>>();
    let mut unique_file_keys = BTreeSet::new();
    for identity in &file_identities {
        if !unique_file_keys.insert(identity.file_key.as_str()) {
            bail!(
                "candidate has duplicate underlying file identity '{}'",
                identity.file_key
            );
        }
    }

    let mut scoped_targets_by_key = BTreeMap::new();
    for season in &request.context.seasons {
        for target in &season.targets {
            if scoped_targets_by_key
                .insert(target.target_key.as_str(), (season, target))
                .is_some()
            {
                bail!(
                    "scoped model request repeats target key '{}'",
                    target.target_key
                );
            }
        }
    }
    let wanted_target_keys = request
        .target
        .wanted_target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut targets = Vec::new();
    let mut seen_targets = BTreeSet::new();
    for target_key in &matched.matched_target_keys {
        if !wanted_target_keys.contains(target_key.as_str()) {
            bail!("model mapping references target outside the requested scope '{target_key}'");
        }
        if !seen_targets.insert(target_key.as_str()) {
            bail!("model mapping repeats target key '{target_key}'");
        }
        let (request_season, request_target) = scoped_targets_by_key
            .get(target_key.as_str())
            .copied()
            .ok_or_else(|| anyhow!("model mapping references unknown target '{target_key}'"))?;
        let mut matching_targets = context.targets.iter().filter(|candidate| {
            candidate.target_key == request_target.target_key
                && candidate.title == request_target.title
                && candidate.season_number == request_target.season_number
                && candidate.episode_number == request_target.episode_number
                && candidate.absolute_episode_number == request_target.absolute_episode_number
                && candidate.tvdb_episode_id == request_target.tvdb_episode_id
                && candidate.anidb_episode_id == request_target.anidb_episode_id
                && candidate
                    .anilist_season_id
                    .as_deref()
                    .map(|id| {
                        id.trim()
                            .eq_ignore_ascii_case(request_season.anilist_id.trim())
                    })
                    .unwrap_or(candidate.season_number == Some(request_season.season_number))
        });
        let target = matching_targets.next().ok_or_else(|| {
            anyhow!(
                "model target '{target_key}' cannot be resolved back to its scoped canonical target"
            )
        })?;
        if matching_targets.next().is_some() {
            bail!("model target '{target_key}' has ambiguous canonical source rows");
        }
        targets.push(target);
    }
    if targets.is_empty() {
        bail!("model mapping must cover at least one target");
    }
    let media_file_indexes = candidate
        .files
        .iter()
        .enumerate()
        .filter_map(|(file_index, file)| {
            (is_anime_media_file(&file.path) && !is_anime_sample_or_extra_file(&file.path))
                .then_some(file_index)
        })
        .collect::<BTreeSet<_>>();
    let selected_files = match matched.selected_file_keys.as_deref() {
        None => Vec::new(),
        Some([]) => bail!("model mapping returned an empty selected file list"),
        Some(file_keys) => file_keys
            .iter()
            .map(|file_key| {
                let file_source = source_map
                    .file_source(&matched.candidate_key, file_key)
                    .ok_or_else(|| anyhow!("model mapping references unknown file '{file_key}'"))?;
                if file_source.candidate_index != candidate_source.candidate_index {
                    bail!("model file source belongs to a different candidate");
                }
                let file = candidate
                    .files
                    .get(file_source.file_index)
                    .ok_or_else(|| anyhow!("model file source index is out of bounds"))?;
                if !selectable_anime_media_file(file) {
                    bail!("model selected an unavailable or non-media file");
                }
                let identity = file_identities
                    .get(file_source.file_index)
                    .cloned()
                    .ok_or_else(|| anyhow!("model file identity index is out of bounds"))?;
                Ok(ModelSelectedAnimeFile {
                    candidate_file_index: file_source.file_index,
                    identity,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };

    let parsed = parse_anime_release_title(&candidate.title);
    let parsed_release_kind = anime_release_kind_for_coverage(&parsed);
    let selected_file_indexes = selected_files
        .iter()
        .map(|selected| selected.candidate_file_index)
        .collect::<BTreeSet<_>>();
    if selected_file_indexes.len() != selected_files.len() {
        bail!("model mapping repeats an underlying candidate file");
    }
    if !selected_file_indexes
        .iter()
        .all(|file_index| media_file_indexes.contains(file_index))
    {
        bail!("model mapping selected a file outside the candidate media inventory");
    }
    if !media_file_indexes.is_empty() && selected_files.is_empty() {
        bail!("inventoried model mapping requires explicit selected files");
    }

    let unselected_media_indexes = media_file_indexes
        .difference(&selected_file_indexes)
        .copied()
        .collect::<Vec<_>>();
    let requires_file_selection = !unselected_media_indexes.is_empty();
    if requires_file_selection {
        if !file_selection_supported {
            bail!("model mapping cannot safely exclude unselected media files on this surface");
        }
        for file_index in &media_file_indexes {
            let file = candidate
                .files
                .get(*file_index)
                .ok_or_else(|| anyhow!("candidate media file index is out of bounds"))?;
            if !candidate_file_has_safe_selection_identity(file) {
                bail!("model mapping cannot safely select every inventoried media file");
            }
        }
    }

    let shape = model_derived_coverage_shape(
        parsed_release_kind,
        targets.len(),
        selected_files.len(),
        media_file_indexes.len(),
    )?;
    let coverage_kind = anime_coverage_kind(shape.release_kind);
    let entries =
        model_derived_coverage_entries(&targets, &selected_files, shape.binding, coverage_kind);
    let selected_file_keys = selected_files
        .iter()
        .map(|selected| selected.identity.file_key.clone())
        .collect();

    Ok(AnimeFileCoveragePlan {
        resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
        resolver_version: ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
        release_kind: shape.release_kind,
        confidence: ReleaseConfidence::High,
        requires_file_list: false,
        requires_file_selection,
        selected_file_keys,
        entries,
        review_reasons: Vec::new(),
        rejection_reasons: Vec::new(),
    })
}

#[derive(Debug, Clone)]
struct ModelSelectedAnimeFile {
    candidate_file_index: usize,
    identity: AcquisitionReleaseFileIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelTargetFileBinding {
    None,
    SharedMultiEpisode,
    Positional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ModelDerivedCoverageShape {
    release_kind: ReleaseKind,
    binding: ModelTargetFileBinding,
}

fn model_derived_coverage_shape(
    parsed_release_kind: ReleaseKind,
    target_count: usize,
    selected_file_count: usize,
    media_file_count: usize,
) -> Result<ModelDerivedCoverageShape> {
    if target_count == 0 {
        bail!("model mapping must cover at least one target");
    }
    let parsed_pack = matches!(
        parsed_release_kind,
        ReleaseKind::SeasonPack | ReleaseKind::MultiSeasonPack | ReleaseKind::SeriesPack
    );

    if media_file_count == 0 {
        if selected_file_count != 0 {
            bail!("model mapping selected files without a candidate media inventory");
        }
        if parsed_pack {
            bail!("pack model mapping requires a real file inventory and explicit selection");
        }
        if parsed_release_kind == ReleaseKind::Unknown {
            bail!("unknown model mapping requires a real file inventory and explicit selection");
        }
        if target_count > 1 {
            if parsed_release_kind != ReleaseKind::MultiEpisode {
                bail!("multi-target model mapping without files requires a parsed non-pack range");
            }
            return Ok(ModelDerivedCoverageShape {
                release_kind: ReleaseKind::MultiEpisode,
                binding: ModelTargetFileBinding::None,
            });
        }
        if parsed_release_kind == ReleaseKind::MultiEpisode {
            bail!("a parsed range cannot collapse to one target without explicit file selection");
        }
        return Ok(ModelDerivedCoverageShape {
            release_kind: ReleaseKind::Single,
            binding: ModelTargetFileBinding::None,
        });
    }

    if selected_file_count == 0 {
        bail!("inventoried model mapping requires explicit selected files");
    }
    if parsed_pack {
        if selected_file_count != target_count {
            bail!("inventoried pack mappings require one selected file per target");
        }
        return Ok(ModelDerivedCoverageShape {
            release_kind: parsed_release_kind,
            binding: ModelTargetFileBinding::Positional,
        });
    }
    if target_count > 1 && selected_file_count == 1 {
        if parsed_release_kind != ReleaseKind::MultiEpisode {
            bail!("one file may cover multiple targets only for a parsed non-pack range");
        }
        return Ok(ModelDerivedCoverageShape {
            release_kind: ReleaseKind::MultiEpisode,
            binding: ModelTargetFileBinding::SharedMultiEpisode,
        });
    }
    if selected_file_count != target_count {
        bail!("model target and selected-file counts do not define a safe positional mapping");
    }

    Ok(ModelDerivedCoverageShape {
        release_kind: if parsed_release_kind == ReleaseKind::MultiEpisode || target_count > 1 {
            ReleaseKind::MultiEpisode
        } else {
            ReleaseKind::Single
        },
        binding: ModelTargetFileBinding::Positional,
    })
}

fn model_derived_coverage_entries(
    targets: &[&crate::acquisition::release_resolution::anime::AnimeCandidateTarget],
    selected_files: &[ModelSelectedAnimeFile],
    binding: ModelTargetFileBinding,
    coverage_kind: crate::acquisition::release_resolution::models::ReleaseCoverageKind,
) -> Vec<AnimeFileCoverageEntry> {
    targets
        .iter()
        .enumerate()
        .map(|(target_index, target)| {
            let selected_file = match binding {
                ModelTargetFileBinding::None => None,
                ModelTargetFileBinding::SharedMultiEpisode => selected_files.first(),
                ModelTargetFileBinding::Positional => selected_files.get(target_index),
            };
            AnimeFileCoverageEntry {
                target_key: target.target_key.clone(),
                canonical_key: target.canonical_key.clone(),
                release_file_key: selected_file.map(|selected| selected.identity.file_key.clone()),
                file_id: selected_file.and_then(|selected| selected.identity.file_id.clone()),
                file_index: selected_file.and_then(|selected| selected.identity.file_index),
                path: selected_file.map(|selected| selected.identity.path.clone()),
                coverage_kind,
                confidence: ReleaseConfidence::High,
                score: None,
                reason: match binding {
                    ModelTargetFileBinding::None => "local_model_canonical_target",
                    ModelTargetFileBinding::SharedMultiEpisode => {
                        "local_model_shared_multi_episode_file"
                    }
                    ModelTargetFileBinding::Positional => "local_model_positional_target_file",
                }
                .to_string(),
                state: ReleaseCoverageState::Planned,
            }
        })
        .collect()
}

fn candidate_file_has_safe_selection_identity(file: &AcquisitionCandidateFile) -> bool {
    file.selectable.unwrap_or(true)
        && file
            .file_id
            .as_deref()
            .is_some_and(|file_id| !file_id.trim().is_empty())
}

#[derive(Debug, Clone)]
struct AcquisitionReleaseFileIdentity {
    file_key: String,
    file_id: Option<String>,
    file_index: Option<i64>,
    path: String,
}

fn acquisition_release_file_identity(
    file: &AcquisitionCandidateFile,
    ordinal: usize,
) -> AcquisitionReleaseFileIdentity {
    let file_index = file
        .file_index
        .or_else(|| i64::try_from(ordinal).ok().map(|value| value + 1));
    let file_id = file
        .file_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let file_key = file_id
        .clone()
        .or_else(|| file_index.map(|value| value.to_string()))
        .unwrap_or_else(|| file.path.clone());
    AcquisitionReleaseFileIdentity {
        file_key,
        file_id,
        file_index,
        path: file.path.clone(),
    }
}

fn acquisition_match_scope(scope: AcquisitionRequestScope) -> AnimeMatchScope {
    match scope {
        AcquisitionRequestScope::Subscription => AnimeMatchScope::Subscription,
        AcquisitionRequestScope::Movie => AnimeMatchScope::Movie,
        AcquisitionRequestScope::Episode => AnimeMatchScope::Episode,
        AcquisitionRequestScope::Season => AnimeMatchScope::Season,
        AcquisitionRequestScope::Range => AnimeMatchScope::Range,
        AcquisitionRequestScope::Missing => AnimeMatchScope::Missing,
        AcquisitionRequestScope::SelectedTargets => AnimeMatchScope::SelectedTargets,
        AcquisitionRequestScope::AnimeArc => AnimeMatchScope::AnimeArc,
    }
}

/// Assess the exact parser-derived audio profile carried by a model-selected
/// candidate against the same effective anime preference used by acquisition.
/// The boolean is the automatic-selection gate: RequireReview is satisfied
/// only by a positive evidence match.
pub(crate) fn assess_acquisition_anime_model_audio_profile(
    subscription: &AcquisitionSubscription,
    audio_profile: AnimeMatchAudioProfile,
) -> (LanguagePreferenceAssessment, bool) {
    let preference = effective_acquisition_anime_language_preference(subscription);
    let evidence = acquisition_model_audio_profile_evidence(audio_profile);
    let assessment = assess_language_preference(&preference, MediaType::Anime, &evidence);
    let required_preference_satisfied = preference.mode != LanguagePreferenceMode::RequireReview
        || assessment.state == LanguagePreferenceAssessmentState::Match;
    (assessment, required_preference_satisfied)
}

/// Assess deterministic provider evidence through the same final language
/// policy used for a model-selected candidate's derived audio profile. Required
/// audio must be positively proven; provider hints alone never turn unknown
/// evidence into a match.
pub(crate) fn assess_acquisition_anime_candidate_audio(
    subscription: &AcquisitionSubscription,
    candidate: &AcquisitionCandidate,
) -> (LanguagePreferenceAssessment, bool) {
    let preference = effective_acquisition_anime_language_preference(subscription);
    let evidence = acquisition_candidate_language_evidence(candidate);
    let assessment = assess_language_preference(&preference, MediaType::Anime, &evidence);
    let model_required_preference = candidate
        .raw
        .as_ref()
        .and_then(|raw| {
            raw.pointer("/serverEvidence/modelMapping/languagePolicy/requiredPreferenceSatisfied")
        })
        .and_then(serde_json::Value::as_bool);
    let required_preference_satisfied = preference.mode != LanguagePreferenceMode::RequireReview
        || model_required_preference
            .unwrap_or(assessment.state == LanguagePreferenceAssessmentState::Match);
    (assessment, required_preference_satisfied)
}

/// Assess the exact provider file list without allowing a broader release-title
/// claim to hide explicit contradictory filename evidence. Generic filenames
/// still fall back to release-level title/language/raw evidence, so common
/// releases do not require a model call merely because the provider omitted
/// audio tags. Unselected provider files are never part of that fallback.
pub(crate) fn assess_acquisition_anime_provider_file_audio(
    subscription: &AcquisitionSubscription,
    candidate: &AcquisitionCandidate,
    selected_file_keys: &[String],
) -> (LanguagePreferenceAssessment, bool) {
    let complete = assess_acquisition_anime_candidate_audio(subscription, candidate);
    let release_level = {
        let mut release_level_candidate = candidate.clone();
        release_level_candidate.files.clear();
        assess_acquisition_anime_candidate_audio(subscription, &release_level_candidate)
    };
    let preference = effective_acquisition_anime_language_preference(subscription);
    if preference.mode != LanguagePreferenceMode::RequireReview || candidate.files.is_empty() {
        return complete;
    }

    let selected_file_keys = selected_file_keys
        .iter()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
        .collect::<BTreeSet<_>>();
    let mut exact_files = candidate.clone();
    if !selected_file_keys.is_empty() {
        exact_files.files = candidate
            .files
            .iter()
            .enumerate()
            .filter_map(|(ordinal, file)| {
                let identity = acquisition_release_file_identity(file, ordinal);
                let selected = selected_file_keys.contains(identity.file_key.as_str())
                    || identity
                        .file_id
                        .as_deref()
                        .is_some_and(|file_id| selected_file_keys.contains(file_id))
                    || identity
                        .file_index
                        .map(|file_index| file_index.to_string())
                        .as_deref()
                        .is_some_and(|file_index| selected_file_keys.contains(file_index))
                    || selected_file_keys.contains(identity.path.as_str());
                selected.then_some(file.clone())
            })
            .collect();
    }
    exact_files.title.clear();
    exact_files.language = None;
    exact_files.raw = None;
    for file in &mut exact_files.files {
        file.path = file
            .path
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(file.path.as_str())
            .to_string();
    }
    let file_evidence = acquisition_candidate_language_evidence(&exact_files);
    let file_assessment = assess_language_preference(&preference, MediaType::Anime, &file_evidence);
    if !selected_file_keys.is_empty() && exact_files.files.is_empty() {
        return (file_assessment, false);
    }
    match file_assessment.state {
        LanguagePreferenceAssessmentState::Match => (file_assessment, true),
        LanguagePreferenceAssessmentState::Mismatch => (file_assessment, false),
        LanguagePreferenceAssessmentState::Unknown | LanguagePreferenceAssessmentState::Off => {
            release_level
        }
    }
}

fn effective_acquisition_anime_language_preference(
    subscription: &AcquisitionSubscription,
) -> AcquisitionLanguagePreference {
    let configured = language_preference_from_quality_profile(
        subscription.quality_profile.as_ref(),
        MediaType::Anime,
    );
    if configured.active() {
        return configured;
    }

    acquisition_anime_audio_preference(subscription)
        .and_then(|preference| preference.to_language_preference(MediaType::Anime))
        .unwrap_or(configured)
}

pub(crate) fn acquisition_model_audio_profile_evidence(
    audio_profile: AnimeMatchAudioProfile,
) -> CandidateLanguageEvidence {
    let mut evidence = CandidateLanguageEvidence::default();
    match audio_profile {
        AnimeMatchAudioProfile::Unknown => {}
        AnimeMatchAudioProfile::DualAudio => {
            evidence.audio.extend(["en".to_string(), "ja".to_string()]);
            evidence.profiles.insert("dual_audio".to_string());
        }
        AnimeMatchAudioProfile::Subbed => {
            evidence.profiles.insert("subbed".to_string());
        }
        AnimeMatchAudioProfile::Dubbed => {
            evidence.audio.insert("en".to_string());
            evidence.profiles.insert("dubbed".to_string());
        }
        AnimeMatchAudioProfile::JaAudioEnSubs => {
            evidence.audio.insert("ja".to_string());
            evidence.subtitles.insert("en".to_string());
            evidence.profiles.insert("ja_audio_en_subs".to_string());
        }
        AnimeMatchAudioProfile::EnAudio => {
            evidence.audio.insert("en".to_string());
            evidence.profiles.insert("en_audio".to_string());
        }
    }
    evidence
}

fn acquisition_anime_audio_preference(
    subscription: &AcquisitionSubscription,
) -> Option<AnimeAudioPreference> {
    subscription
        .quality_profile
        .as_ref()
        .and_then(|profile| {
            profile
                .get("animeAudioPreference")
                .or_else(|| profile.get("anime_audio_preference"))
        })
        .and_then(|value| serde_json::from_value::<AnimeAudioPreference>(value.clone()).ok())
        .map(|preference| preference.normalized())
}

fn acquisition_audio_preference(
    subscription: &AcquisitionSubscription,
) -> AnimeMatchAudioPreference {
    let language_preference = effective_acquisition_anime_language_preference(subscription);
    let explicit = acquisition_anime_audio_preference(subscription);
    let rule = language_preference.rule_for_media_type(MediaType::Anime);
    let mode = match explicit.as_ref().map(|preference| preference.mode) {
        Some(AnimeAudioPreferenceMode::PreferDub) => AnimeMatchAudioPreferenceMode::PreferDub,
        Some(AnimeAudioPreferenceMode::RequireDubReview) => {
            AnimeMatchAudioPreferenceMode::RequireDub
        }
        Some(AnimeAudioPreferenceMode::Any) => AnimeMatchAudioPreferenceMode::Any,
        None => match language_preference.mode {
            LanguagePreferenceMode::Off => AnimeMatchAudioPreferenceMode::Any,
            LanguagePreferenceMode::Prefer => AnimeMatchAudioPreferenceMode::Prefer,
            LanguagePreferenceMode::RequireReview => AnimeMatchAudioPreferenceMode::Require,
        },
    };
    let mut languages = rule.audio;
    if let Some(language) = explicit.and_then(|preference| preference.language) {
        languages.push(language);
    }
    AnimeMatchAudioPreference {
        mode,
        languages: sorted_unique_strings(languages),
        subtitle_languages: sorted_unique_strings(rule.subtitles),
        accepted_profiles: if language_preference.active()
            || mode != AnimeMatchAudioPreferenceMode::Any
        {
            sorted_unique_strings(rule.profiles)
        } else {
            Vec::new()
        },
    }
}

pub(crate) fn acquisition_match_context(
    canonical_title: &str,
    context: &AnimeCandidateScoringContext,
    target: &crate::anime_matching::AnimeMatchTarget,
) -> Result<AnimeMatchContext> {
    let graph_fingerprint = context
        .graph_fingerprint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("anime matcher acquisition context has no graph fingerprint"))?
        .to_string();

    let mut seasons = BTreeMap::<(i32, String), AnimeMatchSeasonContext>::new();
    for target in &context.targets {
        let (season_number, anilist_id) = target_season_identity(target, &context.scoped_aliases)?;
        let entry = seasons
            .entry((season_number, anilist_id.clone()))
            .or_insert_with(|| AnimeMatchSeasonContext {
                season_number,
                anilist_id,
                aliases: Vec::new(),
                targets: Vec::new(),
            });
        entry.targets.push(AnimeMatchContextTarget {
            target_key: target.target_key.clone(),
            title: target.title.clone(),
            season_number: target.season_number,
            episode_number: target.episode_number,
            absolute_episode_number: target.absolute_episode_number,
            tvdb_episode_id: target.tvdb_episode_id.clone(),
            anidb_episode_id: target.anidb_episode_id.clone(),
        });
    }
    if seasons.is_empty() {
        bail!("anime matcher acquisition context has no canonical targets");
    }

    let episode_titles = context
        .targets
        .iter()
        .map(|target| anime_match_alias_equivalence_key(&target.title))
        .collect::<BTreeSet<_>>();
    let scoped_alias_values = context
        .scoped_aliases
        .iter()
        .map(|alias| anime_match_alias_equivalence_key(&alias.display))
        .collect::<BTreeSet<_>>();
    for season in seasons.values_mut() {
        insert_alias(
            &mut season.aliases,
            AnimeMatchAlias {
                value: canonical_title.trim().to_string(),
                kind: AnimeMatchAliasKind::Canonical,
                source: Some("canonical_title".to_string()),
                language: None,
            },
        );
        for alias in &context.aliases {
            let normalized = anime_match_alias_equivalence_key(alias);
            if episode_titles.contains(&normalized) || scoped_alias_values.contains(&normalized) {
                continue;
            }
            insert_alias(
                &mut season.aliases,
                AnimeMatchAlias {
                    value: alias.trim().to_string(),
                    kind: AnimeMatchAliasKind::Synonym,
                    source: Some("graph_alias".to_string()),
                    language: None,
                },
            );
        }
        let season_number = season.season_number;
        let anilist_id = season.anilist_id.clone();
        for alias in context
            .scoped_aliases
            .iter()
            .filter(|alias| scoped_alias_matches_season(alias, season_number, &anilist_id))
        {
            insert_alias(
                &mut season.aliases,
                AnimeMatchAlias {
                    value: alias.display.trim().to_string(),
                    kind: classify_anime_match_alias(
                        alias.language.as_deref(),
                        Some(&alias.source),
                        &alias.display,
                    ),
                    source: Some(alias.source.clone()),
                    language: alias.language.clone(),
                },
            );
        }
        if season.season_number > 1 {
            for (value, source) in [
                (
                    format!("{} Season {}", canonical_title.trim(), season.season_number),
                    "generated_season_ordinal",
                ),
                (
                    format!("{} S{}", canonical_title.trim(), season.season_number),
                    "generated_season_short",
                ),
                (
                    format!("{} S{:02}", canonical_title.trim(), season.season_number),
                    "generated_season_short",
                ),
            ] {
                insert_alias(
                    &mut season.aliases,
                    AnimeMatchAlias {
                        value,
                        kind: AnimeMatchAliasKind::Generated,
                        source: Some(source.to_string()),
                        language: None,
                    },
                );
            }
        }
        season.aliases.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| {
                    anime_match_alias_equivalence_key(&left.value)
                        .cmp(&anime_match_alias_equivalence_key(&right.value))
                })
                .then_with(|| left.source.cmp(&right.source))
        });
        season.targets.sort_by_key(|target| {
            (
                target.season_number.unwrap_or(i32::MAX),
                target.episode_number.unwrap_or(i32::MAX),
                target.absolute_episode_number.unwrap_or(i32::MAX),
                target.target_key.clone(),
            )
        });
    }

    Ok(scope_anime_match_context(
        AnimeMatchContext {
            graph_fingerprint,
            seasons: seasons.into_values().collect(),
        },
        target,
    ))
}

fn target_season_identity(
    target: &crate::acquisition::release_resolution::anime::AnimeCandidateTarget,
    aliases: &[AnimeScopedAlias],
) -> Result<(i32, String)> {
    let direct_anilist_id = target
        .anilist_season_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let anilist_id = direct_anilist_id
        .or_else(|| {
            one_distinct_string(
                aliases
                    .iter()
                    .filter_map(|alias| alias.anilist_season_id.clone()),
            )
        })
        .ok_or_else(|| {
            anyhow!(
                "anime matcher target '{}' has no unambiguous AniList season identity",
                target.target_key
            )
        })?;
    let mapped_seasons_for_id = aliases
        .iter()
        .filter_map(|alias| {
            alias
                .anilist_season_id
                .as_deref()
                .filter(|alias_id| alias_id.trim().eq_ignore_ascii_case(&anilist_id))
                .and(alias.season_number)
        })
        .collect::<BTreeSet<_>>();
    if mapped_seasons_for_id.len() > 1 {
        bail!(
            "anime matcher target '{}' has ambiguous relation seasons for its AniList identity",
            target.target_key
        );
    }
    let season_number = mapped_seasons_for_id
        .first()
        .copied()
        .or(target.season_number)
        .or_else(|| one_distinct_i32(aliases.iter().filter_map(|alias| alias.season_number)))
        .ok_or_else(|| {
            anyhow!(
                "anime matcher target '{}' has no season identity",
                target.target_key
            )
        })?;
    // `target.season_number` may use TVDB episode numbering while scoped
    // aliases use franchise/relation numbering. Validate the resolved pair,
    // not that those two numbering domains happen to be equal.
    if !mapped_seasons_for_id.is_empty() && !mapped_seasons_for_id.contains(&season_number) {
        bail!(
            "anime matcher target '{}' has a conflicting season/AniList identity",
            target.target_key
        );
    }
    let mapped_ids_for_season = aliases
        .iter()
        .filter(|alias| alias.season_number == Some(season_number))
        .filter_map(|alias| alias.anilist_season_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !mapped_ids_for_season.is_empty()
        && !mapped_ids_for_season
            .iter()
            .any(|alias_id| alias_id.eq_ignore_ascii_case(&anilist_id))
    {
        bail!(
            "anime matcher target '{}' has a conflicting season/AniList identity",
            target.target_key
        );
    }
    Ok((season_number, anilist_id))
}

fn scoped_alias_matches_season(
    alias: &AnimeScopedAlias,
    season_number: i32,
    anilist_id: &str,
) -> bool {
    let mut matched = false;
    if let Some(alias_season) = alias.season_number {
        if alias_season != season_number {
            return false;
        }
        matched = true;
    }
    if let Some(alias_id) = alias.anilist_season_id.as_deref() {
        if !alias_id.trim().eq_ignore_ascii_case(anilist_id) {
            return false;
        }
        matched = true;
    }
    matched
}

fn insert_alias(aliases: &mut Vec<AnimeMatchAlias>, alias: AnimeMatchAlias) {
    if alias.value.trim().is_empty() {
        return;
    }
    let key = anime_match_alias_equivalence_key(&alias.value);
    if let Some(existing) = aliases
        .iter_mut()
        .find(|existing| anime_match_alias_equivalence_key(&existing.value) == key)
    {
        if alias_kind_specificity(alias.kind) > alias_kind_specificity(existing.kind) {
            *existing = alias;
        }
        return;
    }
    aliases.push(alias);
}

fn alias_kind_specificity(kind: AnimeMatchAliasKind) -> u8 {
    match kind {
        AnimeMatchAliasKind::Canonical => 6,
        AnimeMatchAliasKind::English => 5,
        AnimeMatchAliasKind::Romaji => 4,
        AnimeMatchAliasKind::Native => 3,
        AnimeMatchAliasKind::Generated => 2,
        AnimeMatchAliasKind::Synonym => 1,
    }
}

pub(crate) fn acquisition_candidate_parse_facts(
    candidate: &AcquisitionCandidate,
) -> AnimeMatchParseFacts {
    let parsed = parse_anime_release_title(&candidate.title);
    let mut title_candidates = Vec::new();
    title_candidates.extend(parsed.normalized_title.clone());
    title_candidates.extend(parsed.series_title.clone());
    title_candidates.extend(parsed.alt_titles.iter().cloned());
    title_candidates.extend(parsed.sonarr_facts.all_titles.iter().cloned());
    title_candidates.extend(parsed.anime_signal_facts.title_candidates.iter().cloned());
    title_candidates.extend(
        parsed
            .anime_signal_facts
            .title_season_alias_candidates
            .iter()
            .cloned(),
    );

    let season_numbers = sorted_unique_i32(
        parsed
            .season_number
            .into_iter()
            .chain(parsed.sonarr_facts.season_number)
            .chain(
                parsed
                    .anime_signal_facts
                    .classifier_hints
                    .iter()
                    .filter_map(|hint| hint.season),
            ),
    );
    let episode_numbers = sorted_unique_i32(
        parsed
            .episode_numbers
            .iter()
            .copied()
            .chain(parsed.sonarr_facts.episode_numbers.iter().copied())
            .chain(
                parsed
                    .anime_signal_facts
                    .classifier_hints
                    .iter()
                    .filter_map(|hint| hint.episode),
            ),
    );
    let absolute_episode_numbers = sorted_unique_i32(
        parsed
            .absolute_episode_numbers
            .iter()
            .copied()
            .chain(parsed.sonarr_facts.absolute_episode_numbers.iter().copied())
            .chain(
                parsed
                    .anime_signal_facts
                    .classifier_hints
                    .iter()
                    .filter_map(|hint| hint.absolute_episode),
            ),
    );

    let evidence = acquisition_candidate_language_evidence(candidate);
    let japanese_audio = evidence.audio.contains("ja")
        || parsed
            .audio_languages
            .iter()
            .any(|language| language == "ja");
    let english_subtitles = evidence.subtitles.contains("en")
        || parsed
            .subtitle_languages
            .iter()
            .any(|language| language == "en");
    let mut audio_profiles = evidence.profiles.into_iter().collect::<Vec<_>>();
    if parsed.quality.dual_audio || parsed.anime_signal_facts.dual_audio {
        audio_profiles.push("dual_audio".to_string());
    }
    if parsed.anime_signal_facts.english_dub {
        audio_profiles.push("dubbed".to_string());
        audio_profiles.push("en_audio".to_string());
    }
    if parsed.quality.multi_sub || parsed.anime_signal_facts.multi_sub {
        audio_profiles.push("subbed".to_string());
    }
    let mut languages = evidence.audio.into_iter().collect::<Vec<_>>();
    languages.extend(evidence.subtitles);
    languages.extend(parsed.audio_languages.iter().cloned());
    languages.extend(parsed.subtitle_languages.iter().cloned());
    let languages = sorted_unique_strings(languages);
    if japanese_audio
        && english_subtitles
        && !audio_profiles.iter().any(|value| value == "dual_audio")
    {
        audio_profiles.push("ja_audio_en_subs".to_string());
    }

    AnimeMatchParseFacts {
        title_candidates: sorted_unique_strings(title_candidates),
        season_numbers,
        episode_numbers,
        absolute_episode_numbers,
        release_kind: Some(parsed.sonarr_facts.release_kind.as_str().to_string())
            .filter(|value| value != "unknown"),
        batch_kind: Some(anime_batch_kind_name(parsed.batch_kind).to_string()),
        audio_profiles: sorted_unique_strings(audio_profiles),
        languages,
    }
}

pub(crate) fn acquisition_candidate_language_evidence(
    candidate: &AcquisitionCandidate,
) -> CandidateLanguageEvidence {
    let mut evidence = CandidateLanguageEvidence::default();
    add_language_evidence_text(&mut evidence, &candidate.title);
    if let Some(language) = candidate.language.as_deref() {
        add_language_evidence_text(&mut evidence, language);
    }
    for file in &candidate.files {
        add_language_evidence_text(&mut evidence, &file.path);
    }
    if let Some(raw) = candidate.raw.as_ref() {
        for pointer in [
            "/language",
            "/languages",
            "/languageProfile",
            "/languageProfiles",
            "/audioLanguage",
            "/audioLanguages",
            "/mediaEvidence/language",
            "/mediaEvidence/languageProfile",
            "/mediaEvidence/languageProfiles",
            "/mediaEvidence/audioLanguage",
            "/mediaEvidence/audioLanguages",
            "/serverEvidence/languagePreference/evidenceAudioLanguages",
            "/serverEvidence/languagePreference/evidenceProfiles",
        ] {
            if let Some(value) = raw.pointer(pointer) {
                add_language_evidence_value(&mut evidence, value);
            }
        }
        for pointer in [
            "/subtitleLanguage",
            "/subtitleLanguages",
            "/mediaEvidence/subtitleLanguage",
            "/mediaEvidence/subtitleLanguages",
            "/serverEvidence/languagePreference/evidenceSubtitleLanguages",
        ] {
            if let Some(value) = raw.pointer(pointer) {
                add_subtitle_language_evidence_value(&mut evidence, value);
            }
        }
    }
    evidence
}

fn anime_batch_kind_name(kind: AnimeBatchKind) -> &'static str {
    match kind {
        AnimeBatchKind::Single => "single",
        AnimeBatchKind::Range => "range",
        AnimeBatchKind::SeasonPack => "season_pack",
        AnimeBatchKind::MultiSeasonPack => "multi_season_pack",
        AnimeBatchKind::CompleteSeries => "complete_series",
        AnimeBatchKind::Movie => "movie",
        AnimeBatchKind::UnknownBatch => "unknown_batch",
    }
}

pub(crate) fn selectable_anime_media_file(file: &AcquisitionCandidateFile) -> bool {
    file.selectable.unwrap_or(true)
        && is_anime_media_file(&file.path)
        && !is_anime_sample_or_extra_file(&file.path)
}

fn is_anime_media_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    [
        ".mkv", ".mp4", ".avi", ".mov", ".m4v", ".wmv", ".ts", ".m2ts", ".webm",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension))
}

fn is_anime_sample_or_extra_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.contains("/sample")
        || lower.contains("sample.")
        || lower.contains("/extras/")
        || lower.contains("/extra/")
        || lower.contains(" creditless ")
        || lower.contains(" ncop")
        || lower.contains(" nced")
}

fn target_sort_key(target: &AcquisitionTarget) -> (i32, i32, i32, String) {
    (
        target.season_number.unwrap_or(i32::MAX),
        target.episode_number.unwrap_or(i32::MAX),
        target.absolute_episode_number.unwrap_or(i32::MAX),
        target.target_key.clone(),
    )
}

fn sorted_unique_strings(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn sorted_unique_i32(values: impl IntoIterator<Item = i32>) -> Vec<i32> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn one_distinct_i32(values: impl IntoIterator<Item = i32>) -> Option<i32> {
    let mut values = values.into_iter().collect::<BTreeSet<_>>();
    (values.len() == 1).then(|| values.pop_first()).flatten()
}

fn one_shared_present_i32(values: impl IntoIterator<Item = Option<i32>>) -> Option<i32> {
    let mut values = values.into_iter();
    let Some(Some(first)) = values.next() else {
        return None;
    };
    values.all(|value| value == Some(first)).then_some(first)
}

fn one_distinct_string(values: impl IntoIterator<Item = String>) -> Option<String> {
    let mut values = values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| (value.to_ascii_lowercase(), value))
        .collect::<BTreeMap<_, _>>();
    (values.len() == 1)
        .then(|| values.pop_first().map(|(_, value)| value))
        .flatten()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use chrono::{Duration as ChronoDuration, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::{
        acquisition::{
            release_resolution::anime::{
                AnimeCandidateInput, AnimeCandidateTarget, AnimeCoverageOptions,
                AnimeReleaseFileInput, AnimeScopedAlias, plan_anime_file_coverage,
                plan_anime_file_coverage_with_options,
            },
            release_resolution::models::ReleaseCoverageKind,
            subscriptions::{
                AcquisitionCompletionPolicy, AcquisitionMetadataPolicy, AcquisitionMonitorPolicy,
                AcquisitionRoutePolicy, AcquisitionSubscriptionStatus, AcquisitionTargetState,
            },
        },
        anime_matching::{
            ANIME_MATCH_SCHEMA_VERSION, AnimeDeterministicResult, AnimeMatchAudioProfile,
            AnimeMatchEngine, AnimeMatchFallbackReason, AnimeMatchRequest, AnimeMatchResponse,
            AnimeMatchingService,
        },
    };

    #[derive(Clone)]
    struct StaticMatchEngine {
        response: AnimeMatchResponse,
    }

    #[async_trait]
    impl AnimeMatchEngine for StaticMatchEngine {
        async fn match_candidates(
            &self,
            _request: AnimeMatchRequest,
        ) -> Result<AnimeMatchResponse> {
            Ok(self.response.clone())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestPersistenceDisposition {
        PersistAnimeRelease,
        InternalManualReview,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct AcquisitionFallbackSemantics {
        selected_candidate_source: String,
        canonical_selection: Vec<(String, Option<String>)>,
        coverage_plan: AnimeFileCoveragePlan,
        selected_file_keys: Vec<String>,
        route_eligible: bool,
        persistence_disposition: TestPersistenceDisposition,
    }

    fn acquisition_fallback_semantics(
        candidate: &AcquisitionCandidate,
        plan: &AnimeFileCoveragePlan,
    ) -> AcquisitionFallbackSemantics {
        let route_eligible = candidate
            .default_route
            .as_deref()
            .is_some_and(|route| candidate.supported_routes.iter().any(|item| item == route));
        let persistence_disposition = if route_eligible
            && matches!(
                plan.confidence,
                ReleaseConfidence::High | ReleaseConfidence::Medium
            )
            && !plan.entries.is_empty()
            && plan.rejection_reasons.is_empty()
        {
            TestPersistenceDisposition::PersistAnimeRelease
        } else {
            TestPersistenceDisposition::InternalManualReview
        };
        AcquisitionFallbackSemantics {
            selected_candidate_source: candidate.source.clone(),
            canonical_selection: plan
                .entries
                .iter()
                .map(|entry| (entry.target_key.clone(), entry.canonical_key.clone()))
                .collect(),
            coverage_plan: plan.clone(),
            selected_file_keys: plan.selected_file_keys.clone(),
            route_eligible,
            persistence_disposition,
        }
    }

    fn tokyo_ghoul_subscription() -> AcquisitionSubscription {
        let now = Utc::now();
        AcquisitionSubscription {
            subscription_id: Uuid::new_v4(),
            media_type: MediaType::Anime,
            title: "Tokyo Ghoul".to_string(),
            normalized_title: "tokyo ghoul".to_string(),
            year: Some(2014),
            external_ids: None,
            idempotency_key: None,
            request_mode: Default::default(),
            request_scope: AcquisitionRequestScope::Episode,
            scope: None,
            metadata_policy: AcquisitionMetadataPolicy::Recurring,
            completion_policy: AcquisitionCompletionPolicy::Manual,
            monitor_policy: AcquisitionMonitorPolicy::AllMissing,
            route_policy: AcquisitionRoutePolicy::DebridFirst,
            source_provider_id: None,
            release_delay_seconds: 0,
            quality_profile: Some(json!({
                "animeAudioPreference": { "mode": "prefer_dub", "language": "en" },
                "languagePreference": {
                    "mode": "prefer",
                    "anime": {
                        "profiles": ["en_audio", "dual_audio", "dubbed"]
                    }
                }
            })),
            metadata_refresh_after: now,
            candidate_search_after: now,
            last_metadata_refresh_at: None,
            last_candidate_search_at: None,
            tracking_started_at: None,
            status: AcquisitionSubscriptionStatus::Active,
            active: true,
            created_at: now,
            updated_at: now,
        }
    }

    fn tokyo_ghoul_target(subscription: &AcquisitionSubscription) -> AcquisitionTarget {
        let now = Utc::now();
        AcquisitionTarget {
            target_id: Uuid::new_v4(),
            subscription_id: subscription.subscription_id,
            target_key: "S02E01".to_string(),
            media_type: MediaType::Anime,
            title: "New Surge".to_string(),
            season_number: Some(2),
            episode_number: Some(1),
            absolute_episode_number: Some(13),
            air_date: None,
            air_time: Some(now - ChronoDuration::days(1)),
            metadata: None,
            state: AcquisitionTargetState::Pending,
            state_reason: None,
            selected_provider_id: None,
            selected_route_logical_id: None,
            selected_candidate: None,
            download_id: None,
            import_event_id: None,
            search_attempts: 0,
            last_search_at: None,
            next_search_after: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn tokyo_ghoul_second_target(subscription: &AcquisitionSubscription) -> AcquisitionTarget {
        let mut target = tokyo_ghoul_target(subscription);
        target.target_key = "S02E02".to_string();
        target.title = "Dancing Flowers".to_string();
        target.episode_number = Some(2);
        target.absolute_episode_number = Some(14);
        target
    }

    fn tokyo_ghoul_context() -> AnimeCandidateScoringContext {
        AnimeCandidateScoringContext {
            graph_fingerprint: Some("rr3-scoped-tokyo-ghoul".to_string()),
            aliases: vec!["Tokyo Ghoul".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Tokyo Ghoul Root A".to_string(),
                    source: "anizip_title".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(2),
                    anilist_season_id: Some("1002".to_string()),
                },
                AnimeScopedAlias {
                    display: "Tokyo Ghoul √A".to_string(),
                    source: "anizip_title".to_string(),
                    language: Some("x-jat".to_string()),
                    season_number: Some(2),
                    anilist_season_id: Some("1002".to_string()),
                },
                AnimeScopedAlias {
                    display: "東京喰種トーキョーグール√A".to_string(),
                    source: "anizip_title".to_string(),
                    language: Some("ja".to_string()),
                    season_number: Some(2),
                    anilist_season_id: Some("1002".to_string()),
                },
                AnimeScopedAlias {
                    display: "Tokyo Ghoul Season 2".to_string(),
                    source: "generated_season_ordinal".to_string(),
                    language: None,
                    season_number: Some(2),
                    anilist_season_id: Some("1002".to_string()),
                },
            ],
            targets: vec![AnimeCandidateTarget {
                target_key: "S02E01".to_string(),
                canonical_key: Some("anilist:1002:S02E01".to_string()),
                title: "New Surge".to_string(),
                season_number: Some(2),
                anilist_season_id: Some("1002".to_string()),
                episode_number: Some(1),
                absolute_episode_number: Some(13),
                tvdb_episode_id: Some("2013".to_string()),
                anidb_episode_id: Some("3013".to_string()),
            }],
        }
    }

    fn tokyo_ghoul_two_episode_context() -> AnimeCandidateScoringContext {
        let mut context = tokyo_ghoul_context();
        context.targets.push(AnimeCandidateTarget {
            target_key: "S02E02".to_string(),
            canonical_key: Some("anilist:1002:S02E02".to_string()),
            title: "Dancing Flowers".to_string(),
            season_number: Some(2),
            anilist_season_id: Some("1002".to_string()),
            episode_number: Some(2),
            absolute_episode_number: Some(14),
            tvdb_episode_id: Some("2014".to_string()),
            anidb_episode_id: Some("3014".to_string()),
        });
        context
    }

    fn candidate(title: &str, source: &str) -> AcquisitionCandidate {
        AcquisitionCandidate {
            id: Some("provider-duplicate".to_string()),
            title: title.to_string(),
            source: source.to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("private-info-hash".to_string()),
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(1_000_000),
            seeders: Some(50),
            language: None,
            cached_debrid: Some(true),
            rank: Some(1),
            score: Some(1.0),
            score_badges: Vec::new(),
            files: vec![
                AcquisitionCandidateFile {
                    file_id: Some("provider-file-duplicate".to_string()),
                    file_index: Some(7),
                    path: format!("{title}.mkv"),
                    size_bytes: Some(1_000_000),
                    selectable: Some(true),
                },
                AcquisitionCandidateFile {
                    file_id: Some("sample".to_string()),
                    file_index: Some(8),
                    path: format!("Extras/sample.{title}.mkv"),
                    size_bytes: Some(100),
                    selectable: Some(true),
                },
            ],
            supported_routes: vec!["acquisition.debrid.default".to_string()],
            default_route: Some("acquisition.debrid.default".to_string()),
            raw: Some(json!({ "providerSecret": "must-not-serialize" })),
        }
    }

    fn selectable_media_file(
        file_id: &str,
        file_index: i64,
        path: &str,
    ) -> AcquisitionCandidateFile {
        AcquisitionCandidateFile {
            file_id: Some(file_id.to_string()),
            file_index: Some(file_index),
            path: path.to_string(),
            size_bytes: Some(1_000_000),
            selectable: Some(true),
        }
    }

    #[test]
    fn alm5_acquisition_adapter_builds_shared_tokyo_ghoul_request_with_private_stable_keys()
    -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let candidates = vec![
            candidate(
                "[Group] Tokyo Ghoul Root A - 01 [1080p] [Dual Audio]",
                "magnet:?xt=urn:btih:first-private",
            ),
            candidate(
                "[Other] Tokyo Ghoul √A - 01 [1080p]",
                "magnet:?xt=urn:btih:second-private",
            ),
        ];

        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "search-group-id",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let request = prepared.request();

        assert_eq!(request.schema_version, ANIME_MATCH_SCHEMA_VERSION);
        assert_eq!(request.request_id, "search-group-id");
        assert_eq!(request.target.canonical_title, "Tokyo Ghoul");
        assert_eq!(request.target.scope, AnimeMatchScope::Episode);
        assert_eq!(request.target.wanted_target_keys, vec!["S02E01"]);
        assert_eq!(request.target.season_number, Some(2));
        assert_eq!(request.target.episode_numbers, vec![1]);
        assert_eq!(request.target.absolute_episode_numbers, vec![13]);
        assert_eq!(
            request.target.audio_preference.mode,
            AnimeMatchAudioPreferenceMode::PreferDub
        );
        assert_eq!(request.target.audio_preference.languages, vec!["en"]);
        assert_eq!(
            request.target.audio_preference.accepted_profiles,
            vec!["dual_audio", "dubbed", "en_audio"]
        );

        assert_eq!(request.context.graph_fingerprint, "rr3-scoped-tokyo-ghoul");
        assert_eq!(request.context.seasons.len(), 1);
        let season = &request.context.seasons[0];
        assert_eq!(season.season_number, 2);
        assert_eq!(season.anilist_id, "1002");
        assert_eq!(season.targets[0].target_key, "S02E01");
        assert_eq!(season.targets[0].absolute_episode_number, Some(13));
        assert_eq!(season.targets[0].tvdb_episode_id.as_deref(), Some("2013"));
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
            alias.value == "Tokyo Ghoul S2"
                && alias.kind == AnimeMatchAliasKind::Generated
                && alias.source.as_deref() == Some("generated_season_short")
        }));
        assert!(season.aliases.iter().any(|alias| {
            alias.value == "Tokyo Ghoul S02"
                && alias.kind == AnimeMatchAliasKind::Generated
                && alias.source.as_deref() == Some("generated_season_short")
        }));

        assert_eq!(request.candidates[0].candidate_key, "candidate-0");
        assert_eq!(request.candidates[1].candidate_key, "candidate-1");
        assert_eq!(request.candidates[0].files.len(), 1);
        assert_eq!(
            request.candidates[0].files[0].file_key,
            "candidate-0-file-0"
        );
        assert!(
            request.candidates[0]
                .parse_facts
                .audio_profiles
                .iter()
                .any(|profile| profile == "dual_audio")
        );

        assert_eq!(
            prepared
                .source_map()
                .candidate_source("candidate-0")
                .copied(),
            Some(AcquisitionAnimeCandidateSource { candidate_index: 0 })
        );
        assert_eq!(
            prepared
                .source_map()
                .file_source("candidate-0", "candidate-0-file-0")
                .copied(),
            Some(AcquisitionAnimeFileSource {
                candidate_index: 0,
                file_index: 0,
            })
        );
        assert!(
            prepared
                .source_map()
                .file_source("candidate-1", "candidate-0-file-0")
                .is_none()
        );

        let serialized = serde_json::to_string(request)?;
        for private in [
            "first-private",
            "second-private",
            "private-info-hash",
            "provider-duplicate",
            "provider-file-duplicate",
            "must-not-serialize",
            "acquisition.debrid.default",
        ] {
            assert!(
                !serialized.contains(private),
                "leaked private value {private}"
            );
        }
        Ok(())
    }

    #[test]
    fn alm5_acquisition_adapter_preserves_unscoped_graph_aliases() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let mut context = tokyo_ghoul_context();
        context.aliases.push("Tokyo Kushu".to_string());
        context.aliases.push("New Surge".to_string());
        context.aliases.push("Tokyo Ghoul Root A".to_string());
        context.aliases.push("Tokyo Ghoul Re".to_string());
        context.scoped_aliases.push(AnimeScopedAlias {
            display: "Tokyo Ghoul".to_string(),
            source: "anilist_season_title".to_string(),
            language: Some("en".to_string()),
            season_number: Some(1),
            anilist_season_id: Some("1001".to_string()),
        });
        context.scoped_aliases.push(AnimeScopedAlias {
            display: "Tokyo Ghoul:Re".to_string(),
            source: "anilist_season_title".to_string(),
            language: Some("en".to_string()),
            season_number: Some(2),
            anilist_season_id: Some("1002".to_string()),
        });
        context.targets.push(AnimeCandidateTarget {
            target_key: "S01E12".to_string(),
            canonical_key: Some("anilist:1001:S01E12".to_string()),
            title: "Ghoul".to_string(),
            season_number: Some(1),
            anilist_season_id: Some("1001".to_string()),
            episode_number: Some(12),
            absolute_episode_number: Some(12),
            tvdb_episode_id: Some("2012".to_string()),
            anidb_episode_id: Some("3012".to_string()),
        });
        let candidates = vec![candidate("Tokyo Kushu - 01", "private")];

        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "graph-alias",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let season_one = prepared
            .request()
            .context
            .seasons
            .iter()
            .find(|season| season.season_number == 1)
            .expect("adjacent first season");
        let season_two = prepared
            .request()
            .context
            .seasons
            .iter()
            .find(|season| season.season_number == 2)
            .expect("wanted second season");
        assert!(season_two.aliases.iter().any(|alias| {
            alias.value == "Tokyo Kushu"
                && alias.kind == AnimeMatchAliasKind::Synonym
                && alias.source.as_deref() == Some("graph_alias")
        }));
        assert!(
            season_two
                .aliases
                .iter()
                .all(|alias| alias.value != "New Surge"),
            "episode display titles must remain targets, not season aliases"
        );
        assert!(
            season_one
                .aliases
                .iter()
                .all(|alias| alias.value != "Tokyo Ghoul Root A"),
            "a scoped season alias must not be reattached through flat graph aliases"
        );
        assert!(season_two.aliases.iter().any(|alias| {
            alias.value == "Tokyo Ghoul Root A" && alias.source.as_deref() == Some("anizip_title")
        }));
        assert!(season_two.aliases.iter().any(|alias| {
            alias.value == "Tokyo Ghoul:Re"
                && alias.source.as_deref() == Some("anilist_season_title")
        }));
        let punctuation_key = anime_match_alias_equivalence_key("Tokyo Ghoul Re");
        assert!(
            season_one.aliases.iter().all(|alias| {
                anime_match_alias_equivalence_key(&alias.value) != punctuation_key
            })
        );
        Ok(())
    }

    #[test]
    fn alm5_audio_profile_inference_preserves_audio_subtitle_direction() {
        let mut english_audio = candidate("Tokyo Ghoul Root A - 01", "private");
        english_audio.raw = Some(json!({
            "mediaEvidence": {
                "audioLanguages": ["en"],
                "subtitleLanguages": ["ja"]
            }
        }));
        let english_audio_facts = acquisition_candidate_parse_facts(&english_audio);
        assert!(
            !english_audio_facts
                .audio_profiles
                .iter()
                .any(|profile| profile == "ja_audio_en_subs")
        );

        let mut japanese_audio = candidate("Tokyo Ghoul Root A - 01", "private");
        japanese_audio.raw = Some(json!({
            "mediaEvidence": {
                "audioLanguages": ["ja"],
                "subtitleLanguages": ["en"]
            }
        }));
        let japanese_audio_facts = acquisition_candidate_parse_facts(&japanese_audio);
        assert!(
            japanese_audio_facts
                .audio_profiles
                .iter()
                .any(|profile| profile == "ja_audio_en_subs")
        );
    }

    #[test]
    fn alm5_acquisition_adapter_keys_are_repeatable_and_ignore_duplicate_provider_ids() -> Result<()>
    {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let candidates = vec![
            candidate("Tokyo Ghoul Root A - 01", "private:first"),
            candidate("Tokyo Ghoul Root A - 01", "private:second"),
        ];
        let prepare = || {
            AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
                "stable-request",
                &subscription,
                &[target.clone()],
                &context,
                &candidates,
            )?)
            .map_err(anyhow::Error::from)
        };

        let first = prepare()?;
        let second = prepare()?;
        assert_eq!(first.request(), second.request());
        assert_eq!(first.source_map().candidate_count(), 2);
        assert_eq!(first.source_map().file_count(), 2);
        Ok(())
    }

    #[test]
    fn alm5_acquisition_and_library_adapters_emit_the_same_logical_request() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let mut release = candidate(
            "[Group] Tokyo Ghoul Root A - 01 [1080p] [Dual Audio]",
            "magnet:?xt=urn:btih:private-acquisition-source",
        );
        release.files = vec![AcquisitionCandidateFile {
            file_id: Some("private-provider-file-id".to_string()),
            file_index: Some(7),
            path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
            size_bytes: Some(1_000_000),
            selectable: Some(true),
        }];

        let acquisition =
            AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
                "search-group-id",
                &subscription,
                &[target],
                &context,
                &[release],
            )?)?;
        let library =
            crate::library::anime_matching_adapter::tests::tokyo_ghoul_library_request_fixture();

        assert_eq!(acquisition.request(), &library);
        assert_eq!(
            serde_json::to_vec(acquisition.request())?,
            serde_json::to_vec(&library)?
        );
        Ok(())
    }

    #[test]
    fn alm5_acquisition_and_library_adapters_agree_when_relation_season_differs() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let mut target = tokyo_ghoul_target(&subscription);
        target.target_key = "S03E01".to_string();
        target.season_number = Some(3);
        let mut context = tokyo_ghoul_context();
        context.targets[0].target_key = "S03E01".to_string();
        context.targets[0].canonical_key = Some("anilist:1002:S03E01".to_string());
        context.targets[0].season_number = Some(3);
        let mut release = candidate(
            "[Group] Tokyo Ghoul Root A - 01 [1080p] [Dual Audio]",
            "magnet:?xt=urn:btih:private-relation-season-source",
        );
        release.files = vec![AcquisitionCandidateFile {
            file_id: Some("private-provider-file-id".to_string()),
            file_index: Some(7),
            path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
            size_bytes: Some(1_000_000),
            selectable: Some(true),
        }];

        let acquisition =
            AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
                "search-group-id",
                &subscription,
                &[target],
                &context,
                &[release],
            )?)?;
        let library = crate::library::anime_matching_adapter::tests::
            tokyo_ghoul_relation_season_mismatch_request_fixture();

        assert_eq!(acquisition.request(), &library);
        assert_eq!(acquisition.request().context.seasons[0].season_number, 2);
        assert_eq!(
            acquisition.request().context.seasons[0].targets[0].season_number,
            Some(3)
        );
        Ok(())
    }

    #[test]
    fn alm5_target_season_identity_rejects_ambiguous_or_conflicting_alias_evidence() {
        let context = tokyo_ghoul_context();
        let mut target = context.targets[0].clone();
        target.anilist_season_id = None;
        let mut ambiguous_aliases = context.scoped_aliases.clone();
        ambiguous_aliases.push(AnimeScopedAlias {
            display: "Conflicting season-two identity".to_string(),
            source: "fixture".to_string(),
            language: None,
            season_number: Some(2),
            anilist_season_id: Some("different-id".to_string()),
        });
        assert!(target_season_identity(&target, &ambiguous_aliases).is_err());

        target.anilist_season_id = Some("unknown-id".to_string());
        assert!(target_season_identity(&target, &context.scoped_aliases).is_err());

        target.anilist_season_id = Some("1002".to_string());
        let mut ambiguous_relation_seasons = context.scoped_aliases.clone();
        ambiguous_relation_seasons.push(AnimeScopedAlias {
            display: "Same AniList entry in another relation slot".to_string(),
            source: "fixture".to_string(),
            language: None,
            season_number: Some(3),
            anilist_season_id: Some("1002".to_string()),
        });
        assert!(target_season_identity(&target, &ambiguous_relation_seasons).is_err());

        target.anilist_season_id = None;
        target.season_number = Some(3);
        let mut missing_identity_with_multiple_relations = context.scoped_aliases.clone();
        missing_identity_with_multiple_relations.push(AnimeScopedAlias {
            display: "Tokyo Ghoul:Re".to_string(),
            source: "fixture".to_string(),
            language: Some("en".to_string()),
            season_number: Some(3),
            anilist_season_id: Some("1003".to_string()),
        });
        assert!(
            target_season_identity(&target, &missing_identity_with_multiple_relations).is_err(),
            "TVDB season numbering must not guess among multiple relation identities"
        );
    }

    #[test]
    fn alm5_mixed_seasonal_and_absolute_targets_have_no_preferred_season() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let seasonal = tokyo_ghoul_target(&subscription);
        let mut absolute = tokyo_ghoul_target(&subscription);
        absolute.target_key = "A0001".to_string();
        absolute.title = "Absolute One".to_string();
        absolute.season_number = None;
        absolute.episode_number = None;
        absolute.absolute_episode_number = Some(1);
        let mut context = tokyo_ghoul_context();
        context.scoped_aliases.push(AnimeScopedAlias {
            display: "Tokyo Ghoul".to_string(),
            source: "anilist_season_title".to_string(),
            language: Some("en".to_string()),
            season_number: Some(1),
            anilist_season_id: Some("1001".to_string()),
        });
        context.targets.push(AnimeCandidateTarget {
            target_key: "A0001".to_string(),
            canonical_key: Some("anilist:1001:A0001".to_string()),
            title: "Absolute One".to_string(),
            season_number: None,
            anilist_season_id: Some("1001".to_string()),
            episode_number: None,
            absolute_episode_number: Some(1),
            tvdb_episode_id: None,
            anidb_episode_id: Some("3001".to_string()),
        });

        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "mixed-numbering",
            &subscription,
            &[seasonal, absolute],
            &context,
            &[candidate("Tokyo Ghoul 01", "private:mixed-numbering")],
        )?)?;

        assert_eq!(prepared.request().target.season_number, None);
        assert_eq!(
            prepared
                .request()
                .context
                .seasons
                .iter()
                .flat_map(|season| season.targets.iter())
                .filter(|target| prepared
                    .request()
                    .target
                    .wanted_target_keys
                    .contains(&target.target_key))
                .count(),
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm5_model_mapping_can_replace_a_wrong_deterministic_hypothesis_with_existing_coverage()
    -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let mut context = tokyo_ghoul_context();
        context.scoped_aliases.push(AnimeScopedAlias {
            display: "Tokyo Ghoul".to_string(),
            source: "anilist_season_title".to_string(),
            language: Some("en".to_string()),
            season_number: Some(1),
            anilist_season_id: Some("1001".to_string()),
        });
        context.targets.push(AnimeCandidateTarget {
            target_key: "S01E01".to_string(),
            canonical_key: Some("anilist:1001:S01E01".to_string()),
            title: "Tragedy".to_string(),
            season_number: Some(1),
            anilist_season_id: Some("1001".to_string()),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            tvdb_episode_id: Some("2001".to_string()),
            anidb_episode_id: Some("3001".to_string()),
        });
        let candidates = vec![AcquisitionCandidate {
            files: Vec::new(),
            ..candidate(
                "[Group] Tokyo Ghoul S01E01 [1080p]",
                "magnet:?xt=urn:btih:wrong-parser",
            )
        }];
        let mut deterministic = plan_anime_file_coverage(
            &context,
            &AnimeCandidateInput {
                title: candidates[0].title.clone(),
                source_kind: candidates[0].source_kind.clone(),
                ..Default::default()
            },
            &[],
        );
        assert!(!deterministic.entries.is_empty());
        assert_eq!(
            deterministic
                .entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["S01E01"])
        );
        deterministic.confidence = ReleaseConfidence::ReviewRequired;
        deterministic
            .review_reasons
            .push("ambiguous_parser_hypothesis".to_string());
        assert_eq!(
            acquisition_anime_deterministic_state(&deterministic),
            DeterministicMatchState::Difficult
        );

        let input = acquisition_anime_match_batch_input(
            "override",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: None,
        };
        let service = AnimeMatchingService::with_engine(Arc::new(StaticMatchEngine {
            response: AnimeMatchResponse {
                schema_version: ANIME_MATCH_SCHEMA_VERSION,
                matches: vec![matched],
            },
        }));
        let outcome = service
            .match_or_fallback(
                AnimeDeterministicResult::difficult(deterministic.clone()),
                input,
                |_, request, matches, source_map| {
                    let mut plans = model_derived_anime_coverage_plans(
                        request,
                        &context,
                        &candidates,
                        subscription.route_policy,
                        matches,
                        source_map,
                    )?;
                    if plans.len() != 1 {
                        bail!("expected exactly one model candidate mapping");
                    }
                    Ok(plans.remove(0).plan)
                },
            )
            .await;
        assert!(outcome.used_model());
        let plan = outcome.value;
        assert_ne!(plan, deterministic);
        assert_eq!(plan.release_kind, ReleaseKind::Single);
        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.entries.len(), 1);
        assert_eq!(plan.entries[0].target_key, "S02E01");
        assert_eq!(
            plan.entries[0].canonical_key.as_deref(),
            Some("anilist:1002:S02E01")
        );
        assert_eq!(plan.entries[0].reason, "local_model_canonical_target");
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Definitive
        );
        Ok(())
    }

    #[test]
    fn alm7_model_range_can_override_deterministic_numbering_without_file_inventory() -> Result<()>
    {
        let subscription = tokyo_ghoul_subscription();
        let targets = vec![
            tokyo_ghoul_target(&subscription),
            tokyo_ghoul_second_target(&subscription),
        ];
        let context = tokyo_ghoul_two_episode_context();
        let candidates = vec![AcquisitionCandidate {
            files: Vec::new(),
            ..candidate(
                "Tokyo Ghoul Root A - 101-102 [1080p]",
                "private:model-range-override",
            )
        }];
        assert_eq!(
            anime_release_kind_for_coverage(&parse_anime_release_title(&candidates[0].title)),
            ReleaseKind::MultiEpisode
        );
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "model-range-override",
            &subscription,
            &targets,
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E02".to_string(), "S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: None,
        };

        let plans = model_derived_anime_coverage_plans(
            prepared.request(),
            &context,
            &candidates,
            subscription.route_policy,
            &[matched],
            prepared.source_map(),
        )?;
        let plan = &plans[0].plan;
        assert_eq!(plan.release_kind, ReleaseKind::MultiEpisode);
        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert!(!plan.requires_file_list);
        assert!(!plan.requires_file_selection);
        assert!(plan.selected_file_keys.is_empty());
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E02", "S02E01"]
        );
        assert!(plan.entries.iter().all(|entry| {
            entry.release_file_key.is_none()
                && entry.reason == "local_model_canonical_target"
                && entry.coverage_kind == ReleaseCoverageKind::MultiEpisodeRange
        }));
        Ok(())
    }

    #[test]
    fn alm7_model_pack_uses_explicit_positional_bindings_independent_of_file_parse() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let targets = vec![
            tokyo_ghoul_target(&subscription),
            tokyo_ghoul_second_target(&subscription),
        ];
        let context = tokyo_ghoul_two_episode_context();
        let candidates = vec![AcquisitionCandidate {
            files: vec![
                selectable_media_file("opaque-alpha", 41, "payload.S01E09.mkv"),
                selectable_media_file("opaque-beta", 42, "payload.S01E10.mkv"),
            ],
            ..candidate(
                "Tokyo Ghoul Root A Season 2 Batch [1080p]",
                "private:model-pack-override",
            )
        }];
        assert_eq!(
            anime_release_kind_for_coverage(&parse_anime_release_title(&candidates[0].title)),
            ReleaseKind::SeasonPack
        );
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "model-pack-override",
            &subscription,
            &targets,
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E02".to_string(), "S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: Some(vec![
                "candidate-0-file-0".to_string(),
                "candidate-0-file-1".to_string(),
            ]),
        };

        let plans = model_derived_anime_coverage_plans(
            prepared.request(),
            &context,
            &candidates,
            subscription.route_policy,
            &[matched],
            prepared.source_map(),
        )?;
        let plan = &plans[0].plan;
        assert_eq!(plan.release_kind, ReleaseKind::SeasonPack);
        assert!(!plan.requires_file_selection);
        assert_eq!(plan.selected_file_keys, vec!["opaque-alpha", "opaque-beta"]);
        assert_eq!(plan.entries.len(), 2);
        assert_eq!(plan.entries[0].target_key, "S02E02");
        assert_eq!(
            plan.entries[0].release_file_key.as_deref(),
            Some("opaque-alpha")
        );
        assert_eq!(plan.entries[0].file_index, Some(41));
        assert_eq!(plan.entries[1].target_key, "S02E01");
        assert_eq!(
            plan.entries[1].release_file_key.as_deref(),
            Some("opaque-beta")
        );
        assert_eq!(plan.entries[1].file_index, Some(42));
        assert!(
            plan.entries
                .iter()
                .all(|entry| entry.reason == "local_model_positional_target_file")
        );
        Ok(())
    }

    #[test]
    fn alm7_one_non_pack_range_file_can_bind_multiple_ordered_targets() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let targets = vec![
            tokyo_ghoul_target(&subscription),
            tokyo_ghoul_second_target(&subscription),
        ];
        let context = tokyo_ghoul_two_episode_context();
        let candidates = vec![AcquisitionCandidate {
            files: vec![selectable_media_file(
                "range-file",
                77,
                "Tokyo Ghoul Root A - 01-02.mkv",
            )],
            ..candidate(
                "Tokyo Ghoul Root A - 01-02 [1080p]",
                "private:shared-range-file",
            )
        }];
        assert_eq!(
            anime_release_kind_for_coverage(&parse_anime_release_title(&candidates[0].title)),
            ReleaseKind::MultiEpisode
        );
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "shared-range-file",
            &subscription,
            &targets,
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string(), "S02E02".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
        };

        let plans = model_derived_anime_coverage_plans(
            prepared.request(),
            &context,
            &candidates,
            subscription.route_policy,
            &[matched],
            prepared.source_map(),
        )?;
        let plan = &plans[0].plan;
        assert_eq!(plan.release_kind, ReleaseKind::MultiEpisode);
        assert!(!plan.requires_file_selection);
        assert_eq!(plan.selected_file_keys, vec!["range-file"]);
        assert_eq!(plan.entries.len(), 2);
        assert!(plan.entries.iter().all(|entry| {
            entry.release_file_key.as_deref() == Some("range-file")
                && entry.file_index == Some(77)
                && entry.reason == "local_model_shared_multi_episode_file"
        }));
        Ok(())
    }

    #[test]
    fn alm7_inspected_surface_can_explicitly_allow_safe_overfetch_exclusion() -> Result<()> {
        let mut subscription = tokyo_ghoul_subscription();
        subscription.route_policy = AcquisitionRoutePolicy::TorrentOnly;
        let targets = vec![
            tokyo_ghoul_target(&subscription),
            tokyo_ghoul_second_target(&subscription),
        ];
        let context = tokyo_ghoul_two_episode_context();
        let candidates = vec![AcquisitionCandidate {
            files: vec![
                selectable_media_file("wanted-one", 1, "opaque-one.mkv"),
                selectable_media_file("wanted-two", 2, "opaque-two.mkv"),
                selectable_media_file("overfetch", 3, "opaque-three.mkv"),
            ],
            ..candidate(
                "Tokyo Ghoul Root A Season 2 Batch [1080p]",
                "private:safe-pack-overfetch",
            )
        }];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "safe-pack-overfetch",
            &subscription,
            &targets,
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string(), "S02E02".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: Some(vec![
                "candidate-0-file-0".to_string(),
                "candidate-0-file-1".to_string(),
            ]),
        };

        assert!(
            model_derived_anime_coverage_plans(
                prepared.request(),
                &context,
                &candidates,
                subscription.route_policy,
                std::slice::from_ref(&matched),
                prepared.source_map(),
            )
            .is_err(),
            "the initial route-derived converter must remain conservative"
        );
        let plans = model_derived_anime_coverage_plans_with_file_selection_support(
            prepared.request(),
            &context,
            &candidates,
            true,
            &[matched],
            prepared.source_map(),
        )?;
        let plan = &plans[0].plan;
        assert!(plan.requires_file_selection);
        assert_eq!(plan.selected_file_keys, vec!["wanted-one", "wanted-two"]);
        assert_eq!(plan.entries.len(), 2);
        assert!(
            plan.entries
                .iter()
                .all(|entry| entry.release_file_key.as_deref() != Some("overfetch"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_model_path_covers_localized_part_absolute_special_ova_and_movie_targets()
    -> Result<()> {
        struct Fixture {
            label: &'static str,
            canonical_title: &'static str,
            release_title: &'static str,
            target_key: &'static str,
            target_title: &'static str,
            season_number: Option<i32>,
            episode_number: Option<i32>,
            absolute_episode_number: Option<i32>,
            request_scope: AcquisitionRequestScope,
            aliases: &'static [(&'static str, &'static str)],
        }

        let fixtures = [
            Fixture {
                label: "romaji_split_cour",
                canonical_title: "Mushoku Tensei: Jobless Reincarnation",
                release_title: "Mushoku Tensei II Part 2 - 01 [1080p]",
                target_key: "S02E13",
                target_title: "Turning Point 3",
                season_number: Some(2),
                episode_number: Some(13),
                absolute_episode_number: Some(36),
                request_scope: AcquisitionRequestScope::Episode,
                aliases: &[
                    ("Mushoku Tensei II Part 2", "x-jat"),
                    ("無職転生 II ～異世界行ったら本気だす～ 第2クール", "ja"),
                ],
            },
            Fixture {
                label: "native_japanese_absolute_only",
                canonical_title: "Frieren: Beyond Journey's End",
                release_title: "葬送のフリーレン - 27 [1080p]",
                target_key: "A0027",
                target_title: "An Era of Humans",
                season_number: None,
                episode_number: None,
                absolute_episode_number: Some(27),
                request_scope: AcquisitionRequestScope::Episode,
                aliases: &[("Sousou no Frieren", "x-jat"), ("葬送のフリーレン", "ja")],
            },
            Fixture {
                label: "ova",
                canonical_title: "My Hero Academia",
                release_title: "Boku no Hero Academia OVA [1080p]",
                target_key: "S00E01",
                target_title: "Save! Rescue Training!",
                season_number: Some(0),
                episode_number: Some(1),
                absolute_episode_number: None,
                request_scope: AcquisitionRequestScope::Episode,
                aliases: &[
                    ("Boku no Hero Academia OVA", "x-jat"),
                    ("僕のヒーローアカデミア OVA", "ja"),
                ],
            },
            Fixture {
                label: "special",
                canonical_title: "Violet Evergarden",
                release_title: "Violet Evergarden Special [1080p]",
                target_key: "S00E01",
                target_title: "The Day You Understand Love Will Surely Come",
                season_number: Some(0),
                episode_number: Some(1),
                absolute_episode_number: None,
                request_scope: AcquisitionRequestScope::Episode,
                aliases: &[
                    ("Violet Evergarden Special", "en"),
                    ("ヴァイオレット・エヴァーガーデン Extra Episode", "ja"),
                ],
            },
            Fixture {
                label: "anime_movie",
                canonical_title: "Made in Abyss",
                release_title: "Made in Abyss Movie 3 - Fukaki Tamashii no Reimei [1080p]",
                target_key: "MOVIE:anilist:36862",
                target_title: "Dawn of the Deep Soul",
                season_number: Some(0),
                episode_number: Some(1),
                absolute_episode_number: None,
                request_scope: AcquisitionRequestScope::Movie,
                aliases: &[
                    ("Made in Abyss Movie 3: Fukaki Tamashii no Reimei", "x-jat"),
                    ("劇場版メイドインアビス 深き魂の黎明", "ja"),
                ],
            },
        ];

        for fixture in fixtures {
            let mut subscription = tokyo_ghoul_subscription();
            subscription.title = fixture.canonical_title.to_string();
            subscription.normalized_title = fixture.canonical_title.to_ascii_lowercase();
            subscription.request_scope = fixture.request_scope;

            let mut target = tokyo_ghoul_target(&subscription);
            target.target_key = fixture.target_key.to_string();
            target.title = fixture.target_title.to_string();
            target.season_number = fixture.season_number;
            target.episode_number = fixture.episode_number;
            target.absolute_episode_number = fixture.absolute_episode_number;

            let season_number = fixture.season_number.unwrap_or(0);
            let anilist_id = format!("fixture-{}", fixture.label);
            let context = AnimeCandidateScoringContext {
                graph_fingerprint: Some(format!("alm7-{}-graph-v1", fixture.label)),
                aliases: vec![fixture.canonical_title.to_string()],
                scoped_aliases: fixture
                    .aliases
                    .iter()
                    .map(|(value, language)| AnimeScopedAlias {
                        display: (*value).to_string(),
                        source: "alm7_validation_fixture".to_string(),
                        language: Some((*language).to_string()),
                        season_number: Some(season_number),
                        anilist_season_id: Some(anilist_id.clone()),
                    })
                    .collect(),
                targets: vec![AnimeCandidateTarget {
                    target_key: fixture.target_key.to_string(),
                    canonical_key: Some(format!("anilist:{anilist_id}:{}", fixture.target_key)),
                    title: fixture.target_title.to_string(),
                    season_number: fixture.season_number,
                    anilist_season_id: Some(anilist_id),
                    episode_number: fixture.episode_number,
                    absolute_episode_number: fixture.absolute_episode_number,
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                }],
            };
            let candidates = vec![candidate(
                fixture.release_title,
                &format!("private:{}", fixture.label),
            )];
            let input = acquisition_anime_match_batch_input(
                format!("alm7-fixture-{}", fixture.label),
                &subscription,
                std::slice::from_ref(&target),
                &context,
                &candidates,
            )?;
            let response = AnimeMatchResponse {
                schema_version: ANIME_MATCH_SCHEMA_VERSION,
                matches: vec![AnimeCandidateMatch {
                    candidate_key: "candidate-0".to_string(),
                    matched_target_keys: vec![fixture.target_key.to_string()],
                    audio_profile: AnimeMatchAudioProfile::Unknown,
                    selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
                }],
            };
            let service =
                AnimeMatchingService::with_engine(Arc::new(StaticMatchEngine { response }));
            let route_policy = subscription.route_policy;
            let outcome = service
                .match_or_fallback(
                    AnimeDeterministicResult::difficult(Vec::new()),
                    input,
                    |_, request, matches, source_map| {
                        model_derived_anime_coverage_plans(
                            request,
                            &context,
                            &candidates,
                            route_policy,
                            matches,
                            source_map,
                        )
                    },
                )
                .await;

            assert!(outcome.used_model(), "fixture {}", fixture.label);
            assert_eq!(outcome.value.len(), 1, "fixture {}", fixture.label);
            let plan = &outcome.value[0].plan;
            assert_eq!(
                plan.confidence,
                ReleaseConfidence::High,
                "fixture {}",
                fixture.label
            );
            assert_eq!(plan.entries.len(), 1, "fixture {}", fixture.label);
            assert_eq!(
                plan.entries[0].target_key, fixture.target_key,
                "fixture {}",
                fixture.label
            );
            assert_eq!(
                plan.entries[0].release_file_key.as_deref(),
                Some("provider-file-duplicate"),
                "fixture {}",
                fixture.label
            );
            assert_eq!(
                plan.selected_file_keys,
                vec!["provider-file-duplicate"],
                "fixture {}",
                fixture.label
            );
        }
        Ok(())
    }

    #[test]
    fn alm7_model_path_accepts_multi_season_and_complete_series_file_lists() -> Result<()> {
        for (release_title, expected_kind, expected_coverage_kind) in [
            (
                "Example Anime S01-S02 1080p WEB-DL",
                ReleaseKind::MultiSeasonPack,
                ReleaseCoverageKind::MultiSeasonPack,
            ),
            (
                "Example Anime Complete Series 1080p WEB-DL",
                ReleaseKind::SeriesPack,
                ReleaseCoverageKind::SeriesPack,
            ),
        ] {
            let subscription = tokyo_ghoul_subscription();
            let mut targets = Vec::new();
            let mut context_targets = Vec::new();
            let mut scoped_aliases = Vec::new();
            let mut files = Vec::new();
            for (ordinal, (season, episode)) in
                [(1, 1), (1, 2), (2, 1), (2, 2)].into_iter().enumerate()
            {
                let mut target = tokyo_ghoul_target(&subscription);
                target.target_key = format!("S{season:02}E{episode:02}");
                target.title = format!("Season {season} Episode {episode}");
                target.season_number = Some(season);
                target.episode_number = Some(episode);
                target.absolute_episode_number = Some(i32::try_from(ordinal)? + 1);
                context_targets.push(AnimeCandidateTarget {
                    target_key: target.target_key.clone(),
                    canonical_key: Some(format!(
                        "anilist:pack-season-{season}:{}",
                        target.target_key
                    )),
                    title: target.title.clone(),
                    season_number: Some(season),
                    anilist_season_id: Some(format!("pack-season-{season}")),
                    episode_number: Some(episode),
                    absolute_episode_number: target.absolute_episode_number,
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                });
                targets.push(target);
                files.push(selectable_media_file(
                    &format!("provider-pack-file-{ordinal}"),
                    i64::try_from(ordinal)? + 1,
                    &format!("opaque-payload-{ordinal}.mkv"),
                ));
            }
            for season in [1, 2] {
                scoped_aliases.push(AnimeScopedAlias {
                    display: format!("Example Anime Season {season}"),
                    source: "alm7_pack_fixture".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(season),
                    anilist_season_id: Some(format!("pack-season-{season}")),
                });
            }
            let context = AnimeCandidateScoringContext {
                graph_fingerprint: Some("alm7-multi-season-pack-graph-v1".to_string()),
                aliases: vec!["Example Anime".to_string()],
                scoped_aliases,
                targets: context_targets,
            };
            let candidates = vec![AcquisitionCandidate {
                files,
                ..candidate(release_title, "private:multi-season-pack")
            }];
            assert_eq!(
                anime_release_kind_for_coverage(&parse_anime_release_title(release_title)),
                expected_kind
            );
            let prepared =
                AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
                    "alm7-multi-season-pack",
                    &subscription,
                    &targets,
                    &context,
                    &candidates,
                )?)?;
            let matched = AnimeCandidateMatch {
                candidate_key: "candidate-0".to_string(),
                matched_target_keys: targets
                    .iter()
                    .map(|target| target.target_key.clone())
                    .collect(),
                audio_profile: AnimeMatchAudioProfile::Unknown,
                selected_file_keys: Some(
                    (0..targets.len())
                        .map(|index| format!("candidate-0-file-{index}"))
                        .collect(),
                ),
            };

            let plans = model_derived_anime_coverage_plans(
                prepared.request(),
                &context,
                &candidates,
                subscription.route_policy,
                &[matched],
                prepared.source_map(),
            )?;
            let plan = &plans[0].plan;
            assert_eq!(plan.release_kind, expected_kind);
            assert_eq!(plan.entries.len(), targets.len());
            assert_eq!(plan.selected_file_keys.len(), targets.len());
            assert!(plan.entries.iter().all(|entry| {
                entry.coverage_kind == expected_coverage_kind
                    && entry.release_file_key.is_some()
                    && entry.reason == "local_model_positional_target_file"
            }));
        }
        Ok(())
    }

    #[test]
    fn alm7_unknown_release_without_inventory_is_rejected() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let candidates = vec![AcquisitionCandidate {
            files: Vec::new(),
            ..candidate(
                "Tokyo Ghoul Root A Complete",
                "private:unknown-no-inventory",
            )
        }];
        assert_eq!(
            anime_release_kind_for_coverage(&parse_anime_release_title(&candidates[0].title)),
            ReleaseKind::Unknown
        );
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "unknown-no-inventory",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: None,
        };

        assert!(
            model_derived_anime_coverage_plans(
                prepared.request(),
                &context,
                &candidates,
                subscription.route_policy,
                &[matched],
                prepared.source_map(),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn alm7_model_audio_profile_enforces_standalone_required_dub_preference() {
        let mut subscription = tokyo_ghoul_subscription();
        subscription.quality_profile = Some(json!({
            "animeAudioPreference": {
                "mode": "require_dub_review",
                "language": "en"
            }
        }));

        let (dubbed, dubbed_satisfied) = assess_acquisition_anime_model_audio_profile(
            &subscription,
            AnimeMatchAudioProfile::Dubbed,
        );
        assert_eq!(dubbed.state, LanguagePreferenceAssessmentState::Match);
        assert!(dubbed_satisfied);
        assert_eq!(dubbed.matching_audio, vec!["en"]);
        assert!(
            dubbed
                .matching_profiles
                .iter()
                .any(|value| value == "dubbed")
        );

        let (subbed, subbed_satisfied) = assess_acquisition_anime_model_audio_profile(
            &subscription,
            AnimeMatchAudioProfile::Subbed,
        );
        assert_eq!(subbed.state, LanguagePreferenceAssessmentState::Mismatch);
        assert!(!subbed_satisfied);

        let (unknown, unknown_satisfied) = assess_acquisition_anime_model_audio_profile(
            &subscription,
            AnimeMatchAudioProfile::Unknown,
        );
        assert_eq!(unknown.state, LanguagePreferenceAssessmentState::Unknown);
        assert!(!unknown_satisfied);
    }

    #[tokio::test]
    async fn alm5_mixed_model_coverage_failure_is_atomic_and_preserves_baseline() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let candidates = vec![
            AcquisitionCandidate {
                files: Vec::new(),
                ..candidate("Tokyo Ghoul Root A - 01", "private:single")
            },
            AcquisitionCandidate {
                files: Vec::new(),
                ..candidate(
                    "Tokyo Ghoul Root A Complete Series",
                    "private:pack-without-files",
                )
            },
        ];
        let baseline = plan_anime_file_coverage(
            &context,
            &AnimeCandidateInput {
                title: "Tokyo Ghoul Root A - 99".to_string(),
                source_kind: "fixture".to_string(),
                ..Default::default()
            },
            &[],
        );
        let input = acquisition_anime_match_batch_input(
            "atomic-coverage",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?;
        let response = AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![
                AnimeCandidateMatch {
                    candidate_key: "candidate-0".to_string(),
                    matched_target_keys: vec!["S02E01".to_string()],
                    audio_profile: AnimeMatchAudioProfile::Unknown,
                    selected_file_keys: None,
                },
                AnimeCandidateMatch {
                    candidate_key: "candidate-1".to_string(),
                    matched_target_keys: vec!["S02E01".to_string()],
                    audio_profile: AnimeMatchAudioProfile::Unknown,
                    selected_file_keys: None,
                },
            ],
        };
        let service = AnimeMatchingService::with_engine(Arc::new(StaticMatchEngine { response }));
        let outcome = service
            .match_or_fallback(
                AnimeDeterministicResult::difficult(baseline.clone()),
                input,
                |_, request, matches, source_map| {
                    let mut plans = model_derived_anime_coverage_plans(
                        request,
                        &context,
                        &candidates,
                        subscription.route_policy,
                        matches,
                        source_map,
                    )?;
                    Ok(plans.remove(0).plan)
                },
            )
            .await;

        assert_eq!(outcome.value, baseline);
        assert_eq!(
            outcome.provenance.reason,
            Some(AnimeMatchFallbackReason::CoverageValidationFailed)
        );
        assert!(!outcome.used_model());
        Ok(())
    }

    #[tokio::test]
    async fn alm5_disabled_engine_preserves_acquisition_downstream_semantics_exactly() -> Result<()>
    {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let mut release = candidate(
            "[Group] Tokyo Ghoul Root A Season 2 Batch [1080p] [Dual Audio]",
            "magnet:?xt=urn:btih:deterministic-pack",
        );
        release.files.truncate(1);
        release.files[0].path = "Tokyo Ghoul Root A - 01 [1080p].mkv".to_string();

        let candidate_input = AnimeCandidateInput {
            title: release.title.clone(),
            source_kind: release.source_kind.clone(),
            quality: release.quality.clone(),
            size_bytes: release.size_bytes,
            seeders: release.seeders,
            cached_debrid: release.cached_debrid,
            rank: release.rank,
            source_score: release.score,
            supported_routes: release.supported_routes.clone(),
            default_route: release.default_route.clone(),
        };
        let release_files = vec![AnimeReleaseFileInput {
            file_key: release.files[0].file_id.clone().expect("fixture file id"),
            file_id: release.files[0].file_id.clone(),
            file_index: release.files[0].file_index,
            path: release.files[0].path.clone(),
            size_bytes: release.files[0]
                .size_bytes
                .and_then(|value| i64::try_from(value).ok()),
            selectable: true,
        }];
        let mut deterministic = plan_anime_file_coverage_with_options(
            &context,
            &candidate_input,
            &release_files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
        );
        assert_eq!(deterministic.confidence, ReleaseConfidence::High);
        assert_eq!(deterministic.entries.len(), 1);
        assert_eq!(deterministic.selected_file_keys.len(), 1);

        // A real difficult deterministic result may retain useful canonical
        // coverage while routing the candidate to internal review. Preserve
        // every downstream field if the optional engine is unavailable.
        deterministic.confidence = ReleaseConfidence::ReviewRequired;
        deterministic
            .review_reasons
            .push("ambiguous_release_identity".to_string());
        let before = acquisition_fallback_semantics(&release, &deterministic);
        assert!(before.route_eligible);
        assert_eq!(
            before.persistence_disposition,
            TestPersistenceDisposition::InternalManualReview
        );
        assert_eq!(before.canonical_selection.len(), 1);
        assert_eq!(before.selected_file_keys.len(), 1);

        let input = acquisition_anime_match_batch_input(
            "disabled-engine-fallback",
            &subscription,
            &[target],
            &context,
            &[release.clone()],
        )?;
        let outcome = AnimeMatchingService::disabled()
            .match_or_fallback(
                AnimeDeterministicResult {
                    state: acquisition_anime_deterministic_state(&deterministic),
                    value: deterministic,
                },
                input,
                |_, _, _, _| -> Result<AnimeFileCoveragePlan> {
                    panic!("disabled engine must not invoke the model override")
                },
            )
            .await;

        assert_eq!(
            outcome.provenance.reason,
            Some(AnimeMatchFallbackReason::EngineUnavailable)
        );
        let after = acquisition_fallback_semantics(&release, &outcome.value);
        assert_eq!(after, before);
        assert_eq!(after.coverage_plan.entries, before.coverage_plan.entries);
        assert_eq!(after.selected_file_keys, before.selected_file_keys);
        assert_eq!(after.route_eligible, before.route_eligible);
        assert_eq!(
            after.persistence_disposition,
            before.persistence_disposition
        );
        Ok(())
    }

    #[test]
    fn alm5_model_mapping_rejects_ambiguous_multi_file_ownership() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let mut first = candidate("Tokyo Ghoul Root A Batch", "private:pack");
        first.files.push(AcquisitionCandidateFile {
            file_id: Some("second-file".to_string()),
            file_index: Some(9),
            path: "Tokyo Ghoul Root A - 02.mkv".to_string(),
            size_bytes: Some(1_000_000),
            selectable: Some(true),
        });
        let candidates = vec![first];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "ambiguous-pack",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: Some(vec![
                "candidate-0-file-0".to_string(),
                "candidate-0-file-1".to_string(),
            ]),
        };

        assert!(
            model_derived_anime_coverage_plans(
                prepared.request(),
                &context,
                &candidates,
                subscription.route_policy,
                &[matched],
                prepared.source_map(),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn alm5_model_range_cannot_collapse_to_one_wanted_episode() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let mut context = tokyo_ghoul_context();
        context.targets = (1..=12)
            .map(|episode| AnimeCandidateTarget {
                target_key: format!("S02E{episode:02}"),
                canonical_key: Some(format!("anilist:1002:S02E{episode:02}")),
                title: format!("Root A Episode {episode}"),
                season_number: Some(2),
                anilist_season_id: Some("1002".to_string()),
                episode_number: Some(episode),
                absolute_episode_number: Some(episode + 12),
                tvdb_episode_id: Some(format!("20{episode:02}")),
                anidb_episode_id: Some(format!("30{episode:02}")),
            })
            .collect();
        let mut release = candidate(
            "Tokyo Ghoul Root A - 01-12 [1080p]",
            "private:multi-episode-range",
        );
        release.files.clear();
        let candidates = vec![release];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "range-single-wanted",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: None,
        };

        assert_eq!(
            anime_release_kind_for_coverage(&parse_anime_release_title(&candidates[0].title)),
            ReleaseKind::MultiEpisode
        );
        assert!(
            model_derived_anime_coverage_plans(
                prepared.request(),
                &context,
                &candidates,
                subscription.route_policy,
                &[matched],
                prepared.source_map(),
            )
            .is_err(),
            "the full deterministic range must not be collapsed to one model-selected target"
        );
        Ok(())
    }

    #[test]
    fn alm5_single_file_binding_does_not_require_selective_download() -> Result<()> {
        let mut subscription = tokyo_ghoul_subscription();
        subscription.route_policy = AcquisitionRoutePolicy::TorrentOnly;
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let mut release = candidate("Tokyo Ghoul Root A - 01", "private:single-file");
        release.files.truncate(1);
        let candidates = vec![release];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "single-file-binding",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
        };

        let plans = model_derived_anime_coverage_plans(
            prepared.request(),
            &context,
            &candidates,
            subscription.route_policy,
            &[matched],
            prepared.source_map(),
        )?;
        assert_eq!(plans.len(), 1);
        assert!(!plans[0].plan.requires_file_selection);
        assert_eq!(
            plans[0].plan.selected_file_keys,
            vec!["provider-file-duplicate"]
        );
        assert_eq!(
            plans[0].plan.entries[0].release_file_key.as_deref(),
            Some("provider-file-duplicate")
        );
        Ok(())
    }

    fn assert_pack_mapping_rejected(
        route_policy: AcquisitionRoutePolicy,
        files: Vec<AcquisitionCandidateFile>,
    ) -> Result<()> {
        let mut subscription = tokyo_ghoul_subscription();
        subscription.route_policy = route_policy;
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let candidates = vec![AcquisitionCandidate {
            files,
            ..candidate(
                "Tokyo Ghoul Root A Complete Series",
                "private:pack-selection",
            )
        }];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "pack-selection",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
        };

        assert!(
            model_derived_anime_coverage_plans(
                prepared.request(),
                &context,
                &candidates,
                subscription.route_policy,
                &[matched],
                prepared.source_map(),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn alm5_model_pack_rejects_hidden_unselectable_overfetch() -> Result<()> {
        assert_pack_mapping_rejected(
            AcquisitionRoutePolicy::DebridFirst,
            vec![
                AcquisitionCandidateFile {
                    file_id: Some("wanted".to_string()),
                    file_index: Some(1),
                    path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
                    size_bytes: Some(1_000_000),
                    selectable: Some(true),
                },
                AcquisitionCandidateFile {
                    file_id: Some("unselectable-overfetch".to_string()),
                    file_index: Some(2),
                    path: "Tokyo Ghoul Root A - 02.mkv".to_string(),
                    size_bytes: Some(1_000_000),
                    selectable: Some(false),
                },
            ],
        )
    }

    #[test]
    fn alm5_model_pack_rejects_route_without_file_selection() -> Result<()> {
        assert_pack_mapping_rejected(
            AcquisitionRoutePolicy::TorrentOnly,
            vec![
                AcquisitionCandidateFile {
                    file_id: Some("wanted".to_string()),
                    file_index: Some(1),
                    path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
                    size_bytes: Some(1_000_000),
                    selectable: Some(true),
                },
                AcquisitionCandidateFile {
                    file_id: Some("selectable-overfetch".to_string()),
                    file_index: Some(2),
                    path: "Tokyo Ghoul Root A - 02.mkv".to_string(),
                    size_bytes: Some(1_000_000),
                    selectable: Some(true),
                },
            ],
        )
    }

    #[test]
    fn alm5_model_mapping_rejects_duplicate_underlying_file_ids() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let candidates = vec![AcquisitionCandidate {
            files: vec![
                AcquisitionCandidateFile {
                    file_id: Some("duplicate".to_string()),
                    file_index: Some(1),
                    path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
                    size_bytes: Some(1_000_000),
                    selectable: Some(true),
                },
                AcquisitionCandidateFile {
                    file_id: Some("duplicate".to_string()),
                    file_index: Some(2),
                    path: "Tokyo Ghoul Root A - 02.mkv".to_string(),
                    size_bytes: Some(1_000_000),
                    selectable: Some(true),
                },
            ],
            ..candidate("Tokyo Ghoul Root A Batch", "private:duplicate-files")
        }];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "duplicate-file-ids",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["S02E01".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
        };

        assert!(
            model_derived_anime_coverage_plans(
                prepared.request(),
                &context,
                &candidates,
                subscription.route_policy,
                &[matched],
                prepared.source_map(),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn alm5_model_mapping_resolves_scoped_target_identity_before_coverage() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let mut target = tokyo_ghoul_target(&subscription);
        target.target_key = "A0001".to_string();
        target.absolute_episode_number = Some(1);
        let mut context = tokyo_ghoul_context();
        context.scoped_aliases.push(AnimeScopedAlias {
            display: "Tokyo Ghoul".to_string(),
            source: "anilist_season_title".to_string(),
            language: Some("en".to_string()),
            season_number: Some(1),
            anilist_season_id: Some("1001".to_string()),
        });
        context.targets = vec![
            AnimeCandidateTarget {
                target_key: "A0001".to_string(),
                canonical_key: Some("anilist:1001:A0001".to_string()),
                title: "Season One Absolute One".to_string(),
                season_number: Some(1),
                anilist_season_id: Some("1001".to_string()),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                tvdb_episode_id: Some("one".to_string()),
                anidb_episode_id: None,
            },
            AnimeCandidateTarget {
                target_key: "A0001".to_string(),
                canonical_key: Some("anilist:1002:A0001".to_string()),
                title: "Season Two Absolute One".to_string(),
                season_number: Some(2),
                anilist_season_id: Some("1002".to_string()),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                tvdb_episode_id: Some("two".to_string()),
                anidb_episode_id: None,
            },
        ];
        let candidates = vec![AcquisitionCandidate {
            files: Vec::new(),
            ..candidate("Tokyo Ghoul Root A - 01", "private:scoped-target")
        }];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "scoped-target",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let matched = AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec!["A0001".to_string()],
            audio_profile: AnimeMatchAudioProfile::Unknown,
            selected_file_keys: None,
        };
        let plans = model_derived_anime_coverage_plans(
            prepared.request(),
            &context,
            &candidates,
            subscription.route_policy,
            &[matched],
            prepared.source_map(),
        )?;

        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].plan.entries[0].canonical_key.as_deref(),
            Some("anilist:1002:A0001")
        );
        Ok(())
    }

    #[test]
    fn alm5_deterministic_state_matches_current_acquisition_acceptance_gate() {
        let mut plan = AnimeFileCoveragePlan {
            resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
            resolver_version: ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
            release_kind: ReleaseKind::Single,
            confidence: ReleaseConfidence::High,
            requires_file_list: false,
            requires_file_selection: false,
            selected_file_keys: Vec::new(),
            entries: vec![AnimeFileCoverageEntry {
                target_key: "S02E01".to_string(),
                canonical_key: None,
                release_file_key: None,
                file_id: None,
                file_index: None,
                path: None,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: None,
                reason: "test".to_string(),
                state: ReleaseCoverageState::Planned,
            }],
            review_reasons: Vec::new(),
            rejection_reasons: Vec::new(),
        };
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Definitive
        );

        // The current acquisition planner gates on confidence, entries, and
        // rejection reasons. Review text is diagnostic once those gates pass.
        plan.review_reasons.push("unresolved".to_string());
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Definitive
        );

        plan.review_reasons.clear();
        plan.confidence = ReleaseConfidence::Medium;
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Definitive
        );

        plan.confidence = ReleaseConfidence::Low;
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Difficult
        );

        plan.confidence = ReleaseConfidence::ReviewRequired;
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Difficult
        );

        plan.confidence = ReleaseConfidence::High;
        plan.rejection_reasons.push("rejected".to_string());
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Difficult
        );

        plan.rejection_reasons.clear();
        plan.entries.clear();
        assert_eq!(
            acquisition_anime_deterministic_state(&plan),
            DeterministicMatchState::Difficult
        );
    }

    #[test]
    fn alm5_model_request_json_contains_no_acquisition_route_or_persistence_fields() -> Result<()> {
        let subscription = tokyo_ghoul_subscription();
        let target = tokyo_ghoul_target(&subscription);
        let context = tokyo_ghoul_context();
        let candidates = vec![candidate("Tokyo Ghoul Root A - 01", "private")];
        let prepared = AnimeMatchingService::prepare_request(acquisition_anime_match_batch_input(
            "wire-boundary",
            &subscription,
            &[target],
            &context,
            &candidates,
        )?)?;
        let value = serde_json::to_value(prepared.request())?;
        let object = value.as_object().expect("request object");
        assert_eq!(
            object.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "candidates".to_string(),
                "context".to_string(),
                "requestId".to_string(),
                "schemaVersion".to_string(),
                "target".to_string(),
            ])
        );
        assert_eq!(value.pointer("/target/mediaType"), Some(&json!("anime")));
        assert!(value.pointer("/routePolicy").is_none());
        assert!(value.pointer("/selectedProviderId").is_none());
        assert!(value.pointer("/persistenceDecision").is_none());
        Ok(())
    }

    #[test]
    fn alm7_provider_file_audio_contradiction_overrides_release_title_claim() {
        let mut subscription = tokyo_ghoul_subscription();
        subscription.quality_profile = Some(json!({
            "animeAudioPreference": { "mode": "require_dub_review", "language": "en" },
            "languagePreference": {
                "mode": "require_review",
                "anime": { "profiles": ["en_audio", "dual_audio", "dubbed"] }
            }
        }));
        let mut candidate = candidate(
            "[Group] Tokyo Ghoul Root A - 01 [Dual Audio]",
            "private:provider-audio",
        );
        candidate.files = vec![
            selectable_media_file(
                "actual-file-1",
                1,
                "[Group] Tokyo Ghoul Root A [Dual Audio]/Episode 01 [Subbed].mkv",
            ),
            selectable_media_file(
                "unselected-dub-extra",
                2,
                "Extras/English Dub Interview.mkv",
            ),
            selectable_media_file("unselected-english-subtitle", 3, "Subs/episode-01.eng.srt"),
        ];

        assert!(
            assess_acquisition_anime_candidate_audio(&subscription, &candidate).1,
            "the broad release-title evidence intentionally demonstrates the conflict"
        );
        let selected = vec!["actual-file-1".to_string()];
        let (assessment, satisfied) =
            assess_acquisition_anime_provider_file_audio(&subscription, &candidate, &selected);
        assert_eq!(
            assessment.state,
            LanguagePreferenceAssessmentState::Mismatch
        );
        assert!(!satisfied);

        candidate.files[0].path = "Episode 01.mkv".to_string();
        assert!(
            assess_acquisition_anime_provider_file_audio(&subscription, &candidate, &selected).1,
            "an untagged provider basename should retain release-level evidence"
        );

        candidate.title = "[Group] Tokyo Ghoul Root A - 01".to_string();
        assert!(
            assess_acquisition_anime_candidate_audio(&subscription, &candidate).1,
            "the broad candidate evidence intentionally includes the unselected dubbed extra"
        );
        assert!(
            !assess_acquisition_anime_provider_file_audio(&subscription, &candidate, &selected).1,
            "an unselected dubbed extra must not satisfy required audio for a generic selected file"
        );
    }
}
