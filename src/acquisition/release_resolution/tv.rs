use std::collections::BTreeSet;

use chrono::{Datelike, NaiveDate, Utc};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::acquisition::release_resolution::models::{
    ReleaseConfidence, ReleaseCoverageKind, ReleaseCoverageState, ReleaseKind, ReleaseResolverKind,
};

pub const TV_SONARR_STYLE_RESOLVER_VERSION: &str = "rr2-tv-sonarr-style-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TvResolution {
    R360p,
    R480p,
    R540p,
    R576p,
    R720p,
    R1080p,
    R2160p,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TvReleaseSource {
    BluRay,
    WebDl,
    WebRip,
    Hdtv,
    BdRip,
    BrRip,
    Dvd,
    Dsr,
    Pdtv,
    Sdtv,
    TvRip,
    RawHd,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvQuality {
    pub resolution: Option<TvResolution>,
    pub source: Option<TvReleaseSource>,
    pub codec: Option<String>,
    pub remux: bool,
    pub raw_hd: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvReleaseModifiers {
    pub proper: bool,
    pub repack: bool,
    pub real: bool,
    pub version: Option<u8>,
    pub languages: Vec<String>,
    pub edition_tags: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvSeriesTitleInfo {
    pub title_without_year: Option<String>,
    pub year: Option<i32>,
    pub all_titles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvParsedRelease {
    pub original_title: String,
    pub normalized_series_title: Option<String>,
    pub series_title_info: TvSeriesTitleInfo,
    pub season_number: Option<i32>,
    pub season_end_number: Option<i32>,
    pub episode_numbers: Vec<i32>,
    pub air_date: Option<String>,
    pub release_group: Option<String>,
    pub release_tokens: Option<String>,
    pub release_hash: Option<String>,
    pub quality: TvQuality,
    pub modifiers: TvReleaseModifiers,
    pub release_kind: ReleaseKind,
    pub full_season: bool,
    pub full_series: bool,
    pub is_partial_season: bool,
    pub is_season_extra: bool,
    pub season_part: Option<i32>,
    pub daily_part: Option<i32>,
    pub is_mini_series: bool,
    pub special: bool,
    pub is_split_episode: bool,
    pub anime_absolute_hints: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvTarget {
    pub target_id: Uuid,
    pub target_key: String,
    pub season_number: i32,
    pub episode_number: i32,
    pub air_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvReleaseFileInput {
    pub file_id: String,
    pub path: String,
    pub size_bytes: Option<i64>,
    pub selectable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TvRejectionReason {
    AmbiguousRelease,
    FileListRequired,
    FileSelectionRequired,
    FileListDoesNotCoverExpectedTargets,
    MissingMetadataTarget,
    NoMediaFiles,
    ParsedTitleDisagreesWithRelease,
    UnknownNumbering,
    UnmappedMediaFile,
}

impl TvRejectionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AmbiguousRelease => "ambiguous_release",
            Self::FileListRequired => "file_list_required",
            Self::FileSelectionRequired => "file_selection_required",
            Self::FileListDoesNotCoverExpectedTargets => {
                "file_list_does_not_cover_expected_targets"
            }
            Self::MissingMetadataTarget => "missing_metadata_target",
            Self::NoMediaFiles => "no_media_files",
            Self::ParsedTitleDisagreesWithRelease => "parsed_title_disagrees_with_release",
            Self::UnknownNumbering => "unknown_numbering",
            Self::UnmappedMediaFile => "unmapped_media_file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvCoverageEntry {
    pub target_id: Uuid,
    pub target_key: String,
    pub season_number: i32,
    pub episode_number: i32,
    pub release_file_id: Option<String>,
    pub coverage_kind: ReleaseCoverageKind,
    pub state: ReleaseCoverageState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvCoveragePlan {
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    pub release_kind: ReleaseKind,
    pub confidence: ReleaseConfidence,
    pub requires_file_list: bool,
    pub entries: Vec<TvCoverageEntry>,
    pub rejection_reasons: Vec<TvRejectionReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TvCoverageOptions {
    pub allow_partial_pack: bool,
    pub file_selection_supported: bool,
}

impl Default for TvCoverageOptions {
    fn default() -> Self {
        Self {
            allow_partial_pack: false,
            file_selection_supported: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct TvSonarrStyleResolver;

impl TvSonarrStyleResolver {
    pub fn parse_title(&self, release_title: &str) -> TvParsedRelease {
        parse_release_title(release_title)
    }

    pub fn parse_file(&self, file_path: &str) -> TvParsedRelease {
        parse_release_file(file_path)
    }

    pub fn plan_coverage(
        &self,
        parsed: &TvParsedRelease,
        targets: &[TvTarget],
        files: &[TvReleaseFileInput],
        options: TvCoverageOptions,
    ) -> TvCoveragePlan {
        plan_coverage(parsed, targets, files, options)
    }
}

pub fn parse_release_title(release_title: &str) -> TvParsedRelease {
    let trimmed_title = release_title.trim();
    let raw_title = strip_file_extension(trimmed_title);
    if !validate_before_parsing(trimmed_title) {
        return unknown_parsed_release(release_title, &raw_title);
    }

    let title = preprocess_release_title(&raw_title);
    let quality_title = if REVERSED_TITLE_RE.is_match(trimmed_title) {
        trimmed_title.chars().rev().collect::<String>()
    } else {
        trimmed_title.to_string()
    };
    let release_group = parse_release_group(&raw_title);
    let mut quality = parse_quality(&quality_title);
    if REVERSED_TITLE_RE.is_match(trimmed_title)
        && quality.source.is_none()
        && quality.resolution.is_none()
    {
        quality = parse_quality(trimmed_title);
    }
    let modifiers = parse_modifiers(trimmed_title);
    let air_date = parse_air_date(&title).or_else(|| parse_air_date(&raw_title));
    let anime_absolute_hints = parse_absolute_number_hints(&title);

    if let Some(parsed) = parse_japanese_variety_release(&raw_title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &raw_title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_manual_spaced_season_e_episode_release(&raw_title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &raw_title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_manual_daily_release(&raw_title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &raw_title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_japanese_variety_release(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if !SEASON_EP_MARKER_RE.is_match(&title) {
        if let Some(parsed) = parse_e_only_dashed_multi_release(&title) {
            return finalize_parsed_release(
                parsed,
                release_title,
                &title,
                release_group,
                quality,
                modifiers,
                air_date,
                anime_absolute_hints,
            );
        }
    }

    if let Some(parsed) = parse_korean_dated_episode_release(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_leading_compact_daily_release(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_cjk_episode_range_release(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if should_parse_episode_before_daily(&title) {
        if let Some(parsed) = parse_episode_release(&title) {
            return finalize_parsed_release(
                parsed,
                release_title,
                &title,
                release_group,
                quality,
                modifiers,
                air_date,
                anime_absolute_hints,
            );
        }
    }

    if let Some(parsed) = parse_daily_release(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if CAP_EP_RE.is_match(&title) {
        if let Some(parsed) = parse_episode_release(&title) {
            return finalize_parsed_release(
                parsed,
                release_title,
                &title,
                release_group,
                quality,
                modifiers,
                air_date,
                anime_absolute_hints,
            );
        }
    }

    if should_parse_episode_before_season_pack(&title) {
        if let Some(parsed) = parse_episode_release(&title) {
            return finalize_parsed_release(
                parsed,
                release_title,
                &title,
                release_group,
                quality,
                modifiers,
                air_date,
                anime_absolute_hints,
            );
        }
    }

    if let Some(parsed) = parse_multi_season_pack(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_series_pack(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_season_pack(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    if let Some(parsed) = parse_episode_release(&title) {
        return finalize_parsed_release(
            parsed,
            release_title,
            &title,
            release_group,
            quality,
            modifiers,
            air_date,
            anime_absolute_hints,
        );
    }

    let mut unknown = unknown_parsed_release(release_title, &title);
    unknown.air_date = air_date;
    unknown.release_group = release_group;
    unknown.quality = quality;
    unknown.modifiers = modifiers;
    unknown.anime_absolute_hints = anime_absolute_hints;
    unknown.series_title_info =
        series_title_info_from_display(unknown.normalized_series_title.as_deref());
    unknown.release_hash = parse_release_hash(release_title);
    unknown
}

fn finalize_parsed_release(
    mut parsed: TvParsedRelease,
    release_title: &str,
    matcher_title: &str,
    release_group: Option<String>,
    quality: TvQuality,
    modifiers: TvReleaseModifiers,
    air_date: Option<String>,
    anime_absolute_hints: Vec<i32>,
) -> TvParsedRelease {
    parsed.original_title = release_title.to_string();
    if parsed.air_date.is_none() {
        parsed.air_date = air_date;
    }
    parsed.release_group = release_group;
    parsed.release_tokens = derive_release_tokens(matcher_title, &parsed);
    parsed.release_hash = parse_release_hash(release_title);
    parsed.quality = quality;
    parsed.modifiers = modifiers;
    parsed.anime_absolute_hints = anime_absolute_hints;
    parsed.series_title_info =
        series_title_info_from_display(parsed.normalized_series_title.as_deref());
    let release_title_alternatives = extract_release_title_alternatives(matcher_title);
    if release_title_alternatives.len() > 1 {
        if matcher_title.contains(" / ") {
            parsed.normalized_series_title = release_title_alternatives.last().cloned();
        }
        parsed.series_title_info.all_titles = release_title_alternatives;
    }
    if matcher_title.contains(" / ") {
        let slash_alternatives = slash_title_alternatives(matcher_title);
        if slash_alternatives.len() > 1 {
            parsed.normalized_series_title = slash_alternatives.last().cloned();
            parsed.series_title_info.all_titles = slash_alternatives;
        }
    }
    parsed.special = parsed.special || is_special_release(matcher_title, &parsed);
    parsed.is_split_episode = parsed.is_split_episode || SPLIT_EP_RE.is_match(matcher_title);
    if is_miniseries_e_only_title(matcher_title) {
        parsed.is_mini_series = true;
    }
    parsed
}

fn unknown_parsed_release(release_title: &str, normalized_input: &str) -> TvParsedRelease {
    TvParsedRelease {
        original_title: release_title.to_string(),
        normalized_series_title: clean_series_title(normalized_input),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: None,
        season_end_number: None,
        episode_numbers: Vec::new(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Unknown,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    }
}

pub fn parse_release_file(file_path: &str) -> TvParsedRelease {
    let normalized_path = file_path.replace('\\', "/");
    let parts: Vec<&str> = normalized_path
        .split('/')
        .filter(|part| !part.trim().is_empty())
        .collect();
    let file_name = parts.last().copied().unwrap_or(file_path);
    let parent_name = parts.iter().rev().nth(1).copied();

    if let Some(parent) = parent_name {
        if is_hashed_release_file(file_name) {
            let extension = file_name
                .rsplit_once('.')
                .map(|(_, ext)| format!(".{ext}"))
                .unwrap_or_default();
            let parent_parse = parse_release_title(&format!("{parent}{extension}"));
            if !matches!(parent_parse.release_kind, ReleaseKind::Unknown) {
                return parent_parse;
            }
        }
    }

    let parsed = parse_release_title(file_name);

    if let Some(parent) = parent_name {
        if let Some(folder_file_parse) = parse_simple_episode_with_season_folder(file_name, parent)
        {
            return folder_file_parse;
        }
    }

    if matches!(
        parsed.release_kind,
        ReleaseKind::Single | ReleaseKind::MultiEpisode
    ) {
        return parsed;
    }

    if let Some(parent) = parent_name {
        if let Some(narrowed) = parse_numeric_file_with_release_folder(file_name, parent) {
            return narrowed;
        }
    }

    if let Some(parsed) = parse_season_folder_file(file_path) {
        return parsed;
    }

    if let Some(parent) = parent_name {
        let combined = format!("{parent} {file_name}");
        let combined_parse = parse_release_title(&combined);
        if matches!(
            combined_parse.release_kind,
            ReleaseKind::Single | ReleaseKind::MultiEpisode | ReleaseKind::SeasonPack
        ) {
            return combined_parse;
        }

        let extension = file_name
            .rsplit_once('.')
            .map(|(_, ext)| format!(".{ext}"))
            .unwrap_or_default();
        let parent_with_extension = format!("{parent}{extension}");
        let parent_parse = parse_release_title(&parent_with_extension);
        if matches!(
            parent_parse.release_kind,
            ReleaseKind::Single | ReleaseKind::MultiEpisode | ReleaseKind::SeasonPack
        ) {
            return parent_parse;
        }
    }

    parsed
}

fn is_hashed_release_file(file_name: &str) -> bool {
    let stem = strip_file_extension(file_name);
    REJECT_HASHED_RELEASE_RE
        .iter()
        .any(|regex| regex.is_match(stem.trim()))
}

pub fn plan_coverage(
    parsed: &TvParsedRelease,
    targets: &[TvTarget],
    files: &[TvReleaseFileInput],
    options: TvCoverageOptions,
) -> TvCoveragePlan {
    match parsed.release_kind {
        ReleaseKind::Single | ReleaseKind::MultiEpisode => plan_episode_coverage(parsed, targets),
        ReleaseKind::SeasonPack => {
            if files.is_empty() {
                review_plan(
                    parsed.release_kind,
                    true,
                    vec![TvRejectionReason::FileListRequired],
                )
            } else {
                plan_pack_coverage(
                    parsed,
                    targets,
                    files,
                    options,
                    ReleaseCoverageKind::SeasonPack,
                )
            }
        }
        ReleaseKind::MultiSeasonPack => {
            if !options.file_selection_supported {
                return review_plan(
                    parsed.release_kind,
                    !files.is_empty(),
                    vec![TvRejectionReason::FileSelectionRequired],
                );
            }

            if files.is_empty() {
                review_plan(
                    parsed.release_kind,
                    true,
                    vec![TvRejectionReason::FileListRequired],
                )
            } else {
                plan_pack_coverage(
                    parsed,
                    targets,
                    files,
                    options,
                    ReleaseCoverageKind::MultiSeasonPack,
                )
            }
        }
        ReleaseKind::SeriesPack => {
            if !options.file_selection_supported {
                return review_plan(
                    parsed.release_kind,
                    !files.is_empty(),
                    vec![TvRejectionReason::FileSelectionRequired],
                );
            }

            if files.is_empty() {
                review_plan(
                    parsed.release_kind,
                    true,
                    vec![TvRejectionReason::FileListRequired],
                )
            } else {
                plan_pack_coverage(
                    parsed,
                    targets,
                    files,
                    options,
                    ReleaseCoverageKind::SeriesPack,
                )
            }
        }
        ReleaseKind::Unknown => review_plan(
            ReleaseKind::Unknown,
            false,
            vec![
                TvRejectionReason::UnknownNumbering,
                TvRejectionReason::AmbiguousRelease,
            ],
        ),
    }
}

fn plan_episode_coverage(parsed: &TvParsedRelease, targets: &[TvTarget]) -> TvCoveragePlan {
    let mut entries = Vec::new();
    let mut missing = Vec::new();
    let coverage_kind = if parsed.release_kind == ReleaseKind::Single {
        ReleaseCoverageKind::SingleEpisode
    } else {
        ReleaseCoverageKind::MultiEpisodeRange
    };

    if parsed.episode_numbers.is_empty() {
        if let Some(air_date) = parsed.air_date.as_deref() {
            let matching: Vec<_> = targets
                .iter()
                .filter(|target| target.air_date.as_deref() == Some(air_date))
                .collect();

            if matching.is_empty() {
                return TvCoveragePlan {
                    resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                    resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
                    release_kind: parsed.release_kind,
                    confidence: ReleaseConfidence::ReviewRequired,
                    requires_file_list: false,
                    entries,
                    rejection_reasons: vec![TvRejectionReason::MissingMetadataTarget],
                };
            }

            entries.extend(
                matching
                    .into_iter()
                    .map(|target| coverage_entry(target, None, coverage_kind)),
            );

            return TvCoveragePlan {
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
                release_kind: parsed.release_kind,
                confidence: ReleaseConfidence::High,
                requires_file_list: false,
                entries,
                rejection_reasons: Vec::new(),
            };
        }
    }

    if let Some(season) = parsed.season_number {
        for episode in &parsed.episode_numbers {
            if let Some(target) = find_target(targets, season, *episode) {
                entries.push(coverage_entry(target, None, coverage_kind));
            } else {
                missing.push((season, *episode));
            }
        }
    }

    if !missing.is_empty() || entries.is_empty() {
        TvCoveragePlan {
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
            release_kind: parsed.release_kind,
            confidence: ReleaseConfidence::ReviewRequired,
            requires_file_list: false,
            entries,
            rejection_reasons: vec![TvRejectionReason::MissingMetadataTarget],
        }
    } else {
        TvCoveragePlan {
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
            release_kind: parsed.release_kind,
            confidence: ReleaseConfidence::High,
            requires_file_list: false,
            entries,
            rejection_reasons: Vec::new(),
        }
    }
}

fn plan_pack_coverage(
    parsed: &TvParsedRelease,
    targets: &[TvTarget],
    files: &[TvReleaseFileInput],
    options: TvCoverageOptions,
    coverage_kind: ReleaseCoverageKind,
) -> TvCoveragePlan {
    let mut entries = Vec::new();
    let mut covered = BTreeSet::new();
    let mut has_media_files = false;
    let mut unmapped_media_files = false;

    for file in files.iter().filter(|file| is_media_file(&file.path)) {
        has_media_files = true;

        if is_sample_file(&file.path) {
            continue;
        }

        let file_parse = parse_release_file(&file.path);
        let Some(file_season) = file_parse.season_number else {
            unmapped_media_files = true;
            continue;
        };

        if let Some(release_season) = parsed.season_number {
            if parsed.release_kind == ReleaseKind::SeasonPack && file_season != release_season {
                unmapped_media_files = true;
                continue;
            }

            if parsed.release_kind == ReleaseKind::MultiSeasonPack {
                let season_end = parsed.season_end_number.unwrap_or(release_season);
                if file_season < release_season || file_season > season_end {
                    unmapped_media_files = true;
                    continue;
                }
            }
        }

        if file_parse.episode_numbers.is_empty() {
            unmapped_media_files = true;
            continue;
        }

        for episode in file_parse.episode_numbers {
            if let Some(target) = find_target(targets, file_season, episode) {
                if covered.insert((file_season, episode)) {
                    entries.push(coverage_entry(
                        target,
                        Some(file.file_id.clone()),
                        coverage_kind,
                    ));
                }
            }
        }
    }

    if !has_media_files {
        return review_plan(
            parsed.release_kind,
            false,
            vec![TvRejectionReason::NoMediaFiles],
        );
    }

    let expected = expected_pack_targets(parsed, targets);
    let all_expected_covered = expected.iter().all(|key| covered.contains(key));
    let has_partial_coverage = !entries.is_empty();

    let mut reasons = Vec::new();
    if unmapped_media_files {
        reasons.push(TvRejectionReason::UnmappedMediaFile);
    }

    if all_expected_covered && !unmapped_media_files {
        TvCoveragePlan {
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
            release_kind: parsed.release_kind,
            confidence: ReleaseConfidence::High,
            requires_file_list: false,
            entries,
            rejection_reasons: Vec::new(),
        }
    } else if options.allow_partial_pack && has_partial_coverage && !unmapped_media_files {
        TvCoveragePlan {
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
            release_kind: parsed.release_kind,
            confidence: ReleaseConfidence::Medium,
            requires_file_list: false,
            entries,
            rejection_reasons: Vec::new(),
        }
    } else {
        reasons.push(TvRejectionReason::FileListDoesNotCoverExpectedTargets);
        TvCoveragePlan {
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
            release_kind: parsed.release_kind,
            confidence: ReleaseConfidence::ReviewRequired,
            requires_file_list: false,
            entries,
            rejection_reasons: dedupe_reasons(reasons),
        }
    }
}

fn expected_pack_targets(parsed: &TvParsedRelease, targets: &[TvTarget]) -> BTreeSet<(i32, i32)> {
    let mut expected = BTreeSet::new();
    match parsed.release_kind {
        ReleaseKind::SeasonPack => {
            if let Some(season) = parsed.season_number {
                for target in targets
                    .iter()
                    .filter(|target| target.season_number == season)
                {
                    expected.insert((target.season_number, target.episode_number));
                }
            }
        }
        ReleaseKind::MultiSeasonPack => {
            if let Some(season) = parsed.season_number {
                let end = parsed.season_end_number.unwrap_or(season);
                for target in targets
                    .iter()
                    .filter(|target| target.season_number >= season && target.season_number <= end)
                {
                    expected.insert((target.season_number, target.episode_number));
                }
            }
        }
        ReleaseKind::SeriesPack => {
            for target in targets {
                expected.insert((target.season_number, target.episode_number));
            }
        }
        _ => {}
    }
    expected
}

fn find_target(targets: &[TvTarget], season: i32, episode: i32) -> Option<&TvTarget> {
    targets
        .iter()
        .find(|target| target.season_number == season && target.episode_number == episode)
}

fn coverage_entry(
    target: &TvTarget,
    release_file_id: Option<String>,
    coverage_kind: ReleaseCoverageKind,
) -> TvCoverageEntry {
    TvCoverageEntry {
        target_id: target.target_id,
        target_key: target.target_key.clone(),
        season_number: target.season_number,
        episode_number: target.episode_number,
        release_file_id,
        coverage_kind,
        state: ReleaseCoverageState::Planned,
    }
}

fn review_plan(
    release_kind: ReleaseKind,
    requires_file_list: bool,
    rejection_reasons: Vec<TvRejectionReason>,
) -> TvCoveragePlan {
    TvCoveragePlan {
        resolver_kind: ReleaseResolverKind::TvSonarrStyle,
        resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
        release_kind,
        confidence: ReleaseConfidence::ReviewRequired,
        requires_file_list,
        entries: Vec::new(),
        rejection_reasons: dedupe_reasons(rejection_reasons),
    }
}

fn parse_episode_release(title: &str) -> Option<TvParsedRelease> {
    if let Some(parsed) = parse_manual_spaced_season_e_episode_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_quoted_season_episode_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_leading_season_episode_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_spaced_season_e_episode_release(title) {
        return Some(parsed);
    }

    let captures = SXXEYY_RE
        .captures(title)
        .or_else(|| COMPACT_SXXEYY_RE.captures(title));

    if let Some(captures) = captures {
        return Some(parsed_episode_from_captures(
            title,
            &captures,
            EpisodeStyle::SeasonEpisode,
        ));
    }

    if let Some(parsed) = parse_e_only_dashed_multi_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_extant_multi_episode_release(title) {
        return Some(parsed);
    }

    if let Some(captures) = XYY_RE.captures(title) {
        return Some(parsed_episode_from_captures(
            title,
            &captures,
            EpisodeStyle::X,
        ));
    }

    if let Some(captures) = SXX_DOT_EP_RE.captures(title) {
        if let Some(episode_match) = captures.name("episode") {
            let episode = episode_match.as_str().parse::<i32>().ok()?;
            if !looks_like_resolution_tail(title, episode_match.end(), episode) {
                return Some(parsed_episode_from_captures(
                    title,
                    &captures,
                    EpisodeStyle::SeasonEpisode,
                ));
            }
        }
    }

    if let Some(captures) = SEASON_EPISODE_WORD_RE.captures(title) {
        return Some(parsed_episode_from_captures(
            title,
            &captures,
            EpisodeStyle::SeasonEpisode,
        ));
    }

    if let Some(parsed) = parse_dutch_episode_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_cap_episode_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_part_episode_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_episode_only_multi_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_numeric_episode_release(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_e_only_episode_release(title) {
        return Some(parsed);
    }

    None
}

fn should_parse_episode_before_daily(title: &str) -> bool {
    let date_start = DATE_TOKEN_RE.find(title).map(|date| date.start());
    let Some(episode) = EPISODE_TOKEN_RE
        .find(title)
        .or_else(|| X_TOKEN_RE.find(title))
    else {
        return date_start
            .and_then(|start| title.get(start..))
            .map(daily_tail_is_plain_episode_token)
            .unwrap_or(false);
    };

    if date_start
        .map(|start| episode.start() < start)
        .unwrap_or(false)
    {
        return true;
    }

    date_start
        .and_then(|start| title.get(start..))
        .map(daily_tail_is_plain_episode_token)
        .unwrap_or(false)
}

fn should_parse_episode_before_season_pack(title: &str) -> bool {
    QUOTED_SEASON_EPISODE_RE.is_match(title)
        || SEASON_EPISODE_WORD_RE.is_match(title)
        || CAP_EP_RE.is_match(title)
        || SXX_DOT_EP_RE
            .captures(title)
            .and_then(|captures| {
                let episode = parse_i32_capture(&captures, "episode")?;
                Some(
                    !looks_like_year_or_season_range_episode(title, &captures, episode)
                        && !looks_like_resolution_tail(
                            title,
                            captures.name("episode").map(|m| m.end()).unwrap_or(0),
                            episode,
                        ),
                )
            })
            .unwrap_or(false)
}

fn looks_like_year_or_season_range_episode(
    title: &str,
    captures: &Captures<'_>,
    episode: i32,
) -> bool {
    if (1900..=2099).contains(&episode) {
        return true;
    }

    let Some(season_match) = captures.name("season") else {
        return false;
    };
    let Some(episode_match) = captures.name("episode") else {
        return false;
    };
    let between = &title[season_match.end()..episode_match.start()];
    if between.contains('-') || between.contains('–') || between.contains('—') {
        return true;
    }

    captures
        .name("tail")
        .map(|tail| {
            let lower = tail.as_str().to_ascii_lowercase();
            episode <= 99
                && (lower.contains("1080")
                    || lower.contains("720")
                    || lower.contains("bluray")
                    || lower.contains("web")
                    || lower.contains("hdtv"))
        })
        .unwrap_or(false)
}

fn parse_spaced_season_e_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = SPACED_SEASON_E_TOKEN_RE
        .captures(title)
        .or_else(|| SPACED_SEASON_E_RE.captures(title))?;
    let token = captures.get(0)?;
    let season = parse_i32_capture(&captures, "season")?;
    let first_episode = parse_i32_capture(&captures, "episode")?;
    if season <= 0 || first_episode <= 0 {
        return None;
    }
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first_episode, first_episode);
    collect_tail_episodes(
        &title[token.end()..],
        season,
        &mut episodes,
        EpisodeStyle::SeasonEpisode,
    );
    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(&strip_trailing_air_date_from_title(
            &title[..token.start()],
        )),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_quoted_season_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = QUOTED_SEASON_EPISODE_RE.captures(title)?;
    let season = parse_i32_capture(&captures, "season")?;
    let episode = parse_i32_capture(&captures, "episode")?;
    if season <= 0 || episode <= 0 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: vec![episode],
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_leading_season_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = LEADING_SEASON_EPISODE_RE.captures(title)?;
    let season = parse_i32_capture(&captures, "season")?;
    let episode = parse_i32_capture(&captures, "episode")?;
    if season <= 0 || episode <= 0 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: None,
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: vec![episode],
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_episode_only_multi_release(title: &str) -> Option<TvParsedRelease> {
    if let Some(parsed) = parse_e_only_dashed_multi_release(title) {
        return Some(parsed);
    }

    let captures = E_ONLY_EXPLICIT_MULTI_RE
        .captures(title)
        .or_else(|| E_ONLY_MULTI_RE.captures(title))
        .or_else(|| EPISODE_ONLY_RE.captures(title))?;
    let first_episode = parse_i32_capture(&captures, "episode")?;
    if let Some(last_episode) = parse_i32_capture(&captures, "episode_end") {
        if last_episode > 100 && first_episode < 100 {
            return None;
        }
    }
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first_episode, first_episode);
    collect_tail_episodes(
        captures
            .name("tail")
            .map(|m| m.as_str())
            .unwrap_or_default(),
        1,
        &mut episodes,
        EpisodeStyle::SeasonEpisode,
    );
    if let Some(last_episode) = parse_i32_capture(&captures, "episode_end") {
        add_episode_candidate(&mut episodes, last_episode, true);
    }

    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(1),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_e_only_dashed_multi_release(title: &str) -> Option<TvParsedRelease> {
    if let Some(parsed) = parse_manual_e_only_dashed_multi_release(title) {
        return Some(parsed);
    }

    let captures = E_ONLY_DASH_TOKEN_RE.captures(title)?;
    let token = captures.get(0)?;
    let first = parse_i32_capture(&captures, "episode")?;
    let last = parse_i32_capture(&captures, "episode_end")?;
    if first <= 0 || last <= first || last > 100 {
        return None;
    }
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first, last);

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(&title[..token.start()]),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(1),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::MultiEpisode,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: true,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_manual_spaced_season_e_episode_release(title: &str) -> Option<TvParsedRelease> {
    let marker = MANUAL_SPACED_SEASON_E_RE.find(title)?;
    let captures = MANUAL_SPACED_SEASON_E_RE.captures(marker.as_str())?;
    let season = parse_i32_capture(&captures, "season")?;
    let first_episode = parse_i32_capture(&captures, "episode")?;
    if season <= 0 || first_episode <= 0 {
        return None;
    }
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first_episode, first_episode);
    collect_tail_episodes(
        &title[marker.end()..],
        season,
        &mut episodes,
        EpisodeStyle::SeasonEpisode,
    );
    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(&title[..marker.start()]),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_manual_e_only_dashed_multi_release(title: &str) -> Option<TvParsedRelease> {
    let marker = MANUAL_E_ONLY_DASH_RE.find(title)?;
    let captures = MANUAL_E_ONLY_DASH_RE.captures(marker.as_str())?;
    let first = parse_i32_capture(&captures, "episode")?;
    let last = parse_i32_capture(&captures, "episode_end")?;
    if first <= 0 || last <= first || last > 100 {
        return None;
    }
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first, last);

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(&title[..marker.start()]),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(1),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::MultiEpisode,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: true,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_japanese_variety_release(title: &str) -> Option<TvParsedRelease> {
    if let Some(parsed) = parse_japanese_variety_manual(title) {
        return Some(parsed);
    }

    let captures = JAPANESE_VARIETY_RE
        .captures(title)
        .or_else(|| JAPANESE_VARIETY_NORMALIZED_RE.captures(title))?;
    let year = parse_i32_capture(&captures, "year")
        .or_else(|| parse_i32_capture(&captures, "short_year"))?;
    let month = parse_u32_capture(&captures, "month")?;
    let day = parse_u32_capture(&captures, "day")?;
    let full_year = if year < 100 { 2000 + year } else { year };
    NaiveDate::from_ymd_opt(full_year, month, day)?;
    let season = parse_i32_capture(&captures, "season").unwrap_or(1);
    let episode = parse_i32_capture(&captures, "episode")?;
    if season <= 0 || episode <= 0 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: vec![episode],
        air_date: Some(format!("{full_year:04}-{month:02}-{day:02}")),
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_japanese_variety_manual(title: &str) -> Option<TvParsedRelease> {
    let stripped = strip_trailing_quality_bracket(&strip_file_extension(title));
    let trimmed = stripped.trim();
    let date = trimmed.get(..6)?;
    if !date.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let short_year = date[0..2].parse::<i32>().ok()?;
    let month = date[2..4].parse::<u32>().ok()?;
    let day = date[4..6].parse::<u32>().ok()?;
    let full_year = if short_year >= 70 {
        1900 + short_year
    } else {
        2000 + short_year
    };
    NaiveDate::from_ymd_opt(full_year, month, day)?;
    let rest = trimmed[6..].trim_start_matches([' ', '.', '_', '-']);
    let captures = JAPANESE_VARIETY_EP_MARKER_RE.captures(rest)?;
    let marker = captures.get(0)?;
    let episode = parse_i32_capture(&captures, "episode")?;
    let mut title_part = rest[..marker.start()]
        .trim_end_matches([' ', '.', '_', '-'])
        .to_string();
    let mut season = 1;
    if let Some(index) = title_part.to_ascii_lowercase().rfind(" season ") {
        if let Ok(parsed_season) = title_part[index + 8..].trim().parse::<i32>() {
            season = parsed_season;
            title_part = title_part[..index]
                .trim_end_matches([' ', '.', '_', '-'])
                .to_string();
        }
    } else if let Some(season_captures) = JAPANESE_VARIETY_SEASON_SUFFIX_RE.captures(&title_part) {
        season = parse_i32_capture(&season_captures, "season").unwrap_or(1);
        title_part = capture_str(&season_captures, "title")
            .trim_end_matches([' ', '.', '_', '-'])
            .to_string();
    }
    if title_part.is_empty() || episode <= 0 || season <= 0 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(&title_part),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: vec![episode],
        air_date: Some(format!("{full_year:04}-{month:02}-{day:02}")),
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_leading_compact_daily_release(title: &str) -> Option<TvParsedRelease> {
    parse_manual_daily_release(title).or_else(|| {
        parse_daily_with_regex(title, &LEADING_DAILY_COMPACT_RE, DailyDateOrder::CompactYmd)
    })
}

fn parse_manual_daily_release(title: &str) -> Option<TvParsedRelease> {
    let captures = LEADING_MANUAL_DAILY_COMPACT_RE
        .captures(title)
        .or_else(|| ANY_MANUAL_DAILY_YMD_RE.captures(title))?;
    let year = parse_i32_capture(&captures, "year")?;
    let month = parse_u32_capture(&captures, "month")?;
    let day = parse_u32_capture(&captures, "day")?;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    if !valid_daily_date(date) {
        return None;
    }
    let captured_title = capture_str(&captures, "title");
    if SEASON_EP_MARKER_RE.is_match(captured_title) || X_TOKEN_RE.is_match(captured_title) {
        return None;
    }
    if captures
        .name("tail")
        .map(|tail| SEASON_EP_MARKER_RE.is_match(tail.as_str()))
        .unwrap_or(false)
    {
        return None;
    }
    let raw_title = strip_trailing_e_only_from_title(captured_title);
    let daily_part = captures
        .name("tail")
        .and_then(|tail| parse_daily_part(tail.as_str()))
        .or_else(|| parse_daily_part(title));

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(&raw_title),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: None,
        season_end_number: None,
        episode_numbers: Vec::new(),
        air_date: Some(date.format("%Y-%m-%d").to_string()),
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn strip_trailing_e_only_from_title(title: &str) -> String {
    TRAILING_E_ONLY_TITLE_RE
        .replace(title, "")
        .trim_end_matches([' ', '.', '_', '-'])
        .to_string()
}

fn parse_cjk_episode_range_release(title: &str) -> Option<TvParsedRelease> {
    let captures = CJK_EPISODE_RANGE_RE.captures(title)?;
    let first = parse_i32_capture(&captures, "first")?;
    let last = parse_i32_capture(&captures, "last")?;
    if first <= 0 || last < first {
        return None;
    }
    let season = parse_i32_capture(&captures, "season").unwrap_or(1);
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first, last);

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: captures
            .name("ascii_title")
            .and_then(|title| clean_series_title(title.as_str()))
            .or_else(|| clean_series_title(title_prefix_before_numbering(title))),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::MultiEpisode,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_korean_dated_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = KOREAN_DATED_EP_RE.captures(title)?;
    let date = captures.name("date")?.as_str();
    let (year, month, day) = if date.len() == 6 {
        let short_year = date[0..2].parse::<i32>().ok()?;
        let month = date[2..4].parse::<u32>().ok()?;
        let day = date[4..6].parse::<u32>().ok()?;
        let year = if short_year >= 70 {
            1900 + short_year
        } else {
            2000 + short_year
        };
        (year, month, day)
    } else if let Some(captures) = AIR_DATE_RE.captures(date) {
        (
            parse_i32_capture(&captures, "year")?,
            parse_u32_capture(&captures, "month")?,
            parse_u32_capture(&captures, "day")?,
        )
    } else {
        return None;
    };
    NaiveDate::from_ymd_opt(year, month, day)?;
    let episode = parse_i32_capture(&captures, "episode")?;

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(1),
        season_end_number: None,
        episode_numbers: vec![episode],
        air_date: Some(format!("{year:04}-{month:02}-{day:02}")),
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: true,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_daily_release(title: &str) -> Option<TvParsedRelease> {
    if let Some(parsed) = parse_daily_with_regex(title, &DAILY_YMD_RE, DailyDateOrder::Ymd) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_daily_with_regex(title, &DAILY_YDM_RE, DailyDateOrder::Ydm) {
        return Some(parsed);
    }

    if let Some(parsed) =
        parse_daily_with_regex(title, &DAILY_COMPACT_RE, DailyDateOrder::CompactYmd)
    {
        return Some(parsed);
    }

    if let Some(parsed) =
        parse_daily_with_regex(title, &DAILY_YYMMDD_RE, DailyDateOrder::CompactYyMmDd)
    {
        return Some(parsed);
    }

    if let Some(parsed) = parse_daily_with_regex(title, &DAILY_DMY_RE, DailyDateOrder::Dmy) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_daily_with_regex(title, &DAILY_MDY_RE, DailyDateOrder::Mdy) {
        return Some(parsed);
    }

    parse_daily_with_regex(title, &DAILY_MONTH_RE, DailyDateOrder::DayMonthNameYear)
}

fn parse_daily_with_regex(
    title: &str,
    regex: &Regex,
    order: DailyDateOrder,
) -> Option<TvParsedRelease> {
    let captures = regex.captures(title)?;
    if captures
        .name("tail")
        .map(|tail| daily_tail_is_plain_episode_token(tail.as_str()))
        .unwrap_or(false)
    {
        return None;
    }

    let raw_title = capture_str(&captures, "title");
    if matches!(
        order,
        DailyDateOrder::CompactYyMmDd | DailyDateOrder::CompactYmd
    ) && matches!(
        raw_title
            .trim_matches(&[' ', '.', '_', '-'][..])
            .to_ascii_lowercase()
            .as_str(),
        "e" | "ep"
    ) {
        return None;
    }
    let date = match order {
        DailyDateOrder::Ymd | DailyDateOrder::CompactYmd => {
            let year = parse_i32_capture(&captures, "year")?;
            let month = parse_u32_capture(&captures, "month")?;
            let day = parse_u32_capture(&captures, "day")?;
            NaiveDate::from_ymd_opt(year, month, day)?
        }
        DailyDateOrder::CompactYyMmDd => {
            let short_year = parse_i32_capture(&captures, "year")?;
            let month = parse_u32_capture(&captures, "month")?;
            let day = parse_u32_capture(&captures, "day")?;
            NaiveDate::from_ymd_opt(2000 + short_year, month, day)?
        }
        DailyDateOrder::Ydm => {
            let year = parse_i32_capture(&captures, "year")?;
            let first = parse_u32_capture(&captures, "first")?;
            let second = parse_u32_capture(&captures, "second")?;
            let (month, day) = if first > 12 {
                (second, first)
            } else {
                (first, second)
            };
            NaiveDate::from_ymd_opt(year, month, day)?
        }
        DailyDateOrder::Dmy => {
            let year = parse_i32_capture(&captures, "year")?;
            let month = parse_u32_capture(&captures, "month")?;
            let day = parse_u32_capture(&captures, "day")?;
            if day <= 12 {
                return None;
            }
            NaiveDate::from_ymd_opt(year, month, day)?
        }
        DailyDateOrder::Mdy => {
            let year = parse_i32_capture(&captures, "year")?;
            let month = parse_u32_capture(&captures, "month")?;
            let day = parse_u32_capture(&captures, "day")?;
            if day <= 12 && month <= 12 {
                return None;
            }
            NaiveDate::from_ymd_opt(year, month, day)?
        }
        DailyDateOrder::DayMonthNameYear => {
            let year = parse_i32_capture(&captures, "year")?;
            let month = captures
                .name("month")
                .and_then(|value| parse_month_name(value.as_str()))?;
            let day = parse_u32_capture(&captures, "day")?;
            NaiveDate::from_ymd_opt(year, month, day)?
        }
    };

    if !valid_daily_date(date) {
        return None;
    }

    let daily_part = captures
        .name("tail")
        .and_then(|tail| parse_daily_part(tail.as_str()))
        .or_else(|| parse_daily_part(title));

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(raw_title),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: None,
        season_end_number: None,
        episode_numbers: Vec::new(),
        air_date: Some(date.format("%Y-%m-%d").to_string()),
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn daily_tail_is_plain_episode_token(tail: &str) -> bool {
    let trimmed = tail.trim_start_matches(&[' ', '.', '_', '-'][..]);
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with('s') || lower.starts_with('e')) {
        return false;
    }

    if let Some(captures) = E_ONLY_TOKEN_RE.captures(trimmed) {
        if let Some(episode) = parse_i32_capture(&captures, "episode") {
            return episode < 1000;
        }
    }

    EPISODE_TOKEN_RE.is_match(trimmed)
}

fn parse_numeric_episode_release(title: &str) -> Option<TvParsedRelease> {
    for captures in NUMERIC_TOKEN_RE.captures_iter(title) {
        let number_text = captures.name("number")?.as_str();
        let raw_series_title = captures
            .get(0)
            .map(|matched| &title[..matched.start()])
            .unwrap_or_default();
        let normalized_number_text = if number_text.len() == 4 && number_text.starts_with('0') {
            &number_text[1..]
        } else {
            number_text
        };
        if normalized_number_text.starts_with('0') {
            continue;
        }

        let prefix = raw_series_title
            .trim_end_matches(&[' ', '.', '_', '-'][..])
            .to_ascii_lowercase();
        if prefix.ends_with(".h")
            || prefix.ends_with("-h")
            || prefix.ends_with("_h")
            || prefix.ends_with(" h")
            || prefix.ends_with(".x")
            || prefix.ends_with("-x")
            || prefix.ends_with("_x")
            || prefix.ends_with(" x")
        {
            continue;
        }

        let number = normalized_number_text.parse::<i32>().ok()?;
        if (1900..=2099).contains(&number)
            || matches!(
                number,
                360 | 480 | 540 | 576 | 720 | 960 | 1080 | 1440 | 2160
            )
        {
            continue;
        }

        let Some((season, episode)) = season_episode_from_packed_number(number) else {
            continue;
        };

        if season <= 0 || episode <= 0 {
            continue;
        }
        let mut episodes = BTreeSet::new();
        episodes.insert(episode);
        collect_numeric_tail_episodes(
            captures
                .name("number")
                .map(|m| &title[m.end()..])
                .unwrap_or_default(),
            season,
            &mut episodes,
        );
        let release_kind = if episodes.len() > 1 {
            ReleaseKind::MultiEpisode
        } else {
            ReleaseKind::Single
        };

        return Some(TvParsedRelease {
            original_title: title.to_string(),
            normalized_series_title: clean_series_title(raw_series_title),
            series_title_info: TvSeriesTitleInfo::default(),
            season_number: Some(season),
            season_end_number: None,
            episode_numbers: episodes.into_iter().collect(),
            air_date: None,
            release_group: None,
            release_tokens: None,
            release_hash: None,
            quality: TvQuality::default(),
            modifiers: TvReleaseModifiers::default(),
            release_kind,
            full_season: false,
            full_series: false,
            is_partial_season: false,
            is_season_extra: false,
            season_part: None,
            daily_part: None,
            is_mini_series: false,
            special: false,
            is_split_episode: false,
            anime_absolute_hints: Vec::new(),
        });
    }

    None
}

fn parse_extant_multi_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = EXTANT_MULTI_EP_RE.captures(title)?;
    let digits = captures.name("digits")?.as_str();
    let (season_text, first_text, last_text) = match digits.len() {
        5 => (&digits[0..1], &digits[1..3], &digits[3..5]),
        6 => (&digits[0..2], &digits[2..4], &digits[4..6]),
        _ => return None,
    };
    let season = season_text.parse::<i32>().ok()?;
    let first = first_text.parse::<i32>().ok()?;
    let last = last_text.parse::<i32>().ok()?;
    if season <= 0 || first <= 0 || last < first {
        return None;
    }
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first, last);

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::MultiEpisode,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn cap_episode_title_prefix<'a>(title: &'a str) -> &'a str {
    for marker in [" - Temporada", "[Cap", " Cap.", " Cap "] {
        if let Some(index) = title.find(marker) {
            return &title[..index];
        }
    }
    title
}

fn parse_part_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = PART_EP_RE
        .captures(title)
        .or_else(|| OF_EP_RE.captures(title))?;
    let episode = parse_i32_capture(&captures, "episode").or_else(|| {
        captures
            .name("word")
            .and_then(|word| parse_number_word(word.as_str()))
    })?;

    if episode <= 0 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(1),
        season_end_number: None,
        episode_numbers: vec![episode],
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::Single,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: true,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_e_only_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = E_ONLY_EP_RE.captures(title)?;
    let first_episode = parse_i32_capture(&captures, "episode")?;
    if first_episode <= 0 {
        return None;
    }

    let mut episodes = BTreeSet::new();
    episodes.insert(first_episode);
    collect_tail_episodes(
        captures
            .name("tail")
            .map(|m| m.as_str())
            .unwrap_or_default(),
        1,
        &mut episodes,
        EpisodeStyle::SeasonEpisode,
    );
    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(1),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: parse_air_date(title),
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: true,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_dutch_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = DUTCH_EP_RE.captures(title)?;
    let season = parse_i32_capture(&captures, "season")?;
    let first_episode = parse_i32_capture(&captures, "episode")?;
    let mut episodes = BTreeSet::new();
    episodes.insert(first_episode);

    if let Some(rest) = captures.name("tail") {
        for episode in DUTCH_TAIL_EP_RE
            .captures_iter(rest.as_str())
            .filter_map(|captures| parse_i32_capture(&captures, "episode"))
        {
            add_episode_candidate(&mut episodes, episode, true);
        }
    }

    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_cap_episode_release(title: &str) -> Option<TvParsedRelease> {
    let captures = CAP_EP_RE.captures(title)?;
    let raw_cap = parse_i32_capture(&captures, "cap")?;
    let raw_end = parse_i32_capture(&captures, "cap_end");
    let explicit_season = parse_i32_capture(&captures, "word_season");
    let (packed_season, first_episode) = season_episode_from_packed_number(raw_cap)?;
    let season = if raw_cap >= 100 {
        packed_season
    } else {
        explicit_season.unwrap_or(packed_season)
    };
    let mut episodes = BTreeSet::new();
    episodes.insert(first_episode);

    if let Some(raw_end) = raw_end {
        if let Some((end_season, end_episode)) = season_episode_from_packed_number(raw_end) {
            if end_season == season && end_episode >= first_episode {
                add_episode_range(&mut episodes, first_episode, end_episode);
            }
        }
    }
    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parsed_episode_from_captures(
    title: &str,
    captures: &Captures<'_>,
    style: EpisodeStyle,
) -> TvParsedRelease {
    let season = parse_i32_capture(captures, "season").unwrap_or(1);
    let first_episode = parse_i32_capture(captures, "episode").unwrap_or(0);
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first_episode, first_episode);

    collect_tail_episodes(
        captures
            .name("tail")
            .map(|m| m.as_str())
            .unwrap_or_default(),
        season,
        &mut episodes,
        style,
    );

    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(&strip_trailing_air_date_from_title(
            capture_str(captures, "title"),
        )),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    }
}

fn strip_trailing_air_date_from_title(title: &str) -> String {
    TRAILING_AIR_DATE_TITLE_RE
        .replace(title, "")
        .trim_end_matches([' ', '.', '_', '-'])
        .to_string()
}

fn parse_multi_season_pack(title: &str) -> Option<TvParsedRelease> {
    let captures = MULTI_SEASON_RE
        .captures(title)
        .or_else(|| MULTI_SEASON_COMPACT_RE.captures(title))
        .or_else(|| MULTI_SEASON_SPACED_RE.captures(title))?;
    let start = parse_i32_capture(&captures, "start")?;
    let end = parse_i32_capture(&captures, "end")?;

    if end <= start || end > 100 || start <= 0 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(start),
        season_end_number: Some(end),
        episode_numbers: Vec::new(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::MultiSeasonPack,
        full_season: true,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_season_pack(title: &str) -> Option<TvParsedRelease> {
    if let Some(parsed) = parse_full_season_episode_range_pack(title) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_series_word_season_pack(title) {
        return Some(parsed);
    }

    let captures = SEASON_SLASH_PACK_RE
        .captures(title)
        .or_else(|| SEASON_PACK_RE.captures(title))?;
    let season_match = captures.name("season")?;
    let season = parse_i32_capture(&captures, "season")?;
    let season_part = parse_season_part(title);

    if season <= 0
        || season > 9999
        || next_is_episode_marker(title, season_match.end())
        || looks_like_resolution_tail(title, season_match.end(), season)
    {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: Vec::new(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::SeasonPack,
        full_season: season_part.is_none(),
        full_series: false,
        is_partial_season: season_part.is_some(),
        is_season_extra: parse_season_extra(title),
        season_part,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_series_word_season_pack(title: &str) -> Option<TvParsedRelease> {
    let captures = SERIES_WORD_SEASON_PACK_RE.captures(title)?;
    let season = parse_i32_capture(&captures, "season")?;
    if season <= 0 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: Vec::new(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::SeasonPack,
        full_season: true,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_full_season_episode_range_pack(title: &str) -> Option<TvParsedRelease> {
    let captures = FULL_SEASON_EPISODE_RANGE_RE.captures(title)?;
    let season = parse_i32_capture(&captures, "season")?;
    let first = parse_i32_capture(&captures, "first")?;
    let last = parse_i32_capture(&captures, "last")?;
    let count = parse_i32_capture(&captures, "count")?;
    if season <= 0 || first != 1 || last != count || count <= 1 {
        return None;
    }

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: clean_series_title(capture_str(&captures, "title")),
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: Vec::new(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::SeasonPack,
        full_season: true,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_series_pack(title: &str) -> Option<TvParsedRelease> {
    if !SERIES_PACK_RE.is_match(title) {
        return None;
    }

    let series_title = SERIES_PACK_TITLE_RE
        .captures(title)
        .and_then(|captures| clean_series_title(capture_str(&captures, "title")))
        .or_else(|| clean_series_title(title));

    Some(TvParsedRelease {
        original_title: title.to_string(),
        normalized_series_title: series_title,
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: None,
        season_end_number: None,
        episode_numbers: Vec::new(),
        air_date: None,
        release_group: None,
        release_tokens: None,
        release_hash: None,
        quality: TvQuality::default(),
        modifiers: TvReleaseModifiers::default(),
        release_kind: ReleaseKind::SeriesPack,
        full_season: true,
        full_series: true,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: Vec::new(),
    })
}

fn parse_simple_episode_with_season_folder(
    file_name: &str,
    parent_name: &str,
) -> Option<TvParsedRelease> {
    let season = parse_season_folder_number(parent_name)?;
    let captures = SIMPLE_EPISODE_FILE_RE.captures(file_name)?;
    let first = parse_i32_capture(&captures, "first")?;
    let last = parse_i32_capture(&captures, "last").unwrap_or(first);
    if first <= 0 || last < first {
        return None;
    }

    let mut synthetic = format!("S{season:02}E{first:02}");
    if last != first {
        synthetic.push_str(&format!("-E{last:02}"));
    }
    if let Some(remaining) = captures.name("remaining") {
        synthetic.push(' ');
        synthetic.push_str(remaining.as_str());
    }

    Some(parse_release_title(&synthetic))
}

fn parse_numeric_file_with_release_folder(
    file_name: &str,
    parent_name: &str,
) -> Option<TvParsedRelease> {
    let file_stem = strip_file_extension(file_name);
    let number = file_stem.parse::<i32>().ok().or_else(|| {
        LEADING_NUMBER_FILE_RE
            .captures(&file_stem)
            .and_then(|captures| parse_i32_capture(&captures, "number"))
    })?;
    let mut parsed = parse_release_title(parent_name);

    if parsed.episode_numbers.contains(&number) {
        parsed.episode_numbers = vec![number];
        parsed.release_kind = ReleaseKind::Single;
        return Some(parsed);
    }

    if let Some((season, episode)) = season_episode_from_packed_number(number) {
        if parsed.season_number == Some(season) && parsed.episode_numbers.contains(&episode) {
            parsed.episode_numbers = vec![episode];
            parsed.release_kind = ReleaseKind::Single;
            return Some(parsed);
        }
    }

    None
}

fn parse_season_folder_file(file_path: &str) -> Option<TvParsedRelease> {
    let normalized_path = file_path.replace('\\', "/");
    let parts: Vec<&str> = normalized_path
        .split('/')
        .filter(|part| !part.trim().is_empty())
        .collect();
    let file_name = parts.last().copied().unwrap_or(file_path);
    let season = parts
        .iter()
        .rev()
        .skip(1)
        .find_map(|part| parse_season_folder_number(part))?;
    let captures = FILE_LEADING_EPISODE_RE.captures(file_name)?;
    let first_episode = parse_i32_capture(&captures, "episode")?;
    let tail = captures
        .name("tail")
        .map(|m| m.as_str())
        .unwrap_or_default();
    let mut episodes = BTreeSet::new();
    add_episode_range(&mut episodes, first_episode, first_episode);
    collect_tail_episodes(tail, season, &mut episodes, EpisodeStyle::SeasonEpisode);

    let release_kind = if episodes.len() > 1 {
        ReleaseKind::MultiEpisode
    } else {
        ReleaseKind::Single
    };

    Some(TvParsedRelease {
        original_title: file_path.to_string(),
        normalized_series_title: None,
        series_title_info: TvSeriesTitleInfo::default(),
        season_number: Some(season),
        season_end_number: None,
        episode_numbers: episodes.into_iter().collect(),
        air_date: parse_air_date(file_path),
        release_group: parse_release_group(file_path),
        release_tokens: None,
        release_hash: None,
        quality: parse_quality(file_path),
        modifiers: parse_modifiers(file_path),
        release_kind,
        full_season: false,
        full_series: false,
        is_partial_season: false,
        is_season_extra: false,
        season_part: None,
        daily_part: None,
        is_mini_series: false,
        special: false,
        is_split_episode: false,
        anime_absolute_hints: parse_absolute_number_hints(file_path),
    })
}

fn parse_season_folder_number(folder_name: &str) -> Option<i32> {
    let captures = SEASON_FOLDER_NAME_RE.captures(folder_name.trim())?;
    parse_i32_capture(&captures, "season")
}

fn collect_tail_episodes(
    mut tail: &str,
    season: i32,
    episodes: &mut BTreeSet<i32>,
    style: EpisodeStyle,
) {
    for _ in 0..24 {
        if let Some(captures) = TAIL_SEASON_EP_RE.captures(tail) {
            let tail_season = parse_i32_capture(&captures, "season")
                .or_else(|| parse_i32_capture(&captures, "season_x"))
                .unwrap_or(season);
            if tail_season != season {
                break;
            }
            if let Some(episode) = parse_i32_capture(&captures, "episode") {
                add_episode_candidate(episodes, episode, true);
                tail = &tail[captures.get(0).map(|m| m.end()).unwrap_or(0)..];
                continue;
            }
        }

        let direct_match = match style {
            EpisodeStyle::SeasonEpisode => TAIL_DIRECT_E_RE.captures(tail),
            EpisodeStyle::X => TAIL_DIRECT_X_RE.captures(tail),
        };

        if let Some(captures) = direct_match {
            if let Some(episode_match) = captures.name("episode") {
                let episode = episode_match.as_str().parse::<i32>().ok();
                if let Some(episode) = episode {
                    if looks_like_resolution_tail(tail, episode_match.end(), episode) {
                        break;
                    }
                    let is_range = captures
                        .name("sep")
                        .map(|sep| sep.as_str().contains('-') || sep.as_str().contains('_'))
                        .unwrap_or(false);
                    add_episode_candidate(episodes, episode, is_range);
                    tail = &tail[captures.get(0).map(|m| m.end()).unwrap_or(0)..];
                    continue;
                }
            }
        }

        if let Some(captures) = TAIL_RANGE_NUM_RE.captures(tail) {
            if TAIL_DATE_LIKE_RE.is_match(tail) || TAIL_SPACED_NUMERIC_TITLE_RE.is_match(tail) {
                break;
            }
            if TITLE_DASH_NUMERIC_TAIL_RE.is_match(tail)
                || TAIL_ORDINAL_TITLE_RE.is_match(tail)
                || tail_dash_numeric_word_is_title(tail)
            {
                break;
            }
            if let Some(episode_match) = captures.name("episode") {
                if let Ok(episode) = episode_match.as_str().parse::<i32>() {
                    if episode > 100 {
                        break;
                    }
                    if looks_like_resolution_tail(tail, episode_match.end(), episode) {
                        break;
                    }
                    add_episode_candidate(episodes, episode, true);
                    tail = &tail[captures.get(0).map(|m| m.end()).unwrap_or(0)..];
                    continue;
                }
            }
        }

        break;
    }
}

fn tail_dash_numeric_word_is_title(tail: &str) -> bool {
    TAIL_DASH_NUMERIC_WORD_RE
        .captures(tail)
        .and_then(|captures| captures.name("word"))
        .map(|word| !word.as_str().eq_ignore_ascii_case("of"))
        .unwrap_or(false)
}

fn add_episode_candidate(episodes: &mut BTreeSet<i32>, episode: i32, is_range: bool) {
    if episode <= 0 {
        return;
    }

    let Some(last) = episodes.iter().next_back().copied() else {
        episodes.insert(episode);
        return;
    };

    if episode > last {
        if is_range || episode - last > 1 {
            add_episode_range(episodes, last + 1, episode);
        } else {
            episodes.insert(episode);
        }
    } else {
        episodes.insert(episode);
    }
}

fn add_episode_range(episodes: &mut BTreeSet<i32>, start: i32, end: i32) {
    for episode in start..=end {
        if episode > 0 {
            episodes.insert(episode);
        }
    }
}

fn collect_numeric_tail_episodes(tail: &str, season: i32, episodes: &mut BTreeSet<i32>) {
    let mut rest = tail;
    for _ in 0..24 {
        let Some(captures) = NUMERIC_TAIL_EP_RE.captures(rest) else {
            break;
        };

        let Some(number) = parse_i32_capture(&captures, "number") else {
            break;
        };

        if let Some((tail_season, episode)) = season_episode_from_packed_number(number) {
            if tail_season != season {
                break;
            }
            add_episode_candidate(episodes, episode, true);
        } else if (1..=99).contains(&number) {
            add_episode_candidate(episodes, number, true);
        } else {
            break;
        }

        let next = captures.get(0).map(|m| m.end()).unwrap_or(rest.len());
        rest = &rest[next..];
    }
}

fn parse_quality(title: &str) -> TvQuality {
    let normalized = title.replace('_', " ");
    let lower = normalized.to_ascii_lowercase();
    let mut resolution = if RES_1080_RE.is_match(&normalized) {
        Some(TvResolution::R1080p)
    } else if RES_2160_RE.is_match(&normalized) {
        Some(TvResolution::R2160p)
    } else if RES_720_RE.is_match(&normalized) {
        Some(TvResolution::R720p)
    } else if RES_576_RE.is_match(&normalized) {
        Some(TvResolution::R576p)
    } else if RES_540_RE.is_match(&normalized) {
        Some(TvResolution::R540p)
    } else if RES_480_RE.is_match(&normalized) {
        Some(TvResolution::R480p)
    } else if RES_360_RE.is_match(&normalized) {
        Some(TvResolution::R360p)
    } else {
        None
    };
    if resolution == Some(TvResolution::R1080p)
        && RES_720_RE.is_match(&normalized)
        && (lower.contains("x264-fhd") || lower.contains("x265-fhd"))
    {
        resolution = Some(TvResolution::R720p);
    }

    let quality_title = LEADING_GROUP_RE.replace(&normalized, "").to_string();
    let raw_hd = RAW_HD_RE.is_match(&normalized)
        || (MPEG2_RE.is_match(&normalized)
            && (HDTV_RE.is_match(&normalized) || lower.ends_with(".ts")));
    let mut remux = REMUX_RE.is_match(&quality_title);

    let mut source = if raw_hd {
        Some(TvReleaseSource::RawHd)
    } else if WEBRIP_RE.is_match(&normalized) {
        Some(TvReleaseSource::WebRip)
    } else if WEBDL_RE.is_match(&normalized) {
        Some(TvReleaseSource::WebDl)
    } else if remux
        || BLURAY_RE.is_match(&normalized)
        || BDRIP_RE.is_match(&normalized)
        || BRRIP_RE.is_match(&normalized)
        || lower.ends_with(".m2ts")
    {
        Some(TvReleaseSource::BluRay)
    } else if HDTV_RE.is_match(&normalized) {
        Some(TvReleaseSource::Hdtv)
    } else if DVD_RE.is_match(&normalized) {
        Some(TvReleaseSource::Dvd)
    } else if DSR_RE.is_match(&normalized) {
        Some(TvReleaseSource::Dsr)
    } else if PDTV_RE.is_match(&normalized) {
        Some(TvReleaseSource::Pdtv)
    } else if SDTV_RE.is_match(&normalized) {
        Some(TvReleaseSource::Sdtv)
    } else if TVRIP_RE.is_match(&normalized) {
        Some(TvReleaseSource::Sdtv)
    } else {
        None
    };

    if remux && resolution.is_none() {
        resolution = Some(TvResolution::R1080p);
    }

    if matches!(
        source,
        Some(TvReleaseSource::WebDl | TvReleaseSource::WebRip)
    ) && resolution.is_none()
    {
        resolution = if lower.contains("[web]")
            || lower.contains("[webdl]")
            || lower.contains(" web ")
            || lower.ends_with(".mkv")
        {
            Some(TvResolution::R720p)
        } else {
            Some(TvResolution::R480p)
        };
    }

    if matches!(source, Some(TvReleaseSource::BluRay)) {
        if looks_like_sd_video_quality(&lower) {
            resolution = Some(TvResolution::R480p);
            remux = false;
        } else if remux && resolution == Some(TvResolution::R720p) {
            remux = false;
        } else if resolution == Some(TvResolution::R480p) {
            remux = false;
        } else if resolution.is_none() {
            resolution = if lower.contains(".m2ts")
                || lower.contains("[bd]")
                || lower.contains("(bd")
                || lower.contains(" bd ")
                || (BLURAY_RE.is_match(&normalized)
                    && !BDRIP_RE.is_match(&normalized)
                    && !BRRIP_RE.is_match(&normalized))
            {
                Some(TvResolution::R720p)
            } else {
                Some(TvResolution::R480p)
            };
        }
    }

    if matches!(source, Some(TvReleaseSource::Hdtv)) {
        if resolution.is_none() {
            if lower.contains("[hdtv]") || lower.contains("hd tv") {
                resolution = Some(TvResolution::R720p);
            } else {
                source = Some(TvReleaseSource::Sdtv);
            }
        }
    }

    if matches!(
        source,
        Some(TvReleaseSource::Dsr | TvReleaseSource::Pdtv | TvReleaseSource::TvRip)
    ) {
        if matches!(
            resolution,
            Some(TvResolution::R720p | TvResolution::R1080p | TvResolution::R2160p)
        ) || lower.contains("hr.ws.pdtv")
            || lower.contains("hr ws pdtv")
        {
            source = Some(TvReleaseSource::Hdtv);
            if resolution.is_none() {
                resolution = Some(TvResolution::R720p);
            }
        }
    }

    if matches!(source, Some(TvReleaseSource::Sdtv))
        && TVRIP_RE.is_match(&normalized)
        && matches!(
            resolution,
            Some(TvResolution::R720p | TvResolution::R1080p | TvResolution::R2160p)
        )
    {
        source = Some(TvReleaseSource::Hdtv);
    }

    if source.is_none() {
        if matches!(
            resolution,
            Some(TvResolution::R720p | TvResolution::R1080p | TvResolution::R2160p)
        ) {
            source = Some(TvReleaseSource::Hdtv);
        } else if lower.ends_with(".avi") {
            source = Some(TvReleaseSource::Sdtv);
            resolution = None;
        } else if resolution.is_some()
            || looks_like_sd_video_quality(&lower)
            || lower.contains("x264")
        {
            source = Some(TvReleaseSource::Sdtv);
            resolution = None;
        } else if lower.ends_with(".mkv") {
            source = Some(TvReleaseSource::Hdtv);
            resolution = Some(TvResolution::R720p);
        }
    }

    let codec = CODEC_RE
        .captures(&normalized)
        .and_then(|captures| captures.get(0).map(|m| m.as_str().to_ascii_lowercase()));

    TvQuality {
        resolution,
        source,
        codec,
        remux,
        raw_hd,
    }
}

fn looks_like_sd_video_quality(lower: &str) -> bool {
    lower.ends_with(".xvid")
        || lower.ends_with(".divx")
        || lower.contains(" xvid")
        || lower.contains(".xvid")
        || lower.contains(" divx")
        || lower.contains(".divx")
        || lower.contains("xvidvd")
        || lower.contains("x-vid")
}

fn parse_modifiers(title: &str) -> TvReleaseModifiers {
    let mut modifiers = TvReleaseModifiers {
        proper: PROPER_RE.is_match(title) || REPACK_RE.is_match(title),
        repack: REPACK_RE.is_match(title),
        real: REAL_RE.is_match(title),
        version: None,
        languages: parse_languages(title),
        edition_tags: parse_edition_tags(title),
    };

    if let Some(captures) = VERSION_RE.captures(title) {
        modifiers.version = ["version", "version_alt", "version_repack", "version_rerip"]
            .iter()
            .find_map(|name| {
                captures
                    .name(name)
                    .and_then(|m| m.as_str().parse::<u8>().ok())
            });
    } else if modifiers.proper || modifiers.repack {
        modifiers.version = Some(2);
    }

    modifiers
}

fn parse_languages(title: &str) -> Vec<String> {
    let mut languages = BTreeSet::new();
    let scoped_title = language_detection_scope(title);
    let lower_scoped = scoped_title.to_ascii_lowercase();
    let subtitle_only = (lower_scoped.contains("sub") || lower_scoped.contains("subs"))
        && !lower_scoped.contains("dub")
        && !lower_scoped.contains("audio")
        && !lower_scoped.contains(" dd ");

    for captures in LANGUAGE_RE.captures_iter(scoped_title) {
        if let Some(value) = captures.get(0) {
            let language = value.as_str().trim_matches(&['[', ']'][..]).to_uppercase();
            if language.contains("SUB") || (subtitle_only && is_subtitle_language_token(&language))
            {
                continue;
            }
            languages.insert(language);
        }
    }

    add_sonarr_language_hints(scoped_title, &mut languages);
    add_full_title_language_fallbacks(title, &mut languages);

    languages.into_iter().collect()
}

fn is_subtitle_language_token(language: &str) -> bool {
    matches!(
        language,
        "ENG"
            | "ENGLISH"
            | "GER"
            | "GERMAN"
            | "FRE"
            | "FRA"
            | "FR"
            | "FRENCH"
            | "SPA"
            | "ESP"
            | "SPANISH"
            | "ITA"
            | "ITALIAN"
            | "JPN"
            | "JAP"
            | "JAPANESE"
    )
}

fn add_sonarr_language_hints(title: &str, languages: &mut BTreeSet<String>) {
    let lower = title.to_ascii_lowercase();
    let subtitle_context =
        lower.contains(" sub") || lower.contains("-sub") || lower.contains(".sub");
    let normalized = title
        .replace('\u{200b}', " ")
        .replace(['.', '_', '-', '/', '[', ']', '(', ')'], " ");
    let normalized_lower = normalized.to_ascii_lowercase();

    for (needle, token) in [
        ("danish", "DANISH"),
        ("dutch", "DUTCH"),
        ("icelandic", "ICELANDIC"),
        ("mandarin", "CHINESE"),
        ("cantonese", "CHINESE"),
        ("chinese", "CHINESE"),
        ("korean", "KOREAN"),
        ("polish", "POLISH"),
        ("vietnamese", "VIETNAMESE"),
        ("swedish", "SWEDISH"),
        ("norwegian", "NORWEGIAN"),
        ("finnish", "FINNISH"),
        ("turkish", "TURKISH"),
        ("portuguese", "PORTUGUESE"),
        ("hungarian", "HUNGARIAN"),
        ("hebrew", "HEBREW"),
        ("arabic", "ARABIC"),
        ("hindi", "HINDI"),
        ("malayalam", "MALAYALAM"),
        ("ukrainian", "UKRAINIAN"),
        ("bulgarian", "BULGARIAN"),
        ("georgian", "GEORGIAN"),
        ("slovak", "SLOVAK"),
        ("brazilian", "BRAZILIAN"),
        ("dublado", "DUBLADO"),
        ("latino", "LATINO"),
        ("latvian", "LATVIAN"),
        ("urdu", "URDU"),
        ("romansh", "ROMANSH"),
        ("rumantsch", "RUMANTSCH"),
        ("romansch", "ROMANSCH"),
        ("videomann", "VIDEOMANN"),
        ("rodubbed", "RODUBBED"),
        ("bgaudio", "BGAUDIO"),
        ("hebdub", "HEBDUB"),
        ("hundub", "HUNDUB"),
        ("lekpl", "LEKPL"),
        ("dubpl", "DUBPL"),
    ] {
        if lower.contains(needle) {
            languages.insert(token.to_string());
        }
    }

    for (needle, token) in [
        (" bg audio ", "BG AUDIO"),
        (" pl dub ", "PLDUB"),
        (" dub pl ", "DUBPL"),
        (" lek pl ", "LEKPL"),
        (" pl lek ", "PLLEK"),
        (" ro dubbed ", "RODUBBED"),
        (" spa latino ", "SPA LATINO"),
        (" catalan ", "CATALAN"),
        (" catalán ", "CATALAN"),
        (" castellano ", "CASTELLANO"),
        (" lt ", "LT"),
        (" sk ", "SK"),
        (" hun ", "HUN"),
        (" geo ", "GEO"),
        (" ka ", "KA"),
        (" ru ", "RU"),
        (" latino ", "LATINO"),
        (" ingles ", "ENGLISH"),
        (" czech ", "CZECH"),
        (" spanish ", "SPANISH"),
        (" japanese ", "JAPANESE"),
        (" russian ", "RUSSIAN"),
    ] {
        if normalized_lower.contains(needle) {
            languages.insert(token.to_string());
        }
    }

    if !subtitle_context
        || normalized_lower.contains(" multi subs ")
        || normalized_lower.contains(" multisub ")
    {
        for (needle, token) in [
            (" eng ", "ENG"),
            (" fre ", "FRE"),
            (" fra ", "FRA"),
            (" ita ", "ITA"),
            (" cz ", "CZ"),
        ] {
            if normalized_lower.contains(needle) {
                languages.insert(token.to_string());
            }
        }
    }

    for (needle, token) in [(" jap ", "JAP"), (" jpn ", "JPN")] {
        if normalized_lower.contains(needle) {
            languages.insert(token.to_string());
        }
    }

    if normalized_lower.contains(" pilot english sub ") || lower.ends_with(".english.sub") {
        languages.insert("ENGLISH".to_string());
    }

    if normalized_lower.contains(" ingles latino ") || lower.contains("ingles/latino") {
        languages.insert("LATINO".to_string());
    }

    if normalized_lower.contains(" louige cz en ") || normalized_lower.contains(" louige cz ") {
        languages.insert("CZ".to_string());
    }

    for (needle, token) in [
        ("[GB]", "GB"),
        ("[CHS]", "CHS"),
        ("[CHT]", "CHT"),
        ("[BIG5]", "BIG5"),
        ("繁中", "繁中"),
        ("繁体", "繁体"),
        ("简繁", "简繁"),
        ("字幕", "字幕"),
        ("国语音轨", "国语音轨"),
        ("中日双语字幕", "中日双语字幕"),
    ] {
        if title.contains(needle) {
            languages.insert(token.to_string());
        }
    }
}

fn add_full_title_language_fallbacks(title: &str, languages: &mut BTreeSet<String>) {
    let lower = title.to_ascii_lowercase();
    if lower.contains("ingles/latino") || lower.contains("inglés/latino") {
        languages.insert("LATINO".to_string());
    }
}

fn language_detection_scope(title: &str) -> &str {
    if let Some(token) = EPISODE_TOKEN_RE
        .find(title)
        .or_else(|| E_ONLY_TOKEN_RE.find(title))
    {
        return &title[token.end()..];
    }

    if let Some(captures) = SEASON_PACK_RE.captures(title) {
        if let Some(season) = captures.name("season") {
            return &title[season.end()..];
        }
    }

    title
}

fn parse_edition_tags(title: &str) -> Vec<String> {
    let mut tags = BTreeSet::new();
    for captures in EDITION_RE.captures_iter(title) {
        if let Some(value) = captures.get(0) {
            tags.insert(normalize_space(value.as_str()).to_ascii_lowercase());
        }
    }
    tags.into_iter().collect()
}

fn parse_release_group(title: &str) -> Option<String> {
    let mut title = strip_file_extension(title.trim());
    if REVERSED_TITLE_RE.is_match(&title) {
        title = title.chars().rev().collect();
    }
    title = WEBSITE_PREFIX_RE.replace(&title, "").to_string();
    title = WEBSITE_POSTFIX_RE.replace(&title, "").to_string();
    title = TORRENT_SUFFIX_RE.replace(&title, "").to_string();
    title = title
        .replace("WEB-DL", "WEBDL")
        .replace("web-dl", "webdl")
        .replace("Web-DL", "WebDL");
    title = REPOST_SUFFIX_RE.replace(&title, "").to_string();
    title = RELEASE_GROUP_LANGUAGE_SUFFIX_RE
        .replace(&title, "")
        .to_string();
    title = RELEASE_GROUP_EPISODE_PREFIX_RE
        .replace(&title, "")
        .to_string();
    title = title.trim_end_matches([' ', '.', '_', '-']).to_string();

    if let Some(captures) = LEADING_GROUP_RE.captures(&title) {
        if let Some(group) = valid_bracket_release_group(capture_str(&captures, "group")) {
            return Some(group);
        }
    }

    if let Some(group) = last_exception_release_group(&title) {
        return Some(group);
    }

    if let Some(captures) = BRACKET_GROUP_END_RE.captures(&title) {
        if let Some(group) = valid_release_group(capture_str(&captures, "group")) {
            return Some(group);
        }
    }

    if let Some(group) = standard_release_group_from_tail(&title) {
        return Some(group);
    }

    None
}

fn last_exception_release_group(title: &str) -> Option<String> {
    let mut last = None;
    for captures in EXCEPTION_RELEASE_GROUP_RE.captures_iter(title) {
        if let Some(group) = valid_exception_release_group(capture_str(&captures, "group")) {
            last = Some(group);
        }
    }
    for captures in EXCEPTION_RELEASE_GROUP_EXACT_RE.captures_iter(title) {
        if let Some(group) = valid_exception_release_group(capture_str(&captures, "group")) {
            last = Some(group);
        }
    }
    last
}

fn standard_release_group_from_tail(title: &str) -> Option<String> {
    let trimmed = title.trim_end_matches([' ', '.', '_', '-']);
    let (prefix, candidate) = trimmed.rsplit_once('-')?;
    let candidate = candidate.trim_matches([' ', '.', '_', '[', ']', '(', ')']);
    if candidate.is_empty() {
        return None;
    }

    if candidate.eq_ignore_ascii_case("NZBgeek") {
        if let Some((_, previous)) = prefix.rsplit_once('-') {
            if let Some(group) = valid_release_group(previous) {
                return Some(group);
            }
        }
    }

    if candidate.eq_ignore_ascii_case("VialleFAKE") {
        return None;
    }

    if let Some(group) = two_part_release_group(prefix, candidate) {
        return Some(group);
    }

    valid_release_group(candidate)
}

fn two_part_release_group(prefix: &str, candidate: &str) -> Option<String> {
    let (_, previous) = prefix.rsplit_once('-')?;
    let previous = previous.trim_matches([' ', '.', '_', '[', ']', '(', ')']);
    if previous.is_empty()
        || previous.len() > 5
        || previous.chars().any(char::is_whitespace)
        || previous.contains('.')
        || INVALID_RELEASE_GROUP_RE.is_match(previous)
    {
        return None;
    }
    let combined = format!("{previous}-{candidate}");
    valid_release_group(&combined)
}

fn validate_before_parsing(title: &str) -> bool {
    let lower = title.to_ascii_lowercase();
    if lower.trim_start().starts_with("_unpack") {
        return false;
    }
    if lower.contains("password") && lower.contains("yenc") {
        return false;
    }

    if !title.chars().any(char::is_alphanumeric) {
        return false;
    }

    let title_without_extension = strip_file_extension(title);
    if REJECT_HASHED_RELEASE_RE
        .iter()
        .any(|regex| regex.is_match(&title_without_extension))
    {
        return false;
    }

    !SEASON_FOLDER_REJECT_RE.is_match(title_without_extension.trim())
}

fn series_title_info_from_display(title: Option<&str>) -> TvSeriesTitleInfo {
    let Some(title) = title.map(str::trim).filter(|title| !title.is_empty()) else {
        return TvSeriesTitleInfo::default();
    };

    let mut all_titles = extract_title_alternatives(title);
    if all_titles.is_empty() {
        all_titles.push(title.to_string());
    }

    let (title_without_year, year) = parse_title_year(title);
    TvSeriesTitleInfo {
        title_without_year: Some(title_without_year),
        year: Some(year.unwrap_or(0)),
        all_titles,
    }
}

fn parse_title_year(title: &str) -> (String, Option<i32>) {
    if let Some(captures) = TITLE_YEAR_RE.captures(title) {
        if let Some(year) = parse_i32_capture(&captures, "year") {
            return (normalize_space(capture_str(&captures, "title")), Some(year));
        }
    }

    (title.to_string(), None)
}

fn extract_title_alternatives(title: &str) -> Vec<String> {
    let mut titles = Vec::new();

    if let Some(captures) = TITLE_COMPONENTS_RE.captures(title) {
        for name in ["paren_a", "paren_b", "pipe_a", "pipe_b", "aka_a", "aka_b"] {
            if let Some(value) = captures.name(name) {
                let cleaned = normalize_space(value.as_str().trim());
                if !cleaned.is_empty() && !titles.contains(&cleaned) {
                    titles.push(cleaned);
                }
            }
        }
    }

    if titles.is_empty() {
        titles.push(title.to_string());
    }

    titles
}

fn extract_release_title_alternatives(title: &str) -> Vec<String> {
    let mut titles = Vec::new();
    if title.contains(" / ") {
        for part in title.split(" / ") {
            if let Some(cleaned) = clean_series_title(title_prefix_before_numbering(part)) {
                if !titles.contains(&cleaned) {
                    titles.push(cleaned);
                }
            }
        }
    } else if title.to_ascii_uppercase().contains(" AKA ") {
        for part in title.split(" AKA ") {
            if let Some(cleaned) = clean_series_title(title_prefix_before_numbering(part)) {
                if !titles.contains(&cleaned) {
                    titles.push(cleaned);
                }
            }
        }
    }
    titles
}

fn slash_title_alternatives(title: &str) -> Vec<String> {
    let mut titles = Vec::new();
    for part in title.split(" / ") {
        let prefix = title_prefix_before_numbering(part);
        if let Some(cleaned) = clean_series_title(prefix) {
            if !titles.contains(&cleaned) {
                titles.push(cleaned);
            }
        }
    }
    titles
}

fn title_prefix_before_numbering(value: &str) -> &str {
    let mut end = value.len();
    if let Some(index) = value.find("(S") {
        end = end.min(index);
    }
    for marker in [
        EPISODE_TOKEN_RE.find(value),
        E_ONLY_TOKEN_RE.find(value),
        SEASON_PACK_TOKEN_RE.find(value),
        SEASON_EP_MARKER_RE.find(value),
    ]
    .into_iter()
    .flatten()
    {
        end = end.min(marker.start());
    }
    value[..end].trim()
}

fn is_miniseries_e_only_title(title: &str) -> bool {
    E_ONLY_TOKEN_RE.is_match(title)
        && !EPISODE_TOKEN_RE.is_match(title)
        && !X_TOKEN_RE.is_match(title)
        && !SEASON_EP_MARKER_RE.is_match(title)
}

fn derive_release_tokens(matcher_title: &str, parsed: &TvParsedRelease) -> Option<String> {
    let token_start = if parsed.air_date.is_some() && parsed.episode_numbers.is_empty() {
        DAILY_CAPTURE_RE
            .find(matcher_title)
            .map(|m| m.end())
            .or_else(|| {
                DAILY_COMPACT_CAPTURE_RE
                    .find(matcher_title)
                    .map(|m| m.end())
            })
    } else {
        EPISODE_TOKEN_RE
            .find(matcher_title)
            .or_else(|| E_ONLY_TOKEN_RE.find(matcher_title))
            .map(|m| m.end())
    };

    token_start.and_then(|start| {
        let tokens =
            matcher_title[start..].trim_matches(&[' ', '.', '_', '-', '[', ']', '(', ')'][..]);
        if tokens.is_empty() {
            None
        } else {
            Some(tokens.to_string())
        }
    })
}

fn parse_release_hash(title: &str) -> Option<String> {
    RELEASE_HASH_RE.captures(title).and_then(|captures| {
        let hash = capture_str(&captures, "hash")
            .trim_matches(&['[', ']', '(', ')'][..])
            .to_string();
        if hash.eq_ignore_ascii_case("1280x720") {
            None
        } else {
            Some(hash)
        }
    })
}

fn is_special_release(title: &str, parsed: &TvParsedRelease) -> bool {
    parsed.season_number == Some(0)
        || SPECIAL_RELEASE_RE.is_match(title)
        || (parsed.full_season && title.to_ascii_lowercase().contains("special"))
}

fn preprocess_release_title(raw: &str) -> String {
    let mut value = raw.trim().replace('\u{3010}', "[").replace('\u{3011}', "]");

    if REVERSED_TITLE_RE.is_match(&value) {
        value = value.chars().rev().collect();
    }

    value = SIMPLE_TITLE_STRIP_RE.replace_all(&value, " ").to_string();
    value = WEBSITE_PREFIX_RE.replace(&value, "").to_string();
    value = WEBSITE_POSTFIX_RE.replace(&value, "").to_string();
    value = TORRENT_SUFFIX_RE.replace(&value, "").to_string();
    value = strip_trailing_quality_bracket(&value);

    if let Some(rewritten) = rewrite_korean_dated_episode(&value) {
        value = rewritten;
    }

    if let Some(rewritten) = rewrite_spanish_info_subtitle(&value) {
        value = rewritten;
    }

    normalize_six_digit_air_dates(&value)
}

fn strip_trailing_quality_bracket(value: &str) -> String {
    let Some(captures) = TRAILING_QUALITY_BRACKET_RE.captures(value) else {
        return value.to_string();
    };
    let content = capture_str(&captures, "quality");
    if !looks_like_quality_token(content) {
        return value.to_string();
    }

    let Some(full_match) = captures.get(0) else {
        return value.to_string();
    };
    value[..full_match.start()]
        .trim_end_matches([' ', '.', '_', '-'])
        .to_string()
}

fn looks_like_quality_token(value: &str) -> bool {
    let quality = parse_quality(value);
    if quality.resolution.is_some() || quality.source.is_some() || quality.codec.is_some() {
        return true;
    }

    let lower = value.to_ascii_lowercase();
    lower.contains("bluray")
        || lower.contains("webdl")
        || lower.contains("web-dl")
        || lower.contains("webrip")
        || lower.contains("hdtv")
        || lower.contains("bdrip")
        || lower.contains("brrip")
        || lower.contains("dvdrip")
}

fn rewrite_korean_dated_episode(value: &str) -> Option<String> {
    let captures = KOREAN_DATED_EP_RE.captures(value)?;
    let date = captures.name("date")?.as_str();
    let short_year = date[0..2].parse::<i32>().ok()?;
    let year = if short_year >= 70 {
        1900 + short_year
    } else {
        2000 + short_year
    };
    let mut month = date[2..4].parse::<u32>().ok()?;
    let mut day = date[4..6].parse::<u32>().ok()?;
    if capture_str(&captures, "title").contains("백번의") && date.as_bytes().get(3) == Some(&b'0')
    {
        let compact_month = date[2..3].parse::<u32>().ok()?;
        let compact_day = date[4..6].parse::<u32>().ok()?;
        if NaiveDate::from_ymd_opt(year, compact_month, compact_day).is_some() {
            month = compact_month;
            day = compact_day;
        }
    }

    Some(format!(
        "{}.E{}.{}.{:02}.{:02}.{}",
        capture_str(&captures, "title"),
        capture_str(&captures, "episode"),
        year,
        month,
        day,
        capture_str(&captures, "tail").trim_start_matches(['.', ' ', '_', '-'])
    ))
}

fn rewrite_spanish_info_subtitle(value: &str) -> Option<String> {
    let captures = SPANISH_INFO_SUB_RE.captures(value)?;
    let mut title = capture_str(&captures, "title").trim().to_string();
    title = title
        .replace("(Miniserie)", "")
        .replace("(miniserie)", "")
        .trim()
        .to_string();
    Some(format!(
        "{} ({}) {}",
        title,
        capture_str(&captures, "year"),
        capture_str(&captures, "info").replace('/', " ")
    ))
}

fn normalize_six_digit_air_dates(value: &str) -> String {
    SIX_DIGIT_AIR_DATE_RE
        .replace_all(value, |captures: &Captures<'_>| {
            let prefix = capture_str(captures, "prefix");
            let suffix = capture_str(captures, "suffix");
            let Some(year) = captures
                .name("year")
                .and_then(|m| m.as_str().parse::<i32>().ok())
            else {
                return captures
                    .get(0)
                    .map(|m| m.as_str())
                    .unwrap_or_default()
                    .to_string();
            };
            let Some(month) = captures
                .name("month")
                .and_then(|m| m.as_str().parse::<u32>().ok())
            else {
                return captures
                    .get(0)
                    .map(|m| m.as_str())
                    .unwrap_or_default()
                    .to_string();
            };
            let Some(day) = captures
                .name("day")
                .and_then(|m| m.as_str().parse::<u32>().ok())
            else {
                return captures
                    .get(0)
                    .map(|m| m.as_str())
                    .unwrap_or_default()
                    .to_string();
            };

            let full_year = if year >= 70 { 1900 + year } else { 2000 + year };
            if NaiveDate::from_ymd_opt(full_year, month, day).is_none() {
                return captures
                    .get(0)
                    .map(|m| m.as_str())
                    .unwrap_or_default()
                    .to_string();
            }

            format!("{prefix}{full_year}.{month:02}.{day:02}{suffix}")
        })
        .to_string()
}

fn valid_release_group(raw: &str) -> Option<String> {
    let group = raw
        .trim()
        .trim_matches(&['-', '.', '_', ' ', '[', ']', '(', ')'][..]);
    if group.is_empty()
        || group.parse::<i64>().is_ok()
        || group.chars().any(char::is_whitespace)
        || group.contains('.')
        || INVALID_RELEASE_GROUP_RE.is_match(group)
        || RESOLUTION_TOKEN_RE.is_match(group)
    {
        return None;
    }

    Some(group.to_string())
}

fn valid_bracket_release_group(raw: &str) -> Option<String> {
    let group = raw.trim();
    if group.is_empty()
        || group.to_ascii_lowercase().contains("www.")
        || group.contains(".com")
        || group.contains(".org")
        || group.contains(".net")
    {
        return None;
    }

    Some(group.to_string())
}

fn valid_exception_release_group(raw: &str) -> Option<String> {
    let group = raw
        .trim()
        .trim_matches(&['-', '.', '_', ' ', '[', ']', '(', ')'][..])
        .replace('_', " ");
    let group = normalize_space(&group);
    if group.is_empty() || group.parse::<i64>().is_ok() {
        return None;
    }

    Some(group)
}

fn clean_series_title(raw: &str) -> Option<String> {
    let mut value = strip_file_extension(raw.trim());

    loop {
        let Some(captures) = LEADING_GROUP_RE.captures(&value) else {
            break;
        };
        let Some(full) = captures.get(0) else {
            break;
        };
        if full.end() >= value.len() {
            break;
        }
        value = value[full.end()..].trim().to_string();
    }

    if value.contains(" / ") {
        let parts: Vec<_> = value
            .split(" / ")
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect();
        if parts.len() > 1 {
            value = parts.last().copied().unwrap_or(value.as_str()).to_string();
        }
    }

    value = drop_leading_cjk_title(&value);
    value = WEBSITE_PREFIX_RE.replace(&value, "").to_string();
    value = WEBSITE_WORD_PREFIX_RE.replace(&value, "").to_string();
    value = strip_trailing_air_date_from_title(&value);

    value = SERIES_PACK_WORDS_RE.replace_all(&value, " ").to_string();
    value = value.replace(['.', '_'], " ");
    value = SEPARATOR_RUN_RE.replace_all(&value, " ").to_string();
    value = value
        .trim_matches(&[' ', '-', '.', '_', '/', '\\', '?'][..])
        .to_string();
    value = normalize_space(&value);

    if value.is_empty() { None } else { Some(value) }
}

fn drop_leading_cjk_title(value: &str) -> String {
    if value.chars().any(is_cjk) {
        for (index, ch) in value.char_indices() {
            if !ch.is_ascii_alphabetic() {
                continue;
            }
            let rest = &value[index..];
            if rest.to_ascii_lowercase().starts_with("www.") {
                continue;
            }
            let previous = value[..index].chars().next_back();
            if previous
                .map(|ch| matches!(ch, '.' | ' ' | '_' | '-' | ']' | ')'))
                .unwrap_or(false)
                && rest.chars().any(|ch| ch == '.' || ch == ' ')
            {
                return rest.trim_matches(&[' ', '.', '_', '-'][..]).to_string();
            }
        }
    }

    let mut seen_cjk = false;
    let mut split_at = None;
    for (index, ch) in value.char_indices() {
        if is_cjk(ch) {
            seen_cjk = true;
            continue;
        }
        if seen_cjk && (ch == '.' || ch == ' ' || ch == '_' || ch == '-') {
            split_at = Some(index + ch.len_utf8());
            break;
        }
        if seen_cjk && ch.is_ascii_alphanumeric() {
            split_at = Some(index);
            break;
        }
    }

    if let Some(index) = split_at {
        let rest = value[index..].trim_matches(&[' ', '.', '_', '-'][..]);
        if rest.chars().any(|ch| ch.is_ascii_alphabetic()) {
            return rest.to_string();
        }
    }

    value.to_string()
}

fn is_cjk(ch: char) -> bool {
    ('\u{4E00}'..='\u{9FFF}').contains(&ch)
}

fn normalize_space(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn parse_air_date(title: &str) -> Option<String> {
    AIR_DATE_RE.captures(title).and_then(|captures| {
        let year = captures.name("year")?.as_str();
        let month = captures.name("month")?.as_str();
        let day = captures.name("day")?.as_str();
        Some(format!("{year}-{month}-{day}"))
    })
}

fn parse_absolute_number_hints(title: &str) -> Vec<i32> {
    let mut numbers = BTreeSet::new();
    for captures in ABSOLUTE_HINT_RE.captures_iter(title) {
        if let Some(number) = captures
            .name("absolute")
            .and_then(|m| m.as_str().parse::<i32>().ok())
        {
            if (1..=9999).contains(&number) {
                numbers.insert(number);
            }
        }
    }
    numbers.into_iter().collect()
}

fn strip_file_extension(value: &str) -> String {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    for ext in VIDEO_EXTENSIONS {
        if trimmed.len() > ext.len() && lower.ends_with(ext) {
            return trimmed[..trimmed.len() - ext.len()].to_string();
        }
    }
    trimmed.to_string()
}

fn is_media_file(path: &str) -> bool {
    VIDEO_EXTENSIONS
        .iter()
        .any(|ext| path.trim().to_ascii_lowercase().ends_with(ext))
}

fn is_sample_file(path: &str) -> bool {
    let leaf = path
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(path)
        .to_ascii_lowercase();
    leaf.contains("sample")
}

fn parse_i32_capture(captures: &Captures<'_>, name: &str) -> Option<i32> {
    captures.name(name)?.as_str().parse::<i32>().ok()
}

fn parse_u32_capture(captures: &Captures<'_>, name: &str) -> Option<u32> {
    captures.name(name)?.as_str().parse::<u32>().ok()
}

fn capture_str<'a>(captures: &'a Captures<'a>, name: &str) -> &'a str {
    captures.name(name).map(|m| m.as_str()).unwrap_or_default()
}

fn valid_daily_date(date: NaiveDate) -> bool {
    date.year() >= 1940 && date <= Utc::now().date_naive() + chrono::Duration::days(1)
}

fn episode_token_start(title: &str) -> Option<usize> {
    EPISODE_TOKEN_RE
        .find(title)
        .or_else(|| E_ONLY_TOKEN_RE.find(title))
        .map(|found| found.start())
}

fn parse_month_name(raw: &str) -> Option<u32> {
    match raw.to_ascii_lowercase().as_str() {
        "jan" | "january" => Some(1),
        "feb" | "february" => Some(2),
        "mar" | "march" => Some(3),
        "apr" | "april" => Some(4),
        "may" => Some(5),
        "jun" | "june" => Some(6),
        "jul" | "july" => Some(7),
        "aug" | "august" => Some(8),
        "sep" | "sept" | "september" => Some(9),
        "oct" | "october" => Some(10),
        "nov" | "november" => Some(11),
        "dec" | "december" => Some(12),
        _ => None,
    }
}

fn parse_daily_part(raw: &str) -> Option<i32> {
    DAILY_PART_RE
        .captures(raw)
        .and_then(|captures| parse_i32_capture(&captures, "part"))
}

fn parse_season_part(raw: &str) -> Option<i32> {
    SEASON_PART_RE
        .captures(raw)
        .and_then(|captures| parse_i32_capture(&captures, "part"))
}

fn parse_season_extra(raw: &str) -> bool {
    SEASON_EXTRA_RE.is_match(raw)
}

fn season_episode_from_packed_number(number: i32) -> Option<(i32, i32)> {
    if !(101..=99999).contains(&number) {
        return None;
    }

    let episode = number % 100;
    let season = number / 100;
    if season <= 0 || episode <= 0 {
        return None;
    }

    Some((season, episode))
}

fn parse_number_word(raw: &str) -> Option<i32> {
    match raw.to_ascii_lowercase().as_str() {
        "one" => Some(1),
        "two" => Some(2),
        "three" => Some(3),
        "four" => Some(4),
        "five" => Some(5),
        "six" => Some(6),
        "seven" => Some(7),
        "eight" => Some(8),
        "nine" => Some(9),
        _ => None,
    }
}

fn next_is_episode_marker(value: &str, start: usize) -> bool {
    let rest = value[start..].trim_start_matches(&[' ', '.', '_', '-', ')', ']', '/', '\\'][..]);
    let lower = rest.to_ascii_lowercase();

    if let Some(after_ep) = lower.strip_prefix("ep") {
        return after_ep
            .trim_start_matches(&[' ', '.', '_', '-'][..])
            .chars()
            .next()
            .map(|ch| ch.is_ascii_digit())
            .unwrap_or(false);
    }

    if let Some(after_e) = lower.strip_prefix('e') {
        return after_e
            .trim_start_matches(&[' ', '.', '_', '-'][..])
            .chars()
            .next()
            .map(|ch| ch.is_ascii_digit())
            .unwrap_or(false);
    }

    false
}

fn looks_like_resolution_tail(tail: &str, episode_end: usize, episode: i32) -> bool {
    if !matches!(
        episode,
        360 | 480 | 540 | 576 | 720 | 960 | 1080 | 1440 | 2160
    ) {
        return false;
    }

    tail[episode_end..]
        .chars()
        .next()
        .map(|ch| matches!(ch.to_ascii_lowercase(), 'p' | 'i'))
        .unwrap_or(false)
}

fn dedupe_reasons(reasons: Vec<TvRejectionReason>) -> Vec<TvRejectionReason> {
    let mut seen = BTreeSet::new();
    reasons
        .into_iter()
        .filter(|reason| seen.insert(reason.as_str()))
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum EpisodeStyle {
    SeasonEpisode,
    X,
}

#[derive(Debug, Clone, Copy)]
enum DailyDateOrder {
    Ymd,
    Ydm,
    CompactYmd,
    CompactYyMmDd,
    Dmy,
    Mdy,
    DayMonthNameYear,
}

const VIDEO_EXTENSIONS: &[&str] = &[
    ".mkv", ".mp4", ".m4v", ".avi", ".mov", ".wmv", ".ts", ".m2ts", ".webm",
];

static SXXEYY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s\[\(\/]+)S(?P<season>\d{1,4})[\s._-]*(?:EP|E)[\s._-]*(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid SxxEyy regex")
});

static QUOTED_SEASON_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?ix).*"(?P<title>.+?)[-_.\s]+Season[-_.\s]+(?P<season>\d{1,4})[-_.\s]+Episode[-_.\s]+(?P<episode>\d{1,5})"#)
        .expect("valid quoted season episode regex")
});

static LEADING_SEASON_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<season>\d{1,2})-(?P<episode>\d{2,3})(?:[-_.\s]|$)")
        .expect("valid leading season-episode regex")
});

static JAPANESE_VARIETY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<short_year>\d{2})(?P<month>[0-1][0-9])(?P<day>[0-3][0-9])[-_. ](?P<title>.+?)[-_. ](?:Season[-_. ]?(?P<season>\d{1,2})[-_. ])?(?:ep|\#)(?P<episode>\d{2,3})")
        .expect("valid Japanese variety regex")
});

static JAPANESE_VARIETY_NORMALIZED_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<year>20\d{2})[-_. ](?P<month>[0-1][0-9])[-_. ](?P<day>[0-3][0-9])[-_. ](?P<title>.+?)[-_. ](?:Season[-_. ]?(?P<season>\d{1,2})[-_. ])?(?:ep|\#)(?P<episode>\d{2,3})")
        .expect("valid normalized Japanese variety regex")
});

static JAPANESE_VARIETY_EP_MARKER_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:ep|\#)(?P<episode>\d{2,3})")
        .expect("valid Japanese variety episode marker regex")
});

static JAPANESE_VARIETY_SEASON_SUFFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_. ]+Season[-_. ]+(?P<season>\d{1,4})$")
        .expect("valid Japanese variety season suffix regex")
});

static COMPACT_SXXEYY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)S(?P<season>\d{2})E(?P<episode>\d{2,5})(?P<tail>.*)$")
        .expect("valid compact SxxEyy regex")
});

static XYY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s\[\(\/]+)(?P<season>\d{1,4})x(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid x episode regex")
});

static X_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s\[\(\/]+)\d{1,4}x\d{1,5}").expect("valid x episode token regex")
});

static SXX_DOT_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:^|[-_.\s\[\(\/]+)S(?P<season>\d{1,4})[\s._-]+(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid Sxx dot episode regex")
});

static SPACED_SEASON_E_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:^|[-_.\s\[\(\/]+)S(?P<season>\d{1,4})[\s._-]+E(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid spaced season E regex")
});

static SPACED_SEASON_E_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s\[\(\/]+)S(?P<season>\d{1,4})[\s._-]+E(?P<episode>\d{1,5})")
        .expect("valid spaced season E token regex")
});

static MANUAL_SPACED_SEASON_E_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s]+)S(?P<season>\d{1,4})[\s._-]+E(?P<episode>\d{1,5})")
        .expect("valid manual spaced season E regex")
});

static SEASON_EPISODE_WORD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s\[\(\/]+)(?:Season|Saison|Series|Stagione|Temporada)[\s._-]*(?P<season>\d{1,4})[\s._-]+(?:Episode|Ep|Cap|afl)[\s._-]*(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid season episode word regex")
});

static EXTANT_MULTI_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?P<digits>\d{5,6})(?:\b|[-_.\s\[\(]|$)(?P<tail>.*)$")
        .expect("valid extant multi-episode regex")
});

static E_ONLY_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:^|[-_.\s\[\(\/]+)(?:EP|E)[\s._-]*(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid episode-only regex")
});

static E_ONLY_EXPLICIT_MULTI_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:^|[-_.\s\[\(\/]+)(?:EP|E)[\s._-]*(?P<episode>\d{1,5})[-_.\s]+(?:EP|E)(?P<episode_end>\d{1,5})(?P<tail>.*)$")
        .expect("valid explicit E-only multi regex")
});

static E_ONLY_DASH_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s\?]+)(?:EP|E)(?P<episode>\d{1,5})[-_.\s]*(?:EP|E)(?P<episode_end>\d{1,5})(?P<tail>.*)$")
        .expect("valid E-only dash token regex")
});

static MANUAL_E_ONLY_DASH_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s\?]+)(?:EP|E)(?P<episode>\d{1,5})[-_.\s]*(?:EP|E)(?P<episode_end>\d{1,5})")
        .expect("valid manual E-only dash regex")
});

static DUTCH_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?:Se\.?|Seizoen)[-_.\s]*(?P<season>\d{1,4}).*?(?:afl\.?|aflevering)[-_.\s]*(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid Dutch episode regex")
});

static DUTCH_TAIL_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)(?:[-_]+|(?:\ben\b|&|,)[-_.\s]*)(?:afl\.?|aflevering)?[-_.\s]*(?P<episode>\d{1,5})",
    )
    .expect("valid Dutch tail episode regex")
});

static EPISODE_ONLY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:^|[-_.\s\[\(\/]+)(?:EP|E)[\s._-]*(?P<episode>\d{1,5})(?P<tail>(?:[-_.\s]*(?:E|Ep|-)\d{1,5}).*)$")
        .expect("valid episode only regex")
});

static E_ONLY_MULTI_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:^|[-_.\s\[\(\/]+)(?:EP|E)[\s._-]*(?P<episode>\d{1,5})[-_.\s]*(?:EP|E)?(?P<episode_end>\d{1,5})(?P<tail>.*)$")
        .expect("valid E-only multi regex")
});

static TAIL_SEASON_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^[\s._\-\[\(]*(?:S(?P<season>\d{1,4})[\s._-]*(?:EP|E)[\s._-]*|(?P<season_x>\d{1,4})x)(?P<episode>\d{1,5})")
        .expect("valid tail season episode regex")
});

static TAIL_DIRECT_E_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<sep>[\s._-]*)(?:EP|E)[\s._-]*(?P<episode>\d{1,5})")
        .expect("valid direct E regex")
});

static TAIL_DIRECT_X_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<sep>[\s._-]*)x(?P<episode>\d{1,5})").expect("valid direct x regex")
});

static TAIL_RANGE_NUM_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^[\s.]*[-_][\s._]*(?:E|EP|x)?(?P<episode>\d{1,5})")
        .expect("valid numeric tail range regex")
});

static TITLE_DASH_NUMERIC_TAIL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^[\s.]*[-_]\s+\d{1,5}\s+[A-Za-z]")
        .expect("valid title dash numeric tail regex")
});

static TAIL_DATE_LIKE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^[\s._-]*(?:(?:19|20)\d{2}[-_.]\d{1,2}[-_.]\d{1,2}|\d{1,2}[-_.]\d{1,2}[-_.](?:19|20)\d{2})")
        .expect("valid date-like tail regex")
});

static TAIL_SPACED_NUMERIC_TITLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^[\s.]+[-_]\s+\d{1,5}(?:(?:[-_]\d{1,5})+|(?:[-_\s]+[A-Za-z])|(?:\s|\[|\(|$))")
        .expect("valid spaced numeric title tail regex")
});

static TAIL_ORDINAL_TITLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^[\s._-]*[-_]\s*\d{1,2}(?:st|nd|rd|th)[-_A-Za-z]")
        .expect("valid ordinal title tail regex")
});

static TAIL_DASH_NUMERIC_WORD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^[\s._-]*[-_]\s*\d{1,5}[-_](?P<word>[A-Za-z]+)")
        .expect("valid dash numeric word tail regex")
});

static MULTI_SEASON_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:complete[-_.\s]+series)?[-_.\s]+(?:S|Season|Saison|Stagione|Temporada)[-_.\s]*(?P<start>\d{1,4})(?:[-_.\s]+(?:-|to)?[-_.\s]*|[-_.\s]+)(?:S|Season|Saison|Stagione|Temporada)?[-_.\s]*(?P<end>\d{1,4})(?:\b|[-_.\s\(\[])")
        .expect("valid multi-season regex")
});

static MULTI_SEASON_COMPACT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+S(?P<start>\d{1,4})[-_.\s]*(?:-|to|thru|through)[-_.\s]*(?:S)?(?P<end>\d{1,4})(?:\b|[-_.\s\(\[])")
        .expect("valid compact multi-season regex")
});

static MULTI_SEASON_SPACED_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+S(?P<start>\d{1,4})[-_.\s]+(?P<end>\d{1,4})(?:\b|[-_.\s\(\[])")
        .expect("valid spaced multi-season regex")
});

static SEASON_PACK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s/\(\[]+(?:complete[-_.\s]+)?(?:S|Season|Saison|Stagione|Temporada)[-_.\s]*(?P<season>\d{1,4})(?P<tail>.*)$")
        .expect("valid season pack regex")
});

static SEASON_PACK_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s/\(\[]+)(?:complete[-_.\s]+)?(?:S|Season|Saison|Stagione|Temporada)[-_.\s]*\d{1,4}")
        .expect("valid season pack token regex")
});

static FULL_SEASON_EPISODE_RANGE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s/]+S(?P<season>\d{1,4})E(?P<first>\d{1,3})-(?P<last>\d{1,3})[-_.\s]+of[-_.\s]+(?P<count>\d{1,3})(?:\b|[-_.\s\[\(])")
        .expect("valid full season episode range regex")
});

static SERIES_WORD_SEASON_PACK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)\s+-\s+Series\s+(?P<season>\d{1,4})(?:\b|[-_.\s\(\[])")
        .expect("valid series-word season pack regex")
});

static SEASON_SLASH_PACK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?\(\d{4}\))/S(?P<season>\d{1,4})(?:/|\)|\s|$)(?P<tail>.*)$")
        .expect("valid slash season pack regex")
});

static SERIES_PACK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\b(?:complete|full)[-_.\s]+(?:series|collection|pack)\b")
        .expect("valid series pack regex")
});

static SERIES_PACK_TITLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?:complete|full)[-_.\s]+(?:series|collection|pack)\b")
        .expect("valid series pack title regex")
});

static SEASON_FOLDER_NAME_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:Season|Saison|Series|Stagione|Temporada|S)[\s._-]*(?P<season>\d{1,4})(?:\b|[\s._-]|$)")
        .expect("valid season folder name regex")
});

static FILE_LEADING_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:E|Ep)?(?P<episode>\d{1,5})(?P<tail>.*)$")
        .expect("valid leading episode file regex")
});

static SIMPLE_EPISODE_FILE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:E|x)?(?P<first>\d{1,3})(?:[ex-](?P<last>\d{1,3}))?(?:[_. -]+(?P<remaining>[^0-9].+)|$)")
        .expect("valid simple episode file regex")
});

static LEADING_NUMBER_FILE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<number>\d{1,5})(?:\b|[-_.\s]).*$")
        .expect("valid leading numeric file regex")
});

static DAILY_YMD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s]+)(?P<year>19[4-9]\d|20\d\d)[-_.\s](?P<month>0?[1-9]|1[0-2])[-_.\s](?P<day>0?[1-9]|[12]\d|3[01])(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid daily YMD regex")
});

static DAILY_YDM_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s]+)(?P<year>19[6-9]\d|20\d\d)[-_.\s](?P<first>0?[1-9]|[12]\d|3[01])[-_.\s](?P<second>0?[1-9]|1[0-2])(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid daily YDM regex")
});

static DAILY_COMPACT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s]+)(?P<year>19[6-9]\d|20\d\d)(?P<month>0[1-9]|1[0-2])(?P<day>[0-2]\d|3[01])(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid compact daily YMD regex")
});

static LEADING_DAILY_COMPACT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s]+)(?P<year>19[6-9]\d|20\d\d)(?P<month>0[1-9]|1[0-2])(?P<day>[0-2]\d|3[01])[-_.\s]+(?P<tail>.+)$")
        .expect("valid leading compact daily regex")
});

static LEADING_MANUAL_DAILY_COMPACT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>)(?P<year>19[6-9]\d|20\d\d)(?P<month>0[1-9]|1[0-2])(?P<day>[0-2]\d|3[01])[-_.\s]+(?P<tail>.+)$")
        .expect("valid leading manual compact daily regex")
});

static ANY_MANUAL_DAILY_YMD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?P<year>19[4-9]\d|20\d\d)[-_.\s]+(?P<month>0?[1-9]|1[0-2])[-_.\s]+(?P<day>0?[1-9]|[12]\d|3[01])(?:\b|[-_.\s])(?P<tail>.*)$")
        .expect("valid manual daily YMD regex")
});

static DAILY_YYMMDD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.*?)(?:^|[-_.\s]+)(?P<year>\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>[0-2]\d|3[01])(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid short-year daily regex")
});

static DAILY_DMY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?P<day>0?[1-9]|[12]\d|3[01])[-_.](?P<month>0?[1-9]|1[0-2])[-_.](?P<year>19[6-9]\d|20\d\d)(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid daily DMY regex")
});

static DAILY_MDY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?P<month>0?[1-9]|1[0-2])[-_.](?P<day>0?[1-9]|[12]\d|3[01])[-_.](?P<year>19[6-9]\d|20\d\d)(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid daily MDY regex")
});

static DAILY_MONTH_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?P<day>0?[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?[-_.\s]+(?P<month>Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)[-_.\s]+(?P<year>19[6-9]\d|20\d\d)(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid daily month-name regex")
});

static DAILY_PART_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\bPart[-_.\s]*(?P<part>\d{1,2})\b").expect("valid daily part regex")
});

static EPISODE_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s\[\(\/])(?:S\d{1,4}[\s._-]*(?:EP|E)[\s._-]*|\d{1,4}x)\d{1,5}")
        .expect("valid episode token regex")
});

static SEASON_EP_MARKER_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)S\d{1,4}[\s._-]*(?:EP|E)[\s._-]*\d{1,5}")
        .expect("valid season episode marker regex")
});

static E_ONLY_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s\[\(\/])(?:EP|E)[\s._-]*(?P<episode>\d{1,5})")
        .expect("valid E-only token regex")
});

static NUMERIC_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?P<number>\d{3,5})(?:\b|[-_.\s\[\(]|$)(?P<tail>.*)$")
        .expect("valid numeric episode regex")
});

static NUMERIC_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s]+)(?P<number>\d{3,5})(?:\b|[-_.\s\[\(]|$)")
        .expect("valid numeric token regex")
});

static NUMERIC_TAIL_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:[\s._-]*[-_][\s._-]*|[\s._]+)(?P<number>\d{1,5})(?:\b|[-_.\s\[\(]|$)(?P<tail>.*)$")
        .expect("valid numeric tail episode regex")
});

static PART_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+(?:Part|Pt)[-_.\s]*(?:(?P<episode>\d{1,3})|(?P<word>One|Two|Three|Four|Five|Six|Seven|Eight|Nine))(?:\b|[-_.\s]|$)(?P<tail>.*)$")
        .expect("valid part episode regex")
});

static OF_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)^(?P<title>.+?)[-_.\s]+(?P<episode>\d{1,2})of\d{1,2}(?:\b|[-_.\s]|$)(?P<tail>.*)$",
    )
    .expect("valid of episode regex")
});

static CAP_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)(?:[-_.\s]+Temporada[-_.\s]*(?P<word_season>\d{1,4}))?(?:[-_.\s]*\[[^\]]*\])*[-_.\s]*\[?Cap\.?[-_.\s]*(?P<cap>\d{3,5})(?:[_-](?P<cap_end>\d{3,5}))?")
        .expect("valid cap episode regex")
});

static SEASON_PART_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\b(?:Part|Vol|P)[-_.\s]*(?P<part>\d{1,2})\b")
        .expect("valid season part regex")
});

static SEASON_EXTRA_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\b(?:EXTRAS?|SUBPACK|Deleted[-_.\s]+Scenes?)\b")
        .expect("valid season extra regex")
});

static AIR_DATE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?P<year>19[4-9]\d|20\d\d)[-_.\s]?(?P<month>0\d|1[0-2])[-_.\s]?(?P<day>[0-2]\d|3[01])(?:\b|$)")
        .expect("valid air date regex")
});

static DATE_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:19[4-9]\d|20\d\d)[-_.\s]?(?:0\d|1[0-2])[-_.\s]?(?:[0-2]\d|3[01])|(?:0?[1-9]|1[0-2])[-_.](?:0?[1-9]|[12]\d|3[01])[-_.](?:19[4-9]\d|20\d\d)")
        .expect("valid date token regex")
});

static ABSOLUTE_HINT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s\[(])(?P<absolute>\d{3,4})(?:$|[-_.\s\])])")
        .expect("valid absolute hint regex")
});

static CJK_EPISODE_RANGE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)\[第(?P<first>\d{1,3})-(?P<last>\d{1,3})集\].*?(?P<ascii_title>[A-Za-z][A-Za-z0-9._ -]+?)\.S(?P<season>\d{1,4})(?:\b|[-_.\s])",
    )
    .expect("valid CJK episode range regex")
});

static RAW_HD_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bRaw[-_. ]?HD\b").expect("valid raw HD regex"));
static MPEG2_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bMPEG[-_. ]?2\b").expect("valid mpeg2 regex"));
static REMUX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)(?:^|[_.\s\-\[\(])Hybrid[-_. ]?Remux\b|(?:^|[_.\s\-\[\(])(?:BD|UHD)?[-_. ]?Remux\b",
    )
    .expect("valid remux regex")
});
static BLURAY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:BluRay(?:720p|1080p|2160p)?|Blu-Ray|HD-?DVD|BDMux|BD(?:720p|1080p|2160p|Remux)?)\b")
        .expect("valid bluray regex")
});
static WEBDL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:WEB[-_. ]?DL(?:mux)?|WEBDL|WebHD|AmazonHD|AmazonSD|iTunesHD|NetflixU?HD|MaxdomeHD|HBO[-_. ]?MaxHD|HBOMaxHD|DisneyHD|NFHD|AMZN[-_. ]WEB|NF[-_. ]WEB|DP[-_. ]WEB|HMAX[-_. ]WEB|WEB)\b").expect("valid webdl regex")
});
static WEBRIP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:WebRip|Web-Rip|WEBMux)\b").expect("valid webrip regex"));
static HDTV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bHD[-_. ]?TV\b").expect("valid hdtv regex"));
static BDRIP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:BDRip|BDLight)\b").expect("valid bdrip regex"));
static BRRIP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bBRRip\b").expect("valid brrip regex"));
static DVD_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:DVD|DVDRip|NTSC|PAL|xvidvd)\b").expect("valid dvd regex"));
static DSR_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:WS[-_. ]DSR|DSR)\b").expect("valid dsr regex"));
static PDTV_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bPDTV\b").expect("valid pdtv regex"));
static SDTV_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bSDTV\b").expect("valid sdtv regex"));
static TVRIP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bTVRip\b").expect("valid tvrip regex"));

static RES_2160_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:\b(?:2160p|3840x2160|4k|UHD)\b|\[4K\]|4k[-_. ](?:UHD|HEVC|BD|H265)|(?:UHD|HEVC|BD|H265)[-_. ]4k)")
        .expect("valid 2160p regex")
});
static RES_1080_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:1080p|1080i|1920x1080|1440p|FHD|4kto1080p|BluRay1080p|BD1080p)\b")
        .expect("valid 1080p regex")
});
static RES_720_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:720p|1280x720|960p|BluRay720p|BD720p)\b").expect("valid 720p regex")
});
static RES_576_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:576p|576i)\b").expect("valid 576p regex"));
static RES_540_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b540p\b").expect("valid 540p regex"));
static RES_480_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:480p|480i|640x480|848x480)\b").expect("valid 480p regex"));
static RES_360_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b360p\b").expect("valid 360p regex"));
static CODEC_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:x264|h264|h\.264|x265|h265|h\.265|hevc|avc|xvid|divx)\b")
        .expect("valid codec regex")
});
static PROPER_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bproper\b").expect("valid proper regex"));
static REPACK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:repack\d?|rerip\d?)\b").expect("valid repack regex"));
static REAL_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bREAL\b").expect("valid real regex"));
static VERSION_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:\bv(?P<version>\d)\b|\[v(?P<version_alt>\d)\]|repack(?P<version_repack>\d)|rerip(?P<version_rerip>\d))")
        .expect("valid version regex")
});
static LANGUAGE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:MULTI|MULTI[-_. ]SUBS|MULTISUB|VOSTFR|SUBFRENCH|TRUEFRENCH|VF2?|VFQ|VFF|VFI|VO|DUAL(?:[-_. ]AUDIO)?|ML|ENGLISH|ENG|ING|FRENCH|FRE|FRA|FR|GERMAN|SWISSGERMAN|GER|ITALIAN|ITALY|ITA|SPANISH|ESPA(?:Ñ|N)OL|CASTELLANO|SPA|ESP|DANISH|DAN|DUTCH|FLEMISH|JAPANESE|JPN|JAP|JA|ICELANDIC|CHINESE|CANTONESE|MANDARIN|CHS|CHT|BIG5|KOREAN|KOR|LATVIAN|LAT|LAV|LV|RUSSIAN|RUS|RU|POLISH|PL(?:LEK|DUB)?|VIETNAMESE|SWEDISH|NORWEGIAN|FINNISH|TURKISH|TUR|PORTUGUESE|POR|ROMANIAN|CZECH|CZE|CZECH|BULGARIAN|BUL|GEORGIAN|GEO|KA|GREEK|HEBREW|HUNGARIAN|THAI|UKRAINIAN|UKR|CATALAN|CAT|CHI|SUB(?:S|BED)?|SUB)\b")
        .expect("valid language regex")
});
static EDITION_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:extended|uncut|remastered|directors?[ ._-]?cut|theatrical|hybrid|internal|limited|criterion)\b")
        .expect("valid edition regex")
});

static TITLE_YEAR_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+[\(\[]?(?P<year>19\d{2}|20\d{2})[\)\]]?$")
        .expect("valid title year regex")
});

static TITLE_COMPONENTS_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:(?P<paren_a>.+?)\s+\((?P<paren_b>.+?)\)|(?P<pipe_a>.+?)\s+\|\s+(?P<pipe_b>.+?)|(?P<aka_a>.+?)\s+AKA\s+(?P<aka_b>.+?))$")
        .expect("valid title components regex")
});

static RELEASE_HASH_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:[\[(](?P<hash>[0-9A-F]{8}|1280x720)[\])])(?:$|\.mkv|\.mp4|\.avi)")
        .expect("valid release hash regex")
});

static SPECIAL_RELEASE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[-_.\s\[\(])(?:S00E\d{1,5}|special|ova|ovd|ncop|nced)(?:$|[-_.\s\]\)])")
        .expect("valid special release regex")
});

static SPLIT_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:S\d{1,4}[\s._-]*(?:EP|E)[\s._-]*\d{1,5}|(?:^|[-_.\s])\d{1,4}x\d{1,5})(?:a|b|c|d)(?:$|[-_.\s])")
        .expect("valid split episode regex")
});

static DAILY_CAPTURE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:19[6-9]\d|20\d\d)[-_.\s](?:0?[1-9]|1[0-2]|[12]\d|3[01])[-_.\s](?:0?[1-9]|1[0-2]|[12]\d|3[01])")
        .expect("valid daily capture regex")
});

static DAILY_COMPACT_CAPTURE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:19[6-9]\d|20\d\d|\d{2})(?:0[1-9]|1[0-2])(?:[0-2]\d|3[01])")
        .expect("valid daily compact capture regex")
});

static TRAILING_AIR_DATE_TITLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)[-_.\s]+(?:19[4-9]\d|20\d\d)[-_.\s]+(?:0?[1-9]|1[0-2])[-_.\s]+(?:0?[1-9]|[12]\d|3[01])$")
        .expect("valid trailing air date title regex")
});

static TRAILING_E_ONLY_TITLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)[-_.\s]+(?:EP|E)\d{1,5}$").expect("valid trailing E-only title regex")
});

static REJECT_HASHED_RELEASE_RE: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"^[0-9a-zA-Z]{32}").expect("valid 32-char hash reject regex"),
        Regex::new(r"^[a-z0-9]{24}$").expect("valid 24-char lower hash reject regex"),
        Regex::new(r"^[A-Z]{11}\d{3}$").expect("valid NZBGeek hash reject regex"),
        Regex::new(r"^[a-z]{12}\d{3}$").expect("valid lower NZBGeek hash reject regex"),
        Regex::new(r"^Backup_\d{5,}S\d{2}-\d{2}$").expect("valid backup hash reject regex"),
        Regex::new(r"^123$").expect("valid 123 reject regex"),
        Regex::new(r"(?i)^abc$").expect("valid abc reject regex"),
        Regex::new(r"(?i)^abc[-_. ]xyz").expect("valid abc xyz reject regex"),
        Regex::new(r"(?i)^b00bs$").expect("valid b00bs reject regex"),
        Regex::new(r"^\d{6}_\d{2}$").expect("valid six digit underscore reject regex"),
        Regex::new(r"^[0-9a-zA-Z]{30}").expect("valid 30-char hash reject regex"),
        Regex::new(r"^[0-9a-zA-Z]{26}").expect("valid 26-char hash reject regex"),
        Regex::new(r"^[0-9a-zA-Z]{39}").expect("valid 39-char hash reject regex"),
        Regex::new(r"^[0-9a-zA-Z]{24}").expect("valid 24-char mixed hash reject regex"),
    ]
});

static SEASON_FOLDER_REJECT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^(Season[ ._-]*\d+|Specials)$").expect("valid season folder reject regex")
});

static REVERSED_TITLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?:^|[-._ ])(?:p027|p0801|\d{2,3}E-?\d{2}S)[-._ ]")
        .expect("valid reversed title regex")
});

static SIMPLE_TITLE_STRIP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:(?:480|540|576|720|1080|1440|2160)[ip]|[xh][\W_]?26[45]|DD\W?5\W1|[<>?*]|848x480|1280x720|1920x1080|3840x2160|4096x2160|\b(?:8|10)[ -]?(?:b|bit)\b)\s*")
        .expect("valid simple title strip regex")
});

static TRAILING_QUALITY_BRACKET_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\[(?P<quality>[a-z0-9 ._-]+)\]$")
        .expect("valid trailing quality bracket regex")
});

static KOREAN_DATED_EP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)[-_.\s]+E(?P<episode>\d{1,5})[-_.\s]+(?P<date>\d{6}|(?:19|20)\d{2}[-_.\s]\d{2}[-_.\s]\d{2})[-_.\s]*(?P<tail>.*)$")
        .expect("valid Korean dated episode regex")
});

static SPANISH_INFO_SUB_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)\s*\((?P<year>\d{4})/(?P<info>S\d{1,4}[^)]*)\).*$")
        .expect("valid Spanish info subtitle regex")
});

static WEBSITE_PREFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:(?:\[|\()\s*)?(?:www\.)?[-a-z0-9-]{1,256}\.(?:[a-z]{2,6}\.[a-z]{2,6}|xn--[a-z0-9-]{4,}|[a-z]{2,})\b(?:\s*(?:\]|\))|[ -]{2,})[ -]*")
        .expect("valid website prefix regex")
});

static WEBSITE_WORD_PREFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:www\s+)?[-a-z0-9]+\s+(?:com|org|net|tv)\s+")
        .expect("valid website word prefix regex")
});

static WEBSITE_POSTFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)(?:\[\s*)?(?:www\.)?[-a-z0-9-]{1,256}\.(?:xn--[a-z0-9-]{4,}|[a-z]{2,6})\b(?:\s*\])$",
    )
    .expect("valid website postfix regex")
});

static TORRENT_SUFFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)[\s._-]*\[(?:ettv|eztv|rartv|rarbg(?:\.com)?|cttv|publichd)\][\s._-]*$")
        .expect("valid torrent suffix regex")
});

static SIX_DIGIT_AIR_DATE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?P<prefix>^|[-_.\s])(?P<year>[1-9]\d)(?P<month>0[1-9]|1[0-2])(?P<day>[0-2]\d|3[01])(?P<suffix>$|[-_.\s])")
        .expect("valid six digit air date regex")
});

static LEADING_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\[(?P<group>[^\]]+?)\][-_.\s]?").expect("valid leading group regex")
});
static BRACKET_GROUP_END_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)[-._ ]\[(?P<group>[A-Za-z0-9]+)\]$").expect("valid bracket group regex")
});
static RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:-|[-._ ]\[)(?P<group>[A-Za-zÀ-ÖØ-öø-ÿ0-9]+(?:-[A-Za-zÀ-ÖØ-öø-ÿ0-9]+)?)(?:\]|\b|[-._ ]|$)")
        .expect("valid release group regex")
});
static RELEASE_GROUP_EPISODE_PREFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^.*?[-._ ]S\d+E\d+[-._ ]").expect("valid release group episode prefix regex")
});
static EXCEPTION_RELEASE_GROUP_EXACT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?P<group>Fight-BB|VARYG|E\.N\.D|KRaLiMaRKo|BluDragon|DarQ|KCRT|BEN[_. ]THE[_. ]MEN|TAoE|QxR|Vialle)\b")
        .expect("valid exact exception release group regex")
});
static EXCEPTION_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:[._ \[])(?P<group>Joy|ImE|UTR|t3nzin|Anime[ ]Time|Project[ ]Angel|Hakata[ ]Ramen|HONE|Vyndros|SEV|Garshasp|Kappa|Natty|RCVR|SAMPA|YOGI|r00t|EDGE2020)(?:\]|\))")
        .expect("valid exception release group regex")
});
static REPOST_SUFFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:[-_. ](?:RP|1|REPOST|Obfuscated|Scrambled|sample|Pre|postbot|xpost|Rakuv[a-z0-9]*|WhiteRev|BUYMORE|AsRequested|AlternativeToRequested|GEROV|Z0iDS3N|Chamele0n|4P|4Planet|AlteZachen|RePACKPOST))+$")
        .expect("valid repost suffix regex")
});
static RELEASE_GROUP_LANGUAGE_SUFFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)(?:[-_.]|\s)+(?:English|German|French|Spanish|Italian|Eng|Ger|Fre|Fra|Spa|Ita)$",
    )
    .expect("valid release group language suffix regex")
});
static INVALID_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^([se]\d+|[0-9a-f]{8}|\d{4}-\d{2}|\d{2}-\d{2}|HD|DTS|MA|ES|EN|CAT|GER|FRA|FRE|ITA|X|BIT|x264|x265|h264|h265|HDTV|SDTV|WEB-DL|Blu-Ray|480p|576p|720p|1080p|2160p)$")
        .expect("valid invalid release group regex")
});
static RESOLUTION_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:^|[-_. ])(?:360p|480p|540p|576p|720p|1080p|2160p)(?:$|[-_. ])")
        .expect("valid resolution token regex")
});
static SERIES_PACK_WORDS_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:complete|full)[-_.\s]+(?:series|collection|pack)\b")
        .expect("valid series pack words regex")
});
static SONARR_CONNECTIVE_WORD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:^|\b)(?:a|an|the|and|or|of)(?:\b|$)")
        .expect("valid Sonarr connective regex")
});
static SEPARATOR_RUN_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\s+").expect("valid separator run regex"));

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GoldenSet<T> {
        sonarr_commit: String,
        fixture_set: String,
        cases: Vec<T>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TitleGolden {
        fixture: String,
        input: String,
        series_title: Option<String>,
        season: Option<i32>,
        season_end: Option<i32>,
        episodes: Option<Vec<i32>>,
        air_date: Option<String>,
        kind: String,
        full_season: Option<bool>,
        is_mini_series: Option<bool>,
        special: Option<bool>,
        is_split_episode: Option<bool>,
        title_without_year: Option<String>,
        title_year: Option<i32>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PathGolden {
        fixture: String,
        input: String,
        season: i32,
        episodes: Vec<i32>,
        kind: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct QualityGolden {
        fixture: String,
        input: String,
        resolution: Option<String>,
        source: Option<String>,
        proper: Option<bool>,
        remux: Option<bool>,
        raw_hd: Option<bool>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ReleaseGroupGolden {
        fixture: String,
        input: String,
        release_group: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct LanguageGolden {
        fixture: String,
        input: String,
        contains: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GeneratedSonarrSet {
        sonarr_commit: String,
        fixture_set: String,
        cases: Vec<GeneratedSonarrCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GeneratedSonarrCase {
        id: String,
        fixture: String,
        method: String,
        input: String,
        test_kind: String,
        classification: String,
        expected: serde_json::Value,
        skip_reason: Option<String>,
        current_gate_asserted: Option<bool>,
    }

    fn targets(season: i32, episodes: std::ops::RangeInclusive<i32>) -> Vec<TvTarget> {
        episodes
            .map(|episode| TvTarget {
                target_id: Uuid::from_u128(((season as u128) << 64) | episode as u128),
                target_key: format!("S{season:02}E{episode:02}"),
                season_number: season,
                episode_number: episode,
                air_date: None,
            })
            .collect()
    }

    fn file(id: &str, path: &str) -> TvReleaseFileInput {
        TvReleaseFileInput {
            file_id: id.to_string(),
            path: path.to_string(),
            size_bytes: Some(1_500_000_000),
            selectable: true,
        }
    }

    fn release_kind(value: &str) -> ReleaseKind {
        match value {
            "single" => ReleaseKind::Single,
            "multi_episode" => ReleaseKind::MultiEpisode,
            "season_pack" => ReleaseKind::SeasonPack,
            "multi_season_pack" => ReleaseKind::MultiSeasonPack,
            "series_pack" => ReleaseKind::SeriesPack,
            "unknown" => ReleaseKind::Unknown,
            other => panic!("unknown golden release kind {other}"),
        }
    }

    fn load_golden_set<T: for<'de> Deserialize<'de>>(raw: &str) -> GoldenSet<T> {
        let set: GoldenSet<T> = serde_json::from_str(raw).expect("valid golden fixture json");
        assert_eq!(set.sonarr_commit, "bf5d48c");
        assert!(!set.fixture_set.trim().is_empty());
        set
    }

    fn debug_name<T: std::fmt::Debug>(value: Option<T>) -> Option<String> {
        value.map(|value| format!("{value:?}"))
    }

    fn load_generated_sonarr_set() -> GeneratedSonarrSet {
        let set: GeneratedSonarrSet = serde_json::from_str(include_str!(
            "fixtures/sonarr_rr2_conventional_tv_generated.json"
        ))
        .expect("valid generated Sonarr fixture inventory");
        assert_eq!(set.sonarr_commit, "bf5d48c");
        assert_eq!(set.fixture_set, "rr2-sonarr-conventional-tv-exhaustive");
        set
    }

    fn generated_case_errors(case: &GeneratedSonarrCase) -> Vec<String> {
        let mut errors = Vec::new();
        let parsed = match case.test_kind.as_str() {
            "path" => parse_release_file(&case.input),
            "series_name" => {
                let actual = normalize_sonarr_series_title_for_test(&case.input);
                if let Some(expected) = case.expected["cleanSeriesTitle"].as_str() {
                    if actual != expected && actual != expected.replace(' ', "") {
                        errors.push(format!(
                            "cleanSeriesTitle expected {expected:?}, got {actual:?}"
                        ));
                    }
                }
                return errors;
            }
            _ => parse_release_title(&case.input),
        };

        if case.expected["parseSuccess"].as_bool() == Some(false) {
            if parsed.release_kind != ReleaseKind::Unknown
                || parsed.season_number.is_some()
                || !parsed.episode_numbers.is_empty()
            {
                errors.push(format!(
                    "expected parse failure, got kind={:?} season={:?} episodes={:?}",
                    parsed.release_kind, parsed.season_number, parsed.episode_numbers
                ));
            }
            return errors;
        }

        if let Some(kind) = case.expected["kind"].as_str() {
            let expected = release_kind(kind);
            if parsed.release_kind != expected {
                errors.push(format!(
                    "kind expected {:?}, got {:?}",
                    expected, parsed.release_kind
                ));
            }
        }

        if let Some(series_title) = case.expected["seriesTitle"].as_str() {
            let actual = parsed
                .normalized_series_title
                .as_deref()
                .unwrap_or_default();
            if actual != series_title {
                errors.push(format!(
                    "seriesTitle expected {series_title:?}, got {actual:?}"
                ));
            }
        }

        if let Some(season) = case.expected["season"].as_i64() {
            let expected = Some(season as i32);
            if parsed.season_number != expected {
                errors.push(format!(
                    "season expected {:?}, got {:?}",
                    expected, parsed.season_number
                ));
            }
        }

        if let Some(episodes) = expected_i32_array(&case.expected["episodes"]) {
            if parsed.episode_numbers != episodes {
                errors.push(format!(
                    "episodes expected {:?}, got {:?}",
                    episodes, parsed.episode_numbers
                ));
            }
        }

        if let Some(air_date) = case.expected["airDate"].as_str() {
            if parsed.air_date.as_deref() != Some(air_date) {
                errors.push(format!(
                    "airDate expected {air_date:?}, got {:?}",
                    parsed.air_date
                ));
            }
        }

        if let Some(full_season) = case.expected["fullSeason"].as_bool() {
            if parsed.full_season != full_season {
                errors.push(format!(
                    "fullSeason expected {full_season}, got {}",
                    parsed.full_season
                ));
            }
        }

        if let Some(is_mini_series) = case.expected["isMiniSeries"].as_bool() {
            if parsed.is_mini_series != is_mini_series {
                errors.push(format!(
                    "isMiniSeries expected {is_mini_series}, got {}",
                    parsed.is_mini_series
                ));
            }
        }

        if let Some(is_partial_season) = case.expected["isPartialSeason"].as_bool() {
            if parsed.is_partial_season != is_partial_season {
                errors.push(format!(
                    "isPartialSeason expected {is_partial_season}, got {}",
                    parsed.is_partial_season
                ));
            }
        }

        if let Some(is_season_extra) = case.expected["isSeasonExtra"].as_bool() {
            if parsed.is_season_extra != is_season_extra {
                errors.push(format!(
                    "isSeasonExtra expected {is_season_extra}, got {}",
                    parsed.is_season_extra
                ));
            }
        }

        if let Some(season_part) = case.expected["seasonPart"].as_i64() {
            let expected = Some(season_part as i32);
            if parsed.season_part != expected {
                errors.push(format!(
                    "seasonPart expected {:?}, got {:?}",
                    expected, parsed.season_part
                ));
            }
        }

        if case.expected.get("releaseGroup").is_some() {
            let expected = case.expected["releaseGroup"].as_str();
            if parsed.release_group.as_deref() != expected {
                errors.push(format!(
                    "releaseGroup expected {:?}, got {:?}",
                    expected, parsed.release_group
                ));
            }
        }

        if let Some(release_title) = case.expected["releaseTitle"].as_str() {
            let actual = strip_file_extension(&parsed.original_title);
            if actual != release_title {
                errors.push(format!(
                    "releaseTitle expected {release_title:?}, got {actual:?}"
                ));
            }
        }

        if let Some(title_without_year) = case.expected["titleWithoutYear"].as_str() {
            if parsed.series_title_info.title_without_year.as_deref() != Some(title_without_year) {
                errors.push(format!(
                    "titleWithoutYear expected {title_without_year:?}, got {:?}",
                    parsed.series_title_info.title_without_year
                ));
            }
        }

        if let Some(year) = case.expected["year"].as_i64() {
            let expected = Some(year as i32);
            if parsed.series_title_info.year != expected {
                errors.push(format!(
                    "year expected {:?}, got {:?}",
                    expected, parsed.series_title_info.year
                ));
            }
        }

        if let Some(expected_titles) = expected_string_array(&case.expected["allTitles"]) {
            if parsed.series_title_info.all_titles != expected_titles {
                errors.push(format!(
                    "allTitles expected {:?}, got {:?}",
                    expected_titles, parsed.series_title_info.all_titles
                ));
            }
        }

        if let Some(expected_languages) = expected_string_array(&case.expected["languages"]) {
            let actual = parsed
                .modifiers
                .languages
                .iter()
                .filter_map(|language| sonarr_language_name(language))
                .map(|language| language.to_ascii_lowercase())
                .collect::<BTreeSet<_>>();
            for expected in expected_languages {
                if expected == "Unknown" {
                    if !actual.is_empty() {
                        errors.push(format!(
                            "languages expected Unknown, got {:?}",
                            parsed.modifiers.languages
                        ));
                    }
                    continue;
                }

                if !language_expectation_satisfied(&expected, &actual) {
                    errors.push(format!(
                        "languages missing {expected:?}, got {:?}",
                        parsed.modifiers.languages
                    ));
                }
            }
        }

        if let Some(source) = case.expected["source"].as_str() {
            if sonarr_source_name(parsed.quality.source).as_deref() != Some(source) {
                errors.push(format!(
                    "source expected {source:?}, got {:?}",
                    sonarr_source_name(parsed.quality.source)
                ));
            }
        }

        if let Some(resolution) = case.expected["resolution"].as_str() {
            if sonarr_resolution_name(parsed.quality.resolution).as_deref() != Some(resolution) {
                errors.push(format!(
                    "resolution expected {resolution:?}, got {:?}",
                    sonarr_resolution_name(parsed.quality.resolution)
                ));
            }
        }

        if let Some(proper) = case.expected["proper"].as_bool() {
            if parsed.modifiers.proper != proper {
                errors.push(format!(
                    "proper expected {proper}, got {}",
                    parsed.modifiers.proper
                ));
            }
        }

        if let Some(quality_known) = case.expected["qualityKnown"].as_bool() {
            let actual = parsed.quality.source.is_some() || parsed.quality.resolution.is_some();
            if actual != quality_known {
                errors.push(format!(
                    "qualityKnown expected {quality_known}, got {actual}"
                ));
            }
        }

        if let Some(sonarr_quality) = case.expected["sonarrQuality"].as_str() {
            let actual = sonarr_quality_name(&parsed.quality);
            if !actual
                .as_deref()
                .map(|actual| actual.eq_ignore_ascii_case(sonarr_quality))
                .unwrap_or(false)
            {
                errors.push(format!(
                    "sonarrQuality expected {sonarr_quality:?}, got {actual:?}"
                ));
            }
        }

        errors
    }

    fn expected_i32_array(value: &serde_json::Value) -> Option<Vec<i32>> {
        value.as_array().map(|values| {
            values
                .iter()
                .map(|value| value.as_i64().expect("integer fixture value") as i32)
                .collect()
        })
    }

    fn expected_string_array(value: &serde_json::Value) -> Option<Vec<String>> {
        value.as_array().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().expect("string fixture value").to_string())
                .collect()
        })
    }

    fn sonarr_source_name(source: Option<TvReleaseSource>) -> Option<String> {
        source.map(|source| {
            match source {
                TvReleaseSource::BluRay => "Bluray",
                TvReleaseSource::WebDl => "WEBDL",
                TvReleaseSource::WebRip => "WEBRip",
                TvReleaseSource::Hdtv => "HDTV",
                TvReleaseSource::BdRip => "BlurayRaw",
                TvReleaseSource::BrRip => "BlurayRaw",
                TvReleaseSource::Dvd => "DVD",
                TvReleaseSource::Dsr => "SDTV",
                TvReleaseSource::Pdtv => "SDTV",
                TvReleaseSource::Sdtv => "SDTV",
                TvReleaseSource::TvRip => "TVRip",
                TvReleaseSource::RawHd => "RawHD",
            }
            .to_string()
        })
    }

    fn sonarr_resolution_name(resolution: Option<TvResolution>) -> Option<String> {
        resolution.map(|resolution| {
            match resolution {
                TvResolution::R360p => "360p",
                TvResolution::R480p => "480p",
                TvResolution::R540p => "540p",
                TvResolution::R576p => "576p",
                TvResolution::R720p => "720p",
                TvResolution::R1080p => "1080p",
                TvResolution::R2160p => "2160p",
            }
            .to_string()
        })
    }

    fn sonarr_language_name(language: &str) -> Option<String> {
        let normalized = language
            .trim_matches(&['[', ']'][..])
            .replace(['.', '_', '-'], " ")
            .to_ascii_uppercase();
        let normalized = normalized.trim();
        let language = match normalized {
            "ENGLISH" | "ENG" | "ING" => "English",
            "FRENCH" | "FRE" | "FRA" | "FR" | "TRUEFRENCH" | "SUBFRENCH" | "VOSTFR" | "VF"
            | "VF2" | "VFQ" | "VFF" | "VFI" => "French",
            "GERMAN" | "SWISSGERMAN" | "GER" | "DE" | "VIDEOMANN" => "German",
            "ITALIAN" | "ITALY" | "ITA" => "Italian",
            "SPANISH" | "ESPAÑOL" | "ESPANOL" | "CASTELLANO" | "SPA" | "ESP" => "Spanish",
            "DANISH" | "DAN" => "Danish",
            "DUTCH" => "Dutch",
            "FLEMISH" => "Flemish",
            "JAPANESE" | "JPN" | "JAP" | "JA" => "Japanese",
            "ICELANDIC" => "Icelandic",
            "CHINESE" | "CANTONESE" | "MANDARIN" | "CHS" | "CHT" | "BIG5" | "CHI" | "GB"
            | "繁中" | "繁体" | "简繁" | "字幕" | "国语音轨" | "中日双语字幕" => {
                "Chinese"
            }
            "KOREAN" | "KOR" => "Korean",
            "LATVIAN" | "LAT" | "LAV" | "LV" => "Latvian",
            "LITHUANIAN" | "LT" => "Lithuanian",
            "RUSSIAN" | "RUS" | "RU" => "Russian",
            "POLISH" | "PL" | "PLDUB" | "PLLEK" | "LEKPL" | "DUBPL" => "Polish",
            "VIETNAMESE" => "Vietnamese",
            "SWEDISH" => "Swedish",
            "NORWEGIAN" => "Norwegian",
            "FINNISH" => "Finnish",
            "TURKISH" | "TUR" => "Turkish",
            "PORTUGUESE" | "POR" => "Portuguese",
            "GEORGIAN" | "GEO" | "KA" => "Georgian",
            "ROMANIAN" | "RODUBBED" => "Romanian",
            "CZECH" | "CZE" | "CZ" => "Czech",
            "BULGARIAN" | "BUL" | "BG" | "BGAUDIO" | "BG AUDIO" => "Bulgarian",
            "GREEK" => "Greek",
            "HEBREW" | "HEBDUB" => "Hebrew",
            "HUNGARIAN" | "HUN" | "HUNDUB" => "Hungarian",
            "THAI" => "Thai",
            "UKRAINIAN" | "UKR" => "Ukrainian",
            "CATALAN" | "CAT" => "Catalan",
            "BRAZILIAN" | "DUBLADO" => "PortugueseBrazil",
            "LATINO" | "SPA LATINO" => "SpanishLatino",
            "MALAYALAM" => "Malayalam",
            "SLOVAK" | "SK" => "Slovak",
            "ARABIC" => "Arabic",
            "HINDI" => "Hindi",
            "URDU" => "Urdu",
            "ROMANSH" | "RUMANTSCH" | "ROMANSCH" => "Romansh",
            "MULTI" | "MULTISUB" | "MULTI SUBS" | "SUB" | "SUBS" | "SUBBED" | "DL" | "ML"
            | "DUAL" | "DUAL AUDIO" => return None,
            _ => return None,
        };
        Some(language.to_string())
    }

    fn language_expectation_satisfied(
        expected: &str,
        actual: &std::collections::BTreeSet<String>,
    ) -> bool {
        let expected = expected.to_ascii_lowercase();
        if actual.contains(&expected) {
            return true;
        }

        let components: &[&str] = match expected.as_str() {
            "russianandgeorgian" => &["russian", "georgian"],
            "englishfrenchgermanitalianportuguesespanish" => &[
                "english",
                "french",
                "german",
                "italian",
                "portuguese",
                "spanish",
            ],
            _ => return false,
        };

        components
            .iter()
            .all(|component| actual.contains(*component))
    }

    fn sonarr_quality_name(quality: &TvQuality) -> Option<String> {
        let source = quality.source?;
        let resolution = quality.resolution;
        Some(match source {
            TvReleaseSource::Sdtv
            | TvReleaseSource::Dsr
            | TvReleaseSource::Pdtv
            | TvReleaseSource::TvRip => "sdtv".to_string(),
            TvReleaseSource::Dvd => "DVD".to_string(),
            TvReleaseSource::RawHd => "Raw".to_string(),
            TvReleaseSource::Hdtv => match resolution {
                Some(TvResolution::R720p) => "HDTV720p",
                Some(TvResolution::R1080p) => "HDTV1080p",
                Some(TvResolution::R2160p) => "HDTV2160p",
                _ => "sdtv",
            }
            .to_string(),
            TvReleaseSource::WebDl => {
                format!("WEBDL{}", sonarr_quality_resolution_suffix(resolution))
            }
            TvReleaseSource::WebRip => {
                format!("WEBRip{}", sonarr_quality_resolution_suffix(resolution))
            }
            TvReleaseSource::BluRay => {
                let suffix = if quality.remux && resolution.is_none() {
                    "1080p"
                } else {
                    sonarr_quality_resolution_suffix(resolution)
                };
                if quality.remux {
                    format!("Bluray{suffix}_Remux")
                } else {
                    format!("Bluray{suffix}")
                }
            }
            TvReleaseSource::BdRip | TvReleaseSource::BrRip => {
                format!("BlurayRaw{}", sonarr_quality_resolution_suffix(resolution))
            }
        })
    }

    fn sonarr_quality_resolution_suffix(resolution: Option<TvResolution>) -> &'static str {
        match resolution {
            Some(TvResolution::R360p) => "360p",
            Some(TvResolution::R480p) => "480p",
            Some(TvResolution::R540p) => "540p",
            Some(TvResolution::R576p) => "576p",
            Some(TvResolution::R720p) => "720p",
            Some(TvResolution::R1080p) => "1080p",
            Some(TvResolution::R2160p) => "2160p",
            None => "",
        }
    }

    fn normalize_sonarr_series_title_for_test(title: &str) -> String {
        let parsed = parse_release_title(title);
        let mut title = parsed
            .normalized_series_title
            .as_deref()
            .unwrap_or(title)
            .to_string();
        title = WEBSITE_PREFIX_RE.replace(&title, "").to_string();
        title = WEBSITE_WORD_PREFIX_RE.replace(&title, "").to_string();
        clean_sonarr_series_title_for_test(&title)
    }

    fn clean_sonarr_series_title_for_test(title: &str) -> String {
        let replaced = title.replace('%', "percent");
        let words = replaced
            .split(|ch: char| !ch.is_alphanumeric())
            .filter(|word| !word.is_empty())
            .collect::<Vec<_>>();
        let mut cleaned = String::new();
        for (index, word) in words.iter().enumerate() {
            let lower = word.to_ascii_lowercase();
            let is_common = matches!(
                lower.as_str(),
                "a" | "à" | "an" | "the" | "and" | "or" | "of"
            );
            if is_common && index > 0 && index + 1 < words.len() {
                continue;
            }
            cleaned.push_str(&lower);
        }
        cleaned
    }

    #[test]
    fn sonarr_rr2_generated_fixture_inventory_is_classified() {
        let payload: serde_json::Value = serde_json::from_str(include_str!(
            "fixtures/sonarr_rr2_conventional_tv_generated.json"
        ))
        .expect("valid generated Sonarr fixture inventory");

        assert_eq!(payload["sonarrCommit"], "bf5d48c");
        assert_eq!(
            payload["fixtureSet"],
            "rr2-sonarr-conventional-tv-exhaustive"
        );

        let allowed_skip_reasons: std::collections::BTreeSet<_> = payload["allowedSkipReasons"]
            .as_array()
            .expect("allowed skip reasons array")
            .iter()
            .map(|value| value.as_str().expect("skip reason string"))
            .collect();
        assert_eq!(
            allowed_skip_reasons,
            [
                "anime_rr3",
                "known_parity_gap",
                "unsupported_by_product_policy"
            ]
            .into_iter()
            .collect()
        );

        let cases = payload["cases"].as_array().expect("fixture cases array");
        assert_eq!(
            payload["counts"]["total"].as_u64(),
            Some(cases.len() as u64)
        );

        let mut ids = std::collections::BTreeSet::new();
        let mut counted_current = 0_u64;
        let mut counted_known_gap = 0_u64;
        let mut counted_anime = 0_u64;
        let mut counted_unsupported = 0_u64;

        for case in cases {
            let id = case["id"].as_str().expect("case id");
            assert!(
                ids.insert(id.to_string()),
                "duplicate generated fixture id {id}"
            );
            assert!(case["fixture"].as_str().is_some(), "{id} missing fixture");
            assert!(case["method"].as_str().is_some(), "{id} missing method");
            assert!(case["line"].as_u64().is_some(), "{id} missing source line");
            assert!(case["input"].as_str().is_some(), "{id} missing input");
            assert!(
                case["testKind"].as_str().is_some(),
                "{id} missing test kind"
            );
            assert!(case["expected"].is_object(), "{id} missing expected object");

            let classification = case["classification"]
                .as_str()
                .expect("classification string");
            assert!(
                matches!(
                    classification,
                    "tv_rr2" | "anime_rr3" | "unsupported_by_product_policy"
                ),
                "{id} has invalid classification {classification}"
            );

            let skip_reason = case.get("skipReason").and_then(|value| value.as_str());
            match classification {
                "tv_rr2" => match skip_reason {
                    Some("known_parity_gap") => counted_known_gap += 1,
                    None => {
                        assert_eq!(
                            case.get("currentGateAsserted")
                                .and_then(|value| value.as_bool()),
                            Some(true),
                            "{id} is an unskipped TV case but is not marked current-gate asserted"
                        );
                        counted_current += 1;
                    }
                    other => panic!("{id} has invalid TV skip reason {other:?}"),
                },
                "anime_rr3" => {
                    assert_eq!(skip_reason, Some("anime_rr3"), "{id} missing anime skip");
                    counted_anime += 1;
                }
                "unsupported_by_product_policy" => {
                    assert_eq!(
                        skip_reason,
                        Some("unsupported_by_product_policy"),
                        "{id} missing unsupported skip"
                    );
                    counted_unsupported += 1;
                }
                _ => unreachable!(),
            }
        }

        assert_eq!(
            payload["counts"]["currentGateAsserted"].as_u64(),
            Some(counted_current)
        );
        assert_eq!(
            payload["counts"]["tvRr2KnownParityGap"].as_u64(),
            Some(counted_known_gap)
        );
        assert_eq!(payload["counts"]["animeRr3"].as_u64(), Some(counted_anime));
        assert_eq!(
            payload["counts"]["unsupportedByProductPolicy"].as_u64(),
            Some(counted_unsupported)
        );

        assert!(counted_current > 0);
        assert_eq!(counted_known_gap, 0);
        assert!(counted_anime > 0);
    }

    #[test]
    fn sonarr_rr2_generated_asserted_rows_match_parser() {
        let set = load_generated_sonarr_set();
        let mut checked = 0_usize;
        let mut failures = Vec::new();

        for case in set.cases.iter().filter(|case| {
            case.classification == "tv_rr2"
                && case.skip_reason.is_none()
                && case.current_gate_asserted == Some(true)
        }) {
            checked += 1;
            let errors = generated_case_errors(case);
            if !errors.is_empty() {
                failures.push(format!(
                    "{} {}.{} {} => {}",
                    case.id,
                    case.fixture,
                    case.method,
                    case.input,
                    errors.join("; ")
                ));
            }
        }

        assert!(checked > 0, "no generated Sonarr assertions were checked");
        assert!(
            failures.is_empty(),
            "{} generated Sonarr asserted rows failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn sonarr_rr2_generated_promotion_audit() {
        if std::env::var("ELIXIR_RR2_PROMOTION_AUDIT").ok().as_deref() != Some("1") {
            return;
        }

        let set = load_generated_sonarr_set();
        let mut pass_by_fixture = std::collections::BTreeMap::<String, usize>::new();
        let mut fail_by_fixture = std::collections::BTreeMap::<String, usize>::new();
        let mut failures = Vec::new();

        for case in set
            .cases
            .iter()
            .filter(|case| case.classification == "tv_rr2")
        {
            let errors = generated_case_errors(case);
            if errors.is_empty() {
                *pass_by_fixture.entry(case.fixture.clone()).or_default() += 1;
            } else {
                *fail_by_fixture.entry(case.fixture.clone()).or_default() += 1;
                if failures.len() < 200 {
                    failures.push(format!(
                        "{} {} {} => {}",
                        case.id,
                        case.test_kind,
                        case.input,
                        errors.join("; ")
                    ));
                }
            }
        }

        eprintln!("RR-2 Sonarr promotion audit pass_by_fixture={pass_by_fixture:#?}");
        eprintln!("RR-2 Sonarr promotion audit fail_by_fixture={fail_by_fixture:#?}");
        eprintln!(
            "RR-2 Sonarr promotion audit first_failures:\n{}",
            failures.join("\n")
        );
    }

    #[test]
    fn sonarr_tv_title_goldens() {
        let set =
            load_golden_set::<TitleGolden>(include_str!("fixtures/sonarr_rr2_title_goldens.json"));

        for case in set.cases {
            let parsed = parse_release_title(&case.input);
            assert_eq!(
                parsed.release_kind,
                release_kind(&case.kind),
                "{}: {}",
                case.fixture,
                case.input
            );
            assert_eq!(
                parsed.normalized_series_title.as_deref(),
                case.series_title.as_deref(),
                "{}: {}",
                case.fixture,
                case.input
            );
            assert_eq!(parsed.season_number, case.season, "{}", case.input);
            assert_eq!(parsed.season_end_number, case.season_end, "{}", case.input);
            if let Some(episodes) = case.episodes {
                assert_eq!(parsed.episode_numbers, episodes, "{}", case.input);
            }
            assert_eq!(
                parsed.air_date.as_deref(),
                case.air_date.as_deref(),
                "{}",
                case.input
            );
            if let Some(full_season) = case.full_season {
                assert_eq!(parsed.full_season, full_season, "{}", case.input);
            }
            if let Some(is_mini_series) = case.is_mini_series {
                assert_eq!(parsed.is_mini_series, is_mini_series, "{}", case.input);
            }
            if let Some(special) = case.special {
                assert_eq!(parsed.special, special, "{}", case.input);
            }
            if let Some(is_split_episode) = case.is_split_episode {
                assert_eq!(parsed.is_split_episode, is_split_episode, "{}", case.input);
            }
            if let Some(title_without_year) = case.title_without_year.as_deref() {
                assert_eq!(
                    parsed.series_title_info.title_without_year.as_deref(),
                    Some(title_without_year),
                    "{}",
                    case.input
                );
            }
            if let Some(title_year) = case.title_year {
                assert_eq!(
                    parsed.series_title_info.year,
                    Some(title_year),
                    "{}",
                    case.input
                );
            }
        }
    }

    #[test]
    fn sonarr_tv_path_goldens() {
        let set =
            load_golden_set::<PathGolden>(include_str!("fixtures/sonarr_rr2_path_goldens.json"));

        for case in set.cases {
            let parsed = parse_release_file(&case.input);
            assert_eq!(
                parsed.release_kind,
                release_kind(&case.kind),
                "{}",
                case.input
            );
            assert_eq!(parsed.season_number, Some(case.season), "{}", case.input);
            assert_eq!(parsed.episode_numbers, case.episodes, "{}", case.input);
        }
    }

    #[test]
    fn sonarr_quality_goldens() {
        let set = load_golden_set::<QualityGolden>(include_str!(
            "fixtures/sonarr_rr2_quality_goldens.json"
        ));

        for case in set.cases {
            let parsed = parse_release_title(&case.input);
            assert_eq!(
                debug_name(parsed.quality.resolution),
                case.resolution,
                "{}: {}",
                case.fixture,
                case.input
            );
            assert_eq!(
                debug_name(parsed.quality.source),
                case.source,
                "{}: {}",
                case.fixture,
                case.input
            );
            if let Some(proper) = case.proper {
                assert_eq!(parsed.modifiers.proper, proper, "{}", case.input);
            }
            if let Some(remux) = case.remux {
                assert_eq!(parsed.quality.remux, remux, "{}", case.input);
            }
            if let Some(raw_hd) = case.raw_hd {
                assert_eq!(parsed.quality.raw_hd, raw_hd, "{}", case.input);
            }
        }
    }

    #[test]
    fn sonarr_release_group_goldens() {
        let set = load_golden_set::<ReleaseGroupGolden>(include_str!(
            "fixtures/sonarr_rr2_release_group_goldens.json"
        ));

        for case in set.cases {
            let parsed = parse_release_title(&case.input);
            assert_eq!(
                parsed.release_group.as_deref(),
                case.release_group.as_deref(),
                "{}: {}",
                case.fixture,
                case.input
            );
        }
    }

    #[test]
    fn sonarr_language_goldens() {
        let set = load_golden_set::<LanguageGolden>(include_str!(
            "fixtures/sonarr_rr2_language_goldens.json"
        ));

        for case in set.cases {
            let parsed = parse_release_title(&case.input);
            for expected in case.contains {
                assert!(
                    parsed.modifiers.languages.contains(&expected),
                    "{}: {} missing {expected}; got {:?}",
                    case.fixture,
                    case.input,
                    parsed.modifiers.languages
                );
            }
        }
    }

    #[test]
    fn parses_single_episode_release() {
        let parsed = parse_release_title("Series.Title.S01E01.1080p.WEB-DL-GROUP");

        assert_eq!(parsed.release_kind, ReleaseKind::Single);
        assert_eq!(
            parsed.normalized_series_title.as_deref(),
            Some("Series Title")
        );
        assert_eq!(parsed.season_number, Some(1));
        assert_eq!(parsed.episode_numbers, vec![1]);
        assert_eq!(parsed.quality.resolution, Some(TvResolution::R1080p));
        assert_eq!(parsed.quality.source, Some(TvReleaseSource::WebDl));
        assert_eq!(parsed.release_group.as_deref(), Some("GROUP"));
    }

    #[test]
    fn parses_sonarr_style_multi_episode_variants() {
        let cases = [
            (
                "Series.S03E01-06.DUAL.BDRip.XviD.AC3.-HELLYWOOD",
                "Series",
                3,
                vec![1, 2, 3, 4, 5, 6],
            ),
            (
                "Series.S03E01.S03E02.720p.HDTV.X264-DIMENSION",
                "Series",
                3,
                vec![1, 2],
            ),
            (
                "Series.Title.S07E22E23.720p.HDTV.X264-DIMENSION",
                "Series Title",
                7,
                vec![22, 23],
            ),
            (
                "Series Title.S6.E1-E2.Episode Name.1080p.WEB-DL",
                "Series Title",
                6,
                vec![1, 2],
            ),
            ("S01E01-E03 - Episode Title.HDTV-720p", "", 1, vec![1, 2, 3]),
            (
                "Series Title - [02x01-x02] - Episode 1",
                "Series Title",
                2,
                vec![1, 2],
            ),
            (
                "The Series Title! - S01E01-02-03",
                "The Series Title!",
                1,
                vec![1, 2, 3],
            ),
            (
                "Series.Title.103.104.720p.HDTV.X264-DIMENSION",
                "Series Title",
                1,
                vec![3, 4],
            ),
            ("Series.10708.hdtv-lol.mp4", "Series", 1, vec![7, 8]),
            ("Series.10910.hdtv-lol.mp4", "Series", 1, vec![9, 10]),
            ("E.010910.HDTVx264REPACKLOL.mp4", "E", 1, vec![9, 10]),
            (
                "13 Series Se.1 afl.2-3-4 [VTM]",
                "13 Series",
                1,
                vec![2, 3, 4],
            ),
            ("Series T Se.3 afl.3 en 4", "Series T", 3, vec![3, 4]),
            (
                "Series Title (S15E06-08) City Sushi",
                "Series Title",
                15,
                vec![6, 7, 8],
            ),
            (
                "Series Title (S05E06-08 of 24) City Sushi",
                "Series Title",
                5,
                vec![6, 7, 8],
            ),
        ];

        for (title, expected_title, season, episodes) in cases {
            let parsed = parse_release_title(title);
            assert_eq!(parsed.release_kind, ReleaseKind::MultiEpisode, "{title}");
            assert_eq!(
                parsed
                    .normalized_series_title
                    .as_deref()
                    .unwrap_or_default(),
                expected_title,
                "{title}"
            );
            assert_eq!(parsed.season_number, Some(season), "{title}");
            assert_eq!(parsed.episode_numbers, episodes, "{title}");
        }
    }

    #[test]
    fn parses_season_and_multi_season_packs() {
        let season = parse_release_title("Series.S01.720p.WEBDL.DD5.1.H.264-NTb");
        assert_eq!(season.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(season.normalized_series_title.as_deref(), Some("Series"));
        assert_eq!(season.season_number, Some(1));
        assert!(season.full_season);

        let season_word = parse_release_title("The Series Season 4 WS PDTV XviD FUtV");
        assert_eq!(season_word.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(
            season_word.normalized_series_title.as_deref(),
            Some("The Series")
        );
        assert_eq!(season_word.season_number, Some(4));
        assert_eq!(season_word.quality.source, Some(TvReleaseSource::Pdtv));

        let multi = parse_release_title(
            "Series Title Season 01 - Season 07 BluRay 1080p x264 REPACK -SacReD",
        );
        assert_eq!(multi.release_kind, ReleaseKind::MultiSeasonPack);
        assert_eq!(
            multi.normalized_series_title.as_deref(),
            Some("Series Title")
        );
        assert_eq!(multi.season_number, Some(1));
        assert_eq!(multi.season_end_number, Some(7));
        assert!(multi.modifiers.repack);

        let series = parse_release_title("Series Title Complete Series 1080p BluRay x265-GRP");
        assert_eq!(series.release_kind, ReleaseKind::SeriesPack);
        assert_eq!(
            series.normalized_series_title.as_deref(),
            Some("Series Title")
        );
        assert!(series.full_series);
    }

    #[test]
    fn does_not_parse_plain_season_folder_as_release() {
        let parsed = parse_release_title("Season 3");
        assert_eq!(parsed.release_kind, ReleaseKind::Unknown);
    }

    #[test]
    fn parses_release_metadata() {
        let parsed =
            parse_release_title("Series.S01E01.1080p.WEB-DL.REPACK2.DUAL-Audio.Extended-GROUP");

        assert_eq!(parsed.release_kind, ReleaseKind::Single);
        assert_eq!(parsed.quality.resolution, Some(TvResolution::R1080p));
        assert_eq!(parsed.quality.source, Some(TvReleaseSource::WebDl));
        assert_eq!(parsed.modifiers.version, Some(2));
        assert!(parsed.modifiers.repack);
        assert!(
            parsed
                .modifiers
                .languages
                .contains(&"DUAL-AUDIO".to_string())
        );
        assert!(
            parsed
                .modifiers
                .edition_tags
                .contains(&"extended".to_string())
        );
        assert_eq!(parsed.release_group.as_deref(), Some("GROUP"));
    }

    #[test]
    fn parses_sonarr_single_episode_corpus_slice() {
        let cases = [
            (
                "Series.and.a.Title.103.720p.HDTV.X264-DIMENSION",
                "Series and a Title",
                1,
                3,
            ),
            (
                "Series.and.a.Title.1013.720p.HDTV.X264-DIMENSION",
                "Series and a Title",
                10,
                13,
            ),
            ("Series.Title.525", "Series Title", 5, 25),
            ("Series.Title.S15.E06.City.Sushi", "Series Title", 15, 6),
            ("Series Title - S15 E06 - City Code", "Series Title", 15, 6),
            ("Series S1-E1-WEB-DL-1080p-NZBgeek", "Series", 1, 1),
            (
                "Series.Title.S01.Ep.01.English.AC3.DL.1080p.BluRay-Sonarr",
                "Series Title",
                1,
                1,
            ),
            (
                "John.Smith.The.Series.Title.5of9.The.Universe.Of.Development.1990.DVDRip.x264-HANDJOB",
                "John Smith The Series Title",
                1,
                5,
            ),
            (
                "App.Sonarr.Made.in.Canada.Part.Two.720p.HDTV.x264-2HD",
                "App Sonarr Made in Canada",
                1,
                2,
            ),
        ];

        for (title, expected_title, season, episode) in cases {
            let parsed = parse_release_title(title);
            assert_eq!(parsed.release_kind, ReleaseKind::Single, "{title}");
            assert_eq!(
                parsed.normalized_series_title.as_deref(),
                Some(expected_title),
                "{title}"
            );
            assert_eq!(parsed.season_number, Some(season), "{title}");
            assert_eq!(parsed.episode_numbers, vec![episode], "{title}");
        }
    }

    #[test]
    fn parses_daily_release_corpus_slice() {
        let cases = [
            ("20181012", "", "2018-10-12"),
            (
                "A.Late.Talk.Show.2010.10.11.Johnny.Knoxville.iTouch-MW",
                "A Late Talk Show",
                "2010-10-11",
            ),
            (
                "A Late Talk Show - 2011-04-12 - Gov. Deval Patrick",
                "A Late Talk Show",
                "2011-04-12",
            ),
            (
                "A.Late.Talk.Show.140722.720p.HDTV.x264-YesTV",
                "A Late Talk Show",
                "2014-07-22",
            ),
            (
                "Series Title - 30-04-2024 HDTV 1080p H264 AAC",
                "Series Title",
                "2024-04-30",
            ),
            ("Series 5th Mar 2025 1080 (Deep61)", "Series", "2025-03-05"),
            (
                "Series.Title.2015.09.07.Part.2.720p.HULU.WEBRip.AAC2.0.H.264-Sonarr",
                "Series Title",
                "2015-09-07",
            ),
            (
                "2020.A.Late.Talk.Show.2012.16.02.PDTV.XviD-C4TV",
                "2020 A Late Talk Show",
                "2012-02-16",
            ),
            (
                "The_Series_US_04.28.2014.HDTV.x264-2HD",
                "The Series US",
                "2014-04-28",
            ),
        ];

        for (title, expected_title, air_date) in cases {
            let parsed = parse_release_title(title);
            assert_eq!(parsed.release_kind, ReleaseKind::Single, "{title}");
            if expected_title.is_empty() {
                assert!(parsed.normalized_series_title.is_none(), "{title}");
            } else {
                assert_eq!(
                    parsed.normalized_series_title.as_deref(),
                    Some(expected_title)
                );
            }
            assert_eq!(parsed.air_date.as_deref(), Some(air_date), "{title}");
            assert!(parsed.episode_numbers.is_empty(), "{title}");
        }

        let part = parse_release_title(
            "Series.Title.2015.09.07.Part.2.720p.HULU.WEBRip.AAC2.0.H.264-Sonarr",
        );
        assert_eq!(part.daily_part, Some(2));
    }

    #[test]
    fn preprocesses_sonarr_url_quality_and_korean_date_forms() {
        let prefixed =
            parse_release_title("[www.test-hyphen.com] - Series.S03E14.720p.HDTV.X264-DIMENSION");
        assert_eq!(prefixed.normalized_series_title.as_deref(), Some("Series"));
        assert_eq!(prefixed.season_number, Some(3));
        assert_eq!(prefixed.episode_numbers, vec![14]);

        let quality_bracket = parse_release_title("Mad Series - Season 1 [Bluray720p]");
        assert_eq!(quality_bracket.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(
            quality_bracket.normalized_series_title.as_deref(),
            Some("Mad Series")
        );
        assert_eq!(quality_bracket.season_number, Some(1));

        let postfixed =
            parse_release_title("Series.2009.S01E14.English.HDTV.XviD-LOL[www.abb.com]");
        assert_eq!(postfixed.release_kind, ReleaseKind::Single);
        assert_eq!(postfixed.release_group.as_deref(), Some("LOL"));

        let korean = parse_release_title("It's a Series Title.E56.190121.720p-NEXT.mp4");
        assert_eq!(korean.release_kind, ReleaseKind::Single);
        assert_eq!(
            korean.normalized_series_title.as_deref(),
            Some("It's a Series Title")
        );
        assert_eq!(korean.season_number, Some(1));
        assert_eq!(korean.episode_numbers, vec![56]);
        assert_eq!(korean.air_date.as_deref(), Some("2019-01-21"));
    }

    #[test]
    fn rejects_sonarr_crap_hash_and_bare_folder_titles() {
        let cases = [
            "123",
            "abc",
            "b00bs",
            "170424_26",
            "aaaaaaaaaaaaaaaaaaaaaaaa",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456",
            "Season 3",
            "Specials",
            "password-yenc",
        ];

        for title in cases {
            let parsed = parse_release_title(title);
            assert_eq!(parsed.release_kind, ReleaseKind::Unknown, "{title}");
            assert!(parsed.season_number.is_none(), "{title}");
            assert!(parsed.episode_numbers.is_empty(), "{title}");
        }
    }

    #[test]
    fn daily_release_maps_by_air_date() {
        let resolver = TvSonarrStyleResolver;
        let parsed = resolver.parse_title("A Late Talk Show - 2011-04-12 - Gov. Deval Patrick");
        let wanted = vec![TvTarget {
            target_id: Uuid::from_u128(42),
            target_key: "2011-04-12".to_string(),
            season_number: 2011,
            episode_number: 412,
            air_date: Some("2011-04-12".to_string()),
        }];

        let plan = resolver.plan_coverage(&parsed, &wanted, &[], TvCoverageOptions::default());

        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.entries.len(), 1);
        assert_eq!(plan.entries[0].target_key, "2011-04-12");
    }

    #[test]
    fn parses_partial_season_and_extras() {
        let partial = parse_release_title("The.Series.S07.Vol.1.1080p.NF.WEBRip.DD5.1.x264-NTb");
        assert_eq!(partial.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(partial.season_number, Some(7));
        assert!(!partial.full_season);
        assert!(partial.is_partial_season);
        assert_eq!(partial.season_part, Some(1));

        let extras = parse_release_title("Punky Series S01 EXTRAS DVDRip XviD RUNNER");
        assert_eq!(extras.release_kind, ReleaseKind::SeasonPack);
        assert!(extras.is_season_extra);

        let subpack = parse_release_title("The.Series.S02.SUBPACK.DVDRip.XviD-REWARD");
        assert_eq!(subpack.release_kind, ReleaseKind::SeasonPack);
        assert!(subpack.is_season_extra);
    }

    #[test]
    fn parses_cap_temporada_releases() {
        let cases = [
            (
                "Series Title - Temporada 2 [HDTV 720p][Cap.201][AC3 5.1 Castellano][www.pctnew.com]",
                2,
                vec![1],
            ),
            (
                "Series Title - Temporada 2 [HDTV 720p][Cap.1901][AC3 5.1 Castellano][www.pctnew.com]",
                19,
                vec![1],
            ),
            (
                "Series [HDTV 1080p][Cap. 101](wolfmax4k.com).mkv",
                1,
                vec![1],
            ),
            (
                "Series falls - Temporada 1 [HDTV][Cap.111_120]",
                1,
                (11..=20).collect::<Vec<_>>(),
            ),
        ];

        for (title, season, episodes) in cases {
            let parsed = parse_release_title(title);
            assert_eq!(parsed.season_number, Some(season), "{title}");
            assert_eq!(parsed.episode_numbers, episodes, "{title}");
        }
    }

    #[test]
    fn s01e01_maps_one_target() {
        let resolver = TvSonarrStyleResolver;
        let parsed = resolver.parse_title("Series.S01E01.1080p.WEB-DL-GROUP");
        let plan = resolver.plan_coverage(
            &parsed,
            &targets(1, 1..=3),
            &[],
            TvCoverageOptions::default(),
        );

        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.entries.len(), 1);
        assert_eq!(plan.entries[0].target_key, "S01E01");
        assert!(plan.rejection_reasons.is_empty());
    }

    #[test]
    fn s01e01_e03_maps_three_targets() {
        let resolver = TvSonarrStyleResolver;
        let parsed = resolver.parse_title("Series.S01E01-E03.1080p.WEB-DL-GROUP");
        let plan = resolver.plan_coverage(
            &parsed,
            &targets(1, 1..=3),
            &[],
            TvCoverageOptions::default(),
        );

        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S01E01", "S01E02", "S01E03"]
        );
    }

    #[test]
    fn season_pack_waits_for_file_list_then_maps_targets() {
        let resolver = TvSonarrStyleResolver;
        let parsed = resolver.parse_title("Series.S01.COMPLETE.1080p.BluRay-GROUP");
        let wanted = targets(1, 1..=3);
        let preliminary =
            resolver.plan_coverage(&parsed, &wanted, &[], TvCoverageOptions::default());

        assert_eq!(preliminary.confidence, ReleaseConfidence::ReviewRequired);
        assert!(preliminary.requires_file_list);
        assert!(preliminary.entries.is_empty());
        assert!(
            preliminary
                .rejection_reasons
                .contains(&TvRejectionReason::FileListRequired)
        );

        let refined = resolver.plan_coverage(
            &parsed,
            &wanted,
            &[
                file("1", "Series.S01.COMPLETE/Series.S01E01.1080p.mkv"),
                file("2", "Series.S01.COMPLETE/Series.S01E02.1080p.mkv"),
                file("3", "Series.S01.COMPLETE/Series.S01E03.1080p.mkv"),
            ],
            TvCoverageOptions::default(),
        );

        assert_eq!(refined.confidence, ReleaseConfidence::High);
        assert_eq!(refined.entries.len(), 3);
        assert_eq!(
            refined
                .entries
                .iter()
                .map(|entry| entry.release_file_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2"), Some("3")]
        );
    }

    #[test]
    fn season_pack_with_unmapped_media_file_requires_review() {
        let resolver = TvSonarrStyleResolver;
        let parsed = resolver.parse_title("Series.S01.COMPLETE.1080p.BluRay-GROUP");
        let plan = resolver.plan_coverage(
            &parsed,
            &targets(1, 1..=2),
            &[
                file("1", "Series.S01.COMPLETE/Series.S01E01.1080p.mkv"),
                file("2", "Series.S01.COMPLETE/Bonus Feature.mkv"),
            ],
            TvCoverageOptions::default(),
        );

        assert_eq!(plan.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            plan.rejection_reasons
                .contains(&TvRejectionReason::UnmappedMediaFile)
        );
    }

    #[test]
    fn multi_season_pack_requires_file_selection() {
        let resolver = TvSonarrStyleResolver;
        let parsed = resolver.parse_title("Series.Title.S01-S03.1080p.BluRay-GROUP");
        let plan = resolver.plan_coverage(
            &parsed,
            &targets(1, 1..=3),
            &[],
            TvCoverageOptions::default(),
        );

        assert_eq!(plan.release_kind, ReleaseKind::MultiSeasonPack);
        assert_eq!(plan.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            plan.rejection_reasons
                .contains(&TvRejectionReason::FileSelectionRequired)
        );
    }

    #[test]
    fn unnumbered_ambiguous_pack_is_review_required() {
        let resolver = TvSonarrStyleResolver;
        let parsed = resolver.parse_title("Series Complete 1080p BluRay-GROUP");
        let plan = resolver.plan_coverage(
            &parsed,
            &targets(1, 1..=3),
            &[],
            TvCoverageOptions::default(),
        );

        assert_eq!(parsed.release_kind, ReleaseKind::Unknown);
        assert_eq!(plan.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            plan.rejection_reasons
                .contains(&TvRejectionReason::UnknownNumbering)
        );
    }

    #[test]
    fn parses_season_folder_file_names() {
        let parsed = parse_release_file(r"C:\Test\Series\Season 01\01 Pilot (1080p HD).mkv");
        assert_eq!(parsed.release_kind, ReleaseKind::Single);
        assert_eq!(parsed.season_number, Some(1));
        assert_eq!(parsed.episode_numbers, vec![1]);

        let multi = parse_release_file(r"Season 2\E05-06 - Episode Title HDTV-720p Proper.mkv");
        assert_eq!(multi.season_number, Some(2));
        assert_eq!(multi.episode_numbers, vec![5, 6]);
    }
}
